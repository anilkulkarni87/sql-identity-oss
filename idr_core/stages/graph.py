"""
Graph processing stage for IDR pipeline.

Handles edge building, label propagation (deterministic connected components),
and fuzzy matching (probabilistic post-processing).
"""

import re
from typing import Dict

from .base import BaseStage


class GraphStage(BaseStage):
    """Builds edges and resolves connected components via label propagation."""

    def build_edges(self) -> int:
        """Build identity edges using anchor-based approach."""
        # This uses the N-1 edge approach (anchor-based) instead of N² fully connected
        self.adapter.execute("""
            CREATE OR REPLACE TABLE idr_work.edges_new AS
            WITH identifier_groups AS (
                SELECT
                    identifier_type,
                    identifier_value_norm,
                    COUNT(*) as group_size,
                    MIN(entity_key) as anchor_key,
                    MAX(max_group_size) as max_allowed
                FROM idr_work.identifiers_all
                GROUP BY identifier_type, identifier_value_norm
                HAVING COUNT(*) > 1
            ),
            valid_groups AS (
                SELECT * FROM identifier_groups
                WHERE group_size <= max_allowed
            ),
            skipped_groups AS (
                SELECT * FROM identifier_groups
                WHERE group_size > max_allowed
            )
            SELECT
                i.entity_key AS left_entity_key,
                g.anchor_key AS right_entity_key,
                i.identifier_type,
                i.identifier_value_norm
            FROM idr_work.identifiers_all i
            JOIN valid_groups g
                ON i.identifier_type = g.identifier_type
                AND i.identifier_value_norm = g.identifier_value_norm
            WHERE i.entity_key != g.anchor_key
        """)

        # Log skipped groups (optional - table may not exist)
        try:
            self.adapter.execute(f"""
                INSERT INTO idr_out.skipped_identifier_groups
                (run_id, identifier_type, identifier_value_norm, group_size, max_allowed, sample_entity_keys, reason, skipped_at)
                SELECT
                    '{self.run_id}' as run_id,
                    identifier_type,
                    identifier_value_norm,
                    group_size,
                    max_allowed,
                    '' as sample_entity_keys,
                    'EXCEEDED_MAX_GROUP_SIZE' as reason,
                    {self._dialect["current_timestamp"]} as skipped_at
                FROM (
                    SELECT
                        identifier_type,
                        identifier_value_norm,
                        COUNT(*) as group_size,
                        MAX(max_group_size) as max_allowed
                    FROM idr_work.identifiers_all
                    GROUP BY identifier_type, identifier_value_norm
                    HAVING COUNT(*) > MAX(max_group_size)
                ) skipped
            """)
        except Exception:
            pass  # Table may have different schema

        count = self.adapter.query_one("SELECT COUNT(*) FROM idr_work.edges_new")

        # Generate edge evidence if enabled
        if self._generate_evidence and count and count > 0:
            try:
                # Use CONCAT for BigQuery/Databricks, || for DuckDB/Snowflake
                if self.adapter.dialect in ("bigquery", "databricks"):
                    rule_id_expr = "CONCAT('exact_', identifier_type)"
                else:
                    rule_id_expr = "'exact_' || identifier_type"

                self.adapter.execute(f"""
                    INSERT INTO idr_out.edge_evidence
                    (run_id, entity_key_a, entity_key_b, rule_id, identifier_type,
                     match_value, score, created_at)
                    SELECT
                        '{self.run_id}' as run_id,
                        left_entity_key as entity_key_a,
                        right_entity_key as entity_key_b,
                        {rule_id_expr} as rule_id,
                        identifier_type,
                        identifier_value_norm as match_value,
                        1.0 as score,
                        {self._dialect["current_timestamp"]} as created_at
                    FROM idr_work.edges_new
                """)
            except Exception as e:
                self._warnings.append(f"Edge evidence generation failed: {e}")

        return count or 0

    def label_propagation(self, max_iters: int) -> tuple:
        """Run label propagation to find connected components."""
        # Initialize labels (each entity starts with itself as label)
        # MUST include all entities from the current run, including singletons.
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.lp_labels AS
            SELECT DISTINCT entity_key, entity_key AS label
            FROM idr_work.entities_delta
            WHERE run_id = '{self.run_id}'
        """)

        # Build undirected edge list
        self.adapter.execute("""
            CREATE OR REPLACE TABLE idr_work.lp_edges AS
            SELECT left_entity_key AS src, right_entity_key AS dst FROM idr_work.edges_new
            UNION ALL
            SELECT right_entity_key AS src, left_entity_key AS dst FROM idr_work.edges_new
        """)

        # Iterative label propagation
        iterations = 0
        for i in range(max_iters):
            iterations = i + 1

            # Propagate minimum label
            self.adapter.execute("""
                CREATE OR REPLACE TABLE idr_work.lp_labels_new AS
                WITH candidate_labels AS (
                    SELECT l.entity_key, l.label AS candidate_label
                    FROM idr_work.lp_labels l
                    UNION ALL
                    SELECT e.src AS entity_key, l2.label AS candidate_label
                    FROM idr_work.lp_edges e
                    JOIN idr_work.lp_labels l2 ON e.dst = l2.entity_key
                )
                SELECT entity_key, MIN(candidate_label) AS label
                FROM candidate_labels
                GROUP BY entity_key
            """)

            # Check for convergence
            changes = self.adapter.query_one("""
                SELECT COUNT(*) FROM idr_work.lp_labels prev
                JOIN idr_work.lp_labels_new curr ON prev.entity_key = curr.entity_key
                WHERE prev.label != curr.label
            """)

            # Swap tables
            self.adapter.execute("DROP TABLE IF EXISTS idr_work.lp_labels")

            # Snowflake requires fully qualified name to keep table in same schema (if not current),
            # while DuckDB/Postgres require unqualified name for rename.
            rename_target = (
                "idr_work.lp_labels" if self.adapter.dialect == "snowflake" else "lp_labels"
            )
            self.adapter.execute(f"ALTER TABLE idr_work.lp_labels_new RENAME TO {rename_target}")

            if changes == 0:
                break

        # Count unique clusters
        clusters = self.adapter.query_one("SELECT COUNT(DISTINCT label) FROM idr_work.lp_labels")

        return iterations, clusters or 0

    def run_fuzzy_matching(self, max_iters: int) -> int:
        """
        Run fuzzy matching on clusters (Post-Processing).

        Refactored for robustness:
        - Strict attribute parsing uses regex tokens (no substring errors)
        - Safe attribute aliasing (e.g. attr_1) prevents keyword collisions
        - Deterministic pairing via MD5 hash comparison
        - Mandatory <a>/<b> placeholders in score expressions
        """
        rules = self.adapter.query(
            "SELECT * FROM idr_meta.fuzzy_rule WHERE is_active = TRUE ORDER BY priority"
        )

        # Dialect helpers
        dialect = self.adapter.dialect
        string_type = self._dialect["string_type"]
        float_type = "FLOAT64" if dialect == "bigquery" else "DOUBLE"
        md5_func = self._dialect["md5"]

        # Ensure output table exists even if no rules
        self.adapter.execute(
            f"CREATE OR REPLACE TABLE idr_work.fuzzy_results (resolved_id {string_type}, super_cluster_id {string_type})"
        )

        if not rules:
            return 0

        # Validate all fuzzy rule metadata before SQL interpolation
        for rule in rules:
            rule_id = rule.get("rule_id", "unknown")
            self._validate_metadata_value(rule_id, "identifier", "fuzzy_rule.rule_id")
            self._validate_metadata_value(
                rule["blocking_key_expr"], "expr", f"fuzzy_rule[{rule_id}].blocking_key_expr"
            )
            self._validate_metadata_value(
                rule["score_expr"], "expr", f"fuzzy_rule[{rule_id}].score_expr"
            )
            self._validate_metadata_value(
                rule["threshold"], "float", f"fuzzy_rule[{rule_id}].threshold"
            )

        # 0. Get known attributes
        known_attrs = set(
            row["attribute_name"].lower()
            for row in self.adapter.query(
                "SELECT DISTINCT attribute_name FROM idr_meta.entity_attribute_mapping"
            )
        )

        # 1. Prepare Cluster Attributes (All distinct values per cluster)
        self.adapter.execute("""
            CREATE OR REPLACE TABLE idr_work.cluster_attributes AS
            SELECT DISTINCT
                l.label AS resolved_id,
                LOWER(a.attribute_name) as attribute_name,
                a.attribute_value
            FROM idr_work.lp_labels l
            JOIN idr_work.entity_attributes a ON l.entity_key = a.entity_key
        """)

        # Initialize edges table
        self.adapter.execute(
            f"CREATE OR REPLACE TABLE idr_work.fuzzy_edges (left_create_id {string_type}, right_create_id {string_type}, score {float_type})"
        )

        for rule in rules:
            rule_id = rule.get("rule_id", "unknown")
            blocking_expr = rule["blocking_key_expr"]
            score_expr = rule["score_expr"]
            threshold = rule["threshold"]

            # --- Validations ---
            if "<a>" in blocking_expr or "<b>" in blocking_expr:
                self._warnings.append(
                    f"Rule {rule_id}: Blocking expression must not use <a>/<b> placeholders. It acts on single entities. Skipping."
                )
                continue

            if "<a>." not in score_expr or "<b>." not in score_expr:
                self._warnings.append(
                    f"Rule {rule_id}: Score expression must use '<a>.<attr>' and '<b>.<attr>' syntax. Skipping."
                )
                continue

            # --- Robust Parsing ---
            # Extract all identifiers using regex
            tokens = set(re.findall(r"[a-zA-Z_]\w*", f"{blocking_expr} {score_expr}"))
            # Normalize tokens to lowercase for matching
            tokens_lower = {t.lower() for t in tokens}

            # Identify referenced attributes
            referenced_attrs = sorted(list(known_attrs & tokens_lower))

            if not referenced_attrs:
                self._warnings.append(
                    f"Rule {rule_id}: No known attributes found in expressions. Skipping."
                )
                continue

            # --- Safe Aliasing ---
            # Map attribute_name -> safe_alias (attr_0, attr_1...)
            attr_map = {name: f"attr_{i}" for i, name in enumerate(referenced_attrs)}

            # Rewrite expressions with safe aliases
            def replace_with_alias(text: str, amap: Dict[str, str]) -> str:
                # Iterate by length desc
                sorted_keys = sorted(amap.keys(), key=len, reverse=True)
                for name in sorted_keys:
                    # Case insensitive replacement of whole words
                    pattern = re.compile(r"\b" + re.escape(name) + r"\b", re.IGNORECASE)
                    text = pattern.sub(amap[name], text)
                return text

            blocking_expr_safe = replace_with_alias(blocking_expr, attr_map)
            score_expr_safe = replace_with_alias(score_expr, attr_map)

            # Replace placeholders
            score_expr_final = score_expr_safe.replace("<a>", "a").replace("<b>", "b")

            # --- Rule-Scoped Profile ---
            # Dynamic pivot with safe aliases
            pivot_cols = ",\n                ".join(
                [
                    f"MAX(CASE WHEN attribute_name = '{name}' THEN attribute_value END) AS {alias}"
                    for name, alias in attr_map.items()
                ]
            )

            # Explicit column list for SELECT to avoid duplicates with *
            # resolved_id + aliases
            select_cols = ", ".join(["resolved_id"] + list(attr_map.values()))

            profile_table = (
                f"idr_work.fuzzy_profile_rule_{rule_id}"
                if str(rule_id).isalnum()
                else "idr_work.fuzzy_profile_temp"
            )

            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE {profile_table} AS
                SELECT
                    resolved_id,
                    {pivot_cols}
                FROM idr_work.cluster_attributes
                WHERE attribute_name IN ('{"', '".join(referenced_attrs)}')
                GROUP BY resolved_id
            """)

            max_block_size = 10000

            # --- Deterministic Pairing ---
            id_compare = f"{md5_func('a.resolved_id')} < {md5_func('b.resolved_id')}"

            self.logger.info(
                f"   ► Processing Rule {rule_id}...",
                extra={"run_id": self.run_id, "stage": "fuzzy_matching", "rule_id": rule_id},
            )

            self.adapter.execute(f"""
                INSERT INTO idr_work.fuzzy_edges
                WITH profile_with_bk AS (
                    SELECT
                        {select_cols},
                        {blocking_expr_safe} AS bk
                    FROM {profile_table}
                    WHERE {blocking_expr_safe} IS NOT NULL
                ),
                block_sizes AS (
                    SELECT bk, COUNT(*) as cnt
                    FROM profile_with_bk
                    GROUP BY bk
                ),
                valid_blocks AS (
                    SELECT bk FROM block_sizes WHERE cnt <= {max_block_size}
                ),
                scored_pairs AS (
                    SELECT
                        a.resolved_id AS left_id,
                        b.resolved_id AS right_id,
                        {score_expr_final} AS score
                    FROM profile_with_bk a
                    JOIN profile_with_bk b ON a.bk = b.bk AND {id_compare}
                    JOIN valid_blocks vb ON a.bk = vb.bk
                )
                SELECT left_id, right_id, score
                FROM scored_pairs
                WHERE score >= {threshold}
            """)

            self.adapter.execute(f"DROP TABLE {profile_table}")

        # 3. Label Propagation for Super Clusters
        self.adapter.execute("""
            CREATE OR REPLACE TABLE idr_work.lp_fuzzy_labels AS
            SELECT DISTINCT resolved_id AS entity_key, resolved_id AS label
            FROM idr_work.cluster_attributes
        """)

        for i in range(max_iters):
            self.adapter.execute("""
                CREATE OR REPLACE TABLE idr_work.lp_fuzzy_labels_new AS
                WITH candidate_labels AS (
                    SELECT l.entity_key, l.label AS candidate_label
                    FROM idr_work.lp_fuzzy_labels l
                    UNION ALL
                    SELECT e.left_create_id AS entity_key, l2.label AS candidate_label
                    FROM idr_work.fuzzy_edges e
                    JOIN idr_work.lp_fuzzy_labels l2 ON e.right_create_id = l2.entity_key
                    UNION ALL
                    SELECT e.right_create_id AS entity_key, l2.label AS candidate_label
                    FROM idr_work.fuzzy_edges e
                    JOIN idr_work.lp_fuzzy_labels l2 ON e.left_create_id = l2.entity_key
                )
                SELECT entity_key, MIN(candidate_label) AS label
                FROM candidate_labels
                GROUP BY entity_key
            """)

            changes = self.adapter.query_one("""
                SELECT COUNT(*) FROM idr_work.lp_fuzzy_labels prev
                JOIN idr_work.lp_fuzzy_labels_new curr ON prev.entity_key = curr.entity_key
                WHERE prev.label != curr.label
             """)

            self.adapter.execute("DROP TABLE idr_work.lp_fuzzy_labels")

            rename_target = (
                "idr_work.lp_fuzzy_labels" if dialect == "snowflake" else "lp_fuzzy_labels"
            )
            self.adapter.execute(
                f"ALTER TABLE idr_work.lp_fuzzy_labels_new RENAME TO {rename_target}"
            )

            if changes == 0:
                break

        # 4. Finalize
        self.adapter.execute("""
            CREATE OR REPLACE TABLE idr_work.fuzzy_results AS
            SELECT entity_key as resolved_id, label as super_cluster_id
            FROM idr_work.lp_fuzzy_labels
        """)

        count = self.adapter.query_one("SELECT COUNT(DISTINCT label) FROM idr_work.lp_fuzzy_labels")
        return count or 0
