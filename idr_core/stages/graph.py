"""
Graph processing stage for IDR pipeline.

Handles edge building, label propagation (deterministic connected components),
and fuzzy matching (probabilistic post-processing).
"""

import re
from typing import Dict

from ..exceptions import GraphNotConverged
from .base import BaseStage, StageContext


class GraphStage(BaseStage):
    """Builds edges and resolves connected components via label propagation."""

    def __init__(self, ctx: StageContext, config=None):
        super().__init__(ctx)
        self.config = config
        self.run_mode = config.run_mode if config else "FULL"

    def _swap_tables(self, base_table: str, new_table: str) -> None:
        """Safely swap new_table into base_table across dialects (BigQuery lacks ALTER TABLE RENAME)."""
        if self.adapter.dialect == "bigquery":
            self.adapter.execute(
                f"CREATE OR REPLACE TABLE {base_table} AS SELECT * FROM {new_table}"
            )
            self.adapter.execute(f"DROP TABLE IF EXISTS {new_table}")
        else:
            self.adapter.execute(f"DROP TABLE IF EXISTS {base_table}")
            rename_target = (
                base_table
                if self.adapter.dialect in ("snowflake", "databricks")
                else base_table.split(".")[-1]
            )
            self.adapter.execute(f"ALTER TABLE {new_table} RENAME TO {rename_target}")

    def _apply_pre_edge_overrides(self) -> None:
        """Remove ISOLATE entities from identifiers_all before anchor edge grouping."""
        try:
            if not self.adapter.table_exists("idr_meta.identity_override"):
                return
            iso_cnt = (
                self.adapter.query_one("""
                SELECT COUNT(*) FROM idr_meta.identity_override
                WHERE is_active = TRUE AND UPPER(override_type) = 'ISOLATE'
            """)
                or 0
            )
            if iso_cnt > 0:
                self.adapter.execute("""
                    DELETE FROM idr_work.identifiers_all
                    WHERE entity_key IN (
                        SELECT entity_key_a FROM idr_meta.identity_override
                        WHERE is_active = TRUE AND UPPER(override_type) = 'ISOLATE'
                    )
                """)
        except Exception as e:
            self.logger.warning(f"Failed to apply ISOLATE overrides: {e}")

    def _apply_post_edge_overrides(self) -> None:
        """Apply DO_NOT_LINK edge exclusions and FORCE_LINK manual edges."""
        try:
            if not self.adapter.table_exists("idr_meta.identity_override"):
                return
            ov_cnt = (
                self.adapter.query_one("""
                SELECT COUNT(*) FROM idr_meta.identity_override WHERE is_active = TRUE
            """)
                or 0
            )
            if ov_cnt == 0:
                return

            # 1. DO_NOT_LINK: remove direct edges and shared-anchor edges between blocked pairs
            dnl_rows = self.adapter.query("""
                SELECT entity_key_a, entity_key_b
                FROM idr_meta.identity_override
                WHERE is_active = TRUE AND UPPER(override_type) = 'DO_NOT_LINK'
            """)
            for row in dnl_rows:
                a = self._validate_metadata_value(
                    row["entity_key_a"], "expr", "override.entity_key_a"
                )
                b = self._validate_metadata_value(
                    row["entity_key_b"], "expr", "override.entity_key_b"
                )
                high_key = max(a, b)
                # Remove direct edge between a and b
                self.adapter.execute(f"""
                    DELETE FROM idr_work.edges_new
                    WHERE (left_entity_key = '{a}' AND right_entity_key = '{b}')
                       OR (left_entity_key = '{b}' AND right_entity_key = '{a}')
                """)
                # Detach high_key from the true anchor of any shared identifier group containing both low_key and high_key
                low_key = min(a, b)
                self.adapter.execute(f"""
                    DELETE FROM idr_work.edges_new
                    WHERE left_entity_key = '{high_key}'
                      AND right_entity_key IN (
                          SELECT MIN(i.entity_key)
                          FROM idr_work.identifiers_all i
                          JOIN (
                              SELECT identifier_type, identifier_value_norm
                              FROM idr_work.identifiers_all
                              WHERE entity_key IN ('{low_key}', '{high_key}')
                              GROUP BY identifier_type, identifier_value_norm
                              HAVING COUNT(DISTINCT entity_key) = 2
                          ) shared ON i.identifier_type = shared.identifier_type
                                  AND i.identifier_value_norm = shared.identifier_value_norm
                          GROUP BY i.identifier_type, i.identifier_value_norm
                      )
                """)

            # 2. FORCE_LINK: inject explicit edges and ensure entities exist in entities_delta
            fl_rows = self.adapter.query("""
                SELECT entity_key_a, entity_key_b, COALESCE(reason, 'FORCE_LINK') AS reason
                FROM idr_meta.identity_override
                WHERE is_active = TRUE AND UPPER(override_type) = 'FORCE_LINK'
            """)
            for row in fl_rows:
                a = self._validate_metadata_value(
                    row["entity_key_a"], "expr", "override.entity_key_a"
                )
                b = self._validate_metadata_value(
                    row["entity_key_b"], "expr", "override.entity_key_b"
                )
                if a == b:
                    continue
                left_k, right_k = (a, b) if a > b else (b, a)
                self.adapter.execute(f"""
                    INSERT INTO idr_work.edges_new (left_entity_key, right_entity_key, identifier_type, identifier_value_norm)
                    SELECT '{left_k}', '{right_k}', 'MANUAL_OVERRIDE', 'FORCE_LINK'
                    WHERE NOT EXISTS (
                        SELECT 1 FROM idr_work.edges_new
                        WHERE left_entity_key = '{left_k}' AND right_entity_key = '{right_k}'
                    )
                """)
                for ekey in (a, b):
                    tbl_prefix = ekey.split(":", 1)[0] if ":" in ekey else "manual"
                    self.adapter.execute(f"""
                        INSERT INTO idr_work.entities_delta (run_id, table_id, entity_key, watermark_value)
                        SELECT '{self.run_id}', '{tbl_prefix}', '{ekey}', NULL
                        WHERE NOT EXISTS (
                            SELECT 1 FROM idr_work.entities_delta WHERE entity_key = '{ekey}'
                        )
                    """)
        except Exception as e:
            self.logger.warning(f"Failed to apply post-edge overrides: {e}")

    def _has_multiple_entity_kinds(self) -> bool:
        """Return True when >= 2 distinct semantic entity_kinds were detected on active sources."""
        return bool(getattr(self.ctx, "has_multiple_entity_kinds", False))

    def build_edges(self) -> int:
        """Build identity edges using anchor-based approach."""
        self._apply_pre_edge_overrides()

        max_allowed_expr = (
            f"LEAST(COALESCE(MAX(max_group_size), 10000), {int(self.config.max_group_size)})"
            if getattr(self.config, "max_group_size", None)
            else "COALESCE(MAX(max_group_size), 10000)"
        )

        # This uses the N-1 edge approach (anchor-based) instead of N² fully connected.
        # Deduplicate (entity_key, identifier_type, identifier_value_norm) first so multiple
        # upstream mappings for the same member cannot emit duplicate edges or inflate counts.
        edges_cluster = (
            "CLUSTER BY left_entity_key, right_entity_key, identifier_type "
            if self.adapter.dialect == "bigquery"
            else ""
        )
        multi_kind = self._has_multiple_entity_kinds()
        if multi_kind:
            src_prefix_expr = (
                "SPLIT(i.entity_key, ':')[SAFE_OFFSET(0)]"
                if self.adapter.dialect == "bigquery"
                else "SPLIT_PART(i.entity_key, ':', 1)"
            )
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.edges_new {edges_cluster}AS
                WITH dedup_identifiers AS (
                    SELECT
                        i.entity_key,
                        LOWER(COALESCE(NULLIF(TRIM(st.entity_kind), ''), 'person')) AS entity_kind,
                        i.identifier_type,
                        i.identifier_value_norm,
                        MAX(i.max_group_size) AS max_group_size
                    FROM idr_work.identifiers_all i
                    LEFT JOIN idr_meta.source_table st
                        ON st.table_id = {src_prefix_expr}
                    WHERE i.identifier_value_norm IS NOT NULL
                      AND LENGTH(TRIM(i.identifier_value_norm)) > 0
                    GROUP BY
                        i.entity_key,
                        LOWER(COALESCE(NULLIF(TRIM(st.entity_kind), ''), 'person')),
                        i.identifier_type,
                        i.identifier_value_norm
                ),
                identifier_groups AS (
                    SELECT
                        entity_kind,
                        identifier_type,
                        identifier_value_norm,
                        COUNT(*) as group_size,
                        MIN(entity_key) as anchor_key,
                        {max_allowed_expr} as max_allowed
                    FROM dedup_identifiers
                    GROUP BY entity_kind, identifier_type, identifier_value_norm
                    HAVING COUNT(*) > 1
                ),
                valid_groups AS (
                    SELECT * FROM identifier_groups
                    WHERE group_size <= max_allowed
                )
                SELECT
                    i.entity_key AS left_entity_key,
                    g.anchor_key AS right_entity_key,
                    i.identifier_type,
                    i.identifier_value_norm
                FROM dedup_identifiers i
                JOIN valid_groups g
                    ON i.entity_kind = g.entity_kind
                    AND i.identifier_type = g.identifier_type
                    AND i.identifier_value_norm = g.identifier_value_norm
                WHERE i.entity_key != g.anchor_key
            """)
        else:
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.edges_new {edges_cluster}AS
                WITH dedup_identifiers AS (
                    SELECT
                        entity_key,
                        identifier_type,
                        identifier_value_norm,
                        MAX(max_group_size) AS max_group_size
                    FROM idr_work.identifiers_all
                    WHERE identifier_value_norm IS NOT NULL
                      AND LENGTH(TRIM(identifier_value_norm)) > 0
                    GROUP BY entity_key, identifier_type, identifier_value_norm
                ),
                identifier_groups AS (
                    SELECT
                        identifier_type,
                        identifier_value_norm,
                        COUNT(*) as group_size,
                        MIN(entity_key) as anchor_key,
                        {max_allowed_expr} as max_allowed
                    FROM dedup_identifiers
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
                FROM dedup_identifiers i
                JOIN valid_groups g
                    ON i.identifier_type = g.identifier_type
                    AND i.identifier_value_norm = g.identifier_value_norm
                WHERE i.entity_key != g.anchor_key
            """)

        self._apply_post_edge_overrides()

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
                        {max_allowed_expr} as max_allowed
                    FROM idr_work.identifiers_all
                    GROUP BY identifier_type, identifier_value_norm
                    HAVING COUNT(*) > {max_allowed_expr}
                ) skipped
            """)
        except Exception:
            pass  # Table may have different schema

        count = self.adapter.query_one("SELECT COUNT(*) FROM idr_work.edges_new")

        # Generate edge evidence if enabled (audit trail for why edges matched)
        if (
            self._generate_evidence
            and count
            and count > 0
            and not (self.config and self.config.dry_run)
        ):
            try:
                if self.run_mode == "FULL":
                    self.adapter.execute("DELETE FROM idr_out.edge_evidence WHERE 1=1")

                # Use CONCAT for BigQuery/Databricks, || for DuckDB/Snowflake
                if self.adapter.dialect in ("bigquery", "databricks"):
                    fallback_rule_expr = "CONCAT('exact_', e.identifier_type)"
                else:
                    fallback_rule_expr = "'exact_' || e.identifier_type"

                float_type = self._dialect.get("float_type", "DOUBLE")
                self.adapter.execute(f"""
                    INSERT INTO idr_out.edge_evidence
                    (run_id, entity_key_a, entity_key_b, rule_id, identifier_type,
                     match_value, score, created_at)
                    SELECT
                        '{self.run_id}' as run_id,
                        e.left_entity_key as entity_key_a,
                        e.right_entity_key as entity_key_b,
                        COALESCE(r.rule_id, {fallback_rule_expr}) as rule_id,
                        e.identifier_type,
                        e.identifier_value_norm as match_value,
                        CAST(1.0 AS {float_type}) as score,
                        {self._dialect["current_timestamp"]} as created_at
                    FROM idr_work.edges_new e
                    LEFT JOIN (
                        SELECT identifier_type, MIN(rule_id) AS rule_id
                        FROM idr_meta.rule
                        WHERE is_active = TRUE
                        GROUP BY identifier_type
                    ) r ON LOWER(e.identifier_type) = LOWER(r.identifier_type)
                """)
            except Exception as e:
                self._warnings.append(f"Edge evidence generation failed: {e}")

        return count or 0

    def label_propagation(self, max_iters: int) -> tuple:
        """Run label propagation to find connected components."""
        # Build undirected edge list
        # If running in incremental mode and historical edges exist,
        # preserve existing edges between candidate entities in delta batch.
        existing_edges_sql = ""
        if self.run_mode == "INCR":
            try:
                edge_count = self.adapter.query_one(
                    "SELECT COUNT(*) FROM idr_out.identity_edges_current"
                )
                if edge_count and edge_count > 0:
                    reextracted_filter = ""
                    if self.adapter.table_exists("idr_work.reextracted_entities"):
                        reextracted_filter = """
                          AND NOT EXISTS (
                              SELECT 1 FROM idr_work.reextracted_entities r
                              WHERE r.entity_key = e.left_entity_key
                                 OR r.entity_key = e.right_entity_key
                          )
                        """
                    excl_edge_filter = ""
                    if self.adapter.table_exists("idr_meta.identifier_exclusion"):
                        excl_e = self._exclusion_match_predicate(
                            "e.identifier_type", "e.identifier_value_norm"
                        )
                        excl_edge_filter = f"AND NOT {excl_e}"
                    existing_edges_sql = f"""
                        UNION ALL
                        SELECT e.left_entity_key AS src, e.right_entity_key AS dst
                        FROM idr_out.identity_edges_current e
                        JOIN idr_work.entities_delta ed1 ON e.left_entity_key = ed1.entity_key
                        JOIN idr_work.entities_delta ed2 ON e.right_entity_key = ed2.entity_key
                        WHERE 1=1
                        {reextracted_filter}
                        {excl_edge_filter}
                        UNION ALL
                        SELECT e.right_entity_key AS src, e.left_entity_key AS dst
                        FROM idr_out.identity_edges_current e
                        JOIN idr_work.entities_delta ed1 ON e.left_entity_key = ed1.entity_key
                        JOIN idr_work.entities_delta ed2 ON e.right_entity_key = ed2.entity_key
                        WHERE 1=1
                        {reextracted_filter}
                        {excl_edge_filter}
                    """
            except Exception:
                pass

        lp_edges_cluster = "CLUSTER BY src, dst " if self.adapter.dialect == "bigquery" else ""
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.lp_edges {lp_edges_cluster}AS
            SELECT left_entity_key AS src, right_entity_key AS dst FROM idr_work.edges_new
            UNION ALL
            SELECT right_entity_key AS src, left_entity_key AS dst FROM idr_work.edges_new
            {existing_edges_sql}
        """)

        # Ensure ISOLATE and DO_NOT_LINK overrides are respected even over historical INCR edges
        try:
            if self.adapter.table_exists("idr_meta.identity_override"):
                ov_cnt = (
                    self.adapter.query_one(
                        "SELECT COUNT(*) FROM idr_meta.identity_override WHERE is_active = TRUE"
                    )
                    or 0
                )
                if ov_cnt > 0:
                    self.adapter.execute("""
                        DELETE FROM idr_work.lp_edges
                        WHERE src IN (
                            SELECT entity_key_a FROM idr_meta.identity_override
                            WHERE is_active = TRUE AND UPPER(override_type) = 'ISOLATE'
                        )
                        OR dst IN (
                            SELECT entity_key_a FROM idr_meta.identity_override
                            WHERE is_active = TRUE AND UPPER(override_type) = 'ISOLATE'
                        )
                    """)
                    self.adapter.execute("""
                        DELETE FROM idr_work.lp_edges
                        WHERE EXISTS (
                            SELECT 1 FROM idr_meta.identity_override o
                            WHERE o.is_active = TRUE
                              AND UPPER(o.override_type) = 'DO_NOT_LINK'
                              AND (
                                  (idr_work.lp_edges.src = o.entity_key_a AND idr_work.lp_edges.dst = o.entity_key_b)
                                  OR (idr_work.lp_edges.src = o.entity_key_b AND idr_work.lp_edges.dst = o.entity_key_a)
                              )
                        )
                    """)
        except Exception:
            pass

        # Check if there are any edges to propagate
        edge_cnt = self.adapter.query_one("SELECT COUNT(*) FROM idr_work.lp_edges") or 0

        if edge_cnt == 0:
            # All entities are singletons; no edge propagation needed
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.lp_labels AS
                SELECT DISTINCT entity_key, entity_key AS label
                FROM idr_work.entities_delta
                WHERE run_id = '{self.run_id}'
            """)
            self._stabilize_resolved_ids()
            clusters = self.adapter.query_one(
                "SELECT COUNT(DISTINCT label) FROM idr_work.lp_labels"
            )
            return 0, clusters or 0

        # Initialize labels for connected nodes only (skipping singletons during iterations)
        self.adapter.execute("""
            CREATE OR REPLACE TABLE idr_work.lp_labels AS
            SELECT DISTINCT src AS entity_key, src AS label
            FROM idr_work.lp_edges
        """)

        # Iterative label propagation on connected subgraph using pointer ping-pong
        # (avoids full CTAS table copy + DROP on every iteration in BigQuery)
        iterations = 0
        changes = 0
        curr_tbl = "idr_work.lp_labels"
        next_tbl = "idr_work.lp_labels_new"
        for i in range(max_iters):
            iterations = i + 1

            # Propagate minimum label
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE {next_tbl} AS
                WITH candidate_labels AS (
                    SELECT l.entity_key, l.label AS candidate_label
                    FROM {curr_tbl} l
                    UNION ALL
                    SELECT e.src AS entity_key, l2.label AS candidate_label
                    FROM idr_work.lp_edges e
                    JOIN {curr_tbl} l2 ON e.dst = l2.entity_key
                )
                SELECT entity_key, MIN(candidate_label) AS label
                FROM candidate_labels
                GROUP BY entity_key
            """)

            # Check for convergence
            changes = (
                self.adapter.query_one(f"""
                SELECT COUNT(*) FROM {curr_tbl} prev
                JOIN {next_tbl} curr ON prev.entity_key = curr.entity_key
                WHERE prev.label != curr.label
            """)
                or 0
            )

            curr_tbl, next_tbl = next_tbl, curr_tbl

            if self.ctx.progress_callback:
                try:
                    self.ctx.progress_callback(
                        {
                            "event": "stage_progress",
                            "run_id": self.run_id,
                            "stage": "label_propagation",
                            "iteration": iterations,
                            "max_iters": max_iters,
                            "changes": int(changes),
                        }
                    )
                except Exception:
                    pass

            if changes == 0:
                break

        if curr_tbl != "idr_work.lp_labels":
            self._swap_tables("idr_work.lp_labels", curr_tbl)
        else:
            self.adapter.execute(f"DROP TABLE IF EXISTS {next_tbl}")

        if changes > 0:
            self._require_convergence("idr_work.lp_edges", "idr_work.lp_labels")

        # Re-attach singletons (entities in current run with no incident edges)
        self.adapter.execute(f"""
            INSERT INTO idr_work.lp_labels (entity_key, label)
            SELECT ed.entity_key, ed.entity_key AS label
            FROM (
                SELECT DISTINCT entity_key
                FROM idr_work.entities_delta
                WHERE run_id = '{self.run_id}'
            ) ed
            WHERE NOT EXISTS (
                SELECT 1 FROM idr_work.lp_labels l WHERE l.entity_key = ed.entity_key
            )
        """)

        # Post-propagation multi-hop DO_NOT_LINK conflict resolution:
        # If a blocked pair (a, b) still shares a label via a multi-identifier transitive path,
        # sever edges incident to high_key = max(a, b) and re-converge so DO_NOT_LINK holds.
        iterations += self._resolve_transitive_do_not_link_conflicts(max_iters)

        self._require_convergence("idr_work.lp_edges", "idr_work.lp_labels")

        # Preserve existing cluster resolved_ids across runs (Largest cluster wins on merge)
        self._stabilize_resolved_ids()

        # Count unique clusters
        clusters = self.adapter.query_one("SELECT COUNT(DISTINCT label) FROM idr_work.lp_labels")

        return iterations, clusters or 0

    def _require_convergence(self, edges, labels, left="src", right="dst", view="default"):
        """An iteration budget is not permission to publish a partial partition."""
        unresolved = int(
            self.adapter.query_one(f"""
            SELECT COUNT(*) FROM {edges} e
            LEFT JOIN {labels} a ON a.entity_key = e.{left}
            LEFT JOIN {labels} b ON b.entity_key = e.{right}
            WHERE a.label IS NULL OR b.label IS NULL OR a.label <> b.label
        """)
            or 0
        )
        if unresolved:
            raise GraphNotConverged(
                f"Graph did not converge for {view}: {unresolved} edges have unresolved endpoints. "
                "Increase max_iters and retry; incomplete graph results cannot be published."
            )

    def _resolve_transitive_do_not_link_conflicts(self, max_iters: int) -> int:
        """Sever multi-hop transitive bridges for any DO_NOT_LINK pair sharing a converged label."""
        try:
            if not self.adapter.table_exists("idr_meta.identity_override"):
                return 0
            conflicts = self.adapter.query("""
                SELECT o.entity_key_a, o.entity_key_b
                FROM idr_meta.identity_override o
                JOIN idr_work.lp_labels la ON la.entity_key = o.entity_key_a
                JOIN idr_work.lp_labels lb ON lb.entity_key = o.entity_key_b
                WHERE o.is_active = TRUE
                  AND UPPER(o.override_type) = 'DO_NOT_LINK'
                  AND la.label = lb.label
            """)
            if not conflicts:
                return 0

            high_keys = sorted(
                {
                    max(
                        self._validate_metadata_value(
                            row["entity_key_a"], "expr", "override.entity_key_a"
                        ),
                        self._validate_metadata_value(
                            row["entity_key_b"], "expr", "override.entity_key_b"
                        ),
                    )
                    for row in conflicts
                }
            )
            high_keys_in = ", ".join(f"'{k}'" for k in high_keys)
            self.adapter.execute(f"""
                DELETE FROM idr_work.lp_edges
                WHERE src IN ({high_keys_in}) OR dst IN ({high_keys_in})
            """)
            if self.adapter.table_exists("idr_work.edges_new"):
                self.adapter.execute(f"""
                    DELETE FROM idr_work.edges_new
                    WHERE left_entity_key IN ({high_keys_in}) OR right_entity_key IN ({high_keys_in})
                """)

            # Re-initialize and re-converge on remaining lp_edges
            edge_cnt = self.adapter.query_one("SELECT COUNT(*) FROM idr_work.lp_edges") or 0
            extra_iters = 0
            if edge_cnt > 0:
                self.adapter.execute("""
                    CREATE OR REPLACE TABLE idr_work.lp_labels AS
                    SELECT DISTINCT src AS entity_key, src AS label
                    FROM idr_work.lp_edges
                """)
                curr_tbl = "idr_work.lp_labels"
                next_tbl = "idr_work.lp_labels_new"
                for _ in range(max_iters):
                    extra_iters += 1
                    self.adapter.execute(f"""
                        CREATE OR REPLACE TABLE {next_tbl} AS
                        WITH candidate_labels AS (
                            SELECT l.entity_key, l.label AS candidate_label
                            FROM {curr_tbl} l
                            UNION ALL
                            SELECT e.src AS entity_key, l2.label AS candidate_label
                            FROM idr_work.lp_edges e
                            JOIN {curr_tbl} l2 ON e.dst = l2.entity_key
                        )
                        SELECT entity_key, MIN(candidate_label) AS label
                        FROM candidate_labels
                        GROUP BY entity_key
                    """)
                    changes = (
                        self.adapter.query_one(f"""
                        SELECT COUNT(*) FROM {curr_tbl} prev
                        JOIN {next_tbl} curr ON prev.entity_key = curr.entity_key
                        WHERE prev.label != curr.label
                    """)
                        or 0
                    )
                    curr_tbl, next_tbl = next_tbl, curr_tbl
                    if changes == 0:
                        break
                if curr_tbl != "idr_work.lp_labels":
                    self._swap_tables("idr_work.lp_labels", curr_tbl)
                else:
                    self.adapter.execute(f"DROP TABLE IF EXISTS {next_tbl}")
            else:
                str_type = self._dialect["string_type"]
                self.adapter.execute(f"""
                    CREATE OR REPLACE TABLE idr_work.lp_labels (entity_key {str_type}, label {str_type})
                """)

            self.adapter.execute(f"""
                INSERT INTO idr_work.lp_labels (entity_key, label)
                SELECT ed.entity_key, ed.entity_key AS label
                FROM (
                    SELECT DISTINCT entity_key
                    FROM idr_work.entities_delta
                    WHERE run_id = '{self.run_id}'
                ) ed
                WHERE NOT EXISTS (
                    SELECT 1 FROM idr_work.lp_labels l WHERE l.entity_key = ed.entity_key
                )
            """)
            return extra_iters
        except Exception as e:
            self.logger.warning(f"Failed to resolve transitive DO_NOT_LINK conflicts: {e}")
            return 0

    def _stabilize_resolved_ids(self) -> None:
        """
        Keep resolved_ids persistent across runs:
        - If new entities join an existing cluster, retain that cluster's existing resolved_id.
        - If multiple existing clusters merge into one component, the largest previous cluster's
          resolved_id wins (tie-broken by oldest timestamp, then lexicographical MIN(resolved_id)).
        - If an existing cluster splits, only one component may claim the previous resolved_id
          (preferring the component containing the anchor entity_key == prev_resolved_id).
        """
        try:
            if not self.adapter.table_exists("idr_out.identity_resolved_membership_current"):
                return
            prev_cnt = (
                self.adapter.query_one(
                    "SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current"
                )
                or 0
            )
            if prev_cnt == 0:
                return

            md5_expr = self._dialect["md5"]("l.label")
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.lp_labels_stabilized AS
                WITH prev_cluster_stats AS (
                    SELECT
                        resolved_id AS prev_resolved_id,
                        COUNT(*) AS prev_size,
                        MIN(updated_at) AS prev_created_at
                    FROM idr_out.identity_resolved_membership_current
                    GROUP BY resolved_id
                ),
                overlap AS (
                    SELECT
                        l.label AS raw_label,
                        m.resolved_id AS prev_resolved_id,
                        pcs.prev_size,
                        pcs.prev_created_at,
                        COUNT(*) AS shared_entities,
                        MAX(CASE WHEN l.entity_key = m.resolved_id THEN 1 ELSE 0 END) AS has_anchor
                    FROM idr_work.lp_labels l
                    JOIN idr_out.identity_resolved_membership_current m
                      ON l.entity_key = m.entity_key
                    JOIN prev_cluster_stats pcs
                      ON m.resolved_id = pcs.prev_resolved_id
                    GROUP BY l.label, m.resolved_id, pcs.prev_size, pcs.prev_created_at
                ),
                claimed_prev AS (
                    SELECT
                        raw_label,
                        prev_resolved_id,
                        prev_size,
                        prev_created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY prev_resolved_id
                            ORDER BY
                                CASE WHEN raw_label = prev_resolved_id THEN 1 ELSE 0 END DESC,
                                has_anchor DESC,
                                shared_entities DESC,
                                raw_label ASC
                        ) AS prev_claim_rn
                    FROM overlap
                ),
                winning_cluster AS (
                    SELECT
                        raw_label,
                        prev_resolved_id AS winning_resolved_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY raw_label
                            ORDER BY
                                prev_size DESC,
                                prev_created_at ASC,
                                prev_resolved_id ASC
                        ) AS win_rn
                    FROM claimed_prev
                    WHERE prev_claim_rn = 1
                ),
                reserved_ids AS (
                    SELECT DISTINCT winning_resolved_id AS reserved_id
                    FROM winning_cluster
                    WHERE win_rn = 1
                    UNION DISTINCT
                    SELECT DISTINCT resolved_id AS reserved_id
                    FROM idr_out.identity_resolved_membership_current
                )
                SELECT
                    l.entity_key,
                    CASE
                        WHEN w.winning_resolved_id IS NOT NULL THEN w.winning_resolved_id
                        WHEN ri.reserved_id IS NOT NULL THEN CONCAT(l.label, ':split:', SUBSTR({md5_expr}, 1, 8))
                        ELSE l.label
                    END AS label,
                    CASE WHEN w.winning_resolved_id IS NULL THEN TRUE ELSE FALSE END AS is_fallback
                FROM idr_work.lp_labels l
                LEFT JOIN winning_cluster w
                  ON l.label = w.raw_label AND w.win_rn = 1
                LEFT JOIN reserved_ids ri
                  ON w.winning_resolved_id IS NULL AND l.label = ri.reserved_id
            """)
            # Guard against the rare case where a fallback candidate (or its ':split:<hash>' suffix)
            # collides with a winning_resolved_id, another component's label, or an untouched historical ID.
            max_retries = 5
            attempt = 0
            while True:
                collisions = self.adapter.query("""
                    SELECT DISTINCT s.label AS colliding_label
                    FROM idr_work.lp_labels_stabilized s
                    JOIN idr_work.lp_labels orig ON s.entity_key = orig.entity_key
                    WHERE s.is_fallback = TRUE
                      AND (
                          EXISTS (
                              SELECT 1 FROM idr_work.lp_labels_stabilized s2
                              JOIN idr_work.lp_labels orig2 ON s2.entity_key = orig2.entity_key
                              WHERE s2.label = s.label AND orig2.label != orig.label
                          )
                          OR s.label IN (
                              SELECT DISTINCT resolved_id
                              FROM idr_out.identity_resolved_membership_current
                          )
                      )
                """)
                if not collisions:
                    break
                if attempt >= max_retries:
                    colliding_list = ", ".join(str(c["colliding_label"]) for c in collisions)
                    raise RuntimeError(
                        f"Fallback cluster ID collision unresolved after {max_retries} retries: {colliding_list}"
                    )
                attempt += 1
                for crow in collisions:
                    clabel = crow["colliding_label"]
                    safe_clabel = clabel.replace("'", "''")
                    new_md5 = self._dialect["md5"](
                        f"CONCAT(orig.label, ':{self.run_id}:{attempt}')"
                    )
                    self.adapter.execute(f"""
                        CREATE OR REPLACE TABLE idr_work.lp_labels_stabilized_next AS
                        SELECT
                            s.entity_key,
                            CASE
                                WHEN s.is_fallback = TRUE AND s.label = '{safe_clabel}'
                                THEN CONCAT(orig.label, ':split:', SUBSTR({new_md5}, 1, 8))
                                ELSE s.label
                            END AS label,
                            s.is_fallback
                        FROM idr_work.lp_labels_stabilized s
                        JOIN idr_work.lp_labels orig ON s.entity_key = orig.entity_key
                    """)
                    self._swap_tables(
                        "idr_work.lp_labels_stabilized", "idr_work.lp_labels_stabilized_next"
                    )
            self.adapter.execute("""
                CREATE OR REPLACE TABLE idr_work.lp_labels_stabilized_final AS
                SELECT entity_key, label FROM idr_work.lp_labels_stabilized
            """)
            self.adapter.execute("DROP TABLE IF EXISTS idr_work.lp_labels_stabilized")
            self._swap_tables("idr_work.lp_labels", "idr_work.lp_labels_stabilized_final")
        except RuntimeError:
            raise
        except Exception as e:
            self.logger.error(f"Failed to stabilize resolved_ids: {e}")
            raise RuntimeError(f"Failed to stabilize resolved_ids: {e}") from e

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

        # 0. Get known attributes (include both mapped attributes and mapped identifier types)
        known_attrs = set(
            row["attribute_name"].lower()
            for row in self.adapter.query(
                "SELECT DISTINCT attribute_name FROM idr_meta.entity_attribute_mapping"
            )
        )
        try:
            known_ids = set(
                row["identifier_type"].lower()
                for row in self.adapter.query(
                    "SELECT DISTINCT identifier_type FROM idr_meta.identifier_mapping"
                )
            )
            known_attrs |= known_ids
        except Exception:
            pass

        # 1. Prepare Cluster Attributes (All distinct values per cluster from attributes + identifiers)
        has_attrs = True
        has_ids = False
        try:
            has_attrs = bool(self.adapter.table_exists("idr_work.entity_attributes"))
            has_ids = bool(self.adapter.table_exists("idr_work.identifiers_all"))
        except Exception:
            pass

        attr_parts = []
        if has_attrs:
            attr_parts.append("""
                SELECT DISTINCT
                    l.label AS resolved_id,
                    LOWER(a.attribute_name) as attribute_name,
                    a.attribute_value
                FROM idr_work.lp_labels l
                JOIN idr_work.entity_attributes a ON l.entity_key = a.entity_key
            """)
        if has_ids:
            attr_parts.append("""
                SELECT DISTINCT
                    l.label AS resolved_id,
                    LOWER(i.identifier_type) as attribute_name,
                    i.identifier_value_norm as attribute_value
                FROM idr_work.lp_labels l
                JOIN idr_work.identifiers_all i ON l.entity_key = i.entity_key
            """)

        if attr_parts:
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.cluster_attributes AS
                {" UNION DISTINCT ".join(attr_parts)}
            """)
        else:
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.cluster_attributes (
                    resolved_id {string_type},
                    attribute_name {string_type},
                    attribute_value {string_type}
                )
            """)

        # Initialize edges table
        self.adapter.execute(
            f"CREATE OR REPLACE TABLE idr_work.fuzzy_edges (left_id {string_type}, right_id {string_type}, score {float_type})"
        )

        multi_kind_fuzzy = self._has_multiple_entity_kinds()
        if multi_kind_fuzzy:
            lbl_prefix_expr = (
                "SPLIT(l.entity_key, ':')[SAFE_OFFSET(0)]"
                if self.adapter.dialect == "bigquery"
                else "SPLIT_PART(l.entity_key, ':', 1)"
            )
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.cluster_entity_kinds AS
                SELECT
                    l.label AS resolved_id,
                    MIN(LOWER(COALESCE(NULLIF(TRIM(st.entity_kind), ''), 'person'))) AS entity_kind
                FROM idr_work.lp_labels l
                LEFT JOIN idr_meta.source_table st
                    ON st.table_id = {lbl_prefix_expr}
                GROUP BY l.label
            """)

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

            kind_join_sql = (
                """
                JOIN idr_work.cluster_entity_kinds ka ON ka.resolved_id = a.resolved_id
                JOIN idr_work.cluster_entity_kinds kb ON kb.resolved_id = b.resolved_id AND ka.entity_kind = kb.entity_kind
                """
                if multi_kind_fuzzy
                else ""
            )

            self.adapter.execute(f"""
                INSERT INTO idr_work.fuzzy_edges (left_id, right_id, score)
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
                    {kind_join_sql}
                )
                SELECT left_id, right_id, score
                FROM scored_pairs
                WHERE score >= {threshold}
            """)

            self.adapter.execute(f"DROP TABLE {profile_table}")

        # 3. Label Propagation for Super Clusters
        fuzzy_edge_cnt = self.adapter.query_one("SELECT COUNT(*) FROM idr_work.fuzzy_edges") or 0
        if fuzzy_edge_cnt == 0:
            self.adapter.execute("""
                CREATE OR REPLACE TABLE idr_work.fuzzy_results AS
                SELECT DISTINCT resolved_id, resolved_id AS super_cluster_id
                FROM idr_work.cluster_attributes
            """)
            count = self.adapter.query_one(
                "SELECT COUNT(DISTINCT super_cluster_id) FROM idr_work.fuzzy_results"
            )
            return count or 0

        # Initialize labels only for connected clusters in fuzzy_edges
        self.adapter.execute("""
            CREATE OR REPLACE TABLE idr_work.lp_fuzzy_labels AS
            SELECT DISTINCT left_id AS entity_key, left_id AS label FROM idr_work.fuzzy_edges
            UNION DISTINCT
            SELECT DISTINCT right_id AS entity_key, right_id AS label FROM idr_work.fuzzy_edges
        """)

        for i in range(max_iters):
            self.adapter.execute("""
                CREATE OR REPLACE TABLE idr_work.lp_fuzzy_labels_new AS
                WITH candidate_labels AS (
                    SELECT l.entity_key, l.label AS candidate_label
                    FROM idr_work.lp_fuzzy_labels l
                    UNION ALL
                    SELECT e.left_id AS entity_key, l2.label AS candidate_label
                    FROM idr_work.fuzzy_edges e
                    JOIN idr_work.lp_fuzzy_labels l2 ON e.right_id = l2.entity_key
                    UNION ALL
                    SELECT e.right_id AS entity_key, l2.label AS candidate_label
                    FROM idr_work.fuzzy_edges e
                    JOIN idr_work.lp_fuzzy_labels l2 ON e.left_id = l2.entity_key
                )
                SELECT entity_key, MIN(candidate_label) AS label
                FROM candidate_labels
                GROUP BY entity_key
            """)

            changes = (
                self.adapter.query_one("""
                SELECT COUNT(*) FROM idr_work.lp_fuzzy_labels prev
                JOIN idr_work.lp_fuzzy_labels_new curr ON prev.entity_key = curr.entity_key
                WHERE prev.label != curr.label
            """)
                or 0
            )

            self._swap_tables("idr_work.lp_fuzzy_labels", "idr_work.lp_fuzzy_labels_new")

            if changes == 0:
                break

        self._require_convergence(
            "idr_work.fuzzy_edges", "idr_work.lp_fuzzy_labels", "left_id", "right_id", "fuzzy"
        )

        # Re-attach clusters with no fuzzy links
        self.adapter.execute("""
            INSERT INTO idr_work.lp_fuzzy_labels (entity_key, label)
            SELECT DISTINCT ca.resolved_id AS entity_key, ca.resolved_id AS label
            FROM idr_work.cluster_attributes ca
            WHERE NOT EXISTS (
                SELECT 1 FROM idr_work.lp_fuzzy_labels l WHERE l.entity_key = ca.resolved_id
            )
        """)

        # 4. Finalize
        self.adapter.execute("""
            CREATE OR REPLACE TABLE idr_work.fuzzy_results AS
            SELECT entity_key as resolved_id, label as super_cluster_id
            FROM idr_work.lp_fuzzy_labels
        """)

        count = self.adapter.query_one(
            "SELECT COUNT(DISTINCT super_cluster_id) FROM idr_work.fuzzy_results"
        )
        return count or 0

    @staticmethod
    def _glob_to_sql_like(pattern: str) -> str:
        safe = str(pattern).strip().replace("'", "''")
        return safe.replace("*", "%").replace("?", "_")

    def resolve_lens(self, lens_def: Dict, max_iters: int) -> Dict[str, int]:
        """Filter superset edges by lens policy and run LP + bijective stabilization for one lens."""
        lens_id = self._validate_metadata_value(lens_def["lens_id"], "identifier", "lens_id")
        include_types = [
            self._validate_metadata_value(
                str(t).strip().upper(), "identifier", "lens.include_types"
            )
            for t in (lens_def.get("include_types") or [])
            if str(t).strip()
        ]
        exclude_types = [
            self._validate_metadata_value(
                str(t).strip().upper(), "identifier", "lens.exclude_types"
            )
            for t in (lens_def.get("exclude_types") or [])
            if str(t).strip()
        ]
        source_filter = [
            str(s).strip() for s in (lens_def.get("source_filter") or []) if str(s).strip()
        ]

        prefix_expr = (
            "SPLIT(entity_key, ':')[SAFE_OFFSET(0)]"
            if self.adapter.dialect == "bigquery"
            else "SPLIT_PART(entity_key, ':', 1)"
        )
        if source_filter:
            like_clauses = [
                f"{prefix_expr} LIKE '{self._glob_to_sql_like(pat)}'" for pat in source_filter
            ]
            src_pred = "(" + " OR ".join(like_clauses) + ")"
        else:
            src_pred = "1=1"

        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.entities_lens AS
            SELECT DISTINCT entity_key
            FROM idr_work.entities_delta
            WHERE run_id = '{self.run_id}' AND {src_pred}
        """)

        type_preds = ["1=1"]
        if include_types:
            in_list = ", ".join(f"'{t}'" for t in include_types)
            type_preds.append(
                f"(UPPER(e.identifier_type) IN ({in_list}) OR UPPER(e.identifier_type) = 'MANUAL_OVERRIDE')"
            )
        if exclude_types:
            ex_list = ", ".join(f"'{t}'" for t in exclude_types)
            type_preds.append(f"UPPER(e.identifier_type) NOT IN ({ex_list})")
        edge_type_pred = " AND ".join(type_preds)

        existing_edges_sql = ""
        if self.run_mode == "INCR" and self.adapter.table_exists("idr_out.identity_edges_current"):
            try:
                edge_cnt = (
                    self.adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_edges_current")
                    or 0
                )
                if edge_cnt > 0:
                    reex_filter = ""
                    if self.adapter.table_exists("idr_work.reextracted_entities"):
                        reex_filter = (
                            "AND NOT EXISTS (SELECT 1 FROM idr_work.reextracted_entities r "
                            "WHERE r.entity_key = e.left_entity_key OR r.entity_key = e.right_entity_key)"
                        )
                    excl_filter = ""
                    if self.adapter.table_exists("idr_meta.identifier_exclusion"):
                        excl_e = self._exclusion_match_predicate(
                            "e.identifier_type", "e.identifier_value_norm"
                        )
                        excl_filter = f"AND NOT {excl_e}"
                    existing_edges_sql = f"""
                        UNION ALL
                        SELECT e.left_entity_key AS src, e.right_entity_key AS dst
                        FROM idr_out.identity_edges_current e
                        JOIN idr_work.entities_lens el1 ON e.left_entity_key = el1.entity_key
                        JOIN idr_work.entities_lens el2 ON e.right_entity_key = el2.entity_key
                        WHERE {edge_type_pred} {reex_filter} {excl_filter}
                        UNION ALL
                        SELECT e.right_entity_key AS src, e.left_entity_key AS dst
                        FROM idr_out.identity_edges_current e
                        JOIN idr_work.entities_lens el1 ON e.left_entity_key = el1.entity_key
                        JOIN idr_work.entities_lens el2 ON e.right_entity_key = el2.entity_key
                        WHERE {edge_type_pred} {reex_filter} {excl_filter}
                    """
            except Exception:
                pass

        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.lp_edges_lens AS
            SELECT e.left_entity_key AS src, e.right_entity_key AS dst
            FROM idr_work.edges_new e
            JOIN idr_work.entities_lens el1 ON e.left_entity_key = el1.entity_key
            JOIN idr_work.entities_lens el2 ON e.right_entity_key = el2.entity_key
            WHERE {edge_type_pred}
            UNION ALL
            SELECT e.right_entity_key AS src, e.left_entity_key AS dst
            FROM idr_work.edges_new e
            JOIN idr_work.entities_lens el1 ON e.left_entity_key = el1.entity_key
            JOIN idr_work.entities_lens el2 ON e.right_entity_key = el2.entity_key
            WHERE {edge_type_pred}
            {existing_edges_sql}
        """)

        if self.adapter.table_exists("idr_meta.identity_override"):
            try:
                ov_cnt = (
                    self.adapter.query_one(
                        "SELECT COUNT(*) FROM idr_meta.identity_override WHERE is_active = TRUE"
                    )
                    or 0
                )
                if ov_cnt > 0:
                    self.adapter.execute("""
                        DELETE FROM idr_work.lp_edges_lens
                        WHERE src IN (
                            SELECT entity_key_a FROM idr_meta.identity_override
                            WHERE is_active = TRUE AND UPPER(override_type) = 'ISOLATE'
                        )
                        OR dst IN (
                            SELECT entity_key_a FROM idr_meta.identity_override
                            WHERE is_active = TRUE AND UPPER(override_type) = 'ISOLATE'
                        )
                    """)
                    self.adapter.execute("""
                        CREATE OR REPLACE TABLE idr_work.lp_edges_lens_filtered AS
                        SELECT e.*
                        FROM idr_work.lp_edges_lens e
                        WHERE NOT EXISTS (
                            SELECT 1 FROM idr_meta.identity_override o
                            WHERE o.is_active = TRUE AND UPPER(o.override_type) = 'DO_NOT_LINK'
                              AND (
                                  (e.src = o.entity_key_a AND e.dst = o.entity_key_b)
                                  OR (e.src = o.entity_key_b AND e.dst = o.entity_key_a)
                              )
                        )
                    """)
                    self._swap_tables("idr_work.lp_edges_lens", "idr_work.lp_edges_lens_filtered")
            except Exception:
                pass

        raw_mgs = lens_def.get("max_group_size")
        if raw_mgs is not None and int(raw_mgs) > 0:
            mgs = int(raw_mgs)
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.lp_edges_lens_capped AS
                WITH hub_nodes AS (
                    SELECT src AS node_key
                    FROM idr_work.lp_edges_lens
                    GROUP BY src
                    HAVING COUNT(*) >= {mgs}
                )
                SELECT e.*
                FROM idr_work.lp_edges_lens e
                WHERE NOT EXISTS (SELECT 1 FROM hub_nodes h WHERE h.node_key = e.src)
                  AND NOT EXISTS (SELECT 1 FROM hub_nodes h WHERE h.node_key = e.dst)
            """)
            self._swap_tables("idr_work.lp_edges_lens", "idr_work.lp_edges_lens_capped")

        edge_cnt = self.adapter.query_one("SELECT COUNT(*) FROM idr_work.lp_edges_lens") or 0
        iterations = 0
        if edge_cnt == 0:
            self.adapter.execute("""
                CREATE OR REPLACE TABLE idr_work.lp_labels_lens AS
                SELECT DISTINCT entity_key, entity_key AS label
                FROM idr_work.entities_lens
            """)
        else:
            self.adapter.execute("""
                CREATE OR REPLACE TABLE idr_work.lp_labels_lens AS
                SELECT DISTINCT src AS entity_key, src AS label
                FROM idr_work.lp_edges_lens
            """)
            curr_tbl = "idr_work.lp_labels_lens"
            next_tbl = "idr_work.lp_labels_lens_new"
            for i in range(max_iters):
                iterations = i + 1
                self.adapter.execute(f"""
                    CREATE OR REPLACE TABLE {next_tbl} AS
                    WITH candidate_labels AS (
                        SELECT l.entity_key, l.label AS candidate_label
                        FROM {curr_tbl} l
                        UNION ALL
                        SELECT e.src AS entity_key, l2.label AS candidate_label
                        FROM idr_work.lp_edges_lens e
                        JOIN {curr_tbl} l2 ON e.dst = l2.entity_key
                    )
                    SELECT entity_key, MIN(candidate_label) AS label
                    FROM candidate_labels
                    GROUP BY entity_key
                """)
                changes = (
                    self.adapter.query_one(f"""
                    SELECT COUNT(*) FROM {curr_tbl} prev
                    JOIN {next_tbl} curr ON prev.entity_key = curr.entity_key
                    WHERE prev.label != curr.label
                """)
                    or 0
                )
                curr_tbl, next_tbl = next_tbl, curr_tbl
                if changes == 0:
                    break
            if curr_tbl != "idr_work.lp_labels_lens":
                self._swap_tables("idr_work.lp_labels_lens", curr_tbl)
            else:
                self.adapter.execute(f"DROP TABLE IF EXISTS {next_tbl}")

            self.adapter.execute("""
                INSERT INTO idr_work.lp_labels_lens (entity_key, label)
                SELECT el.entity_key, el.entity_key AS label
                FROM idr_work.entities_lens el
                WHERE NOT EXISTS (
                    SELECT 1 FROM idr_work.lp_labels_lens l WHERE l.entity_key = el.entity_key
                )
            """)

        iterations += self._resolve_lens_do_not_link_conflicts(max_iters)
        self._require_convergence(
            "idr_work.lp_edges_lens", "idr_work.lp_labels_lens", view=f"lens {lens_id}"
        )
        self._stabilize_lens_resolved_ids(lens_id)
        clusters = int(
            self.adapter.query_one("SELECT COUNT(DISTINCT label) FROM idr_work.lp_labels_lens") or 0
        )
        return {"lens_id": lens_id, "iterations": iterations, "clusters": clusters}

    def _resolve_lens_do_not_link_conflicts(self, max_iters: int) -> int:
        """Sever remaining DO_NOT_LINK bridges (star-hub or multi-hop) in idr_work.lp_edges_lens."""
        try:
            if not self.adapter.table_exists("idr_meta.identity_override"):
                return 0
            conflicts = self.adapter.query("""
                SELECT o.entity_key_a, o.entity_key_b
                FROM idr_meta.identity_override o
                JOIN idr_work.lp_labels_lens la ON la.entity_key = o.entity_key_a
                JOIN idr_work.lp_labels_lens lb ON lb.entity_key = o.entity_key_b
                WHERE o.is_active = TRUE
                  AND UPPER(o.override_type) = 'DO_NOT_LINK'
                  AND la.label = lb.label
            """)
            if not conflicts:
                return 0

            high_keys = sorted(
                {
                    max(
                        self._validate_metadata_value(
                            row["entity_key_a"], "expr", "override.entity_key_a"
                        ),
                        self._validate_metadata_value(
                            row["entity_key_b"], "expr", "override.entity_key_b"
                        ),
                    )
                    for row in conflicts
                }
            )
            high_keys_in = ", ".join(f"'{k}'" for k in high_keys)
            self.adapter.execute(f"""
                DELETE FROM idr_work.lp_edges_lens
                WHERE src IN ({high_keys_in}) OR dst IN ({high_keys_in})
            """)

            edge_cnt = self.adapter.query_one("SELECT COUNT(*) FROM idr_work.lp_edges_lens") or 0
            extra_iters = 0
            if edge_cnt > 0:
                self.adapter.execute("""
                    CREATE OR REPLACE TABLE idr_work.lp_labels_lens AS
                    SELECT DISTINCT src AS entity_key, src AS label
                    FROM idr_work.lp_edges_lens
                """)
                curr_tbl = "idr_work.lp_labels_lens"
                next_tbl = "idr_work.lp_labels_lens_new"
                for _ in range(max_iters):
                    extra_iters += 1
                    self.adapter.execute(f"""
                        CREATE OR REPLACE TABLE {next_tbl} AS
                        WITH candidate_labels AS (
                            SELECT l.entity_key, l.label AS candidate_label
                            FROM {curr_tbl} l
                            UNION ALL
                            SELECT e.src AS entity_key, l2.label AS candidate_label
                            FROM idr_work.lp_edges_lens e
                            JOIN {curr_tbl} l2 ON e.dst = l2.entity_key
                        )
                        SELECT entity_key, MIN(candidate_label) AS label
                        FROM candidate_labels
                        GROUP BY entity_key
                    """)
                    changes = (
                        self.adapter.query_one(f"""
                        SELECT COUNT(*) FROM {curr_tbl} prev
                        JOIN {next_tbl} curr ON prev.entity_key = curr.entity_key
                        WHERE prev.label != curr.label
                    """)
                        or 0
                    )
                    curr_tbl, next_tbl = next_tbl, curr_tbl
                    if changes == 0:
                        break
                if curr_tbl != "idr_work.lp_labels_lens":
                    self._swap_tables("idr_work.lp_labels_lens", curr_tbl)
                else:
                    self.adapter.execute(f"DROP TABLE IF EXISTS {next_tbl}")
            else:
                str_type = self._dialect["string_type"]
                self.adapter.execute(f"""
                    CREATE OR REPLACE TABLE idr_work.lp_labels_lens (entity_key {str_type}, label {str_type})
                """)

            self.adapter.execute("""
                INSERT INTO idr_work.lp_labels_lens (entity_key, label)
                SELECT el.entity_key, el.entity_key AS label
                FROM idr_work.entities_lens el
                WHERE NOT EXISTS (
                    SELECT 1 FROM idr_work.lp_labels_lens l WHERE l.entity_key = el.entity_key
                )
            """)
            return extra_iters
        except Exception as e:
            self.logger.warning(f"Failed to resolve lens DO_NOT_LINK conflicts: {e}")
            return 0

    def _stabilize_lens_resolved_ids(self, lens_id: str) -> None:
        """Stabilize resolved_id values for a specific lens against its own historical membership."""
        try:
            if not self.adapter.table_exists("idr_out.identity_lens_membership_current"):
                return
            prev_cnt = (
                self.adapter.query_one(
                    f"SELECT COUNT(*) FROM idr_out.identity_lens_membership_current WHERE lens_id = '{lens_id}'"
                )
                or 0
            )
            if prev_cnt == 0:
                return

            md5_expr = self._dialect["md5"]("l.label")
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.lp_labels_lens_stabilized AS
                WITH prev_lens_mem AS (
                    SELECT entity_key, resolved_id, updated_at
                    FROM idr_out.identity_lens_membership_current
                    WHERE lens_id = '{lens_id}'
                ),
                prev_cluster_stats AS (
                    SELECT resolved_id AS prev_resolved_id, COUNT(*) AS prev_size, MIN(updated_at) AS prev_created_at
                    FROM prev_lens_mem
                    GROUP BY resolved_id
                ),
                overlap AS (
                    SELECT
                        l.label AS raw_label,
                        m.resolved_id AS prev_resolved_id,
                        pcs.prev_size,
                        pcs.prev_created_at,
                        COUNT(*) AS shared_entities,
                        MAX(CASE WHEN l.entity_key = m.resolved_id THEN 1 ELSE 0 END) AS has_anchor
                    FROM idr_work.lp_labels_lens l
                    JOIN prev_lens_mem m ON l.entity_key = m.entity_key
                    JOIN prev_cluster_stats pcs ON m.resolved_id = pcs.prev_resolved_id
                    GROUP BY l.label, m.resolved_id, pcs.prev_size, pcs.prev_created_at
                ),
                claimed_prev AS (
                    SELECT
                        raw_label, prev_resolved_id, prev_size, prev_created_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY prev_resolved_id
                            ORDER BY
                                CASE WHEN raw_label = prev_resolved_id THEN 1 ELSE 0 END DESC,
                                has_anchor DESC,
                                shared_entities DESC,
                                raw_label ASC
                        ) AS prev_claim_rn
                    FROM overlap
                ),
                winning_cluster AS (
                    SELECT
                        raw_label, prev_resolved_id AS winning_resolved_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY raw_label
                            ORDER BY prev_size DESC, prev_created_at ASC, prev_resolved_id ASC
                        ) AS win_rn
                    FROM claimed_prev
                    WHERE prev_claim_rn = 1
                ),
                reserved_ids AS (
                    SELECT DISTINCT winning_resolved_id AS reserved_id
                    FROM winning_cluster
                    WHERE win_rn = 1
                    UNION DISTINCT
                    SELECT DISTINCT resolved_id AS reserved_id
                    FROM prev_lens_mem
                )
                SELECT
                    l.entity_key,
                    CASE
                        WHEN w.winning_resolved_id IS NOT NULL THEN w.winning_resolved_id
                        WHEN ri.reserved_id IS NOT NULL THEN CONCAT(l.label, ':split:', SUBSTR({md5_expr}, 1, 8))
                        ELSE l.label
                    END AS label,
                    CASE WHEN w.winning_resolved_id IS NULL THEN TRUE ELSE FALSE END AS is_fallback
                FROM idr_work.lp_labels_lens l
                LEFT JOIN winning_cluster w ON l.label = w.raw_label AND w.win_rn = 1
                LEFT JOIN reserved_ids ri ON w.winning_resolved_id IS NULL AND l.label = ri.reserved_id
            """)

            max_retries = 5
            attempt = 0
            while True:
                collisions = self.adapter.query(f"""
                    SELECT DISTINCT s.label AS colliding_label
                    FROM idr_work.lp_labels_lens_stabilized s
                    JOIN idr_work.lp_labels_lens orig ON s.entity_key = orig.entity_key
                    WHERE s.is_fallback = TRUE
                      AND (
                          EXISTS (
                              SELECT 1 FROM idr_work.lp_labels_lens_stabilized s2
                              JOIN idr_work.lp_labels_lens orig2 ON s2.entity_key = orig2.entity_key
                              WHERE s2.label = s.label AND orig2.label != orig.label
                          )
                          OR s.label IN (
                              SELECT DISTINCT resolved_id
                              FROM idr_out.identity_lens_membership_current
                              WHERE lens_id = '{lens_id}'
                          )
                      )
                """)
                if not collisions:
                    break
                if attempt >= max_retries:
                    colliding_list = ", ".join(str(c["colliding_label"]) for c in collisions)
                    raise RuntimeError(
                        f"Lens '{lens_id}' fallback cluster ID collision unresolved after {max_retries} retries: {colliding_list}"
                    )
                attempt += 1
                for crow in collisions:
                    clabel = crow["colliding_label"]
                    safe_clabel = clabel.replace("'", "''")
                    new_md5 = self._dialect["md5"](
                        f"CONCAT(orig.label, ':{self.run_id}:{lens_id}:{attempt}')"
                    )
                    self.adapter.execute(f"""
                        CREATE OR REPLACE TABLE idr_work.lp_labels_lens_stabilized_next AS
                        SELECT
                            s.entity_key,
                            CASE
                                WHEN s.is_fallback = TRUE AND s.label = '{safe_clabel}'
                                THEN CONCAT(orig.label, ':split:', SUBSTR({new_md5}, 1, 8))
                                ELSE s.label
                            END AS label,
                            s.is_fallback
                        FROM idr_work.lp_labels_lens_stabilized s
                        JOIN idr_work.lp_labels_lens orig ON s.entity_key = orig.entity_key
                    """)
                    self._swap_tables(
                        "idr_work.lp_labels_lens_stabilized",
                        "idr_work.lp_labels_lens_stabilized_next",
                    )

            self.adapter.execute("""
                CREATE OR REPLACE TABLE idr_work.lp_labels_lens_stabilized_final AS
                SELECT entity_key, label FROM idr_work.lp_labels_lens_stabilized
            """)
            self.adapter.execute("DROP TABLE IF EXISTS idr_work.lp_labels_lens_stabilized")
            self._swap_tables("idr_work.lp_labels_lens", "idr_work.lp_labels_lens_stabilized_final")
        except RuntimeError:
            raise
        except Exception as e:
            self.logger.error(f"Failed to stabilize lens '{lens_id}' resolved_ids: {e}")
            raise RuntimeError(f"Failed to stabilize lens '{lens_id}' resolved_ids: {e}") from e
