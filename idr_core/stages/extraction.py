"""
Extraction stage for IDR pipeline.

Handles entity extraction from source tables, identifier extraction
and canonicalization, and attribute extraction for fuzzy matching.
"""

import os

from ..canonicalizers import build_canonicalize_expr
from ..config import build_where_clause
from ..source_relation import source_relation_sql
from .base import BaseStage


class ExtractionStage(BaseStage):
    """Extracts entities, identifiers, and attributes from source tables."""

    def _source_relation_sql(
        self,
        table_fqn: str,
        entity_key_expr: str,
        deduplicate: bool = False,
        dedup_order_by: str = "",
        watermark_col: str = "",
    ) -> str:
        return source_relation_sql(table_fqn, entity_key_expr, deduplicate, dedup_order_by, watermark_col)

    def extract_entities(self, run_mode: str) -> int:
        """Extract entities from source tables."""
        try:
            sources = self.adapter.query("""
                SELECT
                    st.table_id, st.table_fqn, st.entity_type, st.entity_kind,
                    st.entity_key_expr, st.watermark_column,
                    COALESCE(st.watermark_lookback_minutes, 0) as lookback,
                    COALESCE(st.deduplicate, FALSE) as deduplicate,
                    st.dedup_order_by,
                    rs.last_watermark_value
                FROM idr_meta.source_table st
                LEFT JOIN idr_meta.run_state rs ON st.table_id = rs.table_id
                WHERE st.is_active = TRUE
            """)
        except Exception:
            try:
                sources = self.adapter.query("""
                    SELECT
                        st.table_id, st.table_fqn, st.entity_type,
                        st.entity_key_expr, st.watermark_column,
                        COALESCE(st.watermark_lookback_minutes, 0) as lookback,
                        COALESCE(st.deduplicate, FALSE) as deduplicate,
                        st.dedup_order_by,
                        rs.last_watermark_value
                    FROM idr_meta.source_table st
                    LEFT JOIN idr_meta.run_state rs ON st.table_id = rs.table_id
                    WHERE st.is_active = TRUE
                """)
            except Exception:
                sources = self.adapter.query("""
                    SELECT
                        st.table_id, st.table_fqn, st.entity_type,
                        st.entity_key_expr, st.watermark_column,
                        COALESCE(st.watermark_lookback_minutes, 0) as lookback,
                        rs.last_watermark_value
                    FROM idr_meta.source_table st
                    LEFT JOIN idr_meta.run_state rs ON st.table_id = rs.table_id
                    WHERE st.is_active = TRUE
                """)

        kinds = {
            str(s.get("entity_kind") or "person").strip().lower() or "person"
            for s in (sources or [])
            if isinstance(s, dict)
        }
        self.ctx.has_multiple_entity_kinds = len(kinds) > 1

        if not sources:
            # Create empty table to ensure downstream steps don't fail
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.entities_delta (
                    run_id {self._dialect["string_type"]},
                    table_id {self._dialect["string_type"]},
                    entity_key {self._dialect["string_type"]},
                    watermark_value {self._dialect["timestamp_type"]}
                )
            """)
            return 0

        has_membership = False
        if run_mode == "INCR":
            try:
                if self.adapter.table_exists("idr_out.identity_resolved_membership_current"):
                    mem_cnt = (
                        self.adapter.query_one(
                            "SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current"
                        )
                        or 0
                    )
                    has_membership = mem_cnt > 0
            except Exception:
                has_membership = False

        # Build UNION query for all sources
        union_parts = []
        self._no_watermark_table_ids = []
        self._source_dedup_meta = {}
        for src in sources:
            # Validate DB-sourced metadata before SQL interpolation
            table_id = self._validate_metadata_value(
                src["table_id"], "identifier", "source_table.table_id"
            )
            table_fqn = self._validate_metadata_value(
                src["table_fqn"], "fqn", "source_table.table_fqn"
            )
            entity_key_expr = self._validate_metadata_value(
                src["entity_key_expr"], "expr", "source_table.entity_key_expr"
            )

            raw_wm = src.get("watermark_column")
            watermark_col = (
                ""
                if raw_wm is None or str(raw_wm).strip().lower() in ("", "none", "null")
                else str(raw_wm).strip()
            )
            if watermark_col:
                self._validate_metadata_value(
                    watermark_col, "identifier", "source_table.watermark_column"
                )
            else:
                self._no_watermark_table_ids.append(table_id)

            # If watermark column exists, cast it. Otherwise use a default old date constant.
            if watermark_col:
                watermark_expr = f"CAST({watermark_col} AS {self._dialect['timestamp_type']})"
            else:
                # Use a default low watermark for full refresh / no-watermark tables
                default_ts = "'1970-01-01 00:00:00'"
                if self.adapter.dialect == "snowflake":
                    watermark_expr = f"{default_ts}::TIMESTAMP"
                else:
                    watermark_expr = f"CAST({default_ts} AS {self._dialect['timestamp_type']})"

            last_wm = src.get("last_watermark_value")
            # Self-heal missing watermark in INCR mode if this table already has resolved entities
            if run_mode == "INCR" and watermark_col and not last_wm and has_membership:
                try:
                    existing_for_table = self.adapter.query_one(
                        f"SELECT 1 FROM idr_out.identity_resolved_membership_current "
                        f"WHERE entity_key LIKE '{table_id}:%' LIMIT 1"
                    )
                    if existing_for_table:
                        recovered_wm = self.adapter.query_one(
                            f"SELECT MAX({watermark_expr}) FROM {table_fqn}"
                        )
                        if recovered_wm:
                            last_wm = str(recovered_wm)
                except Exception:
                    pass

            # Note: For tables WITHOUT a watermark column, build_where_clause returns "1=1"
            # so all rows are scanned in extract_entities, and unchanged entities are pruned
            # via state-diffing against idr_out.identity_identifiers_current in extract_identifiers.
            where_clause = build_where_clause(
                watermark_col,
                last_wm,
                src.get("lookback", 0),
                run_mode,
                self.adapter.dialect,
            )

            deduplicate = bool(src.get("deduplicate", False))
            dedup_order_by = src.get("dedup_order_by") or ""
            self._source_dedup_meta[table_id] = (
                table_fqn,
                entity_key_expr,
                deduplicate,
                dedup_order_by,
                watermark_col,
            )
            source_rel = self._source_relation_sql(
                table_fqn,
                entity_key_expr,
                deduplicate=deduplicate,
                dedup_order_by=dedup_order_by,
                watermark_col=watermark_col,
            )

            union_parts.append(f"""
                SELECT
                    '{self.run_id}' AS run_id,
                    '{table_id}' AS table_id,
                    CONCAT('{table_id}', ':', CAST(({entity_key_expr}) AS {self._dialect["string_type"]})) AS entity_key,
                    {watermark_expr} AS watermark_value
                FROM {source_rel} _idr_src
                WHERE ({where_clause}) AND ({entity_key_expr}) IS NOT NULL
            """)

        union_sql = " UNION ALL ".join(union_parts)

        # Create entities_delta table
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.entities_delta AS
            {union_sql}
        """)

        # Enforce GDPR/CCPA persistent suppression across FULL and INCR reruns
        if self.adapter.table_exists("idr_meta.privacy_suppression"):
            suppressed_count = (
                self.adapter.query_one("SELECT COUNT(*) FROM idr_meta.privacy_suppression") or 0
            )
            if suppressed_count > 0:
                sha_expr = self._dialect["sha256"]("LOWER(TRIM(entity_key))")
                self.adapter.execute(f"""
                    DELETE FROM idr_work.entities_delta
                    WHERE {sha_expr} IN (SELECT entity_key_hash FROM idr_meta.privacy_suppression)
                """)

        self._has_retroactive_governance_changes = False
        self._has_no_wm_deletes = False
        if run_mode == "INCR" and has_membership:
            self._apply_retroactive_governance_in_incr()
            no_wm_tables = [f"'{tid}'" for tid in getattr(self, "_no_watermark_table_ids", [])]
            if no_wm_tables:
                no_wm_in = ", ".join(no_wm_tables)
                split_mem_expr = self._dialect["split_part"]("m.entity_key", ":", 1)
                self.adapter.execute(f"""
                    CREATE OR REPLACE TABLE idr_work.deleted_no_wm_entities AS
                    SELECT DISTINCT m.entity_key
                    FROM idr_out.identity_resolved_membership_current m
                    WHERE {split_mem_expr} IN ({no_wm_in})
                      AND NOT EXISTS (
                          SELECT 1 FROM idr_work.entities_delta e
                          WHERE e.entity_key = m.entity_key
                      )
                """)
                del_cnt = self.adapter.query_one("SELECT COUNT(*) FROM idr_work.deleted_no_wm_entities") or 0
                self._has_no_wm_deletes = del_cnt > 0

        count = self.adapter.query_one("SELECT COUNT(*) FROM idr_work.entities_delta")
        return count or 0

    def _apply_retroactive_governance_in_incr(self) -> None:
        """Seed INCR delta with clusters impacted by new exclusions or active overrides."""
        try:
            str_type = self._dialect["string_type"]
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.gov_affected_entities (
                    entity_key {str_type}
                )
            """)
            impacted_branches = []

            if self.adapter.table_exists("idr_meta.identifier_exclusion"):
                ex_cnt = (
                    self.adapter.query_one("SELECT COUNT(*) FROM idr_meta.identifier_exclusion") or 0
                )
                if ex_cnt > 0:
                    if self.adapter.table_exists("idr_out.identity_identifiers_current"):
                        excl_h = self._exclusion_match_predicate(
                            "h.identifier_type", "h.identifier_value_norm"
                        )
                        impacted_branches.append(f"""
                            SELECT DISTINCT h.entity_key
                            FROM idr_out.identity_identifiers_current h
                            WHERE {excl_h}
                        """)
                    if self.adapter.table_exists("idr_out.identity_edges_current"):
                        excl_e = self._exclusion_match_predicate(
                            "e.identifier_type", "e.identifier_value_norm"
                        )
                        impacted_branches.append(f"""
                            SELECT DISTINCT e.left_entity_key AS entity_key
                            FROM idr_out.identity_edges_current e
                            WHERE {excl_e}
                            UNION DISTINCT
                            SELECT DISTINCT e.right_entity_key AS entity_key
                            FROM idr_out.identity_edges_current e
                            WHERE {excl_e}
                        """)

            if self.adapter.table_exists("idr_meta.identity_override"):
                ov_cnt = (
                    self.adapter.query_one(
                        "SELECT COUNT(*) FROM idr_meta.identity_override"
                    )
                    or 0
                )
                if ov_cnt > 0:
                    # Clusters where DO_NOT_LINK pairs are currently merged together
                    impacted_branches.append("""
                        SELECT DISTINCT m1.entity_key
                        FROM idr_meta.identity_override o
                        JOIN idr_out.identity_resolved_membership_current m1
                          ON m1.entity_key = o.entity_key_a
                        JOIN idr_out.identity_resolved_membership_current m2
                          ON m2.entity_key = o.entity_key_b
                        WHERE o.is_active = TRUE
                          AND UPPER(o.override_type) = 'DO_NOT_LINK'
                          AND m1.resolved_id = m2.resolved_id
                    """)
                    # Deactivated DO_NOT_LINK pairs currently separated that share an identifier
                    if self.adapter.table_exists("idr_out.identity_identifiers_current"):
                        impacted_branches.append("""
                            SELECT DISTINCT m1.entity_key
                            FROM idr_meta.identity_override o
                            JOIN idr_out.identity_resolved_membership_current m1
                              ON m1.entity_key = o.entity_key_a
                            JOIN idr_out.identity_resolved_membership_current m2
                              ON m2.entity_key = o.entity_key_b
                            WHERE o.is_active = FALSE
                              AND UPPER(o.override_type) = 'DO_NOT_LINK'
                              AND m1.resolved_id != m2.resolved_id
                              AND EXISTS (
                                  SELECT 1 FROM idr_out.identity_identifiers_current i1
                                  JOIN idr_out.identity_identifiers_current i2
                                    ON i1.identifier_type = i2.identifier_type
                                   AND i1.identifier_value_norm = i2.identifier_value_norm
                                  WHERE i1.entity_key = o.entity_key_a
                                    AND i2.entity_key = o.entity_key_b
                              )
                            UNION DISTINCT
                            SELECT DISTINCT m2.entity_key
                            FROM idr_meta.identity_override o
                            JOIN idr_out.identity_resolved_membership_current m1
                              ON m1.entity_key = o.entity_key_a
                            JOIN idr_out.identity_resolved_membership_current m2
                              ON m2.entity_key = o.entity_key_b
                            WHERE o.is_active = FALSE
                              AND UPPER(o.override_type) = 'DO_NOT_LINK'
                              AND m1.resolved_id != m2.resolved_id
                              AND EXISTS (
                                  SELECT 1 FROM idr_out.identity_identifiers_current i1
                                  JOIN idr_out.identity_identifiers_current i2
                                    ON i1.identifier_type = i2.identifier_type
                                   AND i1.identifier_value_norm = i2.identifier_value_norm
                                  WHERE i1.entity_key = o.entity_key_a
                                    AND i2.entity_key = o.entity_key_b
                              )
                        """)
                        # Deactivated ISOLATE entities currently isolated that share an identifier with others
                        impacted_branches.append("""
                            SELECT DISTINCT m.entity_key
                            FROM idr_meta.identity_override o
                            JOIN idr_out.identity_resolved_membership_current m
                              ON m.entity_key = o.entity_key_a
                            WHERE o.is_active = FALSE
                              AND UPPER(o.override_type) = 'ISOLATE'
                              AND EXISTS (
                                  SELECT 1 FROM idr_out.identity_identifiers_current i1
                                  JOIN idr_out.identity_identifiers_current i2
                                    ON i1.identifier_type = i2.identifier_type
                                   AND i1.identifier_value_norm = i2.identifier_value_norm
                                  WHERE i1.entity_key = m.entity_key
                                    AND i2.entity_key != m.entity_key
                              )
                        """)
                    # Clusters where ISOLATE entity still shares a cluster with others
                    impacted_branches.append("""
                        SELECT DISTINCT m.entity_key
                        FROM idr_meta.identity_override o
                        JOIN idr_out.identity_resolved_membership_current m
                          ON m.entity_key = o.entity_key_a
                        WHERE o.is_active = TRUE
                          AND UPPER(o.override_type) = 'ISOLATE'
                          AND EXISTS (
                              SELECT 1 FROM idr_out.identity_resolved_membership_current m2
                              WHERE m2.resolved_id = m.resolved_id
                                AND m2.entity_key != m.entity_key
                          )
                    """)
                    # Clusters where FORCE_LINK pairs are currently in different clusters
                    impacted_branches.append("""
                        SELECT DISTINCT m1.entity_key
                        FROM idr_meta.identity_override o
                        JOIN idr_out.identity_resolved_membership_current m1
                          ON m1.entity_key = o.entity_key_a
                        JOIN idr_out.identity_resolved_membership_current m2
                          ON m2.entity_key = o.entity_key_b
                        WHERE o.is_active = TRUE
                          AND UPPER(o.override_type) = 'FORCE_LINK'
                          AND m1.resolved_id != m2.resolved_id
                        UNION DISTINCT
                        SELECT DISTINCT m2.entity_key
                        FROM idr_meta.identity_override o
                        JOIN idr_out.identity_resolved_membership_current m1
                          ON m1.entity_key = o.entity_key_a
                        JOIN idr_out.identity_resolved_membership_current m2
                          ON m2.entity_key = o.entity_key_b
                        WHERE o.is_active = TRUE
                          AND UPPER(o.override_type) = 'FORCE_LINK'
                          AND m1.resolved_id != m2.resolved_id
                    """)

            if not impacted_branches:
                return

            seed_union = " UNION DISTINCT ".join(impacted_branches)
            # Expand impacted seed entities to full cluster co-members
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.gov_affected_entities AS
                WITH seed AS ({seed_union}),
                target_clusters AS (
                    SELECT DISTINCT m.resolved_id
                    FROM seed s
                    JOIN idr_out.identity_resolved_membership_current m
                      ON m.entity_key = s.entity_key
                )
                SELECT DISTINCT m2.entity_key
                FROM target_clusters tc
                JOIN idr_out.identity_resolved_membership_current m2
                  ON tc.resolved_id = m2.resolved_id
                UNION DISTINCT
                SELECT DISTINCT entity_key FROM seed
            """)

            gov_cnt = self.adapter.query_one("SELECT COUNT(*) FROM idr_work.gov_affected_entities") or 0
            if gov_cnt > 0:
                split_expr = self._dialect["split_part"]("g.entity_key", ":", 1)
                self.adapter.execute(f"""
                    INSERT INTO idr_work.entities_delta (run_id, table_id, entity_key, watermark_value)
                    SELECT
                        '{self.run_id}' AS run_id,
                        {split_expr} AS table_id,
                        g.entity_key,
                        CAST(NULL AS {self._dialect["timestamp_type"]}) AS watermark_value
                    FROM idr_work.gov_affected_entities g
                    WHERE g.entity_key NOT IN (
                        SELECT entity_key FROM idr_work.entities_delta WHERE run_id = '{self.run_id}'
                    )
                """)
                self._has_retroactive_governance_changes = True
        except Exception as e:
            self.logger.warning(f"Retroactive governance seed in INCR mode failed: {e}")

    def extract_identifiers(self, run_mode: str = "FULL") -> int:
        """Extract and canonicalize identifiers from entities."""
        # Get identifier mappings (joined with source_table to avoid N+1 lookup queries)
        try:
            mappings = self.adapter.query("""
                SELECT
                    im.table_id, im.identifier_type, im.identifier_value_expr,
                    im.is_hashed, im.source_is_hashed, im.issuer_expr,
                    r.canonicalize, r.max_group_size,
                    st.table_fqn, st.entity_key_expr
                FROM idr_meta.identifier_mapping im
                JOIN idr_meta.rule r ON im.identifier_type = r.identifier_type
                LEFT JOIN idr_meta.source_table st ON im.table_id = st.table_id
                WHERE r.is_active = TRUE
            """)
        except Exception:
            mappings = self.adapter.query("""
                SELECT
                    im.table_id, im.identifier_type, im.identifier_value_expr,
                    im.is_hashed, im.source_is_hashed,
                    r.canonicalize, r.max_group_size,
                    st.table_fqn, st.entity_key_expr
                FROM idr_meta.identifier_mapping im
                JOIN idr_meta.rule r ON im.identifier_type = r.identifier_type
                LEFT JOIN idr_meta.source_table st ON im.table_id = st.table_id
                WHERE r.is_active = TRUE
            """)

        str_type = self._dialect["string_type"]

        if not mappings:
            self._warnings.append("No identifier mappings found")
            # Create empty table to ensure downstream steps don't fail
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.identifiers_all (
                    entity_key {str_type},
                    identifier_type {str_type},
                    identifier_value_norm {str_type},
                    is_hashed BOOLEAN,
                    max_group_size {self._dialect["int_type"]}
                )
            """)
            return 0

        # Build identifier extraction query
        union_parts = []
        for m in mappings:
            # Validate DB-sourced metadata before SQL interpolation
            table_id = self._validate_metadata_value(
                m["table_id"], "identifier", "identifier_mapping.table_id"
            )
            identifier_type = self._validate_metadata_value(
                m["identifier_type"], "identifier", "identifier_mapping.identifier_type"
            )
            value_expr = self._validate_metadata_value(
                m["identifier_value_expr"], "expr", "identifier_mapping.identifier_value_expr"
            )
            max_group_size = self._validate_metadata_value(
                m["max_group_size"], "integer", "rule.max_group_size"
            )
            canonicalize = self._validate_metadata_value(
                m["canonicalize"], "enum", "rule.canonicalize"
            )

            # Raw trimmed expression
            raw_expr = f"TRIM(CAST(({value_expr}) AS {str_type}))"

            # Apply canonicalization (built-in preset, registered fn, or inline SQL with {val})
            pre_hash_norm_expr = build_canonicalize_expr(
                raw_expr, canonicalize, self.adapter.dialect
            )

            # Optional issuer/namespace scoping (e.g., facility_id|mrn, vendor_id|sku)
            raw_issuer = m.get("issuer_expr")
            issuer_sql = (
                self._validate_metadata_value(
                    str(raw_issuer).strip(), "expr", "identifier_mapping.issuer_expr"
                )
                if raw_issuer is not None and str(raw_issuer).strip()
                else ""
            )
            if issuer_sql:
                issuer_clean = f"UPPER(TRIM(CAST(({issuer_sql}) AS {str_type})))"
                norm_expr = f"CONCAT({issuer_clean}, '|', {pre_hash_norm_expr})"
                issuer_where_sql = f"""
                    AND ({issuer_sql}) IS NOT NULL
                    AND LENGTH({issuer_clean}) > 0
                    AND LOWER({issuer_clean}) NOT IN ('null', 'none', 'n/a', 'undefined', 'nan', 'unknown', '')
                """
            else:
                norm_expr = pre_hash_norm_expr
                issuer_where_sql = ""

            # Apply PII hashing if enabled
            is_hashed = bool(m.get("is_hashed", False))
            hash_pii = is_hashed or os.environ.get("IDR_HASH_PII", "").lower() in ("1", "true", "yes")
            source_is_hashed = bool(m.get("source_is_hashed", False))
            if hash_pii and not source_is_hashed:
                if "sha256" in self._dialect:
                    norm_expr = self._dialect["sha256"](norm_expr)
                elif "md5" in self._dialect:
                    norm_expr = self._dialect["md5"](norm_expr)

            is_hashed_literal = (
                self._dialect.get("bool_true", "TRUE")
                if hash_pii or source_is_hashed
                else self._dialect.get("bool_false", "FALSE")
            )

            # Robust null, empty string, sentinel, and post-canonicalization non-empty check
            where_condition = f"""
                ({value_expr}) IS NOT NULL
                AND LENGTH({raw_expr}) > 0
                AND LOWER({raw_expr}) NOT IN ('null', 'none', 'n/a', 'undefined', 'nan', 'unknown', 'redacted', '0', 'default', '')
                AND ({pre_hash_norm_expr}) IS NOT NULL
                AND LENGTH(TRIM({pre_hash_norm_expr})) > 0
                {issuer_where_sql}
            """

            dedup_meta = getattr(self, "_source_dedup_meta", {}).get(table_id)
            if dedup_meta:
                source_rel = self._source_relation_sql(*dedup_meta)
            elif m.get("table_fqn"):
                source_rel = self._validate_metadata_value(
                    m["table_fqn"], "fqn", "source_table.table_fqn"
                )
            else:
                source_rel = self.get_source_fqn(table_id)

            if m.get("entity_key_expr"):
                key_expr = self._validate_metadata_value(
                    m["entity_key_expr"], "expr", "source_table.entity_key_expr"
                )
            else:
                key_expr = self.get_entity_key_expr(table_id)

            union_parts.append(f"""
                SELECT
                    e.entity_key,
                    '{identifier_type}' AS identifier_type,
                    {norm_expr} AS identifier_value_norm,
                    {is_hashed_literal} AS is_hashed,
                    {max_group_size} AS max_group_size
                FROM idr_work.entities_delta e
                JOIN {source_rel} src
                    ON e.table_id = '{table_id}'
                   AND e.entity_key = CONCAT('{table_id}', ':', CAST(({key_expr}) AS {str_type}))
                WHERE {where_condition}
            """)

        union_sql = " UNION ALL ".join(union_parts)

        # Stage 1: Store extracted delta identifiers
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.identifiers_delta AS
            {union_sql}
        """)

        # Apply exclusions (supports exact/wildcard identifier_type and case-insensitive patterns)
        try:
            if self.adapter.table_exists("idr_meta.identifier_exclusion"):
                excl_delta = self._exclusion_match_predicate(
                    "idr_work.identifiers_delta.identifier_type",
                    "idr_work.identifiers_delta.identifier_value_norm",
                )
                self.adapter.execute(f"""
                    DELETE FROM idr_work.identifiers_delta
                    WHERE {excl_delta}
                """)
        except Exception:
            pass  # Exclusion table may not exist - skip silently

        # Stage 1b: State-diff pruning for no-watermark tables in INCR mode.
        # Tables without a watermark column are scanned in full (WHERE 1=1) so in-place UPDATEs
        # to existing rows are never missed. Here we diff their extracted identifiers against
        # idr_out.identity_identifiers_current and prune entities whose identifiers are 100% unchanged.
        no_wm_tables = [f"'{tid}'" for tid in getattr(self, "_no_watermark_table_ids", [])]
        self._has_no_wm_deletes = False
        if (
            run_mode == "INCR"
            and no_wm_tables
            and self.adapter.table_exists("idr_out.identity_identifiers_current")
        ):
            try:
                no_wm_in = ", ".join(no_wm_tables)
                has_mem_tbl = self.adapter.table_exists(
                    "idr_out.identity_resolved_membership_current"
                )
                # Detect hard-deleted rows from un-watermarked tables (present in membership
                # for a no-watermark table_id, but absent from the full table scan in entities_delta).
                split_mem_expr = self._dialect["split_part"]("m.entity_key", ":", 1)
                if has_mem_tbl:
                    self.adapter.execute(f"""
                        CREATE OR REPLACE TABLE idr_work.deleted_no_wm_entities AS
                        SELECT DISTINCT m.entity_key
                        FROM idr_out.identity_resolved_membership_current m
                        WHERE {split_mem_expr} IN ({no_wm_in})
                          AND NOT EXISTS (
                              SELECT 1 FROM idr_work.entities_delta e
                              WHERE e.entity_key = m.entity_key
                          )
                    """)
                    del_cnt = self.adapter.query_one("SELECT COUNT(*) FROM idr_work.deleted_no_wm_entities") or 0
                    self._has_no_wm_deletes = del_cnt > 0
                else:
                    self.adapter.execute(
                        f"CREATE OR REPLACE TABLE idr_work.deleted_no_wm_entities (entity_key {str_type})"
                    )
                new_entity_branch = (
                    f"""
                    UNION DISTINCT
                    SELECT DISTINCT e.entity_key
                    FROM idr_work.entities_delta e
                    WHERE e.table_id IN ({no_wm_in})
                      AND NOT EXISTS (
                          SELECT 1 FROM idr_out.identity_resolved_membership_current m
                          WHERE m.entity_key = e.entity_key
                      )
                    """
                    if has_mem_tbl
                    else ""
                )
                self.adapter.execute(f"""
                    CREATE OR REPLACE TABLE idr_work.changed_no_wm_entities AS
                    SELECT DISTINCT d.entity_key
                    FROM idr_work.identifiers_delta d
                    JOIN idr_work.entities_delta e ON d.entity_key = e.entity_key
                    WHERE e.table_id IN ({no_wm_in})
                      AND NOT EXISTS (
                          SELECT 1 FROM idr_out.identity_identifiers_current h
                          WHERE h.entity_key = d.entity_key
                            AND h.identifier_type = d.identifier_type
                            AND h.identifier_value_norm = d.identifier_value_norm
                      )
                    UNION DISTINCT
                    SELECT DISTINCT e.entity_key
                    FROM idr_work.entities_delta e
                    JOIN idr_out.identity_identifiers_current h ON e.entity_key = h.entity_key
                    WHERE e.table_id IN ({no_wm_in})
                      AND NOT EXISTS (
                          SELECT 1 FROM idr_work.identifiers_delta d
                          WHERE d.entity_key = h.entity_key
                            AND d.identifier_type = h.identifier_type
                            AND d.identifier_value_norm = h.identifier_value_norm
                      )
                    {new_entity_branch}
                """)
                gov_filter = (
                    "AND entity_key NOT IN (SELECT entity_key FROM idr_work.gov_affected_entities)"
                    if self.adapter.table_exists("idr_work.gov_affected_entities")
                    else ""
                )
                self.adapter.execute(f"""
                    DELETE FROM idr_work.entities_delta
                    WHERE table_id IN ({no_wm_in})
                      AND entity_key NOT IN (
                          SELECT entity_key FROM idr_work.changed_no_wm_entities
                      )
                      {gov_filter}
                """)
                self.adapter.execute("""
                    DELETE FROM idr_work.identifiers_delta
                    WHERE entity_key NOT IN (
                        SELECT entity_key FROM idr_work.entities_delta
                    )
                """)
            except Exception as e:
                self.logger.warning(f"No-watermark state diff failed, keeping all rows: {e}")

        # Stage 2: Candidate expansion for incremental processing
        # In INCR mode, find existing entities in idr_out sharing any delta identifiers,
        # pull in their cluster co-members, and union them into identifiers_all.
        if run_mode == "INCR" and self.adapter.table_exists("idr_out.identity_identifiers_current"):
            try:
                # Snapshot entities directly re-extracted in this delta BEFORE cluster expansion
                # (including any hard-deleted no-watermark entities so their stale edges are pruned)
                del_union = (
                    """
                    UNION DISTINCT
                    SELECT DISTINCT entity_key FROM idr_work.deleted_no_wm_entities
                    """
                    if getattr(self, "_has_no_wm_deletes", False)
                    and self.adapter.table_exists("idr_work.deleted_no_wm_entities")
                    else ""
                )
                self.adapter.execute(f"""
                    CREATE OR REPLACE TABLE idr_work.reextracted_entities AS
                    SELECT DISTINCT entity_key FROM idr_work.entities_delta
                    {del_union}
                """)

                # Step 2a: Find matching historical entities sharing any delta identifier,
                # plus re-extracted entities so changed identifiers also pull old cluster co-members
                self.adapter.execute("""
                    CREATE OR REPLACE TABLE idr_work.matched_hist_entities AS
                    SELECT DISTINCT h.entity_key
                    FROM idr_work.identifiers_delta d
                    JOIN idr_out.identity_identifiers_current h
                      ON d.identifier_type = h.identifier_type
                     AND d.identifier_value_norm = h.identifier_value_norm
                    UNION DISTINCT
                    SELECT DISTINCT entity_key
                    FROM idr_work.reextracted_entities
                """)

                # Step 2b: Expand to all cluster co-members of the matched entities
                if self.adapter.table_exists("idr_out.identity_resolved_membership_current"):
                    self.adapter.execute("""
                        CREATE OR REPLACE TABLE idr_work.expanded_cluster_entities AS
                        SELECT DISTINCT m2.entity_key
                        FROM idr_work.matched_hist_entities e
                        JOIN idr_out.identity_resolved_membership_current m
                          ON e.entity_key = m.entity_key
                        JOIN idr_out.identity_resolved_membership_current m2
                          ON m.resolved_id = m2.resolved_id
                    """)
                else:
                    self.adapter.execute("""
                        CREATE OR REPLACE TABLE idr_work.expanded_cluster_entities AS
                        SELECT entity_key FROM idr_work.matched_hist_entities
                    """)

                # Step 2c: Add expanded historical entities to entities_delta for label propagation
                # (watermark_value is NULL to prevent artificially advancing source table watermarks)
                split_expr = self._dialect["split_part"]("e.entity_key", ":", 1)
                self.adapter.execute(f"""
                    INSERT INTO idr_work.entities_delta (run_id, table_id, entity_key, watermark_value)
                    SELECT
                        '{self.run_id}' AS run_id,
                        {split_expr} AS table_id,
                        e.entity_key,
                        NULL AS watermark_value
                    FROM idr_work.expanded_cluster_entities e
                    WHERE e.entity_key NOT IN (
                        SELECT entity_key FROM idr_work.entities_delta WHERE run_id = '{self.run_id}'
                    )
                """)

                # Step 2d: Combine delta identifiers with historical cluster identifiers.
                # For re-extracted entities, prefer fresh identifiers_delta over stale historical ones,
                # and filter out any values matched by active exclusions.
                excl_filter_sql = ""
                if self.adapter.table_exists("idr_meta.identifier_exclusion"):
                    excl_hist = self._exclusion_match_predicate(
                        "h.identifier_type", "h.identifier_value_norm"
                    )
                    excl_filter_sql = f"AND NOT {excl_hist}"
                self.adapter.execute(f"""
                    CREATE OR REPLACE TABLE idr_work.identifiers_all AS
                    SELECT entity_key, identifier_type, identifier_value_norm, is_hashed, max_group_size
                    FROM idr_work.identifiers_delta
                    UNION DISTINCT
                    SELECT h.entity_key, h.identifier_type, h.identifier_value_norm, h.is_hashed, 10000 AS max_group_size
                    FROM idr_work.expanded_cluster_entities exp
                    JOIN idr_out.identity_identifiers_current h
                      ON exp.entity_key = h.entity_key
                    WHERE h.entity_key NOT IN (
                        SELECT entity_key FROM idr_work.reextracted_entities
                    )
                    {excl_filter_sql}
                """)
            except Exception as e:
                self.logger.warning(f"Candidate expansion failed, falling back to delta: {e}")
                self.adapter.execute("""
                    CREATE OR REPLACE TABLE idr_work.identifiers_all AS
                    SELECT * FROM idr_work.identifiers_delta
                """)
        else:
            self.adapter.execute("""
                CREATE OR REPLACE TABLE idr_work.identifiers_all AS
                SELECT * FROM idr_work.identifiers_delta
            """)

        count = self.adapter.query_one("SELECT COUNT(*) FROM idr_work.identifiers_all")
        return count or 0

    def extract_attributes(self) -> int:
        """Extract attributes specifically configured for fuzzy matching."""
        str_type = self._dialect["string_type"]

        # Note: Golden Record survivorship (ProfileBuilder) queries source tables directly.
        # idr_work.entity_attributes is ONLY consumed by GraphStage.run_fuzzy_matching().
        # If there are no active fuzzy rules, skip materializing millions of attribute rows!
        has_fuzzy_rules = False
        try:
            if self.adapter.table_exists("idr_meta.fuzzy_rule"):
                fuzzy_cnt = (
                    self.adapter.query_one(
                        "SELECT COUNT(*) FROM idr_meta.fuzzy_rule WHERE is_active = TRUE"
                    )
                    or 0
                )
                has_fuzzy_rules = fuzzy_cnt > 0
        except Exception:
            has_fuzzy_rules = False

        if not has_fuzzy_rules:
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.entity_attributes (
                    entity_key {str_type},
                    attribute_name {str_type},
                    attribute_value {str_type}
                )
            """)
            return 0

        mappings = self.adapter.query("""
            SELECT
                eam.table_id, eam.attribute_name, eam.attribute_expr,
                st.table_fqn, st.entity_key_expr
            FROM idr_meta.entity_attribute_mapping eam
            LEFT JOIN idr_meta.source_table st ON eam.table_id = st.table_id
        """)

        if not mappings:
            return 0

        union_parts = []
        for m in mappings:
            # Validate DB-sourced metadata before SQL interpolation
            table_id = self._validate_metadata_value(
                m["table_id"], "identifier", "entity_attribute_mapping.table_id"
            )
            attr_name = self._validate_metadata_value(
                m["attribute_name"], "identifier", "entity_attribute_mapping.attribute_name"
            )
            expr = self._validate_metadata_value(
                m["attribute_expr"], "expr", "entity_attribute_mapping.attribute_expr"
            )

            # Cast everything to string for generic processing
            val_expr = f"CAST({expr} AS {str_type})"
            source_fqn = (
                self._validate_metadata_value(m["table_fqn"], "fqn", "source_table.table_fqn")
                if m.get("table_fqn")
                else self.get_source_fqn(table_id)
            )
            key_expr = (
                self._validate_metadata_value(
                    m["entity_key_expr"], "expr", "source_table.entity_key_expr"
                )
                if m.get("entity_key_expr")
                else self.get_entity_key_expr(table_id)
            )

            union_parts.append(f"""
                SELECT
                    e.entity_key,
                    '{attr_name}' AS attribute_name,
                    {val_expr} AS attribute_value
                FROM idr_work.entities_delta e
                JOIN {source_fqn} src
                    ON e.table_id = '{table_id}'
                   AND e.entity_key = CONCAT('{table_id}', ':', CAST(({key_expr}) AS {str_type}))
                WHERE {expr} IS NOT NULL
            """)

        if not union_parts:
            return 0

        union_sql = " UNION ALL ".join(union_parts)

        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.entity_attributes AS
            {union_sql}
        """)

        return self.adapter.query_one("SELECT COUNT(*) FROM idr_work.entity_attributes") or 0

    def get_source_fqn(self, table_id: str) -> str:
        """Get fully qualified table name for a source (cached per stage instance)."""
        safe_id = self._validate_metadata_value(table_id, "identifier", "table_id")
        cache = getattr(self, "_fqn_cache", None)
        if cache is None:
            cache = {}
            self._fqn_cache = cache
        if safe_id in cache:
            return cache[safe_id]
        result = self.adapter.query_one(f"""
            SELECT table_fqn FROM idr_meta.source_table WHERE table_id = '{safe_id}'
        """)
        if result:
            self._validate_metadata_value(result, "fqn", "source_table.table_fqn")
        val = result or table_id
        cache[safe_id] = val
        return val

    def get_entity_key_expr(self, table_id: str) -> str:
        """Get entity key expression for a source (cached per stage instance)."""
        safe_id = self._validate_metadata_value(table_id, "identifier", "table_id")
        cache = getattr(self, "_key_expr_cache", None)
        if cache is None:
            cache = {}
            self._key_expr_cache = cache
        if safe_id in cache:
            return cache[safe_id]
        result = self.adapter.query_one(f"""
            SELECT entity_key_expr FROM idr_meta.source_table WHERE table_id = '{safe_id}'
        """)
        if result:
            self._validate_metadata_value(result, "expr", "source_table.entity_key_expr")
        val = result or "id"
        cache[safe_id] = val
        return val
