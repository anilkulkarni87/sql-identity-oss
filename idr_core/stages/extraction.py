"""
Extraction stage for IDR pipeline.

Handles entity extraction from source tables, identifier extraction
and canonicalization, and attribute extraction for fuzzy matching.
"""

from ..config import build_where_clause
from .base import BaseStage


class ExtractionStage(BaseStage):
    """Extracts entities, identifiers, and attributes from source tables."""

    def extract_entities(self, run_mode: str) -> int:
        """Extract entities from source tables."""
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

        # Build UNION query for all sources
        union_parts = []
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

            watermark_col = src.get("watermark_column") or ""
            if watermark_col:
                self._validate_metadata_value(
                    watermark_col, "identifier", "source_table.watermark_column"
                )

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

            where_clause = build_where_clause(
                watermark_col,
                src.get("last_watermark_value"),
                src.get("lookback", 0),
                run_mode,
                self.adapter.dialect,
            )

            union_parts.append(f"""
                SELECT
                    '{self.run_id}' AS run_id,
                    '{table_id}' AS table_id,
                    CONCAT('{table_id}', ':', CAST(({entity_key_expr}) AS {self._dialect["string_type"]})) AS entity_key,
                    {watermark_expr} AS watermark_value
                FROM {table_fqn}
                WHERE {where_clause}
            """)

        union_sql = " UNION ALL ".join(union_parts)

        # Create entities_delta table
        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.entities_delta AS
            {union_sql}
        """)

        count = self.adapter.query_one("SELECT COUNT(*) FROM idr_work.entities_delta")
        return count or 0

    def extract_identifiers(self) -> int:
        """Extract and canonicalize identifiers from entities."""
        # Get identifier mappings
        mappings = self.adapter.query("""
            SELECT
                im.table_id, im.identifier_type, im.identifier_value_expr, im.is_hashed,
                r.canonicalize, r.max_group_size
            FROM idr_meta.identifier_mapping im
            JOIN idr_meta.rule r ON im.identifier_type = r.identifier_type
            WHERE r.is_active = TRUE
        """)

        if not mappings:
            self._warnings.append("No identifier mappings found")
            # Create empty table to ensure downstream steps don't fail
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.identifiers_all (
                    entity_key {self._dialect["string_type"]},
                    identifier_type {self._dialect["string_type"]},
                    identifier_value_norm {self._dialect["string_type"]},
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

            # Apply canonicalization
            if canonicalize == "LOWERCASE":
                value_expr = f"LOWER({value_expr})"
            elif canonicalize == "UPPERCASE":
                value_expr = f"UPPER({value_expr})"

            union_parts.append(f"""
                SELECT
                    e.entity_key,
                    '{identifier_type}' AS identifier_type,
                    {value_expr} AS identifier_value_norm,
                    {max_group_size} AS max_group_size
                FROM idr_work.entities_delta e
                JOIN {self.get_source_fqn(table_id)} src
                    ON e.entity_key = CONCAT('{table_id}', ':', CAST(({self.get_entity_key_expr(table_id)}) AS {self._dialect["string_type"]}))
                WHERE {value_expr} IS NOT NULL
            """)

        union_sql = " UNION ALL ".join(union_parts)

        self.adapter.execute(f"""
            CREATE OR REPLACE TABLE idr_work.identifiers_all AS
            {union_sql}
        """)

        # Apply exclusions (optional - table may not exist)
        try:
            if self.adapter.table_exists("idr_meta.identifier_exclusion"):
                self.adapter.execute("""
                    DELETE FROM idr_work.identifiers_all
                    WHERE EXISTS (
                        SELECT 1 FROM idr_meta.identifier_exclusion ex
                        WHERE idr_work.identifiers_all.identifier_type = ex.identifier_type
                        AND (
                            (ex.match_type = 'EXACT' AND idr_work.identifiers_all.identifier_value_norm = ex.identifier_value_pattern)
                            OR (ex.match_type = 'LIKE' AND idr_work.identifiers_all.identifier_value_norm LIKE ex.identifier_value_pattern)
                        )
                    )
                """)
        except Exception:
            pass  # Exclusion table may not exist - skip silently

        count = self.adapter.query_one("SELECT COUNT(*) FROM idr_work.identifiers_all")
        return count or 0

    def extract_attributes(self) -> int:
        """Extract attributes specifically configured for fuzzy matching/survivorship."""
        mappings = self.adapter.query("""
            SELECT table_id, attribute_name, attribute_expr
            FROM idr_meta.entity_attribute_mapping
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
            val_expr = f"CAST({expr} AS {self._dialect['string_type']})"

            union_parts.append(f"""
                SELECT
                    e.entity_key,
                    '{attr_name}' AS attribute_name,
                    {val_expr} AS attribute_value
                FROM idr_work.entities_delta e
                JOIN {self.get_source_fqn(table_id)} src
                    ON e.entity_key = CONCAT('{table_id}', ':', CAST(({self.get_entity_key_expr(table_id)}) AS {self._dialect["string_type"]}))
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
        """Get fully qualified table name for a source."""
        # Validate table_id before interpolation
        safe_id = self._validate_metadata_value(table_id, "identifier", "table_id")
        result = self.adapter.query_one(f"""
            SELECT table_fqn FROM idr_meta.source_table WHERE table_id = '{safe_id}'
        """)
        if result:
            # Validate the returned FQN too
            self._validate_metadata_value(result, "fqn", "source_table.table_fqn")
        return result or table_id

    def get_entity_key_expr(self, table_id: str) -> str:
        """Get entity key expression for a source."""
        # Validate table_id before interpolation
        safe_id = self._validate_metadata_value(table_id, "identifier", "table_id")
        result = self.adapter.query_one(f"""
            SELECT entity_key_expr FROM idr_meta.source_table WHERE table_id = '{safe_id}'
        """)
        if result:
            # Validate the returned expression too
            self._validate_metadata_value(result, "expr", "source_table.entity_key_expr")
        return result or "id"
