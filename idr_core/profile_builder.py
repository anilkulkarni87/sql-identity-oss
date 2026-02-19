"""
Profile Builder for IDR.

Generates the Golden Record (Unified Profile) by applying survivorship rules
to the resolved identity clusters.
"""

import json

from .adapters.base import IDRAdapter, get_dialect_config


class ProfileBuilder:
    """
    Builds the unified profile table from resolved identities and source data.
    """

    def __init__(self, adapter: IDRAdapter):
        self.adapter = adapter
        self.dialect_name = adapter.dialect
        self.dialect = get_dialect_config(self.dialect_name)

    def build_profiles(self, run_id: str) -> None:
        """
        Execute the profile building process.

        Args:
            run_id: Current run ID to tag outputs
        """
        # 1. Fetch metadata
        survivorship_rules = self.adapter.query(
            "SELECT * FROM idr_meta.survivorship_rule WHERE is_active = TRUE"
        )
        attr_mappings = self.adapter.query("SELECT * FROM idr_meta.entity_attribute_mapping")
        source_tables = self.adapter.query(
            "SELECT * FROM idr_meta.source_table WHERE is_active = TRUE"
        )

        if not survivorship_rules:
            print("No survivorship rules defined. Skipping profile generation.")
            return

        # 2. Build the Attribute Staging CTE
        # This creates a normalized view of (resolved_id, attribute_name, value, timestamp, priority)
        staging_cte = self._build_staging_cte(survivorship_rules, attr_mappings, source_tables)

        # 3. Build the Pivoted Selection Logic
        # For each attribute, generate the specific aggregation SQL based on strategy
        selection_columns = []
        for rule in survivorship_rules:
            col_sql = self._build_column_selection(rule)
            selection_columns.append(col_sql)

        # 4. Execute final Create Table As Select (CTAS)
        select_clause = ",\n    ".join(selection_columns)

        final_sql = f"""
            CREATE OR REPLACE TABLE idr_out.unified_profile AS
            WITH staging AS (
                {staging_cte}
            )
            SELECT
                resolved_id,
                '{run_id}' as run_id,
                {self.dialect["current_timestamp"]} as updated_at,
                {select_clause}
            FROM staging
            GROUP BY resolved_id
        """

        print("Generating Unified Profiles...")
        self.adapter.execute(final_sql)
        print("✅ Unified Profiles generated.")

    def _build_staging_cte(self, rules, mappings, sources) -> str:
        """
        Generates a massive UNION ALL CTE to fetch attribute values.
        """
        unions = []

        # Organize mappings by table
        # {table_id: {attr_name: expr}}
        table_vars = {}
        for m in mappings:
            t = m["table_id"]
            if t not in table_vars:
                table_vars[t] = {}
            table_vars[t][m["attribute_name"]] = m["attribute_expr"]

        # Organize rules by attribute
        # {attr_name: rule}
        rule_map = {r["attribute_name"]: r for r in rules}

        for src in sources:
            tid = src["table_id"]
            fqn = src["table_fqn"]
            key_expr = src["entity_key_expr"]
            # Default recency is watermark column
            # Complexity: Different rules might want different timestamps.
            # Simplified: Use source watermark as default recency.
            _ = src["watermark_column"]  # available for future per-rule recency

            # For each attribute desired in the outcome, check if this source maps it
            for attr, rule in rule_map.items():
                if tid in table_vars and attr in table_vars[tid]:
                    # We project: resolved_id, attr_name, value, recency, source_id
                    # But we can't UNION mixed types easily.
                    # We must cast everything to STRING for the generic staging,
                    # OR build a wide table. Wide table is safer for types but harder to coalesce?
                    # "Wide" approach:
                    # SELECT resolved_id, 'dig' as src, email, phone, ... FROM ...
                    pass

            # Refined Approach: Wide Table Staging
            # Instead of EAV (Entity-Attribute-Value), we Select ALL mapped attributes from each source
            # and UNION them.
            # Then Aggregation is easier.

        # Let's switch to Wide Table UNION strategy
        # SELECT m.resolved_id, 'digital' as source_id, s.final_at as recency, s.email, s.phone FROM ...

        for src in sources:
            tid = src["table_id"]
            fqn = src["table_fqn"]
            key_expr = src["entity_key_expr"]

            mapped_cols = []
            # For every requested golden attribute, see if this source has it
            for attr, rule in rule_map.items():
                if tid in table_vars and attr in table_vars[tid]:
                    mapped_cols.append(f"{table_vars[tid][attr]} AS {attr}")
                else:
                    mapped_cols.append(f"NULL AS {attr}")

            cols_sql = ",\n                ".join(mapped_cols)

            unions.append(f"""
                SELECT
                    m.resolved_id,
                    '{tid}' AS source_id,
                    {f"src.{src['watermark_column']}" if src.get("watermark_column") else "NULL"} AS recency_ts,
                    {cols_sql}
                FROM {fqn} src
                JOIN idr_out.identity_resolved_membership_current m
                  ON m.entity_key = CONCAT('{tid}:', CAST(src.{key_expr} AS {self.dialect["string_type"]}))
            """)

        return " UNION ALL ".join(unions)

    def _build_column_selection(self, rule) -> str:
        """
        Generates the aggregation logic for a single attribute (e.g. ARG_MAX).
        """
        attr = rule["attribute_name"]
        strategy = rule["strategy"]

        # DuckDB / BigQuery / Snowflake syntax differences handled here

        if strategy == "RECENCY":
            # ARG_MAX(attr, recency_ts)
            # DuckDB: arg_max(val, ts)
            # BigQuery: ARRAY_AGG(x ORDER BY ts DESC LIMIT 1)[OFFSET(0)]
            # Snowflake: MAX_BY(val, ts)
            if self.dialect_name == "duckdb":
                return f"arg_max({attr}, recency_ts) AS {attr}"
            elif self.dialect_name == "snowflake":
                return f"MAX_BY({attr}, recency_ts) AS {attr}"
            elif self.dialect_name == "bigquery":
                return f"ARRAY_AGG({attr} IGNORE NULLS ORDER BY recency_ts DESC LIMIT 1)[SAFE_OFFSET(0)] AS {attr}"

        elif strategy == "PRIORITY":
            # Priority list logic
            # LIST_SORT based on source_priority?
            # Or conditional Logic?
            # CASE logic is complex in aggregation.
            # Easier: Assign specific integer priority to source_id and use ARG_MIN(priority)

            # Parse list
            try:
                p_list = json.loads(rule["source_priority_list"].replace("'", '"'))
            except Exception:
                p_list = []

            # Generate Case-based priority map
            # 1 = High, 999 = Low
            when_clauses = [
                f"WHEN source_id = '{src}' THEN {idx}" for idx, src in enumerate(p_list)
            ]
            else_val = 999

            if not when_clauses:
                priority_expr = str(else_val)
            else:
                priority_expr = f"CASE {' '.join(when_clauses)} ELSE {else_val} END"

            if self.dialect_name == "duckdb":
                return f"arg_min({attr}, {priority_expr}) AS {attr}"
            elif self.dialect_name == "snowflake":
                return f"MIN_BY({attr}, {priority_expr}) AS {attr}"
            elif self.dialect_name == "bigquery":
                return f"ARRAY_AGG({attr} IGNORE NULLS ORDER BY {priority_expr} ASC LIMIT 1)[SAFE_OFFSET(0)] AS {attr}"

        elif strategy == "FREQUENCY":
            # MODE calculation
            if self.dialect_name == "duckdb":
                return f"mode({attr}) AS {attr}"
            elif self.dialect_name == "snowflake":
                return f"MODE({attr}) AS {attr}"
            elif self.dialect_name == "bigquery":
                # BigQuery has no simple MODE function for strings?
                # Approx_top_sum or standard logic
                return f"APPROX_TOP_COUNT({attr}, 1)[SAFE_OFFSET(0)].value AS {attr}"

        # Default fallback
        return f"MAX({attr}) AS {attr}"
