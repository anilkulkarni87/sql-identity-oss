"""
Profile Builder for IDR.

Generates the Golden Record (Unified Profile) by applying survivorship rules
to the resolved identity clusters.
"""

import json

from .adapters.base import IDRAdapter, get_dialect_config
from .config import validate_fqn, validate_identifier, validate_sql_expr


class ProfileBuilder:
    """
    Builds the unified profile table from resolved identities and source data.
    """

    def __init__(self, adapter: IDRAdapter):
        self.adapter = adapter
        self.dialect_name = adapter.dialect
        self.dialect = get_dialect_config(self.dialect_name)

    def build_profiles(self, run_id: str, run_mode: str = "FULL") -> None:
        """
        Execute the profile building process.

        Args:
            run_id: Current run ID to tag outputs
            run_mode: Execution mode ('FULL' or 'INCR')
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

        is_incremental = (
            run_mode == "INCR"
            and self.adapter.table_exists("idr_out.golden_profiles")
            and self.adapter.table_exists("idr_work.impacted_resolved_ids")
        )

        # 2. Build the Attribute Staging CTE
        # In INCR mode, strictly filter staging to impacted cluster members
        staging_cte = self._build_staging_cte(
            survivorship_rules, attr_mappings, source_tables, is_incremental=is_incremental
        )

        if not staging_cte:
            return

        # 3. Build the Pivoted Selection Logic
        # Map both table_fqn and table_id -> validated table_id so source_priority_list
        # can reference either source ID ('crm') or table FQN ('raw.crm').
        source_id_lookup = {}
        for src in source_tables:
            tid = str(src["table_id"])
            fqn = str(src["table_fqn"])
            source_id_lookup[tid] = tid
            source_id_lookup[fqn] = tid
            source_id_lookup[fqn.replace(".", "_")] = tid

        # For each attribute, generate the specific aggregation SQL based on strategy
        selection_columns = []
        attr_names = []
        for rule in survivorship_rules:
            attr_name = validate_identifier(str(rule["attribute_name"]), "attribute_name")
            attr_names.append(attr_name)
            col_sql = self._build_column_selection(rule, source_id_lookup=source_id_lookup)
            selection_columns.append(col_sql)

        # 4. Execute final generation: Upsert in INCR mode, CTAS in FULL mode
        select_clause = ",\n    ".join(selection_columns)
        all_cols = ["resolved_id", "run_id", "updated_at"] + attr_names
        update_cols = ["run_id", "updated_at"] + attr_names

        if is_incremental:
            print("Incrementally updating Golden Profiles for impacted clusters...")
            # Step A: Materialize delta profiles for impacted clusters only
            self.adapter.execute(f"""
                CREATE OR REPLACE TABLE idr_work.golden_profiles_delta AS
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
            """)

            # Step B: Upsert delta profiles into production table
            if self.dialect_name in ("duckdb", "postgres"):
                try:
                    self.adapter.execute(
                        "CREATE UNIQUE INDEX IF NOT EXISTS idx_golden_profiles_pk ON idr_out.golden_profiles (resolved_id)"
                    )
                except Exception:
                    pass
                keys_sql = "resolved_id"
                inserts_sql = ", ".join(all_cols)
                updates_sql = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                upsert_sql = f"""
                    INSERT INTO idr_out.golden_profiles ({inserts_sql})
                    SELECT {inserts_sql} FROM idr_work.golden_profiles_delta
                    ON CONFLICT ({keys_sql}) DO UPDATE SET
                    {updates_sql}
                """
                self.adapter.execute(upsert_sql)
            else:
                join_cond = "T.resolved_id = S.resolved_id"
                updates_sql = ", ".join([f"T.{col} = S.{col}" for col in update_cols])
                inserts_sql = ", ".join(all_cols)
                values_sql = ", ".join([f"S.{col}" for col in all_cols])
                merge_sql = f"""
                    MERGE INTO idr_out.golden_profiles T
                    USING idr_work.golden_profiles_delta S
                    ON {join_cond}
                    WHEN MATCHED THEN
                        UPDATE SET {updates_sql}
                    WHEN NOT MATCHED THEN
                        INSERT ({inserts_sql})
                        VALUES ({values_sql})
                """
                self.adapter.execute(merge_sql)

            # Step C: Cleanup phantom clusters that dissolved / merged away
            self.adapter.execute("""
                DELETE FROM idr_out.golden_profiles
                WHERE NOT EXISTS (
                    SELECT 1 FROM idr_out.identity_resolved_membership_current m
                    WHERE m.resolved_id = idr_out.golden_profiles.resolved_id
                )
            """)
            print("✅ Golden Profiles incrementally updated.")
        else:
            # Databricks Delta Lake requires timestampNtz feature enabled when projecting Parquet timestamps without timezone
            tblproperties_clause = ""
            if self.dialect_name == "databricks":
                tblproperties_clause = "\n            TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')"

            final_sql = f"""
                CREATE OR REPLACE TABLE idr_out.golden_profiles{tblproperties_clause} AS
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

            print("Generating Golden Profiles...")
            self.adapter.execute(final_sql)
            if self.dialect_name in ("duckdb", "postgres"):
                try:
                    self.adapter.execute(
                        "CREATE UNIQUE INDEX IF NOT EXISTS idx_golden_profiles_pk ON idr_out.golden_profiles (resolved_id)"
                    )
                except Exception:
                    pass
            print("✅ Golden Profiles generated.")

    def _build_staging_cte(self, rules, mappings, sources, is_incremental: bool = False) -> str:
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
            tid = validate_identifier(str(src["table_id"]), "source_table.table_id")
            fqn = validate_fqn(str(src["table_fqn"]), "source_table.table_fqn")
            key_expr = validate_sql_expr(str(src["entity_key_expr"]), "source_table.entity_key_expr")
            raw_wm = src.get("watermark_column")
            if raw_wm and str(raw_wm).strip().lower() not in ("", "none", "null"):
                wm_col = validate_identifier(str(raw_wm).strip(), "source_table.watermark_column")
            else:
                wm_col = None

            mapped_cols = []
            # For every requested golden attribute, see if this source maps it
            for attr, rule in rule_map.items():
                safe_attr = validate_identifier(str(attr), "attribute_name")
                if tid in table_vars and attr in table_vars[tid]:
                    safe_expr = validate_sql_expr(str(table_vars[tid][attr]), f"attribute_expr[{attr}]")
                    mapped_cols.append(f"{safe_expr} AS {safe_attr}")
                else:
                    mapped_cols.append(f"NULL AS {safe_attr}")

            cols_sql = ",\n                ".join(mapped_cols)
            ts_type = self.dialect.get("timestamp_type", "TIMESTAMP")
            recency_expr = f"CAST(src.{wm_col} AS {ts_type})" if wm_col else f"CAST(NULL AS {ts_type})"
            where_clause = ""
            if is_incremental:
                where_clause = "\n                WHERE m.resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids)"

            unions.append(f"""
                SELECT
                    m.resolved_id,
                    '{tid}' AS source_id,
                    {recency_expr} AS recency_ts,
                    {cols_sql}
                FROM {fqn} src
                JOIN idr_out.identity_resolved_membership_current m
                  ON m.entity_key = CONCAT('{tid}:', CAST(({key_expr}) AS {self.dialect["string_type"]}))
                {where_clause}
            """)

        return " UNION ALL ".join(unions)

    def _build_column_selection(self, rule, source_id_lookup=None) -> str:
        """
        Generates the aggregation logic for a single attribute (e.g. ARG_MAX).
        """
        attr = validate_identifier(str(rule["attribute_name"]), "attribute_name")
        strategy = rule["strategy"]
        source_id_lookup = source_id_lookup or {}

        # DuckDB / BigQuery / Snowflake / Databricks syntax differences handled here

        if strategy == "RECENCY":
            # ARG_MAX(attr, recency_ts) with fallback sentinel timestamp for un-watermarked dimension tables
            # DuckDB: arg_max(val, ts) ignores NULL weights; coalesce NULL recency_ts so dimension values survive
            # BigQuery: ARRAY_AGG(x IGNORE NULLS ORDER BY ts DESC LIMIT 1)[SAFE_OFFSET(0)]
            # Snowflake / Databricks: MAX_BY(val, ts) ignores NULL weights; coalesce NULL recency_ts
            if self.dialect_name == "duckdb":
                return (
                    f"arg_max({attr}, CASE WHEN {attr} IS NOT NULL "
                    f"THEN COALESCE(recency_ts, TIMESTAMP '1970-01-01 00:00:00') ELSE NULL END) AS {attr}"
                )
            elif self.dialect_name in ("snowflake", "databricks"):
                return (
                    f"MAX_BY({attr}, CASE WHEN {attr} IS NOT NULL "
                    f"THEN COALESCE(recency_ts, TIMESTAMP '1970-01-01 00:00:00') ELSE NULL END) AS {attr}"
                )
            elif self.dialect_name == "bigquery":
                return f"ARRAY_AGG({attr} IGNORE NULLS ORDER BY recency_ts DESC LIMIT 1)[SAFE_OFFSET(0)] AS {attr}"

        elif strategy == "PRIORITY":
            # Priority list logic
            # Assign specific integer priority to source_id and use ARG_MIN / MIN_BY
            try:
                raw_list = rule.get("source_priority_list") or "[]"
                if isinstance(raw_list, list):
                    p_list = raw_list
                else:
                    p_list = json.loads(str(raw_list).replace("'", '"'))
            except Exception:
                p_list = []

            # Generate Case-based priority map (penalize NULL attribute values so non-null wins)
            when_clauses = [f"WHEN {attr} IS NULL THEN 999999"]
            for idx, src in enumerate(p_list):
                resolved_src = source_id_lookup.get(str(src), str(src).replace(".", "_"))
                safe_src = validate_identifier(resolved_src, "source_priority_item")
                when_clauses.append(f"WHEN source_id = '{safe_src}' THEN {idx}")
            else_val = 999

            priority_expr = f"CASE {' '.join(when_clauses)} ELSE {else_val} END"

            if self.dialect_name == "duckdb":
                return f"arg_min({attr}, {priority_expr}) AS {attr}"
            elif self.dialect_name in ("snowflake", "databricks"):
                return f"MIN_BY({attr}, {priority_expr}) AS {attr}"
            elif self.dialect_name == "bigquery":
                return f"ARRAY_AGG({attr} IGNORE NULLS ORDER BY {priority_expr} ASC LIMIT 1)[SAFE_OFFSET(0)] AS {attr}"

        elif strategy == "FREQUENCY":
            # MODE calculation
            if self.dialect_name == "duckdb":
                return f"mode({attr}) AS {attr}"
            elif self.dialect_name in ("snowflake", "databricks"):
                return f"MODE({attr}) AS {attr}"
            elif self.dialect_name == "bigquery":
                return f"APPROX_TOP_COUNT({attr}, 1)[SAFE_OFFSET(0)].value AS {attr}"

        # Default fallback
        return f"MAX({attr}) AS {attr}"
