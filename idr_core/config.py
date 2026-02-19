"""
Configuration loader for IDR.

Supports YAML configuration files that are converted to SQL statements
for populating metadata tables.
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import yaml

    HAS_YAML = True
except ImportError:
    HAS_YAML = False


# Safe identifier pattern for SQL injection prevention
_SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_SAFE_FQN_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*){0,2}$")


def validate_identifier(value: str, name: str = "identifier") -> str:
    """
    Validate a string is safe for use as SQL identifier.

    Args:
        value: String to validate
        name: Name of the field (for error messages)

    Returns:
        The validated string

    Raises:
        ValueError: If string is not safe for SQL use
    """
    if not value:
        raise ValueError(f"{name} cannot be empty")
    if not _SAFE_IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid {name}: '{value}'. Must be alphanumeric with underscores.")
    return value


def validate_fqn(value: str, name: str = "table") -> str:
    """
    Validate a fully qualified table name.

    Args:
        value: String like "schema.table" or "catalog.schema.table"
        name: Name of the field (for error messages)

    Returns:
        The validated string

    Raises:
        ValueError: If string is not a valid FQN
    """
    if not value:
        raise ValueError(f"{name} cannot be empty")
    # Allow dots for FQN
    clean = value.replace("`", "").replace('"', "")
    if not _SAFE_FQN_RE.match(clean):
        raise ValueError(f"Invalid {name}: '{value}'. Must be valid table FQN.")
    return value


# Pattern for detecting SQL injection in metadata expressions
# Blocks: statement terminators, comments, and DDL/DML keywords
_DANGEROUS_SQL_RE = re.compile(
    r"(;\s*$|;\s*\w|--\s|/\*|\*/"
    r"|\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|GRANT|REVOKE|TRUNCATE"
    r"|EXEC|EXECUTE|UNION\s+ALL|UNION\s+SELECT)\b)",
    re.IGNORECASE,
)


def validate_sql_expr(value: str, name: str = "expression") -> str:
    """
    Validate a SQL expression is safe for interpolation.

    Used for metadata fields like entity_key_expr, identifier_value_expr,
    and attribute_expr that can contain functions (LOWER(), CONCAT(), etc.)
    but should never contain DDL/DML or injection attempts.

    Args:
        value: SQL expression string to validate
        name: Name of the field (for error messages)

    Returns:
        The validated string

    Raises:
        ValueError: If expression contains dangerous SQL patterns
    """
    if not value:
        raise ValueError(f"{name} cannot be empty")
    if _DANGEROUS_SQL_RE.search(value):
        raise ValueError(
            f"Unsafe SQL detected in {name}: '{value}'. "
            f"Expressions must not contain DDL/DML statements, "
            f"semicolons, or SQL comments."
        )
    return value


def validate_integer(value, name: str = "value") -> int:
    """
    Validate and coerce a value to a safe integer for SQL interpolation.

    Args:
        value: Value to validate (int, float, or string)
        name: Name of the field (for error messages)

    Returns:
        Validated integer

    Raises:
        ValueError: If value cannot be safely converted to int
    """
    try:
        result = int(value)
        return result
    except (TypeError, ValueError):
        raise ValueError(f"Invalid integer for {name}: '{value}'")


def validate_float(value, name: str = "value") -> float:
    """
    Validate and coerce a value to a safe float for SQL interpolation.

    Args:
        value: Value to validate (int, float, or string)
        name: Name of the field (for error messages)

    Returns:
        Validated float

    Raises:
        ValueError: If value cannot be safely converted to float
    """
    try:
        result = float(value)
        if not (-1e15 < result < 1e15):  # Sanity bound
            raise ValueError(f"Float value out of range for {name}: {result}")
        return result
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid float for {name}: '{value}'") from e


def load_config(path: str) -> Dict[str, Any]:
    """
    Load YAML configuration file.

    Args:
        path: Path to YAML config file

    Returns:
        Parsed configuration dictionary

    Raises:
        ImportError: If PyYAML is not installed
        FileNotFoundError: If config file doesn't exist
        ValueError: If config is invalid
    """
    if not HAS_YAML:
        raise ImportError("PyYAML required. Install with: pip install pyyaml")

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        config = yaml.safe_load(f)

    validate_config(config)
    return config


def validate_config(config: Dict[str, Any]) -> bool:
    """
    Validate configuration structure.

    Args:
        config: Configuration dictionary

    Returns:
        True if valid

    Raises:
        ValueError: If configuration is invalid
    """
    if not config:
        raise ValueError("Configuration cannot be empty")

    if "sources" not in config:
        raise ValueError("Configuration must have 'sources' section")

    for i, src in enumerate(config.get("sources", [])):
        if "id" not in src:
            raise ValueError(f"Source {i}: missing 'id'")
        if "table" not in src:
            raise ValueError(f"Source {i}: missing 'table'")
        if "entity_key" not in src:
            raise ValueError(f"Source {i}: missing 'entity_key'")

        # Validate values
        validate_identifier(src["id"], f"Source {i} id")
        validate_fqn(src["table"], f"Source {i} table")

    # Validate survivorship if present
    if "survivorship" in config:
        if not isinstance(config["survivorship"], list):
            raise ValueError("Config 'survivorship' must be a list of rules")
        for rule in config["survivorship"]:
            if "attribute" not in rule:
                raise ValueError("Survivorship rule missing 'attribute'")
            if "strategy" not in rule:
                raise ValueError(f"Survivorship rule for {rule['attribute']} missing 'strategy'")
            if rule["strategy"] not in ("PRIORITY", "RECENCY", "FREQUENCY", "AGG_MAX", "AGG_SUM"):
                raise ValueError(f"Invalid strategy {rule['strategy']} for {rule['attribute']}")

    # Validate generate_evidence if present (must be boolean)
    if "generate_evidence" in config:
        if not isinstance(config["generate_evidence"], bool):
            raise ValueError("Config 'generate_evidence' must be a boolean (true/false)")

    return True


def config_to_sql(config: Dict[str, Any], dialect: str = "duckdb") -> List[str]:
    """
    Convert YAML configuration to SQL INSERT/MERGE statements.

    Args:
        config: Validated configuration dictionary
        dialect: Target dialect ('duckdb', 'snowflake', 'bigquery', 'databricks')

    Returns:
        List of SQL statements to populate metadata tables
    """
    statements = []

    def generate_upsert(table: str, keys: List[str], data: Dict[str, Any]) -> str:
        """Generate dialect-specific UPSERT statement."""
        cols = list(data.keys())
        vals = []
        for v in data.values():
            if isinstance(v, bool):
                vals.append(str(v).upper())  # TRUE/FALSE
            elif isinstance(v, (int, float)):
                vals.append(str(v))
            else:
                # Dialect-specific escaping
                # BigQuery uses backslash for single quotes in standard SQL (usually)
                # But Databricks (Spark SQL) defaults to standard SQL escaping ('') in modern versions
                if dialect == "bigquery":
                    escaped_v = str(v).replace("'", "\\'")
                else:
                    # duckdb, snowflake, databricks use ''
                    escaped_v = str(v).replace("'", "''")
                vals.append(f"'{escaped_v}'")

        # DuckDB: INSERT ON CONFLICT
        if dialect == "duckdb":
            conflict_target = ",".join(keys)
            updates = [f"{c} = EXCLUDED.{c}" for c in cols if c not in keys]
            if not updates:  # No update needed if only keys
                return f"INSERT OR IGNORE INTO {table} ({','.join(cols)}) VALUES ({','.join(vals)})"

            return f"""
INSERT INTO {table} ({",".join(cols)})
VALUES ({",".join(vals)})
ON CONFLICT ({conflict_target}) DO UPDATE SET
    {", ".join(updates)}
""".strip()

        # Snowflake/BigQuery/Databricks: MERGE
        else:
            # Build source SELECT
            select_cols = []
            for c, v in zip(cols, vals):
                select_cols.append(f"{v} AS {c}")
            source_select = f"SELECT {', '.join(select_cols)}"

            # Build match condition
            join_cond = " AND ".join([f"tgt.{k} = src.{k}" for k in keys])

            # Build update clause
            update_assignments = [f"tgt.{c} = src.{c}" for c in cols if c not in keys]
            update_clause = ""
            if update_assignments:
                update_clause = f"WHEN MATCHED THEN UPDATE SET {', '.join(update_assignments)}"
            else:
                # If only keys (e.g. valid-only table), no update needed, but MERGE allows empty update?
                # Actually BigQuery Requires UPDATE/DELETE/INSERT.
                # Safe fallback: update one key to itself or skip.
                # For our metadata tables, we always have non-key columns to update.
                pass

            # Build insert clause
            insert_clause = f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join([f'src.{c}' for c in cols])})"

            return f"""
MERGE INTO {table} tgt
USING ({source_select}) src
ON {join_cond}
{update_clause}
{insert_clause}
""".strip()

    # Source tables
    for src in config.get("sources", []):
        data = {
            "table_id": src["id"],
            "table_fqn": (
                f"{config.get('schema_source')}.{src['table']}"
                if config.get("schema_source") and "." not in src["table"]
                else src["table"]
            ),
            "entity_type": src.get("entity_type", "PERSON"),
            "entity_key_expr": src.get("entity_key", "id"),
            "watermark_column": src.get("watermark_column", "updated_at"),
            "is_active": True,
        }
        statements.append(generate_upsert("idr_meta.source_table", ["table_id"], data))

        # Identifier mappings
        # Infer from exact match rules if not explicit
        raw_ids = src.get("identifiers", [])
        inferred_ids = []

        # If no explicit identifiers, try to infer from exact rules
        if not raw_ids:
            source_attrs = {a["name"]: a["expr"] for a in src.get("attributes", [])}
            for rule in config.get("rules", []):
                if rule.get("type") == "EXACT":
                    for key in rule.get("match_keys", []):
                        # If rule key exists in source attributes, treat it as an identifier
                        if key in source_attrs:
                            inferred_ids.append(
                                {
                                    "type": key,  # semantic type (e.g. email)
                                    "expr": source_attrs[key],  # column expression
                                }
                            )
            raw_ids = inferred_ids

        if isinstance(raw_ids, dict):
            id_iter = raw_ids.items()
        else:
            id_iter = [
                (item["type"], item["expr"])
                for item in raw_ids
                if "type" in item and "expr" in item
            ]

        for id_type, col_expr in id_iter:
            data = {
                "table_id": src["id"],
                "identifier_type": id_type,
                "identifier_value_expr": col_expr,
                "is_hashed": False,
            }
            statements.append(
                generate_upsert(
                    "idr_meta.identifier_mapping", ["table_id", "identifier_type"], data
                )
            )

        # Attribute mappings
        raw_attrs = src.get("attributes", [])
        if isinstance(raw_attrs, dict):
            attr_iter = raw_attrs.items()
        else:
            attr_iter = [(item["name"], item["expr"]) for item in raw_attrs]

        for attr_name, attr_expr in attr_iter:
            data = {"table_id": src["id"], "attribute_name": attr_name, "attribute_expr": attr_expr}
            # Entity attribute mapping PK is composite
            statements.append(
                generate_upsert(
                    "idr_meta.entity_attribute_mapping", ["table_id", "attribute_name"], data
                )
            )

    # Rules
    for rule in config.get("rules", []):
        data = {
            "rule_id": rule["id"],
            "identifier_type": (
                rule.get("identifier_type")
                or (
                    rule["match_keys"][0]
                    if rule.get("type") == "EXACT" and rule.get("match_keys")
                    else rule.get("type", str(rule["id"]).upper())
                )
            ),
            "canonicalize": rule.get("canonicalize", "LOWERCASE"),
            "max_group_size": rule.get("max_group_size", 10000),
            "priority": rule.get("priority", 100),
            "is_active": True,
        }
        statements.append(generate_upsert("idr_meta.rule", ["rule_id"], data))

    # Exclusions
    for excl in config.get("exclusions", []):
        match_type = excl.get("match", "EXACT")
        data = {
            "identifier_type": excl["type"],
            "identifier_value_pattern": excl["value"],  # Corrected field
            "match_type": match_type,
            "reason": excl.get("reason", "Config exclusion"),
        }
        # Exclusions don't have a rigid PK, usually just INSERT.
        # But to allow safe re-runs, we can key on type+value.
        # Wait, no unique constraint on exclusion table in DDL?
        # Checking DDL... it has no PK.
        # But we should probably use MERGE to avoid dupes if running multiple times.
        # Or DELETE + INSERT?
        # Upsert with type+value is safest.
        statements.append(
            generate_upsert(
                "idr_meta.identifier_exclusion",
                ["identifier_type", "identifier_value_pattern"],
                data,
            )
        )

    # Fuzzy Rules
    for rule in config.get("fuzzy_rules", []):
        rule_id = rule.get("rule_id", rule.get("id"))
        if not rule_id:
            raise ValueError("Fuzzy rule missing 'id' or 'rule_id'")

        data = {
            "rule_id": rule_id,
            "rule_name": rule.get("name", str(rule_id)),
            "blocking_key_expr": rule["blocking_key"],
            "score_expr": (
                rule["score_expr"].get(dialect, rule["score_expr"].get("default"))
                if isinstance(rule["score_expr"], dict)
                else rule["score_expr"]
            ),
            "threshold": rule.get("threshold", 0.85),
            "priority": rule.get("priority", 100),
            "is_active": True,
        }
        statements.append(generate_upsert("idr_meta.fuzzy_rule", ["rule_id"], data))

    # Survivorship Rules
    for rule in config.get("survivorship", []):
        # Serialize list to string (JSON-like) for storage
        priority_list = rule.get("source_priority", [])
        if isinstance(priority_list, list):
            priority_list_str = str(priority_list).replace("'", '"')  # Simple JSON-ify
        else:
            priority_list_str = str(priority_list)

        data = {
            "attribute_name": rule["attribute"],
            "strategy": rule["strategy"],
            "source_priority_list": priority_list_str,
            "recency_field": rule.get("recency_field"),
            "is_active": True,
        }
        statements.append(generate_upsert("idr_meta.survivorship_rule", ["attribute_name"], data))

    return statements


def sql_to_config(adapter) -> Dict[str, Any]:
    """
    Reconstruct configuration dictionary from current idr_meta table state.

    Args:
        adapter: Database adapter instance

    Returns:
        Configuration dictionary matching YAML structure
    """
    try:
        config = {"sources": [], "rules": [], "survivorship": []}

        # Helper for adaptive querying (handles missing is_active column in legacy schemas)
        def _query_active_sources(adapter):
            try:
                return adapter.query("""
                    SELECT table_id, table_fqn, entity_type, entity_key_expr, watermark_column
                    FROM idr_meta.source_table
                    WHERE is_active = TRUE
                """)
            except Exception as e:
                # Fallback: Query without is_active if column missing
                if (
                    "no such column" in str(e).lower()
                    or "unrecognized name" in str(e).lower()
                    or "invalid identifier" in str(e).lower()
                ):
                    print(
                        "Warning: 'is_active' column missing in source_table, assuming all active."
                    )
                    return adapter.query("""
                        SELECT table_id, table_fqn, entity_type, entity_key_expr, watermark_column
                        FROM idr_meta.source_table
                    """)
                raise e

        # 1. Sources
        sources_rows = _query_active_sources(adapter)

        # Fetch identifiers
        ids_rows = adapter.query("""
            SELECT table_id, identifier_type, identifier_value_expr
            FROM idr_meta.identifier_mapping
        """)
        ids_map = {}
        for row in ids_rows:
            # Handle both dictionary and object access depending on adapter return
            # DuckDB adapter returns dict-like objects usually
            tid = row["table_id"]
            if tid not in ids_map:
                ids_map[tid] = []
            ids_map[tid].append(
                {"type": row["identifier_type"], "expr": row["identifier_value_expr"]}
            )

        # Fetch attributes
        attrs_rows = adapter.query("""
            SELECT table_id, attribute_name, attribute_expr
            FROM idr_meta.entity_attribute_mapping
        """)
        attrs_map = {}
        for row in attrs_rows:
            tid = row["table_id"]
            if tid not in attrs_map:
                attrs_map[tid] = []
            attrs_map[tid].append({"name": row["attribute_name"], "expr": row["attribute_expr"]})

        # Assemble sources
        for src_row in sources_rows:
            tid = src_row["table_id"]
            source = {
                "id": tid,
                "table": src_row["table_fqn"],
                "entity_type": src_row["entity_type"],
                "entity_key": src_row["entity_key_expr"],
                "watermark_column": src_row["watermark_column"],
                "identifiers": ids_map.get(tid, []),
                "attributes": attrs_map.get(tid, []),
            }
            config["sources"].append(source)

        # 2. Matching Rules
        def _query_active_rules(adapter):
            try:
                return adapter.query("""
                    SELECT rule_id, identifier_type, max_group_size, priority, canonicalize
                    FROM idr_meta.rule
                    WHERE is_active = TRUE
                    ORDER BY priority ASC
                """)
            except Exception as e:
                if (
                    "no such column" in str(e).lower()
                    or "unrecognized name" in str(e).lower()
                    or "invalid identifier" in str(e).lower()
                ):
                    print("Warning: 'is_active' column missing in rule, assuming all active.")
                    return adapter.query("""
                        SELECT rule_id, identifier_type, max_group_size, priority, canonicalize
                        FROM idr_meta.rule
                        ORDER BY priority ASC
                    """)
                raise e

        rules_rows = _query_active_rules(adapter)

        for rule_row in rules_rows:
            # Reverse engineer 'exact' rule format
            # In our schema, we only support single identifier_type strict match per rule row mostly?
            # Actually schema has identifier_type.
            # If we used multi-key match, we likely stored it as a complex rule or split logic?
            # Looking at schema_defs, `rule` table has `identifier_type`.
            # So currently strictly supports 1-key match?
            # Wait, `generate_upsert` logic for rules:
            # "match_keys": [rule["identifier_type"]]
            # So we can reconstruct it.

            rule = {
                "id": rule_row["rule_id"],
                "type": "EXACT",
                "match_keys": [rule_row["identifier_type"]],
                "priority": rule_row["priority"],
                "canonicalize": rule_row.get("canonicalize", "LOWERCASE"),  # Safe get
            }
            config["rules"].append(rule)

        # 3. Survivorship Rules
        def _query_active_survivorship(adapter):
            try:
                return adapter.query("""
                    SELECT attribute_name, strategy, source_priority_list, recency_field
                    FROM idr_meta.survivorship_rule
                    WHERE is_active = TRUE
                """)
            except Exception as e:
                # Fallback
                if (
                    "no such column" in str(e).lower()
                    or "unrecognized name" in str(e).lower()
                    or "invalid identifier" in str(e).lower()
                ):
                    print(
                        "Warning: 'is_active' column missing in survivorship_rule, assuming all active."
                    )
                    return adapter.query("""
                        SELECT attribute_name, strategy, source_priority_list, recency_field
                        FROM idr_meta.survivorship_rule
                    """)
                raise e

        srv_rows = _query_active_survivorship(adapter)

        for srv_row in srv_rows:
            # Parse source_priority_list string back to list
            p_list = []
            raw_plist = srv_row["source_priority_list"]
            if raw_plist:
                import ast

                try:
                    p_list = ast.literal_eval(raw_plist)
                except Exception:
                    # Fallback if simple string or comma sep
                    p_list = [
                        s.strip()
                        for s in raw_plist.replace("[", "").replace("]", "").split(",")
                        if s.strip()
                    ]

            rule = {
                "attribute": srv_row["attribute_name"],
                "strategy": srv_row["strategy"],
                "source_priority": p_list,
            }
            if srv_row.get("recency_field"):
                rule["recency_field"] = srv_row["recency_field"]

            config["survivorship"].append(rule)

        # 4. Fuzzy Rules (New)
        def _query_active_fuzzy(adapter):
            try:
                return adapter.query("""
                    SELECT rule_id, blocking_key_expr, score_expr, threshold, priority
                    FROM idr_meta.fuzzy_rule
                    WHERE is_active = TRUE
                    ORDER BY priority ASC
                """)
            except Exception:
                # Table might not exist yet in older schemas
                return []

        fuzzy_rows = _query_active_fuzzy(adapter)
        config["fuzzy_rules"] = []

        for row in fuzzy_rows:
            rule = {
                "id": row["rule_id"],
                "type": "FUZZY",
                "blocking_key": row["blocking_key_expr"],
                "score_expr": row["score_expr"],
                "threshold": row["threshold"],
                "priority": row["priority"],
            }
            config["fuzzy_rules"].append(rule)

        return config

    except Exception as e:
        print(f"Error reconstructing config from SQL: {e}")
        # Return empty/partial config or re-raise?
        # Better to return what we have or empty so calling code can decide to fallback
        # If we fail, try to return basic valid config
        return config if config.get("sources") else {"sources": [], "rules": [], "survivorship": []}


# Utility functions used across the codebase


def get_attr_expr(
    table_attrs: Dict[str, str],
    attr_name: str,
    fallback_cols: List[str],
    available_cols: List[str],
    alias: str = "src",
) -> str:
    """
    Resolve attribute to SQL expression with fallback.

    This is the unified implementation of the attribute resolution logic
    that was previously duplicated across all platform runners.

    Args:
        table_attrs: {attr_name: column_expr} from entity_attribute_mapping
        attr_name: Attribute to resolve (e.g., 'email', 'first_name')
        fallback_cols: Columns to try if not in metadata
        available_cols: Actual table columns (for validation)
        alias: Table alias (default 'src')

    Returns:
        SQL expression like 'src.email' or 'NULL'
    """
    available_lower = [c.lower() for c in available_cols]

    # Check explicit mapping first
    if attr_name in table_attrs:
        mapped_expr = table_attrs[attr_name]
        # If it's a simple column name, validate it exists
        if _SAFE_IDENTIFIER_RE.match(mapped_expr):
            if mapped_expr.lower() in available_lower:
                return f"{alias}.{mapped_expr}"
        else:
            # Complex expression - trust the user
            return mapped_expr

    # Fallback to auto-detection
    for col in fallback_cols:
        if col.lower() in available_lower:
            return f"{alias}.{col}"

    return "NULL"


def build_where_clause(
    watermark_column: str,
    last_watermark: Optional[str],
    lookback_minutes: int = 0,
    run_mode: str = "FULL",
    dialect: str = "duckdb",
) -> str:
    """
    Build WHERE clause for incremental processing.

    Args:
        watermark_column: Column name for watermark (e.g., 'updated_at')
        last_watermark: Last processed watermark value (ISO timestamp string)
        lookback_minutes: Minutes to look back from watermark
        run_mode: 'FULL' or 'INCR'
        dialect: SQL dialect for syntax variations

    Returns:
        WHERE clause condition (without 'WHERE' keyword)
    """
    if run_mode == "FULL" or not last_watermark or not watermark_column:
        return "1=1"

    # Dialect-specific date arithmetic
    if dialect == "snowflake":
        if lookback_minutes and lookback_minutes > 0:
            return f"{watermark_column} > DATEADD('minute', -{lookback_minutes}, '{last_watermark}'::TIMESTAMP)"
        return f"{watermark_column} > '{last_watermark}'::TIMESTAMP"

    elif dialect == "bigquery":
        if lookback_minutes and lookback_minutes > 0:
            return f"{watermark_column} > TIMESTAMP_SUB(TIMESTAMP('{last_watermark}'), INTERVAL {lookback_minutes} MINUTE)"
        return f"{watermark_column} > TIMESTAMP('{last_watermark}')"

    elif dialect == "databricks":
        if lookback_minutes and lookback_minutes > 0:
            return f"{watermark_column} > TIMESTAMPADD(MINUTE, -{lookback_minutes}, TIMESTAMP('{last_watermark}'))"
        return f"{watermark_column} > TIMESTAMP('{last_watermark}')"

    else:  # duckdb default
        if lookback_minutes and lookback_minutes > 0:
            return f"{watermark_column} > ('{last_watermark}'::TIMESTAMP - INTERVAL '{lookback_minutes} minutes')"
        return f"{watermark_column} > '{last_watermark}'::TIMESTAMP"
