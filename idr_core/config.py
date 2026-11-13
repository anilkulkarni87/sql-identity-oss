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

# Curated Default Toxic PII Exclusion Pack (prevents Day-1 supernode explosions)
DEFAULT_EXCLUSIONS: List[Dict[str, str]] = [
    # Bogus / System Emails
    {"identifier_type": "EMAIL", "identifier_value_pattern": "test@%", "match_type": "LIKE", "reason": "Default pack: test emails"},
    {"identifier_type": "EMAIL", "identifier_value_pattern": "noreply@%", "match_type": "LIKE", "reason": "Default pack: noreply inbox"},
    {"identifier_type": "EMAIL", "identifier_value_pattern": "no-reply@%", "match_type": "LIKE", "reason": "Default pack: no-reply inbox"},
    {"identifier_type": "EMAIL", "identifier_value_pattern": "noemail@%", "match_type": "LIKE", "reason": "Default pack: placeholder email"},
    {"identifier_type": "EMAIL", "identifier_value_pattern": "null@%", "match_type": "LIKE", "reason": "Default pack: null email"},
    {"identifier_type": "EMAIL", "identifier_value_pattern": "none@%", "match_type": "LIKE", "reason": "Default pack: none email"},
    {"identifier_type": "EMAIL", "identifier_value_pattern": "unknown@%", "match_type": "LIKE", "reason": "Default pack: unknown email"},
    {"identifier_type": "EMAIL", "identifier_value_pattern": "na@%", "match_type": "LIKE", "reason": "Default pack: NA email"},
    {"identifier_type": "EMAIL", "identifier_value_pattern": "%@example.com", "match_type": "LIKE", "reason": "Default pack: RFC 2606 example domain"},
    {"identifier_type": "EMAIL", "identifier_value_pattern": "%@test.com", "match_type": "LIKE", "reason": "Default pack: test domain"},
    # Bogus / Repeated-Digit Phone Numbers
    {"identifier_type": "PHONE", "identifier_value_pattern": "0000000000", "match_type": "EXACT", "reason": "Default pack: all-zeros phone"},
    {"identifier_type": "PHONE", "identifier_value_pattern": "1111111111", "match_type": "EXACT", "reason": "Default pack: repeated-1s phone"},
    {"identifier_type": "PHONE", "identifier_value_pattern": "2222222222", "match_type": "EXACT", "reason": "Default pack: repeated-2s phone"},
    {"identifier_type": "PHONE", "identifier_value_pattern": "3333333333", "match_type": "EXACT", "reason": "Default pack: repeated-3s phone"},
    {"identifier_type": "PHONE", "identifier_value_pattern": "4444444444", "match_type": "EXACT", "reason": "Default pack: repeated-4s phone"},
    {"identifier_type": "PHONE", "identifier_value_pattern": "5555555555", "match_type": "EXACT", "reason": "Default pack: repeated-5s phone"},
    {"identifier_type": "PHONE", "identifier_value_pattern": "6666666666", "match_type": "EXACT", "reason": "Default pack: repeated-6s phone"},
    {"identifier_type": "PHONE", "identifier_value_pattern": "7777777777", "match_type": "EXACT", "reason": "Default pack: repeated-7s phone"},
    {"identifier_type": "PHONE", "identifier_value_pattern": "8888888888", "match_type": "EXACT", "reason": "Default pack: repeated-8s phone"},
    {"identifier_type": "PHONE", "identifier_value_pattern": "9999999999", "match_type": "EXACT", "reason": "Default pack: repeated-9s phone"},
    {"identifier_type": "PHONE", "identifier_value_pattern": "1234567890", "match_type": "EXACT", "reason": "Default pack: sequential digits phone"},
    # Universal Sentinel Values across all identifier types
    {"identifier_type": "ANY", "identifier_value_pattern": "null", "match_type": "EXACT", "reason": "Default pack: literal null"},
    {"identifier_type": "ANY", "identifier_value_pattern": "none", "match_type": "EXACT", "reason": "Default pack: literal none"},
    {"identifier_type": "ANY", "identifier_value_pattern": "n/a", "match_type": "EXACT", "reason": "Default pack: literal n/a"},
    {"identifier_type": "ANY", "identifier_value_pattern": "unknown", "match_type": "EXACT", "reason": "Default pack: literal unknown"},
    {"identifier_type": "ANY", "identifier_value_pattern": "anonymous", "match_type": "EXACT", "reason": "Default pack: literal anonymous"},
    {"identifier_type": "ANY", "identifier_value_pattern": "refused", "match_type": "EXACT", "reason": "Default pack: literal refused"},
]


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
    if config.get("survivorship") is not None:
        if not isinstance(config["survivorship"], list):
            raise ValueError("Config 'survivorship' must be a list of rules")
        for rule in config["survivorship"]:
            if not isinstance(rule, dict):
                continue
            if "attribute" not in rule:
                raise ValueError("Survivorship rule missing 'attribute'")
            if "strategy" not in rule:
                raise ValueError(f"Survivorship rule for {rule['attribute']} missing 'strategy'")
            if rule["strategy"] not in ("PRIORITY", "RECENCY", "FREQUENCY", "AGG_MAX", "AGG_SUM"):
                raise ValueError(f"Invalid strategy {rule['strategy']} for {rule['attribute']}")

    # Validate rules if present
    if config.get("rules") is not None and not isinstance(config["rules"], list):
        raise ValueError("Config 'rules' must be a list")

    # Validate fuzzy_rules if present
    if config.get("fuzzy_rules") is not None and not isinstance(config["fuzzy_rules"], list):
        raise ValueError("Config 'fuzzy_rules' must be a list")

    # Validate generate_evidence if present (must be boolean)
    if "generate_evidence" in config and config.get("generate_evidence") is not None:
        if not isinstance(config["generate_evidence"], bool):
            raise ValueError("Config 'generate_evidence' must be a boolean (true/false)")

    return True


def config_to_sql(
    config: Dict[str, Any],
    dialect: str = "duckdb",
    batched: bool = False,
    chunk_size: int = 100,
) -> List[str]:
    """
    Convert YAML configuration to SQL INSERT/MERGE statements.

    Args:
        config: Validated configuration dictionary
        dialect: Target dialect ('duckdb', 'snowflake', 'bigquery', 'databricks')
        batched: When True, combine rows for the same metadata table into multi-row
            INSERT / MERGE batches (chunk_size rows per statement) to minimize
            warehouse round-trips.
        chunk_size: Maximum rows per batched statement when batched=True.

    Returns:
        List of SQL statements to populate metadata tables
    """
    statements = []
    batched_groups: Dict[tuple, List[Dict[str, Any]]] = {}

    bq_type_map: Optional[Dict[tuple, Any]] = None
    if dialect == "bigquery":
        try:
            from .schema_defs import SYSTEM_TABLES

            bq_type_map = {}
            for t in SYSTEM_TABLES:
                short = t.fqn.split(".")[-1]
                for col in t.columns:
                    bq_type_map[(t.fqn, col.name)] = col.type
                    bq_type_map[(short, col.name)] = col.type
        except Exception:
            bq_type_map = {}

    def _format_val(table: str, col_name: str, v: Any) -> str:
        if v is None:
            if dialect == "bigquery":
                # BigQuery infers untyped NULL as INT64 in SELECT expressions.
                # We must explicitly cast NULL to target column type to prevent 400 type mismatch.
                try:
                    from .schema_defs import ColumnType

                    col_t = (bq_type_map or {}).get((table, col_name)) or (
                        bq_type_map or {}
                    ).get((table.split(".")[-1], col_name))
                    if col_t == ColumnType.INT:
                        return "CAST(NULL AS INT64)"
                    if col_t == ColumnType.DOUBLE:
                        return "CAST(NULL AS FLOAT64)"
                    if col_t == ColumnType.BOOL:
                        return "CAST(NULL AS BOOL)"
                    if col_t == ColumnType.TIMESTAMP:
                        return "CAST(NULL AS TIMESTAMP)"
                    return "CAST(NULL AS STRING)"
                except Exception:
                    return "CAST(NULL AS STRING)"
            return "NULL"
        if isinstance(v, bool):
            return str(v).upper()  # TRUE/FALSE
        if isinstance(v, (int, float)):
            return str(v)
        if dialect == "bigquery":
            escaped_v = str(v).replace("'", "\\'")
        else:
            # duckdb, snowflake, databricks use ''
            escaped_v = str(v).replace("'", "''")
        return f"'{escaped_v}'"

    def _build_single_upsert(table: str, keys: List[str], data: Dict[str, Any]) -> str:
        cols = list(data.keys())
        vals = [_format_val(table, c, v) for c, v in data.items()]

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
        select_cols = [f"{v} AS {c}" for c, v in zip(cols, vals)]
        source_select = f"SELECT {', '.join(select_cols)}"
        join_cond = " AND ".join([f"tgt.{k} = src.{k}" for k in keys])
        update_assignments = [f"tgt.{c} = src.{c}" for c in cols if c not in keys]
        update_clause = (
            f"WHEN MATCHED THEN UPDATE SET {', '.join(update_assignments)}"
            if update_assignments
            else ""
        )
        insert_clause = (
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) "
            f"VALUES ({', '.join([f'src.{c}' for c in cols])})"
        )
        return f"""
MERGE INTO {table} tgt
USING ({source_select}) src
ON {join_cond}
{update_clause}
{insert_clause}
""".strip()

    def _build_batched_upsert(
        table: str, keys: List[str], rows: List[Dict[str, Any]]
    ) -> str:
        if len(rows) == 1:
            return _build_single_upsert(table, keys, rows[0])

        cols = list(rows[0].keys())
        if dialect == "duckdb":
            conflict_target = ",".join(keys)
            updates = [f"{c} = EXCLUDED.{c}" for c in cols if c not in keys]
            values_rows = [
                f"({','.join(_format_val(table, c, r.get(c)) for c in cols)})"
                for r in rows
            ]
            values_block = ",\n    ".join(values_rows)
            if not updates:
                return (
                    f"INSERT OR IGNORE INTO {table} ({','.join(cols)}) "
                    f"VALUES\n    {values_block}"
                )
            return f"""
INSERT INTO {table} ({",".join(cols)})
VALUES
    {values_block}
ON CONFLICT ({conflict_target}) DO UPDATE SET
    {", ".join(updates)}
""".strip()

        select_rows = []
        for r in rows:
            select_cols = [f"{_format_val(table, c, r.get(c))} AS {c}" for c in cols]
            select_rows.append(f"SELECT {', '.join(select_cols)}")
        source_select = "\n    UNION ALL\n    ".join(select_rows)
        join_cond = " AND ".join([f"tgt.{k} = src.{k}" for k in keys])
        update_assignments = [f"tgt.{c} = src.{c}" for c in cols if c not in keys]
        update_clause = (
            f"WHEN MATCHED THEN UPDATE SET {', '.join(update_assignments)}"
            if update_assignments
            else ""
        )
        insert_clause = (
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) "
            f"VALUES ({', '.join([f'src.{c}' for c in cols])})"
        )
        return f"""
MERGE INTO {table} tgt
USING (
    {source_select}
) src
ON {join_cond}
{update_clause}
{insert_clause}
""".strip()

    def generate_upsert(table: str, keys: List[str], data: Dict[str, Any]) -> str:
        """Generate dialect-specific UPSERT statement (or queue for batching)."""
        if batched:
            group_key = (table, tuple(keys))
            batched_groups.setdefault(group_key, []).append(data)
            return ""
        return _build_single_upsert(table, keys, data)

    # Source tables
    for src in (config.get("sources") or []):
        raw_wm = src.get("watermark_column")
        if raw_wm is None:
            raw_wm = src.get("watermark")
        wm_col = (
            None
            if raw_wm is None or str(raw_wm).strip().lower() in ("", "none", "null")
            else str(raw_wm).strip()
        )
        data = {
            "table_id": src["id"],
            "table_fqn": (
                f"{config.get('schema_source')}.{src['table']}"
                if config.get("schema_source") and "." not in src["table"]
                else src["table"]
            ),
            "entity_type": src.get("entity_type", "PERSON"),
            "entity_key_expr": src.get("entity_key", "id"),
            "watermark_column": wm_col,
            "is_active": True,
        }
        statements.append(generate_upsert("idr_meta.source_table", ["table_id"], data))

        # Identifier mappings
        # Infer from exact match rules if not explicit
        raw_ids = src.get("identifiers") or []
        inferred_ids = []

        # If no explicit identifiers, try to infer from exact rules
        if not raw_ids:
            source_attrs = {a["name"]: a["expr"] for a in (src.get("attributes") or [])}
            for rule in (config.get("rules") or []):
                if rule.get("type", "EXACT") == "EXACT":
                    for key in (rule.get("match_keys") or []):
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
                (item["type"], item.get("expr") or item.get("column") or item["type"])
                for item in raw_ids
                if isinstance(item, dict) and "type" in item and ("expr" in item or "column" in item)
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
        raw_attrs = src.get("attributes") or []
        if isinstance(raw_attrs, dict):
            attr_iter = raw_attrs.items()
        else:
            attr_iter = [
                (item["name"], item.get("expr") or item.get("column") or item["name"])
                for item in raw_attrs
                if isinstance(item, dict) and "name" in item
            ]

        for attr_name, attr_expr in attr_iter:
            data = {"table_id": src["id"], "attribute_name": attr_name, "attribute_expr": attr_expr}
            # Entity attribute mapping PK is composite
            statements.append(
                generate_upsert(
                    "idr_meta.entity_attribute_mapping", ["table_id", "attribute_name"], data
                )
            )

    # Rules
    rules_list = config.get("rules") or []
    if not rules_list:
        # Auto-infer rules from distinct identifier types found across sources
        seen_types = set()
        inferred_rules = []
        prio = 1
        for src in (config.get("sources") or []):
            for ident in (src.get("identifiers") or []):
                if isinstance(ident, dict):
                    t = ident.get("type")
                    if t and t not in seen_types:
                        seen_types.add(t)
                        t_lower = t.lower()
                        canon = (
                            "EMAIL_STANDARD"
                            if "email" in t_lower
                            else ("PHONE_US_10" if "phone" in t_lower else "LOWERCASE")
                        )
                        inferred_rules.append(
                            {
                                "id": f"rule_exact_{t.lower()}",
                                "type": "EXACT",
                                "match_keys": [t],
                                "canonicalize": canon,
                                "priority": prio,
                                "max_group_size": 10000,
                            }
                        )
                        prio += 1
        rules_list = inferred_rules

    for rule in rules_list:
        raw_canon = rule.get("canonicalize", "LOWERCASE")
        if isinstance(raw_canon, dict):
            canon_val = raw_canon.get(dialect) or raw_canon.get("default") or "LOWERCASE"
        else:
            canon_val = raw_canon
        data = {
            "rule_id": rule.get("id") or rule.get("rule_id", "rule_1"),
            "identifier_type": (
                rule.get("identifier_type")
                or (
                    rule["match_keys"][0]
                    if rule.get("match_keys")
                    else rule.get("type", str(rule.get("id", "rule")).upper())
                )
            ),
            "canonicalize": canon_val,
            "max_group_size": rule.get("max_group_size", 10000),
            "priority": rule.get("priority", 100),
            "is_active": True,
        }
        statements.append(generate_upsert("idr_meta.rule", ["rule_id"], data))

    # Exclusions (merge default pack if requested + explicit exclusions)
    combined_exclusions = []
    if config.get("use_default_exclusions", False):
        combined_exclusions.extend(DEFAULT_EXCLUSIONS)
    combined_exclusions.extend(config.get("exclusions") or [])

    seen_excl = set()
    for excl in combined_exclusions:
        match_type = excl.get("match_type") or excl.get("match") or "EXACT"
        id_type = excl.get("identifier_type") or excl.get("type")
        val_pattern = excl.get("value") or excl.get("identifier_value_pattern")
        if not id_type or not val_pattern:
            continue
        dedup_key = (str(id_type).upper(), str(val_pattern).lower())
        if dedup_key in seen_excl:
            continue
        seen_excl.add(dedup_key)
        data = {
            "identifier_type": id_type,
            "identifier_value_pattern": val_pattern,
            "match_type": match_type,
            "reason": excl.get("reason", "Config exclusion"),
        }
        statements.append(
            generate_upsert(
                "idr_meta.identifier_exclusion",
                ["identifier_type", "identifier_value_pattern"],
                data,
            )
        )

    # Manual Stewardship Overrides (FORCE_LINK, DO_NOT_LINK, ISOLATE)
    for idx, ov in enumerate(config.get("overrides") or []):
        ov_type = str(ov.get("type") or ov.get("override_type") or "").upper()
        if ov_type not in ("FORCE_LINK", "DO_NOT_LINK", "ISOLATE"):
            raise ValueError(
                f"Invalid override type '{ov_type}'. Must be FORCE_LINK, DO_NOT_LINK, or ISOLATE."
            )
        ekey_a = ov.get("entity_key_a") or ov.get("entity_key")
        ekey_b = ov.get("entity_key_b") or ("__ISOLATE__" if ov_type == "ISOLATE" else "")
        if not ekey_a:
            raise ValueError(f"Override {idx} missing 'entity_key_a' (or 'entity_key')")
        if ov_type in ("FORCE_LINK", "DO_NOT_LINK") and not ekey_b:
            raise ValueError(f"Override {idx} ({ov_type}) requires 'entity_key_b'")
        ov_id = ov.get("id") or ov.get("override_id") or f"ov_{ov_type.lower()}_{idx + 1}"
        data = {
            "override_id": str(ov_id),
            "override_type": ov_type,
            "entity_key_a": str(ekey_a),
            "entity_key_b": str(ekey_b),
            "reason": ov.get("reason", "Manual stewardship override"),
            "created_by": ov.get("created_by", "config"),
            "is_active": bool(ov.get("is_active", True)),
        }
        statements.append(
            generate_upsert(
                "idr_meta.identity_override",
                ["override_type", "entity_key_a", "entity_key_b"],
                data,
            )
        )

    # Fuzzy Rules
    for rule in (config.get("fuzzy_rules") or []):
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
    for rule in (config.get("survivorship") or []):
        # Serialize list to string (JSON-like) for storage
        priority_list = rule.get("source_priority", [])
        if isinstance(priority_list, list):
            priority_list_str = str(priority_list).replace("'", '"')  # Simple JSON-ify
        else:
            priority_list_str = str(priority_list)

        raw_recency = rule.get("recency_field")
        recency_field = (
            None
            if raw_recency is None or str(raw_recency).strip().lower() in ("", "none", "null")
            else str(raw_recency).strip()
        )
        data = {
            "attribute_name": rule["attribute"],
            "strategy": rule["strategy"],
            "source_priority_list": priority_list_str,
            "recency_field": recency_field,
            "is_active": True,
        }
        statements.append(generate_upsert("idr_meta.survivorship_rule", ["attribute_name"], data))

    if batched:
        batched_statements: List[str] = []
        safe_chunk = max(1, int(chunk_size or 100))
        for (table, keys_tuple), rows in batched_groups.items():
            # Deduplicate by primary key (keeping last) so MERGE never hits duplicate source rows
            deduped: Dict[tuple, Dict[str, Any]] = {}
            for row in rows:
                pk = tuple(row.get(k) for k in keys_tuple)
                deduped[pk] = row
            unique_rows = list(deduped.values())
            keys_list = list(keys_tuple)
            for idx in range(0, len(unique_rows), safe_chunk):
                chunk = unique_rows[idx : idx + safe_chunk]
                batched_statements.append(_build_batched_upsert(table, keys_list, chunk))
        return batched_statements

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
                {
                    "type": row["identifier_type"],
                    "column": row["identifier_value_expr"],
                    "expr": row["identifier_value_expr"],
                }
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
            attrs_map[tid].append(
                {
                    "name": row["attribute_name"],
                    "column": row["attribute_expr"],
                    "expr": row["attribute_expr"],
                }
            )

        # Assemble sources
        for src_row in sources_rows:
            tid = src_row["table_id"]
            raw_wm = src_row.get("watermark_column")
            clean_wm = (
                ""
                if raw_wm is None or str(raw_wm).strip().lower() in ("", "none", "null")
                else str(raw_wm).strip()
            )
            source = {
                "id": tid,
                "table": src_row["table_fqn"],
                "entity_type": src_row["entity_type"],
                "entity_key": src_row["entity_key_expr"],
                "watermark_column": clean_wm,
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
            rec_f = srv_row.get("recency_field")
            if rec_f and str(rec_f).strip().lower() not in ("", "none", "null"):
                rule["recency_field"] = str(rec_f).strip()

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

        # 5. Identifier Exclusions
        def _query_exclusions(adapter):
            try:
                return adapter.query("""
                    SELECT identifier_type, identifier_value_pattern, match_type, reason
                    FROM idr_meta.identifier_exclusion
                """)
            except Exception:
                return []

        excl_rows = _query_exclusions(adapter)
        config["exclusions"] = [
            {
                "type": row["identifier_type"],
                "value": row["identifier_value_pattern"],
                "match": row.get("match_type") or "EXACT",
                "reason": row.get("reason") or "Config exclusion",
            }
            for row in excl_rows
        ]

        # 6. Manual Stewardship Overrides
        def _query_overrides(adapter):
            try:
                return adapter.query("""
                    SELECT override_id, override_type, entity_key_a, entity_key_b, reason, created_by
                    FROM idr_meta.identity_override
                    WHERE is_active = TRUE
                """)
            except Exception:
                return []

        ov_rows = _query_overrides(adapter)
        if ov_rows:
            config["overrides"] = [
                {
                    "id": row["override_id"],
                    "type": row["override_type"],
                    "entity_key_a": row["entity_key_a"],
                    "entity_key_b": row.get("entity_key_b") or "",
                    "reason": row.get("reason") or "Manual stewardship override",
                    "created_by": row.get("created_by") or "config",
                }
                for row in ov_rows
            ]

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


def config_to_dbt_seeds(config: Dict[str, Any], output_dir: str) -> Dict[str, str]:
    """
    Export a validated IDR configuration dictionary into the 6 standard
    `dbt_idr` seed CSV files (`idr_sources.csv`, `idr_identifier_mappings.csv`,
    `idr_attribute_mappings.csv`, `idr_rules.csv`, `idr_exclusions.csv`,
    `idr_survivorship_rules.csv`).

    Bridges interactive design-time workflows (UI, CLI, FastMCP agent discovery)
    with governed production `dbt build` pipelines.

    Args:
        config: Validated configuration dictionary
        output_dir: Target directory where CSV seed files will be written

    Returns:
        Dictionary mapping seed filename to absolute file path written
    """
    import csv

    validate_config(config)
    out_path = Path(output_dir).resolve()
    out_path.mkdir(parents=True, exist_ok=True)

    written_files: Dict[str, str] = {}

    def _write_csv(filename: str, fieldnames: List[str], rows: List[Dict[str, Any]]) -> None:
        filepath = out_path / filename
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        written_files[filename] = str(filepath)

    sources_rows: List[Dict[str, Any]] = []
    id_mapping_rows: List[Dict[str, Any]] = []
    attr_mapping_rows: List[Dict[str, Any]] = []

    default_schema = config.get("schema_source") or "main"

    for src in config.get("sources", []):
        src_id = src["id"]
        raw_table = src["table"].replace("`", "").replace('"', "")
        parts = raw_table.split(".")
        if len(parts) == 3:
            db_name, schema_name, table_name = parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            db_name, schema_name, table_name = "", parts[0], parts[1]
        else:
            db_name, schema_name, table_name = "", default_schema, parts[0]

        watermark_col = src.get("watermark_column", "updated_at")
        sources_rows.append(
            {
                "source_id": src_id,
                "source_name": src.get("name", src_id.replace("_", " ").title()),
                "database": src.get("database", db_name),
                "schema": src.get("schema", schema_name),
                "table_name": table_name,
                "entity_key_column": src.get("entity_key", "id"),
                "watermark_column": watermark_col,
                "is_active": "true",
                "description": src.get("description", f"Source table {raw_table}"),
            }
        )

        # Attributes
        raw_attrs = src.get("attributes", [])
        if isinstance(raw_attrs, dict):
            attr_iter = list(raw_attrs.items())
        else:
            attr_iter = [
                (item["name"], item.get("expr") or item.get("column") or item["name"])
                for item in raw_attrs
                if "name" in item
            ]

        seen_attrs = set()
        for attr_name, attr_expr in attr_iter:
            seen_attrs.add(attr_name)
            attr_mapping_rows.append(
                {
                    "source_id": src_id,
                    "attribute_name": attr_name,
                    "column_expr": attr_expr,
                    "description": f"{src_id} {attr_name}",
                }
            )

        if watermark_col and "record_updated_at" not in seen_attrs:
            attr_mapping_rows.append(
                {
                    "source_id": src_id,
                    "attribute_name": "record_updated_at",
                    "column_expr": watermark_col,
                    "description": f"{src_id} watermark timestamp",
                }
            )

        # Identifiers (explicit or inferred from EXACT rules)
        raw_ids = src.get("identifiers", [])
        if not raw_ids:
            source_attrs_map = dict(attr_iter)
            inferred_ids = []
            for rule in config.get("rules", []):
                if rule.get("type") == "EXACT":
                    for key in rule.get("match_keys", []):
                        if key in source_attrs_map:
                            inferred_ids.append({"type": key, "expr": source_attrs_map[key]})
            raw_ids = inferred_ids

        if isinstance(raw_ids, dict):
            id_iter = list(raw_ids.items())
        else:
            id_iter = [
                (item["type"], item.get("expr") or item.get("column") or item["type"])
                for item in raw_ids
                if "type" in item
            ]

        for id_type, col_expr in id_iter:
            id_mapping_rows.append(
                {
                    "source_id": src_id,
                    "identifier_type": str(id_type).upper(),
                    "column_name": col_expr,
                    "is_hashed": str(bool(src.get("is_hashed", False))).lower(),
                    "description": f"{src_id} {id_type}",
                }
            )

    # Rules
    rules_rows: List[Dict[str, Any]] = []
    for rule in config.get("rules", []):
        id_type = rule.get("identifier_type") or (
            rule["match_keys"][0]
            if rule.get("type") == "EXACT" and rule.get("match_keys")
            else rule.get("type", str(rule["id"]))
        )
        rules_rows.append(
            {
                "rule_id": rule["id"],
                "identifier_type": str(id_type).upper(),
                "priority": rule.get("priority", 100),
                "is_active": "true",
                "canonicalize": rule.get("canonicalize", "LOWERCASE"),
                "max_group_size": rule.get("max_group_size", 10000),
                "description": rule.get("description", f"Match rule for {id_type}"),
            }
        )

    # Exclusions (include DEFAULT_EXCLUSIONS when use_default_exclusions is True)
    exclusions_rows: List[Dict[str, Any]] = []
    combined_excl = []
    if config.get("use_default_exclusions", False):
        combined_excl.extend(DEFAULT_EXCLUSIONS)
    combined_excl.extend(config.get("exclusions", []))
    seen_dbt_excl = set()
    for excl in combined_excl:
        id_type = excl.get("identifier_type") or excl.get("type")
        val_pattern = excl.get("value") or excl.get("identifier_value_pattern")
        if not id_type or not val_pattern:
            continue
        dedup_k = (str(id_type).upper(), str(val_pattern).lower())
        if dedup_k in seen_dbt_excl:
            continue
        seen_dbt_excl.add(dedup_k)
        exclusions_rows.append(
            {
                "identifier_type": str(id_type).upper(),
                "identifier_value_pattern": val_pattern,
                "match_type": excl.get("match_type") or excl.get("match") or "EXACT",
                "reason": excl.get("reason", "Config exclusion"),
            }
        )

    # Survivorship Rules
    surv_rows: List[Dict[str, Any]] = []
    for rule in config.get("survivorship", []):
        p_list = rule.get("source_priority", [])
        if isinstance(p_list, list):
            p_str = ",".join(str(x) for x in p_list)
        else:
            p_str = str(p_list or "")
        surv_rows.append(
            {
                "attribute_name": rule["attribute"],
                "strategy": rule["strategy"],
                "source_priority_list": p_str,
                "recency_field": rule.get("recency_field") or "record_updated_at",
                "description": rule.get("description", f"Survivorship for {rule['attribute']}"),
            }
        )

    # Manual Stewardship Overrides (FORCE_LINK, DO_NOT_LINK, ISOLATE)
    override_rows: List[Dict[str, Any]] = []
    for idx, ov in enumerate(config.get("overrides") or []):
        ov_type = str(ov.get("type") or ov.get("override_type") or "").upper().strip()
        ekey_a = ov.get("entity_key_a") or ov.get("entity_key")
        if not ov_type or not ekey_a:
            continue
        ekey_b = ov.get("entity_key_b") or ("__ISOLATE__" if ov_type == "ISOLATE" else "")
        ov_id = ov.get("id") or ov.get("override_id") or f"ov_{ov_type.lower()}_{idx + 1}"
        override_rows.append(
            {
                "override_id": str(ov_id),
                "override_type": ov_type,
                "entity_key_a": str(ekey_a),
                "entity_key_b": str(ekey_b),
                "reason": ov.get("reason", "Manual stewardship override"),
                "created_by": ov.get("created_by", "config"),
                "is_active": "true" if ov.get("is_active", True) else "false",
            }
        )
    if not override_rows:
        # Include an inactive placeholder row so dbt seed CSV type inference succeeds on all warehouses
        override_rows.append(
            {
                "override_id": "ov_sample_1",
                "override_type": "FORCE_LINK",
                "entity_key_a": "crm:1",
                "entity_key_b": "crm:1",
                "reason": "Sample inactive stewardship override",
                "created_by": "config",
                "is_active": "false",
            }
        )

    _write_csv(
        "idr_sources.csv",
        [
            "source_id",
            "source_name",
            "database",
            "schema",
            "table_name",
            "entity_key_column",
            "watermark_column",
            "is_active",
            "description",
        ],
        sources_rows,
    )
    _write_csv(
        "idr_identifier_mappings.csv",
        ["source_id", "identifier_type", "column_name", "is_hashed", "description"],
        id_mapping_rows,
    )
    _write_csv(
        "idr_attribute_mappings.csv",
        ["source_id", "attribute_name", "column_expr", "description"],
        attr_mapping_rows,
    )
    _write_csv(
        "idr_rules.csv",
        [
            "rule_id",
            "identifier_type",
            "priority",
            "is_active",
            "canonicalize",
            "max_group_size",
            "description",
        ],
        rules_rows,
    )
    _write_csv(
        "idr_exclusions.csv",
        ["identifier_type", "identifier_value_pattern", "match_type", "reason"],
        exclusions_rows,
    )
    _write_csv(
        "idr_survivorship_rules.csv",
        ["attribute_name", "strategy", "source_priority_list", "recency_field", "description"],
        surv_rows,
    )
    _write_csv(
        "idr_overrides.csv",
        [
            "override_id",
            "override_type",
            "entity_key_a",
            "entity_key_b",
            "reason",
            "created_by",
            "is_active",
        ],
        override_rows,
    )

    return written_files

