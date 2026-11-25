"""
Configuration validation, safe SQL identifier/expression guards, and query helpers.

Kept free of top-level intra-package imports so low-level modules (`canonicalizers`,
`survivorship`, `schema_manager`, `stages`) can import validators without circular dependencies.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

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
    {"identifier_type": "EMAIL", "identifier_value_pattern": "erased_%@%", "match_type": "LIKE", "reason": "Default pack: GDPR-erased email"},
    {"identifier_type": "EMAIL", "identifier_value_pattern": "%@anonymized.%", "match_type": "LIKE", "reason": "Default pack: anonymized email domain"},
    {"identifier_type": "EMAIL", "identifier_value_pattern": "redacted@%", "match_type": "LIKE", "reason": "Default pack: redacted email"},
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
    {"identifier_type": "ANY", "identifier_value_pattern": "redacted", "match_type": "EXACT", "reason": "Default pack: literal redacted"},
    {"identifier_type": "ANY", "identifier_value_pattern": "---", "match_type": "EXACT", "reason": "Default pack: dash placeholder"},
    {"identifier_type": "ANY", "identifier_value_pattern": "na", "match_type": "EXACT", "reason": "Default pack: literal na"},
]


def validate_identifier(value: str, name: str = "identifier") -> str:
    """Validate a string is safe for use as SQL identifier."""
    if not value:
        raise ValueError(f"{name} cannot be empty")
    if not _SAFE_IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid {name}: '{value}'. Must be alphanumeric with underscores.")
    return value


def validate_fqn(value: str, name: str = "table") -> str:
    """Validate a fully qualified table name (`schema.table` or `catalog.schema.table`)."""
    if not value:
        raise ValueError(f"{name} cannot be empty")
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
    """Validate a SQL expression is safe for interpolation."""
    if not value:
        raise ValueError(f"{name} cannot be empty")
    if _DANGEROUS_SQL_RE.search(value):
        raise ValueError(
            f"Unsafe SQL detected in {name}: '{value}'. "
            f"Expressions must not contain DDL/DML statements, "
            f"semicolons, or SQL comments."
        )
    return value


def validate_integer(value: Any, name: str = "value") -> int:
    """Validate and coerce a value to a safe integer for SQL interpolation."""
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid integer for {name}: '{value}'")


def validate_float(value: Any, name: str = "value") -> float:
    """Validate and coerce a value to a safe float for SQL interpolation."""
    try:
        result = float(value)
        if not (-1e15 < result < 1e15):
            raise ValueError(f"Float value out of range for {name}: {result}")
        return result
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid float for {name}: '{value}'") from e


def resolve_entity_key_expr(src: Dict[str, Any], dialect: str = "duckdb") -> str:
    """
    Resolve a source's `entity_key` into a SQL expression.

    When `entity_key` is `'AUTO'` (or omitted with `key_columns` specified),
    synthesizes a deterministic dialect-aware SHA-256 hash over `key_columns`
    (or all mapped identifier + attribute expressions).
    """
    from ..adapters.base import get_dialect_config

    raw_key = src.get("entity_key")
    is_auto = raw_key is None or str(raw_key).strip().upper() == "AUTO"
    if not is_auto:
        return str(raw_key)

    cols: List[str] = []
    if src.get("key_columns"):
        cols = [str(c) for c in src["key_columns"]]
    else:
        for item in src.get("identifiers") or []:
            if isinstance(item, dict):
                expr = item.get("expr") or item.get("column") or item.get("type")
                if expr:
                    cols.append(str(expr))
        for item in src.get("attributes") or []:
            if isinstance(item, dict):
                expr = item.get("expr") or item.get("column") or item.get("name")
                if expr:
                    cols.append(str(expr))

    if not cols:
        raise ValueError(
            f"Source '{src.get('id', '?')}': entity_key='AUTO' requires 'key_columns' "
            "or at least one mapped identifier/attribute."
        )

    d_cfg = get_dialect_config(dialect)
    str_type = d_cfg["string_type"]
    parts = [f"COALESCE(CAST(({c}) AS {str_type}), '')" for c in cols]
    concat_expr = f"CONCAT_WS('|', {', '.join(parts)})"
    return d_cfg["sha256"](concat_expr)


def get_attr_expr(
    table_attrs: Dict[str, str],
    attr_name: str,
    fallback_cols: List[str],
    available_cols: List[str],
    alias: str = "src",
) -> str:
    """Resolve attribute to SQL expression with fallback."""
    available_lower = [c.lower() for c in available_cols]

    if attr_name in table_attrs:
        mapped_expr = table_attrs[attr_name]
        if _SAFE_IDENTIFIER_RE.match(mapped_expr):
            if mapped_expr.lower() in available_lower:
                return f"{alias}.{mapped_expr}"
        else:
            return mapped_expr

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
    """Build WHERE clause for incremental processing (without 'WHERE' keyword)."""
    if run_mode == "FULL" or not last_watermark or not watermark_column:
        return "1=1"

    if dialect == "snowflake":
        if lookback_minutes and lookback_minutes > 0:
            return f"{watermark_column} > DATEADD('minute', -{lookback_minutes}, '{last_watermark}'::TIMESTAMP)"
        return f"{watermark_column} > '{last_watermark}'::TIMESTAMP"

    if dialect == "bigquery":
        wm_ts = f"CAST({watermark_column} AS TIMESTAMP)"
        if lookback_minutes and lookback_minutes > 0:
            return f"{wm_ts} > TIMESTAMP_SUB(TIMESTAMP('{last_watermark}'), INTERVAL {lookback_minutes} MINUTE)"
        return f"{wm_ts} > TIMESTAMP('{last_watermark}')"

    if dialect == "databricks":
        if lookback_minutes and lookback_minutes > 0:
            return f"{watermark_column} > TIMESTAMPADD(MINUTE, -{lookback_minutes}, TIMESTAMP('{last_watermark}'))"
        return f"{watermark_column} > TIMESTAMP('{last_watermark}')"

    if lookback_minutes and lookback_minutes > 0:
        return f"{watermark_column} > ('{last_watermark}'::TIMESTAMP - INTERVAL '{lookback_minutes} minutes')"
    return f"{watermark_column} > '{last_watermark}'::TIMESTAMP"
