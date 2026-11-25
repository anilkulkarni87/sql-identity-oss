"""
Dialect-aware SQL literal escaping and UPSERT (`ON CONFLICT` / `MERGE INTO`) builders
for `idr_meta.*` configuration persistence.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def escape_sql_string(val: str, dialect: str) -> str:
    """Escape single quotes in SQL string literals across the 4 supported dialects."""
    if dialect == "bigquery":
        return str(val).replace("'", "\\'")
    return str(val).replace("'", "''")


def build_bq_type_map() -> Dict[tuple, Any]:
    """Build `(table, column) -> ColumnType` lookup for typed NULL casts on BigQuery."""
    try:
        from ..schema_defs import SYSTEM_TABLES

        bq_type_map: Dict[tuple, Any] = {}
        for t in SYSTEM_TABLES:
            short = t.fqn.split(".")[-1]
            for col in t.columns:
                bq_type_map[(t.fqn, col.name)] = col.type
                bq_type_map[(short, col.name)] = col.type
        return bq_type_map
    except Exception:
        return {}


def format_upsert_val(
    dialect: str,
    table: str,
    col_name: str,
    val: Any,
    bq_type_map: Optional[Dict[tuple, Any]] = None,
) -> str:
    """Format a Python scalar into a safe dialect-specific SQL literal."""
    if val is None:
        if dialect == "bigquery":
            try:
                from ..schema_defs import ColumnType

                col_t = (bq_type_map or {}).get((table, col_name)) or (bq_type_map or {}).get(
                    (table.split(".")[-1], col_name)
                )
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
    if isinstance(val, bool):
        return str(val).upper()
    if isinstance(val, (int, float)):
        return str(val)
    return f"'{escape_sql_string(str(val), dialect)}'"


def build_single_upsert(
    dialect: str,
    table: str,
    keys: List[str],
    data: Dict[str, Any],
    bq_type_map: Optional[Dict[tuple, Any]] = None,
) -> str:
    """Build a single-row idempotent UPSERT statement for `table`."""
    cols = list(data.keys())
    vals = [format_upsert_val(dialect, table, c, v, bq_type_map) for c, v in data.items()]

    if dialect == "duckdb":
        conflict_target = ",".join(keys)
        updates = [f"{c} = EXCLUDED.{c}" for c in cols if c not in keys]
        if not updates:
            return f"INSERT OR IGNORE INTO {table} ({','.join(cols)}) VALUES ({','.join(vals)})"

        return f"""
INSERT INTO {table} ({",".join(cols)})
VALUES ({",".join(vals)})
ON CONFLICT ({conflict_target}) DO UPDATE SET
    {", ".join(updates)}
""".strip()

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


def build_batched_upsert(
    dialect: str,
    table: str,
    keys: List[str],
    rows: List[Dict[str, Any]],
    bq_type_map: Optional[Dict[tuple, Any]] = None,
) -> str:
    """Build a multi-row batched idempotent UPSERT statement for `table`."""
    if len(rows) == 1:
        return build_single_upsert(dialect, table, keys, rows[0], bq_type_map)

    cols = list(rows[0].keys())
    if dialect == "duckdb":
        conflict_target = ",".join(keys)
        updates = [f"{c} = EXCLUDED.{c}" for c in cols if c not in keys]
        values_rows = [
            f"({','.join(format_upsert_val(dialect, table, c, r.get(c), bq_type_map) for c in cols)})"
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
        select_cols = [
            f"{format_upsert_val(dialect, table, c, r.get(c), bq_type_map)} AS {c}" for c in cols
        ]
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
