"""Shared source-row selection for extraction, profiles and grouped outputs."""

from .config import validate_fqn, validate_identifier, validate_sql_expr


def source_relation_sql(
    table_fqn, entity_key_expr, deduplicate=False, dedup_order_by="", watermark_col=""
):
    table = validate_fqn(table_fqn)
    key = validate_sql_expr(entity_key_expr, "source entity key")
    if not deduplicate:
        return table
    order = (
        validate_sql_expr(dedup_order_by, "dedup_order_by")
        if dedup_order_by
        else (f"{validate_identifier(watermark_col)} DESC" if watermark_col else "1")
    )
    return (
        f"(SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY ({key}) ORDER BY {order}) AS _idr_rn "
        f"FROM {table} WHERE ({key}) IS NOT NULL) _idr_dedup WHERE _idr_rn = 1)"
    )
