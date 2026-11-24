"""Opt-in incremental activation changelog (idr_out.identity_activation_delta) for Reverse-ETL & CRM sync."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from pydantic import BaseModel, ConfigDict, Field

from .adapters.base import get_dialect_config
from .config import validate_identifier
from .config_extensions import read_extensions

ACTIVATION_DELTA_COLUMNS = (
    "run_id",
    "lens_id",
    "resolved_id",
    "activation_status",
    "change_type",
    "previous_resolved_ids_json",
    "superseded_by_resolved_id",
    "cluster_size",
    "entities_added",
    "entities_removed",
    "profile_changed",
    "confidence_score",
    "emitted_at",
)

VALID_CHANGE_TYPES = {
    "NEW",
    "MERGED",
    "SPLIT",
    "PROFILE_CHANGED",
    "RETIRED_MERGED",
    "RETIRED_DELETED",
}


class ActivationDeltaOptions(BaseModel):
    """Configuration block persisted under idr_meta.config_extension['activation_delta']."""

    model_config = ConfigDict(extra="ignore")

    enabled: bool = False
    include_lenses: bool = False
    seed_initial_full: bool = False
    retention_days: int = Field(default=30, ge=0, le=3650)


def validate_activation_delta(raw: Any) -> Dict[str, Any]:
    """Validate an activation_delta config block and return normalized dict."""
    if not isinstance(raw, dict):
        raise ValueError("Config 'activation_delta' must be an object/mapping")
    return ActivationDeltaOptions.model_validate(raw).model_dump()


def resolve_activation_delta_options(
    adapter, emit_override: Optional[bool] = None
) -> ActivationDeltaOptions:
    """Resolve effective activation delta options from warehouse metadata and per-run override."""
    ext = read_extensions(adapter).get("activation_delta")
    opts = ActivationDeltaOptions.model_validate(ext) if isinstance(ext, dict) else ActivationDeltaOptions()
    if emit_override is not None:
        opts = opts.model_copy(update={"enabled": bool(emit_override)})
    return opts


def _json_array_agg_sql(dialect: str, expr: str) -> str:
    if dialect == "snowflake":
        return f"CONCAT('[', COALESCE(LISTAGG({expr}, ',') WITHIN GROUP (ORDER BY {expr}), ''), ']')"
    if dialect == "databricks":
        return f"CONCAT('[', ARRAY_JOIN(ARRAY_SORT(COLLECT_SET({expr})), ','), ']')"
    return f"CONCAT('[', COALESCE(STRING_AGG({expr}, ',' ORDER BY {expr}), ''), ']')"


def build_retention_delete_sql(dialect: str, retention_days: int) -> str:
    """Build single-statement dialect-portable retention pruning SQL."""
    days = max(1, int(retention_days))
    if dialect == "bigquery":
        cutoff = f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)"
    elif dialect == "snowflake":
        cutoff = f"DATEADD('day', -{days}, CURRENT_TIMESTAMP())"
    elif dialect == "databricks":
        cutoff = f"TIMESTAMPADD(DAY, -{days}, CURRENT_TIMESTAMP())"
    else:
        cutoff = f"CURRENT_TIMESTAMP - INTERVAL '{days} days'"
    return f"DELETE FROM idr_out.identity_activation_delta WHERE emitted_at < {cutoff}"


def build_activation_delta_insert_sql(
    dialect: str,
    run_id: str,
    lens_id: str = "default",
    run_mode: str = "FULL",
    profile_attr_cols: Sequence[str] = (),
    has_backup_membership: bool = True,
    has_backup_profiles: bool = True,
    has_clusters_table: bool = True,
    scope_to_affected: bool = False,
) -> str:
    """Build a single-statement 4-dialect INSERT INTO idr_out.identity_activation_delta."""
    d_cfg = get_dialect_config(dialect)
    str_type = d_cfg["string_type"]
    int_type = d_cfg["int_type"]
    float_type = d_cfg.get("float_type", "DOUBLE")
    current_ts = d_cfg["current_timestamp"]

    safe_run_id = str(run_id).replace("'", "''")
    safe_lens = "default" if lens_id == "default" else validate_identifier(lens_id, "lens_id")

    if safe_lens == "default":
        scope_pred = (
            " WHERE resolved_id IN (SELECT resolved_id FROM idr_work.output_affected_profile_ids)"
            if (run_mode == "INCR" and scope_to_affected)
            else ""
        )
        new_m_sql = f"SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current{scope_pred}"
        if has_backup_membership:
            old_m_sql = f"SELECT entity_key, resolved_id FROM idr_work._backup_identity_resolved_membership_current{scope_pred}"
        else:
            old_m_sql = f"SELECT CAST(NULL AS {str_type}) AS entity_key, CAST(NULL AS {str_type}) AS resolved_id WHERE 1=0"
    else:
        new_m_sql = f"SELECT entity_key, resolved_id FROM idr_out.identity_lens_membership_current WHERE lens_id = '{safe_lens}'"
        if has_backup_membership:
            old_m_sql = f"SELECT entity_key, resolved_id FROM idr_work._backup_identity_lens_membership_current WHERE lens_id = '{safe_lens}'"
        else:
            old_m_sql = f"SELECT CAST(NULL AS {str_type}) AS entity_key, CAST(NULL AS {str_type}) AS resolved_id WHERE 1=0"

    safe_attrs = [validate_identifier(c, "profile attribute") for c in profile_attr_cols]
    if safe_lens == "default" and has_backup_profiles:
        gp_scope = (
            " WHERE new_gp.resolved_id IN (SELECT resolved_id FROM idr_work.output_affected_profile_ids)"
            if (run_mode == "INCR" and scope_to_affected)
            else ""
        )
        if safe_attrs:
            diffs = [
                f"((old_gp.{col} IS NULL AND new_gp.{col} IS NOT NULL) "
                f"OR (old_gp.{col} IS NOT NULL AND new_gp.{col} IS NULL) "
                f"OR CAST(old_gp.{col} AS {str_type}) <> CAST(new_gp.{col} AS {str_type}))"
                for col in safe_attrs
            ]
            diff_cond = " OR ".join(diffs)
            profile_diff_sql = f"""
                SELECT
                    new_gp.resolved_id,
                    CASE
                        WHEN old_gp.resolved_id IS NULL THEN TRUE
                        WHEN {diff_cond} THEN TRUE
                        ELSE FALSE
                    END AS is_changed
                FROM idr_out.golden_profiles new_gp
                LEFT JOIN idr_work._backup_golden_profiles old_gp
                  ON old_gp.resolved_id = new_gp.resolved_id{gp_scope}
            """
        else:
            profile_diff_sql = f"""
                SELECT
                    new_gp.resolved_id,
                    CASE WHEN old_gp.resolved_id IS NULL THEN TRUE ELSE FALSE END AS is_changed
                FROM idr_out.golden_profiles new_gp
                LEFT JOIN idr_work._backup_golden_profiles old_gp
                  ON old_gp.resolved_id = new_gp.resolved_id{gp_scope}
            """
    else:
        profile_diff_sql = (
            f"SELECT CAST(NULL AS {str_type}) AS resolved_id, CAST(FALSE AS BOOLEAN) AS is_changed WHERE 1=0"
            if dialect != "bigquery"
            else "SELECT CAST(NULL AS STRING) AS resolved_id, CAST(FALSE AS BOOL) AS is_changed WHERE 1=0"
        )

    if safe_lens == "default" and has_clusters_table:
        conf_join = "LEFT JOIN idr_out.identity_clusters_current ic ON ic.resolved_id = nc.resolved_id"
        conf_expr = f"CAST(ic.confidence_score AS {float_type})"
    else:
        conf_join = ""
        conf_expr = f"CAST(NULL AS {float_type})"

    json_agg = _json_array_agg_sql(dialect, "q_old_id")
    cols_csv = ", ".join(ACTIVATION_DELTA_COLUMNS)

    profile_changed_expr = (
        "COALESCE(pd.is_changed, CASE WHEN nc.distinct_old_clusters = 0 AND oc_self.old_id IS NULL THEN TRUE ELSE FALSE END)"
        if (safe_lens == "default" and not has_backup_profiles)
        else (
            "COALESCE(pd.is_changed, FALSE)"
            if safe_lens == "default"
            else "CASE WHEN nc.distinct_old_clusters = 0 AND oc_self.old_id IS NULL THEN TRUE ELSE FALSE END"
        )
    )

    change_type_case = f"""
        CASE
            WHEN nc.distinct_old_clusters = 0 AND oc_self.old_id IS NULL THEN 'NEW'
            WHEN nc.distinct_old_clusters > 1 THEN 'MERGED'
            WHEN nc.distinct_old_clusters = 1 AND nc.max_origin_dest_count > 1 THEN 'SPLIT'
            WHEN nc.distinct_old_clusters = 1 AND nc.min_old_id <> nc.resolved_id THEN 'MERGED'
            WHEN ({profile_changed_expr}) OR nc.entities_added > 0 OR COALESCE(oc_self.entities_removed, 0) > 0 THEN 'PROFILE_CHANGED'
            ELSE NULL
        END
    """.strip()

    return f"""
INSERT INTO idr_out.identity_activation_delta ({cols_csv})
WITH new_m AS (
    {new_m_sql}
),
old_m AS (
    {old_m_sql}
),
old_clusters AS (
    SELECT
        o.resolved_id AS old_id,
        CAST(COUNT(*) AS {int_type}) AS old_size,
        CAST(COUNT(DISTINCT n.resolved_id) AS {int_type}) AS surviving_dest_count,
        MIN(n.resolved_id) AS min_dest_id,
        CAST(SUM(CASE WHEN n.resolved_id IS NULL OR n.resolved_id <> o.resolved_id THEN 1 ELSE 0 END) AS {int_type}) AS entities_removed
    FROM old_m o
    LEFT JOIN new_m n ON n.entity_key = o.entity_key
    GROUP BY o.resolved_id
),
foreign_origin_pairs AS (
    SELECT DISTINCT
        n.resolved_id,
        CONCAT('"', REPLACE(o.resolved_id, '"', ''), '"') AS q_old_id
    FROM new_m n
    JOIN old_m o ON o.entity_key = n.entity_key
    WHERE o.resolved_id <> n.resolved_id
),
foreign_origins AS (
    SELECT
        resolved_id,
        {json_agg} AS prev_ids_json
    FROM foreign_origin_pairs
    GROUP BY resolved_id
),
new_clusters AS (
    SELECT
        n.resolved_id,
        CAST(COUNT(*) AS {int_type}) AS cluster_size,
        CAST(SUM(CASE WHEN o.resolved_id IS NULL OR o.resolved_id <> n.resolved_id THEN 1 ELSE 0 END) AS {int_type}) AS entities_added,
        CAST(COUNT(DISTINCT o.resolved_id) AS {int_type}) AS distinct_old_clusters,
        MIN(o.resolved_id) AS min_old_id,
        CAST(COALESCE(MAX(oc_origin.surviving_dest_count), 0) AS {int_type}) AS max_origin_dest_count
    FROM new_m n
    LEFT JOIN old_m o ON o.entity_key = n.entity_key
    LEFT JOIN old_clusters oc_origin ON oc_origin.old_id = o.resolved_id
    GROUP BY n.resolved_id
),
profile_diff AS (
    {profile_diff_sql}
),
active_candidates AS (
    SELECT
        '{safe_run_id}' AS run_id,
        '{safe_lens}' AS lens_id,
        nc.resolved_id AS resolved_id,
        'ACTIVE' AS activation_status,
        {change_type_case} AS change_type,
        COALESCE(fo.prev_ids_json, '[]') AS previous_resolved_ids_json,
        CAST(NULL AS {str_type}) AS superseded_by_resolved_id,
        nc.cluster_size AS cluster_size,
        nc.entities_added AS entities_added,
        CAST(COALESCE(oc_self.entities_removed, 0) AS {int_type}) AS entities_removed,
        {profile_changed_expr} AS profile_changed,
        {conf_expr} AS confidence_score,
        {current_ts} AS emitted_at
    FROM new_clusters nc
    LEFT JOIN old_clusters oc_self ON oc_self.old_id = nc.resolved_id
    LEFT JOIN foreign_origins fo ON fo.resolved_id = nc.resolved_id
    LEFT JOIN profile_diff pd ON pd.resolved_id = nc.resolved_id
    {conf_join}
),
retired_rows AS (
    SELECT
        '{safe_run_id}' AS run_id,
        '{safe_lens}' AS lens_id,
        oc.old_id AS resolved_id,
        'RETIRED' AS activation_status,
        CASE WHEN oc.surviving_dest_count > 0 THEN 'RETIRED_MERGED' ELSE 'RETIRED_DELETED' END AS change_type,
        '[]' AS previous_resolved_ids_json,
        CASE WHEN oc.surviving_dest_count > 0 THEN oc.min_dest_id ELSE CAST(NULL AS {str_type}) END AS superseded_by_resolved_id,
        CAST(0 AS {int_type}) AS cluster_size,
        CAST(0 AS {int_type}) AS entities_added,
        oc.old_size AS entities_removed,
        FALSE AS profile_changed,
        CAST(NULL AS {float_type}) AS confidence_score,
        {current_ts} AS emitted_at
    FROM old_clusters oc
    LEFT JOIN new_clusters nc ON nc.resolved_id = oc.old_id
    WHERE nc.resolved_id IS NULL
)
SELECT {cols_csv} FROM active_candidates WHERE change_type IS NOT NULL
UNION ALL
SELECT {cols_csv} FROM retired_rows
""".strip()


def _profile_attribute_columns(adapter) -> List[str]:
    if not adapter.table_exists("idr_out.golden_profiles"):
        return []
    reserved = {"resolved_id", "run_id", "updated_at"}
    cols = adapter.get_table_columns("idr_out.golden_profiles") or []
    return [c["name"] for c in cols if c.get("name") and c["name"].lower() not in reserved]


def materialize_activation_delta(
    adapter,
    run_id: str,
    run_mode: str,
    snapshots: Dict[str, bool],
    emit_override: Optional[bool] = None,
    lens_ids: Sequence[str] = (),
) -> Dict[str, int]:
    """Materialize idr_out.identity_activation_delta inside Stage 8 rollback window when enabled."""
    opts = resolve_activation_delta_options(adapter, emit_override=emit_override)
    if not opts.enabled:
        return {}

    if not adapter.table_exists("idr_out.identity_activation_delta"):
        from .schema_defs import SYSTEM_TABLES
        from .schema_manager import SchemaManager

        sm = SchemaManager(adapter)
        for tdef in SYSTEM_TABLES:
            if tdef.fqn == "idr_out.identity_activation_delta":
                sm._create_table(tdef)
                break

    has_backup_m = bool(
        snapshots.get("identity_resolved_membership_current")
        and adapter.table_exists("idr_work._backup_identity_resolved_membership_current")
    )
    prev_count = (
        int(
            adapter.query_one(
                "SELECT COUNT(*) FROM idr_work._backup_identity_resolved_membership_current"
            )
            or 0
        )
        if has_backup_m
        else 0
    )
    if run_mode == "FULL" and prev_count == 0 and not opts.seed_initial_full:
        return {}

    has_backup_gp = bool(
        snapshots.get("golden_profiles")
        and adapter.table_exists("idr_work._backup_golden_profiles")
        and prev_count > 0
    )
    has_clusters = bool(adapter.table_exists("idr_out.identity_clusters_current"))
    scope_affected = bool(
        run_mode == "INCR" and adapter.table_exists("idr_work.output_affected_profile_ids")
    )
    attr_cols = _profile_attribute_columns(adapter)

    # Ensure idempotency if retried on the same run_id
    safe_run_id = str(run_id).replace("'", "''")
    adapter.execute(
        f"DELETE FROM idr_out.identity_activation_delta WHERE run_id = '{safe_run_id}'"
    )

    sql_default = build_activation_delta_insert_sql(
        dialect=adapter.dialect,
        run_id=run_id,
        lens_id="default",
        run_mode=run_mode,
        profile_attr_cols=attr_cols,
        has_backup_membership=(has_backup_m and prev_count > 0),
        has_backup_profiles=has_backup_gp,
        has_clusters_table=has_clusters,
        scope_to_affected=scope_affected,
    )
    adapter.execute(sql_default)

    if opts.include_lenses and lens_ids:
        has_backup_lens = bool(
            snapshots.get("identity_lens_membership_current")
            and adapter.table_exists("idr_work._backup_identity_lens_membership_current")
            and prev_count > 0
        )
        for lid in lens_ids:
            if not lid or lid == "default":
                continue
            sql_lens = build_activation_delta_insert_sql(
                dialect=adapter.dialect,
                run_id=run_id,
                lens_id=lid,
                run_mode=run_mode,
                profile_attr_cols=(),
                has_backup_membership=has_backup_lens,
                has_backup_profiles=False,
                has_clusters_table=False,
                scope_to_affected=False,
            )
            adapter.execute(sql_lens)

    if opts.retention_days and opts.retention_days > 0:
        adapter.execute(build_retention_delete_sql(adapter.dialect, opts.retention_days))

    return summarize_activation_delta(adapter, run_id=run_id, lens_id="default")


def summarize_activation_delta(
    adapter, run_id: str, lens_id: str = "default"
) -> Dict[str, int]:
    """Return {change_type: count} summary for a run_id and lens_id."""
    if not adapter.table_exists("idr_out.identity_activation_delta"):
        return {}
    safe_run = str(run_id).replace("'", "''")
    safe_lens = "default" if lens_id == "default" else validate_identifier(lens_id, "lens_id")
    rows = adapter.query(f"""
        SELECT change_type, COUNT(*) AS cnt
        FROM idr_out.identity_activation_delta
        WHERE run_id = '{safe_run}' AND lens_id = '{safe_lens}'
        GROUP BY change_type
    """)
    return {
        str(r["change_type"]): int(r["cnt"] or 0)
        for r in (rows or [])
        if r.get("change_type") and int(r.get("cnt") or 0) > 0
    }


def query_activation_delta(
    adapter,
    run_id: Optional[str] = None,
    change_type: Optional[str] = None,
    lens_id: str = "default",
    limit: int = 50,
) -> Dict[str, Any]:
    """Query activation delta summary and sample rows for API / MCP inspection."""
    if not adapter.table_exists("idr_out.identity_activation_delta"):
        return {"run_id": run_id, "lens_id": lens_id, "summary": {}, "rows": []}

    target_run = run_id
    if not target_run:
        latest = adapter.query(
            "SELECT run_id FROM idr_out.identity_activation_delta ORDER BY emitted_at DESC LIMIT 1"
        )
        if not latest:
            return {"run_id": None, "lens_id": lens_id, "summary": {}, "rows": []}
        target_run = str(latest[0]["run_id"])

    safe_run = str(target_run).replace("'", "''")
    safe_lens = "default" if lens_id == "default" else validate_identifier(lens_id, "lens_id")
    safe_limit = max(1, min(int(limit or 50), 500))
    str_type = get_dialect_config(getattr(adapter, "dialect", "duckdb")).get(
        "string_type", "VARCHAR"
    )

    ct_clause = ""
    if change_type and str(change_type).strip().upper() != "ALL":
        ct_upper = str(change_type).strip().upper()
        if ct_upper not in VALID_CHANGE_TYPES:
            raise ValueError(f"Invalid change_type '{change_type}'")
        ct_clause = f" AND change_type = '{ct_upper}'"

    summary = summarize_activation_delta(adapter, run_id=target_run, lens_id=safe_lens)
    rows = adapter.query(f"""
        SELECT
            run_id,
            lens_id,
            resolved_id,
            activation_status,
            change_type,
            previous_resolved_ids_json,
            superseded_by_resolved_id,
            cluster_size,
            entities_added,
            entities_removed,
            profile_changed,
            confidence_score,
            CAST(emitted_at AS {str_type}) AS emitted_at
        FROM idr_out.identity_activation_delta
        WHERE run_id = '{safe_run}' AND lens_id = '{safe_lens}'{ct_clause}
        ORDER BY
            CASE activation_status WHEN 'RETIRED' THEN 0 ELSE 1 END,
            entities_added DESC,
            entities_removed DESC,
            resolved_id ASC
        LIMIT {safe_limit}
    """)
    return {
        "run_id": target_run,
        "lens_id": safe_lens,
        "summary": summary,
        "rows": rows or [],
    }
