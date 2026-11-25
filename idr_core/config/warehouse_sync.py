"""
Warehouse metadata persistence (`apply_config_to_warehouse`), versioned `config_snapshot`
history, and ephemeral `idr_meta.*` dry-run backup/restore helpers.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .loader import validate_config
from .roundtrip_sql import config_to_sql
from .validators import DEFAULT_EXCLUSIONS

META_CONFIG_TABLES = (
    "source_table",
    "identifier_mapping",
    "entity_attribute_mapping",
    "rule",
    "fuzzy_rule",
    "survivorship_rule",
    "identifier_exclusion",
    "identity_override",
    "lens_definition",
)


def snapshot_meta_tables(adapter: Any) -> Dict[str, bool]:
    """Snapshot mutable `idr_meta.*` config tables into `idr_work` before an ephemeral dry run."""
    snapshots: Dict[str, bool] = {}
    for tbl in META_CONFIG_TABLES:
        exists = bool(adapter.table_exists(f"idr_meta.{tbl}"))
        if exists:
            adapter.execute(
                f"CREATE OR REPLACE TABLE idr_work._meta_backup_{tbl} AS SELECT * FROM idr_meta.{tbl}"
            )
        snapshots[tbl] = exists
    return snapshots


def restore_meta_tables(adapter: Any, snapshots: Dict[str, bool]) -> None:
    """Restore `idr_meta.*` config tables from ephemeral dry-run snapshots and clean up backups."""
    for tbl, existed in snapshots.items():
        if not existed:
            continue
        try:
            if adapter.table_exists(f"idr_work._meta_backup_{tbl}"):
                adapter.execute(f"DELETE FROM idr_meta.{tbl} WHERE 1=1")
                adapter.execute(
                    f"INSERT INTO idr_meta.{tbl} SELECT * FROM idr_work._meta_backup_{tbl}"
                )
        except Exception as exc:
            print(f"Warning: Failed to restore idr_meta.{tbl} after dry run: {exc}")
        finally:
            try:
                adapter.execute(f"DROP TABLE IF EXISTS idr_work._meta_backup_{tbl}")
            except Exception:
                pass


def parse_config_snapshot_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Reconstruct an IDRConfig-compatible dictionary from an `idr_out.config_snapshot` row."""
    sources_list = json.loads(row["sources_json"]) if row.get("sources_json") else []
    for src in sources_list:
        if "table_id" in src and "id" not in src:
            src["id"] = src["table_id"]
        if "table_fqn" in src and "table" not in src:
            src["table"] = src["table_fqn"]
        if "entity_key_expr" in src and "entity_key" not in src:
            src["entity_key"] = src["entity_key_expr"]
        if "attributes" not in src:
            src["attributes"] = []
        for ident in src.get("identifiers", []):
            if "expr" in ident and "column" not in ident:
                ident["column"] = ident["expr"]
        for attr in src.get("attributes", []):
            if "expr" in attr and "column" not in attr:
                attr["column"] = attr["expr"]

    config_dict: Dict[str, Any] = {"sources": sources_list}

    if row.get("rules_json"):
        parsed_rules = json.loads(row["rules_json"])
        if isinstance(parsed_rules, dict):
            config_dict["rules"] = parsed_rules.get("rules", [])
            config_dict["fuzzy_rules"] = parsed_rules.get("fuzzy_rules", [])
            config_dict["exclusions"] = parsed_rules.get("exclusions", [])
            config_dict["overrides"] = parsed_rules.get("overrides", [])
            config_dict["use_default_exclusions"] = parsed_rules.get(
                "use_default_exclusions", False
            )
            if parsed_rules.get("evaluation"):
                config_dict["evaluation"] = parsed_rules["evaluation"]
            if parsed_rules.get("grouped_outputs"):
                config_dict["grouped_outputs"] = parsed_rules["grouped_outputs"]
            if parsed_rules.get("activation_delta") is not None:
                config_dict["activation_delta"] = parsed_rules["activation_delta"]
            if parsed_rules.get("ai_assistance"):
                config_dict["ai_assistance"] = parsed_rules["ai_assistance"]
            if parsed_rules.get("lenses"):
                config_dict["lenses"] = parsed_rules["lenses"]
        else:
            config_dict["rules"] = parsed_rules

    if row.get("mappings_json"):
        parsed_surv = json.loads(row["mappings_json"])
        if isinstance(parsed_surv, list):
            config_dict["survivorship"] = [
                s
                for s in parsed_surv
                if isinstance(s, dict) and ("attribute" in s or "attribute_name" in s)
            ]

    return config_dict


def get_config_snapshots(adapter: Any, limit: int = 15) -> List[Dict[str, Any]]:
    """Return recent saved configuration snapshots from `idr_out.config_snapshot`."""
    safe_limit = max(1, min(int(limit or 15), 50))
    if not adapter.table_exists("idr_out.config_snapshot"):
        return []

    rows = adapter.query(f"""
        SELECT config_hash, sources_json, rules_json, mappings_json, created_at
        FROM idr_out.config_snapshot
        ORDER BY created_at DESC
        LIMIT {safe_limit}
    """)
    snapshots: List[Dict[str, Any]] = []
    for row in rows or []:
        try:
            cfg = parse_config_snapshot_row(row)
        except Exception:
            continue
        rules_cnt = len(cfg.get("rules") or []) + len(cfg.get("fuzzy_rules") or [])
        excl_cnt = len(cfg.get("exclusions") or []) + (
            len(DEFAULT_EXCLUSIONS) if cfg.get("use_default_exclusions") else 0
        )
        snapshots.append(
            {
                "config_hash": str(row.get("config_hash") or ""),
                "created_at": str(row.get("created_at") or ""),
                "sources_count": len(cfg.get("sources") or []),
                "rules_count": rules_cnt,
                "exclusions_count": excl_cnt,
                "config": cfg,
            }
        )
    return snapshots


def apply_config_to_warehouse(
    adapter: Any,
    raw_config: Dict[str, Any],
    progress_cb: Optional[Callable[[str, str, int, str], None]] = None,
    record_snapshot: bool = True,
) -> Dict[str, Any]:
    """Validate, clear stale metadata, and persist configuration using batched SQL statements."""
    from ..schema_manager import SchemaManager

    def _report(step: str, label: str, percent: int, detail: str = "") -> None:
        if progress_cb:
            progress_cb(step, label, percent, detail)

    _report(
        "validating",
        "Validating configuration",
        10,
        "Checking sources, rules, and mappings...",
    )
    cfg = dict(raw_config or {})
    for key in ("sources", "rules", "fuzzy_rules", "survivorship", "exclusions", "overrides"):
        if cfg.get(key) is None:
            cfg[key] = []
    for src in cfg.get("sources", []):
        if isinstance(src, dict):
            if src.get("identifiers") is None:
                src["identifiers"] = []
            if src.get("attributes") is None:
                src["attributes"] = []

    validate_config(cfg)
    if cfg.get("ai_assistance"):
        from ..ai_config import candidate_errors

        errors = candidate_errors(cfg)
        if errors:
            raise ValueError("; ".join(errors))

    _report("schema", "Ensuring metadata schema", 20, "Checking idr_meta & idr_out tables...")
    SchemaManager(adapter).initialize(reset=False)

    _report("clearing", "Clearing stale metadata", 30, "Removing obsolete mappings & rules...")
    for meta_table in (
        "idr_meta.config_extension",
        "idr_meta.identifier_mapping",
        "idr_meta.entity_attribute_mapping",
        "idr_meta.source_table",
        "idr_meta.rule",
        "idr_meta.fuzzy_rule",
        "idr_meta.survivorship_rule",
        "idr_meta.lens_definition",
    ):
        try:
            if adapter.table_exists(meta_table):
                adapter.execute(f"DELETE FROM {meta_table} WHERE 1=1")
        except Exception:
            pass

    try:
        if adapter.table_exists("idr_meta.identifier_exclusion"):
            adapter.execute(
                "DELETE FROM idr_meta.identifier_exclusion "
                "WHERE COALESCE(reason, '') IN ('UI exclusion', 'Config exclusion')"
            )
        if adapter.table_exists("idr_meta.identity_override"):
            adapter.execute(
                "DELETE FROM idr_meta.identity_override "
                "WHERE COALESCE(created_by, 'config') IN ('config', 'ui')"
            )
    except Exception:
        pass

    statements = config_to_sql(cfg, dialect=adapter.dialect, batched=True)
    total_batches = len(statements)

    sources_json = json.dumps(cfg.get("sources", []))
    rules_json = json.dumps(
        {
            "rules": cfg.get("rules", []),
            "fuzzy_rules": cfg.get("fuzzy_rules", []),
            "exclusions": cfg.get("exclusions", []),
            "overrides": cfg.get("overrides", []),
            "use_default_exclusions": bool(cfg.get("use_default_exclusions", False)),
            "lenses": cfg.get("lenses", []),
            "ai_assistance": cfg.get("ai_assistance"),
            "evaluation": cfg.get("evaluation"),
            "grouped_outputs": cfg.get("grouped_outputs", []),
            "activation_delta": cfg.get("activation_delta"),
        }
    )
    mappings_json = json.dumps(cfg.get("survivorship", []))
    config_str = sources_json + rules_json + mappings_json
    config_hash = hashlib.sha256(config_str.encode("utf-8")).hexdigest()[:16]
    current_ts = datetime.now(timezone.utc).isoformat()

    def _escape_json(s: str) -> str:
        if adapter.dialect in ("bigquery", "databricks"):
            return s.replace("'", "\\'")
        return s.replace("'", "''")

    sj = _escape_json(sources_json)
    rj = _escape_json(rules_json)
    mj = _escape_json(mappings_json)

    snapshot_sql = f"""
        INSERT INTO idr_out.config_snapshot (config_hash, sources_json, rules_json, mappings_json, created_at)
        VALUES ('{config_hash}', '{sj}', '{rj}', '{mj}', '{current_ts}')
    """

    count = 0
    for idx, stmt in enumerate(statements, start=1):
        pct = 35 + int((idx / max(total_batches, 1)) * 55)
        _report(
            "writing",
            "Writing metadata batches",
            pct,
            f"Executing batch {idx} of {total_batches}...",
        )
        adapter.execute(stmt)
        count += 1

    if record_snapshot:
        _report("snapshot", "Saving config snapshot", 95, "Recording versioned config snapshot...")
        try:
            adapter.execute(snapshot_sql)
        except Exception as snapshot_err:
            print(f"Warning: Failed to save config snapshot: {snapshot_err}")

    msg = f"Configuration saved ({count} batched statements executed)"
    _report("done", "Configuration saved", 100, msg)
    return {
        "status": "ok",
        "message": msg,
        "batches_executed": count,
        "config_hash": config_hash,
    }
