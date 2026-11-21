"""
Pluggable survivorship strategy registry and dialect-aware SQL builder for IDR.

Supports:
1. Built-in strategies:
   - `RECENCY`: Most recently updated non-null value across all sources
   - `PRIORITY`: Highest-priority source from `source_priority_list`
   - `PRIORITY_RECENCY`: Highest-priority source first, breaking ties by latest `recency_ts`
   - `LATEST_FROM_SOURCE`: Latest `recency_ts` restricted strictly to `source_priority_list`
   - `FREQUENCY`: Most frequent non-null value (mode)
   - `ANY_TRUE`: Consent-safe boolean OR (if any linked record is TRUE, result is TRUE)
   - `ALL_TRUE`: Consent-safe boolean AND (TRUE only if all non-null linked records are TRUE)
   - `EARLIEST`: Earliest non-null value by `recency_ts`
   - `AGG_MAX` / `AGG_MIN` / `AGG_SUM`: Numeric/date aggregations
   - `COLLECT_DISTINCT`: Distinct array/list of non-null values
2. Custom Python Registry:
   - `register_survivorship_strategy(name, fn)` where
     `fn(attr: str, rule: dict, dialect: str, source_id_lookup: dict) -> str`
3. Community Plugin Discovery:
   - Auto-loads Python `entry_points(group="idr.survivorship_strategies")`
"""

from __future__ import annotations

import json
import logging
from importlib.metadata import entry_points
from typing import Any, Callable, Dict, List, Optional

from .config import validate_identifier

logger = logging.getLogger(__name__)

SurvivorshipFn = Callable[[str, Dict[str, Any], str, Dict[str, str]], str]


def _parse_priority_list(rule: Dict[str, Any]) -> List[str]:
    raw_list = rule.get("source_priority_list") or rule.get("source_priority") or "[]"
    if isinstance(raw_list, list):
        return [str(x) for x in raw_list]
    try:
        parsed = json.loads(str(raw_list).replace("'", '"'))
        return [str(x) for x in parsed] if isinstance(parsed, list) else []
    except Exception:
        return []


def _build_priority_case(val_expr: str, p_list: List[str], source_id_lookup: Dict[str, str]) -> str:
    when_clauses = [f"WHEN {val_expr} IS NULL THEN 999999"]
    for idx, src in enumerate(p_list):
        resolved_src = source_id_lookup.get(str(src), str(src).replace(".", "_"))
        safe_src = validate_identifier(resolved_src, "source_priority_item")
        when_clauses.append(f"WHEN source_id = '{safe_src}' THEN {idx}")
    return f"CASE {' '.join(when_clauses)} ELSE 999 END"


def selection_order(rule, val_expr, source_id_lookup):
    """Shared ordering for scalar fields and explicitly grouped whole-row winners."""
    strategy = rule["strategy"].upper()
    order = []
    if strategy in ("PRIORITY", "PRIORITY_RECENCY", "LATEST_FROM_SOURCE"):
        order.append(
            (
                _build_priority_case(val_expr, _parse_priority_list(rule), source_id_lookup),
                False,
                False,
            )
        )
    if strategy != "PRIORITY":
        fallback = "9999-12-31" if strategy == "EARLIEST" else "1970-01-01"
        recency = rule.get("_recency_expr", "recency_ts")
        order.append(
            (f"COALESCE({recency}, TIMESTAMP '{fallback} 00:00:00')", strategy != "EARLIEST", True)
        )
    order.append(("source_id", False, False))
    if rule.get("_stable_source_rows"):
        order.append(("source_record_key", False, False))
    return order


def _ordered_value(attr, rule, dialect, source_id_lookup, strict_source=False):
    value = attr
    priority = _parse_priority_list(rule)
    if strict_source and priority:
        sources = ",".join(
            "'" + validate_identifier(source_id_lookup.get(s, s.replace(".", "_")), "source") + "'"
            for s in priority
        )
        value = f"CASE WHEN source_id IN ({sources}) THEN {attr} ELSE NULL END"
    order = selection_order(rule, value, source_id_lookup)
    # Stable value ordering also covers sources which legitimately repeat a key.
    string_type = "STRING" if dialect in ("bigquery", "databricks") else "VARCHAR"
    order.append((f"CAST({value} AS {string_type})", False, False))
    sql_order = ", ".join(expr + (" DESC" if desc else " ASC") for expr, desc, _ in order)
    if dialect == "bigquery":
        return f"ARRAY_AGG({value} IGNORE NULLS ORDER BY {sql_order} LIMIT 1)[SAFE_OFFSET(0)] AS {attr}"
    if dialect == "snowflake":
        return f"GET(ARRAY_AGG({value}) WITHIN GROUP (ORDER BY {sql_order}), 0) AS {attr}"
    if dialect in ("duckdb", "databricks"):
        keys = [f"CASE WHEN {value} IS NULL THEN 1 ELSE 0 END AS missing"]
        for i, (expr, desc, timestamp) in enumerate(order):
            key = (
                (f"EPOCH_US({expr})" if dialect == "duckdb" else f"UNIX_MICROS({expr})")
                if timestamp
                else expr
            )
            keys.append((f"-({key})" if desc else key) + f" AS k{i}")
        if dialect == "duckdb":
            packed = ", ".join(
                part.rsplit(" AS ", 1)[1] + ":=" + part.rsplit(" AS ", 1)[0] for part in keys
            )
            return f"arg_min({value}, STRUCT_PACK({packed})) AS {attr}"
        return f"MIN_BY({value}, STRUCT({', '.join(keys)})) AS {attr}"
    return (
        f"(ARRAY_AGG({value} ORDER BY {sql_order}) FILTER (WHERE {value} IS NOT NULL))[1] AS {attr}"
    )


def _surv_recency(attr, rule, dialect, source_id_lookup):
    return _ordered_value(attr, rule, dialect, source_id_lookup)


def _surv_priority(attr, rule, dialect, source_id_lookup):
    return _ordered_value(attr, rule, dialect, source_id_lookup)


def _surv_priority_recency(attr, rule, dialect, source_id_lookup, strict_source=None):
    strict = bool(strict_source if strict_source is not None else rule.get("strict_source", False))
    return _ordered_value(attr, rule, dialect, source_id_lookup, strict)


def _surv_latest_from_source(
    attr: str, rule: Dict[str, Any], dialect: str, source_id_lookup: Dict[str, str]
) -> str:
    return _surv_priority_recency(attr, rule, dialect, source_id_lookup, strict_source=True)


def _surv_frequency(
    attr: str, rule: Dict[str, Any], dialect: str, source_id_lookup: Dict[str, str]
) -> str:
    if dialect == "duckdb":
        return f"mode({attr}) AS {attr}"
    if dialect in ("snowflake", "databricks"):
        return f"MODE({attr}) AS {attr}"
    if dialect == "bigquery":
        return f"APPROX_TOP_COUNT({attr}, 1)[SAFE_OFFSET(0)].value AS {attr}"
    return f"MODE() WITHIN GROUP (ORDER BY {attr}) AS {attr}"


def _surv_any_true(
    attr: str, rule: Dict[str, Any], dialect: str, source_id_lookup: Dict[str, str]
) -> str:
    return (
        f"CASE WHEN COUNT({attr}) = 0 THEN NULL "
        f"WHEN MAX(CASE WHEN CAST({attr} AS BOOLEAN) THEN 1 ELSE 0 END) = 1 "
        f"THEN TRUE ELSE FALSE END AS {attr}"
    )


def _surv_all_true(
    attr: str, rule: Dict[str, Any], dialect: str, source_id_lookup: Dict[str, str]
) -> str:
    return (
        f"CASE WHEN COUNT({attr}) = 0 THEN NULL "
        f"WHEN MIN(CASE WHEN CAST({attr} AS BOOLEAN) THEN 1 ELSE 0 END) = 1 "
        f"THEN TRUE ELSE FALSE END AS {attr}"
    )


def _surv_earliest(attr, rule, dialect, source_id_lookup):
    return _ordered_value(attr, rule, dialect, source_id_lookup)


def _surv_agg_max(
    attr: str, rule: Dict[str, Any], dialect: str, source_id_lookup: Dict[str, str]
) -> str:
    return f"MAX({attr}) AS {attr}"


def _surv_agg_min(
    attr: str, rule: Dict[str, Any], dialect: str, source_id_lookup: Dict[str, str]
) -> str:
    return f"MIN({attr}) AS {attr}"


def _surv_agg_sum(
    attr: str, rule: Dict[str, Any], dialect: str, source_id_lookup: Dict[str, str]
) -> str:
    return f"SUM({attr}) AS {attr}"


def _surv_collect_distinct(
    attr: str, rule: Dict[str, Any], dialect: str, source_id_lookup: Dict[str, str]
) -> str:
    if dialect == "duckdb":
        return f"LIST(DISTINCT {attr}) FILTER (WHERE {attr} IS NOT NULL) AS {attr}"
    if dialect == "bigquery":
        return f"ARRAY_AGG(DISTINCT {attr} IGNORE NULLS) AS {attr}"
    if dialect == "databricks":
        return f"COLLECT_SET({attr}) AS {attr}"
    return f"ARRAY_AGG(DISTINCT {attr}) AS {attr}"


_SURVIVORSHIP_REGISTRY: Dict[str, SurvivorshipFn] = {
    "RECENCY": _surv_recency,
    "PRIORITY": _surv_priority,
    "PRIORITY_RECENCY": _surv_priority_recency,
    "LATEST_FROM_SOURCE": _surv_latest_from_source,
    "FREQUENCY": _surv_frequency,
    "ANY_TRUE": _surv_any_true,
    "ALL_TRUE": _surv_all_true,
    "EARLIEST": _surv_earliest,
    "AGG_MAX": _surv_agg_max,
    "AGG_MIN": _surv_agg_min,
    "AGG_SUM": _surv_agg_sum,
    "COLLECT_DISTINCT": _surv_collect_distinct,
}

_PLUGINS_LOADED = False


def register_survivorship_strategy(name: str, fn: SurvivorshipFn) -> None:
    """Register a custom survivorship strategy function."""
    if not name or not isinstance(name, str):
        raise ValueError("Survivorship strategy name must be a non-empty string")
    if not callable(fn):
        raise ValueError(
            "Survivorship strategy fn must be callable(attr, rule, dialect, source_id_lookup) -> str"
        )
    _SURVIVORSHIP_REGISTRY[name.strip().upper()] = fn


def load_survivorship_plugins(force: bool = False) -> int:
    """Load third-party survivorship strategies registered via Python entry_points."""
    global _PLUGINS_LOADED
    if _PLUGINS_LOADED and not force:
        return 0
    _PLUGINS_LOADED = True
    loaded = 0
    try:
        eps = entry_points(group="idr.survivorship_strategies")
    except Exception as exc:
        logger.warning("Failed to inspect idr.survivorship_strategies entry points: %s", exc)
        return 0

    for ep in eps:
        try:
            fn = ep.load()
            register_survivorship_strategy(ep.name, fn)
            loaded += 1
        except Exception as exc:
            logger.warning("Failed to load survivorship plugin '%s': %s", ep.name, exc)
    return loaded


def list_survivorship_strategies() -> List[str]:
    """Return sorted list of registered survivorship strategy names."""
    load_survivorship_plugins()
    return sorted(_SURVIVORSHIP_REGISTRY.keys())


def is_registered_survivorship_strategy(name: str) -> bool:
    """Check whether `name` is a registered survivorship strategy."""
    load_survivorship_plugins()
    return str(name or "").strip().upper() in _SURVIVORSHIP_REGISTRY


def build_survivorship_expr(
    attr: str,
    rule: Dict[str, Any],
    dialect: str = "duckdb",
    source_id_lookup: Optional[Dict[str, str]] = None,
) -> str:
    """Build the dialect-specific SQL aggregation expression for a survivorship rule."""
    load_survivorship_plugins()
    strategy = str(rule.get("strategy") or "RECENCY").strip().upper()
    fn = _SURVIVORSHIP_REGISTRY.get(strategy)
    if fn is None:
        logger.warning("Unknown survivorship strategy '%s'; falling back to MAX()", strategy)
        return f"MAX({attr}) AS {attr}"
    return fn(attr, rule, dialect, source_id_lookup or {})
