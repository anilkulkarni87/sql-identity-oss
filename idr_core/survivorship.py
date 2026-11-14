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


def _build_priority_case(
    val_expr: str, p_list: List[str], source_id_lookup: Dict[str, str]
) -> str:
    when_clauses = [f"WHEN {val_expr} IS NULL THEN 999999"]
    for idx, src in enumerate(p_list):
        resolved_src = source_id_lookup.get(str(src), str(src).replace(".", "_"))
        safe_src = validate_identifier(resolved_src, "source_priority_item")
        when_clauses.append(f"WHEN source_id = '{safe_src}' THEN {idx}")
    return f"CASE {' '.join(when_clauses)} ELSE 999 END"


def _surv_recency(
    attr: str, rule: Dict[str, Any], dialect: str, source_id_lookup: Dict[str, str]
) -> str:
    if dialect == "duckdb":
        return (
            f"arg_max({attr}, CASE WHEN {attr} IS NOT NULL "
            f"THEN COALESCE(recency_ts, TIMESTAMP '1970-01-01 00:00:00') ELSE NULL END) AS {attr}"
        )
    if dialect in ("snowflake", "databricks"):
        return (
            f"MAX_BY({attr}, CASE WHEN {attr} IS NOT NULL "
            f"THEN COALESCE(recency_ts, TIMESTAMP '1970-01-01 00:00:00') ELSE NULL END) AS {attr}"
        )
    if dialect == "bigquery":
        return (
            f"ARRAY_AGG({attr} IGNORE NULLS ORDER BY recency_ts DESC LIMIT 1)[SAFE_OFFSET(0)] AS {attr}"
        )
    return f"(ARRAY_AGG({attr} ORDER BY recency_ts DESC NULLS LAST))[1] AS {attr}"


def _surv_priority(
    attr: str, rule: Dict[str, Any], dialect: str, source_id_lookup: Dict[str, str]
) -> str:
    p_list = _parse_priority_list(rule)
    priority_expr = _build_priority_case(attr, p_list, source_id_lookup)
    if dialect == "duckdb":
        return f"arg_min({attr}, {priority_expr}) AS {attr}"
    if dialect in ("snowflake", "databricks"):
        return f"MIN_BY({attr}, {priority_expr}) AS {attr}"
    if dialect == "bigquery":
        return f"ARRAY_AGG({attr} IGNORE NULLS ORDER BY {priority_expr} ASC LIMIT 1)[SAFE_OFFSET(0)] AS {attr}"
    return f"(ARRAY_AGG({attr} ORDER BY {priority_expr} ASC))[1] AS {attr}"


def _surv_priority_recency(
    attr: str,
    rule: Dict[str, Any],
    dialect: str,
    source_id_lookup: Dict[str, str],
    strict_source: Optional[bool] = None,
) -> str:
    p_list = _parse_priority_list(rule)
    is_strict = bool(strict_source if strict_source is not None else rule.get("strict_source", False))

    if is_strict and p_list:
        resolved_sources = [
            f"'{validate_identifier(source_id_lookup.get(str(s), str(s).replace('.', '_')), 'source_priority_item')}'"
            for s in p_list
        ]
        val_expr = f"CASE WHEN source_id IN ({', '.join(resolved_sources)}) THEN {attr} ELSE NULL END"
    else:
        val_expr = attr

    priority_expr = _build_priority_case(val_expr, p_list, source_id_lookup)
    ts_expr = "COALESCE(recency_ts, TIMESTAMP '1970-01-01 00:00:00')"

    if dialect == "duckdb":
        return (
            f"(ARRAY_AGG({val_expr} ORDER BY {priority_expr} ASC, {ts_expr} DESC) "
            f"FILTER (WHERE {val_expr} IS NOT NULL))[1] AS {attr}"
        )
    if dialect == "bigquery":
        return (
            f"ARRAY_AGG({val_expr} IGNORE NULLS ORDER BY {priority_expr} ASC, "
            f"recency_ts DESC LIMIT 1)[SAFE_OFFSET(0)] AS {attr}"
        )
    if dialect == "snowflake":
        return (
            f"GET(ARRAY_AGG(CASE WHEN {val_expr} IS NOT NULL THEN {val_expr} END) "
            f"WITHIN GROUP (ORDER BY {priority_expr} ASC, {ts_expr} DESC), 0) AS {attr}"
        )
    if dialect == "databricks":
        return (
            f"MIN_BY({val_expr}, CASE WHEN {val_expr} IS NOT NULL "
            f"THEN STRUCT({priority_expr} AS p, -UNIX_MICROS({ts_expr}) AS neg_ts) "
            f"ELSE NULL END) AS {attr}"
        )
    return (
        f"(ARRAY_AGG({val_expr} ORDER BY {priority_expr} ASC, {ts_expr} DESC) "
        f"FILTER (WHERE {val_expr} IS NOT NULL))[1] AS {attr}"
    )


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


def _surv_earliest(
    attr: str, rule: Dict[str, Any], dialect: str, source_id_lookup: Dict[str, str]
) -> str:
    if dialect == "duckdb":
        return (
            f"arg_min({attr}, CASE WHEN {attr} IS NOT NULL "
            f"THEN COALESCE(recency_ts, TIMESTAMP '9999-12-31 00:00:00') ELSE NULL END) AS {attr}"
        )
    if dialect in ("snowflake", "databricks"):
        return (
            f"MIN_BY({attr}, CASE WHEN {attr} IS NOT NULL "
            f"THEN COALESCE(recency_ts, TIMESTAMP '9999-12-31 00:00:00') ELSE NULL END) AS {attr}"
        )
    if dialect == "bigquery":
        return (
            f"ARRAY_AGG({attr} IGNORE NULLS ORDER BY recency_ts ASC LIMIT 1)[SAFE_OFFSET(0)] AS {attr}"
        )
    return f"(ARRAY_AGG({attr} ORDER BY recency_ts ASC NULLS LAST))[1] AS {attr}"


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

