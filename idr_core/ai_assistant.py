"""
AI Assistant workflows for sql-identity-oss (CLI + FastAPI).

Provides:
- Connection testing (Ollama / OpenAI / Anthropic / Gemini)
- Plain-English cluster explanations
- Low-confidence cluster triage & override proposals
- Natural-language Q&A over identity graph & run telemetry
- Natural-language config/rule drafting
"""

from __future__ import annotations

import json
import math
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set


def _sanitize_json(obj: Any) -> Any:
    """Recursively replace NaN/Inf floats (e.g. from BigQuery/pandas) with None for strict JSON."""
    if isinstance(obj, dict):
        return {k: _sanitize_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_json(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if hasattr(obj, "isoformat"):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    return obj

from .llm import LLMClient, mask_pii_payload


def get_configured_pii_keys(adapter: Any) -> Set[str]:
    """Collect mapped identifier and attribute column/field names from idr_meta.*."""
    keys: Set[str] = set()
    if adapter is None:
        return keys
    for query, cols in (
        (
            "SELECT identifier_type, identifier_value_expr FROM idr_meta.identifier_mapping",
            ("identifier_type", "identifier_value_expr"),
        ),
        (
            "SELECT attribute_name, attribute_expr FROM idr_meta.entity_attribute_mapping",
            ("attribute_name", "attribute_expr"),
        ),
    ):
        try:
            for row in adapter.query(query):
                for col in cols:
                    val = str(row.get(col) or "").strip().lower()
                    if val and re.match(r"^[a-z0-9_]+$", val):
                        keys.add(val)
        except Exception:
            pass
    return keys


def _apply_pii_mask(adapter: Any, payload: Any, client: LLMClient) -> Any:
    if not client.config.effective_mask_pii:
        return payload
    extra = get_configured_pii_keys(adapter)
    return mask_pii_payload(payload, extra_sensitive_keys=extra)


def test_llm_connection(client: Optional[LLMClient] = None) -> Dict[str, Any]:
    """Ping the configured LLM endpoint and report latency & masking status."""
    client = client or LLMClient()
    start = time.perf_counter()
    reply = client.chat(
        [
            {"role": "system", "content": "Reply with exactly: OK"},
            {"role": "user", "content": "ping"},
        ],
        temperature=0.0,
    )
    latency_ms = round((time.perf_counter() - start) * 1000, 1)
    return {
        "status": "OK",
        "provider": client.config.provider,
        "model": client.config.resolved_model(),
        "is_local": client.config.is_local,
        "mask_pii": client.config.effective_mask_pii,
        "latency_ms": latency_ms,
        "reply": reply.strip(),
    }


def _fetch_cluster_context(adapter: Any, resolved_id: str, max_members: int = 30, max_edges: int = 50) -> Dict[str, Any]:
    safe_id = resolved_id.replace("'", "''")
    cluster_rows = adapter.query(
        f"SELECT * FROM idr_out.identity_clusters_current WHERE resolved_id = '{safe_id}' LIMIT 1"
    )
    if not cluster_rows:
        raise ValueError(f"Cluster '{resolved_id}' not found in idr_out.identity_clusters_current")

    members = adapter.query(
        f"SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current "
        f"WHERE resolved_id = '{safe_id}' LIMIT {int(max_members)}"
    )
    member_keys = [m["entity_key"] for m in members]
    edges: List[Dict[str, Any]] = []
    if member_keys:
        in_clause = ", ".join(f"'{str(k).replace(chr(39), chr(39)*2)}'" for k in member_keys)
        edges = adapter.query(
            f"SELECT left_entity_key, right_entity_key, identifier_type, identifier_value_norm "
            f"FROM idr_out.identity_edges_current "
            f"WHERE left_entity_key IN ({in_clause}) OR right_entity_key IN ({in_clause}) "
            f"LIMIT {int(max_edges)}"
        )

    return {
        "cluster": cluster_rows[0],
        "members": members,
        "edges": edges,
    }


def explain_cluster_with_ai(
    adapter: Any,
    resolved_id: str,
    client: Optional[LLMClient] = None,
    custom_prompt: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate a plain-English explanation of why a cluster merged and any risks."""
    client = client or LLMClient()
    ctx = _fetch_cluster_context(adapter, resolved_id)
    payload = _apply_pii_mask(adapter, ctx, client)

    cluster = payload.get("cluster") or {}
    members = payload.get("members") or []
    edges = payload.get("edges") or []

    # Summarize sources and edge types so small local models (e.g. llama3.2) stay focused
    source_counts: Dict[str, int] = {}
    for m in members:
        ek = str(m.get("entity_key", ""))
        src = ek.split(":", 1)[0] if ":" in ek else "unknown"
        source_counts[src] = source_counts.get(src, 0) + 1

    edge_by_type: Dict[str, List[str]] = {}
    for e in edges:
        itype = str(e.get("identifier_type", "unknown"))
        val = str(e.get("identifier_value_norm", ""))
        pair = f"{e.get('left_entity_key')} <-> {e.get('right_entity_key')} (val={val})"
        edge_by_type.setdefault(itype, []).append(pair)

    edge_summary_lines = []
    for itype, pairs in edge_by_type.items():
        sample = "; ".join(pairs[:3])
        edge_summary_lines.append(f"- {itype}: {len(pairs)} edge(s) | examples: {sample}")

    compact_summary = (
        f"Cluster ID: {resolved_id}\n"
        f"Size: {cluster.get('cluster_size', len(members))} entities | "
        f"Confidence: {cluster.get('confidence_score', 'N/A')} | "
        f"Edge Diversity: {cluster.get('edge_diversity', len(edge_by_type))} | "
        f"Total Edges: {cluster.get('edge_count', len(edges))}\n"
        f"Sources represented: {json.dumps(source_counts)}\n"
        f"Match Edges Breakdown:\n" + ("\n".join(edge_summary_lines) or "- None")
    )

    if custom_prompt and custom_prompt.strip():
        prompt = (
            "You are an Identity Resolution Steward reviewing a customer cluster.\n"
            "Do NOT describe JSON schemas or field names. Answer the user's request directly "
            "using the cluster summary below:\n\n"
            f"User Request: {custom_prompt.strip()}\n\n"
            f"{compact_summary}"
        )
    else:
        prompt = (
            "You are an Identity Resolution Steward reviewing a customer cluster.\n"
            "Do NOT describe JSON schemas or field names. Write 3-5 concise bullet points covering:\n"
            "1. Why these records merged (cite the exact source systems and identifier types).\n"
            "2. Over-merge / supernode risk assessment (based on confidence score, edge diversity, and cluster size).\n"
            "3. Recommended action: KEEP AS-IS, DO_NOT_LINK a suspicious pair, or ADD_EXCLUSION.\n\n"
            f"{compact_summary}"
        )
    explanation = client.chat(
        [
            {
                "role": "system",
                "content": "Give a direct, plain-English data stewardship summary. Never explain JSON structure.",
            },
            {"role": "user", "content": prompt},
        ]
    )
    return {
        "resolved_id": resolved_id,
        "pii_masked": client.config.effective_mask_pii,
        "provider": client.config.provider,
        "model": client.config.resolved_model(),
        "explanation": explanation.strip(),
    }


def triage_with_ai(
    adapter: Any,
    client: Optional[LLMClient] = None,
    max_confidence: float = 0.65,
    limit: int = 8,
) -> Dict[str, Any]:
    """Review low-confidence or large clusters and propose stewardship overrides."""
    client = client or LLMClient()
    candidates = adapter.query(
        f"""
        SELECT resolved_id, cluster_size, confidence_score, edge_diversity, edge_count
        FROM idr_out.identity_clusters_current
        WHERE cluster_size > 1
          AND (confidence_score <= {float(max_confidence)} OR cluster_size >= 10)
        ORDER BY confidence_score ASC, cluster_size DESC
        LIMIT {int(limit)}
        """
    )
    if not candidates:
        return {
            "status": "CLEAN",
            "pii_masked": client.config.effective_mask_pii,
            "proposals": [],
            "message": "No low-confidence or oversized clusters found.",
        }

    enriched = []
    for c in candidates:
        ctx = _fetch_cluster_context(adapter, c["resolved_id"], max_members=15, max_edges=25)
        enriched.append(ctx)

    payload = _apply_pii_mask(adapter, enriched, client)
    prompt = (
        "Review these flagged identity clusters and return JSON with a 'proposals' array.\n"
        "Each proposal must have: resolved_id, action ('KEEP' | 'DO_NOT_LINK' | 'ISOLATE' | 'ADD_EXCLUSION'), "
        "entity_key_a (optional), entity_key_b (optional), reason (1 sentence).\n\n"
        f"Flagged Clusters:\n{json.dumps(payload, default=str)}"
    )
    raw = client.chat(
        [
            {"role": "system", "content": "Return valid JSON only with top-level key 'proposals'."},
            {"role": "user", "content": prompt},
        ],
        json_mode=True,
    )
    try:
        parsed = json.loads(raw)
        proposals = parsed.get("proposals", [])
    except Exception:
        proposals = [{"resolved_id": "parse_error", "action": "REVIEW", "reason": raw[:500]}]

    return {
        "status": "OK",
        "pii_masked": client.config.effective_mask_pii,
        "provider": client.config.provider,
        "model": client.config.resolved_model(),
        "candidates_reviewed": len(candidates),
        "proposals": proposals,
    }


def ask_identity_graph(
    adapter: Any,
    question: str,
    client: Optional[LLMClient] = None,
) -> Dict[str, Any]:
    """Answer a natural-language question about the identity graph using warehouse telemetry."""
    client = client or LLMClient()
    summary: Dict[str, Any] = {}
    try:
        summary["cluster_stats"] = adapter.query(
            """
            SELECT
                COUNT(*) AS total_clusters,
                SUM(CASE WHEN cluster_size = 1 THEN 1 ELSE 0 END) AS singletons,
                SUM(CASE WHEN cluster_size > 1 THEN 1 ELSE 0 END) AS multi_entity_clusters,
                MAX(cluster_size) AS max_cluster_size,
                ROUND(AVG(confidence_score), 3) AS avg_confidence
            FROM idr_out.identity_clusters_current
            """
        )
    except Exception:
        summary["cluster_stats"] = []

    try:
        summary["recent_runs"] = adapter.query(
            "SELECT run_id, run_mode, status, started_at, duration_seconds, "
            "entities_processed, edges_created, clusters_impacted "
            "FROM idr_out.run_history ORDER BY started_at DESC LIMIT 10"
        )
    except Exception:
        summary["recent_runs"] = []

    try:
        summary["skipped_groups"] = adapter.query(
            "SELECT identifier_type, identifier_value_norm, group_size, max_allowed "
            "FROM idr_out.skipped_identifier_groups ORDER BY group_size DESC LIMIT 10"
        )
    except Exception:
        summary["skipped_groups"] = []

    try:
        from .config import sql_to_config

        cfg = sql_to_config(adapter)
        summary["active_config"] = {
            "sources": [
                {
                    "id": s.get("id"),
                    "table": s.get("table"),
                    "entity_key": s.get("entity_key"),
                    "identifiers": [i.get("type") for i in (s.get("identifiers") or []) if i.get("type")],
                }
                for s in (cfg.get("sources") or [])[:25]
            ],
            "rules": cfg.get("rules") or [],
            "fuzzy_rules": cfg.get("fuzzy_rules") or [],
            "lenses": cfg.get("lenses") or [],
            "exclusions_count": len(cfg.get("exclusions") or []),
            "exclusions_sample": (cfg.get("exclusions") or [])[:15],
            "overrides_count": len(cfg.get("overrides") or []),
            "overrides_sample": (cfg.get("overrides") or [])[:15],
            "survivorship_count": len(cfg.get("survivorship") or []),
        }
    except Exception:
        summary["active_config"] = {}

    safe_summary = _apply_pii_mask(adapter, summary, client)
    safe_question = question

    answer = client.chat(
        [
            {
                "role": "system",
                "content": (
                    "You are the SQL Identity Resolution assistant. Answer the user's question "
                    "accurately using the warehouse telemetry provided. Do not invent numbers."
                ),
            },
            {
                "role": "user",
                "content": f"Telemetry:\n{json.dumps(safe_summary, default=str, indent=2)}\n\nQuestion: {safe_question}",
            },
        ]
    )
    return {
        "question": question,
        "pii_masked": client.config.effective_mask_pii,
        "provider": client.config.provider,
        "model": client.config.resolved_model(),
        "answer": answer.strip(),
    }


def suggest_config_with_ai(
    profiler_summary: Dict[str, Any],
    prompt_text: str,
    client: Optional[LLMClient] = None,
) -> Dict[str, Any]:
    """Generate/refine an IDR config dictionary from schema profile + natural-language goal."""
    client = client or LLMClient()
    safe_profile = mask_pii_payload(profiler_summary) if client.config.effective_mask_pii else profiler_summary

    prompt = (
        "Generate a valid sql-identity-oss JSON config matching the user's goal and profiled tables.\n"
        "Return a JSON object with this exact top-level shape:\n"
        '{"sources": [{"id": "table_id", "table": "schema.table", "entity_type": "PERSON", '
        '"entity_key": "pk_col", "watermark_column": "updated_at", '
        '"identifiers": [{"type": "email", "expr": "email_col"}], '
        '"attributes": [{"name": "first_name", "expr": "first_name"}]}], '
        '"rules": [{"id": "100", "type": "EXACT", "match_keys": ["email"], "priority": 1, "canonicalize": "EMAIL_STANDARD"}], '
        '"survivorship": [{"attribute": "first_name", "strategy": "RECENCY"}], '
        '"use_default_exclusions": true, '
        '"lenses": [{"id": "strict_person", "include_types": ["EMAIL", "CUSTOMER_ID"]}, '
        '{"id": "household", "include_types": ["EMAIL", "PHONE", "ADDRESS_ID", "CUSTOMER_ID"], "max_group_size": 25}]}\n\n'
        "Available survivorship strategies: RECENCY, PRIORITY, PRIORITY_RECENCY, LATEST_FROM_SOURCE, "
        "FREQUENCY, ANY_TRUE, ALL_TRUE, EARLIEST, AGG_MAX, AGG_MIN, AGG_SUM, COLLECT_DISTINCT.\n"
        "Available canonicalizers: EMAIL_STANDARD, EMAIL_STRIP_PLUS, PHONE_US_10, PHONE_DIGITS, "
        "ALPHANUMERIC_UPPER, LOWERCASE, UPPERCASE, EXACT.\n\n"
        f"User Goal: {prompt_text}\n\n"
        f"Profiled Schema:\n{json.dumps(safe_profile, default=str)}"
    )
    raw = client.chat(
        [
            {"role": "system", "content": "Return valid JSON only with top-level keys: sources, rules, survivorship, use_default_exclusions, lenses."},
            {"role": "user", "content": prompt},
        ],
        json_mode=True,
    )
    config_dict = json.loads(raw)
    if isinstance(config_dict, dict):
        if "sources" not in config_dict:
            for wrapper_key in ("config", "suggested_config", "idr_config"):
                nested = config_dict.get(wrapper_key)
                if isinstance(nested, dict) and nested.get("sources"):
                    config_dict = nested
                    break
        base_cfg = (
            profiler_summary.get("suggested_config")
            if isinstance(profiler_summary, dict)
            and isinstance(profiler_summary.get("suggested_config"), dict)
            else profiler_summary
        )
        if not config_dict.get("sources") and isinstance(base_cfg, dict) and base_cfg.get("sources"):
            config_dict["sources"] = base_cfg["sources"]
            if not config_dict.get("rules") and base_cfg.get("rules"):
                config_dict["rules"] = base_cfg["rules"]
            if not config_dict.get("survivorship") and base_cfg.get("survivorship"):
                config_dict["survivorship"] = base_cfg["survivorship"]
    return {
        "pii_masked": client.config.effective_mask_pii,
        "provider": client.config.provider,
        "model": client.config.resolved_model(),
        "suggested_config": config_dict,
    }


def get_run_runtime_trend(adapter: Any, days: int = 14) -> Dict[str, Any]:
    """Summarize recent IDR run durations, today's runs, and stage-level timing breakdown."""
    runs: List[Dict[str, Any]] = []
    try:
        runs = adapter.query(
            """
            SELECT
                run_id,
                run_mode,
                status,
                CAST(started_at AS VARCHAR) AS started_at,
                duration_seconds,
                entities_processed,
                edges_created,
                clusters_impacted
            FROM idr_out.run_history
            ORDER BY started_at DESC
            LIMIT 30
            """
        )
    except Exception:
        try:
            runs = adapter.query(
                """
                SELECT
                    run_id,
                    run_mode,
                    status,
                    started_at,
                    duration_seconds,
                    entities_processed,
                    edges_created,
                    clusters_impacted
                FROM idr_out.run_history
                ORDER BY started_at DESC
                LIMIT 30
                """
            )
        except Exception:
            runs = []

    today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    local_prefix = datetime.now().strftime("%Y-%m-%d")
    today_runs = [
        r for r in runs
        if str(r.get("started_at") or "").startswith((today_prefix, local_prefix))
    ]
    durations = [
        float(r["duration_seconds"])
        for r in runs
        if r.get("duration_seconds") is not None and str(r.get("status", "")).upper() in ("SUCCESS", "DRY_RUN_COMPLETE", "")
    ]
    latest = runs[0] if runs else None
    avg_dur = round(sum(durations) / len(durations), 2) if durations else None
    delta_pct = None
    if latest and latest.get("duration_seconds") is not None and avg_dur and avg_dur > 0:
        delta_pct = round(((float(latest["duration_seconds"]) - avg_dur) / avg_dur) * 100.0, 1)

    latest_stages: List[Dict[str, Any]] = []
    if latest and latest.get("run_id"):
        try:
            latest_stages = adapter.query(
                """
                SELECT stage_name, duration_seconds, rows_out
                FROM idr_out.stage_metrics
                WHERE run_id = ?
                ORDER BY stage_order ASC
                """,
                params=[latest["run_id"]],
            )
        except Exception:
            latest_stages = []

    return {
        "total_runs_checked": len(runs),
        "today_runs": today_runs,
        "latest_run": latest,
        "avg_duration_seconds": avg_dur,
        "min_duration_seconds": round(min(durations), 2) if durations else None,
        "max_duration_seconds": round(max(durations), 2) if durations else None,
        "latest_vs_avg_pct": delta_pct,
        "latest_stages": latest_stages,
        "recent_runs": runs[:10],
    }


def find_cluster_bridge(
    adapter: Any,
    entity_key_a: str,
    entity_key_b: str,
    max_edges: int = 500,
) -> Dict[str, Any]:
    """Find the shortest bridge path between two entities and suggest a DO_NOT_LINK/exclusion fix."""
    from collections import deque

    mem_a = adapter.query(
        "SELECT resolved_id FROM idr_out.identity_resolved_membership_current WHERE entity_key = ? LIMIT 1",
        params=[entity_key_a],
    )
    mem_b = adapter.query(
        "SELECT resolved_id FROM idr_out.identity_resolved_membership_current WHERE entity_key = ? LIMIT 1",
        params=[entity_key_b],
    )
    rid_a = mem_a[0]["resolved_id"] if mem_a else None
    rid_b = mem_b[0]["resolved_id"] if mem_b else None

    if not rid_a or not rid_b or rid_a != rid_b:
        return {
            "connected": False,
            "resolved_id_a": rid_a,
            "resolved_id_b": rid_b,
            "hops": 0,
            "path": [],
            "summary": f"{entity_key_a} and {entity_key_b} are not in the same cluster.",
        }

    edge_rows = adapter.query(
        f"""
        SELECT e.left_entity_key, e.right_entity_key, e.identifier_type, e.identifier_value_norm
        FROM idr_out.identity_edges_current e
        JOIN idr_out.identity_resolved_membership_current m1
          ON e.left_entity_key = m1.entity_key
        WHERE m1.resolved_id = ?
        LIMIT {int(max_edges)}
        """,
        params=[rid_a],
    )

    adj: Dict[str, List[Dict[str, str]]] = {}
    for er in edge_rows:
        u = str(er["left_entity_key"])
        v = str(er["right_entity_key"])
        itype = str(er.get("identifier_type") or "")
        ival = str(er.get("identifier_value_norm") or "")
        adj.setdefault(u, []).append({"to": v, "identifier_type": itype, "identifier_value": ival})
        adj.setdefault(v, []).append({"to": u, "identifier_type": itype, "identifier_value": ival})

    queue: deque = deque([(entity_key_a, [])])
    visited = {entity_key_a}
    found_path: List[Dict[str, str]] = []
    while queue:
        curr, path = queue.popleft()
        if curr == entity_key_b:
            found_path = path
            break
        for edge in adj.get(curr, []):
            nxt = edge["to"]
            if nxt not in visited:
                visited.add(nxt)
                queue.append(
                    (
                        nxt,
                        path
                        + [
                            {
                                "from": curr,
                                "to": nxt,
                                "identifier_type": edge["identifier_type"],
                                "identifier_value": edge["identifier_value"],
                            }
                        ],
                    )
                )

    bridge = found_path[-1] if found_path else None
    chain_str = " -> ".join(
        [f"{p['from']} --({p['identifier_type']}:{p['identifier_value']})--> {p['to']}" for p in found_path]
    )
    return {
        "connected": bool(found_path),
        "resolved_id": rid_a,
        "hops": len(found_path),
        "path": found_path,
        "summary": chain_str or f"Both entities belong to {rid_a}, but no direct edge path was found within {max_edges} edges.",
        "recommended_override": (
            {
                "action": "add",
                "override_type": "DO_NOT_LINK",
                "entity_key_a": bridge["from"],
                "entity_key_b": bridge["to"],
                "reason": f"Break bridge on {bridge['identifier_type']}",
            }
            if bridge
            else None
        ),
        "recommended_exclusion": (
            {
                "action": "add",
                "identifier_type": bridge["identifier_type"],
                "identifier_value_pattern": bridge["identifier_value"],
                "match_type": "EXACT",
                "reason": f"Toxic bridge value in {rid_a}",
            }
            if bridge and bridge.get("identifier_value")
            else None
        ),
    }


def diagnose_run_anomalies(adapter: Any) -> Dict[str, Any]:
    """Return a compact, high-signal anomaly & drift report without raw table dumps."""
    trend = get_run_runtime_trend(adapter)
    latest = trend.get("latest_run") or {}
    delta_pct = trend.get("latest_vs_avg_pct")
    stages = trend.get("latest_stages") or []
    slowest_stage = (
        max(stages, key=lambda s: float(s.get("duration_seconds") or 0))
        if stages
        else None
    )

    skipped_hubs: List[Dict[str, Any]] = []
    try:
        skipped_hubs = adapter.query(
            """
            SELECT identifier_type, identifier_value_norm, group_size, max_allowed
            FROM idr_out.skipped_identifier_groups
            ORDER BY group_size DESC
            LIMIT 5
            """
        )
    except Exception:
        skipped_hubs = []

    anomalies: List[str] = []
    if delta_pct is not None and delta_pct >= 25.0:
        anomalies.append(
            f"Latest run `{latest.get('run_id')}` took {latest.get('duration_seconds')}s (+{delta_pct}% vs {trend.get('avg_duration_seconds')}s avg)."
        )
    if slowest_stage:
        anomalies.append(
            f"Slowest stage: `{slowest_stage.get('stage_name')}` ({slowest_stage.get('duration_seconds')}s)."
        )
    if skipped_hubs:
        top = skipped_hubs[0]
        anomalies.append(
            f"Found {len(skipped_hubs)} oversized/skipped identifier hub(s); top hub `{top.get('identifier_type')}:{top.get('identifier_value_norm')}` has {top.get('group_size')} entities (limit {top.get('max_allowed')})."
        )

    return {
        "latest_run_id": latest.get("run_id"),
        "latest_duration_seconds": latest.get("duration_seconds"),
        "avg_duration_seconds": trend.get("avg_duration_seconds"),
        "runtime_delta_pct": delta_pct,
        "slowest_stage": slowest_stage,
        "skipped_hubs": skipped_hubs,
        "anomalies": anomalies,
        "recommended_exclusions": [
            {
                "identifier_type": h.get("identifier_type"),
                "identifier_value_pattern": h.get("identifier_value_norm"),
                "match_type": "EXACT",
                "group_size": h.get("group_size"),
            }
            for h in skipped_hubs
        ],
    }


def draft_lens_from_goal(
    adapter: Any,
    goal: str,
    client: Optional[LLMClient] = None,
) -> Dict[str, Any]:
    """Synthesize a validated lens definition from active warehouse rules & natural-language goal."""
    from .config import sql_to_config

    cfg = sql_to_config(adapter)
    available_types = sorted(
        {
            str(k).strip().upper()
            for r in (cfg.get("rules") or [])
            for k in (r.get("match_keys") or ([r["match_key"]] if r.get("match_key") else []))
            if str(k).strip()
        }
        | {
            str(i.get("type") or "").strip().upper()
            for s in (cfg.get("sources") or [])
            for i in (s.get("identifiers") or [])
            if str(i.get("type") or "").strip()
        }
    )
    available_sources = sorted({str(s.get("id")) for s in (cfg.get("sources") or []) if s.get("id")})

    drafted: Optional[Dict[str, Any]] = None
    if client is not None:
        prompt = (
            "Draft a single SQL Identity Resolution lens JSON object.\n"
            'Schema: {"id": "snake_case_id", "include_types": [...], "exclude_types": [...], "source_filter": [...], "max_group_size": int|null}\n'
            f"Available identifier types: {available_types}\n"
            f"Available source IDs: {available_sources}\n"
            f"User Goal: {goal}"
        )
        try:
            raw = client.chat(
                [
                    {"role": "system", "content": "Return valid JSON only for a single lens object."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.0,
                json_mode=True,
            )
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and (parsed.get("id") or parsed.get("lens_id")):
                drafted = parsed
        except Exception:
            drafted = None

    if not drafted:
        g = (goal or "").lower()
        strict = any(w in g for w in ("strict", "login", "person", "high precision"))
        household = "household" in g or "family" in g
        lid = "strict_person" if strict else ("household" if household else "custom_lens")
        inc = [t for t in available_types if t in ("EMAIL", "CUSTOMER_ID")] if strict else available_types
        if not inc:
            inc = available_types
        drafted = {
            "id": lid,
            "include_types": inc,
            "exclude_types": ["PHONE", "ADDRESS_ID"] if strict else [],
            "source_filter": [],
            **({"max_group_size": 25} if household else {}),
        }

    return {
        "lens": drafted,
        "available_types": available_types,
        "available_sources": available_sources,
    }


def _resolve_target_cluster_id(adapter: Any, resolved_id: Optional[str] = None) -> Optional[str]:
    """Return the given resolved_id, or sample a multi-entity cluster if available."""
    target_rid = (resolved_id or "").strip()
    if target_rid:
        return target_rid
    rows = adapter.query(
        """
        SELECT resolved_id, cluster_size, confidence_score
        FROM idr_out.identity_clusters_current
        WHERE cluster_size BETWEEN 2 AND 10
        ORDER BY cluster_size DESC, resolved_id ASC
        LIMIT 1
        """
    )
    if not rows:
        rows = adapter.query(
            """
            SELECT resolved_id, cluster_size, confidence_score
            FROM idr_out.identity_clusters_current
            WHERE cluster_size > 1
            ORDER BY cluster_size ASC, resolved_id ASC
            LIMIT 1
            """
        )
    if not rows:
        rows = adapter.query(
            "SELECT resolved_id, cluster_size, confidence_score FROM idr_out.identity_clusters_current LIMIT 1"
        )
    return str(rows[0]["resolved_id"]) if rows else None


def _sql_quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def generate_profile_verification_sql(
    adapter: Any,
    resolved_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Build deterministic warehouse SQL queries to verify a unified profile against source tables."""
    from .config import sql_to_config

    target_rid = _resolve_target_cluster_id(adapter, resolved_id)
    if not target_rid:
        return {
            "status": "NO_CLUSTERS",
            "summary": "No clusters found in idr_out.identity_clusters_current.",
            "queries": [],
        }

    members = adapter.query(
        "SELECT entity_key FROM idr_out.identity_resolved_membership_current WHERE resolved_id = ? ORDER BY entity_key ASC LIMIT 50",
        params=[target_rid],
    )
    edges = adapter.query(
        """
        SELECT e.left_entity_key, e.right_entity_key, e.identifier_type, e.identifier_value_norm
        FROM idr_out.identity_edges_current e
        JOIN idr_out.identity_resolved_membership_current m ON e.left_entity_key = m.entity_key
        WHERE m.resolved_id = ?
        LIMIT 50
        """,
        params=[target_rid],
    )

    dialect = str(getattr(adapter, "dialect", "") or "").lower()
    cast_type = "STRING" if dialect == "bigquery" else "VARCHAR"
    rid_lit = _sql_quote(target_rid)

    queries: List[Dict[str, str]] = [
        {
            "title": "Cluster membership & merge edges",
            "sql": (
                "SELECT m.resolved_id, m.entity_key, e.identifier_type, e.identifier_value_norm\n"
                "FROM idr_out.identity_resolved_membership_current m\n"
                "LEFT JOIN idr_out.identity_edges_current e\n"
                "  ON m.entity_key IN (e.left_entity_key, e.right_entity_key)\n"
                f"WHERE m.resolved_id = {rid_lit}\n"
                "ORDER BY m.entity_key, e.identifier_type;"
            ),
        }
    ]

    cfg = sql_to_config(adapter)
    src_by_id = {s["id"]: s for s in (cfg.get("sources") or []) if s.get("id")}
    keys_by_source: Dict[str, List[str]] = {}
    for m in members:
        ek = str(m["entity_key"])
        if ":" not in ek:
            continue
        sid, raw_key = ek.split(":", 1)
        keys_by_source.setdefault(sid, []).append(raw_key)

    union_branches: List[str] = []
    for sid, raw_keys in keys_by_source.items():
        src = src_by_id.get(sid)
        if not src:
            continue
        fqn = src["table"]
        key_expr = src["entity_key"]
        sid_prefix = _sql_quote(f"{sid}:")
        sid_lit = _sql_quote(sid)
        for ident in src.get("identifiers") or []:
            id_type = ident.get("type")
            id_expr = ident.get("expr") or ident.get("column")
            if not id_type or not id_expr:
                continue
            union_branches.append(
                f"SELECT\n"
                f"  {sid_lit} AS source_id,\n"
                f"  CONCAT({sid_prefix}, CAST(({key_expr}) AS {cast_type})) AS entity_key,\n"
                f"  {_sql_quote(id_type)} AS identifier_type,\n"
                f"  CAST(({id_expr}) AS {cast_type}) AS identifier_value\n"
                f"FROM {fqn}\n"
                f"WHERE CONCAT({sid_prefix}, CAST(({key_expr}) AS {cast_type})) IN (SELECT entity_key FROM members)"
            )

        id_cols = [
            i.get("expr") or i.get("column")
            for i in (src.get("identifiers") or [])
            if i.get("expr") or i.get("column")
        ]
        select_cols = ", ".join(dict.fromkeys(id_cols))
        proj = f", {select_cols}" if select_cols else ""
        in_list = ", ".join(_sql_quote(k) for k in raw_keys[:25])
        queries.append(
            {
                "title": f"Source verification: {sid} ({fqn})",
                "sql": (
                    f"SELECT CAST(({key_expr}) AS {cast_type}) AS entity_id{proj}\n"
                    f"FROM {fqn}\n"
                    f"WHERE CAST(({key_expr}) AS {cast_type}) IN ({in_list});"
                ),
            }
        )

    if union_branches:
        queries.insert(
            1,
            {
                "title": "All identifier values across all sources (UNION ALL)",
                "sql": (
                    "WITH members AS (\n"
                    "  SELECT entity_key\n"
                    "  FROM idr_out.identity_resolved_membership_current\n"
                    f"  WHERE resolved_id = {rid_lit}\n"
                    ")\n"
                    + "\nUNION ALL\n".join(union_branches)
                    + "\nORDER BY identifier_type, source_id, entity_key;"
                ),
            },
        )

    try:
        if adapter.table_exists("idr_out.golden_profiles"):
            queries.append(
                {
                    "title": "Unified golden profile",
                    "sql": f"SELECT * FROM idr_out.golden_profiles WHERE resolved_id = {rid_lit};",
                }
            )
    except Exception:
        pass

    merge_types = sorted({str(e["identifier_type"]) for e in edges if e.get("identifier_type")})
    return _sanitize_json(
        {
            "status": "OK" if members else "NOT_FOUND",
            "resolved_id": target_rid,
            "dialect": dialect or "sql",
            "member_count": len(members),
            "edge_count": len(edges),
            "merge_identifier_types": merge_types,
            "sources_matched": list(keys_by_source.keys()),
            "queries": queries,
        }
    )


def validate_unified_profile(
    adapter: Any,
    resolved_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Sample a real multi-entity unified profile and cross-check its edges against raw source tables."""
    from .config import sql_to_config

    target_rid = _resolve_target_cluster_id(adapter, resolved_id)
    if not target_rid:
        return {"status": "NO_CLUSTERS", "summary": "No clusters found in idr_out.identity_clusters_current."}

    members = adapter.query(
        "SELECT entity_key FROM idr_out.identity_resolved_membership_current WHERE resolved_id = ? ORDER BY entity_key ASC LIMIT 15",
        params=[target_rid],
    )
    edges = adapter.query(
        """
        SELECT e.left_entity_key, e.right_entity_key, e.identifier_type, e.identifier_value_norm
        FROM idr_out.identity_edges_current e
        JOIN idr_out.identity_resolved_membership_current m ON e.left_entity_key = m.entity_key
        WHERE m.resolved_id = ?
        LIMIT 25
        """,
        params=[target_rid],
    )

    golden_profile: Optional[Dict[str, Any]] = None
    try:
        if adapter.table_exists("idr_out.golden_profiles"):
            gp_rows = adapter.query(
                "SELECT * FROM idr_out.golden_profiles WHERE resolved_id = ? LIMIT 1",
                params=[target_rid],
            )
            if gp_rows:
                golden_profile = gp_rows[0]
    except Exception:
        golden_profile = None

    # Cross-check member keys against actual configured source tables
    cfg = sql_to_config(adapter)
    src_by_id = {s["id"]: s for s in (cfg.get("sources") or []) if s.get("id")}
    source_checks: List[Dict[str, Any]] = []
    source_rows_found = 0
    for m in members[:6]:
        ek = str(m["entity_key"])
        if ":" not in ek:
            continue
        sid, raw_key = ek.split(":", 1)
        src = src_by_id.get(sid)
        if not src:
            continue
        fqn = src["table"]
        key_expr = src["entity_key"]
        id_cols = [i.get("expr") or i.get("column") for i in (src.get("identifiers") or []) if i.get("expr") or i.get("column")]
        select_cols = ", ".join(dict.fromkeys(id_cols[:4]))
        proj = f", {select_cols}" if select_cols else ""
        try:
            srows = adapter.query(
                f"SELECT CAST(({key_expr}) AS VARCHAR) AS src_key{proj} FROM {fqn} WHERE CAST(({key_expr}) AS VARCHAR) = ? LIMIT 1",
                params=[raw_key],
            )
        except Exception:
            try:
                srows = adapter.query(
                    f"SELECT CAST(({key_expr}) AS STRING) AS src_key{proj} FROM {fqn} WHERE CAST(({key_expr}) AS STRING) = ? LIMIT 1",
                    params=[raw_key],
                )
            except Exception:
                srows = []
        found = bool(srows)
        if found:
            source_rows_found += 1
        source_checks.append(
            {
                "entity_key": ek,
                "source_table": fqn,
                "found_in_source": found,
                "source_identifiers": srows[0] if srows else None,
            }
        )

    status = "VERIFIED" if members and source_rows_found == len(source_checks) and len(source_checks) > 0 else "PARTIAL"
    return _sanitize_json(
        {
            "status": status,
            "resolved_id": target_rid,
            "member_count": len(members),
            "edge_count": len(edges),
            "has_golden_profile": golden_profile is not None,
            "source_rows_checked": len(source_checks),
            "source_rows_matched": source_rows_found,
            "edges": edges[:10],
            "source_checks": source_checks,
            "golden_profile": golden_profile,
        }
    )


def _extract_explicit_resolved_id(message: str, candidate: Any = None) -> Optional[str]:
    """Return a clean resolved_id if explicitly provided, ignoring hallucinated English phrases."""
    if isinstance(candidate, str):
        c = candidate.strip().strip("`'\"")
        if c and " " not in c and c.lower() not in {"null", "none", "unified_profile", "profile"}:
            return c
    m = re.search(r"\b([A-Za-z0-9_]+:[A-Za-z0-9_.-]+)\b", message or "")
    return m.group(1) if m else None


def _fallback_classify_copilot_intent(message: str) -> Dict[str, Any]:
    msg = (message or "").lower()
    wants_sql = bool(re.search(r"\b(sql|query|queries)\b", msg))
    about_profile = any(k in msg for k in ("validate", "verify", "unified profile", "resolved", "merge", "source data", "source table"))
    if wants_sql and about_profile:
        return {"intent": "profile_verification_sql", "resolved_id": _extract_explicit_resolved_id(message)}
    if wants_sql or any(k in msg for k in ("how to ", "how do i ")):
        return {"intent": "general_qa"}
    if any(k in msg for k in ("validate", "verify", "check profile", "unified profile", "source data")):
        return {"intent": "validate_profile", "resolved_id": _extract_explicit_resolved_id(message)}
    if any(k in msg for k in ("anomal", "drift", "toxic", "hub", "skipped", "why slow", "regression")):
        return {"intent": "diagnose_anomalies"}
    if any(k in msg for k in ("draft lens", "create lens", "new lens", "author lens")):
        return {"intent": "draft_lens", "goal": message}
    if any(k in msg for k in ("run idr", "run full", "run incr", "incremental", "full load", "trigger run", "execute idr")):
        mode = "FULL" if "full" in msg else "INCR"
        return {"intent": "propose_run", "mode": mode, "dry_run": "dry" in msg, "lens": None}
    if any(k in msg for k in ("runtime", "run time", "how long", "duration", "slower", "faster", "changed", "recent days", "today")):
        return {"intent": "get_runtime_trend"}
    if "diff" in msg or "compare lens" in msg:
        return {"intent": "diff_lenses", "lens_a": None, "lens_b": None}
    if "doctor" in msg or "health" in msg or "diagnostic" in msg:
        return {"intent": "run_doctor"}
    return {"intent": "general_qa"}


def copilot_chat(
    adapter: Any,
    message: str,
    history: Optional[List[Dict[str, str]]] = None,
    client: Optional[LLMClient] = None,
) -> Dict[str, Any]:
    """Route natural-language operational requests to safe warehouse tools or confirmation cards."""
    client = client or LLMClient()

    router_prompt = (
        "Classify the user's SQL Identity Resolution request into JSON.\n"
        "Allowed intents:\n"
        '1. {"intent": "validate_profile", "resolved_id": string|null}  -- Run a live validation check on a profile. Set resolved_id ONLY if an explicit ID (like src:123) is given; otherwise null.\n'
        '2. {"intent": "profile_verification_sql", "resolved_id": string|null}  -- User asks for SQL queries to verify/validate a unified profile or resolved_id against source tables.\n'
        '3. {"intent": "diagnose_anomalies"}\n'
        '4. {"intent": "draft_lens", "goal": string}\n'
        '5. {"intent": "propose_run", "mode": "INCR"|"FULL", "dry_run": true|false, "lens": string|null}\n'
        '6. {"intent": "get_runtime_trend"}\n'
        '7. {"intent": "diff_lenses", "lens_a": string|null, "lens_b": string|null}\n'
        '8. {"intent": "run_doctor"}\n'
        '9. {"intent": "general_qa"}\n'
        f"User message: {message}"
    )
    parsed_intent: Dict[str, Any] = {}
    try:
        raw = client.chat(
            [
                {"role": "system", "content": "Return valid JSON only."},
                {"role": "user", "content": router_prompt},
            ],
            temperature=0.0,
            json_mode=True,
        )
        candidate = json.loads(raw)
        if isinstance(candidate, dict) and candidate.get("intent"):
            parsed_intent = candidate
    except Exception:
        parsed_intent = {}

    if not parsed_intent:
        parsed_intent = _fallback_classify_copilot_intent(message)

    intent = str(parsed_intent.get("intent") or "general_qa")

    if intent == "validate_profile" and re.search(r"\b(sql|query|queries)\b", message, re.I):
        intent = "profile_verification_sql"

    if intent == "profile_verification_sql":
        rid = _extract_explicit_resolved_id(message, parsed_intent.get("resolved_id"))
        res = generate_profile_verification_sql(adapter, resolved_id=rid)
        if res.get("status") == "NO_CLUSTERS":
            ans = res.get("summary", "No clusters available to generate verification SQL.")
        elif res.get("status") == "NOT_FOUND":
            ans = f"No membership rows found for `{res['resolved_id']}`. Check that the `resolved_id` exists in `idr_out.identity_resolved_membership_current`."
        else:
            merge_via = ", ".join(res.get("merge_identifier_types") or []) or "N/A"
            blocks = [f"**{q['title']}**\n```sql\n{q['sql']}\n```" for q in (res.get("queries") or [])]
            ans = (
                f"Verification SQL for `{res['resolved_id']}` "
                f"({res['member_count']} linked entities, {res['edge_count']} edges; merged via `{merge_via}`):\n\n"
                + "\n\n".join(blocks)
            )
        return _sanitize_json(
            {
                "intent": "profile_verification_sql",
                "answer": ans,
                "proposed_action": None,
                "metrics": res,
                "pii_masked": client.config.effective_mask_pii,
                "provider": client.config.provider,
                "model": client.config.resolved_model(),
            }
        )

    if intent == "validate_profile":
        rid = _extract_explicit_resolved_id(message, parsed_intent.get("resolved_id"))
        res = validate_unified_profile(adapter, resolved_id=rid)
        safe_res = _apply_pii_mask(adapter, res, client)
        if safe_res.get("status") == "NO_CLUSTERS":
            ans = safe_res.get("summary", "No clusters available to validate.")
        else:
            ans = (
                f"**{safe_res['status']}** unified profile `{safe_res['resolved_id']}`: "
                f"{safe_res['member_count']} linked entities, {safe_res['edge_count']} edges, "
                f"{safe_res['source_rows_matched']}/{safe_res['source_rows_checked']} source rows verified in warehouse tables"
                + (" (golden profile present)." if safe_res.get("has_golden_profile") else " (no golden profile row).")
            )
        return _sanitize_json(
            {
                "intent": "validate_profile",
                "answer": ans,
                "proposed_action": None,
                "metrics": safe_res,
                "pii_masked": client.config.effective_mask_pii,
                "provider": client.config.provider,
                "model": client.config.resolved_model(),
            }
        )

    if intent == "diagnose_anomalies":
        report = diagnose_run_anomalies(adapter)
        safe_report = _apply_pii_mask(adapter, report, client)
        anomalies = safe_report.get("anomalies") or []
        summary_text = (
            "\n".join(f"- {a}" for a in anomalies)
            if anomalies
            else "No runtime regressions or oversized identifier hubs detected in recent runs."
        )
        return {
            "intent": "diagnose_anomalies",
            "answer": summary_text,
            "proposed_action": None,
            "metrics": safe_report,
            "pii_masked": client.config.effective_mask_pii,
            "provider": client.config.provider,
            "model": client.config.resolved_model(),
        }

    if intent == "draft_lens":
        goal = str(parsed_intent.get("goal") or message)
        drafted = draft_lens_from_goal(adapter, goal, client=client)
        lens_obj = drafted["lens"]
        return {
            "intent": "draft_lens",
            "answer": f"Drafted lens `{lens_obj.get('id')}`:\n```json\n{json.dumps(lens_obj, indent=2)}\n```",
            "proposed_action": None,
            "metrics": drafted,
            "pii_masked": client.config.effective_mask_pii,
            "provider": client.config.provider,
            "model": client.config.resolved_model(),
        }

    if intent == "propose_run":
        mode = "FULL" if str(parsed_intent.get("mode", "")).upper() == "FULL" else "INCR"
        dry_run = bool(parsed_intent.get("dry_run", False))
        lens = parsed_intent.get("lens") or None
        label = f"Run IDR ({mode})" + (f" · lens: {lens}" if lens else "") + (" [Dry Run]" if dry_run else "")
        return {
            "intent": "propose_run",
            "answer": (
                f"Ready to execute an **{mode}** identity resolution run"
                + (f" for lens `{lens}`" if lens else "")
                + ". Confirm below to start the pipeline."
            ),
            "proposed_action": {
                "type": "run_pipeline",
                "label": label,
                "mode": mode,
                "dry_run": dry_run,
                "lens": lens,
            },
            "metrics": None,
            "pii_masked": client.config.effective_mask_pii,
            "provider": client.config.provider,
            "model": client.config.resolved_model(),
        }

    if intent == "get_runtime_trend":
        trend = get_run_runtime_trend(adapter)
        safe_trend = _apply_pii_mask(adapter, trend, client)
        try:
            answer = client.chat(
                [
                    {
                        "role": "system",
                        "content": (
                            "Summarize the IDR runtime telemetry concisely using ONLY the numbers provided. "
                            "Highlight today's runtime, recent average, trend delta, and slowest stages."
                        ),
                    },
                    {
                        "role": "user",
                        "content": f"Telemetry:\n{json.dumps(safe_trend, default=str)}\n\nQuestion: {message}",
                    },
                ]
            ).strip()
        except Exception:
            latest = trend.get("latest_run") or {}
            answer = (
                f"Latest run `{latest.get('run_id', 'n/a')}` ({latest.get('run_mode', 'n/a')}) took "
                f"`{latest.get('duration_seconds', 'n/a')}s` (average across {trend.get('total_runs_checked', 0)} runs: "
                f"`{trend.get('avg_duration_seconds', 'n/a')}s`)."
            )
        return {
            "intent": "get_runtime_trend",
            "answer": answer,
            "proposed_action": None,
            "metrics": trend,
            "pii_masked": client.config.effective_mask_pii,
            "provider": client.config.provider,
            "model": client.config.resolved_model(),
        }

    if intent == "diff_lenses":
        from .config import compute_lens_diff

        lens_a = parsed_intent.get("lens_a")
        lens_b = parsed_intent.get("lens_b")
        if not lens_a or not lens_b:
            try:
                lrows = adapter.query(
                    "SELECT lens_id FROM idr_meta.lens_definition WHERE is_active = TRUE ORDER BY lens_id ASC LIMIT 2"
                )
                if len(lrows) >= 2:
                    lens_a = lens_a or lrows[0]["lens_id"]
                    lens_b = lens_b or lrows[1]["lens_id"]
            except Exception:
                pass
        if lens_a and lens_b:
            try:
                diff = compute_lens_diff(adapter, str(lens_a), str(lens_b), limit=10)
                return {
                    "intent": "diff_lenses",
                    "answer": (
                        f"Compared `{diff['lens_a']}` ({diff['lens_a_clusters']} clusters) vs "
                        f"`{diff['lens_b']}` ({diff['lens_b_clusters']} clusters) across "
                        f"{diff['entities_compared']} overlapping entities."
                    ),
                    "proposed_action": None,
                    "metrics": diff,
                    "pii_masked": client.config.effective_mask_pii,
                    "provider": client.config.provider,
                    "model": client.config.resolved_model(),
                }
            except Exception as exc:
                return {
                    "intent": "diff_lenses",
                    "answer": f"Could not compare lenses: {exc}",
                    "proposed_action": None,
                    "metrics": None,
                    "pii_masked": client.config.effective_mask_pii,
                    "provider": client.config.provider,
                    "model": client.config.resolved_model(),
                }

    if intent == "run_doctor":
        try:
            from .doctor import IDRDoctor

            report = IDRDoctor(adapter).run_all()
            checks = [
                {"name": c.name, "status": c.status.value, "message": c.message}
                for c in getattr(report, "checks", [])
            ]
            return {
                "intent": "run_doctor",
                "answer": f"Completed {len(checks)} warehouse readiness checks.",
                "proposed_action": None,
                "metrics": {"checks": checks},
                "pii_masked": client.config.effective_mask_pii,
                "provider": client.config.provider,
                "model": client.config.resolved_model(),
            }
        except Exception as exc:
            return {
                "intent": "run_doctor",
                "answer": f"Doctor diagnostics failed: {exc}",
                "proposed_action": None,
                "metrics": None,
                "pii_masked": client.config.effective_mask_pii,
                "provider": client.config.provider,
                "model": client.config.resolved_model(),
            }

    # Fallback: general Q&A over identity graph & telemetry
    qa = ask_identity_graph(adapter, message, client=client)
    return {
        "intent": "general_qa",
        "answer": qa["answer"],
        "proposed_action": None,
        "metrics": None,
        "pii_masked": qa["pii_masked"],
        "provider": qa["provider"],
        "model": qa["model"],
    }

