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
import time
from typing import Any, Dict, List, Optional

from .llm import LLMClient, mask_pii_payload


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
    payload = mask_pii_payload(ctx) if client.config.effective_mask_pii else ctx

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

    payload = mask_pii_payload(enriched) if client.config.effective_mask_pii else enriched
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
            "SELECT run_id, run_mode, status, entities_processed, edges_created, clusters_impacted "
            "FROM idr_out.run_history ORDER BY started_at DESC LIMIT 5"
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

    safe_summary = mask_pii_payload(summary) if client.config.effective_mask_pii else summary
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
        "Available survivorship strategies: RECENCY, PRIORITY, PRIORITY_RECENCY, LATEST_FROM_SOURCE, "
        "FREQUENCY, ANY_TRUE, ALL_TRUE, EARLIEST, AGG_MAX, AGG_MIN, AGG_SUM, COLLECT_DISTINCT.\n"
        "Available canonicalizers: EMAIL_STANDARD, EMAIL_STRIP_PLUS, PHONE_US_10, PHONE_DIGITS, "
        "ALPHANUMERIC_UPPER, LOWERCASE, UPPERCASE, EXACT.\n\n"
        f"User Goal: {prompt_text}\n\n"
        f"Profiled Schema:\n{json.dumps(safe_profile, default=str)}"
    )
    raw = client.chat(
        [
            {"role": "system", "content": "Return valid JSON only representing an IDR config object."},
            {"role": "user", "content": prompt},
        ],
        json_mode=True,
    )
    config_dict = json.loads(raw)
    return {
        "pii_masked": client.config.effective_mask_pii,
        "provider": client.config.provider,
        "model": client.config.resolved_model(),
        "suggested_config": config_dict,
    }

