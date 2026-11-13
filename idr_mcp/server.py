import os
import secrets
import sys
import time
from typing import Any, Dict, Optional

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:

    class FastMCP:  # type: ignore[no-redef]
        """Lightweight fallback when 'mcp' package is not installed."""

        def __init__(self, name: str):
            self.name = name

        def tool(self):
            def _decorator(fn):
                return fn

            return _decorator

        def resource(self, _uri: str):
            def _decorator(fn):
                return fn

            return _decorator

        def prompt(self):
            def _decorator(fn):
                return fn

            return _decorator

        def run(self, **_kwargs):
            raise RuntimeError("The 'mcp' package is required to run the MCP server.")

from idr_core.connection_manager import ConnectionManager, ConnectionState

# Initialize FastMCP Server
mcp = FastMCP("idr-mcp")

# Short-lived confirmation tokens for two-step destructive tool guardrails
_CONFIRMATION_TOKENS: Dict[str, Dict[str, Any]] = {}
_CONFIRMATION_TTL_SECONDS = 300


def get_mcp_http_app():
    """Return FastMCP's Streamable HTTP ASGI app when ``mcp`` is installed, else None."""
    if hasattr(mcp, "streamable_http_app"):
        return mcp.streamable_http_app()
    return None


def _issue_confirmation_token(action_key: str, summary: Dict[str, Any]) -> str:
    """Issue a short-lived single-use confirmation token bound to ``action_key``."""
    now = time.time()
    # Prune expired tokens lazily
    expired = [tok for tok, meta in _CONFIRMATION_TOKENS.items() if meta["expires_at"] < now]
    for tok in expired:
        _CONFIRMATION_TOKENS.pop(tok, None)

    token = f"idr_conf_{secrets.token_urlsafe(16)}"
    _CONFIRMATION_TOKENS[token] = {
        "action_key": action_key,
        "summary": summary,
        "expires_at": now + _CONFIRMATION_TTL_SECONDS,
    }
    return token


def _consume_confirmation_token(token: Optional[str], expected_action_key: str) -> Optional[str]:
    """Validate and consume a confirmation token. Returns error message or None if valid."""
    if not token:
        return "Missing confirmation_token. Run with dry_run=True first to preview blast radius and obtain a confirmation_token."
    meta = _CONFIRMATION_TOKENS.get(token)
    if not meta:
        return "Invalid or already-used confirmation_token. Run with dry_run=True to generate a fresh token."
    if time.time() > meta["expires_at"]:
        _CONFIRMATION_TOKENS.pop(token, None)
        return "confirmation_token expired. Run with dry_run=True to generate a fresh token."
    if meta["action_key"] != expected_action_key:
        return "confirmation_token does not match the target parameters of this call."
    _CONFIRMATION_TOKENS.pop(token, None)
    return None


# --- Helpers ---


def get_adapter():
    """Get the active database adapter from IDR Core or FastAPI IDRManager, auto-recovering if possible."""
    manager = ConnectionManager.instance()
    adapter = manager.get_adapter()
    if not adapter:
        try:
            from idr_api.dependencies import get_manager

            api_adapter = get_manager().get_any_adapter()
            if api_adapter is not None:
                return api_adapter
        except Exception:
            pass
        connect_from_env()
        adapter = manager.get_adapter()

    if not adapter:
        raise RuntimeError(
            "IDR Database not connected (state: DISCONNECTED). "
            "Please configure warehouse environment variables (e.g. IDR_PLATFORM, IDR_DB_PATH) "
            "or connect via the IDR Web UI."
        )
    return adapter


def should_mask_pii() -> bool:
    """Check PII access level from environment. Defaults to MASKED."""
    return os.environ.get("IDR_PII_ACCESS", "").lower() != "full"


def mask_value(value: Any) -> Any:
    """Mask PII value if masking is enabled."""
    if not should_mask_pii():
        return value

    if not value or not isinstance(value, str):
        return value
    if len(value) < 4:
        return "*" * len(value)
    return f"{value[:2]}***{value[-2:]}"


# --- Tools ---


@mcp.tool()
def get_system_status() -> Dict[str, Any]:
    """
    Get current IDR lifecycle state, warehouse connectivity, and recommended next action.
    Lifecycle states: DISCONNECTED, CONNECTED_UNINITIALIZED, CONNECTED_UNCONFIGURED, CONFIGURED_READY, RESOLVED.
    """
    manager = ConnectionManager.instance()
    state = manager.get_state()
    adapter = manager.get_adapter()
    platform = manager._connection_params.get("platform")

    is_configured = False
    has_runs = False
    cluster_count = 0
    suggested_action = "connect_warehouse"

    if state in (ConnectionState.CONNECTED_READY, ConnectionState.CONNECTED_UNINITIALIZED):
        try:
            if adapter and adapter.table_exists("idr_meta.source_table"):
                cnt = adapter.query_one("SELECT COUNT(*) FROM idr_meta.source_table")
                is_configured = (cnt or 0) > 0

            if adapter and adapter.table_exists("idr_out.identity_clusters_current"):
                cluster_count = adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_clusters_current") or 0
                has_runs = cluster_count > 0

            if has_runs:
                state_str = "RESOLVED"
                suggested_action = "resolve_customer_360"
            elif is_configured:
                state_str = "CONFIGURED_READY"
                suggested_action = "run_identity_resolution"
            else:
                state_str = "CONNECTED_UNCONFIGURED"
                suggested_action = "discover_and_suggest_rules"
        except Exception:
            state_str = state.value
    else:
        state_str = state.value

    return {
        "status": "ok",
        "lifecycle_state": state_str,
        "platform": platform,
        "is_configured": is_configured,
        "has_runs": has_runs,
        "cluster_count": cluster_count,
        "suggested_next_action": suggested_action,
        "pii_access_level": "masked" if should_mask_pii() else "full",
    }


@mcp.tool()
def get_cluster(
    resolved_id: str,
    include_edges: bool = False,
    include_entities: bool = False,
    max_entities: int = 50,
    max_edges: int = 50,
) -> Dict[str, Any]:
    """
    Get details for a resolved identity cluster with context-window-safe bounds.
    Returns summary counts and ``truncated: true`` when members or edges exceed limits.
    """
    adapter = get_adapter()

    # Cluster Info
    sql = "SELECT * FROM idr_out.identity_clusters_current WHERE resolved_id = ?"
    clusters = adapter.query(sql, params=[resolved_id])

    if not clusters:
        return {
            "error": f"Cluster {resolved_id} not found",
            "resolved_id": resolved_id,
            "pii_access_level": "masked" if should_mask_pii() else "full",
        }

    result = clusters[0]
    result["pii_access_level"] = "masked" if should_mask_pii() else "full"

    ent_limit = max(1, min(int(max_entities), 500))
    edge_limit = max(1, min(int(max_edges), 500))
    truncated = False

    if include_entities:
        total_ent = adapter.query_one(
            "SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current WHERE resolved_id = ?",
            params=[resolved_id],
        ) or 0
        sql_ent = (
            "SELECT * FROM idr_out.identity_resolved_membership_current "
            f"WHERE resolved_id = ? LIMIT {ent_limit}"
        )
        entities = adapter.query(sql_ent, params=[resolved_id])
        result["entities"] = entities
        result["total_entities"] = int(total_ent)
        if int(total_ent) > len(entities):
            truncated = True

    if include_edges:
        total_edges = adapter.query_one(
            """
            SELECT COUNT(*) FROM idr_out.identity_edges_current
            WHERE left_entity_key IN (SELECT entity_key FROM idr_out.identity_resolved_membership_current WHERE resolved_id = ?)
               OR right_entity_key IN (SELECT entity_key FROM idr_out.identity_resolved_membership_current WHERE resolved_id = ?)
            """,
            params=[resolved_id, resolved_id],
        ) or 0
        sql_edges = f"""
            SELECT * FROM idr_out.identity_edges_current
            WHERE left_entity_key IN (SELECT entity_key FROM idr_out.identity_resolved_membership_current WHERE resolved_id = ?)
               OR right_entity_key IN (SELECT entity_key FROM idr_out.identity_resolved_membership_current WHERE resolved_id = ?)
            LIMIT {edge_limit}
        """
        # Mask identifier_value_norm in edges if needed
        edges = adapter.query(sql_edges, params=[resolved_id, resolved_id])
        distinct_types = sorted(
            {str(e.get("identifier_type")) for e in edges if e.get("identifier_type")}
        )
        if should_mask_pii():
            for e in edges:
                if "identifier_value_norm" in e:
                    e["identifier_value_norm"] = mask_value(e["identifier_value_norm"])
        result["edges"] = edges
        result["total_edges"] = int(total_edges)
        result["distinct_identifier_types"] = distinct_types
        if int(total_edges) > len(edges):
            truncated = True

    result["truncated"] = truncated
    return result


@mcp.tool()
def get_golden_profile(resolved_id: str) -> Dict[str, Any]:
    """
    Retrieve the resolved "Golden Profile" for a cluster (e.g. name, email, phone).
    This reads from the generated `golden_profiles` table.
    """
    adapter = get_adapter()

    try:
        sql = "SELECT * FROM idr_out.golden_profiles WHERE resolved_id = ?"
        profiles = adapter.query(sql, params=[resolved_id])

        if not profiles:
            return {
                "error": f"Golden profile for {resolved_id} not found",
                "resolved_id": resolved_id,
            }

        profile = profiles[0]
        # Mask attributes (preserve system metadata keys)
        system_keys = {"resolved_id", "run_id", "updated_at", "updated_ts"}
        attributes = {}
        for k, v in profile.items():
            if k not in system_keys:
                attributes[k] = mask_value(v)
            elif k == "run_id":
                attributes[k] = v

        return {
            "resolved_id": resolved_id,
            "run_id": profile.get("run_id"),
            "attributes": attributes,
            "pii_access_level": "masked" if should_mask_pii() else "full",
        }
    except Exception as e:
        return {
            "error": f"Failed to query golden profiles: {str(e)}. Ensure the 'golden_profiles' model has been built."
        }


@mcp.tool()
def search_identifier(value: str, identifier_type: str = None, limit: int = 10) -> Dict[str, Any]:
    """
    Search for clusters matching an identifier value (partial match support).
    Searches across all resolved entities (including singletons) via identity_identifiers_current,
    with fallback to identity_edges_current.
    """
    adapter = get_adapter()
    limit = min(limit, 50)
    like_pattern = f"%{value}%"

    params_final = [like_pattern]
    type_filter_id = ""
    type_filter_edge = ""
    if identifier_type:
        type_filter_id = "AND LOWER(i.identifier_type) = LOWER(?)"
        type_filter_edge = "AND LOWER(e.identifier_type) = LOWER(?)"
        params_final.append(identifier_type)

    try:
        rows = []
        if adapter.table_exists("idr_out.identity_identifiers_current"):
            sql_id = f"""
                SELECT DISTINCT c.resolved_id, c.cluster_size
                FROM idr_out.identity_clusters_current c
                JOIN idr_out.identity_resolved_membership_current m ON c.resolved_id = m.resolved_id
                JOIN idr_out.identity_identifiers_current i ON m.entity_key = i.entity_key
                WHERE LOWER(i.identifier_value_norm) LIKE LOWER(?)
                {type_filter_id}
                LIMIT {limit}
            """
            rows = adapter.query(sql_id, params=params_final)

        if not rows:
            sql_edge = f"""
                SELECT DISTINCT c.resolved_id, c.cluster_size
                FROM idr_out.identity_clusters_current c
                JOIN idr_out.identity_resolved_membership_current m ON c.resolved_id = m.resolved_id
                JOIN idr_out.identity_edges_current e ON (m.entity_key = e.left_entity_key OR m.entity_key = e.right_entity_key)
                WHERE LOWER(e.identifier_value_norm) LIKE LOWER(?)
                {type_filter_edge}
                LIMIT {limit}
            """
            rows = adapter.query(sql_edge, params=params_final)

        return {"matches": rows, "pii_access_level": "masked" if should_mask_pii() else "full"}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def list_edges_for_cluster(resolved_id: str, limit: int = 50) -> Dict[str, Any]:
    """List edges within a specific cluster."""
    adapter = get_adapter()
    limit = min(limit, 200)
    try:
        sql = f"""
            SELECT * FROM idr_out.identity_edges_current
            WHERE left_entity_key IN (
                SELECT entity_key FROM idr_out.identity_resolved_membership_current
                WHERE resolved_id = ?
            )
            OR right_entity_key IN (
                SELECT entity_key FROM idr_out.identity_resolved_membership_current
                WHERE resolved_id = ?
            )
            LIMIT {limit}
        """
        rows = adapter.query(sql, params=[resolved_id, resolved_id])

        if should_mask_pii():
            for r in rows:
                if "identifier_value_norm" in r:
                    r["identifier_value_norm"] = mask_value(r["identifier_value_norm"])

        return {"edges": rows}
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def explain_edge(entity_key_a: str, entity_key_b: str) -> Dict[str, Any]:
    """
    Get evidence explaining why two entities are connected.
    Queries `idr_out.edge_evidence` if present, otherwise `idr_out.identity_edges_current`
    (including transitive cluster edges if connected via intermediate nodes).
    """
    adapter = get_adapter()
    try:
        params = [entity_key_a, entity_key_b, entity_key_b, entity_key_a]
        evidence = []

        if adapter.table_exists("idr_out.edge_evidence"):
            sql = """
                SELECT * FROM idr_out.edge_evidence
                WHERE
                    (entity_key_a = ? AND entity_key_b = ?)
                    OR
                    (entity_key_a = ? AND entity_key_b = ?)
            """
            evidence = adapter.query(sql, params=params)

        if not evidence and adapter.table_exists("idr_out.identity_edges_current"):
            sql = """
                SELECT
                    e.left_entity_key AS entity_key_a,
                    e.right_entity_key AS entity_key_b,
                    r.rule_id,
                    e.identifier_type,
                    e.identifier_value_norm AS match_value,
                    e.first_seen_ts,
                    e.last_seen_ts,
                    e.run_id
                FROM idr_out.identity_edges_current e
                LEFT JOIN idr_meta.rule r
                    ON LOWER(e.identifier_type) = LOWER(r.identifier_type)
                WHERE
                    (e.left_entity_key = ? AND e.right_entity_key = ?)
                    OR
                    (e.left_entity_key = ? AND e.right_entity_key = ?)
            """
            evidence = adapter.query(sql, params=params)

        if should_mask_pii():
            for ev in evidence:
                if "match_value" in ev:
                    ev["match_value"] = mask_value(ev["match_value"])

        result: Dict[str, Any] = {
            "evidence": evidence,
            "connection_type": "direct" if evidence else "none",
            "pii_access_level": "masked" if should_mask_pii() else "full",
        }

        if not evidence and adapter.table_exists("idr_out.identity_resolved_membership_current"):
            memberships = adapter.query(
                "SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current WHERE entity_key IN (?, ?)",
                params=[entity_key_a, entity_key_b],
            )
            if len(memberships) == 2 and memberships[0]["resolved_id"] == memberships[1]["resolved_id"]:
                shared_cid = memberships[0]["resolved_id"]
                cluster_edges = list_edges_for_cluster(shared_cid, limit=50).get("edges", [])
                result["connection_type"] = "transitive"
                result["shared_resolved_id"] = shared_cid
                result["cluster_edges"] = cluster_edges

        return result
    except Exception as e:
        return {
            "error": f"Evidence lookup failed: {str(e)}"
        }


@mcp.tool()
def run_history(limit: int = 10) -> Dict[str, Any]:
    """Get recent IDR execution history."""
    adapter = get_adapter()
    limit = min(limit, 100)
    sql = f"SELECT * FROM idr_out.run_history ORDER BY started_at DESC LIMIT {limit}"
    history = adapter.query(sql)
    return {"runs": history}


@mcp.tool()
def latest_run() -> Dict[str, Any]:
    """Get the status of the most recent run."""
    adapter = get_adapter()
    history = adapter.query("SELECT * FROM idr_out.run_history ORDER BY started_at DESC LIMIT 1")
    if history:
        return history[0]
    return {"message": "No runs found"}


@mcp.tool()
def config_snapshot(config_hash: str = None) -> Dict[str, Any]:
    """
    Get the configuration snapshot for a specific run or the latest snapshot.
    Returns the saved sources, rules, and mappings JSON.
    """
    adapter = get_adapter()
    try:
        if config_hash:
            sql = "SELECT * FROM idr_out.config_snapshot WHERE config_hash = ?"
            rows = adapter.query(sql, params=[config_hash])
        else:
            sql = "SELECT * FROM idr_out.config_snapshot ORDER BY created_at DESC LIMIT 1"
            rows = adapter.query(sql)

        if not rows:
            return {"error": "No config snapshot found"}

        row = rows[0]
        return {
            "config_hash": row.get("config_hash"),
            "sources_json": row.get("sources_json"),
            "rules_json": row.get("rules_json"),
            "mappings_json": row.get("mappings_json"),
            "created_at": str(row.get("created_at", "")),
        }
    except Exception as e:
        return {"error": f"Failed to retrieve config snapshot: {str(e)}"}


@mcp.tool()
def list_rules() -> Dict[str, Any]:
    """List configured matching rules."""
    adapter = get_adapter()
    rules = adapter.query("SELECT * FROM idr_meta.rule WHERE is_active = true ORDER BY priority")
    return {"rules": rules}


@mcp.tool()
def list_sources() -> Dict[str, Any]:
    """List configured source tables."""
    adapter = get_adapter()
    sources = adapter.query("SELECT * FROM idr_meta.source_table WHERE is_active = true")
    return {"sources": sources}


@mcp.tool()
def list_skipped_groups(limit: int = 20) -> Dict[str, Any]:
    """List high-cardinality identifier groups skipped by scale guardrails (max_group_size)."""
    adapter = get_adapter()
    limit = min(limit, 100)
    try:
        rows = adapter.query(
            f"SELECT * FROM idr_out.skipped_identifier_groups ORDER BY group_size DESC LIMIT {limit}"
        )
        if should_mask_pii():
            for r in rows:
                if "identifier_value_norm" in r:
                    r["identifier_value_norm"] = mask_value(r["identifier_value_norm"])
        return {
            "skipped_groups": rows,
            "pii_access_level": "masked" if should_mask_pii() else "full",
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def get_metrics_summary() -> Dict[str, Any]:
    """Get high-level identity resolution graph metrics (clusters, entities, edges, avg confidence)."""
    adapter = get_adapter()
    try:
        total_clusters = int(
            adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_clusters_current") or 0
        )
        total_entities = int(
            adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current")
            or 0
        )
        total_edges = int(
            adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_edges_current") or 0
        )
        avg_conf = float(
            adapter.query_one(
                "SELECT COALESCE(AVG(confidence_score), 0.0) FROM idr_out.identity_clusters_current"
            )
            or 0.0
        )
        return {
            "total_clusters": total_clusters,
            "total_entities": total_entities,
            "total_edges": total_edges,
            "avg_confidence": round(avg_conf, 4),
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def explain_cluster(
    resolved_id: str,
    max_entities: int = 50,
    max_edges: int = 50,
) -> Dict[str, Any]:
    """
    Generate a context-window-safe explanation for a resolved cluster:
    cluster metadata (size, confidence_score, primary_reason, super_cluster_id),
    bounded member entities & edges with truncation metadata, and Golden Profile.
    """
    adapter = get_adapter()
    try:
        meta_rows = adapter.query(
            "SELECT * FROM idr_out.identity_clusters_current WHERE resolved_id = ?",
            params=[resolved_id],
        )
        cluster_data = get_cluster(
            resolved_id,
            include_edges=True,
            include_entities=True,
            max_entities=max_entities,
            max_edges=max_edges,
        )
        profile_data = get_golden_profile(resolved_id)
        return {
            "resolved_id": resolved_id,
            "cluster_metadata": meta_rows[0] if meta_rows else None,
            "members": cluster_data.get("entities", []),
            "edges": cluster_data.get("edges", []),
            "summary": {
                "total_entities": cluster_data.get("total_entities", len(cluster_data.get("entities", []))),
                "total_edges": cluster_data.get("total_edges", len(cluster_data.get("edges", []))),
                "distinct_identifier_types": cluster_data.get("distinct_identifier_types", []),
                "truncated": bool(cluster_data.get("truncated", False)),
            },
            "golden_profile": profile_data.get("attributes"),
            "pii_access_level": "masked" if should_mask_pii() else "full",
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def dry_run_pipeline(run_mode: str = "FULL", max_iters: int = 30) -> Dict[str, Any]:
    """
    Execute an IDR dry-run analysis to preview proposed entity additions and cluster moves
    without modifying production identity tables.
    """
    adapter = get_adapter()
    try:
        from dataclasses import asdict

        from idr_core.runner import IDRRunner, RunConfig

        runner = IDRRunner(adapter)
        cfg = RunConfig(run_mode=run_mode, max_iters=max_iters, dry_run=True)
        result = runner.run(cfg)
        payload = asdict(result)
        if adapter.table_exists("idr_out.dry_run_summary"):
            summary_rows = adapter.query(
                "SELECT new_entities, moved_entities, unchanged_entities, total_entities "
                "FROM idr_out.dry_run_summary WHERE run_id = ?",
                params=[result.run_id],
            )
            if summary_rows:
                payload["dry_run_summary"] = summary_rows[0]
        return payload
    except Exception as e:
        return {"error": str(e)}


# --- Agentic Identity Tools (Direction 1 & Direction 2) ---


@mcp.tool()
def discover_and_suggest_rules(
    table_fqn: str = None,
    schema: str = None,
    sample_size: int = 500,
    max_tables: int = None,
    output_file: str = None,
) -> Dict[str, Any]:
    """
    Profile warehouse tables, detect PII and identity attributes using statistical profiling,
    and generate an automated, production-ready declarative IDR configuration (Direction 1).

    Parameters:
        table_fqn: Optional fully-qualified table name (e.g. 'ecommerce_demo.customers').
        schema: Optional schema name (e.g. 'ecommerce_demo') to discover tables in.
        sample_size: Maximum sample rows for profile analysis (default: 500).
        max_tables: Optional maximum tables to profile (default: None = all user tables).
        output_file: Optional file path to persist the generated configuration YAML/JSON.
    """
    try:
        adapter = get_adapter()
    except Exception as e:
        return {
            "status": "ERROR",
            "error": str(e),
            "hint": "Please call mcp.connect_from_env() or configure connection environment variables (IDR_PLATFORM, etc.).",
        }

    sample_size = min(max(sample_size, 50), 5000)

    try:
        from idr_core.profiler import IDRDataProfiler, is_system_table

        candidate_tables = []
        if table_fqn:
            candidate_tables = [table_fqn]
        else:
            all_tables = adapter.list_tables(schema=schema)
            # Filter out system and framework schemas dynamically across all catalogs
            candidate_tables = [
                t for t in all_tables
                if not is_system_table(t)
            ]
            if max_tables and max_tables > 0:
                candidate_tables = candidate_tables[:max_tables]

        if not candidate_tables:
            available_schemas = []
            if hasattr(adapter, "list_schemas"):
                try:
                    available_schemas = adapter.list_schemas()
                except Exception:
                    pass

            return {
                "status": "NO_TABLES_FOUND",
                "tables_profiled": 0,
                "detected_identifiers": [],
                "detected_attributes": [],
                "suggested_config": {"sources": [], "rules": [], "survivorship": [], "exclusions": []},
                "error": f"No user data tables found to profile in schema '{schema or 'default'}'.",
                "available_schemas": available_schemas,
                "active_catalog": getattr(adapter, "catalog", None),
                "hint": f"Available schemas found in Databricks: {available_schemas}. Please pass schema='<name>' using one of these schemas, or specify table_fqn='schema.table'.",
            }

        profiler = IDRDataProfiler(adapter)
        result = profiler.profile_and_generate_config(candidate_tables, sample_size=sample_size)

        # Optional persistence to file (YAML/JSON)
        if output_file and result.get("suggested_config"):
            cfg = result["suggested_config"]
            import os

            target_path = os.path.abspath(output_file)
            parent_dir = os.path.dirname(target_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)

            if output_file.endswith(".json"):
                import json

                with open(target_path, "w", encoding="utf-8") as f:
                    json.dump(cfg, f, indent=2)
            else:
                try:
                    import yaml

                    with open(target_path, "w", encoding="utf-8") as f:
                        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
                except ImportError:
                    import json

                    with open(target_path, "w", encoding="utf-8") as f:
                        json.dump(cfg, f, indent=2)

            result["saved_to_file"] = target_path

        return result

    except Exception as e:
        return {"error": f"Failed to profile and suggest rules: {str(e)}"}


@mcp.tool()
def simulate_rule_impact(rule: Dict[str, Any], dialect: str = None) -> Dict[str, Any]:
    """
    Simulate the impact of adding or modifying a matching rule against existing warehouse data (Direction 1).
    Evaluates proposed edge volume, cluster merges, and detects supernode risks before production deployment.
    """
    adapter = get_adapter()
    match_keys = [str(k).lower() for k in rule.get("match_keys", [])]
    max_group_size = int(rule.get("max_group_size", 50))
    rule_id = rule.get("id", "simulated_rule")

    if not match_keys:
        return {"error": "Rule must specify 'match_keys' list (e.g. ['email'] or ['phone'])."}

    try:
        # Check if idr_out.identity_identifiers_current exists
        if not adapter.table_exists("idr_out.identity_identifiers_current"):
            return {
                "status": "INITIAL_STATE",
                "message": "identity_identifiers_current table not yet populated. Run an initial IDR run to enable simulation.",
                "rule_id": rule_id,
            }

        placeholders = ", ".join(["?"] * len(match_keys))
        sql_summary = f"""
            SELECT
                COUNT(*) as eligible_records,
                COUNT(DISTINCT identifier_value_norm) as distinct_identifiers,
                COUNT(DISTINCT entity_key) as eligible_entities
            FROM idr_out.identity_identifiers_current
            WHERE LOWER(identifier_type) IN ({placeholders})
              AND identifier_value_norm IS NOT NULL
              AND TRIM(identifier_value_norm) != ''
        """
        summary_rows = adapter.query(sql_summary, params=match_keys)
        summary = summary_rows[0] if summary_rows else {}

        # Analyze group size distribution
        sql_groups = f"""
            SELECT
                identifier_value_norm,
                COUNT(*) as group_size
            FROM idr_out.identity_identifiers_current
            WHERE LOWER(identifier_type) IN ({placeholders})
              AND identifier_value_norm IS NOT NULL
              AND TRIM(identifier_value_norm) != ''
            GROUP BY identifier_value_norm
            ORDER BY group_size DESC
            LIMIT 100
        """
        top_groups = adapter.query(sql_groups, params=match_keys)

        max_detected_group = top_groups[0]["group_size"] if top_groups else 0
        supernode_groups = [g for g in top_groups if g["group_size"] > max_group_size]

        # Calculate simulated anchor edges (N - 1 for groups where 1 < N <= max_group_size)
        simulated_edges = sum(
            g["group_size"] - 1 for g in top_groups
            if 1 < g["group_size"] <= max_group_size
        )

        risk_level = "LOW"
        risk_reasons = []
        if max_detected_group > 200:
            risk_level = "HIGH"
            risk_reasons.append(f"Detected giant group of size {max_detected_group} (likely placeholder/shared value)")
        elif supernode_groups:
            risk_level = "MEDIUM"
            risk_reasons.append(f"{len(supernode_groups)} group(s) exceed max_group_size ({max_group_size}) and will be skipped")

        masked_supernodes = []
        for g in supernode_groups[:5]:
            val = g["identifier_value_norm"]
            masked_supernodes.append({
                "identifier_value": mask_value(val),
                "group_size": g["group_size"],
            })

        return {
            "status": "SIMULATION_COMPLETE",
            "rule_id": rule_id,
            "match_keys": match_keys,
            "configured_max_group_size": max_group_size,
            "eligible_entities": summary.get("eligible_entities", 0),
            "distinct_identifiers": summary.get("distinct_identifiers", 0),
            "simulated_anchor_edges": simulated_edges,
            "max_detected_group_size": max_detected_group,
            "supernodes_flagged": len(supernode_groups),
            "sample_supernode_groups": masked_supernodes,
            "risk_level": risk_level,
            "risk_reasons": risk_reasons,
            "pii_access_level": "masked" if should_mask_pii() else "full",
        }

    except Exception as e:
        return {"error": f"Failed to simulate rule impact: {str(e)}"}


@mcp.tool()
def resolve_customer_360(identifier_value: str, identifier_type: str = None) -> Dict[str, Any]:
    """
    Single-call Customer 360 context grounding for AI agents and RAG pipelines (Direction 2).
    Given any identifier (email, phone, loyalty ID), resolves the person's canonical identity,
    golden profile attributes, all linked source accounts, and cross-channel identifier graph.
    """
    adapter = get_adapter()

    try:
        # 1. Search for matching cluster
        search_res = search_identifier(identifier_value, identifier_type=identifier_type, limit=5)
        matches = search_res.get("matches", [])
        if not matches:
            return {
                "found": False,
                "query": identifier_value,
                "message": f"No resolved identity cluster found for '{mask_value(identifier_value)}'.",
            }

        resolved_id = matches[0]["resolved_id"]

        # 2. Retrieve Cluster Details (with member entities and edges)
        cluster_data = get_cluster(resolved_id, include_edges=True, include_entities=True)

        # 3. Retrieve Golden Profile
        profile_data = get_golden_profile(resolved_id)
        golden_profile = profile_data.get("attributes") or {}

        # 4. Aggregate all known identifiers for this resolved person
        known_identifiers = []
        if adapter.table_exists("idr_out.identity_identifiers_current"):
            sql_ids = """
                SELECT DISTINCT i.identifier_type, i.identifier_value_norm
                FROM idr_out.identity_identifiers_current i
                JOIN idr_out.identity_resolved_membership_current m ON i.entity_key = m.entity_key
                WHERE m.resolved_id = ?
            """
            id_rows = adapter.query(sql_ids, params=[resolved_id])
            for r in id_rows:
                val = r["identifier_value_norm"]
                known_identifiers.append({
                    "type": r["identifier_type"],
                    "value": mask_value(val) if should_mask_pii() else val,
                })

        # 5. Extract linked source accounts
        entities = cluster_data.get("entities", [])
        linked_accounts = []
        for ent in entities:
            ekey = ent.get("entity_key", "")
            source_parts = ekey.split(":", 1)
            source_system = source_parts[0] if len(source_parts) == 2 else "unknown"
            source_id = source_parts[1] if len(source_parts) == 2 else ekey
            linked_accounts.append({
                "source_system": source_system,
                "source_id": source_id,
                "entity_key": ekey,
            })

        # 6. Assess cluster risk flags
        risk_flags = []
        cluster_size = cluster_data.get("cluster_size", len(entities))
        if cluster_size > 30:
            risk_flags.append("HIGH_CARDINALITY_CLUSTER")
        confidence_score = cluster_data.get("confidence_score", 1.0)
        if confidence_score and float(confidence_score) < 0.70:
            risk_flags.append("LOW_CONFIDENCE_CLUSTER")

        return {
            "found": True,
            "resolved_id": resolved_id,
            "cluster_size": cluster_size,
            "confidence_score": confidence_score,
            "super_cluster_id": cluster_data.get("super_cluster_id", resolved_id),
            "golden_profile": golden_profile,
            "linked_accounts": linked_accounts,
            "known_identifiers": known_identifiers,
            "risk_flags": risk_flags,
            "pii_access_level": "masked" if should_mask_pii() else "full",
        }

    except Exception as e:
        return {"error": f"Failed to resolve Customer 360 context: {str(e)}"}


@mcp.tool()
def export_dbt_seeds(
    output_dir: str = "dbt_idr/seeds",
    config_yaml: str = None,
) -> Dict[str, Any]:
    """
    Export IDR configuration (either from a provided YAML string or from the active
    warehouse's idr_meta tables) into the 6 standard `dbt_idr` seed CSV files.
    Bridges interactive agent rule discovery & simulation with governed dbt production runs.
    """
    try:
        from idr_core.config import config_to_dbt_seeds, sql_to_config, validate_config

        if config_yaml:
            import yaml

            cfg = yaml.safe_load(config_yaml)
            validate_config(cfg)
        else:
            adapter = get_adapter()
            cfg = sql_to_config(adapter)
            if not cfg.get("sources"):
                return {
                    "status": "ERROR",
                    "error": "No active sources found in idr_meta tables and no config_yaml provided.",
                    "hint": "Call apply_config(config_yaml=...) first or pass config_yaml directly to export_dbt_seeds.",
                }

        written = config_to_dbt_seeds(cfg, output_dir=output_dir)
        return {
            "status": "SUCCESS",
            "output_dir": os.path.abspath(output_dir),
            "files_written": written,
            "sources_count": len(cfg.get("sources", [])),
            "rules_count": len(cfg.get("rules", [])),
            "next_step": f"Run `dbt seed && dbt run && dbt test` inside your dbt project using seeds from {os.path.abspath(output_dir)}.",
        }
    except Exception as e:
        return {"status": "ERROR", "error": f"Failed to export dbt seeds: {str(e)}"}


@mcp.tool()
def forget_customer_identity(
    identifier_value: str = None,
    entity_key: str = None,
    resolved_id: str = None,
    dry_run: bool = True,
    confirmation_token: str = None,
    reason: str = "GDPR_CCPA_RTBF",
) -> Dict[str, Any]:
    """
    Preview or execute a GDPR / CCPA Right-to-be-Forgotten (RTBF) cascading purge.

    Safety guardrail: defaults to ``dry_run=True``, returning a blast-radius summary
    and a short-lived ``confirmation_token``. Pass ``dry_run=False`` together with that
    ``confirmation_token`` to execute the purge.
    """
    try:
        adapter = get_adapter()
        from idr_core.privacy import PrivacyManager

        action_key = f"forget:{identifier_value or ''}:{entity_key or ''}:{resolved_id or ''}"
        if not dry_run:
            err = _consume_confirmation_token(confirmation_token, action_key)
            if err:
                return {"status": "CONFIRMATION_REQUIRED", "error": err}

        pm = PrivacyManager(adapter)
        res = pm.forget(
            identifier_value=identifier_value,
            entity_key=entity_key,
            resolved_id=resolved_id,
            dry_run=dry_run,
            requested_by="mcp_agent",
            reason=reason,
        )
        if dry_run:
            summary = {
                "entities_purged": res.get("entities_purged", 0),
                "edges_purged": res.get("edges_purged", 0),
                "profiles_purged": res.get("profiles_purged", 0),
            }
            res["confirmation_token"] = _issue_confirmation_token(action_key, summary)
            res["confirmation_expires_in_seconds"] = _CONFIRMATION_TTL_SECONDS
            res["blast_radius_summary"] = (
                f"{summary['entities_purged']} entities, "
                f"{summary['edges_purged']} edges, "
                f"{summary['profiles_purged']} profiles"
            )
        return res
    except Exception as e:
        return {"status": "ERROR", "error": f"Failed to execute RTBF forget: {str(e)}"}


@mcp.tool()
def manage_identity_override(
    action: str,
    override_type: str = None,
    entity_key_a: str = None,
    entity_key_b: str = None,
    reason: str = "Manual stewardship override",
    dry_run: bool = False,
    confirmation_token: str = None,
) -> Dict[str, Any]:
    """
    Manage manual stewardship overrides in idr_meta.identity_override.
    Actions: 'list', 'add', 'remove'.
    Override types: 'FORCE_LINK', 'DO_NOT_LINK', 'ISOLATE'.
    Supports ``dry_run=True`` to preview impact and issue a ``confirmation_token``.
    """
    try:
        adapter = get_adapter()
        act = action.strip().lower()
        if act == "list":
            rows = adapter.query(
                "SELECT * FROM idr_meta.identity_override WHERE is_active = TRUE ORDER BY created_at DESC"
            )
            return {"status": "SUCCESS", "overrides": rows}

        if not override_type or not entity_key_a:
            return {"status": "ERROR", "error": "override_type and entity_key_a are required"}

        otype = override_type.strip().upper()
        if otype not in ("FORCE_LINK", "DO_NOT_LINK", "ISOLATE"):
            return {"status": "ERROR", "error": f"Invalid override_type '{otype}'"}

        ek_a = entity_key_a.strip().replace("'", "''")
        ek_b = (entity_key_b.strip() if entity_key_b else "__ISOLATE__").replace("'", "''")
        rsn = (reason or "").replace("'", "''")
        action_key = f"override:{act}:{otype}:{ek_a}:{ek_b}"

        if dry_run and act in ("add", "remove"):
            summary = {"action": act, "override_type": otype, "entity_key_a": ek_a, "entity_key_b": ek_b}
            tok = _issue_confirmation_token(action_key, summary)
            return {
                "status": "DRY_RUN",
                "action": act,
                "override_type": otype,
                "entity_key_a": ek_a,
                "entity_key_b": ek_b,
                "confirmation_token": tok,
                "confirmation_expires_in_seconds": _CONFIRMATION_TTL_SECONDS,
            }

        require_conf = os.environ.get("IDR_MCP_REQUIRE_CONFIRMATION", "").lower() in ("1", "true", "yes")
        if act in ("add", "remove") and (confirmation_token is not None or require_conf):
            err = _consume_confirmation_token(confirmation_token, action_key)
            if err:
                return {"status": "CONFIRMATION_REQUIRED", "error": err}

        if act == "remove":
            adapter.execute(f"""
                DELETE FROM idr_meta.identity_override
                WHERE UPPER(override_type) = '{otype}'
                  AND entity_key_a = '{ek_a}'
                  AND entity_key_b = '{ek_b}'
            """)
            return {"status": "SUCCESS", "action": "removed", "override_type": otype, "entity_key_a": ek_a, "entity_key_b": ek_b}

        if act == "add":
            from idr_core.adapters.base import get_dialect_config

            current_ts = get_dialect_config(adapter.dialect)["current_timestamp"]
            adapter.execute(f"""
                DELETE FROM idr_meta.identity_override
                WHERE UPPER(override_type) = '{otype}'
                  AND entity_key_a = '{ek_a}'
                  AND entity_key_b = '{ek_b}'
            """)
            adapter.execute(f"""
                INSERT INTO idr_meta.identity_override
                (override_type, entity_key_a, entity_key_b, reason, is_active, created_at)
                VALUES ('{otype}', '{ek_a}', '{ek_b}', '{rsn}', TRUE, {current_ts})
            """)
            return {"status": "SUCCESS", "action": "added", "override_type": otype, "entity_key_a": ek_a, "entity_key_b": ek_b}

        return {"status": "ERROR", "error": f"Unknown action '{action}'. Use 'list', 'add', or 'remove'."}
    except Exception as e:
        return {"status": "ERROR", "error": f"Failed to manage override: {str(e)}"}


@mcp.tool()
def get_triage_queue(max_confidence: float = 0.45, limit: int = 20) -> Dict[str, Any]:
    """
    Return the highest-risk, lowest-confidence resolved clusters (and any unlabeled
    evaluation pairs) for agent-driven quality triage and LLM-as-a-judge review.
    """
    adapter = get_adapter()
    safe_limit = max(1, min(int(limit), 100))
    threshold = float(max_confidence)
    try:
        clusters = []
        if adapter.table_exists("idr_out.identity_clusters_current"):
            clusters = adapter.query(
                f"""
                SELECT resolved_id, cluster_size, confidence_score, edge_diversity,
                       max_fanout, primary_reason, updated_at
                FROM idr_out.identity_clusters_current
                WHERE confidence_score <= ?
                ORDER BY confidence_score ASC, cluster_size DESC
                LIMIT {safe_limit}
                """,
                params=[threshold],
            )

        samples = []
        if adapter.table_exists("idr_out.evaluation_sample"):
            samples = adapter.query(
                f"""
                SELECT sample_id, stratum, resolved_id_a, resolved_id_b,
                       entity_key_a, entity_key_b, identifier_type, match_value,
                       confidence_score, cluster_size, human_label
                FROM idr_out.evaluation_sample
                WHERE human_label IS NULL OR TRIM(CAST(human_label AS VARCHAR)) = ''
                ORDER BY confidence_score ASC
                LIMIT {safe_limit}
                """
            )
            if should_mask_pii():
                for row in samples:
                    if row.get("match_value"):
                        row["match_value"] = mask_value(row["match_value"])

        return {
            "status": "SUCCESS",
            "max_confidence": threshold,
            "low_confidence_clusters": clusters,
            "pending_evaluation_samples": samples,
            "pii_access_level": "masked" if should_mask_pii() else "full",
        }
    except Exception as e:
        return {"status": "ERROR", "error": f"Failed to fetch triage queue: {str(e)}"}


@mcp.tool()
def submit_evaluation_label(
    sample_id: str, label: str, rationale: str = ""
) -> Dict[str, Any]:
    """
    Record an audit judgment ('MATCH', 'NON_MATCH', or 'UNCERTAIN') on a pair
    in idr_out.evaluation_sample for closed-loop quality evaluation.
    """
    adapter = get_adapter()
    norm_label = (label or "").strip().upper()
    if norm_label not in ("MATCH", "NON_MATCH", "UNCERTAIN"):
        return {
            "status": "ERROR",
            "error": f"Invalid label '{label}'. Must be MATCH, NON_MATCH, or UNCERTAIN.",
        }
    if not adapter.table_exists("idr_out.evaluation_sample"):
        return {
            "status": "ERROR",
            "error": "idr_out.evaluation_sample does not exist. Run 'idr evaluate' first.",
        }
    try:
        sid = sample_id.strip().replace("'", "''")
        adapter.execute(
            f"UPDATE idr_out.evaluation_sample SET human_label = '{norm_label}' WHERE sample_id = '{sid}'"
        )
        recommendation = None
        if norm_label == "NON_MATCH":
            recommendation = (
                "Consider adding a DO_NOT_LINK / ISOLATE override via manage_identity_override "
                "or excluding the shared identifier value."
            )
        return {
            "status": "SUCCESS",
            "sample_id": sample_id,
            "label": norm_label,
            "rationale": rationale,
            "recommended_next_step": recommendation,
        }
    except Exception as e:
        return {"status": "ERROR", "error": f"Failed to submit evaluation label: {str(e)}"}


# --- MCP Resources & Prompts (Self-Documenting Specs for Agents) ---


@mcp.resource("idr://spec/config-schema")
def resource_config_schema() -> str:
    """YAML/JSON configuration schema for sql-identity-oss."""
    return """# sql-identity-oss Configuration Schema (YAML or JSON)
sources:
  - id: customers                    # Unique logical source ID
    table: db.schema.customers       # Fully-qualified table name
    entity_type: PERSON
    entity_key: customer_id          # Primary row key expression
    watermark: updated_at            # Optional incremental timestamp column
    watermark_lookback: 60           # Optional lookback minutes
    identifiers:
      - type: email                  # Identifier type referenced by rules
        expr: email                  # Column or SQL expression
    attributes:
      - name: first_name             # Output golden profile attribute
        expr: first_name

use_default_exclusions: true

rules:
  - id: "104"
    type: EXACT
    match_keys: [email]
    priority: 1
    canonicalize: EMAIL_STANDARD     # EMAIL_STANDARD | EMAIL_STRIP_PLUS | PHONE_DIGITS | PHONE_US_10 | LOWERCASE | UPPERCASE | ALPHANUMERIC_UPPER | custom {val} SQL
    max_group_size: 50               # Optional circuit breaker

fuzzy_rules: []

survivorship:
  - attribute: first_name
    strategy: RECENCY                # RECENCY | PRIORITY | FREQUENCY | LONGEST
    source_priority: [customers]

exclusions:                          # Optional custom exclusions (EXACT | LIKE)
  - identifier_type: email
    pattern: "%@example.com"
    match_type: LIKE
    reason: "Test domain"

overrides:                           # Optional stewardship overrides (FORCE_LINK | DO_NOT_LINK | ISOLATE)
  - type: DO_NOT_LINK
    entity_key_a: "crm:101"
    entity_key_b: "crm:102"          # Omit entity_key_b for ISOLATE
    reason: "Shared household phone"
"""


@mcp.resource("idr://spec/cli-commands")
def resource_cli_commands() -> str:
    """Headless CLI command reference for sql-identity-oss."""
    return """idr init --platform <duckdb|snowflake|bigquery|databricks>
idr doctor --platform <platform>
idr bootstrap --platform <platform> --schema <schema> --output idr_config.yaml [--apply]
idr apply-config --platform <platform> --config idr_config.yaml
idr run --platform <platform> --mode <FULL|INCR> [--dry-run] [--strict]
idr evaluate --platform <platform>
idr privacy forget --platform <platform> --identifier <val> [--dry-run]
idr ui --platform <platform>
"""


@mcp.resource("idr://spec/canonicalizers")
def resource_canonicalizers() -> str:
    """Built-in canonicalizers and custom {val} SQL template rules."""
    return """Built-in canonicalizers:
- EMAIL_STANDARD: trim + lowercase email normalization
- EMAIL_STRIP_PLUS: strips +tag subaddressing before @ + lowercase
- PHONE_DIGITS: keeps digits only
- PHONE_US_10: digits only, strips leading US '1' to 10 digits
- LOWERCASE / UPPERCASE / TRIM: basic string normalization
- ALPHANUMERIC_UPPER: keeps [A-Z0-9] uppercase
Custom SQL: pass any warehouse-native SQL expression with {val} placeholder.
"""


@mcp.prompt()
def model_config_from_tables(schema: str = "") -> str:
    """Step-by-step prompt guiding an agent to model an IDR config from existing warehouse tables."""
    target = f" in schema '{schema}'" if schema else ""
    return (
        f"Help model a sql-identity-oss configuration from existing warehouse tables{target}.\n"
        "1. Call get_system_status() to verify warehouse connectivity.\n"
        f"2. Call discover_and_suggest_rules(schema={schema!r}) to profile columns, keys, watermarks, and PII.\n"
        "3. Review suggested entity_key, watermark, identifiers, and attributes; drop pure lookup tables.\n"
        "4. Call simulate_rule_impact(...) on key identifiers (email, phone, customer_id) to check max_group_size.\n"
        "5. Read idr://spec/config-schema if needed, then present the final YAML/JSON config and run dry_run_pipeline()."
    )


# --- Entry Point ---


def connect_from_env():
    """Attempt to connect to database using environment variables."""
    manager = ConnectionManager.instance()

    def _get_env(key):
        val = os.environ.get(key)
        return val if val else None

    platform = _get_env("IDR_PLATFORM")
    if not platform:
        # Don't print to stdout as it breaks MCP stdio protocol
        print("IDR_PLATFORM not set. Waiting for manual connection...", file=sys.stderr)
        return

    try:
        print(f"Connecting to {platform}...", file=sys.stderr)

        if platform == "duckdb":
            import duckdb

            db_path = _get_env("IDR_DATABASE") or ":memory:"
            db_path = db_path.strip("'\"")
            read_only = (_get_env("IDR_READ_ONLY") or "").lower() in ("1", "true", "yes")
            if db_path == ":memory:":
                conn = duckdb.connect(db_path)
            else:
                try:
                    conn = duckdb.connect(db_path, read_only=read_only)
                except Exception:
                    conn = duckdb.connect(db_path, read_only=True)
            from idr_core.adapters.duckdb import DuckDBAdapter

            manager.set_adapter(DuckDBAdapter(conn), {"platform": "duckdb", "db_path": db_path})

        elif platform == "bigquery":
            from google.cloud import bigquery

            project = _get_env("IDR_PROJECT")
            client = bigquery.Client(project=project)
            from idr_core.adapters.bigquery import BigQueryAdapter

            manager.set_adapter(
                BigQueryAdapter(client, project), {"platform": "bigquery", "project": project}
            )

        elif platform == "snowflake":
            import snowflake.connector

            conn = snowflake.connector.connect(
                account=_get_env("SNOWFLAKE_ACCOUNT"),
                user=_get_env("SNOWFLAKE_USER"),
                password=_get_env("SNOWFLAKE_PASSWORD"),
                warehouse=_get_env("SNOWFLAKE_WAREHOUSE"),
                database=_get_env("SNOWFLAKE_DATABASE"),
                schema=_get_env("SNOWFLAKE_SCHEMA") or "PUBLIC",
            )
            from idr_core.adapters.snowflake import SnowflakeAdapter

            manager.set_adapter(SnowflakeAdapter(conn), {"platform": "snowflake"})

        elif platform == "databricks":
            from databricks import sql

            host = _get_env("DATABRICKS_HOST")
            if host:
                host = host.replace("https://", "").replace("http://", "").rstrip("/")
            http_path = _get_env("DATABRICKS_HTTP_PATH")
            token = _get_env("DATABRICKS_TOKEN")

            catalog = _get_env("DATABRICKS_CATALOG")
            connect_kwargs = {
                "server_hostname": host,
                "http_path": http_path,
                "access_token": token,
            }
            if catalog:
                connect_kwargs["catalog"] = catalog

            conn = sql.connect(**connect_kwargs)
            from idr_core.adapters.databricks import DatabricksAdapter

            manager.set_adapter(
                DatabricksAdapter(conn, catalog=catalog), {"platform": "databricks"}
            )

        print(f"✓ Connected to {platform}", file=sys.stderr)

    except Exception as e:
        print(f"⚠ Failed to connect to {platform}: {e}", file=sys.stderr)


def main():
    """CLI entrypoint for `idr-mcp` binary."""
    connect_from_env()
    transport = os.environ.get("IDR_MCP_TRANSPORT", "stdio").strip().lower()
    if transport in ("streamable-http", "http"):
        mcp.run(transport="streamable-http")
    else:
        mcp.run()


if __name__ == "__main__":
    main()
