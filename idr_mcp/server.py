import os
import sys
from typing import Any, Dict

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

        def run(self):
            raise RuntimeError("The 'mcp' package is required to run the MCP server.")

from idr_core.connection_manager import ConnectionManager

# Initialize FastMCP Server
mcp = FastMCP("idr-mcp")

# --- Helpers ---


def get_adapter():
    """Get the active database adapter from IDR Core."""
    manager = ConnectionManager.instance()
    adapter = manager.get_adapter()
    if not adapter:
        raise RuntimeError(
            "Database not connected. Please ensure IDR is connected via the host application or environment variables."
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
def get_cluster(
    resolved_id: str, include_edges: bool = False, include_entities: bool = False
) -> Dict[str, Any]:
    """
    Get details for a resolved identity cluster.
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

    if include_entities:
        # Membership table contains only system keys (entity_key, resolved_id) and timestamps.
        # These are not considered PII, so no masking is applied here.
        sql_ent = "SELECT * FROM idr_out.identity_resolved_membership_current WHERE resolved_id = ?"
        entities = adapter.query(sql_ent, params=[resolved_id])
        result["entities"] = entities

    if include_edges:
        sql_edges = """
            SELECT * FROM idr_out.identity_edges_current
            WHERE left_entity_key IN (SELECT entity_key FROM idr_out.identity_resolved_membership_current WHERE resolved_id = ?)
               OR right_entity_key IN (SELECT entity_key FROM idr_out.identity_resolved_membership_current WHERE resolved_id = ?)
        """
        # Mask identifier_value_norm in edges if needed
        edges = adapter.query(sql_edges, params=[resolved_id, resolved_id])
        if should_mask_pii():
            for e in edges:
                if "identifier_value_norm" in e:
                    e["identifier_value_norm"] = mask_value(e["identifier_value_norm"])
        result["edges"] = edges

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
def explain_cluster(resolved_id: str) -> Dict[str, Any]:
    """
    Generate a comprehensive explanation for a resolved cluster:
    cluster metadata (size, confidence_score, primary_reason, super_cluster_id),
    member entities, connecting edges with rules, and the unified Golden Profile.
    """
    adapter = get_adapter()
    try:
        meta_rows = adapter.query(
            "SELECT * FROM idr_out.identity_clusters_current WHERE resolved_id = ?",
            params=[resolved_id],
        )
        cluster_data = get_cluster(resolved_id, include_edges=True, include_entities=True)
        profile_data = get_golden_profile(resolved_id)
        return {
            "resolved_id": resolved_id,
            "cluster_metadata": meta_rows[0] if meta_rows else None,
            "members": cluster_data.get("entities", []),
            "edges": cluster_data.get("edges", []),
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
    table_fqn: str = None, schema: str = None, sample_size: int = 500
) -> Dict[str, Any]:
    """
    Profile warehouse tables, detect PII and identity attributes using semantic analysis,
    and generate an automated, production-ready declarative IDR configuration (Direction 1).

    Parameters:
        table_fqn: Optional fully-qualified table name (e.g. 'ecommerce_demo.customers').
        schema: Optional schema name (e.g. 'ecommerce_demo') to discover tables in.
        sample_size: Maximum sample rows for profile analysis (default: 500).
    """
    try:
        adapter = get_adapter()
    except Exception as e:
        return {
            "status": "ERROR",
            "error": str(e),
            "hint": "Please call mcp.connect_from_env() or configure connection environment variables (IDR_PLATFORM, etc.).",
        }

    sample_size = min(max(sample_size, 50), 2000)

    try:
        candidate_tables = []
        if table_fqn:
            candidate_tables = [table_fqn]
        else:
            all_tables = adapter.list_tables(schema=schema)
            # Filter out system and framework schemas
            candidate_tables = [
                t for t in all_tables
                if not (t.startswith("idr_meta.") or t.startswith("idr_work.") or t.startswith("idr_out."))
            ][:8]

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

        sources = []
        rules = []
        detected_id_types = set()
        detected_attrs = set()

        for fqn in candidate_tables:
            if hasattr(adapter, "get_table_columns"):
                cols = adapter.get_table_columns(fqn)
            elif hasattr(adapter, "get_columns"):
                cols = adapter.get_columns(fqn)
            else:
                continue

            if not cols:
                continue

            table_id = fqn.replace(".", "_")
            col_names = [c["name"] for c in cols]

            # 1. Identify primary entity key
            pk_candidates = [
                c for c in col_names
                if c.lower() in ("id", "customer_id", "user_id", "client_id", "entity_id", "account_id")
            ]
            entity_key_col = pk_candidates[0] if pk_candidates else col_names[0]

            # 2. Identify watermark column
            wm_candidates = [
                c for c in col_names
                if any(w in c.lower() for w in ("updated", "modified", "timestamp", "created", "event_time"))
            ]
            watermark_col = wm_candidates[0] if wm_candidates else None

            # 3. Classify identifiers and attributes
            table_identifiers = []
            table_attributes = []

            for col in col_names:
                c_lower = col.lower()
                if col == entity_key_col or col == watermark_col:
                    continue

                if any(k in c_lower for k in ("email", "mail")):
                    table_identifiers.append({"type": "email", "expr": col})
                    detected_id_types.add("email")
                elif any(k in c_lower for k in ("phone", "mobile", "cell", "tel")):
                    table_identifiers.append({"type": "phone", "expr": col})
                    detected_id_types.add("phone")
                elif any(k in c_lower for k in ("loyalty", "member_id", "card_no", "rewards")):
                    table_identifiers.append({"type": "loyalty", "expr": col})
                    detected_id_types.add("loyalty")
                elif any(k in c_lower for k in ("device", "idfa", "gaid", "cookie")):
                    table_identifiers.append({"type": "device_id", "expr": col})
                    detected_id_types.add("device_id")
                elif any(k in c_lower for k in ("first_name", "fname")):
                    table_attributes.append({"name": "first_name", "expr": col})
                    detected_attrs.add("first_name")
                elif any(k in c_lower for k in ("last_name", "lname", "surname")):
                    table_attributes.append({"name": "last_name", "expr": col})
                    detected_attrs.add("last_name")
                elif any(k in c_lower for k in ("name", "full_name")):
                    table_attributes.append({"name": "full_name", "expr": col})
                    detected_attrs.add("full_name")
                elif any(k in c_lower for k in ("address", "city", "state", "zip", "postal")):
                    table_attributes.append({"name": col, "expr": col})
                    detected_attrs.add(col)

            source_def = {
                "id": table_id,
                "table": fqn,
                "entity_key": entity_key_col,
                "identifiers": table_identifiers,
                "attributes": table_attributes,
            }
            if watermark_col:
                source_def["watermark_column"] = watermark_col
            sources.append(source_def)

        # 4. Generate matching rules based on detected identifiers
        priority = 1
        for id_type in sorted(detected_id_types):
            max_group = 50 if id_type == "email" else (25 if id_type == "phone" else 10)
            rules.append({
                "id": f"exact_{id_type}",
                "type": "EXACT",
                "match_keys": [id_type],
                "priority": priority,
                "max_group_size": max_group,
            })
            priority += 1

        # 5. Generate survivorship strategies
        survivorship = []
        for attr in sorted(detected_attrs):
            strategy = "PRIORITY" if "name" in attr else "RECENCY"
            rule_entry = {"attribute": attr, "strategy": strategy}
            if strategy == "PRIORITY" and sources:
                rule_entry["source_priority"] = [s["table"] for s in sources]
            survivorship.append(rule_entry)

        # 6. Default placeholder exclusions
        exclusions = [
            {"type": "email", "pattern": "test@test.com", "match_type": "EXACT", "reason": "Test account"},
            {"type": "email", "pattern": "%@example.com", "match_type": "LIKE", "reason": "Example domain"},
            {"type": "phone", "pattern": "0000000000", "match_type": "EXACT", "reason": "Placeholder phone"},
            {"type": "phone", "pattern": "1234567890", "match_type": "EXACT", "reason": "Sequential dummy phone"},
        ]

        suggested_config = {
            "sources": sources,
            "rules": rules,
            "survivorship": survivorship,
            "exclusions": exclusions,
        }

        return {
            "status": "SUCCESS",
            "tables_profiled": len(sources),
            "detected_identifiers": list(detected_id_types),
            "detected_attributes": list(detected_attrs),
            "suggested_config": suggested_config,
        }

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


if __name__ == "__main__":
    connect_from_env()
    mcp.run()
