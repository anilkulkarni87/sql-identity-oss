"""
Warehouse dry-run delta attribution (`compute_dry_run_summary`) and multi-lens
cluster boundary comparison (`compute_lens_diff`).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .validators import validate_identifier


def compute_dry_run_summary(
    adapter: Any,
    run_id: str,
    run_mode: str,
    edges_created: int = 0,
    clusters_impacted: int = 0,
) -> Dict[str, Any]:
    """Compute dry-run entity moves, edge delta, and cluster delta from `idr_out.dry_run_summary`."""
    if not adapter.table_exists("idr_out.dry_run_summary"):
        return {"proposed_changes": 0}

    summary_rows = adapter.query(
        "SELECT new_entities, moved_entities, unchanged_entities, total_entities "
        f"FROM idr_out.dry_run_summary WHERE run_id = '{run_id}'"
    )
    if not summary_rows:
        return {"proposed_changes": 0}

    srow = summary_rows[0]
    new_cnt = int(srow.get("new_entities") or 0)
    moved_cnt = int(srow.get("moved_entities") or 0)
    unchanged_cnt = int(srow.get("unchanged_entities") or 0)
    total_cnt = int(srow.get("total_entities") or 0)

    proposed_edges = int(edges_created or 0)
    proposed_clusters = int(clusters_impacted or 0)
    current_edges: Optional[int] = None
    edges_delta = 0
    current_clusters: Optional[int] = None
    clusters_delta = 0

    normalized_mode = "INCR" if str(run_mode).upper().startswith("INCR") else "FULL"
    if adapter.table_exists("idr_out.identity_edges_current"):
        if normalized_mode == "FULL":
            current_edges = int(
                adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_edges_current") or 0
            )
        elif adapter.table_exists("idr_work.lp_labels"):
            current_edges = int(
                adapter.query_one(
                    "SELECT COUNT(*) FROM idr_out.identity_edges_current e "
                    "WHERE e.left_entity_key IN (SELECT entity_key FROM idr_work.lp_labels) "
                    "   OR e.right_entity_key IN (SELECT entity_key FROM idr_work.lp_labels)"
                )
                or 0
            )
        if current_edges is not None:
            edges_delta = proposed_edges - current_edges

    if adapter.table_exists("idr_out.identity_clusters_current"):
        if normalized_mode == "FULL":
            current_clusters = int(
                adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_clusters_current") or 0
            )
            clusters_delta = proposed_clusters - current_clusters
        elif adapter.table_exists("idr_work.lp_labels") and adapter.table_exists(
            "idr_out.identity_resolved_membership_current"
        ):
            current_clusters = int(
                adapter.query_one(
                    "SELECT COUNT(DISTINCT m.resolved_id) "
                    "FROM idr_out.identity_resolved_membership_current m "
                    "JOIN idr_work.lp_labels lp ON lp.entity_key = m.entity_key"
                )
                or 0
            )
            clusters_delta = proposed_clusters - current_clusters

    edges_delta_by_type: Dict[str, int] = {}
    shadow_preserved_entities = 0
    corroborating_identifier_types: List[str] = []

    if adapter.table_exists("idr_out.identity_edges_current") and adapter.table_exists(
        "idr_work.edges_new"
    ):
        edge_scope_where = "1=1"
        if normalized_mode != "FULL" and adapter.table_exists("idr_work.lp_labels"):
            edge_scope_where = (
                "(e.left_entity_key IN (SELECT entity_key FROM idr_work.lp_labels) "
                "OR e.right_entity_key IN (SELECT entity_key FROM idr_work.lp_labels))"
            )

        curr_by_type_rows = adapter.query(
            f"SELECT UPPER(e.identifier_type) AS identifier_type, COUNT(*) AS cnt "
            f"FROM idr_out.identity_edges_current e "
            f"WHERE {edge_scope_where} "
            f"GROUP BY UPPER(e.identifier_type)"
        )
        prop_by_type_rows = adapter.query(
            "SELECT UPPER(identifier_type) AS identifier_type, COUNT(*) AS cnt "
            "FROM idr_work.edges_new "
            "GROUP BY UPPER(identifier_type)"
        )
        curr_map = {
            str(r["identifier_type"]): int(r["cnt"] or 0)
            for r in curr_by_type_rows
            if r.get("identifier_type")
        }
        prop_map = {
            str(r["identifier_type"]): int(r["cnt"] or 0)
            for r in prop_by_type_rows
            if r.get("identifier_type")
        }
        for id_type in sorted(set(curr_map) | set(prop_map)):
            type_delta = prop_map.get(id_type, 0) - curr_map.get(id_type, 0)
            if type_delta != 0:
                edges_delta_by_type[id_type] = type_delta

        shadow_rows = adapter.query(f"""
            WITH curr_norm AS (
                SELECT
                    CASE WHEN e.left_entity_key <= e.right_entity_key THEN e.left_entity_key ELSE e.right_entity_key END AS k1,
                    CASE WHEN e.left_entity_key <= e.right_entity_key THEN e.right_entity_key ELSE e.left_entity_key END AS k2,
                    UPPER(e.identifier_type) AS identifier_type
                FROM idr_out.identity_edges_current e
                WHERE {edge_scope_where}
            ),
            new_norm AS (
                SELECT
                    CASE WHEN n.left_entity_key <= n.right_entity_key THEN n.left_entity_key ELSE n.right_entity_key END AS k1,
                    CASE WHEN n.left_entity_key <= n.right_entity_key THEN n.right_entity_key ELSE n.left_entity_key END AS k2,
                    UPPER(n.identifier_type) AS identifier_type
                FROM idr_work.edges_new n
                GROUP BY 1, 2, 3
            ),
            removed_edges AS (
                SELECT
                    c.k1 AS left_entity_key,
                    c.k2 AS right_entity_key,
                    c.identifier_type
                FROM curr_norm c
                LEFT JOIN new_norm n
                  ON c.k1 = n.k1
                 AND c.k2 = n.k2
                 AND c.identifier_type = n.identifier_type
                WHERE n.k1 IS NULL
            ),
            removed_endpoints AS (
                SELECT entity_key, removed_type
                FROM (
                    SELECT left_entity_key AS entity_key, identifier_type AS removed_type FROM removed_edges
                    UNION ALL
                    SELECT right_entity_key AS entity_key, identifier_type AS removed_type FROM removed_edges
                ) re
                GROUP BY entity_key, removed_type
            ),
            unchanged_removed AS (
                SELECT r.entity_key, r.removed_type
                FROM removed_endpoints r
                JOIN idr_out.dry_run_results d
                  ON d.entity_key = r.entity_key
                WHERE d.run_id = '{run_id}'
                  AND d.change_type = 'UNCHANGED'
            ),
            new_endpoint_types AS (
                SELECT entity_key, corroborating_type
                FROM (
                    SELECT left_entity_key AS entity_key, UPPER(identifier_type) AS corroborating_type
                    FROM idr_work.edges_new
                    UNION ALL
                    SELECT right_entity_key AS entity_key, UPPER(identifier_type) AS corroborating_type
                    FROM idr_work.edges_new
                ) ne
                GROUP BY entity_key, corroborating_type
            )
            SELECT DISTINCT
                u.entity_key,
                n.corroborating_type
            FROM unchanged_removed u
            JOIN new_endpoint_types n
              ON u.entity_key = n.entity_key
             AND n.corroborating_type != u.removed_type
        """)
        if shadow_rows:
            shadow_preserved_entities = len({str(r["entity_key"]) for r in shadow_rows})
            corroborating_identifier_types = sorted(
                {str(r["corroborating_type"]) for r in shadow_rows if r.get("corroborating_type")}
            )

    return {
        "proposed_changes": new_cnt + moved_cnt + abs(edges_delta),
        "new_entities": new_cnt,
        "moved_entities": moved_cnt,
        "unchanged_entities": unchanged_cnt,
        "total_entities": total_cnt,
        "current_edges": current_edges,
        "proposed_edges": proposed_edges,
        "edges_delta": edges_delta,
        "current_clusters": current_clusters,
        "proposed_clusters": proposed_clusters,
        "clusters_delta": clusters_delta,
        "edges_delta_by_type": edges_delta_by_type,
        "shadow_preserved_entities": shadow_preserved_entities,
        "corroborating_identifier_types": corroborating_identifier_types,
    }


def compute_lens_diff(
    adapter: Any, lens_a: str, lens_b: str, limit: int = 25
) -> Dict[str, Any]:
    """Compare resolved cluster boundaries between two lenses (or `'default'` vs a lens)."""
    safe_a = (
        "default"
        if str(lens_a).strip().lower() == "default"
        else validate_identifier(str(lens_a).strip(), "lens_a")
    )
    safe_b = (
        "default"
        if str(lens_b).strip().lower() == "default"
        else validate_identifier(str(lens_b).strip(), "lens_b")
    )
    safe_limit = max(1, min(int(limit or 25), 200))

    def _src_sql(name: str) -> str:
        if name == "default":
            return "(SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current)"
        return f"(SELECT entity_key, resolved_id FROM idr_out.identity_lens_membership_current WHERE lens_id = '{name}')"

    src_a = _src_sql(safe_a)
    src_b = _src_sql(safe_b)

    clusters_a = int(adapter.query_one(f"SELECT COUNT(DISTINCT resolved_id) FROM {src_a}") or 0)
    clusters_b = int(adapter.query_one(f"SELECT COUNT(DISTINCT resolved_id) FROM {src_b}") or 0)
    entities_compared = int(
        adapter.query_one(
            f"SELECT COUNT(*) FROM {src_a} a JOIN {src_b} b ON a.entity_key = b.entity_key"
        )
        or 0
    )

    merged_in_b_only = adapter.query(f"""
        SELECT
            b.resolved_id AS resolved_b,
            COUNT(DISTINCT a.resolved_id) AS distinct_a_clusters,
            COUNT(*) AS entity_count
        FROM {src_a} a
        JOIN {src_b} b ON a.entity_key = b.entity_key
        GROUP BY b.resolved_id
        HAVING COUNT(DISTINCT a.resolved_id) > 1
        ORDER BY entity_count DESC, b.resolved_id ASC
        LIMIT {safe_limit}
    """)

    merged_in_a_only = adapter.query(f"""
        SELECT
            a.resolved_id AS resolved_a,
            COUNT(DISTINCT b.resolved_id) AS distinct_b_clusters,
            COUNT(*) AS entity_count
        FROM {src_a} a
        JOIN {src_b} b ON a.entity_key = b.entity_key
        GROUP BY a.resolved_id
        HAVING COUNT(DISTINCT b.resolved_id) > 1
        ORDER BY entity_count DESC, a.resolved_id ASC
        LIMIT {safe_limit}
    """)

    return {
        "lens_a": safe_a,
        "lens_b": safe_b,
        "lens_a_clusters": clusters_a,
        "lens_b_clusters": clusters_b,
        "entities_compared": entities_compared,
        "merged_in_b_only": merged_in_b_only,
        "merged_in_a_only": merged_in_a_only,
    }
