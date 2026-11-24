"""
Multi-lens identity router — list lenses, browse lens clusters, and diff lenses.
"""

import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from ..dependencies import get_adapter
from ..models import ClusterDetail, ClusterSummary, EdgeInfo, EntityInfo

router = APIRouter(tags=["lenses"])


def _parse_json_list(raw: Any) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    try:
        parsed = json.loads(raw)
        return [str(x) for x in parsed] if isinstance(parsed, list) else []
    except Exception:
        return []


@router.get("/api/lenses")
def list_lenses(adapter=Depends(get_adapter)) -> Dict[str, Any]:
    """List configured active lenses and their materialized cluster/entity counts."""
    if not adapter.table_exists("idr_meta.lens_definition"):
        return {"lenses": []}

    rows = adapter.query(
        """
        SELECT lens_id, include_types_json, exclude_types_json, source_filter_json, max_group_size
        FROM idr_meta.lens_definition
        WHERE is_active = TRUE
        ORDER BY lens_id ASC
        """
    )

    cluster_counts: Dict[str, int] = {}
    entity_counts: Dict[str, int] = {}

    if adapter.table_exists("idr_out.identity_lens_clusters_current"):
        for r in adapter.query(
            "SELECT lens_id, COUNT(*) AS cnt FROM idr_out.identity_lens_clusters_current GROUP BY lens_id"
        ):
            cluster_counts[str(r["lens_id"])] = int(r.get("cnt") or 0)

    if adapter.table_exists("idr_out.identity_lens_membership_current"):
        for r in adapter.query(
            "SELECT lens_id, COUNT(*) AS cnt FROM idr_out.identity_lens_membership_current GROUP BY lens_id"
        ):
            entity_counts[str(r["lens_id"])] = int(r.get("cnt") or 0)

    lenses = []
    for row in rows:
        lid = str(row["lens_id"])
        lenses.append(
            {
                "id": lid,
                "include_types": _parse_json_list(row.get("include_types_json")),
                "exclude_types": _parse_json_list(row.get("exclude_types_json")),
                "source_filter": _parse_json_list(row.get("source_filter_json")),
                "max_group_size": int(row["max_group_size"])
                if row.get("max_group_size") is not None
                else None,
                "cluster_count": cluster_counts.get(lid, 0),
                "entity_count": entity_counts.get(lid, 0),
            }
        )
    return {"lenses": lenses}


@router.get("/api/lenses/diff")
def diff_lenses(
    lens_a: str = Query(..., min_length=1),
    lens_b: str = Query(..., min_length=1),
    limit: int = Query(25, ge=1, le=200),
    adapter=Depends(get_adapter),
) -> Dict[str, Any]:
    """Compare identity clusterings between two lenses."""
    from idr_core.config import compute_lens_diff

    try:
        return compute_lens_diff(adapter, lens_a, lens_b, limit=limit)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(500, f"Failed to diff lenses: {exc}") from exc


@router.get("/api/lenses/{lens_id}/clusters", response_model=List[ClusterSummary])
def list_lens_clusters(
    lens_id: str,
    q: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    adapter=Depends(get_adapter),
):
    """List or search clusters materialized for a specific lens."""
    if not adapter.table_exists("idr_out.identity_lens_clusters_current"):
        return []

    if q and q.strip():
        pattern = f"%{q.strip()}%"
        if adapter.table_exists("idr_out.identity_identifiers_current"):
            rows = adapter.query(
                f"""
                SELECT DISTINCT
                    c.resolved_id,
                    c.cluster_size
                FROM idr_out.identity_lens_clusters_current c
                JOIN idr_out.identity_lens_membership_current m
                    ON c.lens_id = m.lens_id AND c.resolved_id = m.resolved_id
                LEFT JOIN idr_out.identity_identifiers_current i
                    ON m.entity_key = i.entity_key
                WHERE c.lens_id = ?
                  AND (
                    LOWER(COALESCE(i.identifier_value_norm, '')) LIKE LOWER(?)
                    OR LOWER(m.entity_key) LIKE LOWER(?)
                    OR LOWER(c.resolved_id) LIKE LOWER(?)
                  )
                ORDER BY c.cluster_size DESC, c.resolved_id ASC
                LIMIT {limit}
                """,
                params=[lens_id, pattern, pattern, pattern],
            )
        else:
            rows = adapter.query(
                f"""
                SELECT DISTINCT
                    c.resolved_id,
                    c.cluster_size
                FROM idr_out.identity_lens_clusters_current c
                JOIN idr_out.identity_lens_membership_current m
                    ON c.lens_id = m.lens_id AND c.resolved_id = m.resolved_id
                WHERE c.lens_id = ?
                  AND (
                    LOWER(m.entity_key) LIKE LOWER(?)
                    OR LOWER(c.resolved_id) LIKE LOWER(?)
                  )
                ORDER BY c.cluster_size DESC, c.resolved_id ASC
                LIMIT {limit}
                """,
                params=[lens_id, pattern, pattern],
            )
    else:
        rows = adapter.query(
            f"""
            SELECT
                resolved_id,
                cluster_size
            FROM idr_out.identity_lens_clusters_current
            WHERE lens_id = ?
            ORDER BY cluster_size DESC, resolved_id ASC
            LIMIT {limit}
            """,
            params=[lens_id],
        )

    return [
        ClusterSummary(
            resolved_id=r["resolved_id"],
            cluster_size=int(r.get("cluster_size") or 0),
            confidence_score=None,
        )
        for r in rows
    ]


@router.get("/api/lenses/{lens_id}/clusters/{resolved_id}", response_model=ClusterDetail)
def get_lens_cluster_detail(
    lens_id: str,
    resolved_id: str,
    adapter=Depends(get_adapter),
):
    """Get members and edges for a cluster within a specific lens."""
    if not adapter.table_exists("idr_out.identity_lens_membership_current"):
        raise HTTPException(404, "Lens membership table not found")

    member_rows = adapter.query(
        """
        SELECT entity_key
        FROM idr_out.identity_lens_membership_current
        WHERE lens_id = ? AND resolved_id = ?
        ORDER BY entity_key ASC
        """,
        params=[lens_id, resolved_id],
    )
    if not member_rows:
        raise HTTPException(404, f"Cluster {resolved_id} not found in lens {lens_id}")

    entities: List[EntityInfo] = []
    entity_keys: List[str] = []
    for mr in member_rows:
        ek = str(mr["entity_key"])
        entity_keys.append(ek)
        entities.append(
            EntityInfo(
                entity_key=ek,
                source_id=str(mr.get("source_id") or (ek.split(":", 1)[0] if ":" in ek else "")),
                source_key=ek.split(":", 1)[1] if ":" in ek else ek,
            )
        )

    edges: List[EdgeInfo] = []
    if adapter.table_exists("idr_out.identity_edges_current") and entity_keys:
        placeholders = ", ".join("?" for _ in entity_keys)
        edge_rows = adapter.query(
            f"""
            SELECT left_entity_key, right_entity_key, identifier_type, identifier_value_norm
            FROM idr_out.identity_edges_current
            WHERE left_entity_key IN ({placeholders})
              AND right_entity_key IN ({placeholders})
            LIMIT 200
            """,
            params=entity_keys + entity_keys,
        )
        for er in edge_rows:
            edges.append(
                EdgeInfo(
                    left_entity_key=er["left_entity_key"],
                    right_entity_key=er["right_entity_key"],
                    identifier_type=er["identifier_type"],
                    identifier_value=er.get("identifier_value_norm") or "",
                    rule_id=er.get("identifier_type") or "",
                )
            )

    return ClusterDetail(
        resolved_id=resolved_id,
        cluster_size=len(entities),
        confidence_score=0.0,
        entities=entities,
        edges=edges,
        golden_profile=None,
    )
