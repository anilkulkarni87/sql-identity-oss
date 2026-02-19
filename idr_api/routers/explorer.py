"""
Explorer router — entity search and cluster detail.
"""

from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query

from ..dependencies import get_adapter
from ..models import ClusterDetail, ClusterSummary, EdgeInfo, EntityInfo

router = APIRouter(tags=["explorer"])


@router.get("/api/entities/search", response_model=List[ClusterSummary])
def search_entities(q: str = Query(..., min_length=3), adapter=Depends(get_adapter)):
    """Search for entities by identifier value."""

    rows = adapter.query(
        """
        SELECT DISTINCT
            c.resolved_id,
            c.cluster_size,
            0.0 as confidence_score
        FROM idr_out.identity_clusters_current c
        JOIN idr_out.identity_resolved_membership_current m
            ON c.resolved_id = m.resolved_id
        JOIN idr_out.identity_edges_current e
            ON m.entity_key = e.left_entity_key OR m.entity_key = e.right_entity_key
        WHERE LOWER(e.identifier_value_norm) LIKE LOWER(?)
        LIMIT 50
    """,
        params=[f"%{q}%"],
    )

    return [ClusterSummary(**row) for row in rows]


@router.get("/api/clusters/{resolved_id}", response_model=ClusterDetail)
def get_cluster(resolved_id: str, adapter=Depends(get_adapter)):
    """Get full cluster detail with entities and edges."""

    # Cluster info
    try:
        rows = adapter.query(
            """
            SELECT resolved_id, cluster_size, 0.0 as confidence_score
            FROM idr_out.identity_clusters_current
            WHERE resolved_id = ?
        """,
            params=[resolved_id],
        )
        cluster = rows[0] if rows else None
    except Exception as e:
        print(f"Error fetching cluster {resolved_id}: {e}")
        cluster = None

    if not cluster:
        raise HTTPException(404, f"Cluster {resolved_id} not found")

    # Entities
    entities = adapter.query(
        """
        SELECT entity_key, source_id, source_key
        FROM idr_out.identity_resolved_membership_current
        WHERE resolved_id = ?
    """,
        params=[resolved_id],
    )

    # Edges
    edges = adapter.query(
        """
        SELECT DISTINCT
            left_entity_key,
            right_entity_key,
            identifier_type,
            identifier_value_norm as identifier_value,
            rule_id
        FROM idr_out.identity_edges_current
        WHERE left_entity_key IN (
            SELECT entity_key
            FROM idr_out.identity_resolved_membership_current
            WHERE resolved_id = ?
        )
    """,
        params=[resolved_id],
    )

    return ClusterDetail(
        resolved_id=cluster["resolved_id"],
        cluster_size=cluster["cluster_size"],
        confidence_score=cluster.get("confidence_score"),
        entities=[EntityInfo(**e) for e in entities],
        edges=[EdgeInfo(**e) for e in edges],
    )
