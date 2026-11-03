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
    """Search for entities by identifier value, entity_key, or resolved_id."""
    pattern = f"%{q}%"

    try:
        if adapter.table_exists("idr_out.identity_identifiers_current"):
            rows = adapter.query(
                """
                SELECT DISTINCT
                    c.resolved_id,
                    c.cluster_size,
                    COALESCE(c.confidence_score, 0.0) as confidence_score
                FROM idr_out.identity_clusters_current c
                JOIN idr_out.identity_resolved_membership_current m
                    ON c.resolved_id = m.resolved_id
                LEFT JOIN idr_out.identity_identifiers_current i
                    ON m.entity_key = i.entity_key
                WHERE LOWER(i.identifier_value_norm) LIKE LOWER(?)
                   OR LOWER(m.entity_key) LIKE LOWER(?)
                   OR LOWER(c.resolved_id) LIKE LOWER(?)
                LIMIT 50
            """,
                params=[pattern, pattern, pattern],
            )
            if rows:
                return [ClusterSummary(**row) for row in rows]
    except Exception:
        pass

    rows = adapter.query(
        """
        SELECT DISTINCT
            c.resolved_id,
            c.cluster_size,
            COALESCE(c.confidence_score, 0.0) as confidence_score
        FROM idr_out.identity_clusters_current c
        JOIN idr_out.identity_resolved_membership_current m
            ON c.resolved_id = m.resolved_id
        LEFT JOIN idr_out.identity_edges_current e
            ON m.entity_key = e.left_entity_key OR m.entity_key = e.right_entity_key
        WHERE LOWER(e.identifier_value_norm) LIKE LOWER(?)
           OR LOWER(m.entity_key) LIKE LOWER(?)
           OR LOWER(c.resolved_id) LIKE LOWER(?)
        LIMIT 50
    """,
        params=[pattern, pattern, pattern],
    )

    return [ClusterSummary(**row) for row in rows]


@router.get("/api/clusters/{resolved_id}", response_model=ClusterDetail)
def get_cluster(resolved_id: str, adapter=Depends(get_adapter)):
    """Get full cluster detail with entities and edges."""

    # Cluster info
    try:
        rows = adapter.query(
            """
            SELECT resolved_id, cluster_size, COALESCE(confidence_score, 0.0) as confidence_score, super_cluster_id
            FROM idr_out.identity_clusters_current
            WHERE resolved_id = ?
        """,
            params=[resolved_id],
        )
        cluster = rows[0] if rows else None
    except Exception:
        try:
            rows = adapter.query(
                """
                SELECT resolved_id, cluster_size, COALESCE(confidence_score, 0.0) as confidence_score
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
    entities_raw = adapter.query(
        """
        SELECT entity_key
        FROM idr_out.identity_resolved_membership_current
        WHERE resolved_id = ?
    """,
        params=[resolved_id],
    )
    entities = []
    for row in entities_raw:
        ek = row.get("entity_key") or ""
        parts = ek.split(":", 1)
        source_id = parts[0] if len(parts) > 1 else "unknown"
        source_key = parts[1] if len(parts) > 1 else ek
        entities.append(
            EntityInfo(
                entity_key=ek,
                source_id=source_id,
                source_key=source_key,
            )
        )

    # Edges
    edges_raw = adapter.query(
        """
        SELECT DISTINCT
            left_entity_key,
            right_entity_key,
            identifier_type,
            identifier_value_norm as identifier_value
        FROM idr_out.identity_edges_current
        WHERE left_entity_key IN (
            SELECT entity_key
            FROM idr_out.identity_resolved_membership_current
            WHERE resolved_id = ?
        )
    """,
        params=[resolved_id],
    )
    edges = [
        EdgeInfo(
            left_entity_key=e["left_entity_key"],
            right_entity_key=e["right_entity_key"],
            identifier_type=e["identifier_type"],
            identifier_value=e.get("identifier_value") or "",
            rule_id=e.get("rule_id") or f"exact_{e.get('identifier_type', 'unknown')}",
        )
        for e in edges_raw
    ]

    # Golden Profile (if generated)
    golden_profile = None
    for profile_table in ("idr_out.golden_profiles", "idr_out.golden_profile_current"):
        try:
            if adapter.table_exists(profile_table):
                profile_rows = adapter.query(
                    f"SELECT * FROM {profile_table} WHERE resolved_id = ?",
                    params=[resolved_id],
                )
                if profile_rows:
                    raw_profile = dict(profile_rows[0])
                    golden_profile = {
                        k: (str(v) if v is not None and not isinstance(v, (str, int, float, bool)) else v)
                        for k, v in raw_profile.items()
                        if k not in ("resolved_id", "run_id", "updated_at", "updated_ts")
                    }
                    break
        except Exception:
            pass

    return ClusterDetail(
        resolved_id=cluster["resolved_id"],
        cluster_size=cluster["cluster_size"],
        confidence_score=cluster.get("confidence_score"),
        super_cluster_id=cluster.get("super_cluster_id"),
        entities=entities,
        edges=edges,
        golden_profile=golden_profile,
    )
