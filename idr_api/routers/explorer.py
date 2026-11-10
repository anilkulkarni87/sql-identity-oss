"""
Explorer router — entity search and cluster detail.
"""

from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Query

from ..dependencies import get_adapter, require_write_access
from ..models import (
    ClusterDetail,
    ClusterSummary,
    EdgeInfo,
    EntityInfo,
    OverrideRequest,
    PrivacyForgetRequest,
)

router = APIRouter(tags=["explorer"])


@router.get("/api/entities/search", response_model=List[ClusterSummary])
def search_entities(q: str = Query(..., min_length=3), adapter=Depends(get_adapter)):
    """Search for entities by identifier value, entity_key, resolved_id, or retired crosswalk ID."""
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

    # Also check retired resolved_ids in resolved_id_crosswalk
    try:
        if adapter.table_exists("idr_out.resolved_id_crosswalk"):
            cw_rows = adapter.query(
                """
                SELECT DISTINCT
                    c.resolved_id,
                    c.cluster_size,
                    COALESCE(c.confidence_score, 0.0) as confidence_score
                FROM idr_out.resolved_id_crosswalk cw
                JOIN idr_out.identity_clusters_current c
                    ON cw.current_resolved_id = c.resolved_id
                WHERE LOWER(cw.previous_resolved_id) LIKE LOWER(?)
                LIMIT 50
            """,
                params=[pattern],
            )
            if cw_rows:
                return [ClusterSummary(**row) for row in cw_rows]
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

    # Crosswalk history (retired or split resolved_ids involving this cluster)
    crosswalk_history = []
    try:
        if adapter.table_exists("idr_out.resolved_id_crosswalk"):
            cw_rows = adapter.query(
                """
                SELECT previous_resolved_id, current_resolved_id, change_type, entities_moved, run_id, updated_at
                FROM idr_out.resolved_id_crosswalk
                WHERE current_resolved_id = ? OR previous_resolved_id = ?
                ORDER BY updated_at DESC
            """,
                params=[resolved_id, resolved_id],
            )
            for cw in cw_rows:
                crosswalk_history.append(
                    {
                        "previous_resolved_id": cw.get("previous_resolved_id"),
                        "current_resolved_id": cw.get("current_resolved_id"),
                        "change_type": cw.get("change_type"),
                        "entities_moved": cw.get("entities_moved"),
                        "run_id": cw.get("run_id"),
                        "updated_at": str(cw.get("updated_at") or ""),
                    }
                )
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
        crosswalk_history=crosswalk_history,
    )


@router.post("/api/overrides")
def manage_override(
    req: OverrideRequest,
    adapter=Depends(get_adapter),
    _user: Dict[str, Any] = Depends(require_write_access),
):
    """Manage manual stewardship overrides (FORCE_LINK, DO_NOT_LINK, ISOLATE) from the UI."""
    act = (req.action or "add").strip().lower()
    if act == "list":
        if not adapter.table_exists("idr_meta.identity_override"):
            return {"status": "ok", "overrides": []}
        rows = adapter.query(
            "SELECT * FROM idr_meta.identity_override WHERE is_active = TRUE ORDER BY created_at DESC"
        )
        return {"status": "ok", "overrides": rows}

    if not req.override_type or not req.entity_key_a:
        raise HTTPException(400, "override_type and entity_key_a are required")

    otype = req.override_type.strip().upper()
    if otype not in ("FORCE_LINK", "DO_NOT_LINK", "ISOLATE"):
        raise HTTPException(400, f"Invalid override_type '{otype}'")

    ek_a = req.entity_key_a.strip().replace("'", "''")
    ek_b = (req.entity_key_b.strip() if req.entity_key_b else "__ISOLATE__").replace("'", "''")
    rsn = (req.reason or "UI stewardship override").replace("'", "''")

    if act == "remove":
        adapter.execute(f"""
            DELETE FROM idr_meta.identity_override
            WHERE UPPER(override_type) = '{otype}'
              AND entity_key_a = '{ek_a}'
              AND entity_key_b = '{ek_b}'
        """)
        return {"status": "ok", "action": "removed", "override_type": otype, "entity_key_a": ek_a, "entity_key_b": ek_b}

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
        (override_type, entity_key_a, entity_key_b, reason, created_by, is_active, created_at)
        VALUES ('{otype}', '{ek_a}', '{ek_b}', '{rsn}', 'ui', TRUE, {current_ts})
    """)
    return {"status": "ok", "action": "added", "override_type": otype, "entity_key_a": ek_a, "entity_key_b": ek_b}


@router.post("/api/privacy/forget")
def privacy_forget(
    req: PrivacyForgetRequest,
    adapter=Depends(get_adapter),
    _user: Dict[str, Any] = Depends(require_write_access),
):
    """Execute or preview a GDPR / CCPA Right-to-be-Forgotten cascading graph purge from the UI."""
    if not any([req.identifier_value, req.entity_key, req.resolved_id]):
        raise HTTPException(400, "Provide identifier_value, entity_key, or resolved_id")

    from idr_core.privacy import PrivacyManager

    pm = PrivacyManager(adapter)
    return pm.forget(
        identifier_value=req.identifier_value,
        entity_key=req.entity_key,
        resolved_id=req.resolved_id,
        dry_run=req.dry_run,
        requested_by="ui",
        reason=req.reason,
    )
