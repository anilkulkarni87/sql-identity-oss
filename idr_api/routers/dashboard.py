"""
Dashboard metrics router — summary cards, distribution, rule stats, alerts.
"""

from typing import List

from fastapi import APIRouter, Depends

from ..dependencies import get_adapter
from ..models import Alert, ClusterDistribution, MetricsSummary, RuleStats

router = APIRouter(tags=["dashboard"])


@router.get("/api/metrics/summary", response_model=MetricsSummary)
def get_dashboard_metrics(adapter=Depends(get_adapter)):
    """Get summary metrics for dashboard cards."""

    # Last run
    try:
        rows = adapter.query("""
            SELECT run_id, duration_seconds, CAST(started_at AS STRING) as started_at
            FROM idr_out.run_history
            ORDER BY started_at DESC
            LIMIT 1
        """)
        last_run = rows[0] if rows else {}
    except Exception as e:
        print(f"Error fetching last run: {e}")
        last_run = {}

    latest_run_id = last_run.get("run_id")

    # Cluster stats
    try:
        rows = adapter.query("""
            SELECT
                COUNT(*) as total_clusters,
                COALESCE(SUM(cluster_size), 0) as total_entities
            FROM idr_out.identity_clusters_current
        """)
        cluster_stats = rows[0] if rows else {}

        # Calculate avg confidence from clusters table which has confidence_score
        conf_rows = adapter.query(
            "SELECT AVG(confidence_score) as avg_conf FROM idr_out.identity_clusters_current"
        )
        avg_confidence = (
            conf_rows[0]["avg_conf"] if conf_rows and conf_rows[0]["avg_conf"] is not None else 0.0
        )

    except Exception as e:
        print(f"Error fetching cluster stats: {e}")
        cluster_stats = {}
        avg_confidence = 0.0

    # Handle NaN which is not JSON compliant
    import math

    if isinstance(avg_confidence, float) and math.isnan(avg_confidence):
        avg_confidence = 0.0

    # Edge count
    try:
        rows = adapter.query("SELECT COUNT(*) as cnt FROM idr_out.identity_edges_current")
        # query() returns list of dicts, e.g. [{'cnt': 123}]
        edge_count = rows[0] if rows else {}
    except Exception as e:
        # Table might not exist yet if no edges formed
        print(f"Error fetching edge count: {e}")
        edge_count = {}

    return MetricsSummary(
        total_clusters=(cluster_stats.get("total_clusters") or 0) if cluster_stats else 0,
        total_entities=(cluster_stats.get("total_entities") or 0) if cluster_stats else 0,
        total_edges=(edge_count.get("cnt") or 0) if edge_count else 0,
        avg_confidence=round(avg_confidence, 3),
        last_run_id=last_run.get("run_id") if last_run else None,
        last_run_duration=last_run.get("duration_seconds") if last_run else None,
        last_run_started_at=last_run.get("started_at") if last_run else None,
    )


@router.get("/api/metrics/distribution", response_model=List[ClusterDistribution])
def get_cluster_distribution(adapter=Depends(get_adapter)):
    """Get cluster size distribution for histogram."""

    rows = adapter.query("""
        SELECT
            CASE
                WHEN cluster_size = 1 THEN 'singleton'
                WHEN cluster_size <= 5 THEN '2-5'
                WHEN cluster_size <= 20 THEN '6-20'
                WHEN cluster_size <= 100 THEN '21-100'
                ELSE '>100'
            END as bucket,
            COUNT(*) as count
        FROM idr_out.identity_clusters_current
        GROUP BY 1
        ORDER BY
            CASE bucket
                WHEN 'singleton' THEN 1
                WHEN '2-5' THEN 2
                WHEN '6-20' THEN 3
                WHEN '21-100' THEN 4
                ELSE 5
            END
    """)

    return [ClusterDistribution(**row) for row in rows]


@router.get("/api/metrics/rules", response_model=List[RuleStats])
async def get_rule_stats(adapter=Depends(get_adapter)):
    """Get edges created per rule."""

    try:
        rows = adapter.query("""
            SELECT
                rule_id,
                identifier_type,
                COUNT(*) as edges_created
            FROM idr_out.edge_evidence
            GROUP BY rule_id, identifier_type
            ORDER BY edges_created DESC
        """)

        total = sum((row.get("edges_created") or 0) for row in rows)
    except Exception as e:
        print(f"Error fetching rule stats: {e}")
        rows = []
        total = 0

    return [
        RuleStats(
            rule_id=row["rule_id"],
            identifier_type=row.get("identifier_type"),
            edges_created=(row.get("edges_created") or 0),
            percentage=round((row.get("edges_created") or 0) / total * 100, 1) if total > 0 else 0,
        )
        for row in rows
    ]


@router.get("/api/alerts", response_model=List[Alert])
async def get_alerts(adapter=Depends(get_adapter)):
    """Get active warnings and alerts."""
    alerts = []

    # Skipped groups
    try:
        rows = adapter.query("""
            SELECT COUNT(*) as cnt, MAX(group_size) as max_size
            FROM idr_out.skipped_identifier_groups
        """)
        skipped = rows[0] if rows else {}
    except Exception:
        skipped = {}

    if skipped.get("cnt", 0) > 0:
        alerts.append(
            Alert(
                severity="warning",
                message="Groups skipped (exceeded max_group_size)",
                count=skipped["cnt"],
            )
        )

    # Large clusters
    try:
        rows = adapter.query("""
            SELECT COUNT(*) as cnt
            FROM idr_out.identity_clusters_current
            WHERE cluster_size > 1000
        """)
        large = rows[0] if rows else {}
    except Exception:
        large = {}

    if large.get("cnt", 0) > 0:
        alerts.append(
            Alert(
                severity="warning",
                message="Large clusters detected (>1000 entities)",
                count=large["cnt"],
            )
        )

    return alerts
