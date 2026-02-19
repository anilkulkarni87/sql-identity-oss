"""
Run history router — list recent IDR runs.
"""

from typing import List

from fastapi import APIRouter, Depends, Query

from ..dependencies import get_adapter
from ..models import RunSummary

router = APIRouter(tags=["runs"])


@router.get("/api/runs", response_model=List[RunSummary])
async def get_runs(limit: int = Query(20, le=100), adapter=Depends(get_adapter)):
    """Get recent run history."""

    try:
        rows = adapter.query(f"""
            SELECT
                run_id,
                run_mode,
                status,
                CAST(started_at AS STRING) as started_at,
                duration_seconds,
                entities_processed,
                edges_created,
                clusters_impacted
            FROM idr_out.run_history
            ORDER BY started_at DESC
            LIMIT {limit}
        """)

        return [
            RunSummary(
                run_id=row["run_id"],
                run_mode=row["run_mode"],
                status=row["status"],
                started_at=row["started_at"],
                duration_seconds=row.get("duration_seconds"),
                entities_processed=(row.get("entities_processed") or 0),
                edges_created=(row.get("edges_created") or 0),
                clusters_impacted=(row.get("clusters_impacted") or 0),
            )
            for row in rows
        ]
    except Exception as e:
        print(f"Error fetching runs: {e}")
        return []
