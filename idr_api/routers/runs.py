"""
Run history router — list recent IDR runs.
"""

import json
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query

from ..dependencies import get_adapter
from ..models import RunDetail, RunSummary, StageMetricInfo

router = APIRouter(tags=["runs"])


@router.get("/api/runs", response_model=List[RunSummary])
async def get_runs(limit: int = Query(20, le=100), adapter=Depends(get_adapter)):
    """Get recent run history."""

    try:
        from idr_core.adapters.base import get_dialect_config

        str_type = get_dialect_config(getattr(adapter, "dialect", "duckdb")).get(
            "string_type", "VARCHAR"
        )
        rows = adapter.query(f"""
            SELECT
                run_id,
                run_mode,
                status,
                CAST(started_at AS {str_type}) as started_at,
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


@router.get("/api/runs/{run_id}", response_model=RunDetail)
async def get_run_detail(run_id: str, adapter=Depends(get_adapter)):
    """Get detailed run information including stage metrics, warnings, and error messages."""
    from idr_core.adapters.base import get_dialect_config

    str_type = get_dialect_config(getattr(adapter, "dialect", "duckdb")).get(
        "string_type", "VARCHAR"
    )
    rows = adapter.query(
        f"""
        SELECT
            run_id,
            run_mode,
            status,
            CAST(started_at AS {str_type}) as started_at,
            duration_seconds,
            entities_processed,
            edges_created,
            clusters_impacted,
            lp_iterations,
            error_message
        FROM idr_out.run_history
        WHERE run_id = ?
    """,
        params=[run_id],
    )
    if not rows:
        raise HTTPException(404, f"Run {run_id} not found")

    row = rows[0]

    # Warnings (optional column depending on schema evolution)
    warnings_list: List[str] = []
    try:
        w_rows = adapter.query(
            "SELECT warnings FROM idr_out.run_history WHERE run_id = ?",
            params=[run_id],
        )
        raw_w = w_rows[0].get("warnings") if w_rows else None
        if raw_w:
            parsed = json.loads(raw_w) if isinstance(raw_w, str) else raw_w
            if isinstance(parsed, list):
                warnings_list = [str(x) for x in parsed]
    except Exception:
        pass

    stages: List[StageMetricInfo] = []
    try:
        if adapter.table_exists("idr_out.stage_metrics"):
            stage_rows = adapter.query(
                f"""
                SELECT
                    stage_name,
                    stage_order,
                    CAST(started_at AS {str_type}) as started_at,
                    CAST(ended_at AS {str_type}) as ended_at,
                    duration_seconds,
                    rows_out
                FROM idr_out.stage_metrics
                WHERE run_id = ?
                ORDER BY stage_order ASC
            """,
                params=[run_id],
            )
            stages = [
                StageMetricInfo(
                    stage_name=s["stage_name"],
                    stage_order=int(s.get("stage_order") or 0),
                    started_at=s.get("started_at"),
                    ended_at=s.get("ended_at"),
                    duration_seconds=int(s.get("duration_seconds") or 0),
                    rows_out=int(s.get("rows_out") or 0),
                )
                for s in stage_rows
            ]
    except Exception:
        pass

    return RunDetail(
        run_id=row["run_id"],
        run_mode=row["run_mode"],
        status=row["status"],
        started_at=row["started_at"],
        duration_seconds=row.get("duration_seconds"),
        entities_processed=row.get("entities_processed") or 0,
        edges_created=row.get("edges_created") or 0,
        clusters_impacted=row.get("clusters_impacted") or 0,
        lp_iterations=row.get("lp_iterations") or 0,
        error_message=row.get("error_message"),
        warnings=warnings_list,
        stages=stages,
    )
