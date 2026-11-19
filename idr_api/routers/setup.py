import asyncio
import copy
import json
import time
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from idr_core.config import (
    apply_config_to_warehouse,
    compute_dry_run_summary,
    get_config_snapshots,
    parse_config_snapshot_row,
    restore_meta_tables,
    snapshot_meta_tables,
)

from ..dependencies import get_current_user, get_manager, get_user_key, require_write_access

router = APIRouter(prefix="/api/setup", tags=["setup"])
manager = get_manager()

_SETUP_CACHE_TTL_SECONDS = 30.0
_STATUS_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_CONFIG_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}


def _invalidate_setup_cache(user_key: str) -> None:
    _STATUS_CACHE.pop(user_key, None)
    _CONFIG_CACHE.pop(user_key, None)


def _require_user_adapter(current_user: Dict[str, Any]):
    user_key = get_user_key(current_user)
    adapter = manager.get_adapter_for_user(user_key)
    if not adapter:
        raise HTTPException(400, "Not connected")
    return adapter, user_key


class ConnectRequest(BaseModel):
    platform: str
    params: Dict[str, Any]


class TableListRequest(BaseModel):
    schema_name: Optional[str] = None


@router.post("/connect")
async def connect_database(request: ConnectRequest, current_user: Dict[str, Any] = Depends(get_current_user)):
    """
    Establish a temporary connection for the session.
    Credentials are NOT persisted to disk, only in memory.
    """
    try:
        adapter = None
        params = request.params

        if request.platform == "duckdb":
            from idr_core.adapters.duckdb import DuckDBAdapter

            # Allow creating new if not exists for local demo
            path = params.get("path", "retail.duckdb")
            adapter = DuckDBAdapter(path)

        elif request.platform == "snowflake":
            try:
                import snowflake.connector

                from idr_core.adapters.snowflake import SnowflakeAdapter
                from idr_core.connection_manager import build_snowflake_connect_kwargs

                sf_kwargs = build_snowflake_connect_kwargs(params)
                conn = snowflake.connector.connect(**sf_kwargs)
                adapter = SnowflakeAdapter(conn)
            except ImportError:
                raise HTTPException(400, "Snowflake connector not installed")

        elif request.platform == "bigquery":
            try:
                import json

                from google.cloud import bigquery
                from google.oauth2 import service_account

                from idr_core.adapters.bigquery import BigQueryAdapter

                project = params.get("project") or None
                credentials_json = params.get("credentials_json") or params.get("credentials")

                if credentials_json:
                    info = json.loads(credentials_json)
                    creds = service_account.Credentials.from_service_account_info(info)
                    client = bigquery.Client(project=project, credentials=creds)
                else:
                    # Try default auth
                    client = bigquery.Client(project=project)

                adapter = BigQueryAdapter(
                    client,
                    project=project or getattr(client, "project", None),
                    location=params.get("location", "US"),
                )
            except ImportError:
                raise HTTPException(400, "BigQuery client not installed")

        elif request.platform == "databricks":
            try:
                from databricks import sql

                from idr_core.adapters.databricks import DatabricksAdapter
                from idr_core.connection_manager import build_databricks_connect_kwargs

                dbx_kwargs, catalog = build_databricks_connect_kwargs(params)
                conn = sql.connect(**dbx_kwargs)
                adapter = DatabricksAdapter(conn, catalog=catalog)
            except ImportError:
                raise HTTPException(400, "Databricks connector not installed. Run: pip install databricks-sql-connector")

        else:
            raise HTTPException(400, f"Unsupported platform: {request.platform}")

        if adapter:
            uk = get_user_key(current_user)
            _invalidate_setup_cache(uk)
            manager.set_adapter_for_user(
                uk, adapter, {"platform": request.platform, **params}
            )
            return {"status": "connected", "platform": request.platform}

    except Exception as e:
        raise HTTPException(400, f"Connection failed: {str(e)}")


@router.get("/status")
async def get_status(current_user: Dict[str, Any] = Depends(get_current_user)):
    """Check connection and configuration lifecycle status."""
    user_key = get_user_key(current_user)
    connected = manager.is_connected(user_key)
    configured = False
    platform = None
    has_runs = False
    state = "DISCONNECTED"
    suggested_action = "connect_warehouse"

    if connected:
        cached = _STATUS_CACHE.get(user_key)
        if cached and (time.monotonic() - cached[0]) < _SETUP_CACHE_TTL_SECONDS:
            return cached[1]

        adapter = manager.get_adapter_for_user(user_key)
        platform = manager.get_config_for_user(user_key).get("platform")

        try:
            src_row = adapter.query("SELECT 1 AS ok FROM idr_meta.source_table LIMIT 1")
            configured = bool(src_row)
            if configured:
                try:
                    cls_row = adapter.query(
                        "SELECT 1 AS ok FROM idr_out.identity_clusters_current LIMIT 1"
                    )
                    has_runs = bool(cls_row)
                except Exception:
                    has_runs = False
                if has_runs:
                    state = "RESOLVED"
                    suggested_action = "explore_identity_graph"
                else:
                    state = "CONFIGURED_READY"
                    suggested_action = "trigger_resolution_run"
            else:
                state = "CONNECTED_UNCONFIGURED"
                suggested_action = "discover_and_map_sources"
        except Exception:
            try:
                adapter.query_one("SELECT 1")
                state = "CONNECTED_UNCONFIGURED"
                suggested_action = "discover_and_map_sources"
            except Exception:
                state = "STALE"
                suggested_action = "reconnect_warehouse"

    res = {
        "connected": connected,
        "configured": configured,
        "platform": platform,
        "state": state,
        "has_runs": has_runs,
        "suggested_action": suggested_action,
    }
    if connected and state != "STALE":
        _STATUS_CACHE[user_key] = (time.monotonic(), res)
    return res


@router.get("/discovered-connections")
async def get_discovered_connections():
    """Discover available warehouse connections from CLI configs, dbt profiles, env, and ADC."""
    from idr_core.credential_discovery import discover_all_connections

    try:
        conns = discover_all_connections()
    except Exception as e:
        conns = []
    return {"connections": conns}


# Define config model locally since it's not in idr_core
class IDRConfig(BaseModel):
    sources: List[Dict[str, Any]] = []
    rules: Optional[List[Dict[str, Any]]] = None
    fuzzy_rules: Optional[List[Dict[str, Any]]] = None
    survivorship: Optional[List[Dict[str, Any]]] = None
    exclusions: Optional[List[Dict[str, Any]]] = None
    overrides: Optional[List[Dict[str, Any]]] = None
    lenses: Optional[List[Dict[str, Any]]] = None
    use_default_exclusions: Optional[bool] = False


@router.get("/config", response_model=IDRConfig)
def get_current_config(current_user: Dict[str, Any] = Depends(get_current_user)):
    """Get the currently active configuration from metadata tables or latest snapshot."""
    adapter, user_key = _require_user_adapter(current_user)

    cached = _CONFIG_CACHE.get(user_key)
    if cached and (time.monotonic() - cached[0]) < _SETUP_CACHE_TTL_SECONDS:
        return IDRConfig(**cached[1])

    try:
        from idr_core.config import sql_to_config

        # 1. Try to reconstruct from actual metadata tables (Source of Truth)
        active_config = sql_to_config(adapter)
        if active_config and len(active_config.get("sources", [])) > 0:
            _CONFIG_CACHE[user_key] = (time.monotonic(), active_config)
            return IDRConfig(**active_config)

        # 2. Fallback: Fetch latest config snapshot
        rows = adapter.query("""
            SELECT sources_json, rules_json, mappings_json
            FROM idr_out.config_snapshot
            ORDER BY created_at DESC
            LIMIT 1
        """)

        if not rows:
            return IDRConfig(sources=[])

        row = rows[0]
        if row.get("sources_json"):
            parsed = parse_config_snapshot_row(row)
            _CONFIG_CACHE[user_key] = (time.monotonic(), parsed)
            return IDRConfig(**parsed)

        return IDRConfig(sources=[])

    except Exception as e:
        print(f"Error fetching config: {e}")
        return IDRConfig(sources=[])


@router.get("/config/history")
def get_config_history(
    limit: int = 15, current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Return recent saved configuration snapshots from idr_out.config_snapshot for rollback."""
    adapter, _ = _require_user_adapter(current_user)
    return {"snapshots": get_config_snapshots(adapter, limit=limit)}


@router.get("/discover/tables")
async def list_tables(
    schema: Optional[str] = None, current_user: Dict[str, Any] = Depends(get_current_user)
):
    """List tables in the given schema/dataset."""
    adapter, _ = _require_user_adapter(current_user)

    try:
        tables = adapter.list_tables(schema)
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(500, f"Discovery failed: {str(e)}")


@router.get("/discover/columns")
async def list_columns(table: str, current_user: Dict[str, Any] = Depends(get_current_user)):
    """List columns for a specific table."""
    adapter, _ = _require_user_adapter(current_user)

    try:
        columns = adapter.get_table_columns(table)
        return {"columns": columns}
    except Exception as e:
        raise HTTPException(500, f"Column listing failed: {str(e)}")


class ConfigSaveRequest(BaseModel):
    config: Dict[str, Any]


_ACTIVE_SAVE_PROGRESS: Dict[str, Dict[str, Any]] = {}


def _execute_save_config(
    adapter,
    raw_config: Dict[str, Any],
    progress_cb: Optional[Any] = None,
    record_snapshot: bool = True,
) -> Dict[str, Any]:
    """Validate, clear stale metadata, and persist config using shared core helper."""
    return apply_config_to_warehouse(
        adapter,
        raw_config,
        progress_cb=progress_cb,
        record_snapshot=record_snapshot,
    )


@router.post("/config/save")
async def save_config(
    request: ConfigSaveRequest, current_user: Dict[str, Any] = Depends(require_write_access)
):
    """Save configuration to idr_meta tables using batched SQL writes."""
    adapter, user_key = _require_user_adapter(current_user)

    try:
        res = _execute_save_config(adapter, request.config)
        _invalidate_setup_cache(user_key)
        return res
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(500, f"Failed to save configuration: {str(e)}")


@router.post("/config/save/async")
async def save_config_async(
    request: ConfigSaveRequest, current_user: Dict[str, Any] = Depends(require_write_access)
):
    """Start saving configuration asynchronously with live progress tracking."""
    import threading
    import time

    adapter, user_key = _require_user_adapter(current_user)
    _invalidate_setup_cache(user_key)

    existing = _ACTIVE_SAVE_PROGRESS.get(user_key)
    if existing and existing.get("status") == "SAVING":
        return {"status": "already_saving", "message": "A configuration save is already in progress."}

    _ACTIVE_SAVE_PROGRESS[user_key] = {
        "status": "SAVING",
        "step": "validating",
        "label": "Validating configuration",
        "percent": 5,
        "detail": "Starting configuration save...",
        "started_epoch": time.time(),
        "elapsed_seconds": 0.0,
        "message": None,
        "error": None,
    }

    def _progress_cb(step: str, label: str, percent: int, detail: str) -> None:
        prog = _ACTIVE_SAVE_PROGRESS.get(user_key)
        if not prog:
            return
        prog["step"] = step
        prog["label"] = label
        prog["percent"] = percent
        prog["detail"] = detail
        prog["elapsed_seconds"] = round(time.time() - prog["started_epoch"], 1)

    def _worker() -> None:
        try:
            result = _execute_save_config(adapter, request.config, progress_cb=_progress_cb)
            prog = _ACTIVE_SAVE_PROGRESS[user_key]
            prog["status"] = "SUCCESS"
            prog["percent"] = 100
            prog["message"] = result.get("message")
            prog["elapsed_seconds"] = round(time.time() - prog["started_epoch"], 1)
        except Exception as e:
            import traceback

            traceback.print_exc()
            prog = _ACTIVE_SAVE_PROGRESS[user_key]
            prog["status"] = "ERROR"
            prog["error"] = str(e)
            prog["detail"] = str(e)
            prog["elapsed_seconds"] = round(time.time() - prog["started_epoch"], 1)

    threading.Thread(target=_worker, daemon=True).start()
    return {"status": "started", "message": "Configuration save started"}


@router.get("/config/save/progress")
async def get_save_config_progress(current_user: Dict[str, Any] = Depends(get_current_user)):
    """Return live progress for the active or most recent config save."""
    import time

    user_key = get_user_key(current_user)
    prog = _ACTIVE_SAVE_PROGRESS.get(user_key)
    if not prog:
        return JSONResponse(
            content={"status": "IDLE", "percent": 0},
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )
    snapshot = copy.deepcopy(prog)
    if snapshot.get("status") == "SAVING" and snapshot.get("started_epoch"):
        snapshot["elapsed_seconds"] = round(time.time() - snapshot["started_epoch"], 1)
    return JSONResponse(
        content=snapshot,
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


class RunRequest(BaseModel):
    mode: str = "FULL"
    strict: bool = False
    max_iterations: int = 30
    dry_run: bool = False
    max_group_size: Optional[int] = None
    max_moved_entities_pct: Optional[float] = None
    max_cluster_delta_pct: Optional[float] = None
    lens: Optional[str] = None
    skip_lenses: bool = False
    config: Optional[Dict[str, Any]] = None


_ACTIVE_RUN_PROGRESS: Dict[str, Dict[str, Any]] = {}
_RUN_EVENT_QUEUES: Dict[str, List[asyncio.Queue]] = {}


def _get_active_progress(user_key: str) -> Optional[Dict[str, Any]]:
    """Return active run progress for user_key, with fallback to default scope or latest active run."""
    return (
        _ACTIVE_RUN_PROGRESS.get(user_key)
        or _ACTIVE_RUN_PROGRESS.get("__default__")
        or (next(reversed(list(_ACTIVE_RUN_PROGRESS.values())), None) if _ACTIVE_RUN_PROGRESS else None)
    )


def _broadcast_progress_event(user_key: str, event_data: Dict[str, Any]) -> None:
    """Broadcast progress event to all active SSE queues for this user, default scope, or global listeners."""
    targets = {user_key, "__default__", "*"}
    for key in targets:
        for q in list(_RUN_EVENT_QUEUES.get(key, [])):
            try:
                q.put_nowait(event_data)
            except Exception:
                pass


def _build_initial_progress(
    mode: str, dry_run: bool, strict: bool, has_lenses: bool = False
) -> Dict[str, Any]:
    import time

    stage_defs = [
        {"name": "preflight", "label": "Preflight & Schema Check"},
        {"name": "entity_extraction", "label": "Entity Extraction"},
        {"name": "identifier_extraction", "label": "Identifier Extraction"},
        {"name": "attribute_extraction", "label": "Attribute Extraction"},
        {"name": "edge_building", "label": "Edge Building"},
        {"name": "label_propagation", "label": "Label Propagation (Graph)"},
        *([] if strict else [{"name": "fuzzy_matching", "label": "Fuzzy Matching"}]),
        {
            "name": "dry_run_analysis" if dry_run else "output_generation",
            "label": "Dry Run Analysis" if dry_run else "Output & Golden Profiles",
        },
        *(
            [{"name": "lens_resolution", "label": "Multi-Lens Views"}]
            if (has_lenses and not dry_run)
            else []
        ),
    ]
    return {
        "run_id": None,
        "status": "RUNNING",
        "mode": mode,
        "dry_run": dry_run,
        "started_epoch": time.time(),
        "elapsed_seconds": 0.0,
        "current_stage": "preflight",
        "current_detail": "Initializing pipeline...",
        "stages": [
            {
                "name": s["name"],
                "label": s["label"],
                "order": idx + 1,
                "status": "pending",
                "duration_seconds": None,
                "rows_affected": None,
                "detail": None,
            }
            for idx, s in enumerate(stage_defs)
        ],
    }


@router.get("/run/progress")
async def get_run_progress(current_user: Dict[str, Any] = Depends(get_current_user)):
    """Return live stage-by-stage progress for the active or most recent pipeline run."""
    import time

    user_key = get_user_key(current_user)
    prog = _get_active_progress(user_key)
    if not prog:
        return JSONResponse(
            content={"status": "IDLE", "stages": []},
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )

    snapshot = copy.deepcopy(prog)
    if snapshot.get("status") == "RUNNING" and snapshot.get("started_epoch"):
        snapshot["elapsed_seconds"] = round(time.time() - snapshot["started_epoch"], 1)
    return JSONResponse(
        content=snapshot,
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@router.get("/run/events")
async def stream_run_events(
    request: Request,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    """Server-Sent Events (SSE) streaming live stage transitions and iterations."""
    user_key = get_user_key(current_user)
    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    _RUN_EVENT_QUEUES.setdefault(user_key, []).append(q)

    async def event_generator():
        try:
            # Yield current progress snapshot immediately upon connection
            initial = _get_active_progress(user_key)
            if initial:
                yield f"data: {json.dumps(copy.deepcopy(initial))}\n\n"
            # When no run is actively executing, close stream cleanly instead of hanging forever
            if not initial or initial.get("status") not in ("RUNNING", "STARTING"):
                return
            idle_ticks = 0
            while True:
                if await request.is_disconnected():
                    break
                try:
                    evt = await asyncio.wait_for(q.get(), timeout=1.0)
                    idle_ticks = 0
                    yield f"data: {json.dumps(evt)}\n\n"
                    if evt.get("event") == "run_complete":
                        break
                except asyncio.TimeoutError:
                    idle_ticks += 1
                    if idle_ticks >= 15:
                        idle_ticks = 0
                        yield ": keepalive\n\n"
        finally:
            if user_key in _RUN_EVENT_QUEUES and q in _RUN_EVENT_QUEUES[user_key]:
                _RUN_EVENT_QUEUES[user_key].remove(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/run")
async def run_pipeline(
    request: RunRequest, current_user: Dict[str, Any] = Depends(require_write_access)
):
    """Trigger the IDR pipeline with live stage progress tracking."""
    import time

    adapter, user_key = _require_user_adapter(current_user)
    _invalidate_setup_cache(user_key)
    meta_snapshots: Dict[str, bool] = {}

    try:
        from dataclasses import asdict

        from idr_core.runner import IDRRunner, RunConfig

        if request.dry_run and request.config is not None:
            IDRRunner.check_no_active_run_lock(adapter)
            meta_snapshots = snapshot_meta_tables(adapter)
            _execute_save_config(adapter, request.config, record_snapshot=False)

        config = RunConfig(
            run_mode=request.mode,
            strict=request.strict,
            max_iters=request.max_iterations,
            dry_run=request.dry_run,
            max_group_size=request.max_group_size,
            max_moved_entities_pct=request.max_moved_entities_pct,
            max_cluster_delta_pct=request.max_cluster_delta_pct,
            lens=request.lens,
            skip_lenses=request.skip_lenses,
        )

        skip_lenses = request.skip_lenses or (request.lens or "").strip().lower() in ("__none__", "none")
        has_lenses = bool(request.lens) and not skip_lenses
        if not has_lenses and not skip_lenses and not config.dry_run:
            try:
                if adapter.table_exists("idr_meta.lens_definition"):
                    cnt = adapter.query_one(
                        "SELECT COUNT(*) FROM idr_meta.lens_definition WHERE is_active = TRUE"
                    )
                    has_lenses = bool(cnt and int(cnt) > 0)
            except Exception:
                has_lenses = False

        progress_state = _build_initial_progress(
            config.run_mode, config.dry_run, config.strict, has_lenses=has_lenses
        )
        _ACTIVE_RUN_PROGRESS[user_key] = progress_state
        _ACTIVE_RUN_PROGRESS["__default__"] = progress_state
        _broadcast_progress_event(user_key, {"event": "run_start", "progress": copy.deepcopy(progress_state)})

        def _on_progress(evt: Dict[str, Any]) -> None:
            etype = evt.get("event")
            if evt.get("run_id"):
                progress_state["run_id"] = evt["run_id"]
            progress_state["elapsed_seconds"] = round(
                time.time() - progress_state["started_epoch"], 1
            )
            if etype == "stage_start":
                st_name = evt.get("stage")
                progress_state["current_stage"] = st_name
                progress_state["current_detail"] = f"Running {st_name.replace('_', ' ')}..."
                for st in progress_state["stages"]:
                    if st["name"] == st_name:
                        st["status"] = "running"
                        st["started_epoch"] = time.time()
            elif etype == "stage_progress":
                st_name = evt.get("stage")
                it = evt.get("iteration")
                max_it = evt.get("max_iters")
                chg = evt.get("changes", 0)
                detail = f"Iteration {it}/{max_it} ({chg:,} label updates)"
                progress_state["current_detail"] = detail
                for st in progress_state["stages"]:
                    if st["name"] == st_name:
                        st["detail"] = detail
            elif etype == "stage_end":
                st_name = evt.get("stage")
                for st in progress_state["stages"]:
                    if st["name"] == st_name:
                        st["status"] = "completed"
                        st["duration_seconds"] = evt.get("duration_seconds", 0.0)
                        st["rows_affected"] = evt.get("rows_affected", 0)
            elif etype == "run_complete":
                progress_state["status"] = evt.get("status", "SUCCESS")
                progress_state["elapsed_seconds"] = evt.get(
                    "duration_seconds", progress_state["elapsed_seconds"]
                )
                progress_state["current_stage"] = None
                progress_state["current_detail"] = f"Completed ({progress_state['status']})"
                for st in progress_state["stages"]:
                    if st["status"] == "pending":
                        st["status"] = "skipped"

            # Broadcast event with full progress snapshot to SSE listeners
            _broadcast_progress_event(
                user_key,
                {
                    "event": etype,
                    "stage": evt.get("stage"),
                    "progress": copy.deepcopy(progress_state),
                },
            )

        runner = IDRRunner(adapter, progress_callback=_on_progress)
        result = await asyncio.to_thread(runner.run, config)
        res_dict = asdict(result)

        # Record Prometheus pipeline metrics
        try:
            from idr_api.main import (
                CLUSTERS_IMPACTED_TOTAL,
                EDGES_CREATED_TOTAL,
                ENTITIES_PROCESSED_TOTAL,
                LAST_RUN_DURATION_GAUGE,
            )

            if getattr(result, "entities_processed", 0):
                ENTITIES_PROCESSED_TOTAL.inc(max(0, int(result.entities_processed)))
            if getattr(result, "edges_created", 0):
                EDGES_CREATED_TOTAL.inc(max(0, int(result.edges_created)))
            if getattr(result, "clusters_impacted", 0):
                CLUSTERS_IMPACTED_TOTAL.inc(max(0, int(result.clusters_impacted)))
            if getattr(result, "duration_seconds", None) is not None:
                LAST_RUN_DURATION_GAUGE.set(float(result.duration_seconds))
        except Exception:
            pass

        # If dry run, fetch summary metrics to return to UI
        if request.dry_run and result.status == "DRY_RUN_COMPLETE":
            try:
                res_dict["dry_run_summary"] = compute_dry_run_summary(
                    adapter,
                    result.run_id,
                    config.run_mode,
                    edges_created=getattr(result, "edges_created", 0),
                    clusters_impacted=getattr(result, "clusters_impacted", 0),
                )
            except Exception as e:
                print(f"Failed to fetch dry run summary: {e}")
                res_dict["dry_run_summary"] = {"error": str(e)}

        return res_dict

    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(500, f"Pipeline run failed: {str(e)}")
    finally:
        if meta_snapshots:
            restore_meta_tables(adapter, meta_snapshots)


@router.get("/fuzzy-templates")
async def get_fuzzy_templates(current_user: Dict[str, Any] = Depends(get_current_user)):
    """Get supported fuzzy matching templates for the current platform."""
    adapter, _ = _require_user_adapter(current_user)

    dialect = adapter.dialect

    templates = []

    # Common templates approach
    # We define the template with placeholders <a> and <b> for entity aliases

    if dialect == "duckdb":
        templates = [
            {
                "id": "jaro_winkler",
                "label": "Jaro-Winkler Similarity",
                "sql_template": "jaro_winkler_similarity(<a>.{col}, <b>.{col})",
                "default_threshold": 0.85,
                "description": "Best for names and short strings. Returns 0.0-1.0.",
            },
            {
                "id": "levenshtein",
                "label": "Levenshtein Similarity",
                "sql_template": "(1.0 - (levenshtein(<a>.{col}, <b>.{col})::FLOAT / GREATEST(LENGTH(<a>.{col}), LENGTH(<b>.{col}))))",
                "default_threshold": 0.8,
                "description": "Edit distance ratio. Good for minor typos.",
            },
        ]
    elif dialect == "snowflake":
        templates = [
            {
                "id": "jaro_winkler",
                "label": "Jaro-Winkler Similarity",
                "sql_template": "(JAROWINKLER_SIMILARITY(<a>.{col}, <b>.{col}) / 100.0)",
                "default_threshold": 0.85,
                "description": "Normalized Snowflake Jaro-Winkler (0.0-1.0 scale).",
            },
            {
                "id": "edit_distance",
                "label": "Edit Distance Ratio",
                "sql_template": "(1.0 - (EDITDISTANCE(<a>.{col}, <b>.{col}) / GREATEST(LENGTH(<a>.{col}), LENGTH(<b>.{col}))))",
                "default_threshold": 0.8,
                "description": "Normalized Levenshtein distance.",
            },
        ]
    elif dialect == "bigquery":
        templates = [
            {
                "id": "levenshtein_ratio",
                "label": "Levenshtein Ratio (Native)",
                "sql_template": "(1.0 - SAFE_DIVIDE(LEVENSHTEIN(<a>.{col}, <b>.{col}), GREATEST(LENGTH(<a>.{col}), LENGTH(<b>.{col}))))",
                "default_threshold": 0.8,
                "description": "Native BigQuery edit distance normalized.",
            },
            {
                "id": "soundex",
                "label": "Soundex Phonetic Match",
                "sql_template": "CASE WHEN SOUNDEX(<a>.{col}) = SOUNDEX(<b>.{col}) THEN 1.0 ELSE 0.0 END",
                "default_threshold": 1.0,
                "description": "Phonetic similarity using BigQuery SOUNDEX.",
            },
        ]
    elif dialect == "databricks":
        templates = [
            {
                "id": "jaro_winkler",
                "label": "Jaro-Winkler",
                "sql_template": "jaro_winkler(<a>.{col}, <b>.{col})",
                "default_threshold": 0.85,
                "description": "Spark native Jaro-Winkler.",
            },
            {
                "id": "levenshtein",
                "label": "Levenshtein",
                "sql_template": "(1.0 - (levenshtein(<a>.{col}, <b>.{col}) / greatest(length(<a>.{col}), length(<b>.{col}))))",
                "default_threshold": 0.8,
                "description": "Edit distance ratio.",
            },
        ]

    return {"templates": templates}
