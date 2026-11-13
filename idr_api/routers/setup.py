import asyncio
import copy
import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from idr_core.schema_manager import SchemaManager

from ..dependencies import get_current_user, get_manager, get_user_key, require_write_access

router = APIRouter(prefix="/api/setup", tags=["setup"])
manager = get_manager()


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

                raw_account = (params.get("account") or "").strip()
                for prefix in ("https://", "http://"):
                    if raw_account.lower().startswith(prefix):
                        raw_account = raw_account[len(prefix):]
                raw_account = raw_account.split("/")[0].strip()
                if raw_account.lower().endswith(".snowflakecomputing.com"):
                    raw_account = raw_account[: -len(".snowflakecomputing.com")]

                sf_kwargs = {
                    "user": (params.get("user") or "").strip(),
                    "password": params.get("password"),
                    "account": raw_account,
                    "warehouse": (params.get("warehouse") or "").strip() or None,
                    "database": (params.get("database") or "").strip() or None,
                    "schema": (params.get("schema") or "").strip() or None,
                    "login_timeout": 20,
                }
                if params.get("role"):
                    sf_kwargs["role"] = str(params["role"]).strip()
                if params.get("authenticator"):
                    sf_kwargs["authenticator"] = str(params["authenticator"]).strip()

                conn = snowflake.connector.connect(**{k: v for k, v in sf_kwargs.items() if v is not None})
                adapter = SnowflakeAdapter(conn)
            except ImportError:
                raise HTTPException(400, "Snowflake connector not installed")

        elif request.platform == "bigquery":
            try:
                import json

                from google.cloud import bigquery
                from google.oauth2 import service_account

                from idr_core.adapters.bigquery import BigQueryAdapter

                project = params.get("project")
                credentials_json = params.get("credentials_json") or params.get("credentials")

                if credentials_json:
                    info = json.loads(credentials_json)
                    creds = service_account.Credentials.from_service_account_info(info)
                    client = bigquery.Client(project=project, credentials=creds)
                else:
                    # Try default auth
                    client = bigquery.Client(project=project)

                adapter = BigQueryAdapter(
                    client, project=project, location=params.get("location", "US")
                )
            except ImportError:
                raise HTTPException(400, "BigQuery client not installed")

        elif request.platform == "databricks":
            try:
                from databricks import sql

                from idr_core.adapters.databricks import DatabricksAdapter

                raw_host = params.get("server_hostname") or ""
                clean_host = raw_host.strip("'\" \t").replace("https://", "").replace("http://", "").split("/")[0]
                http_path = (params.get("http_path") or "").strip("'\" \t")
                token = (params.get("access_token") or "").strip("'\" \t")
                catalog = (params.get("catalog") or "main").strip("'\" \t")

                conn = sql.connect(
                    server_hostname=clean_host,
                    http_path=http_path,
                    access_token=token,
                )
                adapter = DatabricksAdapter(conn, catalog=catalog)
            except ImportError:
                raise HTTPException(400, "Databricks connector not installed. Run: pip install databricks-sql-connector")

        else:
            raise HTTPException(400, f"Unsupported platform: {request.platform}")

        if adapter:
            manager.set_adapter_for_user(
                get_user_key(current_user), adapter, {"platform": request.platform, **params}
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
        adapter = manager.get_adapter_for_user(user_key)
        platform = manager.get_config_for_user(user_key).get("platform")

        # Check aliveness
        is_alive = True
        try:
            adapter.query_one("SELECT 1")
        except Exception:
            is_alive = False

        if not is_alive:
            state = "STALE"
            suggested_action = "reconnect_warehouse"
        else:
            try:
                if adapter.table_exists("idr_meta.source_table"):
                    count = adapter.query_one("SELECT COUNT(*) FROM idr_meta.source_table")
                    configured = (count or 0) > 0
                    if configured:
                        # Check if runs have been executed
                        if adapter.table_exists("idr_out.identity_clusters_current"):
                            clusters = adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_clusters_current")
                            has_runs = (clusters or 0) > 0

                        if has_runs:
                            state = "RESOLVED"
                            suggested_action = "explore_identity_graph"
                        else:
                            state = "CONFIGURED_READY"
                            suggested_action = "trigger_resolution_run"
                    else:
                        state = "CONNECTED_UNCONFIGURED"
                        suggested_action = "discover_and_map_sources"
                else:
                    state = "CONNECTED_UNINITIALIZED"
                    suggested_action = "initialize_metadata"
            except Exception:
                configured = False
                state = "CONNECTED_UNCONFIGURED"
                suggested_action = "discover_and_map_sources"

    return {
        "connected": connected,
        "configured": configured,
        "platform": platform,
        "state": state,
        "has_runs": has_runs,
        "suggested_action": suggested_action,
    }


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
    use_default_exclusions: Optional[bool] = False


@router.get("/config", response_model=IDRConfig)
def get_current_config(current_user: Dict[str, Any] = Depends(get_current_user)):
    """Get the currently active configuration from metadata tables or latest snapshot."""
    adapter, _ = _require_user_adapter(current_user)

    try:
        from idr_core.config import sql_to_config

        # 1. Try to reconstruct from actual metadata tables (Source of Truth)
        # Check if tables exist first to avoid errors on fresh DB
        if adapter.table_exists("idr_meta.source_table"):
            active_config = sql_to_config(adapter)
            if active_config and len(active_config.get("sources", [])) > 0:
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

        import json

        row = rows[0]

        # Reconstruct IDRConfig from the snapshot
        if row.get("sources_json"):
            sources_list = json.loads(row["sources_json"])

            # Map backend keys to frontend keys if needed (Shim for legacy/drifted data)
            for src in sources_list:
                if "table_id" in src and "id" not in src:
                    src["id"] = src["table_id"]
                if "table_fqn" in src and "table" not in src:
                    src["table"] = src["table_fqn"]
                if "entity_key_expr" in src and "entity_key" not in src:
                    src["entity_key"] = src["entity_key_expr"]
                if "attributes" not in src:
                    src["attributes"] = []
                for ident in src.get("identifiers", []):
                    if "expr" in ident and "column" not in ident:
                        ident["column"] = ident["expr"]
                for attr in src.get("attributes", []):
                    if "expr" in attr and "column" not in attr:
                        attr["column"] = attr["expr"]

            config_dict: Dict[str, Any] = {"sources": sources_list}

            if row.get("rules_json"):
                parsed_rules = json.loads(row["rules_json"])
                if isinstance(parsed_rules, dict):
                    config_dict["rules"] = parsed_rules.get("rules", [])
                    config_dict["fuzzy_rules"] = parsed_rules.get("fuzzy_rules", [])
                    config_dict["exclusions"] = parsed_rules.get("exclusions", [])
                    config_dict["overrides"] = parsed_rules.get("overrides", [])
                    config_dict["use_default_exclusions"] = parsed_rules.get("use_default_exclusions", False)
                else:
                    config_dict["rules"] = parsed_rules

            if row.get("mappings_json"):
                config_dict["survivorship"] = json.loads(row["mappings_json"])

            return IDRConfig(**config_dict)

        return IDRConfig(sources=[])

    except Exception as e:
        print(f"Error fetching config: {e}")
        # Return empty config rather than failing, so UI can just start fresh if needed
        return IDRConfig(sources=[])


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
) -> Dict[str, Any]:
    """Validate, clear stale metadata, and persist config using batched SQL statements."""
    import hashlib
    import json
    from datetime import datetime, timezone

    from idr_core.config import config_to_sql, validate_config

    def _report(step: str, label: str, percent: int, detail: str = "") -> None:
        if progress_cb:
            progress_cb(step, label, percent, detail)

    _report("validating", "Validating configuration", 10, "Checking sources, rules, and mappings...")
    cfg = dict(raw_config or {})
    for key in ("sources", "rules", "fuzzy_rules", "survivorship", "exclusions", "overrides"):
        if cfg.get(key) is None:
            cfg[key] = []
    for src in cfg.get("sources", []):
        if isinstance(src, dict):
            if src.get("identifiers") is None:
                src["identifiers"] = []
            if src.get("attributes") is None:
                src["attributes"] = []

    validate_config(cfg)

    _report("schema", "Ensuring metadata schema", 20, "Checking idr_meta & idr_out tables...")
    schema_manager = SchemaManager(adapter)
    schema_manager.initialize(reset=False)

    _report("clearing", "Clearing stale metadata", 30, "Removing obsolete mappings & rules...")
    for meta_table in (
        "idr_meta.identifier_mapping",
        "idr_meta.entity_attribute_mapping",
        "idr_meta.source_table",
        "idr_meta.rule",
        "idr_meta.fuzzy_rule",
        "idr_meta.survivorship_rule",
    ):
        try:
            if adapter.table_exists(meta_table):
                adapter.execute(f"DELETE FROM {meta_table} WHERE 1=1")
        except Exception:
            pass

    try:
        if adapter.table_exists("idr_meta.identifier_exclusion"):
            adapter.execute(
                "DELETE FROM idr_meta.identifier_exclusion "
                "WHERE COALESCE(reason, '') IN ('UI exclusion', 'Config exclusion')"
            )
        if adapter.table_exists("idr_meta.identity_override"):
            adapter.execute(
                "DELETE FROM idr_meta.identity_override "
                "WHERE COALESCE(created_by, 'config') IN ('config', 'ui')"
            )
    except Exception:
        pass

    statements = config_to_sql(cfg, dialect=adapter.dialect, batched=True)
    total_batches = len(statements)

    sources_json = json.dumps(cfg.get("sources", []))
    rules_json = json.dumps(
        {
            "rules": cfg.get("rules", []),
            "fuzzy_rules": cfg.get("fuzzy_rules", []),
            "exclusions": cfg.get("exclusions", []),
            "overrides": cfg.get("overrides", []),
            "use_default_exclusions": bool(cfg.get("use_default_exclusions", False)),
        }
    )
    mappings_json = json.dumps(cfg.get("survivorship", []))
    config_str = sources_json + rules_json + mappings_json
    config_hash = hashlib.md5(config_str.encode("utf-8")).hexdigest()
    current_ts = datetime.now(timezone.utc).isoformat()

    def escape_json(s: str) -> str:
        if adapter.dialect in ("bigquery", "databricks"):
            return s.replace("'", "\\'")
        return s.replace("'", "''")

    sj = escape_json(sources_json)
    rj = escape_json(rules_json)
    mj = escape_json(mappings_json)

    snapshot_sql = f"""
        INSERT INTO idr_out.config_snapshot (config_hash, sources_json, rules_json, mappings_json, created_at)
        VALUES ('{config_hash}', '{sj}', '{rj}', '{mj}', '{current_ts}')
    """

    count = 0
    for idx, stmt in enumerate(statements, start=1):
        pct = 35 + int((idx / max(total_batches, 1)) * 55)
        _report(
            "writing",
            "Writing metadata batches",
            pct,
            f"Executing batch {idx} of {total_batches}...",
        )
        adapter.execute(stmt)
        count += 1

    _report("snapshot", "Saving config snapshot", 95, "Recording versioned config snapshot...")
    try:
        adapter.execute(snapshot_sql)
    except Exception as snapshot_err:
        print(f"Warning: Failed to save config snapshot: {snapshot_err}")

    msg = f"Configuration saved ({count} batched statements executed)"
    _report("done", "Configuration saved", 100, msg)
    return {"status": "ok", "message": msg, "batches_executed": count}


@router.post("/config/save")
async def save_config(
    request: ConfigSaveRequest, current_user: Dict[str, Any] = Depends(require_write_access)
):
    """Save configuration to idr_meta tables using batched SQL writes."""
    adapter, _ = _require_user_adapter(current_user)

    try:
        return _execute_save_config(adapter, request.config)
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

    adapter, _ = _require_user_adapter(current_user)
    user_key = get_user_key(current_user)

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


def _build_initial_progress(mode: str, dry_run: bool, strict: bool) -> Dict[str, Any]:
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
async def stream_run_events(current_user: Dict[str, Any] = Depends(get_current_user)):
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
            while True:
                try:
                    evt = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {json.dumps(evt)}\n\n"
                    if evt.get("event") == "run_complete":
                        break
                except asyncio.TimeoutError:
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

    try:
        from dataclasses import asdict

        from idr_core.runner import IDRRunner, RunConfig

        config = RunConfig(
            run_mode=request.mode,
            strict=request.strict,
            max_iters=request.max_iterations,
            dry_run=request.dry_run,
            max_group_size=request.max_group_size,
        )

        progress_state = _build_initial_progress(config.run_mode, config.dry_run, config.strict)
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
                if adapter.table_exists("idr_out.dry_run_summary"):
                    summary_rows = adapter.query(
                        "SELECT new_entities, moved_entities, unchanged_entities, total_entities "
                        f"FROM idr_out.dry_run_summary WHERE run_id = '{result.run_id}'"
                    )
                    if summary_rows:
                        srow = summary_rows[0]
                        new_cnt = int(srow.get("new_entities") or 0)
                        moved_cnt = int(srow.get("moved_entities") or 0)
                        unchanged_cnt = int(srow.get("unchanged_entities") or 0)
                        total_cnt = int(srow.get("total_entities") or 0)
                        res_dict["dry_run_summary"] = {
                            "proposed_changes": new_cnt + moved_cnt,
                            "new_entities": new_cnt,
                            "moved_entities": moved_cnt,
                            "unchanged_entities": unchanged_cnt,
                            "total_entities": total_cnt,
                        }
                    else:
                        res_dict["dry_run_summary"] = {"proposed_changes": 0}
            except Exception as e:
                print(f"Failed to fetch dry run summary: {e}")
                res_dict["dry_run_summary"] = {"error": str(e)}

        return res_dict

    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(500, f"Pipeline run failed: {str(e)}")


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
