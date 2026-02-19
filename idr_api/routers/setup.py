from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from idr_core.schema_manager import SchemaManager

from ..dependencies import get_current_user, get_manager, get_user_key

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

                conn = snowflake.connector.connect(
                    user=params.get("user"),
                    password=params.get("password"),
                    account=params.get("account"),
                    warehouse=params.get("warehouse"),
                    database=params.get("database"),
                    schema=params.get("schema"),
                )
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

                conn = sql.connect(
                    server_hostname=params.get("server_hostname"),
                    http_path=params.get("http_path"),
                    access_token=params.get("access_token"),
                )
                adapter = DatabricksAdapter(conn, catalog=params.get("catalog"))
            except ImportError:
                raise HTTPException(400, "Databricks connector not installed")

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
    """Check connection and configuration status."""
    user_key = get_user_key(current_user)
    connected = manager.is_connected(user_key)
    configured = False
    platform = None

    if connected:
        adapter = manager.get_adapter_for_user(user_key)
        platform = manager.get_config_for_user(user_key).get("platform")
        # Lightweight check: does idr_meta.source_table exist and have rows?
        try:
            # Basic check if metadata tables exist
            if adapter.table_exists("idr_meta.source_table"):
                # Count rows
                count = adapter.query_one("SELECT COUNT(*) FROM idr_meta.source_table")
                configured = count > 0
        except Exception:
            configured = False

    return {"connected": connected, "configured": configured, "platform": platform}


# Define config model locally since it's not in idr_core
class IDRConfig(BaseModel):
    sources: List[Dict[str, Any]] = []
    rules: Optional[List[Dict[str, Any]]] = None
    survivorship: Optional[List[Dict[str, Any]]] = None
    # Add other fields as loose dicts to avoid strict validation errors
    # if the stored JSON varies slightly


@router.get("/config", response_model=IDRConfig)
def get_current_config(current_user: Dict[str, Any] = Depends(get_current_user)):
    """Get the currently active configuration from the latest snapshot."""
    adapter, _ = _require_user_adapter(current_user)

    try:
        from idr_core.config import sql_to_config

        # 1. Try to reconstruct from actual metadata tables (Source of Truth)
        # Check if tables exist first to avoid errors on fresh DB
        if adapter.table_exists("idr_meta.source_table"):
            active_config = sql_to_config(adapter)
            if active_config and len(active_config.get("sources", [])) > 0:
                print("Loaded config from SQL Metadata tables.")
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

            config_dict = {"sources": sources_list}

            if row.get("rules_json"):
                config_dict["rules"] = json.loads(row["rules_json"])

            if row.get("mappings_json"):
                # mappings might be survivorship or similar?
                # Adjust based on how config_to_sql saves it.
                pass

            # Actually, simply returning IDRConfig(sources=...) is often enough for the wizard to start
            # But for full restoration, we want rules too.
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


@router.post("/config/save")
async def save_config(
    request: ConfigSaveRequest, current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Save configuration to idr_meta tables."""
    adapter, _ = _require_user_adapter(current_user)

    try:
        from idr_core.config import config_to_sql, validate_config

        # Validate config before generating SQL
        validate_config(request.config)

        # 1. Ensure metadata tables exist
        schema_manager = SchemaManager(adapter)
        schema_manager.initialize(reset=False)

        # 2. Generate SQL from config
        statements = config_to_sql(request.config, dialect=adapter.dialect)

        # 3. Snapshot the config JSON so we can reload it later
        import hashlib
        import json
        from datetime import datetime

        sources_json = json.dumps(request.config.get("sources", []))
        rules_json = json.dumps(request.config.get("rules", []))
        mappings_json = json.dumps(
            request.config.get("survivorship", [])
        )  # Storing survivorship in mappings_json for now

        # Create a hash of the config
        config_str = sources_json + rules_json + mappings_json
        config_hash = hashlib.md5(config_str.encode("utf-8")).hexdigest()

        # Determine how to insert based on dialect
        # DuckDB handles INSERT OR IGNORE, others MERGE.
        # idr_out.config_snapshot has columns: config_hash, sources_json, rules_json, mappings_json, created_at

        current_ts = datetime.utcnow().isoformat()

        # Simple INSERT, duplicates on hash don't strictly matter as we order by created_at DESC LIMIT 1
        # But let's try to be clean.

        # Escape JSON for SQL
        def escape_json(s):
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

        # 4. Execute all statements
        count = 0
        for stmt in statements:
            adapter.execute(stmt)
            count += 1

        # Execute snapshot last
        try:
            adapter.execute(snapshot_sql)
        except Exception as snapshot_err:
            print(f"Warning: Failed to save config snapshot: {snapshot_err}")
            # Non-critical for the run itself, but bad for UI reload.

        return {"status": "ok", "message": f"Configuration saved ({count} statements executed)"}

    except Exception as e:
        raise HTTPException(500, f"Failed to save configuration: {str(e)}")


class RunRequest(BaseModel):
    mode: str = "FULL"
    strict: bool = False
    max_iterations: int = 30
    dry_run: bool = False


@router.post("/run")
async def run_pipeline(request: RunRequest, current_user: Dict[str, Any] = Depends(get_current_user)):
    """Trigger the IDR pipeline."""
    adapter, _ = _require_user_adapter(current_user)

    try:
        from dataclasses import asdict

        from idr_core.runner import IDRRunner, RunConfig

        runner = IDRRunner(adapter)
        config = RunConfig(
            run_mode=request.mode,
            strict=request.strict,
            max_iters=request.max_iterations,
            dry_run=request.dry_run,
        )

        result = runner.run(config)
        res_dict = asdict(result)

        # If dry run, fetch summary metrics to return to UI
        if request.dry_run and result.status == "DRY_RUN_COMPLETE":
            try:
                # Assuming idr_out.dry_run_summary table exists and has specific columns
                # We can fetch a simple summary count for now
                if adapter.table_exists("idr_out.dry_run_summary"):
                    summary_rows = adapter.query(
                        f"SELECT COUNT(*) as proposed_changes FROM idr_out.dry_run_summary WHERE run_id = '{result.run_id}'"
                    )
                    res_dict["dry_run_summary"] = {
                        "proposed_changes": summary_rows[0]["proposed_changes"]
                        if summary_rows
                        else 0
                    }
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
                "sql_template": "JAROWINKLER_SIMILARITY(<a>.{col}, <b>.{col})",
                "default_threshold": 85,
                "description": "Native Snowflake function (0-100 scale). UI normalizes to 0-1.",
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
            # BigQuery needs JS UDFs usually, but we can use generic edit distance logic
            {
                "id": "levenshtein_ratio",
                "label": "Levenshtein Ratio (Native)",
                "sql_template": "(1.0 - SAFE_DIVIDE(LEVENSHTEIN(<a>.{col}, <b>.{col}), GREATEST(LENGTH(<a>.{col}), LENGTH(<b>.{col}))))",
                "default_threshold": 0.8,
                "description": "Native BigQuery edit distance normalized.",
            }
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
