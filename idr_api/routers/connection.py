"""
Connection router — platform connect/disconnect.
"""

import os
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from ..dependencies import get_current_user, get_manager, get_user_key
from ..models import ConnectionRequest, ConnectionResponse

router = APIRouter(tags=["connection"])


def _get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """Get env var, return None if empty string."""
    val = os.environ.get(key, default)
    return val if val else default


@router.post("/api/connect", response_model=ConnectionResponse)
def connect(request: ConnectionRequest, current_user: dict = Depends(get_current_user)):
    """Connect to a data platform.

    Credentials can be provided via:
    1. Request body (UI input) - takes precedence
    2. Environment variables - fallback for production

    Env vars:
    - DuckDB: IDR_DATABASE (preferred), IDR_DB_PATH (legacy)
    - BigQuery: IDR_PROJECT, GOOGLE_APPLICATION_CREDENTIALS
    - Snowflake: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
                 SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE
    - Databricks: DATABRICKS_HOST, DATABRICKS_HTTP_PATH,
                  DATABRICKS_TOKEN, DATABRICKS_CATALOG
    """
    _manager = get_manager()
    user_key = get_user_key(current_user)

    try:
        if request.platform == "duckdb":
            import duckdb

            db_path = (
                request.database
                or _get_env("IDR_DATABASE")
                or _get_env("IDR_DB_PATH")
                or ":memory:"
            )
            # Sanitize path: strip quotes if user pasted them
            db_path = db_path.strip("'\"")

            # Read-write allows first-time setup by creating DB if it doesn't exist.
            conn = duckdb.connect(db_path, read_only=False)
            from idr_core.adapters.duckdb import DuckDBAdapter

            _manager.set_adapter_for_user(user_key, DuckDBAdapter(conn), request.model_dump())

        elif request.platform == "bigquery":
            from google.cloud import bigquery

            project = request.project_id or _get_env("IDR_PROJECT")
            client = bigquery.Client(project=project)
            from idr_core.adapters.bigquery import BigQueryAdapter

            _manager.set_adapter_for_user(user_key, BigQueryAdapter(client, project), request.model_dump())

        elif request.platform == "snowflake":
            # Use Snowflake Python Connector (not Snowpark)
            import snowflake.connector

            conn = snowflake.connector.connect(
                account=request.account or _get_env("SNOWFLAKE_ACCOUNT"),
                user=request.user or _get_env("SNOWFLAKE_USER"),
                password=request.password or _get_env("SNOWFLAKE_PASSWORD"),
                warehouse=request.warehouse or _get_env("SNOWFLAKE_WAREHOUSE"),
                database=request.sf_database or _get_env("SNOWFLAKE_DATABASE"),
                schema=request.schema_name or _get_env("SNOWFLAKE_SCHEMA", "PUBLIC"),
            )
            from idr_core.adapters.snowflake import SnowflakeAdapter

            _manager.set_adapter_for_user(user_key, SnowflakeAdapter(conn), request.model_dump())

        elif request.platform == "databricks":
            # Use Databricks SQL Connector
            from databricks import sql

            conn = sql.connect(
                server_hostname=request.server_hostname or _get_env("DATABRICKS_HOST"),
                http_path=request.http_path or _get_env("DATABRICKS_HTTP_PATH"),
                access_token=request.access_token or _get_env("DATABRICKS_TOKEN"),
            )
            from idr_core.adapters.databricks import DatabricksAdapter

            catalog = request.catalog or _get_env("DATABRICKS_CATALOG")
            _manager.set_adapter_for_user(
                user_key, DatabricksAdapter(conn, catalog=catalog), request.model_dump()
            )

        else:
            raise HTTPException(400, f"Unknown platform: {request.platform}")

        return ConnectionResponse(
            status="connected",
            platform=request.platform,
            message=f"Successfully connected to {request.platform}",
        )

    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/api/disconnect")
def disconnect(current_user: dict = Depends(get_current_user)):
    _manager = get_manager()
    user_key = get_user_key(current_user)
    _manager.disconnect_user(user_key)
    return {"status": "disconnected"}
