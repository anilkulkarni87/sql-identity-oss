"""
IDR API - FastAPI Backend for Identity Resolution UI

This API provides endpoints for:
- Match Quality Dashboard
- Identity Graph Explorer
- Run History & Metrics

All endpoints work across DuckDB, Snowflake, BigQuery, and Databricks.

Router modules:
- routers/connection.py  — platform connect
- routers/dashboard.py   — metrics, distribution, rules, alerts
- routers/explorer.py    — entity search, cluster detail
- routers/runs.py        — run history
- routers/schema.py      — system data model docs
- routers/setup.py       — metadata setup wizard
"""

import os
import sys
import time

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

# Add parent directory to path for idr_core imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from contextlib import asynccontextmanager

from idr_api.dependencies import get_manager
from idr_api.routers import connection, dashboard, explorer, runs, schema, setup

REQUEST_COUNT = Counter(
    "idr_http_requests_total",
    "Total HTTP requests handled by IDR API",
    ["method", "path", "status"],
)
REQUEST_LATENCY = Histogram(
    "idr_http_request_duration_seconds",
    "HTTP request latency in seconds for IDR API",
    ["method", "path"],
)
DB_CONNECTED_GAUGE = Gauge("idr_api_db_connected", "Database connectivity status (1=connected)")


def _parse_cors_origins(value: str) -> list[str]:
    """Parse comma-separated CORS origins from env."""
    origins = [origin.strip() for origin in value.split(",") if origin.strip()]
    return origins or ["http://localhost:3000", "http://localhost:5173"]


def _route_path_template(scope: dict, raw_path: str) -> str:
    """
    Get the route template (e.g. /api/clusters/{resolved_id}) to avoid
    high-cardinality metrics labels from dynamic IDs in URLs.
    """
    route = scope.get("route")
    template = getattr(route, "path", None)
    return template or raw_path


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Auto-connect to DuckDB via env vars or local demo DB."""
    from idr_core.logger_utils import configure_logging

    configure_logging()

    _manager = get_manager()

    env_db_path = os.getenv("IDR_DATABASE") or os.getenv("IDR_DB_PATH")

    if env_db_path:
        try:
            import duckdb

            conn = duckdb.connect(env_db_path)
            from idr_core.adapters.duckdb import DuckDBAdapter

            _manager.set_adapter(
                DuckDBAdapter(conn), {"platform": "duckdb", "db_path": env_db_path}
            )
            print(f"✓ Auto-connected to DuckDB via env: {env_db_path}")
            yield
            return
        except Exception as e:
            print(f"Failed to auto-connect to {env_db_path}: {e}")

    # Fallback: check for local demo DB
    db_paths = [
        "../retail_test.duckdb",
        "retail_test.duckdb",
        os.path.join(os.path.dirname(__file__), "..", "retail_test.duckdb"),
    ]

    for db_path in db_paths:
        if os.path.exists(db_path):
            try:
                import duckdb

                conn = duckdb.connect(db_path, read_only=True)
                from idr_core.adapters.duckdb import DuckDBAdapter

                _manager.set_adapter(
                    DuckDBAdapter(conn), {"platform": "duckdb", "db_path": db_path}
                )
                print(f"✓ Auto-connected to DuckDB: {db_path}")
                break
            except Exception as e:
                print(f"Failed to connect to {db_path}: {e}")
    else:
        print("⚠ No DuckDB database found. Call POST /api/connect to connect.")

    yield


app = FastAPI(
    title="IDR API",
    description="Identity Resolution Dashboard & Explorer API",
    version="0.5.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def metrics_middleware(request, call_next):
    """Collect Prometheus metrics for all HTTP requests."""
    start = time.perf_counter()
    status_code = "500"

    try:
        response = await call_next(request)
        status_code = str(response.status_code)
        return response
    finally:
        path_label = _route_path_template(request.scope, request.url.path)
        duration = time.perf_counter() - start
        REQUEST_COUNT.labels(request.method, path_label, status_code).inc()
        REQUEST_LATENCY.labels(request.method, path_label).observe(duration)

try:
    from fastapi.staticfiles import StaticFiles

    # Serve static files if "static" directory exists (e.g. built React app)
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
    if os.path.isdir(static_dir):
        # Mount static assets
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
        print(f"INFO: Serving UI from {static_dir}")
except ImportError:
    pass  # fastapi.staticfiles might be missing if minimal install (unlikely with fastapi)
except Exception as e:
    print(f"WARNING: Failed to mount static UI: {e}")

# CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=_parse_cors_origins(
        os.getenv("IDR_CORS_ORIGINS", "http://localhost:3000,http://localhost:5173")
    ),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include Routers
app.include_router(setup.router)
app.include_router(connection.router)
app.include_router(dashboard.router)
app.include_router(explorer.router)
app.include_router(runs.router)
app.include_router(schema.router)


@app.get("/api/health")
async def health():
    """Health check endpoint."""
    _manager = get_manager()
    adapter = _manager.get_any_adapter()
    return {
        "status": "healthy",
        "connected": adapter is not None,
        "platform": adapter.dialect if adapter else None,
        "active_connections": _manager.connection_count(),
    }


@app.get("/metrics", include_in_schema=False)
async def metrics():
    """Prometheus metrics endpoint."""
    _manager = get_manager()
    DB_CONNECTED_GAUGE.set(1 if _manager.connection_count() > 0 else 0)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
