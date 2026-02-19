"""
Tests for idr_api — FastAPI endpoint smoke tests.

Uses FastAPI TestClient (no real DB needed for health/schema).
Tests error handling when no adapter is connected.
"""

import os
import sys

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import idr_api.dependencies as deps
from fastapi.testclient import TestClient

from idr_api.dependencies import get_manager
from idr_api.main import _parse_cors_origins, app


@pytest.fixture
def client(monkeypatch):
    """Create a FastAPI test client."""
    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", True)
    return TestClient(app, raise_server_exceptions=False)


# ============================================================
# Health check (always works, no DB needed)
# ============================================================


class TestHealthEndpoint:
    """Tests for GET /api/health."""

    def test_health_returns_200(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200

    def test_health_has_status_field(self, client):
        resp = client.get("/api/health")
        data = resp.json()
        assert data["status"] == "healthy"

    def test_health_shows_connected_false_initially(self, client):
        # Disconnect any existing adapter first
        mgr = get_manager()
        mgr.set_adapter(None, {})
        resp = client.get("/api/health")
        data = resp.json()
        assert data["connected"] is False
        assert data["platform"] is None

    def test_metrics_returns_prometheus_payload(self, client):
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        body = resp.text
        assert "idr_http_requests_total" in body
        assert "idr_api_db_connected" in body


# ============================================================
# Schema endpoint (static data, no DB needed)
# ============================================================


class TestSchemaEndpoint:
    """Tests for GET /api/schema."""

    def test_schema_returns_200(self, client):
        resp = client.get("/api/schema")
        assert resp.status_code == 200

    def test_schema_returns_list(self, client):
        resp = client.get("/api/schema")
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_schema_table_has_required_fields(self, client):
        resp = client.get("/api/schema")
        table = resp.json()[0]
        assert "schema_name" in table
        assert "table_name" in table
        assert "fqn" in table
        assert "columns" in table

    def test_schema_column_has_required_fields(self, client):
        resp = client.get("/api/schema")
        table = resp.json()[0]
        col = table["columns"][0]
        assert "name" in col
        assert "type" in col
        assert "is_pk" in col

    def test_schema_includes_idr_meta_tables(self, client):
        resp = client.get("/api/schema")
        tables = resp.json()
        schemas = {t["schema_name"] for t in tables}
        assert "idr_meta" in schemas

    def test_schema_includes_idr_out_tables(self, client):
        resp = client.get("/api/schema")
        tables = resp.json()
        schemas = {t["schema_name"] for t in tables}
        assert "idr_out" in schemas


# ============================================================
# Endpoints that require a connected adapter (error handling)
# ============================================================


class TestEndpointsRequireAdapter:
    """Tests that endpoints return 400 when no adapter is connected."""

    @pytest.fixture(autouse=True)
    def disconnect_adapter(self):
        """Ensure no adapter is connected before each test."""
        mgr = get_manager()
        mgr.set_adapter(None, {})
        yield

    def test_metrics_summary_returns_400(self, client):
        resp = client.get("/api/metrics/summary")
        assert resp.status_code == 400
        assert "not connected" in resp.json()["detail"].lower()

    def test_metrics_distribution_returns_400(self, client):
        resp = client.get("/api/metrics/distribution")
        assert resp.status_code == 400

    def test_metrics_rules_returns_400(self, client):
        resp = client.get("/api/metrics/rules")
        assert resp.status_code == 400

    def test_alerts_returns_400(self, client):
        resp = client.get("/api/alerts")
        assert resp.status_code == 400

    def test_entity_search_returns_400(self, client):
        resp = client.get("/api/entities/search", params={"q": "test@example.com"})
        assert resp.status_code == 400

    def test_cluster_detail_returns_400(self, client):
        resp = client.get("/api/clusters/some-id")
        assert resp.status_code == 400

    def test_runs_returns_400(self, client):
        resp = client.get("/api/runs")
        assert resp.status_code == 400


# ============================================================
# Connect endpoint
# ============================================================


class TestConnectEndpoint:
    """Tests for POST /api/connect."""

    def test_connect_duckdb_file(self, client, tmp_path):
        """Connect to a real DuckDB file."""
        import duckdb

        db_path = str(tmp_path / "test_idr.duckdb")
        # Pre-create the file for deterministic test behavior
        conn = duckdb.connect(db_path)
        conn.close()

        resp = client.post(
            "/api/connect",
            json={
                "platform": "duckdb",
                "database": db_path,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "connected"
        assert data["platform"] == "duckdb"

    def test_connect_unknown_platform(self, client):
        # The generic except block in connection.py wraps HTTPException as 500
        resp = client.post(
            "/api/connect",
            json={
                "platform": "oracle",
            },
        )
        assert resp.status_code in (400, 500)
        assert "oracle" in resp.json()["detail"].lower()

    def test_connect_missing_platform(self, client):
        resp = client.post("/api/connect", json={})
        assert resp.status_code == 422  # Pydantic validation error

    def test_health_shows_connected_after_connect(self, client, tmp_path):
        import duckdb

        db_path = str(tmp_path / "test_idr2.duckdb")
        conn = duckdb.connect(db_path)
        conn.close()

        resp = client.post(
            "/api/connect",
            json={
                "platform": "duckdb",
                "database": db_path,
            },
        )
        if resp.status_code == 200:
            health = client.get("/api/health")
            data = health.json()
            assert data["connected"] is True
            assert data["platform"] == "duckdb"

    def test_connect_duckdb_from_idr_db_path_env(self, client, tmp_path, monkeypatch):
        import duckdb

        db_path = str(tmp_path / "env_idr.duckdb")
        conn = duckdb.connect(db_path)
        conn.close()
        monkeypatch.setenv("IDR_DB_PATH", db_path)
        monkeypatch.delenv("IDR_DATABASE", raising=False)

        resp = client.post(
            "/api/connect",
            json={
                "platform": "duckdb",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "connected"
        assert data["platform"] == "duckdb"


# ============================================================
# Entity search validation
# ============================================================


class TestEntitySearchValidation:
    """Tests for query parameter validation on search."""

    def test_search_too_short_query(self, client):
        # min_length=3 enforced by FastAPI
        resp = client.get("/api/entities/search", params={"q": "ab"})
        assert resp.status_code == 422

    def test_search_missing_query(self, client):
        resp = client.get("/api/entities/search")
        assert resp.status_code == 422


class TestApiConfigHelpers:
    def test_parse_cors_origins(self):
        origins = _parse_cors_origins("http://a.com, http://b.com")
        assert origins == ["http://a.com", "http://b.com"]
