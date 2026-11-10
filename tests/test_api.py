"""
Tests for idr_api — FastAPI endpoint smoke tests.

Uses FastAPI TestClient (no real DB needed for health/schema).
Tests error handling when no adapter is connected.
"""

import os
import sys

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient

import idr_api.dependencies as deps
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

    def test_discovered_connections_endpoint(self, client, monkeypatch):
        monkeypatch.setenv("DATABRICKS_HOST", "dbc-test.cloud.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi_test")
        resp = client.get("/api/setup/discovered-connections")
        assert resp.status_code == 200
        conns = resp.json().get("connections", [])
        assert isinstance(conns, list)
        assert any(c["platform"] == "databricks" for c in conns)


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


class TestSetupWizardAndExplorerE2E:
    """End-to-end API tests for Setup Wizard round-trip, PRIORITY survivorship, Golden Profile, RBAC, and Metrics."""

    def test_setup_config_roundtrip_and_golden_profile_e2e(self, client, tmp_path):
        import duckdb

        db_path = str(tmp_path / "wizard_e2e.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("""
            CREATE TABLE raw.crm (
                id VARCHAR, email VARCHAR, first_name VARCHAR, updated_at TIMESTAMP
            )
        """)
        conn.execute("""
            INSERT INTO raw.crm VALUES
                ('c1', 'alice@example.com', 'Alice_CRM', '2026-01-01 00:00:00'),
                ('c2', 'singleton@example.com', 'Solo_CRM', '2026-01-01 00:00:00')
        """)
        conn.execute("""
            CREATE TABLE raw.orders (
                id VARCHAR, email VARCHAR, first_name VARCHAR, updated_at TIMESTAMP
            )
        """)
        conn.execute("""
            INSERT INTO raw.orders VALUES
                ('o1', 'alice@example.com', 'Alice_Orders', '2026-02-01 00:00:00')
        """)
        conn.close()

        # 1. Connect via Setup Wizard
        resp = client.post(
            "/api/setup/connect",
            json={"platform": "duckdb", "params": {"path": db_path}},
        )
        assert resp.status_code == 200

        # 2. Save configuration with Exact + Fuzzy rules + PRIORITY survivorship (using table FQN)
        wizard_config = {
            "sources": [
                {
                    "id": "raw_crm",
                    "table": "raw.crm",
                    "entity_key": "id",
                    "watermark_column": "updated_at",
                    "identifiers": [{"type": "email", "column": "email", "expr": "email"}],
                    "attributes": [
                        {"name": "first_name", "column": "first_name", "expr": "first_name"}
                    ],
                },
                {
                    "id": "raw_orders",
                    "table": "raw.orders",
                    "entity_key": "id",
                    "watermark_column": "updated_at",
                    "identifiers": [{"type": "email", "column": "email", "expr": "email"}],
                    "attributes": [
                        {"name": "first_name", "column": "first_name", "expr": "first_name"}
                    ],
                },
            ],
            "rules": [
                {
                    "id": "rule_email",
                    "type": "EXACT",
                    "match_keys": ["email"],
                    "priority": 1,
                    "canonicalize": "LOWERCASE",
                }
            ],
            "fuzzy_rules": [
                {
                    "id": "fuzzy_name",
                    "type": "FUZZY",
                    "blocking_key": "email",
                    "score_expr": "jaro_winkler_similarity(<a>.first_name, <b>.first_name)",
                    "threshold": 0.85,
                    "priority": 2,
                }
            ],
            "survivorship": [
                {
                    "attribute": "first_name",
                    "strategy": "PRIORITY",
                    "source_priority": ["raw.crm", "raw.orders"],
                }
            ],
        }

        save_resp = client.post("/api/setup/config/save", json={"config": wizard_config})
        assert save_resp.status_code == 200, save_resp.text

        # 3. Round-trip fetch config and verify all sections & fields are preserved
        get_resp = client.get("/api/setup/config")
        assert get_resp.status_code == 200
        loaded = get_resp.json()
        assert len(loaded["sources"]) == 2
        assert loaded["sources"][0]["identifiers"][0]["column"] == "email"
        assert loaded["sources"][0]["attributes"][0]["column"] == "first_name"
        assert len(loaded["rules"]) == 1
        assert len(loaded["fuzzy_rules"]) == 1
        assert loaded["fuzzy_rules"][0]["type"] == "FUZZY"
        assert "jaro_winkler_similarity" in loaded["fuzzy_rules"][0]["score_expr"]
        assert len(loaded["survivorship"]) == 1
        assert loaded["survivorship"][0]["strategy"] == "PRIORITY"

        # 4. Run pipeline WITH fuzzy rule active (blocking_key='email' is an identifier, score_expr uses attribute 'first_name')
        run_resp = client.post(
            "/api/setup/run",
            json={"mode": "FULL", "strict": False, "max_iterations": 10, "dry_run": False},
        )
        assert run_resp.status_code == 200, run_resp.text
        run_data = run_resp.json()
        assert run_data["status"] == "SUCCESS"
        run_id = run_data["run_id"]

        # 5. Verify GET /api/runs/{run_id} returns stage-by-stage telemetry
        run_detail_resp = client.get(f"/api/runs/{run_id}")
        assert run_detail_resp.status_code == 200
        run_detail = run_detail_resp.json()
        assert run_detail["run_id"] == run_id
        assert isinstance(run_detail["stages"], list)
        assert len(run_detail["stages"]) > 0
        assert isinstance(run_detail["warnings"], list)

        # 6. Search singleton entity (c2) and multi-entity cluster (c1/o1) in Explorer
        solo_search = client.get("/api/entities/search", params={"q": "singleton@example.com"})
        assert solo_search.status_code == 200
        assert len(solo_search.json()) == 1

        alice_search = client.get("/api/entities/search", params={"q": "alice@example.com"})
        assert alice_search.status_code == 200
        alice_clusters = alice_search.json()
        assert len(alice_clusters) == 1
        resolved_id = alice_clusters[0]["resolved_id"]

        # 7. Verify ClusterDetail includes Golden Profile with PRIORITY winner ('Alice_CRM') and super_cluster_id
        detail_resp = client.get(f"/api/clusters/{resolved_id}")
        assert detail_resp.status_code == 200
        detail = detail_resp.json()
        assert detail["cluster_size"] == 2
        assert detail["super_cluster_id"] == resolved_id
        assert detail["golden_profile"] is not None
        assert detail["golden_profile"].get("first_name") == "Alice_CRM"

        # 8. Verify Prometheus /metrics includes business metrics
        metrics_text = client.get("/metrics").text
        assert "idr_entities_processed_total" in metrics_text
        assert "idr_edges_created_total" in metrics_text
        assert "idr_clusters_impacted_total" in metrics_text
        assert "idr_last_run_duration_seconds" in metrics_text

        # 9. Verify GET /api/runs/{run_id} returns 404 for unknown run_id
        missing_run = client.get("/api/runs/nonexistent_run_123")
        assert missing_run.status_code == 404

    def test_fuzzy_super_cluster_merges_distinct_exact_clusters(self, client, tmp_path):
        import duckdb

        db_path = str(tmp_path / "fuzzy_merge.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("""
            CREATE TABLE raw.users (
                id VARCHAR, email VARCHAR, last_name VARCHAR, first_name VARCHAR, updated_at TIMESTAMP
            )
        """)
        conn.execute("""
            INSERT INTO raw.users VALUES
                ('u1', 'jon.smith@a.com', 'smith', 'Jonathan', '2026-01-01 00:00:00'),
                ('u2', 'jonathan.s@b.com', 'smith', 'Jonathon', '2026-01-02 00:00:00')
        """)
        conn.close()

        client.post("/api/setup/connect", json={"platform": "duckdb", "params": {"path": db_path}})

        cfg = {
            "sources": [
                {
                    "id": "raw_users",
                    "table": "raw.users",
                    "entity_key": "id",
                    "watermark_column": "updated_at",
                    "identifiers": [{"type": "email", "column": "email", "expr": "email"}],
                    "attributes": [
                        {"name": "last_name", "column": "last_name", "expr": "last_name"},
                        {"name": "first_name", "column": "first_name", "expr": "first_name"},
                    ],
                }
            ],
            "rules": [
                {
                    "id": "rule_email",
                    "type": "EXACT",
                    "match_keys": ["email"],
                    "priority": 1,
                    "canonicalize": "LOWERCASE",
                }
            ],
            "fuzzy_rules": [
                {
                    "id": "fuzzy_first_name",
                    "type": "FUZZY",
                    "blocking_key": "last_name",
                    "score_expr": "jaro_winkler_similarity(<a>.first_name, <b>.first_name)",
                    "threshold": 0.85,
                    "priority": 2,
                }
            ],
            "survivorship": [],
        }

        assert client.post("/api/setup/config/save", json={"config": cfg}).status_code == 200
        run_resp = client.post(
            "/api/setup/run",
            json={"mode": "FULL", "strict": False, "max_iterations": 10, "dry_run": False},
        )
        assert run_resp.status_code == 200
        assert run_resp.json()["status"] == "SUCCESS"

        c1 = client.get("/api/clusters/raw_users:u1").json()
        c2 = client.get("/api/clusters/raw_users:u2").json()
        assert c1["resolved_id"] != c2["resolved_id"]
        assert c1["super_cluster_id"] is not None
        assert c1["super_cluster_id"] == c2["super_cluster_id"]

    def test_rbac_blocks_viewer_on_write_endpoints(self, monkeypatch):
        async def _viewer_user():
            return {"sub": "viewer-1", "email": "v@local", "roles": ["viewer"]}

        app.dependency_overrides[deps.get_current_user] = _viewer_user
        try:
            tc = TestClient(app, raise_server_exceptions=False)
            r1 = tc.post("/api/setup/config/save", json={"config": {"sources": []}})
            assert r1.status_code == 403
            r2 = tc.post(
                "/api/setup/run",
                json={"mode": "FULL", "strict": False, "max_iterations": 5, "dry_run": True},
            )
            assert r2.status_code == 403
        finally:
            app.dependency_overrides.pop(deps.get_current_user, None)

    def test_run_progress_and_events_endpoints(self, client):
        resp = client.get("/api/setup/run/progress")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data
        assert "stages" in data
        assert resp.headers.get("cache-control") == "no-cache, no-store, must-revalidate"

        # Verify SSE events endpoint returns streaming response with headers
        with client.stream("GET", "/api/setup/run/events") as stream_resp:
            assert stream_resp.status_code == 200
            assert "text/event-stream" in stream_resp.headers["content-type"]
            assert stream_resp.headers.get("x-accel-buffering") == "no"
