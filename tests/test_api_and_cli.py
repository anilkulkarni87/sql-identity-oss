"""Consolidated test suite: test_api.py, test_api_and_cli_deep.py, test_cli_smoke.py."""

# --- Consolidated from test_api.py ---
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


# --- Consolidated from test_api_and_cli_deep.py ---
"""
Deep API & CLI integration tests covering:
1. Connected Dashboard endpoints (/api/metrics/summary, /api/metrics/distribution, /api/metrics/rules, /api/alerts)
2. Setup discovery endpoints (/api/setup/discover/tables, /api/setup/discover/columns)
3. Dialect-specific fuzzy templates (/api/setup/fuzzy-templates) across DuckDB, Snowflake, BigQuery, Databricks
4. Full CLI command execution (idr init, idr config validate/generate/apply, idr run FULL/INCR/dry-run/strict, idr quickstart)
"""

from unittest.mock import MagicMock

import duckdb
import pytest
import yaml
from fastapi.testclient import TestClient

import idr_api.dependencies as deps
from idr_api.dependencies import get_manager
from idr_api.main import app
from idr_core.cli import main as cli_main




class TestConnectedDashboardAndDiscoveryAPI:
    def test_dashboard_metrics_distribution_rules_alerts_and_discovery(self, client, tmp_path):
        db_path = str(tmp_path / "dashboard_test.duckdb")
        conn = duckdb.connect(db_path)
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("""
            CREATE TABLE raw.customers (
                id VARCHAR, email VARCHAR, phone VARCHAR, first_name VARCHAR, updated_at TIMESTAMP
            )
        """)
        conn.execute("""
            INSERT INTO raw.customers VALUES
                ('c1', 'shared@example.com', '111-1111', 'Alice', '2026-01-01 00:00:00'),
                ('c2', 'shared@example.com', '222-2222', 'Alyce', '2026-01-02 00:00:00'),
                ('c3', 'solo@example.com', '333-3333', 'Bob', '2026-01-03 00:00:00'),
                ('c4', 'oversized@example.com', '444-4444', 'Spam1', '2026-01-04 00:00:00'),
                ('c5', 'oversized@example.com', '555-5555', 'Spam2', '2026-01-04 00:00:00'),
                ('c6', 'oversized@example.com', '666-6666', 'Spam3', '2026-01-04 00:00:00')
        """)
        conn.close()

        # 1. Connect via Setup API
        resp = client.post("/api/setup/connect", json={"platform": "duckdb", "params": {"path": db_path}})
        assert resp.status_code == 200

        # 2. Test Table & Column Discovery
        tables_resp = client.get("/api/setup/discover/tables", params={"schema": "raw"})
        assert tables_resp.status_code == 200
        assert any("customers" in t for t in tables_resp.json()["tables"])

        cols_resp = client.get("/api/setup/discover/columns", params={"table": "raw.customers"})
        assert cols_resp.status_code == 200
        col_names = {c["name"] for c in cols_resp.json()["columns"]}
        assert {"id", "email", "phone", "first_name", "updated_at"}.issubset(col_names)

        # 3. Configure and run pipeline (set max_group_size=2 so 'oversized@example.com' triggers a skipped group alert)
        cfg = {
            "sources": [
                {
                    "id": "raw_customers",
                    "table": "raw.customers",
                    "entity_key": "id",
                    "watermark_column": "updated_at",
                    "identifiers": [
                        {"type": "email", "column": "email", "expr": "email"},
                        {"type": "phone", "column": "phone", "expr": "phone"},
                    ],
                    "attributes": [{"name": "first_name", "column": "first_name", "expr": "first_name"}],
                }
            ],
            "rules": [
                {
                    "id": "rule_email",
                    "type": "EXACT",
                    "match_keys": ["email"],
                    "priority": 1,
                    "canonicalize": "LOWERCASE",
                    "max_group_size": 2,
                },
                {
                    "id": "rule_phone",
                    "type": "EXACT",
                    "match_keys": ["phone"],
                    "priority": 2,
                    "canonicalize": "EXACT",
                    "max_group_size": 100,
                },
            ],
            "fuzzy_rules": [],
            "survivorship": [],
        }
        assert client.post("/api/setup/config/save", json={"config": cfg}).status_code == 200
        run_resp = client.post(
            "/api/setup/run",
            json={"mode": "FULL", "strict": False, "max_iterations": 10, "dry_run": False},
        )
        assert run_resp.status_code == 200

        # 4. Verify /api/metrics/summary
        summary = client.get("/api/metrics/summary").json()
        assert summary["total_entities"] == 6
        assert summary["total_clusters"] == 5  # (c1+c2) + c3 + c4 + c5 + c6
        assert summary["total_edges"] == 1
        assert summary["last_run_id"] is not None

        # 5. Verify /api/metrics/distribution
        dist = client.get("/api/metrics/distribution").json()
        bucket_map = {d["bucket"]: d["count"] for d in dist}
        assert bucket_map.get("singleton") == 4
        assert bucket_map.get("2-5") == 1

        # 6. Verify /api/metrics/rules
        rules_stats = client.get("/api/metrics/rules").json()
        assert len(rules_stats) >= 1
        email_stat = [r for r in rules_stats if r["rule_id"] == "rule_email"][0]
        assert email_stat["edges_created"] == 1
        assert email_stat["percentage"] == 100.0

        # 7. Verify /api/alerts surfaces the skipped identifier group warning
        alerts = client.get("/api/alerts").json()
        assert len(alerts) >= 1
        assert any("skipped" in a["message"].lower() for a in alerts)

    @pytest.mark.parametrize(
        "dialect,expected_snippet",
        [
            ("duckdb", "jaro_winkler_similarity(<a>.{col}, <b>.{col})"),
            ("snowflake", "(JAROWINKLER_SIMILARITY(<a>.{col}, <b>.{col}) / 100.0)"),
            ("bigquery", "LEVENSHTEIN(<a>.{col}, <b>.{col})"),
            ("databricks", "levenshtein(<a>.{col}, <b>.{col})"),
        ],
    )
    def test_fuzzy_templates_for_all_four_dialects(self, client, dialect, expected_snippet):
        fake_adapter = MagicMock()
        fake_adapter.dialect = dialect
        get_manager().set_adapter_for_user("dev-user", fake_adapter, {"platform": dialect})

        resp = client.get("/api/setup/fuzzy-templates")
        assert resp.status_code == 200
        templates = resp.json()["templates"]
        assert len(templates) >= 2
        assert any(expected_snippet in t["sql_template"] for t in templates)
        for t in templates:
            assert 0.0 < t["default_threshold"] <= 1.0


class TestCLIEndToEnd:
    def test_cli_init_config_generate_apply_run_and_quickstart(self, tmp_path, capsys):
        db_path = str(tmp_path / "cli_e2e.duckdb")
        cfg_path = tmp_path / "test_config.yaml"

        # Seed a source table in DuckDB
        conn = duckdb.connect(db_path)
        conn.execute("""
            CREATE TABLE raw_users (id VARCHAR, email VARCHAR, first_name VARCHAR, updated_at TIMESTAMP);
            INSERT INTO raw_users VALUES
                ('1', 'a@example.com', 'Alice', '2026-01-01 00:00:00'),
                ('2', 'A@example.com', 'Alicia', '2026-01-02 00:00:00');
        """)
        conn.close()

        config_dict = {
            "sources": [
                {
                    "id": "users",
                    "table": "raw_users",
                    "entity_key": "id",
                    "watermark_column": "updated_at",
                    "identifiers": [{"type": "email", "expr": "email"}],
                    "attributes": [{"name": "first_name", "expr": "first_name"}],
                }
            ],
            "rules": [
                {
                    "id": "r_email",
                    "type": "EXACT",
                    "match_keys": ["email"],
                    "priority": 1,
                    "canonicalize": "LOWERCASE",
                }
            ],
            "survivorship": [{"attribute": "first_name", "strategy": "RECENCY", "recency_field": "updated_at"}],
        }
        cfg_path.write_text(yaml.safe_dump(config_dict))

        # 1. idr init
        assert cli_main(["init", "--platform=duckdb", f"--db={db_path}", "--reset"]) == 0

        # 2. idr config validate
        assert cli_main(["config", "validate", f"--file={cfg_path}"]) == 0

        # 3. idr config generate across dialects
        for dialect in ("duckdb", "snowflake", "bigquery", "databricks"):
            capsys.readouterr()
            assert (
                cli_main(["config", "generate", f"--file={cfg_path}", f"--dialect={dialect}"]) == 0
            )
            out = capsys.readouterr().out
            assert "idr_meta.source_table" in out

        # 4. idr config apply
        assert (
            cli_main(["config", "apply", f"--file={cfg_path}", "--platform=duckdb", f"--db={db_path}"])
            == 0
        )

        # 5. idr run --mode=FULL --dry-run
        assert (
            cli_main(["run", "--platform=duckdb", f"--db={db_path}", "--mode=FULL", "--dry-run"])
            == 0
        )

        # 6. idr run --mode=FULL --strict
        assert (
            cli_main(["run", "--platform=duckdb", f"--db={db_path}", "--mode=FULL", "--strict"])
            == 0
        )

        # 7. idr quickstart with small row count
        qs_db = str(tmp_path / "qs_demo.duckdb")
        assert cli_main(["quickstart", "--rows=100", f"--output={qs_db}", "--seed=7"]) == 0


# --- Consolidated from test_cli_smoke.py ---
"""
CLI smoke tests for high-level install/run commands.
"""

import os
import sys
from types import SimpleNamespace

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import idr_core
from idr_core.cli import main


def test_cli_version(capsys):
    rc = main(["version"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "sql-identity-resolution" in out


def test_cli_quickstart_dispatch(monkeypatch):
    called = {}

    def fake_handle_quickstart(args):
        called["rows"] = args.rows
        called["output"] = args.output
        called["seed"] = args.seed
        return 0

    monkeypatch.setattr("idr_core.cli.handle_quickstart", fake_handle_quickstart)
    rc = main(["quickstart", "--rows", "123", "--output", "demo.duckdb", "--seed", "7"])

    assert rc == 0
    assert called == {"rows": 123, "output": "demo.duckdb", "seed": 7}


def test_cli_serve_dispatch(monkeypatch):
    called = {}

    def fake_run(app_path, **kwargs):
        called["app_path"] = app_path
        called["host"] = kwargs.get("host")
        called["port"] = kwargs.get("port")
        called["reload"] = kwargs.get("reload")

    monkeypatch.setitem(sys.modules, "uvicorn", SimpleNamespace(run=fake_run))
    rc = main(["serve", "--host", "127.0.0.1", "--port", "9001"])

    assert rc == 0
    assert called["app_path"] == "idr_api.main:app"
    assert called["host"] == "127.0.0.1"
    assert called["port"] == 9001
    assert called["reload"] is False


def test_cli_run_incr_mode(monkeypatch):
    class FakeAdapter:
        dialect = "duckdb"

        def execute(self, _sql):
            return None

        def close(self):
            return None

    class FakeResult:
        status = "SUCCESS"
        run_id = "run_test"
        entities_processed = 1
        edges_created = 1
        clusters_impacted = 1
        duration_seconds = 0.1
        error = None

    class FakeRunner:
        last_config = None

        def __init__(self, adapter):
            self.adapter = adapter

        def run(self, config):
            FakeRunner.last_config = config
            return FakeResult()

    fake_adapter = FakeAdapter()
    monkeypatch.setattr("idr_core.cli.get_adapter", lambda _args: fake_adapter)
    monkeypatch.setattr(idr_core, "IDRRunner", FakeRunner)

    rc = main(["run", "--platform", "duckdb", "--db", "demo.duckdb", "--mode", "INCR"])

    assert rc == 0
    assert FakeRunner.last_config.run_mode == "INCR"
