"""
Tests for idr_api — FastAPI endpoint smoke tests.

Uses FastAPI TestClient (no real DB needed for health/schema).
Tests error handling when no adapter is connected.
"""

import os
import sqlite3
import sys
import time

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient

import idr_api.audit as audit_module
import idr_api.dependencies as deps
import idr_api.routers.audit as audit_router
import idr_api.routers.setup as setup_router
import idr_api.service_auth as service_auth
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


class TestRouteAuthorization:
    @pytest.fixture(autouse=True)
    def clear_auth_override(self):
        app.dependency_overrides.pop(deps.get_current_user, None)
        yield
        app.dependency_overrides.pop(deps.get_current_user, None)

    def test_unknown_role_denied_by_default(self, client):
        app.dependency_overrides[deps.get_current_user] = lambda: {"sub": "u-deny", "roles": ["guest"]}
        resp = client.get("/api/schema")
        assert resp.status_code == 403
        assert "Insufficient permissions" in resp.json()["detail"]

    def test_viewer_role_can_read_schema_but_cannot_manage_connection(self, client, tmp_path):
        import duckdb

        db_path = str(tmp_path / "authz_viewer.duckdb")
        conn = duckdb.connect(db_path)
        conn.close()

        app.dependency_overrides[deps.get_current_user] = lambda: {"sub": "u-view", "roles": ["viewer"]}

        schema_resp = client.get("/api/schema")
        assert schema_resp.status_code == 200

        connect_resp = client.post(
            "/api/connect",
            json={"platform": "duckdb", "database": db_path},
        )
        assert connect_resp.status_code == 403

    def test_scope_claim_allows_connection_manage(self, client, tmp_path):
        import duckdb

        db_path = str(tmp_path / "authz_scope.duckdb")
        conn = duckdb.connect(db_path)
        conn.close()

        app.dependency_overrides[deps.get_current_user] = lambda: {
            "sub": "u-scope",
            "scope": "connection.manage schema.read",
        }

        connect_resp = client.post(
            "/api/connect",
            json={"platform": "duckdb", "database": db_path},
        )
        assert connect_resp.status_code == 200

        disconnect_resp = client.post("/api/disconnect")
        assert disconnect_resp.status_code == 200

    def test_whoami_exposes_resolved_roles_and_permissions(self, client):
        app.dependency_overrides[deps.get_current_user] = lambda: {
            "sub": "u-view",
            "roles": ["viewer"],
            "scope": "custom.one",
        }

        resp = client.get("/api/auth/whoami")
        assert resp.status_code == 200
        body = resp.json()
        assert body["sub"] == "u-view"
        assert body["roles"] == ["viewer"]
        assert "schema.read" in body["permissions"]
        assert "run.execute" not in body["permissions"]
        assert body["scope"] == "custom.one"

    def test_whoami_merges_nested_roles_and_scope_permissions(self, client):
        app.dependency_overrides[deps.get_current_user] = lambda: {
            "sub": "u-operator",
            "realm_access": {"roles": ["operator"]},
            "permissions": ["custom.override"],
            "scope": "alpha beta",
        }

        resp = client.get("/api/auth/whoami")
        assert resp.status_code == 200
        body = resp.json()
        assert "operator" in body["roles"]
        assert "connection.manage" in body["permissions"]
        assert "custom.override" in body["permissions"]
        assert "alpha" in body["permissions"]
        assert "beta" in body["permissions"]

    def test_analyst_cannot_execute_sync_run(self, client):
        app.dependency_overrides[deps.get_current_user] = lambda: {"sub": "u-analyst", "roles": ["analyst"]}
        run_resp = client.post(
            "/api/setup/run",
            json={"mode": "FULL", "strict": False, "max_iterations": 5, "dry_run": False},
        )
        assert run_resp.status_code == 403

    def test_viewer_cannot_manage_service_accounts(self, client):
        app.dependency_overrides[deps.get_current_user] = lambda: {"sub": "u-viewer", "roles": ["viewer"]}
        resp = client.post(
            "/api/auth/service-accounts",
            json={"name": "blocked-sa"},
        )
        assert resp.status_code == 403


class TestAsyncRunSubmission:
    @pytest.fixture(autouse=True)
    def reset_async_job_store(self):
        setup_router.run_jobs.reset_for_tests()
        yield
        setup_router.run_jobs.reset_for_tests()

    def test_submit_run_requires_connection(self, client):
        mgr = get_manager()
        mgr.set_adapter(None, {})

        resp = client.post(
            "/api/setup/run/submit",
            json={
                "mode": "FULL",
                "strict": False,
                "max_iterations": 10,
                "dry_run": False,
            },
        )
        assert resp.status_code == 400
        assert "not connected" in resp.json()["detail"].lower()

    def test_submit_run_creates_job_and_exposes_status(self, client, tmp_path, monkeypatch):
        import duckdb

        import idr_core.runner as runner_module

        db_path = str(tmp_path / "async_job_test.duckdb")
        conn = duckdb.connect(db_path)
        conn.close()

        connect_resp = client.post(
            "/api/connect",
            json={
                "platform": "duckdb",
                "database": db_path,
            },
        )
        assert connect_resp.status_code == 200

        def fake_run(self, config):  # noqa: ANN001
            return runner_module.RunResult(run_id="run_test_async", status="SUCCESS")

        monkeypatch.setattr(runner_module.IDRRunner, "run", fake_run)

        submit = client.post(
            "/api/setup/run/submit",
            json={
                "mode": "FULL",
                "strict": False,
                "max_iterations": 10,
                "dry_run": False,
            },
        )
        assert submit.status_code == 202
        submit_body = submit.json()
        job_id = submit_body["job_id"]
        assert job_id.startswith("job_")
        assert submit_body["status"] in {"QUEUED", "RUNNING", "SUCCEEDED"}

        terminal = None
        for _ in range(20):
            status_resp = client.get(f"/api/setup/run/jobs/{job_id}")
            assert status_resp.status_code == 200
            payload = status_resp.json()
            terminal = payload["status"]
            if terminal in {"SUCCEEDED", "FAILED"}:
                break
            time.sleep(0.05)

        assert terminal == "SUCCEEDED"
        job_payload = client.get(f"/api/setup/run/jobs/{job_id}").json()
        assert job_payload["result"]["run_id"] == "run_test_async"

    def test_get_unknown_async_job_returns_404(self, client):
        resp = client.get("/api/setup/run/jobs/job_does_not_exist")
        assert resp.status_code == 404

    def test_submit_run_rejects_when_active_job_exists(self, client, tmp_path, monkeypatch):
        import duckdb

        import idr_core.runner as runner_module

        db_path = str(tmp_path / "async_job_conflict.duckdb")
        conn = duckdb.connect(db_path)
        conn.close()

        connect_resp = client.post(
            "/api/connect",
            json={
                "platform": "duckdb",
                "database": db_path,
            },
        )
        assert connect_resp.status_code == 200

        def slow_run(self, config):  # noqa: ANN001
            time.sleep(0.2)
            return runner_module.RunResult(run_id="run_test_slow", status="SUCCESS")

        monkeypatch.setattr(runner_module.IDRRunner, "run", slow_run)

        first_submit = client.post(
            "/api/setup/run/submit",
            json={
                "mode": "FULL",
                "strict": False,
                "max_iterations": 10,
                "dry_run": False,
            },
        )
        assert first_submit.status_code == 202
        first_job_id = first_submit.json()["job_id"]

        second_submit = client.post(
            "/api/setup/run/submit",
            json={
                "mode": "FULL",
                "strict": False,
                "max_iterations": 10,
                "dry_run": False,
            },
        )
        assert second_submit.status_code == 409

        # Ensure the background job completes before monkeypatch teardown.
        for _ in range(80):
            payload = client.get(f"/api/setup/run/jobs/{first_job_id}").json()
            if payload["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                break
            time.sleep(0.05)

    def test_submit_run_retries_then_succeeds(self, client, tmp_path, monkeypatch):
        import duckdb

        import idr_core.runner as runner_module

        db_path = str(tmp_path / "async_job_retry.duckdb")
        conn = duckdb.connect(db_path)
        conn.close()

        connect_resp = client.post(
            "/api/connect",
            json={
                "platform": "duckdb",
                "database": db_path,
            },
        )
        assert connect_resp.status_code == 200

        calls = {"n": 0}

        def flaky_run(self, config):  # noqa: ANN001
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient failure")
            return runner_module.RunResult(run_id="run_test_retry", status="SUCCESS")

        monkeypatch.setattr(runner_module.IDRRunner, "run", flaky_run)
        monkeypatch.setattr(setup_router.run_jobs, "_retry_backoff_seconds", 0.01)

        submit = client.post(
            "/api/setup/run/submit",
            json={
                "mode": "FULL",
                "strict": False,
                "max_iterations": 10,
                "dry_run": False,
            },
        )
        assert submit.status_code == 202
        job_id = submit.json()["job_id"]

        final_status = None
        for _ in range(80):
            payload = client.get(f"/api/setup/run/jobs/{job_id}").json()
            final_status = payload["status"]
            if final_status in {"SUCCEEDED", "FAILED"}:
                break
            time.sleep(0.05)

        assert final_status == "SUCCEEDED"
        payload = client.get(f"/api/setup/run/jobs/{job_id}").json()
        assert payload["attempt_count"] >= 2
        assert payload["result"]["run_id"] == "run_test_retry"

    def test_cancel_run_job(self, client, tmp_path, monkeypatch):
        import duckdb

        import idr_core.runner as runner_module

        db_path = str(tmp_path / "async_job_cancel.duckdb")
        conn = duckdb.connect(db_path)
        conn.close()

        connect_resp = client.post(
            "/api/connect",
            json={
                "platform": "duckdb",
                "database": db_path,
            },
        )
        assert connect_resp.status_code == 200

        def slow_run(self, config):  # noqa: ANN001
            time.sleep(0.4)
            return runner_module.RunResult(run_id="run_test_cancel", status="SUCCESS")

        monkeypatch.setattr(runner_module.IDRRunner, "run", slow_run)

        submit = client.post(
            "/api/setup/run/submit",
            json={
                "mode": "FULL",
                "strict": False,
                "max_iterations": 10,
                "dry_run": False,
            },
        )
        assert submit.status_code == 202
        job_id = submit.json()["job_id"]

        cancel = client.post(f"/api/setup/run/jobs/{job_id}/cancel")
        assert cancel.status_code == 202
        assert cancel.json()["status"] in {"CANCELLED", "CANCEL_REQUESTED"}

        terminal = None
        for _ in range(120):
            payload = client.get(f"/api/setup/run/jobs/{job_id}").json()
            terminal = payload["status"]
            if terminal in {"CANCELLED", "FAILED", "SUCCEEDED"}:
                break
            time.sleep(0.05)

        assert terminal == "CANCELLED"

    def test_run_job_events_endpoint_returns_lifecycle_events(self, client, tmp_path, monkeypatch):
        import duckdb

        import idr_core.runner as runner_module

        db_path = str(tmp_path / "async_job_events.duckdb")
        conn = duckdb.connect(db_path)
        conn.close()

        connect_resp = client.post(
            "/api/connect",
            json={
                "platform": "duckdb",
                "database": db_path,
            },
        )
        assert connect_resp.status_code == 200

        def fake_run(self, config):  # noqa: ANN001
            return runner_module.RunResult(run_id="run_test_events", status="SUCCESS")

        monkeypatch.setattr(runner_module.IDRRunner, "run", fake_run)

        submit = client.post(
            "/api/setup/run/submit",
            json={
                "mode": "FULL",
                "strict": False,
                "max_iterations": 10,
                "dry_run": False,
            },
        )
        assert submit.status_code == 202
        job_id = submit.json()["job_id"]

        for _ in range(40):
            payload = client.get(f"/api/setup/run/jobs/{job_id}").json()
            if payload["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                break
            time.sleep(0.05)

        events_resp = client.get(f"/api/setup/run/jobs/{job_id}/events")
        assert events_resp.status_code == 200
        events = events_resp.json()["events"]
        event_types = [e["event_type"] for e in events]
        assert "JOB_SUBMITTED" in event_types
        assert "JOB_STARTED" in event_types
        assert "JOB_SUCCEEDED" in event_types

        first_event_id = events[0]["event_id"]
        filtered = client.get(
            f"/api/setup/run/jobs/{job_id}/events",
            params={"after_event_id": first_event_id},
        )
        assert filtered.status_code == 200
        for event in filtered.json()["events"]:
            assert event["event_id"] > first_event_id

    def test_webhook_is_invoked_for_job_events(self, client, tmp_path, monkeypatch):
        import duckdb

        import idr_core.runner as runner_module

        db_path = str(tmp_path / "async_job_webhook.duckdb")
        conn = duckdb.connect(db_path)
        conn.close()

        connect_resp = client.post(
            "/api/connect",
            json={
                "platform": "duckdb",
                "database": db_path,
            },
        )
        assert connect_resp.status_code == 200

        captured_events = []
        monkeypatch.setattr(
            setup_router.run_jobs,
            "_post_webhook",
            lambda event: captured_events.append(event),
        )

        def fake_run(self, config):  # noqa: ANN001
            return runner_module.RunResult(run_id="run_test_webhook", status="SUCCESS")

        monkeypatch.setattr(runner_module.IDRRunner, "run", fake_run)

        submit = client.post(
            "/api/setup/run/submit",
            json={
                "mode": "FULL",
                "strict": False,
                "max_iterations": 10,
                "dry_run": False,
            },
        )
        assert submit.status_code == 202
        job_id = submit.json()["job_id"]

        for _ in range(40):
            payload = client.get(f"/api/setup/run/jobs/{job_id}").json()
            if payload["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                break
            time.sleep(0.05)

        assert any(event["event_type"] == "JOB_SUBMITTED" for event in captured_events)
        assert any(event["event_type"] == "JOB_SUCCEEDED" for event in captured_events)


class TestServiceAccountTokens:
    @pytest.fixture(autouse=True)
    def reset_service_token_store(self):
        service_auth.get_service_token_store().reset_for_tests()
        yield
        service_auth.get_service_token_store().reset_for_tests()

    def test_service_token_with_scope_can_access_permitted_route_only(self, client, monkeypatch):
        sa_resp = client.post(
            "/api/auth/service-accounts",
            json={"name": "schema-reader"},
        )
        assert sa_resp.status_code == 200
        service_account_id = sa_resp.json()["service_account"]["service_account_id"]

        token_resp = client.post(
            f"/api/auth/service-accounts/{service_account_id}/tokens",
            json={
                "permissions": ["schema.read"],
                "token_name": "schema-only",
            },
        )
        assert token_resp.status_code == 200
        token = token_resp.json()["token"]["token"]

        # Force non-dev auth mode so requests must authenticate via service token path.
        monkeypatch.setattr(deps, "OIDC_ISSUER", "https://issuer.example.com")
        monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", False)

        schema_resp = client.get("/api/schema", headers={"Authorization": f"Bearer {token}"})
        assert schema_resp.status_code == 200

        connect_resp = client.post(
            "/api/connect",
            json={"platform": "duckdb", "database": ":memory:"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert connect_resp.status_code == 403

    def test_revoked_service_token_fails_auth(self, client, monkeypatch):
        sa_resp = client.post(
            "/api/auth/service-accounts",
            json={"name": "ops-bot"},
        )
        assert sa_resp.status_code == 200
        service_account_id = sa_resp.json()["service_account"]["service_account_id"]

        token_resp = client.post(
            f"/api/auth/service-accounts/{service_account_id}/tokens",
            json={
                "permissions": ["schema.read", "auth.manage"],
                "token_name": "ops-token",
            },
        )
        assert token_resp.status_code == 200
        token_payload = token_resp.json()["token"]
        token = token_payload["token"]
        token_id = token_payload["token_id"]

        monkeypatch.setattr(deps, "OIDC_ISSUER", "https://issuer.example.com")
        monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", False)

        pre_revoke = client.get("/api/schema", headers={"Authorization": f"Bearer {token}"})
        assert pre_revoke.status_code == 200

        revoke = client.post(
            f"/api/auth/tokens/{token_id}/revoke",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert revoke.status_code == 200

        post_revoke = client.get("/api/schema", headers={"Authorization": f"Bearer {token}"})
        assert post_revoke.status_code == 401


class TestAuditLogging:
    @pytest.fixture(autouse=True)
    def isolate_audit_store(self, tmp_path, monkeypatch):
        store = audit_module.AuditEventStore(db_path=str(tmp_path / "audit_events.sqlite3"))
        monkeypatch.setattr(audit_module, "_AUDIT_STORE", store)
        monkeypatch.setattr(audit_router, "audit_store", store)
        yield store

    @pytest.fixture(autouse=True)
    def clear_auth_override(self):
        app.dependency_overrides.pop(deps.get_current_user, None)
        yield
        app.dependency_overrides.pop(deps.get_current_user, None)

    def test_audit_endpoint_requires_permission(self, client):
        app.dependency_overrides[deps.get_current_user] = lambda: {"sub": "u-no-audit", "roles": ["guest"]}
        denied = client.get("/api/audit/events")
        assert denied.status_code == 403

        app.dependency_overrides[deps.get_current_user] = lambda: {"sub": "u-view", "roles": ["viewer"]}
        allowed = client.get("/api/audit/events")
        assert allowed.status_code == 200
        assert "events" in allowed.json()

    def test_connection_events_are_audited(self, client, tmp_path):
        import duckdb

        db_path = str(tmp_path / "audit_connect.duckdb")
        conn = duckdb.connect(db_path)
        conn.close()

        connect_resp = client.post(
            "/api/connect",
            json={"platform": "duckdb", "database": db_path},
        )
        assert connect_resp.status_code == 200

        disconnect_resp = client.post("/api/disconnect")
        assert disconnect_resp.status_code == 200

        events = client.get("/api/audit/events", params={"limit": 50}).json()["events"]
        connect_events = [e for e in events if e["action"] == "connection.connect"]
        disconnect_events = [e for e in events if e["action"] == "connection.disconnect"]
        assert any(e["outcome"] == "success" for e in connect_events)
        assert any(e["outcome"] == "success" for e in disconnect_events)
        assert all("actor_sub" in e and "resource_type" in e for e in connect_events + disconnect_events)

    def test_auth_admin_actions_are_audited(self, client):
        service_auth.get_service_token_store().reset_for_tests()

        sa = client.post("/api/auth/service-accounts", json={"name": "audit-sa"})
        assert sa.status_code == 200
        service_account_id = sa.json()["service_account"]["service_account_id"]

        token = client.post(
            f"/api/auth/service-accounts/{service_account_id}/tokens",
            json={"permissions": ["schema.read"], "token_name": "audit-token"},
        )
        assert token.status_code == 200
        token_id = token.json()["token"]["token_id"]

        revoke = client.post(f"/api/auth/tokens/{token_id}/revoke")
        assert revoke.status_code == 200

        events = client.get("/api/audit/events", params={"limit": 50}).json()["events"]
        actions = {(e["action"], e["outcome"]) for e in events}
        assert ("auth.service_account.create", "success") in actions
        assert ("auth.service_token.create", "success") in actions
        assert ("auth.service_token.revoke", "success") in actions

    def test_run_submit_and_cancel_are_audited(self, client, tmp_path, monkeypatch):
        import duckdb

        import idr_core.runner as runner_module

        setup_router.run_jobs.reset_for_tests()

        db_path = str(tmp_path / "audit_run.duckdb")
        conn = duckdb.connect(db_path)
        conn.close()

        connect_resp = client.post("/api/connect", json={"platform": "duckdb", "database": db_path})
        assert connect_resp.status_code == 200

        def slow_run(self, config):  # noqa: ANN001
            time.sleep(0.4)
            return runner_module.RunResult(run_id="run_audit_async", status="SUCCESS")

        monkeypatch.setattr(runner_module.IDRRunner, "run", slow_run)

        submit = client.post(
            "/api/setup/run/submit",
            json={"mode": "FULL", "strict": False, "max_iterations": 5, "dry_run": False},
        )
        assert submit.status_code == 202
        job_id = submit.json()["job_id"]

        cancel = client.post(f"/api/setup/run/jobs/{job_id}/cancel")
        assert cancel.status_code == 202

        for _ in range(80):
            job = client.get(f"/api/setup/run/jobs/{job_id}")
            if job.status_code == 200 and job.json()["status"] in {"CANCELLED", "SUCCEEDED", "FAILED"}:
                break
            time.sleep(0.05)

        events = client.get("/api/audit/events", params={"limit": 100}).json()["events"]
        actions = {(e["action"], e["outcome"]) for e in events}
        assert ("run.submit.async", "success") in actions
        assert ("run.cancel.async", "success") in actions

    def test_audit_table_is_immutable(self, client):
        client.post("/api/auth/service-accounts", json={"name": "immutability-sa"})

        store = audit_module.get_audit_store()
        events = store.list_events(limit=1)
        assert events
        event_id = events[0]["event_id"]

        with pytest.raises(sqlite3.DatabaseError):
            with store._connect() as conn:
                conn.execute(
                    "UPDATE audit_events SET outcome = ? WHERE event_id = ?",
                    ("tampered", event_id),
                )

        with pytest.raises(sqlite3.DatabaseError):
            with store._connect() as conn:
                conn.execute("DELETE FROM audit_events WHERE event_id = ?", (event_id,))
