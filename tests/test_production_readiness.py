"""Tests for the 5 P0 production readiness fixes across extraction, graph, output, runner, and adapters."""

import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.config import config_to_sql
from idr_core.profile_builder import ProfileBuilder
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager


def _setup_crm_source(adapter: DuckDBAdapter, rows: list[tuple[int, str, str]]) -> dict:
    adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    adapter.execute("""
        CREATE OR REPLACE TABLE raw.crm (
            id INTEGER,
            email VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    for r in rows:
        adapter.execute(f"INSERT INTO raw.crm VALUES ({r[0]}, '{r[1]}', TIMESTAMP '{r[2]}')")

    SchemaManager(adapter).initialize(reset=True)
    cfg = {
        "sources": [
            {
                "id": "crm",
                "table": "raw.crm",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [{"type": "EMAIL", "expr": "email"}],
                "attributes": [{"name": "email", "expr": "email"}],
            }
        ],
        "rules": [
            {
                "id": "email_exact",
                "type": "EXACT",
                "match_keys": ["EMAIL"],
                "priority": 1,
                "canonicalize": "LOWERCASE",
                "max_group_size": 100,
            }
        ],
        "survivorship": [{"attribute": "email", "strategy": "RECENCY"}],
    }
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)
    return cfg


def test_watermark_not_advanced_when_profile_builder_fails(monkeypatch):
    adapter = DuckDBAdapter(":memory:")
    _setup_crm_source(adapter, [(1, "a@example.com", "2026-01-01 00:00:00")])

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated warehouse failure during golden profile build")

    monkeypatch.setattr(ProfileBuilder, "build_profiles", _boom)

    result = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert result.status == "FAILED"

    # Watermarks must NOT have moved forward since golden profiles failed
    wm_rows = adapter.query("SELECT * FROM idr_meta.run_state")
    assert len(wm_rows) == 0
    adapter.close()


def test_incr_retroactive_exclusion_splits_cluster_with_zero_new_source_rows():
    adapter = DuckDBAdapter(":memory:")
    _setup_crm_source(
        adapter,
        [
            (1, "noreply@shop.com", "2026-01-01 00:00:00"),
            (2, "noreply@shop.com", "2026-01-01 00:00:00"),
        ],
    )

    r1 = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert r1.status == "SUCCESS"
    clusters_before = adapter.query(
        "SELECT resolved_id, cluster_size FROM idr_out.identity_clusters_current"
    )
    assert len(clusters_before) == 1
    assert clusters_before[0]["cluster_size"] == 2

    # Steward adds exclusion WITHOUT modifying any source rows
    adapter.execute("""
        INSERT INTO idr_meta.identifier_exclusion (identifier_type, identifier_value_pattern, match_type, reason)
        VALUES ('EMAIL', 'noreply@shop.com', 'EXACT', 'Shared role address')
    """)

    r2 = IDRRunner(adapter).run(RunConfig(run_mode="INCR"))
    assert r2.status == "SUCCESS"

    mem_after = adapter.query("SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current")
    assert len({row["resolved_id"] for row in mem_after}) == 2
    edges_after = adapter.query("SELECT * FROM idr_out.identity_edges_current")
    assert len(edges_after) == 0
    adapter.close()


def test_incr_retroactive_do_not_link_override_splits_cluster():
    adapter = DuckDBAdapter(":memory:")
    _setup_crm_source(
        adapter,
        [
            (1, "shared@example.com", "2026-01-01 00:00:00"),
            (2, "shared@example.com", "2026-01-01 00:00:00"),
        ],
    )

    assert IDRRunner(adapter).run(RunConfig(run_mode="FULL")).status == "SUCCESS"
    assert len(adapter.query("SELECT * FROM idr_out.identity_clusters_current")) == 1

    # Steward adds DO_NOT_LINK override with zero new source rows
    adapter.execute("""
        INSERT INTO idr_meta.identity_override
        (override_id, override_type, entity_key_a, entity_key_b, reason, created_by, is_active)
        VALUES ('ov_1', 'DO_NOT_LINK', 'crm:1', 'crm:2', 'Twins sharing email', 'steward', TRUE)
    """)

    r2 = IDRRunner(adapter).run(RunConfig(run_mode="INCR"))
    assert r2.status == "SUCCESS"

    mem_after = adapter.query("SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current")
    assert len({row["resolved_id"] for row in mem_after}) == 2
    assert len(adapter.query("SELECT * FROM idr_out.identity_edges_current")) == 0
    adapter.close()


def test_incr_in_place_identifier_update_removes_stale_edge():
    adapter = DuckDBAdapter(":memory:")
    _setup_crm_source(
        adapter,
        [
            (1, "old@example.com", "2026-01-01 00:00:00"),
            (2, "old@example.com", "2026-01-01 00:00:00"),
        ],
    )

    assert IDRRunner(adapter).run(RunConfig(run_mode="FULL")).status == "SUCCESS"
    assert len(adapter.query("SELECT * FROM idr_out.identity_clusters_current")) == 1
    assert len(adapter.query("SELECT * FROM idr_out.identity_edges_current")) == 1

    # Update crm:1's email in-place and advance watermark
    adapter.execute("""
        UPDATE raw.crm
        SET email = 'new@example.com', updated_at = TIMESTAMP '2026-01-02 00:00:00'
        WHERE id = 1
    """)

    r2 = IDRRunner(adapter).run(RunConfig(run_mode="INCR"))
    assert r2.status == "SUCCESS"

    mem_after = adapter.query("SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current")
    assert len({row["resolved_id"] for row in mem_after}) == 2
    assert len(adapter.query("SELECT * FROM idr_out.identity_edges_current")) == 0
    ids_crm1 = adapter.query(
        "SELECT identifier_value_norm FROM idr_out.identity_identifiers_current WHERE entity_key = 'crm:1'"
    )
    assert [r["identifier_value_norm"] for r in ids_crm1] == ["new@example.com"]
    adapter.close()


def test_custom_schema_mapping_duckdb():
    adapter = DuckDBAdapter(
        ":memory:",
        schema_mapping={"idr_meta": "dev_meta", "idr_work": "dev_work", "idr_out": "dev_out"},
    )
    _setup_crm_source(
        adapter,
        [
            (1, "a@example.com", "2026-01-01 00:00:00"),
            (2, "a@example.com", "2026-01-01 00:00:00"),
        ],
    )
    res = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert res.status == "SUCCESS"

    # Direct raw query against custom schema should contain the resolved clusters
    raw_clusters = adapter.conn.execute("SELECT * FROM dev_out.identity_clusters_current").fetchall()
    assert len(raw_clusters) == 1
    adapter.close()


def test_incr_config_drift_warning():
    adapter = DuckDBAdapter(":memory:")
    _setup_crm_source(
        adapter,
        [(1, "a@example.com", "2026-01-01 00:00:00")],
    )
    r1 = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert r1.status == "SUCCESS"
    assert not any("Configuration drift" in w for w in r1.warnings)

    # Mutate rules after initial FULL run, then run INCR
    adapter.execute("""
        INSERT INTO idr_meta.rule (rule_id, is_active, priority, identifier_type, canonicalize, max_group_size)
        VALUES ('phone_exact', TRUE, 2, 'PHONE', 'NONE', 100)
    """)
    adapter.execute("""
        INSERT INTO raw.crm VALUES (2, 'b@example.com', TIMESTAMP '2026-01-02 00:00:00')
    """)

    r2 = IDRRunner(adapter).run(RunConfig(run_mode="INCR"))
    assert r2.status == "SUCCESS"
    assert any("Configuration drift detected in INCR mode" in w for w in r2.warnings)
    adapter.close()


def test_connection_manager_and_session_store_auto_reconnect(tmp_path):
    import time

    from idr_api.session_store import InMemoryConnectionSessionStore
    from idr_core.connection_manager import ConnectionManager, ConnectionState

    db_file = str(tmp_path / "reconnect.duckdb")
    store = InMemoryConnectionSessionStore(ttl_seconds=1)
    a1 = DuckDBAdapter(db_file)
    store.set_adapter("u1", a1, {"platform": "duckdb", "database": db_file})

    # Force idle expiration without explicit disconnect
    store._states["u1"].last_used_at = time.time() - 10
    reconnected = store.get_adapter("u1")
    assert reconnected is not None
    assert reconnected is not a1
    assert a1.conn is None  # Old idle connection closed cleanly
    assert reconnected.query_one("SELECT 42") == 42

    # Explicit disconnect clears saved config so it will NOT auto-reconnect
    store.disconnect_user("u1")
    assert store.get_adapter("u1") is None

    # ConnectionManager.get_state(auto_reconnect=True) recovers a closed handle
    cm = ConnectionManager.instance()
    cm.disconnect()
    a2 = DuckDBAdapter(db_file)
    cm.set_adapter(a2, {"platform": "duckdb", "database": db_file})
    a2.close()
    assert cm.get_state() == ConnectionState.STALE
    assert cm.get_state(auto_reconnect=True) in (
        ConnectionState.CONNECTED_UNINITIALIZED,
        ConnectionState.CONNECTED_READY,
    )
    cm.disconnect()


def test_api_overrides_override_id_exclusions_and_bounded_clusters(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    import idr_api.dependencies as deps
    from idr_api.main import app

    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", True)

    db_file = str(tmp_path / "api_p0.duckdb")
    adapter = DuckDBAdapter(db_file)
    _setup_crm_source(
        adapter,
        [
            (1, "shared@shop.com", "2026-01-01 00:00:00"),
            (2, "shared@shop.com", "2026-01-01 00:00:00"),
        ],
    )
    assert IDRRunner(adapter).run(RunConfig(run_mode="FULL")).status == "SUCCESS"
    adapter.close()

    tc = TestClient(app)
    r_conn = tc.post("/api/connect", json={"platform": "duckdb", "database": db_file})
    assert r_conn.status_code == 200

    # Bounded cluster query returns truncated=True when capped at max_entities=1
    cid = tc.get("/api/entities/search?q=shared@shop.com").json()[0]["resolved_id"]
    cl_resp = tc.get(f"/api/clusters/{cid}?max_entities=1")
    assert cl_resp.status_code == 200
    assert cl_resp.json()["truncated"] is True
    assert len(cl_resp.json()["entities"]) == 1

    # Override insert populates override_id primary key
    ov_resp = tc.post(
        "/api/overrides",
        json={
            "action": "add",
            "override_type": "DO_NOT_LINK",
            "entity_key_a": "crm:1",
            "entity_key_b": "crm:2",
        },
    )
    assert ov_resp.status_code == 200
    assert ov_resp.json()["override_id"].startswith("ov_")
    listed_ov = tc.post("/api/overrides", json={"action": "list"}).json()["overrides"]
    assert any(o.get("override_id") == ov_resp.json()["override_id"] for o in listed_ov)

    # Value-level exclusion endpoint
    ex_resp = tc.post(
        "/api/exclusions",
        json={
            "action": "add",
            "identifier_type": "EMAIL",
            "identifier_value_pattern": "shared@shop.com",
            "match_type": "EXACT",
        },
    )
    assert ex_resp.status_code == 200
    assert len(tc.post("/api/exclusions", json={"action": "list"}).json()["exclusions"]) >= 1
    tc.post("/api/disconnect")


def test_bigquery_schema_mapping_env_and_word_boundary(monkeypatch):
    from unittest.mock import MagicMock

    from idr_core.adapters.bigquery import BigQueryAdapter

    monkeypatch.setenv("IDR_SCHEMA_PREFIX", "dev_")
    bq = BigQueryAdapter(MagicMock(), project="my-proj")
    sql = bq._prepare_sql_simple("SELECT a.my_idr_out FROM idr_out.identity_clusters_current a")
    assert "`my-proj`.dev_idr_out.identity_clusters_current" in sql
    assert "a.my_idr_out" in sql


def test_mcp_get_system_status_auto_discovers_connected_adapter(tmp_path):
    from idr_api.dependencies import get_manager
    from idr_core.connection_manager import ConnectionManager
    import idr_mcp.server as mcp_srv

    ConnectionManager.instance().disconnect()
    mgr = get_manager()
    mgr.disconnect()

    adapter = DuckDBAdapter(":memory:")
    _setup_crm_source(adapter, [(1, "a@example.com", "2026-01-01 00:00:00")])
    mgr.set_adapter_for_user("test-user", adapter, {"platform": "duckdb"})
    # Simulate ConnectionManager not holding the adapter directly yet
    ConnectionManager.instance()._adapter = None

    status = mcp_srv.get_system_status()
    assert status["lifecycle_state"] == "CONFIGURED_READY"
    assert status["is_configured"] is True
    mgr.disconnect()

