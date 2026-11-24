"""Tests for opt-in idr_out.identity_activation_delta across engine, 4 SQL dialects, API, and MCP."""

import copy
import json

import pytest
from fastapi.testclient import TestClient

import idr_api.dependencies as deps
from idr_api.dependencies import get_manager
from idr_api.main import app
from idr_core.activation_delta import (
    ActivationDeltaOptions,
    build_activation_delta_insert_sql,
    build_retention_delete_sql,
)
from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.config import apply_config_to_warehouse, sql_to_config, validate_config
from idr_core.privacy import PrivacyManager
from idr_core.runner import IDRRunner, RunConfig


@pytest.fixture
def delta_env():
    adapter = DuckDBAdapter(":memory:")
    adapter.execute("CREATE SCHEMA raw")
    adapter.execute("""
        CREATE TABLE raw.customers (
            customer_id VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            city VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    adapter.execute("""
        INSERT INTO raw.customers (customer_id, email, phone, city, updated_at) VALUES
        ('c1', 'alice@acme.io', '5550101', 'Seattle', TIMESTAMP '2026-01-01 00:00:00'),
        ('c2', 'bob@acme.io',   '5550102', 'Austin',  TIMESTAMP '2026-01-01 00:00:00')
    """)
    adapter.execute("""
        CREATE TABLE raw.loyalty (
            member_id VARCHAR,
            email VARCHAR,
            tier VARCHAR
        )
    """)
    adapter.execute("""
        INSERT INTO raw.loyalty (member_id, email, tier) VALUES
        ('l1', 'carol@acme.io', 'GOLD')
    """)
    cfg = {
        "sources": [
            {
                "id": "crm",
                "table": "raw.customers",
                "entity_key": "customer_id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                    {"type": "phone", "expr": "phone"},
                ],
                "attributes": [{"name": "city", "expr": "city"}],
            },
            {
                "id": "loyalty",
                "table": "raw.loyalty",
                "entity_key": "member_id",
                "identifiers": [{"type": "email", "expr": "email"}],
                "attributes": [{"name": "tier", "expr": "tier"}],
            },
        ],
        "rules": [
            {"id": "r_email", "match_keys": ["email"], "canonicalize": "LOWERCASE", "priority": 1},
            {"id": "r_phone", "match_keys": ["phone"], "canonicalize": "EXACT", "priority": 2},
        ],
        "survivorship": [
            {"attribute": "city", "strategy": "RECENCY"},
            {"attribute": "tier", "strategy": "PRIORITY", "source_priority": ["loyalty"]},
        ],
        "lenses": [
            {"id": "email_only", "include_types": ["EMAIL"]},
        ],
    }
    apply_config_to_warehouse(adapter, cfg)
    yield adapter, cfg
    adapter.close()


def test_config_validation_and_round_trip(delta_env):
    adapter, cfg = delta_env
    cfg_with_delta = copy.deepcopy(cfg)
    cfg_with_delta["activation_delta"] = {
        "enabled": True,
        "include_lenses": True,
        "seed_initial_full": False,
        "retention_days": 14,
    }
    assert validate_config(cfg_with_delta) is True
    apply_config_to_warehouse(adapter, cfg_with_delta)
    loaded = sql_to_config(adapter)
    assert loaded["activation_delta"] == {
        "enabled": True,
        "include_lenses": True,
        "seed_initial_full": False,
        "retention_days": 14,
    }

    bad_cfg = copy.deepcopy(cfg)
    bad_cfg["activation_delta"] = {"enabled": True, "retention_days": -5}
    with pytest.raises(ValueError):
        validate_config(bad_cfg)


def test_disabled_by_default_emits_zero_rows(delta_env):
    adapter, _ = delta_env
    res = IDRRunner(adapter).run(RunConfig(run_mode="FULL", strict=True))
    assert res.status == "SUCCESS"
    assert res.activation_delta_summary == {}
    cnt = adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_activation_delta")
    assert cnt == 0


def test_initial_full_skips_day1_backfill_unless_seeded(delta_env):
    adapter, cfg = delta_env
    cfg_enabled = copy.deepcopy(cfg)
    cfg_enabled["activation_delta"] = {"enabled": True, "seed_initial_full": False}
    apply_config_to_warehouse(adapter, cfg_enabled)

    # Empty baseline FULL run skips Day-1 backfill when seed_initial_full is False
    res = IDRRunner(adapter).run(RunConfig(run_mode="FULL", strict=True))
    assert res.status == "SUCCESS"
    assert res.activation_delta_summary == {}
    assert adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_activation_delta") == 0

    # Reset and verify seed_initial_full=True populates initial NEW rows
    adapter2 = DuckDBAdapter(":memory:")
    try:
        adapter2.execute("CREATE SCHEMA raw")
        adapter2.execute(
            "CREATE TABLE raw.customers AS SELECT 'c1' AS customer_id, 'a@b.com' AS email, '5551' AS phone, 'NYC' AS city, TIMESTAMP '2026-01-01' AS updated_at"
        )
        adapter2.execute(
            "CREATE TABLE raw.loyalty AS SELECT 'l1' AS member_id, 'c@d.com' AS email, 'GOLD' AS tier"
        )
        cfg_seeded = copy.deepcopy(cfg)
        cfg_seeded["activation_delta"] = {
            "enabled": True,
            "seed_initial_full": True,
            "include_lenses": True,
        }
        apply_config_to_warehouse(adapter2, cfg_seeded)
        res2 = IDRRunner(adapter2).run(RunConfig(run_mode="FULL", strict=True))
        assert res2.status == "SUCCESS"
        assert res2.activation_delta_summary.get("NEW") == 2
        lens_cnt = adapter2.query_one(
            "SELECT COUNT(*) FROM idr_out.identity_activation_delta WHERE lens_id = 'email_only'"
        )
        assert lens_cnt == 2
    finally:
        adapter2.close()


def test_incremental_lifecycle_new_merge_noop_profile_split_and_delete(delta_env):
    adapter, cfg = delta_env
    cfg["activation_delta"] = {"enabled": True, "include_lenses": False, "retention_days": 30}
    apply_config_to_warehouse(adapter, cfg)

    # 1. Baseline FULL run (c1 -> crm:c1, c2 -> crm:c2, l1 -> loyalty:l1)
    r1 = IDRRunner(adapter).run(RunConfig(run_mode="FULL", strict=True))
    assert r1.status == "SUCCESS"

    # 2. INCR run with:
    # - NEW customer c3
    # - MERGE: c2 updates email to alice@acme.io (merges crm:c2 into crm:c1!)
    # - NO-OP: c1 touched with newer timestamp ONLY? Wait: c1 merges with c2 here, so let's add a separate untouched/no-op test next!
    adapter.execute("""
        INSERT INTO raw.customers (customer_id, email, phone, city, updated_at) VALUES
        ('c3', 'dave@acme.io', '5550103', 'Denver', TIMESTAMP '2026-01-02 00:00:00')
    """)
    adapter.execute("""
        UPDATE raw.customers
        SET email = 'alice@acme.io', updated_at = TIMESTAMP '2026-01-02 00:00:00'
        WHERE customer_id = 'c2'
    """)
    r2 = IDRRunner(adapter).run(RunConfig(run_mode="INCR", strict=True))
    assert r2.status == "SUCCESS"
    assert r2.activation_delta_summary == {
        "NEW": 1,
        "MERGED": 1,
        "RETIRED_MERGED": 1,
    }
    rows_r2 = {
        r["resolved_id"]: r
        for r in adapter.query(
            f"SELECT * FROM idr_out.identity_activation_delta WHERE run_id = '{r2.run_id}'"
        )
    }
    assert rows_r2["crm:c3"]["change_type"] == "NEW"
    assert rows_r2["crm:c3"]["activation_status"] == "ACTIVE"
    assert rows_r2["crm:c3"]["cluster_size"] == 1

    assert rows_r2["crm:c1"]["change_type"] == "MERGED"
    assert rows_r2["crm:c1"]["activation_status"] == "ACTIVE"
    assert json.loads(rows_r2["crm:c1"]["previous_resolved_ids_json"]) == ["crm:c2"]
    assert rows_r2["crm:c1"]["entities_added"] == 1

    assert rows_r2["crm:c2"]["change_type"] == "RETIRED_MERGED"
    assert rows_r2["crm:c2"]["activation_status"] == "RETIRED"
    assert rows_r2["crm:c2"]["superseded_by_resolved_id"] == "crm:c1"
    assert rows_r2["crm:c2"]["cluster_size"] == 0
    assert rows_r2["crm:c2"]["entities_removed"] == 1

    # 3. INCR NO-OP: c3 re-extracted with newer timestamp but identical identifiers & attributes
    adapter.execute("""
        UPDATE raw.customers
        SET updated_at = TIMESTAMP '2026-01-03 00:00:00'
        WHERE customer_id = 'c3'
    """)
    r3 = IDRRunner(adapter).run(RunConfig(run_mode="INCR", strict=True))
    assert r3.status == "SUCCESS"
    assert r3.activation_delta_summary == {}
    assert (
        adapter.query_one(
            f"SELECT COUNT(*) FROM idr_out.identity_activation_delta WHERE run_id = '{r3.run_id}'"
        )
        == 0
    )

    # 4. INCR PROFILE_CHANGED: c3 moves city from Denver -> Boston
    adapter.execute("""
        UPDATE raw.customers
        SET city = 'Boston', updated_at = TIMESTAMP '2026-01-04 00:00:00'
        WHERE customer_id = 'c3'
    """)
    r4 = IDRRunner(adapter).run(RunConfig(run_mode="INCR", strict=True))
    assert r4.status == "SUCCESS"
    assert r4.activation_delta_summary == {"PROFILE_CHANGED": 1}
    row_c3 = adapter.query(
        f"SELECT * FROM idr_out.identity_activation_delta WHERE run_id = '{r4.run_id}'"
    )[0]
    assert row_c3["resolved_id"] == "crm:c3"
    assert row_c3["change_type"] == "PROFILE_CHANGED"
    assert row_c3["profile_changed"] is True

    # 5. INCR SPLIT + RETIRED_DELETED:
    # - c2 changes email back to bob_new@acme.io (splits crm:c1 -> crm:c1 + crm:c2!)
    # - loyalty member l1 is deleted from keyless/no-watermark table raw.loyalty -> RETIRED_DELETED!
    adapter.execute("""
        UPDATE raw.customers
        SET email = 'bob_new@acme.io', updated_at = TIMESTAMP '2026-01-05 00:00:00'
        WHERE customer_id = 'c2'
    """)
    adapter.execute("DELETE FROM raw.loyalty WHERE member_id = 'l1'")
    r5 = IDRRunner(adapter).run(RunConfig(run_mode="INCR", strict=True))
    assert r5.status == "SUCCESS"
    assert r5.activation_delta_summary == {
        "SPLIT": 2,
        "RETIRED_DELETED": 1,
    }
    rows_r5 = {
        r["resolved_id"]: r
        for r in adapter.query(
            f"SELECT * FROM idr_out.identity_activation_delta WHERE run_id = '{r5.run_id}'"
        )
    }
    assert rows_r5["crm:c1"]["change_type"] == "SPLIT"
    assert rows_r5["crm:c1"]["entities_removed"] == 1
    assert rows_r5["crm:c2"]["change_type"] == "SPLIT"
    assert json.loads(rows_r5["crm:c2"]["previous_resolved_ids_json"]) == ["crm:c1"]
    assert rows_r5["loyalty:l1"]["change_type"] == "RETIRED_DELETED"
    assert rows_r5["loyalty:l1"]["activation_status"] == "RETIRED"
    assert rows_r5["loyalty:l1"]["superseded_by_resolved_id"] is None

    # 6. Per-run override emit_activation_delta=False suppresses changelog even when config is enabled
    adapter.execute("""
        UPDATE raw.customers
        SET city = 'Miami', updated_at = TIMESTAMP '2026-01-06 00:00:00'
        WHERE customer_id = 'c3'
    """)
    r6 = IDRRunner(adapter).run(
        RunConfig(run_mode="INCR", strict=True, emit_activation_delta=False)
    )
    assert r6.status == "SUCCESS"
    assert r6.activation_delta_summary == {}

    # 7. GDPR RTBF purges cluster references from identity_activation_delta
    purge = PrivacyManager(adapter).forget(resolved_id="crm:c3", dry_run=False)
    assert purge["status"] == "COMPLETED"
    assert (
        adapter.query_one(
            "SELECT COUNT(*) FROM idr_out.identity_activation_delta WHERE resolved_id = 'crm:c3'"
        )
        == 0
    )


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake", "bigquery", "databricks"])
def test_four_dialect_sql_invariants(dialect):
    sql = build_activation_delta_insert_sql(
        dialect=dialect,
        run_id="run_abc123",
        lens_id="default",
        run_mode="INCR",
        profile_attr_cols=["city", "tier"],
        has_clusters_table=True,
    )
    # Single statement per adapter.execute() (no trailing or internal statement-splitting semicolons)
    assert ";" not in sql.strip()
    assert "INSERT INTO idr_out.identity_activation_delta (" in sql
    if dialect == "bigquery":
        assert "FLOAT64" in sql
        assert "CAST(" not in sql or " AS FLOAT)" not in sql
        assert "STRING_AGG(" in sql
    elif dialect == "snowflake":
        assert "LISTAGG(" in sql
    elif dialect == "databricks":
        assert "COLLECT_SET(" in sql
    else:
        assert "STRING_AGG(" in sql

    prune_sql = build_retention_delete_sql(dialect=dialect, retention_days=30)
    assert ";" not in prune_sql.strip()
    assert "DELETE FROM idr_out.identity_activation_delta" in prune_sql


def test_api_and_mcp_activation_delta_surfaces(delta_env, monkeypatch):
    adapter, cfg = delta_env
    # Baseline FULL run with activation_delta off, then run INCR via API with emit_activation_delta=True
    IDRRunner(adapter).run(RunConfig(run_mode="FULL", strict=True))
    adapter.execute("""
        INSERT INTO raw.customers (customer_id, email, phone, city, updated_at) VALUES
        ('c9', 'zoe@acme.io', '5550999', 'Portland', TIMESTAMP '2026-01-02 00:00:00')
    """)

    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", True)
    mgr = get_manager()
    dev_key = deps.get_user_key({"sub": "dev-user", "email": "dev@local", "roles": ["admin"]})
    mgr.set_adapter(adapter, {"platform": "duckdb"})
    mgr.set_adapter_for_user(dev_key, adapter, {"platform": "duckdb"})

    client = TestClient(app, raise_server_exceptions=False)

    # 1. Schema endpoint includes idr_out.identity_activation_delta
    schema_resp = client.get("/api/schema")
    assert schema_resp.status_code == 200
    fqns = {t["fqn"] for t in schema_resp.json()}
    assert "idr_out.identity_activation_delta" in fqns

    # 2. POST /api/setup/run with emit_activation_delta=True
    run_resp = client.post(
        "/api/setup/run",
        json={
            "mode": "INCR",
            "strict": True,
            "max_iterations": 10,
            "dry_run": False,
            "emit_activation_delta": True,
        },
    )
    assert run_resp.status_code == 200
    run_data = run_resp.json()
    assert run_data["activation_delta_summary"] == {"NEW": 1}
    run_id = run_data["run_id"]

    # 3. GET /api/runs/{run_id} includes activation_delta_summary
    detail_resp = client.get(f"/api/runs/{run_id}")
    assert detail_resp.status_code == 200
    assert detail_resp.json()["activation_delta_summary"] == {"NEW": 1}

    # 4. GET /api/runs/{run_id}/activation-delta returns filterable rows
    rows_resp = client.get(f"/api/runs/{run_id}/activation-delta?change_type=NEW&limit=20")
    assert rows_resp.status_code == 200
    payload = rows_resp.json()
    assert payload["summary"] == {"NEW": 1}
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["resolved_id"] == "crm:c9"

    # 5. MCP tool get_activation_delta
    from idr_mcp.server import get_activation_delta

    mcp_res = get_activation_delta(run_id=run_id, change_type="NEW", limit=10)
    assert mcp_res["run_id"] == run_id
    assert mcp_res["summary"] == {"NEW": 1}
    assert mcp_res["rows"][0]["resolved_id"] == "crm:c9"

    # Ensure ActivationDeltaOptions validates cleanly
    opts = ActivationDeltaOptions(enabled=True)
    assert opts.retention_days == 30

    # 6. GET /api/setup/config preserves activation_delta extension & GET /api/lenses/{id}/clusters returns None confidence
    cfg_enabled = copy.deepcopy(cfg)
    cfg_enabled["activation_delta"] = {"enabled": True, "retention_days": 14}
    apply_config_to_warehouse(adapter, cfg_enabled)
    IDRRunner(adapter).run(RunConfig(run_mode="FULL", strict=True))

    setup_cfg_resp = client.get("/api/setup/config")
    assert setup_cfg_resp.status_code == 200
    assert setup_cfg_resp.json().get("activation_delta", {}).get("enabled") is True

    lens_resp = client.get("/api/lenses/email_only/clusters")
    assert lens_resp.status_code == 200
    lens_clusters = lens_resp.json()
    assert len(lens_clusters) > 0
    assert lens_clusters[0]["confidence_score"] is None

    # 7. GET /api/clusters returns multi-entity clusters first for Explorer cold-start
    top_resp = client.get("/api/clusters?limit=5")
    assert top_resp.status_code == 200
    top_clusters = top_resp.json()
    assert len(top_clusters) >= 1
    assert top_clusters[0]["cluster_size"] >= top_clusters[-1]["cluster_size"]

