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


def test_incr_config_drift_blocks_execution():
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
    assert r2.status == "FAILED"
    assert "--mode FULL" in r2.error
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


def test_snowflake_keypair_and_databricks_oauth_m2m_kwargs(monkeypatch):
    import json
    from io import BytesIO
    from unittest.mock import patch

    from idr_core.connection_manager import (
        build_databricks_connect_kwargs,
        build_snowflake_connect_kwargs,
    )

    # Snowflake RSA Key-Pair (via path + env vars)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "https://xy12345.us-east-1.snowflakecomputing.com/")
    monkeypatch.setenv("SNOWFLAKE_USER", "svc_idr")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PATH", "/keys/rsa_key.p8")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "secret-pass")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "IDR_ROLE")
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)

    sf_kwargs = build_snowflake_connect_kwargs({})
    assert sf_kwargs["account"] == "xy12345.us-east-1"
    assert sf_kwargs["user"] == "svc_idr"
    assert sf_kwargs["private_key_file"] == "/keys/rsa_key.p8"
    assert sf_kwargs["private_key_file_pwd"] == "secret-pass"
    assert sf_kwargs["authenticator"] == "SNOWFLAKE_JWT"
    assert sf_kwargs["role"] == "IDR_ROLE"
    assert "password" not in sf_kwargs

    # Inline PEM RSA private key (generated in-memory with cryptography)
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem_str = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY_PATH", raising=False)
    sf_pem_kwargs = build_snowflake_connect_kwargs({"private_key": pem_str})
    assert isinstance(sf_pem_kwargs["private_key"], bytes)
    assert sf_pem_kwargs["authenticator"] == "SNOWFLAKE_JWT"

    # Databricks OAuth M2M token exchange fallback when databricks.sdk isn't mocked/installed
    class _Resp(BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    with patch(
        "urllib.request.urlopen",
        return_value=_Resp(json.dumps({"access_token": "oauth-m2m-jwt"}).encode("utf-8")),
    ):
        # Force OIDC HTTP exchange path even if databricks.sdk happens to be installed
        import builtins

        real_import = builtins.__import__

        def _no_sdk(name, *args, **kwargs):
            if name.startswith("databricks.sdk"):
                raise ImportError("no sdk")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _no_sdk)
        dbx_kwargs, catalog = build_databricks_connect_kwargs(
            {
                "server_hostname": "https://adb-123.azuredatabricks.net/",
                "http_path": "/sql/1.0/warehouses/abc",
                "client_id": "sp-client-id",
                "client_secret": "sp-client-secret",
                "catalog": "prod_cat",
            }
        )
        assert dbx_kwargs["server_hostname"] == "adb-123.azuredatabricks.net"
        assert dbx_kwargs["http_path"] == "/sql/1.0/warehouses/abc"
        assert dbx_kwargs["access_token"] == "oauth-m2m-jwt"
        assert catalog == "prod_cat"


def _make_fake_redis_and_adapter_classes():
    class FakeRedis:
        def __init__(self):
            self.data = {}

        def set(self, key, value, ex=None):
            self.data[key] = value

        def get(self, key):
            return self.data.get(key)

        def delete(self, key):
            self.data.pop(key, None)

        def ping(self):
            return True

    class DummyAdapter:
        def __init__(self, cfg):
            self.cfg = cfg
            self.closed = False

        def close(self):
            self.closed = True

    return FakeRedis, DummyAdapter


def test_redis_session_store_strips_plaintext_secrets_by_default(monkeypatch):
    import json

    from idr_core.connection_manager import build_snowflake_connect_kwargs
    from idr_enterprise.session_store import RedisConnectionSessionStore

    monkeypatch.delenv("IDR_SESSION_SECRET_KEY", raising=False)
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PATH", "/mounted/keys/rsa_key.p8")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "env-key-pass")

    FakeRedis, DummyAdapter = _make_fake_redis_and_adapter_classes()
    shared_redis = FakeRedis()
    captured = []

    def factory(cfg):
        captured.append(cfg)
        return DummyAdapter(cfg)

    w1 = RedisConnectionSessionStore(ttl_seconds=600, redis_client=shared_redis, adapter_factory=factory)
    w2 = RedisConnectionSessionStore(ttl_seconds=600, redis_client=shared_redis, adapter_factory=factory)

    secret_cfg = {
        "platform": "snowflake",
        "account": "xy12345.us-east-1",
        "user": "svc_idr",
        "password": "plaintext-password-secret",
        "private_key": "-----BEGIN PRIVATE KEY-----\nFAKE\n-----END PRIVATE KEY-----",
        "private_key_passphrase": "inline-key-passphrase",
        "access_token": "raw-access-token",
        "client_secret": "raw-oauth-secret",
    }
    w1.set_adapter("alice", DummyAdapter(secret_cfg), secret_cfg)

    raw_payload = shared_redis.data[w1._lease_key("alice")]
    for leaked in (
        "plaintext-password-secret",
        "-----BEGIN PRIVATE KEY-----",
        "inline-key-passphrase",
        "raw-access-token",
        "raw-oauth-secret",
    ):
        assert leaked not in raw_payload

    stored_cfg = json.loads(raw_payload)["config"]
    for secret_key in ("password", "private_key", "private_key_passphrase", "access_token", "client_secret"):
        assert secret_key not in stored_cfg

    # Worker 2 reconstructs cleanly without secrets in Redis by falling back to env credentials
    recovered_adapter = w2.get_adapter("alice")
    assert recovered_adapter is not None
    assert len(captured) == 1
    sf_kwargs = build_snowflake_connect_kwargs(captured[0])
    assert sf_kwargs["private_key_file"] == "/mounted/keys/rsa_key.p8"
    assert sf_kwargs["private_key_file_pwd"] == "env-key-pass"

    # Defensive scrub against legacy plaintext secrets already present in Redis
    shared_redis.data[w1._lease_key("legacy")] = json.dumps(
        {"updated_at": 1, "config": {"platform": "snowflake", "password": "legacy-plaintext-password"}}
    )
    legacy_cfg = w2._read_config_from_redis("legacy")
    assert legacy_cfg == {"platform": "snowflake"}


def test_redis_session_store_encrypts_secrets_when_key_configured(monkeypatch):
    import json

    from idr_enterprise.session_store import RedisConnectionSessionStore

    monkeypatch.setenv("IDR_SESSION_SECRET_KEY", "test-master-secret-key")
    FakeRedis, DummyAdapter = _make_fake_redis_and_adapter_classes()
    shared_redis = FakeRedis()
    captured = []

    def factory(cfg):
        captured.append(cfg)
        return DummyAdapter(cfg)

    w1 = RedisConnectionSessionStore(ttl_seconds=600, redis_client=shared_redis, adapter_factory=factory)
    w2 = RedisConnectionSessionStore(ttl_seconds=600, redis_client=shared_redis, adapter_factory=factory)

    cfg = {
        "platform": "databricks",
        "server_hostname": "adb-123.azuredatabricks.net",
        "http_path": "/sql/1.0/warehouses/abc",
        "client_id": "sp-client-id",
        "client_secret": "oauth-m2m-super-secret",
        "password": "warehouse-password-secret",
        "credentials_json": {"type": "service_account", "private_key": "sa-private-key"},
    }
    w1.set_adapter("bob", DummyAdapter(cfg), cfg)

    raw_payload = shared_redis.data[w1._lease_key("bob")]
    assert "oauth-m2m-super-secret" not in raw_payload
    assert "warehouse-password-secret" not in raw_payload
    assert "sa-private-key" not in raw_payload

    stored_cfg = json.loads(raw_payload)["config"]
    assert isinstance(stored_cfg["client_secret"], dict) and "__idr_enc__" in stored_cfg["client_secret"]
    assert isinstance(stored_cfg["password"], dict) and "__idr_enc__" in stored_cfg["password"]
    assert isinstance(stored_cfg["credentials_json"], dict) and "__idr_enc__" in stored_cfg["credentials_json"]

    # Worker 2 decrypts secrets transparently when reconstructing adapter
    w2_adapter = w2.get_adapter("bob")
    assert w2_adapter is not None
    assert captured[0]["client_secret"] == "oauth-m2m-super-secret"
    assert captured[0]["password"] == "warehouse-password-secret"
    assert captured[0]["credentials_json"] == {"type": "service_account", "private_key": "sa-private-key"}

    # Mismatched encryption key on another worker drops undecryptable fields safely
    monkeypatch.setenv("IDR_SESSION_SECRET_KEY", "wrong-key")
    mismatched_cfg = w2._read_config_from_redis("bob")
    assert "client_secret" not in mismatched_cfg
    assert "password" not in mismatched_cfg


def test_stage8_output_rollback_on_mid_stage_failure(monkeypatch):
    adapter = DuckDBAdapter(":memory:")
    _setup_crm_source(
        adapter,
        [
            (1, "a@example.com", "2026-01-01 00:00:00"),
            (2, "a@example.com", "2026-01-01 00:00:00"),
        ],
    )

    r1 = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert r1.status == "SUCCESS"

    def _snapshot_rows():
        return {
            "membership": adapter.query(
                "SELECT entity_key, resolved_id, run_id FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
            ),
            "clusters": adapter.query(
                "SELECT resolved_id, cluster_size, run_id FROM idr_out.identity_clusters_current ORDER BY resolved_id"
            ),
            "edges": adapter.query(
                "SELECT left_entity_key, right_entity_key, identifier_type, run_id FROM idr_out.identity_edges_current ORDER BY left_entity_key, right_entity_key"
            ),
            "identifiers": adapter.query(
                "SELECT entity_key, identifier_type, identifier_value_norm, run_id FROM idr_out.identity_identifiers_current ORDER BY entity_key, identifier_type"
            ),
            "run_state": adapter.query(
                "SELECT table_id, last_watermark_value, last_run_id FROM idr_meta.run_state ORDER BY table_id"
            ),
        }

    baseline = _snapshot_rows()
    assert len(baseline["membership"]) == 2
    assert len(baseline["clusters"]) == 1

    # Add new source entities that would mutate all idr_out.*_current tables in Run 2
    adapter.execute("INSERT INTO raw.crm VALUES (3, 'b@example.com', TIMESTAMP '2026-01-02 00:00:00')")
    adapter.execute("INSERT INTO raw.crm VALUES (4, 'b@example.com', TIMESTAMP '2026-01-02 00:00:00')")

    # Fail inside ProfileBuilder after OutputStage has already mutated membership, edges, identifiers, and clusters
    def _boom(*args, **kwargs):
        raise RuntimeError("Simulated warehouse OOM during golden profile build")

    monkeypatch.setattr(ProfileBuilder, "build_profiles", _boom)

    r2 = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert r2.status == "FAILED"

    # All idr_out.*_current tables AND idr_meta.run_state must match Run 1's exact state
    after_failure = _snapshot_rows()
    assert after_failure == baseline

    # Backup tables in idr_work must be cleaned up cleanly
    for tbl in (
        "_backup_identity_resolved_membership_current",
        "_backup_identity_edges_current",
        "_backup_identity_identifiers_current",
        "_backup_identity_clusters_current",
    ):
        assert not adapter.table_exists(f"idr_work.{tbl}")

    # Retry after transient issue must succeed (PRIMARY KEY constraints preserved on rollback)
    monkeypatch.undo()
    r3 = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert r3.status == "SUCCESS"
    final_membership = adapter.query(
        "SELECT entity_key FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
    )
    assert [r["entity_key"] for r in final_membership] == ["crm:1", "crm:2", "crm:3", "crm:4"]
    adapter.close()


def test_bare_like_exclusion_matches_substring_and_dry_run_reports_edge_delta(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    import idr_api.dependencies as deps
    from idr_api.main import app

    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", True)

    mgr = deps.get_manager()
    mgr.disconnect()

    db_file = str(tmp_path / "dry_run_excl.duckdb")
    adapter = DuckDBAdapter(db_file)
    _setup_crm_source(
        adapter,
        [
            (1, "donald.walker3812@gmail.com", "2026-01-01 00:00:00"),
            (2, "donald.walker3812@gmail.com", "2026-01-01 00:00:00"),
            (3, "john_doe@gmail.com", "2026-01-01 00:00:00"),
            (4, "john_doe@gmail.com", "2026-01-01 00:00:00"),
        ],
    )

    r1 = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert r1.status == "SUCCESS"
    assert r1.edges_created == 2

    # Bare LIKE pattern without '%' must still match 'donald.walker3812@gmail.com',
    # whereas EXACT with '_' must NOT act as wildcard against unrelated strings.
    adapter.execute("""
        INSERT INTO idr_meta.identifier_exclusion (identifier_type, identifier_value_pattern, match_type, reason)
        VALUES ('email', 'donald', 'LIKE', 'UI exclusion')
    """)
    adapter.execute("""
        INSERT INTO idr_meta.identifier_exclusion (identifier_type, identifier_value_pattern, match_type, reason)
        VALUES ('email', 'unrelated_underscore@gmail.com', 'EXACT', 'Exact underscore check')
    """)
    adapter.close()

    # Dry Run via /api/setup/run must surface edges_delta and clusters_delta even before writes
    tc = TestClient(app)
    assert tc.post("/api/connect", json={"platform": "duckdb", "database": db_file}).status_code == 200
    dry_resp = tc.post(
        "/api/setup/run",
        json={"mode": "FULL", "strict": False, "max_iterations": 20, "dry_run": True},
    )
    assert dry_resp.status_code == 200
    summary = dry_resp.json()["dry_run_summary"]
    assert summary["current_edges"] == 2
    assert summary["proposed_edges"] == 1
    assert summary["edges_delta"] == -1
    assert summary["current_clusters"] == 2
    assert summary["proposed_clusters"] == 3
    assert summary["clusters_delta"] == 1
    assert summary["proposed_changes"] > 0

    # Real INCR run applies bare LIKE exclusion cleanly while preserving john_doe cluster
    tc.post("/api/disconnect")
    adapter2 = DuckDBAdapter(db_file)
    r2 = IDRRunner(adapter2).run(RunConfig(run_mode="INCR"))
    assert r2.status == "SUCCESS"
    sizes = sorted(
        r["cluster_size"]
        for r in adapter2.query("SELECT cluster_size FROM idr_out.identity_clusters_current")
    )
    assert sizes == [1, 1, 2]
    adapter2.close()


def test_ephemeral_dry_run_does_not_mutate_saved_config_and_history_rollback(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    import idr_api.dependencies as deps
    from idr_api.main import app

    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", True)

    mgr = deps.get_manager()
    mgr.disconnect()

    db_file = str(tmp_path / "ephemeral_dry_run.duckdb")
    adapter = DuckDBAdapter(db_file)
    base_cfg = _setup_crm_source(
        adapter,
        [
            (1, "donald.walker3812@gmail.com", "2026-01-01 00:00:00"),
            (2, "donald.walker3812@gmail.com", "2026-01-01 00:00:00"),
            (3, "jane@example.com", "2026-01-01 00:00:00"),
            (4, "jane@example.com", "2026-01-01 00:00:00"),
        ],
    )
    assert IDRRunner(adapter).run(RunConfig(run_mode="FULL")).status == "SUCCESS"
    adapter.close()

    tc = TestClient(app)
    assert tc.post("/api/connect", json={"platform": "duckdb", "database": db_file}).status_code == 200

    # 1. Save initial clean config (0 exclusions) -> records snapshot #1
    save_v1 = tc.post("/api/setup/config/save", json={"config": base_cfg})
    assert save_v1.status_code == 200

    # 2. Run ephemeral dry run WITH draft exclusion without saving it to idr_meta
    draft_cfg = {
        **base_cfg,
        "exclusions": [
            {
                "type": "email",
                "value": "%donald%",
                "match": "LIKE",
                "reason": "UI exclusion",
            }
        ],
    }
    dry_resp = tc.post(
        "/api/setup/run",
        json={
            "mode": "FULL",
            "strict": False,
            "max_iterations": 20,
            "dry_run": True,
            "config": draft_cfg,
        },
    )
    assert dry_resp.status_code == 200
    summary = dry_resp.json()["dry_run_summary"]
    assert summary["edges_delta"] == -1
    assert summary["clusters_delta"] == 1

    # Saved idr_meta.identifier_exclusion MUST still have 0 exclusions after ephemeral dry run
    after_dry_cfg = tc.get("/api/setup/config").json()
    assert after_dry_cfg.get("exclusions", []) == []

    # 3. Save v2 with an exclusion, then verify /api/setup/config/history returns v2 & v1 for rollback
    assert tc.post("/api/setup/config/save", json={"config": draft_cfg}).status_code == 200
    hist_resp = tc.get("/api/setup/config/history")
    assert hist_resp.status_code == 200
    snaps = hist_resp.json()["snapshots"]
    assert len(snaps) >= 2
    assert snaps[0]["exclusions_count"] == 1
    assert snaps[1]["exclusions_count"] == 0

    # Roll back to v1 snapshot via /api/setup/config/save
    rollback_resp = tc.post("/api/setup/config/save", json={"config": snaps[1]["config"]})
    assert rollback_resp.status_code == 200
    restored_cfg = tc.get("/api/setup/config").json()
    assert restored_cfg.get("exclusions", []) == []
    tc.post("/api/disconnect")


def test_cli_ephemeral_dry_run_and_config_history_rollback(tmp_path, capsys):
    import yaml

    from idr_core.cli import main as cli_main
    from idr_core.config import sql_to_config

    db_file = str(tmp_path / "cli_parity.duckdb")
    adapter = DuckDBAdapter(db_file)
    base_cfg = _setup_crm_source(
        adapter,
        [
            (1, "donald.walker3812@gmail.com", "2026-01-01 00:00:00"),
            (2, "donald.walker3812@gmail.com", "2026-01-01 00:00:00"),
            (3, "jane@example.com", "2026-01-01 00:00:00"),
            (4, "jane@example.com", "2026-01-01 00:00:00"),
        ],
    )
    adapter.close()

    v1_path = tmp_path / "config_v1.yaml"
    v2_path = tmp_path / "config_v2.yaml"
    v1_path.write_text(yaml.safe_dump(base_cfg, sort_keys=False))
    v2_cfg = {
        **base_cfg,
        "exclusions": [
            {
                "type": "email",
                "value": "%donald%",
                "match": "LIKE",
                "reason": "Config exclusion",
            }
        ],
    }
    v2_path.write_text(yaml.safe_dump(v2_cfg, sort_keys=False))

    # 1. Apply v1 permanently & run FULL baseline
    assert cli_main(["config", "apply", "--file", str(v1_path), "--platform", "duckdb", "--db", db_file]) == 0
    assert cli_main(["run", "--platform", "duckdb", "--db", db_file, "--mode", "FULL"]) == 0
    capsys.readouterr()

    # 2. Ephemeral dry-run with v2 YAML (must preview -1 edge / +1 cluster WITHOUT persisting exclusion)
    assert cli_main([
        "run", "--platform", "duckdb", "--db", db_file,
        "--mode", "FULL", "--config", str(v2_path), "--dry-run",
    ]) == 0
    dry_out = capsys.readouterr().out
    assert "Edges Delta:     -1" in dry_out
    assert "Clusters Delta:  +1" in dry_out
    assert "Isolated (--dry-run left active idr_meta unchanged)" in dry_out

    check_conn = DuckDBAdapter(db_file)
    assert sql_to_config(check_conn).get("exclusions", []) == []
    check_conn.close()

    # 3. Permanently apply v2, inspect history, then roll back to v1 cleanly
    assert cli_main(["config", "apply", "--file", str(v2_path), "--platform", "duckdb", "--db", db_file]) == 0
    capsys.readouterr()
    assert cli_main(["config", "history", "--platform", "duckdb", "--db", db_file]) == 0
    hist_out = capsys.readouterr().out
    assert "(latest)" in hist_out

    assert cli_main(["config", "rollback", "--platform", "duckdb", "--db", db_file]) == 0
    rollback_out = capsys.readouterr().out
    assert "Rolled back configuration" in rollback_out

    check_after_rb = DuckDBAdapter(db_file)
    assert sql_to_config(check_after_rb).get("exclusions", []) == []
    check_after_rb.close()


def test_corroborating_edge_shadow_attribution_in_dry_run(tmp_path, capsys):
    import yaml

    from idr_core.cli import main as cli_main
    from idr_core.config import apply_config_to_warehouse, compute_dry_run_summary
    from idr_core.runner import IDRRunner, RunConfig

    db_file = str(tmp_path / "shadow_attr.duckdb")
    adapter = DuckDBAdapter(db_file)
    adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    adapter.execute(
        "CREATE TABLE raw.crm (id INTEGER, email VARCHAR, cust_id VARCHAR, updated_at TIMESTAMP)"
    )
    adapter.execute("""
        INSERT INTO raw.crm VALUES
            (1, 'donald@example.com', 'CUST_99', TIMESTAMP '2026-01-01 00:00:00'),
            (2, 'donald@example.com', 'CUST_99', TIMESTAMP '2026-01-01 00:00:00'),
            (3, 'alice@example.com',  'CUST_01', TIMESTAMP '2026-01-01 00:00:00')
    """)
    adapter.close()

    base_cfg = {
        "sources": [
            {
                "id": "crm",
                "table": "raw.crm",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                    {"type": "customer_id", "expr": "cust_id"},
                ],
                "attributes": [],
            }
        ],
        "rules": [
            {"id": "r_email", "type": "EXACT", "match_keys": ["email"], "canonicalize": "EMAIL_STANDARD"},
            {"id": "r_cust", "type": "EXACT", "match_keys": ["customer_id"], "canonicalize": "EXACT"},
        ],
        "exclusions": [],
        "overrides": [],
        "survivorship": [],
        "use_default_exclusions": False,
    }
    v1_path = tmp_path / "v1.yaml"
    v2_path = tmp_path / "v2.yaml"
    v1_path.write_text(yaml.safe_dump(base_cfg, sort_keys=False))
    v2_cfg = {
        **base_cfg,
        "exclusions": [
            {"type": "email", "value": "donald", "match": "LIKE", "reason": "Test shadow attribution"}
        ],
    }
    v2_path.write_text(yaml.safe_dump(v2_cfg, sort_keys=False))

    assert cli_main(["config", "apply", "--file", str(v1_path), "--platform", "duckdb", "--db", db_file]) == 0
    assert cli_main(["run", "--platform", "duckdb", "--db", db_file, "--mode", "FULL"]) == 0
    capsys.readouterr()

    # Dry run removes the EMAIL edge between crm:1 and crm:2 while CUSTOMER_ID preserves their cluster
    assert cli_main([
        "run", "--platform", "duckdb", "--db", db_file,
        "--mode", "FULL", "--config", str(v2_path), "--dry-run",
    ]) == 0
    out = capsys.readouterr().out
    assert "Moved Entities:  0" in out
    assert "Edges Delta:     -1" in out
    assert "Edges by Type:   EMAIL: -1" in out
    assert "Shadow-Kept:     2 entities (corroborated by: CUSTOMER_ID)" in out

    verify_adapter = DuckDBAdapter(db_file)
    res = IDRRunner(verify_adapter).run(RunConfig(run_mode="FULL", dry_run=True))
    # Re-apply exclusion ephemerally wasn't persisted; test compute_dry_run_summary directly with v2 applied
    apply_config_to_warehouse(verify_adapter, v2_cfg, record_snapshot=False)
    res_v2 = IDRRunner(verify_adapter).run(RunConfig(run_mode="FULL", dry_run=True))
    summary = compute_dry_run_summary(
        verify_adapter,
        res_v2.run_id,
        "FULL",
        edges_created=res_v2.edges_created,
        clusters_impacted=res_v2.clusters_impacted,
    )
    assert res.status == "DRY_RUN_COMPLETE"
    assert summary["edges_delta"] == -1
    assert summary["edges_delta_by_type"] == {"EMAIL": -1}
    assert summary["shadow_preserved_entities"] == 2
    assert summary["corroborating_identifier_types"] == ["CUSTOMER_ID"]
    verify_adapter.close()


def test_stage8_blast_radius_circuit_breaker_rolls_back_outputs(tmp_path, capsys):
    from idr_core.cli import main as cli_main
    from idr_core.config import apply_config_to_warehouse
    from idr_core.runner import IDRRunner, RunConfig

    db_file = str(tmp_path / "circuit_breaker.duckdb")
    adapter = DuckDBAdapter(db_file)
    adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    adapter.execute("CREATE TABLE raw.crm (id INTEGER, email VARCHAR, updated_at TIMESTAMP)")
    adapter.execute("""
        INSERT INTO raw.crm VALUES
            (1, 'a@example.com', TIMESTAMP '2026-01-01 00:00:00'),
            (2, 'a@example.com', TIMESTAMP '2026-01-01 00:00:00'),
            (3, 'b@example.com', TIMESTAMP '2026-01-01 00:00:00'),
            (4, 'b@example.com', TIMESTAMP '2026-01-01 00:00:00')
    """)
    cfg = {
        "sources": [
            {
                "id": "crm",
                "table": "raw.crm",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [{"type": "email", "expr": "email"}],
                "attributes": [],
            }
        ],
        "rules": [{"id": "r1", "type": "EXACT", "match_keys": ["email"], "canonicalize": "EMAIL_STANDARD"}],
        "exclusions": [],
        "overrides": [],
        "survivorship": [],
        "use_default_exclusions": False,
    }
    SchemaManager(adapter).initialize(reset=True)
    apply_config_to_warehouse(adapter, cfg)
    r1 = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert r1.status == "SUCCESS"

    baseline_membership = adapter.query(
        "SELECT entity_key, resolved_id, run_id FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
    )
    baseline_clusters = adapter.query(
        "SELECT resolved_id, cluster_size, run_id FROM idr_out.identity_clusters_current ORDER BY resolved_id"
    )
    baseline_watermarks = adapter.query(
        "SELECT table_id, last_watermark_value, last_run_id FROM idr_meta.run_state ORDER BY table_id"
    )

    # Introduce a FORCE_LINK override uniting the two clusters (2 of 4 entities move = 50%, clusters 2 -> 1 = -50%)
    cfg_merged = {
        **cfg,
        "overrides": [
            {
                "override_id": "ov_merge_all",
                "override_type": "FORCE_LINK",
                "entity_key_a": "crm:1",
                "entity_key_b": "crm:3",
                "reason": "Blast radius test",
            }
        ],
    }
    apply_config_to_warehouse(adapter, cfg_merged)

    r_blocked = IDRRunner(adapter).run(
        RunConfig(run_mode="FULL", max_moved_entities_pct=10.0)
    )
    assert r_blocked.status == "FAILED"
    assert "Blast-radius circuit breaker triggered" in (r_blocked.error or "")

    assert (
        adapter.query(
            "SELECT entity_key, resolved_id, run_id FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
        )
        == baseline_membership
    )
    assert (
        adapter.query(
            "SELECT resolved_id, cluster_size, run_id FROM idr_out.identity_clusters_current ORDER BY resolved_id"
        )
        == baseline_clusters
    )
    assert (
        adapter.query(
            "SELECT table_id, last_watermark_value, last_run_id FROM idr_meta.run_state ORDER BY table_id"
        )
        == baseline_watermarks
    )
    adapter.close()

    # Also test --max-cluster-delta-pct via CLI
    assert cli_main([
        "run", "--platform", "duckdb", "--db", db_file,
        "--mode", "FULL", "--max-cluster-delta-pct", "10.0",
    ]) == 1
    cli_out = capsys.readouterr().out
    assert "Blast-radius circuit breaker triggered" in cli_out

    # Raising the threshold allows the run to commit cleanly
    assert cli_main([
        "run", "--platform", "duckdb", "--db", db_file,
        "--mode", "FULL", "--max-cluster-delta-pct", "60.0", "--max-moved-pct", "60.0",
    ]) == 0
    capsys.readouterr()





