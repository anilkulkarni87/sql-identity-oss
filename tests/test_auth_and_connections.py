"""Consolidated test suite: test_auth.py, test_session_store.py, test_connection_manager.py, test_credential_discovery.py."""

# --- Consolidated from test_auth.py ---
"""
Auth hardening tests for idr_api.dependencies.
"""

import asyncio
import os
import sys

import pytest

pytest.importorskip("fastapi")
from fastapi import HTTPException

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import idr_api.dependencies as deps
from idr_api.dependencies import JWTError


def test_get_jwk_for_token_retries_on_kid_miss(monkeypatch):
    async def fake_fetch_jwks(force=False):
        if force:
            return {"new-kid": {"kid": "new-kid"}}
        return {"old-kid": {"kid": "old-kid"}}

    monkeypatch.setattr(deps.jwt, "get_unverified_header", lambda _t: {"kid": "new-kid"})
    monkeypatch.setattr(deps, "_fetch_jwks", fake_fetch_jwks)

    key = asyncio.run(deps._get_jwk_for_token("token"))
    assert key["kid"] == "new-kid"


def test_get_jwk_for_token_unknown_kid_raises_401(monkeypatch):
    async def fake_fetch_jwks(force=False):
        return {"known-kid": {"kid": "known-kid"}}

    monkeypatch.setattr(deps.jwt, "get_unverified_header", lambda _t: {"kid": "missing-kid"})
    monkeypatch.setattr(deps, "_fetch_jwks", fake_fetch_jwks)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(deps._get_jwk_for_token("token"))

    assert exc.value.status_code == 401
    assert "Unknown signing key id" in exc.value.detail


def test_get_jwk_for_token_missing_kid_raises_401(monkeypatch):
    monkeypatch.setattr(deps.jwt, "get_unverified_header", lambda _t: {})

    with pytest.raises(HTTPException) as exc:
        asyncio.run(deps._get_jwk_for_token("token"))

    assert exc.value.status_code == 401
    assert "missing 'kid'" in exc.value.detail


def test_get_current_user_jwt_decode_error_maps_to_401(monkeypatch):
    monkeypatch.setattr(deps, "OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setattr(deps, "OIDC_AUDIENCE", "account")

    async def fake_get_jwk_for_token(_t):
        return {"kid": "k1"}

    monkeypatch.setattr(deps, "_get_jwk_for_token", fake_get_jwk_for_token)

    def fake_decode(*_args, **_kwargs):
        raise JWTError("bad signature")

    monkeypatch.setattr(deps.jwt, "decode", fake_decode)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(deps.get_current_user(token="token"))

    assert exc.value.status_code == 401
    assert "Could not validate credentials" in exc.value.detail


def test_get_current_user_jwks_fetch_error_maps_to_503(monkeypatch):
    monkeypatch.setattr(deps, "OIDC_ISSUER", "https://issuer.example.com")

    async def fake_get_jwk_for_token(_t):
        raise RuntimeError("jwks unavailable")

    monkeypatch.setattr(deps, "_get_jwk_for_token", fake_get_jwk_for_token)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(deps.get_current_user(token="token"))

    assert exc.value.status_code == 503
    assert "jwks unavailable" in exc.value.detail


def test_get_current_user_dev_mode_returns_default_user(monkeypatch):
    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", True)
    user = asyncio.run(deps.get_current_user(token=None))
    assert user["sub"] == "dev-user"


def test_get_current_user_dev_mode_fails_closed_without_override(monkeypatch):
    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", False)
    monkeypatch.delenv("IDR_ALLOW_INSECURE_DEV_AUTH", raising=False)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(deps.get_current_user(token=None))

    assert exc.value.status_code == 503
    assert "Authentication is not configured" in exc.value.detail


def test_get_current_user_dev_mode_allows_via_dynamic_env(monkeypatch):
    """Verify dynamic env var IDR_ALLOW_INSECURE_DEV_AUTH=true works even if module was loaded when False."""
    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", False)
    monkeypatch.setenv("IDR_ALLOW_INSECURE_DEV_AUTH", "true")

    user = asyncio.run(deps.get_current_user(token=None))
    assert user["sub"] == "dev-user"
    assert user["roles"] == ["admin"]


# --- Consolidated from test_session_store.py ---
"""
Tests for idr_api.session_store plugin loader and default OSS backend.
"""

import os
import sys

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from idr_api.session_store import InMemoryConnectionSessionStore, load_connection_session_store
from idr_enterprise.session_store import (
    EnterpriseInMemoryConnectionSessionStore,
    RedisConnectionSessionStore,
)


class CustomSessionStore:
    def __init__(self, ttl_seconds=0):
        self.ttl_seconds = ttl_seconds

    def set_adapter(self, user_key, adapter, config):
        return None

    def get_adapter(self, user_key):
        return None

    def get_config(self, user_key):
        return {}

    def disconnect_user(self, user_key):
        return None

    def disconnect_all(self):
        return None

    def get_any_adapter(self):
        return None

    def connection_count(self):
        return 0


class BrokenSessionStore:
    pass


def test_default_store_is_in_memory(monkeypatch):
    monkeypatch.delenv("IDR_SESSION_STORE_CLASS", raising=False)
    store = load_connection_session_store(ttl_seconds=42)
    assert isinstance(store, InMemoryConnectionSessionStore)


def test_custom_store_class_is_loaded(monkeypatch):
    monkeypatch.setenv(
        "IDR_SESSION_STORE_CLASS",
        f"{CustomSessionStore.__module__}.CustomSessionStore",
    )
    store = load_connection_session_store(ttl_seconds=123)
    assert isinstance(store, CustomSessionStore)
    assert store.ttl_seconds == 123


def test_custom_store_missing_methods_raises(monkeypatch):
    monkeypatch.setenv(
        "IDR_SESSION_STORE_CLASS",
        f"{BrokenSessionStore.__module__}.BrokenSessionStore",
    )
    with pytest.raises(TypeError, match="missing callable"):
        load_connection_session_store(ttl_seconds=1)


def test_enterprise_store_class_path_is_loadable(monkeypatch):
    monkeypatch.setenv(
        "IDR_SESSION_STORE_CLASS",
        "idr_enterprise.session_store.EnterpriseInMemoryConnectionSessionStore",
    )
    store = load_connection_session_store(ttl_seconds=456)
    assert isinstance(store, EnterpriseInMemoryConnectionSessionStore)


def test_redis_store_requires_redis_url(monkeypatch):
    monkeypatch.delenv("IDR_REDIS_URL", raising=False)
    with pytest.raises(RuntimeError, match="IDR_REDIS_URL"):
        RedisConnectionSessionStore(ttl_seconds=1)


def test_redis_store_reconstructs_adapter_across_workers():
    class FakeRedis:
        def __init__(self):
            self.data = {}

        def set(self, key, value, ex=None):
            self.data[key] = value

        def get(self, key):
            return self.data.get(key)

        def delete(self, key):
            self.data.pop(key, None)

    class DummyAdapter:
        def __init__(self, cfg):
            self.cfg = cfg
            self.closed = False

        def close(self):
            self.closed = True

    shared_redis = FakeRedis()
    reconstructed_calls = []

    def factory(cfg):
        reconstructed_calls.append(cfg)
        return DummyAdapter(cfg)

    worker_a = RedisConnectionSessionStore(
        ttl_seconds=300, redis_client=shared_redis, adapter_factory=factory
    )
    worker_b = RedisConnectionSessionStore(
        ttl_seconds=300, redis_client=shared_redis, adapter_factory=factory
    )

    initial_cfg = {"platform": "duckdb", "path": "/tmp/shared.duckdb"}
    worker_a.set_adapter("user-42", DummyAdapter(initial_cfg), initial_cfg)

    # Worker B has empty local memory, but reconstructs from shared Redis
    recovered = worker_b.get_adapter("user-42")
    assert recovered is not None
    assert recovered.cfg == initial_cfg
    assert worker_b.get_config("user-42") == initial_cfg
    assert len(reconstructed_calls) == 1

    # Disconnecting on Worker A removes Redis lease so a new Worker C won't recover it
    worker_a.disconnect_user("user-42")
    worker_c = RedisConnectionSessionStore(
        ttl_seconds=300, redis_client=shared_redis, adapter_factory=factory
    )
    assert worker_c.get_adapter("user-42") is None


# --- Consolidated from test_connection_manager.py ---
"""
Tests for user-scoped connection state in idr_api.dependencies.IDRManager.
"""

import os
import sys

import pytest

pytest.importorskip("fastapi")

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from idr_api.dependencies import get_manager


class DummyAdapter:
    def __init__(self, name: str):
        self.name = name
        self.closed = False

    def close(self):
        self.closed = True


def test_manager_scopes_connections_per_user():
    mgr = get_manager()
    mgr.disconnect()

    a1 = DummyAdapter("u1")
    a2 = DummyAdapter("u2")

    mgr.set_adapter_for_user("user-1", a1, {"platform": "duckdb"})
    mgr.set_adapter_for_user("user-2", a2, {"platform": "snowflake"})

    assert mgr.connection_count() == 2
    assert mgr.get_adapter_for_user("user-1") is a1
    assert mgr.get_adapter_for_user("user-2") is a2
    assert mgr.get_config_for_user("user-1")["platform"] == "duckdb"
    assert mgr.get_config_for_user("user-2")["platform"] == "snowflake"

    mgr.disconnect_user("user-1")
    assert a1.closed is True
    assert mgr.connection_count() == 1
    assert mgr.get_adapter_for_user("user-1") is None
    assert mgr.get_adapter_for_user("user-2") is a2

    mgr.disconnect()
    assert a2.closed is True
    assert mgr.connection_count() == 0


def test_set_adapter_none_clears_all_connections_for_compatibility():
    mgr = get_manager()
    mgr.disconnect()

    a1 = DummyAdapter("u1")
    a2 = DummyAdapter("u2")
    mgr.set_adapter_for_user("user-1", a1, {})
    mgr.set_adapter_for_user("user-2", a2, {})

    # Backward-compatible cleanup path used in existing tests.
    mgr.set_adapter(None, {})

    assert a1.closed is True
    assert a2.closed is True
    assert mgr.connection_count() == 0


def test_connection_state_machine_transitions():
    from idr_core.connection_manager import ConnectionManager, ConnectionState

    cm = ConnectionManager()
    cm.disconnect()

    assert cm.get_state() == ConnectionState.DISCONNECTED

    class MockAdapter:
        def __init__(self, alive=True, tables=None):
            self.alive = alive
            self.tables = set(tables or [])
            self.closed = False

        def query_one(self, sql, params=None):
            if not self.alive:
                raise RuntimeError("Warehouse connection closed")
            return 1

        def table_exists(self, fqn):
            return fqn in self.tables

        def close(self):
            self.closed = True

    # 1. Connected with schemas ready
    ready_adapter = MockAdapter(alive=True, tables=["idr_meta.source_table"])
    cm.set_adapter(ready_adapter, {"platform": "duckdb"})
    assert cm.get_state() == ConnectionState.CONNECTED_READY

    # 2. Connected but uninitialized (no metadata schema yet)
    uninit_adapter = MockAdapter(alive=True, tables=[])
    cm.set_adapter(uninit_adapter, {"platform": "snowflake"})
    assert cm.get_state() == ConnectionState.CONNECTED_UNINITIALIZED

    # 3. Stale connection (e.g. warehouse auto-suspended or token revoked)
    stale_adapter = MockAdapter(alive=False)
    cm.set_adapter(stale_adapter, {"platform": "bigquery"})
    assert cm.get_state() == ConnectionState.STALE

    # 4. Disconnect returns to DISCONNECTED
    cm.disconnect()
    assert cm.get_state() == ConnectionState.DISCONNECTED
    assert stale_adapter.closed is True


def test_run_status_and_pipeline_stage_enums():
    from idr_core.runner import PipelineStage, RunStatus

    assert RunStatus.SUCCESS == "SUCCESS"
    assert RunStatus.FAILED == "FAILED"
    assert RunStatus.DRY_RUN_COMPLETE == "DRY_RUN_COMPLETE"

    assert PipelineStage.PREFLIGHT == "preflight"
    assert PipelineStage.EDGE_BUILDING == "edge_building"
    assert PipelineStage.LABEL_PROPAGATION == "label_propagation"
    assert PipelineStage.OUTPUT_GENERATION == "output_generation"


# --- Consolidated from test_credential_discovery.py ---
import os
from pathlib import Path
from idr_core.credential_discovery import (
    discover_all,
    discover_all_connections,
    discover_from_databrickscfg,
    discover_from_dbt_profiles,
    discover_from_env,
    discover_from_snowflake_config,
    _mask,
    _sanitize_host,
)


def test_mask_helper():
    assert _mask("secret123456", show_last=4) == "********3456"
    assert _mask("abc", show_last=4) == "****"
    assert _mask("", show_last=4) == ""
    assert _mask(None, show_last=4) == ""


def test_discover_from_env(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://dbc-xyz.cloud.databricks.com/")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi_test_token")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/endpoints/123")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "xy12345.us-east-1")
    monkeypatch.setenv("BIGQUERY_PROJECT", "my-gcp-project")
    monkeypatch.setenv("IDR_DB_PATH", "/tmp/test.duckdb")

    conns = discover_from_env()
    platforms = {c["platform"] for c in conns}
    assert "databricks" in platforms
    assert "snowflake" in platforms
    assert "bigquery" in platforms
    assert "duckdb" in platforms

    db_conn = next(c for c in conns if c["platform"] == "databricks")
    assert db_conn["params"]["server_hostname"] == "dbc-xyz.cloud.databricks.com"
    assert db_conn["params"]["access_token"] == "dapi_test_token"


def test_discover_from_databrickscfg(tmp_path):
    cfg = tmp_path / ".databrickscfg"
    cfg.write_text("""[DEFAULT]
host = https://dbc-default.cloud.databricks.com
token = dapi_default_123
http_path = /sql/1.0/endpoints/abc

[PROD]
host = dbc-prod.cloud.databricks.com
token = dapi_prod_456
""")

    conns = discover_from_databrickscfg(cfg)
    assert len(conns) == 2
    assert conns[0]["name"] == "Databricks CLI Profile [DEFAULT]"
    assert conns[0]["params"]["server_hostname"] == "dbc-default.cloud.databricks.com"
    assert conns[1]["name"] == "Databricks CLI Profile [PROD]"


def test_discover_from_dbt_profiles(tmp_path):
    profiles = tmp_path / "profiles.yml"
    profiles.write_text("""
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: test_account
      user: test_user
      password: test_password
      warehouse: test_wh
      database: test_db
      schema: test_schema
    prod:
      type: databricks
      host: https://dbc-dbt.cloud.databricks.com
      http_path: /sql/endpoints/xyz
      token: dapi_dbt_token
      catalog: analytics
""")

    conns = discover_from_dbt_profiles(profiles)
    assert len(conns) == 2
    sf = next(c for c in conns if c["platform"] == "snowflake")
    assert "default target" in sf["name"]
    assert sf["params"]["account"] == "test_account"

    db = next(c for c in conns if c["platform"] == "databricks")
    assert db["params"]["server_hostname"] == "dbc-dbt.cloud.databricks.com"
    assert db["params"]["catalog"] == "analytics"


def test_discover_all_connections_deduplication(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABRICKS_HOST", "dbc-shared.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "token123")

    cfg = tmp_path / ".databrickscfg"
    cfg.write_text("""[DEFAULT]
host = https://dbc-shared.databricks.com
token = token123
""")

    # When both env and databrickscfg point to same host, discover_all_connections deduplicates
    conns = discover_all_connections()
    db_conns = [c for c in conns if c.get("params", {}).get("server_hostname") == "dbc-shared.databricks.com"]
    assert len(db_conns) == 1
    assert discover_all == discover_all_connections


def test_discover_from_headless_databrickscfg(tmp_path):
    """Test when ~/.databrickscfg starts directly with key-values (no [DEFAULT] header)."""
    cfg = tmp_path / ".databrickscfg"
    cfg.write_text("""host=https://dbc-headless.cloud.databricks.com/
http_path=/sql/1.0/warehouses/abcdef123
token=dapi%special%token%123
""")

    conns = discover_from_databrickscfg(cfg)
    assert len(conns) == 1
    c = conns[0]
    assert c["platform"] == "databricks"
    assert c["name"] == "Databricks CLI Profile [DEFAULT]"
    assert c["params"]["server_hostname"] == "dbc-headless.cloud.databricks.com"
    assert c["params"]["http_path"] == "/sql/1.0/warehouses/abcdef123"
    assert c["params"]["access_token"] == "dapi%special%token%123"


def test_sanitize_host_edge_cases():
    assert _sanitize_host("https://dbc-xyz.cloud.databricks.com/") == "dbc-xyz.cloud.databricks.com"
    assert _sanitize_host("http://dbc-xyz.cloud.databricks.com:443/sql/1.0/endpoints") == "dbc-xyz.cloud.databricks.com:443"
    assert _sanitize_host('"https://dbc-quotes.cloud.databricks.com"') == "dbc-quotes.cloud.databricks.com"
    assert _sanitize_host("   'https://dbc-spaces.cloud.databricks.com/?o=123'  ") == "dbc-spaces.cloud.databricks.com"
    assert _sanitize_host(None) == ""
