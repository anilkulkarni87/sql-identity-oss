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
