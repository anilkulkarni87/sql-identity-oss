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
