"""
Tests for user-scoped connection state in idr_api.dependencies.IDRManager.
"""

import os
import sys

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
