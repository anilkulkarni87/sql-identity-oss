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
