"""
Contract tests for idr_mcp server tool behavior.
"""

import importlib
import sys
import types


def _load_mcp_server(monkeypatch):
    class DummyFastMCP:
        def __init__(self, _name):
            pass

        def tool(self):
            def _decorator(func):
                return func

            return _decorator

        def run(self):
            return None

    mcp_module = types.ModuleType("mcp")
    mcp_server_module = types.ModuleType("mcp.server")
    mcp_fastmcp_module = types.ModuleType("mcp.server.fastmcp")
    mcp_fastmcp_module.FastMCP = DummyFastMCP

    monkeypatch.setitem(sys.modules, "mcp", mcp_module)
    monkeypatch.setitem(sys.modules, "mcp.server", mcp_server_module)
    monkeypatch.setitem(sys.modules, "mcp.server.fastmcp", mcp_fastmcp_module)
    sys.modules.pop("idr_mcp.server", None)

    return importlib.import_module("idr_mcp.server")


def test_get_golden_profile_contract(monkeypatch):
    server = _load_mcp_server(monkeypatch)
    captured = {}

    class FakeAdapter:
        def query(self, sql, params=None):
            captured["sql"] = sql
            captured["params"] = params
            return [
                {
                    "resolved_id": "res_123",
                    "email_primary": "jane.doe@example.com",
                    "phone_primary": "4155559911",
                    "first_name": "Jane",
                    "last_name": "Doe",
                    "updated_ts": "2026-02-25T00:00:00Z",
                }
            ]

    monkeypatch.setattr(server, "get_adapter", lambda: FakeAdapter())
    monkeypatch.delenv("IDR_PII_ACCESS", raising=False)

    response = server.get_golden_profile("res_123")

    assert "idr_out.golden_profile_current" in captured["sql"]
    assert captured["params"] == ["res_123"]
    assert response["resolved_id"] == "res_123"
    assert response["pii_access_level"] == "masked"
    assert response["attributes"]["email_primary"] == "ja***om"
    assert response["attributes"]["phone_primary"] == "41***11"
    assert "updated_ts" not in response["attributes"]


def test_get_cluster_contract_masks_edges_and_includes_members(monkeypatch):
    server = _load_mcp_server(monkeypatch)
    seen = {"queries": []}

    class FakeAdapter:
        def query(self, sql, params=None):
            seen["queries"].append((sql, params))
            if "identity_clusters_current" in sql:
                return [{"resolved_id": "res_1", "cluster_size": 2, "run_id": "run_1"}]
            if "SELECT * FROM idr_out.identity_edges_current" in sql:
                return [
                    {
                        "left_entity_key": "e1",
                        "right_entity_key": "e2",
                        "identifier_type": "email",
                        "identifier_value_norm": "jane.doe@example.com",
                    }
                ]
            if "identity_resolved_membership_current" in sql:
                return [{"entity_key": "e1", "resolved_id": "res_1", "run_id": "run_1"}]
            raise AssertionError(f"Unexpected SQL: {sql}")

    monkeypatch.setattr(server, "get_adapter", lambda: FakeAdapter())
    monkeypatch.delenv("IDR_PII_ACCESS", raising=False)

    response = server.get_cluster("res_1", include_edges=True, include_entities=True)

    assert response["resolved_id"] == "res_1"
    assert response["pii_access_level"] == "masked"
    assert response["entities"][0]["entity_key"] == "e1"
    assert response["edges"][0]["identifier_value_norm"] == "ja***om"
    assert seen["queries"][0][1] == ["res_1"]


def test_get_cluster_contract_not_found_envelope(monkeypatch):
    server = _load_mcp_server(monkeypatch)

    class FakeAdapter:
        def query(self, _sql, params=None):
            assert params == ["missing_cluster"]
            return []

    monkeypatch.setattr(server, "get_adapter", lambda: FakeAdapter())
    response = server.get_cluster("missing_cluster")

    assert response["error"]["schema"] == "mcp_error_v1"
    assert response["error"]["code"] == "MCP_NOT_FOUND"
    assert response["error"]["context"] == {
        "resolved_id": "missing_cluster",
        "tool": "get_cluster",
    }


def test_search_identifier_contract_escapes_literal_input_and_applies_type_filter(monkeypatch):
    server = _load_mcp_server(monkeypatch)
    captured = {}

    class FakeAdapter:
        def query(self, sql, params=None):
            captured["sql"] = sql
            captured["params"] = params
            return [{"resolved_id": "res_12", "cluster_size": 3}]

    monkeypatch.setattr(server, "get_adapter", lambda: FakeAdapter())
    monkeypatch.delenv("IDR_PII_ACCESS", raising=False)

    response = server.search_identifier("john_%\\doe", identifier_type="email", limit=500)

    assert "identifier_type = ?" in captured["sql"]
    assert "LIMIT 50" in captured["sql"]
    assert captured["params"] == [r"%john\_\%\\doe%", "email"]
    assert response["matches"][0]["resolved_id"] == "res_12"
    assert response["pii_access_level"] == "masked"


def test_search_identifier_contract_invalid_limit_returns_error(monkeypatch):
    server = _load_mcp_server(monkeypatch)

    class FakeAdapter:
        def query(self, _sql, params=None):
            raise AssertionError(f"query should not run for invalid limit, params={params}")

    monkeypatch.setattr(server, "get_adapter", lambda: FakeAdapter())

    response = server.search_identifier("john", limit=0)

    assert response["error"]["schema"] == "mcp_error_v1"
    assert response["error"]["code"] == "MCP_INVALID_ARGUMENT"
    assert response["error"]["context"] == {"tool": "search_identifier", "argument": "limit"}


def test_run_history_contract_happy_path_clamps_limit(monkeypatch):
    server = _load_mcp_server(monkeypatch)
    captured = {}

    class FakeAdapter:
        def query(self, sql, params=None):
            captured["sql"] = sql
            captured["params"] = params
            return [{"run_id": "run_42", "status": "succeeded"}]

    monkeypatch.setattr(server, "get_adapter", lambda: FakeAdapter())

    response = server.run_history(limit=999)

    assert "FROM idr_out.run_history" in captured["sql"]
    assert "LIMIT 100" in captured["sql"]
    assert captured["params"] is None
    assert response["runs"][0]["run_id"] == "run_42"


def test_run_history_contract_query_error_envelope(monkeypatch):
    server = _load_mcp_server(monkeypatch)

    class FakeAdapter:
        def query(self, _sql, params=None):
            raise RuntimeError(f"internal details should not leak, params={params}")

    monkeypatch.setattr(server, "get_adapter", lambda: FakeAdapter())
    response = server.run_history(limit=10)

    assert response["error"]["schema"] == "mcp_error_v1"
    assert response["error"]["code"] == "MCP_QUERY_FAILED"
    assert response["error"]["context"]["tool"] == "run_history"
    assert "internal details should not leak" not in response["error"]["message"]


def test_config_snapshot_contract_latest_happy_path(monkeypatch):
    server = _load_mcp_server(monkeypatch)
    captured = {}

    class FakeAdapter:
        def query(self, sql, params=None):
            captured["sql"] = sql
            captured["params"] = params
            return [
                {
                    "config_hash": "cfg_1",
                    "sources_json": '{"sources":[]}',
                    "rules_json": '{"rules":[]}',
                    "mappings_json": '{"mappings":[]}',
                    "created_at": "2026-02-26T10:00:00Z",
                }
            ]

    monkeypatch.setattr(server, "get_adapter", lambda: FakeAdapter())

    response = server.config_snapshot()

    assert "ORDER BY created_at DESC LIMIT 1" in captured["sql"]
    assert captured["params"] is None
    assert response["config_hash"] == "cfg_1"
    assert response["created_at"] == "2026-02-26T10:00:00Z"


def test_config_snapshot_contract_by_hash_and_not_found(monkeypatch):
    server = _load_mcp_server(monkeypatch)
    captured = {}

    class FakeAdapter:
        def query(self, sql, params=None):
            captured["sql"] = sql
            captured["params"] = params
            return []

    monkeypatch.setattr(server, "get_adapter", lambda: FakeAdapter())
    response = server.config_snapshot(config_hash="cfg_missing")

    assert "WHERE config_hash = ?" in captured["sql"]
    assert captured["params"] == ["cfg_missing"]
    assert response["error"]["schema"] == "mcp_error_v1"
    assert response["error"]["code"] == "MCP_NOT_FOUND"
    assert response["error"]["context"]["tool"] == "config_snapshot"


def test_config_snapshot_contract_query_error_envelope(monkeypatch):
    server = _load_mcp_server(monkeypatch)

    class FakeAdapter:
        def query(self, _sql, params=None):
            raise RuntimeError(f"db stack leak check, params={params}")

    monkeypatch.setattr(server, "get_adapter", lambda: FakeAdapter())
    response = server.config_snapshot()

    assert response["error"]["schema"] == "mcp_error_v1"
    assert response["error"]["code"] == "MCP_QUERY_FAILED"
    assert response["error"]["retryable"] is True
    assert response["error"]["context"]["tool"] == "config_snapshot"
    assert "db stack leak check" not in response["error"]["message"]
