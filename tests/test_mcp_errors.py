"""
Error-envelope contract tests for idr_mcp server tools.
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


def test_mcp_not_connected_envelope_is_deterministic(monkeypatch):
    server = _load_mcp_server(monkeypatch)

    def _raise():
        raise RuntimeError("db secret should not leak")

    monkeypatch.setattr(server, "get_adapter", _raise)

    response = server.run_history()
    err = response["error"]

    assert err["schema"] == "mcp_error_v1"
    assert err["code"] == "MCP_NOT_CONNECTED"
    assert err["retryable"] is True
    assert err["context"]["tool"] == "run_history"
    assert "db secret should not leak" not in str(response)


def test_mcp_invalid_argument_error_for_limit(monkeypatch):
    server = _load_mcp_server(monkeypatch)

    class FakeAdapter:
        def query(self, _sql, params=None):
            raise AssertionError(f"query should not be called for invalid limit, params={params}")

    monkeypatch.setattr(server, "get_adapter", lambda: FakeAdapter())

    response = server.search_identifier("john", limit=0)
    err = response["error"]

    assert err["schema"] == "mcp_error_v1"
    assert err["code"] == "MCP_INVALID_ARGUMENT"
    assert err["message"] == "limit must be an integer between 1 and 50."
    assert err["context"] == {"tool": "search_identifier", "argument": "limit"}


def test_mcp_query_failures_do_not_leak_raw_exception_messages(monkeypatch):
    server = _load_mcp_server(monkeypatch)

    class FakeAdapter:
        def query(self, _sql, params=None):
            raise RuntimeError(f"sensitive db failure token, params={params}")

    monkeypatch.setattr(server, "get_adapter", lambda: FakeAdapter())

    response = server.config_snapshot()
    err = response["error"]

    assert err["schema"] == "mcp_error_v1"
    assert err["code"] == "MCP_QUERY_FAILED"
    assert err["retryable"] is True
    assert err["context"]["tool"] == "config_snapshot"
    assert "sensitive db failure token" not in err["message"]
    assert "sensitive db failure token" not in str(response)
