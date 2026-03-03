"""
Secret-loading tests for MCP environment connection setup.
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


def test_connect_from_env_snowflake_prefers_secret_file(monkeypatch, tmp_path):
    server = _load_mcp_server(monkeypatch)
    token_path = tmp_path / "snowflake_password.txt"
    token_path.write_text("sf-file-secret\n", encoding="utf-8")

    captured = {}

    class FakeManager:
        def __init__(self):
            self.adapter = None
            self.metadata = None

        def set_adapter(self, adapter, metadata):
            self.adapter = adapter
            self.metadata = metadata

    fake_manager = FakeManager()
    monkeypatch.setattr(server.ConnectionManager, "instance", staticmethod(lambda: fake_manager))

    snowflake_pkg = types.ModuleType("snowflake")
    snowflake_connector = types.ModuleType("snowflake.connector")

    def _fake_connect(**kwargs):
        captured["kwargs"] = kwargs
        return object()

    snowflake_connector.connect = _fake_connect
    snowflake_pkg.connector = snowflake_connector
    monkeypatch.setitem(sys.modules, "snowflake", snowflake_pkg)
    monkeypatch.setitem(sys.modules, "snowflake.connector", snowflake_connector)

    adapter_mod = types.ModuleType("idr_core.adapters.snowflake")

    class FakeSnowflakeAdapter:
        def __init__(self, conn):
            self.conn = conn

    adapter_mod.SnowflakeAdapter = FakeSnowflakeAdapter
    monkeypatch.setitem(sys.modules, "idr_core.adapters.snowflake", adapter_mod)

    monkeypatch.setenv("IDR_PLATFORM", "snowflake")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "sf-env-secret")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD_FILE", str(token_path))
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "wh")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "db")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "PUBLIC")

    server.connect_from_env()

    assert captured["kwargs"]["password"] == "sf-file-secret"
    assert fake_manager.metadata == {"platform": "snowflake"}


def test_connect_from_env_databricks_prefers_secret_file(monkeypatch, tmp_path):
    server = _load_mcp_server(monkeypatch)
    token_path = tmp_path / "databricks_token.txt"
    token_path.write_text("dbx-file-token\n", encoding="utf-8")

    captured = {}

    class FakeManager:
        def __init__(self):
            self.adapter = None
            self.metadata = None

        def set_adapter(self, adapter, metadata):
            self.adapter = adapter
            self.metadata = metadata

    fake_manager = FakeManager()
    monkeypatch.setattr(server.ConnectionManager, "instance", staticmethod(lambda: fake_manager))

    databricks_pkg = types.ModuleType("databricks")
    databricks_sql = types.ModuleType("databricks.sql")

    def _fake_connect(**kwargs):
        captured["kwargs"] = kwargs
        return object()

    databricks_sql.connect = _fake_connect
    databricks_pkg.sql = databricks_sql
    monkeypatch.setitem(sys.modules, "databricks", databricks_pkg)
    monkeypatch.setitem(sys.modules, "databricks.sql", databricks_sql)

    adapter_mod = types.ModuleType("idr_core.adapters.databricks")

    class FakeDatabricksAdapter:
        def __init__(self, conn, catalog=None):
            self.conn = conn
            self.catalog = catalog

    adapter_mod.DatabricksAdapter = FakeDatabricksAdapter
    monkeypatch.setitem(sys.modules, "idr_core.adapters.databricks", adapter_mod)

    monkeypatch.setenv("IDR_PLATFORM", "databricks")
    monkeypatch.setenv("DATABRICKS_HOST", "dbc-host")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/protocolv1/o/1/1")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dbx-env-token")
    monkeypatch.setenv("DATABRICKS_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("DATABRICKS_CATALOG", "hive_metastore")

    server.connect_from_env()

    assert captured["kwargs"]["access_token"] == "dbx-file-token"
    assert fake_manager.metadata == {"platform": "databricks"}
