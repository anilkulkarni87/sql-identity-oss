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


