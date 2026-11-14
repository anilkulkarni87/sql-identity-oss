"""Tests for Task 1 (dedup/keyless sources), Task 2 (pluggable survivorship), and Task 3 (plugin discovery)."""

from types import SimpleNamespace
from unittest.mock import patch

import duckdb
import pytest

from idr_core.adapters import (
    get_adapter_class,
    list_adapters,
    load_adapter_plugins,
    register_adapter,
)
from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.canonicalizers import (
    is_registered_canonicalizer,
    list_canonicalizers,
    load_canonicalizer_plugins,
    register_canonicalizer,
)
from idr_core.cli import main as cli_main
from idr_core.config import config_to_sql, resolve_entity_key_expr, sql_to_config, validate_config
from idr_core.profile_builder import ProfileBuilder
from idr_core.profiler import IDRDataProfiler
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager
from idr_core.survivorship import (
    build_survivorship_expr,
    is_registered_survivorship_strategy,
    list_survivorship_strategies,
    load_survivorship_plugins,
    register_survivorship_strategy,
)


def _init_duckdb():
    conn = duckdb.connect(":memory:")
    adapter = DuckDBAdapter(conn)
    SchemaManager(adapter).initialize(reset=True)
    return conn, adapter


# ---------------------------------------------------------------------------
# Task 1: Duplicate-Key & Keyless Source Tables
# ---------------------------------------------------------------------------


def test_duplicate_and_null_entity_keys_dedup_e2e():
    conn, adapter = _init_duckdb()
    conn.execute("CREATE SCHEMA raw")
    conn.execute("""
        CREATE TABLE raw.events (
            id VARCHAR,
            email VARCHAR,
            first_name VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO raw.events VALUES
            ('u1', 'old@example.com', 'OldName', TIMESTAMP '2026-01-01 00:00:00'),
            ('u1', 'new@example.com', 'NewName', TIMESTAMP '2026-01-02 00:00:00'),
            (NULL, 'nullkey@example.com', 'Ghost', TIMESTAMP '2026-01-03 00:00:00'),
            ('u2', 'new@example.com', 'Other', TIMESTAMP '2026-01-02 00:00:00')
    """)

    cfg = {
        "sources": [
            {
                "id": "events",
                "table": "raw.events",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "deduplicate": True,
                "dedup_order_by": "updated_at DESC",
                "identifiers": [{"type": "email", "expr": "email"}],
                "attributes": [{"name": "first_name", "expr": "first_name"}],
            }
        ],
        "rules": [{"id": "r_email", "type": "EXACT", "match_keys": ["email"], "priority": 1}],
        "survivorship": [{"attribute": "first_name", "strategy": "RECENCY"}],
    }
    validate_config(cfg)
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    # Round-trip check
    reconstructed = sql_to_config(adapter)
    assert reconstructed["sources"][0]["deduplicate"] is True
    assert reconstructed["sources"][0]["dedup_order_by"] == "updated_at DESC"

    result = IDRRunner(adapter, RunConfig(run_mode="FULL")).run()
    assert result.status == "SUCCESS"
    # NULL row filtered out -> 2 entities (u1, u2), both sharing new@example.com -> 1 cluster
    assert result.entities_processed == 2
    assert adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_clusters_current") == 1
    assert adapter.query_one("SELECT first_name FROM idr_out.golden_profiles") in ("NewName", "Other")


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake", "bigquery", "databricks"])
def test_entity_key_auto_resolution_across_dialects(dialect):
    src = {
        "id": "web_events",
        "table": "raw.web_events",
        "entity_key": "AUTO",
        "key_columns": ["email", "session_id"],
    }
    expr = resolve_entity_key_expr(src, dialect=dialect)
    assert "CONCAT_WS('|'" in expr
    if dialect == "bigquery":
        assert "TO_HEX(SHA256(" in expr
    elif dialect in ("snowflake", "databricks"):
        assert "SHA2(" in expr
    else:
        assert "SHA256(" in expr


def test_entity_key_auto_e2e_duckdb():
    conn, adapter = _init_duckdb()
    conn.execute("CREATE SCHEMA raw")
    conn.execute("CREATE TABLE raw.leads (email VARCHAR, source_tag VARCHAR, ts TIMESTAMP)")
    conn.execute("""
        INSERT INTO raw.leads VALUES
            ('a@test.io', 'ad1', TIMESTAMP '2026-01-01 00:00:00'),
            ('a@test.io', 'ad2', TIMESTAMP '2026-01-02 00:00:00')
    """)
    cfg = {
        "sources": [
            {
                "id": "leads",
                "table": "raw.leads",
                "entity_key": "AUTO",
                "key_columns": ["email", "source_tag"],
                "watermark_column": "ts",
                "identifiers": [{"type": "email", "expr": "email"}],
                "attributes": [{"name": "source_tag", "expr": "source_tag"}],
            }
        ],
        "rules": [{"id": "r_email", "type": "EXACT", "match_keys": ["email"], "priority": 1}],
    }
    validate_config(cfg)
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    result = IDRRunner(adapter, RunConfig(run_mode="FULL")).run()
    assert result.status == "SUCCESS"
    assert result.entities_processed == 2
    assert adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_clusters_current") == 1


def test_profiler_flags_duplicate_entity_keys():
    conn, adapter = _init_duckdb()
    conn.execute("CREATE SCHEMA raw")
    conn.execute("CREATE TABLE raw.dup_tbl (customer_id INT, email VARCHAR, updated_at TIMESTAMP)")
    conn.execute("""
        INSERT INTO raw.dup_tbl VALUES
            (1, 'a@b.com', TIMESTAMP '2026-01-01 00:00:00'),
            (1, 'a@b.com', TIMESTAMP '2026-01-02 00:00:00')
    """)
    res = IDRDataProfiler(adapter).profile_and_generate_config(["raw.dup_tbl"])
    src = res["suggested_config"]["sources"][0]
    assert src.get("deduplicate") is True
    assert res["profiling_diagnostics"]["raw.dup_tbl"]["duplicate_entity_keys_detected"] is True


# ---------------------------------------------------------------------------
# Task 2: Pluggable Survivorship Strategy Registry & Martech Strategies
# ---------------------------------------------------------------------------


def test_martech_survivorship_strategies_duckdb_e2e():
    conn, adapter = _init_duckdb()
    conn.execute("CREATE SCHEMA raw")
    conn.execute("""
        CREATE TABLE raw.shopify (
            id VARCHAR, email VARCHAR, addr VARCHAR, opt_out BOOLEAN, ltv DOUBLE, ts TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE raw.crm (
            id VARCHAR, email VARCHAR, addr VARCHAR, opt_out BOOLEAN, ltv DOUBLE, ts TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO raw.shopify VALUES
            ('s1', 'alice@shop.com', 'Old Shopify St', FALSE, 100.0, TIMESTAMP '2026-01-01 00:00:00'),
            ('s2', 'alice@shop.com', 'Latest Shopify Ave', FALSE, 50.0, TIMESTAMP '2026-01-05 00:00:00')
    """)
    conn.execute("""
        INSERT INTO raw.crm VALUES
            ('c1', 'alice@shop.com', 'Newer CRM Blvd', TRUE, 25.0, TIMESTAMP '2026-01-10 00:00:00')
    """)

    cfg = {
        "sources": [
            {
                "id": "shopify",
                "table": "raw.shopify",
                "entity_key": "id",
                "watermark_column": "ts",
                "identifiers": [{"type": "email", "expr": "email"}],
                "attributes": [
                    {"name": "addr", "expr": "addr"},
                    {"name": "opt_out", "expr": "opt_out"},
                    {"name": "ltv", "expr": "ltv"},
                    {"name": "first_seen", "expr": "ts"},
                ],
            },
            {
                "id": "crm",
                "table": "raw.crm",
                "entity_key": "id",
                "watermark_column": "ts",
                "identifiers": [{"type": "email", "expr": "email"}],
                "attributes": [
                    {"name": "addr", "expr": "addr"},
                    {"name": "opt_out", "expr": "opt_out"},
                    {"name": "ltv", "expr": "ltv"},
                    {"name": "first_seen", "expr": "ts"},
                ],
            },
        ],
        "rules": [{"id": "r_email", "type": "EXACT", "match_keys": ["email"], "priority": 1}],
        "survivorship": [
            {
                "attribute": "addr",
                "strategy": "PRIORITY_RECENCY",
                "source_priority": ["shopify", "crm"],
            },
            {"attribute": "opt_out", "strategy": "ANY_TRUE"},
            {"attribute": "ltv", "strategy": "AGG_SUM"},
            {"attribute": "first_seen", "strategy": "EARLIEST"},
        ],
    }
    validate_config(cfg)
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    result = IDRRunner(adapter, RunConfig(run_mode="FULL")).run()
    assert result.status == "SUCCESS"

    row = adapter.query("SELECT addr, opt_out, ltv, first_seen FROM idr_out.golden_profiles")[0]
    # shopify beats crm (even though crm is newer), and s2 beats s1 by recency within shopify
    assert row["addr"] == "Latest Shopify Ave"
    # ANY_TRUE: crm has opt_out=TRUE so golden profile must be TRUE
    assert row["opt_out"] is True
    # AGG_SUM: 100 + 50 + 25 = 175
    assert row["ltv"] == pytest.approx(175.0)


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake", "bigquery", "databricks"])
@pytest.mark.parametrize(
    "strategy",
    [
        "RECENCY",
        "PRIORITY",
        "PRIORITY_RECENCY",
        "LATEST_FROM_SOURCE",
        "FREQUENCY",
        "ANY_TRUE",
        "ALL_TRUE",
        "EARLIEST",
        "AGG_MAX",
        "AGG_MIN",
        "AGG_SUM",
        "COLLECT_DISTINCT",
    ],
)
def test_all_survivorship_strategies_generate_sql_for_all_dialects(dialect, strategy):
    expr = build_survivorship_expr(
        "attr_col",
        {"strategy": strategy, "source_priority_list": ["crm", "web"]},
        dialect=dialect,
    )
    assert "AS attr_col" in expr


def test_custom_survivorship_strategy_registration():
    register_survivorship_strategy(
        "CUSTOM_FIRST",
        lambda attr, rule, dialect, lookup: f"MIN({attr}) AS {attr}",
    )
    assert is_registered_survivorship_strategy("CUSTOM_FIRST")
    assert "CUSTOM_FIRST" in list_survivorship_strategies()
    assert build_survivorship_expr("tier", {"strategy": "CUSTOM_FIRST"}) == "MIN(tier) AS tier"


# ---------------------------------------------------------------------------
# Task 3: Community Plugin Discovery (entry_points) & CLI
# ---------------------------------------------------------------------------


def test_entry_points_plugin_discovery():
    fake_canon_ep = SimpleNamespace(
        name="TEST_CANON_EP",
        load=lambda: (lambda raw_expr, dialect: f"UPPER({raw_expr})"),
    )
    fake_surv_ep = SimpleNamespace(
        name="TEST_SURV_EP",
        load=lambda: (lambda attr, rule, dialect, lookup: f"MAX({attr}) AS {attr}"),
    )
    fake_adapter_cls = type("FakeClickHouseAdapter", (), {})
    fake_adapter_ep = SimpleNamespace(name="clickhouse", load=lambda: fake_adapter_cls)

    def _mock_eps(group=None):
        if group == "idr.canonicalizers":
            return [fake_canon_ep]
        if group == "idr.survivorship_strategies":
            return [fake_surv_ep]
        if group == "idr.adapters":
            return [fake_adapter_ep]
        return []

    with (
        patch("idr_core.canonicalizers.entry_points", side_effect=_mock_eps),
        patch("idr_core.survivorship.entry_points", side_effect=_mock_eps),
        patch("idr_core.adapters.entry_points", side_effect=_mock_eps),
    ):
        assert load_canonicalizer_plugins(force=True) == 1
        assert load_survivorship_plugins(force=True) == 1
        assert load_adapter_plugins(force=True) == 1

    assert is_registered_canonicalizer("TEST_CANON_EP")
    assert "TEST_CANON_EP" in list_canonicalizers()
    assert is_registered_survivorship_strategy("TEST_SURV_EP")
    assert "clickhouse" in list_adapters()
    assert get_adapter_class("clickhouse") is fake_adapter_cls


def test_cli_plugins_list(capsys):
    rc = cli_main(["plugins", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "PRIORITY_RECENCY" in out
    assert "EMAIL_STANDARD" in out
    assert "duckdb" in out
