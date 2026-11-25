"""
Domain tests for survivorship strategy SQL generation, Martech survivorship E2E,
and `importlib.metadata.entry_points` plugin discovery (`idr plugins list`).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from idr_core.adapters import (
    get_adapter_class,
    list_adapters,
    load_adapter_plugins,
)
from idr_core.canonicalizers import (
    is_registered_canonicalizer,
    list_canonicalizers,
    load_canonicalizer_plugins,
)
from idr_core.cli import main as cli_main
from idr_core.config import config_to_sql, validate_config
from idr_core.runner import IDRRunner, RunConfig
from idr_core.survivorship import (
    build_survivorship_expr,
    is_registered_survivorship_strategy,
    list_survivorship_strategies,
    load_survivorship_plugins,
    register_survivorship_strategy,
)


@pytest.mark.e2e
def test_martech_survivorship_strategies_duckdb_e2e(duckdb_adapter):
    duckdb_adapter.execute("CREATE SCHEMA raw")
    duckdb_adapter.execute("""
        CREATE TABLE raw.shopify (
            id VARCHAR, email VARCHAR, addr VARCHAR, opt_out BOOLEAN, ltv DOUBLE, ts TIMESTAMP
        )
    """)
    duckdb_adapter.execute("""
        CREATE TABLE raw.crm (
            id VARCHAR, email VARCHAR, addr VARCHAR, opt_out BOOLEAN, ltv DOUBLE, ts TIMESTAMP
        )
    """)
    duckdb_adapter.execute("""
        INSERT INTO raw.shopify VALUES
            ('s1', 'alice@shop.com', 'Old Shopify St', FALSE, 100.0, TIMESTAMP '2026-01-01 00:00:00'),
            ('s2', 'alice@shop.com', 'Latest Shopify Ave', FALSE, 50.0, TIMESTAMP '2026-01-05 00:00:00')
    """)
    duckdb_adapter.execute("""
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
        duckdb_adapter.execute(stmt)

    result = IDRRunner(duckdb_adapter, RunConfig(run_mode="FULL")).run()
    assert result.status == "SUCCESS"

    row = duckdb_adapter.query(
        "SELECT addr, opt_out, ltv, first_seen FROM idr_out.golden_profiles"
    )[0]
    assert row["addr"] == "Latest Shopify Ave"
    assert row["opt_out"] is True
    assert row["ltv"] == pytest.approx(175.0)


@pytest.mark.unit
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


@pytest.mark.unit
def test_custom_survivorship_strategy_registration():
    register_survivorship_strategy(
        "CUSTOM_FIRST",
        lambda attr, rule, dialect, lookup: f"MIN({attr}) AS {attr}",
    )
    assert is_registered_survivorship_strategy("CUSTOM_FIRST")
    assert "CUSTOM_FIRST" in list_survivorship_strategies()
    assert build_survivorship_expr("tier", {"strategy": "CUSTOM_FIRST"}) == "MIN(tier) AS tier"


@pytest.mark.unit
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


@pytest.mark.integration
def test_cli_plugins_list(capsys):
    rc = cli_main(["plugins", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "PRIORITY_RECENCY" in out
    assert "EMAIL_STANDARD" in out
    assert "duckdb" in out
