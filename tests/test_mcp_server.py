"""
Tests for idr_mcp.server tools against a DuckDB identity graph.
"""

import os
import sys

import duckdb
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.config import config_to_sql
from idr_core.connection_manager import ConnectionManager
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager
from idr_mcp import server as mcp_server


@pytest.fixture
def mcp_duckdb(monkeypatch):
    conn = duckdb.connect(":memory:")
    adapter = DuckDBAdapter(conn)
    sm = SchemaManager(adapter)
    sm.initialize(reset=True)

    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("""
        CREATE TABLE raw.customers (
            id VARCHAR, email VARCHAR, phone VARCHAR, first_name VARCHAR, updated_at TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO raw.customers VALUES
            ('u1', 'alice@example.com', '555-0100', 'Alice', '2026-01-01 00:00:00'),
            ('u2', 'alice@example.com', '555-0101', 'Alicia', '2026-01-02 00:00:00'),
            ('u3', 'solo@example.com', '555-0199', 'Solo', '2026-01-03 00:00:00'),
            ('u4', 'spam@example.com', '555-0000', 'Spam1', '2026-01-03 00:00:00'),
            ('u5', 'spam@example.com', '555-0000', 'Spam2', '2026-01-03 00:00:00'),
            ('u6', 'spam@example.com', '555-0000', 'Spam3', '2026-01-03 00:00:00')
    """)

    cfg = {
        "sources": [
            {
                "id": "customers",
                "table": "raw.customers",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [{"type": "email", "expr": "email"}],
                "attributes": [{"name": "first_name", "expr": "first_name"}],
            }
        ],
        "rules": [
            {
                "id": "r_email",
                "type": "EXACT",
                "match_keys": ["email"],
                "priority": 1,
                "max_group_size": 2,
            }
        ],
        "survivorship": [
            {"attribute": "first_name", "strategy": "RECENCY"},
        ],
    }
    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    runner = IDRRunner(adapter)
    runner.run(RunConfig(run_mode="FULL"))

    ConnectionManager.instance().set_adapter(adapter, {"platform": "duckdb"})
    yield adapter
    adapter.close()


def test_mcp_search_identifier_finds_multi_and_singleton_clusters(mcp_duckdb):
    res_multi = mcp_server.search_identifier("alice@example.com")
    assert "matches" in res_multi
    assert len(res_multi["matches"]) == 1
    assert res_multi["matches"][0]["cluster_size"] == 2

    # Singleton entity u3 has 0 edges in identity_edges_current, but is found via identity_identifiers_current
    res_solo = mcp_server.search_identifier("solo@example.com", identifier_type="email")
    assert "matches" in res_solo
    assert len(res_solo["matches"]) == 1
    assert res_solo["matches"][0]["cluster_size"] == 1


def test_mcp_get_cluster_and_golden_profile_pii_masking(mcp_duckdb, monkeypatch):
    matches = mcp_server.search_identifier("alice@example.com")["matches"]
    resolved_id = matches[0]["resolved_id"]

    # Default: masked PII
    monkeypatch.delenv("IDR_PII_ACCESS", raising=False)
    cluster_masked = mcp_server.get_cluster(resolved_id, include_edges=True, include_entities=True)
    assert cluster_masked["pii_access_level"] == "masked"
    assert len(cluster_masked["entities"]) == 2
    assert "***" in cluster_masked["edges"][0]["identifier_value_norm"]

    gp_masked = mcp_server.get_golden_profile(resolved_id)
    assert gp_masked["pii_access_level"] == "masked"
    assert gp_masked["run_id"] is not None
    assert not str(gp_masked["run_id"]).startswith("*")
    assert "***" in gp_masked["attributes"]["first_name"]

    # Full PII access
    monkeypatch.setenv("IDR_PII_ACCESS", "full")
    gp_full = mcp_server.get_golden_profile(resolved_id)
    assert gp_full["pii_access_level"] == "full"
    assert gp_full["attributes"]["first_name"] == "Alicia"


def test_mcp_skipped_groups_metrics_and_dry_run(mcp_duckdb, monkeypatch):
    monkeypatch.delenv("IDR_PII_ACCESS", raising=False)
    skipped = mcp_server.list_skipped_groups()
    assert len(skipped["skipped_groups"]) == 1
    assert skipped["skipped_groups"][0]["group_size"] == 3
    assert "***" in skipped["skipped_groups"][0]["identifier_value_norm"]

    summary = mcp_server.get_metrics_summary()
    assert summary["total_entities"] == 6
    assert summary["total_clusters"] >= 4

    dry = mcp_server.dry_run_pipeline(run_mode="FULL", max_iters=5)
    assert dry["status"] == "DRY_RUN_COMPLETE"
    assert "dry_run_summary" in dry
    assert dry["dry_run_summary"]["total_entities"] == 6


def test_mcp_all_14_tools_and_edge_explanation(mcp_duckdb, monkeypatch, tmp_path):
    monkeypatch.setenv("IDR_PII_ACCESS", "full")

    # 1. list_rules & list_sources
    rules_res = mcp_server.list_rules()
    assert len(rules_res["rules"]) == 1
    assert rules_res["rules"][0]["rule_id"] == "r_email"

    sources_res = mcp_server.list_sources()
    assert len(sources_res["sources"]) == 1
    assert sources_res["sources"][0]["table_id"] == "customers"

    # 2. run_history, latest_run, config_snapshot
    history_res = mcp_server.run_history(limit=5)
    assert len(history_res["runs"]) >= 1
    latest = mcp_server.latest_run()
    assert latest["status"] == "SUCCESS"

    snap = mcp_server.config_snapshot()
    assert "config_hash" in snap
    assert snap["config_hash"] is not None

    # 3. search_identifier with case-insensitive identifier_type
    search_upper = mcp_server.search_identifier("alice@example.com", identifier_type="EMAIL")
    assert len(search_upper["matches"]) == 1
    resolved_id = search_upper["matches"][0]["resolved_id"]

    # 4. list_edges_for_cluster & explain_edge (direct)
    edges_res = mcp_server.list_edges_for_cluster(resolved_id)
    assert len(edges_res["edges"]) == 1
    edge_info = mcp_server.explain_edge("customers:u1", "customers:u2")
    assert edge_info["connection_type"] == "direct"
    assert len(edge_info["evidence"]) == 1
    assert edge_info["evidence"][0]["match_value"] == "alice@example.com"

    # 5. explain_cluster includes golden_profile attributes
    cluster_exp = mcp_server.explain_cluster(resolved_id)
    assert cluster_exp["resolved_id"] == resolved_id
    assert cluster_exp["golden_profile"] is not None
    assert cluster_exp["golden_profile"]["first_name"] == "Alicia"

    # 6. connect_from_env via DuckDB file
    db_file = str(tmp_path / "env_mcp.duckdb")
    conn = duckdb.connect(db_file)
    conn.close()
    monkeypatch.setenv("IDR_PLATFORM", "duckdb")
    monkeypatch.setenv("IDR_DATABASE", db_file)
    mcp_server.connect_from_env()
    assert ConnectionManager.instance().get_adapter() is not None


def test_mcp_discover_and_suggest_rules(mcp_duckdb):
    """Test Direction 1: Autonomous profiling and rule suggestion from schema."""
    res = mcp_server.discover_and_suggest_rules()
    assert res.get("status") == "SUCCESS", res
    assert res["tables_profiled"] >= 1
    assert "email" in res["detected_identifiers"]
    assert "first_name" in res["detected_attributes"]

    cfg = res["suggested_config"]
    assert "sources" in cfg
    assert "rules" in cfg
    assert "survivorship" in cfg
    assert "exclusions" in cfg
    assert any(r["match_keys"] == ["email"] for r in cfg["rules"])
    assert any(s["attribute"] == "first_name" for s in cfg["survivorship"])

    # Test targeted table profiling via table_fqn
    res_table = mcp_server.discover_and_suggest_rules(table_fqn="raw.customers")
    assert res_table.get("status") == "SUCCESS", res_table
    assert res_table["tables_profiled"] == 1
    assert res_table["suggested_config"]["sources"][0]["table"] == "raw.customers"


def test_mcp_simulate_rule_impact(mcp_duckdb):
    """Test Direction 1: Safe dry-run rule impact and supernode risk simulation."""
    # Test valid rule simulation
    rule = {
        "id": "sim_email",
        "type": "EXACT",
        "match_keys": ["email"],
        "max_group_size": 10,
    }
    sim = mcp_server.simulate_rule_impact(rule)
    assert sim.get("status") == "SIMULATION_COMPLETE", sim
    assert sim["eligible_entities"] >= 2
    assert sim["simulated_anchor_edges"] >= 1
    assert sim["risk_level"] in ("LOW", "MEDIUM", "HIGH")
    assert isinstance(sim["risk_reasons"], list)

    # Test invalid rule spec (missing match_keys)
    bad_rule = {"id": "bad"}
    bad_res = mcp_server.simulate_rule_impact(bad_rule)
    assert "error" in bad_res


def test_mcp_resolve_customer_360(mcp_duckdb, monkeypatch):
    """Test Direction 2: Single-call Customer 360 context grounding for AI agents."""
    # 1. Successful lookup with masked PII
    monkeypatch.delenv("IDR_PII_ACCESS", raising=False)
    res_masked = mcp_server.resolve_customer_360("alice@example.com")
    assert res_masked["found"] is True
    assert res_masked["cluster_size"] == 2
    assert len(res_masked["linked_accounts"]) == 2
    assert res_masked["pii_access_level"] == "masked"
    assert "***" in res_masked["known_identifiers"][0]["value"]

    # 2. Successful lookup with full PII access
    monkeypatch.setenv("IDR_PII_ACCESS", "full")
    res_full = mcp_server.resolve_customer_360("alice@example.com")
    assert res_full["found"] is True
    assert res_full["pii_access_level"] == "full"
    assert res_full["known_identifiers"][0]["value"] == "alice@example.com"
    assert any(a["source_id"] == "u1" for a in res_full["linked_accounts"])

    # 3. Singleton entity lookup
    solo = mcp_server.resolve_customer_360("solo@example.com")
    assert solo["found"] is True
    assert solo["cluster_size"] == 1
    assert len(solo["linked_accounts"]) == 1

    # 4. Unknown identifier lookup returns found: False gracefully
    unknown = mcp_server.resolve_customer_360("does_not_exist@example.com")
    assert unknown["found"] is False
    assert "query" in unknown



