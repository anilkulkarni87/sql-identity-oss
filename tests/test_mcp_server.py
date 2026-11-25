"""
Tests for idr_mcp.server tools against a DuckDB identity graph.
"""

import hashlib
import hmac
import json
import os
import sys
from unittest.mock import patch

import duckdb
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.config import apply_config_to_warehouse, config_to_sql
from idr_core.connection_manager import ConnectionManager
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager
from idr_core.webhooks import build_webhook_signature, emit_webhook_event
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


@pytest.mark.integration
def test_mcp_domain_neutral_tools_and_legacy_aliases(duckdb_adapter, monkeypatch):
    """Verify resolve_entity_360 and forget_entity_identity match legacy customer aliases."""
    duckdb_adapter.execute("CREATE SCHEMA raw")
    duckdb_adapter.execute(
        "CREATE TABLE raw.accounts (id VARCHAR, domain VARCHAR, org_name VARCHAR)"
    )
    duckdb_adapter.execute("""
        INSERT INTO raw.accounts VALUES
        ('a1', 'acme.io', 'Acme Inc'),
        ('a2', 'acme.io', 'Acme Incorporated')
    """)

    cfg = {
        "sources": [
            {
                "id": "acct",
                "table": "raw.accounts",
                "entity_type": "PRIMARY",
                "entity_kind": "organization",
                "entity_key": "id",
                "identifiers": [{"type": "DOMAIN", "expr": "domain"}],
                "attributes": [{"name": "org_name", "expr": "org_name"}],
            }
        ],
        "rules": [{"id": "exact_domain", "type": "EXACT", "match_keys": ["DOMAIN"], "priority": 1}],
        "survivorship": [{"attribute": "org_name", "strategy": "FREQUENCY"}],
    }
    apply_config_to_warehouse(duckdb_adapter, cfg)
    IDRRunner(duckdb_adapter).run(RunConfig(run_mode="FULL", strict=True))

    ConnectionManager.instance().set_adapter(duckdb_adapter, {"platform": "duckdb"})
    monkeypatch.setenv("IDR_PII_ACCESS", "full")

    ent_360 = mcp_server.resolve_entity_360("acme.io")
    cust_360 = mcp_server.resolve_customer_360("acme.io")
    assert ent_360["found"] is True
    assert ent_360["cluster_size"] == 2
    assert ent_360["resolved_id"] == cust_360["resolved_id"]

    preview = mcp_server.forget_entity_identity(identifier_value="acme.io", dry_run=True)
    assert preview["status"] == "DRY_RUN"
    assert preview["entities_purged"] == 2
    token = preview["confirmation_token"]

    purged = mcp_server.forget_entity_identity(
        identifier_value="acme.io",
        dry_run=False,
        confirmation_token=token,
    )
    assert purged["status"] == "COMPLETED"
    assert purged["entities_purged"] == 2


@pytest.mark.unit
def test_webhook_emitter_and_signature():
    """Verify HMAC-SHA256 signing, payload envelope, and graceful failure handling."""
    captured = {}

    class _Resp:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def _fake_urlopen(req, timeout=3.0):
        captured["url"] = req.full_url
        captured["body"] = req.data
        captured["headers"] = dict(req.header_items())
        return _Resp()

    with patch("urllib.request.urlopen", side_effect=_fake_urlopen):
        ok = emit_webhook_event(
            "run.completed",
            {"run_id": "run_1", "clusters_impacted": 10},
            webhook_url="https://example.com/hook",
            secret="topsecret",
        )

    assert ok is True
    assert captured["url"] == "https://example.com/hook"
    parsed = json.loads(captured["body"].decode("utf-8"))
    assert parsed["event"] == "run.completed"
    assert parsed["data"]["run_id"] == "run_1"
    expected_sig = (
        "sha256=" + hmac.new(b"topsecret", captured["body"], hashlib.sha256).hexdigest()
    )
    assert captured["headers"].get("X-idr-signature") == expected_sig
    assert build_webhook_signature(captured["body"], "topsecret") == expected_sig

    assert emit_webhook_event("run.completed", {}, webhook_url="") is False

    with patch("urllib.request.urlopen", side_effect=OSError("connection refused")):
        assert emit_webhook_event("run.completed", {}, webhook_url="https://example.com/hook") is False


@pytest.mark.integration
def test_context_window_safe_cluster_and_explain(duckdb_adapter, monkeypatch):
    """Verify get_cluster and explain_cluster bound members/edges and report truncated=True."""
    ConnectionManager.instance().set_adapter(duckdb_adapter, {"platform": "duckdb"})
    monkeypatch.setenv("IDR_PII_ACCESS", "masked")

    duckdb_adapter.execute("""
        INSERT INTO idr_out.identity_clusters_current (resolved_id, cluster_size, confidence_score, primary_reason)
        VALUES ('res_big', 5, 0.42, 'email')
    """)
    for i in range(5):
        duckdb_adapter.execute(f"""
            INSERT INTO idr_out.identity_resolved_membership_current (entity_key, resolved_id)
            VALUES ('crm:{i}', 'res_big')
        """)
    for i in range(4):
        duckdb_adapter.execute(f"""
            INSERT INTO idr_out.identity_edges_current
            (run_id, left_entity_key, right_entity_key, identifier_type, identifier_value_norm)
            VALUES ('run_1', 'crm:0', 'crm:{i + 1}', 'email', 'alice@example.com')
        """)

    res = mcp_server.get_cluster(
        "res_big", include_edges=True, include_entities=True, max_entities=2, max_edges=2
    )
    assert len(res["entities"]) == 2
    assert len(res["edges"]) == 2
    assert res["total_entities"] == 5
    assert res["total_edges"] == 4
    assert res["distinct_identifier_types"] == ["email"]
    assert res["truncated"] is True

    exp = mcp_server.explain_cluster("res_big", max_entities=2, max_edges=2)
    assert exp["summary"]["total_entities"] == 5
    assert exp["summary"]["total_edges"] == 4
    assert exp["summary"]["truncated"] is True


@pytest.mark.integration
def test_two_step_confirmation_guardrails(duckdb_adapter):
    """Verify forget_customer_identity and manage_identity_override two-step confirmation."""
    ConnectionManager.instance().set_adapter(duckdb_adapter, {"platform": "duckdb"})

    blocked = mcp_server.forget_customer_identity(
        identifier_value="nobody@example.com", dry_run=False
    )
    assert blocked["status"] == "CONFIRMATION_REQUIRED"

    preview = mcp_server.forget_customer_identity(
        identifier_value="nobody@example.com", dry_run=True
    )
    assert preview["status"] == "DRY_RUN"
    assert preview["confirmation_token"].startswith("idr_conf_")
    assert "entities" in preview["blast_radius_summary"]

    mismatch = mcp_server.forget_customer_identity(
        identifier_value="other@example.com",
        dry_run=False,
        confirmation_token=preview["confirmation_token"],
    )
    assert mismatch["status"] == "CONFIRMATION_REQUIRED"

    executed = mcp_server.forget_customer_identity(
        identifier_value="nobody@example.com",
        dry_run=False,
        confirmation_token=preview["confirmation_token"],
    )
    assert executed["status"] in ("SUCCESS", "NOT_FOUND")

    reused = mcp_server.forget_customer_identity(
        identifier_value="nobody@example.com",
        dry_run=False,
        confirmation_token=preview["confirmation_token"],
    )
    assert reused["status"] == "CONFIRMATION_REQUIRED"

    ov_preview = mcp_server.manage_identity_override(
        action="add",
        override_type="DO_NOT_LINK",
        entity_key_a="crm:1",
        entity_key_b="crm:2",
        dry_run=True,
    )
    assert ov_preview["status"] == "DRY_RUN"
    ov_exec = mcp_server.manage_identity_override(
        action="add",
        override_type="DO_NOT_LINK",
        entity_key_a="crm:1",
        entity_key_b="crm:2",
        confirmation_token=ov_preview["confirmation_token"],
    )
    assert ov_exec["status"] == "SUCCESS"


@pytest.mark.integration
def test_triage_queue_and_submit_evaluation_label(duckdb_adapter, monkeypatch):
    """Verify get_triage_queue and submit_evaluation_label."""
    ConnectionManager.instance().set_adapter(duckdb_adapter, {"platform": "duckdb"})
    monkeypatch.setenv("IDR_PII_ACCESS", "masked")

    duckdb_adapter.execute("""
        INSERT INTO idr_out.identity_clusters_current (resolved_id, cluster_size, confidence_score, primary_reason)
        VALUES ('res_low', 12, 0.30, 'phone'), ('res_high', 2, 0.95, 'email')
    """)
    duckdb_adapter.execute("""
        INSERT INTO idr_out.evaluation_sample
        (sample_id, stratum, resolved_id_a, resolved_id_b, entity_key_a, entity_key_b, identifier_type, match_value, confidence_score, cluster_size)
        VALUES ('smp_1', 'LOW_CONF', 'res_low', 'res_low', 'crm:1', 'crm:2', 'phone', '5551234567', 0.30, 12)
    """)

    queue = mcp_server.get_triage_queue(max_confidence=0.45, limit=10)
    assert queue["status"] == "SUCCESS"
    assert [c["resolved_id"] for c in queue["low_confidence_clusters"]] == ["res_low"]
    assert len(queue["pending_evaluation_samples"]) == 1
    assert queue["pending_evaluation_samples"][0]["match_value"] == "55***67"

    bad = mcp_server.submit_evaluation_label("smp_1", "MAYBE")
    assert bad["status"] == "ERROR"

    ok = mcp_server.submit_evaluation_label(
        "smp_1", "NON_MATCH", rationale="Shared store phone"
    )
    assert ok["status"] == "SUCCESS"
    assert ok["label"] == "NON_MATCH"
    assert "DO_NOT_LINK" in ok["recommended_next_step"]

    queue2 = mcp_server.get_triage_queue(max_confidence=0.45, limit=10)
    assert len(queue2["pending_evaluation_samples"]) == 0


@pytest.mark.integration
def test_fastapi_mounts_remote_mcp_route():
    """Verify /mcp is mounted on idr_api.main.app when FastMCP streamable_http_app is available."""
    import idr_api.main as api_main
    from idr_mcp.server import get_mcp_http_app

    if get_mcp_http_app() is not None:
        mount_paths = [getattr(r, "path", None) for r in api_main.app.routes]
        assert "/mcp" in mount_paths


@pytest.mark.unit
def test_agent_self_discovery_resources_and_docs():
    """Verify MCP resources/prompt, SKILL.md, and docs/llms.txt exist and return valid content."""
    assert "sources:" in mcp_server.resource_config_schema()
    assert "idr bootstrap" in mcp_server.resource_cli_commands()
    assert "EMAIL_STANDARD" in mcp_server.resource_canonicalizers()
    prompt_txt = mcp_server.model_config_from_tables(schema="ecommerce_synth")
    assert "discover_and_suggest_rules" in prompt_txt
    assert "ecommerce_synth" in prompt_txt

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    assert os.path.isfile(os.path.join(repo_root, "skills", "sql-identity-resolution", "SKILL.md"))
    assert os.path.isfile(os.path.join(repo_root, "docs", "llms.txt"))



