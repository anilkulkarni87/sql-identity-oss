"""Tests for agent-native MCP enhancements and webhook hooks."""

import hashlib
import hmac
import json
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.connection_manager import ConnectionManager
from idr_core.schema_defs import SYSTEM_TABLES
from idr_core.webhooks import build_webhook_signature, emit_webhook_event


def _init_duckdb():
    conn = duckdb.connect(":memory:")
    adapter = DuckDBAdapter(conn)
    for schema in ("idr_meta", "idr_work", "idr_out"):
        adapter.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for tbl in SYSTEM_TABLES:
        cols = []
        for c in tbl.columns:
            dtype = {
                "string_type": "VARCHAR",
                "int_type": "BIGINT",
                "bool_type": "BOOLEAN",
                "timestamp_type": "TIMESTAMP",
                "DOUBLE": "DOUBLE",
            }[c.type.value if hasattr(c.type, "value") else c.type]
            cols.append(f"{c.name} {dtype}")
        adapter.execute(f"CREATE TABLE IF NOT EXISTS {tbl.fqn} ({', '.join(cols)})")
    return adapter


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
    expected_sig = "sha256=" + hmac.new(b"topsecret", captured["body"], hashlib.sha256).hexdigest()
    assert captured["headers"].get("X-idr-signature") == expected_sig
    assert build_webhook_signature(captured["body"], "topsecret") == expected_sig

    # Empty URL returns False without raising
    assert emit_webhook_event("run.completed", {}, webhook_url="") is False

    # Network error returns False without raising
    with patch("urllib.request.urlopen", side_effect=OSError("connection refused")):
        assert emit_webhook_event("run.completed", {}, webhook_url="https://example.com/hook") is False


def test_context_window_safe_cluster_and_explain(monkeypatch):
    """Verify get_cluster and explain_cluster bound members/edges and report truncated=True."""
    import idr_mcp.server as mcp_server

    adapter = _init_duckdb()
    ConnectionManager.instance().set_adapter(adapter, {"platform": "duckdb"})
    monkeypatch.setenv("IDR_PII_ACCESS", "masked")

    adapter.execute("""
        INSERT INTO idr_out.identity_clusters_current (resolved_id, cluster_size, confidence_score, primary_reason)
        VALUES ('res_big', 5, 0.42, 'email')
    """)
    for i in range(5):
        adapter.execute(f"""
            INSERT INTO idr_out.identity_resolved_membership_current (entity_key, resolved_id)
            VALUES ('crm:{i}', 'res_big')
        """)
    for i in range(4):
        adapter.execute(f"""
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


def test_two_step_confirmation_guardrails():
    """Verify forget_customer_identity and manage_identity_override two-step confirmation."""
    import idr_mcp.server as mcp_server

    adapter = _init_duckdb()
    ConnectionManager.instance().set_adapter(adapter, {"platform": "duckdb"})

    # 1. Direct forget with dry_run=False without token must be blocked
    blocked = mcp_server.forget_customer_identity(identifier_value="nobody@example.com", dry_run=False)
    assert blocked["status"] == "CONFIRMATION_REQUIRED"

    # 2. Preview with dry_run=True issues confirmation_token + blast_radius_summary
    preview = mcp_server.forget_customer_identity(identifier_value="nobody@example.com", dry_run=True)
    assert preview["status"] == "DRY_RUN"
    assert preview["confirmation_token"].startswith("idr_conf_")
    assert "entities" in preview["blast_radius_summary"]

    # 3. Executing with mismatched target fails
    mismatch = mcp_server.forget_customer_identity(
        identifier_value="other@example.com",
        dry_run=False,
        confirmation_token=preview["confirmation_token"],
    )
    assert mismatch["status"] == "CONFIRMATION_REQUIRED"

    # 4. Executing with valid token succeeds, and token cannot be reused
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

    # 5. Override dry_run preview + confirmed execution
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


def test_triage_queue_and_submit_evaluation_label(monkeypatch):
    """Verify get_triage_queue and submit_evaluation_label."""
    import idr_mcp.server as mcp_server

    adapter = _init_duckdb()
    ConnectionManager.instance().set_adapter(adapter, {"platform": "duckdb"})
    monkeypatch.setenv("IDR_PII_ACCESS", "masked")

    adapter.execute("""
        INSERT INTO idr_out.identity_clusters_current (resolved_id, cluster_size, confidence_score, primary_reason)
        VALUES ('res_low', 12, 0.30, 'phone'), ('res_high', 2, 0.95, 'email')
    """)
    adapter.execute("""
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

    ok = mcp_server.submit_evaluation_label("smp_1", "NON_MATCH", rationale="Shared store phone")
    assert ok["status"] == "SUCCESS"
    assert ok["label"] == "NON_MATCH"
    assert "DO_NOT_LINK" in ok["recommended_next_step"]

    # After labeling, pending_evaluation_samples should be empty
    queue2 = mcp_server.get_triage_queue(max_confidence=0.45, limit=10)
    assert len(queue2["pending_evaluation_samples"]) == 0


def test_fastapi_mounts_remote_mcp_route():
    """Verify /mcp is mounted on idr_api.main.app when FastMCP streamable_http_app is available."""
    import idr_api.main as api_main
    from idr_mcp.server import get_mcp_http_app

    if get_mcp_http_app() is not None:
        mount_paths = [getattr(r, "path", None) for r in api_main.app.routes]
        assert "/mcp" in mount_paths


def test_agent_self_discovery_resources_and_docs():
    """Verify MCP resources/prompt, SKILL.md, and docs/llms.txt exist and return valid content."""
    import os
    import idr_mcp.server as mcp_server

    assert "sources:" in mcp_server.resource_config_schema()
    assert "idr bootstrap" in mcp_server.resource_cli_commands()
    assert "EMAIL_STANDARD" in mcp_server.resource_canonicalizers()
    prompt_txt = mcp_server.model_config_from_tables(schema="ecommerce_synth")
    assert "discover_and_suggest_rules" in prompt_txt
    assert "ecommerce_synth" in prompt_txt

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    assert os.path.isfile(os.path.join(repo_root, "skills", "sql-identity-resolution", "SKILL.md"))
    assert os.path.isfile(os.path.join(repo_root, "docs", "llms.txt"))
