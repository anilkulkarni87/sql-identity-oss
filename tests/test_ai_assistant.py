"""Tests for Ollama-first multi-model LLM client, PII masking, CLI, and FastAPI /api/ai routes."""

import json
from io import BytesIO
from unittest.mock import patch

import duckdb
from fastapi.testclient import TestClient

from idr_api.main import app
from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.ai_assistant import (
    explain_cluster_with_ai,
    triage_with_ai,
)
from idr_core.ai_assistant import (
    test_llm_connection as check_llm_connection,
)
from idr_core.cli import main as cli_main
from idr_core.llm import (
    LLMClient,
    LLMConfig,
    is_local_endpoint,
    list_ollama_models,
    mask_pii_payload,
    mask_pii_text,
)
from idr_core.schema_manager import SchemaManager


def _fake_http_response(payload: dict):
    class _Resp:
        status = 200

        def read(self):
            return json.dumps(payload).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    return _Resp()


def test_local_endpoint_detection_and_default_pii_masking():
    assert is_local_endpoint("ollama", None) is True
    assert is_local_endpoint("ollama", "http://localhost:11434/v1") is True
    assert is_local_endpoint("openai_compatible", "http://127.0.0.1:8000/v1") is True
    assert is_local_endpoint("openai", "https://api.openai.com/v1") is False
    assert is_local_endpoint("anthropic", None) is False

    local_cfg = LLMConfig(provider="ollama", model="llama3.2")
    assert local_cfg.effective_mask_pii is False

    cloud_cfg = LLMConfig(provider="openai", model="gpt-4o-mini", api_key="sk-test")
    assert cloud_cfg.effective_mask_pii is True


def test_mask_pii_text_and_payload():
    raw = "User alice@example.com called from 555-123-4567 (SSN 123-45-6789)"
    masked = mask_pii_text(raw)
    assert "alice@example.com" not in masked
    assert "555-123-4567" not in masked
    assert "123-45-6789" not in masked
    assert "a***@e***.com" in masked
    assert "***-***-4567" in masked
    assert "***-**-6789" in masked

    obj = {
        "email": "bob@company.org",
        "first_name": "Robert",
        "cluster_size": 3,
    }
    masked_obj = mask_pii_payload(obj)
    assert masked_obj["email"] == "b***@c***.org"
    assert masked_obj["first_name"] == "R***t"
    assert masked_obj["cluster_size"] == 3


def test_llm_client_masks_pii_for_cloud_and_preserves_for_ollama():
    captured_bodies = []

    def _mock_urlopen(req, timeout=45.0):
        body = json.loads(req.data.decode("utf-8"))
        captured_bodies.append((req.full_url, body))
        if "anthropic" in req.full_url:
            return _fake_http_response({"content": [{"type": "text", "text": "OK"}]})
        if "generativelanguage" in req.full_url:
            return _fake_http_response({"candidates": [{"content": {"parts": [{"text": "OK"}]}}]})
        return _fake_http_response({"choices": [{"message": {"content": "OK"}}]})

    with patch("urllib.request.urlopen", side_effect=_mock_urlopen):
        # 1. Local Ollama: unmasked by default
        ollama_client = LLMClient(LLMConfig(provider="ollama", model="llama3.2"))
        assert ollama_client.chat([{"role": "user", "content": "Check alice@shop.com"}]) == "OK"
        assert "alice@shop.com" in captured_bodies[-1][1]["messages"][0]["content"]

        # 2. Cloud OpenAI: PII masked automatically
        oa_client = LLMClient(LLMConfig(provider="openai", model="gpt-4o-mini", api_key="sk-1"))
        assert oa_client.chat([{"role": "user", "content": "Check alice@shop.com"}]) == "OK"
        sent = captured_bodies[-1][1]["messages"][0]["content"]
        assert "alice@shop.com" not in sent
        assert "a***@s***.com" in sent

        # 3. Anthropic & Gemini
        ant_client = LLMClient(LLMConfig(provider="anthropic", api_key="ant-1"))
        assert ant_client.chat([{"role": "user", "content": "ping"}]) == "OK"

        gem_client = LLMClient(LLMConfig(provider="gemini", api_key="gem-1"))
        assert gem_client.chat([{"role": "user", "content": "ping"}]) == "OK"


def test_list_ollama_models():
    with patch(
        "urllib.request.urlopen",
        return_value=_fake_http_response({"models": [{"name": "llama3.2:latest"}, {"name": "qwen2.5:7b"}]}),
    ):
        models = list_ollama_models()
        assert models == ["llama3.2:latest", "qwen2.5:7b"]


def test_explain_and_triage_with_duckdb():
    conn = duckdb.connect(":memory:")
    adapter = DuckDBAdapter(conn)
    SchemaManager(adapter).initialize(reset=True)
    adapter.execute("""
        INSERT INTO idr_out.identity_clusters_current
        (resolved_id, cluster_size, confidence_score, edge_diversity, edge_count)
        VALUES ('cl_1', 2, 0.40, 1, 1)
    """)
    adapter.execute("""
        INSERT INTO idr_out.identity_resolved_membership_current (entity_key, resolved_id)
        VALUES ('crm:1', 'cl_1'), ('web:2', 'cl_1')
    """)
    adapter.execute("""
        INSERT INTO idr_out.identity_edges_current
        (left_entity_key, right_entity_key, identifier_type, identifier_value_norm)
        VALUES ('crm:1', 'web:2', 'email', 'alice@example.com')
    """)

    seen_prompts = []

    def _mock_urlopen(req, timeout=45.0):
        body = json.loads(req.data.decode("utf-8"))
        seen_prompts.append(body["messages"][-1]["content"])
        if "proposals" in body["messages"][-1]["content"] or body.get("response_format"):
            return _fake_http_response(
                {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "proposals": [
                                            {
                                                "resolved_id": "cl_1",
                                                "action": "DO_NOT_LINK",
                                                "entity_key_a": "crm:1",
                                                "entity_key_b": "web:2",
                                                "reason": "Low confidence",
                                            }
                                        ]
                                    }
                                )
                            }
                        }
                    ]
                }
            )
        return _fake_http_response({"choices": [{"message": {"content": "- Merged on email"}}]})

    with patch("urllib.request.urlopen", side_effect=_mock_urlopen):
        cloud_client = LLMClient(LLMConfig(provider="openai", api_key="sk-1"))
        exp = explain_cluster_with_ai(adapter, "cl_1", client=cloud_client)
        assert exp["pii_masked"] is True
        assert "alice@example.com" not in seen_prompts[-1]

        triage = triage_with_ai(adapter, client=cloud_client)
        assert triage["proposals"][0]["action"] == "DO_NOT_LINK"


def test_api_ai_endpoints_and_cli(capsys, monkeypatch):
    import idr_api.dependencies as deps

    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", True)
    tc = TestClient(app)
    resp = tc.post(
        "/api/ai/config",
        json={"provider": "ollama", "model": "llama3.2", "base_url": "http://localhost:11434/v1"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["provider"] == "ollama"
    assert data["is_local"] is True
    assert data["mask_pii"] is False

    # Switching to OpenAI enforces mask_pii=True by default and never leaks api_key
    resp2 = tc.post(
        "/api/ai/config",
        json={"provider": "openai", "model": "gpt-4o-mini", "api_key": "sk-secret"},
    )
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert data2["has_api_key"] is True
    assert data2["mask_pii"] is True
    assert "sk-secret" not in resp2.text

    # CLI `idr ai test`
    with patch(
        "urllib.request.urlopen",
        return_value=_fake_http_response({"choices": [{"message": {"content": "OK"}}]}),
    ):
        rc = cli_main(["ai", "test", "--provider", "ollama", "--model", "llama3.2"])
        assert rc == 0
        assert "Connected to ollama (llama3.2)" in capsys.readouterr().out


def test_duckdb_adapter_thread_safety_and_reconnect(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    import idr_api.dependencies as deps

    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", True)

    db_file = str(tmp_path / "concurrent.duckdb")
    adapter = DuckDBAdapter(db_file)
    adapter.execute("CREATE TABLE t (id INTEGER, val VARCHAR)")
    adapter.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")

    def _run_query(i: int):
        rows = adapter.query("SELECT id, val FROM t WHERE id = ?", [(i % 2) + 1])
        assert len(rows) == 1
        return rows[0]["val"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(_run_query, range(40)))
    assert len(results) == 40
    adapter.close()
    adapter.close()  # idempotent

    # Reconnecting to the same DuckDB file via /api/connect should not hang
    tc = TestClient(app)
    r1 = tc.post("/api/connect", json={"platform": "duckdb", "database": db_file})
    assert r1.status_code == 200
    r2 = tc.post("/api/connect", json={"platform": "duckdb", "database": db_file})
    assert r2.status_code == 200
    tc.post("/api/disconnect")


def test_profile_builder_skips_unmapped_sources_and_pushes_prefix():
    from idr_core.profile_builder import ProfileBuilder

    adapter = DuckDBAdapter(":memory:")
    builder = ProfileBuilder(adapter)
    sql = builder._build_staging_cte(
        rules=[{"attribute_name": "email", "strategy": "RECENCY"}],
        mappings=[{"table_id": "crm", "attribute_name": "email", "attribute_expr": "src.email"}],
        sources=[
            {"table_id": "crm", "table_fqn": "raw.crm", "entity_key_expr": "id", "watermark_column": "updated_at"},
            {"table_id": "clickstream", "table_fqn": "raw.clickstream", "entity_key_expr": "event_id", "watermark_column": "ts"},
        ],
    )
    assert "raw.crm" in sql
    assert "m.entity_key LIKE 'crm:%'" in sql
    assert "raw.clickstream" not in sql


def test_schema_aware_pii_masking_and_copilot(tmp_path, monkeypatch):
    import idr_api.dependencies as deps
    from idr_core.ai_assistant import (
        copilot_chat,
        get_configured_pii_keys,
        get_run_runtime_trend,
    )
    from idr_core.llm import mask_pii_payload

    # 1. Dynamic + expanded PII key masking
    masked = mask_pii_payload(
        {"dob": "1990-04-12", "cust_nick": "Ali", "safe_metric": "42"},
        extra_sensitive_keys=["cust_nick"],
    )
    assert masked["dob"] == "1***2"
    assert masked["cust_nick"] == "A***i"
    assert masked["safe_metric"] == "42"

    # 2. Seed DuckDB with metadata + run_history + stage_metrics
    db_file = str(tmp_path / "copilot.duckdb")
    adapter = DuckDBAdapter(db_file)
    SchemaManager(adapter).initialize(reset=True)
    adapter.execute("""
        INSERT INTO idr_meta.entity_attribute_mapping (table_id, attribute_name, attribute_expr)
        VALUES ('crm', 'cust_nick', 'nickname_col')
    """)
    adapter.execute("""
        INSERT INTO idr_out.run_history
            (run_id, run_mode, status, started_at, duration_seconds, entities_processed, edges_created, clusters_impacted)
        VALUES
            ('run_1', 'INCR', 'SUCCESS', CURRENT_TIMESTAMP, 42, 1000, 800, 400),
            ('run_2', 'FULL', 'SUCCESS', CURRENT_TIMESTAMP - INTERVAL '1' DAY, 84, 2000, 1600, 750)
    """)
    adapter.execute("""
        INSERT INTO idr_out.stage_metrics (run_id, stage_name, stage_order, duration_seconds, rows_out)
        VALUES ('run_1', 'label_propagation', 6, 20.0, 400)
    """)

    assert "cust_nick" in get_configured_pii_keys(adapter)
    trend = get_run_runtime_trend(adapter)
    assert trend["total_runs_checked"] == 2
    assert trend["latest_run"]["run_id"] == "run_1"
    assert trend["avg_duration_seconds"] == 63.0

    # 3. Copilot propose_run (INCR & FULL) never auto-executes
    fake_client_run = LLMClient(LLMConfig(provider="ollama", model="llama3.2"))
    with patch.object(
        fake_client_run,
        "chat",
        return_value=json.dumps({"intent": "propose_run", "mode": "INCR", "dry_run": False, "lens": None}),
    ):
        res_incr = copilot_chat(adapter, "Please run IDR incremental", client=fake_client_run)
        assert res_incr["intent"] == "propose_run"
        assert res_incr["proposed_action"]["mode"] == "INCR"
        assert res_incr["pii_masked"] is False

    # 4. Copilot runtime trend via /api/ai/copilot with BYOK (pii_masked=True)
    adapter.close()
    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", True)
    tc = TestClient(app)
    assert tc.post("/api/connect", json={"platform": "duckdb", "database": db_file}).status_code == 200
    assert (
        tc.post(
            "/api/ai/config",
            json={"provider": "openai", "model": "gpt-4o-mini", "api_key": "sk-test"},
        ).status_code
        == 200
    )
    with patch(
        "idr_core.llm.LLMClient.chat",
        side_effect=[
            json.dumps({"intent": "get_runtime_trend"}),
            "Latest INCR run took 42.5s vs 63.75s average.",
        ],
    ):
        resp = tc.post("/api/ai/copilot", json={"message": "What was IDR runtime today?"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["intent"] == "get_runtime_trend"
        assert body["pii_masked"] is True
        assert body["metrics"]["latest_run"]["duration_seconds"] == 42
    tc.post("/api/disconnect")


def test_ask_identity_graph_includes_active_config_summary(tmp_path):
    from idr_core.ai_assistant import ask_identity_graph
    from idr_core.config import apply_config_to_warehouse

    db_file = str(tmp_path / "qa_cfg.duckdb")
    adapter = DuckDBAdapter(db_file)
    try:
        SchemaManager(adapter).initialize(reset=True)
        apply_config_to_warehouse(
            adapter,
            {
                "sources": [
                    {
                        "id": "crm",
                        "table": "raw.crm",
                        "entity_key": "id",
                        "identifiers": [{"type": "email", "expr": "email"}],
                    }
                ],
                "rules": [
                    {"id": "104", "type": "EXACT", "match_keys": ["email"], "priority": 1, "canonicalize": "EMAIL_STANDARD"}
                ],
                "lenses": [{"id": "strict_person", "include_types": ["EMAIL"]}],
            },
        )
        captured_messages = {}
        client = LLMClient(LLMConfig(provider="ollama", model="llama3.2"))

        def fake_chat(messages, **_kwargs):
            captured_messages["messages"] = messages
            return "Active rule 104 matches on email; lens strict_person uses EMAIL."

        with patch.object(client, "chat", side_effect=fake_chat):
            res = ask_identity_graph(adapter, "What rules and lenses are active?", client=client)

        assert "strict_person" in res["answer"]
        prompt_text = captured_messages["messages"][1]["content"]
        assert "active_config" in prompt_text
        assert "strict_person" in prompt_text
        assert "EMAIL_STANDARD" in prompt_text
    finally:
        adapter.close()


def test_forensics_bridge_anomalies_draft_lens_and_mcp(tmp_path):
    from idr_core.ai_assistant import (
        diagnose_run_anomalies,
        draft_lens_from_goal,
        find_cluster_bridge,
    )
    from idr_core.config import apply_config_to_warehouse
    import idr_mcp.server as mcp_srv

    db_file = str(tmp_path / "forensics.duckdb")
    adapter = DuckDBAdapter(db_file)
    try:
        SchemaManager(adapter).initialize(reset=True)
        apply_config_to_warehouse(
            adapter,
            {
                "sources": [
                    {"id": "crm", "table": "raw.crm", "entity_key": "id", "identifiers": [{"type": "email", "expr": "email"}]},
                ],
                "rules": [
                    {"id": "104", "type": "EXACT", "match_keys": ["email"], "priority": 1, "canonicalize": "EMAIL_STANDARD"},
                    {"id": "105", "type": "EXACT", "match_keys": ["phone"], "priority": 2, "canonicalize": "PHONE_DIGITS"},
                ],
            },
        )
        # Seed a 3-hop cluster: crm:1 -> web:2 -> pos:9
        adapter.execute("""
            INSERT INTO idr_out.identity_resolved_membership_current (entity_key, resolved_id)
            VALUES ('crm:1', 'res_1'), ('web:2', 'res_1'), ('pos:9', 'res_1')
        """)
        adapter.execute("""
            INSERT INTO idr_out.identity_edges_current (left_entity_key, right_entity_key, identifier_type, identifier_value_norm)
            VALUES
                ('crm:1', 'web:2', 'EMAIL', 'alice@example.com'),
                ('web:2', 'pos:9', 'PHONE', '5550199')
        """)
        adapter.execute("""
            INSERT INTO idr_out.run_history (run_id, run_mode, status, started_at, duration_seconds, entities_processed, edges_created, clusters_impacted)
            VALUES ('run_fast', 'INCR', 'SUCCESS', CURRENT_TIMESTAMP - INTERVAL '1' DAY, 20, 100, 50, 40),
                   ('run_slow', 'INCR', 'SUCCESS', CURRENT_TIMESTAMP, 100, 100, 50, 40)
        """)
        adapter.execute("""
            INSERT INTO idr_out.stage_metrics (run_id, stage_name, stage_order, duration_seconds, rows_out)
            VALUES ('run_slow', 'label_propagation', 6, 80.0, 40)
        """)
        adapter.execute("""
            INSERT INTO idr_out.skipped_identifier_groups (run_id, identifier_type, identifier_value_norm, group_size, max_allowed)
            VALUES ('run_slow', 'PHONE', '0000000000', 500, 100)
        """)

        bridge = find_cluster_bridge(adapter, "crm:1", "pos:9")
        assert bridge["connected"] is True
        assert bridge["hops"] == 2
        assert bridge["recommended_override"]["override_type"] == "DO_NOT_LINK"
        assert bridge["recommended_exclusion"]["identifier_value_pattern"] == "5550199"

        diag = diagnose_run_anomalies(adapter)
        assert diag["runtime_delta_pct"] > 25.0
        assert diag["slowest_stage"]["stage_name"] == "label_propagation"
        assert len(diag["skipped_hubs"]) == 1

        lens_res = draft_lens_from_goal(adapter, "Create a strict login lens")
        assert lens_res["lens"]["id"] == "strict_person"
        assert "EMAIL" in lens_res["lens"]["include_types"]

        # Compact MCP tools
        with patch.object(mcp_srv, "get_adapter", return_value=adapter), patch.object(mcp_srv, "should_mask_pii", return_value=False):
            mcp_bridge = mcp_srv.explain_cluster_bridge("crm:1", "pos:9")
            assert mcp_bridge["hops"] == 2
            mcp_diag = mcp_srv.diagnose_run_anomalies()
            assert len(mcp_diag["skipped_hubs"]) == 1
    finally:
        adapter.close()


def test_validate_unified_profile_against_source_data(tmp_path):
    from idr_core.ai_assistant import copilot_chat, validate_unified_profile
    from idr_core.config import apply_config_to_warehouse
    from idr_core.runner import IDRRunner, RunConfig

    db_file = str(tmp_path / "validate_prof.duckdb")
    adapter = DuckDBAdapter(db_file)
    try:
        SchemaManager(adapter).initialize(reset=True)
        adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
        adapter.execute("""
            CREATE TABLE raw.crm (id VARCHAR, email VARCHAR, updated_at TIMESTAMP);
            CREATE TABLE raw.pos (id VARCHAR, email VARCHAR, updated_at TIMESTAMP);
            INSERT INTO raw.crm VALUES ('c1', 'alice@example.com', TIMESTAMP '2026-01-01 00:00:00');
            INSERT INTO raw.pos VALUES ('p1', 'alice@example.com', TIMESTAMP '2026-01-01 00:00:00');
        """)
        apply_config_to_warehouse(
            adapter,
            {
                "sources": [
                    {
                        "id": "crm",
                        "table": "raw.crm",
                        "entity_key": "id",
                        "watermark_column": "updated_at",
                        "identifiers": [{"type": "email", "expr": "email"}],
                    },
                    {
                        "id": "pos",
                        "table": "raw.pos",
                        "entity_key": "id",
                        "watermark_column": "updated_at",
                        "identifiers": [{"type": "email", "expr": "email"}],
                    },
                ],
                "rules": [
                    {"id": "104", "type": "EXACT", "match_keys": ["email"], "priority": 1, "canonicalize": "EMAIL_STANDARD"}
                ],
            },
        )
        res = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
        assert res.status == "SUCCESS"

        v = validate_unified_profile(adapter)
        assert v["status"] == "VERIFIED"
        assert v["member_count"] == 2
        assert v["source_rows_matched"] == 2

        client = LLMClient(LLMConfig(provider="ollama", model="llama3.2"))
        with patch.object(
            client,
            "chat",
            return_value=json.dumps({"intent": "validate_profile", "resolved_id": None}),
        ):
            chat_res = copilot_chat(adapter, "Can you validate a unified profile against source data?", client=client)
        assert chat_res["intent"] == "validate_profile"
        assert "VERIFIED" in chat_res["answer"]
        assert "2/2 source rows verified" in chat_res["answer"]
        from idr_core.ai_assistant import _sanitize_json
        sanitized = _sanitize_json({"nan_val": float("nan"), "inf_val": float("inf"), "nested": [{"x": float("-inf")}]})
        assert sanitized == {"nan_val": None, "inf_val": None, "nested": [{"x": None}]}
        json.dumps(chat_res, allow_nan=False)

        # Asking for SQL should route to profile_verification_sql even if the router returns validate_profile
        with patch.object(
            client,
            "chat",
            return_value=json.dumps({"intent": "validate_profile", "resolved_id": "unified profile against source data"}),
        ):
            sql_res = copilot_chat(
                adapter,
                "can you give me the sql for validate a unified profile against source data",
                client=client,
            )
        assert sql_res["intent"] == "profile_verification_sql"
        assert "idr_out.identity_resolved_membership_current" in sql_res["answer"]
        assert "raw.crm" in sql_res["answer"]
        assert "raw.pos" in sql_res["answer"]
        assert "email" in sql_res["metrics"]["merge_identifier_types"]

        # Explicit resolved_id in message should be extracted and used in generated SQL
        with patch.object(
            client,
            "chat",
            return_value=json.dumps({"intent": "profile_verification_sql", "resolved_id": None}),
        ):
            explicit_sql_res = copilot_chat(
                adapter,
                f"generate verification sql for {chat_res['metrics']['resolved_id']}",
                client=client,
            )
        assert explicit_sql_res["intent"] == "profile_verification_sql"
        assert explicit_sql_res["metrics"]["resolved_id"] == chat_res["metrics"]["resolved_id"]
        assert len(explicit_sql_res["metrics"]["queries"]) >= 4
        union_q = next(
            q for q in explicit_sql_res["metrics"]["queries"] if "UNION ALL" in q["title"]
        )
        union_rows = adapter.query(union_q["sql"].rstrip(";"))
        assert len(union_rows) == 2
        assert {r["source_id"] for r in union_rows} == {"crm", "pos"}
        assert {r["identifier_value"] for r in union_rows} == {"alice@example.com"}

        # Hallucinated phrase in resolved_id should be ignored and fall back to sampling a real cluster
        with patch.object(
            client,
            "chat",
            return_value=json.dumps({"intent": "validate_profile", "resolved_id": "unified profile against source data"}),
        ):
            clean_res = copilot_chat(adapter, "validate unified profile against source data", client=client)
        assert clean_res["intent"] == "validate_profile"
        assert clean_res["metrics"]["status"] == "VERIFIED"
        assert clean_res["metrics"]["resolved_id"] == chat_res["metrics"]["resolved_id"]
    finally:
        adapter.close()
