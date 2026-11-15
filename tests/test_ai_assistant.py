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
