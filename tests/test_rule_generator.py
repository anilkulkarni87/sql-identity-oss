"""
Tests for the Zero-PII Pushdown Profiler, Structured LLM Client, and
Two-Pass AI Rule & Config Generator (`idr_core/rule_generator.py`).
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List

import pytest
import yaml

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.config import config_to_sql, validate_config
from idr_core.llm import LLMClient, LLMConfig
from idr_core.profiler import IDRDataProfiler
from idr_core.rule_generator import AIRuleGenerator, render_config_yaml_with_reasoning
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager


def _create_retail_fixture(adapter: DuckDBAdapter) -> None:
    """Create a realistic multi-table DuckDB fixture with recognizable PII values."""
    adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    adapter.execute("""
        CREATE TABLE raw.crm_customers (
            customer_id VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            marketing_opt_in BOOLEAN,
            email_sha256 VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    adapter.execute("""
        CREATE TABLE raw.web_orders (
            order_id VARCHAR,
            buyer_email VARCHAR,
            buyer_phone VARCHAR,
            order_total DOUBLE,
            created_at TIMESTAMP
        )
    """)
    adapter.execute("""
        CREATE TABLE raw.products (
            product_id VARCHAR,
            sku VARCHAR,
            price DOUBLE
        )
    """)

    rows_crm = [
        ("C1", "alice.secret+promo@acme.com", "555-867-5309", "Alice", "Wonderland", True, "2026-01-01 10:00:00"),
        ("C2", "bob.private@acme.com", "(555) 234-5678", "Bob", "Builder", False, "2026-01-02 10:00:00"),
        ("C3", "carol.vip+news@acme.com", "+1-555-987-6543", "Carol", "Danvers", True, "2026-01-03 10:00:00"),
        ("C4", "dave.smith@acme.com", "555-111-2222", "Dave", "Smith", False, "2026-01-04 10:00:00"),
        ("C5", "eve.jones@acme.com", "555-333-4444", "Eve", "Jones", True, "2026-01-05 10:00:00"),
    ]
    for cid, email, phone, fn, ln, optin, ts in rows_crm:
        h = hashlib.sha256(email.encode()).hexdigest()
        adapter.execute(
            f"INSERT INTO raw.crm_customers VALUES "
            f"('{cid}', '{email}', '{phone}', '{fn}', '{ln}', {str(optin).upper()}, '{h}', TIMESTAMP '{ts}')"
        )

    rows_orders = [
        ("O1", "alice.secret+promo@acme.com", "555-867-5309", 99.50, "2026-02-01 12:00:00"),
        ("O2", "bob.private@acme.com", "5552345678", 42.00, "2026-02-02 12:00:00"),
        ("O3", "carol.vip+news@acme.com", "15559876543", 120.00, "2026-02-03 12:00:00"),
        ("O4", "frank.guest@acme.com", "555-444-5555", 19.99, "2026-02-04 12:00:00"),
    ]
    for oid, email, phone, total, ts in rows_orders:
        adapter.execute(
            f"INSERT INTO raw.web_orders VALUES "
            f"('{oid}', '{email}', '{phone}', {total}, TIMESTAMP '{ts}')"
        )

    adapter.execute("""
        INSERT INTO raw.products VALUES
        ('P1', 'SKU-100', 19.99),
        ('P2', 'SKU-200', 49.99)
    """)


class _CapturingMockLLMClient(LLMClient):
    """Mock LLMClient that records all prompt payloads and returns scripted structured responses."""

    def __init__(self, responses: List[Dict[str, Any]], provider: str = "ollama", model: str = "llama3.2"):
        super().__init__(LLMConfig(provider=provider, model=model))
        self._responses = list(responses)
        self.captured_messages: List[List[Dict[str, str]]] = []

    def chat_structured(
        self,
        messages: List[Dict[str, str]],
        json_schema: Dict[str, Any],
        schema_name: str = "structured_output",
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> Dict[str, Any]:
        self.captured_messages.append(messages)
        if not self._responses:
            raise RuntimeError("No more mock responses")
        nxt = self._responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


def test_zero_pii_safe_profiler_and_cross_table_overlap():
    adapter = DuckDBAdapter(":memory:")
    _create_retail_fixture(adapter)

    profiler = IDRDataProfiler(adapter)
    profile = profiler.profile_tables_safe(
        ["raw.crm_customers", "raw.web_orders", "raw.products"],
        sample_size=500,
        compute_overlap=True,
    )

    serialized = json.dumps(profile.to_dict())
    # Assert ZERO raw PII values leaked into SafeSchemaProfile
    for forbidden in (
        "alice.secret+promo@acme.com",
        "bob.private@acme.com",
        "555-867-5309",
        "Wonderland",
        "Danvers",
        "frank.guest@acme.com",
    ):
        assert forbidden not in serialized, f"Raw PII leaked into SafeSchemaProfile: {forbidden}"

    # Verify deterministic hints for small local models (llama3.2)
    crm = profile.table_map()["raw.crm_customers"]
    col_map = {c.name: c for c in crm.columns}
    assert crm.hint_entity_key == "customer_id"
    assert crm.hint_watermark == "updated_at"
    assert col_map["email"].hint_identifier_type == "email"
    assert col_map["email"].hint_canonicalizer == "EMAIL_STRIP_PLUS"  # detected +tag!
    assert col_map["email_sha256"].hint_is_hashed is True
    assert col_map["marketing_opt_in"].hint_role == "consent_flag"

    # Verify products table has no candidate identifiers
    assert profile.table_map()["raw.products"].has_candidate_identifiers is False

    # Verify cross-table overlap computed accurately (3 of 4 web_orders emails match crm_customers)
    email_overlaps = [o for o in profile.overlaps if o.identifier_type == "email"]
    assert len(email_overlaps) == 1
    assert email_overlaps[0].shared_distinct_estimate == 3
    assert email_overlaps[0].overlap_b_in_a == pytest.approx(0.75, abs=0.05)


def test_ai_rule_generator_repair_loop_and_zero_pii_prompts():
    adapter = DuckDBAdapter(":memory:")
    _create_retail_fixture(adapter)

    # Attempt 1 of Pass A hallucinates 'email_addr' (does not exist!)
    bad_pass_a = {
        "tables": [
            {
                "table": "raw.crm_customers",
                "include_in_idr": True,
                "source_id": "crm_customers",
                "entity_type": "CUSTOMER",
                "entity_key": "customer_id",
                "watermark_column": "updated_at",
                "identifiers": [{"type": "email", "expr": "email_addr"}],
                "attributes": [{"name": "first_name", "expr": "first_name"}],
                "reasoning": "CRM master table.",
                "confidence": "high",
                "needs_review": False,
                "risks": [],
            },
            {
                "table": "raw.web_orders",
                "include_in_idr": True,
                "source_id": "web_orders",
                "entity_type": "ORDER",
                "entity_key": "order_id",
                "watermark_column": "created_at",
                "identifiers": [{"type": "email", "expr": "buyer_email"}],
                "attributes": [],
                "reasoning": "Web orders linked by buyer email.",
                "confidence": "high",
                "needs_review": False,
                "risks": [],
            },
            {
                "table": "raw.products",
                "include_in_idr": False,
                "source_id": "products",
                "reasoning": "Catalog table with no customer identifiers.",
                "confidence": "high",
                "needs_review": False,
                "risks": [],
            },
        ]
    }
    good_pass_a = json.loads(json.dumps(bad_pass_a))
    good_pass_a["tables"][0]["identifiers"] = [
        {"type": "email", "expr": "email"},
        {"type": "phone", "expr": "phone"},
    ]
    good_pass_a["tables"][0]["attributes"] = [
        {"name": "first_name", "expr": "first_name"},
        {"name": "marketing_opt_in", "expr": "marketing_opt_in"},
    ]
    good_pass_a["tables"][1]["identifiers"] = [
        {"type": "email", "expr": "buyer_email"},
        {"type": "phone", "expr": "buyer_phone"},
    ]

    # Attempt 1 of Pass B hallucinates invalid canonicalizer 'MAGIC_CANON'
    bad_pass_b = {
        "use_default_exclusions": True,
        "rules": [
            {
                "rule": {
                    "id": "exact_email",
                    "type": "EXACT",
                    "match_keys": ["email"],
                    "canonicalize": "MAGIC_CANON",
                    "priority": 1,
                    "max_group_size": 100,
                },
                "reasoning": "Match on normalized email.",
                "evidence": ["overlap=0.75", "plus_tag_rate=0.40"],
                "risks": [],
                "confidence": "high",
                "needs_review": False,
            }
        ],
        "fuzzy_templates": [],
        "survivorship": [],
        "lenses": [],
    }
    good_pass_b = {
        "use_default_exclusions": True,
        "rules": [
            {
                "rule": {
                    "id": "exact_email",
                    "type": "EXACT",
                    "match_keys": ["email"],
                    "canonicalize": "EMAIL_STRIP_PLUS",
                    "priority": 1,
                    "max_group_size": 100,
                },
                "reasoning": "Plus-tagged emails detected; EMAIL_STRIP_PLUS normalizes sub-addresses.",
                "evidence": ["overlap=0.75", "plus_tag_rate=0.40"],
                "risks": [],
                "confidence": "high",
                "needs_review": False,
            },
            {
                "rule": {
                    "id": "exact_phone",
                    "type": "EXACT",
                    "match_keys": ["phone"],
                    "canonicalize": "PHONE_US_10",
                    "priority": 2,
                    "max_group_size": 50,
                },
                "reasoning": "10/11-digit North American phones normalized via PHONE_US_10.",
                "evidence": ["phone_us10_rate=0.80"],
                "risks": ["Shared household phones possible; capped max_group_size=50."],
                "confidence": "medium",
                "needs_review": True,
            },
        ],
        "fuzzy_templates": [],
        "survivorship": [
            {
                "rule": {
                    "attribute": "first_name",
                    "strategy": "PRIORITY_RECENCY",
                    "source_priority": ["crm_customers"],
                },
                "reasoning": "CRM is authoritative for customer names.",
                "confidence": "high",
                "needs_review": False,
            },
            {
                "rule": {
                    "attribute": "marketing_opt_in",
                    "strategy": "ANY_TRUE",
                },
                "reasoning": "Boolean consent flag preserved with ANY_TRUE.",
                "confidence": "high",
                "needs_review": False,
            },
        ],
        "lenses": [
            {
                "lens": {
                    "id": "strict_person",
                    "include_types": ["EMAIL"],
                    "exclude_types": ["PHONE"],
                },
                "reasoning": "High-precision individual view excluding shared phones.",
                "confidence": "high",
                "needs_review": False,
            }
        ],
    }

    mock_client = _CapturingMockLLMClient([bad_pass_a, good_pass_a, bad_pass_b, good_pass_b])
    plan = AIRuleGenerator(adapter, client=mock_client, max_repairs=2).generate(
        tables=["raw.crm_customers", "raw.web_orders", "raw.products"],
        goal="Unify retail customers across CRM and web orders",
        verify_sql=True,
    )

    assert plan.generation_mode == "ai_repaired"
    assert plan.validation_report.valid is True
    assert plan.validation_report.sql_verified is True
    assert plan.validation_report.repair_attempts == 2
    assert len(plan.suggested_config["sources"]) == 2
    assert len(plan.suggested_config["rules"]) == 2
    assert len(plan.explanations.rules) == 2
    assert plan.explanations.rules[1].needs_review is True

    # Confirm ZERO raw PII was ever sent to the LLM across all 4 calls
    all_prompt_text = json.dumps(mock_client.captured_messages)
    for secret in ("alice.secret", "bob.private", "867-5309", "Wonderland", "Danvers"):
        assert secret not in all_prompt_text

    # Confirm generated YAML is valid and parses cleanly
    yaml_out = render_config_yaml_with_reasoning(plan)
    parsed_yaml = yaml.safe_load(yaml_out)
    assert validate_config(parsed_yaml) is True

    # End-to-end execution check: apply generated config -> run IDRRunner(dry_run=True)
    SchemaManager(adapter).initialize()
    for stmt in config_to_sql(plan.suggested_config, dialect="duckdb"):
        adapter.execute(stmt)
    run_result = IDRRunner(adapter).run(RunConfig(run_mode="FULL", dry_run=True))
    assert run_result.status == "DRY_RUN_COMPLETE"
    assert run_result.entities_processed == 9  # 5 crm + 4 web_orders


def test_deterministic_fallback_when_llm_unavailable():
    adapter = DuckDBAdapter(":memory:")
    _create_retail_fixture(adapter)

    failing_client = _CapturingMockLLMClient([RuntimeError("Connection refused: localhost:11434")])
    plan = AIRuleGenerator(adapter, client=failing_client).generate(
        tables=["raw.crm_customers", "raw.web_orders", "raw.products"],
    )

    assert plan.generation_mode == "deterministic_fallback"
    assert plan.validation_report.valid is True
    assert len(plan.suggested_config["sources"]) == 2
    assert len(plan.explanations.rules) >= 1
    assert any("fell back to deterministic profiler" in w.lower() for w in plan.validation_report.warnings)
