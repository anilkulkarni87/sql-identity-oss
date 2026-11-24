"""Privacy, policy and immutable preview acceptance tests (no live models)."""

import copy
import json

import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.ai_config import AIConfigContext, draft_hash, model_evidence
from idr_core.ai_review import AIReviewService, PreviewAdapter, StaleReview
from idr_core.config import apply_config_to_warehouse, get_config_snapshots, sql_to_config
from idr_core.config_schema import PassATableDecision, PassBRuleOutput
from idr_core.llm import LLMClient, LLMConfig
from idr_core.profiler import IDRDataProfiler
from idr_core.rule_generator import AIRuleGenerator
from idr_core.runner import IDRRunner, RunConfig


@pytest.fixture
def warehouse():
    a = DuckDBAdapter(":memory:")
    a.execute("CREATE SCHEMA raw")
    a.execute("""CREATE TABLE raw.customers AS SELECT * FROM (VALUES
        ('A','alice.secret@acme.org','5552345678','Alice'),
        ('B','bob.private@acme.org','5552345678','Bob'),
        ('C','alice.secret@acme.org','5552345678','Alice')) t(customer_id,email,phone,name)""")
    a.execute(
        "CREATE TABLE raw.events AS SELECT customer_id AS event_id, email, now() created_at FROM raw.customers"
    )
    yield a
    a.close()


def generator(a):
    return AIRuleGenerator(a, LLMClient(LLMConfig(provider="disabled")))


def config():
    return {
        "sources": [
            {
                "id": "crm",
                "table": "raw.customers",
                "entity_key": "customer_id",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                    {"type": "phone", "expr": "phone"},
                ],
                "attributes": [{"name": "name", "expr": "name"}],
            }
        ],
        "rules": [
            {
                "id": "email",
                "match_keys": ["email"],
                "canonicalize": "EMAIL_STANDARD",
                "priority": 1,
                "max_group_size": 25,
            },
            {
                "id": "phone",
                "match_keys": ["phone"],
                "canonicalize": "PHONE_DIGITS",
                "priority": 2,
                "max_group_size": 25,
            },
        ],
        "ai_assistance": AIConfigContext(grain="household").model_dump(),
    }


def test_skipped_group_recommendation_is_bounded_and_comparable(warehouse):
    service = AIReviewService(warehouse)
    cfg = config()
    cfg["rules"][1]["max_group_size"] = 2
    preview = service.preview(cfg)
    report = preview["review"]
    finding = next(f for f in report["findings"] if "skipped phone" in f["title"])
    assert finding["current_value"] == 2
    assert finding["proposed_value"] == 3
    assert finding["patch"]["measured"] is False
    assert "2 → 3" in finding["recommendation"]
    assert finding["metric"]
    compared = service.compare(report["run_id"], cfg, finding["patch"], "default")
    assert compared["comparison"]["same_extraction"]
    assert compared["telemetry"]["edges_by_type"].get("phone", 0) > 0


@pytest.mark.parametrize("smallest,global_cap", [(1000, None), (26, 20)])
def test_skipped_groups_do_not_offer_extreme_or_ineffective_increase(
    warehouse, smallest, global_cap
):
    service = AIReviewService(warehouse)
    finding = service._skipped_findings(
        config(),
        {"max_group_size": global_cap},
        [
            {
                "identifier_type": "email",
                "skipped_groups": 1,
                "min_group_size_seen": smallest,
                "max_group_size_seen": smallest,
            }
        ],
    )[0]
    assert finding["patch"] is None
    assert "Keep" in finding["recommendation"]
    if global_cap:
        assert "global cap 20" in finding["recommendation"]


def test_review_model_cannot_replace_numbers_or_invent_actions(warehouse):
    service = AIReviewService(warehouse)
    cfg = config()
    cfg["rules"][1]["max_group_size"] = 2
    report = service.preview(cfg)["review"]
    requests = []

    class Client:
        config = LLMConfig(provider="openai", api_key="test", model="test")

        def chat_structured(self, messages, schema, name):
            requests.append(json.loads(messages[1]["content"]))
            return {
                "recommendation_ids": ["invented", "finding_1"],
                "headline": "91.93% of entities are singletons",
                "patch": {"action": "delete_everything"},
            }

    reviewed = service.explain(report["run_id"], draft_hash(cfg), "default", Client(), cfg)
    assert requests[0]["candidates"]
    assert "config" not in requests[0]
    assert "alice.secret" not in json.dumps(requests)
    assert "91.93" not in reviewed["headline"]
    assert "of clusters" in reviewed["headline"] and "of entities" in reviewed["headline"]
    assert reviewed["findings"] == report["findings"]


def test_old_review_gets_rule_specific_recommendation_without_new_preview(warehouse):
    service = AIReviewService(warehouse)
    cfg = config()
    report = service.preview(cfg)["review"]
    report["findings"] = [
        {
            "severity": "medium",
            "title": "Some shared identifiers exceeded group limits",
            "detail": "generic",
            "metric": "1",
            "patch": None,
        }
    ]
    report["telemetry"]["skipped_supernodes"] = [
        {"identifier_type": "email", "skipped_groups": 1, "max_group_size_seen": 1000}
    ]
    payload = json.dumps(report).replace("'", "''")
    warehouse.execute(
        f"UPDATE idr_out.ai_review_report SET report_json = '{payload}' WHERE run_id = '{report['run_id']}'"
    )
    upgraded = service.explain(report["run_id"], draft_hash(cfg), "default", draft_config=cfg)
    assert "Review email" in upgraded["findings"][0]["title"]
    assert "Keep email" in upgraded["findings"][0]["recommendation"]
    assert upgraded["findings"][0]["patch"] is None


def test_manual_preview_does_not_invent_an_ai_matching_policy(warehouse):
    cfg = config()
    del cfg["ai_assistance"]
    # Existing phone matching is an explicit engine configuration, not an AI proposal.
    result = AIReviewService(warehouse).preview(cfg)
    assert result["status"] == "DRY_RUN_COMPLETE"
    assert result["review"]["telemetry"]["total_entities"] == 3


def test_preview_allows_human_edited_rules_while_ai_policy_flags_them(warehouse):
    from idr_core.ai_config import policy_errors

    cfg = config()
    ctx = AIConfigContext(grain="individual")
    cfg["ai_assistance"] = ctx.model_dump()
    # AI policy still flags phone under individual grain for AI proposals...
    assert any("allowed matching identifiers" in e for e in policy_errors(cfg, ctx, cfg))
    # ...but explicit human edits in preview are never hard-blocked.
    result = AIReviewService(warehouse).preview(cfg)
    assert result["status"] == "DRY_RUN_COMPLETE"


def test_explicit_exclusions_survive_materialization(warehouse):
    g = generator(warehouse)
    profile = g.profiler.profile_tables_safe(["raw.customers", "raw.events"])
    decisions = [
        PassATableDecision(
            table="raw.customers",
            source_id="crm",
            include_in_idr=True,
            entity_key="customer_id",
            identifiers=[{"type": "email", "expr": "email"}],
            reasoning="Master",
        ),
        PassATableDecision(
            table="raw.events", source_id="events", include_in_idr=False, reasoning="User excluded"
        ),
    ]
    sources, _ = g._materialize_sources(decisions, profile)
    assert [s.table for s in sources] == ["raw.customers"]


def test_explicit_rule_omission_and_limit_are_preserved(warehouse):
    g = generator(warehouse)
    profile = g.profiler.profile_tables_safe(["raw.customers", "raw.events"])
    decisions = [
        PassATableDecision(
            table=t.table_fqn,
            source_id=t.table_fqn.replace(".", "_"),
            include_in_idr=True,
            entity_key=t.hint_entity_key,
            identifiers=[{"type": "email", "expr": "email"}],
            reasoning="Mapped",
        )
        for t in profile.tables
    ]
    sources, exps = g._materialize_sources(decisions, profile)
    b = PassBRuleOutput.model_validate(
        {
            "rules": [
                {
                    "rule": {"id": "email", "match_keys": ["email"], "max_group_size": 10},
                    "reasoning": "Intentional limit",
                }
            ]
        }
    )
    spec, _ = g._assemble_plan(sources, exps, b, profile)
    assert [(r.match_keys, r.max_group_size) for r in spec.rules] == [(["email"], 10)]


@pytest.mark.parametrize("event_policy", ["bridge", "exclude", "include"])
def test_policy_and_fallback_use_same_profile(warehouse, event_policy):
    plan = generator(warehouse).generate(
        ["raw.customers", "raw.events"], context=AIConfigContext(event_policy=event_policy)
    )
    assert plan.validation_report.valid
    assert all("phone" not in r["match_keys"] for r in plan.suggested_config["rules"])
    events = [s for s in plan.suggested_config["sources"] if s["table"] == "raw.events"]
    assert bool(events) == (event_policy != "exclude")
    if events and event_policy == "bridge":
        assert not events[0]["attributes"]


def test_pk_fk_fallback_and_empty_plan(warehouse):
    warehouse.execute(
        "CREATE TABLE raw.patients AS SELECT * FROM (VALUES ('P1','Alice'),('P2','Bob'),('P3','Carol')) t(patient_id,name)"
    )
    warehouse.execute(
        "CREATE TABLE raw.visits AS SELECT 'V'||patient_id visit_id,patient_id,now() occurred_at FROM raw.patients"
    )
    plan = generator(warehouse).generate(["raw.patients", "raw.visits"])
    assert plan.validation_report.valid
    assert len(plan.suggested_config["sources"]) == 2
    assert any("patient_id" in r["match_keys"] for r in plan.suggested_config["rules"])
    warehouse.execute("CREATE TABLE raw.lookup AS SELECT 1 price")
    empty = generator(warehouse).generate(["raw.lookup"])
    assert empty.status == "invalid"


def test_rules_mode_preserves_existing_mapping_and_expression(warehouse):
    cfg = config()
    cfg["sources"][0]["identifiers"][0] = {
        "type": "contact_email",
        "expr": "LOWER(email)",
        "is_hashed": True,
    }
    cfg["ai_assistance"]["allowed_matching_identifiers"] = ["contact_email", "phone"]
    result = generator(warehouse).generate(
        ["raw.customers"],
        current_config=cfg,
        context=AIConfigContext.model_validate(cfg["ai_assistance"]),
        mode="rules",
    )
    assert result.validation_report.valid, result.validation_report.errors
    assert result.suggested_config["sources"] == cfg["sources"]
    assert any("contact_email" in r["match_keys"] for r in result.suggested_config["rules"])


def test_policy_survives_save_load_history_and_rollback(warehouse):
    cfg = config()
    cfg["ai_assistance"]["locked_fields"] = ["/sources/0/id"]
    apply_config_to_warehouse(warehouse, cfg)
    loaded = sql_to_config(warehouse)
    assert loaded["ai_assistance"] == cfg["ai_assistance"]
    history = get_config_snapshots(warehouse)
    changed = copy.deepcopy(cfg)
    changed["ai_assistance"]["grain"] = "account"
    changed["ai_assistance"]["allowed_matching_identifiers"] = ["email", "phone"]
    apply_config_to_warehouse(warehouse, changed)
    apply_config_to_warehouse(warehouse, history[0]["config"])
    assert sql_to_config(warehouse)["ai_assistance"] == cfg["ai_assistance"]


def test_draft_fingerprint_normalizes_ui_shape():
    a = config()
    b = copy.deepcopy(a)
    b.update(exclusions=[], overrides=[], fuzzy_rules=[], survivorship=[], lenses=[])
    b["sources"][0]["identifiers"][0]["column"] = "email"
    assert draft_hash(a) == draft_hash(b)
    b["rules"][0]["max_group_size"] = 10
    assert draft_hash(a) != draft_hash(b)


def test_preview_isolated_and_bound_to_owner_draft_and_lens(warehouse):
    cfg = config()
    apply_config_to_warehouse(warehouse, cfg)
    assert IDRRunner(warehouse).run(RunConfig()).status == "SUCCESS"
    before = warehouse.query(
        "SELECT * FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
    )
    metadata = warehouse.query("SELECT * FROM idr_meta.rule ORDER BY rule_id")
    service = AIReviewService(warehouse, "alice")
    preview = service.preview(cfg)
    report = service.get(preview["run_id"], draft_hash(cfg), "default")
    assert report["telemetry"]["total_clusters"] == 1
    assert (
        warehouse.query(
            "SELECT * FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
        )
        == before
    )
    assert warehouse.query("SELECT * FROM idr_meta.rule ORDER BY rule_id") == metadata
    with pytest.raises(StaleReview):
        AIReviewService(warehouse, "bob").get(preview["run_id"])
    with pytest.raises(StaleReview):
        service.get(preview["run_id"], "stale")
    with pytest.raises(StaleReview):
        service.get(preview["run_id"], lens="household")


def test_comparison_uses_frozen_inputs_and_partition_changes(warehouse):
    cfg = config()
    service = AIReviewService(warehouse)
    preview = service.preview(cfg)
    # Source data changes must not influence the selected comparison.
    warehouse.execute("DELETE FROM raw.customers")
    patch = service._patch(cfg, "remove_rule", "phone", None)
    result = service.compare(preview["run_id"], cfg, patch)
    assert result["telemetry"]["total_entities"] == 3
    assert result["telemetry"]["total_clusters"] == 2
    assert result["comparison"]["changes"]["split_affected_entities"] == 3
    assert result["comparison"]["same_extraction"] is True
    changed = service.patch(cfg, patch)
    with pytest.raises(StaleReview):
        service.patch(changed, patch)


def test_selected_lens_preview_and_trial(warehouse):
    cfg = config()
    cfg["lenses"] = [{"id": "person", "include_types": ["EMAIL"]}]
    service = AIReviewService(warehouse)
    p = service.preview(cfg, {"lens": "person"})
    assert p["review"]["telemetry"]["total_clusters"] == 2
    assert set(p["review"]["telemetry"]["edges_by_type"]) == {"email"}
    patch = service._patch(cfg, "set_lens_types", "person", ["EMAIL", "PHONE"])
    trial = service.compare(p["run_id"], cfg, patch, "person")
    assert trial["telemetry"]["total_clusters"] == 1


def test_expired_inputs_keep_report_but_reject_comparison(warehouse):
    cfg = config()
    s = AIReviewService(warehouse)
    p = s.preview(cfg)
    warehouse.execute("UPDATE idr_out.ai_review_report SET input_expires_at = '2000-01-01'")
    assert s.get(p["run_id"])
    with pytest.raises(StaleReview, match="expired"):
        s.compare(p["run_id"], cfg, s._patch(cfg, "remove_rule", "phone", None))
    s.purge()
    assert s.get(p["run_id"])


def test_failed_preview_drops_scratch_and_never_changes_active_config(warehouse, monkeypatch):
    cfg = config()
    apply_config_to_warehouse(warehouse, cfg)
    before = warehouse.query("SELECT * FROM idr_meta.source_table")

    def fail(*args, **kwargs):
        raise RuntimeError("local error containing sensitive data")

    monkeypatch.setattr(IDRRunner, "run", fail)
    with pytest.raises(RuntimeError):
        AIReviewService(warehouse).preview(cfg)
    assert warehouse.query("SELECT * FROM idr_meta.source_table") == before
    assert not warehouse.query(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'ai_p%'"
    )


@pytest.mark.parametrize("provider", ["ollama", "openai", "anthropic", "gemini"])
def test_actual_wire_boundary_excludes_values_prose_goal_and_sql(warehouse, provider):
    calls = []

    class WireClient(LLMClient):
        def _post_json(self, url, body, headers):
            calls.append(body)
            # Force fallback after the actual provider request has been built.
            raise RuntimeError("offline")

    client = WireClient(LLMConfig(provider=provider, api_key="private-key", model="test"))
    g = AIRuleGenerator(warehouse, client, max_repairs=0)
    # Catalog prose may contain names and real examples; it must not reach AI.
    original = warehouse.get_table_columns
    warehouse.get_table_columns = lambda t: [
        {**c, "description": "Alice Wonderland alice.secret@acme.org 5552345678"}
        for c in original(t)
    ]
    cfg = config()
    cfg["sources"][0]["identifiers"][0]["expr"] = "COALESCE(email, 'private_fallback@secret.org')"
    cfg["exclusions"] = [{"type": "email", "value": "another.private@secret.org"}]
    g.generate(
        ["raw.customers"],
        goal="Alice Wonderland has private diagnosis",
        context=AIConfigContext(grain="household"),
    )
    g.generate(
        ["raw.customers"],
        current_config=cfg,
        context=AIConfigContext(grain="household"),
        mode="rules",
    )
    p = AIReviewService(warehouse).preview(config())
    AIReviewService(warehouse).explain(p["run_id"], draft_hash(config()), "default", client)
    wire = json.dumps(calls)
    assert calls
    for secret in (
        "alice.secret",
        "bob.private",
        "Alice Wonderland",
        "5552345678",
        "private_fallback",
        "another.private",
        "COALESCE",
        "private diagnosis",
    ):
        assert secret not in wire


def test_model_profile_has_no_catalog_prose_or_identifier_values(warehouse):
    profile = IDRDataProfiler(warehouse).profile_tables_safe(["raw.customers"])
    profile.tables[0].description = "Private Person's medical record"
    profile.tables[0].columns[0].description = "Alice Wonderland"
    payload, _ = model_evidence(profile)
    text = json.dumps(payload)
    assert "Private Person" not in text and "Alice Wonderland" not in text
    assert "alice.secret" not in text


def test_account_grain_preserves_account_relationships(warehouse):
    warehouse.execute(
        "CREATE TABLE raw.companies AS SELECT * FROM (VALUES ('ORG1','One'),('ORG2','Two'),('ORG3','Three')) t(company_id,name)"
    )
    warehouse.execute("""CREATE TABLE raw.contacts AS SELECT 'CONTACT'||i AS contact_id,
        'ORG'||((i%3)+1) AS company_id, 'contact'||i||'@acme.org' AS email
        FROM range(18) t(i)""")
    plan = generator(warehouse).generate(
        ["raw.companies", "raw.contacts"], context=AIConfigContext(grain="account")
    )
    assert plan.validation_report.valid, plan.validation_report.errors
    assert any("company_id" in r["match_keys"] for r in plan.suggested_config["rules"])
    assert all("email" not in r["match_keys"] for r in plan.suggested_config["rules"])
    assert len(plan.suggested_config["sources"]) == 2


def test_prehashed_source_preserved_without_double_hashing(warehouse):
    warehouse.execute("ALTER TABLE raw.customers ADD COLUMN email_digest VARCHAR")
    warehouse.execute("UPDATE raw.customers SET email_digest = SHA256(email)")
    cfg = config()
    cfg["sources"][0]["identifiers"] = [
        {"type": "email_hash", "expr": "email_digest", "source_is_hashed": True}
    ]
    cfg["rules"] = [
        {
            "id": "digest",
            "match_keys": ["email_hash"],
            "canonicalize": "EXACT",
            "max_group_size": 25,
        }
    ]
    apply_config_to_warehouse(warehouse, cfg)
    assert sql_to_config(warehouse)["sources"][0]["identifiers"][0]["source_is_hashed"] is True
    result = IDRRunner(warehouse).run(RunConfig())
    assert result.status == "SUCCESS"
    value = warehouse.query_one(
        "SELECT identifier_value_norm FROM idr_out.identity_identifiers_current WHERE entity_key='crm:A'"
    )
    assert value == warehouse.query_one(
        "SELECT email_digest FROM raw.customers WHERE customer_id='A'"
    )


def test_static_contract_rejects_unknown_and_duplicate_references():
    from idr_core.ai_config import candidate_errors

    cfg = config()
    cfg["rules"][0]["match_keys"] = ["not_mapped"]
    cfg["rules"].append(copy.deepcopy(cfg["rules"][0]))
    errors = candidate_errors(cfg)
    assert any("Duplicate" in e for e in errors)
    assert any("mapped" in e for e in errors)


def test_fuzzy_template_executes_and_comparison_measures_it(warehouse):
    from idr_core.config_schema import expand_fuzzy_template

    cfg = config()
    cfg["rules"] = [cfg["rules"][0]]
    cfg["ai_assistance"]["fuzzy"] = True
    warehouse.execute(
        "UPDATE raw.customers SET name='Alice', email='a@acme.org' WHERE customer_id='A'"
    )
    warehouse.execute(
        "UPDATE raw.customers SET name='Alicia', email='b@acme.org' WHERE customer_id='B'"
    )
    cfg["fuzzy_rules"] = [
        expand_fuzzy_template("NAME_FIRST_LAST_JW", "fuzzy_name", "Name", ["name"], threshold=0.8)
    ]
    service = AIReviewService(warehouse)
    preview = service.preview(cfg)
    assert preview["review"]["telemetry"]["total_clusters"] == 1
    patch = service._patch(cfg, "raise_fuzzy_threshold", "fuzzy_name", 0.99)
    trial = service.compare(preview["run_id"], cfg, patch)
    assert trial["telemetry"]["total_clusters"] == 2


def test_labeled_evaluation_stays_local_and_is_frozen(warehouse):
    cfg = config()
    warehouse.execute(
        "CREATE TABLE raw.truth AS SELECT * FROM (VALUES ('crm:A','p1'),('crm:B','p2'),('crm:C','p1')) t(entity_key,truth_id)"
    )
    s = AIReviewService(warehouse)
    baseline = s.preview(cfg, ground_truth={"ground_truth_table": "raw.truth"})
    assert baseline["review"]["evaluation"]["precision"] < 1
    warehouse.execute("DELETE FROM raw.truth")
    trial = s.compare(baseline["run_id"], cfg, s._patch(cfg, "remove_rule", "phone", None))
    assert trial["evaluation"]["precision"] == 1
    assert "truth_id" not in json.dumps(trial)


def test_real_email_containing_one_digit_is_not_a_placeholder(warehouse):
    warehouse.execute("UPDATE raw.customers SET email='person1@acme.org'")
    profile = IDRDataProfiler(warehouse).profile_tables_safe(["raw.customers"])
    assert not any(
        x["identifier_value_pattern"] == "person1@acme.org" for x in profile.discovered_exclusions
    )


def test_api_workflow_generation_preview_review_compare_and_stale_patch(warehouse, monkeypatch):
    from fastapi.testclient import TestClient

    from idr_api.dependencies import get_adapter, get_current_user
    from idr_api.main import app
    from idr_api.routers import ai, setup

    user = {"sub": "workflow-test", "roles": ["admin"]}
    monkeypatch.setattr(
        app,
        "dependency_overrides",
        {
            get_current_user: lambda: user,
            get_adapter: lambda: warehouse,
        },
    )

    class Manager:
        def get_adapter_for_user(self, key):
            return warehouse

    monkeypatch.setattr(setup, "manager", Manager())
    store = ai.LLMConfigStore()
    store.set(user["sub"], LLMConfig(provider="disabled"))
    app.dependency_overrides[ai.get_llm_config_store] = lambda: store
    client = TestClient(app)
    assert (
        client.post("/api/ai/suggest-config", json={"tables": ["raw.customers"]}).status_code == 422
    )
    generated = client.post(
        "/api/ai/suggest-config",
        json={
            "tables": ["raw.customers"],
            "context": AIConfigContext(grain="household").model_dump(),
        },
    )
    assert generated.status_code == 200, generated.text
    proposal = generated.json()
    assert proposal["validation_report"]["valid"]
    cfg = proposal["suggested_config"]
    with monkeypatch.context() as guard:

        def unexpected_review(*args, **kwargs):
            raise AssertionError("Dry run and saved preview retrieval must not invoke AI review")

        guard.setattr(AIReviewService, "explain", unexpected_review)
        preview = client.post(
            "/api/setup/run", json={"mode": "FULL", "dry_run": True, "config": cfg}
        )
        assert preview.status_code == 200, preview.text
        p = preview.json()
        assert client.get(f"/api/ai/reviews/{p['run_id']}").status_code == 200
    invalid = copy.deepcopy(cfg)
    invalid["sources"][0].pop("entity_key", None)
    invalid["sources"][0].pop("key_columns", None)
    rejected = client.post("/api/setup/run", json={"dry_run": True, "config": invalid})
    assert rejected.status_code == 422
    assert "missing 'entity_key'" in rejected.json()["detail"]
    failed_progress = client.get("/api/setup/run/progress").json()
    assert failed_progress["status"] == "FAILED"
    assert failed_progress["current_stage"] is None
    review = client.post(
        "/api/ai/critique-dry-run",
        json={"config": cfg, "run_id": p["run_id"], "draft_hash": p["draft_hash"]},
    )
    assert review.status_code == 200, review.text
    assert client.get(f"/api/ai/reviews/{p['run_id']}").status_code == 200
    phone = next(r for r in cfg["rules"] if "phone" in r["match_keys"])
    patch = AIReviewService._patch(cfg, "remove_rule", phone["id"], None)
    comparison = client.post(
        "/api/ai/compare", json={"run_id": p["run_id"], "config": cfg, "patch": patch}
    )
    assert comparison.status_code == 200, comparison.text
    streamed = client.post(
        "/api/ai/compare/stream", json={"run_id": p["run_id"], "config": cfg, "patch": patch}
    )
    assert streamed.status_code == 200
    assert "event: progress" in streamed.text and "event: result" in streamed.text
    patched = client.post("/api/ai/patch", json={"config": cfg, "patch": patch})
    assert patched.status_code == 200
    assert (
        client.post(
            "/api/ai/patch", json={"config": patched.json()["config"], "patch": patch}
        ).status_code
        == 409
    )
    assert client.post("/api/ai/critique-dry-run", json={"config": cfg}).status_code == 422
    client.close()


def test_saved_draft_can_reopen_its_review_without_losing_limits(warehouse):
    plan = generator(warehouse).generate(
        ["raw.customers"], context=AIConfigContext(grain="household")
    )
    cfg = plan.suggested_config
    cfg["rules"][0]["max_group_size"] = 10
    service = AIReviewService(warehouse)
    preview = service.preview(cfg)
    apply_config_to_warehouse(warehouse, cfg)
    reopened = sql_to_config(warehouse)
    assert reopened["rules"][0]["max_group_size"] == 10
    assert draft_hash(reopened) == draft_hash(cfg)
    assert service.get(preview["run_id"], draft_hash(reopened))


def test_legacy_warehouse_migrates_hash_metadata_without_losing_config(warehouse):
    cfg = config()
    apply_config_to_warehouse(warehouse, cfg)
    warehouse.execute("ALTER TABLE idr_meta.identifier_mapping DROP COLUMN source_is_hashed")
    assert len(sql_to_config(warehouse)["sources"]) == 1
    assert IDRRunner(warehouse).run(RunConfig()).status == "SUCCESS"
    assert "source_is_hashed" in {
        c["name"] for c in warehouse.get_table_columns("idr_meta.identifier_mapping")
    }


def test_unbound_python_review_rejected_and_scratch_tables_not_discovered(warehouse):
    from idr_core.ai_assistant import critique_dry_run_with_ai
    from idr_core.bootstrap import AutoBootstrapper

    with pytest.raises(ValueError, match="run_id"):
        critique_dry_run_with_ai(warehouse, config())
    AIReviewService(warehouse).preview(config())
    assert all("ai_p" not in t for t in AutoBootstrapper(warehouse)._discover_tables())


def test_rules_only_preserves_key_columns_and_preview_supports_them(warehouse):
    cfg = config()
    cfg["sources"][0].pop("entity_key")
    cfg["sources"][0]["key_columns"] = ["customer_id"]
    plan = generator(warehouse).generate(
        ["raw.customers"],
        current_config=cfg,
        context=AIConfigContext(grain="household"),
        mode="rules",
    )
    assert plan.validation_report.valid, plan.validation_report.errors
    assert plan.suggested_config["sources"] == cfg["sources"]
    preview = AIReviewService(warehouse).preview(plan.suggested_config)
    assert preview["review"]["telemetry"]["total_entities"] == 3


def test_incremental_preview_without_delta_has_insufficient_evidence(warehouse):
    cfg = config()
    apply_config_to_warehouse(warehouse, cfg)
    assert IDRRunner(warehouse).run(RunConfig()).status == "SUCCESS"
    preview = AIReviewService(warehouse).preview(cfg, {"run_mode": "INCR"})
    assert preview["status"] == "DRY_RUN_COMPLETE"
    assert preview["review"]["status"] == "insufficient_evidence"
    assert not preview["review"]["findings"]


def test_rules_only_profiles_current_sources_and_uses_exact_for_opaque_ids(warehouse, monkeypatch):
    cfg = config()
    cfg["sources"][0]["identifiers"] = [{"type": "opaque_ref", "expr": "customer_id"}]
    context = AIConfigContext(allowed_matching_identifiers=["opaque_ref"])
    g = generator(warehouse)
    seen = []
    original = g.profiler.profile_tables_safe

    def profile(tables, **kwargs):
        seen.extend(tables)
        return original(tables, **kwargs)

    monkeypatch.setattr(g.profiler, "profile_tables_safe", profile)
    plan = g.generate(["raw.events"], current_config=cfg, context=context, mode="rules")
    assert seen == ["raw.customers"]
    assert plan.validation_report.valid, plan.validation_report.errors
    assert plan.suggested_config["rules"][0]["canonicalize"] == "EXACT"


def test_omitted_table_backfill_has_one_mapping_per_type_and_records_ambiguity(warehouse):
    from idr_core.ai_config import candidate_errors

    warehouse.execute("ALTER TABLE raw.events ADD COLUMN buyer_email VARCHAR")
    warehouse.execute("UPDATE raw.events SET buyer_email=email")
    g = generator(warehouse)
    profile = g.profiler.profile_tables_safe(["raw.customers", "raw.events"])
    # Simulate a model omitting an identifier-bearing table in Pass A.
    decisions = [
        PassATableDecision(
            table="raw.customers",
            source_id="crm",
            include_in_idr=True,
            entity_key="customer_id",
            identifiers=[{"type": "email", "expr": "email"}],
            reasoning="Master",
        )
    ]
    sources, explanations = g._materialize_sources(decisions, profile)
    events = next(s for s in sources if s.table == "raw.events")
    assert len([m for m in events.identifiers if m.type == "email"]) == 1
    assert any(e.target == "raw.events:email" and e.needs_review for e in explanations)
    spec, _ = g._assemble_plan(sources, explanations, PassBRuleOutput(), profile)
    assert not candidate_errors(spec.to_idr_config_dict())


def test_duplicate_model_mappings_are_normalized_and_explained(warehouse):
    g = generator(warehouse)
    profile = g.profiler.profile_tables_safe(["raw.customers"])
    decision = PassATableDecision(
        table="raw.customers",
        source_id="crm",
        include_in_idr=True,
        entity_key="customer_id",
        identifiers=[
            {"type": "Email", "expr": "email"},
            {"type": " email ", "expr": "LOWER(email)"},
        ],
        reasoning="Model proposal",
    )
    sources, explanations = g._materialize_sources([decision], profile)
    assert len([m for m in sources[0].identifiers if m.type == "email"]) == 1
    assert any(e.target == "raw.customers:email" and e.needs_review for e in explanations)


def test_complete_ai_proposal_with_duplicate_profiler_hints_is_applicable(warehouse):
    warehouse.execute("ALTER TABLE raw.events ADD COLUMN buyer_email VARCHAR")
    warehouse.execute("UPDATE raw.events SET buyer_email=email")

    class ModelClient(LLMClient):
        def chat_structured(self, messages, json_schema, schema_name, **kwargs):
            if schema_name == "pass_a_table_mapping":
                return {
                    "tables": [
                        {
                            "table": "raw.customers",
                            "source_id": "crm",
                            "include_in_idr": True,
                            "entity_key": "customer_id",
                            "identifiers": [{"type": "email", "expr": "email"}],
                            "reasoning": "Master",
                        }
                    ]
                }
            return {
                "rules": [
                    {
                        "rule": {
                            "id": "email",
                            "match_keys": ["email"],
                            "canonicalize": "EMAIL_STANDARD",
                            "max_group_size": 25,
                        },
                        "reasoning": "Email matching",
                    }
                ]
            }

    client = ModelClient(LLMConfig(provider="openai", model="test"))
    proposal = AIRuleGenerator(warehouse, client).generate(["raw.customers", "raw.events"])
    assert proposal.validation_report.valid, proposal.validation_report.errors
    assert proposal.status == "needs_decisions"
    assert len(
        {
            i["type"]
            for s in proposal.suggested_config["sources"]
            if s["table"] == "raw.events"
            for i in s["identifiers"]
        }
    ) == len(
        next(s for s in proposal.suggested_config["sources"] if s["table"] == "raw.events")[
            "identifiers"
        ]
    )


def test_bridge_aggregate_requires_field_and_strategy_approval(warehouse):
    from idr_core.ai_config import enforce_policy, policy_errors

    warehouse.execute("ALTER TABLE raw.events ADD COLUMN order_amount DOUBLE DEFAULT 10")
    cfg = config()
    cfg["sources"].append(
        {
            "id": "events",
            "table": "raw.events",
            "entity_key": "event_id",
            "watermark_column": "created_at",
            "identifiers": [{"type": "email", "expr": "email"}],
            "attributes": [{"name": "total_amount", "expr": "order_amount"}],
        }
    )
    cfg["survivorship"] = [{"attribute": "total_amount", "strategy": "AGG_SUM"}]
    context = AIConfigContext(grain="household", table_roles={"raw.events": "EVENT_FACT"})
    cfg["ai_assistance"] = context.model_dump()
    # AI policy still flags unapproved bridge aggregates for AI proposals...
    assert any(
        "explicit aggregate approval in Survivorship" in e
        for e in policy_errors(cfg, context, cfg)
    )
    # ...while explicit human saves and runs succeed without fighting AI policy locks.
    apply_config_to_warehouse(warehouse, cfg)
    context.approved_event_aggregates = {"raw.events": {"total_amount": "AGG_SUM"}}
    cfg["ai_assistance"] = context.model_dump()
    assert policy_errors(cfg, context, cfg) == []
    apply_config_to_warehouse(warehouse, cfg)
    restored = sql_to_config(warehouse)
    assert restored["ai_assistance"]["approved_event_aggregates"] == {
        "raw.events": {"total_amount": "AGG_SUM"}
    }
    result = AIReviewService(warehouse).preview(cfg, {"strict": True})
    assert result["status"] == "DRY_RUN_COMPLETE"
    result = IDRRunner(warehouse).run(RunConfig(strict=True))
    assert result.status == "SUCCESS"
    assert warehouse.query("SELECT total_amount FROM idr_out.golden_profiles") == [
        {"total_amount": 30.0}
    ]
    changed = copy.deepcopy(cfg)
    changed["survivorship"][0]["strategy"] = "RECENCY"
    assert any(
        "explicit aggregate approval" in e for e in policy_errors(changed, context, changed)
    )
    apply_config_to_warehouse(warehouse, changed)
    assert warehouse.query("SELECT strategy FROM idr_meta.survivorship_rule") == [
        {"strategy": "RECENCY"}
    ]
    profile = IDRDataProfiler(warehouse).profile_tables_safe(["raw.customers", "raw.events"])
    profile.table_map()["raw.events"].table_role = "EVENT_FACT"
    context.system_of_record = "raw.events"
    enforced, _ = enforce_policy(cfg, context, profile)
    assert enforced["sources"][1]["attributes"] == cfg["sources"][1]["attributes"]
    assert enforced["survivorship"][0]["strategy"] == "AGG_SUM"


@pytest.mark.parametrize("event_policy", ["exclude", "bridge"])
def test_aggregate_approval_never_grants_unrelated_event_attributes(warehouse, event_policy):
    from idr_core.ai_config import policy_errors

    cfg = config()
    cfg["sources"][0]["table"] = "raw.events"
    context = AIConfigContext(
        grain="household",
        event_policy=event_policy,
        table_roles={"raw.events": "EVENT_FACT"},
        approved_event_aggregates={"raw.events": {"total_amount": "AGG_SUM"}},
    )
    assert policy_errors(cfg, context, cfg)


def test_bridge_aggregate_approval_is_sanitized_at_model_boundary():
    from idr_core.ai_config import model_goal

    context = AIConfigContext(
        approved_event_aggregates={"person.private@example.org": {"private@example.org": "AGG_SUM"}}
    )
    payload = model_goal(context)
    assert "private@example.org" not in payload
    assert "AGG_SUM" in payload
