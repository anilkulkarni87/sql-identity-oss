"""Publication, rebuild and reproducibility safeguards against real DuckDB runs."""

import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.config import apply_config_to_warehouse, sql_to_config
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager


@pytest.fixture
def engine():
    a = DuckDBAdapter(":memory:")
    SchemaManager(a).initialize()
    a.execute("CREATE SCHEMA raw")
    a.execute("""CREATE TABLE raw.records(id VARCHAR, email VARCHAR, phone VARCHAR,
                 account VARCHAR, updated_at TIMESTAMP, city VARCHAR, state VARCHAR)""")
    # Email and phone yield a six-node chain; account supplies a shortcut star.
    for i in range(6):
        a.conn.execute(
            "INSERT INTO raw.records VALUES(?,?,?,?,?,?,?)",
            [
                str(i),
                f"e{i // 2}@company.invalid",
                f"p{(i + 1) // 2}",
                "shared",
                "2026-01-01",
                f"city{i}",
                f"state{i}",
            ],
        )
    cfg = {
        "sources": [
            {
                "id": "records",
                "table": "raw.records",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [{"type": k, "expr": k} for k in ["email", "phone"]],
                "attributes": [{"name": k, "expr": k} for k in ["city", "state"]],
            }
        ],
        "rules": [
            {
                "id": k,
                "match_keys": [k],
                "canonicalize": "EXACT",
                "priority": i,
                "max_group_size": 100,
            }
            for i, k in enumerate(["email", "phone"])
        ],
        "survivorship": [{"attribute": k, "strategy": "RECENCY"} for k in ["city", "state"]],
    }
    apply_config_to_warehouse(a, cfg)
    yield a, cfg
    a.close()


def outputs(a):
    tables = [
        "identity_resolved_membership_current",
        "identity_edges_current",
        "identity_clusters_current",
        "golden_profiles",
        "identity_lens_membership_current",
        "identity_lens_clusters_current",
    ]
    return {
        t: a.conn.execute(f"SELECT * FROM idr_out.{t} ORDER BY ALL").fetchall()
        for t in tables
        if a.table_exists("idr_out." + t)
    }


def run(a, **kw):
    return IDRRunner(a).run(RunConfig(strict=True, **kw))


def test_unconverged_graph_preserves_published_outputs_and_watermarks(engine):
    a, _ = engine
    assert run(a).status == "SUCCESS"
    before = outputs(a)
    watermarks = a.query("SELECT * FROM idr_meta.run_state WHERE table_id<>'__idr_run_lock__'")
    failed = run(a, max_iters=1)
    assert failed.status == "FAILED"
    assert "did not converge" in failed.error
    assert outputs(a) == before
    assert (
        a.query("SELECT * FROM idr_meta.run_state WHERE table_id<>'__idr_run_lock__'") == watermarks
    )
    assert run(a, max_iters=30).status == "SUCCESS"


def test_nonconverged_lens_is_rejected_before_primary_publication(engine):
    a, cfg = engine
    cfg["sources"][0]["identifiers"].append({"type": "account", "expr": "account"})
    cfg["rules"].append(
        {"id": "account", "match_keys": ["account"], "canonicalize": "EXACT", "priority": 3}
    )
    cfg["lenses"] = [{"id": "chain", "include_types": ["email", "phone"]}]
    apply_config_to_warehouse(a, cfg)
    assert run(a).status == "SUCCESS"
    before = outputs(a)
    failed = run(a, max_iters=2)
    assert failed.status == "FAILED"
    assert "lens chain" in failed.error
    assert outputs(a) == before


@pytest.mark.parametrize("mutation", ["rule", "mapping", "attribute", "strict", "cap", "fuzzy"])
def test_incremental_matching_drift_requires_successful_full_rebuild(engine, mutation):
    a, _ = engine
    assert run(a).status == "SUCCESS"
    before = outputs(a)
    options = {}
    if mutation == "rule":
        a.execute("UPDATE idr_meta.rule SET max_group_size=25 WHERE rule_id='email'")
    elif mutation == "mapping":
        a.execute(
            "UPDATE idr_meta.identifier_mapping SET identifier_value_expr='LOWER(email)' WHERE identifier_type='email'"
        )
    elif mutation == "attribute":
        a.execute(
            "UPDATE idr_meta.entity_attribute_mapping SET attribute_expr='UPPER(city)' WHERE attribute_name='city'"
        )
    elif mutation == "strict":
        options["strict"] = False
    elif mutation == "cap":
        options["max_group_size"] = 50
    else:
        a.execute(
            "INSERT INTO idr_meta.fuzzy_rule(rule_id,blocking_key_expr,score_expr,threshold,priority,is_active) VALUES('f','city','1',0.9,1,TRUE)"
        )
    strict = options.pop("strict", True)
    result = IDRRunner(a).run(RunConfig(run_mode="INCR", strict=strict, **options))
    assert result.status == "FAILED"
    assert "--mode FULL" in result.error
    assert outputs(a) == before
    # Failure cannot bless the changed configuration as a successful baseline.
    again = IDRRunner(a).run(RunConfig(run_mode="INCR", strict=strict, **options))
    assert again.status == "FAILED"


def test_iteration_budget_is_not_matching_drift(engine):
    a, _ = engine
    assert run(a, max_iters=20).status == "SUCCESS"
    assert run(a, run_mode="INCR", max_iters=40).status == "SUCCESS_NO_CHANGES"


def test_legacy_baseline_requires_one_full_rebuild(engine):
    a, _ = engine
    assert run(a).status == "SUCCESS"
    a.execute("UPDATE idr_out.run_history SET matching_config_hash=NULL")
    assert run(a, run_mode="INCR").status == "FAILED"
    assert run(a).status == "SUCCESS"
    assert run(a, run_mode="INCR").status == "SUCCESS_NO_CHANGES"


def test_metrics_distinguish_resolved_and_changed_clusters(engine):
    a, _ = engine
    first = run(a)
    assert first.clusters_resolved == first.clusters_impacted == 1
    assert first.clusters_changed == 1
    assert first.source_tables_processed == 1
    row = a.query("SELECT * FROM idr_out.run_history WHERE run_id=?", [first.run_id])[0]
    assert row["ended_at"] is not None
    assert row["duration_seconds_precise"] == pytest.approx(first.duration_seconds)
    assert row["duration_seconds_precise"] > 0
    assert row["source_tables_processed"] == 1
    assert run(a).clusters_changed == 0
    a.execute(
        "INSERT INTO raw.records VALUES('new','unique@company.invalid','newphone','new',TIMESTAMP '2026-01-02','newcity','newstate')"
    )
    assert run(a, run_mode="INCR").clusters_changed == 1


@pytest.mark.parametrize(
    "strategy", ["RECENCY", "EARLIEST", "PRIORITY", "PRIORITY_RECENCY", "LATEST_FROM_SOURCE"]
)
def test_survivorship_is_stable_when_source_scan_order_changes(engine, strategy):
    a, cfg = engine
    for r in cfg["survivorship"]:
        r.update(strategy=strategy, source_priority=["records"])
    apply_config_to_warehouse(a, cfg)
    assert run(a).status == "SUCCESS"
    before = a.query("SELECT city,state FROM idr_out.golden_profiles")
    a.execute("CREATE OR REPLACE TABLE raw.records AS SELECT * FROM raw.records ORDER BY id DESC")
    assert run(a).status == "SUCCESS"
    assert a.query("SELECT city,state FROM idr_out.golden_profiles") == before


def test_grouped_survivorship_keeps_null_from_winning_row(engine):
    a, cfg = engine
    a.execute("UPDATE raw.records SET state=NULL WHERE id='0'")
    for r in cfg["survivorship"]:
        r["row_group"] = "address"
    apply_config_to_warehouse(a, cfg)
    assert sql_to_config(a)["survivorship"][0]["row_group"] == "address"
    assert run(a).status == "SUCCESS"
    assert a.query("SELECT city,state FROM idr_out.golden_profiles") == [
        {"city": "city0", "state": None}
    ]
    a.execute("CREATE OR REPLACE TABLE raw.records AS SELECT * FROM raw.records ORDER BY id DESC")
    assert run(a).status == "SUCCESS"
    assert a.query("SELECT city,state FROM idr_out.golden_profiles") == [
        {"city": "city0", "state": None}
    ]


def test_inconsistent_group_policy_fails_without_publishing(engine):
    a, cfg = engine
    assert run(a).status == "SUCCESS"
    before = outputs(a)
    for r in cfg["survivorship"]:
        r["row_group"] = "address"
    cfg["survivorship"][1]["strategy"] = "EARLIEST"
    apply_config_to_warehouse(a, cfg)
    result = run(a)
    assert result.status == "FAILED"
    assert "must share strategy" in result.error
    assert outputs(a) == before


def test_fuzzy_nonconvergence_blocks_primary_outputs(engine):
    a, cfg = engine
    a.execute("UPDATE raw.records SET email='unique'||id||'@company.invalid',phone='unique'||id")
    cfg["sources"][0]["attributes"].append({"name": "idx", "expr": "CAST(id AS INTEGER)"})
    cfg["fuzzy_rules"] = [
        {
            "id": "adjacent",
            "blocking_key": "1",
            "score_expr": "CASE WHEN ABS(CAST(<a>.idx AS INTEGER) - CAST(<b>.idx AS INTEGER))=1 THEN 1.0 ELSE 0.0 END",
            "threshold": 0.9,
            "priority": 1,
        }
    ]
    apply_config_to_warehouse(a, cfg)
    assert IDRRunner(a).run(RunConfig(strict=False)).status == "SUCCESS"
    before = outputs(a)
    result = IDRRunner(a).run(RunConfig(strict=False, max_iters=1))
    assert result.status == "FAILED"
    assert result.error_code == "GRAPH_NOT_CONVERGED"
    assert "fuzzy" in result.error
    assert outputs(a) == before


def test_completed_partition_is_allowed_on_last_iteration(engine):
    a, _ = engine
    a.execute("DELETE FROM raw.records WHERE id NOT IN ('0','1')")
    result = run(a, max_iters=1)
    assert result.status == "SUCCESS"
    assert result.clusters_resolved == 1
    assert not any("did not fully converge" in w for w in result.warnings)


def test_frequency_ties_are_exact_and_stable(engine):
    a, cfg = engine
    for r in cfg["survivorship"]:
        r["strategy"] = "FREQUENCY"
    apply_config_to_warehouse(a, cfg)
    assert run(a).status == "SUCCESS"
    assert a.query("SELECT city FROM idr_out.golden_profiles") == [{"city": "city0"}]
    a.execute("CREATE OR REPLACE TABLE raw.records AS SELECT * FROM raw.records ORDER BY id DESC")
    assert run(a).status == "SUCCESS"
    assert a.query("SELECT city FROM idr_out.golden_profiles") == [{"city": "city0"}]
    a.execute("UPDATE raw.records SET city='more_frequent' WHERE id IN ('4','5')")
    assert run(a).status == "SUCCESS"
    assert a.query("SELECT city FROM idr_out.golden_profiles") == [{"city": "more_frequent"}]


def test_preview_exposes_actionable_safeguard_error(engine):
    from idr_core.ai_review import AIReviewService, PreviewValidationError

    a, cfg = engine
    assert run(a).status == "SUCCESS"
    with pytest.raises(PreviewValidationError, match="did not converge"):
        AIReviewService(a).preview(cfg, {"strict": True, "max_iters": 1})


def test_round_tripped_default_metadata_does_not_force_rebuild(engine):
    a, _ = engine
    a.execute("UPDATE idr_meta.source_table SET watermark_lookback_minutes=NULL, deduplicate=NULL")
    assert run(a).status == "SUCCESS"
    apply_config_to_warehouse(a, sql_to_config(a))
    assert run(a, run_mode="INCR").status == "SUCCESS_NO_CHANGES"


def test_run_history_api_preserves_precise_timings(engine):
    from idr_api.routers.runs import get_run_detail, get_runs

    a, _ = engine
    result = run(a)
    summary = get_runs(limit=1, adapter=a)[0]
    detail = get_run_detail(result.run_id, adapter=a)
    assert summary.duration_seconds == pytest.approx(result.duration_seconds)
    assert detail.source_tables_processed == 1
    assert detail.clusters_resolved == 1
    assert detail.clusters_changed == 1
    assert any(s.duration_seconds > 0 and s.duration_seconds % 1 != 0 for s in detail.stages)


def test_recency_override_is_used_instead_of_extraction_watermark(engine):
    a, cfg = engine
    a.execute("ALTER TABLE raw.records ADD COLUMN profile_updated_at TIMESTAMP")
    a.execute("UPDATE raw.records SET profile_updated_at=TIMESTAMP '2026-01-01'")
    a.execute("UPDATE raw.records SET profile_updated_at=TIMESTAMP '2026-02-01' WHERE id='4'")
    for r in cfg["survivorship"]:
        r["recency_field"] = "profile_updated_at"
    apply_config_to_warehouse(a, cfg)
    assert run(a).status == "SUCCESS"
    assert a.query("SELECT city,state FROM idr_out.golden_profiles") == [
        {"city": "city4", "state": "state4"}
    ]


def test_identifier_can_also_supply_a_surviving_profile_attribute(engine):
    from idr_core.ai_config import candidate_errors

    a, cfg = engine
    # Matching uses normalized email; the golden field keeps the source value.
    a.execute("UPDATE raw.records SET email='E0@Company.Invalid' WHERE id='0'")
    cfg["rules"][0]["canonicalize"] = "EMAIL_STANDARD"
    cfg["sources"][0]["attributes"].append({"name": "email", "expr": "email"})
    cfg["survivorship"].append({"attribute": "email", "strategy": "RECENCY"})
    assert candidate_errors(cfg) == []
    apply_config_to_warehouse(a, cfg)
    restored = sql_to_config(a)
    assert any(i["type"] == "email" for i in restored["sources"][0]["identifiers"])
    assert any(v["name"] == "email" for v in restored["sources"][0]["attributes"])
    assert run(a).status == "SUCCESS"
    assert a.query("SELECT email FROM idr_out.golden_profiles") == [{"email": "E0@Company.Invalid"}]
    assert a.query(
        "SELECT identifier_value_norm FROM idr_out.identity_identifiers_current WHERE entity_key='records:0' AND identifier_type='email'"
    ) == [{"identifier_value_norm": "e0@company.invalid"}]


def test_profiles_use_the_same_deduplicated_source_as_extraction(engine):
    a, cfg = engine
    a.execute('ALTER TABLE raw.records ADD COLUMN order_amount DOUBLE DEFAULT 10')
    cfg['sources'][0].update(deduplicate=True, dedup_order_by='updated_at DESC')
    cfg['sources'][0]['attributes'] = [{'name': 'total_amount', 'expr': 'order_amount'}]
    cfg['survivorship'] = [{'attribute': 'total_amount', 'strategy': 'AGG_SUM'}]
    apply_config_to_warehouse(a, cfg)
    assert run(a).status == 'SUCCESS'
    a.execute("INSERT INTO raw.records SELECT * REPLACE (TIMESTAMP '2026-02-01' AS updated_at, 50 AS order_amount) FROM raw.records WHERE id='0'")
    incremental = run(a, run_mode='INCR')
    assert incremental.status == 'SUCCESS'
    assert a.query_one('SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current') == 6
    assert a.query_one('SELECT SUM(total_amount) FROM idr_out.golden_profiles') == 100
    assert run(a).status == 'SUCCESS'
    assert a.query_one('SELECT SUM(total_amount) FROM idr_out.golden_profiles') == 100


def test_bigquery_preview_adapter_qualifies_project_and_dataset_mapping():
    from types import SimpleNamespace
    from idr_core.ai_review import PreviewAdapter, _physical

    bq = SimpleNamespace(
        dialect="bigquery",
        project="my-gcp-proj",
        dataset_mapping={"idr_out": "custom_out"},
        schema_mapping={},
    )
    token = "p" + "a" * 24
    scoped = PreviewAdapter(bq, token)
    assert scoped.schema_mapping["idr_meta"] == f"`my-gcp-proj`.ai_{token}_idr_meta"
    assert scoped._rewrite_table_fqn("idr_out.run_history") == f"`my-gcp-proj`.ai_{token}_idr_out.run_history"
    assert _physical(bq, "idr_out.run_history") == "`my-gcp-proj`.custom_out.run_history"
    assert _physical(bq, "idr_meta.run_state") == "`my-gcp-proj`.idr_meta.run_state"


def test_lens_publication_failure_rolls_back_primary_and_lens_outputs(monkeypatch):
    a = DuckDBAdapter(':memory:')
    a.execute('CREATE SCHEMA raw')
    a.execute("CREATE TABLE raw.s (id VARCHAR, email VARCHAR, ts TIMESTAMP)")
    a.execute("INSERT INTO raw.s VALUES ('1','a@x.test',TIMESTAMP '2025-01-01'), ('2','a@x.test',TIMESTAMP '2025-01-02')")
    cfg = {
        'sources': [{'id': 's', 'table': 'raw.s', 'entity_key': 'id', 'watermark_column': 'ts',
                     'identifiers': [{'type': 'email', 'expr': 'email'}]}],
        'rules': [{'id': 'r1', 'match_keys': ['email'], 'priority': 1}],
        'lenses': [{'id': 'strict_email', 'include_types': ['EMAIL']}],
    }
    apply_config_to_warehouse(a, cfg)
    assert run(a).status == 'SUCCESS'
    before_members = a.query_one('SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current')
    before_lens = a.query_one('SELECT COUNT(*) FROM idr_out.identity_lens_membership_current')
    before_wm = a.query_one("SELECT last_watermark_value FROM idr_meta.run_state WHERE table_id='s'")

    # Add a new record and force lens materialization to fail mid-Stage 8
    a.execute("INSERT INTO raw.s VALUES ('3','b@x.test',TIMESTAMP '2025-01-05')")
    from idr_core.stages.output import OutputStage

    def boom(self, lens_id):
        raise RuntimeError("Simulated lens publish crash")

    monkeypatch.setattr(OutputStage, "materialize_lens_output", boom)
    res = run(a, run_mode='INCR')
    assert res.status == 'FAILED'
    # Primary outputs, lens outputs, and watermarks must all remain rolled back
    assert a.query_one('SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current') == before_members
    assert a.query_one('SELECT COUNT(*) FROM idr_out.identity_lens_membership_current') == before_lens
    assert a.query_one("SELECT last_watermark_value FROM idr_meta.run_state WHERE table_id='s'") == before_wm


def test_atomic_run_lock_rejects_concurrent_holder():
    from datetime import datetime, timezone

    a = DuckDBAdapter(':memory:')
    a.execute('CREATE SCHEMA raw')
    a.execute("CREATE TABLE raw.s (id VARCHAR, email VARCHAR)")
    a.execute("INSERT INTO raw.s VALUES ('1','a@x.test')")
    cfg = {
        'sources': [{'id': 's', 'table': 'raw.s', 'entity_key': 'id',
                     'identifiers': [{'type': 'email', 'expr': 'email'}]}],
        'rules': [{'id': 'r1', 'match_keys': ['email'], 'priority': 1}],
    }
    apply_config_to_warehouse(a, cfg)
    utc_now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    a.execute(
        "INSERT INTO idr_meta.run_state (table_id, last_watermark_value, last_run_id, updated_at) "
        f"VALUES ('__idr_run_lock__', NULL, 'other_run', TIMESTAMP '{utc_now}')"
    )
    res = run(a)
    assert res.status == 'FAILED'
    assert 'Concurrent IDR run in progress' in (res.error or '')
