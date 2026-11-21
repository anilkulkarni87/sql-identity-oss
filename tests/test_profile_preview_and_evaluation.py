"""Bounded opt-in profile previews and dataset-independent reviewed-label adapters."""

import copy
import time
from decimal import Decimal

import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.ai_config import draft_hash
from idr_core.ai_review import AIReviewService, StaleReview
from idr_core.config import apply_config_to_warehouse, sql_to_config
from idr_core.evaluation_config import EvaluationOptions, acceptance, materialize_labels
from idr_core.evaluator import IDREvaluator
from idr_core.profile_preview import (
    BudgetAdapter,
    ProfilePreviewOptions,
    preview_profiles,
    query_budget,
)
from idr_core.runner import IDRRunner, RunConfig


@pytest.fixture
def data():
    a = DuckDBAdapter(":memory:")
    a.execute("CREATE SCHEMA raw")
    a.execute(
        "CREATE TABLE raw.people AS SELECT * FROM (VALUES ('a1','same@company.invalid',10.0),('a2','solo@company.invalid',20.0)) t(pk,email,amount)"
    )
    a.execute(
        "CREATE TABLE raw.accounts AS SELECT * FROM (VALUES ('b1','same@company.invalid',5.0),('b2','other@company.invalid',7.0)) t(account_no,mail,value)"
    )
    cfg = {
        "sources": [
            {
                "id": "alpha",
                "table": "raw.people",
                "entity_key": "pk",
                "identifiers": [{"type": "email", "expr": "email"}],
                "attributes": [{"name": "total", "expr": "amount"}],
            },
            {
                "id": "beta",
                "table": "raw.accounts",
                "entity_key": "account_no",
                "identifiers": [{"type": "email", "expr": "mail"}],
                "attributes": [{"name": "total", "expr": "value"}],
            },
        ],
        "rules": [{"id": "email", "match_keys": ["email"], "canonicalize": "EXACT"}],
        "survivorship": [{"attribute": "total", "strategy": "AGG_SUM"}],
    }
    apply_config_to_warehouse(a, cfg)
    yield a, cfg
    a.close()


def table_state(a):
    return {
        t: a.query(f"SELECT * FROM idr_out.{t} ORDER BY ALL")
        for t in [
            "identity_resolved_membership_current",
            "identity_edges_current",
            "golden_profiles",
        ]
    }


def test_validation_and_default_dry_run_do_not_build_profiles(data, monkeypatch):
    a, cfg = data
    monkeypatch.setattr(
        a,
        "query",
        lambda *args, **kwargs: pytest.fail(
            "Metadata-only configuration validation must not query rows"
        ),
    )
    result = preview_profiles(
        AIReviewService(a), cfg, None, None, "default", ProfilePreviewOptions()
    )
    assert result["data_scanned"] is False
    monkeypatch.undo()
    p = AIReviewService(a).preview(cfg, {"strict": True})
    assert p["profile_validation"]["data_scanned"] is False
    token = AIReviewService(a)._row(p["run_id"])["scratch_token"]
    assert not a.table_exists(f"ai_{token}_idr_work.profile_preview_result")


def test_targeted_preview_uses_complete_clusters_and_preserves_outputs(data):
    a, cfg = data
    assert IDRRunner(a).run(RunConfig(strict=True)).status == "SUCCESS"
    before = table_state(a)
    service = AIReviewService(a)
    p = service.preview(cfg, {"strict": True})
    options = ProfilePreviewOptions(
        mode="targeted", attributes=["total"], max_clusters=1, allow_unknown_scan_cost=True
    )
    result = preview_profiles(service, cfg, p["run_id"], p["draft_hash"], "default", options)
    assert result["selected_clusters"] == 1
    assert result["selected_members"] == 2
    assert Decimal(str(result["attributes"][0]["after"]["sample_sum"])) == 15
    assert result["attributes"][0]["changed_profiles"] == 0
    assert table_state(a) == before
    small = options.model_copy(update={"max_members": 1})
    result = preview_profiles(service, cfg, p["run_id"], p["draft_hash"], "default", small)
    assert result["selected_members"] == 1
    assert Decimal(str(result["attributes"][0]["after"]["sample_sum"])) in (20, 7)


def test_preview_requires_explicit_cost_and_full_population_approval(data):
    a, cfg = data
    service = AIReviewService(a)
    p = service.preview(cfg, {"strict": True})
    for options, match in [
        (ProfilePreviewOptions(mode="targeted", attributes=["total"]), "scan cost"),
        (
            ProfilePreviewOptions(mode="full", attributes=["total"], allow_unknown_scan_cost=True),
            "explicit confirmation",
        ),
    ]:
        with pytest.raises(ValueError, match=match):
            preview_profiles(service, cfg, p["run_id"], p["draft_hash"], "default", options)
    result = preview_profiles(
        service,
        cfg,
        p["run_id"],
        p["draft_hash"],
        "default",
        ProfilePreviewOptions(
            mode="full",
            attributes=["total"],
            allow_unknown_scan_cost=True,
            allow_full_population=True,
        ),
    )
    assert result["selected_members"] == 4
    assert Decimal(str(result["attributes"][0]["after"]["sample_sum"])) == 42


def test_profile_preview_is_owner_and_draft_bound(data):
    a, cfg = data
    service = AIReviewService(a, "owner")
    p = service.preview(cfg, {"strict": True})
    options = ProfilePreviewOptions(
        mode="targeted", attributes=["total"], allow_unknown_scan_cost=True
    )
    with pytest.raises(StaleReview):
        preview_profiles(
            AIReviewService(a, "other"), cfg, p["run_id"], p["draft_hash"], "default", options
        )
    changed = copy.deepcopy(cfg)
    changed["survivorship"][0]["strategy"] = "AGG_MAX"
    with pytest.raises(StaleReview):
        preview_profiles(service, changed, p["run_id"], p["draft_hash"], "default", options)


def test_time_budget_blocks_queries_that_start_after_deadline(data):
    a, _ = data
    with pytest.raises(ValueError, match="time budget"):
        with query_budget(a, 0.01) as check:
            bounded = BudgetAdapter(a, check)
            time.sleep(0.03)
            bounded.execute("CREATE TABLE should_not_exist AS SELECT 1")
    assert not a.table_exists("should_not_exist")


def labels(a):
    a.execute(
        "CREATE TABLE raw.reviewed AS SELECT * FROM (VALUES ('left','a1','P1'),('left','a2','P2'),('right','b1','P1'),('right','b2','P3')) t(kind,external_key,reviewed_group)"
    )
    return {
        "ground_truth_table": "raw.reviewed",
        "truth_id_col": "reviewed_group",
        "min_precision": 1,
        "min_recall": 1,
        "min_label_overlap": 1,
        "source_mappings": [
            {
                "source_id": "alpha",
                "key_columns": {"pk": "external_key"},
                "source_column": "kind",
                "source_value": "left",
            },
            {
                "source_id": "beta",
                "key_columns": {"account_no": "external_key"},
                "source_column": "kind",
                "source_value": "right",
            },
        ],
    }


def test_label_mapping_freezes_cohort_and_survives_config_roundtrip(data):
    a, cfg = data
    cfg["evaluation"] = labels(a)
    apply_config_to_warehouse(a, cfg)
    assert sql_to_config(a)["evaluation"] == cfg["evaluation"]
    service = AIReviewService(a)
    p = service.preview(cfg, {"strict": True})
    assert p["review"]["evaluation_acceptance"]["status"] == "passed"
    evaluation = p["review"]["evaluation"]
    assert evaluation["evaluated_entities"] == 4 and evaluation["truth_pairs"] == 1
    assert evaluation["label_overlap"] == 1
    a.execute("UPDATE raw.reviewed SET reviewed_group='changed'")
    patch = service._patch(cfg, "set_group_limit", "email", 10)
    compared = service.compare(p["run_id"], cfg, patch)
    assert compared["evaluation"] == evaluation
    assert compared["evaluation_acceptance"]["status"] == "passed"


def test_duplicate_and_conflicting_labels_are_rejected(data):
    a, cfg = data
    options = labels(a)
    a.execute("INSERT INTO raw.reviewed VALUES('left','a1','another_person')")
    with pytest.raises(ValueError, match="duplicate/conflicting"):
        AIReviewService(a).preview(cfg, {"strict": True}, ground_truth=options)


def test_no_label_overlap_is_not_perfect_accuracy(data):
    a, cfg = data
    IDRRunner(a).run(RunConfig(strict=True))
    a.execute("CREATE TABLE raw.labels(entity_key VARCHAR,truth_id VARCHAR)")
    a.execute("INSERT INTO raw.labels VALUES('missing:1','X'),('missing:2','X')")
    r = IDREvaluator(a).evaluate_ground_truth("raw.labels")
    assert r["evaluated_entities"] == 0
    assert r["precision"] is None and r["recall"] is None
    assert (
        acceptance(r, EvaluationOptions(ground_truth_table="raw.labels"))["status"]
        == "insufficient_evidence"
    )


def test_label_adapter_uses_composite_engine_key_construction(data):
    a, cfg = data
    a.execute(
        "CREATE TABLE raw.assets AS SELECT * FROM (VALUES ('org1','x','one@company.invalid'),('org2','x','two@company.invalid')) t(tenant,asset,mail)"
    )
    cfg["sources"] = [
        {
            "id": "assets",
            "table": "raw.assets",
            "entity_key": "AUTO",
            "key_columns": ["tenant", "asset"],
            "identifiers": [{"type": "email", "expr": "mail"}],
        }
    ]
    cfg["survivorship"] = []
    apply_config_to_warehouse(a, cfg)
    assert IDRRunner(a).run(RunConfig(strict=True)).status == "SUCCESS"
    a.execute(
        "CREATE TABLE raw.asset_labels AS SELECT tenant AS org,asset AS external_id,tenant AS reviewed_group FROM raw.assets"
    )
    options = EvaluationOptions(
        ground_truth_table="raw.asset_labels",
        truth_id_col="reviewed_group",
        source_mappings=[
            {"source_id": "assets", "key_columns": {"tenant": "org", "asset": "external_id"}}
        ],
        min_true_pairs=0,
    )
    normalized = materialize_labels(a, cfg, options)
    r = IDREvaluator(a).evaluate_ground_truth(**normalized)
    assert r["evaluated_entities"] == 2 and r["label_overlap"] == 1
    assert r["recall"] is None


def test_time_budget_interrupts_an_executing_query_and_connection_recovers(data):
    a, _ = data
    with pytest.raises(ValueError, match="time budget"):
        with query_budget(a, 0.01) as check:
            BudgetAdapter(a, check).query_one("SELECT SUM(i*i) FROM range(1000000000) t(i)")
    assert a.query_one("SELECT 1") == 1


def test_normalized_cohort_limit_and_acceptance_failure(data):
    a, cfg = data
    options = labels(a)
    with pytest.raises(ValueError, match="exceeds max_label_rows"):
        materialize_labels(
            a, cfg, EvaluationOptions.model_validate({**options, "max_label_rows": 2})
        )
    a.execute(
        "UPDATE raw.reviewed SET reviewed_group='different' WHERE kind='right' AND external_key='b1'"
    )
    service = AIReviewService(a)
    p = service.preview(cfg, {"strict": True}, ground_truth=options)
    # No true pairs remain, so recall cannot be measured; this cannot pass a gate.
    assert p["review"]["evaluation_acceptance"]["status"] == "insufficient_evidence"
    assert p["review"]["evaluation"]["recall"] is None


def test_http_preview_and_evaluation_are_independent_of_ai(data, monkeypatch):
    from fastapi.testclient import TestClient

    from idr_api.dependencies import get_adapter, get_current_user
    from idr_api.main import app
    from idr_api.routers import setup

    a, cfg = data
    cfg["evaluation"] = labels(a)
    user = {"sub": "generic-review", "roles": ["admin"]}
    monkeypatch.setattr(
        app, "dependency_overrides", {get_current_user: lambda: user, get_adapter: lambda: a}
    )

    class Manager:
        def get_adapter_for_user(self, key):
            return a

    monkeypatch.setattr(setup, "manager", Manager())
    monkeypatch.setattr(
        AIReviewService,
        "explain",
        lambda *args, **kwargs: pytest.fail("Profile preview must not invoke AI"),
    )
    client = TestClient(app)
    validated = client.post("/api/setup/profile-preview", json={"config": cfg})
    assert validated.status_code == 200 and validated.json()["data_scanned"] is False
    p = client.post("/api/setup/run", json={"config": cfg, "dry_run": True, "strict": True})
    assert p.status_code == 200, p.text
    body = p.json()
    assert body["review"]["evaluation_acceptance"]["status"] == "passed"
    response = client.post(
        "/api/setup/profile-preview",
        json={
            "config": cfg,
            "run_id": body["run_id"],
            "draft_hash": body["draft_hash"],
            "options": {
                "mode": "targeted",
                "attributes": ["total"],
                "allow_unknown_scan_cost": True,
                "max_clusters": 1,
            },
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["selected_members"] == 2
    client.close()


def test_missing_label_column_is_actionable_before_graph_execution(data, monkeypatch):
    a, cfg = data
    options = labels(a)
    options["truth_id_col"] = "missing_review_column"
    monkeypatch.setattr(
        IDRRunner,
        "run",
        lambda *args, **kwargs: pytest.fail("Do not run the graph for an invalid label mapping"),
    )
    with pytest.raises(ValueError, match="missing_review_column is missing"):
        AIReviewService(a).preview(cfg, {"strict": True}, ground_truth=options)


def test_no_complete_cluster_does_not_scan_profile_sources(data, monkeypatch):
    a, cfg = data
    # All records join one component larger than the requested member budget.
    a.execute("UPDATE raw.people SET email='same@company.invalid'")
    a.execute("UPDATE raw.accounts SET mail='same@company.invalid'")
    service = AIReviewService(a)
    p = service.preview(cfg, {"strict": True})
    result = preview_profiles(
        service,
        cfg,
        p["run_id"],
        p["draft_hash"],
        "default",
        ProfilePreviewOptions(
            mode="targeted", attributes=["total"], max_members=1, allow_unknown_scan_cost=True
        ),
    )
    assert result["status"] == "no_complete_clusters_within_budget"
    assert result["selected_members"] == 0
    token = service._row(p["run_id"])["scratch_token"]
    assert not a.table_exists(f"ai_{token}_idr_work.profile_preview_values")


def test_cli_acceptance_returns_failure_without_rebuilding_graph(
    data, monkeypatch, tmp_path, capsys
):
    import json

    from idr_core import cli

    a, cfg = data
    assert IDRRunner(a).run(RunConfig(strict=True)).status == "SUCCESS"
    a.execute(
        "CREATE TABLE raw.cli_labels AS SELECT entity_key, 'one_identity' AS truth_id FROM idr_out.identity_resolved_membership_current"
    )
    spec = tmp_path / "acceptance.json"
    spec.write_text(json.dumps({"ground_truth_table": "raw.cli_labels", "min_recall": 1}))
    monkeypatch.setattr(cli, "get_adapter", lambda args: a)
    monkeypatch.setattr(a, "close", lambda: None)
    monkeypatch.setattr(
        IDRRunner,
        "run",
        lambda *args, **kwargs: pytest.fail("Acceptance must reuse existing memberships"),
    )
    assert cli.main(["evaluate", "--platform", "duckdb", "--evaluation-config", str(spec)]) == 1
    assert '"status": "failed"' in capsys.readouterr().out
    assert not a.query(
        "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'eval_labels_%'"
    )


def test_cli_metadata_profile_validation_never_builds_model_client(
    data, monkeypatch, tmp_path, capsys
):
    import json

    from idr_core import cli

    a, cfg = data
    path = tmp_path / "config.json"
    path.write_text(json.dumps(cfg))
    monkeypatch.setattr(cli, "get_adapter", lambda args: a)
    monkeypatch.setattr(a, "close", lambda: None)
    monkeypatch.setattr(
        cli,
        "_build_llm_client_from_args",
        lambda *args: pytest.fail("Profile validation must not create a model client"),
    )
    assert cli.main(["ai", "profile-preview", "--platform", "duckdb", "--config", str(path)]) == 0
    assert '"data_scanned": false' in capsys.readouterr().out


def test_bigquery_query_budget_cancels_job_and_sets_max_bytes_billed():
    from types import SimpleNamespace
    from idr_core.adapters.bigquery import BigQueryAdapter

    captured = {}

    class FakeJob:
        def __init__(self, should_timeout=False):
            self.should_timeout = should_timeout
            self.cancelled = False

        def result(self, timeout=None):
            captured["timeout"] = timeout
            if self.should_timeout:
                raise TimeoutError("deadline")
            return []

        def cancel(self):
            self.cancelled = True
            captured["cancelled"] = True

    class FakeClient:
        project = "demo-proj"

        def __init__(self, should_timeout=False):
            self.should_timeout = should_timeout

        def query(self, sql, location=None, job_config=None):
            captured["job_config"] = job_config
            return FakeJob(self.should_timeout)

    class FakeConfig:
        maximum_bytes_billed = None

    bq = BigQueryAdapter(FakeClient(should_timeout=False), project="demo-proj")
    bq._apply_budget_config = lambda cfg=None: (
        setattr(c := (cfg or FakeConfig()), "maximum_bytes_billed", bq._budget_local.max_bytes_billed) or c
    )
    with query_budget(bq, 5, max_bytes_billed=10_000_000) as check:
        BudgetAdapter(bq, check).execute("SELECT 1")
    assert captured["job_config"].maximum_bytes_billed == 10_000_000
    assert captured["timeout"] is not None and captured["timeout"] > 0

    bq_timeout = BigQueryAdapter(FakeClient(should_timeout=True), project="demo-proj")
    with pytest.raises(ValueError, match="time budget"):
        with query_budget(bq_timeout, 5) as check:
            BudgetAdapter(bq_timeout, check).execute("SELECT 1")
    assert captured.get("cancelled") is True


def test_snowflake_query_budget_dbapi_and_snowpark():
    from idr_core.adapters.snowflake import SnowflakeAdapter

    # 1. DBAPI mode passes per-statement timeout
    seen = {}

    class FakeCursor:
        description = [("val",)]

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, *args, timeout=None):
            seen["timeout"] = timeout
            if "SLOW" in sql:
                raise RuntimeError("Statement reached its statement or warehouse timeout")

        def fetchone(self):
            return (1,)

    class FakeConn:
        def cursor(self):
            return FakeCursor()

    sf = SnowflakeAdapter(FakeConn())
    with query_budget(sf, 5) as check:
        assert BudgetAdapter(sf, check).query_one("SELECT 1") == 1
    assert seen["timeout"] >= 1
    with pytest.raises(ValueError, match="time budget"):
        with query_budget(sf, 5) as check:
            BudgetAdapter(sf, check).execute("SELECT SLOW")

    # 2. Snowpark mode sets and unsets STATEMENT_TIMEOUT_IN_SECONDS
    stmts = []

    class FakeDF:
        def collect(self):
            return [(1,)]

    class FakeSession:
        def sql(self, sql, params=None):
            stmts.append(sql)
            return FakeDF()

        def create_dataframe(self, *args):
            return FakeDF()

    sf_sp = SnowflakeAdapter(FakeSession())
    with query_budget(sf_sp, 7) as check:
        BudgetAdapter(sf_sp, check).execute("SELECT 1")
    assert any("SET STATEMENT_TIMEOUT_IN_SECONDS = 7" in s for s in stmts)
    assert any("UNSET STATEMENT_TIMEOUT_IN_SECONDS" in s for s in stmts)


def test_databricks_query_budget_sql_cursor_and_spark_job_group():
    from idr_core.adapters.databricks import DatabricksAdapter

    # 1. SQL Warehouse connector cancels active cursor on timeout
    cancelled = {"called": False}

    class SlowCursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, *args):
            time.sleep(0.08)

        def cancel(self):
            cancelled["called"] = True

    class FakeDBConn:
        def cursor(self):
            return SlowCursor()

    db = DatabricksAdapter(FakeDBConn())
    with pytest.raises(ValueError, match="time budget"):
        with query_budget(db, 0.02) as check:
            BudgetAdapter(db, check).execute("SELECT 1")
    assert cancelled["called"] is True

    # 2. SparkSession cancels preview job group on timeout
    spark_events = {"set": None, "cancelled": None, "cleared": False}

    class FakeSC:
        def setJobGroup(self, gid, desc):
            spark_events["set"] = gid

        def cancelJobGroup(self, gid):
            spark_events["cancelled"] = gid

        def clearJobGroup(self):
            spark_events["cleared"] = True

    class FakeSpark:
        sparkContext = FakeSC()

        def sql(self, sql):
            time.sleep(0.08)

    db_spark = DatabricksAdapter(FakeSpark())
    with pytest.raises(ValueError, match="time budget"):
        with query_budget(db_spark, 0.02) as check:
            BudgetAdapter(db_spark, check).execute("SELECT 1")
    assert spark_events["set"] and spark_events["cancelled"] == spark_events["set"]
    assert spark_events["cleared"] is True
