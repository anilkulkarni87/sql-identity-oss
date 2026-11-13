"""
Tests for:
1. Batched metadata SQL generation (`config_to_sql(..., batched=True)`) across dialects
   and async config save with progress tracking.
2. Persistent GDPR/CCPA suppression (`idr_meta.privacy_suppression`) ensuring
   forgotten entities are NOT rebuilt on subsequent FULL or INCR pipeline runs.
"""

from idr_api.routers.setup import _execute_save_config
from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.config import config_to_sql, sql_to_config
from idr_core.privacy import PrivacyManager
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager


def _sample_large_config():
    return {
        "sources": [
            {
                "id": f"src_{i}",
                "table": f"raw.customers_{i}",
                "entity_type": "PERSON",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                    {"type": "phone", "expr": "phone"},
                ],
                "attributes": [
                    {"name": "first_name", "expr": "first_name"},
                    {"name": "last_name", "expr": "last_name"},
                    {"name": "city", "expr": "city"},
                ],
            }
            for i in range(6)
        ],
        "rules": [
            {"id": "r_email", "type": "EXACT", "match_keys": ["email"], "priority": 1},
            {"id": "r_phone", "type": "EXACT", "match_keys": ["phone"], "priority": 2},
        ],
        "survivorship": [
            {"attribute": "first_name", "strategy": "RECENCY", "recency_field": "updated_at"},
            {"attribute": "last_name", "strategy": "RECENCY", "recency_field": "updated_at"},
            {"attribute": "city", "strategy": "RECENCY", "recency_field": "updated_at"},
        ],
    }


def test_config_to_sql_batched_reduces_statements_and_roundtrips():
    cfg = _sample_large_config()
    unbatched = config_to_sql(cfg, dialect="duckdb", batched=False)
    batched = config_to_sql(cfg, dialect="duckdb", batched=True)

    # 6 sources + 12 identifiers + 18 attributes + 2 rules + 3 survivorship = 41 unbatched
    assert len(unbatched) == 41
    # Batched emits 1 statement per table = 5 statements total
    assert len(batched) == 5

    adapter = DuckDBAdapter(":memory:")
    SchemaManager(adapter).initialize(reset=True)
    for stmt in batched:
        adapter.execute(stmt)

    reloaded = sql_to_config(adapter)
    assert len(reloaded["sources"]) == 6
    assert len(reloaded["rules"]) == 2
    assert len(reloaded["survivorship"]) == 3


def test_config_to_sql_batched_multidialect_and_dedup():
    cfg = _sample_large_config()
    # Add duplicate survivorship rule for same PK to ensure MERGE deduplicates
    cfg["survivorship"].append(
        {"attribute": "city", "strategy": "FREQUENCY", "recency_field": None}
    )

    for dialect in ("snowflake", "bigquery", "databricks"):
        stmts = config_to_sql(cfg, dialect=dialect, batched=True)
        assert len(stmts) == 5
        assert all("MERGE INTO" in s and "UNION ALL" in s for s in stmts)
        # Ensure 'city' appears only once in the survivorship MERGE source
        surv_stmt = [s for s in stmts if "idr_meta.survivorship_rule" in s][0]
        assert surv_stmt.count("'city' AS attribute_name") == 1


def test_gdpr_forget_persists_across_full_and_incr_reruns():
    adapter = DuckDBAdapter(":memory:")
    SchemaManager(adapter).initialize(reset=True)

    adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    adapter.execute("""
        CREATE TABLE raw.crm_users (
            id VARCHAR,
            email VARCHAR,
            first_name VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    adapter.execute("""
        INSERT INTO raw.crm_users VALUES
            ('u1', 'alice@example.com', 'Alice', TIMESTAMP '2026-01-01 00:00:00'),
            ('u2', 'alice@example.com', 'Alice B', TIMESTAMP '2026-01-02 00:00:00'),
            ('u3', 'bob@example.com', 'Bob', TIMESTAMP '2026-01-01 00:00:00')
    """)

    cfg = {
        "sources": [
            {
                "id": "crm",
                "table": "raw.crm_users",
                "entity_type": "PERSON",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [{"type": "email", "expr": "email"}],
                "attributes": [{"name": "first_name", "expr": "first_name"}],
            }
        ],
        "rules": [{"id": "r_email", "type": "EXACT", "match_keys": ["email"], "priority": 1}],
        "survivorship": [
            {"attribute": "first_name", "strategy": "RECENCY", "recency_field": "updated_at"}
        ],
    }
    for stmt in config_to_sql(cfg, dialect="duckdb", batched=True):
        adapter.execute(stmt)

    # Initial FULL run: Alice (u1, u2) and Bob (u3)
    res1 = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert res1.status == "SUCCESS"
    assert res1.entities_processed == 3
    assert adapter.query_one("SELECT COUNT(*) FROM idr_out.golden_profiles") == 2

    # GDPR forget Alice by email
    pm = PrivacyManager(adapter)
    forget_res = pm.forget(identifier_value="alice@example.com", reason="GDPR Article 17")
    assert forget_res["status"] == "COMPLETED"
    assert sorted(forget_res["entity_keys"]) == ["crm:u1", "crm:u2"]
    assert forget_res["suppressed_entities"] == 2
    assert adapter.query_one("SELECT COUNT(*) FROM idr_meta.privacy_suppression") == 2
    assert adapter.query_one("SELECT COUNT(*) FROM idr_out.golden_profiles") == 1

    # Rerun FULL load: raw.crm_users STILL contains u1 & u2, but they must NOT be rebuilt!
    res2 = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert res2.status == "SUCCESS"
    assert res2.entities_processed == 1
    assert adapter.query_one("SELECT COUNT(*) FROM idr_out.golden_profiles") == 1
    remaining_keys = [
        r["entity_key"]
        for r in adapter.query("SELECT entity_key FROM idr_out.identity_resolved_membership_current")
    ]
    assert remaining_keys == ["crm:u3"]

    # Rerun INCR load: still suppressed
    res3 = IDRRunner(adapter).run(RunConfig(run_mode="INCR"))
    assert res3.status in ("SUCCESS", "SUCCESS_NO_CHANGES")
    assert adapter.query_one("SELECT COUNT(*) FROM idr_out.golden_profiles") == 1


def test_execute_save_config_reports_progress_and_uses_batches():
    adapter = DuckDBAdapter(":memory:")
    SchemaManager(adapter).initialize(reset=True)
    cfg = _sample_large_config()
    events = []

    res = _execute_save_config(
        adapter,
        cfg,
        progress_cb=lambda step, label, pct, detail: events.append((step, pct)),
    )
    assert res["status"] == "ok"
    assert res["batches_executed"] == 5
    assert events[0][0] == "validating"
    assert events[-1] == ("done", 100)
    reloaded = sql_to_config(adapter)
    assert len(reloaded["sources"]) == 6
