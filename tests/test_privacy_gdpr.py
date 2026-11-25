"""
Domain tests for GDPR / CCPA Right-to-be-Forgotten (`PrivacyManager.forget`)
and persistent entity suppression (`idr_meta.privacy_suppression`) across FULL and INCR runs.
"""

from __future__ import annotations

import pytest

from idr_core.config import config_to_sql
from idr_core.privacy import PrivacyManager
from idr_core.runner import IDRRunner, RunConfig


@pytest.mark.e2e
def test_gdpr_forget_persists_across_full_and_incr_reruns(duckdb_adapter):
    duckdb_adapter.execute("CREATE SCHEMA IF NOT EXISTS raw")
    duckdb_adapter.execute("""
        CREATE TABLE raw.crm_users (
            id VARCHAR,
            email VARCHAR,
            first_name VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    duckdb_adapter.execute("""
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
        duckdb_adapter.execute(stmt)

    res1 = IDRRunner(duckdb_adapter).run(RunConfig(run_mode="FULL"))
    assert res1.status == "SUCCESS"
    assert res1.entities_processed == 3
    assert duckdb_adapter.query_one("SELECT COUNT(*) FROM idr_out.golden_profiles") == 2

    pm = PrivacyManager(duckdb_adapter)
    forget_res = pm.forget(identifier_value="alice@example.com", reason="GDPR Article 17")
    assert forget_res["status"] == "COMPLETED"
    assert sorted(forget_res["entity_keys"]) == ["crm:u1", "crm:u2"]
    assert forget_res["suppressed_entities"] == 2
    assert duckdb_adapter.query_one("SELECT COUNT(*) FROM idr_meta.privacy_suppression") == 2
    assert duckdb_adapter.query_one("SELECT COUNT(*) FROM idr_out.golden_profiles") == 1

    # Rerun FULL load: raw.crm_users still contains u1 & u2, yet suppression keeps them purged
    res2 = IDRRunner(duckdb_adapter).run(RunConfig(run_mode="FULL"))
    assert res2.status == "SUCCESS"
    assert res2.entities_processed == 1
    assert duckdb_adapter.query_one("SELECT COUNT(*) FROM idr_out.golden_profiles") == 1
    remaining_keys = [
        r["entity_key"]
        for r in duckdb_adapter.query(
            "SELECT entity_key FROM idr_out.identity_resolved_membership_current"
        )
    ]
    assert remaining_keys == ["crm:u3"]

    res3 = IDRRunner(duckdb_adapter).run(RunConfig(run_mode="INCR"))
    assert res3.status in ("SUCCESS", "SUCCESS_NO_CHANGES")
    assert duckdb_adapter.query_one("SELECT COUNT(*) FROM idr_out.golden_profiles") == 1
