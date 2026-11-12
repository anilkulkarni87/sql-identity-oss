"""
Tests validating resolution of all Genie audit findings:
1. P0: `idr_out.edge_evidence` populated automatically across runs
2. P0: `RECENCY` survivorship preserves un-watermarked dimension attributes (e.g. city)
3. P1: `identity_clusters_current` persists `edge_diversity`, `edge_count`, `match_density`
       and separates single-key vs corroborated vs over-merge fan-out scores
4. P1: Address/household over-merge guardrails in Profiler and Doctor
5. P2: `IDREvaluator` & `idr evaluate` (Precision/Recall/F1 + Stratified Audit Sampling)
"""

import duckdb
import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.cli import main
from idr_core.config import config_to_sql
from idr_core.doctor import CheckStatus, IDRDoctor
from idr_core.evaluator import IDREvaluator
from idr_core.profile_builder import ProfileBuilder
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager


@pytest.fixture
def ecommerce_audit_db(tmp_path):
    """Build a multi-table DuckDB dataset testing evidence, unwatermarked addresses, corroboration, and ground truth."""
    db_file = str(tmp_path / "genie_audit.duckdb")
    conn = duckdb.connect(db_file)

    # 1. Customers table (with watermark)
    conn.execute("""
        CREATE TABLE crm_customers (
            customer_id VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            zip_code VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO crm_customers VALUES
        -- Person A (True ID: P1) - corroborated via email + phone across tables
        ('c1', 'alice@acme-retail.com', '5551112222', 'Alice', 'Smith', '10001', '2026-01-10 10:00:00'),
        -- Person B (True ID: P2) - single-key match via email only
        ('c2', 'bob@acme-retail.com', NULL, 'Bob', 'Jones', '90001', '2026-01-11 10:00:00'),
        -- Person C (True ID: P3) & Person D (True ID: P4) - share an apartment address_id (household overmerge risk)
        ('c3', 'carol@acme-retail.com', '5553334444', 'Carol', 'White', '60601', '2026-01-12 10:00:00'),
        ('c4', 'dave@acme-retail.com', '5554445555', 'Dave', 'Brown', '60601', '2026-01-13 10:00:00'),
        -- Singleton near-miss: two records for Eve (P5) sharing last_name + zip_code but unmatched keys
        ('c5', 'eve.personal@acme-retail.com', NULL, 'Eve', 'Miller', '98101', '2026-01-14 10:00:00'),
        ('c6', 'eve.work@acme-retail.com', NULL, 'Evelyn', 'Miller', '98101', '2026-01-15 10:00:00')
    """)

    # 2. Orders table (with watermark)
    conn.execute("""
        CREATE TABLE orders (
            order_id VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            address_id VARCHAR,
            order_date TIMESTAMP
        )
    """)
    conn.execute("""
        INSERT INTO orders VALUES
        -- Matches c1 on BOTH email and phone (corroboration: edge_diversity = 2)
        ('o1', 'alice@acme-retail.com', '5551112222', 'addr_1', '2026-02-01 12:00:00'),
        -- Matches c2 on email only (single key: edge_diversity = 1)
        ('o2', 'bob@acme-retail.com', NULL, 'addr_2', '2026-02-02 12:00:00'),
        -- Shares addr_apt with c3 and c4
        ('o3', 'carol@acme-retail.com', NULL, 'addr_apt', '2026-02-03 12:00:00'),
        ('o4', 'dave@acme-retail.com', NULL, 'addr_apt', '2026-02-04 12:00:00')
    """)

    # 3. Un-watermarked Addresses table (NO watermark_column -> recency_ts is NULL!)
    conn.execute("""
        CREATE TABLE addresses (
            address_id VARCHAR,
            city VARCHAR,
            state VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO addresses VALUES
        ('addr_1', 'New York', 'NY'),
        ('addr_2', 'Los Angeles', 'CA'),
        ('addr_apt', 'Chicago', 'IL')
    """)

    # 4. Ground truth table for precision/recall verification
    conn.execute("""
        CREATE TABLE gt_mapping (
            entity_key VARCHAR,
            truth_id VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO gt_mapping VALUES
        ('crm:c1', 'P1'), ('ord:o1', 'P1'),
        ('crm:c2', 'P2'), ('ord:o2', 'P2'),
        ('crm:c3', 'P3'), ('ord:o3', 'P3'),
        ('crm:c4', 'P4'), ('ord:o4', 'P4'),
        ('crm:c5', 'P5'), ('crm:c6', 'P5'),
        ('addr:addr_1', 'P1'), ('addr:addr_2', 'P2'), ('addr:addr_apt', 'P3')
    """)

    conn.close()
    return db_file


def test_p0_p1_p2_end_to_end_pipeline(ecommerce_audit_db):
    """Test edge_evidence, unwatermarked RECENCY survivorship, calibrated confidence, and evaluator."""
    adapter = DuckDBAdapter(ecommerce_audit_db)
    sm = SchemaManager(adapter)
    sm.initialize(reset=True)

    cfg = {
        "use_default_exclusions": True,
        "sources": [
            {
                "id": "crm",
                "table": "crm_customers",
                "entity_key": "customer_id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                    {"type": "phone", "expr": "phone"},
                ],
                "attributes": [
                    {"name": "first_name", "expr": "first_name"},
                    {"name": "last_name", "expr": "last_name"},
                    {"name": "zip_code", "expr": "zip_code"},
                ],
            },
            {
                "id": "ord",
                "table": "orders",
                "entity_key": "order_id",
                "watermark_column": "order_date",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                    {"type": "phone", "expr": "phone"},
                    {"type": "address_id", "expr": "address_id"},
                ],
                "attributes": [],
            },
            {
                "id": "addr",
                "table": "addresses",
                "entity_key": "address_id",
                # Intentionally NO watermark_column!
                "identifiers": [{"type": "address_id", "expr": "address_id"}],
                "attributes": [
                    {"name": "city", "expr": "city"},
                    {"name": "state", "expr": "state"},
                ],
            },
        ],
        "rules": [
            {"id": "r_email", "type": "EXACT", "match_keys": ["email"], "canonicalize": "EMAIL_STANDARD", "priority": 1},
            {"id": "r_phone", "type": "EXACT", "match_keys": ["phone"], "canonicalize": "PHONE_DIGITS", "priority": 2},
            {"id": "r_addr", "type": "EXACT", "match_keys": ["address_id"], "canonicalize": "LOWERCASE", "priority": 3, "max_group_size": 100},
        ],
        "survivorship": [
            {"attribute": "first_name", "strategy": "RECENCY"},
            {"attribute": "last_name", "strategy": "RECENCY"},
            {"attribute": "zip_code", "strategy": "RECENCY"},
            # RECENCY on attribute from unwatermarked table! Previously produced 100% NULLs on Databricks/Snowflake/DuckDB
            {"attribute": "city", "strategy": "RECENCY"},
            {"attribute": "state", "strategy": "RECENCY"},
        ],
    }

    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    # 1. Run Pipeline
    runner = IDRRunner(adapter)
    res = runner.run(RunConfig(run_mode="FULL", strict=True))
    assert res.status == "SUCCESS"

    # 2. [P0] Verify edge_evidence was populated!
    ev_cnt = adapter.query_one("SELECT COUNT(*) FROM idr_out.edge_evidence")
    assert ev_cnt > 0, "idr_out.edge_evidence should never be empty when table exists!"

    # 3. [P0] Verify un-watermarked 'city' attribute survived into golden_profiles under RECENCY!
    cities = adapter.query("SELECT resolved_id, city FROM idr_out.golden_profiles WHERE city IS NOT NULL")
    assert len(cities) >= 3, f"Expected city values to survive despite NULL watermark; got {cities}"
    city_set = {r["city"] for r in cities}
    assert {"New York", "Los Angeles", "Chicago"}.issubset(city_set)

    # 4. [P1] Verify corroboration columns persisted & confidence score separated cleanly!
    clusters = {
        r["resolved_id"]: r
        for r in adapter.query("SELECT * FROM idr_out.identity_clusters_current")
    }
    # Find Alice's cluster (corroborated via email + phone -> edge_diversity >= 2)
    alice_rid = adapter.query_one(
        "SELECT resolved_id FROM idr_out.identity_resolved_membership_current WHERE entity_key = 'crm:c1'"
    )
    alice_cluster = clusters[alice_rid]
    assert alice_cluster["edge_diversity"] >= 2
    assert alice_cluster["edge_count"] >= 2
    assert alice_cluster["match_density"] is not None

    # Find Bob's cluster (single-key email + address_id -> let's check scores are strictly < 1.0 for multi-node!)
    bob_rid = adapter.query_one(
        "SELECT resolved_id FROM idr_out.identity_resolved_membership_current WHERE entity_key = 'crm:c2'"
    )
    bob_cluster = clusters[bob_rid]
    assert alice_cluster["confidence_score"] < 1.0  # Calibrated ceiling 0.99, no saturation at 1.0
    assert bob_cluster["confidence_score"] <= alice_cluster["confidence_score"]

    # 5. [P1] Doctor flags address_id household over-merge risk (max_group_size=100 > 50)
    doctor_report = IDRDoctor(adapter).run_diagnostics()
    rule_check = next(c for c in doctor_report.checks if c.name == "Deterministic Matching Rules")
    assert rule_check.status == CheckStatus.WARN
    assert "address_id" in rule_check.message

    # 6. [P2] Evaluator: Supervised Ground-Truth Evaluation
    evaluator = IDREvaluator(adapter)
    gt_eval = evaluator.evaluate_ground_truth("gt_mapping", "entity_key", "truth_id")
    assert gt_eval["status"] == "SUCCESS"
    assert gt_eval["evaluated_entities"] == 13
    assert 0.0 <= gt_eval["precision"] <= 1.0
    assert 0.0 <= gt_eval["recall"] <= 1.0
    assert 0.0 <= gt_eval["f1"] <= 1.0
    # Because c3 and c4 merged via addr_apt, fp_pairs > 0 (caught over-merge!)
    assert gt_eval["fp_pairs"] > 0
    assert len(gt_eval["top_overmerged_clusters"]) >= 1
    # Because c5 and c6 didn't merge, fn_pairs > 0 (caught split!)
    assert gt_eval["fn_pairs"] > 0
    assert len(gt_eval["top_split_entities"]) >= 1

    # 7. [P2] Evaluator: Unsupervised Stratified Audit Sampling
    audit = evaluator.audit_quality_and_sample(sample_per_stratum=10, persist_sample=True)
    assert audit["status"] == "SUCCESS"
    assert audit["samples_persisted"] > 0
    # Should detect Eve Miller (c5, c6) as NEAR_MISS_SINGLETON!
    assert audit["near_miss_candidates_found"] >= 1
    strata_found = {
        r["stratum"] for r in adapter.query("SELECT DISTINCT stratum FROM idr_out.evaluation_sample")
    }
    assert "CORROBORATED_MERGE" in strata_found
    assert "NEAR_MISS_SINGLETON" in strata_found

    adapter.close()

    # 8. CLI `idr evaluate` works cleanly
    ret_gt = main([
        "evaluate", "--platform", "duckdb", "--db", ecommerce_audit_db,
        "--ground-truth-table", "gt_mapping",
    ])
    assert ret_gt == 0

    ret_audit = main(["evaluate", "--platform", "duckdb", "--db", ecommerce_audit_db])
    assert ret_audit == 0


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake", "bigquery", "databricks"])
def test_recency_sql_handles_null_watermark_across_all_dialects(dialect):
    """Verify ProfileBuilder RECENCY safely handles NULL recency_ts across all 4 warehouse dialects."""
    from unittest.mock import MagicMock

    mock_adapter = MagicMock()
    mock_adapter.dialect = dialect
    pb = ProfileBuilder(adapter=mock_adapter)
    sql = pb._build_column_selection({"attribute_name": "city", "strategy": "RECENCY"})
    if dialect == "bigquery":
        assert "ARRAY_AGG(city IGNORE NULLS ORDER BY recency_ts DESC" in sql
    else:
        assert "COALESCE(recency_ts, TIMESTAMP '1970-01-01 00:00:00')" in sql
    assert "city" in sql
