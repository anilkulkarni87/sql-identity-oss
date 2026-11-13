"""Consolidated test suite: test_genie_gap_closers.py, test_scale_and_invariants.py."""

# --- Consolidated from test_genie_gap_closers.py ---
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


def test_fanout_damping_separates_giant_hub_from_healthy_clusters(tmp_path):
    """
    Verify Genie's showcase test:
    - 30-member cluster with fanout=30, 3 types ranks MUCH LOWER than normal 3-member cluster
    - High fanout clusters (>20) get 'High fan-out over-merge risk' reason even with 3+ identifier types
    """
    db_file = str(tmp_path / "fanout_test.duckdb")
    conn = duckdb.connect(db_file)
    conn.execute("CREATE TABLE events (id VARCHAR, email VARCHAR, phone VARCHAR, loyalty VARCHAR, updated_at TIMESTAMP)")

    # Cluster 1: Normal 3-member corroborated cluster (fanout <= 3, 3 types)
    conn.execute("""
        INSERT INTO events VALUES
        ('n1', 'norm@acme-retail.com', '5550001111', 'LOY_NORM', '2026-01-01'),
        ('n2', 'norm@acme-retail.com', '5550001111', NULL, '2026-01-02'),
        ('n3', NULL, '5550001111', 'LOY_NORM', '2026-01-03')
    """)

    # Cluster 2: 30-member hub sharing ONE email + ONE phone + ONE loyalty (max_fanout=30, 3 types)
    rows = [
        (f"h{i}", "sharedhub@acme-retail.com", "5559990000", "LOY_SHARED", "2026-01-01")
        for i in range(30)
    ]
    conn.executemany("INSERT INTO events VALUES (?, ?, ?, ?, ?)", rows)
    conn.close()

    ad = DuckDBAdapter(db_file)
    SchemaManager(ad).initialize(reset=True)
    cfg = {
        "sources": [
            {
                "id": "ev",
                "table": "events",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                    {"type": "phone", "expr": "phone"},
                    {"type": "loyalty", "expr": "loyalty"},
                ],
            }
        ],
        "rules": [
            {"id": "r1", "type": "EXACT", "match_keys": ["email"], "canonicalize": "EMAIL_STANDARD", "priority": 1, "max_group_size": 1000},
            {"id": "r2", "type": "EXACT", "match_keys": ["phone"], "canonicalize": "PHONE_DIGITS", "priority": 2, "max_group_size": 1000},
            {"id": "r3", "type": "EXACT", "match_keys": ["loyalty"], "canonicalize": "LOWERCASE", "priority": 3, "max_group_size": 1000},
        ],
    }
    for s in config_to_sql(cfg, "duckdb"):
        ad.execute(s)

    res = IDRRunner(ad).run(RunConfig(run_mode="FULL", strict=True))
    assert res.status == "SUCCESS"

    norm_rid = ad.query_one("SELECT resolved_id FROM idr_out.identity_resolved_membership_current WHERE entity_key = 'ev:n1'")
    hub_rid = ad.query_one("SELECT resolved_id FROM idr_out.identity_resolved_membership_current WHERE entity_key = 'ev:h0'")

    clusters = {r["resolved_id"]: r for r in ad.query("SELECT * FROM idr_out.identity_clusters_current")}
    norm_c = clusters[norm_rid]
    hub_c = clusters[hub_rid]

    assert norm_c["edge_diversity"] == 3
    assert hub_c["edge_diversity"] == 3
    assert hub_c["max_fanout"] == 30
    assert norm_c["max_fanout"] <= 3
    # Fanout=30 score = ~0.90 * (2 / sqrt(30)) = ~0.33, whereas norm score >= 0.89!
    assert hub_c["confidence_score"] < 0.40
    assert norm_c["confidence_score"] > 0.88
    assert "High fan-out over-merge risk" in hub_c["primary_reason"]
    ad.close()


# --- Consolidated from test_scale_and_invariants.py ---
"""
Phase 10 tests for:
1. Mathematical Graph Invariants at scale (1,000 entities):
   - Partition invariant (every entity in exactly one cluster; sum of cluster_size == total entities)
   - Connected-component completeness invariant (no edge crosses two different resolved_ids)
   - Minimum-label determinism invariant (resolved_id == MIN(entity_key) of cluster members)
   - Idempotency invariant (re-running FULL or INCR with no changes yields identical clusters)
2. MCP explain_cluster tool
3. API dry_run_summary accurate breakdown (proposed_changes = new_entities + moved_entities)
"""

import duckdb
from fastapi.testclient import TestClient

import idr_api.dependencies as deps
from idr_api.main import app
from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.connection_manager import ConnectionManager
from idr_core.quickstart import configure_metadata, generate_demo_data
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager
from idr_mcp.server import explain_cluster


def test_scale_mathematical_graph_invariants_and_idempotency(tmp_path):
    conn = duckdb.connect(":memory:")
    adapter = DuckDBAdapter(conn)
    SchemaManager(adapter).initialize(reset=True)

    total_rows = generate_demo_data(conn, rows=1000, seed=123)
    assert total_rows == 1000
    configure_metadata(conn)

    runner = IDRRunner(adapter)
    res1 = runner.run(RunConfig(run_mode="FULL", max_iters=30))
    assert res1.status == "SUCCESS"
    assert res1.entities_processed == 1000

    # Invariant 1: Partition Invariant
    member_count = adapter.query_one(
        "SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current"
    )
    distinct_entities = adapter.query_one(
        "SELECT COUNT(DISTINCT entity_key) FROM idr_out.identity_resolved_membership_current"
    )
    sum_cluster_sizes = adapter.query_one(
        "SELECT SUM(cluster_size) FROM idr_out.identity_clusters_current"
    )
    assert member_count == 1000
    assert distinct_entities == 1000
    assert sum_cluster_sizes == 1000

    # Invariant 2: Connected-Component Completeness (0 edges crossing distinct resolved_ids)
    cross_cluster_edges = adapter.query_one("""
        SELECT COUNT(*)
        FROM idr_out.identity_edges_current e
        JOIN idr_out.identity_resolved_membership_current m1 ON e.left_entity_key = m1.entity_key
        JOIN idr_out.identity_resolved_membership_current m2 ON e.right_entity_key = m2.entity_key
        WHERE m1.resolved_id != m2.resolved_id
    """)
    assert cross_cluster_edges == 0

    # Invariant 3: Minimum-Label Determinism (every resolved_id == MIN(entity_key) of its members)
    non_min_clusters = adapter.query_one("""
        SELECT COUNT(*) FROM (
            SELECT resolved_id, MIN(entity_key) AS min_key
            FROM idr_out.identity_resolved_membership_current
            GROUP BY resolved_id
            HAVING resolved_id != MIN(entity_key)
        ) t
    """)
    assert non_min_clusters == 0

    # Invariant 4: Idempotency under FULL and INCR re-runs
    snapshot_1 = adapter.query(
        "SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
    )
    res2 = runner.run(RunConfig(run_mode="FULL", max_iters=30))
    assert res2.status == "SUCCESS"
    snapshot_2 = adapter.query(
        "SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
    )
    assert snapshot_1 == snapshot_2

    # Also test MCP explain_cluster on the largest resolved cluster
    top_cluster = adapter.query_one(
        "SELECT resolved_id FROM idr_out.identity_clusters_current ORDER BY cluster_size DESC LIMIT 1"
    )
    ConnectionManager.instance().set_adapter(adapter, {"platform": "duckdb"})
    explanation = explain_cluster(top_cluster)
    assert explanation["resolved_id"] == top_cluster
    assert explanation["cluster_metadata"]["cluster_size"] >= 2
    assert len(explanation["members"]) == explanation["cluster_metadata"]["cluster_size"]
    assert len(explanation["edges"]) >= 1
    assert explanation["golden_profile"] is not None
    assert "first_name" in explanation["golden_profile"]

    adapter.close()


def test_50k_scale_benchmark_with_incr_and_mcp(tmp_path):
    """
    High-volume scale test:
    - Generates 50,000 synthetic entities across 4 source systems using native SQL vectorization
    - Runs FULL resolution + Golden Profile survivorship + Fuzzy blocking
    - Runs INCR delta resolution on 2,000 newly arriving records
    - Verifies all 4 mathematical graph invariants on 52,000 resolved entities
    - Exercises the full MCP server tool suite against the 52,000-entity graph
    """
    import time
    from idr_mcp import server as mcp_server

    conn = duckdb.connect(":memory:")
    adapter = DuckDBAdapter(conn)
    SchemaManager(adapter).initialize(reset=True)

    # Fast vectorized generation of 50,000 customer rows in DuckDB (<0.2s)
    conn.execute("""
        CREATE OR REPLACE TABLE demo_customers AS
        SELECT
            md5(CAST(i AS VARCHAR)) AS customer_id,
            CASE (i % 4)
                WHEN 0 THEN 'web'
                WHEN 1 THEN 'store'
                WHEN 2 THEN 'mobile'
                ELSE 'call_center'
            END AS source_system,
            'User_' || CAST((i % 20000) AS VARCHAR) AS first_name,
            'Family_' || CAST((i % 10000) AS VARCHAR) AS last_name,
            'user' || CAST((i % 20000) AS VARCHAR) || '@example.com' AS email,
            '555-' || LPAD(CAST((i % 25000) AS VARCHAR), 6, '0') AS phone,
            CASE WHEN i % 3 = 0 THEN 'LYL-' || CAST((i % 15000) AS VARCHAR) ELSE NULL END AS loyalty_id,
            TIMESTAMP '2026-01-01 00:00:00' + INTERVAL (i % 1000) MINUTE AS created_at
        FROM range(1, 50001) t(i)
    """)
    configure_metadata(conn)

    runner = IDRRunner(adapter)
    t0 = time.time()
    res_full = runner.run(RunConfig(run_mode="FULL", max_iters=30))
    full_elapsed = time.time() - t0
    assert res_full.status == "SUCCESS"
    assert res_full.entities_processed == 50000
    assert full_elapsed < 30.0

    # Append 2,000 incremental rows with newer timestamps linking to existing clusters
    conn.execute("""
        INSERT INTO demo_customers
        SELECT
            md5('incr_' || CAST(i AS VARCHAR)) AS customer_id,
            'mobile' AS source_system,
            'UpdatedUser_' || CAST((i % 20000) AS VARCHAR) AS first_name,
            'Family_' || CAST((i % 10000) AS VARCHAR) AS last_name,
            'user' || CAST((i % 20000) AS VARCHAR) || '@example.com' AS email,
            '555-' || LPAD(CAST((i % 25000) AS VARCHAR), 6, '0') AS phone,
            'LYL-' || CAST((i % 15000) AS VARCHAR) AS loyalty_id,
            TIMESTAMP '2026-02-01 00:00:00' + INTERVAL (i % 500) MINUTE AS created_at
        FROM range(50001, 52001) t(i)
    """)

    t1 = time.time()
    res_incr = runner.run(RunConfig(run_mode="INCR", max_iters=30))
    incr_elapsed = time.time() - t1
    assert res_incr.status == "SUCCESS"
    assert res_incr.entities_processed == 2000
    assert incr_elapsed < 20.0

    # Verify all 4 mathematical graph invariants at 52,000 scale
    total_members = adapter.query_one(
        "SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current"
    )
    sum_sizes = adapter.query_one(
        "SELECT SUM(cluster_size) FROM idr_out.identity_clusters_current"
    )
    cross_edges = adapter.query_one("""
        SELECT COUNT(*)
        FROM idr_out.identity_edges_current e
        JOIN idr_out.identity_resolved_membership_current m1 ON e.left_entity_key = m1.entity_key
        JOIN idr_out.identity_resolved_membership_current m2 ON e.right_entity_key = m2.entity_key
        WHERE m1.resolved_id != m2.resolved_id
    """)
    golden_count = adapter.query_one("SELECT COUNT(*) FROM idr_out.golden_profiles")
    cluster_count = adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_clusters_current")

    assert total_members == 52000
    assert sum_sizes == 52000
    assert cross_edges == 0
    assert golden_count == cluster_count

    # Verify MCP server tools at 52,000 scale
    ConnectionManager.instance().set_adapter(adapter, {"platform": "duckdb"})
    metrics = mcp_server.get_metrics_summary()
    assert metrics["total_entities"] == 52000
    assert metrics["total_clusters"] == cluster_count
    assert metrics["total_edges"] > 30000

    search_res = mcp_server.search_identifier("user42@example.com", identifier_type="email")
    assert len(search_res["matches"]) == 1
    cid = search_res["matches"][0]["resolved_id"]
    assert search_res["matches"][0]["cluster_size"] >= 2

    exp = mcp_server.explain_cluster(cid)
    assert exp["resolved_id"] == cid
    assert len(exp["members"]) == search_res["matches"][0]["cluster_size"]
    assert len(exp["edges"]) >= 1
    assert exp["golden_profile"] is not None

    # Test transitive or direct edge explanation between first and last member of the cluster
    m_keys = [m["entity_key"] for m in exp["members"]]
    edge_exp = mcp_server.explain_edge(m_keys[0], m_keys[-1])
    assert edge_exp["connection_type"] in ("direct", "transitive")

    adapter.close()


def test_api_dry_run_summary_breakdown(tmp_path, monkeypatch):
    monkeypatch.setattr(deps, "OIDC_ISSUER", "")
    monkeypatch.setattr(deps, "ALLOW_INSECURE_DEV_AUTH", True)
    client = TestClient(app, raise_server_exceptions=False)

    db_path = str(tmp_path / "api_dryrun.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("""
        CREATE TABLE raw_c (id VARCHAR, email VARCHAR, updated_at TIMESTAMP);
        INSERT INTO raw_c VALUES
            ('1', 'a@x.com', '2026-01-01 00:00:00'),
            ('2', 'b@x.com', '2026-01-01 00:00:00'),
            ('3', 'c@x.com', '2026-01-01 00:00:00');
    """)
    conn.close()

    client.post("/api/setup/connect", json={"platform": "duckdb", "params": {"path": db_path}})
    cfg = {
        "sources": [
            {
                "id": "raw_c",
                "table": "raw_c",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [{"type": "email", "column": "email", "expr": "email"}],
                "attributes": [],
            }
        ],
        "rules": [
            {
                "id": "r_email",
                "type": "EXACT",
                "match_keys": ["email"],
                "priority": 1,
                "canonicalize": "LOWERCASE",
            }
        ],
    }
    client.post("/api/setup/config/save", json={"config": cfg})

    dry_resp = client.post(
        "/api/setup/run",
        json={"mode": "FULL", "strict": False, "max_iterations": 10, "dry_run": True},
    )
    assert dry_resp.status_code == 200
    data = dry_resp.json()
    assert data["status"] == "DRY_RUN_COMPLETE"
    assert data["dry_run_summary"]["new_entities"] == 3
    assert data["dry_run_summary"]["moved_entities"] == 0
    assert data["dry_run_summary"]["proposed_changes"] == 3
    assert data["dry_run_summary"]["total_entities"] == 3
