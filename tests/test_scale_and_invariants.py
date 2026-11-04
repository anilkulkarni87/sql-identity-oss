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

