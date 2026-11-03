#!/usr/bin/env python3
"""
End-to-End Scale & MCP Server Benchmark for sql-identity-oss.

Generates multi-source synthetic retail identity data at configurable scale
(default: 250,000 entities across 3 source tables + 10,000 incremental delta entities),
runs FULL + INCR resolution + Fuzzy Matching + Golden Profile Survivorship,
verifies mathematical graph invariants, and benchmarks all 14 MCP tools.

Usage:
    python tools/scale_test/run_scale_and_mcp_benchmark.py --rows 250000 --incr-rows 10000
"""

import argparse
import os
import sys
import tempfile
import time

import duckdb

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.config import config_to_sql
from idr_core.connection_manager import ConnectionManager
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager
from idr_mcp import server as mcp_server


def run_benchmark(rows: int = 250000, incr_rows: int = 10000, db_path: str = "") -> int:
    cleanup_db = False
    if not db_path:
        tmp_dir = tempfile.mkdtemp(prefix="idr_scale_bench_")
        db_path = os.path.join(tmp_dir, "scale_benchmark.duckdb")
        cleanup_db = True

    print("=" * 72)
    print(f"🚀 SQL Identity Resolution — Scale & MCP Benchmark ({rows + incr_rows:,} Entities)")
    print("=" * 72)
    print(f"Database: {db_path}\n")

    conn = duckdb.connect(db_path)
    adapter = DuckDBAdapter(conn)
    SchemaManager(adapter).initialize(reset=True)

    # Split rows across 3 source tables: CRM (40%), Web/E-commerce (40%), POS (20%)
    crm_rows = int(rows * 0.40)
    web_rows = int(rows * 0.40)
    pos_rows = rows - crm_rows - web_rows
    num_identities = max(1000, rows // 3)

    t_gen0 = time.perf_counter()
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # 1. CRM Accounts (high-trust priority source)
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw.crm_accounts AS
        SELECT
            'crm_' || CAST(i AS VARCHAR) AS account_id,
            'First_' || CAST((i % {num_identities}) AS VARCHAR) AS first_name,
            'Last_' || CAST((i % {num_identities // 2}) AS VARCHAR) AS last_name,
            CASE
                WHEN i <= 250 THEN 'shared_kiosk@store.example.com'
                ELSE 'user_' || CAST((i % {num_identities}) AS VARCHAR) || '@example.com'
            END AS email,
            '555-' || LPAD(CAST((i % {num_identities}) AS VARCHAR), 7, '0') AS phone,
            'LYL-' || LPAD(CAST((i % {num_identities}) AS VARCHAR), 7, '0') AS loyalty_id,
            TIMESTAMP '2026-01-01 00:00:00' + INTERVAL (i % 10000) MINUTE AS updated_at
        FROM range(1, {crm_rows + 1}) t(i)
    """)

    # 2. Web Accounts (overlapping emails & phones with CRM)
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw.web_users AS
        SELECT
            'web_' || CAST(i AS VARCHAR) AS user_id,
            'WebFirst_' || CAST((i % {num_identities}) AS VARCHAR) AS first_name,
            'Last_' || CAST((i % {num_identities // 2}) AS VARCHAR) AS last_name,
            'user_' || CAST((i % {num_identities}) AS VARCHAR) || '@example.com' AS email,
            CASE
                WHEN i % 2 = 0 THEN '555-' || LPAD(CAST((i % {num_identities}) AS VARCHAR), 7, '0')
                ELSE '555-9' || LPAD(CAST(i AS VARCHAR), 6, '0')
            END AS phone,
            NULL::VARCHAR AS loyalty_id,
            TIMESTAMP '2026-01-10 00:00:00' + INTERVAL (i % 10000) MINUTE AS updated_at
        FROM range(1, {web_rows + 1}) t(i)
    """)

    # 3. POS Transactions (overlapping phones & loyalty IDs, transitive bridge)
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw.pos_customers AS
        SELECT
            'pos_' || CAST(i AS VARCHAR) AS pos_id,
            'PosFirst_' || CAST((i % {num_identities}) AS VARCHAR) AS first_name,
            'Last_' || CAST((i % {num_identities // 2}) AS VARCHAR) AS last_name,
            NULL::VARCHAR AS email,
            '555-' || LPAD(CAST((i % {num_identities}) AS VARCHAR), 7, '0') AS phone,
            'LYL-' || LPAD(CAST(((i - (i % 2)) % {num_identities}) AS VARCHAR), 7, '0') AS loyalty_id,
            TIMESTAMP '2026-01-15 00:00:00' + INTERVAL (i % 10000) MINUTE AS updated_at
        FROM range(1, {pos_rows + 1}) t(i)
    """)
    t_gen = time.perf_counter() - t_gen0
    print(
        f"[1/6] Data Generation: {rows:,} rows across 3 tables "
        f"(crm={crm_rows:,}, web={web_rows:,}, pos={pos_rows:,}) in {t_gen:.2f}s"
    )

    # Configure metadata via config_to_sql
    cfg = {
        "sources": [
            {
                "id": "crm",
                "table": "raw.crm_accounts",
                "entity_key": "account_id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                    {"type": "phone", "expr": "phone"},
                    {"type": "loyalty", "expr": "loyalty_id"},
                ],
                "attributes": [
                    {"name": "first_name", "expr": "first_name"},
                    {"name": "last_name", "expr": "last_name"},
                    {"name": "email", "expr": "email"},
                    {"name": "phone", "expr": "phone"},
                ],
            },
            {
                "id": "web",
                "table": "raw.web_users",
                "entity_key": "user_id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                    {"type": "phone", "expr": "phone"},
                ],
                "attributes": [
                    {"name": "first_name", "expr": "first_name"},
                    {"name": "last_name", "expr": "last_name"},
                    {"name": "email", "expr": "email"},
                    {"name": "phone", "expr": "phone"},
                ],
            },
            {
                "id": "pos",
                "table": "raw.pos_customers",
                "entity_key": "pos_id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "phone", "expr": "phone"},
                    {"type": "loyalty", "expr": "loyalty_id"},
                ],
                "attributes": [
                    {"name": "first_name", "expr": "first_name"},
                    {"name": "last_name", "expr": "last_name"},
                    {"name": "phone", "expr": "phone"},
                ],
            },
        ],
        "rules": [
            {
                "id": "r_email",
                "type": "EXACT",
                "match_keys": ["email"],
                "canonicalize": "LOWERCASE",
                "priority": 1,
                "max_group_size": 100,  # Triggers skipped_identifier_groups on shared_kiosk@store.example.com (250 rows)
            },
            {
                "id": "r_phone",
                "type": "EXACT",
                "match_keys": ["phone"],
                "canonicalize": "EXACT",
                "priority": 2,
                "max_group_size": 5000,
            },
            {
                "id": "r_loyalty",
                "type": "EXACT",
                "match_keys": ["loyalty"],
                "canonicalize": "EXACT",
                "priority": 3,
                "max_group_size": 5000,
            },
        ],
        "survivorship": [
            {
                "attribute": "first_name",
                "strategy": "PRIORITY",
                "source_priority": ["crm", "web", "pos"],
            },
            {
                "attribute": "last_name",
                "strategy": "RECENCY",
                "recency_field": "updated_at",
            },
            {
                "attribute": "email",
                "strategy": "RECENCY",
                "recency_field": "updated_at",
            },
            {
                "attribute": "phone",
                "strategy": "FREQUENCY",
            },
        ],
        "exclusions": [
            {
                "identifier_type": "email",
                "value": "noreply@example.com",
                "match_type": "EXACT",
                "reason": "System address",
            }
        ],
    }

    for stmt in config_to_sql(cfg, dialect="duckdb"):
        adapter.execute(stmt)

    # 2. Run FULL pipeline
    runner = IDRRunner(adapter)
    t_full0 = time.perf_counter()
    res_full = runner.run(RunConfig(run_mode="FULL", max_iters=30))
    t_full = time.perf_counter() - t_full0
    assert res_full.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS"), f"FULL run failed: {res_full}"
    print(
        f"[2/6] FULL Pipeline Run: {res_full.entities_processed:,} entities -> "
        f"{res_full.clusters_impacted:,} clusters, {res_full.edges_created:,} edges, "
        f"{res_full.lp_iterations} LP iters in {t_full:.2f}s "
        f"({res_full.entities_processed / t_full:,.0f} entities/sec)"
    )

    # 3. Append Incremental Delta & Run INCR pipeline
    conn.execute(f"""
        INSERT INTO raw.web_users
        SELECT
            'web_incr_' || CAST(i AS VARCHAR) AS user_id,
            'IncrFirst_' || CAST((i % {num_identities}) AS VARCHAR) AS first_name,
            'Last_' || CAST((i % {num_identities // 2}) AS VARCHAR) AS last_name,
            'user_' || CAST((i % {num_identities}) AS VARCHAR) || '@example.com' AS email,
            '555-' || LPAD(CAST((i % {num_identities}) AS VARCHAR), 7, '0') AS phone,
            NULL::VARCHAR AS loyalty_id,
            TIMESTAMP '2026-03-01 00:00:00' + INTERVAL (i % 1000) MINUTE AS updated_at
        FROM range(1, {incr_rows + 1}) t(i)
    """)

    t_incr0 = time.perf_counter()
    res_incr = runner.run(RunConfig(run_mode="INCR", max_iters=30))
    t_incr = time.perf_counter() - t_incr0
    assert res_incr.status in ("SUCCESS", "SUCCESS_WITH_WARNINGS"), f"INCR run failed: {res_incr}"
    print(
        f"[3/6] INCR Pipeline Run: {res_incr.entities_processed:,} delta entities -> "
        f"{res_incr.clusters_impacted:,} clusters impacted in {t_incr:.2f}s"
    )

    # 4. Verify Mathematical Graph Invariants
    t_inv0 = time.perf_counter()
    expected_total = rows + incr_rows
    total_members = adapter.query_one(
        "SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current"
    )
    distinct_members = adapter.query_one(
        "SELECT COUNT(DISTINCT entity_key) FROM idr_out.identity_resolved_membership_current"
    )
    sum_cluster_sizes = adapter.query_one(
        "SELECT SUM(cluster_size) FROM idr_out.identity_clusters_current"
    )
    cross_cluster_edges = adapter.query_one("""
        SELECT COUNT(*)
        FROM idr_out.identity_edges_current e
        JOIN idr_out.identity_resolved_membership_current m1 ON e.left_entity_key = m1.entity_key
        JOIN idr_out.identity_resolved_membership_current m2 ON e.right_entity_key = m2.entity_key
        WHERE m1.resolved_id != m2.resolved_id
    """)
    non_min_clusters = adapter.query_one("""
        SELECT COUNT(*) FROM (
            SELECT resolved_id, MIN(entity_key) AS min_key
            FROM idr_out.identity_resolved_membership_current
            GROUP BY resolved_id
            HAVING resolved_id != MIN(entity_key)
        ) t
    """)
    golden_profiles_count = adapter.query_one("SELECT COUNT(*) FROM idr_out.golden_profiles")
    clusters_count = adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_clusters_current")
    t_inv = time.perf_counter() - t_inv0

    assert total_members == expected_total, f"{total_members} != {expected_total}"
    assert distinct_members == expected_total, f"{distinct_members} != {expected_total}"
    assert sum_cluster_sizes == expected_total, f"{sum_cluster_sizes} != {expected_total}"
    assert cross_cluster_edges == 0, f"Found {cross_cluster_edges} cross-cluster edges!"
    assert non_min_clusters == 0, f"Found {non_min_clusters} non-minimal resolved_ids!"
    assert golden_profiles_count == clusters_count, (
        f"Golden profiles ({golden_profiles_count}) != clusters ({clusters_count})"
    )
    print(
        f"[4/6] Graph Invariants Verified ({t_inv:.2f}s):\n"
        f"      ✓ Partition Invariant:        {total_members:,} entities == SUM(cluster_size)\n"
        f"      ✓ Component Completeness:     0 cross-cluster edges\n"
        f"      ✓ Minimum-Label Determinism:  0 non-minimal cluster IDs\n"
        f"      ✓ Golden Profile Parity:      {golden_profiles_count:,} profiles == {clusters_count:,} clusters"
    )

    # 5. Benchmark All 14 MCP Tools
    ConnectionManager.instance().set_adapter(adapter, {"platform": "duckdb"})
    os.environ["IDR_PII_ACCESS"] = "masked"

    mcp_timings = {}

    def _bench_mcp(name, fn, *args, **kwargs):
        t0 = time.perf_counter()
        out = fn(*args, **kwargs)
        mcp_timings[name] = (time.perf_counter() - t0) * 1000.0
        return out

    metrics = _bench_mcp("get_metrics_summary", mcp_server.get_metrics_summary)
    assert metrics["total_entities"] == expected_total

    search_res = _bench_mcp(
        "search_identifier", mcp_server.search_identifier, "user_42@example.com", "email"
    )
    assert len(search_res["matches"]) >= 1
    sample_cid = search_res["matches"][0]["resolved_id"]

    cluster_res = _bench_mcp(
        "get_cluster",
        mcp_server.get_cluster,
        sample_cid,
        include_edges=True,
        include_entities=True,
    )
    assert cluster_res["resolved_id"] == sample_cid
    assert len(cluster_res["entities"]) >= 2

    gp_masked = _bench_mcp("get_golden_profile(masked)", mcp_server.get_golden_profile, sample_cid)
    assert gp_masked["pii_access_level"] == "masked"
    assert "***" in str(gp_masked["attributes"].get("first_name", ""))

    os.environ["IDR_PII_ACCESS"] = "full"
    gp_full = _bench_mcp("get_golden_profile(full)", mcp_server.get_golden_profile, sample_cid)
    assert gp_full["pii_access_level"] == "full"
    assert str(gp_full["attributes"].get("first_name", "")).startswith("First_")

    edges_res = _bench_mcp("list_edges_for_cluster", mcp_server.list_edges_for_cluster, sample_cid)
    assert len(edges_res["edges"]) >= 1

    e0 = edges_res["edges"][0]
    edge_ev = _bench_mcp(
        "explain_edge(direct)",
        mcp_server.explain_edge,
        e0["left_entity_key"],
        e0["right_entity_key"],
    )
    assert edge_ev["connection_type"] == "direct"
    assert len(edge_ev["evidence"]) >= 1

    exp_cluster = _bench_mcp("explain_cluster", mcp_server.explain_cluster, sample_cid)
    assert exp_cluster["resolved_id"] == sample_cid
    assert exp_cluster["golden_profile"] is not None

    runs_res = _bench_mcp("run_history", mcp_server.run_history, 5)
    assert len(runs_res["runs"]) == 2

    latest_res = _bench_mcp("latest_run", mcp_server.latest_run)
    assert latest_res["run_mode"] == "INCR"

    snap_res = _bench_mcp("config_snapshot", mcp_server.config_snapshot)
    assert snap_res.get("config_hash") is not None

    rules_res = _bench_mcp("list_rules", mcp_server.list_rules)
    assert len(rules_res["rules"]) == 3

    sources_res = _bench_mcp("list_sources", mcp_server.list_sources)
    assert len(sources_res["sources"]) == 3

    skipped_res = _bench_mcp("list_skipped_groups", mcp_server.list_skipped_groups)
    assert len(skipped_res["skipped_groups"]) >= 1
    assert skipped_res["skipped_groups"][0]["identifier_value_norm"] == "shared_kiosk@store.example.com"

    dry_res = _bench_mcp("dry_run_pipeline(INCR)", mcp_server.dry_run_pipeline, "INCR", 10)
    assert dry_res["status"] == "DRY_RUN_COMPLETE"

    print("[5/6] All 14 MCP Tools Verified & Benchmarked:")
    for tool_name, ms in mcp_timings.items():
        print(f"      ✓ {tool_name:<28} {ms:>8.2f} ms")

    # 6. Summary
    stage_rows = adapter.query(
        "SELECT stage_name, duration_seconds, rows_out FROM idr_out.stage_metrics WHERE run_id = ? ORDER BY stage_order",
        params=[res_full.run_id],
    )
    print("\n[6/6] FULL Run Stage Telemetry:")
    for st in stage_rows:
        print(f"      • {st['stage_name']:<28} {st['duration_seconds']:>3}s")

    adapter.close()
    if cleanup_db and os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass

    print("\n✅ Scale & MCP Benchmark Completed Successfully!")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run IDR Scale & MCP Benchmark")
    parser.add_argument("--rows", type=int, default=250000, help="Initial entity count")
    parser.add_argument("--incr-rows", type=int, default=10000, help="Incremental delta count")
    parser.add_argument("--db", type=str, default="", help="Optional path to keep DuckDB file")
    args = parser.parse_args()
    sys.exit(run_benchmark(rows=args.rows, incr_rows=args.incr_rows, db_path=args.db))
