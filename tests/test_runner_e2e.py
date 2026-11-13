"""Consolidated test suite: test_new_runner.py, test_pipeline_e2e.py, test_pipeline_deep_scenarios.py."""

# --- Consolidated from test_new_runner.py ---
#!/usr/bin/env python3
"""
Test script for the new idr_core package.

This script tests the unified runner against existing test data.
Run after DDL migration and metadata setup.

Usage:
    python test_new_runner.py --db=path/to/idr.duckdb --mode=FULL
    python test_new_runner.py --db=path/to/idr.duckdb --mode=FULL --dry-run
"""

import argparse
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from idr_core import IDRRunner
from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.runner import RunConfig


def main():
    parser = argparse.ArgumentParser(description="Test new IDR runner with DuckDB")
    parser.add_argument("--db", required=True, help="DuckDB database path")
    parser.add_argument("--mode", choices=["FULL", "INCR"], default="FULL")
    parser.add_argument("--max-iters", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"\n{'=' * 60}")
    print("  IDR Core Package Test")
    print(f"{'=' * 60}")
    print(f"  Database: {args.db}")
    print(f"  Mode:     {args.mode}")
    print(f"  Dry Run:  {args.dry_run}")
    print(f"{'=' * 60}\n")

    # Create adapter
    adapter = DuckDBAdapter(args.db)

    try:
        # Pre-flight checks
        print("Step 1: Validating environment...")

        # Check schemas exist
        schemas = adapter.query("SELECT schema_name FROM information_schema.schemata")
        schema_names = [s["schema_name"] for s in schemas]
        for required in ["idr_meta", "idr_work", "idr_out"]:
            if required not in schema_names:
                print(f"  ❌ Missing schema: {required}")
                return 1
            print(f"  ✅ Schema {required} exists")

        # Check source tables
        sources = adapter.query(
            "SELECT table_id, table_fqn FROM idr_meta.source_table WHERE is_active = TRUE"
        )
        print(f"\n  Found {len(sources)} active source tables:")
        for src in sources:
            exists = adapter.table_exists(src["table_fqn"])
            status = "✅" if exists else "❌"
            count = adapter.query_one(f"SELECT COUNT(*) FROM {src['table_fqn']}") if exists else 0
            print(f"    {status} {src['table_id']}: {src['table_fqn']} ({count:,} rows)")

        # Check rules
        rules = adapter.query(
            "SELECT rule_id, identifier_type FROM idr_meta.rule WHERE is_active = TRUE"
        )
        print(f"\n  Found {len(rules)} active rules:")
        for r in rules:
            print(f"    - {r['rule_id']}: {r['identifier_type']}")

        # Run IDR
        print(f"\nStep 2: Running IDR ({args.mode} mode)...")

        runner = IDRRunner(adapter)
        config = RunConfig(run_mode=args.mode, max_iters=args.max_iters, dry_run=args.dry_run)

        result = runner.run(config)

        # Print results
        print(f"\n{'=' * 60}")
        print(f"  Result: {result.status}")
        print(f"{'=' * 60}")
        print(f"  Run ID:          {result.run_id}")
        print(f"  Entities:        {result.entities_processed:,}")
        print(f"  Identifiers:     {result.identifiers_extracted:,}")
        print(f"  Edges:           {result.edges_created:,}")
        print(f"  Clusters:        {result.clusters_impacted:,}")
        print(f"  LP Iterations:   {result.lp_iterations}")
        print(f"  Duration:        {result.duration_seconds:.1f}s")

        if result.warnings:
            print("\n  Warnings:")
            for w in result.warnings:
                print(f"    ⚠️  {w}")

        if result.error:
            print(f"\n  ❌ Error: {result.error}")
            return 1

        # Verify output
        if not args.dry_run and result.status == "SUCCESS":
            print("\nStep 3: Verifying output tables...")

            membership = adapter.query_one(
                "SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current"
            )
            clusters = adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_clusters_current")
            edges = adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_edges_current")

            print(f"  Memberships:  {membership:,}")
            print(f"  Clusters:     {clusters:,}")
            print(f"  Edges:        {edges:,}")

            # Sample clusters
            sample = adapter.query("""
                SELECT resolved_id, cluster_size
                FROM idr_out.identity_clusters_current
                ORDER BY cluster_size DESC
                LIMIT 5
            """)
            print("\n  Top 5 clusters by size:")
            for s in sample:
                print(f"    - {s['resolved_id'][:30]}... ({s['cluster_size']} members)")

        print(f"\n{'=' * 60}")
        print("  ✅ Test complete!")
        print(f"{'=' * 60}\n")

        return 0 if "SUCCESS" in result.status or result.status == "DRY_RUN_COMPLETE" else 1

    except Exception as e:
        print(f"\n  ❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    finally:
        adapter.close()


if __name__ == "__main__":
    sys.exit(main())


# --- Consolidated from test_pipeline_e2e.py ---
"""
End-to-End integration test suite for SQL Identity Resolution pipeline.

Tests:
1. Full run (FULL mode) end-to-end pipeline execution.
2. Incremental run (INCR mode) with candidate expansion and cluster merging.
3. Whitespace, empty string, and sentinel NULL filtering (prevents mega-clusters).
4. PII hashing (IDR_HASH_PII=1) and deterministic matching on hashed identifiers.
5. Golden profiles survivorship and persistent identifier store verification.
"""

import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import pytest

from idr_core import IDRRunner
from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.runner import RunConfig
from idr_core.schema_manager import SchemaManager


@pytest.fixture
def idr_env(tmp_path):
    """Set up an isolated DuckDB environment with schema and metadata."""
    db_path = str(tmp_path / "idr_test.duckdb")
    adapter = DuckDBAdapter(db_path)
    sm = SchemaManager(adapter)
    sm.initialize(reset=True)

    # Create two source tables: CRM and WEB
    adapter.execute("""
        CREATE TABLE raw_crm (
            customer_id VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            updated_at TIMESTAMP
        );

        CREATE TABLE raw_web (
            session_id VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            updated_at TIMESTAMP
        );
    """)

    # Seed metadata
    adapter.execute("""
        INSERT INTO idr_meta.source_table
        (table_id, table_fqn, entity_type, entity_key_expr, watermark_column, watermark_lookback_minutes, is_active)
        VALUES
        ('crm', 'raw_crm', 'INDIVIDUAL', 'customer_id', 'updated_at', 0, TRUE),
        ('web', 'raw_web', 'INDIVIDUAL', 'session_id', 'updated_at', 0, TRUE);

        INSERT INTO idr_meta.rule
        (rule_id, identifier_type, canonicalize, max_group_size, priority, is_active)
        VALUES
        ('email_rule', 'email', 'LOWERCASE', 1000, 1, TRUE),
        ('phone_rule', 'phone', 'NONE', 1000, 2, TRUE);

        INSERT INTO idr_meta.identifier_mapping
        (table_id, identifier_type, identifier_value_expr, is_hashed)
        VALUES
        ('crm', 'email', 'email', FALSE),
        ('crm', 'phone', 'phone', FALSE),
        ('web', 'email', 'email', FALSE),
        ('web', 'phone', 'phone', FALSE);

        INSERT INTO idr_meta.entity_attribute_mapping
        (table_id, attribute_name, attribute_expr)
        VALUES
        ('crm', 'first_name', 'first_name'),
        ('crm', 'last_name', 'last_name'),
        ('web', 'first_name', 'first_name'),
        ('web', 'last_name', 'last_name');

        INSERT INTO idr_meta.survivorship_rule
        (attribute_name, strategy, source_priority_list, recency_field, is_active)
        VALUES
        ('first_name', 'RECENCY', NULL, 'updated_at', TRUE),
        ('last_name', 'PRIORITY', 'crm,web', NULL, TRUE);
    """)

    return adapter


def test_full_pipeline_run(idr_env):
    """Test full IDR run produces golden_profiles, current edges, and identifiers."""
    adapter = idr_env

    # Insert test data:
    # Cluster 1: Alice (crm_1 and web_1 share email alice@example.com)
    # Cluster 2: Bob (crm_2, singleton)
    adapter.execute("""
        INSERT INTO raw_crm VALUES
        ('crm_1', 'Alice', 'Smith', 'alice@example.com', '111-222-3333', '2026-01-01 10:00:00'),
        ('crm_2', 'Bob', 'Jones', 'bob@example.com', '444-555-6666', '2026-01-01 11:00:00');

        INSERT INTO raw_web VALUES
        ('web_1', 'Alyce', 'Smith', 'Alice@Example.COM', NULL, '2026-01-02 12:00:00');
    """)

    runner = IDRRunner(adapter)
    result = runner.run(RunConfig(run_mode="FULL"))

    assert result.status == "SUCCESS"
    assert result.entities_processed == 3
    assert result.edges_created >= 1

    # 1. Verify golden_profiles exists and is populated
    assert adapter.table_exists("idr_out.golden_profiles")
    profiles = adapter.query("SELECT * FROM idr_out.golden_profiles ORDER BY resolved_id")
    # There should be 2 golden profiles (Alice cluster + Bob singleton)
    assert len(profiles) == 2

    # Verify recency survivorship on Alice: web_1 is more recent ('2026-01-02' > '2026-01-01')
    alice_profile = [p for p in profiles if p["last_name"] == "Smith"][0]
    assert alice_profile["first_name"] == "Alyce"

    # 2. Verify identity_identifiers_current exists and has identifiers
    assert adapter.table_exists("idr_out.identity_identifiers_current")
    id_count = adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_identifiers_current")
    assert id_count >= 3

    # 3. Verify identity_edges_current exists
    assert adapter.table_exists("idr_out.identity_edges_current")
    edge_count = adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_edges_current")
    assert edge_count >= 1

    # 4. Verify cluster confidence is computed
    clusters = adapter.query("SELECT * FROM idr_out.identity_clusters_current")
    assert len(clusters) == 2
    for c in clusters:
        assert c["confidence_score"] is not None
        assert 0.0 <= c["confidence_score"] <= 1.0


def test_incremental_candidate_expansion_and_merging(idr_env):
    """Test incremental mode correctly pulls candidates and merges with historical clusters."""
    adapter = idr_env

    # Step 1: Initial full run
    adapter.execute("""
        INSERT INTO raw_crm VALUES
        ('crm_1', 'Alice', 'Smith', 'alice@example.com', '111-222-3333', '2026-01-01 10:00:00');

        INSERT INTO raw_web VALUES
        ('web_1', 'Alice', 'Smith', 'alice@example.com', NULL, '2026-01-01 10:30:00');
    """)

    runner = IDRRunner(adapter)
    res_full = runner.run(RunConfig(run_mode="FULL"))
    assert res_full.status == "SUCCESS"

    # Get initial resolved_id for Alice
    initial_alice_resolved = adapter.query_one(
        "SELECT resolved_id FROM idr_out.identity_resolved_membership_current WHERE entity_key = 'crm:crm_1'"
    )
    assert initial_alice_resolved is not None

    # Step 2: Insert new record into raw_crm that matches Alice via phone number
    # Timestamp is later than the initial watermark
    adapter.execute("""
        INSERT INTO raw_crm VALUES
        ('crm_3', 'Alice', 'Smith-Brown', 'new_alice@work.com', '111-222-3333', '2026-01-05 09:00:00');
    """)

    # Step 3: Run incremental mode
    res_incr = runner.run(RunConfig(run_mode="INCR"))
    assert res_incr.status == "SUCCESS"

    # Verify that crm_3 merged into Alice's cluster via phone number match with crm_1!
    new_membership = adapter.query(
        "SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current"
    )
    membership_map = {row["entity_key"]: row["resolved_id"] for row in new_membership}

    assert "crm:crm_3" in membership_map
    assert "crm:crm_1" in membership_map
    assert "web:web_1" in membership_map
    # All 3 records must share the exact same resolved_id
    assert membership_map["crm:crm_3"] == membership_map["crm:crm_1"]
    assert membership_map["web:web_1"] == membership_map["crm:crm_1"]

    # Verify no phantom clusters remain in identity_clusters_current
    cluster_rows = adapter.query("SELECT resolved_id FROM idr_out.identity_clusters_current")
    assert len(cluster_rows) == 1
    assert cluster_rows[0]["resolved_id"] == membership_map["crm:crm_1"]


def test_skipped_identifier_groups_logged_when_exceeding_max_group_size(idr_env):
    """Test that oversized identifier groups are skipped and logged to idr_out.skipped_identifier_groups."""
    adapter = idr_env

    # Set max_group_size = 2 for email_rule
    adapter.execute("UPDATE idr_meta.rule SET max_group_size = 2 WHERE rule_id = 'email_rule'")

    # Insert 3 records sharing the same email (exceeds max_group_size=2)
    adapter.execute("""
        INSERT INTO raw_crm VALUES
        ('crm_1', 'A', 'User', 'shared@company.com', '111', '2026-01-01 10:00:00'),
        ('crm_2', 'B', 'User', 'shared@company.com', '222', '2026-01-01 10:00:00'),
        ('crm_3', 'C', 'User', 'shared@company.com', '333', '2026-01-01 10:00:00');
    """)

    runner = IDRRunner(adapter)
    result = runner.run(RunConfig(run_mode="FULL"))
    assert result.status == "SUCCESS"
    assert result.edges_created == 0

    skipped = adapter.query("SELECT * FROM idr_out.skipped_identifier_groups")
    assert len(skipped) == 1
    assert skipped[0]["identifier_value_norm"] == "shared@company.com"
    assert skipped[0]["group_size"] == 3
    assert skipped[0]["max_allowed"] == 2


def test_empty_and_sentinel_null_filtering(idr_env):
    """Test that empty strings, whitespace, and sentinel nulls do not create false clusters."""
    adapter = idr_env

    # Insert records with empty string, whitespace, and 'null' values
    adapter.execute("""
        INSERT INTO raw_crm VALUES
        ('crm_null1', 'NoEmail1', 'Test', '', '   ', '2026-01-01 10:00:00'),
        ('crm_null2', 'NoEmail2', 'Test', 'null', 'NONE', '2026-01-01 10:00:00'),
        ('crm_null3', 'NoEmail3', 'Test', '   ', 'N/A', '2026-01-01 10:00:00');
    """)

    runner = IDRRunner(adapter)
    result = runner.run(RunConfig(run_mode="FULL"))
    assert result.status == "SUCCESS"

    # Ensure NO edges were created between these sentinel records
    edge_count = adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_edges_current")
    assert edge_count == 0

    # Ensure 3 distinct singleton clusters were created
    cluster_count = adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_clusters_current")
    assert cluster_count == 3


def test_pii_hashing_mode(idr_env, monkeypatch):
    """Test that IDR_HASH_PII=1 hashes plaintext identifiers while preserving matching."""
    adapter = idr_env
    monkeypatch.setenv("IDR_HASH_PII", "1")

    adapter.execute("""
        INSERT INTO raw_crm VALUES
        ('crm_1', 'Charlie', 'Day', 'charlie@paddys.com', '555-0101', '2026-01-01 10:00:00');

        INSERT INTO raw_web VALUES
        ('web_1', 'Charles', 'Day', 'charlie@paddys.com', '555-0101', '2026-01-01 10:00:00');
    """)

    runner = IDRRunner(adapter)
    result = runner.run(RunConfig(run_mode="FULL"))
    assert result.status == "SUCCESS"

    # Verify that stored identifiers in idr_out.identity_identifiers_current are SHA-256 hashes
    identifiers = adapter.query(
        "SELECT identifier_value_norm, is_hashed FROM idr_out.identity_identifiers_current"
    )
    assert len(identifiers) >= 2
    for id_row in identifiers:
        val = id_row["identifier_value_norm"]
        # Raw value 'charlie@paddys.com' should NOT appear in plaintext
        assert "charlie@paddys.com" not in val
        # Must be 64-char hex SHA-256
        assert len(val) == 64
        assert id_row["is_hashed"] is True

    # Verify that Charlie Day was still successfully merged into a single cluster!
    clusters = adapter.query("SELECT COUNT(*) as cnt FROM idr_out.identity_clusters_current")
    assert clusters[0]["cnt"] == 1


# --- Consolidated from test_pipeline_deep_scenarios.py ---
"""
Deep E2E scenario tests for SQL Identity Resolution pipeline:
1. Dry-run mode (NEW, MOVED, UNCHANGED diff calculation & non-mutation of current tables)
2. Strict mode (deterministic-only bypass of fuzzy rules vs full fuzzy super-clustering)
3. 4-hop transitive graph resolution across 3 source tables and 4 identifier types
4. Identifier exclusions (EXACT and LIKE wildcard patterns)
5. Preflight concurrency lock & automatic stale RUNNING recovery (>4h)
"""

import pytest

from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager


@pytest.fixture
def duck_env(tmp_path):
    db_path = str(tmp_path / "deep_scenarios.duckdb")
    adapter = DuckDBAdapter(db_path)
    SchemaManager(adapter).initialize(reset=True)
    yield adapter
    adapter.close()


def test_dry_run_mode_computes_diffs_without_mutating_production_tables(duck_env):
    adapter = duck_env
    adapter.execute("""
        CREATE TABLE raw_crm (
            id VARCHAR, email VARCHAR, phone VARCHAR, updated_at TIMESTAMP
        );
        INSERT INTO raw_crm VALUES
            ('c1', 'alice@example.com', '111-1111', '2026-01-01 00:00:00'),
            ('c2', 'bob@example.com', '222-2222', '2026-01-01 00:00:00');

        INSERT INTO idr_meta.source_table VALUES
            ('crm', 'raw_crm', 'PERSON', 'id', 'updated_at', 0, TRUE);
        INSERT INTO idr_meta.rule VALUES
            ('r_email', 'email', 'LOWERCASE', 1000, 1, TRUE),
            ('r_phone', 'phone', 'NONE', 1000, 2, TRUE);
        INSERT INTO idr_meta.identifier_mapping VALUES
            ('crm', 'email', 'email', FALSE),
            ('crm', 'phone', 'phone', FALSE);
    """)

    runner = IDRRunner(adapter)
    baseline = runner.run(RunConfig(run_mode="FULL", dry_run=False))
    assert baseline.status == "SUCCESS"

    before_members = adapter.query(
        "SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
    )
    assert len(before_members) == 2

    # Add c3 (bridges c1 and c2 via phone='222-2222' and email='alice@example.com') and c4 (brand new)
    adapter.execute("""
        INSERT INTO raw_crm VALUES
            ('c3', 'alice@example.com', '222-2222', '2026-01-02 00:00:00'),
            ('c4', 'newuser@example.com', '999-9999', '2026-01-02 00:00:00');
    """)

    dry_res = runner.run(RunConfig(run_mode="FULL", dry_run=True))
    assert dry_res.status == "DRY_RUN_COMPLETE"

    # 1. Production membership table MUST NOT be mutated by dry run
    after_members = adapter.query(
        "SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
    )
    assert after_members == before_members

    # 2. Verify dry_run_results and dry_run_summary
    diffs = adapter.query(
        f"SELECT entity_key, change_type, current_resolved_id, new_resolved_id FROM idr_out.dry_run_results WHERE run_id = '{dry_res.run_id}'"
    )
    diff_map = {r["entity_key"]: r["change_type"] for r in diffs}
    assert diff_map["crm:c1"] == "UNCHANGED"
    assert diff_map["crm:c2"] == "MOVED"  # Merged into crm:c1's cluster!
    assert diff_map["crm:c3"] == "NEW"
    assert diff_map["crm:c4"] == "NEW"

    summary = adapter.query(
        f"SELECT * FROM idr_out.dry_run_summary WHERE run_id = '{dry_res.run_id}'"
    )
    assert len(summary) == 1
    assert summary[0]["total_entities"] == 4
    assert summary[0]["new_entities"] == 2
    assert summary[0]["moved_entities"] == 1
    assert summary[0]["unchanged_entities"] == 1


def test_strict_mode_bypasses_fuzzy_matching(duck_env):
    adapter = duck_env
    adapter.execute("""
        CREATE TABLE raw_users (
            id VARCHAR, email VARCHAR, last_name VARCHAR, first_name VARCHAR, updated_at TIMESTAMP
        );
        INSERT INTO raw_users VALUES
            ('u1', 'steven@a.com', 'miller', 'Steven', '2026-01-01 00:00:00'),
            ('u2', 'stephen@b.com', 'miller', 'Stephen', '2026-01-01 00:00:00');

        INSERT INTO idr_meta.source_table VALUES
            ('users', 'raw_users', 'PERSON', 'id', 'updated_at', 0, TRUE);
        INSERT INTO idr_meta.rule VALUES
            ('r_email', 'email', 'LOWERCASE', 1000, 1, TRUE);
        INSERT INTO idr_meta.identifier_mapping VALUES
            ('users', 'email', 'email', FALSE);
        INSERT INTO idr_meta.entity_attribute_mapping VALUES
            ('users', 'last_name', 'last_name'),
            ('users', 'first_name', 'first_name');
        INSERT INTO idr_meta.fuzzy_rule VALUES
            ('f_name', 'Name Sim', 'last_name', 'jaro_winkler_similarity(<a>.first_name, <b>.first_name)', 0.80, 1, TRUE);
    """)

    runner = IDRRunner(adapter)

    # 1. Run with strict=True -> fuzzy rule must be skipped
    res_strict = runner.run(RunConfig(run_mode="FULL", strict=True))
    assert res_strict.status == "SUCCESS"
    rows_strict = adapter.query(
        "SELECT entity_key, resolved_id, super_cluster_id FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
    )
    assert rows_strict[0]["super_cluster_id"] != rows_strict[1]["super_cluster_id"]

    # 2. Run with strict=False -> fuzzy rule merges u1 and u2 into a shared super_cluster_id
    res_fuzzy = runner.run(RunConfig(run_mode="FULL", strict=False))
    assert res_fuzzy.status == "SUCCESS"
    rows_fuzzy = adapter.query(
        "SELECT entity_key, resolved_id, super_cluster_id FROM idr_out.identity_resolved_membership_current ORDER BY entity_key"
    )
    assert rows_fuzzy[0]["super_cluster_id"] == rows_fuzzy[1]["super_cluster_id"]


def test_four_hop_transitive_chain_across_three_sources(duck_env):
    """
    Chain:
      crm:n1 --(email)--> web:n2 --(phone)--> pos:n3 --(loyalty)--> crm:n4 --(ssn)--> web:n5
    All 5 entities must converge to the lexicographically smallest entity_key ('crm:n1').
    """
    adapter = duck_env
    adapter.execute("""
        CREATE TABLE src_crm (id VARCHAR, email VARCHAR, loyalty VARCHAR, ssn VARCHAR);
        CREATE TABLE src_web (id VARCHAR, email VARCHAR, phone VARCHAR, ssn VARCHAR);
        CREATE TABLE src_pos (id VARCHAR, phone VARCHAR, loyalty VARCHAR);

        INSERT INTO src_crm VALUES
            ('n1', 'chain@example.com', NULL, NULL),
            ('n4', NULL, 'LYL-777', 'SSN-999');
        INSERT INTO src_web VALUES
            ('n2', 'chain@example.com', '555-1234', NULL),
            ('n5', NULL, NULL, 'SSN-999');
        INSERT INTO src_pos VALUES
            ('n3', '555-1234', 'LYL-777');

        INSERT INTO idr_meta.source_table VALUES
            ('crm', 'src_crm', 'PERSON', 'id', NULL, 0, TRUE),
            ('web', 'src_web', 'PERSON', 'id', NULL, 0, TRUE),
            ('pos', 'src_pos', 'PERSON', 'id', NULL, 0, TRUE);

        INSERT INTO idr_meta.rule VALUES
            ('r_email', 'email', 'LOWERCASE', 1000, 1, TRUE),
            ('r_phone', 'phone', 'NONE', 1000, 2, TRUE),
            ('r_loyalty', 'loyalty', 'UPPERCASE', 1000, 3, TRUE),
            ('r_ssn', 'ssn', 'UPPERCASE', 1000, 4, TRUE);

        INSERT INTO idr_meta.identifier_mapping VALUES
            ('crm', 'email', 'email', FALSE),
            ('crm', 'loyalty', 'loyalty', FALSE),
            ('crm', 'ssn', 'ssn', FALSE),
            ('web', 'email', 'email', FALSE),
            ('web', 'phone', 'phone', FALSE),
            ('web', 'ssn', 'ssn', FALSE),
            ('pos', 'phone', 'phone', FALSE),
            ('pos', 'loyalty', 'loyalty', FALSE);
    """)

    res = IDRRunner(adapter).run(RunConfig(run_mode="FULL", max_iters=20))
    assert res.status == "SUCCESS"
    assert res.entities_processed == 5

    members = adapter.query(
        "SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current"
    )
    assert len(members) == 5
    resolved_ids = {m["resolved_id"] for m in members}
    assert resolved_ids == {"crm:n1"}


def test_identifier_exclusions_exact_and_like_patterns(duck_env):
    adapter = duck_env
    adapter.execute("""
        CREATE TABLE raw_leads (id VARCHAR, email VARCHAR, phone VARCHAR);
        INSERT INTO raw_leads VALUES
            ('l1', 'test@test.com', '000-000-0000'),
            ('l2', 'test@test.com', '000-999-9999'),
            ('l3', 'bot1@noreply.org', '555-1111'),
            ('l4', 'bot2@noreply.org', '555-1111');

        INSERT INTO idr_meta.source_table VALUES
            ('leads', 'raw_leads', 'PERSON', 'id', NULL, 0, TRUE);
        INSERT INTO idr_meta.rule VALUES
            ('r_email', 'email', 'LOWERCASE', 1000, 1, TRUE),
            ('r_phone', 'phone', 'NONE', 1000, 2, TRUE);
        INSERT INTO idr_meta.identifier_mapping VALUES
            ('leads', 'email', 'email', FALSE),
            ('leads', 'phone', 'phone', FALSE);

        INSERT INTO idr_meta.identifier_exclusion (identifier_type, identifier_value_pattern, match_type, reason) VALUES
            ('email', 'test@test.com', 'EXACT', 'Test email'),
            ('email', '%@noreply.org', 'LIKE', 'No-reply domain'),
            ('phone', '000-%', 'LIKE', 'Invalid prefix');
    """)

    res = IDRRunner(adapter).run(RunConfig(run_mode="FULL"))
    assert res.status == "SUCCESS"

    # l1 and l2 must NOT link because test@test.com and 000-% are excluded.
    # l3 and l4 MUST link via valid phone 555-1111.
    members = {
        r["entity_key"]: r["resolved_id"]
        for r in adapter.query(
            "SELECT entity_key, resolved_id FROM idr_out.identity_resolved_membership_current"
        )
    }
    assert members["leads:l1"] != members["leads:l2"]
    assert members["leads:l3"] == members["leads:l4"]


def test_preflight_blocks_concurrent_run_and_recovers_stale_run(duck_env):
    adapter = duck_env
    adapter.execute("""
        CREATE TABLE raw_c (id VARCHAR, email VARCHAR);
        INSERT INTO raw_c VALUES ('1', 'a@b.com');
        INSERT INTO idr_meta.source_table VALUES ('c', 'raw_c', 'PERSON', 'id', NULL, 0, TRUE);
        INSERT INTO idr_meta.rule VALUES ('r_email', 'email', 'LOWERCASE', 100, 1, TRUE);
        INSERT INTO idr_meta.identifier_mapping VALUES ('c', 'email', 'email', FALSE);
    """)

    # 1. Insert active RUNNING run (started now)
    adapter.execute("""
        INSERT INTO idr_out.run_history (run_id, run_mode, status, started_at)
        VALUES ('run_active_lock', 'FULL', 'RUNNING', CURRENT_TIMESTAMP)
    """)

    runner = IDRRunner(adapter)
    blocked = runner.run(RunConfig(run_mode="FULL"))
    assert blocked.status == "FAILED"
    assert "Concurrent run detected: run_active_lock" in (blocked.error or "")

    # 2. Age run_active_lock to 5 hours ago -> Preflight marks it INTERRUPTED and succeeds
    adapter.execute("""
        UPDATE idr_out.run_history
        SET started_at = CURRENT_TIMESTAMP - INTERVAL '5 hours'
        WHERE run_id = 'run_active_lock'
    """)

    recovered = runner.run(RunConfig(run_mode="FULL"))
    assert recovered.status == "SUCCESS"

    old_status = adapter.query_one(
        "SELECT status FROM idr_out.run_history WHERE run_id = 'run_active_lock'"
    )
    assert old_status == "INTERRUPTED"
