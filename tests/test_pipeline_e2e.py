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
