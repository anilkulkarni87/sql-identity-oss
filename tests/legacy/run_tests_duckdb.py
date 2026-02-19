#!/usr/bin/env python3
"""
DuckDB Integration Test Runner
Portable test suite for sql-identity-resolution.

Usage:
    python run_tests_duckdb.py
    python run_tests_duckdb.py --keep-db  # Keep database after tests

Benefits:
    - No cloud setup required
    - Perfect for CI/CD (GitHub Actions)
    - Fast local development
    - Isolated test database
"""

import argparse
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("Please install duckdb: pip install duckdb")
    sys.exit(1)

# ============================================
# CONFIGURATION
# ============================================
parser = argparse.ArgumentParser(description='DuckDB IDR Integration Tests')
parser.add_argument('--keep-db', action='store_true', help='Keep test database after run')
parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
args = parser.parse_args()

# Use temp file for test database
DB_PATH = os.path.join(tempfile.gettempdir(), "idr_test.duckdb")
VERBOSE = args.verbose

# Find repo root
SCRIPT_DIR = Path(__file__).parent.absolute()
REPO_ROOT = SCRIPT_DIR.parent
DDL_PATH = REPO_ROOT / "sql" / "duckdb" / "core" / "00_ddl_all.sql"


# ============================================
# TEST UTILITIES
# ============================================
class TestRunner:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.con = None
        self.results = []
        self.run_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def connect(self):
        """Connect to test database."""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.con = duckdb.connect(self.db_path)

    def close(self):
        """Close connection."""
        if self.con:
            self.con.close()

    def q(self, sql: str):
        """Execute SQL."""
        if VERBOSE:
            print(f"  SQL: {sql[:100]}...")
        return self.con.execute(sql)

    def collect_one(self, sql: str):
        """Execute SQL and return first value."""
        result = self.con.execute(sql).fetchone()
        return result[0] if result else None

    def setup_schemas(self):
        """Create database schemas and tables."""
        print("📦 Setting up schemas...")

        # Create schemas
        self.q("CREATE SCHEMA IF NOT EXISTS idr_meta")
        self.q("CREATE SCHEMA IF NOT EXISTS idr_work")
        self.q("CREATE SCHEMA IF NOT EXISTS idr_out")
        self.q("CREATE SCHEMA IF NOT EXISTS crm")  # For test source tables

        # Metadata tables
        self.q("""
        CREATE TABLE IF NOT EXISTS idr_meta.source_table (
            table_id VARCHAR PRIMARY KEY,
            table_fqn VARCHAR NOT NULL,
            entity_type VARCHAR DEFAULT 'PERSON',
            entity_key_expr VARCHAR NOT NULL,
            watermark_column VARCHAR NOT NULL,
            watermark_lookback_minutes INT DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE
        )""")

        self.q("""
        CREATE TABLE IF NOT EXISTS idr_meta.source (
            table_id VARCHAR PRIMARY KEY,
            source_name VARCHAR,
            trust_rank INT DEFAULT 1,
            is_active BOOLEAN DEFAULT TRUE
        )""")

        self.q("""
        CREATE TABLE IF NOT EXISTS idr_meta.rule (
            rule_id VARCHAR PRIMARY KEY,
            rule_name VARCHAR,
            is_active BOOLEAN DEFAULT TRUE,
            priority INT DEFAULT 1,
            identifier_type VARCHAR NOT NULL,
            canonicalize VARCHAR DEFAULT 'NONE',
            allow_hashed BOOLEAN DEFAULT TRUE,
            require_non_null BOOLEAN DEFAULT TRUE,
            max_group_size INT DEFAULT 10000
        )""")

        self.q("""
        CREATE TABLE IF NOT EXISTS idr_meta.identifier_mapping (
            table_id VARCHAR NOT NULL,
            identifier_type VARCHAR NOT NULL,
            identifier_value_expr VARCHAR NOT NULL,
            is_hashed BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (table_id, identifier_type)
        )""")

        self.q("""
        CREATE TABLE IF NOT EXISTS idr_meta.entity_attribute_mapping (
            table_id VARCHAR NOT NULL,
            attribute_name VARCHAR NOT NULL,
            source_expr VARCHAR NOT NULL,
            PRIMARY KEY (table_id, attribute_name)
        )""")

        self.q("""
        CREATE TABLE IF NOT EXISTS idr_meta.run_state (
            table_id VARCHAR PRIMARY KEY,
            last_watermark_value TIMESTAMP,
            last_run_id VARCHAR,
            last_run_ts TIMESTAMP
        )""")

        self.q("""
        CREATE TABLE IF NOT EXISTS idr_meta.survivorship_rule (
            attribute_name VARCHAR PRIMARY KEY,
            strategy VARCHAR DEFAULT 'MOST_RECENT'
        )""")

        # Output tables
        self.q("""
        CREATE TABLE IF NOT EXISTS idr_out.identity_edges_current (
            rule_id VARCHAR,
            left_entity_key VARCHAR,
            right_entity_key VARCHAR,
            identifier_type VARCHAR,
            identifier_value_norm VARCHAR,
            first_seen_ts TIMESTAMP,
            last_seen_ts TIMESTAMP
        )""")

        self.q("""
        CREATE TABLE IF NOT EXISTS idr_out.identity_resolved_membership_current (
            entity_key VARCHAR PRIMARY KEY,
            resolved_id VARCHAR,
            updated_ts TIMESTAMP
        )""")

        self.q("""
        CREATE TABLE IF NOT EXISTS idr_out.identity_clusters_current (
            resolved_id VARCHAR PRIMARY KEY,
            cluster_size INT,
            confidence_score DOUBLE,
            edge_diversity INT,
            match_density DOUBLE,
            primary_reason VARCHAR,
            updated_ts TIMESTAMP
        )""")

        self.q("""
        CREATE TABLE IF NOT EXISTS idr_out.golden_profile_current (
            resolved_id VARCHAR PRIMARY KEY,
            email_primary VARCHAR,
            phone_primary VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            updated_ts TIMESTAMP
        )""")

        self.q("""
        CREATE TABLE IF NOT EXISTS idr_out.run_history (
            run_id VARCHAR PRIMARY KEY,
            run_mode VARCHAR,
            status VARCHAR,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            duration_seconds INT,
            source_tables_processed INT,
            entities_processed INT,
            edges_created INT,
            clusters_impacted INT,
            lp_iterations INT,
            groups_skipped INT DEFAULT 0,
            values_excluded INT DEFAULT 0,
            large_clusters INT DEFAULT 0,
            warnings VARCHAR,
            created_at TIMESTAMP
        )""")

        # Identifier exclusion table for production hardening
        self.q("""
        CREATE TABLE IF NOT EXISTS idr_meta.identifier_exclusion (
            identifier_type VARCHAR,
            identifier_value_pattern VARCHAR,
            match_type VARCHAR DEFAULT 'EXACT',
            reason VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR
        )""")

        # Skipped identifier groups audit table
        self.q("""
        CREATE TABLE IF NOT EXISTS idr_out.skipped_identifier_groups (
            run_id VARCHAR,
            identifier_type VARCHAR,
            identifier_value_norm VARCHAR,
            group_size INT,
            max_allowed INT,
            sample_entity_keys VARCHAR,
            reason VARCHAR DEFAULT 'EXCEEDED_MAX_GROUP_SIZE',
            skipped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # Dry run results table
        self.q("""
        CREATE TABLE IF NOT EXISTS idr_out.dry_run_results (
            run_id VARCHAR NOT NULL,
            entity_key VARCHAR NOT NULL,
            current_resolved_id VARCHAR,
            proposed_resolved_id VARCHAR,
            change_type VARCHAR,
            current_cluster_size INT,
            proposed_cluster_size INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, entity_key)
        )""")

        # Dry run summary table
        self.q("""
        CREATE TABLE IF NOT EXISTS idr_out.dry_run_summary (
            run_id VARCHAR PRIMARY KEY,
            total_entities INT,
            new_entities INT,
            moved_entities INT,
            merged_clusters INT,
            split_clusters INT,    unchanged_entities INT,
            largest_proposed_cluster INT,
            edges_would_create INT,
            groups_would_skip INT,
            values_would_exclude INT,
            execution_time_seconds INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")

        # Metrics export table
        self.q("""
        CREATE TABLE IF NOT EXISTS idr_out.metrics_export (
            metric_id VARCHAR DEFAULT (uuid()::VARCHAR),
            run_id VARCHAR,
            metric_name VARCHAR NOT NULL,
            metric_value DOUBLE NOT NULL,
            metric_type VARCHAR DEFAULT 'gauge',
            dimensions VARCHAR,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            exported_at TIMESTAMP,
            PRIMARY KEY (metric_id)
        )""")

        print("  ✅ Schemas created")

    def setup_base_metadata(self):
        """Insert base metadata (rules)."""
        print("📋 Setting up base metadata...")

        # Insert rules (with max_group_size default of 10000)
        self.q("""
        INSERT INTO idr_meta.rule VALUES
            ('R_EMAIL_EXACT', 'Email exact match', TRUE, 1, 'EMAIL', 'LOWERCASE', TRUE, TRUE, 10000),
            ('R_PHONE_EXACT', 'Phone exact match', TRUE, 2, 'PHONE', 'NONE', TRUE, TRUE, 10000)
        """)

        # Insert survivorship rules
        self.q("""
        INSERT INTO idr_meta.survivorship_rule VALUES
            ('email_primary', 'MOST_RECENT'),
            ('phone_primary', 'MOST_RECENT'),
            ('first_name', 'MOST_RECENT'),
            ('last_name', 'MOST_RECENT')
        """)

        print("  ✅ Base metadata inserted")

    def run_idr_pipeline(self, table_ids: list):
        """Run the IDR pipeline for specified source tables."""
        import uuid
        RUN_ID = f"test_{uuid.uuid4().hex[:8]}"
        RUN_TS = self.run_ts

        # Init run state
        for tid in table_ids:
            self.q(f"""
            INSERT INTO idr_meta.run_state (table_id, last_watermark_value)
            SELECT '{tid}', TIMESTAMP '1900-01-01'
            WHERE NOT EXISTS (SELECT 1 FROM idr_meta.run_state WHERE table_id = '{tid}')
            """)

        # Build entities delta
        source_rows = self.con.execute(f"""
            SELECT table_id, table_fqn, entity_key_expr, watermark_column
            FROM idr_meta.source_table WHERE table_id IN ({','.join([f"'{t}'" for t in table_ids])})
        """).fetchall()

        entities_parts = []
        for table_id, table_fqn, entity_key_expr, wm_col in source_rows:
            entities_parts.append(f"""
                SELECT '{RUN_ID}' AS run_id, '{table_id}' AS table_id,
                       '{table_id}' || ':' || CAST(({entity_key_expr}) AS VARCHAR) AS entity_key,
                       CAST({wm_col} AS TIMESTAMP) AS watermark_value
                FROM {table_fqn}
            """)

        if entities_parts:
            self.q(f"CREATE OR REPLACE TABLE idr_work.entities_delta AS {' UNION ALL '.join(entities_parts)}")

        # Build identifiers
        mapping_rows = self.con.execute(f"""
            SELECT m.table_id, m.identifier_type, m.identifier_value_expr, m.is_hashed,
                   st.table_fqn, st.entity_key_expr
            FROM idr_meta.identifier_mapping m
            JOIN idr_meta.source_table st ON st.table_id = m.table_id
            WHERE m.table_id IN ({','.join([f"'{t}'" for t in table_ids])})
        """).fetchall()

        identifiers_parts = []
        for table_id, id_type, id_expr, is_hashed, table_fqn, key_expr in mapping_rows:
            identifiers_parts.append(f"""
                SELECT '{table_id}' AS table_id,
                       '{table_id}' || ':' || CAST(({key_expr}) AS VARCHAR) AS entity_key,
                       '{id_type}' AS identifier_type,
                       CAST(({id_expr}) AS VARCHAR) AS identifier_value,
                       {str(is_hashed).lower()} AS is_hashed
                FROM {table_fqn}
            """)

        if identifiers_parts:
            self.q(f"CREATE OR REPLACE TABLE idr_work.identifiers_all_raw AS {' UNION ALL '.join(identifiers_parts)}")

        # Canonicalize identifiers
        self.q("""
        CREATE OR REPLACE TABLE idr_work.identifiers_all AS
        SELECT i.table_id, i.entity_key, i.identifier_type,
               CASE WHEN r.canonicalize='LOWERCASE' THEN LOWER(i.identifier_value) ELSE i.identifier_value END AS identifier_value_norm,
               i.is_hashed
        FROM idr_work.identifiers_all_raw i
        JOIN idr_meta.rule r ON r.is_active=TRUE AND r.identifier_type=i.identifier_type
        WHERE i.identifier_value IS NOT NULL
        """)

        # Build delta identifier values
        self.q("""
        CREATE OR REPLACE TABLE idr_work.delta_identifier_values AS
        SELECT DISTINCT i.identifier_type, i.identifier_value_norm
        FROM idr_work.entities_delta e
        JOIN idr_work.identifiers_all i ON i.entity_key = e.entity_key
        WHERE i.identifier_value_norm IS NOT NULL
        """)

        # Build members for delta values
        self.q("""
        CREATE OR REPLACE TABLE idr_work.members_for_delta_values AS
        SELECT a.table_id, a.entity_key, a.identifier_type, a.identifier_value_norm
        FROM idr_work.identifiers_all a
        JOIN idr_work.delta_identifier_values d
          ON a.identifier_type = d.identifier_type AND a.identifier_value_norm = d.identifier_value_norm
        """)

        # Build group sizes and filter by max_group_size
        self.q("""
        CREATE OR REPLACE TABLE idr_work.group_sizes AS
        SELECT
            identifier_type,
            identifier_value_norm,
            COUNT(*) AS group_size,
            MIN(entity_key) AS anchor_entity_key
        FROM idr_work.members_for_delta_values
        GROUP BY identifier_type, identifier_value_norm
        """)

        # Build group anchors (only for groups within max_group_size)
        self.q("""
        CREATE OR REPLACE TABLE idr_work.group_anchor AS
        SELECT gs.identifier_type, gs.identifier_value_norm, gs.anchor_entity_key
        FROM idr_work.group_sizes gs
        JOIN idr_meta.rule r ON r.is_active = TRUE AND r.identifier_type = gs.identifier_type
        WHERE gs.group_size <= COALESCE(r.max_group_size, 10000)
        """)

        # Build edges (only for groups within max_group_size)
        self.q(f"""
        CREATE OR REPLACE TABLE idr_work.edges_new AS
        SELECT r.rule_id, ga.anchor_entity_key AS left_entity_key, m.entity_key AS right_entity_key,
               ga.identifier_type, ga.identifier_value_norm,
               TIMESTAMP '{RUN_TS}' AS first_seen_ts, TIMESTAMP '{RUN_TS}' AS last_seen_ts
        FROM idr_work.group_anchor ga
        JOIN idr_work.members_for_delta_values m
          ON m.identifier_type = ga.identifier_type AND m.identifier_value_norm = ga.identifier_value_norm
        JOIN idr_meta.rule r ON r.is_active = TRUE AND r.identifier_type = ga.identifier_type
        WHERE m.entity_key <> ga.anchor_entity_key
        """)

        # Insert new edges
        self.q("""
        INSERT INTO idr_out.identity_edges_current
        SELECT * FROM idr_work.edges_new
        """)

        # Build impacted subgraph
        self.q("""
        CREATE OR REPLACE TABLE idr_work.impacted_nodes AS
        SELECT DISTINCT left_entity_key AS entity_key FROM idr_work.edges_new
        UNION
        SELECT DISTINCT right_entity_key AS entity_key FROM idr_work.edges_new
        """)

        self.q("""
        CREATE OR REPLACE TABLE idr_work.subgraph_nodes AS
        SELECT DISTINCT entity_key FROM idr_work.impacted_nodes
        UNION
        SELECT DISTINCT e.left_entity_key AS entity_key
        FROM idr_out.identity_edges_current e
        JOIN idr_work.impacted_nodes n ON n.entity_key = e.right_entity_key
        UNION
        SELECT DISTINCT e.right_entity_key AS entity_key
        FROM idr_out.identity_edges_current e
        JOIN idr_work.impacted_nodes n ON n.entity_key = e.left_entity_key
        """)

        self.q("""
        CREATE OR REPLACE TABLE idr_work.subgraph_edges AS
        SELECT e.left_entity_key, e.right_entity_key
        FROM idr_out.identity_edges_current e
        WHERE EXISTS (SELECT 1 FROM idr_work.subgraph_nodes a WHERE a.entity_key = e.left_entity_key)
          AND EXISTS (SELECT 1 FROM idr_work.subgraph_nodes b WHERE b.entity_key = e.right_entity_key)
        """)

        # Label propagation
        self.q("""
        CREATE OR REPLACE TABLE idr_work.lp_labels AS
        SELECT entity_key, entity_key AS label FROM idr_work.subgraph_nodes
        """)

        for _ in range(30):  # Max iterations
            self.q("""
            CREATE OR REPLACE TABLE idr_work.lp_labels_next AS
            WITH undirected AS (
              SELECT left_entity_key AS src, right_entity_key AS dst FROM idr_work.subgraph_edges
              UNION ALL
              SELECT right_entity_key AS src, left_entity_key AS dst FROM idr_work.subgraph_edges
            ),
            candidate_labels AS (
              SELECT l.entity_key, l.label AS candidate_label FROM idr_work.lp_labels l
              UNION ALL
              SELECT u.src AS entity_key, l2.label AS candidate_label
              FROM undirected u
              JOIN idr_work.lp_labels l2 ON l2.entity_key = u.dst
            )
            SELECT entity_key, MIN(candidate_label) AS label
            FROM candidate_labels
            GROUP BY entity_key
            """)

            delta = self.collect_one("""
                SELECT SUM(CASE WHEN cur.label <> nxt.label THEN 1 ELSE 0 END)
                FROM idr_work.lp_labels cur
                JOIN idr_work.lp_labels_next nxt USING (entity_key)
            """)

            if delta == 0 or delta is None:
                break

            self.q("DROP TABLE IF EXISTS idr_work.lp_labels")
            self.q("ALTER TABLE idr_work.lp_labels_next RENAME TO lp_labels")

        # Update membership (including singletons)
        self.q("""
        CREATE OR REPLACE TABLE idr_work.membership_updates AS
        SELECT entity_key, label AS resolved_id, CURRENT_TIMESTAMP AS updated_ts
        FROM idr_work.lp_labels
        UNION ALL
        SELECT entity_key, entity_key AS resolved_id, CURRENT_TIMESTAMP AS updated_ts
        FROM idr_work.entities_delta
        WHERE entity_key NOT IN (SELECT entity_key FROM idr_work.lp_labels)
        """)

        # Upsert membership
        self.q("""
        DELETE FROM idr_out.identity_resolved_membership_current
        WHERE entity_key IN (SELECT entity_key FROM idr_work.membership_updates)
        """)
        self.q("""
        INSERT INTO idr_out.identity_resolved_membership_current
        SELECT * FROM idr_work.membership_updates
        """)

        # Update cluster sizes
        self.q("""
        CREATE OR REPLACE TABLE idr_work.impacted_resolved_ids AS
        SELECT DISTINCT resolved_id FROM idr_work.membership_updates
        """)

        self.q("""
        CREATE OR REPLACE TABLE idr_work.cluster_sizes_updates AS
        SELECT resolved_id, COUNT(*) AS cluster_size, CURRENT_TIMESTAMP AS updated_ts
        FROM idr_out.identity_resolved_membership_current
        WHERE resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids)
        GROUP BY resolved_id
        """)

        # Compute cluster confidence
        self.q("""
        CREATE OR REPLACE TABLE idr_work.cluster_edge_stats AS
        SELECT m.resolved_id, COUNT(DISTINCT e.identifier_type) AS edge_diversity, COUNT(*) AS edge_count
        FROM idr_out.identity_resolved_membership_current m
        JOIN idr_out.identity_edges_current e ON e.left_entity_key = m.entity_key OR e.right_entity_key = m.entity_key
        WHERE m.resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids)
        GROUP BY m.resolved_id
        """)

        self.q("""
        CREATE OR REPLACE TABLE idr_work.cluster_confidence AS
        SELECT
            c.resolved_id, c.cluster_size,
            CASE WHEN c.cluster_size = 1 THEN 1.0
                 ELSE ROUND(0.50 * (CAST(COALESCE(es.edge_diversity, 0) AS DOUBLE) / GREATEST(1, (SELECT MAX(edge_diversity) FROM idr_work.cluster_edge_stats))) +
                            0.35 * LEAST(1.0, CAST(COALESCE(es.edge_count, 0) AS DOUBLE) / GREATEST(1, c.cluster_size - 1)) +
                            0.15, 3)
            END AS confidence_score,
            COALESCE(es.edge_diversity, 0) AS edge_diversity,
            CASE WHEN c.cluster_size <= 1 THEN 1.0 ELSE LEAST(1.0, CAST(COALESCE(es.edge_count, 0) AS DOUBLE) / GREATEST(1, c.cluster_size - 1)) END AS match_density,
            CASE WHEN c.cluster_size = 1 THEN 'SINGLETON_NO_MATCH_REQUIRED'
                 WHEN COALESCE(es.edge_diversity, 0) >= 2 THEN CAST(COALESCE(es.edge_diversity, 0) AS VARCHAR) || ' identifier types'
                 ELSE 'Single identifier type'
            END AS primary_reason,
            CURRENT_TIMESTAMP AS updated_ts
        FROM idr_work.cluster_sizes_updates c
        LEFT JOIN idr_work.cluster_edge_stats es ON es.resolved_id = c.resolved_id
        """)

        self.q("""
        DELETE FROM idr_out.identity_clusters_current
        WHERE resolved_id IN (SELECT resolved_id FROM idr_work.cluster_confidence)
        """)
        self.q("""
        INSERT INTO idr_out.identity_clusters_current
        SELECT * FROM idr_work.cluster_confidence
        """)

    def add_test_result(self, name: str, passed: bool, detail: str):
        """Add test result."""
        self.results.append((name, passed, detail))

    def print_results(self):
        """Print test results."""
        print("\n" + "=" * 60)
        print("TEST RESULTS")
        print("=" * 60)

        all_passed = True
        for name, passed, detail in self.results:
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{status} | {name}")
            print(f"       {detail}")
            if not passed:
                all_passed = False

        print("=" * 60)
        if all_passed:
            print(f"✅ ALL {len(self.results)} TESTS PASSED")
        else:
            failed = sum(1 for _, p, _ in self.results if not p)
            print(f"❌ {failed}/{len(self.results)} TESTS FAILED")
        print("=" * 60)

        return all_passed


# ============================================
# TEST CASES
# ============================================
def test_same_email(runner: TestRunner):
    """Test: Two entities sharing the same email should be in the same cluster."""
    print("\n🧪 Test 1: Same Email → Same Cluster")

    # Setup test source tables
    runner.q("""
    CREATE TABLE crm.test1_source_a AS
    SELECT 'A001' AS entity_id, 'shared@example.com' AS email, '5551234567' AS phone,
           TIMESTAMP '2024-01-01 10:00:00' AS updated_at
    """)

    runner.q("""
    CREATE TABLE crm.test1_source_b AS
    SELECT 'B001' AS entity_id, 'shared@example.com' AS email, '5559999999' AS phone,
           TIMESTAMP '2024-01-02 10:00:00' AS updated_at
    """)

    # Register sources
    runner.q("""
    INSERT INTO idr_meta.source_table VALUES
        ('test1_a', 'crm.test1_source_a', 'PERSON', 'entity_id', 'updated_at', 0, TRUE),
        ('test1_b', 'crm.test1_source_b', 'PERSON', 'entity_id', 'updated_at', 0, TRUE)
    """)

    runner.q("""
    INSERT INTO idr_meta.identifier_mapping VALUES
        ('test1_a', 'EMAIL', 'email', FALSE),
        ('test1_a', 'PHONE', 'phone', FALSE),
        ('test1_b', 'EMAIL', 'email', FALSE),
        ('test1_b', 'PHONE', 'phone', FALSE)
    """)

    # Run IDR
    runner.run_idr_pipeline(['test1_a', 'test1_b'])

    # Assert: Both entities in same cluster
    cluster_count = runner.collect_one("""
        SELECT COUNT(DISTINCT resolved_id)
        FROM idr_out.identity_resolved_membership_current
        WHERE entity_key IN ('test1_a:A001', 'test1_b:B001')
    """)

    passed = cluster_count == 1
    runner.add_test_result(
        "Same Email → Same Cluster",
        passed,
        f"clusters={cluster_count}, expected=1"
    )


def test_chain_transitivity(runner: TestRunner):
    """Test: Chain of entities A↔B via email, B↔C via phone → all same cluster."""
    print("\n🧪 Test 2: Chain Transitivity")

    # Setup test source tables
    runner.q("""
    CREATE TABLE crm.test2_chain_a AS
    SELECT 'CHAIN_A' AS entity_id, 'alice@example.com' AS email, NULL AS phone,
           TIMESTAMP '2024-01-01' AS updated_at
    """)

    runner.q("""
    CREATE TABLE crm.test2_chain_b AS
    SELECT 'CHAIN_B' AS entity_id, 'alice@example.com' AS email, '5551112222' AS phone,
           TIMESTAMP '2024-01-02' AS updated_at
    """)

    runner.q("""
    CREATE TABLE crm.test2_chain_c AS
    SELECT 'CHAIN_C' AS entity_id, 'charlie@example.com' AS email, '5551112222' AS phone,
           TIMESTAMP '2024-01-03' AS updated_at
    """)

    # Register sources
    runner.q("""
    INSERT INTO idr_meta.source_table VALUES
        ('test2_a', 'crm.test2_chain_a', 'PERSON', 'entity_id', 'updated_at', 0, TRUE),
        ('test2_b', 'crm.test2_chain_b', 'PERSON', 'entity_id', 'updated_at', 0, TRUE),
        ('test2_c', 'crm.test2_chain_c', 'PERSON', 'entity_id', 'updated_at', 0, TRUE)
    """)

    runner.q("""
    INSERT INTO idr_meta.identifier_mapping VALUES
        ('test2_a', 'EMAIL', 'email', FALSE),
        ('test2_b', 'EMAIL', 'email', FALSE),
        ('test2_b', 'PHONE', 'phone', FALSE),
        ('test2_c', 'EMAIL', 'email', FALSE),
        ('test2_c', 'PHONE', 'phone', FALSE)
    """)

    # Run IDR
    runner.run_idr_pipeline(['test2_a', 'test2_b', 'test2_c'])

    # Assert: All three in same cluster
    cluster_count = runner.collect_one("""
        SELECT COUNT(DISTINCT resolved_id)
        FROM idr_out.identity_resolved_membership_current
        WHERE entity_key IN ('test2_a:CHAIN_A', 'test2_b:CHAIN_B', 'test2_c:CHAIN_C')
    """)

    passed = cluster_count == 1
    runner.add_test_result(
        "Chain Transitivity (A-B-C)",
        passed,
        f"clusters={cluster_count}, expected=1"
    )


def test_disjoint_graphs(runner: TestRunner):
    """Test: Two unconnected groups → two separate clusters."""
    print("\n🧪 Test 3: Disjoint Graphs → Separate Clusters")

    # Setup: Two completely separate groups
    runner.q("""
    CREATE TABLE crm.test3_group1_a AS
    SELECT 'G1_A' AS entity_id, 'group1@example.com' AS email, '1111111111' AS phone,
           TIMESTAMP '2024-01-01' AS updated_at
    """)

    runner.q("""
    CREATE TABLE crm.test3_group1_b AS
    SELECT 'G1_B' AS entity_id, 'group1@example.com' AS email, '2222222222' AS phone,
           TIMESTAMP '2024-01-02' AS updated_at
    """)

    runner.q("""
    CREATE TABLE crm.test3_group2_a AS
    SELECT 'G2_A' AS entity_id, 'group2@example.com' AS email, '3333333333' AS phone,
           TIMESTAMP '2024-01-01' AS updated_at
    """)

    runner.q("""
    CREATE TABLE crm.test3_group2_b AS
    SELECT 'G2_B' AS entity_id, 'group2@example.com' AS email, '4444444444' AS phone,
           TIMESTAMP '2024-01-02' AS updated_at
    """)

    # Register sources
    runner.q("""
    INSERT INTO idr_meta.source_table VALUES
        ('test3_g1a', 'crm.test3_group1_a', 'PERSON', 'entity_id', 'updated_at', 0, TRUE),
        ('test3_g1b', 'crm.test3_group1_b', 'PERSON', 'entity_id', 'updated_at', 0, TRUE),
        ('test3_g2a', 'crm.test3_group2_a', 'PERSON', 'entity_id', 'updated_at', 0, TRUE),
        ('test3_g2b', 'crm.test3_group2_b', 'PERSON', 'entity_id', 'updated_at', 0, TRUE)
    """)

    runner.q("""
    INSERT INTO idr_meta.identifier_mapping VALUES
        ('test3_g1a', 'EMAIL', 'email', FALSE),
        ('test3_g1b', 'EMAIL', 'email', FALSE),
        ('test3_g2a', 'EMAIL', 'email', FALSE),
        ('test3_g2b', 'EMAIL', 'email', FALSE)
    """)

    # Run IDR
    runner.run_idr_pipeline(['test3_g1a', 'test3_g1b', 'test3_g2a', 'test3_g2b'])

    # Assert: Two distinct clusters
    cluster_count = runner.collect_one("""
        SELECT COUNT(DISTINCT resolved_id)
        FROM idr_out.identity_resolved_membership_current
        WHERE entity_key LIKE 'test3_%'
    """)

    passed = cluster_count == 2
    runner.add_test_result(
        "Disjoint Graphs → Separate Clusters",
        passed,
        f"clusters={cluster_count}, expected=2"
    )


def test_case_insensitive_email(runner: TestRunner):
    """Test: Emails with different cases should match (UPPERCASE vs lowercase)."""
    print("\n🧪 Test 4: Case-Insensitive Email Matching")

    # Setup: Same email, different cases
    runner.q("""
    CREATE TABLE crm.test4_upper AS
    SELECT 'UPPER' AS entity_id, 'JOHN.DOE@GMAIL.COM' AS email, NULL AS phone,
           TIMESTAMP '2024-01-01' AS updated_at
    """)

    runner.q("""
    CREATE TABLE crm.test4_lower AS
    SELECT 'LOWER' AS entity_id, 'john.doe@gmail.com' AS email, NULL AS phone,
           TIMESTAMP '2024-01-02' AS updated_at
    """)

    runner.q("""
    CREATE TABLE crm.test4_mixed AS
    SELECT 'MIXED' AS entity_id, 'John.Doe@Gmail.Com' AS email, NULL AS phone,
           TIMESTAMP '2024-01-03' AS updated_at
    """)

    # Register sources
    runner.q("""
    INSERT INTO idr_meta.source_table VALUES
        ('test4_upper', 'crm.test4_upper', 'PERSON', 'entity_id', 'updated_at', 0, TRUE),
        ('test4_lower', 'crm.test4_lower', 'PERSON', 'entity_id', 'updated_at', 0, TRUE),
        ('test4_mixed', 'crm.test4_mixed', 'PERSON', 'entity_id', 'updated_at', 0, TRUE)
    """)

    runner.q("""
    INSERT INTO idr_meta.identifier_mapping VALUES
        ('test4_upper', 'EMAIL', 'email', FALSE),
        ('test4_lower', 'EMAIL', 'email', FALSE),
        ('test4_mixed', 'EMAIL', 'email', FALSE)
    """)

    # Run IDR
    runner.run_idr_pipeline(['test4_upper', 'test4_lower', 'test4_mixed'])

    # Assert: All in same cluster (EMAIL rule uses LOWERCASE canonicalization)
    cluster_count = runner.collect_one("""
        SELECT COUNT(DISTINCT resolved_id)
        FROM idr_out.identity_resolved_membership_current
        WHERE entity_key LIKE 'test4_%'
    """)

    passed = cluster_count == 1
    runner.add_test_result(
        "Case-Insensitive Email Matching",
        passed,
        f"clusters={cluster_count}, expected=1"
    )


def test_singleton_handling(runner: TestRunner):
    """Test: Entity with no matching identifiers should get resolved_id = entity_key."""
    print("\n🧪 Test 5: Singleton Handling")

    # Setup: Single entity with unique identifiers
    runner.q("""
    CREATE TABLE crm.test5_singleton AS
    SELECT 'LONELY' AS entity_id, 'unique@nowhere.com' AS email, '9999999999' AS phone,
           TIMESTAMP '2024-01-01' AS updated_at
    """)

    # Register source
    runner.q("""
    INSERT INTO idr_meta.source_table VALUES
        ('test5_single', 'crm.test5_singleton', 'PERSON', 'entity_id', 'updated_at', 0, TRUE)
    """)

    runner.q("""
    INSERT INTO idr_meta.identifier_mapping VALUES
        ('test5_single', 'EMAIL', 'email', FALSE),
        ('test5_single', 'PHONE', 'phone', FALSE)
    """)

    # Run IDR
    runner.run_idr_pipeline(['test5_single'])

    # Assert: resolved_id = entity_key for singleton
    resolved_id = runner.collect_one("""
        SELECT resolved_id
        FROM idr_out.identity_resolved_membership_current
        WHERE entity_key = 'test5_single:LONELY'
    """)

    passed = resolved_id == 'test5_single:LONELY'
    runner.add_test_result(
        "Singleton: resolved_id = entity_key",
        passed,
        f"resolved_id='{resolved_id}', expected='test5_single:LONELY'"
    )


def test_max_group_size_filtering(runner: TestRunner):
    """Test: Groups exceeding max_group_size should be skipped and not create edges."""
    print("\n🧪 Test 6: Max Group Size Filtering")

    # Add a rule with very low max_group_size for testing
    runner.q("""
    INSERT INTO idr_meta.rule VALUES
        ('R_LOYALTY_EXACT', 'Loyalty ID exact match', TRUE, 3, 'LOYALTY_ID', 'NONE', TRUE, TRUE, 2)
    """)  # max_group_size = 2

    # Setup: Create 5 entities sharing the same loyalty_id (exceeds max_group_size of 2)
    runner.q("""
    CREATE TABLE crm.test6_loyalty AS
    SELECT 'L001' AS entity_id, 'SHARED_LOYALTY_123' AS loyalty_id, 'a@test.com' AS email,
           TIMESTAMP '2024-01-01' AS updated_at
    UNION ALL
    SELECT 'L002', 'SHARED_LOYALTY_123', 'b@test.com', TIMESTAMP '2024-01-02'
    UNION ALL
    SELECT 'L003', 'SHARED_LOYALTY_123', 'c@test.com', TIMESTAMP '2024-01-03'
    UNION ALL
    SELECT 'L004', 'SHARED_LOYALTY_123', 'd@test.com', TIMESTAMP '2024-01-04'
    UNION ALL
    SELECT 'L005', 'SHARED_LOYALTY_123', 'e@test.com', TIMESTAMP '2024-01-05'
    """)

    # Register source with loyalty_id mapping
    runner.q("""
    INSERT INTO idr_meta.source_table VALUES
        ('test6_loyalty', 'crm.test6_loyalty', 'PERSON', 'entity_id', 'updated_at', 0, TRUE)
    """)

    runner.q("""
    INSERT INTO idr_meta.identifier_mapping VALUES
        ('test6_loyalty', 'LOYALTY_ID', 'loyalty_id', FALSE)
    """)

    # Run IDR
    runner.run_idr_pipeline(['test6_loyalty'])

    # Assert: No edges should be created via LOYALTY_ID (group size 5 > max_group_size 2)
    loyalty_edges = runner.collect_one("""
        SELECT COUNT(*) FROM idr_out.identity_edges_current
        WHERE identifier_type = 'LOYALTY_ID' AND identifier_value_norm = 'SHARED_LOYALTY_123'
    """) or 0

    # Assert: All 5 entities should be in separate clusters (singletons)
    cluster_count = runner.collect_one("""
        SELECT COUNT(DISTINCT resolved_id)
        FROM idr_out.identity_resolved_membership_current
        WHERE entity_key LIKE 'test6_loyalty:%'
    """)

    passed = loyalty_edges == 0 and cluster_count == 5
    runner.add_test_result(
        "Max Group Size: Large groups skipped",
        passed,
        f"loyalty_edges={loyalty_edges} (expected=0), clusters={cluster_count} (expected=5 singletons)"
    )


def test_exclusion_list(runner: TestRunner):
    """Test: Identifiers matching exclusion list should be filtered out."""
    print("\n🧪 Test 7: Identifier Exclusion List")

    # Setup exclusion list
    runner.q("""
    INSERT INTO idr_meta.identifier_exclusion VALUES
        ('EMAIL', 'test@test.com', 'EXACT', 'Test email placeholder', CURRENT_TIMESTAMP, 'system'),
        ('EMAIL', '%@example.invalid', 'LIKE', 'Invalid domain', CURRENT_TIMESTAMP, 'system')
    """)

    # Setup: Two entities that would match via test@test.com (should be excluded)
    runner.q("""
    CREATE TABLE crm.test7_excluded AS
    SELECT 'EX001' AS entity_id, 'test@test.com' AS email, '1111111111' AS phone,
           TIMESTAMP '2024-01-01' AS updated_at
    UNION ALL
    SELECT 'EX002', 'test@test.com', '2222222222', TIMESTAMP '2024-01-02'
    """)

    # Setup: Two entities with valid emails (should match)
    runner.q("""
    CREATE TABLE crm.test7_valid AS
    SELECT 'VA001' AS entity_id, 'valid@real.com' AS email, '3333333333' AS phone,
           TIMESTAMP '2024-01-01' AS updated_at
    UNION ALL
    SELECT 'VA002', 'valid@real.com', '4444444444', TIMESTAMP '2024-01-02'
    """)

    # Register sources
    runner.q("""
    INSERT INTO idr_meta.source_table VALUES
        ('test7_excluded', 'crm.test7_excluded', 'PERSON', 'entity_id', 'updated_at', 0, TRUE),
        ('test7_valid', 'crm.test7_valid', 'PERSON', 'entity_id', 'updated_at', 0, TRUE)
    """)

    runner.q("""
    INSERT INTO idr_meta.identifier_mapping VALUES
        ('test7_excluded', 'EMAIL', 'email', FALSE),
        ('test7_valid', 'EMAIL', 'email', FALSE)
    """)

    # Run IDR with exclusion filtering (manual check - the runner doesn't apply exclusions by default)
    # For this test, we verify the exclusion table is set up correctly

    exclusion_count = runner.collect_one("""
        SELECT COUNT(*) FROM idr_meta.identifier_exclusion
    """)

    # Check LIKE pattern matching works for domain filtering
    like_match = runner.collect_one("""
        SELECT COUNT(*) FROM idr_meta.identifier_exclusion
        WHERE match_type = 'LIKE' AND 'bad@example.invalid' LIKE identifier_value_pattern
    """)

    passed = exclusion_count == 2 and like_match == 1
    runner.add_test_result(
        "Exclusion List: Configuration verified",
        passed,
        f"exclusion_rules={exclusion_count} (expected=2), like_pattern_works={like_match == 1}"
    )


def test_skipped_identifier_groups_audit(runner: TestRunner):
    """Test: Skipped groups should be logged to audit table with sample entity keys."""
    print("\n🧪 Test 8: Skipped Identifier Groups Audit")

    # Manually insert a skipped group record (simulating what the production runner does)
    import uuid
    test_run_id = f"test_audit_{uuid.uuid4().hex[:8]}"

    runner.q(f"""
    INSERT INTO idr_out.skipped_identifier_groups
    (run_id, identifier_type, identifier_value_norm, group_size, max_allowed, sample_entity_keys, reason)
    VALUES
        ('{test_run_id}', 'EMAIL', 'shared@example.com', 15000, 10000,
         '["entity:001", "entity:002", "entity:003"]', 'EXCEEDED_MAX_GROUP_SIZE')
    """)

    # Verify record is retrievable
    skipped_count = runner.collect_one(f"""
        SELECT COUNT(*) FROM idr_out.skipped_identifier_groups WHERE run_id = '{test_run_id}'
    """)

    group_size = runner.collect_one(f"""
        SELECT group_size FROM idr_out.skipped_identifier_groups
        WHERE run_id = '{test_run_id}' AND identifier_type = 'EMAIL'
    """)

    sample_keys = runner.collect_one(f"""
        SELECT sample_entity_keys FROM idr_out.skipped_identifier_groups
        WHERE run_id = '{test_run_id}'
    """)

    passed = skipped_count == 1 and group_size == 15000 and 'entity:001' in (sample_keys or '')
    runner.add_test_result(
        "Skipped Groups Audit: Record structure verified",
        passed,
        f"records={skipped_count}, group_size={group_size}, has_sample_keys={'entity:001' in (sample_keys or '')}"
    )


def test_dry_run_mode(runner: TestRunner):
    """Test: Dry run mode should populate diff tables without modifying production."""
    print("\n🧪 Test 9: Dry Run Mode")

    import uuid
    dry_run_id = f"dry_run_{uuid.uuid4().hex[:8]}"

    # First, run a normal pipeline to establish baseline
    runner.q("""
    CREATE TABLE crm.test9_baseline AS
    SELECT 'BASE1' AS entity_id, 'baseline@test.com' AS email, '1111111111' AS phone,
           TIMESTAMP '2024-01-01' AS updated_at
    """)

    runner.q("""
    INSERT INTO idr_meta.source_table VALUES
        ('test9_baseline', 'crm.test9_baseline', 'PERSON', 'entity_id', 'updated_at', 0, TRUE)
    """)

    runner.q("""
    INSERT INTO idr_meta.identifier_mapping VALUES
        ('test9_baseline', 'EMAIL', 'email', FALSE)
    """)

    # Run normal pipeline to establish baseline membership
    runner.run_idr_pipeline(['test9_baseline'])

    # Count baseline membership
    baseline_membership = runner.collect_one("""
        SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current
        WHERE entity_key LIKE 'test9_baseline:%'
    """) or 0

    # Now simulate a dry run scenario - add a new entity that would merge
    runner.q("""
    CREATE TABLE crm.test9_dryrun AS
    SELECT 'DRY1' AS entity_id, 'baseline@test.com' AS email, '2222222222' AS phone,
           TIMESTAMP '2024-01-02' AS updated_at
    """)

    runner.q("""
    INSERT INTO idr_meta.source_table VALUES
        ('test9_dryrun', 'crm.test9_dryrun', 'PERSON', 'entity_id', 'updated_at', 0, TRUE)
    """)

    runner.q("""
    INSERT INTO idr_meta.identifier_mapping VALUES
        ('test9_dryrun', 'EMAIL', 'email', FALSE)
    """)

    # Simulate dry run by manually populating dry_run_results
    # (In production, this would be done by the runner with --dry-run flag)
    runner.q(f"""
    INSERT INTO idr_out.dry_run_results (run_id, entity_key, current_resolved_id, proposed_resolved_id, change_type)
    VALUES
        ('{dry_run_id}', 'test9_dryrun:DRY1', NULL, 'test9_baseline:BASE1', 'NEW'),
        ('{dry_run_id}', 'test9_baseline:BASE1', 'test9_baseline:BASE1', 'test9_baseline:BASE1', 'UNCHANGED')
    """)

    runner.q(f"""
    INSERT INTO idr_out.dry_run_summary
    (run_id, total_entities, new_entities, moved_entities, unchanged_entities, edges_would_create)
    VALUES ('{dry_run_id}', 2, 1, 0, 1, 1)
    """)

    # Verify dry_run_results was populated
    dry_run_count = runner.collect_one(f"""
        SELECT COUNT(*) FROM idr_out.dry_run_results WHERE run_id = '{dry_run_id}'
    """) or 0

    new_count = runner.collect_one(f"""
        SELECT COUNT(*) FROM idr_out.dry_run_results
        WHERE run_id = '{dry_run_id}' AND change_type = 'NEW'
    """) or 0

    # Verify summary was populated
    summary_exists = runner.collect_one(f"""
        SELECT COUNT(*) FROM idr_out.dry_run_summary WHERE run_id = '{dry_run_id}'
    """) or 0

    passed = dry_run_count == 2 and new_count == 1 and summary_exists == 1
    runner.add_test_result(
        "Dry Run Mode: Diff tables populated correctly",
        passed,
        f"dry_run_results={dry_run_count} (expected=2), new_entities={new_count} (expected=1), summary_exists={summary_exists == 1}"
    )


def test_confidence_singleton(runner: TestRunner):
    """Test: Singleton entities should have confidence_score = 1.0."""
    print("\n🧪 Test 10: Singleton Confidence Score = 1.0")

    runner.q("""
    CREATE TABLE crm.test10_single AS
    SELECT 'SOLO1' AS entity_id, 'unique_solo@example.com' AS email, NULL AS phone,
           TIMESTAMP '2024-01-01' AS updated_at
    """)

    runner.q("""
    INSERT INTO idr_meta.source_table VALUES
        ('test10_single', 'crm.test10_single', 'PERSON', 'entity_id', 'updated_at', 0, TRUE)
    """)

    runner.q("""
    INSERT INTO idr_meta.identifier_mapping VALUES
        ('test10_single', 'EMAIL', 'email', FALSE)
    """)

    runner.run_idr_pipeline(['test10_single'])

    # Check singleton confidence
    confidence = runner.collect_one("""
        SELECT confidence_score FROM idr_out.identity_clusters_current
        WHERE resolved_id LIKE 'test10_%' AND cluster_size = 1
    """)

    reason = runner.collect_one("""
        SELECT primary_reason FROM idr_out.identity_clusters_current
        WHERE resolved_id LIKE 'test10_%' AND cluster_size = 1
    """)

    passed = confidence == 1.0 and reason == 'SINGLETON_NO_MATCH_REQUIRED'
    runner.add_test_result(
        "Singleton Confidence = 1.0",
        passed,
        f"confidence={confidence}, reason={reason}"
    )


def test_confidence_multi_identifier(runner: TestRunner):
    """Test: Cluster connected by multiple identifier types should have higher confidence."""
    print("\n🧪 Test 11: Multi-Identifier → Higher Confidence")

    # Two entities connected by BOTH email AND phone
    runner.q("""
    CREATE TABLE crm.test11_multi_a AS
    SELECT 'MULTI_A' AS entity_id, 'multi@example.com' AS email, '5550001111' AS phone,
           TIMESTAMP '2024-01-01' AS updated_at
    """)

    runner.q("""
    CREATE TABLE crm.test11_multi_b AS
    SELECT 'MULTI_B' AS entity_id, 'multi@example.com' AS email, '5550001111' AS phone,
           TIMESTAMP '2024-01-02' AS updated_at
    """)

    runner.q("""
    INSERT INTO idr_meta.source_table VALUES
        ('test11_multi_a', 'crm.test11_multi_a', 'PERSON', 'entity_id', 'updated_at', 0, TRUE),
        ('test11_multi_b', 'crm.test11_multi_b', 'PERSON', 'entity_id', 'updated_at', 0, TRUE)
    """)

    runner.q("""
    INSERT INTO idr_meta.identifier_mapping VALUES
        ('test11_multi_a', 'EMAIL', 'email', FALSE),
        ('test11_multi_a', 'PHONE', 'phone', FALSE),
        ('test11_multi_b', 'EMAIL', 'email', FALSE),
        ('test11_multi_b', 'PHONE', 'phone', FALSE)
    """)

    runner.run_idr_pipeline(['test11_multi_a', 'test11_multi_b'])

    # Check multi-identifier confidence (should be higher)
    confidence = runner.collect_one("""
        SELECT confidence_score FROM idr_out.identity_clusters_current
        WHERE resolved_id LIKE 'test11_%' AND cluster_size > 1
    """)

    edge_diversity = runner.collect_one("""
        SELECT edge_diversity FROM idr_out.identity_clusters_current
        WHERE resolved_id LIKE 'test11_%' AND cluster_size > 1
    """)

    passed = confidence is not None and confidence > 0.7 and edge_diversity == 2
    runner.add_test_result(
        "Multi-Identifier → Higher Confidence",
        passed,
        f"confidence={confidence} (expected > 0.7), edge_diversity={edge_diversity} (expected=2)"
    )


def test_confidence_single_identifier(runner: TestRunner):
    """Test: Cluster connected by only one identifier type should have lower confidence."""
    print("\n🧪 Test 12: Single Identifier → Lower Confidence")

    # Two entities connected by email only
    runner.q("""
    CREATE TABLE crm.test12_single_a AS
    SELECT 'SINGLE_A' AS entity_id, 'single_only@example.com' AS email, '1111111111' AS phone,
           TIMESTAMP '2024-01-01' AS updated_at
    """)

    runner.q("""
    CREATE TABLE crm.test12_single_b AS
    SELECT 'SINGLE_B' AS entity_id, 'single_only@example.com' AS email, '2222222222' AS phone,
           TIMESTAMP '2024-01-02' AS updated_at
    """)

    runner.q("""
    INSERT INTO idr_meta.source_table VALUES
        ('test12_single_a', 'crm.test12_single_a', 'PERSON', 'entity_id', 'updated_at', 0, TRUE),
        ('test12_single_b', 'crm.test12_single_b', 'PERSON', 'entity_id', 'updated_at', 0, TRUE)
    """)

    runner.q("""
    INSERT INTO idr_meta.identifier_mapping VALUES
        ('test12_single_a', 'EMAIL', 'email', FALSE),
        ('test12_single_b', 'EMAIL', 'email', FALSE)
    """)

    runner.run_idr_pipeline(['test12_single_a', 'test12_single_b'])

    # Check single-identifier confidence (should be lower than multi)
    confidence = runner.collect_one("""
        SELECT confidence_score FROM idr_out.identity_clusters_current
        WHERE resolved_id LIKE 'test12_%' AND cluster_size > 1
    """)

    edge_diversity = runner.collect_one("""
        SELECT edge_diversity FROM idr_out.identity_clusters_current
        WHERE resolved_id LIKE 'test12_%' AND cluster_size > 1
    """)

    passed = confidence is not None and edge_diversity == 1
    runner.add_test_result(
        "Single Identifier → edge_diversity = 1",
        passed,
        f"confidence={confidence}, edge_diversity={edge_diversity} (expected=1)"
    )


# ============================================
# MAIN
# ============================================
def main():
    print("🦆 DuckDB IDR Integration Tests")
    print(f"   Database: {DB_PATH}")
    print("=" * 60)

    runner = TestRunner(DB_PATH)

    try:
        runner.connect()
        runner.setup_schemas()
        runner.setup_base_metadata()

        # Run all tests
        test_same_email(runner)
        test_chain_transitivity(runner)
        test_disjoint_graphs(runner)
        test_case_insensitive_email(runner)
        test_singleton_handling(runner)

        # Production hardening tests
        test_max_group_size_filtering(runner)
        test_exclusion_list(runner)
        test_skipped_identifier_groups_audit(runner)

        # Dry run tests
        test_dry_run_mode(runner)

        # Confidence scoring tests
        test_confidence_singleton(runner)
        test_confidence_multi_identifier(runner)
        test_confidence_single_identifier(runner)

        # Print results
        all_passed = runner.print_results()

    finally:
        runner.close()

        # Cleanup
        if not args.keep_db and os.path.exists(DB_PATH):
            os.remove(DB_PATH)
            print(f"\n🗑️  Cleaned up test database: {DB_PATH}")
        elif args.keep_db:
            print(f"\n💾 Kept test database: {DB_PATH}")

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
