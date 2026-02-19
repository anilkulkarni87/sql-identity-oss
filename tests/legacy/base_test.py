#!/usr/bin/env python3
"""
IDR Test Base Class

Provides an abstract base for cross-platform testing of SQL Identity Resolution.
Platform-specific implementations must implement the abstract methods.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import uuid


class IDRTestBase(ABC):
    """
    Abstract base class for IDR integration tests.

    Platform-specific test classes should inherit from this and implement
    the abstract methods for database connectivity.
    """

    # ============================================
    # ABSTRACT METHODS - Must be implemented
    # ============================================

    @abstractmethod
    def get_connection(self):
        """Get database connection object."""
        pass

    @abstractmethod
    def execute_sql(self, sql: str) -> None:
        """Execute SQL statement (no return value expected)."""
        pass

    @abstractmethod
    def query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL and return list of dicts."""
        pass

    @abstractmethod
    def query_one(self, sql: str) -> Any:
        """Execute SQL and return first column of first row."""
        pass

    @abstractmethod
    def run_idr_pipeline(
        self,
        sources: List[str],
        run_mode: str = 'FULL',
        dry_run: bool = False,
        max_iters: int = 30
    ) -> str:
        """
        Run the IDR pipeline and return the run_id.

        Args:
            sources: List of source table IDs to process
            run_mode: 'FULL' or 'INCR'
            dry_run: If True, preview only
            max_iters: Max label propagation iterations

        Returns:
            run_id: The ID of the completed run
        """
        pass

    @abstractmethod
    def setup_schemas(self) -> None:
        """Create all required schemas and tables."""
        pass

    @abstractmethod
    def teardown(self) -> None:
        """Clean up after tests."""
        pass

    # ============================================
    # HELPER METHODS
    # ============================================

    def setup_test_source(
        self,
        table_id: str,
        data: List[Dict[str, Any]],
        entity_key_column: str = 'entity_id',
        identifiers: Dict[str, str] = None
    ) -> None:
        """
        Set up a test source table with data.

        Args:
            table_id: Unique identifier for this source
            data: List of dicts with entity data
            entity_key_column: Column to use as entity key
            identifiers: Dict mapping identifier_type -> column_name
        """
        # Create test table
        if data:
            columns = list(data[0].keys())
            self.execute_sql(f"DROP TABLE IF EXISTS test_{table_id}")

            # Create table
            col_defs = ', '.join([f"{c} VARCHAR" for c in columns])
            col_defs += ", updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            self.execute_sql(f"CREATE TABLE test_{table_id} ({col_defs})")

            # Insert data
            for row in data:
                values = ', '.join([f"'{row.get(c, '')}'" for c in columns])
                self.execute_sql(f"INSERT INTO test_{table_id} ({', '.join(columns)}) VALUES ({values})")

        # Register in metadata
        self.execute_sql(f"""
            INSERT INTO idr_meta.source_table VALUES
            ('{table_id}', 'test_{table_id}', 'PERSON', '{entity_key_column}', 'updated_at', 0, TRUE)
        """)

        # Set up identifier mappings
        if identifiers:
            for identifier_type, column_name in identifiers.items():
                # Ensure rule exists
                existing = self.query_one(f"SELECT COUNT(*) FROM idr_meta.rule WHERE identifier_type = '{identifier_type}'")
                if not existing:
                    self.execute_sql(f"""
                        INSERT INTO idr_meta.rule VALUES
                        ('{identifier_type.lower()}_rule', '{identifier_type}', 1, TRUE, 10000)
                    """)

                # Add mapping
                self.execute_sql(f"""
                    INSERT INTO idr_meta.identifier_mapping VALUES
                    ('{table_id}', '{identifier_type}', '{column_name}', TRUE)
                """)

    def count(self, table: str) -> int:
        """Count rows in a table."""
        result = self.query_one(f"SELECT COUNT(*) FROM {table}")
        return int(result) if result else 0

    def get_cluster_for_entity(self, entity_key: str) -> Optional[str]:
        """Get the resolved_id for an entity."""
        return self.query_one(f"""
            SELECT resolved_id FROM idr_out.identity_resolved_membership_current
            WHERE entity_key = '{entity_key}'
        """)

    def get_cluster_size(self, resolved_id: str) -> int:
        """Get the size of a cluster."""
        result = self.query_one(f"""
            SELECT cluster_size FROM idr_out.identity_clusters_current
            WHERE resolved_id = '{resolved_id}'
        """)
        return int(result) if result else 0

    def get_entities_in_cluster(self, resolved_id: str) -> List[str]:
        """Get all entity keys in a cluster."""
        results = self.query(f"""
            SELECT entity_key FROM idr_out.identity_resolved_membership_current
            WHERE resolved_id = '{resolved_id}'
        """)
        return [r['entity_key'] for r in results]

    def get_run_status(self, run_id: str) -> str:
        """Get the status of a run."""
        return self.query_one(f"""
            SELECT status FROM idr_out.run_history
            WHERE run_id = '{run_id}'
        """)

    def clear_all_data(self) -> None:
        """Clear all output data (for fresh test runs)."""
        tables = [
            'idr_out.identity_resolved_membership_current',
            'idr_out.identity_clusters_current',
            'idr_out.golden_profile_current',
            'idr_out.run_history',
            'idr_out.dry_run_results',
            'idr_out.dry_run_summary',
            'idr_out.metrics_export',
            'idr_out.skipped_identifier_groups',
        ]
        for table in tables:
            try:
                self.execute_sql(f"DELETE FROM {table}")
            except:
                pass

        # Clear metadata
        for table in ['idr_meta.source_table', 'idr_meta.rule', 'idr_meta.identifier_mapping', 'idr_meta.run_state']:
            try:
                self.execute_sql(f"DELETE FROM {table}")
            except:
                pass

    # ============================================
    # STANDARD TEST CASES
    # ============================================

    def test_same_email_same_cluster(self):
        """Entities with the same email should be in the same cluster."""
        # Setup
        self.clear_all_data()
        self.setup_test_source(
            table_id='test_src',
            data=[
                {'entity_id': 'A', 'email': 'shared@test.com'},
                {'entity_id': 'B', 'email': 'shared@test.com'},
            ],
            identifiers={'EMAIL': 'email'}
        )

        # Execute
        run_id = self.run_idr_pipeline(['test_src'], run_mode='FULL')

        # Assert
        cluster_a = self.get_cluster_for_entity('test_src:A')
        cluster_b = self.get_cluster_for_entity('test_src:B')

        assert cluster_a is not None, "Entity A should have a cluster"
        assert cluster_b is not None, "Entity B should have a cluster"
        assert cluster_a == cluster_b, f"A and B should be in same cluster, but A={cluster_a}, B={cluster_b}"

        print("✅ test_same_email_same_cluster PASSED")

    def test_different_email_different_cluster(self):
        """Entities with different emails should be in different clusters."""
        # Setup
        self.clear_all_data()
        self.setup_test_source(
            table_id='test_src',
            data=[
                {'entity_id': 'A', 'email': 'alice@test.com'},
                {'entity_id': 'B', 'email': 'bob@test.com'},
            ],
            identifiers={'EMAIL': 'email'}
        )

        # Execute
        run_id = self.run_idr_pipeline(['test_src'], run_mode='FULL')

        # Assert
        cluster_a = self.get_cluster_for_entity('test_src:A')
        cluster_b = self.get_cluster_for_entity('test_src:B')

        assert cluster_a is not None, "Entity A should have a cluster"
        assert cluster_b is not None, "Entity B should have a cluster"
        assert cluster_a != cluster_b, f"A and B should be in different clusters, but both are in {cluster_a}"

        print("✅ test_different_email_different_cluster PASSED")

    def test_chain_transitivity(self):
        """A-B and B-C connections should result in A, B, C in same cluster."""
        # Setup
        self.clear_all_data()
        self.setup_test_source(
            table_id='test_src',
            data=[
                {'entity_id': 'A', 'email': 'ab@test.com', 'phone': ''},
                {'entity_id': 'B', 'email': 'ab@test.com', 'phone': '555-0001'},
                {'entity_id': 'C', 'email': '', 'phone': '555-0001'},
            ],
            identifiers={'EMAIL': 'email', 'PHONE': 'phone'}
        )

        # Execute
        run_id = self.run_idr_pipeline(['test_src'], run_mode='FULL')

        # Assert
        cluster_a = self.get_cluster_for_entity('test_src:A')
        cluster_b = self.get_cluster_for_entity('test_src:B')
        cluster_c = self.get_cluster_for_entity('test_src:C')

        assert cluster_a == cluster_b == cluster_c, f"A, B, C should all be in same cluster"

        print("✅ test_chain_transitivity PASSED")

    def test_dry_run_no_commits(self):
        """Dry run should populate dry_run_results but not production tables."""
        # Setup
        self.clear_all_data()
        self.setup_test_source(
            table_id='test_src',
            data=[
                {'entity_id': 'A', 'email': 'new@test.com'},
            ],
            identifiers={'EMAIL': 'email'}
        )

        # Get baseline
        membership_before = self.count('idr_out.identity_resolved_membership_current')

        # Execute dry run
        run_id = self.run_idr_pipeline(['test_src'], run_mode='FULL', dry_run=True)

        # Assert - production unchanged
        membership_after = self.count('idr_out.identity_resolved_membership_current')
        assert membership_before == membership_after, "Membership should not change during dry run"

        # Assert - dry run results populated
        dry_run_count = self.count('idr_out.dry_run_results')
        assert dry_run_count > 0, "Dry run results should be populated"

        # Assert - status is DRY_RUN_COMPLETE
        status = self.get_run_status(run_id)
        assert status == 'DRY_RUN_COMPLETE', f"Status should be DRY_RUN_COMPLETE but was {status}"

        print("✅ test_dry_run_no_commits PASSED")

    def test_singleton_handling(self):
        """Entity with no matching identifiers should be a singleton cluster."""
        # Setup
        self.clear_all_data()
        self.setup_test_source(
            table_id='test_src',
            data=[
                {'entity_id': 'LONE', 'email': 'unique@test.com'},
            ],
            identifiers={'EMAIL': 'email'}
        )

        # Execute
        run_id = self.run_idr_pipeline(['test_src'], run_mode='FULL')

        # Assert
        cluster = self.get_cluster_for_entity('test_src:LONE')
        cluster_size = self.get_cluster_size(cluster) if cluster else 0

        assert cluster is not None, "Singleton should have a cluster"
        assert cluster_size == 1, f"Singleton cluster size should be 1, got {cluster_size}"

        print("✅ test_singleton_handling PASSED")

    def test_case_insensitive_matching(self):
        """Email matching should be case-insensitive."""
        # Setup
        self.clear_all_data()
        self.setup_test_source(
            table_id='test_src',
            data=[
                {'entity_id': 'A', 'email': 'Test@Example.COM'},
                {'entity_id': 'B', 'email': 'test@example.com'},
            ],
            identifiers={'EMAIL': 'email'}
        )

        # Execute
        run_id = self.run_idr_pipeline(['test_src'], run_mode='FULL')

        # Assert
        cluster_a = self.get_cluster_for_entity('test_src:A')
        cluster_b = self.get_cluster_for_entity('test_src:B')

        assert cluster_a == cluster_b, "Case-different emails should match"

        print("✅ test_case_insensitive_matching PASSED")

    def run_all_tests(self) -> Dict[str, bool]:
        """Run all standard tests and return results."""
        tests = [
            ('test_same_email_same_cluster', self.test_same_email_same_cluster),
            ('test_different_email_different_cluster', self.test_different_email_different_cluster),
            ('test_chain_transitivity', self.test_chain_transitivity),
            ('test_dry_run_no_commits', self.test_dry_run_no_commits),
            ('test_singleton_handling', self.test_singleton_handling),
            ('test_case_insensitive_matching', self.test_case_insensitive_matching),
        ]

        results = {}
        for name, test_fn in tests:
            try:
                test_fn()
                results[name] = True
            except Exception as e:
                print(f"❌ {name} FAILED: {e}")
                results[name] = False

        # Summary
        passed = sum(1 for v in results.values() if v)
        total = len(results)
        print(f"\n{'='*50}")
        print(f"Results: {passed}/{total} tests passed")
        print(f"{'='*50}")

        return results
