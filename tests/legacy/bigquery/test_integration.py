#!/usr/bin/env python3
"""
BigQuery Integration Tests (Stub)

This file provides a template for BigQuery-specific tests.
Requires GCP credentials to run.

Environment Variables:
    GOOGLE_APPLICATION_CREDENTIALS: Path to service account JSON
    GCP_PROJECT: GCP project ID
"""

import os
import pytest
from typing import List, Dict, Any

# Import base test class
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from base_test import IDRTestBase


class BigQueryTestRunner(IDRTestBase):
    """BigQuery implementation of IDR tests."""

    def __init__(self):
        self.client = None
        self.project = os.environ.get('GCP_PROJECT')

    def get_connection(self):
        if self.client is None:
            from google.cloud import bigquery
            self.client = bigquery.Client(project=self.project)
        return self.client

    def execute_sql(self, sql: str) -> None:
        sql = self._qualify_tables(sql)
        self.get_connection().query(sql).result()

    def query(self, sql: str) -> List[Dict[str, Any]]:
        sql = self._qualify_tables(sql)
        result = self.get_connection().query(sql).result()
        return [dict(row) for row in result]

    def query_one(self, sql: str) -> Any:
        sql = self._qualify_tables(sql)
        result = self.get_connection().query(sql).result()
        for row in result:
            return row[0]
        return None

    def _qualify_tables(self, sql: str) -> str:
        """Add project qualification to table names."""
        sql = sql.replace('idr_meta.', f'{self.project}.idr_meta.')
        sql = sql.replace('idr_work.', f'{self.project}.idr_work.')
        sql = sql.replace('idr_out.', f'{self.project}.idr_out.')
        sql = sql.replace('test_', f'{self.project}.idr_work.test_')
        return sql

    def run_idr_pipeline(
        self,
        sources: List[str],
        run_mode: str = 'FULL',
        dry_run: bool = False,
        max_iters: int = 30
    ) -> str:
        """Run BigQuery IDR pipeline via subprocess."""
        import subprocess

        cmd = [
            'python',
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                        'sql', 'bigquery', 'core', 'idr_run.py'),
            f'--project={self.project}',
            f'--run-mode={run_mode}',
            f'--max-iters={max_iters}'
        ]
        if dry_run:
            cmd.append('--dry-run')

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"IDR run failed: {result.stderr}")

        # Extract run_id from output
        for line in result.stdout.split('\n'):
            if 'Run ID:' in line:
                return line.split('Run ID:')[1].strip()

        return None

    def setup_schemas(self) -> None:
        """Create BigQuery datasets and tables."""
        from google.cloud import bigquery

        client = self.get_connection()

        # Create datasets
        for dataset_id in ['idr_meta', 'idr_work', 'idr_out']:
            dataset = bigquery.Dataset(f"{self.project}.{dataset_id}")
            dataset.location = "US"
            try:
                client.create_dataset(dataset, exists_ok=True)
            except Exception as e:
                print(f"Warning creating dataset {dataset_id}: {e}")

        # Run DDL
        ddl_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'sql', 'bigquery', 'core', '00_ddl_all.sql'
        )
        if os.path.exists(ddl_path):
            with open(ddl_path) as f:
                ddl = f.read()
            ddl = self._qualify_tables(ddl)
            for statement in ddl.split(';'):
                if statement.strip():
                    try:
                        self.execute_sql(statement)
                    except Exception as e:
                        print(f"Warning: {e}")

    def teardown(self) -> None:
        pass  # BigQuery client doesn't need explicit cleanup


# ============================================
# PYTEST TESTS
# ============================================

@pytest.fixture
def runner():
    """Pytest fixture for BigQuery test runner."""
    runner = BigQueryTestRunner()
    runner.setup_schemas()
    yield runner
    runner.teardown()


@pytest.mark.bigquery
@pytest.mark.skipif(
    not os.environ.get('GCP_PROJECT'),
    reason="GCP_PROJECT environment variable not set"
)
class TestBigQueryIntegration:
    """BigQuery integration tests."""

    def test_same_email_same_cluster(self, runner):
        runner.test_same_email_same_cluster()

    def test_different_email_different_cluster(self, runner):
        runner.test_different_email_different_cluster()

    def test_chain_transitivity(self, runner):
        runner.test_chain_transitivity()

    def test_dry_run_no_commits(self, runner):
        runner.test_dry_run_no_commits()

    def test_singleton_handling(self, runner):
        runner.test_singleton_handling()

    def test_case_insensitive_matching(self, runner):
        runner.test_case_insensitive_matching()


# ============================================
# STANDALONE EXECUTION
# ============================================

if __name__ == '__main__':
    # Check for credentials
    if not os.environ.get('GCP_PROJECT'):
        print("Error: GCP_PROJECT environment variable not set")
        print("\nRequired environment variables:")
        print("  GOOGLE_APPLICATION_CREDENTIALS (path to service account JSON)")
        print("  GCP_PROJECT")
        exit(1)

    runner = BigQueryTestRunner()
    try:
        runner.setup_schemas()
        runner.run_all_tests()
    finally:
        runner.teardown()
