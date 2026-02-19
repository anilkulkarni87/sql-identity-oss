#!/usr/bin/env python3
"""
Snowflake Integration Tests (Stub)

This file provides a template for Snowflake-specific tests.
Requires Snowflake credentials to run.

Environment Variables:
    SNOWFLAKE_ACCOUNT: Snowflake account identifier
    SNOWFLAKE_USER: Username
    SNOWFLAKE_PASSWORD: Password
    SNOWFLAKE_DATABASE: Database name
    SNOWFLAKE_WAREHOUSE: Warehouse name
"""

import os
import pytest
from typing import List, Dict, Any

# Import base test class
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from base_test import IDRTestBase


class SnowflakeTestRunner(IDRTestBase):
    """Snowflake implementation of IDR tests."""

    def __init__(self):
        self.conn = None

    def get_connection(self):
        if self.conn is None:
            import snowflake.connector
            self.conn = snowflake.connector.connect(
                account=os.environ.get('SNOWFLAKE_ACCOUNT'),
                user=os.environ.get('SNOWFLAKE_USER'),
                password=os.environ.get('SNOWFLAKE_PASSWORD'),
                database=os.environ.get('SNOWFLAKE_DATABASE'),
                warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE')
            )
        return self.conn

    def execute_sql(self, sql: str) -> None:
        cursor = self.get_connection().cursor()
        cursor.execute(sql)

    def query(self, sql: str) -> List[Dict[str, Any]]:
        cursor = self.get_connection().cursor()
        cursor.execute(sql)
        columns = [desc[0].lower() for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def query_one(self, sql: str) -> Any:
        cursor = self.get_connection().cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        return row[0] if row else None

    def run_idr_pipeline(
        self,
        sources: List[str],
        run_mode: str = 'FULL',
        dry_run: bool = False,
        max_iters: int = 30
    ) -> str:
        """Run Snowflake stored procedure."""
        cursor = self.get_connection().cursor()
        cursor.execute(f"CALL idr_run('{run_mode}', {max_iters}, {str(dry_run).upper()})")
        result = cursor.fetchone()[0]

        # Extract run_id from result
        # Result format: "STATUS: run_id=xxx, ..."
        if 'run_id=' in result:
            run_id = result.split('run_id=')[1].split(',')[0]
            return run_id
        return None

    def setup_schemas(self) -> None:
        """Run Snowflake DDL."""
        ddl_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'sql', 'snowflake', '00_ddl_all.sql'
        )
        if os.path.exists(ddl_path):
            with open(ddl_path) as f:
                ddl = f.read()
            # Split by semicolon and execute each statement
            for statement in ddl.split(';'):
                if statement.strip():
                    try:
                        self.execute_sql(statement)
                    except Exception as e:
                        print(f"Warning: {e}")

    def teardown(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None


# ============================================
# PYTEST TESTS
# ============================================

@pytest.fixture
def runner():
    """Pytest fixture for Snowflake test runner."""
    runner = SnowflakeTestRunner()
    runner.setup_schemas()
    yield runner
    runner.teardown()


@pytest.mark.snowflake
@pytest.mark.skipif(
    not os.environ.get('SNOWFLAKE_ACCOUNT'),
    reason="Snowflake credentials not set"
)
class TestSnowflakeIntegration:
    """Snowflake integration tests."""

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
    if not os.environ.get('SNOWFLAKE_ACCOUNT'):
        print("Error: SNOWFLAKE_ACCOUNT environment variable not set")
        print("\nRequired environment variables:")
        print("  SNOWFLAKE_ACCOUNT")
        print("  SNOWFLAKE_USER")
        print("  SNOWFLAKE_PASSWORD")
        print("  SNOWFLAKE_DATABASE")
        print("  SNOWFLAKE_WAREHOUSE")
        exit(1)

    runner = SnowflakeTestRunner()
    try:
        runner.setup_schemas()
        runner.run_all_tests()
    finally:
        runner.teardown()
