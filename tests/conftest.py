"""
Shared pytest fixtures for IDR tests.
"""


def pytest_configure(config):
    """Configure custom markers."""
    config.addinivalue_line("markers", "duckdb: marks tests for DuckDB")
    config.addinivalue_line("markers", "snowflake: marks tests for Snowflake")
    config.addinivalue_line("markers", "bigquery: marks tests for BigQuery")
    config.addinivalue_line("markers", "databricks: marks tests for Databricks")
    config.addinivalue_line("markers", "slow: marks tests as slow")
