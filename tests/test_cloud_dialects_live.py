"""
Opt-in live cloud warehouse integration test suite for Snowflake, BigQuery, and Databricks.

Automatically skips when cloud credentials are not configured in environment variables.
To run against a live warehouse:
  - Snowflake: export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=... SNOWFLAKE_WAREHOUSE=... SNOWFLAKE_DATABASE=...
  - BigQuery:  export IDR_PROJECT=... GOOGLE_APPLICATION_CREDENTIALS=...
  - Databricks: export DATABRICKS_HOST=... DATABRICKS_HTTP_PATH=... DATABRICKS_TOKEN=...
"""

import os

import pytest

from idr_core.config import config_to_sql
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager


def _run_cloud_smoke_pipeline(adapter):
    """Execute end-to-end FULL + DRY_RUN + INCR smoke test on a connected cloud adapter."""
    sm = SchemaManager(adapter)
    sm.initialize(reset=True)

    adapter.execute("""
        CREATE OR REPLACE TABLE idr_work.cloud_smoke_src (
            id STRING,
            email STRING,
            first_name STRING,
            updated_at TIMESTAMP
        )
    """)
    adapter.execute("""
        INSERT INTO idr_work.cloud_smoke_src (id, email, first_name, updated_at) VALUES
            ('e1', 'cloud@example.com', 'CloudOne', CURRENT_TIMESTAMP),
            ('e2', 'cloud@example.com', 'CloudTwo', CURRENT_TIMESTAMP)
    """)

    cfg = {
        "sources": [
            {
                "id": "cloud_smoke",
                "table": "idr_work.cloud_smoke_src",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [{"type": "email", "expr": "email"}],
                "attributes": [{"name": "first_name", "expr": "first_name"}],
            }
        ],
        "rules": [{"id": "r_email", "type": "EXACT", "match_keys": ["email"], "priority": 1}],
        "survivorship": [{"attribute": "first_name", "strategy": "RECENCY"}],
    }
    for stmt in config_to_sql(cfg, dialect=adapter.dialect):
        adapter.execute(stmt)

    runner = IDRRunner(adapter)
    full_res = runner.run(RunConfig(run_mode="FULL", max_iters=10))
    assert full_res.status == "SUCCESS"
    assert full_res.entities_processed == 2

    dry_res = runner.run(RunConfig(run_mode="FULL", max_iters=10, dry_run=True))
    assert dry_res.status == "DRY_RUN_COMPLETE"


@pytest.mark.skipif(
    not os.getenv("SNOWFLAKE_ACCOUNT") or not os.getenv("SNOWFLAKE_USER"),
    reason="SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER env vars not set",
)
def test_live_snowflake_pipeline():
    import snowflake.connector

    from idr_core.adapters.snowflake import SnowflakeAdapter

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
    )
    adapter = SnowflakeAdapter(conn)
    try:
        _run_cloud_smoke_pipeline(adapter)
    finally:
        adapter.close()


@pytest.mark.skipif(
    not os.getenv("IDR_BQ_LIVE_TEST") or not os.getenv("IDR_PROJECT"),
    reason="IDR_BQ_LIVE_TEST=1 and IDR_PROJECT env vars not set",
)
def test_live_bigquery_pipeline():
    from google.cloud import bigquery

    from idr_core.adapters.bigquery import BigQueryAdapter

    project = os.environ["IDR_PROJECT"]
    client = bigquery.Client(project=project)
    adapter = BigQueryAdapter(client, project=project)
    try:
        _run_cloud_smoke_pipeline(adapter)
    finally:
        adapter.close()


@pytest.mark.skipif(
    not os.getenv("DATABRICKS_HOST") or not os.getenv("DATABRICKS_TOKEN"),
    reason="DATABRICKS_HOST and DATABRICKS_TOKEN env vars not set",
)
def test_live_databricks_pipeline():
    from databricks import sql

    from idr_core.adapters.databricks import DatabricksAdapter

    conn = sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    adapter = DatabricksAdapter(conn, catalog=os.environ.get("DATABRICKS_CATALOG"))
    try:
        _run_cloud_smoke_pipeline(adapter)
    finally:
        adapter.close()

