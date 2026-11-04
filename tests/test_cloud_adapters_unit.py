"""
Comprehensive unit and multi-dialect SQL generation tests for:
1. BigQueryAdapter
2. SnowflakeAdapter (Snowpark Session mode & DB-API Connector mode)
3. DatabricksAdapter (SparkSession mode & DB-API Connector mode)
4. 4-Dialect Stage SQL Generation (ExtractionStage, GraphStage, OutputStage dry-run & metrics)
"""

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from idr_core.adapters.base import get_dialect_config
from idr_core.adapters.bigquery import BigQueryAdapter
from idr_core.adapters.databricks import DatabricksAdapter
from idr_core.adapters.snowflake import SnowflakeAdapter
from idr_core.runner import RunConfig
from idr_core.stages.base import StageContext
from idr_core.stages.extraction import ExtractionStage
from idr_core.stages.graph import GraphStage
from idr_core.stages.output import OutputStage


class MockColumns(list):
    @property
    def str(self):
        class _StrAccessor:
            def __init__(self, cols):
                self._cols = cols

            def lower(self):
                return [str(c).lower() for c in self._cols]

        return _StrAccessor(self)


class MockDataFrame:
    """Lightweight mock providing .columns and .to_dict('records') without requiring pandas."""

    def __init__(self, records):
        self._records = records
        self.columns = MockColumns(records[0].keys()) if records else MockColumns()

    def to_dict(self, orient="records"):
        if orient == "records" and self._records:
            col_map = dict(zip(list(self._records[0].keys()), self.columns))
            return [{col_map.get(k, k): v for k, v in r.items()} for r in self._records]
        return self._records


# ============================================================
# 1. BigQueryAdapter Unit Tests
# ============================================================


class TestBigQueryAdapterUnit:
    def test_prepare_sql_with_dataset_mapping_and_backticks(self):
        client = MagicMock()
        adapter = BigQueryAdapter(
            client,
            project="my-gcp-proj",
            location="EU",
            dataset_mapping={"idr_out": "prod_idr_out", "idr_meta": "prod_idr_meta"},
        )
        assert adapter.dialect == "bigquery"

        sql = "SELECT * FROM idr_out.identity_clusters_current JOIN idr_meta.rule ON 1=1 JOIN idr_work.lp_labels ON 1=1"
        prepared = adapter._prepare_sql_simple(sql)
        assert "`my-gcp-proj`.prod_idr_out.identity_clusters_current" in prepared
        assert "`my-gcp-proj`.prod_idr_meta.rule" in prepared
        assert "`my-gcp-proj`.idr_work.lp_labels" in prepared

    def test_execute_and_query_normalizes_lowercase_columns(self):
        client = MagicMock()
        job = MagicMock()
        job.to_dataframe.return_value = MockDataFrame([{"RESOLVED_ID": "c1", "CLUSTER_SIZE": 3}])
        job.result.return_value = [("c1",)]
        client.query.return_value = job

        adapter = BigQueryAdapter(client, project="proj-1")
        adapter.execute("CREATE TABLE idr_out.test AS SELECT 1")
        assert "`proj-1`.idr_out.test" in client.query.call_args[0][0]

        rows = adapter.query("SELECT RESOLVED_ID, CLUSTER_SIZE FROM idr_out.test")
        assert rows == [{"resolved_id": "c1", "cluster_size": 3}]

        val = adapter.query_one("SELECT RESOLVED_ID FROM idr_out.test")
        assert val == "c1"

    def test_table_exists_get_columns_and_list_tables(self):
        client = MagicMock()
        mock_table = MagicMock()
        mock_table.schema = [
            SimpleNamespace(name="ENTITY_KEY", field_type="STRING"),
            SimpleNamespace(name="Updated_Ts", field_type="TIMESTAMP"),
        ]
        client.get_table.return_value = mock_table
        client.list_tables.return_value = [
            SimpleNamespace(dataset_id="raw", table_id="crm"),
            SimpleNamespace(dataset_id="raw", table_id="orders"),
        ]

        adapter = BigQueryAdapter(
            client, project="proj-1", dataset_mapping={"idr_out": "custom_out"}
        )
        assert adapter.table_exists("idr_out.identity_clusters_current") is True
        client.get_table.assert_called_with("proj-1.custom_out.identity_clusters_current")

        cols = adapter.get_table_columns("idr_out.identity_clusters_current")
        assert cols == [
            {"name": "entity_key", "type": "STRING"},
            {"name": "updated_ts", "type": "TIMESTAMP"},
        ]

        tables = adapter.list_tables("raw")
        assert tables == ["raw.crm", "raw.orders"]

        client.get_table.side_effect = RuntimeError("404 Not found")
        assert adapter.table_exists("idr_out.missing") is False


# ============================================================
# 2. SnowflakeAdapter Unit Tests (Snowpark + DB-API Modes)
# ============================================================


class TestSnowflakeAdapterUnit:
    def test_snowpark_session_mode(self):
        session = MagicMock()
        session.create_dataframe = MagicMock()
        sql_df = MagicMock()
        sql_df.to_pandas.return_value = MockDataFrame([{"RULE_ID": "r1", "PRIORITY": 1}])
        sql_df.collect.return_value = [("r1", "VARCHAR")]
        session.sql.return_value = sql_df

        adapter = SnowflakeAdapter(session)
        assert adapter.is_snowpark is True
        assert adapter.dialect == "snowflake"

        adapter.execute("CREATE TABLE idr_work.t1 (id VARCHAR)")
        session.sql.assert_called_with("CREATE TABLE idr_work.t1 (id VARCHAR)")

        rows = adapter.query("SELECT * FROM idr_meta.rule WHERE rule_id = ?", ["r1"])
        assert rows == [{"rule_id": "r1", "priority": 1}]

        one = adapter.query_one("SELECT rule_id FROM idr_meta.rule WHERE rule_id = ?", ["r1"])
        assert one == "r1"

        cols = adapter.get_table_columns("idr_meta.rule")
        assert cols == [{"name": "r1", "type": "VARCHAR"}]

    def test_dbapi_connector_mode_rewrites_qmark_to_pyformat(self):
        conn = MagicMock(spec=["cursor", "close"])
        cursor = MagicMock()
        cursor.description = [("RESOLVED_ID",), ("CLUSTER_SIZE",)]
        cursor.fetchall.return_value = [("cluster_1", 5)]
        cursor.fetchone.return_value = (42,)
        conn.cursor.return_value.__enter__.return_value = cursor

        adapter = SnowflakeAdapter(conn)
        assert adapter.is_snowpark is False

        adapter.execute_script("CREATE SCHEMA a; CREATE SCHEMA b;")
        assert cursor.execute.call_count == 2

        rows = adapter.query(
            "SELECT resolved_id, cluster_size FROM idr_out.identity_clusters_current WHERE resolved_id = ?",
            ["cluster_1"],
        )
        assert rows == [{"resolved_id": "cluster_1", "cluster_size": 5}]
        executed_sql, executed_params = cursor.execute.call_args[0]
        assert "%s" in executed_sql
        assert "?" not in executed_sql
        assert executed_params == ["cluster_1"]

        val = adapter.query_one("SELECT COUNT(*) FROM idr_out.run_history WHERE status = ?", ["SUCCESS"])
        assert val == 42


# ============================================================
# 3. DatabricksAdapter Unit Tests (SparkSession + DB-API Modes)
# ============================================================


class TestDatabricksAdapterUnit:
    def test_spark_session_mode_and_param_binding(self):
        spark = MagicMock(spec=["sql"])
        spark_df = MagicMock()
        spark_df.toPandas.return_value = MockDataFrame([{"ENTITY_KEY": "crm:1", "RESOLVED_ID": "crm:1"}])
        spark_df.collect.return_value = [
            ("entity_key", "string", None),
            ("# Partition Information", "", ""),
            ("# col_name", "data_type", "comment"),
        ]
        spark.sql.return_value = spark_df

        adapter = DatabricksAdapter(spark, catalog="unity_main")
        assert adapter.is_spark is True
        assert adapter.dialect == "databricks"
        spark.sql.assert_called_with("USE CATALOG unity_main")

        rows = adapter.query(
            "SELECT * FROM idr_out.identity_resolved_membership_current WHERE entity_key = ? AND active = ? AND cnt > ?",
            ["O'Reilly", True, 5],
        )
        assert rows == [{"entity_key": "crm:1", "resolved_id": "crm:1"}]
        bound_query = spark.sql.call_args[0][0]
        assert "'O''Reilly'" in bound_query
        assert "TRUE" in bound_query
        assert "5" in bound_query

        # Verify get_table_columns filters out '# Partition Information'
        cols = adapter.get_table_columns("idr_out.identity_resolved_membership_current")
        assert cols == [{"name": "entity_key", "type": "string"}]

    def test_dbapi_connector_mode_binds_qmark_params(self):
        conn = MagicMock(spec=["cursor"])
        cursor = MagicMock()
        cursor.description = [("TABLE_ID",), ("IS_ACTIVE",)]
        cursor.fetchall.return_value = [("crm", True)]
        cursor.fetchone.return_value = ("crm",)
        conn.cursor.return_value.__enter__.return_value = cursor

        adapter = DatabricksAdapter(conn)
        assert adapter.is_spark is False

        rows = adapter.query("SELECT table_id, is_active FROM idr_meta.source_table WHERE table_id = ?", ["crm"])
        assert rows == [{"table_id": "crm", "is_active": True}]
        assert "'crm'" in cursor.execute.call_args[0][0]

        val = adapter.query_one("SELECT table_id FROM idr_meta.source_table WHERE table_id = ?", ["crm"])
        assert val == "crm"

        with pytest.raises(ValueError, match="Invalid schema name"):
            adapter.list_tables("bad_schema; DROP TABLE x")


# ============================================================
# 4. 4-Dialect Stage SQL Generation Verification
# ============================================================


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake", "bigquery", "databricks"])
class TestFourDialectStageSQL:
    def _make_ctx(self, dialect: str):
        adapter = MagicMock()
        adapter.dialect = dialect
        adapter.table_exists.return_value = True
        ctx = StageContext(
            adapter=adapter,
            dialect=get_dialect_config(dialect),
            run_id="run_dialect_test",
            logger=logging.getLogger(f"test-{dialect}"),
            warnings=[],
            generate_evidence=True,
        )
        return ctx, adapter

    def test_extraction_stage_entities_and_pii_hashed_identifiers_incr(self, dialect, monkeypatch):
        monkeypatch.setenv("IDR_HASH_PII", "1")
        ctx, adapter = self._make_ctx(dialect)
        stage = ExtractionStage(ctx)

        # 1. extract_entities in INCR mode with watermark
        adapter.query.return_value = [
            {
                "table_id": "crm",
                "table_fqn": "raw.crm",
                "entity_type": "PERSON",
                "entity_key_expr": "id",
                "watermark_column": "updated_at",
                "lookback": 15,
                "last_watermark_value": "2026-01-01 00:00:00",
            }
        ]
        adapter.query_one.return_value = 10
        stage.extract_entities("INCR")

        # 2. extract_identifiers in INCR mode with PII hashing enabled
        adapter.query.return_value = [
            {
                "table_id": "crm",
                "identifier_type": "email",
                "identifier_value_expr": "email",
                "is_hashed": True,
                "canonicalize": "LOWERCASE",
                "max_group_size": 1000,
            }
        ]
        adapter.query_one.side_effect = ["raw.crm", "id", 10]
        stage.extract_identifiers("INCR")

        executed_sqls = [c.args[0] for c in adapter.execute.call_args_list]
        joined = "\n".join(executed_sqls)

        # Ensure no multi-statement SQL was passed
        for sql in executed_sqls:
            assert ";" not in sql.strip().rstrip(";")

        # Check dialect-specific SHA-256 expression
        if dialect == "bigquery":
            assert "TO_HEX(SHA256(" in joined
        elif dialect in ("snowflake", "databricks"):
            assert "SHA2(" in joined
        elif dialect == "duckdb":
            assert "SHA256(" in joined.upper()

        # Check INCR candidate expansion tables were generated
        assert "idr_work.matched_hist_entities" in joined
        assert "idr_work.expanded_cluster_entities" in joined

    def test_graph_stage_fuzzy_matching_and_dry_run_output_sql(self, dialect):
        ctx, adapter = self._make_ctx(dialect)
        graph = GraphStage(ctx)

        # Configure metadata queries for run_fuzzy_matching
        adapter.query.side_effect = [
            [
                {
                    "rule_id": "fuzzy_1",
                    "fuzzy_name": "name_sim",
                    "blocking_key_expr": "email",
                    "score_expr": "jaro_winkler_similarity(<a>.first_name, <b>.first_name)",
                    "threshold": 0.85,
                    "priority": 1,
                }
            ],
            [{"attribute_name": "first_name"}],
            [{"identifier_type": "email"}],
        ]
        adapter.query_one.side_effect = [5, 0, 2]
        clusters = graph.run_fuzzy_matching(max_iters=5)
        assert clusters == 2

        # Dry run output generation
        output = OutputStage(ctx, config=RunConfig(run_mode="FULL", dry_run=True))
        output.generate_dry_run_output()

        for call in adapter.execute.call_args_list:
            sql = call.args[0].strip().rstrip(";")
            assert ";" not in sql, f"Multi-statement SQL found for {dialect}: {sql}"
            if dialect == "bigquery":
                assert "AS FLOAT)" not in sql
