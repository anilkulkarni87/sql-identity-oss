"""
Tests for idr_core.stages.output.OutputStage and multi-dialect stage SQL generation.
"""

import logging
import os
import sys
from unittest.mock import MagicMock

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from idr_core.adapters.base import get_dialect_config
from idr_core.runner import RunConfig
from idr_core.stages.base import StageContext
from idr_core.stages.graph import GraphStage
from idr_core.stages.output import OutputStage
from idr_core.stages.preflight import PreflightStage


def make_output_stage(dialect: str = "snowflake") -> tuple[OutputStage, MagicMock]:
    adapter = MagicMock()
    adapter.dialect = dialect
    # No survivorship rules -> profile builder exits quickly.
    adapter.query.return_value = []
    ctx = StageContext(
        adapter=adapter,
        dialect=get_dialect_config(dialect),
        run_id="run_test_output",
        logger=logging.getLogger("test-output"),
        warnings=[],
        generate_evidence=False,
    )
    stage = OutputStage(ctx, config=RunConfig(run_mode="INCR"))
    return stage, adapter


def test_non_duckdb_edge_merge_has_single_not_matched_clause():
    stage, adapter = make_output_stage("snowflake")
    stage.generate_output()

    edge_merge_sql = [
        call.args[0]
        for call in adapter.execute.call_args_list
        if "MERGE INTO idr_out.identity_edges_current" in call.args[0]
    ]

    assert len(edge_merge_sql) == 1
    assert edge_merge_sql[0].upper().count("WHEN NOT MATCHED THEN") == 1


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake", "bigquery", "databricks"])
def test_output_stage_executes_single_statements_per_call(dialect):
    """Ensure OutputStage never passes semicolon-separated multi-statement SQL to adapter.execute()."""
    stage, adapter = make_output_stage(dialect)
    stage.generate_output()

    for call in adapter.execute.call_args_list:
        sql = call.args[0].strip().rstrip(";")
        assert ";" not in sql, f"Multi-statement SQL found for {dialect}: {sql}"


def test_bigquery_confidence_score_uses_float64_and_concat():
    stage, adapter = make_output_stage("bigquery")
    stage.compute_confidence_scores()

    executed_sqls = "\n".join(call.args[0] for call in adapter.execute.call_args_list)
    assert "AS FLOAT64" in executed_sqls
    assert "AS FLOAT)" not in executed_sqls
    assert "CONCAT(" in executed_sqls


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake", "bigquery", "databricks"])
def test_confidence_scoring_fidelity_and_corroboration_sql(dialect):
    """Verify confidence scoring includes identifier fidelity and tree connectivity across all dialects."""
    stage, adapter = make_output_stage(dialect)
    stage.compute_confidence_scores()

    executed_sqls = "\n".join(call.args[0] for call in adapter.execute.call_args_list)
    assert "max_fidelity" in executed_sqls
    assert "connectivity_ratio" in executed_sqls
    assert "SINGLETON_NO_MATCH_REQUIRED" in executed_sqls
    assert "High Confidence Corroboration" in executed_sqls


@pytest.mark.parametrize(
    "dialect,expected_target",
    [
        ("duckdb", "RENAME TO lp_labels"),
        ("snowflake", "RENAME TO idr_work.lp_labels"),
        ("databricks", "RENAME TO idr_work.lp_labels"),
    ],
)
def test_graph_stage_swap_tables_rename_target(dialect, expected_target):
    adapter = MagicMock()
    adapter.dialect = dialect
    ctx = StageContext(
        adapter=adapter,
        dialect=get_dialect_config(dialect),
        run_id="run_test_graph",
        logger=logging.getLogger("test-graph"),
        warnings=[],
    )
    graph = GraphStage(ctx)
    graph._swap_tables("idr_work.lp_labels", "idr_work.lp_labels_new")

    executed_sqls = [call.args[0] for call in adapter.execute.call_args_list]
    assert any(expected_target in s for s in executed_sqls)


def test_graph_stage_swap_tables_bigquery_uses_ctas():
    adapter = MagicMock()
    adapter.dialect = "bigquery"
    ctx = StageContext(
        adapter=adapter,
        dialect=get_dialect_config("bigquery"),
        run_id="run_test_graph_bq",
        logger=logging.getLogger("test-graph"),
        warnings=[],
    )
    graph = GraphStage(ctx)
    graph._swap_tables("idr_work.lp_labels", "idr_work.lp_labels_new")

    executed_sqls = [call.args[0] for call in adapter.execute.call_args_list]
    assert any("CREATE OR REPLACE TABLE idr_work.lp_labels AS SELECT *" in s for s in executed_sqls)


@pytest.mark.parametrize(
    "dialect,expected_time_expr",
    [
        ("duckdb", "INTERVAL '4 hours'"),
        ("snowflake", "DATEADD('hour', -4, CURRENT_TIMESTAMP())"),
        ("databricks", "TIMESTAMPADD(HOUR, -4, CURRENT_TIMESTAMP())"),
        ("bigquery", "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 HOUR)"),
    ],
)
def test_preflight_stage_dialect_interval_syntax(dialect, expected_time_expr):
    adapter = MagicMock()
    adapter.dialect = dialect
    adapter.table_exists.return_value = True
    adapter.get_table_columns.return_value = [{"name": "config_hash"}]
    adapter.query.side_effect = [
        [],  # running runs check
        [{"table_id": "crm", "table_fqn": "raw.crm"}],  # active sources
        [{"cnt": 1}],  # active rules
        [],  # identifier mappings
    ]
    ctx = StageContext(
        adapter=adapter,
        dialect=get_dialect_config(dialect),
        run_id="run_test_preflight",
        logger=logging.getLogger("test-preflight"),
        warnings=[],
    )
    preflight = PreflightStage(ctx)
    preflight.run()

    executed_sqls = "\n".join(call.args[0] for call in adapter.execute.call_args_list)
    assert expected_time_expr in executed_sqls
