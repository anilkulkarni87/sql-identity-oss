"""Consolidated test suite: test_stages.py, test_output_stage.py, test_profile_builder.py."""

# --- Consolidated from test_stages.py ---
"""
Tests for idr_core.stages — StageContext, BaseStage, and stage helpers.

Unit tests for validation, context construction, and helper methods
in the new stage decomposition.
"""

import logging
import os
import sys
from unittest.mock import MagicMock

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from idr_core.adapters.base import get_dialect_config
from idr_core.stages.base import BaseStage, StageContext

# ============================================================
# Fixtures
# ============================================================


def make_context(dialect="duckdb", run_id="test-run-001"):
    """Create a StageContext with a mock adapter."""
    adapter = MagicMock()
    adapter.dialect = dialect
    return StageContext(
        adapter=adapter,
        dialect=get_dialect_config(dialect),
        run_id=run_id,
        logger=logging.getLogger("test"),
        warnings=[],
        generate_evidence=False,
    )


@pytest.fixture
def ctx():
    return make_context()


@pytest.fixture
def stage(ctx):
    return BaseStage(ctx)


# ============================================================
# StageContext
# ============================================================


class TestStageContext:
    """Tests for StageContext dataclass."""

    def test_construction(self, ctx):
        assert ctx.run_id == "test-run-001"
        assert ctx.adapter is not None
        assert isinstance(ctx.warnings, list)
        assert ctx.generate_evidence is False

    def test_dialect_is_dict(self, ctx):
        assert isinstance(ctx.dialect, dict)
        assert "string_type" in ctx.dialect

    def test_warnings_are_mutable(self, ctx):
        ctx.warnings.append("test warning")
        assert len(ctx.warnings) == 1


# ============================================================
# BaseStage.__init__
# ============================================================


class TestBaseStageInit:
    """Tests for BaseStage constructor."""

    def test_delegates_from_context(self, stage, ctx):
        assert stage.adapter is ctx.adapter
        assert stage.run_id == ctx.run_id
        assert stage.logger is ctx.logger
        assert stage._warnings is ctx.warnings

    def test_dialect_config_accessible(self, stage):
        assert isinstance(stage._dialect, dict)
        assert "string_type" in stage._dialect


# ============================================================
# BaseStage._validate_metadata_value — identifier
# ============================================================


class TestValidateIdentifier:
    """Tests for _validate_metadata_value with type='identifier'."""

    def test_valid_identifier(self, stage):
        assert stage._validate_metadata_value("email", "identifier", "test") == "email"

    def test_valid_underscore_identifier(self, stage):
        assert stage._validate_metadata_value("user_id", "identifier", "test") == "user_id"

    def test_invalid_identifier_with_spaces(self, stage):
        with pytest.raises(RuntimeError, match="Metadata validation failed"):
            stage._validate_metadata_value("user id", "identifier", "test")

    def test_invalid_identifier_sql_injection(self, stage):
        with pytest.raises(RuntimeError):
            stage._validate_metadata_value("email; DROP TABLE", "identifier", "test")

    def test_empty_identifier(self, stage):
        with pytest.raises(RuntimeError):
            stage._validate_metadata_value("", "identifier", "test")


# ============================================================
# BaseStage._validate_metadata_value — fqn
# ============================================================


class TestValidateFqn:
    """Tests for _validate_metadata_value with type='fqn'."""

    def test_two_part_fqn(self, stage):
        assert stage._validate_metadata_value("schema.table", "fqn", "test") == "schema.table"

    def test_three_part_fqn(self, stage):
        assert (
            stage._validate_metadata_value("catalog.schema.table", "fqn", "test")
            == "catalog.schema.table"
        )

    def test_invalid_fqn(self, stage):
        with pytest.raises(RuntimeError):
            stage._validate_metadata_value("schema.table; DROP", "fqn", "test")


# ============================================================
# BaseStage._validate_metadata_value — expr
# ============================================================


class TestValidateExpr:
    """Tests for _validate_metadata_value with type='expr'."""

    def test_simple_column_expr(self, stage):
        assert stage._validate_metadata_value("email", "expr", "test") == "email"

    def test_function_expr(self, stage):
        assert stage._validate_metadata_value("LOWER(email)", "expr", "test") == "LOWER(email)"

    def test_complex_expr(self, stage):
        assert (
            stage._validate_metadata_value("CONCAT(first_name, ' ', last_name)", "expr", "test")
            == "CONCAT(first_name, ' ', last_name)"
        )

    def test_injection_in_expr(self, stage):
        with pytest.raises(RuntimeError):
            stage._validate_metadata_value("id; DROP TABLE users", "expr", "test")


# ============================================================
# BaseStage._validate_metadata_value — integer
# ============================================================


class TestValidateInteger:
    """Tests for _validate_metadata_value with type='integer'."""

    def test_int_value(self, stage):
        assert stage._validate_metadata_value(42, "integer", "test") == 42

    def test_string_int(self, stage):
        assert stage._validate_metadata_value("100", "integer", "test") == 100

    def test_float_coerced(self, stage):
        assert stage._validate_metadata_value(10.0, "integer", "test") == 10

    def test_invalid_string(self, stage):
        with pytest.raises(RuntimeError):
            stage._validate_metadata_value("abc", "integer", "test")


# ============================================================
# BaseStage._validate_metadata_value — float
# ============================================================


class TestValidateFloat:
    """Tests for _validate_metadata_value with type='float'."""

    def test_float_value(self, stage):
        assert stage._validate_metadata_value(0.85, "float", "test") == 0.85

    def test_string_float(self, stage):
        assert stage._validate_metadata_value("0.95", "float", "test") == 0.95

    def test_invalid_float(self, stage):
        with pytest.raises(RuntimeError):
            stage._validate_metadata_value("not_a_number", "float", "test")


# ============================================================
# BaseStage._validate_metadata_value — enum
# ============================================================


class TestValidateEnum:
    """Tests for _validate_metadata_value with type='enum' (canonicalize)."""

    def test_lowercase(self, stage):
        assert stage._validate_metadata_value("LOWERCASE", "enum", "canonicalize") == "LOWERCASE"

    def test_uppercase(self, stage):
        assert stage._validate_metadata_value("UPPERCASE", "enum", "canonicalize") == "UPPERCASE"

    def test_none_string(self, stage):
        assert stage._validate_metadata_value("NONE", "enum", "canonicalize") == "NONE"

    def test_empty_string(self, stage):
        assert stage._validate_metadata_value("", "enum", "canonicalize") == ""

    def test_invalid_enum(self, stage):
        with pytest.raises(RuntimeError, match="Metadata validation failed"):
            stage._validate_metadata_value("INVALID", "enum", "canonicalize")

    def test_case_insensitive_input(self, stage):
        assert stage._validate_metadata_value("lowercase", "enum", "canonicalize") == "LOWERCASE"


# ============================================================
# BaseStage._validate_metadata_value — unknown type
# ============================================================


class TestValidateUnknownType:
    """Tests for _validate_metadata_value with unknown type."""

    def test_unknown_type_raises(self, stage):
        with pytest.raises(RuntimeError):
            stage._validate_metadata_value("value", "unknown_type", "test")


# ============================================================
# Multi-dialect context creation
# ============================================================


class TestMultiDialectContext:
    """Tests that StageContext works across dialects."""

    @pytest.mark.parametrize("dialect", ["duckdb", "snowflake", "bigquery", "databricks"])
    def test_context_creation(self, dialect):
        ctx = make_context(dialect=dialect)
        assert ctx.dialect["string_type"] is not None

    @pytest.mark.parametrize("dialect", ["duckdb", "snowflake", "bigquery", "databricks"])
    def test_stage_creation(self, dialect):
        ctx = make_context(dialect=dialect)
        stage = BaseStage(ctx)
        assert stage._dialect is ctx.dialect

    @pytest.mark.parametrize("dialect", ["duckdb", "snowflake", "bigquery", "databricks"])
    def test_validate_works_across_dialects(self, dialect):
        ctx = make_context(dialect=dialect)
        stage = BaseStage(ctx)
        assert stage._validate_metadata_value("valid_id", "identifier", "test") == "valid_id"


# ============================================================
# Error message quality
# ============================================================


class TestErrorMessages:
    """Tests that error messages are informative."""

    def test_runtime_error_includes_field_name(self, stage):
        with pytest.raises(RuntimeError, match="source_table.table_id"):
            stage._validate_metadata_value("bad id!", "identifier", "source_table.table_id")

    def test_runtime_error_wraps_value_error(self, stage):
        with pytest.raises(RuntimeError) as exc_info:
            stage._validate_metadata_value("", "identifier", "test")
        assert exc_info.value.__cause__ is not None
        assert isinstance(exc_info.value.__cause__, ValueError)


# --- Consolidated from test_output_stage.py ---
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


# --- Consolidated from test_profile_builder.py ---
"""
Tests for idr_core.profile_builder module.

Tests ProfileBuilder._build_column_selection() across dialects
and _build_staging_cte() CTE generation.
"""

import os
import sys
from unittest.mock import MagicMock

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from idr_core.profile_builder import ProfileBuilder

# ============================================================
# Fixtures
# ============================================================


def make_mock_adapter(dialect="duckdb"):
    """Create a mock adapter with the given dialect."""
    adapter = MagicMock()
    adapter.dialect = dialect
    return adapter


@pytest.fixture
def duckdb_builder():
    return ProfileBuilder(make_mock_adapter("duckdb"))


@pytest.fixture
def snowflake_builder():
    return ProfileBuilder(make_mock_adapter("snowflake"))


@pytest.fixture
def bigquery_builder():
    return ProfileBuilder(make_mock_adapter("bigquery"))


@pytest.fixture
def databricks_builder():
    return ProfileBuilder(make_mock_adapter("databricks"))


# ============================================================
# _build_column_selection() — RECENCY strategy
# ============================================================


class TestColumnSelectionRecency:
    """Tests for RECENCY survivorship strategy across dialects."""

    def test_duckdb_uses_arg_max(self, duckdb_builder):
        rule = {"attribute_name": "email", "strategy": "RECENCY"}
        result = duckdb_builder._build_column_selection(rule)
        assert "arg_max" in result
        assert "email" in result
        assert "recency_ts" in result

    def test_snowflake_uses_max_by(self, snowflake_builder):
        rule = {"attribute_name": "email", "strategy": "RECENCY"}
        result = snowflake_builder._build_column_selection(rule)
        assert "MAX_BY" in result
        assert "email" in result

    def test_databricks_uses_max_by(self, databricks_builder):
        rule = {"attribute_name": "email", "strategy": "RECENCY"}
        result = databricks_builder._build_column_selection(rule)
        assert "MAX_BY" in result
        assert "email" in result

    def test_bigquery_uses_array_agg(self, bigquery_builder):
        rule = {"attribute_name": "email", "strategy": "RECENCY"}
        result = bigquery_builder._build_column_selection(rule)
        assert "ARRAY_AGG" in result
        assert "ORDER BY recency_ts DESC" in result
        assert "SAFE_OFFSET(0)" in result


# ============================================================
# _build_column_selection() — PRIORITY strategy
# ============================================================


class TestColumnSelectionPriority:
    """Tests for PRIORITY survivorship strategy."""

    def test_duckdb_uses_arg_min(self, duckdb_builder):
        rule = {
            "attribute_name": "first_name",
            "strategy": "PRIORITY",
            "source_priority_list": '["crm", "web"]',
        }
        result = duckdb_builder._build_column_selection(rule)
        assert "arg_min" in result
        assert "first_name" in result
        assert "CASE" in result
        assert "crm" in result

    def test_snowflake_uses_min_by(self, snowflake_builder):
        rule = {
            "attribute_name": "first_name",
            "strategy": "PRIORITY",
            "source_priority_list": '["crm", "web"]',
        }
        result = snowflake_builder._build_column_selection(rule)
        assert "MIN_BY" in result
        assert "CASE" in result

    def test_databricks_uses_min_by(self, databricks_builder):
        rule = {
            "attribute_name": "first_name",
            "strategy": "PRIORITY",
            "source_priority_list": '["crm", "web"]',
        }
        result = databricks_builder._build_column_selection(rule)
        assert "MIN_BY" in result
        assert "CASE" in result

    def test_bigquery_priority(self, bigquery_builder):
        rule = {
            "attribute_name": "first_name",
            "strategy": "PRIORITY",
            "source_priority_list": '["crm"]',
        }
        result = bigquery_builder._build_column_selection(rule)
        assert "ARRAY_AGG" in result
        assert "ORDER BY" in result

    def test_empty_priority_list_fallback(self, duckdb_builder):
        rule = {
            "attribute_name": "name",
            "strategy": "PRIORITY",
            "source_priority_list": "[]",
        }
        result = duckdb_builder._build_column_selection(rule)
        # Should use 999 as fallback (no CASE)
        assert "999" in result

    def test_priority_order_is_correct(self, duckdb_builder):
        rule = {
            "attribute_name": "addr",
            "strategy": "PRIORITY",
            "source_priority_list": '["crm", "web", "app"]',
        }
        result = duckdb_builder._build_column_selection(rule)
        # crm should be index 0 (highest priority), web=1, app=2
        assert "WHEN source_id = 'crm' THEN 0" in result
        assert "WHEN source_id = 'web' THEN 1" in result
        assert "WHEN source_id = 'app' THEN 2" in result


# ============================================================
# _build_column_selection() — FREQUENCY strategy
# ============================================================


class TestColumnSelectionFrequency:
    """Tests for FREQUENCY survivorship strategy."""

    def test_duckdb_uses_mode(self, duckdb_builder):
        rule = {"attribute_name": "city", "strategy": "FREQUENCY"}
        result = duckdb_builder._build_column_selection(rule)
        assert "mode(" in result.lower()
        assert "city" in result

    def test_snowflake_uses_mode(self, snowflake_builder):
        rule = {"attribute_name": "city", "strategy": "FREQUENCY"}
        result = snowflake_builder._build_column_selection(rule)
        assert "MODE(" in result

    def test_databricks_uses_mode(self, databricks_builder):
        rule = {"attribute_name": "city", "strategy": "FREQUENCY"}
        result = databricks_builder._build_column_selection(rule)
        assert "MODE(" in result

    def test_bigquery_uses_approx_top_count(self, bigquery_builder):
        rule = {"attribute_name": "city", "strategy": "FREQUENCY"}
        result = bigquery_builder._build_column_selection(rule)
        assert "APPROX_TOP_COUNT" in result


# ============================================================
# _build_column_selection() — Unknown strategy
# ============================================================


class TestColumnSelectionFallback:
    """Tests for unknown/default strategy."""

    def test_unknown_strategy_falls_back_to_max(self, duckdb_builder):
        rule = {"attribute_name": "zip", "strategy": "UNKNOWN_STRATEGY"}
        result = duckdb_builder._build_column_selection(rule)
        assert "MAX(zip)" in result

    def test_result_has_alias(self, duckdb_builder):
        rule = {"attribute_name": "phone", "strategy": "RECENCY"}
        result = duckdb_builder._build_column_selection(rule)
        assert "AS phone" in result


# ============================================================
# _build_staging_cte()
# ============================================================


class TestBuildStagingCte:
    """Tests for staging CTE generation."""

    def test_generates_union_all_for_multiple_sources(self, duckdb_builder):
        rules = [{"attribute_name": "email", "strategy": "RECENCY"}]
        mappings = [
            {"table_id": "crm", "attribute_name": "email", "attribute_expr": "email_col"},
        ]
        sources = [
            {
                "table_id": "crm",
                "table_fqn": "raw.crm_contacts",
                "entity_key_expr": "id",
                "watermark_column": "updated_at",
            },
            {
                "table_id": "web",
                "table_fqn": "raw.web_events",
                "entity_key_expr": "user_id",
                "watermark_column": "event_time",
            },
        ]
        result = duckdb_builder._build_staging_cte(rules, mappings, sources)
        assert "UNION ALL" in result
        assert "raw.crm_contacts" in result
        assert "raw.web_events" in result

    def test_null_for_unmapped_attributes(self, duckdb_builder):
        rules = [
            {"attribute_name": "email", "strategy": "RECENCY"},
            {"attribute_name": "phone", "strategy": "RECENCY"},
        ]
        mappings = [
            {"table_id": "crm", "attribute_name": "email", "attribute_expr": "email_col"},
            # phone NOT mapped for crm
        ]
        sources = [
            {
                "table_id": "crm",
                "table_fqn": "raw.crm",
                "entity_key_expr": "id",
                "watermark_column": "ts",
            },
        ]
        result = duckdb_builder._build_staging_cte(rules, mappings, sources)
        assert "NULL AS phone" in result
        assert "email_col AS email" in result

    def test_empty_sources_returns_empty_string(self, duckdb_builder):
        result = duckdb_builder._build_staging_cte([], [], [])
        assert result == ""

    def test_source_without_watermark_uses_null(self, duckdb_builder):
        rules = [{"attribute_name": "email", "strategy": "RECENCY"}]
        mappings = [
            {"table_id": "s1", "attribute_name": "email", "attribute_expr": "email"},
        ]
        sources = [
            {
                "table_id": "s1",
                "table_fqn": "raw.s1",
                "entity_key_expr": "id",
                "watermark_column": None,
            },
        ]
        result = duckdb_builder._build_staging_cte(rules, mappings, sources)
        assert "NULL AS" in result or "CAST(NULL AS" in result
        assert "AS recency_ts" in result


# ============================================================
# Incremental Golden Profile Upsert & Staging
# ============================================================


class TestIncrementalProfileBuilder:
    """Tests for incremental golden profile building and local subgraph filtering."""

    def test_incremental_staging_cte_filters_by_impacted_clusters(self, duckdb_builder):
        rules = [{"attribute_name": "email", "strategy": "RECENCY"}]
        mappings = [{"table_id": "crm", "attribute_name": "email", "attribute_expr": "email"}]
        sources = [{
            "table_id": "crm",
            "table_fqn": "raw.crm",
            "entity_key_expr": "id",
            "watermark_column": "ts",
        }]

        full_sql = duckdb_builder._build_staging_cte(rules, mappings, sources, is_incremental=False)
        assert "WHERE m.resolved_id IN" not in full_sql

        incr_sql = duckdb_builder._build_staging_cte(rules, mappings, sources, is_incremental=True)
        assert "WHERE m.resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids)" in incr_sql

    @pytest.mark.parametrize("dialect,expected_keyword", [
        ("duckdb", "ON CONFLICT (resolved_id) DO UPDATE SET"),
        ("snowflake", "MERGE INTO idr_out.golden_profiles T"),
        ("databricks", "MERGE INTO idr_out.golden_profiles T"),
        ("bigquery", "MERGE INTO idr_out.golden_profiles T"),
    ])
    def test_incremental_build_profiles_uses_upsert_and_cleans_phantoms(self, dialect, expected_keyword):
        adapter = make_mock_adapter(dialect)
        adapter.query.side_effect = [
            [{"attribute_name": "email", "strategy": "RECENCY", "is_active": True}],  # rules
            [{"table_id": "crm", "attribute_name": "email", "attribute_expr": "email"}],  # mappings
            [{"table_id": "crm", "table_fqn": "raw.crm", "entity_key_expr": "id", "watermark_column": "ts", "is_active": True}],  # sources
        ]
        adapter.table_exists.return_value = True

        builder = ProfileBuilder(adapter)
        builder.build_profiles(run_id="run_incr_test", run_mode="INCR")

        executed_sqls = "\n".join(call.args[0] for call in adapter.execute.call_args_list)
        assert expected_keyword in executed_sqls
        assert "golden_profiles_delta" in executed_sqls
        assert "DELETE FROM idr_out.golden_profiles" in executed_sqls
