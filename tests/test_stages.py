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
