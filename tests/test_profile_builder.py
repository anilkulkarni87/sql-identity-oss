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
        assert "NULL AS recency_ts" in result
