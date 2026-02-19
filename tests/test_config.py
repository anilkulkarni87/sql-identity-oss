"""
Tests for idr_core.config module.

Tests config_to_sql() round-trip, multi-dialect SQL generation,
and validate_config() error handling.
"""

import os
import sys

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from idr_core.config import config_to_sql, validate_config

# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def minimal_config():
    """Minimal valid config with one source and one rule."""
    return {
        "sources": [
            {
                "id": "customers",
                "table": "raw.customers",
                "entity_key": "id",
                "watermark_column": "updated_at",
                "identifiers": [
                    {"type": "email", "expr": "email"},
                ],
                "attributes": [
                    {"name": "first_name", "expr": "first_name"},
                ],
            }
        ],
        "rules": [
            {
                "id": 1,
                "type": "EXACT",
                "match_keys": ["email"],
                "canonicalize": "LOWERCASE",
                "max_group_size": 100,
                "priority": 10,
            }
        ],
    }


@pytest.fixture
def full_config():
    """Full config with multiple sources, rules, exclusions, fuzzy, survivorship."""
    return {
        "schema_source": "public",
        "sources": [
            {
                "id": "crm",
                "table": "crm_contacts",
                "entity_key": "contact_id",
                "watermark_column": "modified_at",
                "identifiers": [
                    {"type": "email", "expr": "email_address"},
                    {"type": "phone", "expr": "phone_number"},
                ],
                "attributes": [
                    {"name": "first_name", "expr": "fname"},
                    {"name": "last_name", "expr": "lname"},
                ],
            },
            {
                "id": "web",
                "table": "web_events",
                "entity_key": "user_id",
                "watermark_column": "event_time",
                "identifiers": [
                    {"type": "email", "expr": "login_email"},
                ],
                "attributes": [],
            },
        ],
        "rules": [
            {
                "id": "rule_email",
                "type": "EXACT",
                "match_keys": ["email"],
                "canonicalize": "LOWERCASE",
                "max_group_size": 5000,
                "priority": 10,
            },
            {
                "id": "rule_phone",
                "identifier_type": "phone",
                "canonicalize": "NONE",
                "max_group_size": 100,
                "priority": 20,
            },
        ],
        "exclusions": [
            {
                "type": "email",
                "value": "test@example.com",
                "match": "EXACT",
                "reason": "Test email",
            }
        ],
        "fuzzy_rules": [
            {
                "id": "fuzzy_name",
                "name": "Name Similarity",
                "blocking_key": "LEFT(first_name, 3)",
                "score_expr": "jaro_winkler_similarity(<a>.first_name, <b>.first_name)",
                "threshold": 0.85,
                "priority": 50,
            }
        ],
        "survivorship": [
            {
                "attribute": "email",
                "strategy": "RECENCY",
                "recency_field": "modified_at",
            },
            {
                "attribute": "first_name",
                "strategy": "PRIORITY",
                "source_priority": ["crm", "web"],
            },
        ],
    }


# ============================================================
# config_to_sql() - DuckDB dialect
# ============================================================


class TestConfigToSqlDuckDB:
    """Tests for config_to_sql() with DuckDB dialect."""

    def test_minimal_config_generates_sql(self, minimal_config):
        stmts = config_to_sql(minimal_config, dialect="duckdb")
        assert len(stmts) > 0

    def test_source_table_insert(self, minimal_config):
        stmts = config_to_sql(minimal_config, dialect="duckdb")
        source_stmts = [s for s in stmts if "idr_meta.source_table" in s]
        assert len(source_stmts) >= 1
        assert "customers" in source_stmts[0]
        assert "ON CONFLICT" in source_stmts[0]

    def test_identifier_mapping_insert(self, minimal_config):
        stmts = config_to_sql(minimal_config, dialect="duckdb")
        id_stmts = [s for s in stmts if "idr_meta.identifier_mapping" in s]
        assert len(id_stmts) == 1
        assert "email" in id_stmts[0]

    def test_attribute_mapping_insert(self, minimal_config):
        stmts = config_to_sql(minimal_config, dialect="duckdb")
        attr_stmts = [s for s in stmts if "idr_meta.entity_attribute_mapping" in s]
        assert len(attr_stmts) == 1
        assert "first_name" in attr_stmts[0]

    def test_rule_insert(self, minimal_config):
        stmts = config_to_sql(minimal_config, dialect="duckdb")
        rule_stmts = [s for s in stmts if "idr_meta.rule" in s]
        assert len(rule_stmts) == 1
        assert "LOWERCASE" in rule_stmts[0]

    def test_full_config_covers_all_tables(self, full_config):
        stmts = config_to_sql(full_config, dialect="duckdb")
        joined = "\n".join(stmts)
        assert "idr_meta.source_table" in joined
        assert "idr_meta.identifier_mapping" in joined
        assert "idr_meta.entity_attribute_mapping" in joined
        assert "idr_meta.rule" in joined
        assert "idr_meta.identifier_exclusion" in joined
        assert "idr_meta.fuzzy_rule" in joined
        assert "idr_meta.survivorship_rule" in joined

    def test_exclusion_insert(self, full_config):
        stmts = config_to_sql(full_config, dialect="duckdb")
        excl_stmts = [s for s in stmts if "idr_meta.identifier_exclusion" in s]
        assert len(excl_stmts) == 1
        assert "test@example.com" in excl_stmts[0]

    def test_fuzzy_rule_insert(self, full_config):
        stmts = config_to_sql(full_config, dialect="duckdb")
        fuzzy_stmts = [s for s in stmts if "idr_meta.fuzzy_rule" in s]
        assert len(fuzzy_stmts) == 1
        assert "0.85" in fuzzy_stmts[0]

    def test_survivorship_insert(self, full_config):
        stmts = config_to_sql(full_config, dialect="duckdb")
        surv_stmts = [s for s in stmts if "idr_meta.survivorship_rule" in s]
        assert len(surv_stmts) == 2

    def test_multiple_sources(self, full_config):
        stmts = config_to_sql(full_config, dialect="duckdb")
        source_stmts = [s for s in stmts if "idr_meta.source_table" in s]
        assert len(source_stmts) == 2

    def test_multiple_identifiers_per_source(self, full_config):
        stmts = config_to_sql(full_config, dialect="duckdb")
        id_stmts = [s for s in stmts if "idr_meta.identifier_mapping" in s]
        # crm has 2 identifiers + web has 1 => 3
        assert len(id_stmts) == 3


# ============================================================
# config_to_sql() - Snowflake dialect (MERGE)
# ============================================================


class TestConfigToSqlSnowflake:
    """Tests for config_to_sql() with Snowflake dialect (MERGE syntax)."""

    def test_uses_merge_syntax(self, minimal_config):
        stmts = config_to_sql(minimal_config, dialect="snowflake")
        assert any("MERGE INTO" in s for s in stmts)

    def test_merge_has_matched_and_not_matched(self, minimal_config):
        stmts = config_to_sql(minimal_config, dialect="snowflake")
        source_stmts = [s for s in stmts if "idr_meta.source_table" in s]
        assert len(source_stmts) >= 1
        stmt = source_stmts[0]
        assert "WHEN MATCHED" in stmt
        assert "WHEN NOT MATCHED" in stmt

    def test_merge_join_condition(self, minimal_config):
        stmts = config_to_sql(minimal_config, dialect="snowflake")
        source_stmts = [s for s in stmts if "idr_meta.source_table" in s]
        assert "tgt.table_id = src.table_id" in source_stmts[0]


# ============================================================
# config_to_sql() - BigQuery dialect (backslash escaping)
# ============================================================


class TestConfigToSqlBigQuery:
    """Tests for config_to_sql() with BigQuery dialect."""

    def test_uses_merge_syntax(self, minimal_config):
        stmts = config_to_sql(minimal_config, dialect="bigquery")
        assert any("MERGE INTO" in s for s in stmts)

    def test_bigquery_escaping(self):
        """BigQuery uses backslash escaping for single quotes."""
        config = {
            "sources": [
                {
                    "id": "test_src",
                    "table": "raw.test_table",
                    "entity_key": "id",
                    "watermark_column": "updated_at",
                    "identifiers": [],
                    "attributes": [],
                }
            ],
            "rules": [],
            "exclusions": [
                {
                    "type": "email",
                    "value": "o'brien@test.com",
                    "match": "EXACT",
                    "reason": "Name with apostrophe",
                }
            ],
        }
        stmts = config_to_sql(config, dialect="bigquery")
        excl_stmts = [s for s in stmts if "identifier_exclusion" in s]
        assert len(excl_stmts) == 1
        # BigQuery escapes with backslash
        assert "\\" in excl_stmts[0]


# ============================================================
# config_to_sql() - Edge cases
# ============================================================


class TestConfigToSqlEdgeCases:
    """Edge cases for config_to_sql()."""

    def test_empty_sources(self):
        config = {"sources": [], "rules": []}
        stmts = config_to_sql(config, dialect="duckdb")
        assert stmts == []

    def test_source_with_no_identifiers_infers_from_rules(self):
        """When identifiers are empty, they should be inferred from exact rules."""
        config = {
            "sources": [
                {
                    "id": "s1",
                    "table": "raw.s1",
                    "entity_key": "id",
                    "watermark_column": "ts",
                    "identifiers": [],
                    "attributes": [{"name": "email", "expr": "email_col"}],
                }
            ],
            "rules": [
                {
                    "id": 1,
                    "type": "EXACT",
                    "match_keys": ["email"],
                    "canonicalize": "LOWERCASE",
                    "max_group_size": 100,
                    "priority": 10,
                }
            ],
        }
        stmts = config_to_sql(config, dialect="duckdb")
        id_stmts = [s for s in stmts if "idr_meta.identifier_mapping" in s]
        assert len(id_stmts) >= 1
        assert "email" in id_stmts[0]

    def test_schema_source_prefixed_to_table(self):
        config = {
            "schema_source": "myschema",
            "sources": [
                {
                    "id": "t1",
                    "table": "table1",
                    "entity_key": "id",
                    "watermark_column": "ts",
                    "identifiers": [],
                    "attributes": [],
                }
            ],
            "rules": [],
        }
        stmts = config_to_sql(config, dialect="duckdb")
        source_stmts = [s for s in stmts if "idr_meta.source_table" in s]
        assert "myschema.table1" in source_stmts[0]

    def test_boolean_values_rendered_as_uppercase(self, minimal_config):
        stmts = config_to_sql(minimal_config, dialect="duckdb")
        source_stmts = [s for s in stmts if "idr_meta.source_table" in s]
        assert "TRUE" in source_stmts[0]

    def test_fuzzy_rule_missing_id_raises(self):
        config = {
            "sources": [],
            "rules": [],
            "fuzzy_rules": [
                {
                    "name": "no-id",
                    "blocking_key": "x",
                    "score_expr": "y",
                    "threshold": 0.9,
                }
            ],
        }
        with pytest.raises(ValueError, match="missing"):
            config_to_sql(config, dialect="duckdb")

    def test_dict_score_expr_resolves_dialect(self):
        config = {
            "sources": [],
            "rules": [],
            "fuzzy_rules": [
                {
                    "id": "f1",
                    "blocking_key": "LEFT(first_name, 3)",
                    "score_expr": {
                        "duckdb": "jaro_winkler(a, b)",
                        "snowflake": "JAROWINKLER_SIMILARITY(a, b) / 100.0",
                        "default": "0.0",
                    },
                    "threshold": 0.85,
                }
            ],
        }
        duckdb_stmts = config_to_sql(config, dialect="duckdb")
        fuzzy_stmts = [s for s in duckdb_stmts if "idr_meta.fuzzy_rule" in s]
        assert "jaro_winkler(a, b)" in fuzzy_stmts[0]

        sf_stmts = config_to_sql(config, dialect="snowflake")
        fuzzy_stmts_sf = [s for s in sf_stmts if "idr_meta.fuzzy_rule" in s]
        assert "JAROWINKLER_SIMILARITY" in fuzzy_stmts_sf[0]


# ============================================================
# validate_config()
# ============================================================


class TestValidateConfig:
    """Tests for validate_config()."""

    def test_valid_config_passes(self, minimal_config):
        assert validate_config(minimal_config) is True

    def test_missing_sources_raises(self):
        with pytest.raises(ValueError, match="sources"):
            validate_config({"rules": []})

    def test_source_missing_id_raises(self):
        with pytest.raises(ValueError):
            validate_config(
                {
                    "sources": [{"table": "raw.t1"}],
                    "rules": [],
                }
            )

    def test_source_missing_table_raises(self):
        with pytest.raises(ValueError):
            validate_config(
                {
                    "sources": [{"id": "s1"}],
                    "rules": [],
                }
            )

    def test_empty_config_raises(self):
        with pytest.raises(ValueError):
            validate_config({})
