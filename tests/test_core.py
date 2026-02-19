"""
Unit tests for idr_core module.

Tests the core functionality of the unified IDR package.
"""

import os
import sys

import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from idr_core.config import (
    build_where_clause,
    get_attr_expr,
    validate_config,
    validate_float,
    validate_fqn,
    validate_identifier,
    validate_integer,
    validate_sql_expr,
)
from idr_core.runner import RunConfig, RunResult


class TestValidateIdentifier:
    """Tests for identifier validation."""

    def test_valid_simple_identifier(self):
        assert validate_identifier("email") == "email"
        assert validate_identifier("EMAIL") == "EMAIL"
        assert validate_identifier("user_id") == "user_id"
        assert validate_identifier("table123") == "table123"

    def test_invalid_identifier_with_spaces(self):
        with pytest.raises(ValueError):
            validate_identifier("user id")

    def test_invalid_identifier_with_special_chars(self):
        with pytest.raises(ValueError):
            validate_identifier("email; DROP TABLE")

    def test_invalid_identifier_starting_with_number(self):
        with pytest.raises(ValueError):
            validate_identifier("123table")

    def test_empty_identifier(self):
        with pytest.raises(ValueError):
            validate_identifier("")


class TestValidateFQN:
    """Tests for fully qualified name validation."""

    def test_valid_two_part_name(self):
        assert validate_fqn("schema.table") == "schema.table"

    def test_valid_three_part_name(self):
        assert validate_fqn("catalog.schema.table") == "catalog.schema.table"

    def test_valid_single_name(self):
        assert validate_fqn("table_name") == "table_name"

    def test_invalid_fqn_with_sql_injection(self):
        with pytest.raises(ValueError):
            validate_fqn("schema.table; DROP TABLE users")


class TestValidateSqlExpr:
    """Tests for SQL expression validation (injection prevention)."""

    # Valid expressions that should pass
    def test_simple_column(self):
        assert validate_sql_expr("id") == "id"

    def test_function_call(self):
        assert validate_sql_expr("LOWER(email)") == "LOWER(email)"

    def test_nested_function(self):
        assert (
            validate_sql_expr("CONCAT(first_name, ' ', last_name)")
            == "CONCAT(first_name, ' ', last_name)"
        )

    def test_cast_expression(self):
        assert validate_sql_expr("CAST(id AS VARCHAR)") == "CAST(id AS VARCHAR)"

    def test_coalesce(self):
        assert validate_sql_expr("COALESCE(email, phone)") == "COALESCE(email, phone)"

    def test_arithmetic(self):
        assert validate_sql_expr("price * quantity") == "price * quantity"

    def test_case_expression(self):
        expr = "CASE WHEN status = 'active' THEN 1 ELSE 0 END"
        assert validate_sql_expr(expr) == expr

    # Injection attempts that must be rejected
    def test_rejects_semicolon_drop(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            validate_sql_expr("id; DROP TABLE users")

    def test_rejects_line_comment(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            validate_sql_expr("id -- comment")

    def test_rejects_block_comment_open(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            validate_sql_expr("id /* comment */")

    def test_rejects_union_select(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            validate_sql_expr("1 UNION SELECT * FROM secrets")

    def test_rejects_insert(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            validate_sql_expr("INSERT INTO evil VALUES (1)")

    def test_rejects_delete(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            validate_sql_expr("DELETE FROM users")

    def test_rejects_drop(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            validate_sql_expr("DROP TABLE users")

    def test_rejects_alter(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            validate_sql_expr("ALTER TABLE users ADD evil VARCHAR")

    def test_rejects_grant(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            validate_sql_expr("GRANT ALL ON users TO attacker")

    def test_rejects_truncate(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            validate_sql_expr("TRUNCATE TABLE users")

    def test_rejects_empty_string(self):
        with pytest.raises(ValueError):
            validate_sql_expr("")

    def test_rejects_quoted_injection(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            validate_sql_expr("'; DELETE FROM idr_meta.rule; --")


class TestValidateInteger:
    """Tests for integer validation."""

    def test_valid_int(self):
        assert validate_integer(42) == 42

    def test_valid_string_int(self):
        assert validate_integer("100") == 100

    def test_valid_float_to_int(self):
        assert validate_integer(10.0) == 10

    def test_rejects_non_numeric(self):
        with pytest.raises(ValueError):
            validate_integer("not_a_number")

    def test_rejects_none(self):
        with pytest.raises(ValueError):
            validate_integer(None)


class TestValidateFloat:
    """Tests for float validation."""

    def test_valid_float(self):
        assert validate_float(0.85) == 0.85

    def test_valid_int_to_float(self):
        assert validate_float(1) == 1.0

    def test_valid_string_float(self):
        assert validate_float("0.95") == 0.95

    def test_rejects_non_numeric(self):
        with pytest.raises(ValueError):
            validate_float("not_a_number")

    def test_rejects_none(self):
        with pytest.raises(ValueError):
            validate_float(None)


class TestGetAttrExpr:
    """Tests for attribute expression resolution."""

    def test_finds_mapped_column(self):
        table_attrs = {"email": "email_address"}
        result = get_attr_expr(table_attrs, "email", ["email"], ["email_address", "phone"])
        assert result == "src.email_address"

    def test_uses_fallback_when_no_mapping(self):
        table_attrs = {}
        available = ["email_raw", "phone"]
        result = get_attr_expr(table_attrs, "email", ["email_norm", "email_raw"], available)
        assert result == "src.email_raw"

    def test_returns_null_when_not_found(self):
        table_attrs = {}
        available = ["phone", "name"]
        result = get_attr_expr(table_attrs, "email", ["email"], available)
        assert result == "NULL"

    def test_case_insensitive_column_match(self):
        table_attrs = {"email": "EMAIL_ADDR"}
        result = get_attr_expr(table_attrs, "email", [], ["email_addr", "phone"])
        assert result == "src.EMAIL_ADDR"

    def test_custom_alias(self):
        table_attrs = {"email": "email_col"}
        result = get_attr_expr(table_attrs, "email", [], ["email_col"], alias="t")
        assert result == "t.email_col"


class TestBuildWhereClause:
    """Tests for WHERE clause generation."""

    def test_full_mode_returns_always_true(self):
        result = build_where_clause("updated_at", "2024-01-01", 0, "FULL")
        assert result == "1=1"

    def test_incr_mode_without_watermark(self):
        result = build_where_clause("updated_at", None, 0, "INCR")
        assert result == "1=1"

    def test_incr_mode_duckdb(self):
        result = build_where_clause("updated_at", "2024-01-01T12:00:00", 0, "INCR", "duckdb")
        assert "updated_at >" in result
        assert "2024-01-01T12:00:00" in result

    def test_incr_mode_with_lookback(self):
        result = build_where_clause("updated_at", "2024-01-01T12:00:00", 60, "INCR", "duckdb")
        assert "updated_at >" in result
        assert "60" in result or "minutes" in result

    def test_bigquery_dialect(self):
        result = build_where_clause("updated_at", "2024-01-01", 0, "INCR", "bigquery")
        assert "TIMESTAMP" in result


class TestRunConfig:
    """Tests for RunConfig dataclass."""

    def test_default_values(self):
        config = RunConfig()
        assert config.run_mode == "FULL"
        assert config.max_iters == 30
        assert config.dry_run is False

    def test_custom_values(self):
        config = RunConfig(run_mode="INCR", max_iters=50, dry_run=True)
        assert config.run_mode == "INCR"
        assert config.max_iters == 50
        assert config.dry_run is True

    def test_incremental_alias_normalized(self):
        config = RunConfig(run_mode="INCREMENTAL")
        assert config.run_mode == "INCR"

    def test_invalid_run_mode(self):
        with pytest.raises(ValueError):
            RunConfig(run_mode="INVALID")

    def test_invalid_max_iters(self):
        with pytest.raises(ValueError):
            RunConfig(max_iters=-1)


class TestValidateConfig:
    """Tests for YAML config validation."""

    def test_valid_config(self):
        config = {"sources": [{"id": "crm", "table": "schema.customers", "entity_key": "id"}]}
        assert validate_config(config) is True

    def test_missing_sources(self):
        config = {"rules": []}
        with pytest.raises(ValueError) as exc:
            validate_config(config)
        assert "sources" in str(exc.value)

    def test_source_missing_id(self):
        config = {"sources": [{"table": "schema.table", "entity_key": "id"}]}
        with pytest.raises(ValueError) as exc:
            validate_config(config)
        assert "id" in str(exc.value).lower()

    def test_source_missing_table(self):
        config = {"sources": [{"id": "crm", "entity_key": "id"}]}
        with pytest.raises(ValueError) as exc:
            validate_config(config)
        assert "table" in str(exc.value).lower()


class TestRunResult:
    """Tests for RunResult dataclass."""

    def test_default_values(self):
        result = RunResult(run_id="test_123", status="SUCCESS")
        assert result.run_id == "test_123"
        assert result.status == "SUCCESS"
        assert result.entities_processed == 0
        assert result.warnings == []

    def test_with_metrics(self):
        result = RunResult(
            run_id="test_123",
            status="SUCCESS",
            entities_processed=1000,
            edges_created=500,
            clusters_impacted=200,
        )
        assert result.entities_processed == 1000
        assert result.edges_created == 500
        assert result.clusters_impacted == 200


# Integration test with DuckDB (if available)
class TestDuckDBAdapter:
    """Integration tests with DuckDB adapter."""

    @pytest.fixture
    def adapter(self):
        """Create in-memory DuckDB adapter."""
        try:
            from idr_core.adapters.duckdb import DuckDBAdapter

            adapter = DuckDBAdapter(":memory:")
            yield adapter
            adapter.close()
        except ImportError:
            pytest.skip("DuckDB not installed")

    def test_execute_and_query(self, adapter):
        adapter.execute("CREATE TABLE test (id INT, name VARCHAR)")
        adapter.execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")
        result = adapter.query("SELECT * FROM test ORDER BY id")
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[0]["name"] == "Alice"

    def test_query_one(self, adapter):
        adapter.execute("CREATE TABLE test (cnt INT)")
        adapter.execute("INSERT INTO test VALUES (42)")
        result = adapter.query_one("SELECT cnt FROM test")
        assert result == 42

    def test_table_exists(self, adapter):
        adapter.execute("CREATE TABLE existing_table (id INT)")
        assert adapter.table_exists("existing_table") is True
        assert adapter.table_exists("nonexistent_table") is False

    def test_get_table_columns(self, adapter):
        adapter.execute("CREATE TABLE test (id INT, name VARCHAR, email VARCHAR)")
        columns = adapter.get_table_columns("test")
        col_names = [c["name"] for c in columns]
        assert "id" in col_names
        assert "name" in col_names
        assert "email" in col_names

    def test_dialect(self, adapter):
        assert adapter.dialect == "duckdb"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
