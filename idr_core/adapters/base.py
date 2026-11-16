"""
Abstract base class for platform-specific database adapters.

All platform adapters (DuckDB, BigQuery, Snowflake, Databricks) must
implement this interface to work with the unified IDR runner.
"""

import os
import re
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class IDRAdapter(ABC):
    """
    Abstract interface for platform-specific IDR adapters.

    Each platform (DuckDB, BigQuery, Snowflake, Databricks) implements
    this interface to provide SQL execution capabilities.
    """

    @property
    @abstractmethod
    def dialect(self) -> str:
        """
        Return platform name: 'duckdb', 'bigquery', 'snowflake', 'databricks'

        Used for SQL template rendering and dialect-specific SQL generation.
        """
        pass

    @abstractmethod
    def execute(self, sql: str) -> None:
        """
        Execute a SQL statement that doesn't return results.

        Args:
            sql: SQL statement to execute (DDL, INSERT, UPDATE, etc.)
        """
        pass

    @abstractmethod
    def execute_script(self, sql: str) -> None:
        """
        Execute multiple SQL statements separated by semicolons.

        Args:
            sql: Multi-statement SQL script
        """
        pass

    @abstractmethod
    def query(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as list of dicts.

        Args:
            sql: SELECT statement
            params: Optional list of parameters for binding

        Returns:
            List of row dicts with column names as keys
        """
        pass

    @abstractmethod
    def query_one(self, sql: str, params: Optional[List[Any]] = None) -> Any:
        """
        Execute a SQL query and return the first value of first row.

        Args:
            sql: SELECT statement expected to return single value
            params: Optional list of parameters for binding

        Returns:
            First column of first row, or None if no results
        """
        pass

    @abstractmethod
    def table_exists(self, table_fqn: str) -> bool:
        """
        Check if a table exists.

        Args:
            table_fqn: Fully qualified table name (schema.table or db.schema.table)

        Returns:
            True if table exists, False otherwise
        """
        pass

    @abstractmethod
    def get_table_columns(self, table_fqn: str) -> List[Dict[str, str]]:
        """
        Get list of columns for a table with data types.

        Args:
            table_fqn: Fully qualified table name

        Returns:
            List of dicts: [{'name': 'col1', 'type': 'VARCHAR'}, ...]
        """
        pass

    @abstractmethod
    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """
        List tables in a schema.

        Args:
            schema: Schema name to list tables from. If None, uses default/all.

        Returns:
            List of fully qualified table names or simple names depending on context.
        """
        pass

    def init_schema_mapping(self, schema_mapping: Optional[Dict[str, str]] = None) -> None:
        """Configure logical schema mapping (idr_meta/idr_work/idr_out -> custom schemas)."""
        mapping: Dict[str, str] = {}
        prefix = os.environ.get("IDR_SCHEMA_PREFIX", "").strip()
        if prefix:
            for base in ("idr_meta", "idr_work", "idr_out"):
                mapping[base] = f"{prefix}{base}"
        for base, env_key in (
            ("idr_meta", "IDR_META_SCHEMA"),
            ("idr_work", "IDR_WORK_SCHEMA"),
            ("idr_out", "IDR_OUT_SCHEMA"),
        ):
            env_val = os.environ.get(env_key, "").strip()
            if env_val:
                mapping[base] = env_val
        if schema_mapping:
            for k, v in schema_mapping.items():
                if k in ("idr_meta", "idr_work", "idr_out") and v and str(v).strip():
                    mapping[k] = str(v).strip()
        self.schema_mapping: Dict[str, str] = {
            k: v for k, v in mapping.items() if v != k
        }

    def _rewrite_schemas(self, sql: str) -> str:
        """Rewrite canonical idr_meta/idr_work/idr_out references using self.schema_mapping."""
        mapping = getattr(self, "schema_mapping", None)
        if not mapping or not sql:
            return sql

        def _repl_dot(m: re.Match) -> str:
            return mapping.get(m.group(1).lower(), m.group(1))

        def _repl_stmt(m: re.Match) -> str:
            return f"{m.group(1)}{mapping.get(m.group(2).lower(), m.group(2))}"

        def _repl_quote(m: re.Match) -> str:
            return f"{m.group(1)}{mapping.get(m.group(2).lower(), m.group(2))}{m.group(3)}"

        sql = re.sub(r"\b(idr_meta|idr_work|idr_out)(?=\s*\.)", _repl_dot, sql, flags=re.IGNORECASE)
        sql = re.sub(
            r"(\b(?:CREATE|DROP)\s+SCHEMA(?:\s+IF\s+(?:NOT\s+)?EXISTS)?\s+|\bUSE(?:\s+SCHEMA)?\s+|\bSHOW\s+TABLES\s+IN\s+)(idr_meta|idr_work|idr_out)\b",
            _repl_stmt,
            sql,
            flags=re.IGNORECASE,
        )
        sql = re.sub(
            r"(\btable_schema\s*=\s*['\"])(idr_meta|idr_work|idr_out)(['\"])",
            _repl_quote,
            sql,
            flags=re.IGNORECASE,
        )
        return sql

    def _rewrite_table_fqn(self, table_fqn: str) -> str:
        """Rewrite schema prefix in a dot-separated table_fqn."""
        mapping = getattr(self, "schema_mapping", None)
        if not mapping or not table_fqn or "." not in table_fqn:
            return table_fqn
        return re.sub(
            r"\b(idr_meta|idr_work|idr_out)(?=\s*\.)",
            lambda m: mapping.get(m.group(1).lower(), m.group(1)),
            table_fqn,
            flags=re.IGNORECASE,
        )

    def close(self) -> None:
        """
        Close database connection. Override if cleanup needed.
        """
        pass


# Dialect-specific SQL variations
DIALECT_CONFIG = {
    "duckdb": {
        "current_timestamp": "CURRENT_TIMESTAMP",
        "string_type": "VARCHAR",
        "timestamp_type": "TIMESTAMP",
        "int_type": "INTEGER",
        "float_type": "DOUBLE",
        "bool_true": "TRUE",
        "bool_false": "FALSE",
        "transient_table": "",  # DuckDB doesn't have transient
        "concat": lambda a, b: f"({a} || {b})",
        "coalesce": "COALESCE",
        "ilike": "ILIKE",
        "md5": lambda x: f"MD5({x})",
        "sha256": lambda x: f"SHA256(CAST({x} AS VARCHAR))",
        "split_part": lambda col, delim, idx: f"SPLIT_PART({col}, '{delim}', {idx})",
    },
    "snowflake": {
        "current_timestamp": "CURRENT_TIMESTAMP()",
        "string_type": "VARCHAR",
        "timestamp_type": "TIMESTAMP_NTZ",  # Explicitly use NTZ for Snowflake compatibility
        "int_type": "INTEGER",
        "float_type": "DOUBLE",
        "bool_true": "TRUE",
        "bool_false": "FALSE",
        "transient_table": "TRANSIENT",
        "concat": lambda a, b: f"({a} || {b})",
        "coalesce": "COALESCE",
        "ilike": "ILIKE",
        "md5": lambda x: f"MD5({x})",
        "sha256": lambda x: f"SHA2(CAST({x} AS VARCHAR), 256)",
        "split_part": lambda col, delim, idx: f"SPLIT_PART({col}, '{delim}', {idx})",
    },
    "bigquery": {
        "current_timestamp": "CURRENT_TIMESTAMP()",
        "string_type": "STRING",
        "timestamp_type": "TIMESTAMP",
        "int_type": "INT64",
        "float_type": "FLOAT64",
        "bool_true": "TRUE",
        "bool_false": "FALSE",
        "transient_table": "",  # BigQuery uses temp tables differently
        "concat": lambda a, b: f"CONCAT({a}, {b})",
        "coalesce": "COALESCE",
        "ilike": "LOWER({}) LIKE LOWER({})",  # No native ILIKE
        "md5": lambda x: f"TO_HEX(MD5(CAST({x} AS STRING)))",
        "sha256": lambda x: f"TO_HEX(SHA256(CAST({x} AS STRING)))",
        "split_part": lambda col, delim, idx: f"SPLIT({col}, '{delim}')[SAFE_OFFSET({idx - 1})]",
    },
    "databricks": {
        "current_timestamp": "CURRENT_TIMESTAMP()",
        "string_type": "STRING",
        "timestamp_type": "TIMESTAMP",  # Explicitly use TIMESTAMP (LTZ) to avoid NTZ feature error
        "int_type": "INT",
        "float_type": "DOUBLE",
        "bool_true": "TRUE",
        "bool_false": "FALSE",
        "transient_table": "",  # Databricks uses TEMPORARY
        "concat": lambda a, b: f"CONCAT({a}, {b})",
        "coalesce": "COALESCE",
        "ilike": "ILIKE",
        "md5": lambda x: f"MD5({x})",
        "sha256": lambda x: f"SHA2(CAST({x} AS STRING), 256)",
        "split_part": lambda col, delim, idx: f"SPLIT_PART({col}, '{delim}', {idx})",
    },
}


def get_dialect_config(dialect: str) -> Dict[str, Any]:
    """Get dialect-specific SQL configuration."""
    if dialect not in DIALECT_CONFIG:
        raise ValueError(f"Unknown dialect: {dialect}. Supported: {list(DIALECT_CONFIG.keys())}")
    return DIALECT_CONFIG[dialect]
