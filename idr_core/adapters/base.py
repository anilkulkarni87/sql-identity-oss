"""
Abstract base class for platform-specific database adapters.

All platform adapters (DuckDB, BigQuery, Snowflake, Databricks) must
implement this interface to work with the unified IDR runner.
"""

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
        "bool_true": "TRUE",
        "bool_false": "FALSE",
        "transient_table": "",  # DuckDB doesn't have transient
        "concat": lambda a, b: f"({a} || {b})",
        "coalesce": "COALESCE",
        "ilike": "ILIKE",
        "md5": lambda x: f"MD5({x})",
    },
    "snowflake": {
        "current_timestamp": "CURRENT_TIMESTAMP()",
        "string_type": "VARCHAR",
        "timestamp_type": "TIMESTAMP_NTZ",  # Explicitly use NTZ for Snowflake compatibility
        "int_type": "INTEGER",
        "bool_true": "TRUE",
        "bool_false": "FALSE",
        "transient_table": "TRANSIENT",
        "concat": lambda a, b: f"({a} || {b})",
        "coalesce": "COALESCE",
        "ilike": "ILIKE",
        "md5": lambda x: f"MD5({x})",
    },
    "bigquery": {
        "current_timestamp": "CURRENT_TIMESTAMP()",
        "string_type": "STRING",
        "timestamp_type": "TIMESTAMP",
        "int_type": "INT64",
        "bool_true": "TRUE",
        "bool_false": "FALSE",
        "transient_table": "",  # BigQuery uses temp tables differently
        "concat": lambda a, b: f"CONCAT({a}, {b})",
        "coalesce": "COALESCE",
        "ilike": "LOWER({}) LIKE LOWER({})",  # No native ILIKE
        "md5": lambda x: f"TO_HEX(MD5(CAST({x} AS STRING)))",
    },
    "databricks": {
        "current_timestamp": "CURRENT_TIMESTAMP()",
        "string_type": "STRING",
        "timestamp_type": "TIMESTAMP",  # Explicitly use TIMESTAMP (LTZ) to avoid NTZ feature error
        "int_type": "INT",
        "bool_true": "TRUE",
        "bool_false": "FALSE",
        "transient_table": "",  # Databricks uses TEMPORARY
        "concat": lambda a, b: f"CONCAT({a}, {b})",
        "coalesce": "COALESCE",
        "ilike": "ILIKE",
        "md5": lambda x: f"MD5({x})",
    },
}


def get_dialect_config(dialect: str) -> Dict[str, Any]:
    """Get dialect-specific SQL configuration."""
    if dialect not in DIALECT_CONFIG:
        raise ValueError(f"Unknown dialect: {dialect}. Supported: {list(DIALECT_CONFIG.keys())}")
    return DIALECT_CONFIG[dialect]
