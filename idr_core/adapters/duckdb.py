"""
DuckDB adapter for IDR.

Provides SQL execution capabilities for DuckDB databases,
supporting both file-based and in-memory databases.
"""

from typing import Any, Dict, List, Optional

import duckdb

from .base import IDRAdapter


class DuckDBAdapter(IDRAdapter):
    """
    DuckDB-specific adapter for IDR.

    Example:
        adapter = DuckDBAdapter("my_database.duckdb")
        # or for in-memory:
        adapter = DuckDBAdapter(":memory:")
    """

    def __init__(self, db_path_or_conn: Any):
        """
        Initialize DuckDB connection.

        Args:
            db_path_or_conn: Path to DuckDB file (str) or existing connection object
        """
        if isinstance(db_path_or_conn, str):
            self.db_path = db_path_or_conn
            self.conn = duckdb.connect(db_path_or_conn)
        else:
            self.db_path = ":existing_connection:"
            self.conn = db_path_or_conn

    @property
    def dialect(self) -> str:
        return "duckdb"

    def execute(self, sql: str) -> None:
        """Execute a single SQL statement."""
        self.conn.execute(sql)

    def execute_script(self, sql: str) -> None:
        """Execute multiple SQL statements separated by semicolons."""
        # DuckDB handles multi-statement scripts directly
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                self.conn.execute(stmt)

    def query(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL and return results as list of dicts."""
        if params:
            result = self.conn.execute(sql, params).fetchdf()
        else:
            result = self.conn.execute(sql).fetchdf()
        return result.to_dict("records")

    def query_one(self, sql: str, params: Optional[List[Any]] = None) -> Any:
        """Execute SQL and return first value of first row."""
        if params:
            result = self.conn.execute(sql, params).fetchone()
        else:
            result = self.conn.execute(sql).fetchone()
        return result[0] if result else None

    def table_exists(self, table_fqn: str) -> bool:
        """Check if table exists."""
        try:
            self.conn.execute(f"SELECT 1 FROM {table_fqn} LIMIT 0")
            return True
        except duckdb.CatalogException:
            return False
        except Exception:
            return False

    def get_table_columns(self, table_fqn: str) -> List[Dict[str, str]]:
        """Get column names for a table (lowercase)."""
        # DESCRIBE returns: column_name, column_type, null, key, default, extra
        result = self.conn.execute(f"DESCRIBE {table_fqn}").fetchall()
        return [{"name": row[0].lower(), "type": row[1]} for row in result]

    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List tables in a schema."""
        if schema:
            # parameterized query to prevent injection
            query = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = ?"
            params = [schema]
        else:
            query = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
            params = []

        result = self.conn.execute(query, params).fetchall()
        return [f"{row[0]}.{row[1]}" for row in result]

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
