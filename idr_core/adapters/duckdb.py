"""
DuckDB adapter for IDR.

Provides SQL execution capabilities for DuckDB databases,
supporting both file-based and in-memory databases.
"""

import threading
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

    def __init__(self, db_path_or_conn: Any, schema_mapping: Optional[Dict[str, str]] = None):
        """
        Initialize DuckDB connection.

        Args:
            db_path_or_conn: Path to DuckDB file (str) or existing connection object
            schema_mapping: Optional mapping for idr_meta/idr_work/idr_out schemas
        """
        self._lock = threading.RLock()
        self.init_schema_mapping(schema_mapping)
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
        sql = self._rewrite_schemas(sql)
        with self._lock:
            self.conn.execute(sql)

    def execute_script(self, sql: str) -> None:
        """Execute multiple SQL statements separated by semicolons."""
        # DuckDB handles multi-statement scripts directly
        sql = self._rewrite_schemas(sql)
        with self._lock:
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    self.conn.execute(stmt)

    def query(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL and return results as list of dicts."""
        sql = self._rewrite_schemas(sql)
        with self._lock:
            if params:
                cursor = self.conn.execute(sql, params)
            else:
                cursor = self.conn.execute(sql)

            # Use DB-API style row/column extraction to avoid hard dependency on
            # pandas/numpy for core query behavior.
            rows = cursor.fetchall()
            columns = [col[0] for col in (cursor.description or [])]
            return [dict(zip(columns, row)) for row in rows]

    def query_one(self, sql: str, params: Optional[List[Any]] = None) -> Any:
        """Execute SQL and return first value of first row."""
        sql = self._rewrite_schemas(sql)
        with self._lock:
            if params:
                result = self.conn.execute(sql, params).fetchone()
            else:
                result = self.conn.execute(sql).fetchone()
            return result[0] if result else None

    def table_exists(self, table_fqn: str) -> bool:
        """Check if table exists."""
        table_fqn = self._rewrite_table_fqn(table_fqn)
        with self._lock:
            try:
                self.conn.execute(f"SELECT 1 FROM {table_fqn} LIMIT 0")
                return True
            except duckdb.CatalogException:
                return False
            except Exception:
                return False

    def get_table_columns(self, table_fqn: str) -> List[Dict[str, Any]]:
        """Get column names, types, and optional PK metadata for a table (lowercase)."""
        # DESCRIBE returns: column_name (0), column_type (1), null (2), key (3), default (4), extra (5)
        table_fqn = self._rewrite_table_fqn(table_fqn)
        with self._lock:
            result = self.conn.execute(f"DESCRIBE {table_fqn}").fetchall()
            cols: List[Dict[str, Any]] = []
            for row in result:
                entry: Dict[str, Any] = {"name": row[0].lower(), "type": row[1]}
                if len(row) > 3 and isinstance(row[3], str) and "PRI" in row[3].upper():
                    entry["is_pk"] = True
                cols.append(entry)
            return cols

    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List tables in a schema."""
        if schema:
            schema = self.schema_mapping.get(schema.lower(), schema)
            # parameterized query to prevent injection
            query = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = ?"
            params = [schema]
        else:
            query = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
            params = []

        with self._lock:
            result = self.conn.execute(query, params).fetchall()
            return [f"{row[0]}.{row[1]}" for row in result]

    def close(self) -> None:
        """Close database connection."""
        with self._lock:
            if self.conn:
                self.conn.close()
                self.conn = None
