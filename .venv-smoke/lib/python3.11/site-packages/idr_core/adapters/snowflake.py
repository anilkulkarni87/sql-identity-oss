"""
Snowflake adapter for IDR using Snowpark Python.

This adapter uses Snowpark Python to execute SQL against Snowflake,
enabling the IDR core to run as a stored procedure.
"""

from typing import Any, Dict, List, Optional

from .base import IDRAdapter


class SnowflakeAdapter(IDRAdapter):
    """
    Snowpark Python adapter for Snowflake.

    This adapter is designed to work within a Snowpark stored procedure,
    receiving a Session object from the Snowpark runtime.

    Example (in stored procedure):
        def main(session: Session, run_mode: str, max_iters: int, dry_run: bool):
            adapter = SnowflakeAdapter(session)
            runner = IDRRunner(adapter)
            result = runner.run()
    """

    def __init__(self, session_or_conn):
        """
        Initialize with Snowpark Session OR Python Connector.

        Args:
            session_or_conn: snowflake.snowpark.Session OR snowflake.connector.SnowflakeConnection
        """
        self.conn_obj = session_or_conn
        # Detect mode
        self.is_snowpark = hasattr(session_or_conn, "sql") and hasattr(
            session_or_conn, "create_dataframe"
        )

    @property
    def dialect(self) -> str:
        return "snowflake"

    def execute(self, sql: str) -> None:
        """Execute a single SQL statement."""
        if self.is_snowpark:
            self.conn_obj.sql(sql).collect()
        else:
            with self.conn_obj.cursor() as cur:
                cur.execute(sql)

    def execute_script(self, sql: str) -> None:
        """Execute multiple SQL statements separated by semicolons."""
        # Snowpark doesn't support multi-statement easily without splitting
        # DBAPI does execute_string but standard execute usually one statement
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                if self.is_snowpark:
                    self.conn_obj.sql(stmt).collect()
                else:
                    with self.conn_obj.cursor() as cur:
                        cur.execute(stmt)

    def query(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL and return results as list of dicts."""
        if self.is_snowpark:
            # Snowpark supports params in sql() (usually ? placeholders)
            # Do NOT replace ? with %s here.
            df = self.conn_obj.sql(sql, params).to_pandas()
            df.columns = df.columns.str.lower()
            return df.to_dict("records")
        else:
            # DBAPI Mode (Standard Snowflake Connector) uses paramstyle='pyformat' (%s) by default
            if params and "?" in sql:
                sql = sql.replace("?", "%s")

            with self.conn_obj.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)

                # Fetch results and columns
                cols = [col[0].lower() for col in cur.description]
                rows = cur.fetchall()
                return [dict(zip(cols, row)) for row in rows]

    def query_one(self, sql: str, params: Optional[List[Any]] = None) -> Any:
        """Execute SQL and return first value of first row."""
        if self.is_snowpark:
            # Snowpark supports params
            result = self.conn_obj.sql(sql, params).collect()
            if result and len(result) > 0:
                return result[0][0]
            return None
        else:
            # DBAPI Mode
            if params and "?" in sql:
                sql = sql.replace("?", "%s")

            with self.conn_obj.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                result = cur.fetchone()
                return result[0] if result else None

    def table_exists(self, table_fqn: str) -> bool:
        """Check if table exists."""
        try:
            if self.is_snowpark:
                self.conn_obj.sql(f"SELECT 1 FROM {table_fqn} LIMIT 0").collect()
            else:
                with self.conn_obj.cursor() as cur:
                    cur.execute(f"SELECT 1 FROM {table_fqn} LIMIT 0")
            return True
        except Exception:
            return False

    def get_table_columns(self, table_fqn: str) -> List[Dict[str, str]]:
        """Get column names for a table (lowercase)."""
        # DESCRIBE returns: name, type, kind, null?, default, primary key, unique key, check, expression, comment
        # We need name (0) and type (1)
        if self.is_snowpark:
            result = self.conn_obj.sql(f"DESCRIBE TABLE {table_fqn}").collect()
            return [{"name": row[0].lower(), "type": row[1]} for row in result]
        else:
            with self.conn_obj.cursor() as cur:
                cur.execute(f"DESCRIBE TABLE {table_fqn}")
                result = cur.fetchall()
                return [{"name": row[0].lower(), "type": row[1]} for row in result]

    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List tables in a schema."""
        if schema:
            # Handle Snowpark vs DBAPI param differences
            # Snowpark session.sql() typically needs literal SQL or specific param style dependent on version.
            # Safest is to validate schema identifier and inject formatted string.
            # Assuming schema is a valid identifier (alphanumeric w/ underscores/dots potentially)
            # But "schema" usually is just one identifier.

            # Simple injection guard for schema name
            clean_schema = schema.replace("'", "''").replace("\\", "")

            if self.is_snowpark:
                # Snowpark mode: Construct SQL directly due to ambiguous param support in wrapper
                query = f"SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = '{clean_schema}'"
                params = []
            else:
                # DBAPI mode: Use params
                query = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = %s"
                params = [schema]
        else:
            query = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema != 'INFORMATION_SCHEMA'"
            params = []

        return [f"{row['table_schema']}.{row['table_name']}" for row in self.query(query, params)]

    def close(self) -> None:
        """No-op for Snowpark - session lifecycle managed externally."""
        pass
