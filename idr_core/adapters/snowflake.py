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

    def __init__(self, session_or_conn, schema_mapping: Optional[Dict[str, str]] = None):
        """
        Initialize with Snowpark Session OR Python Connector.

        Args:
            session_or_conn: snowflake.snowpark.Session OR snowflake.connector.SnowflakeConnection
            schema_mapping: Optional mapping for idr_meta/idr_work/idr_out schemas
        """
        self.init_schema_mapping(schema_mapping)
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
        sql = self._rewrite_schemas(sql)
        if self.is_snowpark:
            self.conn_obj.sql(sql).collect()
        else:
            with self.conn_obj.cursor() as cur:
                cur.execute(sql)

    def execute_script(self, sql: str) -> None:
        """Execute multiple SQL statements separated by semicolons."""
        # Snowpark doesn't support multi-statement easily without splitting
        # DBAPI does execute_string but standard execute usually one statement
        sql = self._rewrite_schemas(sql)
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
        sql = self._rewrite_schemas(sql)
        if self.is_snowpark:
            # Snowpark supports params in sql() (usually ? placeholders)
            # Do NOT replace ? with %s here.
            df = self.conn_obj.sql(sql, params).to_pandas()
            df.columns = [str(c).lower() for c in df.columns]
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
        sql = self._rewrite_schemas(sql)
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
        table_fqn = self._rewrite_table_fqn(table_fqn)
        try:
            if self.is_snowpark:
                self.conn_obj.sql(f"SELECT 1 FROM {table_fqn} LIMIT 0").collect()
            else:
                with self.conn_obj.cursor() as cur:
                    cur.execute(f"SELECT 1 FROM {table_fqn} LIMIT 0")
            return True
        except Exception:
            return False

    def get_table_columns(self, table_fqn: str) -> List[Dict[str, Any]]:
        """Get column names, types, and optional PK/comment metadata for a table (lowercase)."""
        # DESCRIBE returns: name (0), type (1), kind (2), null? (3), default (4),
        # primary key (5), unique key (6), check (7), expression (8), comment (9)
        table_fqn = self._rewrite_table_fqn(table_fqn)
        if self.is_snowpark:
            result = self.conn_obj.sql(f"DESCRIBE TABLE {table_fqn}").collect()
        else:
            with self.conn_obj.cursor() as cur:
                cur.execute(f"DESCRIBE TABLE {table_fqn}")
                result = cur.fetchall()
        cols: List[Dict[str, Any]] = []
        for row in result:
            entry: Dict[str, Any] = {"name": row[0].lower(), "type": row[1]}
            if len(row) > 5 and isinstance(row[5], str) and row[5].strip().upper() == "Y":
                entry["is_pk"] = True
            if len(row) > 9 and isinstance(row[9], str) and row[9].strip():
                entry["description"] = row[9].strip()[:200]
            cols.append(entry)
        return cols

    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List tables in a schema (case-insensitive, supports optional db.schema)."""
        if schema:
            schema = self.schema_mapping.get(schema.lower(), schema)
        info_schema = "information_schema.tables"
        schema_filter = schema
        if schema and "." in schema:
            db_part, schema_part = schema.split(".", 1)
            clean_db = db_part.strip().replace('"', "").replace("'", "")
            if clean_db:
                info_schema = f"{clean_db}.information_schema.tables"
            schema_filter = schema_part.strip()

        if schema_filter:
            clean_schema = schema_filter.replace("'", "''").replace("\\", "")
            if self.is_snowpark:
                query = (
                    f"SELECT table_schema, table_name FROM {info_schema} "
                    f"WHERE UPPER(table_schema) = UPPER('{clean_schema}') "
                    f"ORDER BY table_name"
                )
                params = []
            else:
                query = (
                    f"SELECT table_schema, table_name FROM {info_schema} "
                    f"WHERE UPPER(table_schema) = UPPER(%s) "
                    f"ORDER BY table_name"
                )
                params = [schema_filter]
        else:
            query = (
                f"SELECT table_schema, table_name FROM {info_schema} "
                f"WHERE UPPER(table_schema) != 'INFORMATION_SCHEMA' "
                f"ORDER BY table_schema, table_name"
            )
            params = []

        return [f"{row['table_schema']}.{row['table_name']}" for row in self.query(query, params)]

    def close(self) -> None:
        """No-op for Snowpark - session lifecycle managed externally."""
        pass
