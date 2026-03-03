"""
Databricks adapter for IDR.

Provides SQL execution capabilities for Databricks using Spark SQL,
supporting both notebook and job execution contexts.
"""

from typing import Any, Dict, List, Optional

from .base import IDRAdapter


class DatabricksAdapter(IDRAdapter):
    """
    Databricks adapter for IDR using Spark SQL.

    This adapter works with Spark Session in Databricks notebooks
    or Databricks jobs.

    Example (in Databricks notebook):
        adapter = DatabricksAdapter(spark, catalog="my_catalog")
        runner = IDRRunner(adapter)
        result = runner.run()
    """

    def __init__(self, spark_or_conn, catalog: str = None):
        """
        Initialize with Spark session OR Databricks SQL Connector.

        Args:
            spark_or_conn: SparkSession OR databricks.sql.Connection
            catalog: Unity Catalog name (optional)
        """
        self.conn_obj = spark_or_conn
        self.catalog = catalog

        # Detect mode
        # SparkSession has .sql() returning DataFrame, Connection has .cursor()
        self.is_spark = hasattr(spark_or_conn, "sql") and not hasattr(spark_or_conn, "cursor")

        # Set catalog if provided
        if catalog:
            if self.is_spark:
                self.conn_obj.sql(f"USE CATALOG {catalog}")
            # For DBAPI, catalog is usually set in connect(), but we can try USE
            # Note: databricks-sql-connector usually sets catalog in connect params

    @property
    def dialect(self) -> str:
        return "databricks"

    def execute(self, sql: str) -> None:
        """Execute a single SQL statement."""
        if self.is_spark:
            self.conn_obj.sql(sql)
        else:
            with self.conn_obj.cursor() as cur:
                cur.execute(sql)

    def execute_script(self, sql: str) -> None:
        """Execute multiple SQL statements separated by semicolons."""
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                if self.is_spark:
                    self.conn_obj.sql(stmt)
                else:
                    with self.conn_obj.cursor() as cur:
                        cur.execute(stmt)

    def query(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL and return results as list of dicts."""
        if self.is_spark:
            # Spark SQL parameterization is tricky, usually relies on templates/binding
            # We'll assume no params for Spark mode for now (common pattern)
            df = self.conn_obj.sql(sql).toPandas()
            return df.to_dict("records")
        else:
            with self.conn_obj.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)

                if cur.description:
                    cols = [col[0].lower() for col in cur.description]
                    rows = cur.fetchall()
                    return [dict(zip(cols, row)) for row in rows]
                return []

    def query_one(self, sql: str, params: Optional[List[Any]] = None) -> Any:
        """Execute SQL and return first value of first row."""
        if self.is_spark:
            result = self.conn_obj.sql(sql).collect()
            if result and len(result) > 0:
                return result[0][0]
            return None
        else:
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
            if self.is_spark:
                self.conn_obj.sql(f"DESCRIBE TABLE {table_fqn}")
            else:
                with self.conn_obj.cursor() as cur:
                    cur.execute(f"DESCRIBE TABLE {table_fqn}")
            return True
        except Exception:
            return False

    def get_table_columns(self, table_fqn: str) -> List[Dict[str, str]]:
        """Get column names for a table (lowercase)."""
        # DESCRIBE returns col_name (0), data_type (1), comment (2)
        if self.is_spark:
            result = self.conn_obj.sql(f"DESCRIBE TABLE {table_fqn}").collect()
            columns = []
            for row in result:
                col_name = row[0]
                if col_name and not col_name.startswith("#"):
                    columns.append({"name": col_name.lower(), "type": row[1]})
            return columns
        else:
            with self.conn_obj.cursor() as cur:
                cur.execute(f"DESCRIBE TABLE {table_fqn}")
                result = cur.fetchall()
                columns = []
                for row in result:
                    col_name = row[0]
                    if col_name and not col_name.startswith("#"):
                        columns.append({"name": col_name.lower(), "type": row[1]})
                return columns

    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List tables in a schema."""
        if not schema:
            return []

        # Relaxed validation: Allow dots for Unity Catalog (catalog.schema)
        import re

        if not re.match(r"^[a-zA-Z0-9_\.]+$", schema):
            # If strictly needed, we could allow more chars but for "setup wizard" simplicity,
            # we assume standard schema names. If complex chars needed, we would need robust escaping.
            raise ValueError(
                f"Invalid schema name: {schema} (Only alphanumeric, underscore, dot allowed)"
            )

        query = f"SHOW TABLES IN {schema}"

        if self.is_spark:
            result = self.conn_obj.sql(query).collect()
            # Spark SHOW TABLES returns: database, tableName, isTemporary
            return [f"{row['database']}.{row['tableName']}" for row in result]
        else:
            with self.conn_obj.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall()
                # DBAPI fetchall returns tuples. Usually (database, tableName, isTemporary)
                return [f"{row[0]}.{row[1]}" for row in result]

    def close(self) -> None:
        """No-op - Spark session lifecycle managed by Databricks."""
        pass
