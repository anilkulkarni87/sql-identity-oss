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
            else:
                try:
                    with self.conn_obj.cursor() as cur:
                        cur.execute(f"USE CATALOG {catalog}")
                except Exception:
                    pass

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

    @staticmethod
    def _bind_spark_params(sql: str, params: Optional[List[Any]]) -> str:
        """Bind positional '?' parameters safely for Spark SQL execution."""
        if not params or "?" not in sql:
            return sql
        bound = sql
        for p in params:
            if p is None:
                lit = "NULL"
            elif isinstance(p, bool):
                lit = "TRUE" if p else "FALSE"
            elif isinstance(p, (int, float)):
                lit = str(p)
            else:
                escaped = str(p).replace("'", "''")
                lit = f"'{escaped}'"
            bound = bound.replace("?", lit, 1)
        return bound

    def query(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL and return results as list of dicts."""
        if self.is_spark:
            bound_sql = self._bind_spark_params(sql, params)
            df = self.conn_obj.sql(bound_sql).toPandas()
            df.columns = [str(c).lower() for c in df.columns]
            return df.to_dict("records")
        else:
            with self.conn_obj.cursor() as cur:
                if params and "?" in sql:
                    cur.execute(self._bind_spark_params(sql, params))
                elif params:
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
            bound_sql = self._bind_spark_params(sql, params)
            result = self.conn_obj.sql(bound_sql).collect()
            if result and len(result) > 0:
                return result[0][0]
            return None
        else:
            with self.conn_obj.cursor() as cur:
                if params and "?" in sql:
                    cur.execute(self._bind_spark_params(sql, params))
                elif params:
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

    def list_schemas(self) -> List[str]:
        """List available schemas/databases in the active catalog."""
        schemas = []
        queries = []
        if self.catalog:
            queries.append(f"SHOW SCHEMAS IN {self.catalog}")
        queries.append("SHOW SCHEMAS")

        for query in queries:
            try:
                if self.is_spark:
                    rows = self.conn_obj.sql(query).collect()
                    schemas = [r[0] for r in rows if r[0]]
                else:
                    with self.conn_obj.cursor() as cur:
                        cur.execute(query)
                        rows = cur.fetchall()
                        schemas = [r[0] for r in rows if r[0]]
                if schemas:
                    break
            except Exception:
                continue

        return schemas

    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """List tables in a schema, or across user schemas if schema is None."""
        import re

        target_schemas = []
        if schema:
            if not re.match(r"^[a-zA-Z0-9_\.]+$", schema):
                raise ValueError(
                    f"Invalid schema name: {schema} (Only alphanumeric, underscore, dot allowed)"
                )
            target_schemas.append(schema)
            # Also try qualifying with catalog if not already qualified
            if self.catalog and "." not in schema:
                target_schemas.append(f"{self.catalog}.{schema}")
        else:
            found = self.list_schemas()
            target_schemas = [
                s
                for s in found
                if s.lower()
                not in (
                    "information_schema",
                    "sys",
                    "idr_meta",
                    "idr_work",
                    "idr_out",
                )
            ]
            if not target_schemas:
                target_schemas = ["default"]

        all_tables = []
        seen = set()

        for s in target_schemas:
            query = f"SHOW TABLES IN {s}"
            try:
                if self.is_spark:
                    result = self.conn_obj.sql(query).collect()
                    for row in result:
                        tbl = f"{row['database']}.{row['tableName']}"
                        if tbl not in seen:
                            seen.add(tbl)
                            all_tables.append(tbl)
                else:
                    with self.conn_obj.cursor() as cur:
                        cur.execute(query)
                        result = cur.fetchall()
                        for row in result:
                            tbl = f"{row[0]}.{row[1]}"
                            if tbl not in seen:
                                seen.add(tbl)
                                all_tables.append(tbl)
            except Exception:
                continue

        return all_tables

    def close(self) -> None:
        """No-op - Spark session lifecycle managed by Databricks."""
        pass
