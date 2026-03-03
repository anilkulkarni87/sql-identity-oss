"""
Centralized schema management for IDR.
Handles DDL generation and execution across platforms.
"""

from typing import List

from .adapters.base import IDRAdapter, get_dialect_config
from .schema_defs import SYSTEM_TABLES, ColumnType, TableDef


class SchemaManager:
    """Manages IDR database schemas and tables."""

    def __init__(self, adapter: IDRAdapter):
        self.adapter = adapter
        self.dialect_name = adapter.dialect
        self.dialect_config = get_dialect_config(self.dialect_name)

    def initialize(self, reset: bool = False) -> None:
        """
        Initialize all system tables.

        Args:
            reset: If True, drop known schemas before creating.
        """
        schemas = ["idr_meta", "idr_work", "idr_out"]

        if reset:
            print("Resetting schemas...")
            self._drop_schemas(schemas)

        print("Creating schemas...")
        self._create_schemas(schemas)

        print("Creating tables...")
        for table_def in SYSTEM_TABLES:
            self._create_table(table_def)

    def _drop_schemas(self, schemas: List[str]) -> None:
        """Drop schemas and all objects within them."""
        for schema in schemas:
            try:
                if self.dialect_name in ("duckdb", "postgres"):
                    self.adapter.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
                elif self.dialect_name == "bigquery":
                    # BigQuery supports DROP SCHEMA ... CASCADE but it's a DDL operation.
                    # Configured service account must have roles/bigquery.dataOwner
                    self.adapter.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
                else:
                    # Snowflake / Databricks
                    self.adapter.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            except Exception as e:
                print(f"Warning: Failed to drop schema {schema}: {e}")
                if self.dialect_name == "bigquery":
                    print("  Note: BigQuery requires the 'bigquery.datasets.delete' permission.")

    def _create_schemas(self, schemas: List[str]) -> None:
        """Create schemas if they don't exist."""
        for schema in schemas:
            self.adapter.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def _create_table(self, table_def: TableDef) -> None:
        """Generate and execute CREATE TABLE DDL, or ALTER TABLE if columns missing."""

        # 1. Check if table exists
        exists = self.adapter.table_exists(table_def.fqn)

        if not exists:
            # CREATE TABLE logic (existing)
            col_defs = []
            pks = []

            for col in table_def.columns:
                if col.type == ColumnType.DOUBLE:
                    sql_type = "DOUBLE"
                else:
                    sql_type = self.dialect_config.get(col.type.value, "VARCHAR")

                col_def = f"{col.name} {sql_type}"
                if col.default:
                    if col.default == "TRUE":
                        val = self.dialect_config.get("bool_true", "TRUE")
                        col_def += f" DEFAULT {val}"
                    elif col.default == "FALSE":
                        val = self.dialect_config.get("bool_false", "FALSE")
                        col_def += f" DEFAULT {val}"

                col_defs.append(col_def)
                if col.is_pk:
                    pks.append(col.name)

            pk_clause = ""
            if pks:
                pk_clause = f", PRIMARY KEY ({', '.join(pks)})"

            full_ddl = f"""
                CREATE TABLE IF NOT EXISTS {table_def.fqn} (
                    {", ".join(col_defs)}
                    {pk_clause}
                )
            """
            self.adapter.execute(full_ddl)

        else:
            # Table exists: Check for missing columns (Schema Evolution)
            # Adapters typically return 'name' or 'column_name'
            cols = self.adapter.get_table_columns(table_def.fqn)
            existing_cols = set()
            for c in cols:
                # Handle potential variations in adapter return keys
                col_name = c.get("name") or c.get("column_name")
                if col_name:
                    existing_cols.add(col_name.lower())

            for col in table_def.columns:
                if col.name.lower() not in existing_cols:
                    print(f"Schema Evolution: Adding column {col.name} to {table_def.fqn}")

                    if col.type == ColumnType.DOUBLE:
                        sql_type = "DOUBLE"
                    else:
                        sql_type = self.dialect_config.get(col.type.value, "VARCHAR")

                    # ALTER TABLE ADD COLUMN
                    try:
                        self.adapter.execute(
                            f"ALTER TABLE {table_def.fqn} ADD COLUMN {col.name} {sql_type}"
                        )
                    except Exception as e:
                        print(f"Warning: Failed to add column {col.name}: {e}")
