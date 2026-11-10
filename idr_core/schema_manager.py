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

    def initialize(self, reset: bool = False, seed_default_exclusions: bool = False) -> None:
        """
        Initialize all system tables.

        Args:
            reset: If True, drop known schemas before creating.
            seed_default_exclusions: If True, non-destructively seed the curated
                default toxic identifier exclusion pack into `idr_meta.identifier_exclusion`.
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

        if seed_default_exclusions:
            self.seed_default_exclusions()

    def seed_default_exclusions(self) -> int:
        """
        Non-destructively seed DEFAULT_EXCLUSIONS into `idr_meta.identifier_exclusion`.
        Preserves any exclusions manually inserted by users via SQL.
        """
        from .config import DEFAULT_EXCLUSIONS, config_to_sql

        dummy_cfg = {"sources": [], "exclusions": DEFAULT_EXCLUSIONS}
        stmts = config_to_sql(dummy_cfg, dialect=self.dialect_name)
        count = 0
        for stmt in stmts:
            self.adapter.execute(stmt)
            count += 1
        return count

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

    def _get_sql_type(self, col_type: ColumnType) -> str:
        """Resolve database-specific SQL type for a ColumnType."""
        return self.resolve_sql_type(self.dialect_name, self.dialect_config, col_type)

    @staticmethod
    def resolve_sql_type(dialect_name: str, dialect_config: dict, col_type: ColumnType) -> str:
        """Resolve database-specific SQL type for a ColumnType without requiring an adapter."""
        if col_type == ColumnType.DOUBLE:
            return "FLOAT64" if dialect_name == "bigquery" else "DOUBLE"
        elif col_type == ColumnType.BOOL:
            return "BOOL" if dialect_name == "bigquery" else "BOOLEAN"
        elif col_type == ColumnType.INT:
            return dialect_config.get("int_type", "INTEGER")
        elif col_type == ColumnType.TIMESTAMP:
            return dialect_config.get("timestamp_type", "TIMESTAMP")
        elif col_type == ColumnType.STRING:
            return dialect_config.get("string_type", "VARCHAR")
        return dialect_config.get(col_type.value, "VARCHAR")

    @classmethod
    def generate_ddl(cls, dialect_name: str) -> str:
        """Generate canonical multi-statement DDL script for a target dialect from SYSTEM_TABLES."""
        dialect_config = get_dialect_config(dialect_name)
        lines = [
            f"-- Canonical {dialect_name.upper()} DDL generated from idr_core.schema_defs.SYSTEM_TABLES",
            "CREATE SCHEMA IF NOT EXISTS idr_meta;",
            "CREATE SCHEMA IF NOT EXISTS idr_work;",
            "CREATE SCHEMA IF NOT EXISTS idr_out;",
            "",
        ]
        for table_def in SYSTEM_TABLES:
            col_defs = []
            pks = []
            for col in table_def.columns:
                sql_type = cls.resolve_sql_type(dialect_name, dialect_config, col.type)
                col_def = f"    {col.name} {sql_type}"
                # Databricks Delta Lake requires the table feature 'allowColumnDefaults' to be enabled
                # for column DEFAULT clauses. Omitting DEFAULT on Databricks ensures universal compatibility.
                if col.default in ("TRUE", "FALSE") and dialect_name != "databricks":
                    key = "bool_true" if col.default == "TRUE" else "bool_false"
                    col_def += f" DEFAULT {dialect_config.get(key, col.default)}"
                col_defs.append(col_def)
                if col.is_pk:
                    pks.append(col.name)

            if pks:
                if dialect_name in ("bigquery", "databricks"):
                    col_defs.append(f"    PRIMARY KEY ({', '.join(pks)}) NOT ENFORCED")
                else:
                    col_defs.append(f"    PRIMARY KEY ({', '.join(pks)})")

            lines.append(f"CREATE TABLE IF NOT EXISTS {table_def.fqn} (")
            lines.append(",\n".join(col_defs))
            lines.append(");\n")
        return "\n".join(lines)

    def _create_table(self, table_def: TableDef) -> None:
        """Generate and execute CREATE TABLE DDL, or ALTER TABLE if columns missing."""

        # 1. Check if table exists
        exists = self.adapter.table_exists(table_def.fqn)

        if not exists:
            # CREATE TABLE logic (existing)
            col_defs = []
            pks = []

            for col in table_def.columns:
                sql_type = self._get_sql_type(col.type)

                col_def = f"{col.name} {sql_type}"
                # Databricks Delta Lake requires the table feature 'allowColumnDefaults' to be enabled
                # for column DEFAULT clauses. Omitting DEFAULT on Databricks ensures universal compatibility.
                if col.default and self.dialect_name != "databricks":
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
                if self.dialect_name in ("bigquery", "databricks"):
                    pk_clause = f", PRIMARY KEY ({', '.join(pks)}) NOT ENFORCED"
                else:
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
                    sql_type = self._get_sql_type(col.type)

                    # ALTER TABLE ADD COLUMN
                    try:
                        self.adapter.execute(
                            f"ALTER TABLE {table_def.fqn} ADD COLUMN {col.name} {sql_type}"
                        )
                    except Exception as e:
                        print(f"Warning: Failed to add column {col.name}: {e}")
