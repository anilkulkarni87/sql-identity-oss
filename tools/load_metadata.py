#!/usr/bin/env python3
"""
IDR Metadata Loader

Reads a YAML configuration file and populates the idr_meta.* tables
in the target platform (DuckDB, Snowflake, BigQuery, Databricks).

Usage:
    python tools/load_metadata.py --platform=duckdb --db=idr.duckdb --config=config.yaml
    python tools/load_metadata.py --platform=snowflake --config=config.yaml
"""

import argparse
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List

import yaml

# ==========================================
# Schema Definitions
# ==========================================

REQUIRED_META_TABLES = [
    "source_table",
    "source",
    "rule",
    "identifier_mapping",
    "entity_attribute_mapping",
    "survivorship_rule",
]

# ==========================================
# Platform Adapters
# ==========================================


class DbAdapter(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def execute(self, sql: str):
        pass

    @abstractmethod
    def truncate_table(self, table: str):
        pass

    @abstractmethod
    def insert_rows(self, table: str, columns: List[str], rows: List[tuple]):
        pass

    @abstractmethod
    def close(self):
        pass


class DuckDBAdapter(DbAdapter):
    def __init__(self, db_path: str):
        import duckdb

        self.con = duckdb.connect(db_path)

    def connect(self):
        pass

    def execute(self, sql: str):
        self.con.execute(sql)

    def truncate_table(self, table: str):
        self.con.execute(f"DELETE FROM {table}")

    def insert_rows(self, table: str, columns: List[str], rows: List[tuple]):
        if not rows:
            return
        placeholders = ", ".join(["?"] * len(columns))
        col_str = ", ".join(columns)
        self.con.executemany(f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})", rows)

    def close(self):
        self.con.close()


class SnowflakeAdapter(DbAdapter):
    def __init__(self, account, user, password, database, warehouse, schema="IDR_META"):
        import snowflake.connector

        self.ctx = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            database=database,
            warehouse=warehouse,
            schema=schema,
        )
        self.cursor = self.ctx.cursor()

    def connect(self):
        pass

    def execute(self, sql: str):
        self.cursor.execute(sql)

    def truncate_table(self, table: str):
        # Handle schema prefix if needed
        if "." not in table:
            table = f"IDR_META.{table}"
        self.cursor.execute(f"TRUNCATE TABLE {table}")

    def insert_rows(self, table: str, columns: List[str], rows: List[tuple]):
        if not rows:
            return
        if "." not in table:
            table = f"IDR_META.{table}"
        col_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        self.cursor.executemany(f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})", rows)

    def close(self):
        self.cursor.close()
        self.ctx.close()


class BigQueryAdapter(DbAdapter):
    def __init__(self, project, dataset="idr_meta"):
        from google.cloud import bigquery

        self.client = bigquery.Client(project=project)
        self.dataset = dataset

    def connect(self):
        pass

    def execute(self, sql: str):
        self.client.query(sql).result()

    def truncate_table(self, table: str):
        # BigQuery uses TRUNCATE TABLE dataset.table
        fqn = f"{self.project}.{self.dataset}.{table.split('.')[-1]}"
        self.client.query(f"TRUNCATE TABLE {fqn}").result()

    def insert_rows(self, table: str, columns: List[str], rows: List[tuple]):
        if not rows:
            return
        fqn = f"{self.project}.{self.dataset}.{table.split('.')[-1]}"
        # Convert tuples to list of dicts for BQ
        row_dicts = [dict(zip(columns, r)) for r in rows]
        errors = self.client.insert_rows_json(fqn, row_dicts)
        if errors:
            raise RuntimeError(f"BQ Insert Errors: {errors}")

    def close(self):
        pass


class DatabricksAdapter(DbAdapter):
    def __init__(self, server_hostname, http_path, access_token):
        from databricks import sql

        self.conn = sql.connect(
            server_hostname=server_hostname, http_path=http_path, access_token=access_token
        )
        self.cursor = self.conn.cursor()

    def connect(self):
        pass

    def execute(self, sql: str):
        self.cursor.execute(sql)

    def truncate_table(self, table: str):
        self.cursor.execute(f"TRUNCATE TABLE {table}")

    def insert_rows(self, table: str, columns: List[str], rows: List[tuple]):
        if not rows:
            return
        # Databricks SQL connector doesn't support executemany well for inserts,
        # but modern versions do. We'll use parameter binding.
        placeholders = ", ".join(["%s"] * len(columns))
        col_str = ", ".join(columns)

        # Batch insert
        for row in rows:
            self.cursor.execute(f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})", row)

    def close(self):
        self.cursor.close()
        self.conn.close()


# ==========================================
# Config Parser
# ==========================================


def parse_config(config: Dict[str, Any]) -> Dict[str, List[tuple]]:
    """
    Parses YAML config into rows for each idr_meta table.
    Returns: { 'idr_meta.source_table': [ (val1, val2...), ... ] }
    """
    data = {
        "idr_meta.source_table": [],
        "idr_meta.source": [],
        "idr_meta.rule": [],
        "idr_meta.identifier_mapping": [],
        "idr_meta.entity_attribute_mapping": [],
        "idr_meta.survivorship_rule": [],
        "idr_meta.identifier_exclusion": [],
    }

    # 1. Rules
    for r in config.get("rules", []):
        settings = r.get("settings", {})
        data["idr_meta.rule"].append(
            (
                r["rule_id"],
                r.get("description", r["rule_id"]),
                r.get("is_active", True),
                r["priority"],
                r["identifier_type"],
                settings.get("canonicalize", "NONE"),
                settings.get("allow_hashed", True),
                settings.get("require_non_null", True),
                settings.get("max_group_size", 10000),
            )
        )

    # 2. Sources
    for s in config.get("sources", []):
        table_id = s["table_id"]
        # source_table
        data["idr_meta.source_table"].append(
            (
                table_id,
                s["table_fqn"],
                s.get("entity_type", "PERSON"),
                s["entity_key_expr"],
                s["watermark_column"],
                s.get("lookback_minutes", 0),
                True,
            )
        )

        # source
        data["idr_meta.source"].append(
            (table_id, s.get("source_name", table_id.upper()), s.get("trust_rank", 99), True)
        )

        # identifier_mapping
        for m in s.get("identifiers", []):
            data["idr_meta.identifier_mapping"].append(
                (table_id, m["type"], m["expr"], m.get("is_hashed", False))
            )

        # entity_attribute_mapping
        if "attributes" in s:
            for attr_name, attr_expr in s["attributes"].items():
                data["idr_meta.entity_attribute_mapping"].append((table_id, attr_name, attr_expr))

    # 3. Survivorship
    if "survivorship" in config:
        for attr, rule in config["survivorship"].items():
            priority_list = ",".join(rule.get("priority_list", []))
            data["idr_meta.survivorship_rule"].append(
                (
                    attr,
                    rule["strategy"],
                    priority_list if priority_list else None,
                    rule.get("recency_field"),
                )
            )

    # 4. Exclusions
    for e in config.get("exclusions", []):
        data["idr_meta.identifier_exclusion"].append(
            (
                e["identifier_type"],
                e["value"],
                e.get("match_type", "EXACT"),
                e.get("reason", "Manual exclusion"),
            )
        )

    return data


# ==========================================
# Main
# ==========================================


def main():
    parser = argparse.ArgumentParser(description="Load IDR metadata from YAML")
    parser.add_argument(
        "--platform", required=True, choices=["duckdb", "snowflake", "bigquery", "databricks"]
    )
    parser.add_argument("--config", required=True, help="Path to config.yaml")

    # DuckDB args
    parser.add_argument("--db", default="idr.duckdb", help="DuckDB database file")

    # Cloud args (usually env vars, but allow flags)
    parser.add_argument("--project", help="GCP Project")

    args = parser.parse_args()

    # Load YAML
    print(f"📖 Reading config from {args.config}...")
    with open(args.config) as f:
        config = yaml.safe_load(f)

    print("⚙️  Parsing configuration...")
    tables_data = parse_config(config)

    # Connect to Adapter
    adapter = None
    if args.platform == "duckdb":
        adapter = DuckDBAdapter(args.db)
    elif args.platform == "snowflake":
        adapter = SnowflakeAdapter(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        )
    elif args.platform == "bigquery":
        adapter = BigQueryAdapter(project=args.project or os.environ.get("GCP_PROJECT"))
    elif args.platform == "databricks":
        adapter = DatabricksAdapter(
            server_hostname=os.environ["DATABRICKS_HOST"],
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"],
        )

    try:
        print(f"🔌 Connected to {args.platform}")

        # Define Columns (matching table definitions)
        schemas = {
            "idr_meta.rule": [
                "rule_id",
                "rule_name",
                "is_active",
                "priority",
                "identifier_type",
                "canonicalize",
                "allow_hashed",
                "require_non_null",
                "max_group_size",
            ],
            "idr_meta.source_table": [
                "table_id",
                "table_fqn",
                "entity_type",
                "entity_key_expr",
                "watermark_column",
                "watermark_lookback_minutes",
                "is_active",
            ],
            "idr_meta.source": ["table_id", "source_name", "trust_rank", "is_active"],
            "idr_meta.identifier_mapping": [
                "table_id",
                "identifier_type",
                "identifier_value_expr",
                "is_hashed",
            ],
            "idr_meta.entity_attribute_mapping": ["table_id", "attribute_name", "attribute_expr"],
            "idr_meta.survivorship_rule": [
                "attribute_name",
                "strategy",
                "source_priority_list",
                "recency_field",
            ],
            "idr_meta.identifier_exclusion": [
                "identifier_type",
                "identifier_value_pattern",
                "match_type",
                "reason",
            ],
        }

        for table, rows in tables_data.items():
            print(f"   Writing {len(rows)} rows to {table}...")
            adapter.truncate_table(table)
            adapter.insert_rows(table, schemas[table], rows)

        print("✅ Metadata load complete!")

    finally:
        if adapter:
            adapter.close()


if __name__ == "__main__":
    main()
