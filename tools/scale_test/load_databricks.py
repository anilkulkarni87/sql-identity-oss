#!/usr/bin/env python3
"""
Load retail IDR Parquet datasets into Databricks using Spark.

This script is intended to run in Databricks (notebook/job) where `spark` exists
or PySpark is available via databricks-connect.
"""

import argparse
import os
from collections import defaultdict


def discover_tables(input_dir: str) -> dict:
    tables = defaultdict(list)
    for name in os.listdir(input_dir):
        if not name.endswith(".parquet"):
            continue
        base = name.split("_part_")[0]
        tables[base].append(os.path.join(input_dir, name))
    return tables


def main() -> int:
    parser = argparse.ArgumentParser(description="Load Parquet datasets into Databricks.")
    parser.add_argument("--input-dir", required=True, help="Path to generated Parquet files")
    parser.add_argument("--catalog", default=None, help="Unity Catalog (optional)")
    parser.add_argument("--schema", required=True, help="Target schema/database")
    args = parser.parse_args()

    try:
        from pyspark.sql import SparkSession
    except Exception as exc:
        raise SystemExit(f"PySpark not available: {exc}")

    spark = SparkSession.builder.getOrCreate()
    if args.catalog:
        spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.schema}")

    tables = discover_tables(args.input_dir)
    if not tables:
        raise SystemExit("No Parquet files found in input directory.")

    for table, files in sorted(tables.items()):
        df = spark.read.parquet(*sorted(files))
        df.write.mode("overwrite").saveAsTable(f"{args.schema}.{table}")
        print(f"Loaded {table} ({len(files)} files)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
