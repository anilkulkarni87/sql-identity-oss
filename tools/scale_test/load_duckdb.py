#!/usr/bin/env python3
"""
Load retail IDR Parquet datasets into DuckDB.
"""

import argparse
import os
from collections import defaultdict

import duckdb


def discover_tables(input_dir: str) -> dict:
    tables = defaultdict(list)
    for name in os.listdir(input_dir):
        if not name.endswith(".parquet"):
            continue
        base = name.split("_part_")[0]
        tables[base].append(os.path.join(input_dir, name))
    return tables


def main() -> int:
    parser = argparse.ArgumentParser(description="Load Parquet datasets into DuckDB.")
    parser.add_argument("--input-dir", required=True, help="Path to generated Parquet files")
    parser.add_argument("--db", required=True, help="DuckDB database file")
    parser.add_argument("--schema", default=None, help="Optional schema name")
    args = parser.parse_args()

    tables = discover_tables(args.input_dir)
    if not tables:
        raise SystemExit("No Parquet files found in input directory.")

    conn = duckdb.connect(args.db)
    if args.schema:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {args.schema}")
        conn.execute(f"SET schema '{args.schema}'")

    for table, files in sorted(tables.items()):
        files_list = ", ".join([f"'{f}'" for f in sorted(files)])
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT * FROM read_parquet([{files_list}])
        """)
        print(f"Loaded {table} ({len(files)} files)")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
