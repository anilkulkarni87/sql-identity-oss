#!/usr/bin/env python3
"""
Load retail IDR Parquet datasets into Snowflake using PUT/COPY.
"""

import argparse
import os
from collections import defaultdict

import snowflake.connector


def discover_tables(input_dir: str) -> dict:
    tables = defaultdict(list)
    for name in os.listdir(input_dir):
        if not name.endswith(".parquet"):
            continue
        base = name.split("_part_")[0]
        tables[base].append(os.path.join(input_dir, name))
    return tables


def main() -> int:
    parser = argparse.ArgumentParser(description="Load Parquet datasets into Snowflake.")
    parser.add_argument("--input-dir", required=True, help="Path to generated Parquet files")
    parser.add_argument("--account", required=True, help="Snowflake account")
    parser.add_argument("--user", required=True, help="Snowflake user")
    parser.add_argument("--password", required=True, help="Snowflake password")
    parser.add_argument("--warehouse", required=True, help="Snowflake warehouse")
    parser.add_argument("--database", required=True, help="Snowflake database")
    parser.add_argument("--schema", required=True, help="Snowflake schema")
    parser.add_argument("--stage", default="@~", help="Stage to use (default: @~)")
    args = parser.parse_args()

    tables = discover_tables(args.input_dir)
    if not tables:
        raise SystemExit("No Parquet files found in input directory.")

    conn = snowflake.connector.connect(
        account=args.account,
        user=args.user,
        password=args.password,
        warehouse=args.warehouse,
        database=args.database,
        schema=args.schema,
    )
    cur = conn.cursor()
    cur.execute("CREATE FILE FORMAT IF NOT EXISTS idr_parquet TYPE=PARQUET")

    for table, files in sorted(tables.items()):
        stage_path = f"{args.stage}/idr_load/{table}"
        for path in sorted(files):
            cur.execute(f"PUT file://{path} {stage_path} AUTO_COMPRESS=FALSE")

        cur.execute(f"""
            CREATE OR REPLACE TABLE {table} USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(INFER_SCHEMA(
                    LOCATION=>'{stage_path}',
                    FILE_FORMAT=>'idr_parquet'
                ))
            )
        """)
        cur.execute(f"""
            COPY INTO {table}
            FROM '{stage_path}'
            FILE_FORMAT=(TYPE=PARQUET)
        """)
        print(f"Loaded {table} ({len(files)} files)")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
