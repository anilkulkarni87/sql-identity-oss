#!/usr/bin/env python3
"""
Load retail IDR Parquet datasets into BigQuery from local files.
"""

import argparse
import os
from collections import defaultdict

from google.cloud import bigquery


def discover_tables(input_dir: str) -> dict:
    tables = defaultdict(list)
    for name in os.listdir(input_dir):
        if not name.endswith(".parquet"):
            continue
        base = name.split("_part_")[0]
        tables[base].append(os.path.join(input_dir, name))
    return tables


def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str) -> None:
    dataset_ref = bigquery.Dataset(dataset_id)
    dataset_ref.location = location
    try:
        client.get_dataset(dataset_id)
    except Exception:
        client.create_dataset(dataset_ref, exists_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Load Parquet datasets into BigQuery.")
    parser.add_argument("--input-dir", required=True, help="Path to generated Parquet files")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", required=True, help="BigQuery dataset")
    parser.add_argument("--location", default="US", help="BigQuery location")
    parser.add_argument("--create-dataset", action="store_true", help="Create dataset if missing")
    args = parser.parse_args()

    tables = discover_tables(args.input_dir)
    if not tables:
        raise SystemExit("No Parquet files found in input directory.")

    client = bigquery.Client(project=args.project)
    dataset_id = f"{args.project}.{args.dataset}"
    if args.create_dataset:
        ensure_dataset(client, dataset_id, args.location)

    for table, files in sorted(tables.items()):
        table_id = f"{dataset_id}.{table}"
        files = sorted(files)
        for idx, path in enumerate(files):
            write_disposition = (
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if idx == 0
                else bigquery.WriteDisposition.WRITE_APPEND
            )
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=write_disposition,
            )
            with open(path, "rb") as f:
                job = client.load_table_from_file(
                    f, table_id, location=args.location, job_config=job_config
                )
            job.result()
        print(f"Loaded {table} ({len(files)} files)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
