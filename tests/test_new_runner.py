#!/usr/bin/env python3
"""
Test script for the new idr_core package.

This script tests the unified runner against existing test data.
Run after DDL migration and metadata setup.

Usage:
    python test_new_runner.py --db=path/to/idr.duckdb --mode=FULL
    python test_new_runner.py --db=path/to/idr.duckdb --mode=FULL --dry-run
"""

import argparse
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from idr_core import IDRRunner
from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.runner import RunConfig


def main():
    parser = argparse.ArgumentParser(description="Test new IDR runner with DuckDB")
    parser.add_argument("--db", required=True, help="DuckDB database path")
    parser.add_argument("--mode", choices=["FULL", "INCR"], default="FULL")
    parser.add_argument("--max-iters", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"\n{'=' * 60}")
    print("  IDR Core Package Test")
    print(f"{'=' * 60}")
    print(f"  Database: {args.db}")
    print(f"  Mode:     {args.mode}")
    print(f"  Dry Run:  {args.dry_run}")
    print(f"{'=' * 60}\n")

    # Create adapter
    adapter = DuckDBAdapter(args.db)

    try:
        # Pre-flight checks
        print("Step 1: Validating environment...")

        # Check schemas exist
        schemas = adapter.query("SELECT schema_name FROM information_schema.schemata")
        schema_names = [s["schema_name"] for s in schemas]
        for required in ["idr_meta", "idr_work", "idr_out"]:
            if required not in schema_names:
                print(f"  ❌ Missing schema: {required}")
                return 1
            print(f"  ✅ Schema {required} exists")

        # Check source tables
        sources = adapter.query(
            "SELECT table_id, table_fqn FROM idr_meta.source_table WHERE is_active = TRUE"
        )
        print(f"\n  Found {len(sources)} active source tables:")
        for src in sources:
            exists = adapter.table_exists(src["table_fqn"])
            status = "✅" if exists else "❌"
            count = adapter.query_one(f"SELECT COUNT(*) FROM {src['table_fqn']}") if exists else 0
            print(f"    {status} {src['table_id']}: {src['table_fqn']} ({count:,} rows)")

        # Check rules
        rules = adapter.query(
            "SELECT rule_id, identifier_type FROM idr_meta.rule WHERE is_active = TRUE"
        )
        print(f"\n  Found {len(rules)} active rules:")
        for r in rules:
            print(f"    - {r['rule_id']}: {r['identifier_type']}")

        # Run IDR
        print(f"\nStep 2: Running IDR ({args.mode} mode)...")

        runner = IDRRunner(adapter)
        config = RunConfig(run_mode=args.mode, max_iters=args.max_iters, dry_run=args.dry_run)

        result = runner.run(config)

        # Print results
        print(f"\n{'=' * 60}")
        print(f"  Result: {result.status}")
        print(f"{'=' * 60}")
        print(f"  Run ID:          {result.run_id}")
        print(f"  Entities:        {result.entities_processed:,}")
        print(f"  Identifiers:     {result.identifiers_extracted:,}")
        print(f"  Edges:           {result.edges_created:,}")
        print(f"  Clusters:        {result.clusters_impacted:,}")
        print(f"  LP Iterations:   {result.lp_iterations}")
        print(f"  Duration:        {result.duration_seconds:.1f}s")

        if result.warnings:
            print("\n  Warnings:")
            for w in result.warnings:
                print(f"    ⚠️  {w}")

        if result.error:
            print(f"\n  ❌ Error: {result.error}")
            return 1

        # Verify output
        if not args.dry_run and result.status == "SUCCESS":
            print("\nStep 3: Verifying output tables...")

            membership = adapter.query_one(
                "SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current"
            )
            clusters = adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_clusters_current")
            edges = adapter.query_one("SELECT COUNT(*) FROM idr_out.identity_edges_current")

            print(f"  Memberships:  {membership:,}")
            print(f"  Clusters:     {clusters:,}")
            print(f"  Edges:        {edges:,}")

            # Sample clusters
            sample = adapter.query("""
                SELECT resolved_id, cluster_size
                FROM idr_out.identity_clusters_current
                ORDER BY cluster_size DESC
                LIMIT 5
            """)
            print("\n  Top 5 clusters by size:")
            for s in sample:
                print(f"    - {s['resolved_id'][:30]}... ({s['cluster_size']} members)")

        print(f"\n{'=' * 60}")
        print("  ✅ Test complete!")
        print(f"{'=' * 60}\n")

        return 0 if "SUCCESS" in result.status or result.status == "DRY_RUN_COMPLETE" else 1

    except Exception as e:
        print(f"\n  ❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    finally:
        adapter.close()


if __name__ == "__main__":
    sys.exit(main())
