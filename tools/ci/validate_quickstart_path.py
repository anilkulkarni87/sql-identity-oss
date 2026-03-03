#!/usr/bin/env python
"""
Validate the OSS golden path quickstart completes within a target duration.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb

from idr_core.quickstart import run_quickstart


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate quickstart golden path timing.")
    parser.add_argument("--max-seconds", type=float, default=600.0)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", default="tmp/quickstart_ci.duckdb")
    parser.add_argument("--report", default="tmp/quickstart_ci_report.json")
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    start = time.perf_counter()
    rc = run_quickstart(output=str(output_path), rows=args.rows, seed=args.seed, verbose=False)
    elapsed = time.perf_counter() - start

    entities = 0
    clusters = 0
    profiles = None
    if rc == 0 and output_path.exists():
        conn = duckdb.connect(str(output_path), read_only=True)
        try:
            clusters = int(
                conn.execute("SELECT COUNT(*) FROM idr_out.identity_clusters_current").fetchone()[0]
            )
            entities = int(
                conn.execute("SELECT COALESCE(SUM(cluster_size), 0) FROM idr_out.identity_clusters_current").fetchone()[0]
            )
            profile_exists = conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = 'idr_out' AND table_name = 'golden_profile_current'
                """
            ).fetchone()[0]
            if int(profile_exists) > 0:
                profiles = int(
                    conn.execute("SELECT COUNT(*) FROM idr_out.golden_profile_current").fetchone()[0]
                )
        finally:
            conn.close()

    report = {
        "max_seconds": args.max_seconds,
        "elapsed_seconds": elapsed,
        "rows": args.rows,
        "run_exit_code": rc,
        "entities": entities,
        "clusters": clusters,
        "profiles": profiles,
        "status": "pass",
    }

    if rc != 0:
        report["status"] = "fail"
        report["failure_reason"] = "quickstart_run_failed"
    elif elapsed > args.max_seconds:
        report["status"] = "fail"
        report["failure_reason"] = "quickstart_exceeded_time_budget"
    elif entities <= 0 or clusters <= 0:
        report["status"] = "fail"
        report["failure_reason"] = "quickstart_outputs_missing"

    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
