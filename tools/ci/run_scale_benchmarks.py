#!/usr/bin/env python3
"""
Repeatable scale benchmark harness for CI.

Outputs:
- One aggregate versioned JSON artifact per run
- One per-profile JSON artifact
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from idr_core import __version__ as idr_version
from idr_core.config import config_to_sql
from idr_core.quickstart import configure_metadata, generate_demo_data
from idr_core.runner import IDRRunner, RunConfig
from idr_core.schema_manager import SchemaManager

DEFAULT_PROFILES_PATH = Path("tools/ci/benchmark_profiles.json")
DEFAULT_OUTPUT_DIR = Path("benchmark_artifacts")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_git_sha() -> str:
    github_sha = os.environ.get("GITHUB_SHA")
    if github_sha:
        return github_sha
    try:
        return (
            subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL)
            .strip()
        )
    except Exception:
        return "unknown"


def _percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(float(v) for v in values)
    pos = (len(ordered) - 1) * max(0.0, min(1.0, q))
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    if lo == hi:
        return ordered[lo]
    frac = pos - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def _rollup(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"min": 0.0, "max": 0.0, "avg": 0.0, "p50": 0.0, "p95": 0.0}
    floats = [float(v) for v in values]
    return {
        "min": min(floats),
        "max": max(floats),
        "avg": sum(floats) / len(floats),
        "p50": _percentile(floats, 0.50),
        "p95": _percentile(floats, 0.95),
    }


def _sql_compile_workload_config() -> Dict[str, Any]:
    return {
        "sources": [
            {
                "id": "customers",
                "table": "raw.customers",
                "entity_key": "customer_id",
                "identifiers": [
                    {"type": "EMAIL", "expr": "LOWER(email)"},
                    {"type": "PHONE", "expr": "phone_number"},
                    {"type": "LOYALTY", "expr": "loyalty_id"},
                ],
                "attributes": [
                    {"name": "email", "expr": "LOWER(email)"},
                    {"name": "first_name", "expr": "first_name"},
                    {"name": "last_name", "expr": "last_name"},
                ],
            },
            {
                "id": "orders",
                "table": "raw.orders",
                "entity_key": "order_id",
                "identifiers": [
                    {"type": "EMAIL", "expr": "LOWER(customer_email)"},
                    {"type": "PHONE", "expr": "customer_phone"},
                ],
                "attributes": [
                    {"name": "email", "expr": "LOWER(customer_email)"},
                    {"name": "order_total", "expr": "CAST(order_total AS VARCHAR)"},
                ],
            },
        ],
        "rules": [
            {
                "id": "email_exact",
                "type": "EXACT",
                "match_keys": ["EMAIL"],
                "priority": 1,
                "max_group_size": 200000,
            },
            {
                "id": "phone_exact",
                "type": "EXACT",
                "match_keys": ["PHONE"],
                "priority": 2,
                "max_group_size": 100000,
            },
            {
                "id": "loyalty_exact",
                "type": "EXACT",
                "match_keys": ["LOYALTY"],
                "priority": 3,
                "max_group_size": 10000,
            },
        ],
        "survivorship": [
            {
                "attribute": "email",
                "strategy": "PRIORITY",
                "source_priority": ["customers", "orders"],
            }
        ],
    }


def _run_duckdb_pipeline_profile(profile: Dict[str, Any], output_dir: Path) -> Dict[str, Any]:
    import duckdb

    from idr_core.adapters.duckdb import DuckDBAdapter

    profile_id = str(profile["id"])
    rows = int(profile.get("rows", 10000))
    repetitions = max(1, int(profile.get("repetitions", 1)))
    seed = int(profile.get("seed", 42))
    strict = bool(profile.get("strict", False))
    max_iters = int(profile.get("max_iters", 30))

    samples: List[Dict[str, Any]] = []
    run_durations: List[float] = []
    total_durations: List[float] = []
    schema_durations: List[float] = []
    data_durations: List[float] = []
    metadata_durations: List[float] = []

    for idx in range(repetitions):
        db_path = output_dir / f"{profile_id}_iter{idx + 1}.duckdb"
        if db_path.exists():
            db_path.unlink()
        wal_path = Path(str(db_path) + ".wal")
        if wal_path.exists():
            wal_path.unlink()

        started = time.perf_counter()
        conn = duckdb.connect(str(db_path))
        adapter = DuckDBAdapter(conn)
        try:
            t0 = time.perf_counter()
            schema_mgr = SchemaManager(adapter)
            schema_mgr.initialize(reset=True)
            schema_seconds = time.perf_counter() - t0

            t0 = time.perf_counter()
            generated_rows = generate_demo_data(conn, rows=rows, seed=seed + idx)
            data_seconds = time.perf_counter() - t0

            t0 = time.perf_counter()
            configure_metadata(conn)
            metadata_seconds = time.perf_counter() - t0

            t0 = time.perf_counter()
            result = IDRRunner(adapter).run(
                RunConfig(run_mode="FULL", strict=strict, max_iters=max_iters)
            )
            run_seconds = time.perf_counter() - t0
            total_seconds = time.perf_counter() - started

            clusters = conn.execute(
                "SELECT COUNT(*) FROM idr_out.identity_clusters_current"
            ).fetchone()[0]
            memberships = conn.execute(
                "SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current"
            ).fetchone()[0]
            edges = conn.execute("SELECT COUNT(*) FROM idr_out.identity_edges_current").fetchone()[0]

            samples.append(
                {
                    "iteration": idx + 1,
                    "rows_generated": int(generated_rows),
                    "status": result.status,
                    "run_id": result.run_id,
                    "run_duration_seconds": float(run_seconds),
                    "total_duration_seconds": float(total_seconds),
                    "schema_init_seconds": float(schema_seconds),
                    "data_generation_seconds": float(data_seconds),
                    "metadata_config_seconds": float(metadata_seconds),
                    "entities_processed": int(result.entities_processed),
                    "edges_created": int(result.edges_created),
                    "clusters_impacted": int(result.clusters_impacted),
                    "clusters_current_count": int(clusters),
                    "membership_current_count": int(memberships),
                    "edges_current_count": int(edges),
                }
            )

            run_durations.append(float(run_seconds))
            total_durations.append(float(total_seconds))
            schema_durations.append(float(schema_seconds))
            data_durations.append(float(data_seconds))
            metadata_durations.append(float(metadata_seconds))
        finally:
            conn.close()

    successful_runs = sum(
        1
        for sample in samples
        if str(sample.get("status", "")).startswith("SUCCESS")
        or str(sample.get("status", "")) == "DRY_RUN_COMPLETE"
    )
    run_success_rate = float(successful_runs) / float(len(samples)) if samples else 0.0

    return {
        "profile_id": profile_id,
        "platform": "duckdb",
        "mode": "pipeline",
        "status": "success",
        "started_at": _utc_now_iso(),
        "samples": samples,
        "metrics": {
            "rows": rows,
            "repetitions": repetitions,
            "run_success_rate": run_success_rate,
            "run_duration_seconds": _rollup(run_durations),
            "total_duration_seconds": _rollup(total_durations),
            "schema_init_seconds": _rollup(schema_durations),
            "data_generation_seconds": _rollup(data_durations),
            "metadata_config_seconds": _rollup(metadata_durations),
        },
    }


def _run_sql_compile_profile(profile: Dict[str, Any]) -> Dict[str, Any]:
    profile_id = str(profile["id"])
    platform_name = str(profile.get("platform", "unknown"))
    repetitions = max(1, int(profile.get("repetitions", 25)))
    config_payload = _sql_compile_workload_config()

    samples: List[Dict[str, Any]] = []
    durations: List[float] = []
    statement_count = 0

    for idx in range(repetitions):
        started = time.perf_counter()
        statements = config_to_sql(config_payload, dialect=platform_name)
        elapsed = time.perf_counter() - started
        statement_count = len(statements)
        durations.append(float(elapsed))
        samples.append(
            {
                "iteration": idx + 1,
                "duration_seconds": float(elapsed),
                "statement_count": statement_count,
            }
        )

    return {
        "profile_id": profile_id,
        "platform": platform_name,
        "mode": "sql_compile",
        "status": "success",
        "started_at": _utc_now_iso(),
        "samples": samples,
        "metrics": {
            "repetitions": repetitions,
            "compile_success_rate": 1.0,
            "statement_count": int(statement_count),
            "compile_duration_seconds": _rollup(durations),
        },
    }


def _run_api_latency_profile(profile: Dict[str, Any]) -> Dict[str, Any]:
    from fastapi.testclient import TestClient

    import idr_api.dependencies as deps
    from idr_api.main import app

    profile_id = str(profile["id"])
    endpoints = profile.get("endpoints", ["/api/health", "/api/schema", "/api/auth/whoami"])
    warmup_requests = max(0, int(profile.get("warmup_requests", 2)))
    requests_per_endpoint = max(1, int(profile.get("requests_per_endpoint", 30)))

    if not isinstance(endpoints, list) or not endpoints:
        raise ValueError("api_latency profile requires non-empty 'endpoints' list")

    deps.OIDC_ISSUER = ""
    deps.ALLOW_INSECURE_DEV_AUTH = True

    # Suppress per-request httpx noise in benchmark logs.
    prev_httpx_level = logging.getLogger("httpx").level
    prev_httpcore_level = logging.getLogger("httpcore").level
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    samples: List[Dict[str, Any]] = []
    latencies: List[float] = []
    error_count = 0
    per_endpoint_latencies: Dict[str, List[float]] = {str(e): [] for e in endpoints}
    per_endpoint_errors: Dict[str, int] = {str(e): 0 for e in endpoints}

    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            for endpoint in endpoints:
                endpoint = str(endpoint)
                for _ in range(warmup_requests):
                    client.get(endpoint)

                for iteration in range(requests_per_endpoint):
                    started = time.perf_counter()
                    response = client.get(endpoint)
                    elapsed = time.perf_counter() - started
                    ok = 200 <= int(response.status_code) < 400
                    if not ok:
                        error_count += 1
                        per_endpoint_errors[endpoint] += 1
                    per_endpoint_latencies[endpoint].append(float(elapsed))
                    latencies.append(float(elapsed))
                    samples.append(
                        {
                            "endpoint": endpoint,
                            "iteration": iteration + 1,
                            "status_code": int(response.status_code),
                            "latency_seconds": float(elapsed),
                        }
                    )
    finally:
        logging.getLogger("httpx").setLevel(prev_httpx_level)
        logging.getLogger("httpcore").setLevel(prev_httpcore_level)

    total_requests = len(samples)
    success_count = total_requests - error_count
    error_rate = float(error_count) / float(total_requests) if total_requests else 1.0

    endpoint_metrics: Dict[str, Any] = {}
    for endpoint in endpoints:
        endpoint = str(endpoint)
        endpoint_count = len(per_endpoint_latencies[endpoint])
        endpoint_error_rate = (
            float(per_endpoint_errors[endpoint]) / float(endpoint_count)
            if endpoint_count
            else 1.0
        )
        endpoint_metrics[endpoint] = {
            "request_count": endpoint_count,
            "error_count": int(per_endpoint_errors[endpoint]),
            "error_rate": endpoint_error_rate,
            "latency_seconds": _rollup(per_endpoint_latencies[endpoint]),
        }

    return {
        "profile_id": profile_id,
        "platform": "api",
        "mode": "api_latency",
        "status": "success",
        "started_at": _utc_now_iso(),
        "samples": samples,
        "metrics": {
            "request_count": int(total_requests),
            "success_count": int(success_count),
            "error_count": int(error_count),
            "success_rate": (float(success_count) / float(total_requests)) if total_requests else 0.0,
            "error_rate": error_rate,
            "request_latency_seconds": _rollup(latencies),
            "by_endpoint": endpoint_metrics,
        },
    }


def _run_profile(profile: Dict[str, Any], output_dir: Path) -> Dict[str, Any]:
    mode = str(profile.get("mode", "")).strip().lower()
    platform_name = str(profile.get("platform", "")).strip().lower()
    if mode == "pipeline" and platform_name == "duckdb":
        return _run_duckdb_pipeline_profile(profile, output_dir=output_dir)
    if mode == "sql_compile":
        return _run_sql_compile_profile(profile)
    if mode == "api_latency":
        return _run_api_latency_profile(profile)
    raise ValueError(f"Unsupported benchmark profile mode/platform: mode={mode}, platform={platform_name}")


def run_benchmarks(
    profiles: List[Dict[str, Any]],
    output_dir: Path,
    run_label: str,
) -> Dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    git_sha = _safe_git_sha()
    generated_at = _utc_now_iso()

    profile_results: List[Dict[str, Any]] = []
    for profile in profiles:
        profile_id = str(profile.get("id", "unknown"))
        print(f"[benchmark] running profile={profile_id}")
        try:
            result = _run_profile(profile, output_dir=output_dir)
        except Exception as exc:
            result = {
                "profile_id": profile_id,
                "platform": profile.get("platform"),
                "mode": profile.get("mode"),
                "status": "failed",
                "error": str(exc),
                "traceback": traceback.format_exc(limit=10),
                "started_at": _utc_now_iso(),
                "samples": [],
                "metrics": {},
            }
        profile_results.append(result)
        (output_dir / f"benchmark_profile_{profile_id}.json").write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    summary = {
        "success": sum(1 for r in profile_results if r.get("status") == "success"),
        "failed": sum(1 for r in profile_results if r.get("status") == "failed"),
        "skipped": sum(1 for r in profile_results if r.get("status") == "skipped"),
        "total": len(profile_results),
    }

    aggregate = {
        "artifact_version": "1.0",
        "run_label": run_label,
        "generated_at": generated_at,
        "project_version": idr_version,
        "git_sha": git_sha,
        "python_version": sys.version,
        "system": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
        },
        "profiles": profile_results,
        "summary": summary,
    }

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    sha12 = git_sha[:12] if git_sha != "unknown" else "unknown"
    aggregate_name = f"benchmark_metrics_v{idr_version}_{timestamp}_{sha12}.json"
    aggregate_path = output_dir / aggregate_name
    aggregate_path.write_text(
        json.dumps(aggregate, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "benchmark_metrics_latest.json").write_text(
        json.dumps(aggregate, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print(f"[benchmark] wrote aggregate artifact: {aggregate_path}")
    print(f"[benchmark] summary: {summary}")
    return aggregate


def _load_profiles(profiles_path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(profiles_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("profiles"), list):
        raise ValueError("Profiles file must be a JSON object with a 'profiles' list")
    return payload["profiles"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run CI scale benchmark harness")
    parser.add_argument(
        "--profiles",
        default=str(DEFAULT_PROFILES_PATH),
        help="Path to benchmark profile JSON file",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for benchmark artifacts",
    )
    parser.add_argument(
        "--run-label",
        default="manual",
        help="Logical run label (for example: ci, nightly, release-candidate)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    profiles_path = Path(args.profiles)
    output_dir = Path(args.output_dir)

    if not profiles_path.exists():
        print(f"Profiles file not found: {profiles_path}", file=sys.stderr)
        return 2

    try:
        profiles = _load_profiles(profiles_path)
        aggregate = run_benchmarks(profiles=profiles, output_dir=output_dir, run_label=args.run_label)
    except Exception as exc:
        print(f"Benchmark harness failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 1

    failed = int(aggregate.get("summary", {}).get("failed", 0))
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
