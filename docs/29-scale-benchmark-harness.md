# Scale Benchmark Harness

This repository includes a repeatable benchmark harness for CI:

- Runner script: `tools/ci/run_scale_benchmarks.py`
- Profile matrix: `tools/ci/benchmark_profiles.json`
- SLO policy: `tools/ci/benchmark_slo_thresholds.json`
- SLO gate: `tools/ci/check_benchmark_slos.py`
- CI job: `.github/workflows/test.yml` (`benchmark-harness`)

## What It Produces

Each run emits versioned JSON artifacts:

- `benchmark_metrics_v<version>_<timestamp>_<sha>.json` (aggregate)
- `benchmark_metrics_latest.json` (latest aggregate snapshot)
- `benchmark_profile_<profile_id>.json` (one per profile)

Artifacts include:
- project version + git SHA
- runtime metadata (python/system)
- per-profile status/metrics/samples
- aggregate summary (`success`, `failed`, `skipped`, `total`)

## Profile Types

The harness supports two modes:

- `pipeline`:
  - currently implemented for `duckdb`
  - measures schema init, demo data generation, metadata setup, and full IDR run duration
- `sql_compile`:
  - runs dialect-specific config-to-SQL generation benchmark
  - used for `snowflake`, `bigquery`, and `databricks` CI profiles

## Run Locally

```bash
python tools/ci/run_scale_benchmarks.py \
  --profiles tools/ci/benchmark_profiles.json \
  --output-dir benchmark_artifacts \
  --run-label local
```

Or via Make:

```bash
make benchmark
```

## CI Integration

`benchmark-harness` runs in GitHub Actions on Python 3.11 after tests:

1. installs pinned dependencies from `requirements/ci.lock`
2. runs benchmark harness against checked-in profiles
3. uploads `benchmark_artifacts/` as workflow artifacts

This is the baseline required for E04-T01: benchmark evidence becomes reproducible and archived per workflow run.

## SLO Enforcement

After benchmark generation, CI enforces SLO thresholds from `tools/ci/benchmark_slo_thresholds.json`.

```bash
python tools/ci/check_benchmark_slos.py \
  --benchmark-json benchmark_artifacts/benchmark_metrics_latest.json \
  --thresholds tools/ci/benchmark_slo_thresholds.json \
  --report benchmark_artifacts/benchmark_slo_report.json
```

The job fails if any SLO rule is breached. This is the E04-T02 release gate.
