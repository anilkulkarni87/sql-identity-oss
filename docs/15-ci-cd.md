# CI/CD

Continuous integration and deployment pipelines for SQL Identity Resolution.

## GitHub Actions

### Test Workflows

Automated testing on pull requests ensures stability across platforms.

```yaml
# .github/workflows/test.yml
name: Test
on: [push, pull_request]
jobs:
  test-duckdb:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: '3.11'}
      - run: pip install duckdb pytest
      - run: python -m pytest tests/ --ignore=tests/legacy -v
```

### Package Smoke Gate

`test.yml` also builds wheel/sdist and installs from the built wheel before tests.

```yaml
jobs:
  package-smoke:
    steps:
      - run: python -m build
      - run: twine check dist/*
      - run: |
          python -m venv .venv-smoke
          . .venv-smoke/bin/activate
          pip install dist/*.whl
          idr version
          idr doctor --json
```

`test.yml` includes `quickstart-golden-path`:
- runs `tools/ci/validate_quickstart_path.py`
- enforces end-to-end quickstart completion in `<= 600s`
- uploads timing evidence JSON

### Lockfile Enforcement

CI dependency installation uses pinned lockfiles:
- `requirements/ci.lock` for test, lint, and DDL validation jobs
- `requirements/release.lock` for package/release artifact jobs
- `requirements/docs.lock` for docs workflow

UI dependencies are verified with `npm ci` to enforce `idr_ui/package-lock.json`.

### SBOM Verification

`test.yml` includes an `sbom-verify` job that:
1. Builds Python artifacts (`dist/*`)
2. Builds API and UI Docker images
3. Generates SPDX JSON SBOMs for each artifact/image
4. Verifies SBOM structure in CI using `jq` checks
5. Uploads SBOMs as workflow artifacts

### Enterprise E2E Gate

`test.yml` includes `enterprise-e2e` that validates the enterprise stack end-to-end:
1. Boots `docker-compose.enterprise.yml`
2. Verifies Keycloak realm bootstrap
3. Verifies API health and Prometheus health
4. Confirms `/metrics` payload
5. Confirms auth enforcement on protected endpoints
6. Retrieves OIDC token and validates authenticated API access
7. Verifies Prometheus target health for `idr-api`

`test.yml` also includes `enterprise-ha-e2e` for the HA baseline:
1. Boots `docker-compose.enterprise.ha.yml`
2. Waits for health of `api-a`, `api-b`, API load balancer, Redis, and Keycloak
3. Re-runs the same auth/metrics/Prometheus verification contract through the HA gateway

### Operability Drill Gates

`test.yml` includes:
- `backup-restore-drill`
  - generates synthetic control-plane state
  - runs `tools/ci/run_backup_restore_drill.py` with incident simulation
  - enforces `RPO=0` and bounded `RTO`
  - uploads drill evidence JSON
- `rollback-dryrun`
  - validates pinned-image rollback rendering via `tools/ci/validate_prod_rollback.sh`
  - confirms `idr-api` and `idr-ui` resolve to the rollback tag before execution
- `secrets-posture`
  - validates compose files do not contain plaintext runtime secret values
  - validates required enterprise admin secret injection syntax
  - validates runtime webhook bearer token rotation using `*_FILE` secret path

### Scale Benchmark Harness Gate

`test.yml` includes `benchmark-harness` to produce reproducible performance artifacts:
1. Loads profile definitions from `tools/ci/benchmark_profiles.json`
2. Runs `tools/ci/run_scale_benchmarks.py`
3. Enforces SLO thresholds via `tools/ci/check_benchmark_slos.py` and `tools/ci/benchmark_slo_thresholds.json`
4. Emits versioned benchmark JSON artifacts (aggregate + per-profile)
5. Uploads `benchmark_artifacts/` (including SLO report) to the workflow as evidence

Benchmark modes in CI:
- `duckdb` full pipeline benchmark (`pipeline` mode)
- `snowflake`, `bigquery`, `databricks` SQL compilation benchmarks (`sql_compile` mode)
- API latency profile (`api_latency` mode) for core endpoints

### Resilience Regression Gate

Queue resilience regressions are covered in unit tests (`tests/test_job_manager_resilience.py`) for:
- transient DB failure retry semantics
- worker crash/restart recovery from `RUNNING`/`CANCEL_REQUESTED`
- max-attempt exhaustion handling during restart recovery
- cancellation recovery to deterministic terminal state

### Release Workflow

`release.yml` is tag-driven (`v*`) and produces reproducible artifacts:
- Python wheel + sdist
- Wheel install smoke check
- GHCR images for `idr-api` and `idr-ui`
- Trivy vulnerability gating on dist artifacts and container images (fails on HIGH/CRITICAL)
- Signed build provenance attestations for dist artifacts and pushed images
- Keyless Cosign signatures for API/UI images + signature verification in pipeline

Manual runs are available through `workflow_dispatch` to optionally push images or publish package.
Release summary includes an attestation link:
`https://github.com/<owner>/<repo>/attestations`

### Helm Package Gate

`test.yml` includes `helm-package`:
- `helm lint deployment/helm/idr-enterprise`
- `helm template ... -f values.example.yaml`

### Cloud Deploy Pipeline Template

Reference template for staged cloud deploy + verification:
- `tools/ci/templates/github-actions-cloud-deploy.yml`

Template flow:
1. bootstrap Kubernetes secrets
2. deploy plan gate (`tools/deploy/idr_deploy.sh --mode plan`)
3. deploy apply (`tools/deploy/idr_deploy.sh --mode apply`)
4. post-deploy verification (`idr doctor --target cluster`)

For full infra provisioning (network + cluster + ingress + secret manager), use Terraform modules in:
- `deployment/terraform/`

### DDL Validation

Ensure SQL schema definitions are valid before merging.

```yaml
# .github/workflows/validate-ddl.yml
name: Validate DDL
on:
  pull_request:
    paths: ['sql/**/*.sql']
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install duckdb
      - run: |
          python -c "
          import duckdb
          conn = duckdb.connect(':memory:')
          with open('sql/ddl/duckdb.sql') as f:
              conn.execute(f.read())
          print('DuckDB DDL valid')
          "
```

## Pre-commit Hooks

Use pre-commit to catch issues locally.

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/psf/black
    rev: 23.12.1
    hooks:
      - id: black
        language_version: python3.11

  - repo: local
    hooks:
      - id: validate-ddl
        name: Validate DuckDB DDL
        entry: python -c "import duckdb; duckdb.connect(':memory:').execute(open('sql/ddl/duckdb.sql').read())"
        language: python
        files: sql/ddl/.*\.sql$
        additional_dependencies: [duckdb]
```

## Secrets Management

| Secret | Description |
|--------|-------------|
| `SNOWFLAKE_ACCOUNT` | Snowflake Account ID |
| `SNOWFLAKE_USER` | Service Account User |
| `GCP_PROJECT` | BigQuery Project ID |
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_TOKEN` | Access Token |

Use **GitHub Environments** (`snowflake-test`, `gcp-test`) to isolate secrets.
