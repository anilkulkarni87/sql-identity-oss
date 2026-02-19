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
```

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

### Release Workflow

`release.yml` is tag-driven (`v*`) and produces reproducible artifacts:
- Python wheel + sdist
- Wheel install smoke check
- GHCR images for `idr-api` and `idr-ui`
- Trivy vulnerability gating on dist artifacts and container images (fails on HIGH/CRITICAL)
- Signed build provenance attestations for dist artifacts and pushed images

Manual runs are available through `workflow_dispatch` to optionally push images or publish package.
Release summary includes an attestation link:
`https://github.com/<owner>/<repo>/attestations`

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
