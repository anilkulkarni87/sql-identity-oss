# CI/CD

Continuous integration and deployment for SQL Identity Resolution.

---

## GitHub Actions

### Test Workflow

```yaml
# .github/workflows/test.yml
name: Test

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test-duckdb:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install duckdb pytest

      - name: Run tests
        run: |
          python -m pytest tests/ --ignore=tests/legacy -v

  lint:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install linters
        run: |
          pip install flake8 black

      - name: Lint with flake8
        run: |
          flake8 idr_core/ idr_api/ tests/ tools/ --max-line-length=120 --exclude='**/legacy/**'

      - name: Check formatting with black
        run: |
          black --check sql/ tools/ tests/
```

### Snowflake Test Workflow

```yaml
# .github/workflows/test-snowflake.yml
name: Test Snowflake

on:
  workflow_dispatch:
  schedule:
    - cron: '0 0 * * 0'  # Weekly

jobs:
  test-snowflake:
    runs-on: ubuntu-latest
    environment: snowflake-test

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install snowflake-connector-python pytest

      - name: Run Snowflake tests
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
          SNOWFLAKE_DATABASE: IDR_TEST
        run: |
          python -m pytest tests/legacy/snowflake/test_integration.py -v
```

### BigQuery Test Workflow

```yaml
# .github/workflows/test-bigquery.yml
name: Test BigQuery

on:
  workflow_dispatch:
  schedule:
    - cron: '0 0 * * 0'  # Weekly

jobs:
  test-bigquery:
    runs-on: ubuntu-latest
    environment: gcp-test

    permissions:
      contents: read
      id-token: write

    steps:
      - uses: actions/checkout@v4

      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ secrets.WIF_PROVIDER }}
          service_account: ${{ secrets.GCP_SERVICE_ACCOUNT }}

      - name: Set up Cloud SDK
        uses: google-github-actions/setup-gcloud@v2

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install google-cloud-bigquery pytest

      - name: Run BigQuery tests
        env:
          GCP_PROJECT: ${{ secrets.GCP_PROJECT }}
        run: |
          python -m pytest tests/legacy/bigquery/test_integration.py -v
```

---

## Documentation Deployment

```yaml
# .github/workflows/docs.yml
name: Deploy Docs

on:
  push:
    branches: [main]
    paths:
      - 'docs/**'
      - 'mkdocs.yml'

jobs:
  deploy:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install mkdocs-material mkdocs-mermaid2-plugin

      - name: Deploy to GitHub Pages
        run: |
          mkdocs gh-deploy --force
```

---

## Release Workflow

```yaml
# .github/workflows/release.yml
name: Release

on:
  push:
    tags:
      - 'v*'

jobs:
  release:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Create Release
        uses: softprops/action-gh-release@v1
        with:
          generate_release_notes: true
          files: |
            sql/*/00_ddl_all.sql
```

---

## DDL Validation

Validate DDL syntax before merging:

```yaml
# .github/workflows/validate-ddl.yml
name: Validate DDL

on:
  pull_request:
    paths:
      - 'sql/**/*.sql'

jobs:
  validate:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install DuckDB
        run: pip install duckdb

      - name: Validate DuckDB DDL
        run: |
          python -c "
          import duckdb
          conn = duckdb.connect(':memory:')
          with open('sql/ddl/duckdb.sql') as f:
              conn.execute(f.read())
          print('DuckDB DDL valid')
          "
```

---

## Branch Protection

### Recommended Settings

```yaml
# Branch protection rules for 'main'

required_status_checks:
  strict: true
  checks:
    - test-duckdb
    - lint

required_pull_request_reviews:
  required_approving_review_count: 1
  dismiss_stale_reviews: true
  require_code_owner_reviews: true

restrictions:
  # Restrict who can push to main
  users: []
  teams: [maintainers]
```

---

## Pre-commit Hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/psf/black
    rev: 23.12.1
    hooks:
      - id: black
        language_version: python3.11
        files: \.(py)$

  - repo: https://github.com/pycqa/flake8
    rev: 7.0.0
    hooks:
      - id: flake8
        args: [--max-line-length=120]

  - repo: local
    hooks:
      - id: validate-ddl
        name: Validate DuckDB DDL
        entry: python -c "import duckdb; duckdb.connect(':memory:').execute(open('sql/ddl/duckdb.sql').read())"
        language: python
        files: sql/ddl/.*\.sql$
        additional_dependencies: [duckdb]
```

Install:
```bash
pip install pre-commit
pre-commit install
```

---

## Secrets Management

### Required Secrets

| Secret | Platform | Description |
|--------|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Snowflake | Account identifier |
| `SNOWFLAKE_USER` | Snowflake | Username |
| `SNOWFLAKE_PASSWORD` | Snowflake | Password |
| `GCP_PROJECT` | BigQuery | GCP project ID |
| `WIF_PROVIDER` | BigQuery | Workload Identity Federation provider |
| `GCP_SERVICE_ACCOUNT` | BigQuery | Service account email |
| `DATABRICKS_HOST` | Databricks | Workspace URL |
| `DATABRICKS_TOKEN` | Databricks | Personal access token |

### GitHub Environments

Use separate environments for secrets:

- `snowflake-test`
- `gcp-test`
- `databricks-test`

---

## Next Steps

- [Security](security.md)
- [Scheduling](scheduling.md)
- [Troubleshooting](../guides/troubleshooting.md)
