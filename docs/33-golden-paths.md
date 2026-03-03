# Golden Paths

This page defines the supported adoption paths for OSS and enterprise users, with explicit prerequisites, commands, and expected outcomes.

## Prerequisite Matrix

| Path | Minimum Prerequisites |
|---|---|
| Local OSS (DuckDB) | Python 3.11+, `pip` |
| Warehouse CLI | Python 3.11+, platform credentials |
| UI validation | Node.js 20+, npm |
| MCP validation | Python 3.11+, IDR dataset (`DuckDB` or warehouse) |
| Enterprise Compose | Docker + Docker Compose |
| Enterprise Helm | Kubernetes + Helm 3 |

## Path A: Local OSS (DuckDB, under 10 minutes)

Commands:

```bash
python -m pip install "sql-identity-resolution[duckdb]"
idr quickstart --rows 10000 --output quickstart_demo.duckdb
idr doctor --json
```

Expected outcome:
- quickstart finishes successfully in <= 10 minutes
- `quickstart_demo.duckdb` is created
- doctor output has no required failures

Verification gate:
- CI job `quickstart-golden-path`

## Path B: Warehouse-Only CLI (No UI)

Commands:

```bash
python -m pip install "sql-identity-resolution[all]"
idr init --platform duckdb --db warehouse.duckdb
idr config validate --file config.yaml
idr config apply --platform duckdb --db warehouse.duckdb --file config.yaml
idr run --platform duckdb --db warehouse.duckdb --mode FULL
```

Expected outcome:
- config validates and applies successfully
- `idr run` returns success status

Notes:
- Replace `duckdb` with `bigquery`, `snowflake`, or `databricks` plus required environment credentials.

## Path C: UI Golden Path (Auth + Setup + Runs Shell)

Commands (from repo root):

```bash
cd idr_ui
npm ci --ignore-scripts --no-audit
npm run test
npm run test:e2e
```

Expected outcome:
- unit tests pass (`vitest`)
- Playwright smoke test passes:
  - authenticated app shell renders
  - setup page renders
  - runs page renders

Verification gate:
- CI jobs `ui-unit` and `ui-e2e`

Reference:
- `09-ui.md`

## Path D: MCP Golden Path (Contract + Secrets + Errors)

Commands (from repo root):

```bash
python -m pip install -r requirements/ci.lock
python -m pytest tests/test_mcp_contract.py tests/test_mcp_secrets.py tests/test_mcp_errors.py -q
```

Optional runtime smoke:

```bash
export IDR_PLATFORM=duckdb
export IDR_DATABASE=./quickstart_demo.duckdb
idr mcp
```

Expected outcome:
- MCP contract test suite passes
- error envelopes are deterministic (`mcp_error_v1`)
- `*_FILE` secret-loading behavior is validated by tests

Verification gate:
- CI job `mcp-contract`

Reference:
- `11-mcp.md`

## Path E: Enterprise Self-Hosted (Docker Compose)

Single-package startup with SSO and metrics:

```bash
bash tools/deploy/enterprise_up.sh
```

Expected outcome:
- secrets prepared without plaintext in compose file
- `docker-compose.enterprise.yml` stack becomes healthy
- OIDC-protected API and observability endpoints are reachable

## Path F: Enterprise Self-Hosted (Kubernetes/Helm)

Commands:

```bash
helm upgrade --install idr-enterprise deployment/helm/idr-enterprise \
  --namespace idr --create-namespace \
  -f deployment/helm/idr-enterprise/values.example.yaml
```

Expected outcome:
- chart renders and deploys successfully
- secret references and OIDC values are injected from chart values

Accelerated cloud path:
- Use `docs/40-cloud-deployment-quickstart.md` with:
  - `tools/deploy/bootstrap_k8s_secrets.sh`
  - `tools/deploy/bootstrap_oidc_values.sh`
  - `tools/deploy/idr_deploy.sh`

## Troubleshooting Links

- UI setup and usage: `09-ui.md`
- API endpoint reference: `10-api.md`
- MCP server and error envelope reference: `11-mcp.md`
- UI auth + MCP operator deterministic runbook: `38-ui-auth-mcp-operator-runbook.md`
- Cloud deployment acceleration runbook: `40-cloud-deployment-quickstart.md`
- Common operator failures and recovery: `14-troubleshooting.md`
