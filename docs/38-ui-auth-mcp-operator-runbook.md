# UI Auth and MCP Operator Runbook

This runbook provides deterministic support steps for:
- UI authentication failures (`401`)
- UI authorization failures (`403`)
- MCP startup/connection failures (`MCP_NOT_CONNECTED`)

Use this with:
- `docs/09-ui.md`
- `docs/11-mcp.md`
- `docs/33-golden-paths.md`

## Scope and Preconditions

- API base URL: `http://localhost:8000` (adjust if deployed elsewhere)
- UI base URL: `http://localhost:3000` (or deployment URL)
- For authenticated API checks, export a bearer token:

```bash
export IDR_TOKEN="<bearer-token>"
```

## 2-Minute Triage

Run these in order before deep debugging:

```bash
curl -sS http://localhost:8000/api/health
curl -sS -H "Authorization: Bearer $IDR_TOKEN" http://localhost:8000/api/auth/whoami
python -m pytest tests/test_mcp_contract.py tests/test_mcp_secrets.py tests/test_mcp_errors.py -q
```

Expected:
- `/api/health` returns JSON with `"status": "healthy"`
- `/api/auth/whoami` returns `sub`, `roles`, and `permissions` for valid token
- MCP test suite passes without contract or secret-loading regressions

---

## A. UI Auth Incident Runbook

### A1. `401 Unauthorized` or session-expired loop

Typical symptoms:
- UI banner: "Your session has expired. Redirecting to sign in..."
- Repeated redirects to IdP
- `/api/auth/whoami` returns `401`

#### Step 1: Verify API rejects/accepts token deterministically

```bash
curl -i -sS http://localhost:8000/api/auth/whoami | head -n 1
curl -i -sS -H "Authorization: Bearer $IDR_TOKEN" http://localhost:8000/api/auth/whoami | head -n 1
```

Expected:
- no token: `HTTP/1.1 401`
- valid token: `HTTP/1.1 200`

If valid token still returns `401`, continue to Step 2.

#### Step 2: Inspect token claims (`iss`, `aud`, `exp`)

```bash
python - <<'PY'
import base64, json, os, time
t = os.environ.get("IDR_TOKEN", "")
if not t or "." not in t:
    raise SystemExit("IDR_TOKEN missing or malformed")
payload = t.split(".")[1]
payload += "=" * (-len(payload) % 4)
claims = json.loads(base64.urlsafe_b64decode(payload.encode()).decode())
print(json.dumps({
    "iss": claims.get("iss"),
    "aud": claims.get("aud"),
    "exp": claims.get("exp"),
    "exp_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(claims.get("exp", 0))) if claims.get("exp") else None
}, indent=2))
PY
```

Expected:
- `iss` matches `IDR_AUTH_ISSUER`
- `aud` includes `IDR_AUTH_AUDIENCE` (default `account`)
- `exp` is in the future

#### Step 3: Validate API auth environment

For local shell:

```bash
env | grep -E '^IDR_AUTH_|^IDR_ALLOW_INSECURE_DEV_AUTH'
```

For enterprise compose:

```bash
docker compose -f docker-compose.enterprise.yml exec api sh -lc 'env | grep -E "^IDR_AUTH_|^IDR_ALLOW_INSECURE_DEV_AUTH"'
```

Expected:
- `IDR_AUTH_ISSUER` set (OIDC mode)
- `IDR_AUTH_AUDIENCE` aligned with token audience
- `IDR_ALLOW_INSECURE_DEV_AUTH=false` in enterprise

#### Step 4: Validate JWKS reachability from API host

```bash
JWKS_URL="${IDR_AUTH_JWKS_URL:-${IDR_AUTH_ISSUER%/}/protocol/openid-connect/certs}"
curl -i -sS "$JWKS_URL" | head -n 5
```

Expected:
- HTTP `200`
- JSON response containing key set (`keys`)

If this fails, fix network/DNS/TLS reachability to IdP.

#### Step 5: Validate UI auth config

Local UI dev:

```bash
env | grep -E '^VITE_AUTH_|^VITE_ALLOW_INSECURE_DEV_AUTH|^VITE_API_BASE_URL'
```

Compose UI container:

```bash
docker compose -f docker-compose.enterprise.yml exec ui sh -lc 'env | grep -E "^VITE_AUTH_|^VITE_ALLOW_INSECURE_DEV_AUTH|^VITE_API_BASE_URL"'
```

Expected:
- `VITE_AUTH_AUTHORITY` and `VITE_AUTH_CLIENT_ID` are set
- `VITE_ALLOW_INSECURE_DEV_AUTH=false` for enterprise
- `VITE_API_BASE_URL` points to reachable API route (`/api` by default)

#### Step 6: Confirm recovery

```bash
curl -sS -H "Authorization: Bearer $IDR_TOKEN" http://localhost:8000/api/auth/whoami
```

Then reload UI and verify protected pages render without redirect loop.

### A2. `403 Forbidden` access denied

Typical symptoms:
- UI "Forbidden (403)" state
- buttons disabled with permission hints
- API responses include `Insufficient permissions. Required: ...`

#### Step 1: Read effective permissions from API

```bash
curl -sS -H "Authorization: Bearer $IDR_TOKEN" http://localhost:8000/api/auth/whoami
```

Inspect `permissions` and `roles` in response.

#### Step 2: Reproduce forbidden endpoint directly

```bash
curl -i -sS -H "Authorization: Bearer $IDR_TOKEN" "http://localhost:8000/api/runs?limit=1" | head -n 20
```

Expected forbidden response:
- HTTP `403`
- `detail` includes required permission names

#### Step 3: Compare against permission map

| UI section | Required permissions |
|---|---|
| Dashboard | `metrics.read`, `connection.read` |
| Setup Wizard (view) | `connection.read`, `config.read` |
| Setup connect/update | `connection.manage` |
| Setup save config | `config.manage` |
| Setup run now | `run.execute` |
| Setup submit async run | `run.submit` |
| Job status/history | `jobs.read` |
| Cancel run job | `run.cancel` |
| Explorer | `explorer.read` |
| Runs | `runs.read` |
| Data Model | `schema.read` |
| Settings | `connection.read` |
| Auth admin APIs | `auth.read` / `auth.manage` |
| Audit APIs | `audit.read` |

#### Step 4: Remediate role mapping

If roles do not map to required permissions, set `IDR_AUTHZ_ROLE_PERMISSIONS_JSON` and restart API.

Example:

```bash
export IDR_AUTHZ_ROLE_PERMISSIONS_JSON='{"viewer":["connection.read","config.read","metrics.read","runs.read","explorer.read","schema.read","jobs.read","audit.read","auth.read"]}'
```

Recheck `whoami` and retry the endpoint/UI flow.

---

## B. MCP Operator Incident Runbook

### B1. Startup cannot connect (`MCP_NOT_CONNECTED`)

Typical symptoms:
- stderr: `IDR_PLATFORM not set. Waiting for manual connection...`
- stderr: `Failed to connect to <platform>. Check connection variables and credentials.`
- tools return `{"error":{"code":"MCP_NOT_CONNECTED",...}}`

#### Step 1: Validate required environment variables

```bash
env | grep -E '^IDR_PLATFORM=|^IDR_DATABASE=|^IDR_PROJECT=|^GOOGLE_APPLICATION_CREDENTIALS=|^SNOWFLAKE_|^DATABRICKS_'
```

Expected:
- one platform selected in `IDR_PLATFORM`
- platform-specific connection variables are present

#### Step 2: Platform-specific preflight checks

DuckDB:

```bash
test -n "$IDR_DATABASE" && test -f "$IDR_DATABASE" && echo "duckdb path OK"
```

BigQuery:

```bash
test -n "$IDR_PROJECT" && test -f "$GOOGLE_APPLICATION_CREDENTIALS" && echo "bigquery creds OK"
```

Snowflake:

```bash
test -n "$SNOWFLAKE_ACCOUNT" && test -n "$SNOWFLAKE_USER" && test -n "$SNOWFLAKE_WAREHOUSE" && test -n "$SNOWFLAKE_DATABASE" && echo "snowflake base env OK"
```

Databricks:

```bash
test -n "$DATABRICKS_HOST" && test -n "$DATABRICKS_HTTP_PATH" && test -n "$DATABRICKS_CATALOG" && echo "databricks base env OK"
```

#### Step 3: Validate secret-file loading (`*_FILE`) when used

Snowflake:

```bash
test -z "$SNOWFLAKE_PASSWORD_FILE" || { test -r "$SNOWFLAKE_PASSWORD_FILE" && echo "snowflake password file readable"; }
```

Databricks:

```bash
test -z "$DATABRICKS_TOKEN_FILE" || { test -r "$DATABRICKS_TOKEN_FILE" && echo "databricks token file readable"; }
```

Notes:
- `SNOWFLAKE_PASSWORD_FILE` takes precedence over `SNOWFLAKE_PASSWORD`
- `DATABRICKS_TOKEN_FILE` takes precedence over `DATABRICKS_TOKEN`

#### Step 4: Validate MCP contract/security suite

```bash
python -m pytest tests/test_mcp_contract.py tests/test_mcp_secrets.py tests/test_mcp_errors.py -q
```

Expected:
- all tests pass
- secret-file precedence and deterministic error envelopes are intact

#### Step 5: Start MCP and inspect startup logs

```bash
idr mcp 2> /tmp/idr_mcp_startup.log
```

Open `/tmp/idr_mcp_startup.log` and verify:
- `Connecting to <platform>...`
- `✓ Connected to <platform>`

If not connected, fix missing/invalid env and restart.

### B2. MCP returns `MCP_QUERY_FAILED` or `MCP_NOT_FOUND`

Run:

```bash
python -m pytest tests/test_mcp_errors.py tests/test_mcp_contract.py -q
```

Interpretation:
- `MCP_QUERY_FAILED`: operational query failure (connection/query path), retryable
- `MCP_NOT_FOUND`: valid request, requested entity/config does not exist
- raw DB stack traces must not appear in tool response payload

---

## Escalation Bundle (Attach to Incident)

Collect:
- `/api/health` output
- `/api/auth/whoami` output (redacted token)
- failing API call response headers/body (`-i`)
- relevant auth env snapshot (`IDR_AUTH_*`, `VITE_AUTH_*`, `IDR_AUTHZ_ROLE_PERMISSIONS_JSON`)
- `/tmp/idr_mcp_startup.log`
- MCP contract test output

This artifact set is sufficient for deterministic triage of `401`, `403`, and MCP connection failures.
