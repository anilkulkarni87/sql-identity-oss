# Enterprise Local Stack

This environment simulates a production-grade deployment with SSO, RBAC, and Observability.

## Services

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| **IDR UI** | [http://localhost:3000](http://localhost:3000) | - | Main Application |
| **Keycloak** | [http://localhost:8080](http://localhost:8080) | `admin` / `$IDR_KEYCLOAK_ADMIN_PASSWORD` | Identity Provider (SSO) |
| **Keycloak Test User** | Login via UI | `test` / `test` | Pre-provisioned app user |
| **Grafana** | [http://localhost:3001](http://localhost:3001) | `admin` / `$IDR_GRAFANA_ADMIN_PASSWORD` | Metrics Dashboard |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | - | Metrics Scraper |
| **IDR API** | [http://localhost:8000/docs](http://localhost:8000/docs) | - | OpenAPI / Swagger |
| **Redis** | `redis://localhost:6379/0` | - | Session lease backend |

## Getting Started

1.  **Start the Stack**:
    Preferred single command:
    ```bash
    bash tools/deploy/enterprise_up.sh
    ```

    Or manual startup:
    ```bash
    export IDR_KEYCLOAK_ADMIN_PASSWORD="$(python - <<'PY'
import secrets
print(secrets.token_urlsafe(24))
PY
)"
    export IDR_GRAFANA_ADMIN_PASSWORD="$(python - <<'PY'
import secrets
print(secrets.token_urlsafe(24))
PY
)"
    ```

    Optional webhook callback auth using file-backed secret:
    ```bash
    export IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN_FILE=/run/secrets/idr_run_job_webhook_bearer_token
    ```

    ```bash
    docker-compose -f docker-compose.enterprise.yml up -d --build
    ```

    **HA Baseline (active-active API + LB)**:
    ```bash
    docker-compose -f docker-compose.enterprise.ha.yml up -d --build
    ```

2.  **Keycloak Bootstrap (Automated)**:
    *   Realm `idr-realm`, client `idr-web`, and user `test` are imported at startup from `deployment/keycloak/idr-realm.json`.
    *   UI login credentials: `test` / `test`.

3.  **SSO Configuration**:
    The stack is pre-configured in `docker-compose.enterprise.yml`.

    *   **Frontend (Build Args)**:
        *   `VITE_AUTH_AUTHORITY`: URL of OIDC Provider (e.g. Keycloak Realm)
        *   `VITE_AUTH_CLIENT_ID`: Client ID for the UI
    *   **Backend (Env Vars)**:
        *   `IDR_AUTH_ISSUER`: OIDC Issuer URL (Validation)
    *   `IDR_AUTH_AUDIENCE`: Expected Audience (default: account)
    *   `IDR_AUTH_JWKS_URL`: Internal URL to fetch keys (useful for Docker networking)
    *   `IDR_AUTH_JWKS_TTL_SECONDS`: JWKS cache TTL (default: 300)
    *   `IDR_AUTH_JWKS_HTTP_TIMEOUT_SECONDS`: JWKS fetch timeout (default: 5)
    *   `IDR_AUTHZ_ROLE_PERMISSIONS_JSON`: Optional JSON override for role-to-permission mapping
    *   `IDR_SERVICE_AUTH_DB_PATH`: SQLite path for service-account token store
    *   `IDR_AUDIT_DB_PATH`: SQLite path for immutable audit event store
    *   `IDR_CONNECTION_IDLE_TTL_SECONDS`: Per-user connection idle timeout (default: 3600)
    *   `IDR_SESSION_STORE_CLASS`: Optional enterprise session backend class path
    *   `IDR_REDIS_URL`: Redis connection URL for `RedisConnectionSessionStore`
    *   `IDR_REDIS_NAMESPACE`: Redis key prefix for session leases
    *   `IDR_PIP_EXTRAS`: API image dependency extras (default includes `enterprise`)
    *   `IDR_RUN_JOB_DB_PATH`: SQLite path for async run job durability
    *   `IDR_RUN_JOB_MAX_ATTEMPTS`: Max retries for async run jobs
    *   `IDR_RUN_JOB_RETRY_BACKOFF_SECONDS`: Retry backoff base for async run jobs
    *   `IDR_RUN_JOB_WEBHOOK_URL`: Optional callback URL for run job lifecycle events
    *   `IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN`: Optional bearer token for webhook auth
    *   `IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN_FILE`: Preferred file-mounted bearer token (`*_FILE` pattern)
    *   `IDR_RUN_JOB_WEBHOOK_TIMEOUT_SECONDS`: Webhook delivery timeout

4.  **Dev Mode (No Auth)**:
    To disable SSO for local testing, comment out the `VITE_AUTH_*` args in `docker-compose.enterprise.yml` for the `ui` service and `IDR_AUTH_*` env vars for the `api` service, and set:
    *   `VITE_ALLOW_INSECURE_DEV_AUTH=true`
    *   `IDR_ALLOW_INSECURE_DEV_AUTH=true`

5.  **Verify Metrics**:
    *   Run a job in IDR UI.
    *   Confirm API metrics endpoint responds: http://localhost:8000/metrics
    *   Check Grafana Dashboards -> "IDR Dashboard".
    *   You should see "Entities Processed" spike.

6.  **Stop**:
    ```bash
    docker-compose -f docker-compose.enterprise.yml down
    ```

## HA Validation (Clean Host)

Run full end-to-end validation for the HA profile:

```bash
EXPECT_HEALTHY_SERVICES=api-a,api-b,api,redis,keycloak \
bash tools/ci/verify_enterprise_stack.sh docker-compose.enterprise.ha.yml
```

This checks:
- service health for both API nodes and load balancer
- OIDC bootstrap and token flow
- auth-protected API behavior
- Prometheus target health

The verification script auto-generates runtime values for `IDR_KEYCLOAK_ADMIN_PASSWORD` and `IDR_GRAFANA_ADMIN_PASSWORD` if they are unset.

## Backup/Restore Drill

```bash
python tools/ci/run_backup_restore_drill.py \
  --file idr_database=/data/idr.duckdb \
  --file run_jobs=/data/control/idr_run_jobs.sqlite3 \
  --file service_auth=/data/control/idr_service_auth.sqlite3 \
  --file audit=/data/control/idr_audit.sqlite3 \
  --backup-dir ./ops_backups \
  --evidence-file ./ops_backups/restore_drill_report.json \
  --simulate-incident \
  --max-rto-seconds 300 \
  --max-rpo-bytes 0
```

## Rollback Dry-Run Validation

```bash
bash tools/ci/validate_prod_rollback.sh 0.5.1 docker-compose.prod.yml .env
```

## Helm (Kubernetes) Package

```bash
helm upgrade --install idr-enterprise deployment/helm/idr-enterprise \
  --namespace idr --create-namespace \
  -f deployment/helm/idr-enterprise/values.example.yaml
```

Cloud accelerated path:

```bash
bash tools/deploy/bootstrap_k8s_secrets.sh \
  --namespace idr \
  --secret-name idr-enterprise-secrets

bash tools/deploy/idr_deploy.sh \
  --provider aws \
  --mode apply \
  --use-existing-secret idr-enterprise-secrets
```

Terraform full provisioning modules are available under `deployment/terraform/`.
