# Enterprise Local Stack

This environment simulates a production-grade deployment with SSO, RBAC, and Observability.

## Services

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| **IDR UI** | [http://localhost:3000](http://localhost:3000) | - | Main Application |
| **Keycloak** | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` | Identity Provider (SSO) |
| **Keycloak Test User** | Login via UI | `test` / `test` | Pre-provisioned app user |
| **Grafana** | [http://localhost:3001](http://localhost:3001) | `admin` / `admin` | Metrics Dashboard |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | - | Metrics Scraper |
| **IDR API** | [http://localhost:8000/docs](http://localhost:8000/docs) | - | OpenAPI / Swagger |
| **Redis** | `redis://localhost:6379/0` | - | Session lease backend |

## Getting Started

1.  **Start the Stack**:
    ```bash
    docker-compose -f docker-compose.enterprise.yml up -d --build
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
    *   `IDR_CONNECTION_IDLE_TTL_SECONDS`: Per-user connection idle timeout (default: 3600)
    *   `IDR_SESSION_STORE_CLASS`: Optional enterprise session backend class path
    *   `IDR_REDIS_URL`: Redis connection URL for `RedisConnectionSessionStore`
    *   `IDR_REDIS_NAMESPACE`: Redis key prefix for session leases
    *   `IDR_PIP_EXTRAS`: API image dependency extras (default includes `enterprise`)

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
