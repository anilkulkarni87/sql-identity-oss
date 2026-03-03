# HA Deployment Baseline

This is the E05-T01 high-availability baseline for enterprise evaluation.

Artifacts:
- Compose profile: `docker-compose.enterprise.ha.yml`
- API load balancer config: `deployment/nginx/api-lb.conf`
- Verification script: `tools/ci/verify_enterprise_stack.sh`

## Topology

- `api-a`, `api-b`: active-active API nodes
- `api`: NGINX load balancer (public port `8000`) routing to both API nodes
- `ui`: static UI container
- `keycloak`: OIDC provider
- `redis`: shared session lease backend
- `prometheus`, `grafana`: observability

Shared control-plane state for API nodes is persisted under `/data/control`:
- `IDR_RUN_JOB_DB_PATH`
- `IDR_SERVICE_AUTH_DB_PATH`
- `IDR_AUDIT_DB_PATH`

## Clean Host Validation (Recommended)

Prerequisites:
- Docker Engine
- Docker Compose v2

Run end-to-end validation:

```bash
EXPECT_HEALTHY_SERVICES=api-a,api-b,api,redis,keycloak \
bash tools/ci/verify_enterprise_stack.sh docker-compose.enterprise.ha.yml
```

This validates:
- stack boots from scratch
- service-level health for HA API nodes + core dependencies
- OIDC bootstrap and token flow
- protected endpoint auth enforcement
- Prometheus target health

## Manual Bring-Up

Set required admin credentials first:

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

```bash
docker compose -f docker-compose.enterprise.ha.yml up -d --build
```

Check health:

```bash
curl -sS http://localhost:8000/api/health
curl -sS http://localhost:9090/-/healthy
curl -sS http://localhost:8080/realms/idr-realm/.well-known/openid-configuration
```

Stop:

```bash
docker compose -f docker-compose.enterprise.ha.yml down -v
```

## Notes

- This is a practical HA baseline for evaluation and operational hardening.
- For production-grade HA, run external managed backing services (OIDC, Redis, and durable SQL backends) and move to orchestrated deployment (for example Helm/Kubernetes) in E05 follow-ons.
