# Distribution Paths

Use this guide to choose a reproducible install path for your environment.

## Decision Tree

1. Need only CLI/library on a developer machine?
   - Use `pip` extras (`duckdb`, `api`, `enterprise`, `mcp`, or `all`).
2. Need web UI + API locally with source checkout?
   - Use `docker compose up --build`.
3. Need reproducible deployment from prebuilt artifacts?
   - Use `docker compose -f docker-compose.prod.yml --env-file .env up -d`.
4. Need SSO + observability stack for enterprise evaluation?
   - Use `docker compose -f docker-compose.enterprise.yml up -d --build`.

## Compatibility Matrix

| Path | Artifact | Runtime | Best For |
|---|---|---|---|
| Python package | Wheel / sdist (`dist/*`) | Python 3.9-3.12 | CLI automation, notebook workflows |
| Dev Compose | Source build (`idr_api/Dockerfile`, `idr_ui/Dockerfile`) | Docker Engine + Compose | Local development |
| Prod Compose | Pinned GHCR images (`idr-api`, `idr-ui`) | Docker Engine + Compose | Reproducible app deployment |
| Enterprise Compose | Source build + Keycloak + Grafana + Prometheus | Docker Engine + Compose | Security/ops validation |

## Runtime Compatibility

| Component | Supported |
|---|---|
| Python runtime | 3.9, 3.10, 3.11, 3.12 |
| Operating systems (pip path) | macOS, Linux, Windows (WSL recommended for Docker workflows) |
| Container runtime | Docker Engine with Docker Compose v2 |

## Reproducible Commands

### Python Artifact (wheel/sdist)
```bash
python -m pip install --upgrade pip
python -m pip install -r requirements/release.lock
python -m build
twine check dist/*
python -m venv .venv-smoke
. .venv-smoke/bin/activate
pip install dist/*.whl
idr version
```

### Production Compose (Pinned Images)
```bash
cp .env.example .env
# Set IDR_IMAGE_TAG to a released version, then:
docker compose -f docker-compose.prod.yml --env-file .env up -d
```

### Local Dev Compose (Build From Source)
```bash
docker compose up --build
```

## CI Lockfiles

Pinned lockfiles used in automation:
- `requirements/ci.lock`
- `requirements/release.lock`
- `requirements/docs.lock`

UI dependency lockfile:
- `idr_ui/package-lock.json` (validated in CI via `npm ci`)

Enterprise session-store extension:
- `IDR_SESSION_STORE_CLASS=idr_enterprise.session_store.EnterpriseInMemoryConnectionSessionStore`
- Redis-backed option: `IDR_SESSION_STORE_CLASS=idr_enterprise.session_store.RedisConnectionSessionStore`
