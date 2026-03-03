# Installation

Use pip extras to install the components you need.

Quick path selection:
- `pip` install: best for CLI and library usage.
- `docker compose up --build`: best for local UI + API development.
- `docker compose -f docker-compose.prod.yml --env-file .env up -d`: best for reproducible deployments from pinned images.
- `docker compose -f docker-compose.enterprise.yml up -d --build`: best for SSO/observability validation.
- `docker compose -f docker-compose.enterprise.ha.yml up -d --build`: best for enterprise HA baseline validation.
- `bash tools/deploy/enterprise_up.sh`: best for single-command enterprise bring-up.
- `helm upgrade --install ... deployment/helm/idr-enterprise`: best for Kubernetes self-hosted enterprise deployments.

Base + DuckDB (local/dev):
```bash
pip install "sql-identity-resolution[duckdb]"
```

API server:
```bash
pip install "sql-identity-resolution[api]"
```

Standalone UI + API (Docker):
```bash
docker compose up --build
```

Pinned production images (reproducible):
```bash
cp .env.example .env
docker compose -f docker-compose.prod.yml --env-file .env up -d
```

MCP server:
```bash
pip install "sql-identity-resolution[mcp]"
```

All platforms:
```bash
pip install "sql-identity-resolution[all]"
```

Verify:
```bash
idr version
idr doctor
```

See `27-distribution.md` for compatibility matrix and deployment decision tree.

CI and release automation use pinned lockfiles under `requirements/*.lock`.
