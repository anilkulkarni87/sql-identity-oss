# Versioning and Upgrade Policy

## Versioning Policy

- Project follows Semantic Versioning (`MAJOR.MINOR.PATCH`).
- `PATCH`: bug fixes and security fixes, no intentional API break.
- `MINOR`: backward-compatible feature additions, new optional config fields.
- `MAJOR`: breaking API/config/schema changes.

Release artifacts are version-pinned:
- Python dist (`dist/*.whl`, `dist/*.tar.gz`)
- GHCR images (`idr-api`, `idr-ui`) with tag and digest
- SBOM + vulnerability scan evidence
- provenance attestations and signatures

## Compatibility Contract

- Python: `3.9` to `3.12`
- API: additive changes in minor releases, breaking changes only in major releases
- Config schema: deprecated fields stay for at least one minor release before removal

## Upgrade Playbook (Compose)

1. Back up state:
   - follow `docs/31-backup-restore-rollback.md`
2. Pin target image tag:
   - set `IDR_IMAGE_TAG=<target>`
3. Deploy:
   - `docker compose -f docker-compose.prod.yml --env-file .env up -d --force-recreate`
4. Validate:
   - `/api/health` returns 200
   - `/metrics` returns Prometheus payload
   - protected API endpoints enforce auth as expected
5. If failure:
   - run rollback procedure in `docs/31-backup-restore-rollback.md`

## Upgrade Playbook (Helm)

1. Back up operational state (DB and control-plane stores).
2. Diff values:
   - `helm diff upgrade ...` (recommended)
3. Upgrade:
   - `helm upgrade idr-enterprise deployment/helm/idr-enterprise -n idr -f values.yaml`
4. Validate:
   - API health and metrics endpoint
   - OIDC login flow
   - Grafana datasource/dashboard connectivity
5. Roll back if needed:
   - `helm rollback idr-enterprise <revision>`

## Breaking Change Policy

- Breaking changes require:
  - migration notes in release docs
  - explicit "Breaking Changes" section
  - one minor release of deprecation warning where feasible

## Release Readiness Checklist

- smoke-tested install paths (`pip`, compose, enterprise compose)
- lockfile-based reproducible build
- SBOM generated and validated
- vulnerability scans pass at configured severity gate
