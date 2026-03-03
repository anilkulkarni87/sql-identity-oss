# Secrets Management and Rotation

This is the E05-T03 enterprise operability artifact for runtime secrets posture.

## Goals

- No plaintext secrets in checked-in runtime compose configuration.
- `*_FILE` secret loading pattern supported for API runtime credentials.
- Rotation procedure validated in CI without API process restart.

## Supported Runtime Secret Pattern

API runtime now resolves secrets as:
1. `<SECRET_NAME>_FILE` (preferred)
2. `<SECRET_NAME>` (fallback)

Implemented paths:
- `SNOWFLAKE_PASSWORD` / `SNOWFLAKE_PASSWORD_FILE`
- `DATABRICKS_TOKEN` / `DATABRICKS_TOKEN_FILE`
- `IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN` / `IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN_FILE`

## Compose Baseline Requirements

Enterprise compose profiles (`docker-compose.enterprise.yml`, `docker-compose.enterprise.ha.yml`) require:
- `IDR_KEYCLOAK_ADMIN_PASSWORD`
- `IDR_GRAFANA_ADMIN_PASSWORD`

Both are injected at runtime from environment (no hardcoded defaults in compose).

## Rotation Procedure (Webhook Bearer Token)

1. Mount or project the token as a file path visible to the API container.
2. Set `IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN_FILE` to that path.
3. Rotate by atomically replacing the file contents.
4. Submit any new async run event (or wait for next event delivery).
5. Confirm downstream webhook receives Authorization header with the new token.

The API reads webhook token per delivery, so restart is not required.

## CI Validation

`test.yml` includes `secrets-posture` job:

```bash
python tools/ci/validate_secrets_posture.py
```

Validation covers:
- compose files reject plaintext sensitive env values
- API services expose file-based secret env wiring
- webhook bearer token rotation behavior works at runtime
