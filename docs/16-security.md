# Security

Security best practices for deploying Identity Resolution.

## Principle of Least Privilege

### Snowflake
Create a dedicated `IDR_EXECUTOR` role.

```sql
CREATE ROLE IDR_EXECUTOR;
-- Grant usage on warehouse and database
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE IDR_EXECUTOR;
GRANT USAGE ON DATABASE analytics TO ROLE IDR_EXECUTOR;

-- Read-only on source data
GRANT SELECT ON ALL TABLES IN SCHEMA crm TO ROLE IDR_EXECUTOR;

-- Full control of IDR schemas
GRANT ALL ON SCHEMA idr_meta TO ROLE IDR_EXECUTOR;
GRANT ALL ON SCHEMA idr_work TO ROLE IDR_EXECUTOR;
GRANT ALL ON SCHEMA idr_out TO ROLE IDR_EXECUTOR;
```

### BigQuery
Use a dedicated Service Account with granular IAM roles.
*   `roles/bigquery.jobUser` (Project level)
*   `roles/bigquery.dataViewer` (Source datasets)
*   `roles/bigquery.dataEditor` (IDR datasets)

## Data Protection

### PII Handling
*   **Encryption**: Ensure encryption at rest and in transit (standard on cloud DWHs).
*   **Retention**: Regularly clean up `idr_out.dry_run_results` and `idr_work` tables.
*   **Masking**: Apply Dynamic Data Masking policies on `golden_profile_current` output if accessed by broad teams.

### Credential Management
*   **Never** hardcode passwords in scripts or config files.
*   Use environment variables (`SNOWFLAKE_PASSWORD`).
*   Prefer file-backed secret injection for runtime tokens/passwords using the `*_FILE` pattern (for example, `DATABRICKS_TOKEN_FILE`, `SNOWFLAKE_PASSWORD_FILE`, `IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN_FILE`).
*   Rotate secrets without restarting API processes by updating the mounted secret file in place.
*   In production, inject secrets via AWS Secrets Manager / GCP Secret Manager / Azure Key Vault.

## Network Security
*   **Snowflake**: Use Network Policies to whitelist IP ranges.
*   **BigQuery**: Use VPC Service Controls.
*   **Databricks**: Deploy in a private subnet with PrivateLink.

## API Token Validation

The API validates bearer tokens against OIDC JWKS with:
- `kid`-based signing key selection
- Audience and issuer enforcement
- JWKS cache with configurable TTL (`IDR_AUTH_JWKS_TTL_SECONDS`)
- Configurable JWKS fetch timeout (`IDR_AUTH_JWKS_HTTP_TIMEOUT_SECONDS`)

## API Authorization (RBAC/ABAC)

Protected API routes are permission-gated and return `403` if permissions are missing.

- Role-based permissions are derived from token claims:
  - `roles`
  - `realm_access.roles` (Keycloak-compatible)
  - `resource_access.<client>.roles`
- Scope/permission claims are also accepted:
  - `scope` (space-delimited)
  - `scp`
  - `permissions`

Default roles:
- `admin` / `idr_admin`: full access (`*`)
- `viewer`: read-only API access
- `analyst`: read + async run submission/cancel
- `operator`: analyst + connection/config management + sync run execute

You can override the role map with:
- `IDR_AUTHZ_ROLE_PERMISSIONS_JSON` (JSON object mapping role -> permission list)

### Service Accounts and API Tokens

The API supports service-account tokens for machine-to-machine access.

- Token creation and revocation endpoints are under `/api/auth/*`.
- Tokens are scoped by explicit permissions (for example: `schema.read`, `jobs.read`).
- Token secrets are returned only at creation time; only hashes are stored.
- Token metadata is persisted in SQLite (`IDR_SERVICE_AUTH_DB_PATH`).

## Immutable Audit Logging

Security-relevant control-plane actions are written to an append-only audit log:
- Connection management (`connection.connect`, `connection.disconnect`)
- Config lifecycle (`config.save`)
- Run control (`run.execute.sync`, `run.submit.async`, `run.cancel.async`, `run.execute.async`)
- Service-account administration (`auth.service_account.create`, `auth.service_token.create`, `auth.service_token.revoke`)

Audit entries include:
- Actor (`actor_sub`, `actor_type`)
- Action (`action`)
- Resource (`resource_type`, `resource_id`)
- Result (`outcome`)
- Optional context payload (`details`)

Storage is SQLite-backed and immutable by schema triggers (no UPDATE/DELETE allowed):
- `IDR_AUDIT_DB_PATH` (default: `/tmp/idr_audit.sqlite3`)
