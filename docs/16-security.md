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
