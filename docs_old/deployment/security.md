# Security

Security best practices for SQL Identity Resolution deployments.

---

## Principle of Least Privilege

Grant only the minimum permissions needed for the IDR runner.

### DuckDB

DuckDB runs locally with file permissions:

```bash
# Restrict file access
chmod 600 idr.duckdb

# Run with non-root user
useradd -r -s /bin/false idr-runner
chown idr-runner:idr-runner idr.duckdb
# Run checks as the idr-runner user
idr run --platform=duckdb --db=idr.duckdb
```

### Snowflake

```sql
-- Create dedicated role
CREATE ROLE IDR_EXECUTOR;

-- Grant only required permissions
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE IDR_EXECUTOR;
GRANT USAGE ON DATABASE analytics TO ROLE IDR_EXECUTOR;

-- Read-only on source schemas
GRANT USAGE ON SCHEMA crm TO ROLE IDR_EXECUTOR;
GRANT SELECT ON ALL TABLES IN SCHEMA crm TO ROLE IDR_EXECUTOR;
GRANT SELECT ON FUTURE TABLES IN SCHEMA crm TO ROLE IDR_EXECUTOR;

-- Full access on IDR schemas
GRANT ALL ON SCHEMA idr_meta TO ROLE IDR_EXECUTOR;
GRANT ALL ON SCHEMA idr_work TO ROLE IDR_EXECUTOR;
GRANT ALL ON SCHEMA idr_out TO ROLE IDR_EXECUTOR;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA idr_meta TO ROLE IDR_EXECUTOR;
GRANT ALL ON ALL TABLES IN SCHEMA idr_work TO ROLE IDR_EXECUTOR;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA idr_out TO ROLE IDR_EXECUTOR;

-- Create service user
CREATE USER idr_service_account
  PASSWORD = 'CHANGE_ME'
  DEFAULT_ROLE = IDR_EXECUTOR
  DEFAULT_WAREHOUSE = compute_wh;
GRANT ROLE IDR_EXECUTOR TO USER idr_service_account;
```

### BigQuery

```bash
# Create service account
gcloud iam service-accounts create idr-runner \
  --display-name="IDR Runner Service Account"

# Grant minimal permissions
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:idr-runner@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Grant dataset-level access
bq query --use_legacy_sql=false "
  GRANT \`roles/bigquery.dataViewer\` ON SCHEMA crm
  TO 'serviceAccount:idr-runner@PROJECT_ID.iam.gserviceaccount.com'
"

bq query --use_legacy_sql=false "
  GRANT \`roles/bigquery.dataEditor\` ON SCHEMA idr_meta
  TO 'serviceAccount:idr-runner@PROJECT_ID.iam.gserviceaccount.com'
"

bq query --use_legacy_sql=false "
  GRANT \`roles/bigquery.dataEditor\` ON SCHEMA idr_out
  TO 'serviceAccount:idr-runner@PROJECT_ID.iam.gserviceaccount.com'
"
```

### Databricks

```python
# Unity Catalog permissions
spark.sql("""
  GRANT USAGE ON CATALOG main TO `idr-runner-group`
""")

spark.sql("""
  GRANT SELECT ON SCHEMA main.crm TO `idr-runner-group`
""")

spark.sql("""
  GRANT ALL PRIVILEGES ON SCHEMA main.idr_meta TO `idr-runner-group`
""")

spark.sql("""
  GRANT ALL PRIVILEGES ON SCHEMA main.idr_out TO `idr-runner-group`
""")
```

---

## Secrets Management

### Never Hardcode Credentials

```python
# ❌ Bad
password = "my_secret_password"

# ✅ Good
import os
password = os.environ.get('DB_PASSWORD')
```

### Environment Variables

```bash
# .env (never commit this file!)
SNOWFLAKE_ACCOUNT=xxx
SNOWFLAKE_USER=idr_service_account
SNOWFLAKE_PASSWORD=xxx
```

```bash
# Add to .gitignore
echo ".env" >> .gitignore
echo "*.pem" >> .gitignore
echo "*.json" >> .gitignore  # For service account keys
```

### Cloud Secret Managers

=== "AWS Secrets Manager"
    ```python
    import boto3
    import json

    client = boto3.client('secretsmanager')
    secret = client.get_secret_value(SecretId='idr/snowflake')
    creds = json.loads(secret['SecretString'])
    ```

=== "GCP Secret Manager"
    ```python
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/PROJECT/secrets/idr-credentials/versions/latest"
    response = client.access_secret_version(request={"name": name})
    password = response.payload.data.decode("UTF-8")
    ```

=== "Azure Key Vault"
    ```python
    from azure.keyvault.secrets import SecretClient
    from azure.identity import DefaultAzureCredential

    vault_url = "https://idr-vault.vault.azure.net/"
    client = SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())
    password = client.get_secret("snowflake-password").value
    ```

=== "HashiCorp Vault"
    ```python
    import hvac

    client = hvac.Client(url='https://vault.company.com')
    secret = client.secrets.kv.v2.read_secret_version(path='idr/snowflake')
    password = secret['data']['data']['password']
    ```

---

## Data Protection

### PII Handling

Identity resolution inherently deals with PII. Protect it:

1. **Encrypt at rest**: Enable encryption on all databases/storage
2. **Encrypt in transit**: Use TLS/SSL connections
3. **Mask in logs**: Never log raw identifier values
4. **Limit retention**: Delete old dry run data

```sql
-- Clean up old dry run data (contains PII)
DELETE FROM idr_out.dry_run_results
WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '7 days';
```

### Column-Level Encryption

If needed, encrypt sensitive columns:

```sql
-- Snowflake example
ALTER TABLE idr_out.golden_profile_current MODIFY COLUMN email_primary
  SET MASKING POLICY email_mask;

CREATE MASKING POLICY email_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ADMIN', 'IDR_EXECUTOR') THEN val
    ELSE '***MASKED***'
  END;
```

---

## Network Security

### Private Endpoints

=== "Snowflake"
    - Use AWS PrivateLink or Azure Private Link
    - Restrict network policies

=== "BigQuery"
    - Use VPC Service Controls
    - Configure Private Google Access

=== "Databricks"
    - Deploy in private subnet
    - Use Private Link

### Firewall Rules

```sql
-- Snowflake Network Policy
CREATE NETWORK POLICY idr_policy
  ALLOWED_IP_LIST = ('10.0.0.0/8', '192.168.0.0/16')
  BLOCKED_IP_LIST = ('0.0.0.0/0');

ALTER USER idr_service_account SET NETWORK_POLICY = idr_policy;
```

---

## Audit Logging

### Enable Platform Audit Logs

=== "Snowflake"
    ```sql
    -- Query access history
    SELECT *
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
    WHERE USER_NAME = 'IDR_SERVICE_ACCOUNT'
    ORDER BY QUERY_START_TIME DESC;
    ```

=== "BigQuery"
    ```sql
    -- Cloud Audit Logs (BigQuery Admin Activity)
    SELECT *
    FROM `project.region.cloudaudit_googleapis_com_activity`
    WHERE protopayload_auditlog.methodName LIKE '%bigquery%'
    ```

=== "Databricks"
    - Enable audit logging in Admin Console
    - Logs go to cloud storage

### Application-Level Logging

The IDR runner logs all activity to `idr_out.run_history`:

```sql
SELECT
    run_id,
    run_mode,
    started_at,
    ended_at,
    status,
    entities_processed
FROM idr_out.run_history
ORDER BY started_at DESC;
```

---

## Compliance

### GDPR

- **Right to be forgotten**: Delete entity from source tables, run FULL mode to remove from clusters
- **Data portability**: Query membership table for entity's cluster info
- **Purpose limitation**: Only use IDR for stated purposes

### CCPA

- Similar to GDPR requirements
- Maintain audit trail of processing

### SOC 2

- Enable all audit logging
- Implement access controls
- Document procedures

---

## Security Checklist

### Pre-Deployment

- [ ] Service accounts created with minimal permissions
- [ ] Secrets stored in secret manager (not code)
- [ ] Network policies configured
- [ ] Encryption enabled (at rest and in transit)
- [ ] Audit logging enabled

### Operations

- [ ] Regular credential rotation (90 days)
- [ ] Review access logs monthly
- [ ] Clean up old dry run data
- [ ] Monitor for unusual activity

### Incident Response

- [ ] Document data breach procedures
- [ ] Test rollback procedures
- [ ] Maintain contact list for security incidents

---

## Next Steps

- [Scheduling](scheduling.md)
- [CI/CD](ci-cd.md)
- [Troubleshooting](../guides/troubleshooting.md)
