# Production Deployment: BigQuery

This guide details the exact steps to deploy SQL Identity Resolution (IDR) to a production Google BigQuery environment.

---

## Prerequisites

- **GCP Project**: A Google Cloud Project with BigQuery API enabled.
- **Service Account**: A service account with `BigQuery Admin` or `BigQuery Data Editor` + `BigQuery Job User` roles.
- **Python Environment**: For running `idr`.

---

## Step 1: Schema Setup

### 1.2 Initialize Schema
Run the IDR CLI to initialize the necessary datasets and tables.

```bash
idr config apply --platform bigquery --project your-project-id --file production.yaml
```

---

## Step 2: Configuration

Create a `production.yaml` file defining your rules and sources.

**Example `production.yaml`:**
```yaml
rules:
  - id: email_exact
    type: EXACT
    match_keys: [EMAIL]
    priority: 1
    canonicalize: LOWERCASE

sources:
  - id: ga4_events
    table: your-project-id.analytics_123456.events_*
    entity_key: user_pseudo_id
    trust_rank: 2
    identifiers:
      - type: COOKIE
        expr: user_pseudo_id
```

---

## Step 3: Metadata Loading



To update metadata later without full redeploy, use `idr config apply`.

```bash
./idr config apply --platform bigquery --project your-project-id --file production.yaml
```

### Fuzzy Matching Configuration

> **Important**: BigQuery does not have a native Jaro-Winkler function. Use **Normalized Levenshtein** for fuzzy matching, which runs natively without JavaScript UDF overhead.

Configure `idr_meta.fuzzy_rule` with a BigQuery-native `score_expr`:

```sql
INSERT INTO `your-project-id`.idr_meta.fuzzy_rule
(rule_id, rule_name, blocking_key_expr, score_expr, threshold, priority, is_active)
VALUES (
  'FR_NAME_BIGQUERY',
  'Name Fuzzy Match',
  'CONCAT(SOUNDEX(last_name), ''_'', LEFT(first_name, 1))',
  '1 - SAFE_DIVIDE(CAST(EDIT_DISTANCE(<a>, <b>) AS FLOAT64), CAST(GREATEST(LENGTH(<a>), LENGTH(<b>), 1) AS FLOAT64))',
  0.8,  -- Lower threshold than Jaro-Winkler (0.9) since Levenshtein is stricter
  100,
  TRUE
);
```

| Score | Meaning |
|-------|---------|
| 1.0 | Identical strings |
| 0.8+ | Very similar (e.g., "John" vs "Jon") |
| 0.5 | Moderate similarity |
| 0.0 | Completely different |

---

## Step 4: Execution & Scheduling

The IDR process is a Python application that orchestrates BigQuery jobs. It runs on the client (e.g., Cloud Run, Airflow Worker) and sends SQL commands to BigQuery.

### Option A: Cloud Run Jobs (Recommended)

1.  Containerize the application (Dockerfile provided in repo).
2.  Deploy to Cloud Run Jobs.
3.  Set environment variables: `IDR_PLATFORM=bigquery`, `IDR_PROJECT=...`.

**Command:**
```bash
idr run --platform=bigquery --project=your-project-id --mode=FULL
```

### Option B: Cloud Composer (Airflow)

Use `BashOperator` to execute the CLI.

```python
run_idr = BashOperator(
    task_id='run_idr',
    bash_command='idr run --platform=bigquery --project={{ var.value.gcp_project }} --mode=FULL',
    dag=dag
)
```

---

## Step 5: Monitoring

Monitor the pipeline using the `idr_out` tables.

**Check Run History:**
```sql
SELECT run_id, status, duration_seconds, entities_processed
FROM `your-project-id.idr_out.run_history`
ORDER BY started_at DESC
LIMIT 10;
```
