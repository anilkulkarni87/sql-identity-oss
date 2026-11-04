# Troubleshooting & Common Issues

Solutions and diagnostic steps for common identity resolution and warehouse issues.

---

## 1. Warehouse & Platform Specific Issues

### Databricks: `WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED`
- **Cause**: Delta Lake tables by default require the table feature `allowColumnDefaults` to be enabled to accept column `DEFAULT` clauses.
- **Resolution**: IDR's schema manager automatically suppresses `DEFAULT` clauses when creating Delta tables. If running custom DDL scripts, ensure column defaults are omitted from Delta `CREATE TABLE` statements or execute:
  ```sql
  ALTER TABLE tableName SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
  ```

### Databricks: Schema & Catalog Resolution
- **Cause**: Unity Catalog workspaces require queries to know the target catalog.
- **Resolution**: Set `export DATABRICKS_CATALOG="your_catalog"` or configure default catalog in your warehouse settings.

### BigQuery: `bigquery.datasets.delete permission denied`
- **Cause**: When calling `idr init --reset` with BigQuery, dropping datasets requires `roles/bigquery.dataOwner` or `bigquery.datasets.delete`.
- **Resolution**: Grant the service account `roles/bigquery.dataOwner` on the target project, or run `idr init` without `--reset` to alter/create tables in place without dropping the datasets.

### Snowflake: Query Timeout on Multi-Million Row Datasets
- **Cause**: An undersized warehouse (X-Small) running heavy initial full label propagation.
- **Resolution**: Temporarily scale warehouse to Medium or Large for initial historical load:
  ```sql
  ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'LARGE';
  ```

### DuckDB: `Failed to create checkpoint ... No space left on device`
- **Cause**: Generating very large synthetic datasets (e.g. 100k+ rows) in temporary file directories on full local partitions.
- **Resolution**: Use `duckdb.connect(":memory:")` for test suites, or point `--db` to an external drive with adequate disk space.

---

## 2. Graph & Data Quality Issues

### Giant Clusters & Graph Explosion
- **Symptom**: A single cluster contains thousands or millions of entities (e.g. `cluster_size > 10000`).
- **Cause**: Common sentinel values like `null`, `n/a`, `unknown@domain.com`, or shared store tablet emails.
- **Resolution**:
  1. Add the problematic value to `idr_meta.identifier_exclusion` (or via the UI Setup Wizard Step 4):
     ```sql
     INSERT INTO idr_meta.identifier_exclusion (identifier_type, identifier_value_pattern, match_type, reason)
     VALUES ('email', 'shared_kiosk@store.com', 'EXACT', 'Shared store device');
     ```
  2. Set `max_group_size` on the matching rule (e.g. `100` or `5000`). Oversized groups will be automatically skipped and logged to `idr_out.skipped_identifier_groups`.

### Label Propagation Not Converging
- **Symptom**: Pipeline run completes with warning: `Label propagation did not fully converge after 30 iterations`.
- **Cause**: Highly complex cyclical graph chains.
- **Resolution**:
  - Increase `max_iters`:
    ```bash
    idr run --platform [platform] --mode FULL --max-iters 50
    ```
  - Inspect `idr_out.skipped_identifier_groups` to check for unintentional hubs connecting disparate networks.

---

## 3. Operational & Recovery Diagnostics

### Concurrent Run Lock / Stale Run Detected
- **Symptom**: `PreflightStage` fails with `Concurrent run detected: run_...`.
- **Cause**: A previous worker process crashed or was terminated while in `RUNNING` status.
- **Resolution**: IDR automatically marks stale runs older than 4 hours as `INTERRUPTED`. To manually clear an active lock:
  ```sql
  UPDATE idr_out.run_history
  SET status = 'INTERRUPTED', error_message = 'Manually cleared'
  WHERE status = 'RUNNING';
  ```

### Inspecting Execution Stage Telemetry
To diagnose where execution time was spent during a run:
```sql
SELECT
    stage_name,
    stage_order,
    duration_seconds,
    rows_out
FROM idr_out.stage_metrics
WHERE run_id = 'your_run_id'
ORDER BY stage_order;
```
