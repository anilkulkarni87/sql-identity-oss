# User Guide: UI Mode

This guide explains how to use the IDR Web Interface for interactive configuration, monitoring, and exploration. This mode is best for Data Stewards, Analysts, and initial setup.

## 1. Installation

The UI is bundled with the API server. Install with the `[api]` extra.

```bash
pip install sql-identity-resolution[api]
```

## 2. Starting the Server

Start the API and UI server using the CLI:

```bash
idr serve --port 8000
```

Access the UI at `http://localhost:8000`.

## 3. Initialization & Configuration (Setup Wizard)

1.  **Connect**: Open the UI. If not configured, you will be redirected to the **Setup Wizard**.
2.  **Platform Selection**: Choose your platform (Snowflake/BigQuery/Databricks/DuckDB) and enter credentials.
3.  **Source Mapping**: Select source tables from your warehouse. Map columns to IDR types (Identifier, Attribute).
    *   *Tip*: Ensure your timestamp columns are mapped correctly for recency calculations.
4.  **Rules Config**: Define matching rules (e.g., "Email Exact Match") and assign priorities.
5.  **Survivorship**: Choose strategies for Golden Profile attributes (Priority Source, Recency, Frequency).
6.  **Review & Run**: Save your configuration. The Wizard initializes the `idr_meta` tables and can trigger the first run immediately.

## 4. Execution (Dashboard)

Once set up, use the **Dashboard** to trigger runs manually.

*   **Full Run**: Click `Run Pipeline` -> `Full Refresh`. Warning: This rebuilds the entire graph.
*   **Incremental Run**: Click `Run Pipeline` -> `Incremental`. Processes only changed records since the last watermark.

## 5. Verification & Exploration

The UI provides powerful tools to verify results:

*   **Metric Cards**: Track Total Clusters, Entity Counts, and Average Confidence.
*   **Cluster Explorer**: Search for a specific email or phone number to see its resolved cluster.
    *   *Visuals*: View the "Spider Graph" of connected entities.
    *   *Details*: Drill down into matching rules to see exactly *why* two records linked.
*   **Run History**: View logs and duration for past executions.

## 6. Understanding Logging

*   **Server Logs**: The terminal where `idr serve` is running outputs standard logs (INFO/WARNING/ERROR).
*   **Run History**: The Dashboard's "Last Run" card shows the duration of the most recent execution. Detailed run metadata is stored in the `idr_out.run_history` table.

## 7. Production Deployment (API Trigger)

You can keep the IDR Server running for monitoring and trigger pipelines remotely via the REST API.

**Example: Airflow HttpOperator**

```python
from airflow.providers.http.operators.http import SimpleHttpOperator

trigger_run = SimpleHttpOperator(
    task_id='trigger_idr',
    http_conn_id='idr_api_connection',
    endpoint='/api/setup/run',
    method='POST',
    data='{"mode": "INCR", "strict": false}',
    headers={"Content-Type": "application/json"},
)
```
