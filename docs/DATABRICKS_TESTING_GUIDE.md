# Databricks Free Tier Testing & Walkthrough Guide

This comprehensive, step-by-step walkthrough is designed for testing **SQL Identity Resolution (`v0.6.0`)** on **Databricks Free Tier** (Community Edition or 14-Day Free Cloud Trial).

!!! tip "Quick Start with Databricks Free Tier"
    This entire walkthrough works out-of-the-box using either **Databricks Community Edition** (hive_metastore) or a **Databricks Free Cloud Trial** on AWS/Azure/GCP with Unity Catalog.

---

## 🗺️ The Testing Journey

<div class="grid cards" markdown>

-   **1. Ingest Test Data**
    ---
    Choose between real synthetic parquet files from `Ecommerce_synthetic` or zero-install SQL generation.
    
    [:octicons-arrow-right-24: Ingestion Options](#part-2-ingest-test-data)

-   **2. Interactive Web UI**
    ---
    Walk through the 6-Step Setup Wizard (`/setup`) and visually explore the Golden Profile graph in `/explorer`.
    
    [:octicons-arrow-right-24: UI Walkthrough](#part-3-testing-via-the-interactive-web-ui)

-   **3. Autonomous AI MCP Tools**
    ---
    Run `discover_and_suggest_rules`, `simulate_rule_impact`, and `resolve_customer_360` from Python or AI assistants.
    
    [:octicons-arrow-right-24: Agentic Tools](#part-4-testing-the-new-agentic-mcp-tools)

-   **4. Delta Lake Verification**
    ---
    Inspect final Golden Customer Profiles, cluster memberships, and cardinality audit logs in Databricks SQL.
    
    [:octicons-arrow-right-24: SQL Queries](#part-5-inspect-results-directly-in-databricks-sql)

</div>

---

## 📋 Part 1: Prerequisites & Databricks Connection Setup

To connect SQL Identity Resolution to Databricks, retrieve your 3 core connection parameters from your Databricks workspace:

| Parameter | Type | Where to Find in Databricks UI | Example Value |
| :--- | :--- | :--- | :--- |
| **`DATABRICKS_HOST`** | URL | Browser address bar (remove trailing slash) | `https://dbc-xxxx.cloud.databricks.com` |
| **`DATABRICKS_TOKEN`** | Secret | <kbd>User Profile</kbd> → <kbd>Settings</kbd> → <kbd>Developer</kbd> → <kbd>Access tokens</kbd> | `dapi1234567890abcdef...` |
| **`DATABRICKS_HTTP_PATH`** | Endpoint | **SQL Warehouse**: <kbd>SQL Warehouses</kbd> → <kbd>Connection details</kbd> → <kbd>HTTP path</kbd><br/>**Compute Cluster**: <kbd>Compute</kbd> → <kbd>Configuration</kbd> → <kbd>Advanced</kbd> → <kbd>JDBC/ODBC</kbd> | `/sql/1.0/warehouses/xxxx` |
| **`DATABRICKS_CATALOG`** | Name | **Community Edition**: `hive_metastore`<br/>**Unity Catalog**: your catalog name (e.g. `main`) | `hive_metastore` |

---

## 📦 Part 2: Ingest Test Data

You have two convenient options for loading sample retail data into Databricks:

=== "Option A: Using Synthetic Parquet (`Ecommerce_synthetic`)"

    If you have the sibling repository [`Ecommerce_synthetic`](https://github.com/anilkulkarni87/ecommerce_synthetic) cloned locally:

    #### 1. Generate the Parquet Dataset
    ```bash
    cd /Users/harihiom/Anil/git/Ecommerce_synthetic
    source .venv/bin/activate
    ecommerce-synth generate --tier small --output ./output/demo_small --seed 42
    ```
    This generates 19 interconnected tables in `./output/demo_small/`. The key tables for identity resolution are:
    
    * `customers.parquet` (CRM accounts: `customer_id`, `first_name`, `last_name`, `email`, `phone`, `created_at`)
    * `instore_accounts.parquet` (Physical store POS: `instore_account_id`, `customer_id`, `phone_number`, `created_at`)
    * `loyalty_members.parquet` (Rewards program: `loyalty_id`, `customer_id`, `tier`, `points_balance`)
    * `orders_online.parquet` (E-Commerce orders: `order_id`, `customer_id`, `order_date`)
    * `support_tickets.parquet` (Customer support: `ticket_id`, `customer_id`, `created_at`)

    #### 2. Upload to Databricks
    1. In your Databricks workspace, click <kbd>Catalog</kbd> or <kbd>Workspace</kbd> → <kbd>+ Add Data</kbd> → <kbd>Upload data to a table</kbd>.
    2. Choose target catalog & schema (e.g. `ecommerce_demo` or `default`).
    3. Drag & drop `customers.parquet`, `instore_accounts.parquet`, `loyalty_members.parquet`, and `orders_online.parquet`.
    4. Click **Create Table** for each file.

=== "Option B: Zero-Install Pure SQL Generation (Recommended for Quick Testing)"

    No local files needed! Run this script directly in a **Databricks SQL Worksheet** or Notebook:

    ```sql
    -- 1. Create target schema
    CREATE SCHEMA IF NOT EXISTS ecommerce_demo;
    USE ecommerce_demo;

    -- 2. CRM Customers
    CREATE OR REPLACE TABLE ecommerce_demo.customers (
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        email STRING,
        phone STRING,
        created_at TIMESTAMP
    );

    INSERT INTO ecommerce_demo.customers VALUES
        ('crm_1', 'Alice', 'Smith', 'alice.smith@example.com', '555-0101', TIMESTAMP '2026-01-01 09:00:00'),
        ('crm_2', 'Bob', 'Jones', 'bob.jones@example.com', '555-0202', TIMESTAMP '2026-01-02 10:00:00'),
        ('crm_3', 'Support', 'Generic', 'shared_support@example.com', '555-9999', TIMESTAMP '2026-01-03 11:00:00');

    -- 3. In-Store Retail POS Accounts
    CREATE OR REPLACE TABLE ecommerce_demo.instore_accounts (
        instore_account_id STRING,
        customer_name STRING,
        phone_number STRING,
        created_at TIMESTAMP
    );

    INSERT INTO ecommerce_demo.instore_accounts VALUES
        -- Links to Alice via Phone
        ('pos_101', 'Alice M. Smith', '555-0101', TIMESTAMP '2026-02-01 14:30:00'),
        -- Links to Bob via Phone
        ('pos_102', 'Robert Jones', '555-0202', TIMESTAMP '2026-02-02 16:45:00'),
        -- High-cardinality group (shared support phone to test cardinality guardrails)
        ('pos_103', 'Guest POS A', '555-9999', TIMESTAMP '2026-02-03 10:00:00'),
        ('pos_104', 'Guest POS B', '555-9999', TIMESTAMP '2026-02-03 11:00:00'),
        ('pos_105', 'Guest POS C', '555-9999', TIMESTAMP '2026-02-03 12:00:00');

    -- 4. Mobile Loyalty Members
    CREATE OR REPLACE TABLE ecommerce_demo.loyalty_members (
        loyalty_id STRING,
        account_email STRING,
        tier STRING,
        enrolled_at TIMESTAMP
    );

    INSERT INTO ecommerce_demo.loyalty_members VALUES
        -- Links to Alice via Email (creates full 3-source connected component: CRM <-> POS <-> LOYALTY)
        ('loy_777', 'alice.smith@example.com', 'GOLD', TIMESTAMP '2026-03-01 18:00:00'),
        -- Singleton individual
        ('loy_888', 'charlie.brown@example.com', 'BRONZE', TIMESTAMP '2026-03-02 12:00:00');
    ```

---

## 🖥️ Part 3: Testing via the Interactive Web UI

The Web UI provides a 6-step Setup Wizard, an Explorer for customer graph visualization, and an Operational Dashboard.

### 1. Launch the Services

=== "Terminal 1: Start Backend API"
    ```bash
    cd /Users/harihiom/Anil/git/sql-identity-oss
    idr serve --port 8000
    ```

    * Swagger UI documentation: `http://localhost:8000/docs`
    * Prometheus metrics: `http://localhost:8000/metrics`

=== "Terminal 2: Start React Frontend"
    ```bash
    cd /Users/harihiom/Anil/git/sql-identity-oss/idr_ui
    npm run dev
    ```

    * Access the web dashboard: `http://localhost:5173`

---

### 2. The 6-Step Setup Wizard (`/setup`)

Navigate to **`http://localhost:5173/setup`** in your browser:

1. **Step 1: Connect (`/setup/connect`)**
   * Platform: Select **Databricks**.
   * Fill in `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, and `Catalog` (`hive_metastore` or `main`).
   * Click **Test Connection** → Displays a green **Connected** status badge.
2. **Step 2: Discover (`/setup/discover`)**
   * Select your schema: `ecommerce_demo`.
   * Click on `customers`, `instore_accounts`, and `loyalty_members` to preview column names and types.
3. **Step 3: Map Identifiers & Attributes (`/setup/map`)**
   * **`customers`**:
     * Entity Key: `customer_id`
     * Identifiers: `email` → `EMAIL`, `phone` → `PHONE`
     * Attributes: `first_name`, `last_name`
   * **`instore_accounts`**:
     * Entity Key: `instore_account_id`
     * Identifiers: `phone_number` → `PHONE`
     * Attributes: `customer_name`
   * **`loyalty_members`**:
     * Entity Key: `loyalty_id`
     * Identifiers: `account_email` → `EMAIL`
     * Attributes: `tier`
4. **Step 4: Rules (`/setup/rules`)**
   * Add Rule 1: Match Key `email`, Type `EXACT`, Priority `1`, `max_group_size = 50`.
   * Add Rule 2: Match Key `phone`, Type `EXACT`, Priority `2`, `max_group_size = 25`.
   * *(Optional)*: Add Fuzzy Rule on `first_name` using Jaro-Winkler similarity.
5. **Step 5: Survivorship (`/setup/survivorship`)**
   * `first_name`: `SOURCE_PRIORITY` (`customers`, `instore_accounts`)
   * `email`: `RECENCY`
   * `tier`: `RECENCY`
6. **Step 6: Review & Run (`/setup/review`)**
   * Click **Dry Run Preview** → Displays expected cluster diff scorecard:
     * `New Entities`: 7
     * `Moved Clusters`: 0
     * `Unchanged`: 0
   * Click **Run Pipeline** → Watch live stage-by-stage execution (`extraction` → `graph` → `cluster` → `metrics` → `output`).

---

### 3. Explorer & Customer 360 Visualization (`/explorer`)

1. Open **Explorer** from the navigation sidebar.
2. Search for: `alice.smith@example.com` or `555-0101`.
3. Inspect the **Cluster Details**:
   * **Cluster Size**: `3` (records unified from CRM, POS, and Loyalty).
   * **Confidence Score**: `0.98`
   * **Golden Customer Profile**:
     * Name: `Alice`
     * Primary Email: `alice.smith@example.com`
     * Primary Phone: `555-0101`
     * Tier: `GOLD`
4. Interact with the **Graph Canvas**:
   * Inspect visual nodes representing `crm:crm_1`, `pos:pos_101`, and `loyalty:loy_777`.
   * Click edges to verify the matching rules applied (`exact_email` and `exact_phone`).

---

## 🤖 Part 4: Testing the New Agentic MCP Tools

The new agentic tools in `v0.6.0` allow AI assistants (Cursor, Claude, or Python agents) to autonomously profile schemas, simulate rule impact, and retrieve Customer 360 context.

### 1. Configure Connection Environment

```bash
export IDR_PLATFORM=databricks
export DATABRICKS_HOST="https://your-databricks-instance.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi_your_token_here"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your_warehouse_id"
export DATABRICKS_CATALOG="hive_metastore" # or "main"
export IDR_PII_ACCESS="full"               # unmasked PII output
```

### 2. Interactive Python Script

Execute this script to exercise the 3 agentic tools directly against your Databricks tables:

```python
import json
from idr_mcp import server as mcp

# Connect to Databricks using the environment variables
mcp.connect_from_env()

print("=" * 80)
print("1. Testing discover_and_suggest_rules()")
print("=" * 80)

# Autonomous table profiling & PII discovery
discovery = mcp.discover_and_suggest_rules()

print(f"Tables Profiled:      {discovery.get('tables_profiled')}")
print(f"Detected Identifiers: {discovery.get('detected_identifiers')}")
print(f"Detected Attributes:  {discovery.get('detected_attributes')}")
print("\nSuggested Declarative Rules:")
print(json.dumps(discovery["suggested_config"]["rules"], indent=2))


print("\n" + "=" * 80)
print("2. Testing simulate_rule_impact() on Candidate Rules")
print("=" * 80)

# Simulate candidate rule with max_group_size = 2
candidate_rule = {
    "id": "exact_phone",
    "type": "EXACT",
    "match_keys": ["phone"],
    "max_group_size": 2
}
simulation = mcp.simulate_rule_impact(candidate_rule)

print(f"Rule ID:                 {simulation.get('rule_id')}")
print(f"Eligible Entities:       {simulation.get('eligible_entities')}")
print(f"Simulated Anchor Edges:  {simulation.get('simulated_anchor_edges')}")
print(f"Supernode Groups Alert:  {simulation.get('supernodes_flagged')}")
print(f"Risk Level:              {simulation.get('risk_level')}")
print(f"Risk Reasons:            {simulation.get('risk_reasons')}")


print("\n" + "=" * 80)
print("3. Testing resolve_customer_360() Single-Call Context Grounding")
print("=" * 80)

# Resolve Alice using her phone number
c360 = mcp.resolve_customer_360("555-0101")

print(f"Found:            {c360.get('found')}")
print(f"Resolved ID:      {c360.get('resolved_id')}")
print(f"Cluster Size:     {c360.get('cluster_size')} linked accounts")
print(f"Confidence Score: {c360.get('confidence_score')}")

print("\nGolden Master Profile:")
print(json.dumps(c360.get("golden_profile"), indent=2))

print("\nLinked Source Accounts:")
for acc in c360.get("linked_accounts", []):
    print(f"  • System: {acc['source_system']:<20} Entity: {acc['entity_key']}")

print("\nAll Known Cross-Channel Identifiers:")
for idn in c360.get("known_identifiers", []):
    print(f"  • [{idn['type'].upper()}]: {idn['value']}")
```

---

## 🔍 Part 5: Inspect Results Directly in Databricks SQL

In your **Databricks SQL Worksheet**, run these validation queries against the Delta Lake serving tables:

```sql
-- 1. View Golden Master Customer Profiles
SELECT * FROM idr_out.golden_customer_profiles;

-- 2. View Cluster Memberships across CRM, POS, and Loyalty
SELECT 
    m.entity_key,
    m.resolved_id,
    c.cluster_size,
    c.confidence_score
FROM idr_out.identity_resolved_membership_current m
JOIN idr_out.identity_clusters_current c ON m.resolved_id = c.resolved_id
ORDER BY c.cluster_size DESC, m.resolved_id;

-- 3. View Skipped High-Cardinality Groups (Auditing protection against shared_support@example.com)
SELECT * FROM idr_out.skipped_identifier_groups;

-- 4. View Stage Metrics Telemetry
SELECT 
    stage_name, 
    records_in, 
    records_out, 
    duration_ms 
FROM idr_out.stage_metrics
ORDER BY started_at DESC;
```

---

## 💡 Troubleshooting & Gotchas

??? tip "Compute Cold Starts & 503 Service Unavailable"
    Databricks Free Tier compute (SQL Warehouse or Community Cluster) auto-terminates after 20 minutes of inactivity. If a query or API call fails with `Connection refused` or `503 Service Unavailable`, open your Databricks workspace and verify that the compute is in **Running** state.

??? note "Delta Lake Column Defaults"
    Databricks Delta Lake requires explicit feature flag activation for column defaults on certain classic runtime clusters. IDR automatically suppresses invalid default clauses in DDL to ensure table creation succeeds seamlessly across all cluster types.

??? info "Unity Catalog vs Hive Metastore Paths"
    * If using **Unity Catalog**, tables are addressed as `<catalog>.<schema>.<table>` (e.g. `main.idr_out.golden_customer_profiles`).
    * If using **Community Edition**, set `DATABRICKS_CATALOG="hive_metastore"`.
