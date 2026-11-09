# Google Cloud BigQuery Testing & Walkthrough Guide

This comprehensive, step-by-step walkthrough is designed for testing **SQL Identity Resolution (`v0.6.1`)** on **Google Cloud BigQuery** using either the **BigQuery Sandbox** (100% free, 10 GB storage & 1 TB query analysis per month, no credit card required) or a standard Google Cloud project.

!!! tip "Quick Start with BigQuery Sandbox"
    You can run BigQuery without providing a credit card via the [BigQuery Sandbox](https://cloud.google.com/bigquery/docs/sandbox). Simply sign in to the [Google Cloud Console](https://console.cloud.google.com/bigquery) with any Google account and create a new project.

---

## 🗺️ The Testing Journey

<div class="grid cards" markdown="1">

-   :material-database-arrow-up:{ .lg .middle } __1. Ingest Test Data__

    ---

    Choose between real synthetic parquet files from `Ecommerce_synthetic` or zero-install pure SQL generation in BigQuery Studio.
    
    [:octicons-arrow-right-24: Ingestion Options](#part-2-ingest-test-data)

-   :material-monitor-dashboard:{ .lg .middle } __2. Interactive Web UI__

    ---

    Connect via 1-Click Zero-Retype discovery, walk through the 6-Step Setup Wizard (`/setup`), and visually explore the Golden Profile graph in `/explorer`.
    
    [:octicons-arrow-right-24: UI Walkthrough](#part-3-testing-via-the-interactive-web-ui)

-   :material-robot:{ .lg .middle } __3. Autonomous AI MCP Tools__

    ---

    Run `discover_and_suggest_rules`, `simulate_rule_impact`, and `resolve_customer_360` from Cursor, Claude Desktop, or Antigravity via FastMCP.
    
    [:octicons-arrow-right-24: Agentic Tools](#part-4-testing-autonomous-ai-mcp-tools)

-   :material-shield-search:{ .lg .middle } __4. BigQuery Studio Verification__

    ---

    Inspect final Golden Customer Profiles, Spanning Tree confidence scores, and cluster memberships directly in BigQuery SQL.
    
    [:octicons-arrow-right-24: SQL Queries](#part-5-inspect-results-directly-in-bigquery-studio-sql)

</div>

---

## 📋 Part 1: Prerequisites & Connection Setup

### A. Core Connection Parameters
Retrieve your Google Cloud BigQuery project information:

| Parameter | Type | Where to Find in Google Cloud Console | Example Value |
| :--- | :--- | :--- | :--- |
| **`BIGQUERY_PROJECT_ID`** | Identifier | Top navigation project switcher in GCP Console | `my-data-project-123` |
| **`BIGQUERY_DATASET`** | Dataset | Target dataset under your project | `ecommerce_demo` |
| **`BIGQUERY_LOCATION`** | Region | Dataset processing location (defaults to `US`) | `US` or `europe-west1` |

---

### B. Authentication & Zero-Retype Auto-Discovery (New in v0.6.1)
SQL Identity Resolution natively detects Google Cloud Application Default Credentials (ADC):

1. **Option 1: Google Cloud CLI (Recommended for Local Dev)**
   Run this single command in your terminal:
   ```bash
   gcloud auth application-default login
   ```
   This writes credentials to `~/.config/gcloud/application_default_credentials.json`. The IDR Setup Wizard automatically detects your active GCP Project and presents a **"Discovered Local Credentials"** card with **1-Click Connect**.

2. **Option 2: Service Account Key (Production / CI)**
   Generate a Service Account Key JSON with `BigQuery Admin` or `BigQuery Data Editor` + `BigQuery Job User` roles:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/sa-key.json"
   export IDR_PROJECT="my-data-project-123"
   ```

3. **Option 3: dbt Profiles**
   If you have a `~/.dbt/profiles.yml` with a BigQuery target, IDR auto-detects the project, dataset, and keyfile path.

---

## 📦 Part 2: Ingest Test Data

You have two convenient options for loading sample retail data into BigQuery:

=== "Option A: Zero-Install Pure SQL Generation (Recommended for Quick Testing)"

    No local files or uploads required! Paste this SQL script directly into a **BigQuery Studio SQL Query tab** ([console.cloud.google.com/bigquery](https://console.cloud.google.com/bigquery)):

    ```sql
    -- 1. Create Target Dataset
    CREATE SCHEMA IF NOT EXISTS `ecommerce_demo`
    OPTIONS (location = 'US');

    -- 2. CRM Customers (Primary CRM record)
    CREATE OR REPLACE TABLE `ecommerce_demo.customers` (
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        email STRING,
        phone STRING,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );

    INSERT INTO `ecommerce_demo.customers` (customer_id, first_name, last_name, email, phone) VALUES
        ('CRM_001', 'Alice', 'Smith', 'alice.smith@example.com', '+1-555-0101'),
        ('CRM_002', 'Robert', 'Taylor', 'bob.taylor@work.com', '+1-555-0102'),
        ('CRM_003', 'Charlie', 'Davis', 'charlie.d@gmail.com', '+1-555-0103'),
        ('CRM_004', 'David', 'Miller', 'david.miller@example.com', '+1-555-0104'),
        ('CRM_005', 'Eva', 'Wilson', 'eva.wilson@corp.org', '+1-555-0105');

    -- 3. In-Store Point of Sale (POS Accounts - shares phone & alternate emails)
    CREATE OR REPLACE TABLE `ecommerce_demo.instore_accounts` (
        instore_account_id STRING,
        phone_number STRING,
        alt_email STRING,
        registered_store STRING,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );

    INSERT INTO `ecommerce_demo.instore_accounts` (instore_account_id, phone_number, alt_email, registered_store) VALUES
        ('POS_901', '+1-555-0101', 'asmith_home@gmail.com', 'Store_Chicago_01'),  -- Matches Alice via phone
        ('POS_902', '+1-555-0102', 'bob.personal@gmail.com', 'Store_Austin_02'),   -- Matches Bob via phone
        ('POS_903', '+1-555-9999', 'unknown.shopper@retail.com', 'Store_NYC_01');

    -- 4. Loyalty Rewards Program (Shares alternate email & loyalty ID)
    CREATE OR REPLACE TABLE `ecommerce_demo.loyalty_members` (
        loyalty_id STRING,
        contact_email STRING,
        tier STRING,
        points_balance INT64,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );

    INSERT INTO `ecommerce_demo.loyalty_members` (loyalty_id, contact_email, tier, points_balance) VALUES
        ('LOYALTY_A1', 'asmith_home@gmail.com', 'Platinum', 12450),  -- Bridges Alice's in-store account via email
        ('LOYALTY_B2', 'bob.taylor@work.com', 'Gold', 8200),         -- Bridges Bob's CRM account via email
        ('LOYALTY_C3', 'charlie.d@gmail.com', 'Silver', 3100);       -- Bridges Charlie's CRM account via email

    -- 5. E-Commerce Orders
    CREATE OR REPLACE TABLE `ecommerce_demo.orders_online` (
        order_id STRING,
        customer_email STRING,
        customer_phone STRING,
        order_total NUMERIC,
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );

    INSERT INTO `ecommerce_demo.orders_online` (order_id, customer_email, customer_phone, order_total) VALUES
        ('ORD_8801', 'alice.smith@example.com', NULL, 189.50),
        ('ORD_8802', NULL, '+1-555-0102', 450.00),
        ('ORD_8803', 'charlie.d@gmail.com', '+1-555-0103', 75.25);
    ```

=== "Option B: Upload Synthetic Parquet (`Ecommerce_synthetic`)"

    If you have the sibling repository [`Ecommerce_synthetic`](https://github.com/anilkulkarni87/ecommerce_synthetic) cloned locally:

    #### 1. Generate Parquet Dataset
    ```bash
    cd /path/to/Ecommerce_synthetic
    ecommerce-synth generate --tier small --output ./output/demo_small --seed 42
    ```

    #### 2. Load Parquet into BigQuery via `bq` CLI or Cloud Console
    ```bash
    # Create dataset
    bq --location=US mk -d ecommerce_demo

    # Load tables from local Parquet
    bq load --source_format=PARQUET ecommerce_demo.customers ./output/demo_small/customers.parquet
    bq load --source_format=PARQUET ecommerce_demo.instore_accounts ./output/demo_small/instore_accounts.parquet
    bq load --source_format=PARQUET ecommerce_demo.loyalty_members ./output/demo_small/loyalty_members.parquet
    bq load --source_format=PARQUET ecommerce_demo.orders_online ./output/demo_small/orders_online.parquet
    ```

---

## 🖥️ Part 3: Testing via the Interactive Web UI

### 1. Launch Backend API & React UI
In your terminal, launch the IDR API server and frontend:

```bash
# Terminal 1: Start IDR API
python -m idr_api.main

# Terminal 2: Start IDR React UI
cd idr_ui && npm run dev
```

Navigate your browser to: **`http://localhost:5173/setup`**

---

### 2. Step 1: Connect to BigQuery
- Select **BigQuery** as the Platform.
- If you ran `gcloud auth application-default login`, the **Discovered Local Credentials** card will show your Google Cloud project. Click **1-Click Connect**.
- Alternatively, enter your **GCP Project ID**: `my-data-project-123`
- Click **Test & Continue**.

---

### 3. Step 2: Select Sources & Entity Keys
IDR queries BigQuery `INFORMATION_SCHEMA` to discover tables. Select your 4 source tables and primary entity keys:

| Table Name | Entity Key Column | Description |
| :--- | :--- | :--- |
| `ecommerce_demo.customers` | `customer_id` | CRM Account Records |
| `ecommerce_demo.instore_accounts` | `instore_account_id` | In-Store POS Accounts |
| `ecommerce_demo.loyalty_members` | `loyalty_id` | Rewards Members |
| `ecommerce_demo.orders_online` | `order_id` | Web E-Commerce Transactions |

---

### 4. Step 3: Map Identifiers
Map columns that share identity linkages across systems:

| Table | Identifier Type | Source Column | Normalization |
| :--- | :--- | :--- | :--- |
| `customers` | `EMAIL` | `email` | Lowercase + Trim |
| `customers` | `PHONE` | `phone` | E.164 Clean |
| `instore_accounts` | `PHONE` | `phone_number` | E.164 Clean |
| `instore_accounts` | `EMAIL` | `alt_email` | Lowercase + Trim |
| `loyalty_members` | `EMAIL` | `contact_email` | Lowercase + Trim |
| `orders_online` | `EMAIL` | `customer_email` | Lowercase + Trim |
| `orders_online` | `PHONE` | `customer_phone` | E.164 Clean |

---

### 5. Step 4: Configure Deterministic Match Rules
Define match rules in priority order:

1. **Rule 1 (`EXACT_EMAIL`)**:
   - Priority: `1`
   - Identifier: `EMAIL`
   - Weight: `1.0`
2. **Rule 2 (`EXACT_PHONE`)**:
   - Priority: `2`
   - Identifier: `PHONE`
   - Weight: `0.9`

---

### 6. Step 5: Configure Golden Profile Survivorship
Define how customer attributes survive into the 360 view:

| Attribute | Survivorship Strategy | Watermark / Source |
| :--- | :--- | :--- |
| `first_name` | `RECENCY` | `created_at` DESC |
| `last_name` | `RECENCY` | `created_at` DESC |
| `email` | `SOURCE_PRIORITY` | `customers` $\rightarrow$ `loyalty_members` $\rightarrow$ `instore_accounts` |
| `phone` | `RECENCY` | `created_at` DESC |

---

### 7. Step 6: Execute Resolution
- Click **Run Identity Resolution**.
- Execution runs natively inside Google BigQuery as parallel SQL query jobs.
- Watch live stage progress: `Preflight` $\rightarrow$ `Extraction` $\rightarrow$ `Graph & Clustering` $\rightarrow$ `Golden Profiles` $\rightarrow$ `Complete`.

---

### 8. Inspect the Entity Graph in `/explorer`
Click **Explore Graph** or navigate to `http://localhost:5173/explorer`:
- Search for `Alice` or `alice.smith@example.com`.
- Inspect the **Resolved Customer 360 Cluster**:
  - Notice that `CRM_001` (CRM), `POS_901` (In-Store), `LOYALTY_A1` (Loyalty), and `ORD_8801` (Online Order) are unified into a single Golden Profile!
  - **Spanning Tree Confidence Score**: Displays `1.0` (deterministic match with full edge audit).
  - Inspect the interactive Node-Link visualization and survivorship decision tree.

---

## 🤖 Part 4: Testing Autonomous AI MCP Tools

With the `.cursor/mcp.json` configuration or FastMCP CLI, AI agents can inspect schemas, suggest rules, and query Customer 360 records in BigQuery without manual SQL:

### 1. Start the FastMCP Server for BigQuery
```bash
export IDR_PLATFORM="bigquery"
export IDR_PROJECT="my-data-project-123"

python -m idr_mcp
```

### 2. Example Agent Prompts (in Cursor or Claude Desktop)
- *"Profile the tables in dataset ecommerce_demo and suggest deterministic identity resolution match rules."*
- *"Simulate what happens if we add an exact phone match rule with weight 0.9."*
- *"Look up the Customer 360 profile for alice.smith@example.com including all linked accounts and confidence score."*

---

## 🔍 Part 5: Inspect Results Directly in BigQuery Studio SQL

Open BigQuery Studio and verify that IDR created all output tables and golden profiles:

```sql
-- 1. Inspect Resolved Customer Clusters
SELECT 
    resolved_id,
    cluster_size,
    confidence_score,
    created_at
FROM `idr_out.identity_clusters_current`
ORDER BY cluster_size DESC;

-- 2. Inspect Entity Membership (Cross-System Linkage)
SELECT 
    m.resolved_id,
    m.source_id,
    m.source_key,
    m.entity_key,
    m.created_at
FROM `idr_out.identity_resolved_membership_current` m
ORDER BY m.resolved_id, m.source_id;

-- 3. Inspect Golden Customer Profiles (Merged 360 Records)
SELECT 
    resolved_id,
    cluster_size,
    first_name,
    last_name,
    email,
    phone
FROM `idr_out.golden_profiles_current`;

-- 4. Inspect Match Audit & Edge Lineage (Zero Hallucination Proof)
SELECT 
    e.left_entity_key,
    e.right_entity_key,
    e.identifier_type,
    e.identifier_value,
    e.rule_id,
    e.confidence_score
FROM `idr_out.identity_edges_current` e;

-- 5. Check High-Cardinality Safeguards (Skipped Groups)
SELECT * FROM `idr_out.skipped_identifier_groups`;
```

---

## 🧹 Part 6: Cleanup & Teardown

To tear down all test datasets when you finish testing:

```sql
-- Drop IDR datasets and demo data
DROP SCHEMA IF EXISTS `idr_meta` CASCADE;
DROP SCHEMA IF EXISTS `idr_work` CASCADE;
DROP SCHEMA IF EXISTS `idr_out` CASCADE;
DROP SCHEMA IF EXISTS `ecommerce_demo` CASCADE;
```
*(Or in terminal via `bq rm -r -f -d my-data-project-123:ecommerce_demo`)*.
