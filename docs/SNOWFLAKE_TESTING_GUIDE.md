# Snowflake Free Trial Testing & Walkthrough Guide

This comprehensive, step-by-step walkthrough is designed for testing **SQL Identity Resolution (`v0.6.1`)** on **Snowflake** using either a **Snowflake 30-Day Free Trial** ($400 free credit, no credit card required) or an existing enterprise Snowflake account.

!!! tip "Quick Start with Snowflake Free Trial"
    You can spin up a free trial in 2 minutes at [signup.snowflake.com](https://signup.snowflake.com/). Select **Standard** or **Enterprise** edition in any cloud region (AWS, Azure, or GCP). A default `COMPUTE_WH` warehouse is automatically provisioned.

---

## 🗺️ The Testing Journey

<div class="grid cards" markdown="1">

-   :material-database-arrow-up:{ .lg .middle } __1. Ingest Test Data__

    ---

    Choose between real synthetic parquet files from `Ecommerce_synthetic` or zero-install pure SQL generation in Snowsight.
    
    [:octicons-arrow-right-24: Ingestion Options](#part-2-ingest-test-data)

-   :material-monitor-dashboard:{ .lg .middle } __2. Interactive Web UI__

    ---

    Connect via 1-Click Zero-Retype discovery, walk through the 6-Step Setup Wizard (`/setup`), and visually explore the Golden Profile graph in `/explorer`.
    
    [:octicons-arrow-right-24: UI Walkthrough](#part-3-testing-via-the-interactive-web-ui)

-   :material-robot:{ .lg .middle } __3. Autonomous AI MCP Tools__

    ---

    Run `discover_and_suggest_rules`, `simulate_rule_impact`, and `resolve_customer_360` from Cursor, Claude Desktop, or Antigravity via FastMCP.
    
    [:octicons-arrow-right-24: Agentic Tools](#part-4-testing-autonomous-ai-mcp-tools)

-   :material-shield-search:{ .lg .middle } __4. Snowsight SQL Verification__

    ---

    Inspect final Golden Customer Profiles, Spanning Tree confidence scores, and cluster memberships directly in Snowflake SQL.
    
    [:octicons-arrow-right-24: SQL Queries](#part-5-inspect-results-directly-in-snowsight-sql)

</div>

---

## 📋 Part 1: Prerequisites & Connection Setup

### A. Core Connection Parameters
Retrieve your Snowflake connection details from the Snowsight UI or account welcome email:

| Parameter | Type | Where to Find in Snowflake / Snowsight | Example Value |
| :--- | :--- | :--- | :--- |
| **`SNOWFLAKE_ACCOUNT`** | Locator/Org | Lower-left user profile $\rightarrow$ **Account** $\rightarrow$ **Copy Account Identifier** (format: `orgname-accountname` or `xy12345.us-east-1`) | `xy12345.us-east-1` |
| **`SNOWFLAKE_USER`** | Username | Your Snowflake login name | `DEMO_USER` |
| **`SNOWFLAKE_PASSWORD`** | Secret | Your Snowflake login password | `YourStrongPassword123!` |
| **`SNOWFLAKE_WAREHOUSE`** | Compute | **Admin** $\rightarrow$ **Warehouses** | `COMPUTE_WH` |
| **`SNOWFLAKE_DATABASE`** | Database | **Data** $\rightarrow$ **Databases** | `DEMO_DB` (or `CUSTOMER_IDR`) |
| **`SNOWFLAKE_SCHEMA`** | Schema | Target schema under the database | `PUBLIC` (or `ECOMMERCE_DEMO`) |
| **`SNOWFLAKE_ROLE`** | Role | Default role (optional, defaults to primary role) | `ACCOUNTADMIN` or `SYSADMIN` |

---

### B. Zero-Retype Credential Auto-Discovery (New in v0.6.1)
SQL Identity Resolution automatically detects existing Snowflake credentials on your local machine:
- **Snowflake CLI**: `~/.snowflake/connections.toml` or `~/.snowflake/config.toml`
- **dbt Profiles**: `~/.dbt/profiles.yml` or `./profiles.yml`
- **Environment Variables**: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`

If you use `~/.snowflake/connections.toml`, ensure an entry exists:
```toml
[demo]
account = "xy12345.us-east-1"
user = "DEMO_USER"
password = "YourStrongPassword123!"
warehouse = "COMPUTE_WH"
database = "DEMO_DB"
schema = "PUBLIC"
```
The IDR Setup Wizard (`http://localhost:5173/setup`) will display a **"Discovered Local Credentials"** card with a **1-Click Connect** button.

---

## 📦 Part 2: Ingest Test Data

You have two convenient options for loading sample retail data into Snowflake:

=== "Option A: Zero-Install Pure SQL Generation (Recommended for Quick Testing)"

    No local files or uploads required! Paste this SQL script directly into a **Snowsight Worksheet** (`Projects` $\rightarrow$ `Worksheets` $\rightarrow$ `+ SQL Worksheet`):

    ```sql
    -- 1. Setup Context
    USE WAREHOUSE COMPUTE_WH;
    CREATE DATABASE IF NOT EXISTS DEMO_DB;
    USE DATABASE DEMO_DB;
    CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DEMO;
    USE SCHEMA ECOMMERCE_DEMO;

    -- 2. CRM Customers (Primary CRM record)
    CREATE OR REPLACE TABLE DEMO_DB.ECOMMERCE_DEMO.CUSTOMERS (
        customer_id VARCHAR(50),
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        email VARCHAR(255),
        phone VARCHAR(50),
        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );

    INSERT INTO DEMO_DB.ECOMMERCE_DEMO.CUSTOMERS (customer_id, first_name, last_name, email, phone) VALUES
        ('CRM_001', 'Alice', 'Smith', 'alice.smith@example.com', '+1-555-0101'),
        ('CRM_002', 'Robert', 'Taylor', 'bob.taylor@work.com', '+1-555-0102'),
        ('CRM_003', 'Charlie', 'Davis', 'charlie.d@gmail.com', '+1-555-0103'),
        ('CRM_004', 'David', 'Miller', 'david.miller@example.com', '+1-555-0104'),
        ('CRM_005', 'Eva', 'Wilson', 'eva.wilson@corp.org', '+1-555-0105');

    -- 3. In-Store Point of Sale (POS Accounts - shares phone & alternate emails)
    CREATE OR REPLACE TABLE DEMO_DB.ECOMMERCE_DEMO.INSTORE_ACCOUNTS (
        instore_account_id VARCHAR(50),
        phone_number VARCHAR(50),
        alt_email VARCHAR(255),
        registered_store VARCHAR(50),
        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );

    INSERT INTO DEMO_DB.ECOMMERCE_DEMO.INSTORE_ACCOUNTS (instore_account_id, phone_number, alt_email, registered_store) VALUES
        ('POS_901', '+1-555-0101', 'asmith_home@gmail.com', 'Store_Chicago_01'),  -- Matches Alice via phone
        ('POS_902', '+1-555-0102', 'bob.personal@gmail.com', 'Store_Austin_02'),   -- Matches Bob via phone
        ('POS_903', '+1-555-9999', 'unknown.shopper@retail.com', 'Store_NYC_01');

    -- 4. Loyalty Rewards Program (Shares alternate email & loyalty ID)
    CREATE OR REPLACE TABLE DEMO_DB.ECOMMERCE_DEMO.LOYALTY_MEMBERS (
        loyalty_id VARCHAR(50),
        contact_email VARCHAR(255),
        tier VARCHAR(50),
        points_balance NUMBER(10, 0),
        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );

    INSERT INTO DEMO_DB.ECOMMERCE_DEMO.LOYALTY_MEMBERS (loyalty_id, contact_email, tier, points_balance) VALUES
        ('LOYALTY_A1', 'asmith_home@gmail.com', 'Platinum', 12450),  -- Bridges Alice's in-store account via email
        ('LOYALTY_B2', 'bob.taylor@work.com', 'Gold', 8200),         -- Bridges Bob's CRM account via email
        ('LOYALTY_C3', 'charlie.d@gmail.com', 'Silver', 3100);       -- Bridges Charlie's CRM account via email

    -- 5. E-Commerce Orders
    CREATE OR REPLACE TABLE DEMO_DB.ECOMMERCE_DEMO.ORDERS_ONLINE (
        order_id VARCHAR(50),
        customer_email VARCHAR(255),
        customer_phone VARCHAR(50),
        order_total NUMBER(12, 2),
        order_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );

    INSERT INTO DEMO_DB.ECOMMERCE_DEMO.ORDERS_ONLINE (order_id, customer_email, customer_phone, order_total) VALUES
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

    #### 2. Load Parquet into Snowflake via SnowSQL or Internal Stage
    ```sql
    USE DATABASE DEMO_DB;
    USE SCHEMA ECOMMERCE_DEMO;

    -- Create file format for Parquet
    CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT TYPE = 'PARQUET';

    -- Create temporary stage
    CREATE OR REPLACE STAGE IDR_DATA_STAGE FILE_FORMAT = PARQUET_FORMAT;
    ```
    Upload files using SnowSQL CLI:
    ```bash
    snowsql -a xy12345.us-east-1 -u DEMO_USER -d DEMO_DB -s ECOMMERCE_DEMO \
      -q "PUT file://./output/demo_small/customers.parquet @IDR_DATA_STAGE AUTO_COMPRESS=TRUE;"
    ```
    And create table directly via `INFER_SCHEMA`:
    ```sql
    CREATE OR REPLACE TABLE CUSTOMERS
      USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(INFER_SCHEMA(LOCATION=>'@IDR_DATA_STAGE/customers.parquet', FILE_FORMAT=>'PARQUET_FORMAT')));

    COPY INTO CUSTOMERS FROM @IDR_DATA_STAGE/customers.parquet FILE_FORMAT = (TYPE = 'PARQUET') MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
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

### 2. Step 1: Connect to Snowflake
- Select **Snowflake** as the Platform.
- If you have `~/.snowflake/connections.toml` or `~/.dbt/profiles.yml`, the **Discovered Local Credentials** card will show your detected connection. Click **1-Click Connect**.
- Alternatively, enter your parameters manually:
  - **Account**: `xy12345.us-east-1`
  - **User**: `DEMO_USER`
  - **Password**: `YourStrongPassword123!`
  - **Warehouse**: `COMPUTE_WH`
  - **Database**: `DEMO_DB`
  - **Schema**: `ECOMMERCE_DEMO`
- Click **Test & Continue**.

---

### 3. Step 2: Select Sources & Entity Keys
IDR profiles your tables in Snowflake. Select your 4 source tables and primary entity keys:

| Table Name | Entity Key Column | Description |
| :--- | :--- | :--- |
| `CUSTOMERS` | `CUSTOMER_ID` | CRM Account Records |
| `INSTORE_ACCOUNTS` | `INSTORE_ACCOUNT_ID` | In-Store POS Accounts |
| `LOYALTY_MEMBERS` | `LOYALTY_ID` | Rewards Members |
| `ORDERS_ONLINE` | `ORDER_ID` | Web E-Commerce Transactions |

---

### 4. Step 3: Map Identifiers
Map columns that share identity linkages across systems:

| Table | Identifier Type | Source Column | Normalization |
| :--- | :--- | :--- | :--- |
| `CUSTOMERS` | `EMAIL` | `EMAIL` | Lowercase + Trim |
| `CUSTOMERS` | `PHONE` | `PHONE` | E.164 Clean |
| `INSTORE_ACCOUNTS` | `PHONE` | `PHONE_NUMBER` | E.164 Clean |
| `INSTORE_ACCOUNTS` | `EMAIL` | `ALT_EMAIL` | Lowercase + Trim |
| `LOYALTY_MEMBERS` | `EMAIL` | `CONTACT_EMAIL` | Lowercase + Trim |
| `ORDERS_ONLINE` | `EMAIL` | `CUSTOMER_EMAIL` | Lowercase + Trim |
| `ORDERS_ONLINE` | `PHONE` | `CUSTOMER_PHONE` | E.164 Clean |

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
| `email` | `SOURCE_PRIORITY` | `CUSTOMERS` $\rightarrow$ `LOYALTY_MEMBERS` $\rightarrow$ `INSTORE_ACCOUNTS` |
| `phone` | `RECENCY` | `created_at` DESC |

---

### 7. Step 6: Execute Resolution
- Click **Run Identity Resolution**.
- Execution runs natively inside your Snowflake Virtual Warehouse (`COMPUTE_WH`).
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

With the `.cursor/mcp.json` configuration or FastMCP CLI, AI agents can inspect schemas, suggest rules, and query Customer 360 records in Snowflake without manual SQL:

### 1. Start the FastMCP Server for Snowflake
```bash
export IDR_PLATFORM="snowflake"
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
export SNOWFLAKE_USER="DEMO_USER"
export SNOWFLAKE_PASSWORD="YourStrongPassword123!"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_DATABASE="DEMO_DB"
export SNOWFLAKE_SCHEMA="ECOMMERCE_DEMO"

python -m idr_mcp
```

### 2. Example Agent Prompts (in Cursor or Claude Desktop)
- *"Profile the tables in DEMO_DB.ECOMMERCE_DEMO and suggest deterministic identity resolution match rules."*
- *"Simulate what happens if we add an exact phone match rule with weight 0.9."*
- *"Look up the Customer 360 profile for alice.smith@example.com including all linked accounts and confidence score."*

---

## 🔍 Part 5: Inspect Results Directly in Snowsight SQL

Open a Snowsight Worksheet and verify that IDR created all output tables and golden profiles:

```sql
USE WAREHOUSE COMPUTE_WH;
USE DATABASE DEMO_DB;

-- 1. Inspect Resolved Customer Clusters
SELECT 
    resolved_id,
    cluster_size,
    confidence_score,
    created_at
FROM DEMO_DB.IDR_OUT.IDENTITY_CLUSTERS_CURRENT
ORDER BY cluster_size DESC;

-- 2. Inspect Entity Membership (Cross-System Linkage)
SELECT 
    m.resolved_id,
    m.source_id,
    m.source_key,
    m.entity_key,
    m.created_at
FROM DEMO_DB.IDR_OUT.IDENTITY_RESOLVED_MEMBERSHIP_CURRENT m
ORDER BY m.resolved_id, m.source_id;

-- 3. Inspect Golden Customer Profiles (Merged 360 Records)
SELECT 
    resolved_id,
    cluster_size,
    first_name,
    last_name,
    email,
    phone
FROM DEMO_DB.IDR_OUT.GOLDEN_PROFILES_CURRENT;

-- 4. Inspect Match Audit & Edge Lineage (Zero Hallucination Proof)
SELECT 
    e.left_entity_key,
    e.right_entity_key,
    e.identifier_type,
    e.identifier_value,
    e.rule_id,
    e.confidence_score
FROM DEMO_DB.IDR_OUT.IDENTITY_EDGES_CURRENT e;

-- 5. Check High-Cardinality Safeguards (Skipped Groups)
SELECT * FROM DEMO_DB.IDR_OUT.SKIPPED_IDENTIFIER_GROUPS;
```

---

## 🧹 Part 6: Cleanup & Teardown

To tear down all test objects when you finish testing:

```sql
USE WAREHOUSE COMPUTE_WH;

-- Drop demo tables and schemas
DROP SCHEMA IF EXISTS DEMO_DB.IDR_META CASCADE;
DROP SCHEMA IF EXISTS DEMO_DB.IDR_WORK CASCADE;
DROP SCHEMA IF EXISTS DEMO_DB.IDR_OUT CASCADE;
DROP SCHEMA IF EXISTS DEMO_DB.ECOMMERCE_DEMO CASCADE;

-- Optional: Drop database entirely
-- DROP DATABASE IF EXISTS DEMO_DB;
```
