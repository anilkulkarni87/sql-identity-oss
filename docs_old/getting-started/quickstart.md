---
tags:
  - getting-started
  - quickstart
  - tutorial
  - setup
---

# Act II: First Contact

**Seeing is believing.**

Before we dive into architecture diagrams and configuration YAMLs, let's just make it work. In the next 5 minutes, you will spin up a full identity resolution pipeline on your local machine.

---

## The 60-Second Demo

We provide a self-contained **Quickstart** command that sets up a local DuckDB environment, generates data, and runs the pipeline.

### 1. Install

```bash
pip install "sql-identity-resolution[duckdb,api,mcp]"
```

### 2. Run

```bash
idr quickstart
```

### 3. What just happened?
In about 10 seconds, the engine:
1.  Created a local database (`qs_test.duckdb` or similar).
2.  Generated **1,000+ synthetic customers** with messy data (typos, shared emails).
3.  Ran the **Identity Resolution** pipeline.
4.  Displayed a summary of clusters and edges.

---

## 🖥️ Graphical Interface

Once the data is generated, you can explore it visually.

```bash
idr serve
```

Open **http://localhost:8000** to see:
- **Dashboard**: High-level metrics.
- **Identity Graph**: Visual explorer for customer clusters.
- **Config Editor**: UI for rules and mappings.

---

## 🤖 AI Agent Integration

You can also talk to your data using Claude or Cursor.

```bash
export IDR_PLATFORM=duckdb
export IDR_DATABASE=./my_db.duckdb
idr mcp
```

This starts an **MCP Server** that allows AI agents to query the identity graph contextually.

---

## The Unified Workflow

Now that you've seen it locally, how do you run this on **Snowflake**, **BigQuery**, or **Databricks**?

You don't need to rewrite code. You just switch the **Platform Adapter**.

### 1. Initialize
Prepare your database schema:

```bash
idr init --platform [snowflake|bigquery|databricks]
```

### 2. Configure
Apply your `config.yaml` rules (Act III):

```bash
idr config apply --platform [platform] --file config.yaml
```

### 3. Run
Launch the resolution pipeline:

```bash
idr run --platform [platform] --mode FULL
```

---

## Next Station: Building Your Truth

You've seen the engine run. Now it's time to teach it *your* business rules.

[:octicons-arrow-right-24: Act III: Building Your Truth](../guides/configuration.md)
