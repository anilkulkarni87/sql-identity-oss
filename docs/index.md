---
hide:
  - navigation
  - toc
---

<style>
.idr-hero {
  text-align: center;
  padding: 3rem 1.5rem 2rem;
  margin: 0.5rem 0 2rem;
  background: radial-gradient(circle at 50% 0%, rgba(56, 189, 248, 0.15) 0%, rgba(99, 102, 241, 0.08) 50%, transparent 100%);
  border-radius: 1.25rem;
  border: 1px solid var(--md-default-fg-color--lightest, #e2e8f0);
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.06);
}
[data-md-color-scheme="slate"] .idr-hero {
  background: radial-gradient(circle at 50% 0%, rgba(56, 189, 248, 0.18) 0%, rgba(99, 102, 241, 0.08) 50%, transparent 100%);
  border-color: rgba(56, 189, 248, 0.25);
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
}
.idr-hero-badge {
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.35rem 0.9rem;
  font-size: 0.82rem;
  font-weight: 600;
  color: #0284c7;
  background-color: rgba(2, 132, 199, 0.08);
  border: 1px solid rgba(2, 132, 199, 0.25);
  border-radius: 9999px;
  margin-bottom: 1.25rem;
}
[data-md-color-scheme="slate"] .idr-hero-badge {
  color: #38bdf8;
  background-color: rgba(56, 189, 248, 0.12);
  border-color: rgba(56, 189, 248, 0.35);
}
.idr-hero-title {
  font-size: 2.75rem !important;
  font-weight: 800 !important;
  line-height: 1.15 !important;
  letter-spacing: -0.03em !important;
  margin: 0 0 1rem 0 !important;
  color: var(--md-default-fg-color, #0f172a) !important;
}
[data-md-color-scheme="slate"] .idr-hero-title {
  color: #f8fafc !important;
}
.idr-gradient-text {
  background: linear-gradient(135deg, #0284c7 0%, #6366f1 50%, #8b5cf6 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}
[data-md-color-scheme="slate"] .idr-gradient-text {
  background: linear-gradient(135deg, #38bdf8 0%, #818cf8 50%, #c084fc 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}
.idr-hero-subtitle {
  font-size: 1.15rem !important;
  line-height: 1.6 !important;
  max-width: 700px;
  margin: 0 auto 1.75rem !important;
  color: var(--md-default-fg-color--light, #64748b) !important;
}
.idr-hero-cta {
  display: flex;
  justify-content: center;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.75rem;
  margin-bottom: 1.5rem;
}
.idr-hero-cta .md-button {
  margin: 0 !important;
}
.idr-stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 1rem;
  margin: 1.5rem 0 2.5rem;
}
.idr-stat-card {
  text-align: center;
  padding: 1.25rem 1rem;
  background-color: var(--md-default-bg-color, #ffffff);
  border: 1px solid var(--md-default-fg-color--lightest, #e2e8f0);
  border-radius: 0.75rem;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
}
[data-md-color-scheme="slate"] .idr-stat-card {
  background-color: rgba(22, 27, 34, 0.8);
  border-color: rgba(56, 189, 248, 0.2);
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
}
.idr-stat-number {
  font-size: 2.25rem;
  font-weight: 800;
  line-height: 1.1;
  color: #0284c7;
  font-family: var(--md-code-font, monospace);
}
[data-md-color-scheme="slate"] .idr-stat-number {
  color: #38bdf8;
}
.idr-stat-label {
  font-size: 0.82rem;
  font-weight: 500;
  color: var(--md-default-fg-color--light, #64748b);
  margin-top: 0.35rem;
}
</style>

<div class="idr-hero">
  <div class="idr-hero-badge">
    ⚡ Warehouse-Native Identity Resolution v0.6.1 • Apache 2.0
  </div>
  <h1 class="idr-hero-title">
    Deterministic Customer 360.<br/>
    <span class="idr-gradient-text">Zero Data Movement.</span>
  </h1>
  <p class="idr-hero-subtitle">
    Unify fragmented customer records natively inside DuckDB, Snowflake, BigQuery, and Databricks.
    Pure SQL connected components, deterministic confidence scoring, and 17 FastMCP agent tools.
  </p>
  <div class="idr-hero-cta">
    <a href="03-quickstart/" class="md-button md-button--primary">Get Started in 60s →</a>
    <a href="https://github.com/anilkulkarni87/sql-identity-oss" class="md-button" target="_blank">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" style="vertical-align: text-bottom; margin-right: 4px;"><path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/></svg>
      View on GitHub
    </a>
  </div>
  <p align="center" style="margin: 0; display: flex; justify-content: center; align-items: center; gap: 0.5rem; flex-wrap: wrap;">
    <a href="https://github.com/anilkulkarni87/sql-identity-oss/actions/workflows/test.yml"><img src="https://github.com/anilkulkarni87/sql-identity-oss/actions/workflows/test.yml/badge.svg" alt="CI Tests"></a>
    <img src="https://img.shields.io/badge/Python-3.9_%7C_3.10_%7C_3.11_%7C_3.12_%7C_3.13-blue.svg" alt="Python Versions">
    <a href="https://github.com/anilkulkarni87/sql-identity-oss/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License"></a>
    <img src="https://img.shields.io/badge/Scale-50k_entities%2Fsec-success.svg" alt="Scale Verified">
  </p>
</div>

<div class="idr-stats-grid">
  <div class="idr-stat-card">
    <div class="idr-stat-number">0</div>
    <div class="idr-stat-label">Data Movement / Egress</div>
  </div>
  <div class="idr-stat-card">
    <div class="idr-stat-number">50,000+</div>
    <div class="idr-stat-label">Entities / Sec Scale</div>
  </div>
  <div class="idr-stat-card">
    <div class="idr-stat-number">4</div>
    <div class="idr-stat-label">Cloud Warehouses Supported</div>
  </div>
  <div class="idr-stat-card">
    <div class="idr-stat-number">17</div>
    <div class="idr-stat-label">FastMCP Agent Tools</div>
  </div>
</div>

## 🧩 Architectural Highlights

<div class="grid cards" markdown="1">

-   :material-shield-check:{ .lg .middle } __Zero Data Egress__

    ---

    All customer data stays entirely inside your warehouse security perimeter (DuckDB, Snowflake, BigQuery, Databricks). Fully compliant with HIPAA, GDPR, and CCPA.

-   :material-target:{ .lg .middle } __Deterministic & Auditable__

    ---

    Every edge, cluster, and golden attribute is backed by explicit match rules and Spanning Tree Connectivity scoring. No probabilistic black-box hallucinations.

-   :material-graphql:{ .lg .middle } __Anchor-Based Graph Building__

    ---

    Avoids Cartesian join blowouts on common identifiers by connecting groups via star-topology anchor edges (`O(N)` complexity instead of `O(N²)`).

-   :material-sync:{ .lg .middle } __Incremental Delta Upsert__

    ---

    Local Subgraph Stitching isolates only impacted clusters during incremental runs, updating 1.5M clusters in seconds without full historical table scans.

-   :material-key-chain:{ .lg .middle } __Zero-Retype Credentials__

    ---

    Auto-detects active credentials from `~/.databrickscfg`, `~/.snowflake/connections.toml`, `~/.dbt/profiles.yml`, and Google Cloud ADC with 1-click connect.

-   :material-robot:{ .lg .middle } __AI Agent Ready (MCP)__

    ---

    Includes 17 FastMCP tools allowing Cursor, Claude Desktop, and Antigravity to autonomously profile schemas, simulate rule impact, and resolve Customer 360 records.

</div>

---

## 📊 Comparison: In-Warehouse IDR vs. Alternatives

| Capability | `sql-identity-oss` | SaaS CDPs (Segment / Hightouch) | Legacy MDM (Tamr / Informatica) | Ad-Hoc SQL JOINs |
| :--- | :--- | :--- | :--- | :--- |
| **Data Movement** | **Zero (In-Warehouse)** | High (Data synced to vendor cloud) | High (Data extracted to external clusters) | Zero |
| **Cost / License** | **Free / Apache 2.0 (OSS)** | \$30k–\$150k+/yr | \$100k+/yr enterprise licenses | High engineering hours |
| **Graph Resolution** | **Multi-hop Label Propagation** | Limited / Flat rules | ML black box | Fails on >2 hops (Join blowouts) |
| **Auditability** | **100% Explainable SQL & Lineage** | Closed vendor black box | Statistical probabilities | Custom scripts |
| **Survivorship** | **Dynamic (Recency, Priority, Mode)** | Basic / First seen | Complex custom rules | Manual CASE/COALESCE |
| **dbt & SQL Native** | **Full dbt package (`dbt_idr`)** | No (Proprietary UI/APIs) | No (Custom pipelines) | Manual SQL |
| **AI Agent Readiness** | **17 FastMCP Tools included** | Closed API | Closed API | None |

---

## ⚡ 60-Second Quickstart

Try identity resolution immediately using your local DuckDB engine—no cloud credentials or setup required:

```bash
# 1. Install IDR with DuckDB, API, and MCP extras
pip install "sql-identity-resolution[duckdb,api,mcp]"

# 2. Run the automated quickstart
idr quickstart
```

This single command synthesizes 10,000 multi-source customer records across Web, Store, Mobile, and Call Center, injects cross-channel identifier overlaps (Email, Phone, Loyalty ID), provisions schemas (`idr_meta`, `idr_work`, `idr_out`), applies matching rules, and materializes golden customer profiles with full survivorship logic.

---

## 🏗️ Supported Warehouses & Execution Matrix

| Warehouse | Connection Mode | Incremental Merge | Distributed Scale | Production Ready |
| :--- | :--- | :--- | :--- | :--- |
| **DuckDB** | Local file / In-memory | `INSERT INTO` (Upsert) | Single-node (up to 50M rows) | Yes (Dev / Edge) |
| **Snowflake** | Snowpark / DB-API connector | `MERGE INTO` | Multi-cluster warehouse (100M+) | Yes (Enterprise) |
| **BigQuery** | Cloud SDK / Service Account | `MERGE` | Serverless slot autoscaling | Yes (Enterprise) |
| **Databricks** | Spark Connect / Unity Catalog | `MERGE INTO` (Delta) | Photon / Multi-worker clusters | Yes (Enterprise) |

---

## 🧩 Architectural Pipeline

```mermaid
flowchart TD
    subgraph Sources["1. Raw Source Systems"]
        direction LR
        CRM["CRM Accounts"]
        Web["E-Commerce Orders"]
        POS["Retail POS Records"]
    end

    subgraph Stages["2. Pluggable SQL Pipeline Stages"]
        direction TB
        S1["1. Extraction & Normalization<br/>(Cleanse & hash PII identifiers)"]
        S2["2. Graph Construction<br/>(Anchor-based N-1 star topology edges)"]
        S3["3. Clustering & Connected Components<br/>(Iterative SQL label propagation)"]
        S4["4. Graph & Quality Metrics<br/>(Confidence scoring & cluster size telemetry)"]
        S5["5. Golden Profile Output<br/>(Survivorship rules: recency, priority, frequency)"]
        S6["6. Reconciliation & Lineage<br/>(Audit trail & delta watermarks)"]
        S1 --> S2 --> S3 --> S4 --> S5 --> S6
    end

    subgraph Targets["3. Serving Schema (idr_out)"]
        direction LR
        Clusters["identity_clusters_current"]
        Membership["identity_resolved_membership_current"]
        Profiles["golden_customer_profiles"]
    end

    Sources ==> S1
    S6 ==> Targets
```

---

## 🛠️ Multiple Ways to Operate

=== "CLI"
    ```bash
    # Run full identity resolution
    idr run --config idr_config.yaml --mode FULL

    # Run incremental resolution on new data
    idr run --config idr_config.yaml --mode INCR

    # Inspect graph health and clusters
    idr metrics --output json
    ```

=== "FastAPI & REST"
    ```bash
    # Start REST API with Swagger docs and Prometheus metrics
    idr-api --host 0.0.0.0 --port 8000
    # Visit http://localhost:8000/docs
    ```

=== "Model Context Protocol (MCP)"
    ```bash
    # Start MCP server for Claude Desktop / Cursor / Antigravity
    idr-mcp
    ```

=== "React Web UI"
    ```bash
    # Launch interactive visual dashboard
    cd idr_ui && npm run dev
    # Access cluster exploration, graph metrics, and pipeline controls
    ```

=== "dbt Integration"
    ```yaml
    # packages.yml
    packages:
      - package: anilkulkarni87/dbt_idr
        version: 0.6.1
    ```

---

## 📚 Documentation Navigation

* **[Overview](01-overview.md)**: Deep dive into the architecture, design principles, and comparison with legacy CDPs.
* **[Quickstart](03-quickstart.md)**: Step-by-step hands-on guide from zero to resolved customer profiles.
* **[Installation](02-installation.md)**: Pip extras, virtual environments, Docker Compose, and production containers.
* **[Platform Adapters](06-platforms.md)**: Detailed configuration guides for Snowflake, BigQuery, Databricks, and DuckDB.
* **[Matching Algorithm](19-matching-algorithm.md)**: Mathematical underpinnings of deterministic matching and label propagation.
* **[Schema Reference](08-schema.md)**: Comprehensive DDL reference for `idr_meta`, `idr_work`, and `idr_out`.
* **[MCP Server](11-mcp.md)**: Connect LLMs and autonomous agents to your identity resolution pipeline.
* **[Scale Testing](13-scale-testing.md)**: Benchmarks and test recipes for datasets from 100K to 10M+ records.
* **[Troubleshooting](14-troubleshooting.md)**: Diagnostic recipes for graph explosion, slow label propagation, and warehouse timeouts.

---

## 📄 License & Community

SQL Identity Resolution is open-source software licensed under the [Apache License, Version 2.0](https://github.com/anilkulkarni87/sql-identity-oss/blob/main/LICENSE). Contributions, bug reports, and feature requests are welcome on [GitHub](https://github.com/anilkulkarni87/sql-identity-oss)!
