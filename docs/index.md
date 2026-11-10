---
title: SQL Identity Resolution
hide:
  - navigation
---

<div class="idr-hero" markdown>

<div class="idr-hero-badge">
  <span>⚡ Open-Source Warehouse-Native Identity Engine · v0.6.1</span>
</div>

<h1 class="idr-hero-title">
  Unify Customer Identities <span class="idr-gradient-text">Directly Inside Your Data Warehouse</span>
</h1>

<p class="idr-hero-subtitle">
  Production-grade deterministic graph resolution, blocking-based fuzzy super-clustering, golden profile survivorship, persistent cluster IDs, and GDPR erasure across <strong>DuckDB</strong>, <strong>Snowflake</strong>, <strong>BigQuery</strong>, and <strong>Databricks</strong>. Zero data movement. 100% SQL-auditable.
</p>

<div class="idr-hero-cta">
  <a href="getting-started/" class="idr-btn-primary">Get Started in 60 Seconds →</a>
  <a href="concepts-and-architecture/" class="idr-btn-secondary">Explore Architecture</a>
  <a href="https://github.com/anilkulkarni87/sql-identity-oss" class="idr-btn-secondary">GitHub Repository</a>
</div>

</div>

<div class="idr-stats-grid">
  <div class="idr-stat-card">
    <div class="idr-stat-number">4 Warehouses</div>
    <div class="idr-stat-label">DuckDB · Snowflake · BigQuery · Databricks</div>
  </div>
  <div class="idr-stat-card">
    <div class="idr-stat-number">8 Stages</div>
    <div class="idr-stat-label">Incremental Delta + State-Diff Graph Engine</div>
  </div>
  <div class="idr-stat-card">
    <div class="idr-stat-number">100% Persistent</div>
    <div class="idr-stat-label">Cross-Run Resolved IDs + Audit Crosswalk</div>
  </div>
  <div class="idr-stat-card">
    <div class="idr-stat-number">5 Interfaces</div>
    <div class="idr-stat-label">CLI · React UI · FastAPI · MCP · dbt Package</div>
  </div>
</div>

---

## ⚡ 60-Second Quickstart

Run a complete synthetic multi-source identity resolution pipeline locally on DuckDB:

```bash
# 1. Install with DuckDB, Web UI, and MCP extras
pip install "sql-identity-resolution[duckdb,ui,mcp]"

# 2. Run the 10,000-row synthetic quickstart demo
idr quickstart --rows 10000 --output demo.duckdb

# 3. Launch the interactive React Web UI & Cluster Explorer
idr ui --db demo.duckdb
```

---

## 🏗️ Enterprise Capabilities Built In

<div class="idr-cards-grid">
  <div class="idr-card">
    <h3>🔗 Deterministic + Fuzzy Graph Engine</h3>
    <p>Anchor-based star edges ($N-1$ edges per group) prevent $O(N^2)$ clique explosion, paired with iterative Label Propagation and optional blocking-key fuzzy super-clustering.</p>
  </div>
  <div class="idr-card">
    <h3>🧼 Domain Canonicalizers & Custom SQL</h3>
    <p>Built-in normalizers (<code>EMAIL_STANDARD</code>, <code>EMAIL_STRIP_PLUS</code>, <code>PHONE_DIGITS</code>, <code>PHONE_US_10</code>, <code>ALPHANUMERIC_UPPER</code>) plus custom warehouse SQL templates via <code>{val}</code>.</p>
  </div>
  <div class="idr-card">
    <h3>🛡️ Poison-Pill Exclusions & Guardrails</h3>
    <p>Built-in blocklist (<code>use_default_exclusions: true</code>), wildcard <code>ANY</code> rules, warehouse-table exclusions, and <code>max_group_size</code> thresholds stop bad data from creating mega-clusters.</p>
  </div>
  <div class="idr-card">
    <h3>📌 Persistent Resolved IDs & Crosswalk</h3>
    <p>"Largest Cluster Wins" stabilization preserves existing <code>resolved_id</code> values across incremental runs and logs every cluster merge or split into <code>idr_out.resolved_id_crosswalk</code>.</p>
  </div>
  <div class="idr-card">
    <h3>⚖️ Manual Stewardship Overrides</h3>
    <p>Override graph links with <code>FORCE_LINK</code>, <code>DO_NOT_LINK</code>, and <code>ISOLATE</code> via YAML, SQL (<code>idr_meta.identity_override</code>), Web UI Explorer, or <code>dbt_idr</code> seeds.</p>
  </div>
  <div class="idr-card">
    <h3>🗑️ GDPR / CCPA Right-to-be-Forgotten</h3>
    <p>Purge entities or identifier values across all output and staging tables with <code>idr privacy forget</code> while recording an immutable compliance receipt in <code>idr_meta.privacy_audit_log</code>.</p>
  </div>
</div>

---

## 🔄 End-to-End Architecture

```mermaid
flowchart LR
    subgraph Sources["Warehouse Source Tables"]
        S1["CRM Customers"]
        S2["E-Commerce Orders"]
        S3["POS & Loyalty"]
        S4["Clickstream / Events"]
    end

    subgraph Engine["8-Stage SQL Identity Resolution Engine (idr_core / dbt_idr)"]
        E1["1-3. Delta + State-Diff Extraction\n& Canonicalization"]
        E2["4-5. Anchor Edge Builder\n& Stewardship Overrides"]
        E3["6-7. Label Propagation +\nPersistent ID Stabilization"]
        E4["8. Golden Profile Survivorship\n& Crosswalk Ledger"]
        E1 --> E2 --> E3 --> E4
    end

    subgraph Outputs["Warehouse Output Tables (idr_out)"]
        O1["identity_resolved_membership_current"]
        O2["identity_clusters_current"]
        O3["golden_profile_current"]
        O4["resolved_id_crosswalk"]
    end

    subgraph Tooling["Interfaces & Integrations"]
        T1["React Web UI & Explorer"]
        T2["CLI & FastAPI"]
        T3["FastMCP Server (AI Agents)"]
    end

    Sources --> E1
    E4 --> Outputs
    Outputs --> Tooling
```

---

## 📚 Documentation Map

| Guide | What You Will Learn |
| :--- | :--- |
| **[Getting Started](getting-started.md)** | Installation extras, 5-minute end-to-end CLI & UI workflow, and zero-config credential discovery. |
| **[Concepts & Architecture](concepts-and-architecture.md)** | Identifiers vs. attributes, the 8 pipeline stages, incremental watermark + no-watermark state-diffing, and persistent `resolved_id` stabilization. |
| **[Configuration & Governance](configuration-and-governance.md)** | Full YAML reference, domain canonicalizers, default exclusions, manual overrides (`FORCE_LINK`, `DO_NOT_LINK`, `ISOLATE`), and GDPR erasure. |
| **[Interfaces & dbt](interfaces-and-dbt.md)** | Complete CLI reference, Web UI Setup Wizard & Cluster Explorer, FastAPI endpoints, FastMCP AI agent tools, and the `dbt_idr` package. |
| **[Warehouses & Schema](warehouses-and-schema.md)** | DuckDB, Snowflake, BigQuery, and Databricks setup, complete DDL schema tables (`idr_meta`, `idr_work`, `idr_out`), and production operations. |
