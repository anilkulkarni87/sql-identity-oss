---
title: SQL Identity Resolution
hide:
  - navigation
  - toc
---

<div class="idr-hero" markdown>

<div class="idr-hero-badge">
  <span>⚡ Headless, Warehouse-Native Identity Engine · v0.6.1</span>
</div>

<h1 class="idr-hero-title">
  Unify Customer Identities <span class="idr-gradient-text">Directly Inside Your Data Warehouse</span>
</h1>

<p class="idr-hero-subtitle">
  Headless-first deterministic graph resolution, blocking-based fuzzy super-clustering, golden profile survivorship, persistent cluster IDs, and GDPR erasure across <strong>DuckDB</strong>, <strong>Snowflake</strong>, <strong>BigQuery</strong>, and <strong>Databricks</strong>. Run via CLI, Python, dbt, FastAPI, or Remote MCP—with an optional Explorer UI. Zero data movement. 100% SQL-auditable.
</p>

<div class="idr-hero-cta">
  <a href="getting-started/" class="idr-btn-primary">Get Started in 60 Seconds →</a>
  <a href="concepts-and-architecture/" class="idr-btn-secondary">Explore Architecture</a>
  <a href="https://github.com/anilkulkarni87/sql-identity-oss" class="idr-btn-secondary">GitHub Repository</a>
</div>

<div class="idr-pills">
  <span class="idr-pill">❄️ Snowflake</span>
  <span class="idr-pill">🧱 Databricks</span>
  <span class="idr-pill">☁️ BigQuery</span>
  <span class="idr-pill">🦆 DuckDB</span>
  <span class="idr-pill">🤖 23 MCP Tools + /mcp</span>
  <span class="idr-pill">📦 dbt Package</span>
  <span class="idr-pill">🪝 Webhooks</span>
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

### 🎬 Live Cloud Benchmarks (6.3M Entities · 15 Source Tables)

All three cloud warehouses resolved **6,297,049** entities and **5,381,635** edges into **1,544,042** identical clusters from a single portable config:

<div class="idr-stats-grid">
  <div class="idr-stat-card">
    <div class="idr-stat-number">170s</div>
    <div class="idr-stat-label">Snowflake (2.8 min · ~37k ent/s)</div>
  </div>
  <div class="idr-stat-card">
    <div class="idr-stat-number">296s</div>
    <div class="idr-stat-label">Databricks (4.9 min · ~21k ent/s)</div>
  </div>
  <div class="idr-stat-card">
    <div class="idr-stat-number">306s</div>
    <div class="idr-stat-label">BigQuery (5.1 min · ~21k ent/s)</div>
  </div>
</div>

👉 [View Full Stage-by-Stage Production Benchmark Breakdown →](warehouses-and-schema.md#5-production-benchmarks-real-world-runtimes)

<div class="idr-media-frame">
  <img src="assets/recordings/idr-databricks-run.gif" alt="IDR Full Pipeline Run on Databricks" />
  <div class="idr-media-caption">End-to-end 6.3M-entity resolution run inside Databricks SQL Warehouse</div>
</div>

---

## 🔍 Before → After in 10 Seconds

<div class="idr-cards-grid">
  <div class="idr-card">
    <h3>Before: Fragmented Warehouse Tables</h3>
    <p>
      <code>CUSTOMERS</code>: <code>cust_101</code> · <code>alice+vip@gmail.com</code><br/>
      <code>ORDERS_ONLINE</code>: <code>ord_502</code> · <code>alice@gmail.com</code> · <code>+1-415-555-0199</code><br/>
      <code>INSTORE_ACCOUNTS</code>: <code>pos_980</code> · <code>(415) 555-0199</code>
    </p>
  </div>
  <div class="idr-card">
    <h3>After: Unified Golden Profile</h3>
    <p>
      <strong>Persistent ID:</strong> <code>resolved_id = res_8f3a1c</code><br/>
      <strong>Linked Keys:</strong> <code>cust_101</code>, <code>ord_502</code>, <code>pos_980</code><br/>
      <strong>Golden Record:</strong> Canonical email, phone, survivorship attributes, confidence score, and audit crosswalk.
    </p>
  </div>
</div>

---

## 🏗️ Enterprise Capabilities Built In

<div class="idr-cards-grid">
  <div class="idr-card">
    <h3>🔗 Deterministic + Fuzzy Graph Engine</h3>
    <p>Anchor-based star edges (<code>N-1</code> edges per group) prevent <code>O(N²)</code> clique explosion, paired with iterative Label Propagation and optional blocking-key fuzzy super-clustering.</p>
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
        S3["POS and Loyalty"]
        S4["Clickstream Events"]
    end

    subgraph Engine["8-Stage SQL Identity Resolution Engine"]
        E1["Stages 1-3: Delta Extraction and Canonicalization"]
        E2["Stages 4-5: Star Edge Builder and Overrides"]
        E3["Stages 6-7: Label Propagation and ID Stabilization"]
        E4["Stage 8: Golden Profile Survivorship and Crosswalk"]
        E1 --> E2
        E2 --> E3
        E3 --> E4
    end

    subgraph Outputs["Warehouse Output Tables (idr_out)"]
        O1["identity_resolved_membership_current"]
        O2["identity_clusters_current"]
        O3["golden_profile_current"]
        O4["resolved_id_crosswalk"]
    end

    subgraph Tooling["Interfaces and Integrations"]
        T1["React Web UI and Explorer"]
        T2["CLI and FastAPI"]
        T3["FastMCP Server (AI Agents)"]
    end

    S1 --> E1
    S2 --> E1
    S3 --> E1
    S4 --> E1
    E4 --> O1
    E4 --> O2
    E4 --> O3
    E4 --> O4
    O1 --> T1
    O2 --> T2
    O3 --> T3
```

---

## 📚 Documentation Map

| Guide | What You Will Learn |
| :--- | :--- |
| **[Getting Started](getting-started.md)** | Installation extras, 5-minute end-to-end CLI & UI workflow, and zero-config credential discovery. |
| **[Concepts & Architecture](concepts-and-architecture.md)** | Identifiers vs. attributes, the 8 pipeline stages, incremental watermark + no-watermark state-diffing, and persistent `resolved_id` stabilization. |
| **[Algorithm & Graph Engine](algorithm-and-graph-engine.md)** | Mathematical graph formulation, $\mathcal{O}(N)$ anchor star reduction theorem, pure SQL iterative label propagation, and ID stabilization invariants. |
| **[Configuration & Governance](configuration-and-governance.md)** | Full YAML reference, domain canonicalizers, default exclusions, manual overrides (`FORCE_LINK`, `DO_NOT_LINK`, `ISOLATE`), and GDPR erasure. |
| **[Interfaces & dbt](interfaces-and-dbt.md)** | Complete CLI reference, Web UI Setup Wizard & Cluster Explorer, FastAPI endpoints, FastMCP AI agent tools, and the `dbt_idr` package. |
| **[Warehouses & Schema](warehouses-and-schema.md)** | DuckDB, Snowflake, BigQuery, and Databricks setup, complete DDL schema tables (`idr_meta`, `idr_work`, `idr_out`), and production operations. |
