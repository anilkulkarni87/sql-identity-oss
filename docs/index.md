# SQL Identity Resolution (`sql-identity-oss`)

<p align="center">
  <strong>Warehouse-Native, Deterministic Identity Resolution & Customer 360 for Modern Data Platforms</strong>
</p>

<p align="center">
  <a href="https://github.com/anilkulkarni87/sql-identity-oss/actions/workflows/test.yml"><img src="https://github.com/anilkulkarni87/sql-identity-oss/actions/workflows/test.yml/badge.svg" alt="CI Tests"></a>
  <a href="https://pypi.org/project/sql-identity-resolution/"><img src="https://img.shields.io/pypi/v/sql-identity-resolution.svg" alt="PyPI"></a>
  <a href="https://github.com/anilkulkarni87/sql-identity-oss/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License"></a>
  <a href="https://anilkulkarni87.github.io/sql-identity-oss/"><img src="https://img.shields.io/badge/docs-GitHub_Pages-indigo.svg" alt="Documentation"></a>
</p>

---

## 🚀 Why SQL Identity Resolution?

Unifying fragmented customer records across CRM, point-of-sale, e-commerce, mobile applications, and support tickets has historically required either **costly SaaS identity vendors** (sending sensitive PII outside your cloud boundary) or **opaque black-box ML tools**.

**`sql-identity-oss`** resolves identity **entirely inside your data warehouse**:

* **Zero Data Egress**: All data stays inside your security perimeter (DuckDB, Snowflake, BigQuery, Databricks). Fully compliant with HIPAA, GDPR, and CCPA.
* **Deterministic & Fully Auditable**: Every edge, link, and cluster decision is documented with confidence scores, rule IDs, and full lineage. No probabilistic hallucinations.
* **Anchor-Based Edge Building ($O(N)$ vs $O(N^2)$)**: Avoids Cartesian blowouts on common identifiers by connecting groups via star-topology anchor edges.
* **Pure SQL Connected Components**: Distributed Label Propagation executes natively in SQL without requiring graph database infrastructure (Neo4j, AWS Neptune).
* **Frictionless Zero-Retype Setup**: Auto-detects existing local credentials from `~/.databrickscfg`, `~/.snowflake/config.toml`, `~/.dbt/profiles.yml`, and Google Cloud ADC.
* **AI-Ready with Model Context Protocol (MCP)**: Native FastMCP server exposing 17 tools for AI agents (Cursor, Claude Desktop, Antigravity) to inspect, configure, autonomously profile schemas, simulate rule risks, and resolve Customer 360 contexts.
* **Rich Developer Ecosystem**: First-class Python CLI (`idr`), FastAPI REST service, Prometheus metrics, interactive React Web UI, and native dbt package (`dbt_idr`).

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

This single command:

1. Synthesizes 10,000 multi-source customer records across Web, Store, Mobile, and Call Center.
2. Injects cross-channel identifier overlaps (Email, Phone, Loyalty ID).
3. Provisions schemas (`idr_meta`, `idr_work`, `idr_out`) and applies matching rules.
4. Executes the 6-stage identity resolution pipeline.
5. Materializes cluster memberships and golden customer profiles with survivorship logic.

---

## 🏗️ Supported Warehouses & Execution Matrix

| Warehouse | Connection Mode | Incremental Merge | Distributed Scale | Production Ready |
| :--- | :--- | :--- | :--- | :--- |
| **DuckDB** | Local file / In-memory | `INSERT INTO` | Single-node (up to 50M rows) | Yes (Dev / Edge) |
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

