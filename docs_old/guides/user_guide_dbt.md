# User Guide: dbt Mode

This guide explains how to integrate IDR into your **dbt** (data build tool) workflow. This mode is best for analytics teams who want to version-control identity logic alongside their transformation layer.

> **Note**: The dbt package currently supports **Deterministic (Exact Match)** resolution only. For fuzzy matching, use the CLI or UI modes.

## 1. Installation

Add the package to your `packages.yml`:

```yaml
packages:
  - git: "https://github.com/anilkulkarni87/sql-identity-resolution"
    subdirectory: "dbt_idr"
    revision: main
```

Run dbt deps:
```bash
dbt deps
```

## 2. Initialization & Configuration (Seeds)

In dbt mode, you drive configuration via **dbt Seeds** (CSVs). You do not need to run `idr init`.

1.  Create CSV files in your `seeds/` directory matching the IDR schema:
    *   `idr_sources.csv`: Define source tables.
    *   `idr_rules.csv`: Define matching rules.
    *   `idr_identifier_mappings.csv`: Map columns.

**Example `idr_sources.csv`:**
```csv
source_id,source_name,database,schema,table_name,entity_key_column,watermark_column,is_active
crm,CRM System,ANALYTICS,CRM,customers,cust_id,updated_at,true
```

2.  Run seeds to load config:
```bash
dbt seed --select dbt_idr
```

## 3. Execution

Run the dbt models to execute identity resolution.

```bash
dbt run --select dbt_idr
```

**How it works**:
1.  dbt reads the configuration seeds.
2.  It uses Jinja macros to generate complex SQL for `int_edges`, `int_labels` (Label Propagation), and `identity_clusters`.
3.  It materializes the results as tables in your warehouse.

## 4. Verification

Use dbt tests to verify data integrity:

```bash
dbt test --select dbt_idr
```

This checks for uniqueness, referential integrity, and disconnected subgraphs.

## 5. Differences from CLI/UI

| Feature | CLI / UI (Python Native) | dbt Mode (SQL Only) |
| :--- | :--- | :--- |
| **Logic Engine** | Python + SQL | SQL (Jinja Macros) |
| **Fuzzy Matching** | ✅ Yes (Blocking + Scoring) | ❌ No (Exact Only) |
| **Configuration** | YAML / UI Wizard | CSV Seeds |
| **Orchestrator** | Airflow / Bash | Airflow / dbt Cloud |
| **State Management** | `idr_meta` Tables | `seed` Tables |

## 7. Advanced Configuration (Project Variables)

Override defaults in your `dbt_project.yml`:

```yaml
vars:
  idr_dry_run: false
  idr_max_lp_iterations: 30
  idr_large_cluster_threshold: 5000
  idr_default_max_group_size: 10000
```

## 8. Output Models & Scoring

Each resolved cluster includes quality metrics:

| Metric | Description |
|--------|-------------|
| `edge_diversity` | Number of distinct identifier types |
| `match_density` | Ratio of actual to possible edges |
| `confidence_score` | Weighted score (0.0-1.0) |

## 9. Comparison: dbt vs Native CLI

| Feature | Native CLI | dbt Package |
|---------|------------|-------------|
| **Execution** | `idr run` | `dbt run` |
| **Config** | YAML / DB Tables | CSV Seeds |
| **Deterministic** | ✅ | ✅ |
| **Fuzzy Matching** | ✅ (Blocking + Scoring) | ❌ (Exact Only) |
| **Cross-Platform** | ✅ | ✅ |
