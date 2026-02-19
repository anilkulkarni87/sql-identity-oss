# Running Identity Resolution

There are three main ways to run the SQL Identity Resolution pipeline. Please refer to the specific guides below for detailed instructions.

## 1. CLI (Recommended for Production)

The main way to orchestrate runs in production (Airflow, Cron, etc.).

*   **[User Guide: CLI Mode](user_guide_cli.md)**

## 2. UI (Interactive & Setup)

A web-based interface for initial setup, monitoring, and ad-hoc runs.

*   **[User Guide: UI Mode](user_guide_ui.md)**

## 3. DBT (Analytics Engineering)

Native integration for dbt workflows using the `dbt_idr` package.

*   **[User Guide: DBT Mode](user_guide_dbt.md)**

---

## Which Mode Should I Use?

| Feature | CLI | UI | DBT |
|---------|-----|----|-----|
| **Fuzzy Matching** | ✅ | ✅ | ❌ (Deterministic Only) |
| **Orchestration** | ✅ (Airflow/Bash) | ❌ (Manual/API) | ✅ (dbt Cloud/Airflow) |
| **Interactive Config** | ❌ (YAML only) | ✅ (Wizard) | ❌ (Seeds) |
| **Best For** | Production Pipelines | Initial Setup & Demo | dbt-centric Teams |
