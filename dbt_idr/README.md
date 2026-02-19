# IDR dbt Package

This dbt package implements the core identity resolution logic in SQL, allowing you to run the IDR pipeline directly within your dbt project.

## Feature Parity

| Feature | Core Runner (Python) | dbt Package | Notes |
|---------|----------------------|-------------|-------|
| Identity Resolution | ✅ | ✅ | Recursive CTEs used for all platforms. |
| Fuzzy Matching | ✅ | ❌ | dbt package currently supports deterministic resolution only. |
| Config Snapshots | ✅ | ❌ | |
| Edge Evidence | ✅ | ❌ | |
| Golden Profiles | ✅ | ⚠️ | Supports `RECENCY` and `PRIORITY` (via simple CSV). `FREQUENCY` not supported. |
| Strict Mode | ✅ | N/A | dbt package is effectively "strict mode" (deterministic only). |

## Usage

### 1. Label Propagation
The `int_labels` model uses an iterative approach (Recursive CTE) to resolve connected components.
- **BigQuery**: Now uses Recursive CTEs (same as other platforms).
- **Configuration**: Set `idr_max_lp_iterations` in `dbt_project.yml` (default: 30).

### 2. Golden Profiles
The `golden_profiles` model builds unified profiles based on `idr_survivorship_rules`.
- Supported Strategies:
    - **RECENCY**: Picks the most recently updated value (`record_updated_at`).
    - **PRIORITY**: Picks based on source priority list (e.g., `["crm", "web"]`). values from `crm` win over `web`.
- **Note**: `FREQUENCY` strategy is currently not supported in dbt.

## Platform Specifics

- **BigQuery**: Ensure your dataset supports Recursive CTEs (standard in modern BQ).
- **Schema Naming**: This package uses `idr_sources`, `idr_rules` etc. as seeds/sources, mapping roughly to `idr_meta` in the core runner.
