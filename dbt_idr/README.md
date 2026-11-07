# IDR dbt Package

This dbt package implements the core identity resolution logic in SQL, allowing you to run the IDR pipeline directly within your dbt project.

## Feature Parity

| Feature | Core Runner (Python) | dbt Package | Notes |
|---------|----------------------|-------------|-------|
| Identity Resolution | ✅ | ✅ | Recursive CTEs used for all platforms. |
| Fuzzy Matching | ✅ | ❌ | dbt package currently supports deterministic resolution only. |
| Config Snapshots | ✅ | ❌ | |
| Edge Evidence | ✅ | ❌ | |
| Golden Profiles | ✅ | ✅ | Full parity: `RECENCY`, `PRIORITY`, and `FREQUENCY` (mode with recency tie-breaker). |
| Confidence Scoring | ✅ | ✅ | Grounded Spanning Tree Connectivity & Identifier Fidelity scoring. |
| Strict Mode | ✅ | N/A | dbt package is effectively "strict mode" (deterministic only). |

## Usage

### 1. Label Propagation
The `int_labels` model uses an iterative approach (Recursive CTE) to resolve connected components.
- **BigQuery**: Uses Recursive CTEs (same as other platforms).
- **Configuration**: Set `idr_max_lp_iterations` in `dbt_project.yml` (default: 30).

### 2. Golden Profiles
The `golden_profiles` model dynamically builds unified profiles based on `idr_survivorship_rules`.
- Supported Strategies:
    - **RECENCY**: Picks the most recently updated value (`record_updated_at`).
    - **PRIORITY**: Picks based on source priority list (e.g., `["crm", "web"]`). Values from `crm` win over `web`.
    - **FREQUENCY**: Calculates the statistical mode (most frequent non-null value across the cluster), with ties broken by recency.

### 3. Identity Clusters & Confidence Scoring
The `identity_clusters` mart calculates explainable, deterministic confidence scores for every resolved cluster:
- **Spanning Tree Connectivity**: Evaluates cluster graph density via $\min(1.0, \frac{E}{V-1})$ instead of unrealistic clique density.
- **Identifier Fidelity**: Weights matches by identifier reliability (SSN: 1.00, Account/Loyalty: 0.98, Email: 0.96, Phone: 0.93, Device/Session: 0.85).
- **Multi-Identifier Corroboration**: Bonuses for clusters corroborated across 2 (+0.02) or 3+ (+0.04) distinct identifier types.
- **Runaway Over-Merge Protection**: Penalizes single-identifier megaclusters ($V > 250$).

## Platform Specifics

- **BigQuery**: Ensure your dataset supports Recursive CTEs (standard in modern BQ).
- **Schema Naming**: This package uses `idr_sources`, `idr_rules` etc. as seeds/sources, mapping roughly to `idr_meta` in the core runner.
