# IDR dbt Package (`dbt_idr`)

`dbt_idr` is the **lightweight, batch-oriented dbt package** for SQL Identity Resolution. It runs deterministic anchor-star identity stitching, stewardship overrides (`FORCE_LINK`, `DO_NOT_LINK`, `ISOLATE`), cluster confidence scoring, and golden profile survivorship directly inside a dbt project.

> **Batch Starter vs. Full Engine**: Use `dbt_idr` for full-refresh / batch dbt DAGs. For **incremental cluster splits** (retroactive governance seeding, un-watermarked `NOT EXISTS` diffing, `reextracted_entities` invalidation), **cross-run `resolved_id` stabilization & crosswalk**, **pre-write blast-radius gates**, and **advisory run locks**, use the core Python runner (`idr run` via `idr_core`)—which shares the exact same metadata model (`idr dbt export`).

## Feature Parity (`idr_core` vs. `dbt_idr`)

| Feature | Core Runner (`idr_core`) | dbt Package (`dbt_idr`) | Notes |
|---------|--------------------------|-------------------------|-------|
| Anchor-Star $N-1$ Edges & `max_group_size` | ✅ | ✅ | Bounded star reduction + `skipped_identifier_groups` telemetry. |
| Stewardship Overrides (`FORCE_LINK`, `DO_NOT_LINK`, `ISOLATE`) | ✅ (+ multi-hop bridge repair) | ✅ (edge-level) | `dbt_idr` detaches `max(a, b)` from shared anchor groups in `int_edges.sql`. |
| Golden Profiles (`RECENCY`, `PRIORITY`, `FREQUENCY`) | ✅ | ✅ | Full parity in `golden_profiles.sql`. |
| Confidence Scoring & Dry-Run Summary | ✅ | ✅ | `identity_clusters.sql`, `dry_run_results.sql`, `dry_run_summary.sql`. |
| Incremental Cluster Splits & Governance Seeding | ✅ | ❌ | `idr_core` only (`gov_affected_entities`, `reextracted_entities`). |
| Cross-Run `resolved_id` Stabilization & Crosswalk | ✅ | ❌ | `idr_core` only (dual-window `ROW_NUMBER()` + collision-checked fallback). |
| Pre-Write Blast-Radius Gate & Compensating Recovery | ✅ | ❌ | `idr_core` only (`_backup_*` snapshots + advisory run lock). |
| Fuzzy Matching, Config Snapshots & Edge Evidence | ✅ | ❌ | `dbt_idr` is deterministic-only. |

## How `dbt_idr` Differs from Other dbt Identity Packages

Unlike ID-stitching-only dbt packages (such as `rudderlabs/id_stitching` or `tasman_identity_graph`), `dbt_idr` provides:
- **Supernode protection out of the box**: `max_group_size` bounds anchor-star edge generation and logs skipped poison-pill groups (`skipped_identifier_groups.sql`).
- **Stewardship overrides in SQL**: `FORCE_LINK`, `DO_NOT_LINK` (with higher-key shared-anchor detachment), and `ISOLATE` are first-class models.
- **Survivorship & explainability**: Built-in `golden_profiles.sql`, `identity_clusters.sql` confidence metrics, and `rule_match_audit.sql`—all sharing a metadata contract with `idr_core` so you can graduate to `idr run` without redesigning your rules.

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
