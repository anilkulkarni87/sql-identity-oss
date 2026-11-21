# Output previews and repeatable acceptance checks

## Cost-aware profile previews

Dry Run builds graph diagnostics and validates profile definitions using metadata. It does **not** automatically build golden profiles or invoke AI review. In Run & Verify, expand **Golden profile validation and optional preview** for separate actions:

1. Validate profile configuration without scanning source records.
2. Select individual profile fields or grouped outputs and preview complete clusters, with explicit cluster/member limits and a timeout.
3. Choose the full evaluated population only with separate confirmation and an adequate member budget.

Data previews enforce query timeouts natively across all four supported warehouses: **DuckDB** (`conn.interrupt()`), **BigQuery** (per-job `result(timeout=...)` + `job.cancel()`, plus optional `max_bytes_billed` scan-cost enforcement via `QueryJobConfig`), **Snowflake** (DBAPI statement `timeout` / Snowpark session `STATEMENT_TIMEOUT_IN_SECONDS`), and **Databricks** (SQL cursor `cancel()` / Spark `cancelJobGroup()`). A member limit limits result scope, not source scan size: a selected cluster can still require scanning a large source unless `max_bytes_billed` is set on BigQuery. Unknown scan cost requires explicit acknowledgement. This is not a 50M-row performance guarantee.

Targeted previews select deterministic complete clusters and omit clusters exceeding the member budget; they do not sample records within a cluster. Their sums are exact for selected profiles, not population estimates. Comparisons use identical saved resolved IDs only. Graph membership is frozen in the saved preview, while source field values are read at preview time. No raw profile values are sent to AI. Published output tables remain untouched.

```sh
# Metadata only; no model call or graph run
idr ai profile-preview --platform duckdb --db warehouse.duckdb --config config.yaml

# Explicit bounded data preview against a previously saved graph preview
idr ai profile-preview --platform duckdb --db warehouse.duckdb --config config.yaml \
  --run-id dry_run_EXAMPLE --preview-mode targeted --attributes total_amount \
  --max-clusters 25 --max-members 10000 --timeout-seconds 15 --allow-source-scan
```

Profile aggregation uses the same configured source deduplication as entity extraction, in both FULL and INCR runs. Provide a total `dedup_order_by` ordering when duplicate records can tie; ambiguous ordering cannot establish which source row is authoritative. SUM still requires a business-grain decision: summing an order header plus its line items or summing account balance snapshots is not safe merely because the column is numeric. Currency and overlapping sources require review. Existing outputs need a FULL rebuild to incorporate changed extraction/output semantics.

## Generic grouped current state

A scalar golden field cannot represent several independent states per subject. Configure a grouped output when state is scoped by dimensions such as category, channel, region or purpose. The engine assumes no domain-specific statuses. Each winning row supplies its fields together, retaining provenance.

```yaml
grouped_outputs:
  - id: current_preferences
    dimensions: [category, channel]
    fields: [state]
    strategy: RECENCY
    conflict_policy: error
    null_dimensions: error
    sources:
      - source_id: preferences # must exist in sources
        dimensions:
          category: preference_category
          channel: contact_channel
        fields:
          state: preference_state
        recency_field: effective_at
```

Configure this through **Grouped state outputs (advanced)** in Survivorship or YAML. The example publishes `idr_out.grouped_current_preferences`, keyed by resolved identity plus category/channel. `PRIORITY_RECENCY` additionally accepts `source_priority`, ordered from highest authority. Source timestamp columns must be present in the mapped source.

Equally authoritative rows with different states are conflicts, even though a record key could break a SQL ordering tie. Default `error` stops publication. Explicit `mark` emits NULL state fields and a conflict flag, with no arbitrary winning business state. Null dimensions are rejected unless `null_dimensions: keep` is explicit. Outputs include `_idr_source_id`, `_idr_source_key`, `_idr_effective_at`, `_idr_conflict`, `run_id` and `updated_at`.

FULL rebuilds publish the complete output. INCR recomputes complete state for affected subjects; publication failures restore profiles, crosswalk and grouped outputs alongside graph membership. Changing grouped definitions requires FULL. Removing a definition stops refreshing its existing table; it does not destructively delete that table. These mechanics select state; domain policies about eligibility, expiry, withdrawal, or permitted use must be explicitly modeled and reviewed.

## Reviewed labels across different schemas

Evaluation consumes reviewed identity groups. Configure the label table/columns once; source mappings translate local record keys using the same configured engine key construction, including composite/AUTO keys. Labels remain in the warehouse. No specific table or customer column is assumed.

```json
{
  "ground_truth_table": "quality.reviewed_records",
  "truth_id_col": "reviewed_identity",
  "source_mappings": [
    {
      "source_id": "accounts",
      "key_columns": {"account_id": "reviewed_record_key"},
      "source_column": "record_kind",
      "source_value": "account"
    }
  ],
  "max_label_rows": 100000,
  "min_evaluated_entities": 100,
  "min_label_overlap": 0.95,
  "min_true_pairs": 20,
  "min_precision": 0.99,
  "min_recall": 0.90
}
```

`key_columns` maps **source column -> label column**. For composite keys, map every input column used by the configured key expression. Multiple mappings can use different `table` and `truth_id_col` overrides. The optional discriminator is paired `source_column`/`source_value`. If labels already contain engine entity keys (including their source prefix), omit mappings and configure `entity_key_col` instead.

Store this object under `evaluation` in application config, use the optional labeled-evaluation editor in Run & Verify, or keep it in a separate JSON file for CI:

```sh
# Evaluate already published memberships; no graph/profile rebuild or AI call
idr evaluate --platform duckdb --db warehouse.duckdb --evaluation-config acceptance.json

# Evaluate a new isolated graph preview
idr run --platform duckdb --db warehouse.duckdb --dry-run --evaluation-config acceptance.json
```

The standalone `evaluate` command exits 0 only for passed acceptance, otherwise 1. Evaluation in Dry Run is reported separately from successful graph execution and does not automatically gate production publication. CI should use the standalone acceptance command against the intended result or inspect preview acceptance explicitly.

Duplicate/conflicting label keys are rejected; oversized label cohorts are rejected rather than silently truncated. Metrics with no denominator are N/A. Minimum overlap, record count and same-identity-pair checks report `insufficient_evidence`; measured thresholds below target report `failed`. Limits bound label materialization, not all underlying source scans.

Results describe the labeled population only. Maintain a versioned, representative reviewed cohort with both same-identity and different-identity examples; use a held-out cohort for acceptance. Unlabeled data can support structural checks but cannot prove identity precision/recall. This makes the acceptance mechanism reusable across domains without pretending every dataset has ground truth.
