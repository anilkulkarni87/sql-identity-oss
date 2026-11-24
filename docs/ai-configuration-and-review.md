# PII-safe AI configuration and review

AI assistance produces a first draft and explains measured review evidence. It does
not save a proposal or execute a warehouse preview automatically.

## Workflow

1. In **Sources & Mappings**, choose individual, household, or account grain; event
   handling; and an optional system of record. Expand **Matching permissions and
   locked decisions** to approve identifier types, exclude tables, or lock fields.
2. Select **AI Suggest Config**. Inspect the proposal, errors, assumptions, and
   current/proposed diff. **Apply proposal to draft** changes only the draft.
3. Start a **Dry Run** in **Run & Verify**. FULL is the default; INCR reports apply
   only to their evaluated population. Select a named lens explicitly to review it.
4. Inspect graph diagnostics and optional labeled evaluation. Compare a selected
   rule or lens change explicitly; each comparison runs against the same retained
   extraction as its baseline, rather than reading changing source tables again.
5. Apply a proposed change to the draft, then run a fresh preview. Old findings and
   patches become stale when the draft or selected view changes.
6. Save and execute the real pipeline explicitly when satisfied.

Dry Run and loading a saved preview collect or retrieve local evidence without
invoking the model or opening the AI review panel. Select **Analyze & Refine with
AI** explicitly to request recommendations. Existing manual configurations do not
inherit an AI matching policy; a saved explicit policy is still enforced.

**Suggest Rules** uses the current mappings, identifiers, aliases, expressions,
exclusions, and survivorship. It cannot substitute a newly inferred data model.
Changing source policy in rules-only mode may require a full configuration proposal.

## Policy and first-draft behavior

Configurations may contain an optional, versioned `ai_assistance` block:

```yaml
ai_assistance:
  version: 1
  grain: individual
  event_policy: bridge
  excluded_tables: [raw.clickstream]
  system_of_record: raw.crm
  allowed_matching_identifiers: [email, customer_id]
  locked_fields: [/sources/0/entity_key]
  fuzzy: false
  objectives: [precision, cross_source]
  requested_lenses: []
```

The generator also records inferred table roles in this block. Policy and locks
survive YAML export, saving, reopening, history, and rollback. Existing configs
without this block remain valid. API generation requires explicit policy or saved
context; UI controls and CLI defaults provide it for new drafts.

Explicit policy wins over model output. Excluded sources are not added back.
Mapped identifiers do not automatically become matching rules. Individual matching
does not automatically use shared phones, addresses, devices, or account keys;
approve such identifiers explicitly when appropriate. Account keys are kept distinct
from person identifiers. Bridge event tables contribute links and omit profile
attributes by default. In Survivorship, explicitly approve individual Sum, Min or
Max contributions; each approval is bound to the source, attribute and strategy.
This does not deduplicate business transactions or establish currency compatibility.

An approved aggregate is stored as, for example:

```yaml
approved_event_aggregates:
  main.orders_online:
    total_amount: AGG_SUM
```

This block belongs under `ai_assistance`. Switching the strategy requires renewed
approval. Excluded sources remain excluded, and other bridge attributes remain
blocked. Event handling **Include fully** explicitly allows general profile
attributes from event sources.

Intentional group limits are preserved.

Provider failure uses the same profile, assembler, and validation path to produce a
conservative deterministic fallback. Empty or unusable plans are invalid. Plans are
marked `ready_for_preview`, `needs_decisions`, or `invalid`; confidence labels are
descriptive, not calibrated probabilities. Invalid proposals cannot be applied.

Local objective hints recognize precision, coverage, cross-source matching, and
auditability. Other free text stays local. Use structured controls for matching
grain, event policy, approved identifiers, exclusions, and survivorship authority.

## Model boundary

Generation, repair, and review use explicit serializers. Model requests contain
sanitized schema names, declared types, constraints, counts, rates, aggregate
overlap, structured policy, and aggregate graph/evaluation metrics.

Raw records, identifier values, value hashes, identity/cluster IDs, custom SQL and
literals, value-specific exclusions, catalog prose, secrets, and warehouse exception
text are not included. Sensitive schema names are represented by local references.
Source profiling, exclusion discovery, evaluation labels, and retained comparison
inputs stay within the configured execution environment. These restrictions apply
to local and remote models. Existing masking remains an additional safeguard.

This contract is specific to configuration generation and review; other Copilot
features retain their existing privacy settings and permissions.

## Preview and review semantics

Each preview has isolated scratch schemas, a run ID, a versioned draft fingerprint,
and a selected graph view. Active metadata, source watermarks, saved configuration
history, and production membership/profile outputs are not replaced by previews.
Baseline output copies and record-level working data remain inside the warehouse.
Warehouse permissions must allow scratch schema creation and deletion.

Reports are owner/connection-bound and stored in `idr_out.ai_review_report`. Later
reviews use the saved aggregate evidence, not the latest shared work tables. Report
retrieval does not require re-running the graph. Load a saved preview by its run ID
in **Run & Verify**; applying or comparing changes still requires a matching draft.

Statuses are `no_detected_risks`, `needs_review`, `high_risk`, and
`insufficient_evidence`. A high singleton rate is not automatically a defect.
Large clusters and edge counts are diagnostic signals, not proof of identity error.
Rule edge counts are observational; the explicit comparison measures graph effects.

Review recommendations name the affected rule, show the aggregate evidence, and
state the expected effect and tradeoff. A skipped group just above a rule's cap can
offer a bounded limit comparison; extreme fanout, shared rules, locked policy, or a
lower global cap keep the guardrail in place and require local inspection. Such
trials are unmeasured until explicitly compared. AI may prioritize existing
recommendations, but cannot replace the reported numbers or invent executable
changes. Singleton percentages distinguish clusters from entities.

Supported comparison changes are exact-rule removal, group-limit adjustment,
stricter fuzzy thresholds, and lens included types. Normalization/source changes
require a new preview. Rules sharing an identifier type also require a new preview
because retained normalized evidence cannot reliably separate their contributions.
Partition comparisons count records in split or merged components independently of
arbitrary resolved-ID naming.

Named-lens previews use that lens's deterministic graph filters and cap. Primary
fuzzy refinement is reported separately. Lens cluster deltas use the saved lens
baseline; saved per-lens edge baselines are unavailable and are not invented.

An optional warehouse truth table with `entity_key` and `truth_id` columns enables
precision/recall evaluation. Labels are frozen locally with the preview. Only
aggregate metrics reach AI; absent labels do not imply measured accuracy.

Reports default to 30 days and comparison inputs to 24 hours. Configure
`IDR_AI_REPORT_DAYS` and `IDR_AI_INPUT_HOURS` to change retention. Bounded cleanup
runs when a preview starts; there is no scheduler. Use the explicit purge command
to remove retained workspaces and reports without waiting for another preview.

## CLI and API

```bash
idr ai generate-config --platform duckdb --db demo.duckdb \
  --tables raw.crm,raw.orders --grain individual --event-policy bridge \
  --allow-match email,customer_id --output candidate.yaml

idr ai generate-config --platform duckdb --db demo.duckdb \
  --tables raw.crm,raw.orders --rules-only --current-config candidate.yaml \
  --output candidate-rules.yaml

idr run --platform duckdb --db demo.duckdb --mode FULL \
  --config candidate.yaml --dry-run --critique

idr ai review --platform duckdb --db demo.duckdb --run-id dry_run_EXAMPLE
idr ai compare --platform duckdb --db demo.duckdb --run-id dry_run_EXAMPLE \
  --config candidate.yaml --action set_group_limit --target exact_email --value 10
idr ai purge-reviews --platform duckdb --db demo.duckdb
```

Use `--allow-fuzzy` to opt into generated fuzzy rules, and
`idr run --dry-run --ground-truth-table raw.labels` for optional labeled evaluation.
CLI invalid generation exits nonzero without writing a usable configuration;
`--reasoning-out` can still write the local validation report. AI assistance does
not introduce an automatic apply or production-run command.

- Existing `POST /api/ai/suggest-config` and `/suggest-config/stream` accept `mode`,
  `current_config`, structured `context`, and optional `base_draft_hash`.
- Generation retains `suggested_config`, explanations and validation fields, and
  adds proposal IDs, status, fingerprints, decisions and a local-only diff.
- `POST /api/setup/run` with `dry_run=true` returns an isolated preview, aggregate
  `review`, and `draft_hash`; optional `ground_truth` accepts evaluator column options.
- `POST /api/ai/critique-dry-run` requires `config`, `run_id`, `draft_hash`, and `lens`.
  Legacy unbound critique requests are rejected; stale evidence returns HTTP 409.
  Python critique helpers likewise require an isolated preview `run_id`.
- `POST /api/ai/compare` and `/compare/stream` (SSE) accept `run_id`, `config`, `patch`, and `lens`.
- `POST /api/ai/patch` validates a fingerprint-bound patch and returns an updated
  draft with `review_stale=true`. It does not save warehouse configuration.
- `GET /api/ai/reviews/{run_id}` retrieves owned aggregate evidence;
  `POST /api/ai/reviews/purge` removes owned retained reports/inputs.

Normal schema initialization adds the report table and optional
`identifier_mapping.source_is_hashed` column. Use explicit column lists in direct
SQL inserts. `source_is_hashed` preserves source digests without hashing them again;
the existing `is_hashed` flag continues to request hashing raw source values.

Legacy fuzzy template names remain accepted; generated name templates use portable
prefix blocking with dialect-specific paired similarity expressions, without a
phonetic UDF requirement.

See [output previews and repeatable acceptance checks](output-previews-and-evaluation.md) for source deduplication in profile aggregation, explicit data-preview budgets, grouped state output semantics and configurable label acceptance.
