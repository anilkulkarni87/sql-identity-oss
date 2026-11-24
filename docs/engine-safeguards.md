# Engine safeguards

These checks apply to engine runs, including runs initiated through the UI, CLI,
and isolated AI previews. They do not enable identifiers or change matching rules.

## Complete graphs before publication

The engine verifies that every active edge has two labeled endpoints in the same
component. An incomplete partition raises `GRAPH_NOT_CONVERGED` and fails the run
before current outputs or source watermarks are written. Increase `max_iters` and
retry. A valid partition reached on the last allowed iteration is accepted; an
extra iteration solely to observe zero changes is not required.

This applies to deterministic matching, fuzzy refinement, and named lenses.
All selected lenses are prepared and validated in work tables before primary
output publication. This is a convergence safeguard, not a new cross-platform
transaction guarantee for arbitrary warehouse failures during output writes.

## Incremental configuration baseline

A successful run records a versioned matching fingerprint covering sources,
identifier mappings, exact/fuzzy rules, profile mappings and survivorship, plus
`strict` and the global group limit. An INCR request with a different fingerprint
fails with `FULL_REBUILD_REQUIRED` before extraction and publication. Failed runs
and dry runs cannot establish a new successful production baseline.

Run one successful FULL rebuild after upgrading an existing database whose history
has no matching fingerprint. New empty databases can still start with INCR.
Iteration limits and blast-radius thresholds are retry controls and do not force
a rebuild. Exclusions and identity overrides retain their existing retroactive
incremental processing path rather than being included in this fingerprint.

## Reproducible survivorship

RECENCY, EARLIEST, PRIORITY, PRIORITY_RECENCY and LATEST_FROM_SOURCE resolve ties
using source ID and source record key, then the scalar value for repeated keys.
The source watermark supplies the default timestamp; an explicit `recency_field`
overrides it and must exist on sources supplying the attribute. The legacy
`record_updated_at` alias uses the source watermark when no physical column has
that name. FREQUENCY uses
exact counts and a stable value ordering when counts tie.

Attributes remain independent by default. To keep related fields from the same
winning source row, give them the same optional `row_group`:

```yaml
survivorship:
  - attribute: city
    strategy: RECENCY
    row_group: postal_address
  - attribute: state
    strategy: RECENCY
    row_group: postal_address
```

Group members must share a row-selection strategy, source priority and recency
field. Null fields on the winning row remain null; another row cannot fill them
independently. A group does not preserve multiple consent purposes, addresses or
transactions: those still need an appropriate data model.

A source column can be both an identifier and a profile attribute. In Sources &
Mappings, select both **Identifier** and **Attribute**, then configure the attribute
in Survivorship. Matching normalization and hashing settings do not transform the
separate profile attribute expression. Identifiers alone do not become golden
profile fields automatically.

## Metrics and compatibility

- `clusters_resolved` is the number of deterministic clusters in the evaluated
  graph. `clusters_impacted` remains its legacy API alias.
- `clusters_changed` counts distinct primary cluster IDs affected by record
  additions/removals or changes in resolved/supercluster membership. Both retired
  and new IDs count. It is not a count of profiles with changed attribute values.
- `source_tables_processed` records the verified active-source count.
- Run completion populates `ended_at`. Fractional run and stage durations are
  stored in additive `duration_seconds_precise` columns. The run-history API
  returns these as `duration_seconds`, falling back to legacy integer durations
  for older records. Existing integer columns remain compatible with old clients.

New columns are added during schema initialization/preflight. Historical timestamps,
source counts, precise durations and changed-cluster counts are not reconstructed.

See [output previews and repeatable acceptance checks](output-previews-and-evaluation.md) for source deduplication in profile aggregation, explicit data-preview budgets, grouped state output semantics and configurable label acceptance.
