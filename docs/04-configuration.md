# Configuration Guide

Configuration is managed via YAML files and the `idr config` command.
**Source of Truth**: `idr_core/config.py` definitions.

## Structure

```yaml
sources:
  - id: "users"
    table: "raw.users"
    entity_key: "user_id"
    entity_type: "PERSON"
    watermark_column: "updated_at"
    identifiers:
      - type: "email"
        expr: "LOWER(email)"
    attributes:
      - name: "first_name"
        expr: "first_name"

rules:
  - id: "email_exact"
    type: "EXACT"
    match_keys: ["email"]
    priority: 10

fuzzy_rules:
  - id: "name_fuzzy"
    blocking_key: "metaphone(last_name)"
    score_expr: "jaro_winkler(a.name, b.name)"
    threshold: 0.9

survivorship:
  - attribute: "first_name"
    strategy: "RECENCY"
    recency_field: "updated_at"
```

## Sections

### `sources` (Required)
Defines input tables.
*   `id`: Unique identifier for the source (used in lineage).
*   `table`: Fully qualified table name (`schema.table`).
*   `entity_key`: Unique primary key of the source entity.
*   `identifiers`: List of identity columns to extract.
*   `attributes`: List of profile attributes to extract.

### `rules`
Deterministic matching rules.
*   `type`: Currently only `EXACT` is supported.
*   `match_keys`: List of identifier types to match on (e.g., `["email"]`).
*   `priority`: Lower number = higher priority edge.

### `survivorship`
Golden profile resolution logic.
*   `strategy`:
    *   `RECENCY`: Latest value wins.
    *   `PRIORITY`: Source with highest trust rank wins.
    *   `FREQUENCY`: Most common value wins.
    *   `AGG_MAX`, `AGG_SUM`: Aggregation functions.

### `fuzzy_rules`
Probabilistic matching (requires fuzzy mode).
*   `blocking_key`: SQL expression to limit comparison scope.
*   `score_expr`: SQL expression returning float 0.0-1.0.
*   `threshold`: Cutoff score for a match.
