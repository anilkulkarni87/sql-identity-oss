# Schema Reference

This reference documents the database tables managed by the `idr` CLI.
**Source of Truth**: `sql/ddl/*.sql` files are the definitive source.

## idr_meta (Configuration)

Tables in this schema control the behavior of the identity resolution pipeline.

### source_table
Registry of source tables to process.

| Column | Type | Description |
|--------|------|-------------|
| `table_id` | STRING (PK) | Unique identifier for source table |
| `table_fqn` | STRING | Fully qualified name (schema.table) |
| `entity_type` | STRING | Entity type (e.g., PERSON) |
| `entity_key_expr` | STRING | SQL to generate unique entity key |
| `watermark_column` | STRING | Column for incremental processing |
| `watermark_lookback_minutes` | INT | Buffer time for late arriving data |
| `is_active` | BOOL | Whether to include this source |

### rule
Deterministic matching rules.

| Column | Type | Description |
|--------|------|-------------|
| `rule_id` | STRING (PK) | Unique rule identifier |
| `rule_name` | STRING | Descriptive name |
| `identifier_type` | STRING | Type of identifier to match |
| `canonicalize` | STRING | Normalization (LOWERCASE, EXACT) |
| `allow_hashed` | BOOL | Allow pre-hashed values |
| `require_non_null` | BOOL | Skip if NULL |
| `max_group_size` | INT | Max entities per group (hub breaking) |
| `priority` | INT | Processing priority |
| `is_active` | BOOL | Enabled status |

### fuzzy_rule
Probabilistic matching rules (used when `--strict` is not set).

| Column | Type | Description |
|--------|------|-------------|
| `rule_id` | STRING (PK) | Unique rule identifier |
| `rule_name` | STRING | Descriptive name |
| `blocking_key_expr` | STRING | Pre-filter expression |
| `score_expr` | STRING | Similarity function call |
| `threshold` | DOUBLE | Match threshold (0.0-1.0) |
| `priority` | INT | Processing priority |
| `is_active` | BOOL | Enabled status |

### identifier_mapping
Maps source columns to standardized identifier types.

| Column | Type | Description |
|--------|------|-------------|
| `table_id` | STRING (PK) | FK to source_table |
| `identifier_type` | STRING (PK) | Identifier type (email, phone) |
| `identifier_value_expr` | STRING | SQL expression to extract value |
| `is_hashed` | BOOL | If value is already hashed |

### entity_attribute_mapping
Maps source columns to golden profile attributes.

| Column | Type | Description |
|--------|------|-------------|
| `table_id` | STRING (PK) | FK to source_table |
| `attribute_name` | STRING (PK) | Normalized attribute name |
| `attribute_expr` | STRING | SQL expression for value |

### survivorship_rule
Rules for resolving Golden Profile conflicts.

| Column | Type | Description |
|--------|------|-------------|
| `attribute_name` | STRING (PK) | Attribute to resolve |
| `strategy` | STRING | RECENCY, PRIORITY, FREQUENCY |
| `source_priority_list` | STRING | Prioritized list of table_ids |
| `recency_field` | STRING | Field for RECENCY strategy |
| `is_active` | BOOL | Enabled status |

### identifier_exclusion
Blocklist for specific identifier values (e.g., 'null', 'test').

| Column | Type | Description |
|--------|------|-------------|
| `identifier_type` | STRING | Type of identifier |
| `identifier_value_pattern` | STRING | Value or LIKE pattern |
| `match_type` | STRING | 'EXACT' or 'LIKE' |
| `reason` | STRING | Audit reason |
| `created_at` | TIMESTAMP | Creation time |
| `created_by` | STRING | Creator |

### run_state
Internal state tracking for incremental processing.

| Column | Type | Description |
|--------|------|-------------|
| `table_id` | STRING (PK) | Source table ID |
| `last_watermark_value` | TIMESTAMP | Last processed timestamp |
| `last_run_id` | STRING | Last successful run ID |
| `last_run_ts` | TIMESTAMP | Last run timestamp |

### config
Global system configuration.

| Column | Type | Description |
|--------|------|-------------|
| `config_key` | STRING (PK) | Configuration setting name |
| `config_value` | STRING | Setting value |
| `description` | STRING | Setting description |
| `updated_at` | TIMESTAMP | Last update time |
| `updated_by` | STRING | User who updated |

---

## idr_out (Output)

Results of the resolution process.

### identity_resolved_membership_current
Maps entities to Resolved Identity Clusters.

| Column | Type | Description |
|--------|------|-------------|
| `entity_key` | STRING (PK) | Unique source entity key |
| `resolved_id` | STRING | The Identity Cluster ID |
| `run_id` | STRING | Run responsible for last update |
| `super_cluster_id` | STRING | Optional higher-level grouping |
| `updated_ts` | TIMESTAMP | Last update time |

### identity_clusters_current
Master list of Identity Clusters.

| Column | Type | Description |
|--------|------|-------------|
| `resolved_id` | STRING (PK) | Identity Cluster ID |
| `cluster_size` | BIGINT | Count of entities |
| `confidence_score` | DOUBLE | Quality score (0.0-1.0) |
| `primary_reason` | STRING | Reason for score |
| `run_id` | STRING | Last update run |
| `updated_ts` | TIMESTAMP | Last update time |

### identity_edges_current
Graph edges between entities.

| Column | Type | Description |
|--------|------|-------------|
| `left_entity_key` | STRING (PK) | Entity A |
| `right_entity_key` | STRING (PK) | Entity B |
| `identifier_type` | STRING (PK) | Linking identifier type |
| `identifier_value_norm` | STRING | Normalized value |
| `rule_id` | STRING | Rule responsible for edge |
| `first_seen_ts` | TIMESTAMP | Creation time |
| `last_seen_ts` | TIMESTAMP | Last verified time |
| `run_id` | STRING | Last update run |

### run_history
Audit log of executed runs.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | STRING (PK) | Unique Run ID |
| `run_mode` | STRING | FULL or INCR |
| `status` | STRING | SUCCESS, FAILED |
| `entities_processed` | BIGINT | Count |
| `edges_created` | BIGINT | Count |
| `duration_seconds` | BIGINT | Execution time |
| `error_message` | STRING | Error details (if failed) |
| `watermarks_json` | STRING | JSON snapshot of watermarks |
