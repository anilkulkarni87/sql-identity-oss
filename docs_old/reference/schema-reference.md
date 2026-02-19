---
tags:
  - schema
  - ddl
  - tables
  - reference
---

# Schema Reference

Complete DDL reference for all SQL Identity Resolution tables.

---

## Quick Links

- [idr_meta Schema](#idr_meta-configuration)
- [idr_work Schema](#idr_work-processing)
- [idr_out Schema](#idr_out-output)

---

## idr_meta (Configuration)

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

### source

Source system metadata (trust, priority).

| Column | Type | Description |
|--------|------|-------------|
| `table_id` | STRING (PK) | Reference to source_table |
| `source_name` | STRING | Display name |
| `trust_rank` | INT | Priority (1=Highest) |
| `is_active` | BOOL | Enabled status |

### rule

Deterministic matching rules.

| Column | Type | Description |
|--------|------|-------------|
| `rule_id` | STRING (PK) | Unique rule identifier |
| `rule_name` | STRING | Descriptive name |
| `identifier_type` | STRING | Type of identifier (EMAIL, etc.) |
| `canonicalize` | STRING | Normalization strategy (LOWERCASE, EXACT) |
| `allow_hashed` | BOOL | Allow pre-hashed values |
| `require_non_null` | BOOL | Skip if NULL |
| `max_group_size` | INT | Max entities per group |
| `priority` | INT | Processing priority |
| `is_active` | BOOL | Enabled status |

### fuzzy_rule

Probabilistic matching rules.

| Column | Type | Description |
|--------|------|-------------|
| `rule_id` | STRING (PK) | Unique rule identifier |
| `rule_name` | STRING | Descriptive name |
| `blocking_key_expr` | STRING | Blocking key logic (e.g., zip_code) |
| `score_expr` | STRING | Similarity function call |
| `threshold` | DOUBLE | Match threshold (0.0-1.0) |
| `priority` | INT | Processing priority |
| `is_active` | BOOL | Enabled status |

### identifier_mapping

Mapping of source columns to identifiers.

| Column | Type | Description |
|--------|------|-------------|
| `table_id` | STRING (PK) | FK to source_table |
| `identifier_type` | STRING (PK) | Identifier type |
| `identifier_value_expr` | STRING | SQL expression to extract value |
| `is_hashed` | BOOL | If value is already hashed |

### entity_attribute_mapping

Mapping for Golden Profile generation.

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
| `source_priority_list` | STRING | Comma-separated list for PRIORITY |
| `recency_field` | STRING | Field for RECENCY strategy |
| `is_active` | BOOL | Enabled status |

### identifier_exclusion

Blocklist for identifier values.

| Column | Type | Description |
|--------|------|-------------|
| `identifier_type` | STRING (PK) | Type of identifier |
| `identifier_value_pattern` | STRING (PK) | Value or LIKE pattern |
| `match_type` | STRING | EXACT or LIKE |
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

## idr_work (Processing)

Transient tables used during execution.

*   `entities_delta`: Entities changed since watermark.
*   `identifiers`: Extracted identifiers for delta.
*   `edges_new`: Newly discovered edges.
*   `lp_labels`: Label propagation state.
*   `cluster_sizes_updates`: Updates to cluster counts.

---

## idr_out (Output)

### identity_resolved_membership_current

Maps source entities to Resolved Identity Clusters.

| Column | Type | Description |
|--------|------|-------------|
| `entity_key` | STRING (PK) | Unique source entity key |
| `resolved_id` | STRING | The Identity Cluster ID |
| `source_id` | STRING | Parsed source identifier |
| `source_key` | STRING | Original key from source table |
| `run_id` | STRING | Run responsible for last update |
| `super_cluster_id` | STRING | Optional higher-level grouping |
| `updated_at` | TIMESTAMP | Last update time |

### identity_clusters_current

Master list of Identity Clusters.

| Column | Type | Description |
|--------|------|-------------|
| `resolved_id` | STRING (PK) | Identity Cluster ID |
| `cluster_size` | INT | Count of entities |
| `confidence_score` | DOUBLE | Quality score (0.0-1.0) |
| `edge_diversity` | INT | Number of unique edge types |
| `match_density` | DOUBLE | Graph density metric |
| `primary_reason` | STRING | Reason for score |
| `run_id` | STRING | Last update run |
| `super_cluster_id` | STRING | Optional grouping |
| `updated_at` | TIMESTAMP | Last update time |
| `updated_at` | TIMESTAMP | Last update time |

### identity_edges_current

Graph edges between entities.

| Column | Type | Description |
|--------|------|-------------|
| `rule_id` | STRING | Rule responsible for edge |
| `left_entity_key` | STRING (PK) | Entity A |
| `right_entity_key` | STRING (PK) | Entity B |
| `identifier_type` | STRING (PK) | Linking identifier type |
| `identifier_value_norm` | STRING | Normalized value |
| `first_seen_ts` | TIMESTAMP | Creation time |
| `last_seen_ts` | TIMESTAMP | Last verified time |
| `run_id` | STRING | Last update run |

### run_history

Audit log of executed runs.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | STRING (PK) | Unique Run ID |
| `run_mode` | STRING | FULL or INCR |
| `status` | STRING | SUCCESS, FAILED, etc. |
| `entities_processed` | INT | Count |
| `edges_created` | INT | Count |
| `edges_updated` | INT | Count |
| `clusters_impacted` | INT | Count |
| `lp_iterations` | INT | Number of propagation rounds |
| `source_tables_processed` | INT | Number of sources read |
| `groups_skipped` | INT | Identifier groups skipped (max size) |
| `values_excluded` | INT | Values matching exclusion list |
| `large_clusters` | INT | Clusters exceeding size threshold |
| `warnings` | STRING | JSON array of warnings |
| `error_message` | STRING | Error details (if failed) |
| `error_stage` | STRING | Stage where error occurred |
| `duration_seconds` | INT | Execution time |
| `watermarks_json` | STRING | JSON snapshot of watermarks |

### edge_evidence

Detailed audit trail for edge creation (Optional).

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | STRING | Run ID |
| `entity_key_a` | STRING | Entity A |
| `entity_key_b` | STRING | Entity B |
| `rule_id` | STRING | Rule responsible |
| `score` | DOUBLE | Match score |
