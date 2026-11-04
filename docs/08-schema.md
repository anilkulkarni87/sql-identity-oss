# Schema & Data Model Reference

IDR organizes all state into three schemas across all 4 supported data warehouses:

1. **`idr_meta`**: Configuration, rules, mappings, and watermarks.
2. **`idr_work`**: Transient staging tables used during pipeline execution.
3. **`idr_out`**: Production identity graph, resolved clusters, golden profiles, and telemetry.

---

## 1. Metadata Schema (`idr_meta`)

### `idr_meta.source_table`
Registry of source tables to be ingested.

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `table_id` | STRING | PK | Unique identifier for source table (e.g. `crm`, `pos`). |
| `table_fqn` | STRING | | Fully qualified table name (`schema.table` or `catalog.schema.table`). |
| `entity_type` | STRING | | Entity category (e.g. `PERSON`, `ACCOUNT`). |
| `entity_key_expr` | STRING | | SQL expression producing the source entity key. |
| `watermark_column` | STRING | | Timestamp/date column used for incremental watermark tracking. |
| `watermark_lookback_minutes`| INT | | Lookback buffer window in minutes for late-arriving records. |
| `is_active` | BOOLEAN | | Whether this source table is processed during pipeline execution. |

### `idr_meta.rule`
Deterministic matching rules.

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `rule_id` | STRING | PK | Unique rule identifier (e.g. `r_email`). |
| `identifier_type` | STRING | | Standardized identifier type to match (e.g. `EMAIL`, `PHONE`). |
| `canonicalize` | STRING | | Normalization strategy (`LOWERCASE`, `UPPERCASE`, or `EXACT`). |
| `max_group_size` | INT | | Guardrail limit. Groups larger than this are skipped. |
| `priority` | INT | | Execution and evidence ranking priority (lower = higher priority). |
| `is_active` | BOOLEAN | | Whether this rule is enabled. |

### `idr_meta.identifier_mapping`
Maps source table columns/expressions to standardized identifier types.

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `table_id` | STRING | PK | Foreign key to `source_table.table_id`. |
| `identifier_type` | STRING | PK | Identifier category (e.g. `EMAIL`, `PHONE`, `LOYALTY`). |
| `identifier_value_expr` | STRING | | SQL expression extracting the raw identifier value. |
| `is_hashed` | BOOLEAN | | Whether source values are already hashed (e.g. SHA-256). |

### `idr_meta.entity_attribute_mapping`
Maps source columns to profile attributes for Golden Profile survivorship.

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `table_id` | STRING | PK | Foreign key to `source_table.table_id`. |
| `attribute_name` | STRING | PK | Standardized profile attribute name (e.g. `first_name`, `email`). |
| `attribute_expr` | STRING | | SQL expression extracting the attribute value. |

### `idr_meta.survivorship_rule`
Rules for resolving Golden Profile attribute values.

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `attribute_name` | STRING | PK | Target profile attribute. |
| `strategy` | STRING | | Survivorship strategy: `RECENCY`, `PRIORITY`, or `FREQUENCY`. |
| `source_priority_list` | STRING | | JSON list of prioritized `table_id`s for `PRIORITY` strategy. |
| `recency_field` | STRING | | Optional source timestamp field for `RECENCY` strategy. |
| `is_active` | BOOLEAN | | Enabled status. |

### `idr_meta.fuzzy_rule`
Probabilistic matching rules (used when `--strict` is not enabled).

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `rule_id` | STRING | PK | Unique fuzzy rule ID. |
| `rule_name` | STRING | | Descriptive rule name. |
| `blocking_key_expr` | STRING | | SQL expression partitioning records before similarity comparisons. |
| `score_expr` | STRING | | Dialect-appropriate similarity expression returning 0.0 to 1.0. |
| `threshold` | DOUBLE | | Minimum score required to merge clusters. |
| `priority` | INT | | Execution priority. |
| `is_active` | BOOLEAN | | Enabled status. |

### `idr_meta.identifier_exclusion`
Blocklist for filtering out placeholder, system, or generic identifiers.

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `identifier_type` | STRING | PK | Identifier type (e.g. `EMAIL`, `PHONE`). |
| `identifier_value_pattern` | STRING | PK | Exact string or SQL wildcard pattern (`%`). |
| `match_type` | STRING | | `EXACT` or `LIKE`. |
| `reason` | STRING | | Audit reason (e.g. "Shared store tablet"). |

### `idr_meta.run_state`
Tracks high-watermarks for incremental processing.

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `table_id` | STRING | PK | Source table ID. |
| `last_watermark_value` | TIMESTAMP | | Highest watermark processed in last successful run. |
| `last_run_id` | STRING | | ID of the run that updated this watermark. |
| `updated_at` | TIMESTAMP | | Timestamp of state update. |

---

## 2. Production Output Schema (`idr_out`)

### `idr_out.identity_resolved_membership_current`
Maps every source entity to its resolved Identity Cluster ID.

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `entity_key` | STRING | PK | Unique entity key (`{table_id}:{source_key}`). |
| `resolved_id` | STRING | | The resolved Identity Cluster ID (minimum entity key in cluster). |
| `run_id` | STRING | | ID of the run that produced or updated this membership. |
| `super_cluster_id` | STRING | | Higher-level cluster ID if merged by probabilistic fuzzy matching. |
| `updated_at` | TIMESTAMP | | Timestamp when membership was materialized. |

### `idr_out.identity_clusters_current`
Master list of active identity clusters with quality and confidence metrics.

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `resolved_id` | STRING | PK | Unique Cluster ID. |
| `cluster_size` | INT | | Count of member entities in this cluster. |
| `run_id` | STRING | | ID of the run that updated this cluster. |
| `super_cluster_id` | STRING | | Higher-level fuzzy cluster ID. |
| `confidence_score` | DOUBLE | | Composite quality score (0.0 to 1.0) based on edge diversity and density. |
| `primary_reason` | STRING | | Explanation of the confidence score. |
| `updated_at` | TIMESTAMP | | Last update timestamp. |

### `idr_out.identity_edges_current`
Graph edges representing direct links between entities.

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `left_entity_key` | STRING | PK | Source entity A. |
| `right_entity_key` | STRING | PK | Source entity B. |
| `identifier_type` | STRING | PK | Identifier type that formed the link. |
| `identifier_value_norm`| STRING | | Normalized matching value. |
| `first_seen_ts` | TIMESTAMP | | Timestamp when edge was first observed. |
| `last_seen_ts` | TIMESTAMP | | Timestamp when edge was most recently confirmed. |
| `run_id` | STRING | | Run ID that created/updated the edge. |

### `idr_out.identity_identifiers_current`
Current registry of all entity identifiers for fast incremental candidate expansion.

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `entity_key` | STRING | PK | Source entity key. |
| `identifier_type` | STRING | PK | Standardized identifier type. |
| `identifier_value_norm`| STRING | | Normalized value. |
| `is_hashed` | BOOLEAN | | Whether value was pre-hashed. |
| `run_id` | STRING | | Last update run ID. |
| `updated_at` | TIMESTAMP | | Last update timestamp. |

### `idr_out.golden_profiles`
The unified 360° customer profile table generated by applying survivorship rules across all cluster members.

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `resolved_id` | STRING | PK | Cluster ID. |
| `run_id` | STRING | | Generation run ID. |
| `updated_at` | TIMESTAMP | | Generation timestamp. |
| `[attribute_name]` | Various | | Resolved golden attribute values (e.g. `first_name`, `email`, `phone`). |

### `idr_out.skipped_identifier_groups`
Audit log of oversized identifier groups skipped to prevent hub flattening.

| Column | Type | Description |
|---|---|---|
| `run_id` | STRING | Run during which the group was bypassed. |
| `identifier_type` | STRING | Type of identifier (e.g. `EMAIL`). |
| `identifier_value_norm`| STRING | Normalized value that exceeded group size limits. |
| `group_size` | INT | Count of entities sharing this identifier. |
| `max_allowed` | INT | Configured `max_group_size` threshold. |
| `sample_entity_keys` | STRING | Sample entity keys for inspection. |
| `reason` | STRING | Reason for skipping (`EXCEEDED_MAX_GROUP_SIZE`). |
| `skipped_at` | TIMESTAMP | Audit timestamp. |

### `idr_out.run_history`
Complete operational audit trail of all pipeline runs.

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `run_id` | STRING | PK | Unique Run ID. |
| `run_mode` | STRING | | `FULL` or `INCR`. |
| `status` | STRING | | `SUCCESS`, `SUCCESS_WITH_WARNINGS`, `FAILED`, `INTERRUPTED`, or `DRY_RUN_COMPLETE`. |
| `started_at` | TIMESTAMP | | Pipeline execution start time. |
| `ended_at` | TIMESTAMP | | Pipeline execution completion time. |
| `duration_seconds` | INT | | Wall-clock execution time in seconds. |
| `entities_processed` | INT | | Number of entities extracted and resolved. |
| `edges_created` | INT | | Count of edges formed. |
| `clusters_impacted` | INT | | Count of clusters created or modified. |
| `lp_iterations` | INT | | Iterations required for Label Propagation convergence. |
| `warnings` | STRING | | JSON list of operational warnings. |
| `error_message` | STRING | | Error details if failed. |
| `config_hash` | STRING | | SHA-256 hash of configuration applied. |

### `idr_out.stage_metrics`
Fine-grained telemetry capturing per-stage duration and row counts.

| Column | Type | Description |
|---|---|---|
| `run_id` | STRING | Associated run ID. |
| `stage_name` | STRING | Stage (`preflight`, `entity_extraction`, `identifier_extraction`, `edge_building`, `label_propagation`, `fuzzy_matching`, `output_generation`). |
| `stage_order` | INT | Execution order. |
| `duration_seconds` | INT | Time spent in stage. |
| `rows_out` | INT | Number of rows processed or produced. |

### `idr_out.config_snapshot`
Saved snapshots of sources, rules, and mappings for historical reproducibility.

| Column | Type | Primary Key | Description |
|---|---|---|---|
| `config_hash` | STRING | PK | Hash of the configuration. |
| `sources_json` | STRING | | JSON representation of source tables and mappings. |
| `rules_json` | STRING | | JSON representation of rules. |
| `mappings_json` | STRING | | JSON representation of identifier mappings. |
| `created_at` | TIMESTAMP | | Snapshot creation timestamp. |

### `idr_out.dry_run_results` & `idr_out.dry_run_summary`
Generated during `dry_run=True` execution to preview proposed entity migrations without modifying production tables.
