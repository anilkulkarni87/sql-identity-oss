-- Observability DDL: run_history and stage_metrics tables
-- Run after 00_ddl_meta.sql and 01_ddl_outputs.sql

-- Run history: track each IDR execution
CREATE TABLE IF NOT EXISTS idr_out.run_history (
  run_id STRING,
  run_mode STRING,           -- INCR or FULL
  status STRING,             -- RUNNING, SUCCESS, ERROR
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds BIGINT,

  -- Key metrics
  entities_processed BIGINT,
  edges_created BIGINT,
  edges_updated BIGINT,
  clusters_impacted BIGINT,
  lp_iterations INT,

  -- Source breakdown
  source_tables_processed INT,
  watermarks_json STRING,    -- JSON object with per-table watermarks

  -- Error info
  error_message STRING,
  error_stage STRING,

  -- Audit
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Stage metrics: detailed timing per stage
CREATE TABLE IF NOT EXISTS idr_out.stage_metrics (
  run_id STRING,
  stage_name STRING,         -- e.g. 'build_entities_delta', 'build_edges', 'label_propagation'
  stage_order INT,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds BIGINT,
  rows_in BIGINT,
  rows_out BIGINT,
  notes STRING
);

-- Identifier quality tracking
CREATE TABLE IF NOT EXISTS idr_out.identifier_quality (
  run_id STRING,
  identifier_type STRING,
  identifier_value_norm STRING,
  entity_count BIGINT,       -- Number of entities with this identifier
  flagged_reason STRING,     -- e.g. 'excessive_matches', 'null_value', 'default_value'
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
