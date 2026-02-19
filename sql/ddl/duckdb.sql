-- DuckDB DDL: All metadata and output tables
-- Run this first to set up the IDR schemas
--
-- Usage:
--   duckdb idr.duckdb < 00_ddl_all.sql

-- ============================================
-- METADATA SCHEMA
-- ============================================
CREATE SCHEMA IF NOT EXISTS idr_meta;

CREATE TABLE IF NOT EXISTS idr_meta.source_table (
  table_id VARCHAR,
  table_fqn VARCHAR,
  entity_type VARCHAR,
  entity_key_expr VARCHAR,
  watermark_column VARCHAR,
  watermark_lookback_minutes INTEGER,
  is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS idr_meta.run_state (
  table_id VARCHAR PRIMARY KEY,
  last_watermark_value TIMESTAMP,
  last_run_id VARCHAR,
  last_run_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_meta.source (
  table_id VARCHAR,
  source_name VARCHAR,
  trust_rank INTEGER,
  is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS idr_meta.rule (
  rule_id VARCHAR,
  rule_name VARCHAR,
  is_active BOOLEAN,
  priority INTEGER,
  identifier_type VARCHAR,
  canonicalize VARCHAR,
  allow_hashed BOOLEAN,
  require_non_null BOOLEAN,
  max_group_size INTEGER DEFAULT 10000  -- Skip identifier groups larger than this
);

CREATE TABLE IF NOT EXISTS idr_meta.identifier_mapping (
  table_id VARCHAR,
  identifier_type VARCHAR,
  identifier_value_expr VARCHAR,
  is_hashed BOOLEAN
);

CREATE TABLE IF NOT EXISTS idr_meta.entity_attribute_mapping (
  table_id VARCHAR,
  attribute_name VARCHAR,
  attribute_expr VARCHAR
);

CREATE TABLE IF NOT EXISTS idr_meta.survivorship_rule (
  attribute_name VARCHAR,
  strategy VARCHAR,
  source_priority_list VARCHAR,
  recency_field VARCHAR
);

CREATE TABLE IF NOT EXISTS idr_meta.fuzzy_rule (
    rule_id VARCHAR NOT NULL PRIMARY KEY,
    rule_name VARCHAR NOT NULL,
    blocking_key_expr VARCHAR NOT NULL,
    score_expr VARCHAR NOT NULL,
    threshold DOUBLE DEFAULT 0.85,
    priority INTEGER DEFAULT 100,
    is_active BOOLEAN DEFAULT TRUE
);

-- Exclusion list for known bad identifier values
CREATE TABLE IF NOT EXISTS idr_meta.identifier_exclusion (
  identifier_type VARCHAR,
  identifier_value_pattern VARCHAR,  -- Exact value or LIKE pattern (with %)
  match_type VARCHAR DEFAULT 'EXACT', -- EXACT or LIKE
  reason VARCHAR,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  created_by VARCHAR
);

-- ============================================
-- WORK SCHEMA
-- ============================================
CREATE SCHEMA IF NOT EXISTS idr_work;

-- ============================================
-- OUTPUT SCHEMA
-- ============================================
CREATE SCHEMA IF NOT EXISTS idr_out;

CREATE TABLE IF NOT EXISTS idr_out.identity_edges_current (
  rule_id VARCHAR,
  left_entity_key VARCHAR,
  right_entity_key VARCHAR,
  identifier_type VARCHAR,
  identifier_value_norm VARCHAR,
  first_seen_ts TIMESTAMP,
  last_seen_ts TIMESTAMP,
  PRIMARY KEY (left_entity_key, right_entity_key, identifier_type)
);

CREATE TABLE IF NOT EXISTS idr_out.identity_resolved_membership_current (
  entity_key VARCHAR PRIMARY KEY,
  resolved_id VARCHAR,
  source_id VARCHAR,       -- Parsed source identifier (e.g., 'orders', 'crm')
  source_key VARCHAR,      -- Original key from source table
  updated_ts TIMESTAMP,
  run_id VARCHAR,
  super_cluster_id VARCHAR -- Fuzzy merged ID (nullable)
);

CREATE TABLE IF NOT EXISTS idr_out.identity_clusters_current (
  resolved_id VARCHAR PRIMARY KEY,
  cluster_size BIGINT,
  -- Confidence scoring columns
  confidence_score DOUBLE,
  edge_diversity INTEGER,
  match_density DOUBLE,
  primary_reason VARCHAR,
  updated_ts TIMESTAMP,
  run_id VARCHAR,
  super_cluster_id VARCHAR -- Fuzzy merged ID (nullable)
);

CREATE TABLE IF NOT EXISTS idr_out.golden_profile_current (
  resolved_id VARCHAR,
  email_primary VARCHAR,
  phone_primary VARCHAR,
  first_name VARCHAR,
  last_name VARCHAR,
  updated_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.rule_match_audit_current (
  run_id VARCHAR,
  rule_id VARCHAR,
  edges_created BIGINT,
  started_at TIMESTAMP,
  ended_at TIMESTAMP
);

-- Audit table for identifier groups that exceeded max_group_size
CREATE TABLE IF NOT EXISTS idr_out.skipped_identifier_groups (
  run_id VARCHAR,
  identifier_type VARCHAR,
  identifier_value_norm VARCHAR,
  group_size BIGINT,
  max_allowed BIGINT,
  sample_entity_keys VARCHAR,  -- JSON array of first 5 entity keys
  reason VARCHAR DEFAULT 'EXCEEDED_MAX_GROUP_SIZE',
  skipped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- OBSERVABILITY TABLES
-- ============================================
CREATE TABLE IF NOT EXISTS idr_out.run_history (
  run_id VARCHAR,
  run_mode VARCHAR,
  status VARCHAR,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds BIGINT,
  entities_processed BIGINT,
  edges_created BIGINT,
  edges_updated BIGINT,
  clusters_impacted BIGINT,
  lp_iterations INTEGER,
  source_tables_processed INTEGER,
  -- New observability columns
  groups_skipped INTEGER DEFAULT 0,      -- Identifier groups skipped (max_group_size)
  values_excluded INTEGER DEFAULT 0,     -- Values matched exclusion list
  large_clusters INTEGER DEFAULT 0,      -- Clusters with 1000+ entities
  warnings VARCHAR,                       -- JSON array of warning messages
  watermarks_json VARCHAR,
  error_message VARCHAR,
  error_stage VARCHAR,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.stage_metrics (
  run_id VARCHAR,
  stage_name VARCHAR,
  stage_order INTEGER,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds BIGINT,
  rows_in BIGINT,
  rows_out BIGINT,
  notes VARCHAR
);

-- ============================================
-- CONFIGURATION TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS idr_meta.config (
  config_key VARCHAR PRIMARY KEY,
  config_value VARCHAR NOT NULL,
  description VARCHAR,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_by VARCHAR
);

-- Insert default configuration values
INSERT INTO idr_meta.config (config_key, config_value, description) VALUES
  ('large_cluster_threshold', '5000', 'Cluster size threshold for warnings and alerts'),
  ('dry_run_retention_days', '7', 'Days to retain dry run results before cleanup'),
  ('max_lp_iterations', '30', 'Maximum label propagation iterations')
ON CONFLICT (config_key) DO NOTHING;

-- ============================================
-- DRY RUN TABLES
-- ============================================
CREATE TABLE IF NOT EXISTS idr_out.dry_run_results (
  run_id VARCHAR NOT NULL,
  entity_key VARCHAR NOT NULL,
  current_resolved_id VARCHAR,           -- Current cluster (NULL if new entity)
  proposed_resolved_id VARCHAR,          -- Computed by dry run
  change_type VARCHAR,                   -- 'NEW', 'MOVED', 'MERGED', 'UNCHANGED'
  current_cluster_size INTEGER,
  proposed_cluster_size INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (run_id, entity_key)
);

CREATE TABLE IF NOT EXISTS idr_out.dry_run_summary (
  run_id VARCHAR PRIMARY KEY,
  total_entities INTEGER,
  new_entities INTEGER,                  -- First time seen
  moved_entities INTEGER,                -- Changed cluster
  merged_clusters INTEGER,               -- Clusters that merged
  split_clusters INTEGER,                -- Clusters that split
  unchanged_entities INTEGER,
  largest_proposed_cluster INTEGER,
  edges_would_create INTEGER,
  groups_would_skip INTEGER,
  values_would_exclude INTEGER,
  execution_time_seconds INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- METRICS EXPORT TABLE (Plugin Framework)
-- ============================================
CREATE TABLE IF NOT EXISTS idr_out.metrics_export (
  metric_id VARCHAR DEFAULT (uuid()::VARCHAR),
  run_id VARCHAR,
  metric_name VARCHAR NOT NULL,
  metric_value DOUBLE NOT NULL,
  metric_type VARCHAR DEFAULT 'gauge',   -- 'gauge', 'counter', 'histogram'
  dimensions VARCHAR,                     -- JSON: {"rule_id": "R_EMAIL", "stage": "edges"}
  recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  exported_at TIMESTAMP,                  -- NULL until exported by plugin
  PRIMARY KEY (metric_id)
);
