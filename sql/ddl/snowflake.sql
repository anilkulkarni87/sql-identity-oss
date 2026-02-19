-- Snowflake DDL: All metadata and output tables
-- Run this first to set up the IDR schema

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
  table_id VARCHAR,
  last_watermark_value TIMESTAMP_NTZ,
  last_run_id VARCHAR,
  last_run_ts TIMESTAMP_NTZ
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

-- Fuzzy Logic Polyfill for Standard SQL Consistency (Returns 0.0-1.0)
CREATE OR REPLACE FUNCTION idr_meta.jaro_winkler_similarity(s1 VARCHAR, s2 VARCHAR)
RETURNS FLOAT
AS 'JAROWINKLER_SIMILARITY(s1, s2) / 100.0';

-- Exclusion list for known bad identifier values
CREATE TABLE IF NOT EXISTS idr_meta.identifier_exclusion (
  identifier_type VARCHAR,
  identifier_value_pattern VARCHAR,
  match_type VARCHAR DEFAULT 'EXACT',
  reason VARCHAR,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  created_by VARCHAR
);

-- ============================================
-- WORK SCHEMA (transient tables)
-- ============================================
CREATE SCHEMA IF NOT EXISTS idr_work;

-- Work tables are created as TRANSIENT for performance
-- They are recreated each run

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
  first_seen_ts TIMESTAMP_NTZ,
  last_seen_ts TIMESTAMP_NTZ,
  run_id VARCHAR
);

CREATE TABLE IF NOT EXISTS idr_out.identity_resolved_membership_current (
  entity_key VARCHAR,
  resolved_id VARCHAR,
  source_id VARCHAR,       -- Parsed source identifier (e.g., 'orders', 'crm')
  source_key VARCHAR,      -- Original key from source table
  updated_ts TIMESTAMP_NTZ,
  run_id VARCHAR,
  super_cluster_id VARCHAR -- Fuzzy merged ID (nullable)
);

CREATE TABLE IF NOT EXISTS idr_out.identity_clusters_current (
  resolved_id VARCHAR,
  cluster_size INTEGER,
  -- Confidence scoring columns
  confidence_score FLOAT,
  edge_diversity INTEGER,
  match_density FLOAT,
  primary_reason VARCHAR,
  updated_ts TIMESTAMP_NTZ,
  run_id VARCHAR,
  super_cluster_id VARCHAR -- Fuzzy merged ID (nullable)
);

CREATE TABLE IF NOT EXISTS idr_out.golden_profile_current (
  resolved_id VARCHAR,
  email_primary VARCHAR,
  phone_primary VARCHAR,
  first_name VARCHAR,
  last_name VARCHAR,
  updated_ts TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS idr_out.rule_match_audit_current (
  run_id VARCHAR,
  rule_id VARCHAR,
  edges_created INTEGER,
  started_at TIMESTAMP_NTZ,
  ended_at TIMESTAMP_NTZ
);

-- Audit table for identifier groups that exceeded max_group_size
CREATE TABLE IF NOT EXISTS idr_out.skipped_identifier_groups (
  run_id VARCHAR,
  identifier_type VARCHAR,
  identifier_value_norm VARCHAR,
  group_size INTEGER,
  max_allowed INTEGER,
  sample_entity_keys VARCHAR,
  reason VARCHAR DEFAULT 'EXCEEDED_MAX_GROUP_SIZE',
  skipped_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- OBSERVABILITY TABLES
-- ============================================
CREATE TABLE IF NOT EXISTS idr_out.run_history (
  run_id VARCHAR,
  run_mode VARCHAR,
  status VARCHAR,
  started_at TIMESTAMP_NTZ,
  ended_at TIMESTAMP_NTZ,
  duration_seconds INTEGER,
  entities_processed INTEGER,
  edges_created INTEGER,
  edges_updated INTEGER,
  clusters_impacted INTEGER,
  lp_iterations INTEGER,
  source_tables_processed INTEGER,
  groups_skipped INTEGER DEFAULT 0,
  values_excluded INTEGER DEFAULT 0,
  large_clusters INTEGER DEFAULT 0,
  warnings VARCHAR,
  watermarks_json VARCHAR,
  error_message VARCHAR,
  error_stage VARCHAR,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS idr_out.stage_metrics (
  run_id VARCHAR,
  stage_name VARCHAR,
  stage_order INTEGER,
  started_at TIMESTAMP_NTZ,
  ended_at TIMESTAMP_NTZ,
  duration_seconds INTEGER,
  rows_in INTEGER,
  rows_out INTEGER,
  notes VARCHAR
);

-- ============================================
-- CONFIGURATION TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS idr_meta.config (
  config_key VARCHAR PRIMARY KEY,
  config_value VARCHAR NOT NULL,
  description VARCHAR,
  updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  updated_by VARCHAR
);

-- Insert default configuration values (Snowflake MERGE)
MERGE INTO idr_meta.config tgt
USING (
  SELECT 'large_cluster_threshold' AS config_key, '5000' AS config_value, 'Cluster size threshold for warnings' AS description
  UNION ALL
  SELECT 'dry_run_retention_days', '7', 'Days to retain dry run results'
  UNION ALL
  SELECT 'max_lp_iterations', '30', 'Maximum label propagation iterations'
) src ON tgt.config_key = src.config_key
WHEN NOT MATCHED THEN INSERT (config_key, config_value, description) VALUES (src.config_key, src.config_value, src.description);

-- ============================================
-- DRY RUN TABLES
-- ============================================
CREATE TABLE IF NOT EXISTS idr_out.dry_run_results (
  run_id VARCHAR NOT NULL,
  entity_key VARCHAR NOT NULL,
  current_resolved_id VARCHAR,
  proposed_resolved_id VARCHAR,
  change_type VARCHAR,
  current_cluster_size INTEGER,
  proposed_cluster_size INTEGER,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (run_id, entity_key)
);

CREATE TABLE IF NOT EXISTS idr_out.dry_run_summary (
  run_id VARCHAR PRIMARY KEY,
  total_entities INTEGER,
  new_entities INTEGER,
  moved_entities INTEGER,
  merged_clusters INTEGER,
  split_clusters INTEGER,
  unchanged_entities INTEGER,
  largest_proposed_cluster INTEGER,
  edges_would_create INTEGER,
  groups_would_skip INTEGER,
  values_would_exclude INTEGER,
  execution_time_seconds INTEGER,
  created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- METRICS EXPORT TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS idr_out.metrics_export (
  metric_id VARCHAR DEFAULT UUID_STRING(),
  run_id VARCHAR,
  metric_name VARCHAR NOT NULL,
  metric_value FLOAT NOT NULL,
  metric_type VARCHAR DEFAULT 'gauge',
  dimensions VARCHAR,
  recorded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  exported_at TIMESTAMP_NTZ,
  PRIMARY KEY (metric_id)
);
