-- ==========================================
-- SQL Identity Resolution - Databricks DDL
-- ==========================================
-- This file defines the full schema for IDR.
-- Note: These tables are also created inline by IDR_Run.py and IDR_QuickStart.py.
-- Running this file is recommended for fresh deployments.

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS idr_meta;
CREATE SCHEMA IF NOT EXISTS idr_work;
CREATE SCHEMA IF NOT EXISTS idr_out;

-- ==========================================
-- Metadata Tables (idr_meta)
-- ==========================================

CREATE TABLE IF NOT EXISTS idr_meta.source_table (
  table_id STRING,
  table_fqn STRING,
  entity_type STRING,
  entity_key_expr STRING,
  watermark_column STRING,
  watermark_lookback_minutes INT,
  is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS idr_meta.run_state (
  table_id STRING,
  last_watermark_value TIMESTAMP,
  last_run_id STRING,
  last_run_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_meta.source (
  table_id STRING,
  source_name STRING,
  trust_rank INT,
  is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS idr_meta.rule (
  rule_id STRING,
  rule_name STRING,
  is_active BOOLEAN,
  priority INT,
  identifier_type STRING,
  canonicalize STRING,
  allow_hashed BOOLEAN,
  require_non_null BOOLEAN,
  max_group_size INT
);

CREATE TABLE IF NOT EXISTS idr_meta.identifier_mapping (
  table_id STRING,
  identifier_type STRING,
  identifier_value_expr STRING,
  is_hashed BOOLEAN
);

CREATE TABLE IF NOT EXISTS idr_meta.entity_attribute_mapping (
  table_id STRING,
  attribute_name STRING,
  attribute_expr STRING
);

CREATE TABLE IF NOT EXISTS idr_meta.survivorship_rule (
  attribute_name STRING,
  strategy STRING,
  source_priority_list STRING,
  recency_field STRING
);

CREATE TABLE IF NOT EXISTS idr_meta.fuzzy_rule (
    rule_id STRING NOT NULL,
    rule_name STRING NOT NULL,
    blocking_key_expr STRING NOT NULL,
    score_expr STRING NOT NULL,
    threshold DOUBLE DEFAULT 0.85,
    priority INTEGER DEFAULT 100,
    is_active BOOLEAN DEFAULT TRUE
);

-- Fuzzy Logic Polyfill for Standard SQL Consistency (Returns 0.0-1.0)
CREATE OR REPLACE FUNCTION idr_meta.jaro_winkler_similarity(s1 STRING, s2 STRING)
RETURNS DOUBLE
RETURN jaro_winkler(s1, s2);

CREATE TABLE IF NOT EXISTS idr_meta.identifier_exclusion (
  identifier_type STRING,
  identifier_value_pattern STRING,
  match_type STRING DEFAULT 'EXACT',
  reason STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  created_by STRING
);

CREATE TABLE IF NOT EXISTS idr_meta.config (
  config_key STRING NOT NULL,
  config_value STRING NOT NULL,
  description STRING,
  updated_at TIMESTAMP,
  updated_by STRING
);

-- ==========================================
-- Output Tables (idr_out)
-- ==========================================

CREATE TABLE IF NOT EXISTS idr_out.identity_edges_current (
  rule_id STRING,
  left_entity_key STRING,
  right_entity_key STRING,
  identifier_type STRING,
  identifier_value_norm STRING,
  first_seen_ts TIMESTAMP,
  last_seen_ts TIMESTAMP,
  run_id STRING
);

CREATE TABLE IF NOT EXISTS idr_out.identity_resolved_membership_current (
  entity_key STRING,
  resolved_id STRING,
  source_id STRING,        -- Parsed source identifier (e.g., 'orders', 'crm')
  source_key STRING,       -- Original key from source table
  updated_ts TIMESTAMP,
  run_id STRING,
  super_cluster_id STRING  -- Fuzzy merged ID (nullable)
);

CREATE TABLE IF NOT EXISTS idr_out.identity_clusters_current (
  resolved_id STRING,
  cluster_size BIGINT,
  -- Confidence scoring columns
  confidence_score DOUBLE,
  edge_diversity INT,
  match_density DOUBLE,
  primary_reason STRING,
  updated_ts TIMESTAMP,
  run_id STRING,
  super_cluster_id STRING  -- Fuzzy merged ID (nullable)
);

CREATE TABLE IF NOT EXISTS idr_out.golden_profile_current (
  resolved_id STRING,
  email_primary STRING,
  phone_primary STRING,
  first_name STRING,
  last_name STRING,
  updated_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.rule_match_audit_current (
  run_id STRING,
  rule_id STRING,
  edges_created BIGINT,
  started_at TIMESTAMP,
  ended_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.skipped_identifier_groups (
  run_id STRING,
  identifier_type STRING,
  identifier_value_norm STRING,
  group_size BIGINT,
  max_allowed BIGINT,
  sample_entity_keys STRING,
  reason STRING,
  skipped_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.run_history (
  run_id STRING,
  run_mode STRING,
  status STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds BIGINT,
  entities_processed BIGINT,
  edges_created BIGINT,
  edges_updated BIGINT,
  clusters_impacted BIGINT,
  lp_iterations INT,
  source_tables_processed INT,
  groups_skipped INT,
  values_excluded INT,
  large_clusters INT,
  warnings STRING,
  watermarks_json STRING,
  error_message STRING,
  error_stage STRING,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.stage_metrics (
  run_id STRING,
  stage_name STRING,
  stage_order INT,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds BIGINT,
  rows_in BIGINT,
  rows_out BIGINT,
  notes STRING
);

CREATE TABLE IF NOT EXISTS idr_out.dry_run_results (
  run_id STRING NOT NULL,
  entity_key STRING NOT NULL,
  current_resolved_id STRING,
  proposed_resolved_id STRING,
  change_type STRING,
  current_cluster_size INT,
  proposed_cluster_size INT,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.dry_run_summary (
  run_id STRING NOT NULL,
  total_entities INT,
  new_entities INT,
  moved_entities INT,
  merged_clusters INT,
  split_clusters INT,
  unchanged_entities INT,
  largest_proposed_cluster INT,
  edges_would_create INT,
  groups_would_skip INT,
  values_would_exclude INT,
  execution_time_seconds INT,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.metrics_export (
  metric_id STRING,
  run_id STRING,
  metric_name STRING NOT NULL,
  metric_value DOUBLE NOT NULL,
  metric_type STRING,
  dimensions STRING,
  recorded_at TIMESTAMP,
  exported_at TIMESTAMP
);

-- ==========================================
-- Default Configuration
-- ==========================================

MERGE INTO idr_meta.config tgt
USING (
  SELECT 'large_cluster_threshold' AS config_key, '5000' AS config_value, 'Cluster size threshold for warnings' AS description
  UNION ALL SELECT 'dry_run_retention_days', '7', 'Days to retain dry run results'
  UNION ALL SELECT 'max_lp_iterations', '30', 'Maximum label propagation iterations'
) src ON tgt.config_key = src.config_key
WHEN NOT MATCHED THEN INSERT (config_key, config_value, description) VALUES (src.config_key, src.config_value, src.description);
