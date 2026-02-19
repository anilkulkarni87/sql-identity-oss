-- BigQuery DDL: All metadata and output tables
-- Run this first to set up the IDR datasets
--
-- Usage:
--   1. Replace PROJECT_ID with your GCP project
--   2. Run in BigQuery console or bq CLI

-- ============================================
-- METADATA DATASET
-- ============================================
CREATE SCHEMA IF NOT EXISTS idr_meta;

CREATE TABLE IF NOT EXISTS idr_meta.source_table (
  table_id STRING,
  table_fqn STRING,          -- project.dataset.table format
  entity_type STRING,
  entity_key_expr STRING,
  watermark_column STRING,
  watermark_lookback_minutes INT64,
  is_active BOOL
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
  trust_rank INT64,
  is_active BOOL
);

CREATE TABLE IF NOT EXISTS idr_meta.rule (
  rule_id STRING,
  rule_name STRING,
  is_active BOOL,
  priority INT64,
  identifier_type STRING,
  canonicalize STRING,
  allow_hashed BOOL,
  require_non_null BOOL,
  max_group_size INT64  -- Skip identifier groups larger than this (default 10000)
);

CREATE TABLE IF NOT EXISTS idr_meta.identifier_mapping (
  table_id STRING,
  identifier_type STRING,
  identifier_value_expr STRING,
  is_hashed BOOL
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
    threshold FLOAT64 DEFAULT 0.85,
    priority INT64 DEFAULT 100,
    is_active BOOLEAN DEFAULT TRUE
);

-- Fuzzy Logic Polyfill for Standard SQL
-- ⚠️ WARNING: This JavaScript UDF has significant per-call overhead (~1-10ms).
-- For large-scale fuzzy matching, use native EDIT_DISTANCE instead:
--   score_expr: '1 - SAFE_DIVIDE(CAST(EDIT_DISTANCE(<a>, <b>) AS FLOAT64), CAST(GREATEST(LENGTH(<a>), LENGTH(<b>), 1) AS FLOAT64))'
--   threshold: 0.8 (lower than Jaro-Winkler since Levenshtein is stricter)
-- The UDF is kept for backward compatibility only.
CREATE FUNCTION IF NOT EXISTS idr_meta.jaro_winkler_similarity(s1 STRING, s2 STRING)
RETURNS FLOAT64
LANGUAGE js
AS """
  if (s1 === null || s2 === null) return null;
  // Jaro-Winkler implementation
  var m = 0;
  var len1 = s1.length;
  var len2 = s2.length;
  if (len1 === 0 || len2 === 0) return 0.0;

  var match_dist = Math.floor(Math.max(len1, len2) / 2) - 1;
  var match1 = new Array(len1);
  var match2 = new Array(len2);
  for(var i=0; i<len1; i++) {
    var start = Math.max(0, i-match_dist);
    var end = Math.min(i+match_dist+1, len2);
    for(var j=start; j<end; j++) {
      if(match2[j]) continue;
      if(s1.charAt(i) !== s2.charAt(j)) continue;
      match1[i] = true;
      match2[j] = true;
      m++;
      break;
    }
  }

  if (m === 0) return 0.0;

  var k = 0;
  var t = 0;
  for(var i=0; i<len1; i++) {
    if(match1[i]) {
      while(!match2[k]) k++;
      if(s1.charAt(i) !== s2.charAt(k)) t++;
      k++;
    }
  }
  t = t / 2.0;

  var jaro = (m / len1 + m / len2 + (m - t) / m) / 3.0;
  var p = 0.1;
  var l = 0;
  while(l < 4 && l < len1 && l < len2 && s1.charAt(l) === s2.charAt(l)) l++;

  return jaro + l * p * (1 - jaro);
""";

-- Exclusion list for known bad identifier values
CREATE TABLE IF NOT EXISTS idr_meta.identifier_exclusion (
  identifier_type STRING,
  identifier_value_pattern STRING,
  match_type STRING DEFAULT 'EXACT',  -- EXACT or LIKE
  reason STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  created_by STRING
);

-- ============================================
-- WORK DATASET (temporary tables)
-- ============================================
CREATE SCHEMA IF NOT EXISTS idr_work;

-- Work tables are created as needed and replaced each run

-- ============================================
-- OUTPUT DATASET
-- ============================================
CREATE SCHEMA IF NOT EXISTS idr_out;

CREATE TABLE IF NOT EXISTS idr_out.identity_edges_current (
  rule_id STRING,
  left_entity_key STRING,
  right_entity_key STRING,
  identifier_type STRING,
  identifier_value_norm STRING,
  first_seen_ts TIMESTAMP,
  last_seen_ts TIMESTAMP
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
  super_cluster_id STRING, -- Fuzzy merged ID (nullable)
  cluster_size INT64,
  -- Confidence scoring columns
  confidence_score FLOAT64,
  edge_diversity INT64,
  match_density FLOAT64,
  primary_reason STRING,
  updated_ts TIMESTAMP
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
  edges_created INT64,
  started_at TIMESTAMP,
  ended_at TIMESTAMP
);

-- Audit table for identifier groups that exceeded max_group_size
CREATE TABLE IF NOT EXISTS idr_out.skipped_identifier_groups (
  run_id STRING,
  identifier_type STRING,
  identifier_value_norm STRING,
  group_size INT64,
  max_allowed INT64,
  sample_entity_keys STRING,  -- JSON array of sample entity keys
  reason STRING,
  skipped_at TIMESTAMP
);

-- ============================================
-- OBSERVABILITY TABLES
-- ============================================
CREATE TABLE IF NOT EXISTS idr_out.run_history (
  run_id STRING,
  run_mode STRING,
  status STRING,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds INT64,
  entities_processed INT64,
  edges_created INT64,
  edges_updated INT64,
  clusters_impacted INT64,
  lp_iterations INT64,
  source_tables_processed INT64,
  groups_skipped INT64,
  values_excluded INT64,
  large_clusters INT64,
  warnings STRING,
  watermarks_json STRING,
  error_message STRING,
  error_stage STRING,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.stage_metrics (
  run_id STRING,
  stage_name STRING,
  stage_order INT64,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_seconds INT64,
  rows_in INT64,
  rows_out INT64,
  notes STRING
);

-- ============================================
-- CONFIGURATION TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS idr_meta.config (
  config_key STRING NOT NULL,
  config_value STRING NOT NULL,
  description STRING,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_by STRING
);

-- Insert default configuration values (run separately after table creation)
-- MERGE INTO idr_meta.config AS tgt
-- USING (SELECT 'large_cluster_threshold' AS config_key, '5000' AS config_value, 'Cluster size threshold' AS description) AS src
-- ON tgt.config_key = src.config_key
-- WHEN NOT MATCHED THEN INSERT (config_key, config_value, description) VALUES (src.config_key, src.config_value, src.description);

-- ============================================
-- DRY RUN TABLES
-- ============================================
CREATE TABLE IF NOT EXISTS idr_out.dry_run_results (
  run_id STRING NOT NULL,
  entity_key STRING NOT NULL,
  current_resolved_id STRING,
  proposed_resolved_id STRING,
  change_type STRING,
  current_cluster_size INT64,
  proposed_cluster_size INT64,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS idr_out.dry_run_summary (
  run_id STRING NOT NULL,
  total_entities INT64,
  new_entities INT64,
  moved_entities INT64,
  merged_clusters INT64,
  split_clusters INT64,
  unchanged_entities INT64,
  largest_proposed_cluster INT64,
  edges_would_create INT64,
  groups_would_skip INT64,
  values_would_exclude INT64,
  execution_time_seconds INT64,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- METRICS EXPORT TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS idr_out.metrics_export (
  metric_id STRING DEFAULT GENERATE_UUID(),
  run_id STRING,
  metric_name STRING NOT NULL,
  metric_value FLOAT64 NOT NULL,
  metric_type STRING DEFAULT 'gauge',
  dimensions STRING,
  recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  exported_at TIMESTAMP
);
