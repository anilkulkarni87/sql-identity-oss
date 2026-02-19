
CREATE SCHEMA IF NOT EXISTS idr_work;
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
  source_id STRING,       -- Parsed source identifier (e.g., 'orders', 'crm')
  source_key STRING,      -- Original key from source table
  updated_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS idr_out.identity_clusters_current (
  resolved_id STRING,
  cluster_size BIGINT,
  -- Confidence scoring columns
  confidence_score DOUBLE,       -- 0.0-1.0 weighted quality score
  edge_diversity INT,            -- count of distinct identifier_types in cluster edges
  match_density DOUBLE,          -- actual_edges / max_possible_edges
  primary_reason STRING,         -- human-readable explanation of score
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
  edges_created BIGINT,
  started_at TIMESTAMP,
  ended_at TIMESTAMP
);
