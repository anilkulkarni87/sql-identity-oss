
CREATE SCHEMA IF NOT EXISTS idr_meta;

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
  max_group_size INT DEFAULT 10000  -- Skip identifier groups larger than this
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

-- Exclusion list for known bad identifier values
CREATE TABLE IF NOT EXISTS idr_meta.identifier_exclusion (
  identifier_type STRING,
  identifier_value_pattern STRING,
  match_type STRING DEFAULT 'EXACT',  -- EXACT or LIKE
  reason STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  created_by STRING
);
