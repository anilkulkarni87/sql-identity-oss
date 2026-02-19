-- ============================================================
-- DDL Migration for idr_core Package Compatibility
-- ============================================================
-- Run this to update existing DDL to work with the new unified runner
--
-- Usage:
--   duckdb idr.duckdb < ddl_migration.sql
-- ============================================================

-- Add missing columns to run_history (required by new runner)
ALTER TABLE idr_out.run_history ADD COLUMN IF NOT EXISTS is_dry_run BOOLEAN DEFAULT FALSE;
ALTER TABLE idr_out.run_history ADD COLUMN IF NOT EXISTS identifiers_extracted BIGINT DEFAULT 0;

-- Add unique constraints for upsert support
-- Note: DuckDB uses CREATE UNIQUE INDEX, then ON CONFLICT works

-- Drop and recreate identity_resolved_membership_current with PK
CREATE OR REPLACE TABLE idr_out.identity_resolved_membership_current_new (
  entity_key VARCHAR PRIMARY KEY,
  resolved_id VARCHAR NOT NULL,
  source_id VARCHAR,
  source_key VARCHAR,
  updated_ts TIMESTAMP,
  run_id VARCHAR
);

INSERT INTO idr_out.identity_resolved_membership_current_new
SELECT entity_key, resolved_id, source_id, source_key, updated_ts, NULL as run_id
FROM idr_out.identity_resolved_membership_current;

DROP TABLE IF EXISTS idr_out.identity_resolved_membership_current;
ALTER TABLE idr_out.identity_resolved_membership_current_new
  RENAME TO identity_resolved_membership_current;

-- Drop and recreate identity_clusters_current with PK
CREATE OR REPLACE TABLE idr_out.identity_clusters_current_new (
  resolved_id VARCHAR PRIMARY KEY,
  cluster_size BIGINT,
  confidence_score DOUBLE,
  edge_diversity INTEGER,
  match_density DOUBLE,
  primary_reason VARCHAR,
  updated_ts TIMESTAMP,
  run_id VARCHAR
);

INSERT INTO idr_out.identity_clusters_current_new
SELECT resolved_id, cluster_size, confidence_score, edge_diversity,
       match_density, primary_reason, updated_ts, NULL as run_id
FROM idr_out.identity_clusters_current;

DROP TABLE IF EXISTS idr_out.identity_clusters_current;
ALTER TABLE idr_out.identity_clusters_current_new
  RENAME TO identity_clusters_current;

-- Add composite unique index for edges
CREATE UNIQUE INDEX IF NOT EXISTS idx_edges_unique
ON idr_out.identity_edges_current (left_entity_key, right_entity_key, identifier_type);

-- Add run_id column to edges if missing
ALTER TABLE idr_out.identity_edges_current ADD COLUMN IF NOT EXISTS run_id VARCHAR;

-- Add unique constraint for run_state
CREATE UNIQUE INDEX IF NOT EXISTS idx_run_state_pk
ON idr_meta.run_state (table_id);

-- Add unique constraint for identifier_mapping
CREATE UNIQUE INDEX IF NOT EXISTS idx_identifier_mapping_pk
ON idr_meta.identifier_mapping (table_id, identifier_type);

-- Add unique constraint for entity_attribute_mapping
CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_attr_mapping_pk
ON idr_meta.entity_attribute_mapping (table_id, attribute_name);

-- Make skipped_identifier_groups work without sample_entity_keys
ALTER TABLE idr_out.skipped_identifier_groups
  ALTER COLUMN sample_entity_keys DROP NOT NULL;

-- Verification
SELECT 'Migration complete' as status;
SELECT table_name, COUNT(*) as indexes
FROM information_schema.table_constraints
WHERE constraint_type = 'PRIMARY KEY'
GROUP BY table_name;
