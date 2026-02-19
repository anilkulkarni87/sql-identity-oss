-- Incremental edge build:
-- 1) Find identifier values in delta
-- 2) Pull all members (delta + existing) for those values from full identifiers store
-- 3) Build anchor edges (N-1 per group) to avoid O(N^2)
-- 4) MERGE into idr_out.identity_edges_current

-- Required: a full identifiers table/view that contains ALL identifiers, not just delta.
-- In V1, users can point this to their standardized identifiers table.
-- Adapter should create/refresh `idr_work.identifiers_all` with columns:
--   table_id, entity_key, identifier_type, identifier_value_norm, is_hashed

-- Step A: delta values
DROP TABLE IF EXISTS idr_work.delta_identifier_values;
CREATE TABLE idr_work.delta_identifier_values AS
SELECT DISTINCT identifier_type, identifier_value_norm
FROM idr_work.identifiers_delta
WHERE identifier_value_norm IS NOT NULL;

-- Step B: all members for those values
DROP TABLE IF EXISTS idr_work.members_for_delta_values;
CREATE TABLE idr_work.members_for_delta_values AS
SELECT a.table_id, a.entity_key, a.identifier_type, a.identifier_value_norm
FROM idr_work.identifiers_all a
JOIN idr_work.delta_identifier_values d
  ON a.identifier_type = d.identifier_type
 AND a.identifier_value_norm = d.identifier_value_norm;

-- Step C: choose anchor per identifier group
DROP TABLE IF EXISTS idr_work.group_anchor;
CREATE TABLE idr_work.group_anchor AS
SELECT
  identifier_type,
  identifier_value_norm,
  MIN(entity_key) AS anchor_entity_key
FROM idr_work.members_for_delta_values
GROUP BY identifier_type, identifier_value_norm;

-- Step D: build edges from anchor to others
DROP TABLE IF EXISTS idr_work.edges_new;
CREATE TABLE idr_work.edges_new AS
SELECT
  r.rule_id,
  ga.anchor_entity_key AS left_entity_key,
  m.entity_key AS right_entity_key,
  ga.identifier_type,
  ga.identifier_value_norm,
  CAST('${RUN_TS}' AS TIMESTAMP) AS first_seen_ts,
  CAST('${RUN_TS}' AS TIMESTAMP) AS last_seen_ts
FROM idr_work.group_anchor ga
JOIN idr_work.members_for_delta_values m
  ON m.identifier_type = ga.identifier_type
 AND m.identifier_value_norm = ga.identifier_value_norm
JOIN idr_meta.rule r
  ON r.is_active = TRUE
 AND r.identifier_type = ga.identifier_type
WHERE m.entity_key <> ga.anchor_entity_key;

-- Step E: merge into current edges (adapter implements MERGE/UPSERT)
-- Common representation: create a staging table and let adapter do platform MERGE.
