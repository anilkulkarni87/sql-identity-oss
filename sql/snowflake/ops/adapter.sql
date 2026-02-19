-- Snowflake adapter helpers (placeholders).
-- 1) Use CONCAT for string concat
-- 2) Use MERGE for upserts
-- 3) Use CREATE OR REPLACE TRANSIENT TABLE for work tables (optional)

-- Example MERGE for edges_current
-- MERGE INTO idr_out.identity_edges_current tgt
-- USING idr_work.edges_new src
-- ON tgt.rule_id = src.rule_id
-- AND tgt.left_entity_key = src.left_entity_key
-- AND tgt.right_entity_key = src.right_entity_key
-- AND tgt.identifier_type = src.identifier_type
-- AND tgt.identifier_value_norm = src.identifier_value_norm
-- WHEN MATCHED THEN UPDATE SET last_seen_ts = src.last_seen_ts
-- WHEN NOT MATCHED THEN INSERT (rule_id,left_entity_key,right_entity_key,identifier_type,identifier_value_norm,first_seen_ts,last_seen_ts)
-- VALUES (src.rule_id,src.left_entity_key,src.right_entity_key,src.identifier_type,src.identifier_value_norm,src.first_seen_ts,src.last_seen_ts);

-- Swap pattern for label propagation
-- ALTER TABLE idr_work.lp_labels SWAP WITH idr_work.lp_labels_next;
