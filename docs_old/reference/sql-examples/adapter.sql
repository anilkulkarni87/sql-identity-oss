-- Databricks adapter helpers (placeholders).
-- 1) Use concat() for string concat if needed
-- 2) MERGE INTO is supported for Delta tables

-- Example MERGE for edges_current (Delta):
-- MERGE INTO idr_out.identity_edges_current tgt
-- USING idr_work.edges_new src
-- ON tgt.rule_id = src.rule_id
-- AND tgt.left_entity_key = src.left_entity_key
-- AND tgt.right_entity_key = src.right_entity_key
-- AND tgt.identifier_type = src.identifier_type
-- AND tgt.identifier_value_norm = src.identifier_value_norm
-- WHEN MATCHED THEN UPDATE SET last_seen_ts = src.last_seen_ts
-- WHEN NOT MATCHED THEN INSERT *;

-- Swap labels:
-- ALTER TABLE idr_work.lp_labels RENAME TO lp_labels_old;
-- ALTER TABLE idr_work.lp_labels_next RENAME TO lp_labels;
-- DROP TABLE idr_work.lp_labels_old;
