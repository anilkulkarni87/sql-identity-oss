-- Snowflake example: incremental run + label propagation loop (pseudo runnable with substitutions).
-- Set variables as needed (or orchestrator substitutes):
-- SET RUN_ID = '...'; SET RUN_TS = CURRENT_TIMESTAMP(); SET RUN_MODE = 'INCR';

-- 0) DDL (first time)
--   run sql/common/00_ddl_meta.sql and 01_ddl_outputs.sql

-- 1) Build entities/identifiers delta
-- NOTE: entities_delta template requires dynamic SQL per source table.
-- In Snowflake, you can generate SQL using RESULT_SCAN + EXECUTE IMMEDIATE in a stored proc.
-- For OSS V1, we recommend orchestrator renders per-table SELECTs.

-- 2) Build edges incrementally
--   run sql/common/20_build_edges_incremental.sql
--   MERGE edges_new into idr_out.identity_edges_current (see adapter.sql)

-- 3) Build impacted subgraph
--   run sql/common/30_build_impacted_subgraph.sql

-- 4) Initialize labels (run once per run)
CREATE OR REPLACE TABLE idr_work.lp_labels AS
SELECT entity_key, entity_key AS label
FROM idr_work.subgraph_nodes;

-- 5) Loop label propagation until convergence (or max iterations)
-- Pseudocode:
-- FOR i IN 1..MAX_ITERS:
--   run sql/common/31_label_propagation_step.sql
--   IF (SELECT delta_changed FROM idr_work.lp_stats WHERE run_id=$RUN_ID) = 0 THEN BREAK;
--   ALTER TABLE idr_work.lp_labels SWAP WITH idr_work.lp_labels_next;

-- 6) Update membership/clusters/golden
--   run sql/common/40_update_membership_current.sql then MERGE into membership_current
--   run sql/common/41_update_clusters_current.sql then MERGE into clusters_current
--   run sql/common/50_build_golden_profile_incremental.sql then MERGE into golden_profile_current
