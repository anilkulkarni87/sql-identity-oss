-- Databricks example: incremental run + label propagation loop (pseudo runnable).
-- Use a Workflow job / notebook to execute these SQL statements.

-- 0) DDL first time:
--   run sql/common/00_ddl_meta.sql and 01_ddl_outputs.sql

-- 1) Build entities/identifiers delta
-- Like Snowflake, entities_delta requires generating per-source SELECTs from metadata.

-- 2) Build edges
--   run sql/common/20_build_edges_incremental.sql
--   MERGE edges_new into idr_out.identity_edges_current (see docs/reference/sql-examples/adapter.sql)

-- 3) Subgraph
--   run sql/common/30_build_impacted_subgraph.sql

-- 4) Initialize labels
CREATE OR REPLACE TABLE idr_work.lp_labels AS
SELECT entity_key, entity_key AS label
FROM idr_work.subgraph_nodes;

-- 5) Loop (in notebook code):
-- for i in range(MAX_ITERS):
--   run sql/common/31_label_propagation_step.sql
--   if spark.sql("select delta_changed from idr_work.lp_stats").first()[0] == 0: break
--   swap tables (see docs/reference/sql-examples/adapter.sql)

-- 6) Update outputs
--   run sql/common/40_update_membership_current.sql then MERGE into membership_current
--   run sql/common/41_update_clusters_current.sql then MERGE into clusters_current
--   run sql/common/50_build_golden_profile_incremental.sql then MERGE into golden_profile_current
