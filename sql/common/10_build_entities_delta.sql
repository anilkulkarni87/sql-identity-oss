-- Builds idr_work.entities_delta for this run.
-- Assumptions:
--   - Users provide per-table entities in their source tables (table_fqn)
--   - Framework reads watermark_column + last_watermark_value to select deltas
-- Required columns in each source table for entities:
--   - columns referenced by entity_key_expr
--   - watermark_column (timestamp)
--   - optional: record_updated_at, first_name, last_name, email, phone, etc.

-- Orchestrator should substitute:
--   ${RUN_ID}, ${RUN_TS}, ${RUN_MODE}  (INCR|FULL)

DROP TABLE IF EXISTS idr_work.entities_delta;

CREATE TABLE idr_work.entities_delta AS
SELECT
  '${RUN_ID}' AS run_id,
  st.table_id,
  st.table_fqn,
  -- namespace + user-defined key expression
  CONCAT(st.table_id, ':', CAST((${ENTITY_KEY_EXPR_PLACEHOLDER}) AS STRING)) AS entity_key,
  -- watermark capture for run_state update
  CAST((${WATERMARK_COL_PLACEHOLDER}) AS TIMESTAMP) AS watermark_value,
  -- raw entity attributes (best-effort; extend as needed)
  CAST(NULL AS TIMESTAMP) AS record_updated_at,
  CAST(NULL AS STRING) AS first_name,
  CAST(NULL AS STRING) AS last_name,
  CAST(NULL AS STRING) AS email_raw,
  CAST(NULL AS STRING) AS phone_raw
FROM idr_meta.source_table st
JOIN idr_meta.run_state rs ON rs.table_id = st.table_id
-- The framework cannot inline dynamic SQL in a portable way.
-- In practice, an adapter script will materialize per-table SELECTs using st.table_fqn, st.entity_key_expr, st.watermark_column.
WHERE st.is_active = TRUE;

-- NOTE:
-- This file is a template. Platform adapters generate concrete SQL per configured source table.
