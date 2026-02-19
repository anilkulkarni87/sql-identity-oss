-- Update run_state per table_id after successful run.
-- Adapter should compute max watermark_value per table_id from entities_delta and update run_state.
-- This template expects a precomputed table idr_work.watermark_updates(table_id, new_watermark_value)

-- Example:
-- MERGE INTO idr_meta.run_state rs
-- USING idr_work.watermark_updates u
-- ON rs.table_id = u.table_id
-- WHEN MATCHED THEN UPDATE SET last_watermark_value = u.new_watermark_value, last_run_id='${RUN_ID}', last_run_ts='${RUN_TS}'
-- WHEN NOT MATCHED THEN INSERT (...)
