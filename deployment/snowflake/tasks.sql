-- Snowflake Tasks & Scheduling for SQL Identity Resolution
-- Provides automated scheduling for incremental runs, full refreshes, retention cleanup, and failure alerting.

-- ============================================
-- 1. OPTIONAL: SNOWPARK PYTHON STORED PROCEDURE
-- ============================================
-- If you run IDR directly inside Snowflake compute (without external Airflow/CLI runner):
-- CREATE OR REPLACE PROCEDURE idr_execute_run(run_mode STRING, max_iters INT)
--   RETURNS STRING
--   LANGUAGE PYTHON
--   RUNTIME_VERSION = '3.11'
--   PACKAGES = ('snowflake-snowpark-python', 'pyyaml')
--   HANDLER = 'main'
-- AS
-- $$
-- def main(session, run_mode, max_iters):
--     from idr_core.adapters.snowflake import SnowflakeAdapter
--     from idr_core.runner import IDRRunner, RunConfig
--     adapter = SnowflakeAdapter(session=session)
--     runner = IDRRunner(adapter)
--     result = runner.run(RunConfig(run_mode=run_mode, max_iters=max_iters))
--     return result.status
-- $$;

-- ============================================
-- 2. HOURLY INCREMENTAL RUN TASK
-- ============================================
-- Note: Replace the procedure call below with your preferred execution method
-- (e.g., Snowpark procedure, Snowflake Container Service, or Airflow trigger)

CREATE OR REPLACE TASK idr_hourly_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '60 MINUTE'
AS
  -- CALL idr_execute_run('INCR', 30);
  SELECT 'IDR hourly incremental run scheduled via external runner or Snowpark' AS status;

-- Enable the task when ready:
-- ALTER TASK idr_hourly_task RESUME;


-- ============================================
-- 3. WEEKLY FULL REFRESH TASK
-- ============================================

CREATE OR REPLACE TASK idr_weekly_full_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 2 * * 0 UTC'  -- Sunday 2am UTC
AS
  -- CALL idr_execute_run('FULL', 30);
  SELECT 'IDR weekly full refresh scheduled via external runner or Snowpark' AS status;

-- Enable the task when ready:
-- ALTER TASK idr_weekly_full_task RESUME;


-- ============================================
-- 4. DAILY RETENTION & WORK TABLE CLEANUP TASK
-- ============================================

CREATE OR REPLACE TASK idr_daily_cleanup_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 3 * * * UTC'  -- Daily 3am UTC
AS
BEGIN
  -- 1. Purge dry-run preview results older than 7 days
  DELETE FROM idr_out.dry_run_results
  WHERE created_at < DATEADD('day', -7, CURRENT_TIMESTAMP());

  DELETE FROM idr_out.dry_run_summary
  WHERE created_at < DATEADD('day', -7, CURRENT_TIMESTAMP());

  -- 2. Purge skipped identifier group logs older than 30 days
  DELETE FROM idr_out.skipped_identifier_groups
  WHERE skipped_at < DATEADD('day', -30, CURRENT_TIMESTAMP());

  -- 3. Truncate ephemeral staging tables in idr_work if no run is active
  LET active_runs INT := (
    SELECT COUNT(*) FROM idr_out.run_history
    WHERE status = 'RUNNING'
      AND started_at > DATEADD('hour', -2, CURRENT_TIMESTAMP())
  );

  IF (active_runs = 0) THEN
    TRUNCATE TABLE IF EXISTS idr_work.entities_delta;
    TRUNCATE TABLE IF EXISTS idr_work.identifiers_extracted;
    TRUNCATE TABLE IF EXISTS idr_work.identifiers_all;
    TRUNCATE TABLE IF EXISTS idr_work.edges_new;
    TRUNCATE TABLE IF EXISTS idr_work.lp_edges;
  END IF;
END;

-- Enable the cleanup task when ready:
-- ALTER TASK idr_daily_cleanup_task RESUME;


-- ============================================
-- 5. FAILURE ALERTING STREAM & TASK
-- ============================================

-- Create stream on run_history to capture new pipeline executions
CREATE OR REPLACE STREAM idr_run_history_stream ON TABLE idr_out.run_history;

-- Alert on any failed pipeline runs
CREATE OR REPLACE TASK idr_alert_on_failure_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('idr_run_history_stream')
AS
BEGIN
  -- Insert into an alert log or call notification integration
  INSERT INTO idr_out.stage_metrics (run_id, stage, duration_seconds, rows_processed, metrics_json, recorded_at)
  SELECT
    run_id,
    'alert_failed_run',
    0,
    0,
    OBJECT_CONSTRUCT(
      'alert', 'IDR pipeline run failed',
      'error_message', error_message,
      'run_mode', run_mode,
      'started_at', started_at
    )::VARCHAR,
    CURRENT_TIMESTAMP()
  FROM idr_run_history_stream
  WHERE METADATA$ACTION = 'INSERT'
    AND status = 'FAILED';

  -- Optional: Send email via Snowflake Notification Integration
  -- CALL SYSTEM$SEND_EMAIL('idr_email_int', 'data-team@company.com', 'IDR Run Failure Alert', 'An IDR pipeline execution failed.');
END;

-- Enable alerting when ready:
-- ALTER TASK idr_alert_on_failure_task RESUME;


-- ============================================
-- 6. TASK MONITORING QUERIES
-- ============================================

-- Check task definitions
SHOW TASKS LIKE 'IDR_%';

-- Check execution history of IDR tasks
SELECT
  NAME,
  STATE,
  SCHEDULED_TIME,
  COMPLETED_TIME,
  RETURN_VALUE,
  ERROR_CODE,
  ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME LIKE 'IDR_%'
ORDER BY SCHEDULED_TIME DESC
LIMIT 50;
