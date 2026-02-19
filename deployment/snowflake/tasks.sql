-- Snowflake Task for IDR Scheduling
-- Creates tasks to run IDR hourly and cleanup old data

-- ============================================
-- HOURLY INCREMENTAL TASK
-- ============================================

CREATE OR REPLACE TASK idr_hourly_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '60 MINUTE'
AS
  CALL idr_run('INCR', 30, FALSE);

-- Enable the task
ALTER TASK idr_hourly_task RESUME;


-- ============================================
-- WEEKLY FULL RUN TASK
-- ============================================

CREATE OR REPLACE TASK idr_weekly_full_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 2 * * 0 UTC'  -- Sunday 2am UTC
AS
  CALL idr_run('FULL', 30, FALSE);

-- Enable the task
ALTER TASK idr_weekly_full_task RESUME;


-- ============================================
-- DAILY CLEANUP TASK
-- ============================================

CREATE OR REPLACE TASK idr_daily_cleanup_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 3 * * * UTC'  -- Daily 3am UTC
AS
BEGIN
  -- Get retention days from config
  LET retention_days INT := (
    SELECT TRY_CAST(config_value AS INT)
    FROM idr_meta.config
    WHERE config_key = 'dry_run_retention_days'
  );

  IF (retention_days IS NULL) THEN
    retention_days := 7;
  END IF;

  -- Clean up old dry run results
  DELETE FROM idr_out.dry_run_results
  WHERE created_at < DATEADD('day', -retention_days, CURRENT_TIMESTAMP);

  DELETE FROM idr_out.dry_run_summary
  WHERE created_at < DATEADD('day', -retention_days, CURRENT_TIMESTAMP);

  -- Clean up old metrics (90 days)
  DELETE FROM idr_out.metrics_export
  WHERE recorded_at < DATEADD('day', -90, CURRENT_TIMESTAMP);

  -- Clean up old skipped groups (30 days)
  DELETE FROM idr_out.skipped_identifier_groups
  WHERE skipped_at < DATEADD('day', -30, CURRENT_TIMESTAMP);

END;

-- Enable the task
ALTER TASK idr_daily_cleanup_task RESUME;


-- ============================================
-- MONITORING QUERIES
-- ============================================

-- View task status
SHOW TASKS LIKE 'IDR%';

-- View task history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME LIKE 'IDR_%'
ORDER BY SCHEDULED_TIME DESC
LIMIT 50;

-- View scheduled runs
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'IDR_HOURLY_TASK'
  AND STATE = 'SCHEDULED'
ORDER BY SCHEDULED_TIME;


-- ============================================
-- ALERTING STREAM AND TASK
-- ============================================

-- Create stream on run_history for alerting
CREATE OR REPLACE STREAM idr_run_history_stream ON TABLE idr_out.run_history;

-- Create task to send alerts on failures
CREATE OR REPLACE TASK idr_alert_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('idr_run_history_stream')
AS
BEGIN
  -- Check for failures
  LET failures RESULTSET := (
    SELECT run_id, status, ended_at
    FROM idr_run_history_stream
    WHERE status = 'FAILED'
    AND METADATA$ACTION = 'INSERT'
  );

  -- Send email for each failure (requires email integration)
  -- CALL SYSTEM$SEND_EMAIL(
  --   'idr_alerts',
  --   'alerts@company.com',
  --   'IDR Run Failed',
  --   'Check run_history for details'
  -- );

  -- Log alert
  INSERT INTO idr_out.metrics_export (run_id, metric_name, metric_value, dimensions, recorded_at)
  SELECT
    run_id,
    'idr_alert_sent',
    1,
    OBJECT_CONSTRUCT('status', status),
    CURRENT_TIMESTAMP
  FROM TABLE(failures);

END;

-- ALTER TASK idr_alert_task RESUME;
