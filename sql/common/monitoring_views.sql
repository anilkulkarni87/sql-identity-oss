-- Monitoring views for identity resolution health
-- Run after observability DDL

-- Recent run summary
CREATE OR REPLACE VIEW idr_out.v_run_summary AS
SELECT
  run_id,
  run_mode,
  status,
  started_at,
  ended_at,
  duration_seconds,
  entities_processed,
  edges_created,
  clusters_impacted,
  lp_iterations,
  source_tables_processed,
  error_message
FROM idr_out.run_history
ORDER BY started_at DESC;

-- Cluster health: size distribution
CREATE OR REPLACE VIEW idr_out.v_cluster_health AS
SELECT
  CASE
    WHEN cluster_size = 1 THEN '01_singleton'
    WHEN cluster_size <= 5 THEN '02_small (2-5)'
    WHEN cluster_size <= 20 THEN '03_medium (6-20)'
    WHEN cluster_size <= 100 THEN '04_large (21-100)'
    WHEN cluster_size <= 1000 THEN '05_xlarge (101-1000)'
    ELSE '06_giant (1000+)'
  END AS size_bucket,
  COUNT(*) AS cluster_count,
  SUM(cluster_size) AS total_entities,
  MIN(cluster_size) AS min_size,
  MAX(cluster_size) AS max_size,
  AVG(cluster_size) AS avg_size
FROM idr_out.identity_clusters_current
GROUP BY 1
ORDER BY 1;

-- Identifier quality issues
CREATE OR REPLACE VIEW idr_out.v_identifier_issues AS
SELECT
  identifier_type,
  identifier_value_norm,
  COUNT(*) AS entity_count,
  CASE
    WHEN COUNT(*) > 10000 THEN 'CRITICAL: Excessive matches (>10K)'
    WHEN COUNT(*) > 1000 THEN 'WARNING: High matches (>1K)'
    WHEN COUNT(*) > 100 THEN 'INFO: Elevated matches (>100)'
    ELSE 'OK'
  END AS severity,
  'Review identifier quality - may indicate data issue' AS recommendation
FROM idr_work.identifiers_all
WHERE identifier_value_norm IS NOT NULL
GROUP BY 1, 2
HAVING COUNT(*) > 100
ORDER BY entity_count DESC;

-- Edge creation by rule
CREATE OR REPLACE VIEW idr_out.v_rule_effectiveness AS
SELECT
  r.rule_id,
  r.rule_name,
  r.identifier_type,
  COALESCE(a.total_edges, 0) AS total_edges_created,
  COALESCE(a.recent_edges, 0) AS edges_last_run,
  r.priority
FROM idr_meta.rule r
LEFT JOIN (
  SELECT
    rule_id,
    SUM(edges_created) AS total_edges,
    MAX(CASE WHEN run_id = (SELECT MAX(run_id) FROM idr_out.rule_match_audit_current)
        THEN edges_created ELSE 0 END) AS recent_edges
  FROM idr_out.rule_match_audit_current
  GROUP BY rule_id
) a ON a.rule_id = r.rule_id
WHERE r.is_active = true
ORDER BY r.priority;

-- Run performance trends
CREATE OR REPLACE VIEW idr_out.v_run_trends AS
SELECT
  DATE(started_at) AS run_date,
  COUNT(*) AS run_count,
  SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
  SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
  AVG(duration_seconds) AS avg_duration_seconds,
  SUM(entities_processed) AS total_entities,
  SUM(edges_created) AS total_edges
FROM idr_out.run_history
GROUP BY 1
ORDER BY 1 DESC;
