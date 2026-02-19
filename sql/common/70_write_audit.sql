-- Write audit summary for edges created per rule for this run.
DROP TABLE IF EXISTS idr_work.audit_edges_created;
CREATE TABLE idr_work.audit_edges_created AS
SELECT
  CAST('${RUN_ID}' AS STRING) AS run_id,
  rule_id,
  COUNT(*) AS edges_created,
  CAST('${RUN_TS}' AS TIMESTAMP) AS started_at,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS ended_at
FROM idr_work.edges_new
GROUP BY rule_id;

-- Adapter MERGE/REPLACE into idr_out.rule_match_audit_current (run_id + rule_id).
