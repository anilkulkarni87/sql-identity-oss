-- One iteration step of label propagation on impacted subgraph.
-- Orchestrator repeats this step until delta_changed = 0 (or max iterations).

-- Initialization (run once per run before looping):
--   Create idr_work.lp_labels with each node labeled by itself.
-- Adapters / run examples show the initialization and looping pattern.

-- Compute next labels = min(self label and neighbor labels)
DROP TABLE IF EXISTS idr_work.lp_labels_next;
CREATE TABLE idr_work.lp_labels_next AS
WITH undirected AS (
  SELECT left_entity_key AS src, right_entity_key AS dst FROM idr_work.subgraph_edges
  UNION ALL
  SELECT right_entity_key AS src, left_entity_key AS dst FROM idr_work.subgraph_edges
),
candidate_labels AS (
  SELECT l.entity_key, l.label AS candidate_label
  FROM idr_work.lp_labels l
  UNION ALL
  SELECT u.src AS entity_key, l2.label AS candidate_label
  FROM undirected u
  JOIN idr_work.lp_labels l2 ON l2.entity_key = u.dst
)
SELECT
  entity_key,
  MIN(candidate_label) AS label
FROM candidate_labels
GROUP BY entity_key;

-- Stats for convergence
DROP TABLE IF EXISTS idr_work.lp_stats;
CREATE TABLE idr_work.lp_stats AS
SELECT
  CAST('${RUN_ID}' AS STRING) AS run_id,
  SUM(CASE WHEN cur.label <> nxt.label THEN 1 ELSE 0 END) AS delta_changed,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS computed_at
FROM idr_work.lp_labels cur
JOIN idr_work.lp_labels_next nxt USING (entity_key);

-- Swap tables (adapters may implement as RENAME)
-- After swap, next iteration runs again.
