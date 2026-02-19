-- After convergence, lp_labels contains final labels for subgraph nodes.
-- We define resolved_id = label (deterministic for current graph state).
-- Update membership_current for subgraph_nodes AND singletons (entities with no edges).
DROP TABLE IF EXISTS idr_work.membership_updates;
CREATE TABLE idr_work.membership_updates AS
SELECT
  l.entity_key,
  l.label AS resolved_id,
  split(l.entity_key, ':')[0] AS source_id,
  split(l.entity_key, ':')[1] AS source_key,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_ts
FROM idr_work.lp_labels l
UNION ALL
-- Include singletons (entities with no edges) - they get resolved_id = entity_key
SELECT
  e.entity_key,
  e.entity_key AS resolved_id,
  split(e.entity_key, ':')[0] AS source_id,
  split(e.entity_key, ':')[1] AS source_key,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_ts
FROM idr_work.entities_delta e
WHERE e.entity_key NOT IN (SELECT entity_key FROM idr_work.lp_labels);

-- Adapter should MERGE membership_updates into idr_out.identity_resolved_membership_current
-- using entity_key as the key.
