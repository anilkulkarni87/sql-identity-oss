-- Recompute cluster sizes for impacted resolved_ids.
-- impacted resolved_ids are those present in membership_updates plus neighbors that may have changed.
DROP TABLE IF EXISTS idr_work.impacted_resolved_ids;
CREATE TABLE idr_work.impacted_resolved_ids AS
SELECT DISTINCT resolved_id FROM idr_work.membership_updates;

DROP TABLE IF EXISTS idr_work.cluster_sizes_updates;
CREATE TABLE idr_work.cluster_sizes_updates AS
SELECT
  m.resolved_id,
  COUNT(*) AS cluster_size,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_ts
FROM idr_out.identity_resolved_membership_current m
JOIN idr_work.impacted_resolved_ids i ON i.resolved_id = m.resolved_id
GROUP BY m.resolved_id;

-- Adapter MERGE into idr_out.identity_clusters_current by resolved_id.
