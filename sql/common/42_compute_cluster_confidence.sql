-- Compute cluster confidence metrics for impacted clusters
-- Run AFTER 41_update_clusters_current.sql
--
-- Confidence Formula (50/35/15 weighting):
--   confidence = 0.5 * (edge_diversity / max_diversity) +
--                0.35 * match_density +
--                0.15 * size_factor
--
-- Singletons: confidence = 1.0, reason = 'SINGLETON_NO_MATCH_REQUIRED'

-- Step 1: Get edges per cluster for impacted clusters
DROP TABLE IF EXISTS idr_work.cluster_edge_stats;
CREATE TABLE idr_work.cluster_edge_stats AS
SELECT
    m.resolved_id,
    COUNT(DISTINCT e.identifier_type) AS edge_diversity,
    COUNT(*) AS edge_count
FROM idr_out.identity_resolved_membership_current m
JOIN idr_out.identity_edges_current e
  ON e.left_entity_key = m.entity_key OR e.right_entity_key = m.entity_key
WHERE m.resolved_id IN (SELECT resolved_id FROM idr_work.impacted_resolved_ids)
GROUP BY m.resolved_id;

-- Step 2: Compute density (edges / max_possible_edges)
-- For n nodes, max edges in star topology (our edge pattern) = n - 1
-- For fully connected graph = n*(n-1)/2
DROP TABLE IF EXISTS idr_work.cluster_density;
CREATE TABLE idr_work.cluster_density AS
SELECT
    c.resolved_id,
    c.cluster_size,
    COALESCE(es.edge_diversity, 0) AS edge_diversity,
    COALESCE(es.edge_count, 0) AS edge_count,
    CASE
        WHEN c.cluster_size <= 1 THEN 1.0  -- Singletons have perfect density
        ELSE LEAST(1.0, CAST(COALESCE(es.edge_count, 0) AS DOUBLE) /
             (CAST(c.cluster_size AS DOUBLE) - 1))  -- Star topology: max = n-1
    END AS match_density
FROM idr_work.cluster_sizes_updates c
LEFT JOIN idr_work.cluster_edge_stats es ON es.resolved_id = c.resolved_id;

-- Step 3: Get max edge diversity across all clusters for normalization
DROP TABLE IF EXISTS idr_work.max_diversity;
CREATE TABLE idr_work.max_diversity AS
SELECT GREATEST(1, MAX(edge_diversity)) AS max_div FROM idr_work.cluster_density;

-- Step 4: Compute weighted confidence score with primary reason
DROP TABLE IF EXISTS idr_work.cluster_confidence;
CREATE TABLE idr_work.cluster_confidence AS
SELECT
    cd.resolved_id,
    cd.cluster_size,
    cd.edge_diversity,
    cd.match_density,
    CASE
        WHEN cd.cluster_size = 1 THEN 1.0
        ELSE ROUND(
            0.50 * (CAST(cd.edge_diversity AS DOUBLE) / md.max_div) +
            0.35 * cd.match_density +
            0.15 * 1.0,  -- Non-singleton bonus
            3
        )
    END AS confidence_score,
    CASE
        WHEN cd.cluster_size = 1 THEN 'SINGLETON_NO_MATCH_REQUIRED'
        WHEN cd.edge_diversity >= 3 AND cd.match_density >= 0.8 THEN
            CAST(cd.edge_diversity AS VARCHAR) || ' identifier types, high density'
        WHEN cd.edge_diversity >= 2 AND cd.match_density >= 0.5 THEN
            CAST(cd.edge_diversity AS VARCHAR) || ' identifier types, moderate density'
        WHEN cd.edge_diversity = 1 AND cd.match_density >= 0.8 THEN
            'Single identifier type, high density'
        WHEN cd.edge_diversity = 1 AND cd.match_density < 0.5 THEN
            'Single identifier type, chain pattern'
        ELSE
            CAST(cd.edge_diversity AS VARCHAR) || ' identifier type(s), ' ||
            CASE WHEN cd.match_density >= 0.5 THEN 'moderate' ELSE 'low' END || ' density'
    END AS primary_reason,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_ts
FROM idr_work.cluster_density cd
CROSS JOIN idr_work.max_diversity md;

-- Adapter: MERGE cluster_confidence into idr_out.identity_clusters_current
-- The adapter layer (DuckDB/Snowflake/BigQuery/Databricks) implements the platform-specific MERGE.
