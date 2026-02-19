-- Performance Optimization: Table Clustering and Z-Ordering
-- Run after initial population to optimize query performance
-- Recommended for datasets > 1M rows

-- ============================================
-- IDENTITY EDGES
-- ============================================
-- Optimize for lookups by identifier type and value
OPTIMIZE idr_out.identity_edges_current
ZORDER BY (identifier_type, identifier_value_norm);

-- ============================================
-- IDENTIFIERS (Work table)
-- ============================================
-- Optimize for edge building joins
OPTIMIZE idr_work.identifiers_all
ZORDER BY (identifier_type, identifier_value_norm);

-- ============================================
-- MEMBERSHIP
-- ============================================
-- Optimize for cluster lookups
OPTIMIZE idr_out.identity_resolved_membership_current
ZORDER BY (resolved_id);

-- ============================================
-- GOLDEN PROFILE
-- ============================================
OPTIMIZE idr_out.golden_profile_current
ZORDER BY (resolved_id);

-- ============================================
-- RUN HISTORY
-- ============================================
-- Optimize for recent run queries
OPTIMIZE idr_out.run_history
ZORDER BY (started_at);

-- ============================================
-- RECOMMENDED SPARK CONFIGS
-- ============================================
-- Add these to your cluster or job configuration for large datasets:
--
-- # Increase shuffle partitions for > 10M rows
-- spark.conf.set("spark.sql.shuffle.partitions", "200")
--
-- # Enable adaptive query execution
-- spark.conf.set("spark.sql.adaptive.enabled", "true")
-- spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
--
-- # Optimize Delta writes
-- spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
-- spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
--
-- # For very large clusters
-- spark.conf.set("spark.sql.shuffle.partitions", "400")

-- ============================================
-- VACUUM OLD FILES (Run periodically)
-- ============================================
-- Remove old Delta log files to reduce storage
-- VACUUM idr_out.identity_edges_current RETAIN 168 HOURS;
-- VACUUM idr_out.identity_resolved_membership_current RETAIN 168 HOURS;
-- VACUUM idr_out.golden_profile_current RETAIN 168 HOURS;

-- ============================================
-- ANALYZE TABLES (Update statistics)
-- ============================================
ANALYZE TABLE idr_out.identity_edges_current COMPUTE STATISTICS;
ANALYZE TABLE idr_out.identity_resolved_membership_current COMPUTE STATISTICS;
ANALYZE TABLE idr_out.identity_clusters_current COMPUTE STATISTICS;
ANALYZE TABLE idr_out.golden_profile_current COMPUTE STATISTICS;
