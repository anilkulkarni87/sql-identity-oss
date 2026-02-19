-- Test: Two disjoint groups should remain separate clusters
-- Group 1: D1, D2 share email
-- Group 2: D3, D4 share phone (different from group 1)
-- Expected: Two distinct clusters

-- Setup: Create test source tables
CREATE OR REPLACE TABLE idr_test.disjoint_1 AS
SELECT 'D1' AS entity_id, 'group1@example.com' AS email, NULL AS phone, TIMESTAMP('2024-01-01') AS updated_at
UNION ALL
SELECT 'D2' AS entity_id, 'group1@example.com' AS email, NULL AS phone, TIMESTAMP('2024-01-01') AS updated_at;

CREATE OR REPLACE TABLE idr_test.disjoint_2 AS
SELECT 'D3' AS entity_id, 'group2@example.com' AS email, '5553334444' AS phone, TIMESTAMP('2024-01-01') AS updated_at
UNION ALL
SELECT 'D4' AS entity_id, 'group2-other@example.com' AS email, '5553334444' AS phone, TIMESTAMP('2024-01-01') AS updated_at;

-- Expected graph:
--   D1 ----(email)---- D2          D3 ----(phone)---- D4
--   [Cluster 1]                    [Cluster 2]
--
-- No edges between the two groups -> Two separate clusters

-- Assertions (run after IDR):
-- SELECT COUNT(DISTINCT resolved_id) AS unique_clusters
-- FROM idr_out.identity_resolved_membership_current
-- WHERE entity_key IN ('disjoint_1:D1', 'disjoint_1:D2', 'disjoint_2:D3', 'disjoint_2:D4');
-- Expected: 2

-- Cluster 1 check:
-- SELECT COUNT(DISTINCT resolved_id) FROM idr_out.identity_resolved_membership_current
-- WHERE entity_key IN ('disjoint_1:D1', 'disjoint_1:D2');
-- Expected: 1

-- Cluster 2 check:
-- SELECT COUNT(DISTINCT resolved_id) FROM idr_out.identity_resolved_membership_current
-- WHERE entity_key IN ('disjoint_2:D3', 'disjoint_2:D4');
-- Expected: 1
