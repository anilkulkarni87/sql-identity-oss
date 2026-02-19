-- Test: Chain of three entities linked through different identifiers
-- A and B share email, B and C share phone -> All should be in same cluster
-- This tests transitivity of identity resolution

-- Setup: Create test source tables
CREATE OR REPLACE TABLE idr_test.chain_a AS
SELECT
  'CHAIN_A' AS entity_id,
  'alice@example.com' AS email,
  NULL AS phone,  -- No phone for A
  TIMESTAMP('2024-01-01 10:00:00') AS updated_at;

CREATE OR REPLACE TABLE idr_test.chain_b AS
SELECT
  'CHAIN_B' AS entity_id,
  'alice@example.com' AS email,  -- Same email as A
  '5551112222' AS phone,         -- Has phone
  TIMESTAMP('2024-01-02 10:00:00') AS updated_at;

CREATE OR REPLACE TABLE idr_test.chain_c AS
SELECT
  'CHAIN_C' AS entity_id,
  'charlie@example.com' AS email,  -- Different email
  '5551112222' AS phone,            -- Same phone as B
  TIMESTAMP('2024-01-03 10:00:00') AS updated_at;

-- Expected graph:
--   A ----(email)---- B ----(phone)---- C
--
-- All three should get the same resolved_id because:
-- - A and B are linked via email
-- - B and C are linked via phone
-- - Transitivity: A-B-C form a connected component

-- Assertions (run after IDR):
-- SELECT COUNT(DISTINCT resolved_id) AS unique_clusters
-- FROM idr_out.identity_resolved_membership_current
-- WHERE entity_key IN ('chain_a:CHAIN_A', 'chain_b:CHAIN_B', 'chain_c:CHAIN_C');
-- Expected: 1 (all in same cluster)

-- SELECT COUNT(*) AS edge_count FROM idr_out.identity_edges_current
-- WHERE (left_entity_key LIKE 'chain_%' OR right_entity_key LIKE 'chain_%');
-- Expected: 2 (A-B via email, B-C or anchor edges)
