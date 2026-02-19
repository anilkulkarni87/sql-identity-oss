-- Test: Two entities sharing the same email should be in the same cluster
-- Expected: Both entities get the same resolved_id

-- Setup: Create test source tables
CREATE OR REPLACE TABLE idr_test.source_a AS
SELECT
  'A001' AS entity_id,
  'shared@example.com' AS email,
  '5551234567' AS phone,
  TIMESTAMP('2024-01-01 10:00:00') AS updated_at;

CREATE OR REPLACE TABLE idr_test.source_b AS
SELECT
  'B001' AS entity_id,
  'shared@example.com' AS email,  -- Same email as A001
  '5559999999' AS phone,          -- Different phone
  TIMESTAMP('2024-01-02 10:00:00') AS updated_at;

-- Expected outputs after IDR run:
-- 1. Edge created: source_a:A001 <-> source_b:B001 via email
-- 2. Same resolved_id for both entities

-- Assertions (run after IDR):
-- SELECT COUNT(*) AS edge_count FROM idr_out.identity_edges_current
-- WHERE identifier_type = 'EMAIL' AND identifier_value_norm = 'shared@example.com';
-- Expected: 1

-- SELECT COUNT(DISTINCT resolved_id) AS unique_clusters
-- FROM idr_out.identity_resolved_membership_current
-- WHERE entity_key IN ('source_a:A001', 'source_b:B001');
-- Expected: 1
