-- Test: Case-insensitive email matching
-- UPPER case email should match lower case email
-- Tests the canonicalize=LOWERCASE rule

-- Setup: Create test source tables with different email cases
CREATE OR REPLACE TABLE idr_test.case_upper AS
SELECT
  'CASE_UP' AS entity_id,
  'JOHN.DOE@EXAMPLE.COM' AS email,  -- UPPERCASE
  NULL AS phone,
  TIMESTAMP('2024-01-01 10:00:00') AS updated_at;

CREATE OR REPLACE TABLE idr_test.case_lower AS
SELECT
  'CASE_LOW' AS entity_id,
  'john.doe@example.com' AS email,  -- lowercase
  NULL AS phone,
  TIMESTAMP('2024-01-02 10:00:00') AS updated_at;

CREATE OR REPLACE TABLE idr_test.case_mixed AS
SELECT
  'CASE_MIX' AS entity_id,
  'John.Doe@Example.COM' AS email,  -- Mixed case
  NULL AS phone,
  TIMESTAMP('2024-01-03 10:00:00') AS updated_at;

-- Expected: All three should be in the same cluster because:
-- - Rule has canonicalize=LOWERCASE
-- - All emails normalize to 'john.doe@example.com'

-- Assertions (run after IDR):
-- SELECT COUNT(DISTINCT resolved_id) AS unique_clusters
-- FROM idr_out.identity_resolved_membership_current
-- WHERE entity_key IN ('case_upper:CASE_UP', 'case_lower:CASE_LOW', 'case_mixed:CASE_MIX');
-- Expected: 1

-- Verify normalized value in edges:
-- SELECT DISTINCT identifier_value_norm FROM idr_out.identity_edges_current
-- WHERE identifier_type = 'EMAIL'
-- AND (left_entity_key LIKE 'case_%' OR right_entity_key LIKE 'case_%');
-- Expected: 'john.doe@example.com' (all lowercase)
