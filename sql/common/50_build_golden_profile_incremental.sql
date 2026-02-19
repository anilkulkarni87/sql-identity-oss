-- Incremental golden profile rebuild for impacted resolved_ids.
-- Supports survivorship strategies: MOST_RECENT (default), MOST_FREQUENT, FIRST_SEEN
-- Reads strategy from idr_meta.survivorship_rule table.
--
-- Assumes a canonical entities table `idr_work.entities_all` exists with entity attributes:
--   entity_key, table_id, record_updated_at, first_name, last_name, email_raw, phone_raw
--
-- NOTE: This version uses MOST_RECENT as default since Spark/Snowflake SQL doesn't easily
--       support reading metadata dynamically. For MOST_FREQUENT or FIRST_SEEN, the Python
--       wrapper should pre-process the survivorship_rule table and substitute the CTEs.

DROP TABLE IF EXISTS idr_work.golden_updates;

-- Precompute value frequencies for MOST_FREQUENT (used if configured)
DROP TABLE IF EXISTS idr_work.attr_value_counts;
CREATE TABLE idr_work.attr_value_counts AS
WITH impacted AS (
  SELECT DISTINCT resolved_id FROM idr_work.impacted_resolved_ids
),
members AS (
  SELECT m.resolved_id, m.entity_key
  FROM idr_out.identity_resolved_membership_current m
  JOIN impacted i ON i.resolved_id = m.resolved_id
),
ent AS (
  SELECT e.*, m.resolved_id
  FROM idr_work.entities_all e
  JOIN members m ON m.entity_key = e.entity_key
)
SELECT
  resolved_id,
  'email' AS attr_type,
  email_raw AS attr_value,
  COUNT(*) AS cnt,
  MIN(COALESCE(record_updated_at, TIMESTAMP '1900-01-01')) AS first_seen,
  MAX(COALESCE(record_updated_at, TIMESTAMP '1900-01-01')) AS last_seen
FROM ent WHERE email_raw IS NOT NULL
GROUP BY resolved_id, email_raw
UNION ALL
SELECT resolved_id, 'phone', phone_raw, COUNT(*), MIN(COALESCE(record_updated_at, TIMESTAMP '1900-01-01')), MAX(COALESCE(record_updated_at, TIMESTAMP '1900-01-01'))
FROM ent WHERE phone_raw IS NOT NULL GROUP BY resolved_id, phone_raw
UNION ALL
SELECT resolved_id, 'first_name', first_name, COUNT(*), MIN(COALESCE(record_updated_at, TIMESTAMP '1900-01-01')), MAX(COALESCE(record_updated_at, TIMESTAMP '1900-01-01'))
FROM ent WHERE first_name IS NOT NULL GROUP BY resolved_id, first_name
UNION ALL
SELECT resolved_id, 'last_name', last_name, COUNT(*), MIN(COALESCE(record_updated_at, TIMESTAMP '1900-01-01')), MAX(COALESCE(record_updated_at, TIMESTAMP '1900-01-01'))
FROM ent WHERE last_name IS NOT NULL GROUP BY resolved_id, last_name;

-- Read survivorship rules into a temp reference table
DROP TABLE IF EXISTS idr_work.survivorship_config;
CREATE TABLE idr_work.survivorship_config AS
SELECT
  attribute_name,
  COALESCE(UPPER(strategy), 'MOST_RECENT') AS strategy
FROM idr_meta.survivorship_rule
WHERE attribute_name IN ('email', 'phone', 'first_name', 'last_name');

-- Build golden profile using strategy-aware ranking
CREATE TABLE idr_work.golden_updates AS
WITH impacted AS (
  SELECT DISTINCT resolved_id FROM idr_work.impacted_resolved_ids
),
-- Get configured strategies (with MOST_RECENT as default)
strategies AS (
  SELECT
    COALESCE(MAX(CASE WHEN attribute_name = 'email' THEN strategy END), 'MOST_RECENT') AS email_strategy,
    COALESCE(MAX(CASE WHEN attribute_name = 'phone' THEN strategy END), 'MOST_RECENT') AS phone_strategy,
    COALESCE(MAX(CASE WHEN attribute_name = 'first_name' THEN strategy END), 'MOST_RECENT') AS first_name_strategy,
    COALESCE(MAX(CASE WHEN attribute_name = 'last_name' THEN strategy END), 'MOST_RECENT') AS last_name_strategy
  FROM idr_work.survivorship_config
),
-- Rank email values based on strategy
email_ranked AS (
  SELECT
    ac.resolved_id,
    ac.attr_value AS email_raw,
    ROW_NUMBER() OVER (
      PARTITION BY ac.resolved_id
      ORDER BY
        CASE s.email_strategy
          WHEN 'MOST_FREQUENT' THEN ac.cnt
          WHEN 'FIRST_SEEN' THEN -UNIX_TIMESTAMP(ac.first_seen)
          ELSE UNIX_TIMESTAMP(ac.last_seen)  -- MOST_RECENT
        END DESC,
        ac.attr_value
    ) AS rn
  FROM idr_work.attr_value_counts ac
  CROSS JOIN strategies s
  WHERE ac.attr_type = 'email'
),
-- Rank phone values
phone_ranked AS (
  SELECT
    ac.resolved_id,
    ac.attr_value AS phone_raw,
    ROW_NUMBER() OVER (
      PARTITION BY ac.resolved_id
      ORDER BY
        CASE s.phone_strategy
          WHEN 'MOST_FREQUENT' THEN ac.cnt
          WHEN 'FIRST_SEEN' THEN -UNIX_TIMESTAMP(ac.first_seen)
          ELSE UNIX_TIMESTAMP(ac.last_seen)
        END DESC,
        ac.attr_value
    ) AS rn
  FROM idr_work.attr_value_counts ac
  CROSS JOIN strategies s
  WHERE ac.attr_type = 'phone'
),
-- Rank first_name values
first_name_ranked AS (
  SELECT
    ac.resolved_id,
    ac.attr_value AS first_name,
    ROW_NUMBER() OVER (
      PARTITION BY ac.resolved_id
      ORDER BY
        CASE s.first_name_strategy
          WHEN 'MOST_FREQUENT' THEN ac.cnt
          WHEN 'FIRST_SEEN' THEN -UNIX_TIMESTAMP(ac.first_seen)
          ELSE UNIX_TIMESTAMP(ac.last_seen)
        END DESC,
        ac.attr_value
    ) AS rn
  FROM idr_work.attr_value_counts ac
  CROSS JOIN strategies s
  WHERE ac.attr_type = 'first_name'
),
-- Rank last_name values
last_name_ranked AS (
  SELECT
    ac.resolved_id,
    ac.attr_value AS last_name,
    ROW_NUMBER() OVER (
      PARTITION BY ac.resolved_id
      ORDER BY
        CASE s.last_name_strategy
          WHEN 'MOST_FREQUENT' THEN ac.cnt
          WHEN 'FIRST_SEEN' THEN -UNIX_TIMESTAMP(ac.first_seen)
          ELSE UNIX_TIMESTAMP(ac.last_seen)
        END DESC,
        ac.attr_value
    ) AS rn
  FROM idr_work.attr_value_counts ac
  CROSS JOIN strategies s
  WHERE ac.attr_type = 'last_name'
)
-- Final join to get best values per resolved_id
SELECT
  i.resolved_id,
  e.email_raw AS email_primary,
  p.phone_raw AS phone_primary,
  f.first_name,
  l.last_name,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS updated_ts
FROM impacted i
LEFT JOIN email_ranked e ON e.resolved_id = i.resolved_id AND e.rn = 1
LEFT JOIN phone_ranked p ON p.resolved_id = i.resolved_id AND p.rn = 1
LEFT JOIN first_name_ranked f ON f.resolved_id = i.resolved_id AND f.rn = 1
LEFT JOIN last_name_ranked l ON l.resolved_id = i.resolved_id AND l.rn = 1;
