-- Builds idr_work.identifiers_delta for this run using configured rules and source tables.
-- Output schema:
--   run_id, table_id, entity_key, identifier_type, identifier_value_norm, is_hashed, watermark_value

DROP TABLE IF EXISTS idr_work.identifiers_delta;

CREATE TABLE idr_work.identifiers_delta AS
SELECT
  '${RUN_ID}' AS run_id,
  e.table_id,
  e.entity_key,
  r.identifier_type,
  CASE
    WHEN r.canonicalize = 'LOWERCASE' THEN LOWER(i.identifier_value)
    ELSE i.identifier_value
  END AS identifier_value_norm,
  i.is_hashed,
  e.watermark_value
FROM idr_work.entities_delta e
JOIN idr_meta.rule r
  ON r.is_active = TRUE
JOIN (
  -- Adapter will generate unioned identifier extracts per source table.
  -- This placeholder assumes an intermediate table idr_work.identifiers_extracted exists:
  SELECT table_id, entity_key, identifier_type, identifier_value, is_hashed
  FROM idr_work.identifiers_extracted
) i
  ON i.table_id = e.table_id
 AND i.entity_key = e.entity_key
 AND i.identifier_type = r.identifier_type
WHERE (r.require_non_null = FALSE OR i.identifier_value IS NOT NULL);
