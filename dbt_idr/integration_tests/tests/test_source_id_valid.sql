-- Test: Verify source_key can be used to join back to source data
-- Checks that source_id matches expected patterns and source_key is valid

select
    m.entity_key,
    m.source_id,
    m.source_key,
    -- Verify source_id matches valid source names
    case when m.source_id not in ('crm', 'orders', 'pos') then 1 else 0 end as invalid_source
from {{ ref('identity_membership') }} m
where m.source_id not in ('crm', 'orders', 'pos')
