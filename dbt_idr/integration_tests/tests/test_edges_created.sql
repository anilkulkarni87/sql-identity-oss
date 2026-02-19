-- Test: Verify edges were created
-- Should have edges from shared emails/phones

with edge_count as (
    select count(*) as cnt from {{ ref('identity_edges') }}
)
select 'FAIL: No edges created' as error
from edge_count
where cnt = 0
