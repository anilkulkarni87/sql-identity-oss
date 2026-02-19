-- Test: Verify clusters were created
-- Expected: Multiple clusters from matching identifiers

with cluster_count as (
    select count(*) as cnt from {{ ref('identity_clusters') }}
)
select 'FAIL: No clusters created' as error
from cluster_count
where cnt = 0
