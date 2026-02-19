-- Test: Verify golden profiles have email values
-- At least some profiles should have email_primary populated

with profiles_with_email as (
    select count(*) as cnt from {{ ref('golden_profiles') }}
    where email_primary is not null
)
select 'FAIL: No golden profiles have email' as error
from profiles_with_email
where cnt = 0
