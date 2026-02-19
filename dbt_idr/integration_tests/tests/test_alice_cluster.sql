-- Test: Verify Alice cluster has multiple members
-- alice@example.com appears in CRM rows 1 and 4, and orders

with alice_cluster as (
    select m.resolved_id, count(*) as member_count
    from {{ ref('identity_membership') }} m
    where m.entity_key like 'crm:1' or m.entity_key like 'crm:4'
       or m.entity_key like 'orders:101' or m.entity_key like 'orders:106'
    group by m.resolved_id
)
select 'FAIL: Alice cluster should have multiple members' as error
from alice_cluster
where member_count < 2
