-- Test: Verify source_id and source_key columns are correctly parsed
-- All membership records should have non-null source_id and source_key

select *
from {{ ref('identity_membership') }}
where source_id is null
   or source_key is null
   or source_id = ''
   or source_key = ''
