{{
    config(
        materialized='table',
        tags=['idr', 'marts', 'observability']
    )
}}

{#
  Rule match audit: Counts edges created per rule/identifier type.
  Matches rule_match_audit_current in SQL implementation.
#}

select
    '{{ invocation_id }}' as run_id,
    'rule_' || identifier_type as rule_id,
    count(*) as edges_created,
    min(created_at) as started_at,
    max(created_at) as ended_at
from {{ ref('int_edges') }}
group by identifier_type
