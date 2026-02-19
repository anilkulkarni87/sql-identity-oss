{{
    config(
        materialized='table',
        tags=['idr', 'marts', 'output']
    )
}}

{#
  Output model: Persistent identity edges.
  Matches the schema of identity_edges_current in the SQL implementation.
#}

select
    'rule_' || identifier_type as rule_id,
    left_entity_key,
    right_entity_key,
    identifier_type,
    identifier_value_norm,
    created_at as first_seen_ts,
    created_at as last_seen_ts
from {{ ref('int_edges') }}
