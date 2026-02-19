{{
    config(
        materialized='table',
        tags=['idr', 'marts', 'observability']
    )
}}

{#
  Skipped identifier groups model: Audit of groups that exceeded max_group_size.
  Helps users identify problematic identifier values (e.g., shared emails).
#}

with identifier_groups as (
    select
        identifier_type,
        identifier_value_norm,
        max_group_size,
        count(*) as group_size,
        min(entity_key) as sample_entity_key
    from {{ ref('stg_identifiers') }}
    where identifier_value_norm is not null
    group by identifier_type, identifier_value_norm, max_group_size
),

skipped_groups as (
    select
        '{{ invocation_id }}' as run_id,
        identifier_type,
        identifier_value_norm,
        group_size,
        max_group_size as max_allowed,
        sample_entity_key,
        'EXCEEDED_MAX_GROUP_SIZE' as reason,
        current_timestamp as skipped_at
    from identifier_groups
    where group_size > max_group_size
)

select * from skipped_groups
