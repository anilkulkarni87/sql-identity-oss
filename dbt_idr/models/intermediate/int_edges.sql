{{
    config(
        materialized='table',
        tags=['idr', 'intermediate']
    )
}}

{#
  Intermediate model: Build edges between entities sharing identifiers.

  This model:
  1. Groups entities by identifier value
  2. Filters groups exceeding max_group_size
  3. Creates edges using anchor-based approach (MIN entity as anchor)
#}

with identifier_groups as (
    -- Count entities per identifier value
    select
        identifier_type,
        identifier_value_norm,
        max_group_size,
        count(*) as group_size,
        min(entity_key) as anchor_entity_key
    from {{ ref('stg_identifiers') }}
    where identifier_value_norm is not null
    group by identifier_type, identifier_value_norm, max_group_size
),

valid_groups as (
    -- Filter out oversized groups
    select
        identifier_type,
        identifier_value_norm,
        anchor_entity_key
    from identifier_groups
    where group_size <= max_group_size
),

edges as (
    -- Create edges from each entity to the anchor
    select distinct
        vg.anchor_entity_key as left_entity_key,
        i.entity_key as right_entity_key,
        vg.identifier_type,
        vg.identifier_value_norm,
        current_timestamp as created_at
    from valid_groups vg
    inner join {{ ref('stg_identifiers') }} i
        on i.identifier_type = vg.identifier_type
        and i.identifier_value_norm = vg.identifier_value_norm
    where i.entity_key != vg.anchor_entity_key
)

select * from edges
