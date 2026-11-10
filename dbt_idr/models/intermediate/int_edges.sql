{{
    config(
        materialized='table',
        tags=['idr', 'intermediate']
    )
}}

{#
  Intermediate model: Build edges between entities sharing identifiers.

  This model:
  1. Applies ISOLATE overrides from idr_overrides before grouping
  2. Groups entities by identifier value
  3. Filters groups exceeding max_group_size
  4. Creates edges using anchor-based approach (MIN entity as anchor)
  5. Applies DO_NOT_LINK and FORCE_LINK overrides from idr_overrides
#}

with active_overrides as (
    select
        override_id,
        upper(override_type) as override_type,
        entity_key_a,
        entity_key_b,
        coalesce(reason, 'Manual stewardship override') as reason
    from {{ ref('idr_overrides') }}
    where cast(is_active as {{ dbt.type_string() }}) in ('true', 'TRUE', '1')
),

filtered_identifiers as (
    select i.*
    from {{ ref('stg_identifiers') }} i
    where i.identifier_value_norm is not null
      and not exists (
          select 1
          from active_overrides ov
          where ov.override_type = 'ISOLATE'
            and ov.entity_key_a = i.entity_key
      )
),

identifier_groups as (
    -- Count entities per identifier value
    select
        identifier_type,
        identifier_value_norm,
        max_group_size,
        count(*) as group_size,
        min(entity_key) as anchor_entity_key
    from filtered_identifiers
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

raw_edges as (
    -- Create edges from each entity to the anchor
    select distinct
        vg.anchor_entity_key as left_entity_key,
        i.entity_key as right_entity_key,
        vg.identifier_type,
        vg.identifier_value_norm,
        current_timestamp as created_at
    from valid_groups vg
    inner join filtered_identifiers i
        on i.identifier_type = vg.identifier_type
        and i.identifier_value_norm = vg.identifier_value_norm
    where i.entity_key != vg.anchor_entity_key
),

blocked_shared_groups as (
    select distinct
        case when ov.entity_key_a > ov.entity_key_b then ov.entity_key_a else ov.entity_key_b end as detach_entity_key,
        i1.identifier_type,
        i1.identifier_value_norm
    from active_overrides ov
    inner join filtered_identifiers i1
        on i1.entity_key = ov.entity_key_a
    inner join filtered_identifiers i2
        on i2.entity_key = ov.entity_key_b
        and i2.identifier_type = i1.identifier_type
        and i2.identifier_value_norm = i1.identifier_value_norm
    where ov.override_type = 'DO_NOT_LINK'
),

filtered_edges as (
    select e.*
    from raw_edges e
    where not exists (
        select 1
        from active_overrides dnl
        where dnl.override_type = 'DO_NOT_LINK'
          and (
              (e.left_entity_key = dnl.entity_key_a and e.right_entity_key = dnl.entity_key_b)
              or (e.left_entity_key = dnl.entity_key_b and e.right_entity_key = dnl.entity_key_a)
          )
    )
    and not exists (
        select 1
        from blocked_shared_groups bsg
        where (e.left_entity_key = bsg.detach_entity_key or e.right_entity_key = bsg.detach_entity_key)
          and e.identifier_type = bsg.identifier_type
          and e.identifier_value_norm = bsg.identifier_value_norm
    )
),

force_link_edges as (
    select distinct
        case when entity_key_a < entity_key_b then entity_key_a else entity_key_b end as left_entity_key,
        case when entity_key_a < entity_key_b then entity_key_b else entity_key_a end as right_entity_key,
        cast('MANUAL_OVERRIDE' as {{ dbt.type_string() }}) as identifier_type,
        cast('FORCE_LINK' as {{ dbt.type_string() }}) as identifier_value_norm,
        current_timestamp as created_at
    from active_overrides
    where override_type = 'FORCE_LINK'
      and entity_key_a is not null
      and entity_key_b is not null
      and entity_key_a != entity_key_b
      and not exists (
          select 1
          from filtered_edges fe
          where (fe.left_entity_key = entity_key_a and fe.right_entity_key = entity_key_b)
             or (fe.left_entity_key = entity_key_b and fe.right_entity_key = entity_key_a)
      )
),

edges as (
    select * from filtered_edges
    union all
    select * from force_link_edges
)

select * from edges
