{{
    config(
        materialized='table',
        tags=['idr', 'marts', 'dry_run'],
        enabled=var('idr_dry_run', false)
    )
}}

{#
  Dry run results: Compares proposed changes against current production state.

  Only runs when idr_dry_run=true.
  Shows per-entity changes: NEW, MOVED, UNCHANGED.
#}

{% set production_schema = target.schema ~ '_idr_out' %}

with proposed_membership as (
    select entity_key, resolved_id
    from {{ ref('identity_membership') }}
),

{# Try to read current production membership - may not exist #}
current_membership as (
    {% if execute %}
        {% set check_table %}
            select count(*) as cnt
            from information_schema.tables
            where table_schema = '{{ production_schema }}'
              and table_name = 'identity_membership'
        {% endset %}
        {% set result = run_query(check_table) %}
        {% if result.columns[0].values()[0] > 0 %}
            select entity_key, resolved_id
            from {{ production_schema }}.identity_membership
        {% else %}
            select cast(null as {{ dbt.type_string() }}) as entity_key,
                   cast(null as {{ dbt.type_string() }}) as resolved_id
            where 1 = 0
        {% endif %}
    {% else %}
        select cast(null as {{ dbt.type_string() }}) as entity_key,
               cast(null as {{ dbt.type_string() }}) as resolved_id
        where 1 = 0
    {% endif %}
),

comparison as (
    select
        coalesce(p.entity_key, c.entity_key) as entity_key,
        c.resolved_id as current_resolved_id,
        p.resolved_id as proposed_resolved_id,
        case
            when c.entity_key is null then 'NEW'
            when p.entity_key is null then 'REMOVED'
            when c.resolved_id != p.resolved_id then 'MOVED'
            else 'UNCHANGED'
        end as change_type
    from proposed_membership p
    full outer join current_membership c on p.entity_key = c.entity_key
)

select
    '{{ invocation_id }}' as run_id,
    entity_key,
    current_resolved_id,
    proposed_resolved_id,
    change_type,
    current_timestamp as created_at
from comparison
where change_type != 'UNCHANGED'
