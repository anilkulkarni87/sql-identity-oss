{{
    config(
        materialized='table',
        tags=['idr', 'marts']
    )
}}

{#
  Mart model: Golden profiles - FULLY DYNAMIC based on survivorship rules.

  Automatically generates ranking CTEs and output columns based on:
  - idr_survivorship_rules: defines which attributes exist and how to pick winners
  - idr_attribute_mappings: maps source columns to those attributes
  - idr_sources: defines where to pull data from
#}

{% set sources_query %}
    select * from {{ ref('idr_sources') }} where is_active = true
{% endset %}

{% set attr_mappings_query %}
    select * from {{ ref('idr_attribute_mappings') }}
{% endset %}

{% set survivorship_query %}
    select * from {{ ref('idr_survivorship_rules') }}
{% endset %}

{# Execute queries at compile time #}
{% if execute %}
    {% set sources_results = run_query(sources_query) %}
    {% set attr_results = run_query(attr_mappings_query) %}
    {% set surv_results = run_query(survivorship_query) %}

    {% set sources_list = sources_results.rows %}
    {% set attr_list = attr_results.rows %}
    {% set surv_list = surv_results.rows %}
{% else %}
    {% set sources_list = [] %}
    {% set attr_list = [] %}
    {% set surv_list = [] %}
{% endif %}

{# Get list of attributes from survivorship rules #}
{% set attributes = [] %}
{% for rule in surv_list %}
    {% do attributes.append(rule['attribute_name']) %}
{% endfor %}

{# Build dynamic entity queries - one per source #}
{% set entity_queries = [] %}

{% for source in sources_list %}
    {% set source_id = source['source_id'] %}
    {% set db = source['database'] %}
    {% set schema = source['schema'] %}
    {% set table = source['table_name'] %}
    {% set entity_key_col = source['entity_key_column'] %}
    {# Handle empty database field (for DuckDB) #}
    {% if db and db != '' %}
        {% set source_fqn = db ~ '.' ~ schema ~ '.' ~ table %}
    {% else %}
        {% set source_fqn = schema ~ '.' ~ table %}
    {% endif %}

    {# Build attr_map for this source #}
    {% set attr_map = {} %}
    {% for attr in attr_list %}
        {% if attr['source_id'] == source_id %}
            {% do attr_map.update({attr['attribute_name']: attr['column_expr']}) %}
        {% endif %}
    {% endfor %}

    {# Get recency field for this source #}
    {% set recency_col = attr_map.get('record_updated_at', "'1900-01-01'") %}

    {# Build SELECT list for all attributes #}
    {% set select_exprs = [] %}
    {% do select_exprs.append("'" ~ source_id ~ "' as source_id") %}
    {% do select_exprs.append("concat('" ~ source_id ~ "', ':', cast(" ~ entity_key_col ~ " as " ~ dbt.type_string() ~ ")) as entity_key") %}

    {% for attr in attributes %}
        {% set col_expr = attr_map.get(attr, 'null') %}
        {% do select_exprs.append("cast(" ~ col_expr ~ " as " ~ dbt.type_string() ~ ") as " ~ attr) %}
    {% endfor %}

    {% do select_exprs.append("coalesce(cast(" ~ recency_col ~ " as " ~ dbt.type_timestamp() ~ "), cast('1900-01-01' as " ~ dbt.type_timestamp() ~ ")) as record_updated_at") %}

    {% set query %}
select {{ select_exprs | join(',\n       ') }}
from {{ source_fqn }}
    {% endset %}

    {% do entity_queries.append(query) %}
{% endfor %}

with membership as (
    select distinct resolved_id, entity_key
    from {{ ref('identity_membership') }}
),

entity_attributes as (
    {% if entity_queries | length > 0 %}
        {{ entity_queries | join('\nunion all\n') }}
    {% else %}
        {# Empty fallback with dynamic columns #}
        select
            cast(null as {{ dbt.type_string() }}) as source_id,
            cast(null as {{ dbt.type_string() }}) as entity_key
            {% for attr in attributes %}
            , cast(null as {{ dbt.type_string() }}) as {{ attr }}
            {% endfor %}
            , cast(null as {{ dbt.type_timestamp() }}) as record_updated_at
        where 1 = 0
    {% endif %}
),

cluster_entities as (
    select
        m.resolved_id,
        e.*
    from membership m
    left join entity_attributes e on e.entity_key = m.entity_key
)

{# Generate ranking CTEs for each attribute dynamically #}
{% for attr_name in attributes %}
    {# Find rule for this attribute #}
    {% set ns = namespace(rule=None) %}
    {% for r in surv_list %}
        {% if r['attribute_name'] == attr_name %}
            {% set ns.rule = r %}
        {% endif %}
    {% endfor %}

    {% set rule = ns.rule %}
    {% set order_clause = "record_updated_at desc" %}

    {% if rule and rule['strategy'] == 'PRIORITY' %}
        {# Parse priority list (simplified comma-split) #}
        {% set priorities = rule['source_priority_list'].replace('[','').replace(']','').replace('"','').replace("'","").split(',') %}
        {% set case_parts = [] %}
        {% for p in priorities %}
            {% do case_parts.append("when source_id = '" ~ p.strip() ~ "' then " ~ loop.index) %}
        {% endfor %}
        {% set order_clause = "case " ~ (case_parts | join(' ')) ~ " else 999 end asc, record_updated_at desc" %}
    {% endif %}

, {{ attr_name }}_ranked as (
    select resolved_id, {{ attr_name }},
           row_number() over (partition by resolved_id order by {{ order_clause }}) as rn
    from cluster_entities
    where {{ attr_name }} is not null
)
{% endfor %}

{# Final golden profile - dynamically build columns #}
, golden_profiles as (
    select distinct
        m.resolved_id
        {% for attr in attributes %}
        , {{ attr }}_r.{{ attr }} as {{ attr }}_primary
        {% endfor %}
        , current_timestamp as updated_at
    from membership m
    {% for attr in attributes %}
    left join {{ attr }}_ranked {{ attr }}_r
        on {{ attr }}_r.resolved_id = m.resolved_id and {{ attr }}_r.rn = 1
    {% endfor %}
)

select * from golden_profiles
