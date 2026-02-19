{{
    config(
        materialized='table',
        tags=['idr', 'staging']
    )
}}

{#
  Staging model: Extract identifiers from all configured sources.

  AUTOMATICALLY generates SQL from seed metadata tables:
  - idr_sources: defines source tables
  - idr_identifier_mappings: maps columns to identifier types
  - idr_rules: defines normalization and max_group_size
  - idr_exclusions: values to filter out

  The Jinja loops through the seed data to build UNION ALL queries.
#}

{% set sources_query %}
    select * from {{ ref('idr_sources') }} where is_active = true
{% endset %}

{% set mappings_query %}
    select
        m.*,
        r.canonicalize,
        r.priority,
        coalesce(r.max_group_size, {{ var('idr_default_max_group_size') }}) as max_group_size
    from {{ ref('idr_identifier_mappings') }} m
    inner join {{ ref('idr_rules') }} r on m.identifier_type = r.identifier_type
    where r.is_active = true
{% endset %}

{# Execute queries at compile time to get metadata #}
{% if execute %}
    {% set sources_results = run_query(sources_query) %}
    {% set mappings_results = run_query(mappings_query) %}

    {% set sources_list = sources_results.rows %}
    {% set mappings_list = mappings_results.rows %}
{% else %}
    {% set sources_list = [] %}
    {% set mappings_list = [] %}
{% endif %}

{# Build dynamic UNION ALL queries #}
{% set union_queries = [] %}

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

    {# Find all mappings for this source #}
    {% for mapping in mappings_list %}
        {% if mapping['source_id'] == source_id %}
            {% set identifier_type = mapping['identifier_type'] %}
            {% set column_name = mapping['column_name'] %}
            {% set is_hashed = mapping['is_hashed'] %}
            {% set canonicalize = mapping['canonicalize'] %}
            {% set priority = mapping['priority'] %}
            {% set max_group_size = mapping['max_group_size'] %}

            {# Build normalization expression #}
            {% if canonicalize == 'LOWERCASE' %}
                {% set norm_expr = 'lower(' ~ column_name ~ ')' %}
            {% elif canonicalize == 'UPPERCASE' %}
                {% set norm_expr = 'upper(' ~ column_name ~ ')' %}
            {% else %}
                {% set norm_expr = column_name %}
            {% endif %}

            {% set query %}
select
    '{{ source_id }}' as source_id,
    concat('{{ source_id }}', ':', cast({{ entity_key_col }} as {{ dbt.type_string() }})) as entity_key,
    '{{ identifier_type }}' as identifier_type,
    cast({{ column_name }} as {{ dbt.type_string() }}) as identifier_value_raw,
    cast({{ norm_expr }} as {{ dbt.type_string() }}) as identifier_value_norm,
    {{ is_hashed }} as is_hashed,
    {{ priority }} as priority,
    {{ max_group_size }} as max_group_size,
    current_timestamp as extracted_at
from {{ source_fqn }}
where {{ column_name }} is not null
            {% endset %}

            {% do union_queries.append(query) %}
        {% endif %}
    {% endfor %}
{% endfor %}

{# Build the combined query with exclusion filtering #}
with raw_identifiers as (
    {% if union_queries | length > 0 %}
        {{ union_queries | join('\nunion all\n') }}
    {% else %}
        select
            cast(null as {{ dbt.type_string() }}) as source_id,
            cast(null as {{ dbt.type_string() }}) as entity_key,
            cast(null as {{ dbt.type_string() }}) as identifier_type,
            cast(null as {{ dbt.type_string() }}) as identifier_value_raw,
            cast(null as {{ dbt.type_string() }}) as identifier_value_norm,
            cast(null as boolean) as is_hashed,
            cast(null as integer) as priority,
            cast(null as integer) as max_group_size,
            cast(null as {{ dbt.type_timestamp() }}) as extracted_at
        where 1 = 0
    {% endif %}
),

exclusions as (
    select identifier_type, identifier_value_pattern, match_type
    from {{ ref('idr_exclusions') }}
),

{# Filter out excluded identifiers #}
filtered_identifiers as (
    select r.*
    from raw_identifiers r
    where not exists (
        select 1 from exclusions e
        where e.identifier_type = r.identifier_type
          and (
            (e.match_type = 'EXACT' and r.identifier_value_norm = e.identifier_value_pattern)
            or (e.match_type = 'LIKE' and r.identifier_value_norm like e.identifier_value_pattern)
          )
    )
)

select * from filtered_identifiers
