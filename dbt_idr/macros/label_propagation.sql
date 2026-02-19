{#
  Iterative label propagation macro.

  This runs label propagation until convergence by:
  1. Creating initial labels table
  2. Iteratively propagating minimum labels
  3. Stopping when no changes occur

  Usage: Call via dbt run-operation before running int_labels model,
         OR use as a pre-hook on int_labels.
#}

{% macro run_label_propagation(max_iterations=30) %}

    {% set create_initial %}
        create or replace table {{ target.schema }}_idr_work.lp_labels as
        select distinct entity_key, entity_key as label
        from (
            select left_entity_key as entity_key from {{ target.schema }}_idr_work.int_edges
            union
            select right_entity_key as entity_key from {{ target.schema }}_idr_work.int_edges
        )
    {% endset %}

    {% do run_query(create_initial) %}
    {{ log("LP: Initialized labels", info=True) }}

    {% for i in range(max_iterations) %}

        {# Propagate step #}
        {% set propagate %}
            create or replace table {{ target.schema }}_idr_work.lp_labels_next as
            select
                n.entity_key,
                least(n.label, coalesce(min(neighbor.label), n.label)) as label
            from {{ target.schema }}_idr_work.lp_labels n
            left join (
                select left_entity_key as src, right_entity_key as dst from {{ target.schema }}_idr_work.int_edges
                union all
                select right_entity_key as src, left_entity_key as dst from {{ target.schema }}_idr_work.int_edges
            ) e on e.src = n.entity_key
            left join {{ target.schema }}_idr_work.lp_labels neighbor on neighbor.entity_key = e.dst
            group by n.entity_key, n.label
        {% endset %}

        {% do run_query(propagate) %}

        {# Check for convergence #}
        {% set check_delta %}
            select count(*) as delta
            from {{ target.schema }}_idr_work.lp_labels o
            join {{ target.schema }}_idr_work.lp_labels_next n on o.entity_key = n.entity_key
            where o.label != n.label
        {% endset %}

        {% set delta_result = run_query(check_delta) %}
        {% set delta = delta_result.columns[0].values()[0] %}

        {{ log("LP iteration " ~ (i+1) ~ ": " ~ delta ~ " changes", info=True) }}

        {# Swap tables #}
        {% set swap %}
            drop table if exists {{ target.schema }}_idr_work.lp_labels;
            {% if target.type == 'bigquery' %}
            alter table {{ target.schema }}_idr_work.lp_labels_next rename to lp_labels
            {% else %}
            alter table {{ target.schema }}_idr_work.lp_labels_next rename to {{ target.schema }}_idr_work.lp_labels
            {% endif %}
        {% endset %}
        {% do run_query(swap) %}

        {% if delta == 0 %}
            {{ log("LP: Converged after " ~ (i+1) ~ " iterations", info=True) }}
            {% break %}
        {% endif %}

    {% endfor %}

{% endmacro %}
