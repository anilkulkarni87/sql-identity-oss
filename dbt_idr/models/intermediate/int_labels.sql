{{
    config(
        materialized='table',
        tags=['idr', 'intermediate']
    )
}}

{#
  Intermediate model: Label propagation to find connected components.

  Uses recursive CTE to propagate minimum labels through edges.
  Each entity gets assigned to the minimum entity_key in its connected component.

  Cross-platform compatible: Snowflake, BigQuery, Databricks, DuckDB
#}

with recursive

all_nodes as (
    select distinct left_entity_key as entity_key from {{ ref('int_edges') }}
    union
    select distinct right_entity_key as entity_key from {{ ref('int_edges') }}
),

undirected_edges as (
    select left_entity_key as src, right_entity_key as dst from {{ ref('int_edges') }}
    union
    select right_entity_key as src, left_entity_key as dst from {{ ref('int_edges') }}
),

label_propagation as (
    select
        entity_key,
        entity_key as label,
        0 as iteration
    from all_nodes

    union all

    select
        lp.entity_key,
        least(lp.label, e.dst) as label,
        lp.iteration + 1
    from label_propagation lp
    join undirected_edges e on e.src = lp.entity_key
    where lp.iteration < {{ var('idr_max_lp_iterations', 30) }}
      and e.dst < lp.label
),

final_labels as (
    select
        entity_key,
        min(label) as label
    from label_propagation
    group by entity_key
)

select
    entity_key,
    label as resolved_id,
    current_timestamp as updated_at
from final_labels
