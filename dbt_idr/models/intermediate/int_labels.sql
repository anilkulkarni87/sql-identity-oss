{{
    config(
        materialized='table',
        tags=['idr', 'intermediate']
    )
}}

{#
  Intermediate model: Label propagation to find connected components.

  Uses a linear, single-reference iterative CTE chain with self-edges and
  per-hop GROUP BY deduplication to propagate the minimum entity_key across
  each connected component without exponential CTE inlining or cycle blowup.

  Cross-platform compatible: Snowflake, BigQuery, Databricks, DuckDB
#}

{% set raw_iters = var('idr_max_lp_iterations', 10) | int %}
{% set num_iters = 12 if raw_iters > 12 else (1 if raw_iters < 1 else raw_iters) %}

with all_nodes as (
    select distinct left_entity_key as entity_key from {{ ref('int_edges') }}
    union
    select distinct right_entity_key as entity_key from {{ ref('int_edges') }}
),

undirected_edges_with_self as (
    select left_entity_key as src, right_entity_key as dst from {{ ref('int_edges') }}
    union
    select right_entity_key as src, left_entity_key as dst from {{ ref('int_edges') }}
    union
    select entity_key as src, entity_key as dst from all_nodes
),

lp_0 as (
    select
        entity_key,
        entity_key as label
    from all_nodes
)
{% for i in range(1, num_iters + 1) %}
, lp_{{ i }} as (
    select
        e.dst as entity_key,
        min(n.label) as label
    from lp_{{ i - 1 }} n
    inner join undirected_edges_with_self e on e.src = n.entity_key
    group by e.dst
)
{% endfor %}

select
    entity_key,
    label as resolved_id,
    current_timestamp as updated_at
from lp_{{ num_iters }}
