{{
    config(
        materialized='table',
        tags=['idr', 'marts', 'observability']
    )
}}

{#
  Stage metrics model: Captures row counts at each pipeline stage.
  Provides observability into the IDR pipeline execution.
#}

with stages as (
    select 1 as stage_order, 'stg_identifiers' as stage_name,
           (select count(*) from {{ ref('stg_identifiers') }}) as rows_out
    union all
    select 2, 'int_edges',
           (select count(*) from {{ ref('int_edges') }})
    union all
    select 3, 'int_labels',
           (select count(*) from {{ ref('int_labels') }})
    union all
    select 4, 'identity_membership',
           (select count(*) from {{ ref('identity_membership') }})
    union all
    select 5, 'identity_clusters',
           (select count(*) from {{ ref('identity_clusters') }})
    union all
    select 6, 'golden_profiles',
           (select count(*) from {{ ref('golden_profiles') }})
    union all
    select 7, 'skipped_identifier_groups',
           (select count(*) from {{ ref('skipped_identifier_groups') }})
)

select
    '{{ invocation_id }}' as run_id,
    stage_name,
    stage_order,
    rows_out,
    current_timestamp as recorded_at
from stages
order by stage_order
