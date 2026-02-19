{{
    config(
        materialized='table',
        tags=['idr', 'marts', 'dry_run'],
        enabled=var('idr_dry_run', false)
    )
}}

{#
  Dry run summary: Aggregate statistics of proposed changes.

  Only runs when idr_dry_run=true.
#}

with changes as (
    select * from {{ ref('dry_run_results') }}
),

proposed_clusters as (
    select * from {{ ref('identity_clusters') }}
)

select
    '{{ invocation_id }}' as run_id,
    (select count(*) from changes) as total_changes,
    (select count(*) from changes where change_type = 'NEW') as new_entities,
    (select count(*) from changes where change_type = 'MOVED') as moved_entities,
    (select count(*) from changes where change_type = 'REMOVED') as removed_entities,
    (select count(*) from proposed_clusters) as proposed_clusters_total,
    (select max(cluster_size) from proposed_clusters) as largest_proposed_cluster,
    (select count(*) from proposed_clusters where cluster_size >= {{ var('idr_large_cluster_threshold') }}) as large_clusters,
    (select count(*) from {{ ref('int_edges') }}) as edges_would_create,
    (select count(*) from {{ ref('skipped_identifier_groups') }}) as groups_would_skip,
    current_timestamp as created_at
