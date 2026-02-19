{{
    config(
        materialized='table',
        tags=['idr', 'marts', 'observability']
    )
}}

{#
  Run history model: Captures IDR run metadata.
#}

select
    '{{ invocation_id }}' as run_id,
    cast('{{ run_started_at }}' as {{ dbt.type_timestamp() }}) as started_at,
    current_timestamp as ended_at,
    (select count(*) from {{ ref('stg_identifiers') }}) as identifiers_extracted,
    (select count(*) from {{ ref('int_edges') }}) as edges_created,
    (select count(*) from {{ ref('identity_clusters') }}) as clusters_total,
    (select count(*) from {{ ref('identity_membership') }}) as entities_resolved,
    (select count(*) from {{ ref('identity_clusters') }} where cluster_size >= {{ var('idr_large_cluster_threshold', 5000) }}) as large_clusters,
    'SUCCESS' as status,
    '{{ target.name }}' as target_name,
    '{{ target.type }}' as target_type
