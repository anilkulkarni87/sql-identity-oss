{{
    config(
        materialized='table',
        tags=['idr', 'marts']
    )
}}

{#
  Mart model: Identity clusters with confidence scoring.

  Calculates:
  - cluster_size: number of entities in cluster
  - edge_diversity: count of distinct identifier types
  - match_density: actual edges / possible edges
  - confidence_score: weighted combination (50% diversity, 35% density, 15% size)
  - primary_reason: human-readable explanation
#}

with cluster_sizes as (
    select
        resolved_id,
        count(*) as cluster_size
    from {{ ref('identity_membership') }}
    group by resolved_id
),

cluster_edge_stats as (
    select
        m.resolved_id,
        count(distinct e.identifier_type) as edge_diversity,
        count(*) as edge_count,
        max(
            case
                when lower(e.identifier_type) in ('ssn', 'tax_id', 'national_id') then 1.00
                when lower(e.identifier_type) like '%customer%'
                  or lower(e.identifier_type) like '%account%'
                  or lower(e.identifier_type) like '%loyalty%' then 0.98
                when lower(e.identifier_type) like '%email%' then 0.96
                when lower(e.identifier_type) like '%phone%' then 0.93
                when lower(e.identifier_type) like '%device%'
                  or lower(e.identifier_type) like '%session%' then 0.85
                else 0.78
            end
        ) as max_fidelity,
        max(
            case
                when lower(e.identifier_type) like '%customer%'
                  or lower(e.identifier_type) like '%account%'
                  or lower(e.identifier_type) like '%loyalty%'
                  or lower(e.identifier_type) in ('ssn', 'tax_id') then 1
                else 0
            end
        ) as has_strong_key,
        max(case when lower(e.identifier_type) like '%email%' then 1 else 0 end) as has_email,
        max(case when lower(e.identifier_type) like '%phone%' then 1 else 0 end) as has_phone
    from {{ ref('identity_membership') }} m
    inner join {{ ref('int_edges') }} e
        on e.left_entity_key = m.entity_key
        or e.right_entity_key = m.entity_key
    group by m.resolved_id
),

clusters_with_confidence as (
    select
        cs.resolved_id,
        cs.cluster_size,

        -- Edge diversity (number of identifier types)
        coalesce(es.edge_diversity, 0) as edge_diversity,

        -- Connectivity ratio (E >= V - 1 in hub/spoke trees)
        case
            when cs.cluster_size <= 1 then 1.0
            else least(1.0, coalesce(es.edge_count, 0) * 1.0 / greatest(1.0, (cs.cluster_size - 1.0)))
        end as connectivity_ratio,

        -- Confidence score grounded on identifier fidelity + corroboration
        case
            when cs.cluster_size = 1 then 1.0
            else round(
                least(
                    1.0,
                    greatest(
                        0.50,
                        (coalesce(es.max_fidelity, 0.78) *
                         case
                            when cs.cluster_size <= 1 then 1.0
                            else least(1.0, coalesce(es.edge_count, 0) * 1.0 / greatest(1.0, (cs.cluster_size - 1.0)))
                         end) +
                        case
                            when coalesce(es.edge_diversity, 0) >= 3 then 0.04
                            when coalesce(es.edge_diversity, 0) = 2 then 0.02
                            else 0.0
                        end -
                        case
                            when cs.cluster_size > 250 and coalesce(es.edge_diversity, 0) <= 1 then 0.12
                            else 0.0
                        end
                    )
                ),
                3
            )
        end as confidence_score,

        -- Explainable primary reason
        case
            when cs.cluster_size = 1 then 'SINGLETON_NO_MATCH_REQUIRED'
            when cs.cluster_size > 250 and coalesce(es.edge_diversity, 0) <= 1 then
                concat('Large cluster via single identifier (', cast(cs.cluster_size as {{ dbt.type_string() }}), ' entities)')
            when coalesce(es.edge_diversity, 0) >= 3 then
                concat(cast(es.edge_diversity as {{ dbt.type_string() }}), ' identifier types (High Confidence Corroboration)')
            when coalesce(es.edge_diversity, 0) = 2 then
                concat(cast(es.edge_diversity as {{ dbt.type_string() }}), ' identifier types (High Confidence Corroboration)')
            when coalesce(es.has_strong_key, 0) = 1 then
                'Deterministic account/customer key match (High Confidence)'
            when coalesce(es.has_email, 0) = 1 then
                'Deterministic exact email match'
            when coalesce(es.has_phone, 0) = 1 then
                'Deterministic exact phone match'
            else
                concat('Deterministic match via ', cast(coalesce(es.edge_diversity, 0) as {{ dbt.type_string() }}), ' identifier type(s)')
        end as primary_reason,

        current_timestamp as updated_at

    from cluster_sizes cs
    left join cluster_edge_stats es on es.resolved_id = cs.resolved_id
)

select * from clusters_with_confidence
