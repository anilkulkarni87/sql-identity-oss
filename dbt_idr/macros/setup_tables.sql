{#
  Setup macro: Create empty metadata tables for manual INSERT.

  Usage:
    dbt run-operation setup_idr_tables

  This creates empty tables with the correct schema so users can
  INSERT their configuration directly instead of using CSV seeds.
#}

{% macro setup_idr_tables() %}

    {{ log("Creating IDR metadata tables...", info=True) }}

    {# idr_sources #}
    {% set create_sources %}
        create table if not exists {{ target.schema }}_idr_meta.idr_sources (
            source_id varchar(255) not null,
            source_name varchar(255),
            database varchar(255),
            schema varchar(255) not null,
            table_name varchar(255) not null,
            entity_key_column varchar(255) not null,
            watermark_column varchar(255),
            is_active boolean default true,
            description varchar(1000),
            primary key (source_id)
        )
    {% endset %}
    {% do run_query(create_sources) %}
    {{ log("  ✓ idr_sources", info=True) }}

    {# idr_rules #}
    {% set create_rules %}
        create table if not exists {{ target.schema }}_idr_meta.idr_rules (
            rule_id varchar(255) not null,
            identifier_type varchar(100) not null,
            priority integer default 1,
            is_active boolean default true,
            canonicalize varchar(50) default 'NONE',
            max_group_size integer default 10000,
            description varchar(1000),
            primary key (rule_id)
        )
    {% endset %}
    {% do run_query(create_rules) %}
    {{ log("  ✓ idr_rules", info=True) }}

    {# idr_identifier_mappings #}
    {% set create_mappings %}
        create table if not exists {{ target.schema }}_idr_meta.idr_identifier_mappings (
            source_id varchar(255) not null,
            identifier_type varchar(100) not null,
            column_name varchar(255) not null,
            is_hashed boolean default false,
            description varchar(1000)
        )
    {% endset %}
    {% do run_query(create_mappings) %}
    {{ log("  ✓ idr_identifier_mappings", info=True) }}

    {# idr_exclusions #}
    {% set create_exclusions %}
        create table if not exists {{ target.schema }}_idr_meta.idr_exclusions (
            identifier_type varchar(100) not null,
            identifier_value_pattern varchar(500) not null,
            match_type varchar(50) default 'EXACT',
            reason varchar(500)
        )
    {% endset %}
    {% do run_query(create_exclusions) %}
    {{ log("  ✓ idr_exclusions", info=True) }}

    {# idr_attribute_mappings #}
    {% set create_attr_mappings %}
        create table if not exists {{ target.schema }}_idr_meta.idr_attribute_mappings (
            source_id varchar(255) not null,
            attribute_name varchar(100) not null,
            column_expr varchar(500) not null,
            description varchar(500)
        )
    {% endset %}
    {% do run_query(create_attr_mappings) %}
    {{ log("  ✓ idr_attribute_mappings", info=True) }}

    {# idr_survivorship_rules #}
    {% set create_survivorship %}
        create table if not exists {{ target.schema }}_idr_meta.idr_survivorship_rules (
            attribute_name varchar(100) not null,
            strategy varchar(50) not null,
            source_priority_list varchar(500),
            recency_field varchar(255),
            description varchar(500),
            primary key (attribute_name)
        )
    {% endset %}
    {% do run_query(create_survivorship) %}
    {{ log("  ✓ idr_survivorship_rules", info=True) }}

    {{ log("Done! Tables created in schema: " ~ target.schema ~ "_idr_meta", info=True) }}
    {{ log("", info=True) }}
    {{ log("Next steps:", info=True) }}
    {{ log("  1. INSERT your configuration into the tables", info=True) }}
    {{ log("  2. Run: dbt run --select dbt_idr", info=True) }}

{% endmacro %}
