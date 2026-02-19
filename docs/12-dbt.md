# dbt Package

The dbt package provides deterministic-only identity resolution using SQL models.

Install:
```yaml
packages:
  - git: "https://github.com/anilkulkarni87/sql-identity-resolution"
    subdirectory: "dbt_idr"
    revision: main
```

Seed configuration:
- `idr_sources.csv`
- `idr_rules.csv`
- `idr_identifier_mappings.csv`
- `idr_attribute_mappings.csv`
- `idr_survivorship_rules.csv`
- `idr_exclusions.csv`

Run:
```bash
dbt deps
dbt seed --select dbt_idr
dbt run --select dbt_idr
```

Notes:
- Fuzzy matching is not supported in dbt.
- Golden profiles support RECENCY and PRIORITY.
