---
name: sql-identity-resolution
description: Warehouse-native deterministic identity resolution and Customer 360 context grounding across DuckDB, Snowflake, BigQuery, and Databricks. Use when resolving duplicate customer records, profiling tables for PII/identifiers, simulating match rules, exporting dbt_idr seeds, or grounding AI agents with unified Customer 360 profiles.
---

# SQL Identity Resolution (`sql-identity-oss`) Agent Skill

Use this skill whenever the user wants to:
1. **Discover & Configure Identity Resolution:** Profile warehouse tables (`DuckDB`, `Snowflake`, `BigQuery`, `Databricks`) to identify entity keys, matchable identifiers (`EMAIL`, `PHONE`, `LOYALTY_ID`, etc.), and survivorship rules.
2. **Simulate & Prevent Supernodes:** Preview the impact of matching rules (`max_group_size` guardrails, skipped identifier groups, dry-run cluster movement) before touching production tables.
3. **Export to Governed `dbt` Production (`dbt_idr`):** Compile discovered or active YAML/metadata configurations directly into the 6 standard `dbt_idr` seed CSVs.
4. **Resolve Customer 360 Context (Agent Grounding):** Look up any customer identifier (`email`, `phone`, `entity_key`) to retrieve the deterministic Golden Profile, linked cross-system accounts, and Spanning Tree confidence score before taking downstream actions.

---

## Standard Agentic Workflow (MCP Tools)

When connected to the `idr` FastMCP server (`idr-mcp` or `python -m idr_mcp.server`), always follow this state-machine sequence:

### Step 1: Check System State (`get_system_status`)
Call `get_system_status()` first. It returns one of four explicit states:
- `DISCONNECTED`: No active warehouse adapter. Set `IDR_PLATFORM` / `IDR_DATABASE` or connect via the Setup API.
- `CONNECTED_UNINITIALIZED`: Connected to the warehouse, but `idr_meta` tables or rules are not yet initialized. Proceed to Step 2.
- `CONNECTED_READY`: Metadata and rules exist. Check `has_runs` to see if clusters are populated.
- `STALE`: Connection dropped; `get_adapter()` will auto-heal from saved credentials.

### Step 2: Zero-Config Discovery (`discover_and_suggest_rules`)
Call `discover_and_suggest_rules(schema="<schema>", sample_size=500, output_file="idr_config.yaml")` to:
- Statistically scan candidate tables.
- Auto-detect primary keys, watermarks, PII columns (`EMAIL`, `PHONE`), and survivorship attributes.
- Produce a validated `suggested_config` YAML structure.

### Step 3: Pre-Flight Simulation (`simulate_rule_impact` & `dry_run_pipeline`)
Never run unverified rules in production without checking for giant-cluster snowballs:
1. Call `simulate_rule_impact(rule={"id": "email_exact", "match_keys": ["email"], "max_group_size": 50})` to detect placeholder values (`test@example.com`, `0000000000`) that exceed `max_group_size`.
2. Call `apply_config(config_yaml=..., initialize_schema=True)` followed by `dry_run_pipeline(run_mode="FULL")` to inspect `new_entities` and `moved_entities` without mutating production tables.

### Step 4: Execute or Export to `dbt_idr`
Choose the target runtime:
- **In-Place Warehouse Execution:** Call `trigger_pipeline_run(run_mode="FULL")` (or `"INCR"` for incremental watermark processing).
- **Governed `dbt` Handoff:** Call `export_dbt_seeds(output_dir="dbt_idr/seeds", config_yaml=...)` (or CLI `idr dbt export --file idr_config.yaml --output-dir dbt_idr/seeds`) to generate:
  - `idr_sources.csv`
  - `idr_identifier_mappings.csv`
  - `idr_attribute_mappings.csv`
  - `idr_rules.csv`
  - `idr_exclusions.csv`
  - `idr_survivorship_rules.csv`

### Step 5: Deterministic Customer 360 Grounding (`resolve_customer_360`)
Before executing customer-facing actions (support tickets, billing adjustments, CRM merges):
1. Call `resolve_customer_360(identifier_value="<email_or_phone>")`.
2. Inspect `confidence_score` and `risk_flags`:
   - If `confidence_score >= 0.90` and `risk_flags` is empty: safe for automated agent execution.
   - If `LOW_CONFIDENCE_CLUSTER` or `HIGH_CARDINALITY_CLUSTER` is present: call `explain_cluster(resolved_id)` or `explain_edge(entity_key_a, entity_key_b)` and request human confirmation.

---

## CLI Quick Reference

```bash
# Launch interactive Studio UI + API in browser (zero Node.js required):
idr ui

# Run 60-second local DuckDB demo:
idr quickstart

# Run IDR pipeline (platform auto-detected from --db, --project, or env):
idr run --db my_warehouse.duckdb --config idr_config.yaml --mode FULL

# Export YAML or live warehouse metadata to dbt_idr seed CSVs:
idr dbt export --file idr_config.yaml --output-dir dbt_idr/seeds

# Start FastMCP server over stdio or Streamable HTTP:
idr-mcp
IDR_MCP_TRANSPORT=streamable-http idr-mcp

# Headless CLI lifecycle:
idr init --platform <duckdb|snowflake|bigquery|databricks>
idr doctor --platform <platform>
idr bootstrap --platform <platform> --schema <schema> --output idr_config.yaml [--apply]
idr apply-config --platform <platform> --config idr_config.yaml
idr run --platform <platform> --mode <FULL|INCR> [--dry-run] [--strict]
idr evaluate --platform <platform>
idr privacy forget --platform <platform> --identifier <val> [--dry-run]
```

---

## Config Modeling Reference (`idr_config.yaml` / `.json`)

```yaml
sources:
  - id: customers                    # Unique logical source ID
    table: db.schema.customers       # FQN (db.schema.table for Snowflake, project.dataset.table for BQ, schema.table for Databricks/DuckDB)
    entity_type: PERSON
    entity_key: customer_id          # Primary row key expression
    watermark: updated_at            # Optional incremental timestamp column
    watermark_lookback: 60           # Optional lookback minutes
    identifiers:
      - type: email
        expr: email
      - type: phone
        expr: phone_number
    attributes:
      - name: first_name
        expr: first_name

use_default_exclusions: true

rules:
  - id: "104"
    type: EXACT
    match_keys: [email]
    priority: 1
    canonicalize: EMAIL_STANDARD     # EMAIL_STANDARD | EMAIL_STRIP_PLUS | PHONE_DIGITS | PHONE_US_10 | LOWERCASE | UPPERCASE | ALPHANUMERIC_UPPER | custom {val} SQL
    max_group_size: 50               # Optional supernode circuit breaker

survivorship:
  - attribute: first_name
    strategy: RECENCY                # RECENCY | PRIORITY | FREQUENCY | LONGEST
    source_priority: [customers]

exclusions:                          # Optional custom poison-pill exclusions
  - identifier_type: email           # email | phone | ANY
    pattern: "%@example.com"
    match_type: LIKE                 # EXACT | LIKE
    reason: "Synthetic test domain"

overrides:                           # Optional manual stewardship overrides
  - type: DO_NOT_LINK                # FORCE_LINK | DO_NOT_LINK | ISOLATE
    entity_key_a: "crm:101"
    entity_key_b: "crm:102"          # Required for FORCE_LINK / DO_NOT_LINK; omit for ISOLATE
    reason: "Shared household phone"
```

### Agentic Safety & Triage Tools (MCP)
- **Remote MCP**: FastAPI mounts Streamable HTTP at `POST /mcp` (shared OAuth/SSO & `ConnectionManager`).
- **Two-Step Guardrails**: `forget_customer_identity` defaults to `dry_run=True` and issues a 5-minute `confirmation_token` + `blast_radius_summary` required for `dry_run=False`. `manage_identity_override` also supports `dry_run=True` preview tokens.
- **Triage Loop**: `get_triage_queue(max_confidence=0.45)` + `submit_evaluation_label(sample_id, label, rationale)`.
- **Bounded Payloads**: `get_cluster` and `explain_cluster` cap members/edges (`max_entities`, `max_edges`) and return `truncated: true` when exceeded.
- **Resources & Prompts**: `idr://spec/config-schema`, `idr://spec/cli-commands`, `idr://spec/canonicalizers`, and prompt `model_config_from_tables`.

### Warehouse Env Vars
- **DuckDB**: `IDR_PLATFORM=duckdb`, `IDR_DATABASE=demo.duckdb`
- **Snowflake**: `IDR_PLATFORM=snowflake`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`
- **BigQuery**: `IDR_PLATFORM=bigquery`, `IDR_PROJECT`
- **Databricks**: `IDR_PLATFORM=databricks`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CATALOG`

