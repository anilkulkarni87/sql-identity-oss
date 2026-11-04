# Configuration Guide

IDR configurations are managed through declarative YAML manifests or through the interactive Web Setup Wizard.

---

## 📄 Complete Example Manifest

```yaml
sources:
  - id: "crm"
    table: "raw.crm_customers"
    entity_key: "customer_id"
    entity_type: "PERSON"
    watermark_column: "updated_at"
    watermark_lookback_minutes: 0
    identifiers:
      - type: "email"
        expr: "email"
      - type: "phone"
        expr: "phone"
      - type: "loyalty"
        expr: "loyalty_id"
    attributes:
      - name: "first_name"
        expr: "first_name"
      - name: "last_name"
        expr: "last_name"
      - name: "email"
        expr: "email"
      - name: "phone"
        expr: "phone"

  - id: "web"
    table: "raw.web_users"
    entity_key: "user_id"
    entity_type: "PERSON"
    watermark_column: "last_login_ts"
    identifiers:
      - type: "email"
        expr: "user_email"
      - type: "phone"
        expr: "mobile_phone"
    attributes:
      - name: "first_name"
        expr: "first_name"
      - name: "last_name"
        expr: "last_name"
      - name: "email"
        expr: "user_email"

rules:
  - id: "r_email"
    type: "EXACT"
    match_keys: ["email"]
    canonicalize: "LOWERCASE"
    priority: 1
    max_group_size: 100

  - id: "r_phone"
    type: "EXACT"
    match_keys: ["phone"]
    canonicalize: "EXACT"
    priority: 2
    max_group_size: 5000

  - id: "r_loyalty"
    type: "EXACT"
    match_keys: ["loyalty"]
    canonicalize: "EXACT"
    priority: 3
    max_group_size: 1000

fuzzy_rules:
  - id: "fuzzy_last_name"
    name: "Fuzzy Last Name within Soundex Block"
    blocking_key: "SUBSTRING(last_name, 1, 3)"
    score_expr:
      duckdb: "jaro_winkler_similarity(a.last_name, b.last_name)"
      snowflake: "JAROWINKLER_SIMILARITY(a.last_name, b.last_name) / 100.0"
      bigquery: "1.0 - (EDIT_DISTANCE(a.last_name, b.last_name) / GREATEST(LENGTH(a.last_name), LENGTH(b.last_name), 1))"
      databricks: "jaro_winkler(a.last_name, b.last_name)"
    threshold: 0.85
    priority: 10

survivorship:
  - attribute: "first_name"
    strategy: "PRIORITY"
    source_priority: ["crm", "web"]

  - attribute: "last_name"
    strategy: "RECENCY"
    recency_field: "updated_at"

  - attribute: "email"
    strategy: "RECENCY"
    recency_field: "updated_at"

  - attribute: "phone"
    strategy: "FREQUENCY"

exclusions:
  - identifier_type: "email"
    value: "support@company.com"
    match_type: "EXACT"
    reason: "Internal support ticketing inbox"

  - identifier_type: "email"
    value: "%@noreply.example.com"
    match_type: "LIKE"
    reason: "Automated notification domain"
```

---

## 🛠️ Section Breakdown

### 1. `sources`
Defines source tables to ingest into the identity graph.

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | string | Yes | Unique source identifier (used in lineage and priority survivorship). |
| `table` | string | Yes | Fully-qualified table name (`schema.table` or `catalog.schema.table`). |
| `entity_key` | string | Yes | Unique primary key column / expression in the source table. |
| `entity_type` | string | No | Entity categorization (default: `"PERSON"`). |
| `watermark_column` | string | No | Timestamp column used for incremental processing (`INCR` mode). |
| `watermark_lookback_minutes` | integer | No | Buffer window (minutes) for late-arriving records (default: `0`). |
| `identifiers` | list | Yes | List of identifier mapping specs (`type`, `expr`, `is_hashed`). |
| `attributes` | list | No | List of profile attributes for Golden Profile generation (`name`, `expr`). |

### 2. `rules` (Deterministic Matching)
Exact matching rules that form graph edges.

| Field | Type | Default | Description |
|---|---|---|---|
| `id` | string | Required | Unique rule ID. |
| `type` | string | `"EXACT"` | Match type. |
| `match_keys` | list | Required | Identifier types that must match (e.g. `["email"]`). |
| `canonicalize` | string | `"LOWERCASE"` | Value normalization: `"LOWERCASE"`, `"UPPERCASE"`, or `"EXACT"`. |
| `priority` | integer | `100` | Rule priority (lower number = higher edge priority in evidence). |
| `max_group_size` | integer | `10000` | High-cardinality guardrail. Groups larger than this are skipped and logged to `idr_out.skipped_identifier_groups`. |

### 3. `fuzzy_rules` (Probabilistic Super-Clustering)
Optional refinement rules executed when `--strict` is not enabled.

| Field | Type | Description |
|---|---|---|
| `id` | string | Unique fuzzy rule ID. |
| `name` | string | Human-readable rule description. |
| `blocking_key` | string | SQL expression partitioning candidates before cross-comparison (e.g., soundex or first 3 characters). |
| `score_expr` | string or dict | Similarity score expression yielding 0.0 to 1.0. Can be a string or a dialect-keyed dictionary (`duckdb`, `snowflake`, `bigquery`, `databricks`). |
| `threshold` | float | Minimum score (0.0 to 1.0) required to merge clusters into a Super-Cluster. |
| `priority` | integer | Execution priority. |

> [!TIP]
> In the Web UI Setup Wizard, choosing a fuzzy template (Jaro-Winkler, Levenshtein, Soundex) automatically substitutes `{col}` with your selected attribute and selects the target warehouse's native function.

### 4. `survivorship` (Golden Record Resolution)
Controls how attributes from multiple source records within a cluster are consolidated into `idr_out.golden_profiles`.

| Strategy | Description | Required Parameters |
|---|---|---|
| **`RECENCY`** | Selects the non-null value from the record with the most recent timestamp. | `recency_field` (or defaults to source watermark column) |
| **`PRIORITY`** | Selects the value from the highest-ranked source system. Automatically skips NULL values. | `source_priority` (list of source table `id`s or table names) |
| **`FREQUENCY`** | Selects the most frequently occurring value across all cluster members. | None |

### 5. `exclusions` (Identifier Blocklist)
Blocks specific values from linking records to prevent hub-and-spoke false clustering.

| Field | Type | Description |
|---|---|---|
| `identifier_type` | string | Identifier type to filter (e.g. `email`, `phone`). |
| `value` | string | Value or pattern to block. |
| `match_type` | string | `"EXACT"` (exact string match) or `"LIKE"` (SQL wildcard matching with `%`). |
| `reason` | string | Optional description for auditing. |

---

## ⚡ CLI Configuration Workflow

```bash
# 1. Validate your YAML configuration syntax
idr config validate --file config.yaml

# 2. Preview the SQL DDL statements generated for your target database
idr config generate --file config.yaml --dialect [duckdb|snowflake|bigquery|databricks]

# 3. Apply configuration to your target warehouse
idr config apply --file config.yaml --platform snowflake
```
