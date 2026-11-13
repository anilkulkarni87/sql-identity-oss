---
title: Configuration, Canonicalizers & Governance
---

# Configuration, Canonicalizers & Enterprise Governance

`sql-identity-resolution` is configured declaratively via YAML (`idr apply-config`) or interactively through the Web UI Setup Wizard. All configuration state lives in `idr_meta.*` tables inside your warehouse.

---

## 1. Complete YAML Manifest Reference

!!! warning "Prerequisite: Unique, Non-Null `entity_key` per Row"
    Every registered source table **must** expose a unique, non-null `entity_key` expression per row. Graph node identity (`<table_id>:<entity_key>`), incremental watermarks (`INCR`), persistent `resolved_id` tracking, and warehouse `MERGE` operations (`identity_identifiers_current`, `identity_edges_current`) all assume one unique key per source row.

    - **No single primary key column?** Pass a deterministic SQL expression, e.g. `entity_key: "CONCAT(tenant_id, '_', row_id)"` or `SHA2(CONCAT_WS('|', col_a, col_b), 256)`.
    - **Event / transaction logs** (e.g. `orders`, `web_events` where `customer_id` repeats): Use the row's own unique key (`order_id`, `event_id`) as `entity_key` and map `customer_id` under `identifiers:`.
    - **Tables with duplicate keys** (e.g. raw CDC staging tables): Point `table:` at a deduplicated view (`QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1`).

```yaml
# Enable built-in poison-pill identifier exclusions (noreply@, test@, 0000000000, etc.)
use_default_exclusions: true

# 1. Source Tables
sources:
  - id: crm
    table: raw.crm_customers
    entity_key: customer_id
    entity_type: PERSON
    watermark_column: updated_at
    watermark_lookback_minutes: 0
    identifiers:
      - type: EMAIL
        expr: email
      - type: PHONE
        expr: phone
    attributes:
      - name: first_name
        expr: first_name
      - name: last_name
        expr: last_name
      - name: email
        expr: email
      - name: phone
        expr: phone
      - name: record_updated_at
        expr: updated_at

  - id: loyalty_lookup
    table: raw.loyalty_cards
    entity_key: card_id
    entity_type: PERSON
    # Omit watermark_column if the table has no timestamp;
    # INCR runs will scan all rows and state-diff identifiers automatically.
    identifiers:
      - type: EMAIL
        expr: member_email
      - type: LOYALTY
        expr: loyalty_number
    attributes: []

# 2. Deterministic Matching Rules
rules:
  - id: email_standard_match
    type: EXACT
    match_keys: [EMAIL]
    canonicalize: EMAIL_STANDARD
    priority: 1
    max_group_size: 10000

  - id: phone_us10_match
    type: EXACT
    match_keys: [PHONE]
    canonicalize: PHONE_US_10
    priority: 2
    max_group_size: 5000

  - id: loyalty_custom_sql
    type: EXACT
    match_keys: [LOYALTY]
    # Custom inline SQL canonicalizer using {val} placeholder
    canonicalize: "REGEXP_REPLACE(UPPER(TRIM({val})), '^0+', '')"
    priority: 3
    max_group_size: 100

# 3. Optional Probabilistic Fuzzy Rules (Post-Deterministic Super-Clustering)
fuzzy_rules:
  - id: fuzzy_name_match
    name: First + Last Name Jaro-Winkler
    blocking_key: "CONCAT(SOUNDEX(last_name), '_', SUBSTR(UPPER(first_name), 1, 1))"
    score_expr:
      default: "idr_meta.jaro_winkler_similarity(<a>, <b>)"
      bigquery: "1.0 - SAFE_DIVIDE(CAST(EDIT_DISTANCE(<a>, <b>) AS FLOAT64), CAST(GREATEST(LENGTH(<a>), LENGTH(<b>), 1) AS FLOAT64))"
    threshold: 0.88
    priority: 100

# 4. Golden Profile Survivorship Rules
survivorship:
  - attribute: email
    strategy: PRIORITY
    source_priority: [crm, loyalty_lookup]
    recency_field: record_updated_at

  - attribute: first_name
    strategy: RECENCY
    recency_field: record_updated_at

  - attribute: phone
    strategy: FREQUENCY
    recency_field: record_updated_at

# 5. Custom Identifier Exclusions (Supplementing use_default_exclusions)
exclusions:
  - type: EMAIL
    value: "%@internal-corp.com"
    match: LIKE
    reason: Internal employee emails
  - type: ANY
    value: "REDACTED"
    match: EXACT
    reason: Masked compliance value across all identifier types

# 6. Manual Stewardship Overrides
overrides:
  - type: FORCE_LINK
    entity_key_a: "crm:1001"
    entity_key_b: "loyalty_lookup:5099"
    reason: Verified VIP account merge
  - type: DO_NOT_LINK
    entity_key_a: "crm:1001"
    entity_key_b: "crm:2044"
    reason: Shared family phone number — distinct individuals
  - type: ISOLATE
    entity_key_a: "crm:9999"
    reason: Shared store kiosk account — never merge
```

---

## 2. Built-In Domain Canonicalizers & Custom SQL

Each deterministic rule in `rules` specifies a `canonicalize` strategy (`idr_core/canonicalizers.py` and `dbt_idr/models/staging/stg_identifiers.sql`). You can choose from **9 built-in cross-warehouse canonicalizers** or write **custom inline warehouse SQL**:

| Canonicalizer Name | Behavior | Example Input → Normalized Output |
| :--- | :--- | :--- |
| `EMAIL_STANDARD` | Trims, lowercases, and strips dots (`.`) from the local part of `@gmail.com` / `@googlemail.com` addresses while preserving `+tag` aliases. | `'  John.Doe+vip@Gmail.com '` → `'johndoe+vip@gmail.com'` |
| `EMAIL_STRIP_PLUS` | Performs all `EMAIL_STANDARD` normalizations **plus** strips `+tag` sub-addressing from the local part across all email domains. | `'John.Doe+promo@gmail.com'` → `'johndoe@gmail.com'` |
| `PHONE_DIGITS` | Strips all non-numeric characters (`[^0-9]`); returns `NULL` if fewer than 7 digits remain. | `'+1 (800) 555-0199'` → `'18005550199'` |
| `PHONE_US_10` | Strips non-digits, removes a leading US country code `1` from 11-digit numbers, and returns the 10-digit subscriber number (or `NULL` if `< 7` digits). | `'+1 (415) 555-2671'` → `'4155552671'` |
| `ALPHANUMERIC_UPPER` | Uppercases and strips all non-alphanumeric characters (`[^A-Z0-9]`). Ideal for loyalty cards, VINs, and tax/device IDs. | `'vip-9901 a'` → `'VIP9901A'` |
| `LOWERCASE` | `LOWER(TRIM(val))` | `' Alice@Example.com '` → `'alice@example.com'` |
| `UPPERCASE` | `UPPER(TRIM(val))` | `'abc-123'` → `'ABC-123'` |
| `EXACT` / `NONE` | `TRIM(val)` (preserves case and internal punctuation) | `' Token_AbC '` → `'Token_AbC'` |
| **Custom SQL (`{val}`)** | Any SQL expression containing `{val}` (where `{val}` is replaced by `CAST(<identifier_expr> AS VARCHAR)`). | `"REGEXP_REPLACE(LOWER(TRIM({val})), '^0+', '')"` |

---

## 3. Poison-Pill Exclusions (`idr_meta.identifier_exclusion`)

Bad placeholder identifiers (`null`, `unknown`, `0000000000`, `noreply@...`) can link thousands of unrelated customers into a single giant cluster. `sql-identity-resolution` provides three layers of protection:

1. **Built-In Default Exclusion Library (`use_default_exclusions: true`)**:
   - Seeds 30+ battle-tested exclusions across `ANY` (`null`, `none`, `n/a`, `na`, `unknown`, `tbd`, `test`, `dummy`, `anonymous`, `redacted`, `0`, `-1`), `EMAIL` (`%@example.%`, `%@test.%`, `%@mailinator.com`, `noreply@%`, `no-reply@%`, `donotreply@%`, `info@%`, `support@%`, `admin@%`, `sales@%`, `billing@%`), and `PHONE` (`0000000000`–`9999999999`, `1234567890`, `11234567890`, `0000000`).
2. **Custom YAML Exclusions**:
   - Add domain-specific patterns under `exclusions:` with `type` (`EMAIL`, `PHONE`, or `ANY` / `*` for all types) and `match` (`EXACT` or `LIKE`).
3. **Direct Warehouse Table Management**:
   - Data stewards or upstream ETL jobs can insert rows directly into `idr_meta.identifier_exclusion` without touching YAML:
     ```sql
     INSERT INTO idr_meta.identifier_exclusion
       (identifier_type, identifier_value_pattern, match_type, reason)
     VALUES
       ('PHONE', '8005551212', 'EXACT', 'Corporate call center main line'),
       ('ANY', 'POS_GUEST_%', 'LIKE', 'Anonymous store register tokens');
     ```
   - `idr apply-config` preserves manually inserted warehouse rows unless `--reset` is explicitly passed.

---

## 4. Manual Identity Stewardship Overrides (`idr_meta.identity_override`)

When automated rules need human-in-the-loop correction, stewards can define overrides via **YAML (`overrides:`)**, the **Web UI Cluster Explorer**, **dbt seeds (`idr_overrides.csv`)**, or **direct SQL**:

| Override Type | Effect on Graph Resolution |
| :--- | :--- |
| **`FORCE_LINK`** | Injects a synthetic edge between `entity_key_a` and `entity_key_b` so both records always resolve into the same cluster, even if they share zero identifiers. |
| **`DO_NOT_LINK`** | Removes any direct edge between `entity_key_a` and `entity_key_b` **and** detaches the higher entity key from any shared identifier anchor group containing both records. |
| **`ISOLATE`** | Strips all identifiers from `entity_key_a` prior to edge building so `entity_key_a` is guaranteed to remain a singleton cluster (`resolved_id = entity_key_a`). |

```sql
INSERT INTO idr_meta.identity_override
  (override_id, override_type, entity_key_a, entity_key_b, reason, created_by, is_active)
VALUES
  ('ov_vip_1', 'FORCE_LINK', 'crm:1001', 'orders:8842', 'Confirmed same customer by support ticket #492', 'steward@company.com', TRUE);
```

---

## 5. GDPR / CCPA Right-to-be-Forgotten (`idr privacy forget`)

To comply with data deletion requests (GDPR Article 17 / CCPA), `PrivacyManager` (`idr_core/privacy.py`) purges an entity (or all entities matching an identifier value) across every IDR output and work table and writes a permanent compliance receipt into `idr_meta.privacy_audit_log`.

### CLI Usage

```bash
# Forget a specific entity key across all IDR output & work tables
idr privacy forget --platform duckdb --db demo.duckdb \
  --entity-key "crm:10042" \
  --reason "GDPR erasure request #REQ-2026-881" \
  --requested-by "privacy-team@company.com"

# Or forget all entities associated with an email/phone identifier value
idr privacy forget --platform duckdb --db demo.duckdb \
  --identifier-value "alice@example.com" \
  --identifier-type EMAIL \
  --reason "CCPA consumer deletion request"
```

### What Gets Purged & Logged
1. Resolves all target `entity_key` values and affected `resolved_id` clusters.
2. Deletes matching rows from:
   - `idr_out.identity_resolved_membership_current`
   - `idr_out.identity_identifiers_current`
   - `idr_out.resolved_id_crosswalk`
   - `idr_work.entities_delta`, `idr_work.identifiers_extracted`, `idr_work.identifiers_all`, `idr_work.entity_attributes`, `idr_work.edges_new`, `idr_work.lp_labels`
3. Recomputes or removes affected cluster summaries in `idr_out.identity_clusters_current` and `idr_out.golden_profile_current`.
4. Inserts an immutable audit record (`request_id`, `request_type`, `target_value`, `affected_entities_count`, `affected_clusters_count`, `tables_purged`, `requested_by`, `reason`, `executed_at`, `status`) into `idr_meta.privacy_audit_log`.

