# Metadata Configuration Guide

How to configure sql-identity-resolution using YAML or SQL.

---

## Overview

The IDR system is **metadata-driven**—you describe your source tables via configuration tables, and the system dynamically generates SQL to extract identifiers and build identity graphs.

```
Your Source Tables → Metadata Configuration → IDR Run → Unified Identities
```

---

## Quick Start: YAML Configuration

The recommended way to configure IDR is using a `config.yaml` file and the `idr config apply` command.

### 1. Define `config.yaml`

```yaml
# Source Tables
sources:
  - id: crm
    table: "mydb.sales.customers"
    entity_key: "customer_id"
    entity_type: "PERSON"
    identifiers:
       - type: EMAIL
         expr: "lower(email)"
    attributes:
       - name: "first_name"
         expr: "first_name"
       - name: "phone_raw"
         expr: "phone"

  - id: web
    table: "mydb.web.signups"
    entity_key: "user_id"
    entity_type: "PERSON"
    identifiers:
       - type: EMAIL
         expr: "lower(user_email)"
    attributes:
       - name: "first_name"
         expr: "given_name"

# Matching Rules
rules:
  - id: email_rule
    identifier_type: "EMAIL"
    priority: 1
    canonicalize: "LOWERCASE"

# Golden Profile Survivorship
survivorship:
  - attribute: "first_name"
    strategy: "PRIORITY"
    source_priority: ["crm", "web"]  # Trust CRM first, then Web
```

### 2. Apply Configuration

```bash
# automatically populates idr_meta tables
idr config apply --file config.yaml --platform duckdb --db idr.duckdb
```

---

## Configuration Tables

| Table | Purpose |
|-------|---------|
| `idr_meta.source_table` | Register your source tables |
| `idr_meta.rule` | Configure identifier types and matching rules |
| `idr_meta.identifier_mapping` | Map source columns to identifier types |
| `idr_meta.entity_attribute_mapping` | Map columns for golden profile |
| `idr_meta.survivorship_rule` | Define which value wins for each attribute |

---

## Advanced: SQL Reference

If you prefer manual control, you can insert directly into the `idr_meta` tables.

### Manual Source Registration

Add each source table to `idr_meta.source_table`:

```sql
INSERT INTO idr_meta.source_table (
    table_id,           -- Your unique identifier for this source
    table_fqn,          -- Fully qualified table name
    entity_type,        -- PERSON, etc.
    entity_key_expr,    -- SQL expression for unique entity key
    watermark_column,   -- Timestamp column for incremental processing
    watermark_lookback_minutes,
    is_active
) VALUES
    ('crm',
     'mydb.sales.customers',
     'PERSON',
     'customer_id',
     'last_modified_date',
     0,
     TRUE);
```

### Map Identifiers

```sql
INSERT INTO idr_meta.identifier_mapping
    (table_id, identifier_type, identifier_value_expr, is_hashed)
VALUES
    ('crm', 'EMAIL', 'email_address', FALSE),
    ('crm', 'PHONE', 'mobile_phone', FALSE);
```

### Configure Rules

```sql
INSERT INTO idr_meta.rule (
    rule_id, is_active, priority, identifier_type, canonicalize
) VALUES
    ('email_exact', TRUE, 1, 'EMAIL', 'LOWERCASE');
```

### Survivorship Rules

```sql
INSERT INTO idr_meta.survivorship_rule (attribute_name, strategy, source_priority_list) VALUES
    ('first_name', 'PRIORITY', '["crm", "web"]'),
    ('phone_primary', 'RECENCY', NULL);
```

---

## Validation

After configuring metadata, verify:

```sql
-- Check sources registered
SELECT table_id, table_fqn, is_active FROM idr_meta.source_table;
```
