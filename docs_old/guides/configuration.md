---
tags:
  - configuration
  - rules
  - identity-modeling
---

# Act III: Building Your Truth

**The engine knows how to match graphs, but it doesn't know *your* business.**

Is a shared email enough to link two customers? What about a shared phone number? What if that phone number belongs to a shared office lobby?

In this Act, you will teach the engine how to define "Identity" for your organization.

---

## The Master Plan (`config.yaml`)

While you *can* write raw SQL to configure the system, the unified workflow uses a readable YAML file.

### 1. Mapping Reality (Sources)

First, tell the engine where your data lives.

```yaml
sources:
  - id: customers
    table: crm.customers
    entity_key: customer_id
    watermark_column: updated_at
    identifiers:
      - type: EMAIL
        expr: lower(email_address)  # Normalize inputs!

  - id: transactions
    table: pos.transactions
    entity_key: user_id
    identifiers:
      - type: CARD_HASH
        expr: card_fingerprint
```

### 2. Defining Identity (Rules)

Now, define the "Physics of Identity". Which links are strong enough to merge profiles?

```yaml
rules:
  # High Confidence: Use strict matching
  - id: email_exact
    type: EXACT
    # Current version supports single-key strict matching per rule
    match_keys: [EMAIL]
    priority: 1
    max_group_size: 5000  # Safety valve for shared emails

  # Lower Confidence rules can be added here
```

### 3. Advanced: Fuzzy Rules

For probabilistic matching (e.g., "John Smith" vs "Jon Smith"), you can define `fuzzy_rules`. The `score_expr` must use functions available on your target platform.

```yaml
fuzzy_rules:
  - id: FR_NAME
    name: "Name Fuzzy Match"
    # Blocking: Use native phonetic function to reduce candidate set
    blocking_key: "SOUNDEX(first_name)"
    # Scoring: Use platform-specific similarity function
    score_expr: "idr_meta.jaro_winkler_similarity(<a>, <b>)"
    threshold: 0.85
    priority: 100
```

#### Platform-Specific `score_expr` Options

| Platform | `score_expr` Example | Notes |
|----------|---------------------|-------|
| **Snowflake** | `idr_meta.jaro_winkler_similarity(<a>, <b>)` | Native JAROWINKLER_SIMILARITY |
| **Databricks** | `idr_meta.jaro_winkler_similarity(<a>, <b>)` | Native function |
| **DuckDB** | `jaro_winkler_similarity(<a>, <b>)` | Native function |

### 4. Handling Noise (Exclusions)

Real data is messy. You will find thousands of customers with the email `null@test.com`. Tell the engine to ignore these.

```yaml
exclusions:
  - type: EMAIL
    value: "test@test.com"
  - type: PHONE
    value: "0000000000"
```

---

## Advanced: The Metadata Tables

Under the hood, the builder converts your YAML into standard SQL INSERTs into the metadata tables.

| Layer | Table | Purpose |
|:---|:---|:---|
| **Sources** | `idr_meta.source_table` | Registry of active tables. |
| **Mapping** | `idr_meta.identifier_mapping` | How to extract keys (SQL expressions). |
| **Logic** | `idr_meta.rule` | Priorities and safety limits. |
| **Safety** | `idr_meta.identifier_exclusion` | Blocklist for bad data. |

### Example: Manual Rule Injection

```sql
INSERT INTO idr_meta.rule (rule_id, identifier_type, priority, is_active)
VALUES ('loyalty_card', 'LOYALTY_ID', 1, TRUE);
```

---

## Next Station: Production

[:octicons-arrow-right-24: Act IV: Production](production-hardening.md)
