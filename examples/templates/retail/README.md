# Retail Industry Template
# For retailers.

## Overview

This template is designed for retail companies with:
- E-commerce (web + mobile)
- Physical stores (POS)
- Loyalty programs
- Multiple customer touchpoints

## Identifiers Used

| Identifier | Priority | Max Group Size | Notes |
|------------|----------|----------------|-------|
| EMAIL | 1 | 50,000 | Primary identifier |
| PHONE | 2 | 10,000 | Secondary |
| LOYALTY_ID | 3 | 100 | High confidence |
| ADDRESS | 4 | 5,000 | Household-level |

## Source Tables

Typical retail sources:

```sql
-- Register your sources
INSERT INTO idr_meta.source_table
(table_id, table_fqn, entity_type, entity_key_expr, watermark_column, watermark_lookback_minutes, is_active) VALUES
('ecommerce', 'sales.ecommerce_customers', 'PERSON', 'customer_id', 'updated_at', 0, TRUE),
('store_pos', 'sales.store_transactions', 'PERSON', 'transaction_id', 'transaction_date', 0, TRUE),
('loyalty', 'marketing.loyalty_members', 'PERSON', 'member_id', 'enrollment_date', 0, TRUE),
('mobile_app', 'sales.app_users', 'PERSON', 'user_id', 'last_active', 0, TRUE),
('call_center', 'support.customer_calls', 'PERSON', 'call_id', 'call_date', 0, TRUE);
```

## Rules

```sql
-- Configure identifier rules
INSERT INTO idr_meta.rule
(rule_id, identifier_type, canonicalize, max_group_size, priority, is_active) VALUES
('email_rule', 'EMAIL', 'EMAIL_STANDARD', 50000, 1, TRUE),
('phone_rule', 'PHONE', 'PHONE_US_10', 10000, 2, TRUE),
('loyalty_rule', 'LOYALTY_ID', 'UPPERCASE', 100, 3, TRUE),
('address_rule', 'ADDRESS', 'UPPERCASE', 5000, 4, TRUE);
```

## Identifier Mappings

```sql
-- E-commerce
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
('ecommerce', 'EMAIL', 'email', FALSE),
('ecommerce', 'PHONE', 'phone', FALSE),
('ecommerce', 'LOYALTY_ID', 'loyalty_number', FALSE),
('ecommerce', 'ADDRESS', 'shipping_address', FALSE);

-- Store POS
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
('store_pos', 'EMAIL', 'receipt_email', FALSE),
('store_pos', 'PHONE', 'customer_phone', FALSE),
('store_pos', 'LOYALTY_ID', 'loyalty_scan', FALSE);

-- Loyalty
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
('loyalty', 'EMAIL', 'email_address', FALSE),
('loyalty', 'PHONE', 'mobile_phone', FALSE),
('loyalty', 'LOYALTY_ID', 'member_id', FALSE),
('loyalty', 'ADDRESS', 'mailing_address', FALSE);

-- Mobile App
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
('mobile_app', 'EMAIL', 'registered_email', FALSE),
('mobile_app', 'PHONE', 'device_phone', FALSE),
('mobile_app', 'LOYALTY_ID', 'linked_loyalty', FALSE);
```

## Exclusions

Common retail exclusions:

```sql
-- Exclude test/generic values
INSERT INTO idr_meta.identifier_exclusion (identifier_type, identifier_value_pattern, match_type, reason) VALUES
('EMAIL', 'test@%', 'LIKE', 'Test emails'),
('EMAIL', '%@example.com', 'LIKE', 'Example domain'),
('EMAIL', 'noreply@%', 'LIKE', 'No-reply emails'),
('PHONE', '0000000000', 'EXACT', 'Placeholder phone'),
('PHONE', '1111111111', 'EXACT', 'Placeholder phone');
```

## Expected Results

For a typical retailer with 20M customer records:

| Metric | Expected Range |
|--------|----------------|
| Unique clusters | 8-12M |
| Average cluster size | 1.8-2.5 |
| Singletons | 35-50% |
| Large clusters (>100) | <0.1% |

## Sample Queries

```sql
-- Find customers with activity across multiple channels
SELECT
    c.resolved_id,
    c.cluster_size,
    COUNT(DISTINCT m.entity_key) as touchpoints
FROM idr_out.identity_clusters_current c
JOIN idr_out.identity_resolved_membership_current m ON c.resolved_id = m.resolved_id
WHERE c.cluster_size > 3
GROUP BY 1, 2
ORDER BY touchpoints DESC
LIMIT 100;

-- Attribution: which channel acquired each customer?
SELECT
    m.resolved_id,
    MIN(src.created_at) as first_touch,
    FIRST(src.source_system ORDER BY src.created_at) as acquisition_channel
FROM idr_out.identity_resolved_membership_current m
JOIN your_source_table src ON m.entity_key = src.customer_id
GROUP BY 1;
```
