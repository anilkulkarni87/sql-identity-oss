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
INSERT INTO idr_meta.source_table VALUES
('ecommerce', 'sales.ecommerce_customers', 'PERSON', 'customer_id', 'updated_at', 0, TRUE),
('store_pos', 'sales.store_transactions', 'PERSON', 'transaction_id', 'transaction_date', 0, TRUE),
('loyalty', 'marketing.loyalty_members', 'PERSON', 'member_id', 'enrollment_date', 0, TRUE),
('mobile_app', 'sales.app_users', 'PERSON', 'user_id', 'last_active', 0, TRUE),
('call_center', 'support.customer_calls', 'PERSON', 'call_id', 'call_date', 0, TRUE);
```

## Rules

```sql
-- Configure identifier rules
INSERT INTO idr_meta.rule VALUES
('email_rule', 'EMAIL', 1, TRUE, 50000),
('phone_rule', 'PHONE', 2, TRUE, 10000),
('loyalty_rule', 'LOYALTY_ID', 3, TRUE, 100),
('address_rule', 'ADDRESS', 4, TRUE, 5000);
```

## Identifier Mappings

```sql
-- E-commerce
INSERT INTO idr_meta.identifier_mapping VALUES
('ecommerce', 'EMAIL', 'email', TRUE),
('ecommerce', 'PHONE', 'phone', TRUE),
('ecommerce', 'LOYALTY_ID', 'loyalty_number', TRUE),
('ecommerce', 'ADDRESS', 'shipping_address', TRUE);

-- Store POS
INSERT INTO idr_meta.identifier_mapping VALUES
('store_pos', 'EMAIL', 'receipt_email', TRUE),
('store_pos', 'PHONE', 'customer_phone', TRUE),
('store_pos', 'LOYALTY_ID', 'loyalty_scan', TRUE);

-- Loyalty
INSERT INTO idr_meta.identifier_mapping VALUES
('loyalty', 'EMAIL', 'email_address', TRUE),
('loyalty', 'PHONE', 'mobile_phone', TRUE),
('loyalty', 'LOYALTY_ID', 'member_id', FALSE),  -- Already normalized
('loyalty', 'ADDRESS', 'mailing_address', TRUE);

-- Mobile App
INSERT INTO idr_meta.identifier_mapping VALUES
('mobile_app', 'EMAIL', 'registered_email', TRUE),
('mobile_app', 'PHONE', 'device_phone', TRUE),
('mobile_app', 'LOYALTY_ID', 'linked_loyalty', TRUE);
```

## Exclusions

Common retail exclusions:

```sql
-- Exclude test/generic values
INSERT INTO idr_meta.identifier_exclusion VALUES
('EMAIL', 'test@%', TRUE, 'Test emails', CURRENT_TIMESTAMP),
('EMAIL', '%@example.com', TRUE, 'Example domain', CURRENT_TIMESTAMP),
('EMAIL', 'noreply@%', TRUE, 'No-reply emails', CURRENT_TIMESTAMP),
('PHONE', '0000000000', FALSE, 'Placeholder phone', CURRENT_TIMESTAMP),
('PHONE', '1111111111', FALSE, 'Placeholder phone', CURRENT_TIMESTAMP);
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
