# B2B SaaS Industry Template
# For lead deduplication and account matching

## Overview

B2B matching focused on:
- Lead deduplication
- Account/company matching
- Contact-to-account association

## Identifiers Used

| Identifier | Priority | Max Group Size | Notes |
|------------|----------|----------------|-------|
| EMAIL | 1 | 10,000 | Work email (primary) |
| DOMAIN | 2 | 5,000 | Email domain for company matching |
| COMPANY_NORM | 3 | 1,000 | Normalized company name |
| PHONE | 4 | 5,000 | Business phone |
| LINKEDIN | 5 | 100 | LinkedIn profile URL |

## Source Tables

```sql
INSERT INTO idr_meta.source_table
(table_id, table_fqn, entity_type, entity_key_expr, watermark_column, watermark_lookback_minutes, is_active) VALUES
('crm_leads', 'sales.crm_leads', 'CONTACT', 'lead_id', 'created_date', 0, TRUE),
('crm_contacts', 'sales.crm_contacts', 'CONTACT', 'contact_id', 'last_modified', 0, TRUE),
('marketing', 'marketing.email_subscribers', 'CONTACT', 'subscriber_id', 'signup_date', 0, TRUE),
('website', 'analytics.form_submissions', 'CONTACT', 'submission_id', 'submitted_at', 0, TRUE),
('enrichment', 'data.clearbit_enriched', 'CONTACT', 'record_id', 'enriched_at', 0, TRUE);
```

## Rules

```sql
INSERT INTO idr_meta.rule
(rule_id, identifier_type, canonicalize, max_group_size, priority, is_active) VALUES
('email_rule', 'EMAIL', 'EMAIL_STANDARD', 10000, 1, TRUE),
('domain_rule', 'DOMAIN', 'LOWERCASE', 5000, 2, TRUE),
('company_rule', 'COMPANY_NORM', 'ALPHANUMERIC_UPPER', 1000, 3, TRUE),
('phone_rule', 'PHONE', 'PHONE_DIGITS', 5000, 4, TRUE),
('linkedin_rule', 'LINKEDIN', 'LOWERCASE', 100, 5, TRUE);
```

## Identifier Mappings

```sql
-- CRM Leads
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
('crm_leads', 'EMAIL', 'email', FALSE),
('crm_leads', 'DOMAIN', 'SPLIT_PART(email, ''@'', 2)', FALSE),
('crm_leads', 'COMPANY_NORM', 'company', FALSE),
('crm_leads', 'PHONE', 'phone', FALSE);

-- CRM Contacts
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
('crm_contacts', 'EMAIL', 'work_email', FALSE),
('crm_contacts', 'DOMAIN', 'SPLIT_PART(work_email, ''@'', 2)', FALSE),
('crm_contacts', 'COMPANY_NORM', 'account_name', FALSE),
('crm_contacts', 'PHONE', 'direct_phone', FALSE),
('crm_contacts', 'LINKEDIN', 'linkedin_url', FALSE);

-- Marketing
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
('marketing', 'EMAIL', 'subscriber_email', FALSE),
('marketing', 'DOMAIN', 'SPLIT_PART(subscriber_email, ''@'', 2)', FALSE),
('marketing', 'COMPANY_NORM', 'provided_company', FALSE);

-- Enrichment
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
('enrichment', 'EMAIL', 'email', FALSE),
('enrichment', 'DOMAIN', 'company_domain', FALSE),
('enrichment', 'COMPANY_NORM', 'company_name', FALSE),
('enrichment', 'LINKEDIN', 'linkedin_handle', FALSE);
```

## Exclusions

```sql
-- B2B specific exclusions
INSERT INTO idr_meta.identifier_exclusion (identifier_type, identifier_value_pattern, match_type, reason) VALUES
-- Free email providers (not useful for B2B matching)
('DOMAIN', 'gmail.com', 'EXACT', 'Personal email'),
('DOMAIN', 'yahoo.com', 'EXACT', 'Personal email'),
('DOMAIN', 'hotmail.com', 'EXACT', 'Personal email'),
('DOMAIN', 'outlook.com', 'EXACT', 'Personal email'),
-- Generic company names
('COMPANY_NORM', 'TEST%', 'LIKE', 'Test company'),
('COMPANY_NORM', 'UNKNOWN', 'EXACT', 'Unknown company'),
('COMPANY_NORM', 'N/A', 'EXACT', 'Missing company'),
-- Invalid LinkedIn
('LINKEDIN', '%/in/null%', 'LIKE', 'Invalid LinkedIn');
```

## Company Name Normalization

Consider normalizing company names before matching:

```sql
-- Example normalization function
CREATE OR REPLACE FUNCTION normalize_company(name VARCHAR)
RETURNS VARCHAR AS $$
SELECT REGEXP_REPLACE(
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            UPPER(TRIM(name)),
            '(,? ?(INC|LLC|LTD|CORP|CORPORATION|COMPANY|CO)\.?)$', ''),
        '[^A-Z0-9 ]', ''),
    '  +', ' ')
$$;
```

## Expected Results

| Metric | Expected Range |
|--------|----------------|
| Lead deduplication rate | 20-40% |
| Cross-source matches | 30-50% |
| Company match rate | 60-80% |
