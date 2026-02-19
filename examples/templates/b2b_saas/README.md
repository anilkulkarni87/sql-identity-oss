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
INSERT INTO idr_meta.source_table VALUES
('crm_leads', 'sales.crm_leads', 'CONTACT', 'lead_id', 'created_date', 0, TRUE),
('crm_contacts', 'sales.crm_contacts', 'CONTACT', 'contact_id', 'last_modified', 0, TRUE),
('marketing', 'marketing.email_subscribers', 'CONTACT', 'subscriber_id', 'signup_date', 0, TRUE),
('website', 'analytics.form_submissions', 'CONTACT', 'submission_id', 'submitted_at', 0, TRUE),
('enrichment', 'data.clearbit_enriched', 'CONTACT', 'record_id', 'enriched_at', 0, TRUE);
```

## Rules

```sql
INSERT INTO idr_meta.rule VALUES
('email_rule', 'EMAIL', 1, TRUE, 10000),
('domain_rule', 'DOMAIN', 2, TRUE, 5000),
('company_rule', 'COMPANY_NORM', 3, TRUE, 1000),
('phone_rule', 'PHONE', 4, TRUE, 5000),
('linkedin_rule', 'LINKEDIN', 5, TRUE, 100);
```

## Identifier Mappings

```sql
-- CRM Leads
INSERT INTO idr_meta.identifier_mapping VALUES
('crm_leads', 'EMAIL', 'email', TRUE),
('crm_leads', 'DOMAIN', 'SPLIT_PART(email, ''@'', 2)', FALSE),
('crm_leads', 'COMPANY_NORM', 'company', TRUE),
('crm_leads', 'PHONE', 'phone', TRUE);

-- CRM Contacts
INSERT INTO idr_meta.identifier_mapping VALUES
('crm_contacts', 'EMAIL', 'work_email', TRUE),
('crm_contacts', 'DOMAIN', 'SPLIT_PART(work_email, ''@'', 2)', FALSE),
('crm_contacts', 'COMPANY_NORM', 'account_name', TRUE),
('crm_contacts', 'PHONE', 'direct_phone', TRUE),
('crm_contacts', 'LINKEDIN', 'linkedin_url', TRUE);

-- Marketing
INSERT INTO idr_meta.identifier_mapping VALUES
('marketing', 'EMAIL', 'subscriber_email', TRUE),
('marketing', 'DOMAIN', 'SPLIT_PART(subscriber_email, ''@'', 2)', FALSE),
('marketing', 'COMPANY_NORM', 'provided_company', TRUE);

-- Enrichment
INSERT INTO idr_meta.identifier_mapping VALUES
('enrichment', 'EMAIL', 'email', TRUE),
('enrichment', 'DOMAIN', 'company_domain', FALSE),
('enrichment', 'COMPANY_NORM', 'company_name', TRUE),
('enrichment', 'LINKEDIN', 'linkedin_handle', TRUE);
```

## Exclusions

```sql
-- B2B specific exclusions
INSERT INTO idr_meta.identifier_exclusion VALUES
-- Free email providers (not useful for B2B matching)
('DOMAIN', 'gmail.com', FALSE, 'Personal email', CURRENT_TIMESTAMP),
('DOMAIN', 'yahoo.com', FALSE, 'Personal email', CURRENT_TIMESTAMP),
('DOMAIN', 'hotmail.com', FALSE, 'Personal email', CURRENT_TIMESTAMP),
('DOMAIN', 'outlook.com', FALSE, 'Personal email', CURRENT_TIMESTAMP),
-- Generic company names
('COMPANY_NORM', 'TEST%', TRUE, 'Test company', CURRENT_TIMESTAMP),
('COMPANY_NORM', 'UNKNOWN', FALSE, 'Unknown company', CURRENT_TIMESTAMP),
('COMPANY_NORM', 'N/A', FALSE, 'Missing company', CURRENT_TIMESTAMP),
-- Invalid LinkedIn
('LINKEDIN', '%/in/null%', TRUE, 'Invalid LinkedIn', CURRENT_TIMESTAMP);
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
