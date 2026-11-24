# Financial Services Industry Template
# For banks, insurance, fintech

## Overview

Account and customer matching for:
- Multi-product customers
- Household relationships
- Regulatory compliance (KYC)

## Identifiers Used

| Identifier | Priority | Max Group Size | Notes |
|------------|----------|----------------|-------|
| SSN | 1 | 10 | Full SSN (highest confidence) |
| ACCOUNT_ID | 2 | 50 | Account numbers |
| EMAIL | 3 | 10,000 | Contact email |
| PHONE | 4 | 5,000 | Primary phone |
| NAME_ADDRESS | 5 | 500 | Name + mailing address |

## Source Tables

```sql
INSERT INTO idr_meta.source_table
(table_id, table_fqn, entity_type, entity_key_expr, watermark_column, watermark_lookback_minutes, is_active) VALUES
('checking', 'banking.checking_accounts', 'CUSTOMER', 'account_holder_id', 'last_activity', 0, TRUE),
('savings', 'banking.savings_accounts', 'CUSTOMER', 'owner_id', 'last_activity', 0, TRUE),
('credit_card', 'cards.cardholders', 'CUSTOMER', 'cardholder_id', 'statement_date', 0, TRUE),
('loans', 'lending.loan_applicants', 'CUSTOMER', 'applicant_id', 'application_date', 0, TRUE),
('kyc', 'compliance.kyc_records', 'CUSTOMER', 'customer_id', 'verification_date', 0, TRUE);
```

## Rules

```sql
INSERT INTO idr_meta.rule
(rule_id, identifier_type, canonicalize, max_group_size, priority, is_active) VALUES
('ssn_rule', 'SSN', 'PHONE_DIGITS', 10, 1, TRUE),          -- Very strict (SSN should be unique)
('account_rule', 'ACCOUNT_ID', 'UPPERCASE', 50, 2, TRUE),
('email_rule', 'EMAIL', 'EMAIL_STANDARD', 10000, 3, TRUE),
('phone_rule', 'PHONE', 'PHONE_US_10', 5000, 4, TRUE),
('name_addr_rule', 'NAME_ADDRESS', 'UPPERCASE', 500, 5, TRUE);
```

## Identifier Mappings

```sql
-- Checking accounts
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
('checking', 'SSN', 'tax_id', FALSE),
('checking', 'ACCOUNT_ID', 'account_number', FALSE),
('checking', 'EMAIL', 'email_address', FALSE),
('checking', 'PHONE', 'primary_phone', FALSE),
('checking', 'NAME_ADDRESS', 'UPPER(name) || ''|'' || UPPER(mailing_address)', FALSE);

-- Credit cards
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
('credit_card', 'SSN', 'ssn', FALSE),
('credit_card', 'ACCOUNT_ID', 'card_number', FALSE),  -- Use card # as account
('credit_card', 'EMAIL', 'contact_email', FALSE),
('credit_card', 'PHONE', 'mobile_phone', FALSE);

-- KYC
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
('kyc', 'SSN', 'verified_ssn', FALSE),
('kyc', 'EMAIL', 'verified_email', FALSE),
('kyc', 'PHONE', 'verified_phone', FALSE),
('kyc', 'NAME_ADDRESS', 'UPPER(legal_name) || ''|'' || UPPER(verified_address)', FALSE);
```

## Exclusions

```sql
INSERT INTO idr_meta.identifier_exclusion (identifier_type, identifier_value_pattern, match_type, reason) VALUES
('SSN', '000000000', 'EXACT', 'Invalid SSN'),
('SSN', '999999999', 'EXACT', 'Invalid SSN'),
('SSN', '123456789', 'EXACT', 'Test SSN'),
('EMAIL', '%@test.com', 'LIKE', 'Test domain'),
('ACCOUNT_ID', '0%', 'LIKE', 'Invalid account');
```

## Regulatory Notes

⚠️ **Compliance Considerations:**
- SSN handling must comply with data protection regulations
- Audit trail required for all identity changes
- KYC verified data should take precedence in golden profile
- Consider PII encryption at rest
- Dry run mandatory before production changes
