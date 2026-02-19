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
INSERT INTO idr_meta.source_table VALUES
('checking', 'banking.checking_accounts', 'CUSTOMER', 'account_holder_id', 'last_activity', 0, TRUE),
('savings', 'banking.savings_accounts', 'CUSTOMER', 'owner_id', 'last_activity', 0, TRUE),
('credit_card', 'cards.cardholders', 'CUSTOMER', 'cardholder_id', 'statement_date', 0, TRUE),
('loans', 'lending.loan_applicants', 'CUSTOMER', 'applicant_id', 'application_date', 0, TRUE),
('kyc', 'compliance.kyc_records', 'CUSTOMER', 'customer_id', 'verification_date', 0, TRUE);
```

## Rules

```sql
INSERT INTO idr_meta.rule VALUES
('ssn_rule', 'SSN', 1, TRUE, 10),          -- Very strict (SSN should be unique)
('account_rule', 'ACCOUNT_ID', 2, TRUE, 50),
('email_rule', 'EMAIL', 3, TRUE, 10000),
('phone_rule', 'PHONE', 4, TRUE, 5000),
('name_addr_rule', 'NAME_ADDRESS', 5, TRUE, 500);
```

## Identifier Mappings

```sql
-- Checking accounts
INSERT INTO idr_meta.identifier_mapping VALUES
('checking', 'SSN', 'tax_id', FALSE),
('checking', 'ACCOUNT_ID', 'account_number', FALSE),
('checking', 'EMAIL', 'email_address', TRUE),
('checking', 'PHONE', 'primary_phone', TRUE),
('checking', 'NAME_ADDRESS', 'UPPER(name) || ''|'' || UPPER(mailing_address)', FALSE);

-- Credit cards
INSERT INTO idr_meta.identifier_mapping VALUES
('credit_card', 'SSN', 'ssn', FALSE),
('credit_card', 'ACCOUNT_ID', 'card_number', FALSE),  -- Use card # as account
('credit_card', 'EMAIL', 'contact_email', TRUE),
('credit_card', 'PHONE', 'mobile_phone', TRUE);

-- KYC
INSERT INTO idr_meta.identifier_mapping VALUES
('kyc', 'SSN', 'verified_ssn', FALSE),
('kyc', 'EMAIL', 'verified_email', TRUE),
('kyc', 'PHONE', 'verified_phone', TRUE),
('kyc', 'NAME_ADDRESS', 'UPPER(legal_name) || ''|'' || UPPER(verified_address)', FALSE);
```

## Exclusions

```sql
INSERT INTO idr_meta.identifier_exclusion VALUES
('SSN', '000000000', FALSE, 'Invalid SSN', CURRENT_TIMESTAMP),
('SSN', '999999999', FALSE, 'Invalid SSN', CURRENT_TIMESTAMP),
('SSN', '123456789', FALSE, 'Test SSN', CURRENT_TIMESTAMP),
('EMAIL', '%@test.com', TRUE, 'Test domain', CURRENT_TIMESTAMP),
('ACCOUNT_ID', '0%', TRUE, 'Invalid account', CURRENT_TIMESTAMP);
```

## Regulatory Notes

⚠️ **Compliance Considerations:**
- SSN handling must comply with data protection regulations
- Audit trail required for all identity changes
- KYC verified data should take precedence in golden profile
- Consider PII encryption at rest
- Dry run mandatory before production changes
