# Healthcare Industry Template
# For hospitals, health systems, and payers

## Overview

Healthcare patient matching with focus on:
- High accuracy requirements (life-safety)
- HIPAA compliance considerations
- Multiple facility/system integration

## Identifiers Used

| Identifier | Priority | Max Group Size | Notes |
|------------|----------|----------------|-------|
| MRN | 1 | 50 | Medical Record Number (facility-specific) |
| SSN_LAST4 | 2 | 500 | Last 4 of SSN (not full SSN for privacy) |
| NAME_DOB | 3 | 100 | First + Last + DOB combo |
| EMAIL | 4 | 10,000 | Patient portal email |
| PHONE | 5 | 5,000 | Contact phone |

## Source Tables

```sql
INSERT INTO idr_meta.source_table
(table_id, table_fqn, entity_type, entity_key_expr, watermark_column, watermark_lookback_minutes, is_active) VALUES
('emr_facility_a', 'clinical.patients_facility_a', 'PATIENT', 'patient_id', 'last_visit', 0, TRUE),
('emr_facility_b', 'clinical.patients_facility_b', 'PATIENT', 'patient_id', 'last_visit', 0, TRUE),
('billing', 'claims.patient_accounts', 'PATIENT', 'account_id', 'service_date', 0, TRUE),
('portal', 'digital.patient_portal_users', 'PATIENT', 'user_id', 'last_login', 0, TRUE),
('lab', 'clinical.lab_orders', 'PATIENT', 'order_id', 'order_date', 0, TRUE);
```

## Rules

```sql
INSERT INTO idr_meta.rule
(rule_id, identifier_type, canonicalize, max_group_size, priority, is_active) VALUES
('mrn_rule', 'MRN', 'UPPERCASE', 50, 1, TRUE),           -- Very low max (MRNs should be unique per facility)
('ssn_rule', 'SSN_LAST4', 'PHONE_DIGITS', 500, 2, TRUE), -- Low max (collision risk)
('name_dob_rule', 'NAME_DOB', 'UPPERCASE', 100, 3, TRUE),-- Medium (common names exist)
('email_rule', 'EMAIL', 'EMAIL_STANDARD', 10000, 4, TRUE),
('phone_rule', 'PHONE', 'PHONE_US_10', 5000, 5, TRUE);
```

## Identifier Mappings

```sql
-- EMR Facility A
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
('emr_facility_a', 'MRN', '''FacA_'' || mrn', FALSE),  -- Prefix to make unique
('emr_facility_a', 'SSN_LAST4', 'RIGHT(ssn, 4)', FALSE),
('emr_facility_a', 'NAME_DOB', 'UPPER(first_name) || ''|'' || UPPER(last_name) || ''|'' || dob', FALSE),
('emr_facility_a', 'EMAIL', 'email', FALSE),
('emr_facility_a', 'PHONE', 'phone', FALSE);

-- EMR Facility B
INSERT INTO idr_meta.identifier_mapping (table_id, identifier_type, identifier_value_expr, is_hashed) VALUES
('emr_facility_b', 'MRN', '''FacB_'' || medical_record_number', FALSE),
('emr_facility_b', 'SSN_LAST4', 'RIGHT(social_security, 4)', FALSE),
('emr_facility_b', 'NAME_DOB', 'UPPER(given_name) || ''|'' || UPPER(family_name) || ''|'' || birth_date', FALSE),
('emr_facility_b', 'EMAIL', 'contact_email', FALSE),
('emr_facility_b', 'PHONE', 'contact_phone', FALSE);
```

## Exclusions

```sql
-- Healthcare-specific exclusions
INSERT INTO idr_meta.identifier_exclusion (identifier_type, identifier_value_pattern, match_type, reason) VALUES
('SSN_LAST4', '0000', 'EXACT', 'Invalid SSN'),
('SSN_LAST4', '9999', 'EXACT', 'Invalid SSN'),
('NAME_DOB', 'UNKNOWN%', 'LIKE', 'Unknown patient'),
('NAME_DOB', 'JOHN|DOE|%', 'LIKE', 'Placeholder name'),
('NAME_DOB', 'JANE|DOE|%', 'LIKE', 'Placeholder name'),
('EMAIL', '%@hospital.internal', 'LIKE', 'Internal emails');
```

## Compliance Notes

⚠️ **HIPAA Considerations:**
- Never use full SSN as an identifier in logs
- Ensure audit logging is enabled
- Dry run before any production changes
- Maintain access controls on IDR tables
- Consider data minimization in work tables

## Expected Results

| Metric | Expected Range |
|--------|----------------|
| Match rate across facilities | 60-80% |
| False positive rate | <0.1% |
| Singletons | 30-50% |
