---
tags:
  - example
  - tutorial
  - customer-360
  - walkthrough
---

# Example: Customer Data Unification

A complete walkthrough of unifying customer data from three source systems.

---

## The Scenario

You have customer data in three systems:

| System | Records | Identifiers |
|--------|---------|-------------|
| CRM | 500K | email, phone, customer_id |
| E-commerce | 2M | email, loyalty_id |
| Support Tickets | 300K | email, phone |

**Goal**: Create a unified customer view with a single `resolved_id` per real customer.

---

## Step 1: Sample Data

### CRM Customers

```sql
-- crm.customers
| customer_id | email              | phone      | first_name | created_at |
|-------------|--------------------| -----------|------------|------------|
| C001        | john@acme.com      | 5551234567 | John       | 2024-01-01 |
| C002        | jane@example.org   | 5559876543 | Jane       | 2024-01-02 |
| C003        | bob@test.com       | NULL       | Bob        | 2024-01-03 |
```

### E-commerce Orders

```sql
-- ecom.orders
| order_id | customer_email     | loyalty_id | order_date |
|----------|--------------------|------------|------------|
| O001     | john@acme.com      | LY100      | 2024-02-01 |
| O002     | JOHN@ACME.COM      | LY100      | 2024-02-15 |  -- Same person, different case
| O003     | jane@example.org   | LY200      | 2024-02-20 |
| O004     | alice@new.com      | NULL       | 2024-02-25 |
```

### Support Tickets

```sql
-- support.tickets
| ticket_id | contact_email | contact_phone | created_at |
|-----------|---------------|---------------|------------|
| T001      | john@acme.com | 5551234567    | 2024-03-01 |
| T002      | bob@test.com  | 5550001111    | 2024-03-02 |  -- Bob now has a phone!
| T003      | new@person.com| 5552223333    | 2024-03-03 |
```

---

## Step 2: Configure Sources

```sql
-- Register source tables
INSERT INTO idr_meta.source_table VALUES
  ('crm_customers', 'crm.customers', 'PERSON', 'customer_id', 'created_at', 0, TRUE),
  ('ecom_orders', 'ecom.orders', 'PERSON', 'order_id', 'order_date', 0, TRUE),
  ('support_tickets', 'support.tickets', 'PERSON', 'ticket_id', 'created_at', 0, TRUE);

-- Source priority for golden profile
INSERT INTO idr_meta.source VALUES
  ('crm_customers', 'CRM', 1, TRUE),      -- Highest priority
  ('ecom_orders', 'E-commerce', 2, TRUE),
  ('support_tickets', 'Support', 3, TRUE);
```

---

## Step 3: Configure Rules

```sql
-- Matching rules
INSERT INTO idr_meta.rule VALUES
  ('email_exact', 'EMAIL', 1, TRUE, 10000),   -- Highest priority, max 10K per group
  ('phone_exact', 'PHONE', 2, TRUE, 5000),
  ('loyalty_exact', 'LOYALTY', 3, TRUE, 1000);

-- Exclusions (common values to skip)
INSERT INTO idr_meta.identifier_exclusion VALUES
  ('EMAIL', 'noreply@%', TRUE, 'Generic noreply'),
  ('EMAIL', 'test@%', TRUE, 'Test emails'),
  ('PHONE', '0000000000', FALSE, 'Invalid phone');
```

---

## Step 4: Map Identifiers

```sql
-- Tell IDR where to find identifiers in each source
INSERT INTO idr_meta.identifier_mapping VALUES
  ('crm_customers', 'EMAIL', 'email', TRUE),
  ('crm_customers', 'PHONE', 'phone', TRUE),
  ('ecom_orders', 'EMAIL', 'customer_email', TRUE),
  ('ecom_orders', 'LOYALTY', 'loyalty_id', TRUE),
  ('support_tickets', 'EMAIL', 'contact_email', TRUE),
  ('support_tickets', 'PHONE', 'contact_phone', TRUE);
```

---

## Step 5: Run Dry Run

```bash
```bash
# Preview changes
idr run --platform=duckdb --db=idr.duckdb --mode=FULL --dry-run
```
```

**Output:**
```
============================================================
DRY RUN SUMMARY
============================================================
New Entities:      10
Edges Would Create: 8
Largest Cluster:   4 entities (john@acme.com chain)

REVIEW: SELECT * FROM idr_out.dry_run_results
============================================================
```

---

## Step 6: Review Results

```sql
-- Check proposed clusters
SELECT
  entity_key,
  proposed_resolved_id,
  change_type
FROM idr_out.dry_run_results
ORDER BY proposed_resolved_id;
```

**Expected Results:**

| entity_key | proposed_resolved_id | change_type |
|------------|---------------------|-------------|
| crm_customers:C001 | crm_customers:C001 | NEW |
| ecom_orders:O001 | crm_customers:C001 | NEW |
| ecom_orders:O002 | crm_customers:C001 | NEW |
| support_tickets:T001 | crm_customers:C001 | NEW |
| crm_customers:C002 | crm_customers:C002 | NEW |
| ecom_orders:O003 | crm_customers:C002 | NEW |
| crm_customers:C003 | crm_customers:C003 | NEW |
| support_tickets:T002 | crm_customers:C003 | NEW |
| ecom_orders:O004 | ecom_orders:O004 | NEW |
| support_tickets:T003 | support_tickets:T003 | NEW |

**John's cluster** (resolved_id = `crm_customers:C001`) contains 4 entities:
- CRM customer (via email)
- 2 orders (via email, normalized case)
- Support ticket (via email AND phone)

**Bob's cluster** (resolved_id = `crm_customers:C003`) shows transitive linking!
- CRM has Bob with email but no phone
- Support ticket has Bob's email AND a phone
- They're linked via email

---

## Step 7: Commit Run

```bash
```bash
# Execute and commit
idr run --platform=duckdb --db=idr.duckdb --mode=FULL
```
```

---

## Step 8: Query Unified Data

### Get cluster membership

```sql
SELECT
  resolved_id,
  COUNT(*) as entity_count,
  LISTAGG(entity_key, ', ') as entities
FROM idr_out.identity_resolved_membership_current
GROUP BY resolved_id
ORDER BY entity_count DESC;
```

### Get golden profiles

```sql
SELECT
  resolved_id,
  email_primary,
  phone_primary,
  first_name,
  last_name
FROM idr_out.golden_profile_current;
```

### Join to get customer 360 view

```sql
SELECT
  g.resolved_id as unified_customer_id,
  g.email_primary,
  g.phone_primary,
  COUNT(DISTINCT CASE WHEN m.entity_key LIKE 'ecom_%' THEN m.entity_key END) as order_count,
  COUNT(DISTINCT CASE WHEN m.entity_key LIKE 'support_%' THEN m.entity_key END) as ticket_count
FROM idr_out.golden_profile_current g
JOIN idr_out.identity_resolved_membership_current m ON m.resolved_id = g.resolved_id
GROUP BY 1, 2, 3;
```

---

## Key Takeaways

1. **Case normalization** - `john@acme.com` and `JOHN@ACME.COM` matched
2. **Transitive linking** - Bob linked via email chain
3. **Multi-identifier** - John matched via both email AND phone
4. **Source priority** - CRM data used for golden profile (priority=1)
5. **Dry run first** - Always preview before committing

---

## Next Steps

- [Production Hardening](production-hardening.md) - Add exclusions and limits
- [Metrics & Monitoring](metrics-monitoring.md) - Track cluster quality
- [dbt Package](dbt-package.md) - Run via dbt workflow
