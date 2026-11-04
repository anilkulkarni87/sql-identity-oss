# How It Works: An Intuitive Guide

Identity resolution can sound complex, but its core principle is simple: **connecting fragmented records that belong to the exact same real-world individual or household**.

This guide explains how `sql-identity-oss` resolves identity records step-by-step using an intuitive business card analogy.

---

## 📇 The Business Card Analogy

Imagine a shoebox filled with mixed-up business cards collected across different company touchpoints:

```
[ Card A: In-Store POS ]
Name:  "John Smith"
Email: john.smith@gmail.com
Phone: (not provided)

[ Card B: Web Checkout ]
Name:  "J. Smith"
Email: john.smith@gmail.com
Phone: (555) 019-2834

[ Card C: Mobile App ]
Name:  "Johnny S."
Email: (not provided)
Phone: 555-019-2834
Device: iOS-UUID-88912
```

At first glance, a naive database query cannot link **Card A** to **Card C** because they share no attributes in common (Card A only has an email, Card C only has a phone and device ID).

Here is how SQL Identity Resolution connects them deterministically in standard SQL:

---

## Step 1: PII Cleansing & Normalization

Raw data is notoriously inconsistent—casing differs, phones have dashes or parentheses, and whitespace creeps in.

The engine normalizes all extracted identifiers into uniform tokens:
* `john.smith@gmail.com` -> `john.smith@gmail.com` (lowercased)
* `(555) 019-2834` -> `5550192834` (digits only)
* `555-019-2834` -> `5550192834` (digits only)

---

## Step 2: Direct Match Linking (Card A ↔ Card B)

Card A and Card B both possess `john.smith@gmail.com`.
The engine creates a link (graph edge) between Card A and Card B.

```
[ Card A ] <====== EMAIL: john.smith@gmail.com ======> [ Card B ]
```

---

## Step 3: Transitive Chaining (Card B ↔ Card C)

Card B also contains phone `5550192834`.
Card C also contains phone `5550192834`.
The engine creates a link between Card B and Card C.

```
[ Card A ] <==== EMAIL ====> [ Card B ] <==== PHONE ====> [ Card C ]
```

---

## Step 4: Connected Components via Label Propagation

Now we have a chain:
$$\text{Card A} \longleftrightarrow \text{Card B} \longleftrightarrow \text{Card C}$$

Through **transitive closure**, because A is linked to B, and B is linked to C, **A, B, and C all belong to the same person**.

The engine assigns all three records the same canonical cluster identifier:
$$\text{resolved\_id} = \text{"RES-882014"}$$

---

## Step 5: Constructing the Golden Customer Record

With the cluster established, survivorship rules merge the best pieces of information from each card into one master profile:

```json
{
  "resolved_id": "RES-882014",
  "golden_name": "John Smith",
  "primary_email": "john.smith@gmail.com",
  "primary_phone": "5550192834",
  "known_devices": ["iOS-UUID-88912"],
  "sources_linked": ["in_store_pos", "web_checkout", "mobile_app"],
  "record_count": 3,
  "confidence_score": 0.98
}
```

---

## ⚡ What Makes This Engine Different?

| Traditional CDP / SaaS | SQL Identity Resolution (`sql-identity-oss`) |
| :--- | :--- |
| **Data Leaves Warehouse**: PII is copied to third-party SaaS cloud. | **Zero Data Egress**: 100% executes inside DuckDB, Snowflake, BigQuery, or Databricks. |
| **Black-Box Scoring**: Opaque machine learning algorithms merge records unexpectedly. | **100% Deterministic & Auditable**: Every edge is traced to a rule and exact identifier match. |
| **Expensive SaaS Pricing**: Pay-per-profile subscriptions that scale exponentially. | **Open-Source & Cloud-Native**: Compute scales linearly with warehouse credits. |
| **Separate Graph Infrastructure**: Requires hosting and maintaining Neo4j or Amazon Neptune. | **Pure SQL Execution**: Runs directly using warehouse SQL and relational tables. |
