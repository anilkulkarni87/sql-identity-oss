---
tags:
  - faq
  - questions
  - troubleshooting
  - help
---

# Frequently Asked Questions

Quick answers to common questions about SQL Identity Resolution.

---

## General Questions

### What is identity resolution?

Identity resolution is the process of matching and linking records across multiple data sources that refer to the same real-world entity (customer, user, device, etc.). It creates a unified view from fragmented data.

### What's the difference between deterministic and probabilistic matching?

| Approach | How It Works | Pros | Cons |
|----------|--------------|------|------|
| **Deterministic** | Exact match on identifiers (email, phone, ID) | Explainable, fast, low false positives | Misses fuzzy matches |
| **Probabilistic** | Similarity scoring + ML | Catches more matches | Black box, more false positives |

SQL Identity Resolution uses a **hybrid approach**:
1.  **Deterministic** (Label Propagation) for high-confidence links.
2.  **Probabilistic** (Fuzzy Matching) for nuanced matches (names, addresses) with strict score thresholds.

### Can I use this with my existing CDP?

Yes! SQL Identity Resolution creates identity clusters that can be exported to any system:

- **As a replacement**: Use it instead of your CDP's built-in resolution
- **As a supplement**: Run it alongside and compare results
- **As an upstream source**: Feed resolved IDs back to your CDP

### Does this handle GDPR/CCPA compliance?

Your data **never leaves your data warehouse**. This is a key advantage over SaaS solutions. You maintain full control over:

- Data residency
- Access logging
- Right to deletion
- Consent management

---

## Technical Questions

### How does label propagation work?

Label propagation is a graph algorithm that assigns each node (entity) to a cluster:

1. Each entity starts with its own label (entity_key)
2. Each iteration, entities adopt the minimum label of their neighbors
3. Repeat until no labels change
4. All entities with the same label form a cluster

Typically converges in 5-15 iterations.

### What happens when two systems have the same email?

They get linked! That's the point. If `CRM:customer_123` and `Ecommerce:order_456` both have `john@acme.com`, they'll be in the same cluster with the same `resolved_id`.

### How do I prevent over-matching?

Use **identifier exclusions** for common/shared values:

```sql
INSERT INTO idr_meta.identifier_exclusion VALUES
  ('EMAIL', 'noreply@%', TRUE, 'Generic noreply address'),
  ('PHONE', '0000000000', FALSE, 'Invalid phone');
```

And set **max_group_size** limits:

```sql
INSERT INTO idr_meta.rule VALUES
  ('email_exact', 'EMAIL', 1, TRUE, 10000);  -- Skip if >10K matches
```

### Can I run this incrementally?

Yes! Set `run_mode='INCR'` to only process records with `watermark_column > last_watermark_value`. This is the recommended production mode.

---

## Performance Questions

### How long does it take?

| Records | Platform | Time |
|---------|----------|------|
| 100K | DuckDB | ~5 seconds |
| 1M | DuckDB | ~30 seconds |
| 10M | Snowflake (XS) | ~3 minutes |
| 100M | Snowflake (M) | ~15 minutes |

See [Benchmark Results](../performance/benchmark-results.md) for detailed metrics.

### What's the memory requirement?

- **DuckDB**: ~2GB RAM per 1M records
- **Cloud platforms**: Handled by the warehouse (scale compute as needed)

### Can I parallelize the run?

The algorithm runs as a single job, but:

- Internal SQL operations use parallel execution
- You can partition large datasets by entity_type
- Incremental mode reduces per-run volume

### Why is fuzzy matching slow on BigQuery?

BigQuery does not have a native Jaro-Winkler function. The JavaScript UDF has significant overhead (~1-10ms per call), causing O(n²) explosion on large datasets.

**Solution**: Use **Normalized Levenshtein** with native `EDIT_DISTANCE`:

```sql
-- Use this score_expr in fuzzy_rule for BigQuery:
score_expr = '1 - SAFE_DIVIDE(CAST(EDIT_DISTANCE(<a>, <b>) AS FLOAT64), CAST(GREATEST(LENGTH(<a>), LENGTH(<b>), 1) AS FLOAT64))'
threshold = 0.8  -- Lower than Jaro-Winkler since Levenshtein is stricter
```

This runs orders of magnitude faster as it uses native BigQuery functions.

---

## Integration Questions

### How do I schedule runs?

| Platform | Scheduler |
|----------|-----------|
| DuckDB | Cron, Airflow, dbt Cloud |
| Snowflake | Tasks + Streams |
| BigQuery | Cloud Scheduler |
| Databricks | Jobs / Workflows |

See [Scheduling Guide](../deployment/scheduling.md).

### Can I use this with dbt?

Yes! The `dbt_idr` package provides native dbt integration:

```bash
dbt deps  # Install package
dbt seed  # Load configuration
dbt run --select dbt_idr  # Run IDR
```

See the `dbt-idr` package documentation for more details.

### How do I export results to downstream systems?

Query the output tables directly:

```sql
-- Export to downstream
SELECT
  m.entity_key,
  m.resolved_id,
  g.email_primary,
  g.phone_primary
FROM idr_out.identity_resolved_membership_current m
JOIN idr_out.golden_profile_current g ON g.resolved_id = m.resolved_id
```

---

## Still have questions?

- [Open a GitHub Issue](https://github.com/anilkulkarni87/sql-identity-resolution/issues)
- [Check the Troubleshooting Guide](../guides/troubleshooting.md)
