# Scaling Strategies & High-Volume Operations

Scaling identity resolution to tens or hundreds of millions of entities introduces unique graph and database challenges. This guide outlines battle-tested architectural strategies to process high-volume workloads reliably and cost-effectively.

---

## ⚡ 1. Tackling Graph Bottlenecks

### Problem A: The Supernode Phenomenon
A "supernode" occurs when an identifier is shared across a massive number of records. Common real-world examples:

* Placeholder emails: `test@test.com`, `noemail@store.com`, `guest@checkout.com`
* Default phone numbers: `000-000-0000`, `123-456-7890`, company switchboards
* Null or empty strings that bypassed ingestion validation

If an identifier is shared by 100,000 records, building an unconstrained clique creates:
$$\frac{100,000 \times 99,999}{2} \approx 5 \text{ Billion Edges}$$

This will exhaust warehouse memory, cause query spills, and merge unrelated customers into a single gargantuan cluster.

#### Solutions in `sql-identity-oss`:
1. **`max_group_size` Thresholding**: Set per-rule in `idr_meta.identifier_mapping` (default: 50). Groups exceeding this size are safely bypassed from edge formation and logged to `idr_out.skipped_identifier_groups`.
2. **Exclusion Registry**: Maintain exact and domain wildcard exclusions in `idr_meta.identifier_exclusion`.
3. **Anchor Topology**: Even within allowed groups ($N \le 50$), only $N - 1$ edges are formed instead of $N(N - 1) / 2$.

---

### Problem B: Deep Linear Chains & Convergence Latency
When individuals share intermediate devices or addresses across extended time periods, long chains can emerge ($A \leftrightarrow B \leftrightarrow C \leftrightarrow \dots \leftrightarrow Z$).
In naive label propagation, propagating the minimum label through a chain of diameter $D$ requires $D$ iterative SQL steps.

#### Solutions:
1. **Tune `max_lp_iterations`**: The default is 30 iterations. Most enterprise datasets converge in 4 to 8 iterations. If iteration counts consistently approach 30, investigate whether false-positive linkages (e.g. shared family phones or retail store devices) are creating artificial bridges.
2. **Bi-directional Edge Doubling**: Edges are evaluated symmetrically (`src -> dst` and `dst -> src`), ensuring labels propagate in both directions concurrently and halving the required steps to span a chain.

---

## 🔄 2. Incremental Mode (`--mode INCR`)

Running full identity resolution (`FULL` mode) across 100M records daily is compute-inefficient. Instead, use **Incremental Resolution**:

```bash
idr run --platform snowflake --mode INCR
```

### How Incremental Mode Works:
1. **Delta Entity Identification**: Inspects source tables using high-watermark timestamps (`updated_at > last_successful_run_at`).
2. **Impacted Subgraph Discovery**: Identifies all existing entities and clusters that share identifiers with the incoming delta entities.
3. **Localized Label Propagation**: Re-evaluates label propagation **only within the impacted connected components**.
4. **Target Table Merge**: Uses atomic warehouse `MERGE INTO` statements to update `identity_clusters_current` and `identity_resolved_membership_current` without touching unaffected clusters.

**Impact**: In typical production retail environments where 2-5% of profiles change daily, incremental mode reduces processed edge volume by **90-95%**, cutting runtime and warehouse credits by up to **80%**.

---

## 🗄️ 3. Physical Storage & Clustering Optimization

When tables in `idr_work` and `idr_out` exceed 100GB, optimize warehouse physical layout:

### Snowflake
```sql
ALTER TABLE idr_out.identity_resolved_membership_current
CLUSTER BY (resolved_id, entity_key);

ALTER TABLE idr_out.identity_clusters_current
CLUSTER BY (resolved_id);
```

### BigQuery
```sql
-- Partition by extraction date and cluster by hash
CREATE OR REPLACE TABLE idr_work.identifiers_work
PARTITION BY DATE(extracted_at)
CLUSTER BY identifier_type, identifier_hash;
```

### Databricks
```sql
OPTIMIZE idr_out.identity_resolved_membership_current
ZORDER BY (resolved_id, entity_key);
```
