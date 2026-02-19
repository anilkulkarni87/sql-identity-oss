# Architecture

This document describes the system architecture of SQL Identity Resolution.

---

## High-Level Architecture

```mermaid
graph TB
    subgraph Sources["Source Layer"]
        S1[CRM System]
        S2[E-commerce]
        S3[Mobile App]
        S4[Support Tickets]
    end

    subgraph Meta["Metadata Layer"]
        M1["source_table (registry)"]
        M2["rule (exact matching)"]
        M3["fuzzy_rule (probabilistic)"]
        M4["identifier_mapping"]
        M5["survivorship_rule"]
    end

    subgraph Process["Processing Layer"]
        P1["Extract Entities (delta)"]
        P2["Build Edges (Exact)"]
        P3["Label Propagation (Strict)"]
        P4["Fuzzy Matching (Optional)"]
        P5["Assign Clusters"]
    end

    subgraph Output["Output Layer"]
        O1["identity_resolved_membership_current"]
        O2["identity_clusters_current"]
        O3["golden_profile_current"]
        O4["run_history"]
    end

    Sources --> P1
    Meta --> P1
    P1 --> P2
    P2 --> P3
    P3 --> P4
    P4 --> P5
    P5 --> Output
```

---

## Cross-Platform Design

The same core logic runs on all platforms via adapters:

```mermaid
graph LR
    subgraph Core["Core Logic"]
        A[DDL Schema]
        B[Edge Building (SQL)]
        C[Label Propagation (SQL)]
        D[Fuzzy Logic (Platform Native)]
    end

    subgraph Adapters["Platform Adapters"]
        DA["DuckDB"]
        SN["Snowflake"]
        BQ["BigQuery"]
        DB["Databricks"]
    end

    Core --> DA
    Core --> SN
    Core --> BQ
    Core --> DB
```

---

## Schema Design

### idr_meta (Configuration)

| Table | Purpose |
|-------|---------|
| `source_table` | Registry of source tables to process |
| `rule` | Exact matching rules |
| `fuzzy_rule` | Probabilistic matching rules |
| `identifier_mapping` | Maps source columns to identifier types |
| `entity_attribute_mapping` | Maps attributes (e.g., name, address) |
| `survivorship_rule` | Logic for selection Golden Profile value |
| `identifier_exclusion` | Blocklist for bad data |
| `run_state` | Watermark tracking |

### idr_work (Processing)

Transient tables used during execution.

| Table | Purpose |
|-------|---------|
| `entities_delta` | Entities to process this run |
| `identifiers` | Extracted identifier values |
| `edges_new` | Entity pairs with matching identifiers |
| `lp_labels` | Label propagation state |
| `fuzzy_results` | Probabilistic match pairs |

### idr_out (Output)

| Table | Purpose |
|-------|---------|
| `identity_resolved_membership_current` | Entity → Cluster mapping |
| `identity_clusters_current` | Cluster metadata (size, confidence) |
| `golden_profile_current` | Best-record profiles per cluster |
| `run_history` | Audit log of all runs |
| `dry_run_results` | Proposed changes (Dry Run Mode) |
| `skipped_identifier_groups` | Audit of skipped supernodes |

---

## Next Steps

- [Matching Algorithm](matching-algorithm.md) - Deep dive into label propagation and fuzzy logic
- [Data Model](data-model.md) - Complete schema reference
- [Configuration](../guides/configuration.md) - Setting up rules
