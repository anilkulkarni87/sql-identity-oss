# Overview

SQL Identity Resolution (IDR) is a warehouse-native identity graph engine. It links records across sources using deterministic rules, and optionally refines clusters using fuzzy matching.

Core ideas:
- Deterministic matching builds an identity graph using exact identifiers (email, phone, loyalty_id, etc.).
- Label propagation resolves connected components into clusters.
- Optional fuzzy rules merge clusters using blocking + scoring.
- Output tables provide resolved membership, cluster metrics, and golden profiles.

Primary interfaces:
- CLI: `idr` (recommended for production)
- UI: browser-based setup wizard and explorer
- API: FastAPI backend used by the UI
- MCP server: agent access to results
- dbt package: deterministic-only SQL implementation

Typical flow:
1. Initialize schemas and tables.
2. Apply a YAML configuration (sources, rules, survivorship).
3. Run the pipeline (FULL or INCR).
4. Explore results via SQL, UI, API, or MCP.
