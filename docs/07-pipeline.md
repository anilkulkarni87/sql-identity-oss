# Pipeline Deep Dive

The runner orchestrates four stages: preflight, extraction, graph, and output.

## 1. Preflight
- Checks for concurrent runs.
- Verifies source tables exist.
- Applies schema upgrades (best-effort).
- Validates identifier mappings against actual columns.

## 2. Extraction
- Builds `idr_work.entities_delta` based on watermarks (FULL or INCR).
- Extracts identifiers into `idr_work.identifiers_all`.
- Extracts attributes into `idr_work.entity_attributes` for fuzzy and survivorship.
- Applies exclusions from `idr_meta.identifier_exclusion` if present.

## 3. Graph
- Builds edges with an anchor-based N-1 approach (avoids O(N^2)).
- Skips identifier groups larger than `max_group_size`.
- Label propagation resolves connected components into clusters.
- Optional fuzzy matching builds super-clusters (skipped in `--strict`).

## 4. Output
- Upserts membership, edges, and clusters.
- Computes confidence scores (edge diversity + match density).
- Updates watermarks in `idr_meta.run_state`.
- Builds golden profiles via `ProfileBuilder`.

## Dry run mode
- Runs all stages but writes to `idr_out.dry_run_results` and `idr_out.dry_run_summary`.
- Does not modify production tables.
