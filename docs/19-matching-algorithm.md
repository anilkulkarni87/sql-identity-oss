# Matching Algorithm

The two-stage algorithm for entity resolution.

## Phase 1: Deterministic (Strict)

### 1. Extraction
Extracts identifiers (Email, Phone) from source records based on `identifier_mapping`.
*   **Normalization**: Lowercase, trim, remove non-digits.

### 2. Edge Building
Connects entities sharing the same normalized identifier.
*   **Anchor Optimization**: To avoid $O(N^2)$ connections in large groups, all members connect to a single "Anchor" (lowest `entity_key`).

### 3. Label Propagation
Iteratively propagates the Cluster ID across the graph.
*   **Convergence**: When no labels change, Connected Components are found.

## Phase 2: Fuzzy (Optional)

If `--strict=false`:
1.  **Blocking**: Groups candidates by a key (e.g., `Metaphone(Name)`).
2.  **Scoring**: Calculates similarity (e.g., Jaro-Winkler).
3.  **Threshold**: Merges clusters if Score > Threshold.
