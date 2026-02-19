# Scaling Considerations

Strategies for processing 100M+ entities.

## Bottlenecks

### 1. Supernodes (Giant Groups)
A single identifier (e.g., `null`) connecting 1M records creates 1 trillion potential edges ($N^2$).
*   **Fix**: Use `identifier_exclusion` table.
*   **Fix**: Set `max_group_size` (default 50) in rules.

### 2. Deep Chains
A string of A->B->C...->Z that requires many Label Propagation iterations.
*   **Fix**: Increase `max_lp_iterations`.
*   **Fix**: Investigate data quality (shared devices/phones).

## Partitioning
For tables > 100GB:
*   Cluster `identifiers` by `(identifier_type, identifier_value_norm)`.
*   Cluster `edges` by `identifier_type`.

## Incremental Mode
Use `idr run --mode INCR` for daily updates. It only processes:
1.  New records.
2.  Records connected to new records.
This reduces volume by 95%+.
