# Troubleshooting

## Common Issues

### 1. "Table not found" errors
**Cause**: Metadata tables not initialized.
**Fix**: Run `idr init` for your platform.

### 2. "Giant Cluster" warnings
**Cause**: A common value (e.g., "null", "n/a") is linking thousands of entities.
**Fix**: Add the value to `idr_meta.identifier_exclusion`.
```sql
INSERT INTO idr_meta.identifier_exclusion (identifier_type, identifier_value_pattern, match_type, reason)
VALUES ('email', 'no-reply@%', 'LIKE', 'Generic email');
```

### 3. Pipeline failing at "Label Propagation"
**Cause**: Graph explosion due to high connectivity.
**Fix**:
*   Check `skipped_identifier_groups` table for clues.
*   Lower `max_group_size` in `idr_meta.rule`.
*   Increase `max_lp_iterations` if it's just slow to converge.

## Debugging

### Enable Verbose Logging
Set `LOG_LEVEL=DEBUG` in your environment.

### Check Run History
```sql
SELECT * FROM idr_out.run_history ORDER BY started_at DESC LIMIT 1;
```

### Dry Run
Use `--dry-run` to see what changes *would* happen without committing them.
```bash
idr run --platform bigquery --mode INCR --dry-run
```
