# Quickstart (DuckDB)

Run a full demo locally in minutes.

```bash
pip install "sql-identity-resolution[duckdb]"
idr quickstart
```

Optional flags:
```bash
idr quickstart --rows=50000 --output=demo.duckdb --seed=42
```

What it does:
- Generates synthetic retail data
- Initializes schemas and metadata
- Runs the identity pipeline
- Prints a run summary

Next steps:
- Inspect outputs with SQL (see 08-schema.md)
- Apply your own config (see 04-configuration.md)
