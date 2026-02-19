# Production Deployment: DuckDB

While DuckDB is primarily embedded, it can be used in "production" for single-node analytical workloads, data apps (Streamlit/Rill), or generating extracts for other systems.

---

## Prerequisites

- **Python Environment**: Python 3.9+.
- **Persistent Storage**: A location for the `.duckdb` file (e.g., EBS volume, local disk).

---

## Step 1: Schema Setup

### 1.1 Initialize Database
Run the IDR CLI to create the tables in your production database file.

```bash
# Initialize schema and load config
idr config apply \
  --platform duckdb \
  --db /path/to/prod.duckdb \
  --file production.yaml
```

---

## Step 2: Configuration

Create a `production.yaml` config file.

```yaml
rules:
  - id: email_exact
    type: EXACT
    match_keys: [EMAIL]
    priority: 1
    canonicalize: LOWERCASE

sources:
  - id: local_csv
    table: "read_csv_auto('data/*.csv')"
    entity_key: user_id
    identifiers:
      - type: EMAIL
        expr: email
```

---

## Step 3: Metadata Loading

To update metadata, simply run `idr config apply` again.

---

## Step 4: Execution & Scheduling

Run the `idr` CLI pointing to your production database file.

### Cron Job

```bash
# Run daily at 3 AM
0 3 * * * idr run --platform=duckdb --db=/path/to/prod.duckdb --mode=FULL >> /var/log/idr.log 2>&1
```

### Running as a Library (Advanced)
(Legacy/Alternative Method)

You can also import the runner to embed identity resolution directly in your FastAPI or Flask app:

```python
from idr_core.adapters.duckdb import DuckDBAdapter
from idr_core.runner import IdentityRunner
import duckdb

conn = duckdb.connect("my_db.duckdb")
adapter = DuckDBAdapter(conn)
runner = IdentityRunner(adapter)

runner.run(mode="FULL")
```

---

## Step 5: Consuming Results

You can query the results directly using the DuckDB CLI or Python client.

```bash
duckdb prod.duckdb "SELECT * FROM idr_out.golden_profile_current LIMIT 5"
```
