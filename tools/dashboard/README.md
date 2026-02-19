# IDR Dashboard Generator

Generate static HTML dashboards from your identity resolution data.

## Installation

```bash
pip install duckdb  # For DuckDB
# or
pip install snowflake-connector-python  # For Snowflake
# or
pip install google-cloud-bigquery  # For BigQuery
```

## Usage

### DuckDB

```bash
python tools/dashboard/generator.py \
    --platform=duckdb \
    --connection=idr.duckdb \
    --output=dashboard.html
```

### Snowflake

```bash
python tools/dashboard/generator.py \
    --platform=snowflake \
    --account=your_account \
    --user=your_user \
    --password=your_password \
    --database=your_database \
    --output=dashboard.html
```

Or using environment variables:
```bash
export SNOWFLAKE_ACCOUNT=xxx
export SNOWFLAKE_USER=xxx
export SNOWFLAKE_PASSWORD=xxx

python tools/dashboard/generator.py \
    --platform=snowflake \
    --connection=placeholder \
    --output=dashboard.html
```

### BigQuery

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json

python tools/dashboard/generator.py \
    --platform=bigquery \
    --project=your-project \
    --output=dashboard.html
```

## Features

- Run history with status badges
- Cluster size distribution chart
- Top 10 largest clusters
- Recent dry run summaries
- Skipped identifier groups audit
- Beautiful dark theme

## Output

The generator creates a self-contained HTML file that can be opened in any browser.
No web server required.

```bash
open dashboard.html
```
