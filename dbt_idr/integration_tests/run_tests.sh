#!/bin/bash
# Run dbt_idr integration tests with DuckDB
# Prerequisites: pip install dbt-duckdb

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "dbt_idr Integration Tests (DuckDB)"
echo "=========================================="

# Clean previous run
rm -f test_idr.duckdb

# Install dependencies
echo ""
echo "📦 Installing dependencies..."
dbt deps --profiles-dir .

# Load seeds (sample data + IDR config)
echo ""
echo "🌱 Loading seed data..."
dbt seed --profiles-dir .

# Run all models
echo ""
echo "🚀 Running IDR models..."
dbt run --profiles-dir .

# Run tests
echo ""
echo "🧪 Running tests..."
dbt test --profiles-dir .

echo ""
echo "=========================================="
echo "✅ All tests passed!"
echo "=========================================="

# Print summary
echo ""
echo "📊 Results Summary:"
dbt run --profiles-dir . --select stage_metrics --quiet 2>/dev/null || true

echo ""
echo "You can explore the results with:"
echo "  duckdb test_idr.duckdb"
echo "  > SELECT * FROM main_idr_out.identity_clusters;"
