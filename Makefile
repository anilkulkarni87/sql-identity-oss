# SQL Identity Resolution Makefile
# One-liner commands for common operations

.PHONY: demo test docs clean install help lint benchmark generate

# Default target
help:
	@echo "SQL Identity Resolution - Available Commands"
	@echo "============================================="
	@echo ""
	@echo "  make demo      - Run demo with idr_core package"
	@echo "  make test      - Run all tests with pytest"
	@echo "  make lint      - Lint Python source"
	@echo "  make docs      - Serve documentation locally"
	@echo "  make install   - Install Python dependencies"
	@echo "  make clean     - Remove generated files"
	@echo "  make generate  - Generate scale test data"
	@echo ""

# Install dependencies
install:
	pip install -e ".[dev,duckdb]" 2>/dev/null || pip install duckdb pyarrow pyyaml pytest

# Run demo with idr_core package
demo: install
	@echo ""
	@echo "🔗 SQL Identity Resolution - Demo with idr_core"
	@echo "================================================"
	@echo ""
	@echo "Running quickstart demo (10K rows)..."
	@idr quickstart --rows=10000 --output=demo.duckdb --seed=42
	@echo ""
	@echo "============================================"
	@echo "✅ Demo complete!"
	@echo ""
	@echo "Results:"
	@echo "  • Database: demo.duckdb"
	@echo ""
	@echo "Query results:"
	@python -c "import duckdb; c=duckdb.connect('demo.duckdb'); print('  Clusters:', c.execute('SELECT COUNT(*) FROM idr_out.identity_clusters_current').fetchone()[0]); print('  Entities:', c.execute('SELECT COUNT(*) FROM idr_out.identity_resolved_membership_current').fetchone()[0])"
	@echo ""

# Run all tests with pytest
test:
	@echo "Running tests..."
	pip install pytest --quiet
	python -m pytest tests/ --ignore=tests/legacy -v

# Serve documentation locally
docs:
	pip install mkdocs-material mkdocs-mermaid2-plugin pymdown-extensions
	mkdocs serve

# Build documentation
docs-build:
	pip install mkdocs-material mkdocs-mermaid2-plugin pymdown-extensions
	mkdocs build

# Generate scale test data
generate:
	@echo "Generating 20M row test dataset..."
	pip install pyarrow
	python tools/scale_test/data_generator.py --rows=20000000 --seed=42 --output=data/

# Clean generated files
clean:
	rm -f demo.duckdb
	rm -f demo_results.html
	rm -rf data/
	rm -rf site/
	rm -rf tools/scale_test/test_data/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# Quick lint
lint:
	pip install flake8
	flake8 idr_core/ idr_api/ tests/ tools/ --max-line-length=120 --ignore=E501,W503

# Run benchmark
benchmark:
	@echo "Running DuckDB benchmark..."
	python tools/scale_test/benchmark.py \
		--platform=duckdb \
		--data=data/customers_all.parquet \
		--rows=20000000 \
		--db=benchmark.duckdb \
		--output=benchmarks/
