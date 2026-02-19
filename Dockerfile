FROM python:3.11-slim

LABEL org.opencontainers.image.title="SQL Identity Resolution"
LABEL org.opencontainers.image.description="Production-grade deterministic identity resolution"
LABEL org.opencontainers.image.source="https://github.com/anilkulkarni87/sql-identity-resolution"

WORKDIR /app

# Install the package with DuckDB support
COPY pyproject.toml README.md ./
COPY idr_core/ idr_core/
COPY idr_api/ idr_api/
COPY examples/ examples/
COPY sql/ sql/

RUN pip install --no-cache-dir ".[duckdb]"

# Create output directory
RUN mkdir -p /output

# Default command: run demo via supported CLI
CMD ["idr", "quickstart", "--rows=10000", "--output=/output/demo.duckdb", "--seed=42"]
