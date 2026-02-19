# Build from repo root: docker build -f idr_api/Dockerfile .
FROM python:3.11-slim

WORKDIR /app
ARG IDR_PIP_EXTRAS=api,duckdb

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install the package with API + DuckDB support
COPY pyproject.toml ./
COPY Readme.md Readme.md
COPY idr_core/ idr_core/
COPY idr_api/ idr_api/
COPY idr_enterprise/ idr_enterprise/

RUN pip install --no-cache-dir ".[${IDR_PIP_EXTRAS}]"

# Create data directory
RUN mkdir -p /data

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

CMD ["uvicorn", "idr_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
