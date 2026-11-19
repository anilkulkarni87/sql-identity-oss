# All-in-one SQL Identity Resolution container (React UI + FastAPI + /mcp + all 4 warehouse drivers)
# Build: docker build -t sql-identity-oss .
# Run:   docker run -p 8000:8000 -v $(pwd)/data:/data sql-identity-oss

FROM node:20-alpine AS ui-builder
WORKDIR /ui
COPY idr_ui/package*.json ./
RUN npm ci
COPY idr_ui/ ./
RUN npm run build

FROM python:3.11-slim

LABEL org.opencontainers.image.title="SQL Identity Resolution"
LABEL org.opencontainers.image.description="Headless, warehouse-native identity resolution with bundled UI and /mcp"
LABEL org.opencontainers.image.source="https://github.com/anilkulkarni87/sql-identity-oss"

WORKDIR /app
ARG IDR_PIP_EXTRAS=all

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml Readme.md ./
COPY idr_core/ idr_core/
COPY idr_api/ idr_api/
COPY idr_mcp/ idr_mcp/
COPY idr_enterprise/ idr_enterprise/
COPY examples/ examples/
COPY sql/ sql/
COPY --from=ui-builder /ui/dist idr_api/static

RUN pip install --no-cache-dir ".[${IDR_PIP_EXTRAS}]" \
    && mkdir -p /data /output

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

CMD ["uvicorn", "idr_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
