# API Reference (Summary)

The API is a FastAPI service used by the UI. It also supports direct use.

Base URL: http://localhost:8000

## Health
- `GET /api/health`

## Connection
- `POST /api/connect` (connect for dashboard/explorer)

## Setup Wizard
- `POST /api/setup/connect`
- `GET /api/setup/status`
- `GET /api/setup/config`
- `GET /api/setup/discover/tables?schema=...`
- `GET /api/setup/discover/columns?table=...`
- `POST /api/setup/config/save`
- `POST /api/setup/run`
- `GET /api/setup/fuzzy-templates`

## Metrics
- `GET /api/metrics/summary`
- `GET /api/metrics/distribution`
- `GET /api/metrics/rules`
- `GET /api/alerts`

## Explorer
- `GET /api/entities/search?q=...`
- `GET /api/clusters/{resolved_id}`

## Runs
- `GET /api/runs`

## Schema
- `GET /api/schema`

Notes:
- The API uses the current adapter from the ConnectionManager.
- Setup endpoints are intended for the wizard and can initialize metadata tables.
