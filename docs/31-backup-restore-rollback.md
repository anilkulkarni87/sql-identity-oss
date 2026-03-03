# Backup, Restore, and Rollback Runbook

This runbook defines the E05-T02 operability procedures and drill commands.

## Scope

Critical state for enterprise deployment:
- Primary DuckDB file: `/data/idr.duckdb`
- Async run queue DB: `/data/control/idr_run_jobs.sqlite3`
- Service token DB: `/data/control/idr_service_auth.sqlite3`
- Audit log DB: `/data/control/idr_audit.sqlite3`
- Redis durability (AOF) when enabled

## Backup Procedure (Compose Baseline)

1. Quiesce writes (recommended):
   - pause job submission and admin mutations
   - wait for active runs to complete
2. Snapshot state files into a backup directory:

```bash
python tools/ci/run_backup_restore_drill.py \
  --file idr_database=/data/idr.duckdb \
  --file run_jobs=/data/control/idr_run_jobs.sqlite3 \
  --file service_auth=/data/control/idr_service_auth.sqlite3 \
  --file audit=/data/control/idr_audit.sqlite3 \
  --backup-dir ./ops_backups \
  --evidence-file ./ops_backups/latest_backup_report.json \
  --max-rto-seconds 300 \
  --max-rpo-bytes 0
```

The script writes:
- timestamped snapshot folder
- evidence JSON containing checksums, RTO, and RPO metrics

## Restore Drill Procedure

Run with incident simulation enabled:

```bash
python tools/ci/run_backup_restore_drill.py \
  --file idr_database=/data/idr.duckdb \
  --file run_jobs=/data/control/idr_run_jobs.sqlite3 \
  --file service_auth=/data/control/idr_service_auth.sqlite3 \
  --file audit=/data/control/idr_audit.sqlite3 \
  --backup-dir ./ops_backups \
  --evidence-file ./ops_backups/restore_drill_report.json \
  --simulate-incident \
  --max-rto-seconds 300 \
  --max-rpo-bytes 0
```

Pass criteria:
- restore integrity verification succeeds
- observed `RTO <= max_rto_seconds`
- observed `RPO bytes <= max_rpo_bytes`

## Rollback Procedure (Pinned Image Deployments)

Use previous known-good image tag in `.env`:

```bash
# Example: rollback to v0.5.1
export IDR_IMAGE_TAG=0.5.1
docker compose -f docker-compose.prod.yml --env-file .env up -d --force-recreate
```

Post-rollback checks:

```bash
curl -sS http://localhost:8000/api/health
curl -sS http://localhost:8000/metrics | head
```

## Rollback Dry-Run Validation

Validate the rollback rendering before execution:

```bash
bash tools/ci/validate_prod_rollback.sh 0.5.1 docker-compose.prod.yml .env
```

This verifies `idr-api` and `idr-ui` images render with the intended rollback tag.

## Evidence Requirements

Store these artifacts per drill:
- backup/restore drill report JSON
- deployment health check output
- rollback dry-run validation log

Target objectives (default):
- `RTO <= 300s`
- `RPO = 0 bytes`
