# Phase A Launch Checklist

Use this checklist to drive weekly go/no-go tracking from March 2, 2026.

## Scope and Ownership

- [ ] PRD approved and non-goals locked.
- [ ] Epics and ticket owners assigned with backups.
- [ ] Cross-team dependency map reviewed.

## Security and Governance

- [ ] Route-level authorization tests cover all protected endpoints.
- [ ] Service account token scopes validated.
- [ ] Immutable audit events generated for config, run, and admin actions.
- [ ] Security controls mapped to enterprise questionnaire baseline.

## Runtime Reliability

- [ ] Async run API returns job IDs and never blocks request lifecycle.
- [ ] Worker retry policies implemented and tested.
- [ ] Run cancellation is safe and deterministic.
- [ ] Failure recovery tests (worker kill/DB transient/timeout) pass.

## Scale and Performance

- [ ] Benchmark harness runs on certified profiles.
- [ ] SLO thresholds documented and enforced in CI release gates.
- [ ] Benchmark report includes throughput, latency, and failure rates.

## Operability

- [ ] HA deployment guide validated from clean environment.
- [ ] Backup and restore drill completed with evidence.
- [ ] Upgrade and rollback runbook validated.
- [ ] Runtime secrets follow `*_FILE`/vault-injection pattern with rotation drill evidence.
- [ ] Alerting and on-call escalation documented.

## Customer Launch Readiness

- [ ] Two design partners onboarded to UAT plan.
- [ ] UAT scripts executed and signed off.
- [ ] GA release notes, support SLAs, and migration guide published.
- [ ] Final launch review completed with explicit go/no-go decision.

## Evidence Links

Add links to CI runs, dashboards, docs, and UAT records here before launch.
