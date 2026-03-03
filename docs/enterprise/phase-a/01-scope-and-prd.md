# Phase A Scope and PRD

## Document Control

- Version: 0.1
- Phase: A (Scope Freeze)
- Start Date: March 2, 2026
- Target Signoff Date: March 6, 2026
- Owner: AK

## Product Thesis

Build an enterprise add-on for SQL Identity Resolution that keeps the OSS engine intact while adding governance, reliability, and support capabilities required by enterprise buyers.

## Problem Statement

Current OSS capabilities are strong for adoption and technical validation, but enterprise procurement requires additional guarantees:
- Clear authorization boundaries and auditability.
- Predictable runtime behavior under failure.
- Supportable deployment and upgrade model.
- Evidence-backed performance and reliability claims.

## Goals (Phase A)

- Freeze GA scope for the first enterprise release.
- Set launch gates with measurable acceptance criteria.
- Lock ownership, sequencing, and dependency model.

## Non-Goals (for Enterprise v1)

- Fully autonomous AI-driven matching decisions.
- New matching algorithm families beyond current deterministic + existing fuzzy modes.
- Full multi-region active-active control plane.
- Custom UI theming marketplace.

## In-Scope Features for Enterprise v1

1. Async run orchestration with durable job state.
2. Route-level authorization + scoped service tokens.
3. Immutable audit trail for security-relevant actions.
4. SLO-backed scale certification and resilience tests.
5. Enterprise runbooks (backup/restore, rollback, incident handling).
6. Design-partner UAT and launch package.

## Out-of-Scope Features for Enterprise v1

1. Fine-grained billing metering platform.
2. Policy-as-code external engine integration.
3. Cross-region data replication orchestration.
4. Marketplace app ecosystem.

## Personas and Success Metrics

- Data Platform Lead: run reliability >= 99.5% successful runs on certified workloads.
- Security Architect: 100% protected route authorization coverage in tests.
- Analytics Engineering Team: P95 run monitoring visibility with actionable failure states.
- Procurement/Sponsor: design-partner signoff + support SLAs defined.

## Release Gates

A gate passes only with linked evidence:
- G1 Security: authz matrix tests + audit log validation report.
- G2 Reliability: queue/retry/cancel and failure recovery tests.
- G3 Scale: published benchmark report for supported profiles.
- G4 Operability: backup/restore and rollback drills completed.
- G5 Customer Readiness: two UAT signoffs and GA docs package.

## Dependencies

- Stable CI execution for integration jobs.
- Dedicated owner capacity for BE, SE, PE, QA, CE.
- Access to design-partner environments for UAT.

## Signoff Table

| Function | Owner | Status |
|---|---|---|
| Product/Program | AK | Pending |
| Engineering | BE | Pending |
| Security | SE | Pending |
| Platform/SRE | PE | Pending |
| QA/Release | QA | Pending |
| Customer Engineering | CE | Pending |
