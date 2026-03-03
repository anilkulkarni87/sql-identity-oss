# Phase A RAID Log

Track risks, assumptions, issues, and dependencies for enterprise GA.

## Risks

| ID | Risk | Owner | Likelihood | Impact | Mitigation | Status |
|---|---|---|---|---|---|---|
| R-01 | Async orchestration slips due to hidden runner coupling | BE | Medium | High | Spike in week 1, isolate runner contracts early | Open |
| R-02 | Authorization retrofits cause route regressions | SE | Medium | High | Add deny-by-default tests and staged rollout | Open |
| R-03 | Scale claims not reproducible across target platforms | PE | Medium | High | Lock benchmark datasets/configs and publish methodology | Open |
| R-04 | Design-partner timelines shift UAT window | CE | Medium | Medium | Keep backup partner list and parallel onboarding | Open |

## Assumptions

| ID | Assumption | Owner | Validation Plan | Status |
|---|---|---|---|---|
| A-01 | Team has capacity for BE/SE/PE/QA workstreams in parallel | AK | Confirm staffing by March 4, 2026 | Open |
| A-02 | CI minutes and infra budget can support new benchmark jobs | PE | Estimate and approve by March 6, 2026 | Open |
| A-03 | Two design partners available for May 2026 UAT | CE | Confirm by March 20, 2026 | Open |

## Issues

| ID | Issue | Owner | Next Action | Target Date | Status |
|---|---|---|---|---|---|
| I-01 | Compose availability differs across developer environments | PE | Standardize compose detection and local setup docs | March 3, 2026 | Open |
| I-02 | Packaging metadata compatibility can fail CI depending on toolchain | QA | Pin/check build backend in release lock and smoke test | March 3, 2026 | Mitigated |

## Dependencies

| ID | Dependency | Owner | Needed By | Status |
|---|---|---|---|---|
| D-01 | Access to enterprise test environments (Snowflake/Databricks/BigQuery) | PE | April 6, 2026 | Open |
| D-02 | Security review for token model and audit schema | SE | March 20, 2026 | Open |
| D-03 | Design-partner legal + data access agreements | CE | April 24, 2026 | Open |
