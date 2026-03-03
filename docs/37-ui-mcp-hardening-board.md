# UI + MCP Hardening Execution Board

This board turns the identified `idr_ui` and `idr_mcp` gaps into a concrete execution plan with epics, ticket-level tasks, owners, dependencies, and acceptance criteria.

Planning assumption: kickoff on **March 2, 2026** with completion target by **March 27, 2026**.

## Definition of Done

All are true:
- MCP tools return correct data against current IDR schema (`golden_profile_current`, run history, evidence paths).
- MCP secret handling supports `*_FILE` patterns and avoids plaintext-only runtime coupling.
- UI is authorization-aware (not only authentication-aware) with clear deny states and session-expiry handling.
- UI has automated test coverage (unit + browser smoke) enforced in CI.
- Release pipeline verifies UI/MCP install paths and enterprise deployment docs reflect the supported golden paths.

## Owner Map

| Owner ID | Owner | Role | Primary Scope |
|---|---|---|---|
| AK | Product/Program | Prioritization + release decisions | Scope sequencing and go/no-go |
| BE | Backend Lead | API + MCP engineering | MCP contracts, adapter behavior |
| SE | Security Lead | AppSec/IAM | Secret handling, authz UX controls |
| UX | UI Lead | Frontend UX engineering | Permission-aware UI and error states |
| QA | QA/Release Lead | Automation + release confidence | UI/MCP test gates |
| PE | Platform/SRE Lead | CI/CD + deployment | workflow gates, package validation |

## Epic Timeline

| Epic | Name | Owner | Window | Exit Gate |
|---|---|---|---|---|
| EPIC-07 | MCP Contract + Security Hardening | BE | Mar 2 - Mar 7 | MCP tools pass schema/PII contract tests |
| EPIC-08 | UI Authorization + Reliability Hardening | UX | Mar 4 - Mar 14 | UI enforces permission model with clear 401/403 UX |
| EPIC-09 | UI/MCP Test Harness + CI Gates | QA | Mar 10 - Mar 21 | Unit + browser smoke + MCP contract tests run in CI |
| EPIC-10 | Adoption Docs + Launch Verification | AK | Mar 17 - Mar 27 | Golden paths and support runbooks validated by clean-host checks |

## Ticket Board

| Ticket | Epic | Title | Owner | Estimate | Depends On | Acceptance Test |
|---|---|---|---|---|---|---|
| E07-T01 | EPIC-07 | Align MCP golden-profile table contract to `idr_out.golden_profile_current` | BE | 1d | None | `pytest tests/test_mcp_contract.py::test_get_golden_profile_contract -q` passes against DuckDB fixture. |
| E07-T02 | EPIC-07 | Integrate `idr_core.secrets` in MCP env connection path (`*_FILE` support) | SE | 1d | E07-T01 | `pytest tests/test_mcp_secrets.py -q` verifies Snowflake/Databricks token/password resolution precedence. |
| E07-T03 | EPIC-07 | Standardize MCP error envelopes (deterministic error codes + safe messages) | BE | 2d | E07-T01 | Tool failures return expected envelope and no raw stack traces in outputs. |
| E07-T04 | EPIC-07 | Add MCP contract tests for `search_identifier`, `get_cluster`, `run_history`, `config_snapshot` | QA | 3d | E07-T01 | `pytest tests/test_mcp_contract.py -q` covers happy + error + masking paths. |
| E07-T05 | EPIC-07 | Add MCP CI gate to test workflow | PE | 1d | E07-T04 | New workflow job fails on MCP regression and publishes test report artifact. |
| E08-T01 | EPIC-08 | Add frontend permission model from `/api/auth/whoami` | UX | 2d | None | UI auth context exposes resolved permission set and role claims. |
| E08-T02 | EPIC-08 | Enforce permission-aware nav/routes/actions with explicit access-denied states | UX | 3d | E08-T01 | Unauthorized actions no longer appear or are disabled with clear explanation. |
| E08-T03 | EPIC-08 | Add global 401/403/session-expiry handling in API client + route layer | UX | 2d | E08-T01 | Expired token path redirects/re-auths; 403 renders actionable denied state. |
| E08-T04 | EPIC-08 | Remove token logging and tighten frontend auth/security defaults | SE | 1d | E08-T03 | No token values written to browser console/network debug logs by app code. |
| E08-T05 | EPIC-08 | Replace `any` in Setup flow with strict TypeScript interfaces | UX | 3d | E08-T01 | `npm run build` passes with tightened types and no new `any` in setup components. |
| E08-T06 | EPIC-08 | Add app-level error boundary + recoverability UX | UX | 2d | E08-T03 | Frontend runtime error shows fallback with retry/navigation options. |
| E09-T01 | EPIC-09 | Add Vitest + React Testing Library harness to UI | QA | 2d | E08-T01 | `npm run test` executes and reports coverage in CI artifact. |
| E09-T02 | EPIC-09 | Add UI unit tests for auth provider, protected route, permission guards | QA | 3d | E09-T01, E08-T03 | Guard behavior validated for auth success, auth error, and denied permission states. |
| E09-T03 | EPIC-09 | Add API client tests (token header, parseError, 401/403 handling) | QA | 2d | E09-T01 | Client tests assert auth header injection and robust error mapping. |
| E09-T04 | EPIC-09 | Add Playwright smoke flow (login/setup/runs shell render) | QA | 3d | E08-T02 | Browser smoke suite runs headless in CI on PRs. |
| E09-T05 | EPIC-09 | Wire `ui-unit`, `ui-e2e`, and `mcp-contract` jobs into `test.yml` | PE | 2d | E09-T02, E09-T04, E07-T05 | PR is blocked on failing UI/MCP quality gates. |
| E10-T01 | EPIC-10 | Update docs with explicit UI/MCP golden paths and prerequisites | AK | 2d | E09-T05 | `docs/33-golden-paths.md` + related docs include verified commands and expected outcomes. |
| E10-T02 | EPIC-10 | Publish UI auth troubleshooting + MCP operator runbook | AK | 2d | E08-T03, E07-T04 | Support runbook resolves 401/403/MCP connection failures with deterministic checks. |
| E10-T03 | EPIC-10 | Execute clean-host validation and capture launch evidence pack | QA | 3d | E10-T01, E10-T02 | Evidence includes CI links, quickstart timing report, enterprise stack check, and MCP contract report. |

## Execution Order

1. EPIC-07 (`E07-T01` through `E07-T05`)
2. EPIC-08 (`E08-T01` through `E08-T06`)
3. EPIC-09 (`E09-T01` through `E09-T05`)
4. EPIC-10 (`E10-T01` through `E10-T03`)

## Reporting Artifacts

- Board (this file): `docs/37-ui-mcp-hardening-board.md`
- Importable tickets CSV: `docs/enterprise/phase-b/execution-board.csv`
- Existing launch criteria mapping: `docs/36-launch-success-criteria.md`
- E10-T03 evidence runbook: `docs/39-launch-evidence-pack.md`
