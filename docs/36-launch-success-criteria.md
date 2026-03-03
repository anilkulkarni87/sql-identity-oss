# Launch Success Criteria

This document maps launch criteria to automated evidence.

## Criterion 1

New user can run end-to-end in under 10 minutes with one documented path.

Evidence:
- documented path: `docs/33-golden-paths.md` (Path A)
- automated gate: `quickstart-golden-path` in `.github/workflows/test.yml`
- artifact: `quickstart_ci_report.json` with elapsed seconds and output verification
- launch evidence capture: `python tools/ci/capture_launch_evidence.py` (writes timestamped evidence pack under `tmp/launch_evidence/`)

## Criterion 2

Enterprise admin can deploy with SSO + metrics using a single deployment package and minimal manual steps.

Evidence:
- Docker package path: `tools/deploy/enterprise_up.sh`
- Kubernetes package path: `deployment/helm/idr-enterprise`
- SSO wiring and bootstrap: enterprise compose + Keycloak realm import
- metrics stack wiring: Prometheus scrape + Grafana datasource/dashboard provisioning
- automated gates:
  - `enterprise-e2e`
  - `enterprise-ha-e2e`
  - `helm-package`
- launch evidence pack includes: `enterprise-stack.log` (and optional `enterprise-ha-stack.log`)

## Criterion 3

Every release produces verified, reproducible artifacts with smoke-tested install paths.

Evidence:
- lockfile-based dependency installation (`requirements/*.lock`, `npm ci`)
- package smoke install (`package-smoke`)
- release artifact build + wheel smoke run (`release.yml`)
- SBOM generation + verification (`sbom-verify`)
- vulnerability scanning (Trivy gates in test/release workflows)
- provenance attestations + cosign signatures in `release.yml`
- consolidated release evidence index: `launch_evidence_index.json` + `launch_evidence_summary.md` in `tmp/launch_evidence/<timestamp>/`
