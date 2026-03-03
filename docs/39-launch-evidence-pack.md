# Launch Evidence Pack (E10-T03)

This runbook executes clean-host validation and produces a timestamped launch evidence pack.

Ticket mapping:
- `E10-T03` in `docs/37-ui-mcp-hardening-board.md`

## Goal

Produce a single artifact bundle that includes:
- CI workflow links (test + release)
- quickstart timing report
- enterprise stack verification report
- MCP contract report

## Prerequisites

- Python 3.11+
- Docker + Docker Compose
- Repo checked out locally

Optional for HA check:
- resources to run `docker-compose.enterprise.ha.yml`

## One-Command Execution

From repo root:

```bash
python tools/ci/capture_launch_evidence.py \
  --output-dir tmp/launch_evidence \
  --compose-file docker-compose.enterprise.yml \
  --run-ha-check
```

## Fresh Machine Command Set (Copy/Paste)

Use this exact sequence on a different machine.

```bash
# 1) clone and enter repo
git clone https://github.com/<org>/<repo>.git
cd <repo>

# 2) verify prerequisites
python3 --version
docker --version
docker compose version

# 3) run E10-T03 evidence capture (includes quickstart + MCP + enterprise checks)
python3 tools/ci/capture_launch_evidence.py \
  --output-dir tmp/launch_evidence \
  --compose-file docker-compose.enterprise.yml \
  --run-ha-check

# 4) inspect latest evidence pack
LATEST="$(ls -1dt tmp/launch_evidence/* | head -n 1)"
echo "LATEST_EVIDENCE_DIR=$LATEST"
ls -la "$LATEST"
sed -n '1,220p' "$LATEST/launch_evidence_summary.md"
cat "$LATEST/quickstart_ci_report.json"
```

If the run fails, share:

```bash
LATEST="$(ls -1dt tmp/launch_evidence/* | head -n 1)"
sed -n '1,260p' "$LATEST/launch_evidence_summary.md"
sed -n '1,260p' "$LATEST/enterprise-stack.log"
sed -n '1,220p' "$LATEST/mcp-contract.log"
```

If you want explicit CI run URLs in the summary, pass:

```bash
python tools/ci/capture_launch_evidence.py \
  --ci-test-workflow-url "https://github.com/<org>/<repo>/actions/runs/<test_run_id>" \
  --ci-release-workflow-url "https://github.com/<org>/<repo>/actions/runs/<release_run_id>" \
  --ci-quickstart-artifact-url "https://github.com/<org>/<repo>/actions/runs/<quickstart_run_id>" \
  --ci-mcp-artifact-url "https://github.com/<org>/<repo>/actions/runs/<mcp_run_id>" \
  --ci-enterprise-artifact-url "https://github.com/<org>/<repo>/actions/runs/<enterprise_run_id>"
```

## Individual Test Commands (Manual)

These are the exact checks executed for E10-T03.

```bash
# quickstart timing report
python3 tools/ci/validate_quickstart_path.py \
  --max-seconds 600 \
  --rows 10000 \
  --output tmp/quickstart_ci.duckdb \
  --report tmp/quickstart_ci_report.json

# MCP contract + secrets + error-envelope checks
python3 -m pytest \
  tests/test_mcp_contract.py \
  tests/test_mcp_secrets.py \
  tests/test_mcp_errors.py \
  -q

# enterprise stack verification
bash tools/ci/verify_enterprise_stack.sh docker-compose.enterprise.yml

# enterprise HA verification
EXPECT_HEALTHY_SERVICES=api-a,api-b,api,redis,keycloak \
  bash tools/ci/verify_enterprise_stack.sh docker-compose.enterprise.ha.yml
```

## Output Structure

The command writes a timestamped folder under `tmp/launch_evidence/`:

- `launch_evidence_summary.md`: human-readable release evidence summary
- `launch_evidence_index.json`: machine-readable index of checks, return codes, and links
- `quickstart_ci_report.json`: quickstart timing gate result (`elapsed_seconds`, status)
- `mcp-contract.log`: MCP contract/secrets/errors test log
- `mcp-contract-junit.xml`: MCP contract JUnit report
- `enterprise-stack.log`: enterprise compose verification output
- `enterprise-ha-stack.log`: HA verification output (when `--run-ha-check`)
- `logs/`: setup and per-step logs

## Pass/Fail Rules

Required checks:
- quickstart timing gate: pass
- MCP contract suite: pass
- enterprise stack verification: pass

Additionally required when `--run-ha-check` is set:
- enterprise HA stack verification: pass

The command exits non-zero if any required check fails.

## Fast Debug Mode

Skip enterprise checks while iterating locally:

```bash
python tools/ci/capture_launch_evidence.py \
  --skip-enterprise-check \
  --skip-venv-setup \
  --python-bin .venv/bin/python \
  --quickstart-rows 1000
```

Use this only for script validation, not release evidence signoff.
