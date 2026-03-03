#!/usr/bin/env bash
#
# Final validation + commit flow for enterprise readiness completion.
# Run from repo root:
#   bash tools/ci/final_validation_commands.sh
# Print E10-T03 command pack:
#   bash tools/ci/final_validation_commands.sh --print-e10-t03
#
set -euo pipefail

if [ "${1:-}" = "--print-e10-t03" ]; then
  cat <<'EOF'
# E10-T03 Launch Evidence Pack (fresh machine)

# 1) clone and enter repo
git clone https://github.com/<org>/<repo>.git
cd <repo>

# 2) verify prerequisites
python3 --version
docker --version
docker compose version

# 3) run E10-T03 evidence capture (quickstart + MCP + enterprise + HA checks)
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

# 5) if run fails, collect logs
LATEST="$(ls -1dt tmp/launch_evidence/* | head -n 1)"
sed -n '1,260p' "$LATEST/launch_evidence_summary.md"
sed -n '1,260p' "$LATEST/enterprise-stack.log"
sed -n '1,220p' "$LATEST/mcp-contract.log"

# Optional: run checks individually
python3 tools/ci/validate_quickstart_path.py \
  --max-seconds 600 \
  --rows 10000 \
  --output tmp/quickstart_ci.duckdb \
  --report tmp/quickstart_ci_report.json

python3 -m pytest \
  tests/test_mcp_contract.py \
  tests/test_mcp_secrets.py \
  tests/test_mcp_errors.py \
  -q

bash tools/ci/verify_enterprise_stack.sh docker-compose.enterprise.yml

EXPECT_HEALTHY_SERVICES=api-a,api-b,api,redis,keycloak \
  bash tools/ci/verify_enterprise_stack.sh docker-compose.enterprise.ha.yml
EOF
  exit 0
fi

cd /Users/harihiom/Anil/hobby_projects/sql-identity-resolution-main

export LOG_FILE="/tmp/idr_final_validation_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1
echo "LOG_FILE=$LOG_FILE"

echo "== Step 1: branch =="
git checkout -b "codex/phase34-finalize-$(date +%Y%m%d-%H%M%S)"

echo "== Step 2: prerequisites =="
python3 --version
docker compose version
if ! command -v helm >/dev/null 2>&1; then
  echo "Helm not found. Install it (macOS: brew install helm) and re-run."
  exit 1
fi
helm version

echo "== Step 3: python env =="
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements/ci.lock
python -m pip install -r requirements/release.lock

echo "== Step 4: local validation =="
PYTHONPATH=. ruff check idr_core idr_api tests tools
PYTHONPATH=. pytest tests/test_api.py tests/test_auth.py tests/test_job_manager_resilience.py tests/test_secrets.py tests/test_cli_smoke.py tests/test_doctor.py tests/test_quickstart_path_gate.py -q
PYTHONPATH=. python tools/ci/validate_secrets_posture.py
PYTHONPATH=. python tools/ci/validate_quickstart_path.py \
  --max-seconds 600 \
  --rows 10000 \
  --output tmp/quickstart_ci_local.duckdb \
  --report tmp/quickstart_ci_local_report.json
python -m build
twine check dist/*
python -m venv .venv-smoke
source .venv-smoke/bin/activate
python -m pip install dist/*.whl
idr version
idr doctor --json
deactivate
source .venv/bin/activate

echo "== Step 5: UI build =="
pushd idr_ui >/dev/null
npm ci --ignore-scripts --no-audit
npm run build
popd >/dev/null

echo "== Step 6: enterprise compose validation =="
# Ensure all compose validation scripts share one consistent runtime secret set.
if [ -z "${IDR_KEYCLOAK_ADMIN_PASSWORD:-}" ]; then
  export IDR_KEYCLOAK_ADMIN_PASSWORD="$(
    python - <<'PY'
import secrets
print(secrets.token_urlsafe(32))
PY
  )"
  echo "Set IDR_KEYCLOAK_ADMIN_PASSWORD for validation run."
fi

if [ -z "${IDR_GRAFANA_ADMIN_PASSWORD:-}" ]; then
  export IDR_GRAFANA_ADMIN_PASSWORD="$(
    python - <<'PY'
import secrets
print(secrets.token_urlsafe(32))
PY
  )"
  echo "Set IDR_GRAFANA_ADMIN_PASSWORD for validation run."
fi

bash tools/deploy/enterprise_up.sh docker-compose.enterprise.yml
bash tools/ci/verify_enterprise_stack.sh docker-compose.enterprise.yml
EXPECT_HEALTHY_SERVICES=api-a,api-b,api,redis,keycloak \
  bash tools/ci/verify_enterprise_stack.sh docker-compose.enterprise.ha.yml

echo "== Step 7: helm validation =="
helm lint deployment/helm/idr-enterprise
helm template idr-enterprise deployment/helm/idr-enterprise \
  -f deployment/helm/idr-enterprise/values.example.yaml \
  > /tmp/idr-enterprise-rendered.yaml
test -s /tmp/idr-enterprise-rendered.yaml

echo "== Step 8: stage + commit =="
git add -A
git reset tmp/ || true
git status --short
git diff --staged --stat
git diff --staged --name-only

git commit -m "Complete Phase 3/4 enterprise readiness: doctor, golden paths, helm package, CI/release hardening"
git push -u origin "$(git branch --show-current)"

echo "== Step 9: optional PR =="
echo "gh pr create --fill --base main --head \"\$(git branch --show-current)\""

echo "Done. Share this with assistant:"
echo "1) LOG_FILE path"
echo "2) git diff --staged --name-only (before commit, if you pause)"
echo "3) failing command output if anything breaks"
