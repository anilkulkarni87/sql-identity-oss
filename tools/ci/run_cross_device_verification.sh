#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run cross-device verification from repo root or any subdirectory.

Usage:
  bash tools/ci/run_cross_device_verification.sh [options]

Options:
  --provider <none|aws|gcp|azure>  Cloud provider for optional terraform plan (default: none)
  --run-terraform-plan             Run terraform plan for selected provider module
  --run-docker-checks              Run enterprise docker runtime checks
  --log-file <path>                Log file path (default: /tmp/idr_cross_device_verify_<timestamp>.log)
  -h, --help                       Show help

Examples:
  bash tools/ci/run_cross_device_verification.sh
  bash tools/ci/run_cross_device_verification.sh --provider aws --run-terraform-plan
  bash tools/ci/run_cross_device_verification.sh --run-docker-checks
  bash tools/ci/run_cross_device_verification.sh --provider gcp --run-terraform-plan --run-docker-checks

Terraform plan behavior:
  - Expects deployment/terraform/<provider-module>/terraform.tfvars to exist.
  - If missing, copies terraform.tfvars.example and exits with instructions.
EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd"
    exit 1
  fi
}

PROVIDER="none"
RUN_TERRAFORM_PLAN=0
RUN_DOCKER_CHECKS=0
LOG_FILE=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --provider)
      PROVIDER="${2:-}"
      shift 2
      ;;
    --run-terraform-plan)
      RUN_TERRAFORM_PLAN=1
      shift
      ;;
    --run-docker-checks)
      RUN_DOCKER_CHECKS=1
      shift
      ;;
    --log-file)
      LOG_FILE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

case "$PROVIDER" in
  none|aws|gcp|azure) ;;
  *)
    echo "Invalid --provider value: $PROVIDER"
    usage
    exit 1
    ;;
esac

if [ "$RUN_TERRAFORM_PLAN" -eq 1 ] && [ "$PROVIDER" = "none" ]; then
  echo "--run-terraform-plan requires --provider <aws|gcp|azure>"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

if [ -z "$LOG_FILE" ]; then
  LOG_FILE="/tmp/idr_cross_device_verify_$(date +%Y%m%d_%H%M%S).log"
fi

mkdir -p "$(dirname "$LOG_FILE")"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "LOG_FILE=$LOG_FILE"
echo "REPO_ROOT=$REPO_ROOT"
echo "PROVIDER=$PROVIDER"
echo "RUN_TERRAFORM_PLAN=$RUN_TERRAFORM_PLAN"
echo "RUN_DOCKER_CHECKS=$RUN_DOCKER_CHECKS"
echo

echo "== Step 1: prerequisite command checks =="
require_cmd python3
require_cmd node
require_cmd npm
require_cmd helm
require_cmd terraform
python3 --version
node --version
npm --version
helm version
terraform version

if [ "$RUN_DOCKER_CHECKS" -eq 1 ]; then
  require_cmd docker
  docker --version
  docker compose version
fi

echo
echo "== Step 2: python environment + backend checks =="
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements/ci.lock
python -m pip install -r requirements/release.lock
python -m pip install -e .

PYTHONPATH=. ruff check idr_core idr_api tests tools
PYTHONPATH=. pytest \
  tests/test_api.py \
  tests/test_auth.py \
  tests/test_cli_smoke.py \
  tests/test_doctor.py \
  tests/test_mcp_contract.py \
  tests/test_mcp_secrets.py \
  tests/test_mcp_errors.py \
  -q

echo
echo "== Step 3: UI dependency/build checks =="
pushd idr_ui >/dev/null
npm ci --ignore-scripts --no-audit
npm run build
popd >/dev/null

echo
echo "== Step 4: package reproducibility + install smoke =="
python -m build
twine check dist/*

python3 -m venv .venv-smoke
source .venv-smoke/bin/activate
python -m pip install dist/*.whl
idr version
idr doctor --json
deactivate
source .venv/bin/activate

echo
echo "== Step 5: Helm chart static checks =="
helm lint deployment/helm/idr-enterprise
helm template idr-enterprise deployment/helm/idr-enterprise \
  -f deployment/helm/idr-enterprise/values.example.yaml \
  > /tmp/idr-enterprise-rendered.yaml
test -s /tmp/idr-enterprise-rendered.yaml

echo
echo "== Step 6: Terraform static validation (all providers) =="
terraform fmt -recursive deployment/terraform

for module in aws-eks gcp-gke azure-aks; do
  pushd "deployment/terraform/${module}" >/dev/null
  terraform init -backend=false
  terraform validate
  popd >/dev/null
done

if [ "$RUN_TERRAFORM_PLAN" -eq 1 ]; then
  echo
  echo "== Step 7: Terraform plan ($PROVIDER) =="
  case "$PROVIDER" in
    aws) module_dir="deployment/terraform/aws-eks"; dns_resource="aws_route53_record.idr" ;;
    gcp) module_dir="deployment/terraform/gcp-gke"; dns_resource="google_dns_record_set.idr" ;;
    azure) module_dir="deployment/terraform/azure-aks"; dns_resource="azurerm_dns_a_record.idr" ;;
    *) echo "Unsupported provider value: $PROVIDER"; exit 1 ;;
  esac

  pushd "$module_dir" >/dev/null
  if [ ! -f terraform.tfvars ]; then
    cp terraform.tfvars.example terraform.tfvars
    echo "Created $module_dir/terraform.tfvars from example."
    echo "Edit terraform.tfvars with real values, then rerun this script."
    exit 1
  fi

  terraform init -backend=false
  terraform plan -input=false -var-file=terraform.tfvars -out=tfplan
  terraform show -no-color tfplan > /tmp/idr_"$PROVIDER"_tfplan.txt
  if grep -q "$dns_resource" /tmp/idr_"$PROVIDER"_tfplan.txt; then
    echo "Found DNS resource in plan output: $dns_resource"
  else
    echo "DNS resource not found in plan output: $dns_resource"
    echo "If this was expected, verify create_dns_record and DNS zone settings in terraform.tfvars."
  fi
  popd >/dev/null
fi

if [ "$RUN_DOCKER_CHECKS" -eq 1 ]; then
  echo
  echo "== Step 8: optional enterprise runtime checks (docker) =="
  bash tools/ci/verify_enterprise_stack.sh docker-compose.enterprise.yml
  EXPECT_HEALTHY_SERVICES=api-a,api-b,api,redis,keycloak \
    bash tools/ci/verify_enterprise_stack.sh docker-compose.enterprise.ha.yml
fi

echo
echo "Verification completed."
echo "Share back:"
echo "1) LOG_FILE=$LOG_FILE"
echo "2) Any failing section from the log"
if [ "$RUN_TERRAFORM_PLAN" -eq 1 ]; then
  echo "3) Terraform plan result and whether DNS resource appeared"
fi
