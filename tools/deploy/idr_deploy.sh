#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHART_PATH="$ROOT_DIR/deployment/helm/idr-enterprise"
PROFILE="enterprise"
PROVIDER=""
MODE="apply"
NAMESPACE="idr"
RELEASE="idr-enterprise"
RUN_DOCTOR=1
API_URL=""
METRICS_URL=""
WHOAMI_URL=""
TOKEN_ENV="IDR_TOKEN"
TIMEOUT_SECONDS="5"
USE_EXISTING_SECRET=""
KEYCLOAK_ADMIN_PASSWORD="${IDR_KEYCLOAK_ADMIN_PASSWORD:-}"
GRAFANA_ADMIN_PASSWORD="${IDR_GRAFANA_ADMIN_PASSWORD:-}"
RUN_JOB_WEBHOOK_BEARER_TOKEN="${IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN:-}"

declare -a EXTRA_VALUES=()
declare -a EXTRA_SET_ARGS=()

usage() {
  cat <<'EOF'
Usage:
  bash tools/deploy/idr_deploy.sh --provider aws|gcp|azure [options]

Options:
  --provider <aws|gcp|azure>           Cloud provider preset (required)
  --profile <enterprise>               Deployment profile (default: enterprise)
  --mode <plan|apply>                  Plan (lint/template) or apply (helm upgrade --install)
  --namespace <name>                   Kubernetes namespace (default: idr)
  --release <name>                     Helm release name (default: idr-enterprise)
  --values <file>                      Additional values file (repeatable)
  --set <key=value>                    Additional --set override (repeatable)
  --use-existing-secret <name>         Existing Kubernetes secret name for chart secret refs
  --keycloak-admin-password <value>    Secret value (used when not using existing secret)
  --grafana-admin-password <value>     Secret value (used when not using existing secret)
  --run-job-webhook-bearer-token <v>   Optional webhook bearer token secret
  --api-url <url>                      Optional externally reachable API health URL for post-deploy doctor
  --metrics-url <url>                  Optional metrics URL for post-deploy doctor
  --whoami-url <url>                   Optional whoami URL for post-deploy doctor
  --token-env <var>                    Env var containing bearer token for doctor auth check
  --timeout-seconds <n>                HTTP timeout for doctor checks (default: 5)
  --no-doctor                          Skip post-deploy doctor check
  -h, --help                           Show this help

Examples:
  bash tools/deploy/idr_deploy.sh --provider aws --mode plan

  bash tools/deploy/idr_deploy.sh \
    --provider gcp \
    --use-existing-secret idr-enterprise-secrets \
    --values ./values.prod.yaml \
    --api-url https://idr.example.com/api/health \
    --metrics-url https://idr.example.com/metrics \
    --whoami-url https://idr.example.com/api/auth/whoami
EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd"
    exit 1
  fi
}

generate_secret() {
  python - <<'PY'
import secrets
print(secrets.token_urlsafe(32))
PY
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --provider)
      PROVIDER="$2"
      shift 2
      ;;
    --profile)
      PROFILE="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    --release)
      RELEASE="$2"
      shift 2
      ;;
    --values)
      EXTRA_VALUES+=("$2")
      shift 2
      ;;
    --set)
      EXTRA_SET_ARGS+=("$2")
      shift 2
      ;;
    --use-existing-secret)
      USE_EXISTING_SECRET="$2"
      shift 2
      ;;
    --keycloak-admin-password)
      KEYCLOAK_ADMIN_PASSWORD="$2"
      shift 2
      ;;
    --grafana-admin-password)
      GRAFANA_ADMIN_PASSWORD="$2"
      shift 2
      ;;
    --run-job-webhook-bearer-token)
      RUN_JOB_WEBHOOK_BEARER_TOKEN="$2"
      shift 2
      ;;
    --api-url)
      API_URL="$2"
      shift 2
      ;;
    --metrics-url)
      METRICS_URL="$2"
      shift 2
      ;;
    --whoami-url)
      WHOAMI_URL="$2"
      shift 2
      ;;
    --token-env)
      TOKEN_ENV="$2"
      shift 2
      ;;
    --timeout-seconds)
      TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    --no-doctor)
      RUN_DOCTOR=0
      shift 1
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

if [[ -z "$PROVIDER" ]]; then
  echo "--provider is required."
  usage
  exit 1
fi

if [[ "$PROFILE" != "enterprise" ]]; then
  echo "Unsupported profile: $PROFILE (supported: enterprise)"
  exit 1
fi

if [[ "$MODE" != "plan" && "$MODE" != "apply" ]]; then
  echo "Unsupported --mode: $MODE (expected plan|apply)"
  exit 1
fi

case "$PROVIDER" in
  aws) PRESET_FILE="$ROOT_DIR/deployment/helm/presets/aws-eks.yaml" ;;
  gcp) PRESET_FILE="$ROOT_DIR/deployment/helm/presets/gcp-gke.yaml" ;;
  azure) PRESET_FILE="$ROOT_DIR/deployment/helm/presets/azure-aks.yaml" ;;
  *)
    echo "Unsupported provider: $PROVIDER (supported: aws|gcp|azure)"
    exit 1
    ;;
esac

if [[ ! -f "$PRESET_FILE" ]]; then
  echo "Preset file not found: $PRESET_FILE"
  exit 1
fi

require_cmd helm
if [[ "$MODE" == "apply" ]]; then
  require_cmd kubectl
fi

COMMON_ARGS=(
  --namespace "$NAMESPACE"
  -f "$PRESET_FILE"
)

for values_file in "${EXTRA_VALUES[@]}"; do
  COMMON_ARGS+=(-f "$values_file")
done

if [[ -n "$USE_EXISTING_SECRET" ]]; then
  COMMON_ARGS+=(--set "secrets.create=false")
  COMMON_ARGS+=(--set "secrets.existingSecretName=$USE_EXISTING_SECRET")
else
  # Generate ephemeral secrets when not supplied.
  if [[ -z "$KEYCLOAK_ADMIN_PASSWORD" ]]; then
    KEYCLOAK_ADMIN_PASSWORD="$(generate_secret)"
    echo "Generated KEYCLOAK admin password for this deployment."
  fi
  if [[ -z "$GRAFANA_ADMIN_PASSWORD" ]]; then
    GRAFANA_ADMIN_PASSWORD="$(generate_secret)"
    echo "Generated Grafana admin password for this deployment."
  fi
  COMMON_ARGS+=(--set-string "secrets.keycloakAdminPassword=$KEYCLOAK_ADMIN_PASSWORD")
  COMMON_ARGS+=(--set-string "secrets.grafanaAdminPassword=$GRAFANA_ADMIN_PASSWORD")
  COMMON_ARGS+=(--set-string "secrets.runJobWebhookBearerToken=$RUN_JOB_WEBHOOK_BEARER_TOKEN")
fi

for set_kv in "${EXTRA_SET_ARGS[@]}"; do
  COMMON_ARGS+=(--set "$set_kv")
done

HELM_ARGS=(
  upgrade --install "$RELEASE" "$CHART_PATH"
  --create-namespace
  "${COMMON_ARGS[@]}"
)

echo "Preset: $PRESET_FILE"
echo "Release: $RELEASE"
echo "Namespace: $NAMESPACE"

if [[ "$MODE" == "plan" ]]; then
  echo "Running Helm lint..."
  helm lint "$CHART_PATH"
  echo "Rendering manifest preview..."
  helm template "$RELEASE" "$CHART_PATH" \
    "${COMMON_ARGS[@]}" >/tmp/idr-enterprise-rendered.yaml
  echo "Plan complete: /tmp/idr-enterprise-rendered.yaml"
  exit 0
fi

echo "Deploying chart..."
helm "${HELM_ARGS[@]}"

echo "Waiting for primary deployments..."
kubectl -n "$NAMESPACE" rollout status deploy/"$RELEASE"-api --timeout=180s || true
kubectl -n "$NAMESPACE" rollout status deploy/"$RELEASE"-ui --timeout=180s || true

echo "Service summary:"
kubectl -n "$NAMESPACE" get svc

if [[ "$RUN_DOCTOR" -eq 1 ]]; then
  if command -v idr >/dev/null 2>&1 && [[ -n "$API_URL" && -n "$METRICS_URL" ]]; then
    DOCTOR_CMD=(
      idr doctor --target cluster
      --api-url "$API_URL"
      --metrics-url "$METRICS_URL"
      --timeout-seconds "$TIMEOUT_SECONDS"
      --token-env "$TOKEN_ENV"
    )
    if [[ -n "$WHOAMI_URL" ]]; then
      DOCTOR_CMD+=(--whoami-url "$WHOAMI_URL")
    fi
    echo "Running post-deploy doctor..."
    "${DOCTOR_CMD[@]}"
  else
    echo "Skipping post-deploy doctor."
    echo "To run manually:"
    echo "  idr doctor --target cluster --api-url <...> --metrics-url <...> --whoami-url <...>"
  fi
fi

echo "Deployment completed."
