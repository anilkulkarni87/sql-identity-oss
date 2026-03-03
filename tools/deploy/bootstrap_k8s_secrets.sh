#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="idr"
SECRET_NAME="idr-enterprise-secrets"
KEYCLOAK_ADMIN_PASSWORD="${IDR_KEYCLOAK_ADMIN_PASSWORD:-}"
GRAFANA_ADMIN_PASSWORD="${IDR_GRAFANA_ADMIN_PASSWORD:-}"
RUN_JOB_WEBHOOK_BEARER_TOKEN="${IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN:-}"

usage() {
  cat <<'EOF'
Usage:
  bash tools/deploy/bootstrap_k8s_secrets.sh [options]

Options:
  --namespace <name>                   Kubernetes namespace (default: idr)
  --secret-name <name>                 Secret name (default: idr-enterprise-secrets)
  --keycloak-admin-password <value>    Keycloak admin password
  --grafana-admin-password <value>     Grafana admin password
  --run-job-webhook-bearer-token <v>   Optional webhook bearer token
  -h, --help                           Show help

Environment fallbacks:
  IDR_KEYCLOAK_ADMIN_PASSWORD
  IDR_GRAFANA_ADMIN_PASSWORD
  IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN
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
    --namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    --secret-name)
      SECRET_NAME="$2"
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

require_cmd kubectl

if [[ -z "$KEYCLOAK_ADMIN_PASSWORD" ]]; then
  KEYCLOAK_ADMIN_PASSWORD="$(generate_secret)"
  echo "Generated KEYCLOAK admin password."
fi
if [[ -z "$GRAFANA_ADMIN_PASSWORD" ]]; then
  GRAFANA_ADMIN_PASSWORD="$(generate_secret)"
  echo "Generated Grafana admin password."
fi

kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"

kubectl -n "$NAMESPACE" create secret generic "$SECRET_NAME" \
  --from-literal=KEYCLOAK_ADMIN_PASSWORD="$KEYCLOAK_ADMIN_PASSWORD" \
  --from-literal=GF_SECURITY_ADMIN_PASSWORD="$GRAFANA_ADMIN_PASSWORD" \
  --from-literal=IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN="$RUN_JOB_WEBHOOK_BEARER_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Secret applied:"
echo "  namespace: $NAMESPACE"
echo "  name: $SECRET_NAME"
echo ""
echo "Use with Helm:"
echo "  --set secrets.create=false --set secrets.existingSecretName=$SECRET_NAME"
