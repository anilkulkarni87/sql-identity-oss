#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${1:-docker-compose.enterprise.yml}"
VERIFY="${VERIFY:-1}"
MAX_RETRIES="${MAX_RETRIES:-90}"
# Keycloak realm bootstrap/import can take several minutes on cold starts.
KEYCLOAK_MAX_RETRIES="${KEYCLOAK_MAX_RETRIES:-600}"
SLEEP_SECONDS="${SLEEP_SECONDS:-2}"
DESKTOP_COMPOSE_BIN="/Applications/Docker.app/Contents/Resources/cli-plugins/docker-compose"

declare -a COMPOSE_BIN

detect_compose_bin() {
  if docker compose version >/dev/null 2>&1; then
    COMPOSE_BIN=(docker compose)
    return
  fi

  if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_BIN=(docker-compose)
    return
  fi

  if [ -x "$DESKTOP_COMPOSE_BIN" ]; then
    COMPOSE_BIN=("$DESKTOP_COMPOSE_BIN")
    return
  fi

  echo "Compose CLI not found. Install Docker Compose v2 or docker-compose."
  exit 1
}

compose() {
  "${COMPOSE_BIN[@]}" "$@"
}

generate_secret() {
  python - <<'PY'
import secrets
print(secrets.token_urlsafe(32))
PY
}

ensure_runtime_secret_env() {
  local var_name="$1"
  if [ -z "${!var_name:-}" ]; then
    local generated
    generated="$(generate_secret)"
    export "${var_name}=${generated}"
    echo "Set ${var_name} from generated runtime secret."
  fi
}

wait_for_status_200() {
  local url="$1"
  local label="$2"
  local max_retries="${3:-$MAX_RETRIES}"
  local attempt=1

  echo "Waiting for ${label}: ${url} (max retries: ${max_retries})"
  while [ "$attempt" -le "$max_retries" ]; do
    status="$(curl -s -o /dev/null -w "%{http_code}" "$url" || true)"
    if [ "$status" = "200" ]; then
      echo "✓ ${label} is ready"
      return 0
    fi
    sleep "$SLEEP_SECONDS"
    attempt=$((attempt + 1))
  done

  echo "✗ Timed out waiting for ${label}"
  return 1
}

detect_compose_bin
echo "Using compose command: ${COMPOSE_BIN[*]}"

ensure_runtime_secret_env "IDR_KEYCLOAK_ADMIN_PASSWORD"
ensure_runtime_secret_env "IDR_GRAFANA_ADMIN_PASSWORD"

echo "Starting enterprise stack from ${COMPOSE_FILE}..."
compose -f "$COMPOSE_FILE" up -d --build

if [ "$VERIFY" = "1" ]; then
  wait_for_status_200 "http://localhost:8080/realms/idr-realm/.well-known/openid-configuration" "Keycloak realm" "$KEYCLOAK_MAX_RETRIES"
  wait_for_status_200 "http://localhost:8000/api/health" "IDR API health"
  wait_for_status_200 "http://localhost:9090/-/healthy" "Prometheus health"
  wait_for_status_200 "http://localhost:3001/api/health" "Grafana health"
  echo "✓ Enterprise stack verified."
fi

echo ""
echo "Enterprise stack is running."
echo "UI:       http://localhost:3000"
echo "API:      http://localhost:8000/docs"
echo "Keycloak: http://localhost:8080"
echo "Grafana:  http://localhost:3001"
