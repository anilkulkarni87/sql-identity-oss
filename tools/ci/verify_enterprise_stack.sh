#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${1:-docker-compose.enterprise.yml}"
MAX_RETRIES="${MAX_RETRIES:-90}"
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

wait_for_status_200() {
  local url="$1"
  local label="$2"
  local attempt=1

  echo "Waiting for ${label}: ${url}"
  while [ "$attempt" -le "$MAX_RETRIES" ]; do
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

on_exit() {
  local exit_code="$1"
  if [ "$exit_code" -ne 0 ]; then
    echo ""
    echo "Enterprise stack verification failed. Dumping logs..."
    compose -f "$COMPOSE_FILE" logs --no-color || true
  fi

  echo ""
  echo "Tearing down enterprise stack..."
  compose -f "$COMPOSE_FILE" down -v || true
}

trap 'on_exit $?' EXIT

detect_compose_bin
echo "Using compose command: ${COMPOSE_BIN[*]}"

echo "Starting enterprise stack from ${COMPOSE_FILE}..."
compose -f "$COMPOSE_FILE" up -d --build

wait_for_status_200 "http://localhost:8080/realms/idr-realm/.well-known/openid-configuration" "Keycloak realm"
wait_for_status_200 "http://localhost:8000/api/health" "IDR API health"
wait_for_status_200 "http://localhost:9090/-/healthy" "Prometheus health"

echo "Validating /metrics payload..."
metrics_payload="$(curl -sS http://localhost:8000/metrics)"
echo "$metrics_payload" | grep -q "idr_http_requests_total"
echo "$metrics_payload" | grep -q "idr_api_db_connected"

echo "Validating auth protection on /api/schema..."
schema_noauth_status="$(curl -s -o /tmp/schema_noauth.json -w "%{http_code}" http://localhost:8000/api/schema || true)"
if [ "$schema_noauth_status" != "401" ]; then
  echo "Expected /api/schema to return 401 without token; got ${schema_noauth_status}"
  exit 1
fi

echo "Requesting OIDC token for test user..."
token_response="$(curl -sS -X POST \
  "http://localhost:8080/realms/idr-realm/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=password" \
  --data-urlencode "client_id=idr-web" \
  --data-urlencode "username=test" \
  --data-urlencode "password=test")"

access_token="$(
  python -c 'import json,sys; print(json.loads(sys.stdin.read()).get("access_token",""))' <<<"$token_response"
)"

if [ -z "$access_token" ]; then
  echo "Failed to retrieve access token. Response:"
  echo "$token_response"
  exit 1
fi

echo "Validating authenticated access to /api/schema..."
schema_auth_status="$(curl -s -o /tmp/schema_auth.json -w "%{http_code}" \
  -H "Authorization: Bearer ${access_token}" \
  http://localhost:8000/api/schema || true)"

if [ "$schema_auth_status" != "200" ]; then
  echo "Expected /api/schema to return 200 with token; got ${schema_auth_status}"
  cat /tmp/schema_auth.json || true
  exit 1
fi

python - <<'PY'
import json
from pathlib import Path

body = json.loads(Path("/tmp/schema_auth.json").read_text())
if not isinstance(body, list) or not body:
    raise SystemExit("Authenticated /api/schema payload is empty or invalid")
if "schema_name" not in body[0]:
    raise SystemExit("Authenticated /api/schema payload missing schema_name")
print("✓ Authenticated schema response validated")
PY

echo "Validating Prometheus target health..."
prom_targets="$(curl -sS http://localhost:9090/api/v1/targets)"

PROM_TARGETS="$prom_targets" python - <<'PY'
import json
import os

payload = json.loads(os.environ["PROM_TARGETS"])
targets = payload.get("data", {}).get("activeTargets", [])

for t in targets:
    labels = t.get("labels", {})
    if labels.get("job") == "idr-api" and t.get("health") == "up":
        print("✓ Prometheus target idr-api is up")
        raise SystemExit(0)

raise SystemExit("idr-api target was not healthy in Prometheus")
PY

echo ""
echo "✅ Enterprise stack verification passed."
