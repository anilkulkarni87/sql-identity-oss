#!/usr/bin/env bash
set -euo pipefail

ROLLBACK_TAG="${1:?Usage: validate_prod_rollback.sh <rollback_tag> [compose_file] [env_file]}"
COMPOSE_FILE="${2:-docker-compose.prod.yml}"
ENV_FILE="${3:-.env.example}"
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

if [ ! -f "$COMPOSE_FILE" ]; then
  echo "Compose file not found: $COMPOSE_FILE"
  exit 1
fi

if [ ! -f "$ENV_FILE" ]; then
  echo "Env file not found: $ENV_FILE"
  exit 1
fi

detect_compose_bin
echo "Using compose command: ${COMPOSE_BIN[*]}"

echo "Validating rollback tag rendering for ${COMPOSE_FILE} (IDR_IMAGE_TAG=${ROLLBACK_TAG})..."
rendered="$(
  IDR_IMAGE_TAG="$ROLLBACK_TAG" compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" config
)"

echo "$rendered" | grep -qE "image: .*/idr-api:${ROLLBACK_TAG}$" || {
  echo "Rollback validation failed: idr-api image did not render with tag ${ROLLBACK_TAG}"
  exit 1
}

echo "$rendered" | grep -qE "image: .*/idr-ui:${ROLLBACK_TAG}$" || {
  echo "Rollback validation failed: idr-ui image did not render with tag ${ROLLBACK_TAG}"
  exit 1
}

echo "✅ Rollback dry-run validation passed for tag ${ROLLBACK_TAG}"
