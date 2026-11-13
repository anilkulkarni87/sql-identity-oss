#!/usr/bin/env bash
set -euo pipefail

APP_NAME="${1:-sql-idr}"
WORKSPACE_USER="${2:-}"

if ! command -v databricks >/dev/null 2>&1; then
  echo "Error: 'databricks' CLI not found on PATH."
  echo ""
  echo "Install it on macOS with Homebrew:"
  echo "  brew tap databricks/tap && brew install databricks"
  echo ""
  echo "Then authenticate to your workspace:"
  echo "  databricks auth login --host https://<your-workspace>.cloud.databricks.com"
  exit 1
fi

if [ -z "$WORKSPACE_USER" ]; then
  if ! ME_JSON=$(databricks current-user me --output json 2>/dev/null); then
    echo "Error: Not authenticated to Databricks CLI."
    echo "Run: databricks auth login --host https://<your-workspace>.cloud.databricks.com"
    exit 1
  fi
  WORKSPACE_USER=$(printf '%s' "$ME_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin)['userName'])")
fi

TARGET_DIR="/Workspace/Users/${WORKSPACE_USER}/apps/${APP_NAME}"

echo "1. Building React UI..."
npm --prefix idr_ui ci --silent || npm --prefix idr_ui install --silent
npm --prefix idr_ui run build

echo "2. Syncing UI bundle into idr_api/static..."
rm -rf idr_api/static
mkdir -p idr_api/static
cp -R idr_ui/dist/* idr_api/static/

echo "3. Staging clean app bundle (excluding .venv, node_modules, caches, symlinks)..."
STAGING_DIR=$(mktemp -d)
trap 'rm -rf "$STAGING_DIR"' EXIT

cp app.yaml requirements.txt pyproject.toml "$STAGING_DIR/"
rsync -a --exclude='__pycache__' --exclude='*.pyc' --exclude='.DS_Store' idr_core/ "$STAGING_DIR/idr_core/"
rsync -a --exclude='__pycache__' --exclude='*.pyc' --exclude='.DS_Store' idr_api/ "$STAGING_DIR/idr_api/"

echo "4. Creating app '${APP_NAME}' (if not already created)..."
databricks apps create "${APP_NAME}" 2>/dev/null || true

echo "5. Uploading clean bundle to ${TARGET_DIR}..."
databricks workspace delete "${TARGET_DIR}" --recursive 2>/dev/null || true
databricks workspace mkdirs "${TARGET_DIR}"
databricks workspace import-dir "${STAGING_DIR}" "${TARGET_DIR}" --overwrite

echo "6. Deploying Databricks App '${APP_NAME}'..."
databricks apps deploy "${APP_NAME}" --source-code-path "${TARGET_DIR}"

echo "Done! Check status & URL with: databricks apps get ${APP_NAME}"
