#!/usr/bin/env bash
set -euo pipefail

ISSUER="${IDR_AUTH_ISSUER:-}"
JWKS_URL="${IDR_AUTH_JWKS_URL:-}"
AUDIENCE="${IDR_AUTH_AUDIENCE:-account}"
OUTPUT_FILE=""
DISABLE_KEYCLOAK=0

usage() {
  cat <<'EOF'
Usage:
  bash tools/deploy/bootstrap_oidc_values.sh --issuer <url> [options]

Options:
  --issuer <url>            OIDC issuer URL (required)
  --jwks-url <url>          JWKS URL (default: <issuer>/protocol/openid-connect/certs)
  --audience <value>        API audience claim (default: account)
  --output <file>           Output values file (default: /tmp/idr-oidc-values.yaml)
  --disable-keycloak        Disable bundled Keycloak in generated values
  -h, --help                Show help

Environment fallbacks:
  IDR_AUTH_ISSUER, IDR_AUTH_JWKS_URL, IDR_AUTH_AUDIENCE
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --issuer)
      ISSUER="$2"
      shift 2
      ;;
    --jwks-url)
      JWKS_URL="$2"
      shift 2
      ;;
    --audience)
      AUDIENCE="$2"
      shift 2
      ;;
    --output)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    --disable-keycloak)
      DISABLE_KEYCLOAK=1
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

if [[ -z "$ISSUER" ]]; then
  echo "--issuer is required."
  usage
  exit 1
fi

if [[ -z "$JWKS_URL" ]]; then
  JWKS_URL="${ISSUER%/}/protocol/openid-connect/certs"
fi

if [[ -z "$OUTPUT_FILE" ]]; then
  OUTPUT_FILE="/tmp/idr-oidc-values.yaml"
fi

cat >"$OUTPUT_FILE" <<EOF
api:
  oidc:
    issuer: "$ISSUER"
    jwksUrl: "$JWKS_URL"
  env:
    IDR_AUTH_AUDIENCE: "$AUDIENCE"
EOF

if [[ "$DISABLE_KEYCLOAK" -eq 1 ]]; then
  cat >>"$OUTPUT_FILE" <<'EOF'

keycloak:
  enabled: false
EOF
fi

echo "Generated OIDC override values: $OUTPUT_FILE"
echo ""
echo "Apply with:"
echo "  helm upgrade --install idr-enterprise deployment/helm/idr-enterprise \\"
echo "    --namespace idr --create-namespace \\"
echo "    -f <cloud-preset.yaml> -f $OUTPUT_FILE"
