#!/usr/bin/env bash
set -euo pipefail

# ROOT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR=$(pwd)
ENV_FILE="$ROOT_DIR/.env"

if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1091
  source "$ENV_FILE"
else
  echo "Missing .env at $ENV_FILE" >&2
  exit 1
fi

: "${JWT_TOKEN:?JWT_TOKEN must be set in .env}"
: "${WATTNET_TOKEN:?WATTNET_TOKEN must be set in .env}"

curl -X POST "https://mc-a4.lab.uvalight.net/gd-ci-api/ci" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -H "aggregate: true" \
  -d '{"lat":45.071,"lon":7.652,"start":"2024-05-01T10:30:00Z","end":"2024-05-01T13:30:00Z","pue":1.7}'

curl -s -X GET "https://api.wattnet.eu/v1/footprints?lat=45.071&lon=7.652&footprint_type=carbon&start=2024-05-01T10:30:00Z&end=2024-05-01T13:30:00Z&aggregate=false" \
  -H "Authorization: Bearer $WATTNET_TOKEN" \
  -H "Accept: application/json" \
  -H "aggregate: true"

curl -v -H "Authorization: Bearer $JWT_TOKEN" "https://mc-a4.lab.uvalight.net/gd-cim-api/verify_token"

curl -X GET https://mc-a4.lab.uvalight.net/gd-cim-api/verify_token \
-H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json"
