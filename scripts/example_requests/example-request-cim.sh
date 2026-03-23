#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"
BASE_URL="${BASE_URL:-https://greendigit-cim.sztaki.hu}"

if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1091
  source "$ENV_FILE"
else
  echo "Missing .env at $ENV_FILE" >&2
  exit 1
fi

: "${JWT_TOKEN:?JWT_TOKEN must be set in .env}"
: "${CIM_EMAIL:?CIM_EMAIL must be set in .env so publisher_email matches the JWT subject}"

PUBLISHER_EMAIL="${PUBLISHER_EMAIL:-$CIM_EMAIL}"

build_payload() {
  local start="$1"
  local end="$2"
  local limit="$3"
  local end_inclusive="${4:-true}"

  jq -nc \
    --arg publisher_email "$PUBLISHER_EMAIL" \
    --arg start "$start" \
    --arg end "$end" \
    --argjson limit_docs "$limit" \
    --argjson end_inclusive "$end_inclusive" \
    '{
      publisher_email: $publisher_email,
      start: $start,
      end: $end,
      end_inclusive: $end_inclusive,
      limit_docs: $limit_docs
    }'
}

curl -sS -X POST "$BASE_URL/gd-cim-api/v1/submit-cim" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "$(build_payload "2026-02-06T00:00:00Z" "2026-02-08T00:00:00Z" 10)"

# To automatically extract the event_ids

curl -sS -X POST "$BASE_URL/gd-cim-api/v1/submit-cim" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "$(build_payload "2026-02-06T00:00:00Z" "2026-02-08T00:00:00Z" 10)" \
| jq -r '.cim_response.results[].cnr_response.event_id'

curl -sS 'http://localhost:8033/get-cnr-entry/568339' | jq

# Small request
curl -sS -X POST "$BASE_URL/gd-cim-api/v1/submit-cim" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "$(build_payload "2026-01-16T12:00:00Z" "2026-01-16T12:05:00Z" 500)"

START="2025-09-01T00:00:00Z"
END="2025-12-31T23:59:59Z"
t="$START"

while [[ "$t" < "$END" ]]; do
  t2="$(date -u -d "$t + 15 minutes" +%Y-%m-%dT%H:%M:%SZ)"
  [[ "$t2" > "$END" ]] && t2="$END"

  echo "window $t -> $t2" >&2 # For logs

  curl -sS -X POST "$BASE_URL/gd-cim-api/v1/submit-cim" \
    -H "Authorization: Bearer $JWT_TOKEN" \
    -H 'Content-Type: application/json' \
    -d "$(build_payload "$t" "$t2" 1000 false)"

  echo
  t="$t2"
done
