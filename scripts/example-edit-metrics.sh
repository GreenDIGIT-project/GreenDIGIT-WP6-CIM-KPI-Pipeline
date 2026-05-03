#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(pwd)
ENV_FILE="$ROOT_DIR/.env"
BASE_URL="${BASE_URL:-https://greendigit-cim.sztaki.hu/gd-cim-api/v1}"

if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1091
  source "$ENV_FILE"
fi

: "${JWT_TOKEN:?JWT_TOKEN must be set in .env or env}"

# Optional second user token for auth-scope checks.
OTHER_JWT_TOKEN="${OTHER_JWT_TOKEN:-}"

START="${START:-2026-03-01T00:00:00Z}"
END="${END:-2026-03-31T23:59:59Z}"
SITE_NAME="${SITE_NAME:-EGI.SARA.nl}"
OWNER="${OWNER:-DIRAC}"
ACTIVITY="${ACTIVITY:-grid}"
SITE_ID="${SITE_ID:-123}"

echo "# Mongo/CIM records: list with recursive filters"
curl -sS -G "$BASE_URL/cim-records" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  --data-urlencode "filter_key=SiteName=$SITE_NAME" \
  --data-urlencode "filter_key=Owner=$OWNER" \
  --data-urlencode "start=$START" \
  --data-urlencode "end=$END" \
  --data-urlencode "limit=20"
echo
echo

echo "# Mongo/CIM records: count"
curl -sS -G "$BASE_URL/cim-records/count" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  --data-urlencode "filter_key=SiteName=$SITE_NAME" \
  --data-urlencode "start=$START" \
  --data-urlencode "end=$END"
echo
echo

echo "# Mongo/CIM auth scoping: second token should not see the first user's records"
if [ -n "$OTHER_JWT_TOKEN" ]; then
  curl -sS -G "$BASE_URL/cim-records" \
    -H "Authorization: Bearer $OTHER_JWT_TOKEN" \
    --data-urlencode "filter_key=SiteName=$SITE_NAME" \
    --data-urlencode "start=$START" \
    --data-urlencode "end=$END" \
    --data-urlencode "limit=20"
else
  echo "Set OTHER_JWT_TOKEN to run this check."
fi
echo
echo

echo "# Mongo/CIM pagination: limit + offset"
curl -sS -G "$BASE_URL/cim-records" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  --data-urlencode "start=$START" \
  --data-urlencode "end=$END" \
  --data-urlencode "limit=5" \
  --data-urlencode "offset=5"
echo
echo

echo "# Mongo/CIM pagination: page overrides offset"
curl -sS -G "$BASE_URL/cim-records" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  --data-urlencode "start=$START" \
  --data-urlencode "end=$END" \
  --data-urlencode "limit=5" \
  --data-urlencode "offset=999" \
  --data-urlencode "page=2"
echo
echo

echo "# Mongo/CIM validation: start must be <= end"
curl -sS -G "$BASE_URL/cim-records" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  --data-urlencode "start=$END" \
  --data-urlencode "end=$START"
echo
echo

echo "# Mongo/CIM delete: unmatched filter_key reporting"
curl -sS -X POST "$BASE_URL/cim-db/delete" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"filter_key\": [\"SiteName=$SITE_NAME\", \"Owner=THIS_OWNER_DOES_NOT_EXIST\"],
    \"start\": \"$START\",
    \"end\": \"$END\"
  }"
echo
echo

echo "# Mongo/CIM delete: empty-result case"
curl -sS -X POST "$BASE_URL/cim-db/delete" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"filter_key\": [\"SiteName=THIS_SITE_DOES_NOT_EXIST\"],
    \"start\": \"$START\",
    \"end\": \"$END\"
  }"
echo
echo

echo "# Mongo/CIM delete: partial-delete case"
echo "# Use a broad SiteName plus a narrower Owner to remove only a subset of the time-window candidates."
curl -sS -X POST "$BASE_URL/cim-db/delete" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"filter_key\": [\"SiteName=$SITE_NAME\", \"Owner=$OWNER\"],
    \"start\": \"$START\",
    \"end\": \"$END\"
  }"
echo
echo

echo "# CNR SQL records: list"
curl -sS -G "$BASE_URL/cnr-records" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  --data-urlencode "site_id=$SITE_ID" \
  --data-urlencode "vo=$OWNER" \
  --data-urlencode "activity=$ACTIVITY" \
  --data-urlencode "start=$START" \
  --data-urlencode "end=$END" \
  --data-urlencode "limit=20"
echo
echo

echo "# CNR SQL records: count"
curl -sS -G "$BASE_URL/cnr-records/count" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  --data-urlencode "site_id=$SITE_ID" \
  --data-urlencode "vo=$OWNER" \
  --data-urlencode "activity=$ACTIVITY" \
  --data-urlencode "start=$START" \
  --data-urlencode "end=$END"
echo
echo

echo "# CNR SQL pagination: limit + offset"
curl -sS -G "$BASE_URL/cnr-records" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  --data-urlencode "start=$START" \
  --data-urlencode "end=$END" \
  --data-urlencode "limit=5" \
  --data-urlencode "offset=5"
echo
echo

echo "# CNR SQL overlap semantics"
echo "# Expected match rule on adapter side: event_start_timestamp <= end AND event_end_timestamp >= start."
curl -sS -G "$BASE_URL/cnr-records" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  --data-urlencode "start=2026-03-10T12:00:00Z" \
  --data-urlencode "end=2026-03-10T12:15:00Z" \
  --data-urlencode "limit=20"
echo
echo

echo "# CNR SQL validation: start must be <= end"
curl -sS -G "$BASE_URL/cnr-records" \
  -H "Authorization: Bearer $JWT_TOKEN" \
  --data-urlencode "start=$END" \
  --data-urlencode "end=$START"
echo
echo

echo "# CNR SQL delete is disabled"
# curl -sS -X POST "$BASE_URL/cnr-db/delete" \
#   -H "Authorization: Bearer $JWT_TOKEN" \
#   -H "Content-Type: application/json" \
#   -d "{
#     \"site_id\": $SITE_ID,
#     \"vo\": \"$OWNER\",
#     \"activity\": \"$ACTIVITY\",
#     \"start\": \"$START\",
#     \"end\": \"$END\"
#   }"
echo
echo
