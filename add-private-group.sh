email="test@uva.nl"

grep -qxF "$email" submit_emails.txt ||
  printf '%s\n' "$email" >> submit_emails.txt

grep -qxF "$email" dashboards_emails.txt ||
  printf '%s\n' "$email" >> dashboards_emails.txt

BASE_URL="https://greendigit-cim.sztaki.hu"
TEST_PASSWORD="test-goncalo"

TEST_TOKEN="$(
  curl -sS -G "$BASE_URL/gd-cim-api/v1/token" \
    --data-urlencode "email=test@uva.nl" \
    --data-urlencode "password=$TEST_PASSWORD" |
  jq -r '.access_token'
)"

scripts/manage-user-role.sh bootstrap

# Create group
scripts/manage-user-role.sh group create test-uva
scripts/manage-user-role.sh group add-user test-uva test@uva.nl

# Test group
scripts/manage-user-role.sh user show test@uva.nl
scripts/manage-user-role.sh group members test-uva

curl -sS -X POST "$BASE_URL/gd-cim-api/v1/submit" \
  -H "Authorization: Bearer $TEST_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "group": "test-uva",
    "test_marker": "private-group-isolation-test-001",
    "site": "DUMMY-UVA",
    "energy_wh": 12.5,
    "timestamp": "2026-09-25T12:00:00Z"
  }' | jq

curl -sS -G "$BASE_URL/gd-cim-api/v1/cim-records" \
  -H "Authorization: Bearer $TEST_TOKEN" \
  --data-urlencode "filter_key=test_marker=private-group-isolation-test-001" |
  jq