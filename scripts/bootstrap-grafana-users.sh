#!/bin/sh
set -eu

GRAFANA_URL="${GRAFANA_URL:-http://grafana:3000}"
ADMIN_USER="${GRAFANA_ADMIN_USER:-admin}"
ADMIN_PASS="${GRAFANA_ADMIN_PASSWORD:-admin}"
SEED_USERS="${GRAFANA_SEED_USERS:-}"

if [ -z "$SEED_USERS" ]; then
  echo "[grafana-bootstrap] GRAFANA_SEED_USERS is empty; nothing to do."
  exit 0
fi

echo "[grafana-bootstrap] Waiting for Grafana at ${GRAFANA_URL}..."
i=0
until curl -fsS "${GRAFANA_URL}/api/health" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 120 ]; then
    echo "[grafana-bootstrap] Grafana did not become ready in time."
    exit 1
  fi
  sleep 2
done

echo "[grafana-bootstrap] Grafana is ready; provisioning users."

fail=0
OLD_IFS="$IFS"
IFS=','
set -- $SEED_USERS
IFS="$OLD_IFS"

for login in "$@"; do
  login="$(echo "$login" | tr -d '[:space:]')"
  [ -z "$login" ] && continue

  key="$(echo "$login" | tr '[:lower:]-.' '[:upper:]__')"
  eval "name=\${GRAFANA_NAME_${key}:-$login}"
  eval "email=\${GRAFANA_EMAIL_${key}:-${login}@local.invalid}"
  eval "password=\${GRAFANA_PASS_${key}:-}"

  if [ -z "$password" ]; then
    echo "[grafana-bootstrap] Missing password var GRAFANA_PASS_${key}; skipping ${login}."
    fail=1
    continue
  fi

  payload_create="$(printf '{"name":"%s","email":"%s","login":"%s","password":"%s"}' "$name" "$email" "$login" "$password")"
  response="$(curl -sS -u "${ADMIN_USER}:${ADMIN_PASS}" -H "Content-Type: application/json" -X POST "${GRAFANA_URL}/api/admin/users" -d "${payload_create}" -w '\n%{http_code}')"
  code="$(printf '%s' "$response" | tail -n1)"
  body="$(printf '%s' "$response" | sed '$d')"

  if [ "$code" = "200" ]; then
    echo "[grafana-bootstrap] Created user ${login}."
    continue
  fi

  if [ "$code" = "409" ] || [ "$code" = "412" ]; then
    lookup="$(curl -sS -u "${ADMIN_USER}:${ADMIN_PASS}" "${GRAFANA_URL}/api/users/lookup?loginOrEmail=${login}" -w '\n%{http_code}')"
    lookup_code="$(printf '%s' "$lookup" | tail -n1)"
    lookup_body="$(printf '%s' "$lookup" | sed '$d')"
    if [ "$lookup_code" != "200" ]; then
      echo "[grafana-bootstrap] User exists but lookup failed for ${login}: ${lookup_body}"
      fail=1
      continue
    fi

    user_id="$(printf '%s' "$lookup_body" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p' | head -n1)"
    if [ -z "$user_id" ]; then
      echo "[grafana-bootstrap] Could not parse user id for ${login}."
      fail=1
      continue
    fi

    payload_pass="$(printf '{"password":"%s"}' "$password")"
    pass_resp="$(curl -sS -u "${ADMIN_USER}:${ADMIN_PASS}" -H "Content-Type: application/json" -X PUT "${GRAFANA_URL}/api/admin/users/${user_id}/password" -d "${payload_pass}" -w '\n%{http_code}')"
    pass_code="$(printf '%s' "$pass_resp" | tail -n1)"
    pass_body="$(printf '%s' "$pass_resp" | sed '$d')"
    if [ "$pass_code" = "200" ]; then
      echo "[grafana-bootstrap] Updated password for existing user ${login}."
    else
      echo "[grafana-bootstrap] Failed to update password for ${login}: ${pass_body}"
      fail=1
    fi

    payload_user="$(printf '{"name":"%s","email":"%s","login":"%s"}' "$name" "$email" "$login")"
    user_resp="$(curl -sS -u "${ADMIN_USER}:${ADMIN_PASS}" -H "Content-Type: application/json" -X PUT "${GRAFANA_URL}/api/users/${user_id}" -d "${payload_user}" -w '\n%{http_code}')"
    user_code="$(printf '%s' "$user_resp" | tail -n1)"
    user_body="$(printf '%s' "$user_resp" | sed '$d')"
    if [ "$user_code" = "200" ]; then
      echo "[grafana-bootstrap] Updated profile for existing user ${login}."
    else
      echo "[grafana-bootstrap] Failed to update profile for ${login}: ${user_body}"
      fail=1
    fi
    continue
  fi

  echo "[grafana-bootstrap] Failed creating ${login}: ${body}"
  fail=1
done

if [ "$fail" -ne 0 ]; then
  exit 1
fi

echo "[grafana-bootstrap] Provisioning complete."
