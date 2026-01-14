#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "$0")" && pwd)"
JS_FILE="${JS_FILE:-"$SCRIPT_DIR/fetch_metrics_data.js"}"
ALLOWED_FILE=${ALLOWED_FILE:-"allowed_emails.txt"}
ANALYSIS_DIR=${ANALYSIS_DIR:-"analysis"}
MONGO_URI=${MONGO_URI:-"mongodb://metrics-db:27017/?replicaSet=rs0"}
DB_NAME=${DB_NAME:-"metricsdb"}
COLL_NAME=${COLL_NAME:-"metrics"}
MONGO_SH=${MONGO_SH:-"docker compose exec -T metrics-db mongosh"}
ALLOW_DISK_USE=${ALLOW_DISK_USE:-"true"}

if [[ ! -f "$ALLOWED_FILE" ]]; then
  echo "allowed emails file not found: $ALLOWED_FILE" >&2
  exit 1
fi

mapfile -t EMAILS < <(grep -v '^[[:space:]]*$' "$ALLOWED_FILE" | grep -v '^[[:space:]]*#' | tr -d '\r')
if (( ${#EMAILS[@]} == 0 )); then
  echo "No emails to process." >&2
  exit 0
fi

mkdir -p "$ANALYSIS_DIR"

EMAILS_JSON=$(printf '%s\n' "${EMAILS[@]}" | sed 's/\\/\\\\/g; s/"/\\"/g; s/^/"/; s/$/"/' | paste -sd, -)
EMAILS_JSON="[$EMAILS_JSON]"

read -r -a MONGO_CMD <<<"$MONGO_SH"

if [[ "${MONGO_CMD[*]}" == *"exec"* ]]; then
  env_flags=(
    "-e" "EMAILS_JSON=$EMAILS_JSON"
    "-e" "ANALYSIS_DIR=$ANALYSIS_DIR"
    "-e" "DB_NAME=$DB_NAME"
    "-e" "COLL_NAME=$COLL_NAME"
    "-e" "ALLOW_DISK_USE=$ALLOW_DISK_USE"
  )
  for i in "${!MONGO_CMD[@]}"; do
    if [[ "${MONGO_CMD[$i]}" == "exec" ]]; then
      MONGO_CMD=( "${MONGO_CMD[@]:0:$((i+1))}" "${env_flags[@]}" "${MONGO_CMD[@]:$((i+1))}" )
      break
    fi
  done
fi

EMAILS_JSON="$EMAILS_JSON" \
ANALYSIS_DIR="$ANALYSIS_DIR" \
DB_NAME="$DB_NAME" \
COLL_NAME="$COLL_NAME" \
ALLOW_DISK_USE="$ALLOW_DISK_USE" \
if [[ -f "$JS_FILE" ]]; then
  "${MONGO_CMD[@]}" "$MONGO_URI" --quiet < "$JS_FILE"
else
  "${MONGO_CMD[@]}" "$MONGO_URI" --quiet "$JS_FILE"
fi
