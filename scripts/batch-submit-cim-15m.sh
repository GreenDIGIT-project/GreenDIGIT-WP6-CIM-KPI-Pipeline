#!/usr/bin/env bash
set -euo pipefail

# Iterate 15-minute windows from START..END and replay stored metrics through AuthServer -> CIM.
#
# This script is safe against high submission rates because it still paginates within each window
# using (after_timestamp, after_id) until the window is exhausted.

# Command:
# export JWT_TOKEN=$JWT_TOKEN # Assuming that it is already here.
# START='2025-09-01T00:00:00Z' \
# END="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
# LIMIT_DOCS=1000 \
# WINDOW_MINUTES=15 \
# OUT_DIR=analysis/submit_cim_15m \
# scripts/batch-submit-cim-15m.sh

AUTH_BASE=${AUTH_BASE:-"http://localhost:8000/gd-cim-api/v1"}
ENDPOINT="$AUTH_BASE/submit-cim"

START=${START:-"2025-08-01T00:00:00Z"}
END=${END:-""}
WINDOW_MINUTES=${WINDOW_MINUTES:-15}
LIMIT_DOCS=${LIMIT_DOCS:-1000}
SLEEP_BETWEEN=${SLEEP_BETWEEN:-0.1}
OUT_DIR=${OUT_DIR:-"analysis/submit_cim_15m"}

EMAILS=("atsareg@in2p3.fr" "kostashn@gmail.com")

if [[ -z "${JWT_TOKEN:-}" ]]; then
  echo "JWT_TOKEN is required" >&2
  exit 1
fi

if [[ -z "$END" ]]; then
  END=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
fi

mkdir -p "$OUT_DIR"

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
slugify() { echo "$1" | tr -c 'A-Za-z0-9' '_' | tr '[:upper:]' '[:lower:]'; }

iso_add_minutes() {
  # $1 = ISO8601, $2 = minutes
  date -u -d "$1 + $2 minutes" +"%Y-%m-%dT%H:%M:%SZ"
}

submit_page() {
  local email="$1"
  local win_start="$2"
  local win_end="$3"
  local after_ts="$4"
  local after_id="$5"

  local payload
  if [[ -n "$after_ts" && -n "$after_id" ]]; then
    payload=$(jq -cn --arg email "$email" --arg start "$win_start" --arg end "$win_end" \
      --arg after_ts "$after_ts" --arg after_id "$after_id" --argjson limit "$LIMIT_DOCS" \
      '{publisher_email:$email,start:$start,end:$end,end_inclusive:false,limit_docs:$limit,after_timestamp:$after_ts,after_id:$after_id}')
  else
    payload=$(jq -cn --arg email "$email" --arg start "$win_start" --arg end "$win_end" --argjson limit "$LIMIT_DOCS" \
      '{publisher_email:$email,start:$start,end:$end,end_inclusive:false,limit_docs:$limit}')
  fi

  local tmp_body tmp_hdr status
  tmp_body=$(mktemp)
  tmp_hdr=$(mktemp)

  if ! curl -sS -D "$tmp_hdr" -o "$tmp_body" -X POST "$ENDPOINT" \
      -H "Authorization: Bearer $JWT_TOKEN" \
      -H 'Content-Type: application/json' \
      -d "$payload"; then
    rm -f "$tmp_body" "$tmp_hdr"
    echo "curl failed" >&2
    return 2
  fi

  status=$(awk 'NR==1{print $2}' "$tmp_hdr")
  cat "$tmp_body"

  rm -f "$tmp_body" "$tmp_hdr"
  [[ "$status" == "200" ]] && return 0
  [[ "$status" == "404" ]] && return 10
  return 1
}

process_email() {
  local email="$1"
  local slug
  slug=$(slugify "$email")

  echo "[$(timestamp)] email=$email start=$START end=$END window=${WINDOW_MINUTES}m limit_docs=$LIMIT_DOCS" >&2

  local win_start="$START"
  while [[ "$win_start" < "$END" ]]; do
    local win_end
    win_end=$(iso_add_minutes "$win_start" "$WINDOW_MINUTES")
    if [[ "$win_end" > "$END" ]]; then
      win_end="$END"
    fi

    local after_ts=""
    local after_id=""
    local page=0

    while true; do
      page=$((page+1))
      echo "[$(timestamp)] email=$email window=$win_start..$win_end page=$page after_ts=${after_ts:-<none>}" >&2

      local resp rc
      set +e
      resp=$(submit_page "$email" "$win_start" "$win_end" "$after_ts" "$after_id")
      rc=$?
      set -e

      local base="$OUT_DIR/${slug}_$(echo "$win_start" | tr -cd '0-9T')_page_${page}.json"
      echo "$resp" > "$base"

      if [[ $rc -eq 10 ]]; then
        break
      fi
      if [[ $rc -ne 0 ]]; then
        echo "[$(timestamp)] email=$email failed rc=$rc resp=$(echo "$resp" | head -c 400)" >&2
        break 2
      fi

      echo "$resp" | jq -r '.cim_response.results[].cnr_response.event_id' \
        | awk -v e="$email" '{print e","$0}' \
        >> "$OUT_DIR/event_ids.csv"

      after_ts=$(echo "$resp" | jq -r '.next_after_timestamp // empty')
      after_id=$(echo "$resp" | jq -r '.next_after_id // empty')

      loaded=$(echo "$resp" | jq -r '.docs_loaded // 0')
      if [[ "$loaded" -lt "$LIMIT_DOCS" ]]; then
        break
      fi

      sleep "$SLEEP_BETWEEN"
    done

    win_start="$win_end"
  done
}

for e in "${EMAILS[@]}"; do
  process_email "$e"
done

echo "[$(timestamp)] done. Outputs in $OUT_DIR" >&2
