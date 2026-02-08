#!/usr/bin/env bash
set -euo pipefail

# RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
# OUT_DIR=analysis/submit_cim
# mkdir -p "$OUT_DIR"

# nohup env JWT_TOKEN="$JWT_TOKEN" \
#   START='2025-08-01T00:00:00Z' \
#   END="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
#   LIMIT_DOCS=500 \
#   OUT_DIR="$OUT_DIR" \
#   scripts/batch-submit-cim.sh \
#   > "$OUT_DIR/run_${RUN_ID}.log" 2>&1 &

# echo $! > "$OUT_DIR/run_${RUN_ID}.pid"
# echo "$RUN_ID" > "$OUT_DIR/run_latest.txt"

# kill "$(cat analysis/submit_cim/run.pid)"
# pkill -f 'scripts/batch-submit-cim\.sh' || true


# Batch replay of stored partner payloads through AuthServer -> CIM -> SQL adapter.
#
# Requirements:
# - JWT token in env var JWT_TOKEN
# - jq installed (for parsing responses)
#
# Defaults:
# - START: 2025-08-01T00:00:00Z
# - END: now (UTC)
# - LIMIT_DOCS: 500
#
# Resume:
# - Set RESUME=1 (default) to continue from cursor files in OUT_DIR.

AUTH_BASE=${AUTH_BASE:-"http://localhost:8000/gd-cim-api/v1"}
ENDPOINT="$AUTH_BASE/submit-cim"

START=${START:-"2025-08-01T00:00:00Z"}
END=${END:-""}
LIMIT_DOCS=${LIMIT_DOCS:-1000}
SLEEP_BETWEEN=${SLEEP_BETWEEN:-0.2}
OUT_DIR=${OUT_DIR:-"analysis/submit_cim"}
RESUME=${RESUME:-1}

if [[ -z "${JWT_TOKEN:-}" ]]; then
  echo "JWT_TOKEN is required" >&2
  exit 1
fi

if [[ -z "${END}" ]]; then
  END=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
fi

mkdir -p "$OUT_DIR"

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

slugify() {
  echo "$1" | tr -c 'A-Za-z0-9' '_' | tr '[:upper:]' '[:lower:]'
}

submit_page() {
  local email="$1"
  local after_ts="$2"
  local after_id="$3"

  local payload
  if [[ -n "$after_ts" && -n "$after_id" ]]; then
    payload=$(jq -cn --arg email "$email" --arg start "$START" --arg end "$END" --arg after_ts "$after_ts" --arg after_id "$after_id" --argjson limit "$LIMIT_DOCS" \
      '{publisher_email:$email,start:$start,end:$end,limit_docs:$limit,after_timestamp:$after_ts,after_id:$after_id}')
  elif [[ -n "$after_ts" ]]; then
    payload=$(jq -cn --arg email "$email" --arg start "$START" --arg end "$END" --arg after_ts "$after_ts" --argjson limit "$LIMIT_DOCS" \
      '{publisher_email:$email,start:$start,end:$end,limit_docs:$limit,after_timestamp:$after_ts}')
  else
    payload=$(jq -cn --arg email "$email" --arg start "$START" --arg end "$END" --argjson limit "$LIMIT_DOCS" \
      '{publisher_email:$email,start:$start,end:$end,limit_docs:$limit}')
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

  echo "[$(timestamp)] email=$email start=$START end=$END limit_docs=$LIMIT_DOCS" >&2

  local cursor_file="$OUT_DIR/${slug}.cursor.json"
  local after_ts=""
  local after_id=""
  if [[ "$RESUME" == "1" && -f "$cursor_file" ]]; then
    after_ts=$(jq -r '.after_ts // empty' "$cursor_file" 2>/dev/null || true)
    after_id=$(jq -r '.after_id // empty' "$cursor_file" 2>/dev/null || true)
    if [[ -n "$after_ts" ]]; then
      echo "[$(timestamp)] email=$email resuming after_ts=$after_ts after_id=${after_id:-<none>}" >&2
    fi
  fi

  local page=0
  while true; do
    page=$((page+1))
    echo "[$(timestamp)] email=$email page=$page after_ts=${after_ts:-<none>} after_id=${after_id:-<none>}" >&2

    local resp rc
    set +e
    resp=$(submit_page "$email" "$after_ts" "$after_id")
    rc=$?
    set -e

    echo "$resp" > "$OUT_DIR/${slug}_page_${page}.json"

    if [[ $rc -eq 10 ]]; then
      echo "[$(timestamp)] email=$email done (no more docs in window)" >&2
      break
    fi
    if [[ $rc -ne 0 ]]; then
      echo "[$(timestamp)] email=$email failed rc=$rc resp=$(echo "$resp" | head -c 400)" >&2
      break
    fi

    # Append event ids for quick CNR lookups
    echo "$resp" | jq -r '.cim_response.results[].cnr_response.event_id' \
      | awk -v e="$email" '{print e","$0}' \
      >> "$OUT_DIR/event_ids.csv"

    # Update cursor (persist for resume)
    after_ts=$(echo "$resp" | jq -r '.next_after_timestamp // empty')
    after_id=$(echo "$resp" | jq -r '.next_after_id // empty')
    jq -cn --arg after_ts "$after_ts" --arg after_id "$after_id" --arg updated_at "$(timestamp)" \
      '{after_ts:$after_ts,after_id:$after_id,updated_at:$updated_at}' > "$cursor_file"

    loaded=$(echo "$resp" | jq -r '.docs_loaded // 0')
    if [[ "$loaded" -lt "$LIMIT_DOCS" ]]; then
      echo "[$(timestamp)] email=$email done (last page loaded=$loaded)" >&2
      break
    fi

    sleep "$SLEEP_BETWEEN"
  done
}

# Emails to process (current request)
process_email "atsareg@in2p3.fr"
# process_email "kostashn@gmail.com"

echo "[$(timestamp)] done. Outputs in $OUT_DIR" >&2
