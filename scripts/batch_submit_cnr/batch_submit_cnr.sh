#!/bin/bash

set -euo pipefail

cd /home/ubuntu/GreenDIGIT-WP6-CIM-KPI-Pipeline

# 0) Export DB env vars for the loader (uses CNR_POSTEGRESQL_* from .env)
set -a; source .env; set +a

LAST_EXPORTED_FILE="scripts/batch_submit_cnr/last_exported.txt"

usage() {
  echo "Usage: $0 [--end-time <ISO8601>]"
  echo "Example: $0 --end-time 2026-03-09T23:59:59Z"
}

END_TIME_ARG=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --end-time)
      if [[ $# -lt 2 ]]; then
        echo "Error: --end-time requires a value" >&2
        usage
        exit 1
      fi
      END_TIME_ARG="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ ! -f "$LAST_EXPORTED_FILE" ]]; then
  echo "Error: missing start marker file: $LAST_EXPORTED_FILE" >&2
  exit 1
fi

START="$(tr -d '[:space:]' < "$LAST_EXPORTED_FILE")"
if [[ -z "$START" ]]; then
  echo "Error: $LAST_EXPORTED_FILE is empty" >&2
  exit 1
fi

# Interpret last watermark as the previous window end (inclusive),
# then advance by 1 second to start the next window without overlap.
# Example: 2026-03-10T23:59:59Z -> 2026-03-11T00:00:00Z
START="$(date -u -d "$START + 1 second" +"%Y-%m-%dT%H:%M:%SZ")"

YESTERDAY_END="$(date -u -d 'yesterday 23:59:59' +"%Y-%m-%dT%H:%M:%SZ")"
if [[ -n "$END_TIME_ARG" ]]; then
  END="$(date -u -d "$END_TIME_ARG" +"%Y-%m-%dT%H:%M:%SZ")"
else
  END="$YESTERDAY_END"
fi

START_EPOCH="$(date -u -d "$START" +%s)"
END_EPOCH="$(date -u -d "$END" +%s)"
YESTERDAY_END_EPOCH="$(date -u -d "$YESTERDAY_END" +%s)"

if [[ "$END_EPOCH" -le "$START_EPOCH" ]]; then
  echo "Error: end-time ($END) must be later than start-time ($START)" >&2
  exit 1
fi

# Safeguard for regular cron mode (no explicit --end-time):
# if watermark is already yesterday (or later), we already exported today's window.
if [[ -z "$END_TIME_ARG" && "$START_EPOCH" -ge "$YESTERDAY_END_EPOCH" ]]; then
  echo "Stop: latest exported watermark ($START) is already yesterday-or-later ($YESTERDAY_END)." >&2
  echo "Stop: refusing second export for the same daily window." >&2
  exit 1
fi

echo "[batch_submit_cnr] START=$START END=$END"

DUMP_ROOT="/~/data"
DUMP_BASE="${DUMP_ROOT}/${START_EPOCH}_${END_EPOCH}_dump"
rm -rf "$DUMP_BASE"
mkdir -p "$DUMP_BASE"
echo "[batch_submit_cnr] DUMP_BASE=$DUMP_BASE"

# Publisher filter (CSV); used in mongoexport and process_dump.
# Default source is submit_emails.txt at repo root (one email per line).
# Env var EMAILS still overrides file-based loading.
EMAILS_FILE_DEFAULT="submit_emails.txt"
EMAILS_FILE_FALLBACK="submit_email.txt"
EMAILS_FILE=""
if [[ -n "${EMAILS:-}" ]]; then
  :
elif [[ -f "$EMAILS_FILE_DEFAULT" ]]; then
  EMAILS_FILE="$EMAILS_FILE_DEFAULT"
elif [[ -f "$EMAILS_FILE_FALLBACK" ]]; then
  EMAILS_FILE="$EMAILS_FILE_FALLBACK"
fi

if [[ -n "${EMAILS:-}" ]]; then
  :
elif [[ -n "$EMAILS_FILE" ]]; then
  EMAILS="$(awk '
    /^[[:space:]]*($|#)/ { next }
    { gsub(/\r/, "", $0); gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0); if ($0 != "") print $0 }
  ' "$EMAILS_FILE" | paste -sd, -)"
else
  echo "Error: EMAILS not set and no $EMAILS_FILE_DEFAULT / $EMAILS_FILE_FALLBACK found." >&2
  exit 1
fi

if [[ -z "$EMAILS" ]]; then
  echo "Error: no publisher emails resolved (EMAILS/file)." >&2
  exit 1
fi

# 1) Dump the current CIM MetricsDB data:
# Export only documents inside [START, END] by timestamp.
# Note: `timestamp` in this collection is stored as a string, not BSON Date.
EMAILS_JSON_ARRAY=$(
  printf '%s' "$EMAILS" | awk -F',' '
    BEGIN { printf "[" }
    {
      for (i=1; i<=NF; i++) {
        gsub(/^[ \t]+|[ \t]+$/, "", $i)
        if ($i != "") {
          if (n++) printf ","
          gsub(/"/, "\\\"", $i)
          printf "\"%s\"", $i
        }
      }
    }
    END { printf "]" }
  '
)

MONGO_QUERY="{\"timestamp\":{\"\$gte\":\"$START\",\"\$lte\":\"$END\"},\"publisher_email\":{\"\$in\":$EMAILS_JSON_ARRAY}}"

# docker compose exec -T metrics-db mongodump --db metricsdb --out /dump
docker compose exec -T metrics-db \
     mongoexport --db metricsdb --collection metrics \
     --query "$MONGO_QUERY" \
     --type=json --out /dump/metrics.jsonl

# Copy it to the local folder, outside docker.
mkdir -p "$DUMP_BASE/01_mongo"
docker cp "$(docker compose ps -q metrics-db):/dump/metrics.jsonl" "$DUMP_BASE/01_mongo/"

# Print min/max timestamp from local mongoexport (top-level `timestamp`).
# ./bin/python ./print_minmax_timestamp.py

mkdir -p "$DUMP_BASE/02_dump_processed/"
# 2) Convert Mongo export -> CNR envelopes JSONL (filtered) (CIM-compatible)
./bin/python ./scripts/batch_submit_cnr/process_dump.py "$DUMP_BASE/01_mongo/metrics.jsonl" \
  --emails "$EMAILS" \
  --out-dir "$DUMP_BASE/02_dump_processed" \
  --cache-granularity-s 86400
  # --disable-kpi-enri≈chment
  # --start $START \
  # --end "$END" \
# Note: we do not need this normally, as the timestamp filtering is done in the mongoexport already.

# 2) One-time: install Postgres driver in this repo venv
source bin/activate
pip install -q psycopg2-binary==2.9.10

# 3) Direct-load envelopes into CNR Postgres (no HTTP)
python3 scripts/batch_submit_cnr/load_envelopes_direct_cnr.py \
  "$DUMP_BASE"/02_dump_processed/envelopes_*.jsonl \
  --batch-size 5000

# # Check number of entries from the raw data
# wc -l $DUMP_BASE/01_mongo/metrics.jsonl
# wc -l $DUMP_BASE/02_dump_processed/envelopes_*.jsonl

# # Check PostgreSQL entries (see if it matches)
# PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
# psql -h "$CNR_HOST" -p 5432 -U "$CNR_USER" -d "$CNR_GD_DB" -c "
# SELECT count(*) FROM monitoring.fact_site_event;
# "

# Persist watermark for next cron run only after successful completion.
printf "%s\n" "$END" > "$LAST_EXPORTED_FILE"
echo "[batch_submit_cnr] Updated $LAST_EXPORTED_FILE to $END"

# Aggregating materialised values in CNR SQL
./scripts/pre_aggregate_sql.sh
