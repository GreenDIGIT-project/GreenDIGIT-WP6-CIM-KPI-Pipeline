#!/bin/bash

cd /home/ubuntu/GreenDIGIT-WP6-CIM-KPI-Pipeline

# 0) Export DB env vars for the loader (uses CNR_POSTEGRESQL_* from .env)
set -a; source .env; set +a

# 1) Convert Mongo export -> CNR envelopes JSONL (filtered)
END="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
./bin/python process_dump.py dump/mongo/metrics.jsonl \
  --emails 'atsareg@in2p3.fr,kostashn@gmail.com' \
  --start '2025-08-01T00:00:00Z' \
  --end "$END" \
  --out-dir dump/sql_cnr

# 2) One-time: install Postgres driver in this repo venv
source bin/activate
pip install -q psycopg2-binary==2.9.10

# 3) Direct-load envelopes into CNR Postgres (no HTTP)
python3 scripts/load-envelopes-direct-to-cnr.py \
  analysis/dump_processed/envelopes_atsareg_in2p3_fr.jsonl \
  analysis/dump_processed/envelopes_kostashn_gmail_com.jsonl \
  --batch-size 5000

# ROOT_DIR=$(pwd)
# ENV_FILE="$ROOT_DIR/.env"

# if [ -f "$ENV_FILE" ]; then
#   # shellcheck disable=SC1091
#   source "$ENV_FILE"
# else
#   echo "Missing .env at $ENV_FILE" >&2
#   exit 1
# fi

# : "${CNR_POSTGRESQL_PASSWORD:?CNR_POSTGRESQL_PASSWORD must be set in .env}"
# : "${CNR_HOST:?CNR_HOST must be set in .env}"
# : "${CNR_USER:?CNR_USER must be set in .env}"
# : "${CNR_GD_DB:?CNR_GD_DB must be set in .env}"

# # 1. Dump the current CIM MetricsDB data:

# # docker compose exec -it metrics-db mongodump --db metricsdb --out /dump
# docker compose exec -it metrics-db \
#      mongoexport --db metricsdb --collection metrics \
#      --type=json --out /dump/metrics.jsonl

# # Copy it to the local folder, outside docker.
# mkdir -p dump/mongo # in case you haven't yet.
# mkdir -p dump/sql_cnr
# docker cp $(docker compose ps -q metrics-db):/dump/metrics.jsonl ./dump/mongo/


# python3 process_dump.py dump/mongo/metrics.jsonl \
#   --emails 'atsareg@in2p3.fr,kostashn@gmail.com' \
#   --start '2025-08-01T00:00:00Z' \
#   --end   "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
#   --out-dir dump/sql_cnr/

# # Script to batch submit the metrics to SoBigData MetricsDB
# PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" psql -h $CNR_HOST -p 5432 -U $CNR_USER -d $CNR_GD_DB