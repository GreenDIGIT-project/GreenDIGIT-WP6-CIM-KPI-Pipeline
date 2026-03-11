#!/usr/bin/env bash
set -euo pipefail

set -a
source .env
set +a

# Ensure the materialized view exists (idempotent).
PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
psql -h "$CNR_HOST" -p 5432 -U "$CNR_USER" -d "$CNR_GD_DB" \
  -f _test_sql/preaggregate_15m.sql

# Refresh materialized data after updates.
PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
psql -h "$CNR_HOST" -p 5432 -U "$CNR_USER" -d "$CNR_GD_DB" \
  -c "REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.mv_fact_site_event_15m;"
