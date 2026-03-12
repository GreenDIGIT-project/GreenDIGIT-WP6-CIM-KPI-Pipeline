#!/usr/bin/env bash
set -euo pipefail

set -a
source .env
set +a

# Rebuild the materialized view schema/data.
# CREATE MATERIALIZED VIEW ... AS SELECT already populates data,
# so an immediate refresh is optional.
PSQL_COMMON_ARGS=(
  -h "$CNR_HOST"
  -p 5432
  -U "$CNR_USER"
  -d "$CNR_GD_DB"
  -v ON_ERROR_STOP=1
)

# Ensure the materialized view exists (idempotent).
PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
psql "${PSQL_COMMON_ARGS[@]}" \
  -f _test_sql/preaggregate_15m.sql

# Optional explicit refresh (default: skip, because rebuild already populated data).
# Set PREAGG_REFRESH_AFTER_REBUILD=true to force refresh.
if [[ "${PREAGG_REFRESH_AFTER_REBUILD:-false}" == "true" ]]; then
  PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
  psql "${PSQL_COMMON_ARGS[@]}" \
    -c "REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.mv_fact_site_event_15m;"
fi
