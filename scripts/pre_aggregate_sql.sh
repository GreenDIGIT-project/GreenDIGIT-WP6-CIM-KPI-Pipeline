#!/usr/bin/env bash
set -euo pipefail

set -a
source .env
set +a

EXCLUDE_SITES_FILE="_sql_cnr/exclude_sites"

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

PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
psql "${PSQL_COMMON_ARGS[@]}" <<'SQL'
TRUNCATE TABLE monitoring.reporting_excluded_sites;
SQL

if [[ -f "$EXCLUDE_SITES_FILE" ]]; then
  while IFS= read -r raw_site; do
    site="${raw_site#"${raw_site%%[![:space:]]*}"}"
    site="${site%"${site##*[![:space:]]}"}"
    [[ -z "$site" || "${site:0:1}" == "#" ]] && continue

    escaped_site=${site//\'/\'\'}
    PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
    psql "${PSQL_COMMON_ARGS[@]}" \
      -c "INSERT INTO monitoring.reporting_excluded_sites (site) VALUES ('$escaped_site') ON CONFLICT (site) DO NOTHING;"
  done < "$EXCLUDE_SITES_FILE"
fi

# Optional explicit refresh (default: skip, because rebuild already populated data).
# Set PREAGG_REFRESH_AFTER_REBUILD=true to force refresh.
if [[ "${PREAGG_REFRESH_AFTER_REBUILD:-false}" == "true" ]]; then
  PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
  psql "${PSQL_COMMON_ARGS[@]}" \
    -c "REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.mv_fact_site_event_15m_base;"
fi
