#!/usr/bin/env bash
set -euo pipefail

set -a
source .env
set +a

EXCLUDE_SITES_FILE="_sql_cnr/exclude_sites"
EXCLUDE_VOS_FILE="_sql_cnr/exclude_vos"
PUBLIC_DASHBOARD_SQL="_test_sql/public_dashboard_views.sql"
PUBLIC_ONLY="${PREAGG_PUBLIC_ONLY:-false}"

usage() {
  cat <<'EOF'
Usage: scripts/pre_aggregate_sql.sh [--public-only]

Options:
  --public-only    Refresh only the public dashboard materialized views and grants.

Environment:
  PREAGG_PUBLIC_ONLY=true  Same as --public-only.
EOF
}

for arg in "$@"; do
  case "$arg" in
    --public-only)
      PUBLIC_ONLY=true
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      usage >&2
      exit 2
      ;;
  esac
done

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

refresh_public_dashboard_views() {
  if ! PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
    psql "${PSQL_COMMON_ARGS[@]}" -Atc \
      "SELECT to_regclass('monitoring.mv_public_dashboard_resource_selection') IS NOT NULL;" \
    | grep -qx "t"; then
    echo "Public dashboard views are missing; creating them from $PUBLIC_DASHBOARD_SQL"
    PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
    psql "${PSQL_COMMON_ARGS[@]}" \
      -f "$PUBLIC_DASHBOARD_SQL"
  fi

  PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
  psql "${PSQL_COMMON_ARGS[@]}" \
    -c "REFRESH MATERIALIZED VIEW monitoring.mv_public_dashboard_resource_selection;"

  PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
  psql "${PSQL_COMMON_ARGS[@]}" \
    -c "REFRESH MATERIALIZED VIEW monitoring.mv_public_dashboard_15m;"

  PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
  psql "${PSQL_COMMON_ARGS[@]}" \
    -c "REFRESH MATERIALIZED VIEW monitoring.mv_public_dashboard_resource_listing;"

  if [[ -n "${CNR_PUBLIC_USER:-}" ]]; then
    PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
    psql "${PSQL_COMMON_ARGS[@]}" -v public_user="$CNR_PUBLIC_USER" <<'SQL'
SELECT format('GRANT USAGE ON SCHEMA monitoring TO %I', :'public_user')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'public_user') \gexec
SELECT format('GRANT SELECT ON monitoring.v_public_dashboard_15m TO %I', :'public_user')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'public_user') \gexec
SELECT format('GRANT SELECT ON monitoring.v_public_dashboard_resource_listing TO %I', :'public_user')
WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'public_user') \gexec
SQL
  fi
}

if [[ "$PUBLIC_ONLY" == "true" ]]; then
  refresh_public_dashboard_views
  exit 0
fi

# Ensure the materialized view exists (idempotent).
PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
psql "${PSQL_COMMON_ARGS[@]}" \
  -f _test_sql/preaggregate_15m.sql

PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
psql "${PSQL_COMMON_ARGS[@]}" <<'SQL'
TRUNCATE TABLE monitoring.reporting_excluded_sites;
TRUNCATE TABLE monitoring.reporting_excluded_vos;
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

if [[ -f "$EXCLUDE_VOS_FILE" ]]; then
  while IFS= read -r raw_vo; do
    vo="${raw_vo#"${raw_vo%%[![:space:]]*}"}"
    vo="${vo%"${vo##*[![:space:]]}"}"
    [[ -z "$vo" || "${vo:0:1}" == "#" ]] && continue

    escaped_vo=${vo//\'/\'\'}
    PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
    psql "${PSQL_COMMON_ARGS[@]}" \
      -c "INSERT INTO monitoring.reporting_excluded_vos (vo) VALUES ('$escaped_vo') ON CONFLICT (vo) DO NOTHING;"
  done < "$EXCLUDE_VOS_FILE"
fi

# Optional explicit refresh (default: skip, because rebuild already populated data).
# Set PREAGG_REFRESH_AFTER_REBUILD=true to force refresh.
if [[ "${PREAGG_REFRESH_AFTER_REBUILD:-false}" == "true" ]]; then
  PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
  psql "${PSQL_COMMON_ARGS[@]}" \
    -c "REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.mv_fact_site_event_15m_base;"
fi

PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
psql "${PSQL_COMMON_ARGS[@]}" \
  -c "REFRESH MATERIALIZED VIEW monitoring.mv_reporting_resource_listing;"

refresh_public_dashboard_views
