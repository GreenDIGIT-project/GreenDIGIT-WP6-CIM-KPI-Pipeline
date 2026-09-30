#!/usr/bin/env bash
set -euo pipefail

set -a
source .env
set +a

EXCLUDE_SITES_FILE="_sql_cnr/exclude_sites"
EXCLUDE_VOS_FILE="_sql_cnr/exclude_vos"
PUBLIC_DASHBOARD_SQL="_test_sql/public_dashboard_views.sql"
PUBLIC_ONLY="${PREAGG_PUBLIC_ONLY:-false}"
BATCH_SIZE="${PREAGG_BATCH_SIZE:-10000}"

usage() {
  cat <<'EOF'
Usage: scripts/pre_aggregate_sql.sh [--public-only] [--batch-size ROWS]

Options:
  --public-only    Refresh only the public dashboard materialized views and grants.
  --batch-size     Source events per committed rebuild batch (default: 10000).

Environment:
  PREAGG_PUBLIC_ONLY=true  Same as --public-only.
  PREAGG_BATCH_SIZE=10000  Default batch size when --batch-size is omitted.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --public-only)
      PUBLIC_ONLY=true
      shift
      ;;
    --batch-size)
      if [[ $# -lt 2 ]]; then
        echo "--batch-size requires a value" >&2
        exit 2
      fi
      BATCH_SIZE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! "$BATCH_SIZE" =~ ^[1-9][0-9]*$ ]]; then
  echo "Batch size must be a positive integer: $BATCH_SIZE" >&2
  exit 2
fi

# Keep both sides of long SSL connections active. Each data batch should be
# short, but final index/view creation can still run for a while.
export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-15}"
export PGKEEPALIVES="${PGKEEPALIVES:-1}"
export PGKEEPALIVESIDLE="${PGKEEPALIVESIDLE:-60}"
export PGKEEPALIVESINTERVAL="${PGKEEPALIVESINTERVAL:-30}"
export PGKEEPALIVESCOUNT="${PGKEEPALIVESCOUNT:-10}"
export PGAPPNAME="${PGAPPNAME:-greendigit-preaggregate}"

# Prevent a manual run from corrupting the staging table used by the cron run.
PREAGG_LOCK_FILE="${PREAGG_LOCK_FILE:-/tmp/greendigit-preaggregate.lock}"
exec 9>"$PREAGG_LOCK_FILE"
if ! flock -n 9; then
  echo "Another pre-aggregation run is already active ($PREAGG_LOCK_FILE)" >&2
  exit 1
fi

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

# Build partial aggregates in isolated, committed event-id batches. The
# currently published dashboard objects remain untouched during this phase.
PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
psql "${PSQL_COMMON_ARGS[@]}" \
  -f _test_sql/preaggregate_15m_prepare.sql

snapshot_bounds="$({
  PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
  psql "${PSQL_COMMON_ARGS[@]}" -At -F '|' -c \
    "SELECT COALESCE(MIN(event_id)::bigint - 1, 0), COALESCE(MAX(event_id)::bigint, 0) FROM monitoring.fact_site_event;"
})"
IFS='|' read -r batch_after snapshot_max <<< "$snapshot_bounds"

if [[ ! "$batch_after" =~ ^-?[0-9]+$ || ! "$snapshot_max" =~ ^[0-9]+$ ]]; then
  echo "Invalid event-id bounds returned by PostgreSQL: $snapshot_bounds" >&2
  exit 1
fi

echo "[preaggregate] snapshot event_id range: $((batch_after + 1))..$snapshot_max; batch_size=$BATCH_SIZE"

while (( batch_after < snapshot_max )); do
  batch_through="$({
    PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
    psql "${PSQL_COMMON_ARGS[@]}" \
      -v batch_after="$batch_after" \
      -v snapshot_max="$snapshot_max" \
      -v batch_size="$BATCH_SIZE" \
      -At <<'SQL'
SELECT COALESCE(MAX(event_id), :batch_after::bigint)
FROM (
  SELECT event_id
  FROM monitoring.fact_site_event
  WHERE event_id > :batch_after::bigint
    AND event_id <= :snapshot_max::bigint
  ORDER BY event_id
  LIMIT :batch_size::integer
) batch_ids;
SQL
  })"

  if [[ -z "$batch_through" || "$batch_through" -le "$batch_after" ]]; then
    echo "Unable to advance pre-aggregation after event_id=$batch_after" >&2
    exit 1
  fi

  echo "[preaggregate] aggregating event_id ($batch_after, $batch_through]"
  PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
  psql "${PSQL_COMMON_ARGS[@]}" \
    -v batch_after="$batch_after" \
    -v batch_through="$batch_through" \
    -f _test_sql/preaggregate_15m_batch.sql

  batch_after="$batch_through"
done

PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
psql "${PSQL_COMMON_ARGS[@]}" \
  -c "ANALYZE monitoring.fact_site_event_15m_build;"

echo "[preaggregate] batches complete; building replacement objects and swapping atomically"
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

PGPASSWORD="$CNR_POSTEGRESQL_PASSWORD" \
psql "${PSQL_COMMON_ARGS[@]}" \
  -c "REFRESH MATERIALIZED VIEW monitoring.mv_reporting_resource_listing;"

refresh_public_dashboard_views
