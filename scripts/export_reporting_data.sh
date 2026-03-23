#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${REPO_ROOT}/analysis/reporting_export"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--output-dir DIR]" >&2
      exit 1
      ;;
  esac
done

if [[ ! -f "${REPO_ROOT}/.env" ]]; then
  echo "Missing ${REPO_ROOT}/.env" >&2
  exit 1
fi

set -a
source "${REPO_ROOT}/.env"
set +a

mkdir -p "${OUTPUT_DIR}"

PSQL_COMMON_ARGS=(
  -h "${CNR_HOST}"
  -p 5432
  -U "${CNR_USER}"
  -d "${CNR_GD_DB}"
  -v ON_ERROR_STOP=1
)

export PGPASSWORD="${CNR_POSTEGRESQL_PASSWORD}"

RESOURCE_CSV="${OUTPUT_DIR}/resource_listing.csv"
RECORD_CSV="${OUTPUT_DIR}/record_listing.csv"
SUMMARY_JSON="${OUTPUT_DIR}/reporting_summary.json"

psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${RESOURCE_CSV}"
COPY (
  SELECT
    r.vo,
    r.activity,
    r.site,
    r.country,
    r.active_since,
    r.last_seen,
    r.span_months,
    r.continuity_pct,
    r.metrics_reported,
    r.total_records,
    r.energy_wh,
    r.cfp_g,
    r.volume_of_data_bytes,
    r.source_db_presence
  FROM monitoring.v_reporting_resource_listing r
  ORDER BY r.energy_wh DESC, r.total_records DESC, r.vo, r.site
) TO STDOUT WITH (FORMAT CSV, HEADER true)
SQL

psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${RECORD_CSV}"
COPY (
  SELECT
    r.time_bucket,
    r.vo,
    r.activity,
    r.site,
    r.records,
    r.energy_wh,
    r.cfp_g,
    r.work,
    r.ncores
  FROM monitoring.v_reporting_record_listing r
  ORDER BY r.time_bucket DESC, r.energy_wh DESC, r.vo, r.site
) TO STDOUT WITH (FORMAT CSV, HEADER true)
SQL

psql "${PSQL_COMMON_ARGS[@]}" -t -A <<SQL > "${SUMMARY_JSON}"
SELECT jsonb_pretty(
  jsonb_build_object(
    'exported_at_utc', to_char(timezone('UTC', now()), 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    'source_views', jsonb_build_array(
      'monitoring.v_reporting_resource_listing',
      'monitoring.v_reporting_record_listing'
    ),
    'files', jsonb_build_object(
      'resource_listing_csv', '${RESOURCE_CSV}',
      'record_listing_csv', '${RECORD_CSV}'
    ),
    'row_counts', jsonb_build_object(
      'resource_listing', (SELECT COUNT(*) FROM monitoring.v_reporting_resource_listing),
      'record_listing', (SELECT COUNT(*) FROM monitoring.v_reporting_record_listing)
    )
  )
);
SQL

echo "Export complete:"
echo "  - ${RESOURCE_CSV}"
echo "  - ${RECORD_CSV}"
echo "  - ${SUMMARY_JSON}"
