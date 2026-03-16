#!/usr/bin/env bash
set -euo pipefail

# Export SARA-MATRIX (grid) data from the 15-minute materialized view.
# Usage:
#   ./ecml-pkdd/download_sara_matrix_data.sh [--output-dir DIR] [--from TS] [--to TS] [--anonymize true|false] [--anon-salt SALT]
# Example:
#   ./ecml-pkdd/download_sara_matrix_data.sh --from "2025-01-01 00:00:00" --to "2026-03-12 00:00:00"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/data"
FROM_TS=""
TO_TS=""
ANONYMIZE="true"
ANON_SALT="${ANON_SALT:-$RANDOM-$RANDOM-$(date +%s%N)}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --from)
      FROM_TS="$2"
      shift 2
      ;;
    --to)
      TO_TS="$2"
      shift 2
      ;;
    --anonymize)
      ANONYMIZE="$2"
      shift 2
      ;;
    --anon-salt)
      ANON_SALT="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

ANONYMIZE="$(echo "${ANONYMIZE}" | tr '[:upper:]' '[:lower:]')"
if [[ "${ANONYMIZE}" != "true" && "${ANONYMIZE}" != "false" ]]; then
  echo "--anonymize must be true or false (got: ${ANONYMIZE})" >&2
  exit 1
fi

if [[ ! -f "${REPO_ROOT}/.env" ]]; then
  echo "Missing ${REPO_ROOT}/.env" >&2
  exit 1
fi

set -a
source "${REPO_ROOT}/.env"
set +a

mkdir -p "${OUTPUT_DIR}"

COND_FROM=""
COND_TO=""
if [[ -n "${FROM_TS}" ]]; then
  COND_FROM="AND m.bucket_15m >= '${FROM_TS}'::timestamp"
fi
if [[ -n "${TO_TS}" ]]; then
  COND_TO="AND m.bucket_15m <= '${TO_TS}'::timestamp"
fi

BASE_WHERE="
  WHERE m.activity = 'grid'
    AND m.site = 'SARA-MATRIX'
    ${COND_FROM}
    ${COND_TO}
"

PSQL_COMMON_ARGS=(
  -h "${CNR_HOST}"
  -p 5432
  -U "${CNR_USER}"
  -d "${CNR_GD_DB}"
  -v ON_ERROR_STOP=1
)

export PGPASSWORD="${CNR_POSTEGRESQL_PASSWORD}"

CSV_15M="${OUTPUT_DIR}/sara_matrix_15m.csv"
CSV_1H="${OUTPUT_DIR}/sara_matrix_hourly.csv"
CSV_1D="${OUTPUT_DIR}/sara_matrix_daily.csv"
META_JSON="${OUTPUT_DIR}/sara_matrix_metadata.json"

if [[ "${ANONYMIZE}" == "true" ]]; then
  psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${CSV_15M}"
COPY (
  SELECT
    m.bucket_15m,
    'site_' || SUBSTRING(md5('${ANON_SALT}' || m.site_id::text) FROM 1 FOR 10) AS site_id,
    'vo_' || SUBSTRING(md5('${ANON_SALT}' || COALESCE(m.vo,'Unknown')) FROM 1 FOR 10) AS vo,
    m.activity,
    m.activity || '_site_' || SUBSTRING(md5('${ANON_SALT}' || m.site) FROM 1 FOR 10) AS site,
    m.jobs AS records,
    m.energy_wh,
    m.cfp_g,
    m.work,
    m.ncores
  FROM monitoring.mv_fact_site_event_15m m
  ${BASE_WHERE}
  ORDER BY m.bucket_15m ASC, vo ASC, site_id ASC
) TO STDOUT WITH (FORMAT CSV, HEADER true)
SQL
else
  psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${CSV_15M}"
COPY (
  SELECT
    m.bucket_15m,
    m.site_id,
    m.vo,
    m.activity,
    m.site,
    m.jobs AS records,
    m.energy_wh,
    m.cfp_g,
    m.work,
    m.ncores
  FROM monitoring.mv_fact_site_event_15m m
  ${BASE_WHERE}
  ORDER BY m.bucket_15m ASC, m.vo ASC, m.site_id ASC
) TO STDOUT WITH (FORMAT CSV, HEADER true)
SQL
fi

if [[ "${ANONYMIZE}" == "true" ]]; then
  psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${CSV_1H}"
COPY (
  SELECT
    date_trunc('hour', m.bucket_15m) AS bucket_1h,
    m.activity || '_site_' || SUBSTRING(md5('${ANON_SALT}' || m.site) FROM 1 FOR 10) AS site,
    SUM(COALESCE(m.jobs,0)) AS records,
    SUM(COALESCE(m.energy_wh,0)) AS energy_wh,
    SUM(COALESCE(m.cfp_g,0)) AS cfp_g,
    SUM(COALESCE(m.work,0)) AS work,
    SUM(COALESCE(m.ncores,0)) AS ncores,
    ROUND(
      (SUM(COALESCE(m.ncores,0))::numeric / NULLIF(SUM(COALESCE(m.jobs,0)),0)),
      6
    ) AS ncores_per_record
  FROM monitoring.mv_fact_site_event_15m m
  ${BASE_WHERE}
  GROUP BY 1,2
  ORDER BY 1,2
) TO STDOUT WITH (FORMAT CSV, HEADER true)
SQL
else
  psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${CSV_1H}"
COPY (
  SELECT
    date_trunc('hour', m.bucket_15m) AS bucket_1h,
    m.site,
    SUM(COALESCE(m.jobs,0)) AS records,
    SUM(COALESCE(m.energy_wh,0)) AS energy_wh,
    SUM(COALESCE(m.cfp_g,0)) AS cfp_g,
    SUM(COALESCE(m.work,0)) AS work,
    SUM(COALESCE(m.ncores,0)) AS ncores,
    ROUND(
      (SUM(COALESCE(m.ncores,0))::numeric / NULLIF(SUM(COALESCE(m.jobs,0)),0)),
      6
    ) AS ncores_per_record
  FROM monitoring.mv_fact_site_event_15m m
  ${BASE_WHERE}
  GROUP BY 1,2
  ORDER BY 1,2
) TO STDOUT WITH (FORMAT CSV, HEADER true)
SQL
fi

if [[ "${ANONYMIZE}" == "true" ]]; then
  psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${CSV_1D}"
COPY (
  SELECT
    date_trunc('day', m.bucket_15m) AS bucket_1d,
    m.activity || '_site_' || SUBSTRING(md5('${ANON_SALT}' || m.site) FROM 1 FOR 10) AS site,
    SUM(COALESCE(m.jobs,0)) AS records,
    SUM(COALESCE(m.energy_wh,0)) AS energy_wh,
    SUM(COALESCE(m.cfp_g,0)) AS cfp_g,
    SUM(COALESCE(m.work,0)) AS work,
    SUM(COALESCE(m.ncores,0)) AS ncores,
    ROUND(
      (SUM(COALESCE(m.ncores,0))::numeric / NULLIF(SUM(COALESCE(m.jobs,0)),0)),
      6
    ) AS ncores_per_record
  FROM monitoring.mv_fact_site_event_15m m
  ${BASE_WHERE}
  GROUP BY 1,2
  ORDER BY 1,2
) TO STDOUT WITH (FORMAT CSV, HEADER true)
SQL
else
  psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${CSV_1D}"
COPY (
  SELECT
    date_trunc('day', m.bucket_15m) AS bucket_1d,
    m.site,
    SUM(COALESCE(m.jobs,0)) AS records,
    SUM(COALESCE(m.energy_wh,0)) AS energy_wh,
    SUM(COALESCE(m.cfp_g,0)) AS cfp_g,
    SUM(COALESCE(m.work,0)) AS work,
    SUM(COALESCE(m.ncores,0)) AS ncores,
    ROUND(
      (SUM(COALESCE(m.ncores,0))::numeric / NULLIF(SUM(COALESCE(m.jobs,0)),0)),
      6
    ) AS ncores_per_record
  FROM monitoring.mv_fact_site_event_15m m
  ${BASE_WHERE}
  GROUP BY 1,2
  ORDER BY 1,2
) TO STDOUT WITH (FORMAT CSV, HEADER true)
SQL
fi

psql "${PSQL_COMMON_ARGS[@]}" -t -A <<SQL > "${META_JSON}"
SELECT jsonb_pretty(
  jsonb_build_object(
    'site', 'SARA-MATRIX',
    'site_anonymized', 'grid_site_' || SUBSTRING(md5('${ANON_SALT}' || 'SARA-MATRIX') FROM 1 FOR 10),
    'activity', 'grid',
    'source', 'monitoring.mv_fact_site_event_15m',
    'anonymization', jsonb_build_object(
      'enabled', ${ANONYMIZE},
      'vo_rule', 'vo_<first10(md5(salt||vo))>',
      'site_rule', '<activity>_site_<first10(md5(salt||site))>',
      'site_id_rule', 'site_<first10(md5(salt||site_id))>'
    ),
    'filters', jsonb_build_object(
      'from', NULLIF('${FROM_TS}',''),
      'to', NULLIF('${TO_TS}','')
    ),
    'files', jsonb_build_object(
      'dataset_15m_csv', '${CSV_15M}',
      'dataset_hourly_csv', '${CSV_1H}',
      'dataset_daily_csv', '${CSV_1D}'
    ),
    'stats', (
      SELECT jsonb_build_object(
        'records_15m', COUNT(*),
        'start_timestamp', MIN(m.bucket_15m),
        'end_timestamp', MAX(m.bucket_15m),
        'total_records', SUM(COALESCE(m.jobs,0)),
        'sum_energy_wh', SUM(COALESCE(m.energy_wh,0)),
        'sum_cfp_g', SUM(COALESCE(m.cfp_g,0)),
        'sum_work', SUM(COALESCE(m.work,0)),
        'sum_ncores', SUM(COALESCE(m.ncores,0))
      )
      FROM monitoring.mv_fact_site_event_15m m
      ${BASE_WHERE}
    )
  )
);
SQL

echo "Export complete:"
echo "  - ${CSV_15M}"
echo "  - ${CSV_1H}"
echo "  - ${CSV_1D}"
echo "  - ${META_JSON}"
