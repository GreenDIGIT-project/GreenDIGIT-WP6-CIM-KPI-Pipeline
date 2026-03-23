#!/usr/bin/env bash
set -euo pipefail

# Export summary-site grid data from the 15-minute materialized view.
# Usage:
#   ./ecml-pkdd/download_summary_data.sh [--output-dir DIR] [--from TS] [--to TS] [--site SITE] [--sites "SITE_A,SITE_B"] [--anonymize true|false] [--anon-salt SALT]
# Example:
#   ./ecml-pkdd/download_summary_data.sh --from "2025-01-01 00:00:00" --to "2026-03-12 00:00:00"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LAST_EXPORTED_FILE="${REPO_ROOT}/scripts/batch_submit_cnr/last_exported.txt"
ANON_SALT_FILE="${REPO_ROOT}/ecml-pkdd/data/.dirac_anon_salt"
OUTPUT_DIR="${SCRIPT_DIR}/data"
FROM_TS="2025-11-19 00:00:00"
TO_TS=""
SITE_FILTERS=()
ANONYMIZE="true"
ANON_SALT="${ANON_SALT:-}"

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
    --site)
      SITE_FILTERS+=("$2")
      shift 2
      ;;
    --sites)
      IFS=',' read -r -a SITE_FILTERS <<< "$2"
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

if [[ ! -f "${LAST_EXPORTED_FILE}" ]]; then
  echo "Missing ${LAST_EXPORTED_FILE}" >&2
  exit 1
fi

if [[ -z "${TO_TS}" ]]; then
  TO_TS="$(tr -d '[:space:]' < "${LAST_EXPORTED_FILE}")"
  if [[ -z "${TO_TS}" ]]; then
    echo "${LAST_EXPORTED_FILE} is empty" >&2
    exit 1
  fi
  TO_TS="${TO_TS%Z}"
  TO_TS="${TO_TS/T/ }"
fi

set -a
source "${REPO_ROOT}/.env"
set +a

if [[ -z "${ANON_SALT}" && -n "${DIRAC_ANON_SALT:-}" ]]; then
  ANON_SALT="${DIRAC_ANON_SALT}"
fi

if [[ -z "${ANON_SALT}" && -f "${ANON_SALT_FILE}" ]]; then
  ANON_SALT="$(tr -d '[:space:]' < "${ANON_SALT_FILE}")"
fi

if [[ -z "${ANON_SALT}" ]]; then
  mkdir -p "$(dirname "${ANON_SALT_FILE}")"
  ANON_SALT="$(date +%s%N)-$RANDOM-$RANDOM"
  printf "%s\n" "${ANON_SALT}" > "${ANON_SALT_FILE}"
fi

if [[ ${#SITE_FILTERS[@]} -eq 0 ]]; then
  if [[ -z "${DIRAC_DEFAULT_SITES:-}" ]]; then
    echo "DIRAC_DEFAULT_SITES is not set in ${REPO_ROOT}/.env" >&2
    exit 1
  fi
  IFS=',' read -r -a SITE_FILTERS <<< "${DIRAC_DEFAULT_SITES}"
fi

if [[ ${#SITE_FILTERS[@]} -eq 0 ]]; then
  echo "At least one site must be provided via --site/--sites or DIRAC_DEFAULT_SITES" >&2
  exit 1
fi

mkdir -p "${OUTPUT_DIR}"

COND_FROM=""
COND_TO=""
if [[ -n "${FROM_TS}" ]]; then
  COND_FROM="AND m.bucket_15m >= '${FROM_TS}'::timestamp"
fi
if [[ -n "${TO_TS}" ]]; then
  COND_TO="AND m.bucket_15m <= '${TO_TS}'::timestamp"
fi

SITE_ARRAY_SQL="ARRAY["
for site in "${SITE_FILTERS[@]}"; do
  SITE_ARRAY_SQL="${SITE_ARRAY_SQL}'${site//\'/''}',"
done
SITE_ARRAY_SQL="${SITE_ARRAY_SQL%,}]::text[]"

BASE_WHERE="
  WHERE m.activity = 'grid'
    AND m.site = ANY(${SITE_ARRAY_SQL})
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

CSV_15M="${OUTPUT_DIR}/summary_sites_15m.csv"
CSV_1H="${OUTPUT_DIR}/summary_sites_hourly.csv"
CSV_1D="${OUTPUT_DIR}/summary_sites_daily.csv"
CSV_RAW="${OUTPUT_DIR}/summary_sites_raw.csv"
META_JSON="${OUTPUT_DIR}/summary_sites_metadata.json"

RAW_WHERE="
  WHERE s.site_type::text = 'grid'
    AND s.description = ANY(${SITE_ARRAY_SQL})
"

if [[ -n "${FROM_TS}" ]]; then
  RAW_WHERE="${RAW_WHERE}
    AND f.event_start_timestamp >= '${FROM_TS}'::timestamp"
fi
if [[ -n "${TO_TS}" ]]; then
  RAW_WHERE="${RAW_WHERE}
    AND f.event_start_timestamp <= '${TO_TS}'::timestamp"
fi

if [[ "${ANONYMIZE}" == "true" ]]; then
  psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${CSV_RAW}"
COPY (
  WITH filtered_events AS (
    SELECT
      f.event_id,
      f.event_start_timestamp,
      f.site_id,
      COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown') AS vo,
      s.site_type::text AS activity,
      f.energy_wh,
      f.cfp_g,
      f.work,
      f.pue,
      f.ci_g
    FROM monitoring.fact_site_event f
    JOIN monitoring.sites s ON s.site_id = f.site_id
    ${RAW_WHERE}
  ),
  detail_grid_by_event AS (
    SELECT
      dg.event_id,
      SUM(COALESCE(dg.ncores, 0)) AS ncores
    FROM monitoring.detail_grid dg
    JOIN filtered_events fe ON fe.event_id = dg.event_id
    GROUP BY 1
  )
  SELECT
    fe.event_id,
    fe.event_start_timestamp,
    'site_' || SUBSTRING(md5('${ANON_SALT}' || fe.site_id::text) FROM 1 FOR 10) AS site_id,
    'vo_' || SUBSTRING(md5('${ANON_SALT}' || fe.vo) FROM 1 FOR 10) AS vo,
    fe.activity,
    COALESCE(fe.energy_wh, 0) AS energy_wh,
    COALESCE(
      CASE
        WHEN fe.energy_wh IS NOT NULL AND fe.pue IS NOT NULL AND fe.ci_g IS NOT NULL
          THEN (fe.energy_wh / 1000.0) * fe.pue * fe.ci_g
        ELSE fe.cfp_g::double precision
      END,
      0
    ) AS cfp_g,
    COALESCE(fe.work, 0) AS work,
    COALESCE(dg.ncores, 0) AS ncores,
    fe.pue,
    fe.ci_g
  FROM filtered_events fe
  LEFT JOIN detail_grid_by_event dg ON dg.event_id = fe.event_id
) TO STDOUT WITH (FORMAT CSV, HEADER true)
SQL
else
  psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${CSV_RAW}"
COPY (
  WITH filtered_events AS (
    SELECT
      f.event_id,
      f.event_start_timestamp,
      f.site_id,
      COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown') AS vo,
      s.site_type::text AS activity,
      f.energy_wh,
      f.cfp_g,
      f.work,
      f.pue,
      f.ci_g
    FROM monitoring.fact_site_event f
    JOIN monitoring.sites s ON s.site_id = f.site_id
    ${RAW_WHERE}
  ),
  detail_grid_by_event AS (
    SELECT
      dg.event_id,
      SUM(COALESCE(dg.ncores, 0)) AS ncores
    FROM monitoring.detail_grid dg
    JOIN filtered_events fe ON fe.event_id = dg.event_id
    GROUP BY 1
  )
  SELECT
    fe.event_id,
    fe.event_start_timestamp,
    fe.site_id,
    fe.vo,
    fe.activity,
    COALESCE(fe.energy_wh, 0) AS energy_wh,
    COALESCE(
      CASE
        WHEN fe.energy_wh IS NOT NULL AND fe.pue IS NOT NULL AND fe.ci_g IS NOT NULL
          THEN (fe.energy_wh / 1000.0) * fe.pue * fe.ci_g
        ELSE fe.cfp_g::double precision
      END,
      0
    ) AS cfp_g,
    COALESCE(fe.work, 0) AS work,
    COALESCE(dg.ncores, 0) AS ncores,
    fe.pue,
    fe.ci_g
  FROM filtered_events fe
  LEFT JOIN detail_grid_by_event dg ON dg.event_id = fe.event_id
) TO STDOUT WITH (FORMAT CSV, HEADER true)
SQL
fi

if [[ "${ANONYMIZE}" == "true" ]]; then
  psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${CSV_15M}"
COPY (
  SELECT
    m.bucket_15m,
    'site_' || SUBSTRING(md5('${ANON_SALT}' || m.site_id::text) FROM 1 FOR 10) AS site_id,
    'vo_' || SUBSTRING(md5('${ANON_SALT}' || COALESCE(m.vo,'Unknown')) FROM 1 FOR 10) AS vo,
    m.activity,
    m.records AS records,
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
    m.records AS records,
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
    'site_' || SUBSTRING(md5('${ANON_SALT}' || m.site_id::text) FROM 1 FOR 10) AS site_id,
    SUM(COALESCE(m.records,0)) AS records,
    SUM(COALESCE(m.energy_wh,0)) AS energy_wh,
    SUM(COALESCE(m.cfp_g,0)) AS cfp_g,
    SUM(COALESCE(m.work,0)) AS work,
    SUM(COALESCE(m.ncores,0)) AS ncores,
    ROUND(
      (SUM(COALESCE(m.ncores,0))::numeric / NULLIF(SUM(COALESCE(m.records,0)),0)),
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
    m.site_id,
    SUM(COALESCE(m.records,0)) AS records,
    SUM(COALESCE(m.energy_wh,0)) AS energy_wh,
    SUM(COALESCE(m.cfp_g,0)) AS cfp_g,
    SUM(COALESCE(m.work,0)) AS work,
    SUM(COALESCE(m.ncores,0)) AS ncores,
    ROUND(
      (SUM(COALESCE(m.ncores,0))::numeric / NULLIF(SUM(COALESCE(m.records,0)),0)),
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
    'site_' || SUBSTRING(md5('${ANON_SALT}' || m.site_id::text) FROM 1 FOR 10) AS site_id,
    SUM(COALESCE(m.records,0)) AS records,
    SUM(COALESCE(m.energy_wh,0)) AS energy_wh,
    SUM(COALESCE(m.cfp_g,0)) AS cfp_g,
    SUM(COALESCE(m.work,0)) AS work,
    SUM(COALESCE(m.ncores,0)) AS ncores,
    ROUND(
      (SUM(COALESCE(m.ncores,0))::numeric / NULLIF(SUM(COALESCE(m.records,0)),0)),
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
    m.site_id,
    SUM(COALESCE(m.records,0)) AS records,
    SUM(COALESCE(m.energy_wh,0)) AS energy_wh,
    SUM(COALESCE(m.cfp_g,0)) AS cfp_g,
    SUM(COALESCE(m.work,0)) AS work,
    SUM(COALESCE(m.ncores,0)) AS ncores,
    ROUND(
      (SUM(COALESCE(m.ncores,0))::numeric / NULLIF(SUM(COALESCE(m.records,0)),0)),
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
WITH selected_sites AS (
  SELECT unnest(${SITE_ARRAY_SQL}) AS site
),
anonymized_sites AS (
  SELECT
    site,
    'site-' || SUBSTRING(md5('${ANON_SALT}' || site) FROM 1 FOR 10) AS site_anonymized
  FROM selected_sites
)
SELECT jsonb_pretty(
  jsonb_build_object(
    'sites', (
      SELECT jsonb_agg(site_anonymized ORDER BY site_anonymized)
      FROM anonymized_sites
    ),
    'activity', 'grid',
    'source', 'monitoring.mv_fact_site_event_15m',
    'anonymization', jsonb_build_object(
      'enabled', ${ANONYMIZE},
      'vo_rule', 'vo_<first10(md5(salt||vo))>',
      'site_rule', 'site-<first10(md5(salt||site))>',
      'site_id_rule', 'site_<first10(md5(salt||site_id))>'
    ),
    'filters', jsonb_build_object(
      'from', NULLIF('${FROM_TS}',''),
      'to', NULLIF('${TO_TS}','')
    ),
    'files', jsonb_build_object(
      'dataset_raw_csv', '${CSV_RAW}',
      'dataset_15m_csv', '${CSV_15M}',
      'dataset_hourly_csv', '${CSV_1H}',
      'dataset_daily_csv', '${CSV_1D}'
    ),
    'stats', (
      SELECT jsonb_build_object(
        'records_15m', COUNT(*),
        'start_timestamp', MIN(m.bucket_15m),
        'end_timestamp', MAX(m.bucket_15m),
        'total_records', SUM(COALESCE(m.records,0)),
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
echo "  - ${CSV_RAW}"
echo "  - ${CSV_15M}"
echo "  - ${CSV_1H}"
echo "  - ${CSV_1D}"
echo "  - ${META_JSON}"
