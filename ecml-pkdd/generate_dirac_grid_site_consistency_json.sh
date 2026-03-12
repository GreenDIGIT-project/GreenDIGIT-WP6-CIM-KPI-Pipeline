#!/usr/bin/env bash
set -euo pipefail

# Generate a consistency JSON report from monitoring.mv_fact_site_event_15m.
# Defaults: activity=grid, auto-select best site by longest coverage then continuity.
#
# Usage:
#   ./ecml-pkdd/generate_dirac_grid_site_consistency_json.sh
#   ./ecml-pkdd/generate_dirac_grid_site_consistency_json.sh --site SARA-MATRIX
#   ./ecml-pkdd/generate_dirac_grid_site_consistency_json.sh --activity grid --output ecml-pkdd/dirac_grid_site_consistency.json

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ACTIVITY="grid"
SITE=""
OUTPUT="${SCRIPT_DIR}/dirac_grid_site_consistency.json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --activity)
      ACTIVITY="$2"
      shift 2
      ;;
    --site)
      SITE="$2"
      shift 2
      ;;
    --output)
      OUTPUT="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
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

mkdir -p "$(dirname "${OUTPUT}")"

PSQL_COMMON_ARGS=(
  -h "${CNR_HOST}"
  -p 5432
  -U "${CNR_USER}"
  -d "${CNR_GD_DB}"
  -v ON_ERROR_STOP=1
  -t
  -A
)

export PGPASSWORD="${CNR_POSTEGRESQL_PASSWORD}"

if [[ -n "${SITE}" ]]; then
  SITE_SQL="'${SITE//\'/''}'"
else
  SITE_SQL="NULL"
fi

psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${OUTPUT}"
WITH params AS (
  SELECT '${ACTIVITY}'::text AS activity, ${SITE_SQL}::text AS site
),
candidate AS (
  SELECT
    m.site,
    MIN(m.bucket_15m) AS start_ts,
    MAX(m.bucket_15m) AS end_ts,
    COUNT(*) AS active_slots,
    ((EXTRACT(EPOCH FROM (MAX(m.bucket_15m)-MIN(m.bucket_15m)))/900)::bigint + 1) AS coverage_slots
  FROM monitoring.mv_fact_site_event_15m m
  JOIN params p ON p.activity = m.activity
  GROUP BY 1
),
best_site AS (
  SELECT c.site
  FROM candidate c
  JOIN params p ON true
  WHERE p.site IS NULL OR c.site = p.site
  ORDER BY
    (EXTRACT(EPOCH FROM (c.end_ts-c.start_ts))) DESC,
    (c.active_slots::numeric / NULLIF(c.coverage_slots,0)) DESC,
    c.site ASC
  LIMIT 1
),
site_15m AS (
  SELECT
    m.bucket_15m,
    SUM(COALESCE(m.jobs,0))::bigint AS jobs,
    SUM(COALESCE(m.energy_wh,0))::double precision AS energy_wh,
    SUM(COALESCE(m.cfp_g,0))::double precision AS cfp_g
  FROM monitoring.mv_fact_site_event_15m m
  JOIN params p ON p.activity = m.activity
  JOIN best_site b ON b.site = m.site
  GROUP BY 1
),
span AS (
  SELECT MIN(bucket_15m) AS start_ts, MAX(bucket_15m) AS end_ts, COUNT(*) AS records_15m
  FROM site_15m
),
gaps AS (
  SELECT
    prev_ts + interval '15 minutes' AS gap_start,
    bucket_15m - interval '15 minutes' AS gap_end,
    ((EXTRACT(EPOCH FROM (bucket_15m - prev_ts))/900)::bigint - 1) AS missing_15m_intervals
  FROM (
    SELECT bucket_15m, LAG(bucket_15m) OVER (ORDER BY bucket_15m) AS prev_ts
    FROM site_15m
  ) x
  WHERE prev_ts IS NOT NULL AND (bucket_15m - prev_ts) > interval '24 hours'
),
hourly AS (
  SELECT
    date_trunc('hour', bucket_15m) AS bucket_1h,
    SUM(jobs)::bigint AS jobs,
    SUM(energy_wh)::double precision AS energy_wh,
    SUM(cfp_g)::double precision AS cfp_g
  FROM site_15m
  GROUP BY 1
),
daily AS (
  SELECT
    date_trunc('day', bucket_15m) AS bucket_1d,
    SUM(jobs)::bigint AS jobs,
    SUM(energy_wh)::double precision AS energy_wh,
    SUM(cfp_g)::double precision AS cfp_g
  FROM site_15m
  GROUP BY 1
),
cutoff AS (
  SELECT start_ts + ((end_ts-start_ts)*0.8) AS ts_cutoff FROM span
),
dist AS (
  SELECT
    MIN(energy_wh) AS energy_min,
    MAX(energy_wh) AS energy_max,
    percentile_cont(0.05) WITHIN GROUP (ORDER BY energy_wh) AS energy_p05,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY energy_wh) AS energy_p25,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY energy_wh) AS energy_p50,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY energy_wh) AS energy_p75,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY energy_wh) AS energy_p95,
    MIN(cfp_g) AS cfp_min,
    MAX(cfp_g) AS cfp_max,
    percentile_cont(0.05) WITHIN GROUP (ORDER BY cfp_g) AS cfp_p05,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY cfp_g) AS cfp_p25,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY cfp_g) AS cfp_p50,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY cfp_g) AS cfp_p75,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY cfp_g) AS cfp_p95
  FROM site_15m
),
out AS (
  SELECT jsonb_build_object(
    'selected_site', (SELECT site FROM best_site),
    'selection_basis', jsonb_build_object(
      'method', 'longest coverage then highest 15-min continuity from materialized view',
      'activity', (SELECT activity FROM params)
    ),
    'time_coverage', jsonb_build_object(
      'start_timestamp', (SELECT start_ts FROM span),
      'end_timestamp', (SELECT end_ts FROM span),
      'total_duration_months', ROUND((EXTRACT(EPOCH FROM ((SELECT end_ts FROM span)-(SELECT start_ts FROM span)))/3600/24/30.4375)::numeric,2),
      'known_gaps_downtime_periods', COALESCE((
        SELECT jsonb_agg(jsonb_build_object(
          'gap_start', gap_start,
          'gap_end', gap_end,
          'missing_15m_intervals', missing_15m_intervals,
          'missing_hours', ROUND((missing_15m_intervals*0.25)::numeric,2)
        ) ORDER BY gap_start)
        FROM gaps
      ), '[]'::jsonb)
    ),
    'frequency_granularity', jsonb_build_object(
      'source_granularity', '15-minute buckets (materialized view)',
      'derived_granularities', jsonb_build_array('hourly','daily'),
      'aggregation_method_per_variable', jsonb_build_object(
        'jobs', jsonb_build_object('15m','sum','hourly','sum of 15m','daily','sum of 15m'),
        'energy_wh', jsonb_build_object('15m','sum','hourly','sum of 15m','daily','sum of 15m'),
        'cfp_g', jsonb_build_object('15m','sum','hourly','sum of 15m','daily','sum of 15m')
      )
    ),
    'size', jsonb_build_object(
      'records_15m', (SELECT records_15m FROM span),
      'records_hourly', (SELECT COUNT(*) FROM hourly),
      'records_daily', (SELECT COUNT(*) FROM daily),
      'number_of_features', 4,
      'features', jsonb_build_array('bucket_15m','jobs','energy_wh','cfp_g'),
      'dataset_size_on_disk_bytes_estimated', (SELECT SUM(pg_column_size((bucket_15m, jobs, energy_wh, cfp_g))) FROM site_15m),
      'train_test_split_method', 'temporal 80/20 by timestamp',
      'train_records_15m', (SELECT COUNT(*) FROM site_15m s CROSS JOIN cutoff c WHERE s.bucket_15m <= c.ts_cutoff),
      'test_records_15m', (SELECT COUNT(*) FROM site_15m s CROSS JOIN cutoff c WHERE s.bucket_15m > c.ts_cutoff)
    ),
    'volume', jsonb_build_object(
      'total_jobs_observed', (SELECT SUM(jobs) FROM site_15m),
      'energy_wh_distribution_15m', (SELECT jsonb_build_object('min',energy_min,'max',energy_max,'p05',energy_p05,'p25',energy_p25,'p50',energy_p50,'p75',energy_p75,'p95',energy_p95) FROM dist),
      'cfp_g_distribution_15m', (SELECT jsonb_build_object('min',cfp_min,'max',cfp_max,'p05',cfp_p05,'p25',cfp_p25,'p50',cfp_p50,'p75',cfp_p75,'p95',cfp_p95) FROM dist)
    ),
    'metadata_after_anonymisation', jsonb_build_object(
      'site_type', (SELECT activity FROM params),
      'activity_class', (SELECT activity FROM params),
      'configuration_tags', jsonb_build_array('source:monitoring.mv_fact_site_event_15m','aggregation:15m','anonymised:true'),
      'site_anonymised_id', (SELECT 'grid_site_' || substring(md5(site) from 1 for 10) FROM best_site)
    )
  ) AS payload
)
SELECT jsonb_pretty(payload) FROM out;
SQL

echo "JSON report written to: ${OUTPUT}"
