#!/usr/bin/env bash
set -euo pipefail

# Generate a consistency JSON report from monitoring.mv_fact_site_event_15m.
# Defaults: activity=grid, auto-select best site by longest coverage then continuity.
#
# Usage:
#   ./ecml-pkdd/generate_dirac_grid_site_consistency_json.sh
#   ./ecml-pkdd/generate_dirac_grid_site_consistency_json.sh --site SITE_NAME
#   ./ecml-pkdd/generate_dirac_grid_site_consistency_json.sh --sites "SITE_A,SITE_B,SITE_C"
#   ./ecml-pkdd/generate_dirac_grid_site_consistency_json.sh --activity grid --start "2025-11-19 00:00:00" --output ecml-pkdd/dirac_grid_site_consistency.json
#   ./ecml-pkdd/generate_dirac_grid_site_consistency_json.sh --summary-output ecml-pkdd/dirac_grid_site_summary.csv --anon-salt my-seed
#   ./ecml-pkdd/generate_dirac_grid_site_consistency_json.sh --tex-output ecml-pkdd/dirac_grid_site_summary.tex

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LAST_EXPORTED_FILE="${REPO_ROOT}/scripts/batch_submit_cnr/last_exported.txt"
ANON_SALT_FILE="${REPO_ROOT}/ecml-pkdd/data/.dirac_anon_salt"

ACTIVITY="grid"
OUTPUT="${SCRIPT_DIR}/dirac_grid_site_consistency.json"
SUMMARY_OUTPUT="${SCRIPT_DIR}/dirac_grid_site_summary.csv"
TEX_OUTPUT="${SCRIPT_DIR}/dirac_grid_site_summary.tex"
SITE_FILTERS=()
START_TS="2025-11-19 00:00:00"
END_TS=""
ANON_SALT="${ANON_SALT:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --activity)
      ACTIVITY="$2"
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
    --output)
      OUTPUT="$2"
      shift 2
      ;;
    --summary-output)
      SUMMARY_OUTPUT="$2"
      shift 2
      ;;
    --tex-output)
      TEX_OUTPUT="$2"
      shift 2
      ;;
    --start)
      START_TS="$2"
      shift 2
      ;;
    --end)
      END_TS="$2"
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

if [[ ! -f "${REPO_ROOT}/.env" ]]; then
  echo "Missing ${REPO_ROOT}/.env" >&2
  exit 1
fi

if [[ ! -f "${LAST_EXPORTED_FILE}" ]]; then
  echo "Missing ${LAST_EXPORTED_FILE}" >&2
  exit 1
fi

if [[ -z "${END_TS}" ]]; then
  END_TS="$(tr -d '[:space:]' < "${LAST_EXPORTED_FILE}")"
  if [[ -z "${END_TS}" ]]; then
    echo "${LAST_EXPORTED_FILE} is empty" >&2
    exit 1
  fi
  END_TS="${END_TS%Z}"
  END_TS="${END_TS/T/ }"
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

mkdir -p "$(dirname "${OUTPUT}")"
mkdir -p "$(dirname "${SUMMARY_OUTPUT}")"
mkdir -p "$(dirname "${TEX_OUTPUT}")"

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

SITE_ARRAY_SQL="ARRAY["
for site in "${SITE_FILTERS[@]}"; do
  SITE_ARRAY_SQL="${SITE_ARRAY_SQL}'${site//\'/''}',"
done
SITE_ARRAY_SQL="${SITE_ARRAY_SQL%,}]::text[]"

psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${SUMMARY_OUTPUT}"
COPY (
  WITH params AS (
    SELECT
      '${ACTIVITY}'::text AS activity,
      ${SITE_ARRAY_SQL} AS selected_sites,
      NULLIF('${START_TS}','')::timestamp AS start_ts,
      NULLIF('${END_TS}','')::timestamp AS end_ts
  ),
  site_stats AS (
    SELECT
      m.site,
      MIN(m.bucket_15m) AS start_ts,
      MAX(m.bucket_15m) AS end_ts,
      COUNT(*) AS active_slots,
      ((EXTRACT(EPOCH FROM (MAX(m.bucket_15m) - MIN(m.bucket_15m))) / 900)::bigint + 1) AS coverage_slots,
      COUNT(*) FILTER (WHERE COALESCE(m.cfp_g, 0) = 0) AS zero_cfp_slots,
      SUM(COALESCE(m.records, 0))::bigint AS total_records,
      SUM(COALESCE(m.ncores, 0))::bigint AS total_ncores
    FROM monitoring.mv_fact_site_event_15m m
    JOIN params p ON p.activity = m.activity
    WHERE
      m.site = ANY(p.selected_sites)
      AND
      (p.start_ts IS NULL OR m.bucket_15m >= p.start_ts)
      AND (p.end_ts IS NULL OR m.bucket_15m <= p.end_ts)
    GROUP BY 1
  )
  SELECT
    'site-' || substr(md5('${ANON_SALT}' || s.site), 1, 10) AS "RI",
    ROUND((EXTRACT(EPOCH FROM (s.end_ts - s.start_ts)) / 3600 / 24 / 30.4375)::numeric, 2) AS "Span (months)",
    ROUND((100.0 * s.active_slots / NULLIF(s.coverage_slots, 0))::numeric, 2) AS "Continuity (%)",
    ROUND((100.0 * s.zero_cfp_slots / NULLIF(s.active_slots, 0))::numeric, 3) AS "Zero CFP (%)",
    s.total_records AS "Total records",
    s.total_ncores AS "Total ncores"
  FROM site_stats s
  ORDER BY "Span (months)" DESC, "Continuity (%)" DESC, "RI" ASC
) TO STDOUT WITH (FORMAT CSV, HEADER true)
SQL

psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${TEX_OUTPUT}"
WITH params AS (
  SELECT
    '${ACTIVITY}'::text AS activity,
    ${SITE_ARRAY_SQL} AS selected_sites,
    NULLIF('${START_TS}','')::timestamp AS start_ts,
    NULLIF('${END_TS}','')::timestamp AS end_ts
),
site_stats AS (
  SELECT
    m.site,
    MIN(m.bucket_15m) AS start_ts,
    MAX(m.bucket_15m) AS end_ts,
    COUNT(*) AS active_slots,
    ((EXTRACT(EPOCH FROM (MAX(m.bucket_15m) - MIN(m.bucket_15m))) / 900)::bigint + 1) AS coverage_slots,
    COUNT(*) FILTER (WHERE COALESCE(m.cfp_g, 0) = 0) AS zero_cfp_slots,
    SUM(COALESCE(m.records, 0))::bigint AS total_records,
    'site-' || substr(md5('${ANON_SALT}' || m.site), 1, 10) AS randomized_name
  FROM monitoring.mv_fact_site_event_15m m
  JOIN params p ON p.activity = m.activity
  WHERE
    m.site = ANY(p.selected_sites)
    AND (p.start_ts IS NULL OR m.bucket_15m >= p.start_ts)
    AND (p.end_ts IS NULL OR m.bucket_15m <= p.end_ts)
  GROUP BY 1
),
overall_span AS (
  SELECT
    MIN(start_ts) AS start_ts,
    MAX(end_ts) AS end_ts
  FROM site_stats
),
summary_rows AS (
  SELECT
    randomized_name,
    ROUND((EXTRACT(EPOCH FROM (end_ts - start_ts)) / 3600 / 24 / 30.4375)::numeric, 2) AS span_months,
    ROUND((100.0 * active_slots / NULLIF(coverage_slots, 0))::numeric, 2) AS continuity_pct,
    ROUND((100.0 * zero_cfp_slots / NULLIF(active_slots, 0))::numeric, 3) AS zero_cfp_pct,
    to_char(total_records, 'FM999,999,999,999,999') AS total_records_fmt
  FROM site_stats
),
table_rows AS (
  SELECT string_agg(
    '\texttt{' || randomized_name || '} & '
    || to_char(span_months, 'FM999999990.00') || ' & '
    || to_char(continuity_pct, 'FM999999990.00') || ' & '
    || to_char(zero_cfp_pct, 'FM999999990.000') || ' & '
    || total_records_fmt || ' \\\\',
    E'\n'
    ORDER BY span_months DESC, continuity_pct DESC, randomized_name ASC
  ) AS rows
  FROM summary_rows
)
SELECT
  'The released dataset\footnote{\url{${ECML_PKDD_GDRIVE_URL}}} contains '
  || (SELECT COUNT(*) FROM summary_rows)
  || ' anonymised grid site time series with \texttt{site\_type=grid} and \texttt{activity\allowbreak\_class=grid}.'
  || E'\n\n'
  || '\subsubsection*{Datasets}'
  || E'\n'
  || 'The series are aggregated into 15-minute buckets spanning'
  || E'\n'
  || '\texttt{'
  || to_char((SELECT start_ts FROM overall_span), 'YYYY-MM-DD"T"HH24:MI:SS')
  || '} to \texttt{'
  || to_char((SELECT end_ts FROM overall_span), 'YYYY-MM-DD"T"HH24:MI:SS')
  || '} ('
  || to_char(ROUND((EXTRACT(EPOCH FROM ((SELECT end_ts FROM overall_span) - (SELECT start_ts FROM overall_span))) / 3600 / 24 / 30.4375)::numeric, 2), 'FM999999990.00')
  || ' months). Derived hourly and daily aggregates are provided as sums of 15-minute buckets.'
  || E'\n'
  || '\begin{table}[h]'
  || E'\n'
  || '\centering'
  || E'\n'
  || '\begin{tabular}{l r r r r}'
  || E'\n'
  || '\hline'
  || E'\n'
  || 'RI & Span (months) & Continuity (\%) & Zero CFP (\%) & Total records \\\\'
  || E'\n'
  || '\hline'
  || E'\n'
  || (SELECT rows FROM table_rows)
  || E'\n'
  || '\hline'
  || E'\n'
  || '\end{tabular}'
  || E'\n'
  || '\end{table}';
SQL

psql "${PSQL_COMMON_ARGS[@]}" <<SQL > "${OUTPUT}"
WITH params AS (
  SELECT
    '${ACTIVITY}'::text AS activity,
    ${SITE_ARRAY_SQL} AS selected_sites,
    NULLIF('${START_TS}','')::timestamp AS start_ts,
    NULLIF('${END_TS}','')::timestamp AS end_ts
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
  WHERE
    m.site = ANY(p.selected_sites)
    AND (p.start_ts IS NULL OR m.bucket_15m >= p.start_ts)
    AND (p.end_ts IS NULL OR m.bucket_15m <= p.end_ts)
  GROUP BY 1
),
best_site AS (
  SELECT c.site
  FROM candidate c
  ORDER BY
    (EXTRACT(EPOCH FROM (c.end_ts-c.start_ts))) DESC,
    (c.active_slots::numeric / NULLIF(c.coverage_slots,0)) DESC,
    c.site ASC
  LIMIT 1
),
site_summary AS (
  SELECT
    c.site,
    ROUND((EXTRACT(EPOCH FROM (c.end_ts - c.start_ts)) / 3600 / 24 / 30.4375)::numeric, 2) AS span_months,
    ROUND((100.0 * c.active_slots / NULLIF(c.coverage_slots, 0))::numeric, 2) AS continuity_pct,
    ROUND(
      (
        100.0 * (
          SELECT COUNT(*)
          FROM monitoring.mv_fact_site_event_15m m
          JOIN params p ON p.activity = m.activity
          WHERE m.site = c.site
            AND (p.start_ts IS NULL OR m.bucket_15m >= p.start_ts)
            AND (p.end_ts IS NULL OR m.bucket_15m <= p.end_ts)
            AND COALESCE(m.cfp_g, 0) = 0
        ) / NULLIF(c.active_slots, 0)
      )::numeric,
      3
    ) AS zero_cfp_pct,
    (
      SELECT SUM(COALESCE(m.records, 0))::bigint
      FROM monitoring.mv_fact_site_event_15m m
      JOIN params p ON p.activity = m.activity
      WHERE m.site = c.site
        AND (p.start_ts IS NULL OR m.bucket_15m >= p.start_ts)
        AND (p.end_ts IS NULL OR m.bucket_15m <= p.end_ts)
    ) AS total_records,
    (
      SELECT SUM(COALESCE(m.ncores, 0))::bigint
      FROM monitoring.mv_fact_site_event_15m m
      JOIN params p ON p.activity = m.activity
      WHERE m.site = c.site
        AND (p.start_ts IS NULL OR m.bucket_15m >= p.start_ts)
        AND (p.end_ts IS NULL OR m.bucket_15m <= p.end_ts)
    ) AS total_ncores,
    'site-' || substr(md5('${ANON_SALT}' || c.site), 1, 10) AS randomized_name
  FROM candidate c
),
raw_mv AS (
  SELECT
    m.bucket_15m,
    m.site_id,
    m.vo,
    m.activity,
    m.site,
    m.records AS jobs,
    m.energy_wh,
    m.cfp_g,
    m.work,
    m.ncores
  FROM monitoring.mv_fact_site_event_15m m
  JOIN params p ON p.activity = m.activity
  JOIN best_site b ON b.site = m.site
  WHERE
    (p.start_ts IS NULL OR m.bucket_15m >= p.start_ts)
    AND (p.end_ts IS NULL OR m.bucket_15m <= p.end_ts)
),
series_15m AS (
  SELECT
    bucket_15m,
    SUM(COALESCE(jobs,0))::bigint AS jobs,
    SUM(COALESCE(energy_wh,0))::double precision AS energy_wh,
    SUM(COALESCE(cfp_g,0))::double precision AS cfp_g
  FROM raw_mv
  GROUP BY 1
),
span AS (
  SELECT MIN(bucket_15m) AS start_ts, MAX(bucket_15m) AS end_ts, COUNT(*) AS records_15m
  FROM series_15m
),
gaps AS (
  SELECT
    prev_ts + interval '15 minutes' AS gap_start,
    bucket_15m - interval '15 minutes' AS gap_end,
    ((EXTRACT(EPOCH FROM (bucket_15m - prev_ts))/900)::bigint - 1) AS missing_15m_intervals
  FROM (
    SELECT bucket_15m, LAG(bucket_15m) OVER (ORDER BY bucket_15m) AS prev_ts
    FROM series_15m
  ) x
  WHERE prev_ts IS NOT NULL AND (bucket_15m - prev_ts) > interval '24 hours'
),
hourly AS (
  SELECT
    date_trunc('hour', bucket_15m) AS bucket_1h,
    SUM(jobs)::bigint AS jobs,
    SUM(energy_wh)::double precision AS energy_wh,
    SUM(cfp_g)::double precision AS cfp_g
  FROM series_15m
  GROUP BY 1
),
daily AS (
  SELECT
    date_trunc('day', bucket_15m) AS bucket_1d,
    SUM(jobs)::bigint AS jobs,
    SUM(energy_wh)::double precision AS energy_wh,
    SUM(cfp_g)::double precision AS cfp_g
  FROM series_15m
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
  FROM series_15m
),
zero_stats AS (
  SELECT
    COUNT(*) AS n,
    COUNT(*) FILTER (WHERE jobs = 0) AS jobs_zero,
    COUNT(*) FILTER (WHERE energy_wh = 0) AS energy_zero,
    COUNT(*) FILTER (WHERE cfp_g = 0) AS cfp_zero,
    COUNT(*) FILTER (WHERE jobs > 0 AND energy_wh = 0) AS jobs_pos_energy_zero,
    COUNT(*) FILTER (WHERE jobs > 0 AND cfp_g = 0) AS jobs_pos_cfp_zero
  FROM series_15m
),
raw_dups AS (
  SELECT
    COUNT(*) FILTER (WHERE c > 1) AS duplicate_siteid_bucket_groups,
    COALESCE(SUM(c - 1) FILTER (WHERE c > 1), 0) AS duplicate_rows_excess
  FROM (
    SELECT site_id, bucket_15m, COUNT(*) AS c
    FROM raw_mv
    GROUP BY 1,2
  ) x
),
series_dups AS (
  SELECT COUNT(*) - COUNT(DISTINCT bucket_15m) AS duplicate_bucket_rows
  FROM series_15m
),
neg_raw AS (
  SELECT
    COUNT(*) FILTER (WHERE COALESCE(jobs,0) < 0) AS neg_jobs,
    COUNT(*) FILTER (WHERE COALESCE(energy_wh,0) < 0) AS neg_energy_wh,
    COUNT(*) FILTER (WHERE COALESCE(cfp_g,0) < 0) AS neg_cfp_g,
    COUNT(*) FILTER (WHERE COALESCE(work,0) < 0) AS neg_work,
    COUNT(*) FILTER (WHERE COALESCE(ncores,0) < 0) AS neg_ncores
  FROM raw_mv
),
neg_series AS (
  SELECT
    COUNT(*) FILTER (WHERE jobs < 0) AS neg_jobs,
    COUNT(*) FILTER (WHERE energy_wh < 0) AS neg_energy_wh,
    COUNT(*) FILTER (WHERE cfp_g < 0) AS neg_cfp_g
  FROM series_15m
),
mono AS (
  SELECT COUNT(*) AS non_monotonic_pairs
  FROM (
    SELECT bucket_15m, LAG(bucket_15m) OVER (ORDER BY bucket_15m) AS prev_ts
    FROM series_15m
  ) x
  WHERE prev_ts IS NOT NULL AND bucket_15m <= prev_ts
),
horizons AS (
  SELECT 4::int AS horizon_steps, '1h'::text AS horizon_name
  UNION ALL
  SELECT 96::int AS horizon_steps, '24h'::text AS horizon_name
),
baseline AS (
  SELECT
    h.horizon_steps,
    h.horizon_name,
    COUNT(*) AS n_points,
    AVG(ABS(s_t.energy_wh - s_prev.energy_wh)) AS mae_energy_wh,
    AVG(ABS(s_t.cfp_g - s_prev.cfp_g)) AS mae_cfp_g,
    AVG(2 * ABS(s_t.energy_wh - s_prev.energy_wh) / NULLIF(ABS(s_t.energy_wh) + ABS(s_prev.energy_wh), 0)) * 100 AS smape_energy_pct,
    AVG(2 * ABS(s_t.cfp_g - s_prev.cfp_g) / NULLIF(ABS(s_t.cfp_g) + ABS(s_prev.cfp_g), 0)) * 100 AS smape_cfp_pct
  FROM horizons h
  JOIN series_15m s_t ON true
  JOIN series_15m s_prev ON s_prev.bucket_15m = s_t.bucket_15m - (h.horizon_steps * interval '15 minutes')
  CROSS JOIN cutoff c
  WHERE s_t.bucket_15m > c.ts_cutoff
  GROUP BY 1,2
),
baseline_json AS (
  SELECT COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'horizon_steps_15m', horizon_steps,
        'horizon_name', horizon_name,
        'points', n_points,
        'mae_energy_wh', mae_energy_wh,
        'mae_cfp_g', mae_cfp_g,
        'smape_energy_pct', smape_energy_pct,
        'smape_cfp_pct', smape_cfp_pct,
        'composite_smape_pct', (smape_energy_pct + smape_cfp_pct) / 2.0
      ) ORDER BY horizon_steps
    ),
    '[]'::jsonb
  ) AS payload
  FROM baseline
),
out AS (
  SELECT jsonb_build_object(
    'selected_site', (SELECT site FROM best_site),
    'selected_site_randomized', (SELECT randomized_name FROM site_summary WHERE site = (SELECT site FROM best_site)),
    'selection_basis', jsonb_build_object(
      'method', 'longest coverage then highest 15-min continuity from materialized view',
      'activity', (SELECT activity FROM params),
      'selected_sites', to_jsonb((SELECT selected_sites FROM params)),
      'start_filter', (SELECT start_ts FROM params),
      'end_filter', (SELECT end_ts FROM params)
    ),
    'data_dictionary_provenance', jsonb_build_object(
      'records', jsonb_build_object(
        'definition', 'COUNT(*) of monitoring.fact_site_event rows aggregated into each MV bucket/group',
        'event_semantics', 'includes all ingested rows regardless of status/job_finished (no status filter in MV)',
        'aggregation', '15m sum; hourly/daily are sums of 15m'
      ),
      'cfp_g', jsonb_build_object(
        'formula_in_mv', 'CASE WHEN energy_wh AND pue AND ci_g are present THEN (energy_wh/1000)*pue*ci_g ELSE cfp_g END',
        'ci_source', 'CI_g can come from partner payload or KPI service (WattNet-backed CI endpoint)',
        'unit', 'gCO2e'
      ),
      'units', jsonb_build_object(
        'energy_wh', 'Wh',
        'energy_kwh_conversion', 'kWh = Wh / 1000',
        'cfp_g', 'gCO2e',
        'cfp_kg_conversion', 'kgCO2e = gCO2e / 1000',
        'ci_g', 'gCO2e/kWh',
        'pue', 'dimensionless'
      ),
      'source_tables', jsonb_build_array(
        'monitoring.fact_site_event',
        'monitoring.detail_grid',
        'monitoring.mv_fact_site_event_15m'
      )
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
    'site_summary_table', COALESCE((
      SELECT jsonb_agg(
        jsonb_build_object(
          'RI', randomized_name,
          'Span (months)', span_months,
          'Continuity (%)', continuity_pct,
          'Zero CFP (%)', zero_cfp_pct,
          'Total records', total_records,
          'Total ncores', total_ncores
        )
        ORDER BY span_months DESC, continuity_pct DESC, randomized_name ASC
      )
      FROM site_summary
    ), '[]'::jsonb),
    'timestamp_policy', jsonb_build_object(
      'db_timestamp_type', 'timestamp without time zone',
      'pipeline_policy', 'timestamps normalized to UTC then stored as UTC-naive',
      'recommended_interpretation', 'treat as UTC',
      'is_continuous_15m_series', ((SELECT records_15m FROM span) = ((EXTRACT(EPOCH FROM ((SELECT end_ts FROM span)-(SELECT start_ts FROM span)))/900)::bigint + 1)),
      'missing_interval_handling', 'missing 15m intervals are dropped (not materialized with null rows)'
    ),
    'frequency_granularity', jsonb_build_object(
      'source_granularity', '15-minute buckets (materialized view)',
      'derived_granularities', jsonb_build_array('hourly','daily'),
      'aggregation_method_per_variable', jsonb_build_object(
        'records', jsonb_build_object('15m','sum','hourly','sum of 15m','daily','sum of 15m'),
        'energy_wh', jsonb_build_object('15m','sum','hourly','sum of 15m','daily','sum of 15m'),
        'cfp_g', jsonb_build_object('15m','sum','hourly','sum of 15m','daily','sum of 15m')
      )
    ),
    'size', jsonb_build_object(
      'records_15m', (SELECT records_15m FROM span),
      'records_hourly', (SELECT COUNT(*) FROM hourly),
      'records_daily', (SELECT COUNT(*) FROM daily),
      'number_of_features', 4,
      'features', jsonb_build_array('bucket_15m','records','energy_wh','cfp_g'),
      'dataset_size_on_disk_bytes_estimated', (SELECT SUM(pg_column_size((bucket_15m, jobs, energy_wh, cfp_g))) FROM series_15m),
      'train_test_split_method', 'temporal 80/20 by timestamp',
      'train_records_15m', (SELECT COUNT(*) FROM series_15m s CROSS JOIN cutoff c WHERE s.bucket_15m <= c.ts_cutoff),
      'test_records_15m', (SELECT COUNT(*) FROM series_15m s CROSS JOIN cutoff c WHERE s.bucket_15m > c.ts_cutoff)
    ),
    'volume', jsonb_build_object(
      'total_records_observed', (SELECT SUM(jobs) FROM series_15m),
      'energy_wh_distribution_15m', (SELECT jsonb_build_object('min',energy_min,'max',energy_max,'p05',energy_p05,'p25',energy_p25,'p50',energy_p50,'p75',energy_p75,'p95',energy_p95) FROM dist),
      'cfp_g_distribution_15m', (SELECT jsonb_build_object('min',cfp_min,'max',cfp_max,'p05',cfp_p05,'p25',cfp_p25,'p50',cfp_p50,'p75',cfp_p75,'p95',cfp_p95) FROM dist)
    ),
    'missingness_vs_zeros', jsonb_build_object(
      'pct_zero_records', ROUND(100.0 * (SELECT jobs_zero FROM zero_stats) / NULLIF((SELECT n FROM zero_stats),0), 4),
      'pct_zero_energy_wh', ROUND(100.0 * (SELECT energy_zero FROM zero_stats) / NULLIF((SELECT n FROM zero_stats),0), 4),
      'pct_zero_cfp_g', ROUND(100.0 * (SELECT cfp_zero FROM zero_stats) / NULLIF((SELECT n FROM zero_stats),0), 4),
      'pct_records_gt_0_and_energy_wh_eq_0', ROUND(100.0 * (SELECT jobs_pos_energy_zero FROM zero_stats) / NULLIF((SELECT n FROM zero_stats),0), 4),
      'pct_records_gt_0_and_cfp_g_eq_0', ROUND(100.0 * (SELECT jobs_pos_cfp_zero FROM zero_stats) / NULLIF((SELECT n FROM zero_stats),0), 4)
    ),
    'integrity_checks', jsonb_build_object(
      'duplicate_site_id_bucket_15m_groups_in_mv', (SELECT duplicate_siteid_bucket_groups FROM raw_dups),
      'duplicate_rows_excess_in_mv', (SELECT duplicate_rows_excess FROM raw_dups),
      'duplicate_bucket_15m_rows_in_challenge_series', (SELECT duplicate_bucket_rows FROM series_dups),
      'negative_values_counts_mv', (SELECT row_to_json(neg_raw) FROM neg_raw),
      'negative_values_counts_challenge_series', (SELECT row_to_json(neg_series) FROM neg_series),
      'non_monotonic_timestamp_pairs_in_challenge_series', (SELECT non_monotonic_pairs FROM mono)
    ),
    'status_quality_snapshot', jsonb_build_object(
      'note', 'For performance, this report does not rescan raw fact_site_event for full status distribution.',
      'records_counting_rule', 'records are counted from MV as COUNT(*) over ingested events without status filtering.'
    ),
    'challenge_mechanics', jsonb_build_object(
      'train_test_cut_timestamp', (SELECT ts_cutoff FROM cutoff),
      'forecast_horizons', jsonb_build_array(
        jsonb_build_object('name', '1h', 'steps_15m', 4, 'minutes', 60),
        jsonb_build_object('name', '24h', 'steps_15m', 96, 'minutes', 1440)
      ),
      'required_submission_schema', jsonb_build_object(
        'format', 'long',
        'columns', jsonb_build_array(
          'series_id',
          'forecast_timestamp_utc',
          'horizon_steps_15m',
          'energy_wh_pred',
          'cfp_g_pred'
        )
      ),
      'evaluation', jsonb_build_object(
        'primary_metric', 'sMAPE',
        'target_metrics', jsonb_build_object(
          'energy_wh', 'sMAPE(energy_wh_true, energy_wh_pred)',
          'cfp_g', 'sMAPE(cfp_g_true, cfp_g_pred)'
        ),
        'multi_target_composition', 'composite = 0.5 * sMAPE_energy + 0.5 * sMAPE_cfp'
      ),
      'baseline', jsonb_build_object(
        'method', 'persistence (last observed value at t predicts t+h)',
        'scores', (SELECT payload FROM baseline_json)
      )
    ),
    'metadata_after_anonymisation', jsonb_build_object(
      'site_type', (SELECT activity FROM params),
      'activity_class', (SELECT activity FROM params),
      'configuration_tags', jsonb_build_array('source:monitoring.mv_fact_site_event_15m','aggregation:15m','anonymised:true'),
      'site_anonymised_id', (SELECT 'grid_site_' || substring(md5(site) from 1 for 10) FROM best_site),
      'site_randomized_name', (SELECT randomized_name FROM site_summary WHERE site = (SELECT site FROM best_site)),
      'site_randomized_name_rule', 'site-<first10(md5(anon_salt||site))>',
      'summary_table_csv', '${SUMMARY_OUTPUT}'
    )
  ) AS payload
)
SELECT jsonb_pretty(payload) FROM out;
SQL

echo "JSON report written to: ${OUTPUT}"
echo "Summary table written to: ${SUMMARY_OUTPUT}"
echo "LaTeX snippet written to: ${TEX_OUTPUT}"
