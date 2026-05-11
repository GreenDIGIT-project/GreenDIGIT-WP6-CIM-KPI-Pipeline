#!/usr/bin/env bash
set -euo pipefail

# Rewrite CNR monitoring.fact_site_event.owner values that were populated from
# raw DIRAC OwnerGroup instead of raw Owner.
#
# Dry-run by default. Add --yes to update rows.
#
# Usage:
#   scripts/cnr_utilities/cnr-rewrite-owner-groups.sh
#   scripts/cnr_utilities/cnr-rewrite-owner-groups.sh --start 2026-03-11T00:00:00Z --yes
#   scripts/cnr_utilities/cnr-rewrite-owner-groups.sh --start 2026-03-11T00:00:00Z --end 2026-05-11T00:00:00Z --yes
#
# Mapping:
#   wenmr_user          -> enmr.eu
#   biomed_user         -> biomed
#   biomed_green        -> biomed
#   km3net_user         -> km3net.org
#   km3net_admin        -> km3net.org
#   km3net_acc          -> km3net.org
#   scigne_user         -> vo.scigne.fr
#   scigne_fluka        -> vo.scigne.fr
#   francegrilles_user  -> vo.france-grilles.fr

START_TS=""
END_TS=""
CONFIRM="false"
REFRESH="true"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start)
      START_TS="${2:-}"
      shift 2
      ;;
    --end)
      END_TS="${2:-}"
      shift 2
      ;;
    --yes)
      CONFIRM="true"
      shift
      ;;
    --no-refresh)
      REFRESH="false"
      shift
      ;;
    -h|--help)
      sed -n '1,32p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

CNR_SQL_HOST="${CNR_POSTEGRESQL_HOST:-${CNR_HOST:-}}"
CNR_SQL_PORT="${CNR_POSTEGRESQL_PORT:-5432}"
CNR_SQL_USER="${CNR_POSTEGRESQL_USER:-${CNR_USER:-}}"
CNR_SQL_DB="${CNR_POSTEGRESQL_DB:-${CNR_GD_DB:-}}"
CNR_SQL_PASSWORD="${CNR_POSTEGRESQL_PASSWORD:-${CNR_POSTGRESQL_PASSWORD:-${CNR_PASSWORD:-}}}"

missing=()
[[ -z "$CNR_SQL_HOST" ]] && missing+=("CNR_POSTEGRESQL_HOST/CNR_HOST")
[[ -z "$CNR_SQL_USER" ]] && missing+=("CNR_POSTEGRESQL_USER/CNR_USER")
[[ -z "$CNR_SQL_DB" ]] && missing+=("CNR_POSTEGRESQL_DB/CNR_GD_DB")
[[ -z "$CNR_SQL_PASSWORD" ]] && missing+=("CNR_POSTEGRESQL_PASSWORD/CNR_POSTGRESQL_PASSWORD/CNR_PASSWORD")
if [[ ${#missing[@]} -gt 0 ]]; then
  echo "Missing DB env vars: ${missing[*]}" >&2
  exit 2
fi

if [[ -n "$START_TS" ]]; then
  date -u -d "$START_TS" >/dev/null
fi
if [[ -n "$END_TS" ]]; then
  date -u -d "$END_TS" >/dev/null
fi
if [[ -n "$START_TS" && -n "$END_TS" ]]; then
  start_epoch="$(date -u -d "$START_TS" +%s)"
  end_epoch="$(date -u -d "$END_TS" +%s)"
  if [[ "$start_epoch" -gt "$end_epoch" ]]; then
    echo "start must be <= end: start=$START_TS end=$END_TS" >&2
    exit 2
  fi
fi

PSQL_COMMON_ARGS=(
  -h "$CNR_SQL_HOST"
  -p "$CNR_SQL_PORT"
  -U "$CNR_SQL_USER"
  -d "$CNR_SQL_DB"
  -v ON_ERROR_STOP=1
  -P pager=off
  -v rewrite_start="$START_TS"
  -v rewrite_end="$END_TS"
)

echo "[cnr-rewrite-owner-groups] connecting host=$CNR_SQL_HOST port=$CNR_SQL_PORT db=$CNR_SQL_DB user=$CNR_SQL_USER"
echo "[cnr-rewrite-owner-groups] start=${START_TS:-<none>} end=${END_TS:-<none>}"
echo "[cnr-rewrite-owner-groups] mode=$([[ "$CONFIRM" == "true" ]] && echo update || echo dry-run) refresh=$REFRESH"

PGPASSWORD="$CNR_SQL_PASSWORD" psql "${PSQL_COMMON_ARGS[@]}" <<'SQL'
WITH mapping(wrong_owner, correct_owner) AS (
  VALUES
    ('wenmr_user', 'enmr.eu'),
    ('biomed_user', 'biomed'),
    ('biomed_green', 'biomed'),
    ('km3net_user', 'km3net.org'),
    ('km3net_admin', 'km3net.org'),
    ('km3net_acc', 'km3net.org'),
    ('scigne_user', 'vo.scigne.fr'),
    ('scigne_fluka', 'vo.scigne.fr'),
    ('francegrilles_user', 'vo.france-grilles.fr')
)
SELECT
  m.wrong_owner,
  m.correct_owner,
  COUNT(f.*) AS rows,
  MIN(f.event_start_timestamp) AS min_start,
  MAX(f.event_start_timestamp) AS max_start
FROM mapping m
LEFT JOIN monitoring.fact_site_event f
  ON LOWER(COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown')) = LOWER(m.wrong_owner)
 AND (NULLIF(:'rewrite_start', '') IS NULL OR f.event_start_timestamp >= NULLIF(:'rewrite_start', '')::timestamptz)
 AND (NULLIF(:'rewrite_end', '') IS NULL OR f.event_start_timestamp <= NULLIF(:'rewrite_end', '')::timestamptz)
GROUP BY m.wrong_owner, m.correct_owner
ORDER BY m.wrong_owner;

WITH mapping(wrong_owner, correct_owner) AS (
  VALUES
    ('wenmr_user', 'enmr.eu'),
    ('biomed_user', 'biomed'),
    ('biomed_green', 'biomed'),
    ('km3net_user', 'km3net.org'),
    ('km3net_admin', 'km3net.org'),
    ('km3net_acc', 'km3net.org'),
    ('scigne_user', 'vo.scigne.fr'),
    ('scigne_fluka', 'vo.scigne.fr'),
    ('francegrilles_user', 'vo.france-grilles.fr')
)
SELECT COUNT(*) AS total_rows_matching_rewrite
FROM monitoring.fact_site_event f
JOIN mapping m
  ON LOWER(COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown')) = LOWER(m.wrong_owner)
WHERE (NULLIF(:'rewrite_start', '') IS NULL OR f.event_start_timestamp >= NULLIF(:'rewrite_start', '')::timestamptz)
  AND (NULLIF(:'rewrite_end', '') IS NULL OR f.event_start_timestamp <= NULLIF(:'rewrite_end', '')::timestamptz);
SQL

if [[ "$CONFIRM" != "true" ]]; then
  echo "[cnr-rewrite-owner-groups] dry run only. Re-run with --yes to update."
  exit 0
fi

PGPASSWORD="$CNR_SQL_PASSWORD" psql "${PSQL_COMMON_ARGS[@]}" <<'SQL'
WITH mapping(wrong_owner, correct_owner) AS (
  VALUES
    ('wenmr_user', 'enmr.eu'),
    ('biomed_user', 'biomed'),
    ('biomed_green', 'biomed'),
    ('km3net_user', 'km3net.org'),
    ('km3net_admin', 'km3net.org'),
    ('km3net_acc', 'km3net.org'),
    ('scigne_user', 'vo.scigne.fr'),
    ('scigne_fluka', 'vo.scigne.fr'),
    ('francegrilles_user', 'vo.france-grilles.fr')
),
updated AS (
  UPDATE monitoring.fact_site_event f
     SET owner = m.correct_owner
    FROM mapping m
   WHERE LOWER(COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown')) = LOWER(m.wrong_owner)
     AND (NULLIF(:'rewrite_start', '') IS NULL OR f.event_start_timestamp >= NULLIF(:'rewrite_start', '')::timestamptz)
     AND (NULLIF(:'rewrite_end', '') IS NULL OR f.event_start_timestamp <= NULLIF(:'rewrite_end', '')::timestamptz)
  RETURNING 1
)
SELECT COUNT(*) AS updated_rows FROM updated;
SQL

if [[ "$REFRESH" == "true" ]]; then
  echo "[cnr-rewrite-owner-groups] refreshing monitoring.mv_fact_site_event_15m_base"
  PGPASSWORD="$CNR_SQL_PASSWORD" psql "${PSQL_COMMON_ARGS[@]}" \
    -c "REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.mv_fact_site_event_15m_base;"

  echo "[cnr-rewrite-owner-groups] refreshing monitoring.mv_reporting_resource_listing"
  PGPASSWORD="$CNR_SQL_PASSWORD" psql "${PSQL_COMMON_ARGS[@]}" \
    -c "REFRESH MATERIALIZED VIEW monitoring.mv_reporting_resource_listing;"
fi

echo "[cnr-rewrite-owner-groups] done"
