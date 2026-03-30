#!/usr/bin/env bash
set -euo pipefail

# Query CNR monitoring rows for a single grid JobID / execunitid.
#
# Usage:
#   scripts/cnr_utilities/cnr-query-job.sh --job-id 213217892

JOB_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --job-id|--execunitid)
      JOB_ID="${2:-}"
      shift 2
      ;;
    -h|--help)
      sed -n '1,20p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$JOB_ID" ]]; then
  echo "Provide --job-id \"<ExecUnitID/JobID>\"." >&2
  exit 2
fi

set -a
source .env
set +a

PSQL_COMMON_ARGS=(
  -h "${CNR_HOST}"
  -p "${CNR_POSTEGRESQL_PORT:-5432}"
  -U "${CNR_USER}"
  -d "${CNR_GD_DB}"
  -v ON_ERROR_STOP=1
)

PGPASSWORD="${CNR_POSTEGRESQL_PASSWORD}" \
psql "${PSQL_COMMON_ARGS[@]}" \
  -v job_id="${JOB_ID}" \
  -f _sql_cnr/query_jobid.sql
