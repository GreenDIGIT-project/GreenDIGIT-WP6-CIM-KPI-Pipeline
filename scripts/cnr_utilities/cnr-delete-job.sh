#!/usr/bin/env bash
set -euo pipefail

# Delete CNR monitoring rows for a single job / execunitid.
# This is destructive for the selected job identifier.
#
# Usage:
#   scripts/cnr_utilities/cnr-delete-job.sh --job-id "77666a0e-5aac-409d-befd-e427386b554b"
#   scripts/cnr_utilities/cnr-delete-job.sh --job-id "77666a0e-5aac-409d-befd-e427386b554b" --yes
#
# By default it prints matching rows and counts. Add --yes to delete.
#
# It runs inside the `sql-adapter` container so it uses the same env
# (host/db/user/pass).

JOB_ID=""
CONFIRM="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --job-id|--execunitid)
      JOB_ID="${2:-}"
      shift 2
      ;;
    --yes)
      CONFIRM="true"
      shift
      ;;
    -h|--help)
      sed -n '1,30p' "$0"
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

docker compose exec -T \
  -e CNR_DELETE_JOB_ID="$JOB_ID" \
  -e CNR_DELETE_CONFIRM="$CONFIRM" \
  sql-adapter python - <<'PY'
import os
import psycopg2

host = os.environ.get("CNR_POSTEGRESQL_HOST", "greendigit-postgresql.cloud.d4science.org")
db = os.environ.get("CNR_POSTEGRESQL_DB", "greendigit-db")
user = os.environ.get("CNR_POSTEGRESQL_USER", "greendigit-u")
port = int(os.environ.get("CNR_POSTEGRESQL_PORT", "5432"))
password = os.environ.get("CNR_POSTEGRESQL_PASSWORD")

job_id = (os.environ.get("CNR_DELETE_JOB_ID") or "").strip()
confirm = (os.environ.get("CNR_DELETE_CONFIRM") or "false").strip().lower() == "true"

if not password:
    raise SystemExit("CNR_POSTEGRESQL_PASSWORD is not set in the sql-adapter env")

if not job_id:
    raise SystemExit("Missing CNR_DELETE_JOB_ID")

dsn = f"dbname={db} user={user} host={host} password={password} port={port}"
print(f"[cnr-delete-job] connecting host={host} port={port} db={db} user={user}")
print(f"[cnr-delete-job] target execunitid={job_id!r}")

base_where = "f.execunitid = %s"
params = [job_id]

with psycopg2.connect(dsn) as conn:
    with conn.cursor() as cur:
        query = (
            "SELECT f.event_id, s.site_type::text, s.description, f.startexectime, "
            "       f.stopexectime"
        )
        query += (
            " FROM monitoring.fact_site_event f "
            " JOIN monitoring.sites s ON s.site_id = f.site_id "
            f" WHERE {base_where} "
            " ORDER BY f.event_id"
        )
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        if not rows:
            raise SystemExit(f"No CNR rows found for execunitid={job_id!r}")

        event_ids = [int(r[0]) for r in rows]

        print(f"[cnr-delete-job] matching fact rows: {len(rows)}")
        for row in rows:
            print(f"[cnr-delete-job] match {row}")

        cur.execute("SELECT COUNT(*) FROM monitoring.detail_grid WHERE event_id = ANY(%s)", (event_ids,))
        grid_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM monitoring.detail_cloud WHERE event_id = ANY(%s)", (event_ids,))
        cloud_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM monitoring.detail_network WHERE event_id = ANY(%s)", (event_ids,))
        network_count = cur.fetchone()[0]

        print(f"[cnr-delete-job] detail_grid rows to delete: {grid_count}")
        print(f"[cnr-delete-job] detail_cloud rows to delete: {cloud_count}")
        print(f"[cnr-delete-job] detail_network rows to delete: {network_count}")

        if not confirm:
            print("[cnr-delete-job] dry run only. Re-run with --yes to delete.")
            raise SystemExit(0)

        cur.execute("DELETE FROM monitoring.detail_grid WHERE event_id = ANY(%s)", (event_ids,))
        cur.execute("DELETE FROM monitoring.detail_cloud WHERE event_id = ANY(%s)", (event_ids,))
        cur.execute("DELETE FROM monitoring.detail_network WHERE event_id = ANY(%s)", (event_ids,))
        cur.execute(f"DELETE FROM monitoring.fact_site_event f WHERE {base_where}", tuple(params))

        cur.execute("SELECT COUNT(*) FROM monitoring.fact_site_event f WHERE " + base_where, tuple(params))
        remaining = cur.fetchone()[0]
        if remaining:
            raise SystemExit(
                f"Delete verification failed: execunitid={job_id!r} still has {remaining} fact rows"
            )

print("[cnr-delete-job] done")
PY
