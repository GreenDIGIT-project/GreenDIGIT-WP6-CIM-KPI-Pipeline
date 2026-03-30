#!/usr/bin/env bash
set -euo pipefail

# Delete CNR monitoring rows for one or more site_ids within a time window.
# This is destructive for the selected rows only.
#
# Usage:
#   scripts/cnr_utilities/cnr-truncate-site-window.sh \
#     --site-id 22 --site-id 72 \
#     --start "2026-03-23T00:00:00Z" \
#     --end "2026-03-24T23:59:59Z"
#
# Add --yes to actually delete. Without --yes it runs as a dry run and prints counts.
#
# Window rule:
#   event_start_timestamp <= end AND event_end_timestamp >= start
#
# It runs inside the `sql-adapter` container so it uses the same env
# (host/db/user/pass).

SITE_IDS=()
START_TS=""
END_TS=""
CONFIRM="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --site-id)
      SITE_IDS+=("${2:-}")
      shift 2
      ;;
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
    -h|--help)
      sed -n '1,35p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ "${#SITE_IDS[@]}" -eq 0 ]]; then
  echo "Provide at least one --site-id <id>." >&2
  exit 2
fi

for site_id in "${SITE_IDS[@]}"; do
  if [[ ! "$site_id" =~ ^[0-9]+$ ]]; then
    echo "Invalid --site-id value: $site_id" >&2
    exit 2
  fi
done

if [[ -z "$START_TS" || -z "$END_TS" ]]; then
  echo "Provide both --start and --end in ISO-8601 UTC form, e.g. 2026-03-23T00:00:00Z." >&2
  exit 2
fi

SITE_IDS_CSV="$(IFS=,; printf '%s' "${SITE_IDS[*]}")"

docker compose exec -T \
  -e TRUNCATE_SITE_IDS="$SITE_IDS_CSV" \
  -e TRUNCATE_START_TS="$START_TS" \
  -e TRUNCATE_END_TS="$END_TS" \
  -e TRUNCATE_CONFIRM="$CONFIRM" \
  sql-adapter python - <<'PY'
import os
from datetime import datetime, timezone

import psycopg2

host = os.environ.get("CNR_POSTEGRESQL_HOST", "greendigit-postgresql.cloud.d4science.org")
db = os.environ.get("CNR_POSTEGRESQL_DB", "greendigit-db")
user = os.environ.get("CNR_POSTEGRESQL_USER", "greendigit-u")
port = int(os.environ.get("CNR_POSTEGRESQL_PORT", "5432"))
password = os.environ.get("CNR_POSTEGRESQL_PASSWORD")

site_ids_raw = (os.environ.get("TRUNCATE_SITE_IDS") or "").strip()
start_raw = (os.environ.get("TRUNCATE_START_TS") or "").strip()
end_raw = (os.environ.get("TRUNCATE_END_TS") or "").strip()
confirm = (os.environ.get("TRUNCATE_CONFIRM") or "false").strip().lower() == "true"

if not password:
    raise SystemExit("CNR_POSTEGRESQL_PASSWORD is not set in the sql-adapter env")

if not site_ids_raw:
    raise SystemExit("Missing TRUNCATE_SITE_IDS")
if not start_raw or not end_raw:
    raise SystemExit("Missing TRUNCATE_START_TS / TRUNCATE_END_TS")

def parse_iso_utc(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)

try:
    start_dt = parse_iso_utc(start_raw)
    end_dt = parse_iso_utc(end_raw)
except Exception as exc:
    raise SystemExit(f"Invalid timestamp format: {exc}")

if start_dt > end_dt:
    raise SystemExit(f"start must be <= end: start={start_raw} end={end_raw}")

site_ids = []
for part in site_ids_raw.split(","):
    part = part.strip()
    if not part:
        continue
    try:
        site_ids.append(int(part))
    except Exception:
        raise SystemExit(f"Invalid site_id={part!r}")

if not site_ids:
    raise SystemExit("No site_ids parsed")

dsn = f"dbname={db} user={user} host={host} password={password} port={port}"
print(f"[cnr-truncate-site-window] connecting host={host} port={port} db={db} user={user}")
print(f"[cnr-truncate-site-window] site_ids={site_ids}")
print(f"[cnr-truncate-site-window] start={start_raw} end={end_raw}")

with psycopg2.connect(dsn) as conn:
    with conn.cursor() as cur:
        all_event_ids = []

        for site_id in site_ids:
            cur.execute(
                "SELECT site_id, site_type::text, description "
                "FROM monitoring.sites WHERE site_id = %s",
                (site_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise SystemExit(f"Site not found: site_id={site_id}")
            site_id_db, site_type, site_description = row

            print(
                f"[cnr-truncate-site-window] target site_id={site_id_db} "
                f"site_type={site_type} description={site_description!r}"
            )

            cur.execute(
                """
                SELECT event_id
                FROM monitoring.fact_site_event
                WHERE site_id = %s
                  AND event_start_timestamp <= %s
                  AND event_end_timestamp >= %s
                ORDER BY event_id
                """,
                (site_id_db, end_dt, start_dt),
            )
            event_ids = [int(r[0]) for r in cur.fetchall()]
            all_event_ids.extend(event_ids)
            print(f"[cnr-truncate-site-window] fact_site_event rows to delete for site_id={site_id_db}: {len(event_ids)}")

        # Keep ordering stable while deduplicating.
        seen = set()
        deduped_event_ids = []
        for event_id in all_event_ids:
            if event_id not in seen:
                seen.add(event_id)
                deduped_event_ids.append(event_id)

        if not deduped_event_ids:
            print("[cnr-truncate-site-window] no matching rows found in the requested window")
            raise SystemExit(0)

        cur.execute("SELECT COUNT(*) FROM monitoring.detail_grid WHERE event_id = ANY(%s)", (deduped_event_ids,))
        grid_count = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM monitoring.detail_cloud WHERE event_id = ANY(%s) OR site_id = ANY(%s)",
            (deduped_event_ids, deduped_event_ids),
        )
        cloud_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM monitoring.detail_network WHERE event_id = ANY(%s)", (deduped_event_ids,))
        network_count = cur.fetchone()[0]

        print(f"[cnr-truncate-site-window] total fact_site_event rows to delete: {len(deduped_event_ids)}")
        print(f"[cnr-truncate-site-window] detail_grid rows to delete: {grid_count}")
        print(f"[cnr-truncate-site-window] detail_cloud rows to delete: {cloud_count}")
        print(f"[cnr-truncate-site-window] detail_network rows to delete: {network_count}")

        if not confirm:
            print("[cnr-truncate-site-window] dry run only. Re-run with --yes to delete.")
            raise SystemExit(0)

        cur.execute("DELETE FROM monitoring.detail_grid WHERE event_id = ANY(%s)", (deduped_event_ids,))
        cur.execute(
            "DELETE FROM monitoring.detail_cloud WHERE event_id = ANY(%s) OR site_id = ANY(%s)",
            (deduped_event_ids, deduped_event_ids),
        )
        cur.execute("DELETE FROM monitoring.detail_network WHERE event_id = ANY(%s)", (deduped_event_ids,))
        cur.execute("DELETE FROM monitoring.fact_site_event WHERE event_id = ANY(%s)", (deduped_event_ids,))

        cur.execute("SELECT COUNT(*) FROM monitoring.fact_site_event WHERE event_id = ANY(%s)", (deduped_event_ids,))
        remaining = cur.fetchone()[0]
        if remaining:
            raise SystemExit(f"Delete verification failed: {remaining} fact_site_event rows still remain")

print("[cnr-truncate-site-window] done")
PY
