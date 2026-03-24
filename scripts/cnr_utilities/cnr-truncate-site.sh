#!/usr/bin/env bash
set -euo pipefail

# Delete CNR monitoring rows for a single monitoring.sites record.
# This is destructive for the selected site.
#
# Usage:
#   scripts/cnr_utilities/cnr-truncate-site.sh --site "SoBigData-datacenter" --yes
#   scripts/cnr_utilities/cnr-truncate-site.sh --site-id 123 --yes
#
# It runs inside the `sql-adapter` container so it uses the same env
# (host/db/user/pass).

SITE_DESC=""
SITE_ID=""
CONFIRM="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --site)
      SITE_DESC="${2:-}"
      shift 2
      ;;
    --site-id)
      SITE_ID="${2:-}"
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

if [[ "$CONFIRM" != "true" ]]; then
  echo "Refusing to run without --yes (this deletes CNR rows for one site)." >&2
  exit 2
fi

if [[ -n "$SITE_DESC" && -n "$SITE_ID" ]]; then
  echo "Use either --site or --site-id, not both." >&2
  exit 2
fi

if [[ -z "$SITE_DESC" && -z "$SITE_ID" ]]; then
  echo "Provide one selector: --site \"<description>\" or --site-id <id>." >&2
  exit 2
fi

if [[ -n "$SITE_ID" && ! "$SITE_ID" =~ ^[0-9]+$ ]]; then
  echo "--site-id must be an integer." >&2
  exit 2
fi

docker compose exec -T \
  -e TRUNCATE_SITE_DESC="$SITE_DESC" \
  -e TRUNCATE_SITE_ID="$SITE_ID" \
  sql-adapter python - <<'PY'
import os
import psycopg2

host = os.environ.get("CNR_POSTEGRESQL_HOST", "greendigit-postgresql.cloud.d4science.org")
db = os.environ.get("CNR_POSTEGRESQL_DB", "greendigit-db")
user = os.environ.get("CNR_POSTEGRESQL_USER", "greendigit-u")
port = int(os.environ.get("CNR_POSTEGRESQL_PORT", "5432"))
password = os.environ.get("CNR_POSTEGRESQL_PASSWORD")

site_desc = (os.environ.get("TRUNCATE_SITE_DESC") or "").strip()
site_id_raw = (os.environ.get("TRUNCATE_SITE_ID") or "").strip()

if not password:
    raise SystemExit("CNR_POSTEGRESQL_PASSWORD is not set in the sql-adapter env")

if site_desc and site_id_raw:
    raise SystemExit("Use either --site or --site-id, not both.")

if not site_desc and not site_id_raw:
    raise SystemExit("Provide one selector: --site or --site-id.")

dsn = f"dbname={db} user={user} host={host} password={password} port={port}"
print(f"[cnr-truncate-site] connecting host={host} port={port} db={db} user={user}")

with psycopg2.connect(dsn) as conn:
    with conn.cursor() as cur:
        if site_id_raw:
            site_id = int(site_id_raw)
            cur.execute(
                "SELECT site_id, site_type::text, description FROM monitoring.sites WHERE site_id = %s",
                (site_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise SystemExit(f"Site not found: site_id={site_id}")
            site_id, site_type, site_description = row
        else:
            cur.execute(
                "SELECT site_id, site_type::text, description "
                "FROM monitoring.sites "
                "WHERE description = %s "
                "ORDER BY site_id ASC",
                (site_desc,),
            )
            rows = cur.fetchall()
            if not rows:
                raise SystemExit(f"No site found with description={site_desc!r}")
            if len(rows) > 1:
                ids = ", ".join(str(r[0]) for r in rows)
                raise SystemExit(
                    "Description is not unique. Use --site-id instead. "
                    f"Matching site_ids: {ids}"
                )
            site_id, site_type, site_description = rows[0]

        print(
            f"[cnr-truncate-site] target site_id={site_id} "
            f"site_type={site_type} description={site_description!r}"
        )

        cur.execute("SELECT COUNT(*) FROM monitoring.fact_site_event WHERE site_id = %s", (site_id,))
        fact_count = cur.fetchone()[0]
        print(f"[cnr-truncate-site] fact_site_event rows to delete: {fact_count}")

        cur.execute("SELECT COUNT(*) FROM monitoring.detail_grid WHERE site_id = %s", (site_id,))
        grid_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM monitoring.detail_cloud WHERE site_id = %s", (site_id,))
        cloud_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM monitoring.detail_network WHERE site_id = %s", (site_id,))
        network_count = cur.fetchone()[0]

        print(f"[cnr-truncate-site] detail_grid rows to delete: {grid_count}")
        print(f"[cnr-truncate-site] detail_cloud rows to delete: {cloud_count}")
        print(f"[cnr-truncate-site] detail_network rows to delete: {network_count}")

        # Remove details first, then fact, then site.
        # Delete by site_id, and by event_id linked to fact rows for safety.
        cur.execute(
            "DELETE FROM monitoring.detail_grid "
            "WHERE site_id = %s "
            "   OR event_id IN (SELECT event_id FROM monitoring.fact_site_event WHERE site_id = %s)",
            (site_id, site_id),
        )
        cur.execute(
            "DELETE FROM monitoring.detail_cloud "
            "WHERE site_id = %s "
            "   OR event_id IN (SELECT event_id FROM monitoring.fact_site_event WHERE site_id = %s)",
            (site_id, site_id),
        )
        cur.execute(
            "DELETE FROM monitoring.detail_network "
            "WHERE site_id = %s "
            "   OR event_id IN (SELECT event_id FROM monitoring.fact_site_event WHERE site_id = %s)",
            (site_id, site_id),
        )
        cur.execute("DELETE FROM monitoring.fact_site_event WHERE site_id = %s", (site_id,))
        cur.execute("DELETE FROM monitoring.sites WHERE site_id = %s", (site_id,))

        cur.execute("SELECT COUNT(*) FROM monitoring.sites WHERE site_id = %s", (site_id,))
        still_exists = cur.fetchone()[0]
        if still_exists:
            raise SystemExit(f"Delete verification failed: site_id={site_id} still exists")

print("[cnr-truncate-site] done")
PY
