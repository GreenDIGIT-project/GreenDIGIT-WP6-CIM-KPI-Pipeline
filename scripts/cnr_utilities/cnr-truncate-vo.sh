#!/usr/bin/env bash
set -euo pipefail

# Delete CNR monitoring rows for a single VO value.
# This is destructive for the selected VO.
#
# Usage:
#   scripts/cnr_utilities/cnr-truncate-vo.sh --vo "vo.example" --yes
#
# It runs inside the `sql-adapter` container so it uses the same env
# (host/db/user/pass).

VO=""
CONFIRM="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --vo)
      VO="${2:-}"
      shift 2
      ;;
    --yes)
      CONFIRM="true"
      shift
      ;;
    -h|--help)
      sed -n '1,24p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ "$CONFIRM" != "true" ]]; then
  echo "Refusing to run without --yes (this deletes CNR rows for one VO)." >&2
  exit 2
fi

if [[ -z "$VO" ]]; then
  echo "Provide --vo \"<owner>\"." >&2
  exit 2
fi

docker compose exec -T \
  -e TRUNCATE_VO="$VO" \
  sql-adapter python - <<'PY'
import os

import psycopg2

host = os.environ.get("CNR_POSTEGRESQL_HOST", "greendigit-postgresql.cloud.d4science.org")
db = os.environ.get("CNR_POSTEGRESQL_DB", "greendigit-db")
user = os.environ.get("CNR_POSTEGRESQL_USER", "greendigit-u")
port = int(os.environ.get("CNR_POSTEGRESQL_PORT", "5432"))
password = os.environ.get("CNR_POSTEGRESQL_PASSWORD")

vo = (os.environ.get("TRUNCATE_VO") or "").strip()

if not password:
    raise SystemExit("CNR_POSTEGRESQL_PASSWORD is not set in the sql-adapter env")

if not vo:
    raise SystemExit("Provide --vo")

dsn = f"dbname={db} user={user} host={host} password={password} port={port}"
print(f"[cnr-truncate-vo] connecting host={host} port={port} db={db} user={user}")
print(f"[cnr-truncate-vo] target vo={vo!r}")

with psycopg2.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM monitoring.fact_site_event f
            WHERE LOWER(COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown'))
                = LOWER(COALESCE(NULLIF(TRIM(%s), ''), 'Unknown'))
            """,
            (vo,),
        )
        fact_count = cur.fetchone()[0]

        if not fact_count:
            print("[cnr-truncate-vo] no matching rows found")
            raise SystemExit(0)

        cur.execute(
            """
            SELECT COUNT(DISTINCT f.site_id)
            FROM monitoring.fact_site_event f
            WHERE LOWER(COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown'))
                = LOWER(COALESCE(NULLIF(TRIM(%s), ''), 'Unknown'))
            """,
            (vo,),
        )
        site_count = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COALESCE(s.site_type::text, 'unknown'), COUNT(*)
            FROM monitoring.fact_site_event f
            LEFT JOIN monitoring.sites s ON s.site_id = f.site_id
            WHERE LOWER(COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown'))
                = LOWER(COALESCE(NULLIF(TRIM(%s), ''), 'Unknown'))
            GROUP BY 1
            ORDER BY 1
            """,
            (vo,),
        )
        activity_counts = cur.fetchall()

        cur.execute(
            """
            SELECT COUNT(*)
            FROM monitoring.detail_grid dg
            WHERE dg.event_id IN (
                SELECT f.event_id
                FROM monitoring.fact_site_event f
                WHERE LOWER(COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown'))
                    = LOWER(COALESCE(NULLIF(TRIM(%s), ''), 'Unknown'))
            )
            """,
            (vo,),
        )
        grid_count = cur.fetchone()[0]
        cur.execute(
            """
            SELECT COUNT(*)
            FROM monitoring.detail_cloud dc
            WHERE dc.event_id IN (
                SELECT f.event_id
                FROM monitoring.fact_site_event f
                WHERE LOWER(COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown'))
                    = LOWER(COALESCE(NULLIF(TRIM(%s), ''), 'Unknown'))
            )
               OR dc.site_id IN (
                SELECT f.site_id
                FROM monitoring.fact_site_event f
                WHERE LOWER(COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown'))
                    = LOWER(COALESCE(NULLIF(TRIM(%s), ''), 'Unknown'))
            )
            """,
            (vo, vo),
        )
        cloud_count = cur.fetchone()[0]
        cur.execute(
            """
            SELECT COUNT(*)
            FROM monitoring.detail_network dn
            WHERE dn.event_id IN (
                SELECT f.event_id
                FROM monitoring.fact_site_event f
                WHERE LOWER(COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown'))
                    = LOWER(COALESCE(NULLIF(TRIM(%s), ''), 'Unknown'))
            )
            """,
            (vo,),
        )
        network_count = cur.fetchone()[0]

        print(f"[cnr-truncate-vo] fact_site_event rows to delete: {fact_count}")
        print(f"[cnr-truncate-vo] distinct sites affected: {site_count}")
        print(f"[cnr-truncate-vo] detail_grid rows to delete: {grid_count}")
        print(f"[cnr-truncate-vo] detail_cloud rows to delete: {cloud_count}")
        print(f"[cnr-truncate-vo] detail_network rows to delete: {network_count}")
        for activity, count in activity_counts:
            print(f"[cnr-truncate-vo] activity={activity} fact_rows={count}")

        cur.execute(
            """
            DELETE FROM monitoring.detail_grid
            WHERE event_id IN (
                SELECT f.event_id
                FROM monitoring.fact_site_event f
                WHERE LOWER(COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown'))
                    = LOWER(COALESCE(NULLIF(TRIM(%s), ''), 'Unknown'))
            )
            """,
            (vo,),
        )
        cur.execute(
            """
            DELETE FROM monitoring.detail_cloud
            WHERE event_id IN (
                SELECT f.event_id
                FROM monitoring.fact_site_event f
                WHERE LOWER(COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown'))
                    = LOWER(COALESCE(NULLIF(TRIM(%s), ''), 'Unknown'))
            )
               OR site_id IN (
                SELECT f.site_id
                FROM monitoring.fact_site_event f
                WHERE LOWER(COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown'))
                    = LOWER(COALESCE(NULLIF(TRIM(%s), ''), 'Unknown'))
            )
            """,
            (vo, vo),
        )
        cur.execute(
            """
            DELETE FROM monitoring.detail_network
            WHERE event_id IN (
                SELECT f.event_id
                FROM monitoring.fact_site_event f
                WHERE LOWER(COALESCE(NULLIF(TRIM(f.owner), ''), 'Unknown'))
                    = LOWER(COALESCE(NULLIF(TRIM(%s), ''), 'Unknown'))
            )
            """,
            (vo,),
        )
        cur.execute(
            "DELETE FROM monitoring.fact_site_event "
            "WHERE LOWER(COALESCE(NULLIF(TRIM(owner), ''), 'Unknown')) "
            "    = LOWER(COALESCE(NULLIF(TRIM(%s), ''), 'Unknown'))",
            (vo,),
        )

        cur.execute(
            "SELECT COUNT(*) FROM monitoring.fact_site_event "
            "WHERE LOWER(COALESCE(NULLIF(TRIM(owner), ''), 'Unknown')) "
            "    = LOWER(COALESCE(NULLIF(TRIM(%s), ''), 'Unknown'))",
            (vo,),
        )
        remaining = cur.fetchone()[0]
        if remaining:
            raise SystemExit(f"Delete verification failed: {remaining} fact_site_event rows still remain for vo={vo!r}")

    conn.commit()
    conn.autocommit = True
    with conn.cursor() as cur:
        print("[cnr-truncate-vo] refreshing monitoring.mv_fact_site_event_15m_base")
        cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY monitoring.mv_fact_site_event_15m_base")
        print("[cnr-truncate-vo] refreshing monitoring.mv_reporting_resource_listing")
        cur.execute("REFRESH MATERIALIZED VIEW monitoring.mv_reporting_resource_listing")

print("[cnr-truncate-vo] done")
PY
