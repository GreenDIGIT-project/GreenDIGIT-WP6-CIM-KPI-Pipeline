#!/usr/bin/env bash
set -euo pipefail

# Truncate all CNR monitoring tables written by the sql-adapter.
# This is destructive.
#
# Usage:
#   scripts/cnr-truncate-all.sh --yes
#
# It runs inside the `sql-adapter` container so it uses the same env (host/db/user/pass).

if [[ "${1:-}" != "--yes" ]]; then
  echo "Refusing to run without --yes (this deletes ALL CNR entries)." >&2
  exit 2
fi

docker compose exec -T sql-adapter python - <<'PY'
import os
import psycopg2

host = os.environ.get("CNR_POSTEGRESQL_HOST", "greendigit-postgresql.cloud.d4science.org")
db = os.environ.get("CNR_POSTEGRESQL_DB", "greendigit-db")
user = os.environ.get("CNR_POSTEGRESQL_USER", "greendigit-u")
port = int(os.environ.get("CNR_POSTEGRESQL_PORT", "5432"))
password = os.environ.get("CNR_POSTEGRESQL_PASSWORD")

if not password:
    raise SystemExit("CNR_POSTEGRESQL_PASSWORD is not set in the sql-adapter env")

dsn = f"dbname={db} user={user} host={host} password={password} port={port}"

print(f"[cnr-truncate] connecting host={host} port={port} db={db} user={user}")

with psycopg2.connect(dsn) as conn:
    with conn.cursor() as cur:
        # Counts before
        cur.execute("SELECT COUNT(*) FROM monitoring.fact_site_event")
        before = cur.fetchone()[0]
        print(f"[cnr-truncate] fact_site_event rows before: {before}")

        # Truncate the known tables. CASCADE handles FK order.
        cur.execute(
            "TRUNCATE "
            "monitoring.detail_grid, "
            "monitoring.detail_cloud, "
            "monitoring.detail_network, "
            "monitoring.fact_site_event, "
            "monitoring.sites, "
            "monitoring.site_type_detail "
            "RESTART IDENTITY CASCADE"
        )

        cur.execute("SELECT COUNT(*) FROM monitoring.fact_site_event")
        after = cur.fetchone()[0]
        print(f"[cnr-truncate] fact_site_event rows after: {after}")

print("[cnr-truncate] done")
PY
