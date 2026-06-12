#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

import psycopg2


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        os.environ.setdefault(key, value)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Look up CNR monitoring.sites rows by site description."
    )
    parser.add_argument("site_name", help="Exact monitoring.sites.description value")
    args = parser.parse_args()

    root_dir = Path(__file__).resolve().parents[2]
    load_dotenv(root_dir / ".env")

    dsn = (
        f"dbname={os.environ['CNR_GD_DB']} "
        f"user={os.environ['CNR_USER']} "
        f"host={os.environ['CNR_HOST']} "
        f"password={os.environ['CNR_POSTEGRESQL_PASSWORD']} "
        f"port={os.environ.get('CNR_POSTEGRESQL_PORT', '5432')}"
    )

    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT site_id, site_type::text, description
            FROM monitoring.sites
            WHERE description = %s
            ORDER BY site_id
            """,
            (args.site_name,),
        )
        rows = cur.fetchall()

    if not rows:
        print(f"No site found for description={args.site_name!r}")
        return 1

    for site_id, site_type, description in rows:
        print(f"{site_id}\t{site_type}\t{description}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
