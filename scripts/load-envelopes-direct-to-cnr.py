#!/usr/bin/env python3
"""
Load CNR envelopes JSONL (from process_dump.py) directly into the CNR Postgres DB.

This bypasses HTTP (cim-service/sql-adapter) but inserts rows "as expected" into:
  - monitoring.sites (+ monitoring.site_type_detail mapping)
  - monitoring.fact_site_event
  - monitoring.detail_{grid,cloud,network}

Env vars (supports both naming styles seen in this repo):
  - CNR_POSTEGRESQL_HOST or CNR_HOST
  - CNR_POSTEGRESQL_PORT (default 5432)
  - CNR_POSTEGRESQL_USER or CNR_USER
  - CNR_POSTEGRESQL_PASSWORD or CNR_POSTGRESQL_PASSWORD or CNR_PASSWORD
  - CNR_POSTEGRESQL_DB or CNR_GD_DB

Input: JSONL where each line is an envelope dict with keys:
  - sites.site_type
  - fact_site_event (dict)
  - detail_grid/detail_cloud/detail_network (dicts)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import psycopg2
import psycopg2.extras


def _env(*names: str, default: Optional[str] = None) -> Optional[str]:
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip() != "":
            return v
    return default


def _dsn_from_env() -> str:
    host = _env("CNR_POSTEGRESQL_HOST", "CNR_HOST")
    port = int(_env("CNR_POSTEGRESQL_PORT", default="5432") or "5432")
    user = _env("CNR_POSTEGRESQL_USER", "CNR_USER")
    password = _env("CNR_POSTEGRESQL_PASSWORD", "CNR_POSTGRESQL_PASSWORD", "CNR_PASSWORD")
    dbname = _env("CNR_POSTEGRESQL_DB", "CNR_GD_DB")

    missing = [k for k, v in [("host", host), ("user", user), ("password", password), ("dbname", dbname)] if not v]
    if missing:
        raise SystemExit(f"Missing DB env vars: {', '.join(missing)}")

    # Avoid hanging forever if the remote DB isn't reachable.
    return f"dbname={dbname} user={user} host={host} password={password} port={port} connect_timeout=10"


def iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for ln_no, ln in enumerate(f, start=1):
            s = ln.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except json.JSONDecodeError as e:
                raise SystemExit(f"{path}:{ln_no}: invalid JSON: {e}") from e
            if not isinstance(obj, dict):
                raise SystemExit(f"{path}:{ln_no}: expected JSON object, got {type(obj).__name__}")
            yield obj


def ensure_site_type_mapping(cur, site_type: str) -> str:
    mapping = {"cloud": "detail_cloud", "network": "detail_network", "grid": "detail_grid"}
    detail_table = mapping[site_type]
    cur.execute(
        "INSERT INTO monitoring.site_type_detail (site_type, detail_table_name) "
        "VALUES (%s::monitoring.site_type, %s) "
        "ON CONFLICT (site_type) DO NOTHING",
        (site_type, detail_table),
    )
    return detail_table


def get_or_create_site_id(cur, site_type: str, description: str) -> int:
    cur.execute(
        "SELECT s.site_id FROM monitoring.sites s "
        "WHERE s.site_type = %s::monitoring.site_type AND s.description = %s",
        (site_type, description),
    )
    row = cur.fetchone()
    if row:
        return int(row[0])
    cur.execute(
        "INSERT INTO monitoring.sites (site_type, description) "
        "VALUES (%s::monitoring.site_type, %s) RETURNING site_id",
        (site_type, description),
    )
    return int(cur.fetchone()[0])


FACT_COLS = [
    "event_start_timestamp",
    "event_end_timestamp",
    "job_finished",
    "CI_g",
    "CFP_g",
    "PUE",
    "site",
    "energy_wh",
    "work",
    "startexectime",
    "stopexectime",
    "status",
    "owner",
    "execunitid",
    "execunitfinished",
]


def _fact_tuple(site_id: int, fact: Dict[str, Any]) -> Tuple[Any, ...]:
    # Keep order aligned with insert statement below.
    return (site_id, *[fact.get(k) for k in FACT_COLS])


def _coalesce(fact: Dict[str, Any], *keys: str) -> Optional[Any]:
    for k in keys:
        v = fact.get(k)
        if v not in (None, ""):
            return v
    return None


def _parse_bool(v: Any) -> Optional[bool]:
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        try:
            return bool(int(v))
        except Exception:
            return None
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "done", "finished", "success", "succeeded"}:
        return True
    if s in {"0", "false", "no", "n", "running", "pending", "completing"}:
        return False
    return None


def normalise_fact_required_fields(fact: Dict[str, Any]) -> None:
    """
    Ensure NOT NULL timestamp fields required by the DB are populated.

    Partner payloads often omit StopExecTime, but cnr_transform still emits the
    key with value None. The DB schema requires startexectime/stopexectime.
    """
    start = _coalesce(fact, "startexectime", "event_start_timestamp", "event_start_time", "recorded_at")
    end = _coalesce(fact, "stopexectime", "event_end_timestamp", "event_end_times", "recorded_at")

    if start is None and end is not None:
        start = end
    if end is None and start is not None:
        end = start

    if start is not None and fact.get("startexectime") in (None, ""):
        fact["startexectime"] = start
    if end is not None and fact.get("stopexectime") in (None, ""):
        fact["stopexectime"] = end

    if start is not None and fact.get("event_start_timestamp") in (None, ""):
        fact["event_start_timestamp"] = start
    if end is not None and fact.get("event_end_timestamp") in (None, ""):
        fact["event_end_timestamp"] = end

    # execunitid is NOT NULL in fact_site_event.
    if fact.get("execunitid") in (None, ""):
        # Prefer the deterministic event_id present in the envelope (from cnr_transform).
        ev = fact.get("event_id")
        if ev not in (None, ""):
            fact["execunitid"] = f"missing:execunitid:event_id={ev}"
        else:
            # Last resort: stable-ish fallback.
            fact["execunitid"] = "missing:execunitid:event_id=unknown"

    # Required booleans in the DB schema.
    status = str(fact.get("status") or "").strip().lower()
    status_finished = status in {"done", "finished", "success", "succeeded"}
    status_unfinished = status in {"running", "pending", "completing"}

    execunitfinished = _parse_bool(fact.get("execunitfinished"))
    job_finished = _parse_bool(fact.get("job_finished"))

    if execunitfinished is None:
        if job_finished is not None:
            execunitfinished = job_finished
        elif status_finished:
            execunitfinished = True
        elif status_unfinished:
            execunitfinished = False
        else:
            execunitfinished = False

    if job_finished is None:
        job_finished = execunitfinished

    if fact.get("execunitfinished") in (None, ""):
        fact["execunitfinished"] = execunitfinished
    if fact.get("job_finished") in (None, ""):
        fact["job_finished"] = job_finished


def insert_fact_events_bulk(cur, rows: List[Tuple[Any, ...]]) -> List[int]:
    sql = (
        "INSERT INTO monitoring.fact_site_event "
        "(site_id,"
        + ",".join(FACT_COLS)
        + ") VALUES %s RETURNING event_id"
    )
    psycopg2.extras.execute_values(cur, sql, rows, page_size=min(5000, max(1, len(rows))))
    return [int(r[0]) for r in cur.fetchall()]


def insert_detail_bulk(cur, site_type: str, rows: List[Tuple[Any, ...]]) -> None:
    if not rows:
        return

    if site_type == "grid":
        cols = [
            "site_id",
            "event_id",
            "execunitid",
            "wallclocktime_s",
            "cpunormalizationfactor",
            "ncores",
            "normcputime_s",
            "efficiency",
            "tdp_w",
            "totalcputime_s",
            "scaledcputime_s",
        ]
        sql = "INSERT INTO monitoring.detail_grid (" + ",".join(cols) + ") VALUES %s"
    elif site_type == "cloud":
        cols = [
            "event_id",
            "site_id",
            "execunitid",
            "wallclocktime_s",
            "suspendduration_s",
            "cpuduration_s",
            "cpunormalizationfactor",
            "efficiency",
            "cloud_type",
            "compute_service",
        ]
        sql = "INSERT INTO monitoring.detail_cloud (" + ",".join(cols) + ") VALUES %s"
    elif site_type == "network":
        cols = [
            "site_id",
            "event_id",
            "execunitid",
            "amountofdatatransferred",
            "networktype",
            "measurementtype",
            "destinationexecunitid",
        ]
        sql = "INSERT INTO monitoring.detail_network (" + ",".join(cols) + ") VALUES %s"
    else:
        raise SystemExit(f"Unsupported site_type={site_type!r}")

    psycopg2.extras.execute_values(cur, sql, rows, page_size=min(5000, max(1, len(rows))))


def _detail_row(site_type: str, env: Dict[str, Any], site_id: int, event_id: int) -> Tuple[Any, ...]:
    fact = env.get("fact_site_event") or {}
    execunitid = (fact.get("execunitid") or "") if isinstance(fact, dict) else ""

    if site_type == "grid":
        d = env.get("detail_grid") or {}
        return (
            site_id,
            event_id,
            execunitid,
            d.get("wallclocktime_s"),
            d.get("cpunormalizationfactor"),
            d.get("ncores"),
            d.get("normcputime_s"),
            d.get("efficiency"),
            d.get("tdp_w"),
            d.get("totalcputime_s"),
            d.get("scaledcputime_s"),
        )
    if site_type == "cloud":
        d = env.get("detail_cloud") or {}
        return (
            event_id,
            site_id,
            execunitid,
            d.get("wallclocktime_s"),
            d.get("suspendduration_s"),
            d.get("cpuduration_s"),
            d.get("cpunormalizationfactor"),
            d.get("efficiency"),
            d.get("cloud_type"),
            d.get("compute_service"),
        )
    if site_type == "network":
        d = env.get("detail_network") or {}
        return (
            site_id,
            event_id,
            execunitid,
            d.get("amountofdatatransferred"),
            d.get("networktype"),
            d.get("measurementtype"),
            d.get("destinationexecunitid"),
        )
    raise SystemExit(f"Unsupported site_type={site_type!r}")


@dataclass
class BatchGroup:
    site_type: str
    envs: List[Dict[str, Any]]
    site_ids: List[int]


def load_files(paths: List[Path], batch_size: int, dry_run: bool) -> None:
    dsn = _dsn_from_env() if not dry_run else ""

    # Cache: (site_type, site_description) -> site_id
    site_cache: Dict[Tuple[str, str], int] = {}
    mapping_cache: Dict[str, str] = {}

    # In dry-run mode we don't need a DB connection; we still normalise and count.
    conn = None if dry_run else psycopg2.connect(dsn)
    try:
        if conn is not None:
            conn.autocommit = False
            cur_cm = conn.cursor()
        else:
            cur_cm = None

        if cur_cm is not None:
            cur = cur_cm
        else:
            cur = None

        if cur is None:
            # No DB operations; just parse/normalise and count.
            total_envs = 0
            total_inserted = 0
            for path in paths:
                for env in iter_jsonl(path):
                    total_envs += 1
                    sites = env.get("sites") or {}
                    site_type = sites.get("site_type") if isinstance(sites, dict) else None
                    if site_type not in ("grid", "cloud", "network"):
                        continue
                    fact = env.get("fact_site_event") or {}
                    if not isinstance(fact, dict):
                        continue
                    normalise_fact_required_fields(fact)
                    total_inserted += 1
                    if total_inserted % batch_size == 0:
                        print(f"inserted={total_inserted} seen={total_envs}", flush=True)
            print(f"inserted={total_inserted} seen={total_envs}", flush=True)
            return

        # DB mode below
        with cur:
            total_envs = 0
            total_inserted = 0

            def flush(groups: Dict[str, BatchGroup]) -> None:
                nonlocal total_inserted
                if not groups:
                    return

                for st, grp in groups.items():
                    if st not in mapping_cache:
                        mapping_cache[st] = ensure_site_type_mapping(cur, st)

                    # Build fact rows
                    fact_rows: List[Tuple[Any, ...]] = []
                    for env, site_id in zip(grp.envs, grp.site_ids):
                        fact = env.get("fact_site_event")
                        if not isinstance(fact, dict):
                            raise SystemExit("Envelope missing fact_site_event dict")
                        normalise_fact_required_fields(fact)
                        fact_rows.append(_fact_tuple(site_id, fact))

                    if dry_run:
                        total_inserted += len(fact_rows)
                        continue

                    event_ids = insert_fact_events_bulk(cur, fact_rows)

                    # Build detail rows aligned with returned event_ids
                    detail_rows = [_detail_row(st, env, site_id, ev_id) for env, site_id, ev_id in zip(grp.envs, grp.site_ids, event_ids)]
                    insert_detail_bulk(cur, st, detail_rows)

                    total_inserted += len(fact_rows)

                if dry_run:
                    conn.rollback()
                    # Rollback undoes any mapping/site inserts we did while scanning,
                    # so cached ids/mappings are no longer valid.
                    site_cache.clear()
                    mapping_cache.clear()
                else:
                    conn.commit()

            groups: Dict[str, BatchGroup] = {}

            for path in paths:
                for env in iter_jsonl(path):
                    total_envs += 1

                    sites = env.get("sites") or {}
                    site_type = None
                    if isinstance(sites, dict):
                        site_type = sites.get("site_type")
                    if site_type not in ("grid", "cloud", "network"):
                        continue  # ignore unknown payloads

                    # IMPORTANT: in the CNR schema, monitoring.sites.site_type has an FK to
                    # monitoring.site_type_detail(site_type). Ensure the mapping row exists
                    # before we try to insert into monitoring.sites.
                    if site_type not in mapping_cache:
                        mapping_cache[site_type] = ensure_site_type_mapping(cur, site_type)

                    fact = env.get("fact_site_event") or {}
                    if not isinstance(fact, dict):
                        continue
                    normalise_fact_required_fields(fact)

                    site_desc = fact.get("site")
                    if site_desc is None or str(site_desc).strip() == "":
                        site_desc = "unknown"
                    site_desc = str(site_desc)

                    key = (site_type, site_desc)
                    site_id = site_cache.get(key)
                    if site_id is None:
                        site_id = get_or_create_site_id(cur, site_type, site_desc)
                        site_cache[key] = site_id

                    grp = groups.get(site_type)
                    if grp is None:
                        grp = BatchGroup(site_type=site_type, envs=[], site_ids=[])
                        groups[site_type] = grp
                    grp.envs.append(env)
                    grp.site_ids.append(site_id)

                    if sum(len(g.envs) for g in groups.values()) >= batch_size:
                        flush(groups)
                        groups.clear()
                        print(f"inserted={total_inserted} seen={total_envs}", flush=True)

            flush(groups)
            groups.clear()
            print(f"inserted={total_inserted} seen={total_envs}", flush=True)

    finally:
        if conn is not None:
            conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Direct-load CNR envelopes JSONL into Postgres (no HTTP).")
    ap.add_argument("path", nargs="+", type=Path, help="One or more envelopes_*.jsonl files")
    ap.add_argument("--batch-size", type=int, default=5000, help="Envelopes per transaction (default: 5000)")
    ap.add_argument("--dry-run", action="store_true", help="Parse and count, but do not commit inserts")
    args = ap.parse_args()

    paths = [p for p in args.path]
    for p in paths:
        if not p.exists():
            raise SystemExit(f"Not found: {p}")

    load_files(paths, batch_size=max(1, args.batch_size), dry_run=bool(args.dry_run))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
