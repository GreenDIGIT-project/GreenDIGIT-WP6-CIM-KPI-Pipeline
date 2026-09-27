#!/usr/bin/env python3
"""Dry-run-by-default, restartable group backfill for MongoDB and PostgreSQL."""

from __future__ import annotations

import argparse
import os
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any


DEFAULT_BATCH_SIZE = 100_000
MONGO_UNGROUPED = {
    "$or": [
        {"group": {"$exists": False}},
        {"group": None},
        {"group": ""},
    ]
}


def membership_map(db_path: Path) -> dict[str, set[str]]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT lower(u.email),g.name FROM users u "
            "JOIN user_groups ug ON ug.user_id=u.id "
            "JOIN groups g ON g.id=ug.group_id"
        ).fetchall()
    result: dict[str, set[str]] = {}
    for email, group in rows:
        result.setdefault(email, set()).add(group)
    return result


def classify(
    email: str | None, memberships: dict[str, set[str]]
) -> tuple[str | None, str]:
    groups = memberships.get((email or "").strip().lower(), set())
    if len(groups) > 1:
        return None, "ambiguous_multi_group_publisher"
    if groups == {"greendigit"}:
        return "greendigit", "greendigit_member"
    if not groups or groups == {"public"}:
        return "public", "public_default"
    return None, "ambiguous_private_group"


def _report_ambiguous(
    source: str,
    record_id: Any,
    email: str | None,
    reason: str,
    counts: Counter,
    example_limit: int,
) -> None:
    counts["ambiguous"] += 1
    if counts["ambiguous_examples"] < example_limit:
        print(
            f"AMBIGUOUS {source} id={record_id} "
            f"publisher={email!r} reason={reason}"
        )
        counts["ambiguous_examples"] += 1


def _mongo_query(before_id: Any | None) -> dict[str, Any]:
    if before_id is None:
        return MONGO_UNGROUPED
    return {"$and": [MONGO_UNGROUPED, {"_id": {"$lt": before_id}}]}


def _apply_mongo_batch(collection, planned: list[tuple[Any, str]]) -> None:
    if not planned:
        return
    try:
        from pymongo import UpdateOne
    except ImportError:  # Lightweight fallback used by unit tests.
        for document_id, group in planned:
            collection.update_one(
                {"_id": document_id, **MONGO_UNGROUPED},
                {"$set": {"group": group, "body.group": group}},
            )
        return

    operations = [
        UpdateOne(
            {"_id": document_id, **MONGO_UNGROUPED},
            {"$set": {"group": group, "body.group": group}},
        )
        for document_id, group in planned
    ]
    collection.bulk_write(operations, ordered=False)


def backfill_mongo(
    collection,
    memberships: dict[str, set[str]],
    apply: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    ambiguous_example_limit: int = 20,
    max_batches: int | None = None,
) -> Counter:
    """Process MongoDB newest-first by descending ObjectId, one batch at a time."""
    counts: Counter = Counter()
    before_id = None
    batch_number = 0

    while True:
        cursor = (
            collection.find(
                _mongo_query(before_id), {"_id": 1, "publisher_email": 1}
            )
            .sort("_id", -1)
            .limit(batch_size)
        )
        documents = list(cursor)
        if not documents:
            break

        batch_number += 1
        planned: list[tuple[Any, str]] = []
        for document in documents:
            email = document.get("publisher_email")
            group, reason = classify(email, memberships)
            counts["scanned"] += 1
            counts[reason] += 1
            if group:
                planned.append((document["_id"], group))
            else:
                _report_ambiguous(
                    "mongo",
                    document["_id"],
                    email,
                    reason,
                    counts,
                    ambiguous_example_limit,
                )

        if apply:
            _apply_mongo_batch(collection, planned)
            counts["updated"] += len(planned)

        before_id = documents[-1]["_id"]
        print(
            f"mongo batch={batch_number} scanned={counts['scanned']} "
            f"updated={counts['updated']} cursor_id={before_id}"
        )
        if max_batches is not None and batch_number >= max_batches:
            break

    counts.pop("ambiguous_examples", None)
    return counts


def backfill_postgres(
    conn,
    memberships: dict[str, set[str]],
    apply: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    ambiguous_example_limit: int = 20,
    max_batches: int | None = None,
) -> Counter:
    """Process PostgreSQL newest-first by descending event_id and commit per batch."""
    counts: Counter = Counter()
    before_event_id: int | None = None
    batch_number = 0

    while True:
        clauses = ["(group_name IS NULL OR btrim(group_name)='')"]
        params: list[Any] = []
        if before_event_id is not None:
            clauses.append("event_id < %s")
            params.append(before_event_id)
        params.append(batch_size)
        locking = " FOR UPDATE SKIP LOCKED" if apply else ""
        sql = (
            "SELECT event_id,publisher_email FROM monitoring.fact_site_event "
            f"WHERE {' AND '.join(clauses)} "
            f"ORDER BY event_id DESC LIMIT %s{locking}"
        )

        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
        if not rows:
            if not apply:
                conn.rollback()
            break

        batch_number += 1
        planned: list[tuple[int, str]] = []
        for event_id, email in rows:
            group, reason = classify(email, memberships)
            counts["scanned"] += 1
            counts[reason] += 1
            if group:
                planned.append((event_id, group))
            else:
                _report_ambiguous(
                    "postgres",
                    event_id,
                    email,
                    reason,
                    counts,
                    ambiguous_example_limit,
                )

        if apply:
            if planned:
                from psycopg2.extras import execute_values

                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        "UPDATE monitoring.fact_site_event AS f "
                        "SET group_name=v.group_name "
                        "FROM (VALUES %s) AS v(event_id,group_name) "
                        "WHERE f.event_id=v.event_id "
                        "AND (f.group_name IS NULL OR btrim(f.group_name)='')",
                        planned,
                        page_size=min(10_000, len(planned)),
                    )
            conn.commit()
            counts["updated"] += len(planned)
        else:
            # End each read transaction so a long dry run retains no snapshot.
            conn.rollback()

        before_event_id = rows[-1][0]
        print(
            f"postgres batch={batch_number} scanned={counts['scanned']} "
            f"updated={counts['updated']} cursor_event_id={before_event_id}"
        )
        if max_batches is not None and batch_number >= max_batches:
            break

    counts.pop("ambiguous_examples", None)
    return counts


def positive_int(raw: str) -> int:
    value = int(raw)
    if value < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return value


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    try:
        from dotenv import load_dotenv
    except ImportError as exc:
        raise SystemExit(
            "Missing local dependencies. Activate the virtualenv and run: "
            "python -m pip install -r migration/requirements.txt"
        ) from exc
    load_dotenv(root / ".env")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--auth-db", type=Path, default=root / "_auth_server/users.db"
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--skip-mongo", action="store_true")
    parser.add_argument("--skip-postgres", action="store_true")
    parser.add_argument(
        "--batch-size", type=positive_int, default=DEFAULT_BATCH_SIZE
    )
    parser.add_argument(
        "--ambiguous-example-limit", type=positive_int, default=20
    )
    parser.add_argument(
        "--max-batches",
        type=positive_int,
        help="Stop after this many batches per selected data store",
    )
    args = parser.parse_args()

    memberships = membership_map(args.auth_db)
    mode = "APPLY" if args.apply else "DRY RUN"
    print(
        f"mode={mode} batch_size={args.batch_size} "
        "order=newest-first (descending insertion id)"
    )

    if not args.skip_mongo:
        from pymongo import MongoClient

        client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
        try:
            collection = client[os.getenv("MONGO_DB", "metricsdb")][
                os.getenv("MONGO_COLLECTION", "metrics")
            ]
            counts = backfill_mongo(
                collection,
                memberships,
                args.apply,
                args.batch_size,
                args.ambiguous_example_limit,
                args.max_batches,
            )
            print("mongo:", dict(counts))
        finally:
            client.close()

    if not args.skip_postgres:
        import psycopg2

        conn = psycopg2.connect(
            host=os.environ["CNR_HOST"],
            port=os.getenv("CNR_POSTEGRESQL_PORT", "5432"),
            user=os.environ["CNR_USER"],
            password=os.environ["CNR_POSTEGRESQL_PASSWORD"],
            dbname=os.environ["CNR_GD_DB"],
        )
        try:
            counts = backfill_postgres(
                conn,
                memberships,
                args.apply,
                args.batch_size,
                args.ambiguous_example_limit,
                args.max_batches,
            )
            print("postgres:", dict(counts))
        finally:
            conn.close()


if __name__ == "__main__":
    main()
