#!/usr/bin/env python3
"""Validate generated Mongo documents and CNR envelopes without writing databases."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


def dt(raw: str) -> datetime:
    value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def lines(path: Path):
    with path.open(encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            if line.strip():
                yield number, json.loads(line)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=Path)
    parser.add_argument("--start", default="2026-09-24T15:01:18.353800Z")
    parser.add_argument("--end", default="2026-09-25T09:01:21.136415Z")
    args = parser.parse_args()
    start, end = dt(args.start), dt(args.end)
    errors: list[str] = []
    mongo_counts = Counter()
    identities = set()
    mongo_path = args.folder / "mongo_documents.jsonl"
    for number, doc in lines(mongo_path):
        stamp = dt(doc["timestamp"])
        if not start <= stamp < end:
            errors.append(f"mongo line {number}: timestamp outside gap")
        if doc.get("group") != "greendigit" or doc.get("body", {}).get("group") != "greendigit":
            errors.append(f"mongo line {number}: missing greendigit group")
        if not doc.get("body", {}).get("_backfill", {}).get("estimated"):
            errors.append(f"mongo line {number}: missing estimated provenance")
        key = doc.get("body", {}).get("ExecUnitID") or doc.get("body", {}).get("JobID")
        if not key:
            errors.append(f"mongo line {number}: missing identifier")
        else:
            body = doc.get("body", {})
            identity = (
                key,
                body.get("StartExecTime"),
                body.get("StopExecTime") or body.get("EndExecTime"),
                body.get("Status"),
            )
            if identity in identities:
                errors.append(f"mongo line {number}: duplicate generated identifier/time tuple {identity}")
            identities.add(identity)
        mongo_counts[doc.get("publisher_email")] += 1

    envelope_counts = Counter()
    for path in sorted((args.folder / "postgres_envelopes").glob("envelopes_*.jsonl")):
        for number, env in lines(path):
            fact = env.get("fact_site_event") or {}
            if not fact.get("execunitid") or not fact.get("event_start_timestamp") or not fact.get("event_end_timestamp"):
                errors.append(f"{path.name} line {number}: missing required fact fields")
            envelope_counts[fact.get("publisher_email")] += 1

    result = {
        "ok": not errors,
        "mongo_records": sum(mongo_counts.values()),
        "postgres_envelopes": sum(envelope_counts.values()),
        "mongo_by_publisher": dict(sorted(mongo_counts.items())),
        "postgres_by_publisher": dict(sorted(envelope_counts.items())),
        "errors": errors[:100],
    }
    print(json.dumps(result, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
