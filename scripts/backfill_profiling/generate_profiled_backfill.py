#!/usr/bin/env python3
"""Generate a reviewable, synthetic backfill from several historical windows.

The generator never writes to MongoDB or PostgreSQL.  It profiles individual
records in equal-length non-overlapping windows around the gap, takes the median count
per publisher/site/type/hour, and copies records from the closest-volume donor
window.  Event/submission times are shifted into the target gap and identifiers
are deterministically replaced to avoid collisions with the donor records.
"""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import re
import statistics
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from pymongo import MongoClient


UTC = timezone.utc
RUN_ID = "20260927_backfill_profiled"
ID_KEYS = {
    "execunitid", "jobid", "vmuuid", "destinationexecunitid",
    "workloadid", "workload_id",
}
TIME_KEYS = {
    "timestamp", "ts", "submissiontime", "submittime", "recordedat",
    "startexectime", "stopexectime", "endexectime",
    "starttime", "endtime", "eventstart", "eventend",
    "eventstarttimestamp", "eventendtimestamp",
}
NAMESPACE = uuid.UUID("b26c6a18-0f36-5b29-a4c4-42c97b60d527")
NORMALISED_ID_KEYS = {re.sub(r"[^a-z0-9]", "", key.lower()) for key in ID_KEYS}
NORMALISED_TIME_KEYS = {re.sub(r"[^a-z0-9]", "", key.lower()) for key in TIME_KEYS}


def parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def iso_micro(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat(timespec="microseconds")


def iso_z(dt: datetime, microseconds: bool = False) -> str:
    spec = "microseconds" if microseconds else "seconds"
    return dt.astimezone(UTC).isoformat(timespec=spec).replace("+00:00", "Z")


def normalise_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", key.lower())


def first_value(entry: dict[str, Any], *keys: str) -> Any:
    by_norm = {normalise_key(str(k)): v for k, v in entry.items()}
    for key in keys:
        value = by_norm.get(normalise_key(key))
        if value not in (None, ""):
            return value
    return None


def site_of(entry: dict[str, Any]) -> str | None:
    value = first_value(entry, "SiteGOCDB", "SiteName", "Site")
    return str(value).strip() if value not in (None, "") else None


def kind_of(entry: dict[str, Any]) -> str:
    keys = {normalise_key(str(k)) for k in entry}
    if keys & {"amountofdatatransferred", "networktype", "measurementtype", "destinationexecunitid"}:
        return "network"
    if keys & {"cloudtype", "cloudcomputeservice", "cpudurations", "suspenddurations"}:
        return "cloud"
    return "grid"


def eligible(entry: Any) -> bool:
    """Keep only entries the CNR converter can timestamp and identify."""
    if not isinstance(entry, dict) or not site_of(entry):
        return False
    identifier = first_value(entry, "ExecUnitID", "JobID")
    start = first_value(entry, "StartExecTime", "SubmissionTime")
    end = first_value(entry, "StopExecTime", "EndExecTime")
    return identifier not in (None, "") and (parse_dt(start) is not None or parse_dt(end) is not None)


def entries(body: Any) -> Iterable[dict[str, Any]]:
    if isinstance(body, dict):
        if eligible(body):
            yield body
    elif isinstance(body, list):
        for item in body:
            if eligible(item):
                yield item


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_") or "unknown"


@dataclass(frozen=True, order=True)
class Group:
    publisher: str
    site: str
    kind: str


def group_for(publisher: str, entry: dict[str, Any]) -> Group:
    return Group(publisher.lower(), site_of(entry) or "unknown", kind_of(entry))


def shifted_identifier(publisher: str, raw: Any, delta: timedelta) -> str:
    token = f"{RUN_ID}|{publisher.lower()}|{raw}|{int(delta.total_seconds())}"
    return f"bf-{RUN_ID}-{uuid.uuid5(NAMESPACE, token)}"


def shift_tree(value: Any, delta: timedelta, publisher: str, key: str = "") -> Any:
    if isinstance(value, dict):
        return {k: shift_tree(v, delta, publisher, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [shift_tree(v, delta, publisher, key) for v in value]

    nk = normalise_key(key)
    if nk in NORMALISED_ID_KEYS and value not in (None, "", 0):
        return shifted_identifier(publisher, value, delta)

    if nk in NORMALISED_TIME_KEYS:
        dt = parse_dt(value)
        if dt is not None:
            return iso_z(dt + delta, microseconds=dt.microsecond != 0)
        if isinstance(value, (int, float)) and value > 1_000_000_000:
            return type(value)(value + int(delta.total_seconds()))
    return value


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mongo-uri",
        default="mongodb://127.0.0.1:27017/?directConnection=true&readPreference=secondaryPreferred",
    )
    parser.add_argument("--db", default="metricsdb")
    parser.add_argument("--collection", default="metrics")
    parser.add_argument("--start", default="2026-09-24T15:01:18.353800Z")
    parser.add_argument("--end", default="2026-09-25T09:01:21.136415Z")
    parser.add_argument("--history-days", type=int, default=7)
    parser.add_argument("--future-days", type=int, default=2)
    parser.add_argument("--min-active-days", type=int, default=3)
    parser.add_argument("--slot-seconds", type=int, default=3600)
    parser.add_argument("--group", default="greendigit")
    parser.add_argument("--out", type=Path, default=Path("backfill") / RUN_ID)
    args = parser.parse_args()

    target_start = parse_dt(args.start)
    target_end = parse_dt(args.end)
    if target_start is None or target_end is None or target_start >= target_end:
        raise SystemExit("Invalid --start/--end")
    if args.history_days < 3 or args.future_days < 0 or args.min_active_days < 1:
        raise SystemExit("Use at least 3 history days and a positive active-day threshold")

    out = args.out.resolve()
    payload_dir = out / "mongo_api_payloads"
    postgres_dir = out / "postgres_envelopes"
    out.mkdir(parents=True, exist_ok=True)
    payload_dir.mkdir(parents=True, exist_ok=True)
    postgres_dir.mkdir(parents=True, exist_ok=True)
    for stale in payload_dir.glob("*.jsonl"):
        stale.unlink()
    for stale in postgres_dir.glob("*.jsonl"):
        stale.unlink()

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=10_000)
    collection = client[args.db][args.collection]
    client.admin.command("ping")

    duration = target_end - target_start
    slot_delta = timedelta(seconds=args.slot_seconds)
    slot_count = (int(duration.total_seconds()) + args.slot_seconds - 1) // args.slot_seconds
    comparison_offsets = list(range(1, args.history_days + 1)) + list(range(-1, -args.future_days - 1, -1))

    # counts[history_day][slot][group] = eligible entry count
    counts: dict[int, dict[int, Counter[Group]]] = defaultdict(lambda: defaultdict(Counter))
    active_days: dict[Group, set[int]] = defaultdict(set)
    query_projection = {"timestamp": 1, "publisher_email": 1, "body": 1, "group": 1}

    for day in comparison_offsets:
        donor_start = target_start - timedelta(days=day)
        donor_end = donor_start + duration
        query = {"timestamp": {"$gte": iso_micro(donor_start), "$lt": iso_micro(donor_end)}}
        cursor = collection.find(query, query_projection).sort("timestamp", 1)
        for doc in cursor:
            doc_dt = parse_dt(doc.get("timestamp"))
            publisher = str(doc.get("publisher_email") or "").strip().lower()
            if doc_dt is None or not publisher:
                continue
            slot = min(slot_count - 1, max(0, int((doc_dt - donor_start).total_seconds() // args.slot_seconds)))
            for entry in entries(doc.get("body")):
                group = group_for(publisher, entry)
                counts[day][slot][group] += 1
                active_days[group].add(day)

    usual_groups = {
        g
        for g, days in active_days.items()
        if len(days) >= args.min_active_days or ({1, -1} <= days)
    }
    plan: dict[tuple[int, Group], tuple[int, int, list[int]]] = {}
    profile_rows: list[dict[str, Any]] = []
    for slot in range(slot_count):
        for group in sorted(usual_groups):
            series = [counts[day][slot][group] for day in comparison_offsets]
            # A feed introduced during the baseline should not be treated as absent
            # before its first active day.  The group must still satisfy
            # --min-active-days, but its hourly median is taken over active days.
            active_series = [counts[day][slot][group] for day in sorted(active_days[group])]
            expected = int(statistics.median(active_series))
            if expected <= 0:
                continue
            donor_day = min(
                comparison_offsets,
                key=lambda day: (abs(counts[day][slot][group] - expected), -counts[day][slot][group], day),
            )
            available = counts[donor_day][slot][group]
            generated = min(expected, available)
            if generated <= 0:
                continue
            plan[(slot, group)] = (donor_day, generated, series)
            profile_rows.append({
                "slot": slot,
                "target_slot_start": iso_z(target_start + slot * slot_delta, True),
                "target_slot_end": iso_z(min(target_end, target_start + (slot + 1) * slot_delta), True),
                "publisher_email": group.publisher,
                "site": group.site,
                "kind": group.kind,
                "active_history_days": len(active_days[group]),
                "history_counts": ",".join(str(v) for v in series),
                "median_expected": expected,
                "donor_offset_days": donor_day,
                "generated": generated,
            })

    # Cadences such as six-hour network bursts drift across hour boundaries.
    # If an otherwise-usual group has no hourly plan, profile the whole gap-sized
    # window and select one complete closest-volume donor window.
    planned_groups = {group for (_slot, group) in plan}
    for group in sorted(usual_groups - planned_groups):
        series = [sum(counts[day][slot][group] for slot in range(slot_count)) for day in comparison_offsets]
        active_series = [
            sum(counts[day][slot][group] for slot in range(slot_count))
            for day in sorted(active_days[group])
        ]
        expected = int(statistics.median(active_series))
        if expected <= 0:
            continue
        donor_day = min(
            comparison_offsets,
            key=lambda day: (abs(sum(counts[day][slot][group] for slot in range(slot_count)) - expected), day),
        )
        available = sum(counts[donor_day][slot][group] for slot in range(slot_count))
        generated = min(expected, available)
        if generated <= 0:
            continue
        plan[(-1, group)] = (donor_day, generated, series)
        profile_rows.append({
            "slot": -1,
            "target_slot_start": iso_z(target_start, True),
            "target_slot_end": iso_z(target_end, True),
            "publisher_email": group.publisher,
            "site": group.site,
            "kind": group.kind,
            "active_history_days": len(active_days[group]),
            "history_counts": ",".join(str(v) for v in series),
            "median_expected": expected,
            "donor_offset_days": donor_day,
            "generated": generated,
        })

    mongo_path = out / "mongo_documents.jsonl"
    payload_handles: dict[str, Any] = {}
    emitted: Counter[tuple[int, Group]] = Counter()
    totals_by_group: Counter[Group] = Counter()
    total = 0
    with mongo_path.open("w", encoding="utf-8") as mongo_out:
        for day in comparison_offsets:
            donor_start = target_start - timedelta(days=day)
            donor_end = donor_start + duration
            delta = timedelta(days=day)
            query = {"timestamp": {"$gte": iso_micro(donor_start), "$lt": iso_micro(donor_end)}}
            cursor = collection.find(query, query_projection).sort("timestamp", 1)
            for doc in cursor:
                doc_dt = parse_dt(doc.get("timestamp"))
                publisher = str(doc.get("publisher_email") or "").strip().lower()
                if doc_dt is None or not publisher:
                    continue
                slot = min(slot_count - 1, max(0, int((doc_dt - donor_start).total_seconds() // args.slot_seconds)))
                target_doc_ts = doc_dt + delta
                if not (target_start <= target_doc_ts < target_end):
                    continue
                for entry in entries(doc.get("body")):
                    group = group_for(publisher, entry)
                    plan_key = (slot, group) if (slot, group) in plan else (-1, group)
                    selected = plan.get(plan_key)
                    if selected is None or selected[0] != day or emitted[plan_key] >= selected[1]:
                        continue
                    shifted = shift_tree(copy.deepcopy(entry), delta, publisher)
                    shifted["group"] = args.group
                    shifted["_backfill"] = {
                        "run_id": RUN_ID,
                        "estimated": True,
                        "method": "median-profile-nearest-donor",
                        "donor_submission_timestamp": str(doc.get("timestamp")),
                        "history_days": args.history_days,
                        "future_days": args.future_days,
                    }
                    mongo_doc = {
                        "publisher_email": publisher,
                        "group": args.group,
                        "timestamp": iso_micro(target_doc_ts),
                        "body": shifted,
                    }
                    mongo_out.write(json.dumps(mongo_doc, separators=(",", ":"), default=str) + "\n")
                    handle = payload_handles.get(publisher)
                    if handle is None:
                        handle = (payload_dir / f"{slug(publisher)}.jsonl").open("w", encoding="utf-8")
                        payload_handles[publisher] = handle
                    handle.write(json.dumps(shifted, separators=(",", ":"), default=str) + "\n")
                    emitted[plan_key] += 1
                    totals_by_group[group] += 1
                    total += 1

    for handle in payload_handles.values():
        handle.close()

    profile_csv = out / "profile.csv"
    fields = list(profile_rows[0]) if profile_rows else []
    with profile_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        if fields:
            writer.writeheader()
            writer.writerows(profile_rows)

    summary = {
        "run_id": RUN_ID,
        "generated_at": iso_z(datetime.now(UTC), True),
        "target_gap": {"start": iso_z(target_start, True), "end": iso_z(target_end, True), "seconds": duration.total_seconds()},
        "method": "For each one-hour target slot and publisher/site/type, take the median eligible-record count over seven preceding and two following non-overlapping windows and clone the closest-volume donor slot.",
        "history_days": args.history_days,
        "future_days": args.future_days,
        "comparison_offsets_days": comparison_offsets,
        "minimum_active_days": args.min_active_days,
        "continuity_exception": "Groups active in both the immediately preceding (+1) and following (-1) windows are included even if they have fewer than minimum_active_days.",
        "slot_seconds": args.slot_seconds,
        "usual_groups": len(usual_groups),
        "planned_groups_slots": len(plan),
        "generated_records": total,
        "generated_by_publisher_site_type": [
            {"publisher_email": g.publisher, "site": g.site, "kind": g.kind, "records": n}
            for g, n in sorted(totals_by_group.items())
        ],
        "excluded": "Entries without a canonical site, ExecUnitID/JobID, or parseable execution timestamp are not synthesized.",
        "files": {},
    }
    summary_path = out / "profile_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    candidates = [mongo_path, profile_csv, summary_path, *sorted(payload_dir.glob("*.jsonl"))]
    manifest = {
        "run_id": RUN_ID,
        "files": [
            {"path": str(path.relative_to(out)), "bytes": path.stat().st_size, "sha256": sha256(path)}
            for path in candidates
        ],
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(out), "generated_records": total, "profile_rows": len(profile_rows)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
