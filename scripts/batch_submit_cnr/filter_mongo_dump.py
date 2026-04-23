#!/usr/bin/env python3
"""Stream-filter a Mongo JSONL export by raw body fields.

Designed for very large dumps: reads one line at a time and writes matching
documents back out as JSONL without loading the source file into memory.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable, Optional


def _normalize(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _matches_exact(candidate: Any, allowed: set[str], *, ignore_case: bool) -> bool:
    value = _normalize(candidate)
    if value is None:
        return False
    if ignore_case:
        return value.casefold() in allowed
    return value in allowed


def _iter_jsonl(path: Path) -> Iterable[tuple[int, dict[str, Any]]]:
    with path.open("r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, 1):
            line = raw.strip()
            if not line:
                continue
            obj = json.loads(line)
            if not isinstance(obj, dict):
                continue
            yield line_no, obj


def _matches_field(body: dict[str, Any], field: str, allowed: set[str], *, ignore_case: bool) -> tuple[bool, Optional[str]]:
    value = _normalize(body.get(field))
    if value is None:
        return False, None
    return _matches_exact(value, allowed, ignore_case=ignore_case), value


def main() -> int:
    ap = argparse.ArgumentParser(description="Filter a large Mongo JSONL export by body.Owner and/or body.Site.")
    ap.add_argument("dump", type=Path, help="Path to source JSONL dump.")
    ap.add_argument(
        "--owner",
        action="append",
        default=[],
        help="Exact Owner value to keep. Repeatable.",
    )
    ap.add_argument(
        "--site",
        action="append",
        default=[],
        help="Exact Site value to keep. Repeatable.",
    )
    ap.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Output JSONL path for matching documents.",
    )
    ap.add_argument(
        "--summary-out",
        type=Path,
        default=None,
        help="Optional JSON summary output path.",
    )
    ap.add_argument(
        "--ignore-case",
        action="store_true",
        help="Case-insensitive Owner comparison.",
    )
    ap.add_argument(
        "--progress-every",
        type=int,
        default=200000,
        help="Log progress every N scanned lines. Use 0 to disable.",
    )
    args = ap.parse_args()

    owners_raw = [_normalize(v) for v in args.owner]
    owners = [v for v in owners_raw if v is not None]
    sites_raw = [_normalize(v) for v in args.site]
    sites = [v for v in sites_raw if v is not None]
    if not owners and not sites:
        raise SystemExit("Provide at least one --owner and/or --site value.")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    if args.summary_out is not None:
        args.summary_out.parent.mkdir(parents=True, exist_ok=True)

    owner_allowed = {v.casefold() for v in owners} if args.ignore_case else set(owners)
    site_allowed = {v.casefold() for v in sites} if args.ignore_case else set(sites)
    owner_counts = {v: 0 for v in owners}
    site_counts = {v: 0 for v in sites}

    scanned = 0
    matched = 0
    bad_json = 0
    missing_body = 0
    missing_owner = 0
    missing_site = 0

    with args.dump.open("r", encoding="utf-8") as src, args.out.open("w", encoding="utf-8") as dst:
        for line_no, raw in enumerate(src, 1):
            line = raw.strip()
            if not line:
                continue
            scanned += 1
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                bad_json += 1
                continue
            if not isinstance(obj, dict):
                missing_body += 1
                continue
            body = obj.get("body")
            if not isinstance(body, dict):
                missing_body += 1
                continue
            owner_match = not owners
            site_match = not sites
            owner = None
            site = None

            if owners:
                owner_match, owner = _matches_field(body, "Owner", owner_allowed, ignore_case=args.ignore_case)
                if owner is None:
                    missing_owner += 1
            if sites:
                site_match, site = _matches_field(body, "Site", site_allowed, ignore_case=args.ignore_case)
                if site is None:
                    missing_site += 1

            if not owner_match or not site_match:
                continue

            dst.write(json.dumps(obj, separators=(",", ":")) + "\n")
            matched += 1

            if owner is not None:
                for configured in owners:
                    if args.ignore_case:
                        if owner.casefold() == configured.casefold():
                            owner_counts[configured] += 1
                    elif owner == configured:
                        owner_counts[configured] += 1
            if site is not None:
                for configured in sites:
                    if args.ignore_case:
                        if site.casefold() == configured.casefold():
                            site_counts[configured] += 1
                    elif site == configured:
                        site_counts[configured] += 1

            if args.progress_every > 0 and scanned % args.progress_every == 0:
                print(
                    f"[filter_mongo_dump] scanned={scanned} matched={matched} "
                    f"bad_json={bad_json} missing_body={missing_body} "
                    f"missing_owner={missing_owner} missing_site={missing_site}",
                    flush=True,
                )

    summary = {
        "dump": str(args.dump),
        "out": str(args.out),
        "owners": owners,
        "sites": sites,
        "ignore_case": args.ignore_case,
        "scanned_docs": scanned,
        "matched_docs": matched,
        "bad_json_docs": bad_json,
        "missing_body_docs": missing_body,
        "missing_owner_docs": missing_owner,
        "missing_site_docs": missing_site,
        "matched_per_owner": owner_counts,
        "matched_per_site": site_counts,
    }

    if args.summary_out is not None:
        args.summary_out.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
