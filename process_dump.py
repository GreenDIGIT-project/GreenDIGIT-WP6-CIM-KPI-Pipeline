#!/usr/bin/env python3
"""Offline dump processor.

Reads a Mongo export (JSONL or JSON array/object) and converts stored `body` payloads
into CNR envelopes using the same CNR transform logic as cim-service.

Output is JSONL of envelopes (one per metric entry) so you can bulk-load later.

Typical mongoexport JSONL line looks like:
  {"timestamp":"...","publisher_email":"...","body":{...}}

```bash
python3 process_dump.py /path/to/mongoexport.jsonl \
  --emails 'atsareg@in2p3.fr,kostashn@gmail.com' \
  --start '2025-08-01T00:00:00Z' \
  --end   "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --out-dir analysis/dump_processed
```


We also support input where each line is directly a metric entry (body).

This script does NOT call KPI-service (no network enrichment).
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple


def _to_iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_dt(raw: str) -> Optional[datetime]:
    s = str(raw).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return _to_iso_z(value)
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [jsonable(v) for v in value]
    return value


def _infer_times(fact: Dict[str, Any]) -> Tuple[datetime, datetime]:
    """Best-effort start/stop for required SQL adapter fields."""
    start_raw = (
        fact.get("startexectime")
        or fact.get("event_start_timestamp")
        or fact.get("event_start_time")
        or fact.get("event_start")
    )
    stop_raw = (
        fact.get("stopexectime")
        or fact.get("event_end_timestamp")
        or fact.get("event_end_times")
        or fact.get("event_end")
    )

    def _coerce_dt(v: Any) -> Optional[datetime]:
        if v is None:
            return None
        if isinstance(v, datetime):
            return _ensure_utc(v)
        try:
            return _ensure_utc(datetime.fromisoformat(str(v).replace("Z", "+00:00")))
        except Exception:
            return None

    start = _coerce_dt(start_raw)
    stop = _coerce_dt(stop_raw)

    # If one side is missing, keep the one we have and set the other to the same value.
    if start is None and stop is not None:
        stop = stop.replace(microsecond=0)
        return stop, stop
    if stop is None and start is not None:
        start = start.replace(microsecond=0)
        return start, start
    if start is None and stop is None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        return now, now

    return start.replace(microsecond=0), stop.replace(microsecond=0)


def _load_cnr_converter():
    # Import cnr_transform from ./_cim without requiring it to be a package.
    repo_root = Path(__file__).resolve().parent
    cim_dir = repo_root / "_cim"
    sys.path.insert(0, str(cim_dir))
    from cnr_transform import CNRConverter  # type: ignore

    return CNRConverter


@dataclass
class InputDoc:
    publisher_email: Optional[str]
    timestamp: Optional[str]
    body: Any


def iter_input_docs(path: Path) -> Iterator[InputDoc]:
    """Yield InputDoc from JSONL or JSON (array/object)."""
    text = path.read_text(encoding="utf-8")

    # Heuristic: JSONL if multiple lines each starting with '{'
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if len(lines) > 1 and all(ln.lstrip().startswith("{") for ln in lines[: min(5, len(lines))]):
        for ln in lines:
            obj = json.loads(ln)
            if isinstance(obj, dict) and "body" in obj:
                yield InputDoc(
                    publisher_email=(obj.get("publisher_email") or obj.get("publisher") or None),
                    timestamp=(obj.get("timestamp") or obj.get("ts") or None),
                    body=obj.get("body"),
                )
            else:
                # Treat as direct metric entry
                yield InputDoc(publisher_email=None, timestamp=None, body=obj)
        return

    # Otherwise treat as JSON
    obj = json.loads(text)
    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict) and "body" in item:
                yield InputDoc(
                    publisher_email=(item.get("publisher_email") or item.get("publisher") or None),
                    timestamp=(item.get("timestamp") or item.get("ts") or None),
                    body=item.get("body"),
                )
            else:
                yield InputDoc(publisher_email=None, timestamp=None, body=item)
        return

    if isinstance(obj, dict) and "body" in obj:
        yield InputDoc(
            publisher_email=(obj.get("publisher_email") or obj.get("publisher") or None),
            timestamp=(obj.get("timestamp") or obj.get("ts") or None),
            body=obj.get("body"),
        )
        return

    # Fallback: treat as direct metric entry
    yield InputDoc(publisher_email=None, timestamp=None, body=obj)


def slugify(email: str) -> str:
    out = []
    for ch in email.lower():
        out.append(ch if ch.isalnum() else "_")
    return "".join(out).strip("_")


def build_envelope(payload_type: str, fact: Dict[str, Any], detail_table: str, detail: Dict[str, Any]) -> Dict[str, Any]:
    env: Dict[str, Any] = {
        "site": fact.get("site"),
        "duration_s": None,
        "sites": {"site_type": payload_type},
        "fact_site_event": fact,
        "detail_table": detail_table,
        "detail_cloud": {},
        "detail_grid": {},
        "detail_network": {},
    }
    env[detail_table] = detail

    # Ensure required timestamps for the SQL adapter schema.
    start_dt, stop_dt = _infer_times(fact)
    start_iso = _to_iso_z(start_dt)
    stop_iso = _to_iso_z(stop_dt)

    def _put(k: str, v: str) -> None:
        if k not in fact or fact.get(k) in (None, ""):
            fact[k] = v

    def _putv(k: str, v: Any) -> None:
        if k not in fact or fact.get(k) in (None, ""):
            fact[k] = v

    _put("event_start_timestamp", start_iso)
    _put("event_end_timestamp", stop_iso)
    _put("event_start_time", start_iso)
    _put("event_end_times", stop_iso)
    _put("startexectime", start_iso)
    _put("stopexectime", stop_iso)

    # DB-level NOT NULL booleans; keep them consistent.
    if fact.get("execunitfinished") in (None, ""):
        # Prefer job_finished if present; else infer from status.
        jf = fact.get("job_finished")
        if isinstance(jf, bool):
            fact["execunitfinished"] = jf
        else:
            st = str(fact.get("status") or "").strip().lower()
            fact["execunitfinished"] = st in {"done", "finished", "success", "succeeded"}

    if fact.get("job_finished") in (None, ""):
        euf = fact.get("execunitfinished")
        fact["job_finished"] = bool(euf) if isinstance(euf, bool) else False

    # DB-level NOT NULL execunitid: prefer explicit sentinel if partner omitted it.
    if fact.get("execunitid") in (None, ""):
        ev = fact.get("event_id")
        if ev not in (None, ""):
            fact["execunitid"] = f"missing:execunitid:event_id={ev}"
        else:
            fact["execunitid"] = "missing:execunitid:event_id=unknown"

    return jsonable(env)


def main() -> int:
    ap = argparse.ArgumentParser(description="Convert Mongo export to CNR envelopes (JSONL).")
    ap.add_argument("dump", type=Path, help="Path to mongoexport file (JSONL or JSON).")
    ap.add_argument("--out-dir", type=Path, default=Path("analysis/dump_processed"))
    ap.add_argument("--emails", type=str, default="", help="Comma-separated publisher_email filter (optional).")
    ap.add_argument("--start", type=str, default="", help="Filter by document timestamp >= start (ISO).")
    ap.add_argument("--end", type=str, default="", help="Filter by document timestamp <= end (ISO).")

    args = ap.parse_args()

    emails: Optional[set[str]] = None
    if args.emails.strip():
        emails = {e.strip().lower() for e in args.emails.split(",") if e.strip()}

    start_dt = _parse_iso_dt(args.start) if args.start else None
    end_dt = _parse_iso_dt(args.end) if args.end else None

    args.out_dir.mkdir(parents=True, exist_ok=True)

    CNRConverter = _load_cnr_converter()
    converter = CNRConverter()

    by_email_out: Dict[str, Any] = {}

    total_docs = 0
    total_entries = 0
    total_envelopes = 0
    total_errors = 0

    for doc in iter_input_docs(args.dump):
        total_docs += 1
        pub = doc.publisher_email.lower() if isinstance(doc.publisher_email, str) else None

        if emails is not None:
            if pub is None or pub not in emails:
                continue

        if doc.timestamp and (start_dt or end_dt):
            ts_dt = _parse_iso_dt(doc.timestamp)
            if ts_dt is None:
                # Skip if timestamp filter requested and we can't parse
                continue
            if start_dt is not None and ts_dt < start_dt:
                continue
            if end_dt is not None and ts_dt > end_dt:
                continue

        key = pub or "unknown"
        if key not in by_email_out:
            out_path = args.out_dir / f"envelopes_{slugify(key)}.jsonl"
            err_path = args.out_dir / f"errors_{slugify(key)}.jsonl"
            by_email_out[key] = {
                "out": out_path.open("w", encoding="utf-8"),
                "err": err_path.open("w", encoding="utf-8"),
                "out_path": out_path,
                "err_path": err_path,
                "envelopes": 0,
                "errors": 0,
            }

        sink = by_email_out[key]

        body = doc.body
        total_entries += 1

        try:
            recs = converter.convert(body)
        except Exception as exc:
            sink["errors"] += 1
            total_errors += 1
            sink["err"].write(json.dumps({"error": str(exc), "publisher_email": pub, "timestamp": doc.timestamp}) + "\n")
            continue

        for rec in recs:
            try:
                env = build_envelope(rec.payload_type, dict(rec.fact_site_event), rec.detail_table, dict(rec.detail_row))
            except Exception as exc:
                sink["errors"] += 1
                total_errors += 1
                sink["err"].write(
                    json.dumps({"error": str(exc), "publisher_email": pub, "timestamp": doc.timestamp, "raw": rec.raw}) + "\n"
                )
                continue

            sink["out"].write(json.dumps(env, separators=(",", ":")) + "\n")
            sink["envelopes"] += 1
            total_envelopes += 1

    for sink in by_email_out.values():
        sink["out"].close()
        sink["err"].close()

    summary = {
        "dump": str(args.dump),
        "out_dir": str(args.out_dir),
        "docs_seen": total_docs,
        "entries_seen": total_entries,
        "envelopes_written": total_envelopes,
        "errors": total_errors,
        "per_publisher": {
            k: {
                "envelopes": v["envelopes"],
                "errors": v["errors"],
                "out": str(v["out_path"]),
                "errors_out": str(v["err_path"]),
            }
            for k, v in by_email_out.items()
        },
    }
    (args.out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
