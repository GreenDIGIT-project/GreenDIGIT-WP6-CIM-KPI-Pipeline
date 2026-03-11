import json
from datetime import datetime, timezone
from pathlib import Path

target = Path("$DUMP_BASE/01_mongo/metrics.jsonl")
if not target.exists():
    raise SystemExit(f"[batch_submit_cnr] could not find copied metrics.jsonl at {target}")

min_u = None
max_u = None
bad = 0
lines = 0
with target.open("r", encoding="utf-8") as f:
    for ln in f:
        s = ln.strip()
        if not s:
            continue
        lines += 1
        try:
            obj = json.loads(s)
            ts = obj.get("timestamp")
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc)
            u = int(dt.timestamp())
        except Exception:
            bad += 1
            continue
        min_u = u if min_u is None else min(min_u, u)
        max_u = u if max_u is None else max(max_u, u)

if min_u is None or max_u is None:
    raise SystemExit("[batch_submit_cnr] timestamp stats unavailable (no parseable timestamp)")

min_iso = datetime.fromtimestamp(min_u, tz=timezone.utc).isoformat().replace("+00:00", "Z")
max_iso = datetime.fromtimestamp(max_u, tz=timezone.utc).isoformat().replace("+00:00", "Z")
print(f"[batch_submit_cnr] mongoexport lines={lines} bad_ts={bad}")
print(f"[batch_submit_cnr] mongoexport min_ts={min_iso} ({min_u})")
print(f"[batch_submit_cnr] mongoexport max_ts={max_iso} ({max_u})")