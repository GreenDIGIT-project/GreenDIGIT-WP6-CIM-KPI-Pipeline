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

This script can enrich missing CI/PUE via KPI-service with local caches.

CFP policy for offline consistency:
- If CFP is missing or 0.0, recompute when CI_g + PUE + energy_wh are valid.
- If CI/PUE are not available, set CFP_g to null (SQL NULL via JSON null), never to fallback 0.0.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse
import urllib.error
import urllib.request


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
    # Import cnr_transform from repo-level ./_cim without requiring a package install.
    script_path = Path(__file__).resolve()
    cim_dir: Optional[Path] = None
    for parent in script_path.parents:
        candidate = parent / "_cim" / "cnr_transform.py"
        if candidate.is_file():
            cim_dir = candidate.parent
            break
    if cim_dir is None:
        raise RuntimeError(f"Could not locate _cim/cnr_transform.py from {script_path}")
    sys.path.insert(0, str(cim_dir))
    from cnr_transform import CNRConverter  # type: ignore

    return CNRConverter


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_kpi_base() -> str:
    kpi_base = os.getenv("KPI_BASE", "").strip().rstrip("/")
    if kpi_base:
        return kpi_base
    base_url = os.getenv("BASE_URL", "").strip().rstrip("/")
    if base_url:
        return f"{base_url}/gd-kpi-api/v1"
    return "https://greendigit-cim.sztaki.hu/gd-kpi-api/v1"


def _normalize_kpi_base_for_runtime(kpi_base: str) -> str:
    parsed = urlparse(kpi_base)
    if parsed.hostname != "kpi-service":
        return kpi_base
    try:
        socket.gethostbyname("kpi-service")
        return kpi_base
    except Exception:
        # Host-side runs cannot resolve Docker service DNS name; use public ingress URL instead.
        return "https://greendigit-cim.sztaki.hu/gd-kpi-api/v1"


def _hour_bucket(dt: datetime, granularity_s: int) -> datetime:
    epoch = int(_ensure_utc(dt).timestamp())
    bucket = epoch - (epoch % granularity_s)
    return datetime.fromtimestamp(bucket, tz=timezone.utc)


def _site_key(site: str) -> str:
    return " ".join(site.strip().lower().split())


def _coerce_lat_lon(item: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    lat = _as_float(item.get("lat"))
    if lat is None:
        lat = _as_float(item.get("latitude"))
    lon = _as_float(item.get("lon"))
    if lon is None:
        lon = _as_float(item.get("lng"))
    if lon is None:
        lon = _as_float(item.get("longitude"))
    return lat, lon


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _post_json(url: str, payload: Dict[str, Any], auth_header: str, timeout_s: int = 20) -> Optional[Dict[str, Any]]:
    req = urllib.request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": auth_header,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


class KPIEnricher:
    def __init__(
        self,
        kpi_base: str,
        jwt_token: str,
        cache_granularity_s: int,
        sites_map_path: Path,
        pue_cache_path: Path,
        ci_cache_path: Path,
    ) -> None:
        self.kpi_base = kpi_base.rstrip("/")
        self.auth_header = f"Bearer {jwt_token}"
        self.cache_granularity_s = max(60, int(cache_granularity_s))
        self.sites_map_path = sites_map_path
        self.pue_cache_path = pue_cache_path
        self.ci_cache_path = ci_cache_path
        self.stats: Dict[str, int] = {
            "pue_cache_hit": 0,
            "pue_sites_map_hit": 0,
            "pue_api_hit": 0,
            "pue_api_fail": 0,
            "ci_cache_hit": 0,
            "ci_api_hit": 0,
            "ci_api_fail": 0,
        }
        self._dirty_pue_cache = False
        self._dirty_ci_cache = False

        self.sites_map: Dict[str, Dict[str, Any]] = {}
        self.pue_cache: Dict[str, Dict[str, Any]] = {}
        self.ci_cache: Dict[str, Dict[str, Any]] = {}
        self._load_sources()

    def _load_sources(self) -> None:
        sites_obj = _read_json(self.sites_map_path, [])
        if isinstance(sites_obj, list):
            for item in sites_obj:
                if not isinstance(item, dict):
                    continue
                name = item.get("site_name")
                if not isinstance(name, str) or not name.strip():
                    continue
                self.sites_map[_site_key(name)] = item

        pue_obj = _read_json(self.pue_cache_path, {})
        if isinstance(pue_obj, dict):
            self.pue_cache = pue_obj

        ci_obj = _read_json(self.ci_cache_path, {})
        if isinstance(ci_obj, dict):
            self.ci_cache = ci_obj

    def persist(self) -> None:
        if self._dirty_pue_cache:
            self.pue_cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.pue_cache_path.write_text(json.dumps(self.pue_cache, indent=2), encoding="utf-8")
        if self._dirty_ci_cache:
            self.ci_cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.ci_cache_path.write_text(json.dumps(self.ci_cache, indent=2), encoding="utf-8")

    def resolve_pue_lat_lon(self, site_name: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        key = _site_key(site_name)
        local_pue: Optional[float] = None
        local_lat: Optional[float] = None
        local_lon: Optional[float] = None

        cached = self.pue_cache.get(key)
        if isinstance(cached, dict):
            pue = _as_float(cached.get("pue"))
            lat, lon = _coerce_lat_lon(cached)
            if pue is not None and lat is not None and lon is not None:
                self.stats["pue_cache_hit"] += 1
                return pue, lat, lon

        local = self.sites_map.get(key)
        if isinstance(local, dict):
            local_pue = _as_float(local.get("pue"))
            local_lat, local_lon = _coerce_lat_lon(local)
            if local_pue is not None and local_lat is not None and local_lon is not None:
                self.stats["pue_sites_map_hit"] += 1
                self.pue_cache[key] = {
                    "site_name": site_name,
                    "pue": local_pue,
                    "lat": local_lat,
                    "lon": local_lon,
                    "cached_at": _to_iso_z(datetime.now(timezone.utc)),
                    "source": "sites_latlngpue",
                }
                self._dirty_pue_cache = True
                return local_pue, local_lat, local_lon

        payload = {"site_name": site_name}
        resp = _post_json(f"{self.kpi_base}/pue", payload, self.auth_header)
        if not isinstance(resp, dict):
            self.stats["pue_api_fail"] += 1
            return local_pue, local_lat, local_lon

        pue = _as_float(resp.get("pue"))
        loc = resp.get("location") if isinstance(resp.get("location"), dict) else {}
        lat = _as_float(loc.get("latitude"))
        lon = _as_float(loc.get("longitude"))
        if pue is None or lat is None or lon is None:
            self.stats["pue_api_fail"] += 1
            return local_pue, local_lat, local_lon

        self.stats["pue_api_hit"] += 1
        self.pue_cache[key] = {
            "site_name": site_name,
            "pue": pue,
            "lat": lat,
            "lon": lon,
            "cached_at": _to_iso_z(datetime.now(timezone.utc)),
            "source": "kpi_api",
        }
        self._dirty_pue_cache = True
        return pue, lat, lon

    def resolve_ci(self, lat: float, lon: float, when: datetime) -> Optional[float]:
        bucket_start = _hour_bucket(when, self.cache_granularity_s)
        key = f"{lat:.6f},{lon:.6f}|{int(bucket_start.timestamp())}"
        cached = self.ci_cache.get(key)
        if isinstance(cached, dict):
            ci_cached = _as_float(cached.get("ci_g"))
            if ci_cached is not None:
                self.stats["ci_cache_hit"] += 1
                return ci_cached

        bucket_end = datetime.fromtimestamp(int(bucket_start.timestamp()) + self.cache_granularity_s, tz=timezone.utc)
        payload = {
            "lat": lat,
            "lon": lon,
            "start": _to_iso_z(bucket_start),
            "end": _to_iso_z(bucket_end),
            "wattnet_params": {"aggregate": "avg"},
        }
        resp = _post_json(f"{self.kpi_base}/ci", payload, self.auth_header)
        if not isinstance(resp, dict):
            self.stats["ci_api_fail"] += 1
            return None
        ci_val = _as_float(resp.get("ci_gco2_per_kwh"))
        if ci_val is None:
            self.stats["ci_api_fail"] += 1
            return None

        self.stats["ci_api_hit"] += 1
        self.ci_cache[key] = {
            "ci_g": ci_val,
            "lat": lat,
            "lon": lon,
            "bucket_start": _to_iso_z(bucket_start),
            "bucket_end": _to_iso_z(bucket_end),
            "cached_at": _to_iso_z(datetime.now(timezone.utc)),
            "source": "kpi_api",
        }
        self._dirty_ci_cache = True
        return ci_val


@dataclass
class InputDoc:
    publisher_email: Optional[str]
    timestamp: Optional[str]
    body: Any


def iter_input_docs(path: Path) -> Iterator[InputDoc]:
    """Yield InputDoc from JSONL or JSON (array/object)."""
    # Streaming JSONL path (fast, low memory): extension hint or first-lines heuristic.
    suffix = path.suffix.lower()
    if suffix in {".jsonl", ".ndjson"}:
        with path.open("r", encoding="utf-8") as fh:
            for ln in fh:
                s = ln.strip()
                if not s:
                    continue
                obj = json.loads(s)
                if isinstance(obj, dict) and "body" in obj:
                    yield InputDoc(
                        publisher_email=(obj.get("publisher_email") or obj.get("publisher") or None),
                        timestamp=(obj.get("timestamp") or obj.get("ts") or None),
                        body=obj.get("body"),
                    )
                else:
                    yield InputDoc(publisher_email=None, timestamp=None, body=obj)
        return

    with path.open("r", encoding="utf-8") as fh:
        probe: List[str] = []
        for ln in fh:
            s = ln.strip()
            if not s:
                continue
            probe.append(s)
            if len(probe) >= 5:
                break
    if len(probe) > 1 and all(ln.lstrip().startswith("{") for ln in probe):
        with path.open("r", encoding="utf-8") as fh:
            for ln in fh:
                s = ln.strip()
                if not s:
                    continue
                obj = json.loads(s)
                if isinstance(obj, dict) and "body" in obj:
                    yield InputDoc(
                        publisher_email=(obj.get("publisher_email") or obj.get("publisher") or None),
                        timestamp=(obj.get("timestamp") or obj.get("ts") or None),
                        body=obj.get("body"),
                    )
                else:
                    yield InputDoc(publisher_email=None, timestamp=None, body=obj)
        return

    # Otherwise treat as JSON
    text = path.read_text(encoding="utf-8")
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


def _as_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    if s.lower() in {"null", "none", "nan"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _is_missing(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        s = v.strip().lower()
        return s in {"", "null", "none"}
    return False


def _reason_no_ci_pue(ci: Optional[float], pue: Optional[float]) -> str:
    if ci is None and pue is None:
        return "no_ci_and_pue"
    if ci is None:
        return "no_ci"
    return "no_pue"


def apply_cfp_policy(fact: Dict[str, Any], enricher: Optional[KPIEnricher]) -> Optional[Dict[str, str]]:
    """
    Returns audit dict only for rows looked into:
      {"outcome": "injected"|"null", "reason": "..."}
    Returns None when row was not part of CFP review population.
    """
    raw_cfp = fact.get("CFP_g")
    if raw_cfp is None and "CFPg" in fact:
        raw_cfp = fact.get("CFPg")
    cfp_num = _as_float(raw_cfp)

    looked_into = _is_missing(raw_cfp) or (cfp_num == 0.0)
    if not looked_into:
        return None

    ci = _as_float(fact.get("CI_g") if fact.get("CI_g") is not None else fact.get("CIg"))
    pue = _as_float(fact.get("PUE") if fact.get("PUE") is not None else fact.get("pue"))
    lat, lon = _coerce_lat_lon(fact)

    # Prefer normalized field name, keep legacy fallback.
    energy_wh = _as_float(fact.get("energy_wh") if fact.get("energy_wh") is not None else fact.get("EnergyWh"))

    site_name = str(fact.get("site") or "").strip()
    if enricher is not None and site_name and (pue is None or ci is None or lat is None or lon is None):
        pue_resolved, lat_resolved, lon_resolved = enricher.resolve_pue_lat_lon(site_name)
        if pue is None and pue_resolved is not None:
            pue = pue_resolved
            fact["PUE"] = pue
        if lat is None and lat_resolved is not None:
            lat = lat_resolved
            fact["lat"] = lat
        if lon is None and lon_resolved is not None:
            lon = lon_resolved
            fact["lon"] = lon

    if enricher is not None and ci is None and lat is not None and lon is not None:
        when = _infer_times(fact)[0]
        ci_resolved = enricher.resolve_ci(lat, lon, when)
        if ci_resolved is not None:
            ci = ci_resolved
            fact["CI_g"] = ci

    if ci is not None and pue is not None and energy_wh is not None:
        cfp_g = (energy_wh / 1000.0) * pue * ci
        fact["CFP_g"] = round(cfp_g, 4)
        fact.pop("CFPg", None)
        return {"outcome": "injected", "reason": "computed_from_ci_pue_energy"}

    fact["CFP_g"] = None
    fact.pop("CFPg", None)
    if ci is None or pue is None:
        return {"outcome": "null", "reason": _reason_no_ci_pue(ci, pue)}
    return {"outcome": "null", "reason": "other"}


def main() -> int:
    ap = argparse.ArgumentParser(description="Convert Mongo export to CNR envelopes (JSONL).")
    ap.add_argument("dump", type=Path, help="Path to mongoexport file (JSONL or JSON).")
    ap.add_argument("--out-dir", type=Path, default=Path("analysis/dump_processed"))
    ap.add_argument("--emails", type=str, default="", help="Comma-separated publisher_email filter (optional).")
    ap.add_argument("--start", type=str, default="", help="Filter by document timestamp >= start (ISO).")
    ap.add_argument("--end", type=str, default="", help="Filter by document timestamp <= end (ISO).")
    ap.add_argument("--kpi-base", type=str, default=_default_kpi_base(), help="KPI API base URL.")
    ap.add_argument("--cache-granularity-s", type=int, default=3600, help="CI cache granularity in seconds.")
    ap.add_argument(
        "--sites-map-path",
        type=Path,
        default=_repo_root() / "_kpi_cache" / "sites_latlngpue.json",
        help="Path to local sites map used for PUE/lat/lon lookup.",
    )
    ap.add_argument(
        "--pue-cache-path",
        type=Path,
        default=_repo_root() / "scripts" / "batch_submit_cnr" / "cache_pue.json",
        help="Path to persistent PUE cache file.",
    )
    ap.add_argument(
        "--ci-cache-path",
        type=Path,
        default=_repo_root() / "scripts" / "batch_submit_cnr" / "cache_ci.json",
        help="Path to persistent CI cache file.",
    )
    ap.add_argument(
        "--disable-kpi-enrichment",
        action="store_true",
        help="Disable KPI enrichment calls/cache and keep only in-row CI/PUE values.",
    )
    ap.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Log progress every N processed metrics (0 disables progress logs).",
    )

    args = ap.parse_args()
    args.kpi_base = _normalize_kpi_base_for_runtime(args.kpi_base)

    emails: Optional[set[str]] = None
    if args.emails.strip():
        emails = {e.strip().lower() for e in args.emails.split(",") if e.strip()}

    start_dt = _parse_iso_dt(args.start) if args.start else None
    end_dt = _parse_iso_dt(args.end) if args.end else None

    args.out_dir.mkdir(parents=True, exist_ok=True)

    enricher: Optional[KPIEnricher] = None
    if not args.disable_kpi_enrichment:
        jwt_token = os.getenv("JWT_TOKEN", "").strip().strip("'").strip('"')
        if jwt_token:
            enricher = KPIEnricher(
                kpi_base=args.kpi_base,
                jwt_token=jwt_token,
                cache_granularity_s=args.cache_granularity_s,
                sites_map_path=args.sites_map_path,
                pue_cache_path=args.pue_cache_path,
                ci_cache_path=args.ci_cache_path,
            )
        else:
            print("[process_dump] JWT_TOKEN not found; KPI enrichment disabled.", file=sys.stderr)

    CNRConverter = _load_cnr_converter()
    converter = CNRConverter()

    by_email_out: Dict[str, Any] = {}

    total_docs = 0
    total_entries = 0
    total_envelopes = 0
    total_errors = 0
    total_cfp_looked_into = 0
    total_cfp_injected = 0
    total_cfp_null = 0
    total_cfp_null_no_ci_pue = 0
    total_cfp_null_other = 0
    total_metrics_processed = 0
    bucket_start_metrics = 0
    bucket_start_pue_req = 0
    bucket_start_ci_req = 0

    def _req_totals() -> Tuple[int, int]:
        if enricher is None:
            return 0, 0
        pue_req = int(enricher.stats.get("pue_api_hit", 0)) + int(enricher.stats.get("pue_api_fail", 0))
        ci_req = int(enricher.stats.get("ci_api_hit", 0)) + int(enricher.stats.get("ci_api_fail", 0))
        return pue_req, ci_req

    def _maybe_log_progress(force: bool = False) -> None:
        nonlocal bucket_start_metrics, bucket_start_pue_req, bucket_start_ci_req
        if args.progress_every <= 0:
            return
        if not force and (total_metrics_processed % args.progress_every) != 0:
            return
        if force and total_metrics_processed == bucket_start_metrics:
            return
        pue_req_total, ci_req_total = _req_totals()
        pue_req_bucket = pue_req_total - bucket_start_pue_req
        ci_req_bucket = ci_req_total - bucket_start_ci_req
        metrics_bucket = total_metrics_processed - bucket_start_metrics
        print(
            (
                f"[process_dump] processed_metrics={total_metrics_processed} "
                f"bucket_metrics={metrics_bucket} "
                f"req_pue={pue_req_total} (bucket={pue_req_bucket}) "
                f"req_ci={ci_req_total} (bucket={ci_req_bucket}) "
                f"docs_seen={total_docs} entries_seen={total_entries}"
            ),
            file=sys.stderr,
            flush=True,
        )
        bucket_start_metrics = total_metrics_processed
        bucket_start_pue_req = pue_req_total
        bucket_start_ci_req = ci_req_total

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
                "cfp_looked_into": 0,
                "cfp_injected": 0,
                "cfp_null": 0,
                "cfp_null_no_ci_pue": 0,
                "cfp_null_other": 0,
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
            total_metrics_processed += 1
            try:
                fact = dict(rec.fact_site_event)
                cfp_audit = apply_cfp_policy(fact, enricher)
                if cfp_audit is not None:
                    sink["cfp_looked_into"] += 1
                    total_cfp_looked_into += 1
                    if cfp_audit["outcome"] == "injected":
                        sink["cfp_injected"] += 1
                        total_cfp_injected += 1
                    else:
                        sink["cfp_null"] += 1
                        total_cfp_null += 1
                        if cfp_audit["reason"] in {"no_ci", "no_pue", "no_ci_and_pue"}:
                            sink["cfp_null_no_ci_pue"] += 1
                            total_cfp_null_no_ci_pue += 1
                        else:
                            sink["cfp_null_other"] += 1
                            total_cfp_null_other += 1

                env = build_envelope(rec.payload_type, fact, rec.detail_table, dict(rec.detail_row))
            except Exception as exc:
                sink["errors"] += 1
                total_errors += 1
                sink["err"].write(
                    json.dumps({"error": str(exc), "publisher_email": pub, "timestamp": doc.timestamp, "raw": rec.raw}) + "\n"
                )
            else:
                sink["out"].write(json.dumps(env, separators=(",", ":")) + "\n")
                sink["envelopes"] += 1
                total_envelopes += 1

            _maybe_log_progress(force=False)

    for sink in by_email_out.values():
        sink["out"].close()
        sink["err"].close()
    if enricher is not None:
        enricher.persist()

    _maybe_log_progress(force=True)

    summary = {
        "dump": str(args.dump),
        "out_dir": str(args.out_dir),
        "docs_seen": total_docs,
        "entries_seen": total_entries,
        "envelopes_written": total_envelopes,
        "errors": total_errors,
        "kpi_enrichment": {
            "enabled": enricher is not None,
            "kpi_base": args.kpi_base,
            "cache_granularity_s": args.cache_granularity_s,
            "sites_map_path": str(args.sites_map_path),
            "pue_cache_path": str(args.pue_cache_path),
            "ci_cache_path": str(args.ci_cache_path),
            "stats": (enricher.stats if enricher is not None else {}),
        },
        "cfp_review": {
            "looked_into_rows": total_cfp_looked_into,
            "successfully_injected_rows": total_cfp_injected,
            "null_rows": total_cfp_null,
            "null_reasons": {
                "no_ci_pue": total_cfp_null_no_ci_pue,
                "other": total_cfp_null_other,
            },
        },
        "per_publisher": {
            k: {
                "envelopes": v["envelopes"],
                "errors": v["errors"],
                "cfp_review": {
                    "looked_into_rows": v["cfp_looked_into"],
                    "successfully_injected_rows": v["cfp_injected"],
                    "null_rows": v["cfp_null"],
                    "null_reasons": {
                        "no_ci_pue": v["cfp_null_no_ci_pue"],
                        "other": v["cfp_null_other"],
                    },
                },
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
