# cnr_transform.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Literal, Optional, Tuple
import re
import zlib


PayloadType = Literal["grid", "cloud", "network"]


@dataclass(frozen=True)
class ConvertedRecord:
    payload_type: PayloadType
    fact_site_event: Dict[str, Any]
    detail_table: str
    detail_row: Dict[str, Any]
    raw: Dict[str, Any]


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _norm_key(k: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", k.lower())


def _index_keys(d: Dict[str, Any]) -> Dict[str, str]:
    """Map normalised key -> original key (first occurrence wins)."""
    idx: Dict[str, str] = {}
    for k in d.keys():
        nk = _norm_key(str(k))
        idx.setdefault(nk, k)
    return idx


def _get(d: Dict[str, Any], idx: Dict[str, str], *candidates: str) -> Any:
    """Fetch first matching candidate key (case/format-insensitive)."""
    for c in candidates:
        # exact
        if c in d:
            return d[c]
        # normalised
        nk = _norm_key(c)
        if nk in idx:
            return d[idx[nk]]
    return None


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    s = str(v).strip()
    if s == "":
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(int(v))
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "done", "finished"}:
        return True
    if s in {"0", "false", "no", "n", "running", "pending"}:
        return False
    return None


def parse_timestamp(v: Any) -> Optional[datetime]:
    """
    Parse:
      - "2026-01-15 09:20:10"
      - "2026-01-15T00:00:01Z"
      - "2026-01-14T20:07:04.773563Z"
    Returns UTC naive datetime (tz stripped).
    """
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        return dt.replace(tzinfo=None)

    s = str(v).strip()
    if s == "":
        return None

    # Unix seconds (rare, but happens)
    if re.fullmatch(r"\d{10}(\.\d+)?", s):
        try:
            ts = float(s)
            return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
        except ValueError:
            pass

    # ISO with Z
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    # Try fromisoformat (handles "+00:00" and microseconds)
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        return dt.replace(tzinfo=None)
    except ValueError:
        pass

    # Try common "YYYY-MM-DD HH:MM:SS"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=None)
        except ValueError:
            continue

    return None


def normalise_payload(payload: Any) -> List[Dict[str, Any]]:
    """
    Step 1: detect whether payload is an array or a single object.
    """
    if payload is None:
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        return [payload]
    raise TypeError(f"Unsupported payload type: {type(payload).__name__}")


def detect_payload_type(entry: Dict[str, Any]) -> PayloadType:
    """
    Heuristic detection:
      - network: has nested 'detail_network' or network-ish keys
      - cloud: has CloudType/CloudComputeService/CpuDuration_s
      - else: grid
    """
    idx = _index_keys(entry)

    if _get(entry, idx, "detail_network") is not None:
        return "network"

    network_markers = ("amountofdatatransferred", "networktype", "measurementtype", "destinationexecunitid")
    if any(_get(entry, idx, m) is not None for m in network_markers):
        return "network"

    cloud_markers = ("cloudtype", "cloudcomputeservice", "cpuduration_s", "suspendduration_s")
    if any(_get(entry, idx, m) is not None for m in cloud_markers):
        return "cloud"

    return "grid"


def default_event_id(execunitid: Any, site: Optional[str], start: Optional[datetime]) -> int:
    """
    fact_site_event.event_id is integer in your schema.
    - If ExecUnitID is numeric, reuse it.
    - Else generate deterministic 31-bit int via crc32 over (execunitid|site|start).
    """
    ei = _to_int(execunitid)
    if ei is not None:
        return ei
    seed = f"{execunitid}|{site or ''}|{start.isoformat() if start else ''}"
    return zlib.crc32(seed.encode("utf-8")) & 0x7FFFFFFF


class CNRConverter:
    """
    Converts partner-submitted entries into CNR-compatible rows for:
      - fact_site_event
      - detail_grid / detail_cloud / detail_network

    site_id_resolver: optional function mapping site string -> integer site_id (FK to sites table).
      - If omitted, site_id will be None and you can fill it later.
    event_id_fn: optional override for event_id generation.
    recorded_at_fn: optional override for recorded_at timestamp.
    """

    def __init__(
        self,
        site_id_resolver: Optional[Callable[[str], int]] = None,
        event_id_fn: Optional[Callable[[Any, Optional[str], Optional[datetime]], int]] = None,
        recorded_at_fn: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self.site_id_resolver = site_id_resolver
        self.event_id_fn = event_id_fn or default_event_id
        self.recorded_at_fn = recorded_at_fn or _utcnow_naive

    def convert(self, payload: Any) -> List[ConvertedRecord]:
        out: List[ConvertedRecord] = []
        for entry in normalise_payload(payload):
            ptype = detect_payload_type(entry)
            fact, detail_table, detail = self._convert_one(entry, ptype)
            out.append(
                ConvertedRecord(
                    payload_type=ptype,
                    fact_site_event=fact,
                    detail_table=detail_table,
                    detail_row=detail,
                    raw=entry,
                )
            )
        return out

    def _resolve_site(self, entry: Dict[str, Any], idx: Dict[str, str]) -> Optional[str]:
        # Prefer GOCDB-style if present, then explicit SiteName, then Site.
        site = _get(entry, idx, "SiteGOCDB", "SiteName", "Site", "site")
        if site is None:
            return None
        s = str(site).strip()
        return s if s else None

    def _resolve_site_id(self, site: Optional[str]) -> Optional[int]:
        if site is None or self.site_id_resolver is None:
            return None
        return self.site_id_resolver(site)

    def _convert_one(self, entry: Dict[str, Any], ptype: PayloadType) -> Tuple[Dict[str, Any], str, Dict[str, Any]]:
        idx = _index_keys(entry)

        site = self._resolve_site(entry, idx)
        site_id = self._resolve_site_id(site)

        execunitid = _get(entry, idx, "ExecUnitID", "JobID", "execunitid")
        execunitid_str = None if execunitid is None else str(execunitid)

        status = _get(entry, idx, "Status", "status")
        status_str = None if status is None else str(status)

        owner = _get(entry, idx, "Owner", "owner")
        owner_str = None if owner is None else str(owner)

        # Times
        submit_time = parse_timestamp(_get(entry, idx, "SubmissionTime", "submit_time"))
        start_exec = parse_timestamp(_get(entry, idx, "StartExecTime", "StartExecTime", "startexectime"))
        stop_exec = parse_timestamp(_get(entry, idx, "StopExecTime", "EndExecTime", "StopExecTime", "stopexectime", "EndExecTime"))

        # Metrics (may be present or filled later)
        pue = _to_float(_get(entry, idx, "PUE", "pue"))
        ci_g = _to_int(_get(entry, idx, "CI_g", "CIg", "ci_g"))
        cfp_g = _to_float(_get(entry, idx, "CFP_g", "CFPg", "cfp_g"))

        energy_wh = _to_float(_get(entry, idx, "Energy_wh", "EnergyWh", "energy_wh", "EnergyWh"))
        work = _to_float(_get(entry, idx, "Work", "work"))

        exec_finished = _to_bool(_get(entry, idx, "ExecUnitFinished", "execunitfinished"))
        # If missing, infer from status
        if exec_finished is None and status_str is not None:
            if status_str.strip().lower() in {"done", "finished", "success", "succeeded"}:
                exec_finished = True
            elif status_str.strip().lower() in {"running", "pending"}:
                exec_finished = False

        event_id = self.event_id_fn(execunitid, site, start_exec or submit_time)
        recorded_at = self.recorded_at_fn()

        # Your schema has both event_start_time / event_end_times and also start/stop exec time columns.
        # Sensible default:
        # - event_start_time = SubmissionTime if available else StartExecTime
        # - event_end_times = Stop/End exec time
        fact: Dict[str, Any] = {
            "event_id": event_id,
            "site_id": site_id,
            "event_start_time": submit_time or start_exec,
            "event_end_times": stop_exec,
            "recorded_at": recorded_at,
            "job_finished": bool(exec_finished) if exec_finished is not None else None,
            "CI_g": ci_g,
            "CFP_g": cfp_g,
            "PUE": pue,
            "site": site,
            "energy_wh": energy_wh,
            "work": work,
            "startexectime": start_exec,
            "stopexectime": stop_exec,
            "status": status_str,
            "owner": owner_str,
            "execunitid": execunitid_str,
            "execunitfinished": bool(exec_finished) if exec_finished is not None else None,
        }

        if ptype == "grid":
            return fact, "detail_grid", self._detail_grid(entry, idx, event_id, site_id, execunitid_str)
        if ptype == "cloud":
            return fact, "detail_cloud", self._detail_cloud(entry, idx, event_id, site_id, execunitid_str)
        return fact, "detail_network", self._detail_network(entry, idx, event_id, site_id, execunitid_str)

    def _detail_grid(self, entry: Dict[str, Any], idx: Dict[str, str], event_id: int, site_id: Optional[int], execunitid: Optional[str]) -> Dict[str, Any]:
        return {
            # detail_id intentionally omitted (assume DB auto-generates)
            "site_id": site_id,
            "event_id": event_id,
            "execunitid": execunitid,
            "wallclocktime_s": _to_int(_get(entry, idx, "WallClockTime_s", "WallClockTime(s)", "wallclocktime_s")),
            "cpunormalizationfactor": _to_float(_get(entry, idx, "CPUNormalizationFactor", "cpunormalizationfactor")),
            "ncores": _to_int(_get(entry, idx, "NCores", "ncores")),
            "normcputime_s": _to_int(_get(entry, idx, "NormCPUTime_s", "NormCPUTime(s)", "normcputime_s")),
            "efficiency": _to_float(_get(entry, idx, "Efficiency", "efficiency")),
            "tdp_w": _to_int(_get(entry, idx, "TDP_w", "TDP(W)", "tdp_w")),
            "totalcputime_s": _to_int(_get(entry, idx, "TotalCPUTime_s", "TotalCPUTime(s)", "totalcputime_s")),
            "scaledcputime_s": _to_int(_get(entry, idx, "ScaledCPUTime_s", "ScaledCPUTime(s)", "scaledcputime_s")),
        }

    def _detail_cloud(self, entry: Dict[str, Any], idx: Dict[str, str], event_id: int, site_id: Optional[int], execunitid: Optional[str]) -> Dict[str, Any]:
        return {
            "event_id": event_id,
            "site_id": site_id,
            "execunitid": execunitid,
            "wallclocktime_s": _to_int(_get(entry, idx, "WallClockTime_s", "WallClockTime(s)", "wallclocktime_s")),
            "suspendduration_s": _to_int(_get(entry, idx, "SuspendDuration_s", "suspendduration_s")),
            "cpuduration_s": _to_int(_get(entry, idx, "CpuDuration_s", "CPUDuration_s", "cpuduration_s")),
            "cpunormalizationfactor": _to_float(_get(entry, idx, "CPUNormalizationFactor", "cpunormalizationfactor")),
            "efficiency": _to_float(_get(entry, idx, "Efficiency", "efficiency")),
            "cloud_type": _get(entry, idx, "CloudType", "cloud_type"),
            "compute_service": _get(entry, idx, "CloudComputeService", "compute_service"),
        }

    def _detail_network(self, entry: Dict[str, Any], idx: Dict[str, str], event_id: int, site_id: Optional[int], execunitid: Optional[str]) -> Dict[str, Any]:
        nested = _get(entry, idx, "detail_network") or {}
        if not isinstance(nested, dict):
            nested = {}

        nidx = _index_keys(nested)

        return {
            "detail_id": None,  # safe placeholder if your insert layer wants explicit None; remove if you prefer
            "site_id": site_id,
            "event_id": event_id,
            "execunitid": execunitid,
            "amountofdatatransferred": _to_int(_get(nested, nidx, "AmountOfDataTransferred", "amountofdatatransferred")),
            "networktype": _get(nested, nidx, "NetworkType", "networktype"),
            "measurementtype": _get(nested, nidx, "MeasurementType", "measurementtype"),
            "destinationexecunitid": _get(nested, nidx, "DestinationExecUnitID", "destinationexecunitid"),
        }


# Optional: enrichment hook (step 3) you can wire later.
def enrich_fact_with_ci_pue_cfp(
    fact: Dict[str, Any],
    ci_g: Optional[int] = None,
    pue: Optional[float] = None,
    cfp_g: Optional[float] = None,
) -> Dict[str, Any]:
    fact = dict(fact)
    if ci_g is not None:
        fact["CI_g"] = ci_g
    if pue is not None:
        fact["PUE"] = pue
    if cfp_g is not None:
        fact["CFP_g"] = cfp_g
    return fact
