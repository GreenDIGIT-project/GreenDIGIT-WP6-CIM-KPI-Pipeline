import http.server
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from cnr_transform import CNRConverter, ConvertedRecord

LISTEN_PORT = int(os.getenv("LISTEN_PORT", "8012"))
KPI_BASE = os.getenv("KPI_BASE", "http://kpi-service:8011")
CNR_SQL_FORWARD_URL = os.getenv("CNR_SQL_FORWARD_URL", "http://sql-adapter:8033/cnr-sql-service")
PUE_FALLBACK = float(os.getenv("PUE_DEFAULT", "1.7"))

converter = CNRConverter()


def _to_iso_z(dt: datetime) -> str:
    """Return UTC ISO string with Z suffix."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def jsonable(value: Any) -> Any:
    """Recursively convert datetimes to ISO strings so JSON serialization succeeds."""
    if isinstance(value, datetime):
        return _to_iso_z(value)
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [jsonable(v) for v in value]
    return value


def _duration_seconds(fact: Dict[str, Any]) -> Optional[float]:
    start = fact.get("startexectime")
    stop = fact.get("stopexectime")
    if isinstance(start, datetime) and isinstance(stop, datetime):
        return (stop - start).total_seconds()
    return None


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def _infer_times(fact: Dict[str, Any]) -> tuple[datetime, datetime, datetime]:
    start_raw = fact.get("startexectime") or fact.get("event_start_timestamp") or fact.get("event_start_time")
    stop_raw = fact.get("stopexectime") or fact.get("event_end_timestamp") or fact.get("event_end_times")

    def _coerce_dt(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return _ensure_utc(value)
        try:
            return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
        except Exception:
            return None

    start = _coerce_dt(start_raw)
    stop = _coerce_dt(stop_raw)

    if not start or not stop:
        now = datetime.now(timezone.utc)
        return now, now, now

    when = stop
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return start, stop, when


def _post_json(url: str, payload: Dict[str, Any], auth_header: Optional[str]) -> Dict[str, Any]:
    data_bytes = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "cim-service/1.0",
    }
    if auth_header:
        headers["Authorization"] = auth_header

    req = urllib.request.Request(url, data=data_bytes, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as response:
        body = response.read().decode("utf-8")
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {"raw": body}


def fetch_pue(site_name: str, auth_header: Optional[str]) -> Optional[Dict[str, Any]]:
    try:
        return _post_json(f"{KPI_BASE}/pue", {"site_name": site_name}, auth_header)
    except Exception as exc:
        print(f"[cim] PUE lookup failed for {site_name}: {exc}", flush=True)
        return None


def fetch_ci(
    lat: float,
    lon: float,
    start: datetime,
    end: datetime,
    pue: float,
    energy_wh: Optional[float],
    auth_header: Optional[str],
) -> Optional[Dict[str, Any]]:
    body: Dict[str, Any] = {
        "lat": lat,
        "lon": lon,
        "pue": pue,
        "start": _to_iso_z(start),
        "end": _to_iso_z(end),
    }
    if energy_wh is not None:
        body["energy_wh"] = energy_wh
    try:
        return _post_json(f"{KPI_BASE}/ci", body, auth_header)
    except Exception as exc:
        print(f"[cim] CI lookup failed: {exc}", flush=True)
        return None


def to_envelope(rec: ConvertedRecord) -> Dict[str, Any]:
    """
    Map ConvertedRecord into the MetricsEnvelope / CNR payload shape.
    """
    fact = dict(rec.fact_site_event)
    detail = dict(rec.detail_row)

    env: Dict[str, Any] = {
        "site": fact.get("site"),
        "duration_s": _duration_seconds(fact),
        "sites": {"site_type": rec.payload_type},
        "fact_site_event": fact,
        "detail_table": rec.detail_table,
        # Provide all detail_* keys to satisfy older schemas that expect detail_cloud.
        "detail_cloud": {},
        "detail_grid": {},
        "detail_network": {},
    }
    env[rec.detail_table] = detail
    return jsonable(env)


class CIMHandler(http.server.BaseHTTPRequestHandler):
    def _json_response(self, status: int, payload: Dict[str, Any]) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode("utf-8"))

    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        raw_body = self.rfile.read(length) if length else b""

        try:
            incoming = json.loads(raw_body.decode("utf-8") or "null")
        except json.JSONDecodeError as exc:
            self._json_response(400, {"error": f"Invalid JSON: {exc}"})
            return

        try:
            records = converter.convert(incoming)
        except Exception as exc:
            self._json_response(400, {"error": f"Transform failed: {exc}"})
            return

        if not records:
            self._json_response(400, {"error": "No valid metric entries in payload"})
            return

        auth_header = self.headers.get("Authorization")
        results: List[Dict[str, Any]] = []

        for rec in records:
            envelope = to_envelope(rec)
            fact = envelope["fact_site_event"]
            site_name = fact.get("site")

            # Preserve partner CI/CFP before overriding
            partner_ci = fact.get("CI_g") or fact.get("CIg")
            partner_cfp = fact.get("CFP_g") or fact.get("CFPg")
            if partner_ci is not None:
                fact["CI_site_g"] = partner_ci
            if partner_cfp is not None:
                fact["CFP_site_g"] = partner_cfp
            # Remove partner values so we can inject our own
            fact.pop("CI_g", None)
            fact.pop("CIg", None)
            fact.pop("CFP_g", None)
            fact.pop("CFPg", None)

            # Resolve PUE
            resolved_pue = fact.get("PUE")
            pue_resp = None
            if resolved_pue is None and site_name:
                pue_resp = fetch_pue(site_name, auth_header)
                if pue_resp:
                    resolved_pue = pue_resp.get("pue")
            if resolved_pue is None:
                resolved_pue = PUE_FALLBACK
            try:
                resolved_pue = float(resolved_pue)
            except Exception:
                resolved_pue = PUE_FALLBACK

            # Resolve lat/lon (from PUE response if available)
            lat = None
            lon = None
            if pue_resp:
                loc = pue_resp.get("location") or {}
                lat = loc.get("latitude")
                lon = loc.get("longitude")

            # Energy
            energy_wh = envelope.get("energy_wh") or fact.get("energy_wh") or fact.get("EnergyWh")
            if energy_wh is not None:
                try:
                    energy_wh = float(energy_wh)
                except Exception:
                    energy_wh = None

            # Time window for CI
            _, _, when = _infer_times(fact)
            ci_start = when - timedelta(hours=1)
            ci_end = when + timedelta(hours=2)

            # CI lookup (preferred) and CFP from CI endpoint
            ci_g = None
            cfp_g = None
            if lat is not None and lon is not None:
                ci_resp = fetch_ci(lat, lon, ci_start, ci_end, resolved_pue, energy_wh, auth_header)
                if ci_resp:
                    ci_val = ci_resp.get("ci_gco2_per_kwh") or ci_resp.get("ci_g")
                    if isinstance(ci_val, (int, float)):
                        ci_g = float(ci_val)
                    cfp_val = ci_resp.get("cfp_g")
                    if isinstance(cfp_val, (int, float)):
                        cfp_g = float(cfp_val)

            # Fallback CFP calculation if CI and energy available
            if cfp_g is None and ci_g is not None and energy_wh is not None:
                cfp_g = (energy_wh / 1000.0) * resolved_pue * ci_g

            # Final injection into fact
            fact["PUE"] = resolved_pue
            if ci_g is not None:
                fact["CI_g"] = ci_g
            if cfp_g is not None:
                fact["CFP_g"] = round(cfp_g, 4)
            if energy_wh is not None and "energy_wh" not in fact:
                fact["energy_wh"] = energy_wh

            # Forward to SQL adapter (CNR)
            try:
                print(f"[cim] Forwarding {rec.payload_type} metric to SQL adapter ({CNR_SQL_FORWARD_URL})", flush=True)
                response = _post_json(CNR_SQL_FORWARD_URL, envelope, auth_header)
                results.append({"detail_table": rec.detail_table, "status": "ok", "cnr_response": response})
            except urllib.error.HTTPError as exc:
                error_body = exc.read().decode("utf-8", "replace")
                print(f"[cim] CNR error {exc.code}: {error_body[:200]}", flush=True)
                self._json_response(exc.code, {"error": error_body})
                return
            except Exception as exc:
                print(f"[cim] Forwarding to SQL failed: {exc}", flush=True)
                self._json_response(502, {"error": f"Forwarding failed: {exc}"})
                return

        self._json_response(200, {"forwarded": len(results), "results": results})

    def log_message(self, format: str, *args: Any) -> None:  # type: ignore[override]
        # Silence default http.server logging to keep Docker logs clean.
        return


if __name__ == "__main__":
    print(f"cim-service listening on port {LISTEN_PORT}, using KPI at {KPI_BASE} and SQL adapter at {CNR_SQL_FORWARD_URL}", flush=True)
    http.server.HTTPServer(("0.0.0.0", LISTEN_PORT), CIMHandler).serve_forever()
