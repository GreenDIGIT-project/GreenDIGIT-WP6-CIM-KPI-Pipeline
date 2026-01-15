import http.server
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from cnr_transform import CNRConverter, ConvertedRecord

LISTEN_PORT = int(os.getenv("LISTEN_PORT", "8012"))
TARGET_URL = os.getenv("TARGET_URL", "http://kpi-service:8011/transform-and-forward")

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


def to_envelope(rec: ConvertedRecord) -> Dict[str, Any]:
    """
    Map ConvertedRecord into the MetricsEnvelope shape expected by KPI service.
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


def forward_to_kpi(payload: Dict[str, Any], auth_header: Optional[str]) -> tuple[int, str]:
    data_bytes = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "cim-service/1.0",
    }
    if auth_header:
        headers["Authorization"] = auth_header

    req = urllib.request.Request(TARGET_URL, data=data_bytes, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as response:
        body = response.read().decode("utf-8")
        return response.status, body


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
            print(f"[cim] Forwarding {rec.payload_type} metric to KPI ({TARGET_URL})", flush=True)
            try:
                status, body = forward_to_kpi(envelope, auth_header)
                print(f"[cim] KPI response status={status}", flush=True)
                results.append({"detail_table": rec.detail_table, "status": status})
            except urllib.error.HTTPError as exc:
                error_body = exc.read().decode("utf-8", "replace")
                print(f"[cim] Upstream error {exc.code}: {error_body[:200]}", flush=True)
                self._json_response(exc.code, {"error": error_body})
                return
            except Exception as exc:
                print(f"[cim] Forwarding failed: {exc}", flush=True)
                self._json_response(502, {"error": f"Forwarding failed: {exc}"})
                return

        self._json_response(200, {"forwarded": len(results), "results": results})

    def log_message(self, format: str, *args: Any) -> None:  # type: ignore[override]
# Silence default http.server logging to keep Docker logs clean.
        return


if __name__ == "__main__":
    print(f"cim-service listening on port {LISTEN_PORT}, forwarding to {TARGET_URL}", flush=True)
    http.server.HTTPServer(("0.0.0.0", LISTEN_PORT), CIMHandler).serve_forever()
