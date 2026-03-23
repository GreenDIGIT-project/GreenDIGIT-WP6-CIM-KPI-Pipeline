import os
import time
from typing import Any

import requests

PROBE_INTERVAL_S = int(os.getenv("HEALTH_PROBE_INTERVAL_S", "60"))
SQL_AUDIT_HEALTH_URL = os.getenv("SQL_HEALTH_AUDIT_URL", "http://sql-adapter:8033/service-health")

TARGETS = [
    ("publication-api-a", "http://cim-fastapi-a:8000/v1/health"),
    ("publication-api-b", "http://cim-fastapi-b:8000/v1/health"),
    ("cim-transform", "http://cim-service:8012/health"),
    ("kpi-service", "http://kpi-service:8011/v1/health"),
    ("sql-adapter", "http://sql-adapter:8033/health"),
    ("grafana-proxy", "http://grafana-auth-proxy:8044/health"),
]

http = requests.Session()


def probe(service_name: str, target: str) -> dict[str, Any]:
    start = time.perf_counter()
    ok = False
    status_code = None
    detail = None
    try:
        resp = http.get(target, timeout=10)
        status_code = resp.status_code
        ok = resp.ok
        try:
            detail = resp.text[:500]
        except Exception:
            detail = None
    except requests.RequestException as exc:
        detail = str(exc)
    latency_ms = int((time.perf_counter() - start) * 1000)
    return {
        "service_name": service_name,
        "target": target,
        "ok": ok,
        "status_code": status_code,
        "latency_ms": latency_ms,
        "detail": detail,
    }


def main() -> int:
    while True:
        rows = [probe(service_name, target) for service_name, target in TARGETS]
        try:
            resp = http.post(SQL_AUDIT_HEALTH_URL, json={"rows": rows}, timeout=20)
            resp.raise_for_status()
            print(f"[health-probe] stored {len(rows)} rows", flush=True)
        except requests.RequestException as exc:
            print(f"[health-probe] failed to publish probe rows: {exc}", flush=True)
        time.sleep(PROBE_INTERVAL_S)


if __name__ == "__main__":
    raise SystemExit(main())
