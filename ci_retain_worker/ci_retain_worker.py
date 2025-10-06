import os, time, requests
from datetime import datetime, timezone
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from bson import ObjectId

# ---- Config ----
INTERVAL = int(os.getenv("RETAIN_POLL_SECONDS", "3600"))
PRETEND_VALID = os.getenv("PRETEND_VALID", "true").lower() == "true"
CI_VALID_URL = os.getenv("CI_VALID_URL", "http://ci-calc:8011/ci-valid")
GD_TOKEN     = os.getenv("JWT_TOKEN")  # same token you use elsewhere

# Mongo URIs
metrics_uri = os.environ.get(
    "METRICS_MONGO_URI",
    "mongodb://metrics-db-1:27017,metrics-db-2:27017,metrics-db-3:27017/?replicaSet=rs0"
)
retain_uri = os.environ["RETAIN_MONGO_URI"]
db_name    = os.environ.get("RETAIN_DB_NAME", "ci-retainment-db")
coll_name  = os.environ.get("RETAIN_COLL", "pending_ci")

# Wattprint (used only if PRETEND_VALID=false)
WATTPRINT_BASE = os.environ.get("WATTPRINT_BASE", "https://api.wattprint.eu")
WATTPRINT_COOKIE = os.environ.get("WATTPRINT_COOKIE")  # _oauth2_proxy cookie value

# Defaults
pue_default = float(os.environ.get("PUE_DEFAULT", "1.4"))

# ---- Clients ----
metrics_cli = MongoClient(metrics_uri, appname="ci-metrics-db", serverSelectionTimeoutMS=5000)
retain_cli  = MongoClient(retain_uri,  appname="ci-retain-worker", serverSelectionTimeoutMS=5000)

# wait for both primaries to be available
def wait_ready():
    for name, cli in (("metrics", metrics_cli), ("retain", retain_cli)):
        for _ in range(60):
            try:
                cli.admin.command("ping")
                break
            except ServerSelectionTimeoutError:
                print(f"[{datetime.now()}][worker] waiting for {name} mongo PRIMARY...", flush=True)
                time.sleep(1)

metrics_coll = metrics_cli["metricsdb"]["metrics"]
retain_coll  = retain_cli[db_name][coll_name]
sess = requests.Session()

def to_iso_z(dt):
    if isinstance(dt, str):
        return dt
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace('+00:00','Z')
    return str(dt)

def wp_fetch(lat, lon, start, end):
    url = f"{WATTPRINT_BASE}/v1/footprints"
    headers = {"Accept": "application/json"}
    if WATTPRINT_COOKIE:
        headers["Cookie"] = f"_oauth2_proxy={WATTPRINT_COOKIE}"
    r = sess.get(url, params={
        "lat": lat, "lon": lon, "footprint_type": "carbon",
        "start": start.isoformat().replace("+00:00","Z"),
        "end": end.isoformat().replace("+00:00","Z"),
        "aggregate": "true"
    }, headers=headers, timeout=20, allow_redirects=False)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        if not data:
            raise RuntimeError("Wattprint returned empty list")
        return data[0]
    return data

def compute_out(res, pue_val, energy_kwh):
    ci = float(res["value"])  # gCO2/kWh
    eff = ci * float(pue_val)
    cfp_g = eff * energy_kwh if energy_kwh is not None else None
    return {
        "source": "wattprint",
        "zone": res.get("zone"),
        "datetime": res.get("end") or res.get("start"),
        "ci_gco2_per_kwh": ci,
        "pue": float(pue_val),
        "effective_ci_gco2_per_kwh": eff,
        "cfp_g": cfp_g,
        "cfp_kg": (cfp_g / 1000.0) if cfp_g is not None else None
    }

def main():
    wait_ready()
    print(f"[{datetime.now()}][worker] started (interval={INTERVAL}s, pretend_valid={PRETEND_VALID})", flush=True)
    while True:
        try:
          for doc in retain_coll.find({"valid": False}):
            try:
                lat, lon = doc["lat"], doc["lon"]
                start, end = doc["request_time"][0], doc["request_time"][1]
                pue_val = doc.get("pue", pue_default)
                energy_kwh = doc.get("energy_kwh")
                metric_id = doc.get("metric_id")

                # Build request for /ci-valid (force compute; ignores 'valid' flag)
                headers = {"Content-Type": "application/json"}
                if GD_TOKEN:
                        headers["Authorization"] = f"Bearer {GD_TOKEN}"
                req = {
                        "lat": float(lat), "lon": float(lon), "pue": float(pue_val),
                        "energy_kwh": energy_kwh, "time": to_iso_z(end)
                }

                # Compute CI+CFP via CI service
                r = sess.post(CI_VALID_URL, json=req, headers=headers, timeout=20)
                r.raise_for_status()
                out = r.json()
                now = datetime.now(timezone.utc)

                # Mark retained doc as validated (note why)
                retain_coll.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"valid": True, "validated_at": now,
                                "note": "validated via /ci-valid" if not PRETEND_VALID else "pretend_valid via /ci-valid"}}
                )

                # Merge into metricsdb if we know which metric
                if metric_id:
                        try:
                            oid = ObjectId(metric_id) if isinstance(metric_id, str) else metric_id
                            metrics_coll.update_one(
                                {"_id": oid},
                                {"$set": {"cfp_ci_service": out, "cfp_ci_service_at": now}}
                            )
                            print(f"[{datetime.now()}][worker] merged CI into metrics _id={metric_id}", flush=True)
                        except Exception as e:
                            print(f"[{datetime.now()}][worker] merge failed for metric_id={metric_id}: {e}", flush=True)
                else:
                        print(f"[{datetime.now()}][worker] no metric_id in retained doc; skip merge", flush=True)

            except Exception as e:
                print(f"[{datetime.now()}][worker] error processing doc _id={doc.get('_id')}: {e}", flush=True)

        except Exception as outer:
            print(f"[{datetime.now()}][worker] loop error: {outer}", flush=True)
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
