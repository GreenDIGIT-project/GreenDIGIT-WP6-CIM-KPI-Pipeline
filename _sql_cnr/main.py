from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
import traceback, logging

from cnr_db import (
    init_pool, get_conn, put_conn, ensure_site_type_mapping,
    get_or_create_site, insert_fact_event, insert_detail,
    delete_event, find_detail_table_for_event
)
from schemas import CloudDetail, NetworkDetail, GridDetail, Envelope

app = FastAPI(title="CNR Metrics Submission API", version="0.1.0")

logger = logging.getLogger("adapter")
logging.basicConfig(level=logging.INFO)

def _submit_one(cur, payload: Envelope, site_cache: dict, mapping_cache: dict) -> dict:
    site_type = payload.sites.site_type
    if site_type not in mapping_cache:
        mapping_cache[site_type] = ensure_site_type_mapping(cur, site_type)

    site_desc = payload.fact_site_event.get("site")
    site_key = (site_type, site_desc)
    if site_key in site_cache:
        site_id = site_cache[site_key]
    else:
        site_id = get_or_create_site(cur, site_type, site_desc)
        site_cache[site_key] = site_id

    if site_type == "cloud":
        detail = payload.detail_cloud or {}
        CloudDetail(**detail)
    elif site_type == "network":
        detail = payload.detail_network or {}
        NetworkDetail(**detail)
    elif site_type == "grid":
        detail = payload.detail_grid or {}
        GridDetail(**detail)
    else:
        raise HTTPException(status_code=400, detail="Unsupported site_type")

    f = payload.fact_site_event
    event_id = insert_fact_event(cur, site_id, f)
    execunitid = f.get("execunitid")
    insert_detail(cur, site_type, site_id, event_id, execunitid, detail)

    return {"ok": True, "event_id": event_id, "detail_table": mapping_cache[site_type], "site_id": site_id}


@app.middleware("http")
async def log_exceptions(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception as e:
        print("[adapter] Exception:", e, flush=True)
        traceback.print_exc()
        raise

@app.on_event("startup")
def _startup():
    init_pool()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/cnr-sql-adapter")
def submit_metrics(payload: Envelope):
    print("Submitting metrics...")
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                site_cache: dict = {}
                mapping_cache: dict = {}
                res = _submit_one(cur, payload, site_cache, mapping_cache)
                print(f"[DEBUG]: Site type: {payload.sites.site_type}")
                print(f"[DEBUG]: Generated event_id: {res.get('event_id')}", flush=True)
                return JSONResponse(res)
    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.errors())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        put_conn(conn)


@app.post("/cnr-sql-adapter-bulk")
def submit_metrics_bulk(payloads: list[Envelope]):
    """
    Bulk submission to avoid per-entry HTTP overhead.
    Processes all envelopes in a single DB transaction.
    """
    print(f"Submitting metrics bulk... count={len(payloads)}")
    if not payloads:
        raise HTTPException(status_code=400, detail="Empty payload list")
    conn = get_conn()
    try:
        results: list[dict] = []
        with conn:
            with conn.cursor() as cur:
                site_cache: dict = {}
                mapping_cache: dict = {}
                for p in payloads:
                    results.append(_submit_one(cur, p, site_cache, mapping_cache))
        return {"ok": True, "count": len(payloads), "results": results}
    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.errors())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        put_conn(conn)

@app.get("/get-cnr-entry/{event_id}")
def get_cnr_entry(event_id: int):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                site_type, detail_table = find_detail_table_for_event(cur, event_id)

                cur.execute(
                    "SELECT f.*, s.site_type::text AS site_type, s.description AS site_description "
                    "FROM monitoring.fact_site_event f "
                    "JOIN monitoring.sites s ON s.site_id = f.site_id "
                    "WHERE f.event_id = %s",
                    (event_id,),
                )
                fact = cur.fetchone()
                if not fact:
                    raise HTTPException(status_code=404, detail="Event not found")

                cur.execute(f"SELECT * FROM monitoring.{detail_table} WHERE event_id = %s", (event_id,))
                detail = cur.fetchone()

                return {
                    "event_id": event_id,
                    "site_type": site_type,
                    "detail_table": detail_table,
                    "fact": fact,
                    "detail": detail,
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        put_conn(conn)

@app.delete("/delete-cnr-entry/{event_id}")
def delete_cnr_entry(event_id: int):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                site_type = delete_event(cur, event_id)
        return {"ok": True, "deleted_event_id": event_id, "site_type": site_type}
    except ValueError:
        raise HTTPException(status_code=404, detail="Event not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        put_conn(conn)
