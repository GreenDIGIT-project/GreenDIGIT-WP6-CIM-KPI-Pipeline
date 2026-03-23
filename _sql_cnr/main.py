from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError
from datetime import datetime, timezone
from typing import Any, Optional
import traceback
import logging


from cnr_db import (
    init_pool, get_conn, put_conn, ensure_site_type_mapping,
    get_or_create_site, insert_fact_event, insert_detail,
    delete_event, find_detail_table_for_event, ensure_aux_tables,
    insert_enrichment_audit, insert_ingestion_audit_rows, insert_service_health_rows
)
from schemas import (
    CloudDetail, NetworkDetail, GridDetail, Envelope,
    IngestionAuditPayload, ServiceHealthPayload
)

app = FastAPI(title="CNR Metrics Submission API", version="0.1.0")

logger = logging.getLogger("adapter")
logging.basicConfig(level=logging.INFO)

RECORDS_MAX_LIMIT = 500

class CNRDeleteRequest(BaseModel):
    site_id: Optional[int] = None
    vo: Optional[str] = None
    activity: Optional[str] = None
    start: datetime
    end: datetime

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
    insert_enrichment_audit(cur, event_id, payload.audit.model_dump() if payload.audit else None)

    return {"ok": True, "event_id": event_id, "detail_table": mapping_cache[site_type], "site_id": site_id}

def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _row_to_dict(cur, row) -> Optional[dict[str, Any]]:
    if row is None:
        return None
    cols = [desc[0] for desc in cur.description]
    return dict(zip(cols, row))


def _fetchone_dict(cur) -> Optional[dict[str, Any]]:
    return _row_to_dict(cur, cur.fetchone())


def _fetchall_dict(cur) -> list[dict[str, Any]]:
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in rows]


def _build_filters(
    cur,
    *,
    site_id: Optional[int],
    vo: Optional[str],
    activity: Optional[str],
    start: Optional[datetime],
    end: Optional[datetime],
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if site_id is not None:
        clauses.append("f.site_id = %s")
        params.append(site_id)

    if vo:
        clauses.append("LOWER(COALESCE(f.owner, '')) = LOWER(%s)")
        params.append(vo)

    if activity:
        clauses.append("s.site_type::text = %s")
        params.append(activity)

    if start is not None and end is not None:
        clauses.append("f.event_start_timestamp <= %s")
        params.append(_ensure_utc(end))
        clauses.append("f.event_end_timestamp >= %s")
        params.append(_ensure_utc(start))

    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where_sql, params


def _get_cnr_entry_dict(cur, event_id: int) -> dict[str, Any]:
    site_type, detail_table = find_detail_table_for_event(cur, event_id)

    cur.execute(
        "SELECT f.*, s.site_type::text AS site_type, s.description AS site_description "
        "FROM monitoring.fact_site_event f "
        "JOIN monitoring.sites s ON s.site_id = f.site_id "
        "WHERE f.event_id = %s",
        (event_id,),
    )
    fact = _fetchone_dict(cur)
    if not fact:
        raise HTTPException(status_code=404, detail="Event not found")

    cur.execute(f"SELECT * FROM monitoring.{detail_table} WHERE event_id = %s", (event_id,))
    detail = _fetchone_dict(cur)

    return {
        "event_id": event_id,
        "site_type": site_type,
        "detail_table": detail_table,
        "fact": fact,
        "detail": detail,
    }

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
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_aux_tables(cur)
    finally:
        put_conn(conn)

@app.get("/health")
def health():
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"status": "ok", "db": "ok"}
    except Exception as e:
        logger.exception("healthcheck failed")
        raise HTTPException(status_code=503, detail={"status": "degraded", "db": str(e)})
    finally:
        put_conn(conn)

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

@app.post("/ingestion-audit")
def submit_ingestion_audit(payload: IngestionAuditPayload):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_aux_tables(cur)
                insert_ingestion_audit_rows(cur, [row.model_dump() for row in payload.rows])
        return {"ok": True, "rows": len(payload.rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        put_conn(conn)

@app.post("/service-health")
def submit_service_health(payload: ServiceHealthPayload):
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_aux_tables(cur)
                insert_service_health_rows(cur, [row.model_dump() for row in payload.rows])
        return {"ok": True, "rows": len(payload.rows)}
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
                return _get_cnr_entry_dict(cur, event_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Event not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        put_conn(conn)


@app.get("/cnr-db/records")
def list_cnr_records(
    site_id: Optional[int] = Query(default=None),
    vo: Optional[str] = Query(default=None),
    activity: Optional[str] = Query(default=None),
    start: Optional[datetime] = Query(default=None),
    end: Optional[datetime] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=RECORDS_MAX_LIMIT),
    offset: int = Query(default=0, ge=0),
):
    if (start is None) != (end is None):
        raise HTTPException(status_code=400, detail="Provide both start and end, or neither")
    if start is not None and end is not None and _ensure_utc(start) > _ensure_utc(end):
        raise HTTPException(status_code=400, detail="start must be <= end")

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                where_sql, params = _build_filters(
                    cur,
                    site_id=site_id,
                    vo=vo,
                    activity=activity,
                    start=start,
                    end=end,
                )
                cur.execute(
                    "SELECT f.event_id "
                    "FROM monitoring.fact_site_event f "
                    "JOIN monitoring.sites s ON s.site_id = f.site_id "
                    f"{where_sql} "
                    "ORDER BY f.event_start_timestamp DESC, f.event_id DESC "
                    "LIMIT %s OFFSET %s",
                    (*params, limit, offset),
                )
                event_rows = _fetchall_dict(cur)
                records = [_get_cnr_entry_dict(cur, int(row["event_id"])) for row in event_rows]
                return {
                    "ok": True,
                    "site_id": site_id,
                    "vo": vo,
                    "activity": activity,
                    "limit": limit,
                    "offset": offset,
                    "returned": len(records),
                    "records": records,
                }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        put_conn(conn)


@app.get("/cnr-db/records/count")
def count_cnr_records(
    site_id: Optional[int] = Query(default=None),
    vo: Optional[str] = Query(default=None),
    activity: Optional[str] = Query(default=None),
    start: Optional[datetime] = Query(default=None),
    end: Optional[datetime] = Query(default=None),
):
    if (start is None) != (end is None):
        raise HTTPException(status_code=400, detail="Provide both start and end, or neither")
    if start is not None and end is not None and _ensure_utc(start) > _ensure_utc(end):
        raise HTTPException(status_code=400, detail="start must be <= end")

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                where_sql, params = _build_filters(
                    cur,
                    site_id=site_id,
                    vo=vo,
                    activity=activity,
                    start=start,
                    end=end,
                )
                cur.execute(
                    "SELECT COUNT(*) AS count "
                    "FROM monitoring.fact_site_event f "
                    "JOIN monitoring.sites s ON s.site_id = f.site_id "
                    f"{where_sql}",
                    tuple(params),
                )
                row = cur.fetchone()
                return {
                    "ok": True,
                    "site_id": site_id,
                    "vo": vo,
                    "activity": activity,
                    "count": int(row[0] if row else 0),
                }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        put_conn(conn)


@app.post("/cnr-db/delete")
def delete_cnr_records(payload: CNRDeleteRequest):
    start = _ensure_utc(payload.start)
    end = _ensure_utc(payload.end)
    if start > end:
        raise HTTPException(status_code=400, detail="start must be <= end")

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                where_sql, params = _build_filters(
                    cur,
                    site_id=payload.site_id,
                    vo=payload.vo,
                    activity=payload.activity,
                    start=start,
                    end=end,
                )
                cur.execute(
                    "SELECT f.event_id "
                    "FROM monitoring.fact_site_event f "
                    "JOIN monitoring.sites s ON s.site_id = f.site_id "
                    f"{where_sql} "
                    "ORDER BY f.event_id DESC",
                    tuple(params),
                )
                event_rows = _fetchall_dict(cur)
                event_ids = [int(row["event_id"]) for row in event_rows]

                deleted = 0
                for event_id in event_ids:
                    delete_event(cur, event_id)
                    deleted += 1

                return {
                    "ok": True,
                    "site_id": payload.site_id,
                    "vo": payload.vo,
                    "activity": payload.activity,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "deleted_count": deleted,
                }
    except HTTPException:
        raise
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
