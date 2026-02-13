import os
from typing import Optional, Tuple
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv

load_dotenv()

DB_PASSWORD = os.environ.get("CNR_POSTEGRESQL_PASSWORD")
DB_USER = os.environ.get("CNR_POSTEGRESQL_USER", "greendigit-u")
DB_NAME = os.environ.get("CNR_POSTEGRESQL_DB", "greendigit-db")
DB_HOST = os.environ.get("CNR_POSTEGRESQL_HOST", "greendigit-postgresql.cloud.d4science.org")
DB_PORT = int(os.environ.get("CNR_POSTEGRESQL_PORT", "5432"))

pool: Optional[SimpleConnectionPool] = None
_FACT_INSERT_KEYS: Optional[list[str]] = None
_FACT_INSERT_SQL: Optional[str] = None

def init_pool(minconn: int = 1, maxconn: int = 5):
    global pool
    if pool is None:
        dsn = f"dbname={DB_NAME} user={DB_USER} host={DB_HOST} password={DB_PASSWORD} port={DB_PORT}"
        pool = SimpleConnectionPool(minconn, maxconn, dsn=dsn)

def get_conn():
    assert pool is not None, "DB pool not initialised"
    return pool.getconn()

def put_conn(conn):
    assert pool is not None, "DB pool not initialised"
    pool.putconn(conn)

def ensure_site_type_mapping(cur, site_type: str) -> str:
    mapping = { "cloud": "detail_cloud", "network": "detail_network", "grid": "detail_grid" }
    detail_table = mapping[site_type]
    cur.execute(
        "INSERT INTO monitoring.site_type_detail (site_type, detail_table_name) "
        "VALUES (%s::monitoring.site_type, %s) ON CONFLICT (site_type) DO NOTHING",
        (site_type, detail_table),
    )
    return detail_table

def get_or_create_site(cur, site_type: str, description: str) -> int:
    cur.execute(
        "SELECT s.site_id FROM monitoring.sites s "
        "WHERE s.site_type = %s::monitoring.site_type AND s.description = %s",
        (site_type, description),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO monitoring.sites (site_type, description) "
        "VALUES (%s::monitoring.site_type, %s) RETURNING site_id",
        (site_type, description),
    )
    return cur.fetchone()[0]

def insert_fact_event(cur, site_id: int, fact: dict) -> int:
    global _FACT_INSERT_KEYS, _FACT_INSERT_SQL

    if _FACT_INSERT_KEYS is None or _FACT_INSERT_SQL is None:
        base_keys = [
            "event_start_timestamp",
            "event_end_timestamp",
            "job_finished",
            "CI_g",
            "CFP_g",
            "PUE",
            "site",
            "energy_wh",
            "work",
            "startexectime",
            "stopexectime",
            "status",
            "owner",
            "execunitid",
            "execunitfinished",
        ]
        cur.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema='monitoring' AND table_name='fact_site_event' AND column_name='publisher_email' "
            "LIMIT 1"
        )
        has_pub = cur.fetchone() is not None
        if has_pub:
            base_keys.append("publisher_email")

        _FACT_INSERT_KEYS = base_keys
        cols = ",".join(["site_id"] + base_keys)
        placeholders = ",".join(["%s"] * (1 + len(base_keys)))
        _FACT_INSERT_SQL = f"INSERT INTO monitoring.fact_site_event ({cols}) VALUES ({placeholders}) RETURNING event_id"

    values = [fact.get(k) for k in _FACT_INSERT_KEYS]
    cur.execute(_FACT_INSERT_SQL, (site_id, *values))
    return cur.fetchone()[0]

def insert_detail(cur, site_type: str, site_id: int, event_id: int, execunitid: str, detail: dict):
    if site_type == "cloud":
        cur.execute(
            "INSERT INTO monitoring.detail_cloud "
            "(event_id,site_id,execunitid,wallclocktime_s,suspendduration_s,cpuduration_s,"
            " cpunormalizationfactor,efficiency,cloud_type,compute_service) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (event_id, site_id, execunitid,
             detail.get("wallclocktime_s"), detail.get("suspendduration_s"),
             detail.get("cpuduration_s"), detail.get("cpunormalizationfactor"),
             detail.get("efficiency"), detail.get("cloud_type"), detail.get("compute_service")),
        )
    elif site_type == "network":
        cur.execute(
            "INSERT INTO monitoring.detail_network "
            "(site_id,event_id,execunitid,amountofdatatransferred,networktype,measurementtype,destinationexecunitid) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (site_id, event_id, execunitid,
             detail.get("amountofdatatransferred"), detail.get("networktype"),
             detail.get("measurementtype"), detail.get("destinationexecunitid")),
        )
    elif site_type == "grid":
        cur.execute(
            "INSERT INTO monitoring.detail_grid "
            "(site_id,event_id,execunitid,wallclocktime_s,cpunormalizationfactor,ncores,normcputime_s,"
            " efficiency,tdp_w,totalcputime_s,scaledcputime_s) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (site_id, event_id, execunitid,
             detail.get("wallclocktime_s"), detail.get("cpunormalizationfactor"),
             detail.get("ncores"), detail.get("normcputime_s"),
             detail.get("efficiency"), detail.get("tdp_w"),
             detail.get("totalcputime_s"), detail.get("scaledcputime_s")),
        )
    else:
        raise ValueError(f"Unsupported site_type {site_type}")

def find_detail_table_for_event(cur, event_id: int) -> Tuple[str, str]:
    cur.execute(
        "SELECT s.site_type::text, std.detail_table_name "
        "FROM monitoring.fact_site_event f "
        "JOIN monitoring.sites s ON s.site_id = f.site_id "
        "JOIN monitoring.site_type_detail std ON std.site_type = s.site_type "
        "WHERE f.event_id = %s",
        (event_id,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError("Event not found")
    return row[0], row[1]

def delete_event(cur, event_id: int):
    site_type, detail_table = find_detail_table_for_event(cur, event_id)
    cur.execute(f"DELETE FROM monitoring.{detail_table} WHERE event_id = %s", (event_id,))
    cur.execute("DELETE FROM monitoring.fact_site_event WHERE event_id = %s", (event_id,))
    return site_type
