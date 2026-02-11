"""
MongoDB persistence helpers for the Auth/Metrics API.

This repo historically ran `login_server.py` inside a container image that
already provided `metrics_store`. Keeping a local copy here makes the code
importable when running outside that image.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from dotenv import load_dotenv
from pymongo import MongoClient, InsertOne
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "metricsdb")
COLL_NAME = os.getenv("COLL_NAME", "metrics")
MONGO_SERVER_SELECTION_TIMEOUT_MS = int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS", "5000"))
MONGO_CONNECT_TIMEOUT_MS = int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "5000"))

# MongoClient does not connect until first operation, so import remains safe in dev environments.
_client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=MONGO_SERVER_SELECTION_TIMEOUT_MS,
    connectTimeoutMS=MONGO_CONNECT_TIMEOUT_MS,
)
_db: Database = _client[DB_NAME]
_col: Collection = _db[COLL_NAME]


def _utc_now_iso_micro() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def store_metric(*, publisher_email: str, body: Any) -> Dict[str, Any]:
    doc = {
        "publisher_email": str(publisher_email).strip().lower(),
        "timestamp": _utc_now_iso_micro(),
        "body": body,
    }
    try:
        res = _col.insert_one(doc)
    except PyMongoError as exc:
        return {"ok": False, "error": str(exc)}
    return {"ok": True, "inserted_id": str(res.inserted_id)}


def store_metrics_bulk(*, publisher_email: str, bodies: Iterable[Any], timestamp: Optional[str] = None) -> Dict[str, Any]:
    ts = timestamp or _utc_now_iso_micro()
    ops: List[InsertOne] = []
    email = str(publisher_email).strip().lower()
    for b in bodies:
        ops.append(InsertOne({"publisher_email": email, "timestamp": ts, "body": b}))
    if not ops:
        return {"ok": True, "inserted_count": 0}
    try:
        res = _col.bulk_write(ops, ordered=False)
    except PyMongoError as exc:
        return {"ok": False, "error": str(exc)}
    return {"ok": True, "inserted_count": int(res.inserted_count)}
