from fastapi import FastAPI, Depends, HTTPException, status, Request, Body, Query, APIRouter, Path as PathParam
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, OAuth2PasswordRequestForm
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import List, Dict, Any
from passlib.context import CryptContext
from jose import JWTError, jwt
from typing import Optional
import time

import os, json, zlib
from dotenv import load_dotenv
from metrics_store import store_metric, _col
from sqlalchemy import create_engine, Column, String, Integer, ForeignKey, UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime, timezone
import traceback, uuid
from pathlib import Path
import base64
import requests
from bson import ObjectId




load_dotenv()  # loads from .env in the current folder by default
ACCESS_CONTACT_EMAIL = os.getenv("ACCESS_CONTACT_EMAIL", "g.j.teixeiradepinhoferreira@uva.nl")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
_static_candidates = [
    os.getenv("STATIC_DIR"),
    str(Path(__file__).resolve().parent / "static"),
    str(PROJECT_ROOT / "static"),
    "/app/static",
]
STATIC_DIR = None
for candidate in _static_candidates:
    if not candidate:
        continue
    candidate_path = Path(candidate).resolve()
    if candidate_path.is_dir():
        STATIC_DIR = candidate_path
        break
if STATIC_DIR is None:
    raise RuntimeError(
        "No static directory found. Checked: "
        + ", ".join(candidate for candidate in _static_candidates if candidate)
    )

def embedded_png_data_url(filename: str) -> str:
    image_path = STATIC_DIR / filename
    encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
    return f"data:image/png;base64,{encoded}"

tags_metadata = [
    {
        "name": "Auth",
        "description": "Login to obtain a JWT Bearer token. Use this token in `Authorization: Bearer <token>` on all protected endpoints.",
    },
    {
        "name": "Metrics",
        "description": "Submit and list metrics. **Requires** `Authorization: Bearer <token>`.",
    },
]


app = FastAPI(
    title="GreenDIGIT WP6 CIM Metrics API",
    version="1.0.0",
    openapi_tags=tags_metadata,
    swagger_ui_parameters={"persistAuthorization": True},
    root_path=os.getenv("FASTAPI_ROOT_PATH", "/gd-cim-api"),
    docs_url="/v1/docs",
    openapi_url="/v1/openapi.json",
)
router = APIRouter(prefix="/v1")
app.description = (
    "API for publishing metrics for GreenDIGIT WP6 partners (IFcA, DIRAC, and UTH).\n\n"
    "**Authentication**\n\n"
    "- Obtain a token via **POST /v1/login** using form fields `email` and `password`, "
    "or via **GET /v1/token** with query parameters `email` and `password`. "
    "Your email must be registered beforehand. If it fails (wrong password/unknown), "
    f"please contact {ACCESS_CONTACT_EMAIL}.\n"
    "- Then include `Authorization: Bearer <token>` on all protected requests.\n"
    "- Tokens expire after 1 day — regenerate when needed.\n"
    "- Access is role-based. `submit` is required to store metrics, `publish` is required to replay/publish stored metrics to the CIM/CNR pipeline, and `dashboards` is required for private Grafana dashboards.\n\n"
    "**Metrics read/delete endpoints**\n\n"
    "- `GET /v1/cim-records` and `GET /v1/cim-records/count` list/count raw records stored in the internal MongoDB for the authenticated user.\n"
    "- `POST /v1/cim-db/delete` deletes internal MongoDB records for the authenticated user within a time window and filtered by repeatable `filter_key` expressions.\n"
    "- `POST /v1/submit` stores metrics for authenticated users with the `submit` role.\n"
    "- `POST /v1/submit-cim` replays stored metrics through CIM conversion for authenticated users with the `publish` role.\n"
    "- `GET /v1/cnr-records` and `GET /v1/cnr-records/count` query CNR SQL records by `site_id`, `vo`, `activity`, and time window.\n"
    "- `POST /v1/cnr-db/delete` is disabled.\n"
    "- Example request snippets are available in `scripts/example-edit-metrics.sh`, `scripts/example_requests/example-request-metrics.sh`, and `scripts/example_requests/example-request-cim.sh`.\n\n"
    "**Example auth flow**\n\n"
    "1. `GET /v1/token?email=demo.publisher@example.org&password=correct-horse-battery-staple`\n"
    "2. Use the returned token as `Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.demo.signature`\n"
    "3. Call a protected endpoint such as `GET /v1/cim-records?start=2026-03-01T00:00:00Z&end=2026-03-31T23:59:59Z&limit=20`\n\n"
    "### Funding and acknowledgements\n"
    "This work is funded from the European Union’s Horizon Europe research and innovation programme "
    "through the [GreenDIGIT project](https://greendigit-project.eu/), under the grant agreement "
    "No. [101131207](https://cordis.europa.eu/project/id/101131207).\n\n"
    # GitHub badge (Markdown)
    "[![GitHub Repo](https://img.shields.io/badge/github-GreenDIGIT--AuthServer-blue?logo=github)]"
    "(https://github.com/g-uva/GreenDIGIT-AuthServer)\n\n"
    # Logos (HTML so we can size them)
    f'<p><img src="{embedded_png_data_url("EN-Funded-by-the-EU-POS-2.png")}" alt="Funded by the EU" width="160"> '
    f'<img src="{embedded_png_data_url("cropped-GD_logo.png")}" alt="GreenDIGIT" width="120"></p>'
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
security = HTTPBearer()

# Secret key for JWT
SECRET_KEY = os.environ["JWT_GEN_SEED_TOKEN"]
if not SECRET_KEY:
    raise RuntimeError("JWT_GEN_SEED_TOKEN not valid. You must generate a valid token on the server. :)")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_SECONDS = 86400 # 1 day
JWT_ISSUER = os.environ.get("JWT_ISSUER", "greendigit-login-uva")
BULK_MAX_OPS = int(os.getenv("BULK_MAX_OPS", "1000"))
CIM_INTERNAL_ENDPOINT = os.getenv("CIM_INTERNAL_ENDPOINT", "http://cim-service:8012/transform-and-forward")
ADMIN_EMAILS = {e.strip().lower() for e in os.getenv("ADMIN_EMAILS", "").split(",") if e.strip()}
VALID_ROLES = {"submit", "publish", "dashboards"}
CIM_SUBMIT_TIMEOUT_SECONDS = int(os.getenv("CIM_SUBMIT_TIMEOUT_SECONDS", "900"))
METRICS_ME_MAX_LIMIT = int(os.getenv("METRICS_ME_MAX_LIMIT", "1000"))
MONGO_SERVER_SELECTION_TIMEOUT_MS = int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS", "5000"))
MONGO_CONNECT_TIMEOUT_MS = int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "5000"))

RECORDS_MAX_LIMIT = int(os.getenv("RECORDS_MAX_LIMIT", "500"))
CNR_SQL_API_BASE = os.getenv("CNR_SQL_API_BASE", "http://sql-adapter:8033")

# SQLite setup
SQLALCHEMY_DATABASE_URL = "sqlite:///./users.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)

class UserRole(Base):
    __tablename__ = "user_roles"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    role = Column(String, nullable=False, index=True)
    __table_args__ = (UniqueConstraint("user_id", "role", name="uq_user_roles_user_id_role"),)

Base.metadata.create_all(bind=engine)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class SubmitData(BaseModel):
    field1: str
    field2: int

class GetTokenRequest(BaseModel):
    email: str
    password: str

    class Config:
        schema_extra = {
            "example": {
                "email": "demo.publisher@example.org",
                "password": "correct-horse-battery-staple",
            }
        }
    
class MetricItem(BaseModel):
    node: str
    metric: str
    value: float
    timestamp: str
    cfp_ci_service: Dict[str, Any] = Field(..., description="Embedded CI service response")

    class Config:
        schema_extra = {
            "example": {
                "node": "RAL-LCG2-worker-01",
                "metric": "energy_wh",
                "value": 8500.0,
                "timestamp": "2024-05-01T10:30:00Z",
                "cfp_ci_service": {
                    "ci_gco2kwh": 172.4,
                    "pue": 1.4,
                    "cfp_g": 2.05,
                },
            }
        }

class PostCimJsonRequest(BaseModel):
    publisher_email: str
    job_id: str
    metrics: List[MetricItem]

    class Config:
        schema_extra = {
            "example": {
                "publisher_email": "demo.publisher@example.org",
                "job_id": "job-42",
                "metrics": [
                    {
                        "node": "RAL-LCG2-worker-01",
                        "metric": "energy_wh",
                        "value": 8500.0,
                        "timestamp": "2024-05-01T10:30:00Z",
                        "cfp_ci_service": {
                            "ci_gco2kwh": 172.4,
                            "pue": 1.4,
                            "cfp_g": 2.05,
                        },
                    }
                ],
            }
        }

class SubmitCIMRequest(BaseModel):
    publisher_email: str = Field(..., description="Target publisher email to pull records for (MongoDB field: publisher_email).")
    start: Optional[datetime] = Field(default=None, description="Start time (UTC) for MongoDB timestamp filtering (inclusive).")
    end: Optional[datetime] = Field(default=None, description="End time (UTC) for MongoDB timestamp filtering (inclusive).")
    end_inclusive: bool = Field(default=True, description="Whether `end` is inclusive. Use false for half-open windows [start, end).")
    entry_id: Optional[str] = Field(default=None, description="Optional MongoDB _id of a specific stored entry to replay.")
    limit_docs: int = Field(default=50, ge=1, le=5000, description="Max MongoDB documents to load when using start/end.")
    after_timestamp: Optional[datetime] = Field(
        default=None,
        description="Pagination cursor: only return docs with timestamp > after_timestamp (or same timestamp but _id > after_id).",
    )
    after_id: Optional[str] = Field(default=None, description="Pagination cursor: last seen MongoDB _id (ObjectId as hex string).")

    class Config:
        schema_extra = {
            "examples": {
                "time_window": {
                    "summary": "Replay a time window",
                    "value": {
                        "publisher_email": "demo.publisher@example.org",
                        "start": "2026-02-06T00:00:00Z",
                        "end": "2026-02-08T00:00:00Z",
                        "limit_docs": 10,
                    },
                },
                "half_open_window": {
                    "summary": "Replay a 15-minute half-open window",
                    "value": {
                        "publisher_email": "demo.publisher@example.org",
                        "start": "2025-09-01T00:00:00Z",
                        "end": "2025-09-01T00:15:00Z",
                        "end_inclusive": False,
                        "limit_docs": 1000,
                    },
                },
                "cursor_pagination": {
                    "summary": "Replay the next page with a cursor",
                    "value": {
                        "publisher_email": "demo.publisher@example.org",
                        "start": "2026-02-06T00:00:00Z",
                        "end": "2026-02-08T00:00:00Z",
                        "limit_docs": 10,
                        "after_timestamp": "2026-02-06T00:14:03.000000Z",
                        "after_id": "65c1d7ec4a5dd865d6f5a001",
                    },
                },
                "entry_id": {
                    "summary": "Replay one stored entry by Mongo ObjectId",
                    "value": {
                        "publisher_email": "demo.publisher@example.org",
                        "entry_id": "65c1d7ec4a5dd865d6f5a001",
                    },
                },
            }
        }

class CIMDeleteRequest(BaseModel):
    filter_key: List[str] = Field(
        ...,
        description="Conjunction of recursive Mongo key filters in `key=value` form. Example: ['SiteName=EGI.SARA.nl', 'Owner=DIRAC'].",
    )
    start: datetime = Field(..., description="Inclusive start timestamp (UTC).")
    end: datetime = Field(..., description="Inclusive end timestamp (UTC).")

    class Config:
        schema_extra = {
            "examples": {
                "partial_delete": {
                    "summary": "Delete a subset within a time window",
                    "value": {
                        "filter_key": ["SiteName=EGI.SARA.nl", "Owner=DIRAC"],
                        "start": "2026-03-01T00:00:00Z",
                        "end": "2026-03-31T23:59:59Z",
                    },
                },
                "empty_result": {
                    "summary": "Delete with no matches",
                    "value": {
                        "filter_key": ["SiteName=THIS_SITE_DOES_NOT_EXIST"],
                        "start": "2026-03-01T00:00:00Z",
                        "end": "2026-03-31T23:59:59Z",
                    },
                },
            }
        }

# class CNRDeleteRequest(BaseModel):
#     site_id: Optional[int] = Field(default=None, description="Optional site_id filter.")
#     vo: Optional[str] = Field(default=None, description="Optional VO/owner filter.")
#     activity: Optional[str] = Field(default=None, description="Optional activity/site_type filter (cloud|grid|network).")
#     start: datetime = Field(..., description="Inclusive start timestamp (UTC).")
#     end: datetime = Field(..., description="Inclusive end timestamp (UTC).")
#
#     class Config:
#         schema_extra = {
#             "example": {
#                 "site_id": 123,
#                 "vo": "DIRAC",
#                 "activity": "grid",
#                 "start": "2026-03-01T00:00:00Z",
#                 "end": "2026-03-31T23:59:59Z",
#             }
#         }


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _iso_utc_micro(dt: datetime) -> str:
    """ISO string in UTC with microseconds always present (lexicographic ordering matches chronology)."""
    return _ensure_utc(dt).isoformat(timespec="microseconds")

def _coerce_object_id(raw: str) -> ObjectId:
    try:
        return ObjectId(str(raw))
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid entry_id (expected Mongo ObjectId): {raw}")

def _parse_iso_dt_or_400(raw: str, label: str) -> datetime:
    s = str(raw).strip()
    if not s:
        raise HTTPException(status_code=400, detail=f"Missing {label} datetime")
    try:
        # Accept "Z" suffix, normalise to UTC.
        return _ensure_utc(datetime.fromisoformat(s.replace("Z", "+00:00")))
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid {label} datetime (expected ISO 8601): {raw}")

def _split_start_end(raw: str) -> tuple[str, str]:
    """
    Path param parsing for "start_end".
    Supports separators that are safe-ish in URLs:
      - `--` (recommended)
      - `_`
      - `..`
      - `,`
    """
    s = str(raw).strip()
    for sep in ("--", "_", "..", ","):
        if sep in s:
            a, b = s.split(sep, 1)
            a = a.strip()
            b = b.strip()
            if a and b:
                return a, b
    raise HTTPException(
        status_code=400,
        detail="Invalid start_end format. Expected '<start>--<end>' or '<start>_<end>' (ISO 8601).",
    )


def _store_metric_in_col(*, col, publisher_email: str, body: Any) -> Dict[str, Any]:
    doc = {
        "publisher_email": str(publisher_email).strip().lower(),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
        "body": body,
    }
    try:
        res = col.insert_one(doc)
    except PyMongoError as exc:
        return {"ok": False, "error": str(exc)}
    return {"ok": True, "inserted_id": str(res.inserted_id)}


def _normalize_site(value: Any) -> str:
    return str(value).strip().lower()


def _parse_candidate_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    s = str(value).strip()
    if not s:
        return None
    # Try ISO-8601 first (including trailing Z).
    try:
        return _ensure_utc(datetime.fromisoformat(s.replace("Z", "+00:00")))
    except Exception:
        pass
    # DIRAC often uses "YYYY-MM-DD HH:MM:SS" without timezone.
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return _ensure_utc(datetime.strptime(s, fmt))
        except Exception:
            continue
    return None


def _doc_matches_time_window(doc: dict[str, Any], start_dt: datetime, end_dt: datetime) -> bool:
    keys = {"timestamp", "Timestamp", "EndExecTime", "StartExecTime", "SubmissionTime"}
    candidates: list[Any] = [doc.get("timestamp")]

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                if k in keys:
                    candidates.append(v)
                walk(v)
            return
        if isinstance(node, list):
            for item in node:
                walk(item)

    walk(doc.get("body"))
    for raw in candidates:
        dt = _parse_candidate_dt(raw)
        if dt is not None and start_dt <= dt <= end_dt:
            return True
    return False


def _doc_matches_site(doc: dict[str, Any], site: str) -> bool:
    target = _normalize_site(site)
    site_keys = {"site", "Site", "SiteName", "SiteGOCDB", "SiteDIRAC", "site_id"}

    def walk(node: Any) -> bool:
        if isinstance(node, dict):
            for k, v in node.items():
                if k in site_keys and v is not None and _normalize_site(v) == target:
                    return True
                if walk(v):
                    return True
            return False
        if isinstance(node, list):
            for item in node:
                if walk(item):
                    return True
            return False
        return False

    # Check both top-level doc and body payload recursively.
    return walk(doc)

def _parse_filter_exprs(raw_filters: Optional[List[str]]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for raw in raw_filters or []:
        s = str(raw).strip()
        if not s:
            continue
        for sep in ("=", ":"):
            if sep in s:
                key, value = s.split(sep, 1)
                key = key.strip()
                value = value.strip()
                if key and value:
                    pairs.append((key, value))
                    break
        else:
            raise HTTPException(status_code=400, detail=f"Invalid filter_key entry: {raw}. Expected key=value")
    return pairs


def _node_has_key_value(node: Any, key: str, value: str) -> bool:
    key_l = key.strip().lower()
    value_l = str(value).strip().lower()

    if isinstance(node, dict):
        for k, v in node.items():
            if str(k).strip().lower() == key_l and v is not None and str(v).strip().lower() == value_l:
                return True
            if _node_has_key_value(v, key, value):
                return True
        return False

    if isinstance(node, list):
        return any(_node_has_key_value(item, key, value) for item in node)

    return False


def _doc_matches_all_filter_exprs(doc: dict[str, Any], filters: list[tuple[str, str]]) -> bool:
    return all(_node_has_key_value(doc, key, value) for key, value in filters)


def _find_unmatched_filter_exprs(candidates: list[dict[str, Any]], filters: list[tuple[str, str]]) -> list[str]:
    unmatched: list[str] = []
    for key, value in filters:
        if not any(_node_has_key_value(doc, key, value) for doc in candidates):
            unmatched.append(f"{key}={value}")
    return unmatched


def _resolve_limit_offset_page(limit: Optional[int], offset: Optional[int], page: Optional[int], cap: int) -> tuple[int, int]:
    effective_limit = cap if limit is None else min(int(limit), cap)
    effective_offset = int(offset or 0)
    if page is not None:
        if int(page) < 1:
            raise HTTPException(status_code=400, detail="page must be >= 1")
        effective_offset = (int(page) - 1) * effective_limit
    return effective_limit, effective_offset


def _serialise_mongo_doc(doc: dict[str, Any]) -> dict[str, Any]:
    out = dict(doc)
    if "_id" in out:
        out["_id"] = str(out["_id"])
    if "timestamp" in out and not isinstance(out["timestamp"], str):
        out["timestamp"] = str(out["timestamp"])
    return out


def _forward_sql_adapter(method: str, path: str, *, params: Optional[dict[str, Any]] = None, json_body: Optional[dict[str, Any]] = None) -> Any:
    url = f"{CNR_SQL_API_BASE}{path}"
    try:
        response = requests.request(method, url, params=params, json=json_body, timeout=(10, 120))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to call SQL adapter: {exc}")

    try:
        payload = response.json()
    except Exception:
        payload = {"raw": (response.text or "")[:2000]}

    if not response.ok:
        raise HTTPException(status_code=response.status_code, detail=payload)
    return payload

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def load_allowed_emails():
    path = os.path.join(os.path.dirname(__file__), "allowed_emails.txt")
    if not os.path.exists(path):
        return set()
    with open(path, "r") as f:
        return set(
            line.strip().lower()
            for line in f
            if line.strip() and not line.lstrip().startswith("#")
        )

def _load_email_file(filename: str) -> set[str]:
    path = Path(__file__).resolve().parent / filename
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8") as f:
        return {
            line.strip().lower()
            for line in f
            if line.strip() and not line.lstrip().startswith("#")
        }

def _normalise_role(role: str) -> str:
    role = (role or "").strip().lower()
    if role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail=f"Unknown role: {role}")
    return role

def grant_user_role(db: Session, user: User, role: str) -> bool:
    role = _normalise_role(role)
    result = db.execute(
        UserRole.__table__
        .insert()
        .prefix_with("OR IGNORE")
        .values(user_id=user.id, role=role)
    )
    return bool(result.rowcount)

def bootstrap_roles_from_files(db: Session) -> int:
    changed = 0
    for email in _load_email_file("allowed_emails.txt"):
        user = db.query(User).filter(User.email == email).first()
        if user:
            changed += int(grant_user_role(db, user, "submit"))
            changed += int(grant_user_role(db, user, "dashboards"))
    for email in _load_email_file("submit_emails.txt"):
        user = db.query(User).filter(User.email == email).first()
        if user:
            changed += int(grant_user_role(db, user, "publish"))
    if changed:
        db.commit()
    return changed

def get_user_roles(email: str, db: Session) -> list[str]:
    email = email.strip().lower()
    rows = (
        db.query(UserRole.role)
        .join(User, UserRole.user_id == User.id)
        .filter(User.email == email)
        .order_by(UserRole.role)
        .all()
    )
    return [row[0] for row in rows]

def user_has_role(email: str, role: str, db: Session) -> bool:
    role = _normalise_role(role)
    email = email.strip().lower()
    return (
        db.query(UserRole)
        .join(User, UserRole.user_id == User.id)
        .filter(User.email == email, UserRole.role == role)
        .first()
        is not None
    )

def _ensure_bootstrap_roles_for_user(db: Session, user: User) -> None:
    email = user.email.strip().lower()
    changed = False
    if email in _load_email_file("allowed_emails.txt"):
        changed = grant_user_role(db, user, "submit") or changed
        changed = grant_user_role(db, user, "dashboards") or changed
    if email in _load_email_file("submit_emails.txt"):
        changed = grant_user_role(db, user, "publish") or changed
    if changed:
        db.commit()

with SessionLocal() as _bootstrap_db:
    bootstrap_roles_from_files(_bootstrap_db)

def access_not_allowed_response(email: str) -> HTMLResponse:
    safe_email = email.strip().lower()
    return HTMLResponse(
        f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Access request needed</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7f4;
            color: #25302b;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 24px;
        }}
        .message {{
            max-width: 560px;
            background: #fff;
            border: 1px solid #dfe7df;
            border-radius: 8px;
            padding: 28px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        }}
        h1 {{ color: #215f32; font-size: 1.5rem; margin-bottom: 12px; }}
        p {{ line-height: 1.5; margin-bottom: 12px; }}
        a {{ color: #1f5f8f; font-weight: 650; }}
    </style>
</head>
<body>
    <main class="message">
        <h1>Access request needed</h1>
        <p>The email <strong>{safe_email}</strong> is not currently allowed to register for this service.</p>
        <p>To request access to dashboards and/or permission to submit metrics, contact <a href="mailto:{ACCESS_CONTACT_EMAIL}">{ACCESS_CONTACT_EMAIL}</a>.</p>
        <p>After access is granted, return to the login page and register with your email and password.</p>
    </main>
</body>
</html>""",
        status_code=403,
    )

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security), db: Session = Depends(get_db)):
    token = credentials.credentials
    try:
        payload = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=[ALGORITHM],
            options={"require": ["sub", "exp", "iat", "nbf", "iss"]},
            issuer=JWT_ISSUER
        )
        email: str = payload.get("sub")
        if email is None:
            raise HTTPException(status_code=401, detail="Invalid token")
        user = db.query(User).filter(User.email == email).first()
        if not user:
            raise HTTPException(status_code=401, detail="Invalid token")
        return email
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

def require_role(role: str):
    role = _normalise_role(role)

    def dependency(email: str = Depends(verify_token), db: Session = Depends(get_db)) -> str:
        if not user_has_role(email, role, db):
            raise HTTPException(status_code=403, detail=f"Missing required role: {role}")
        return email

    return dependency

@router.get("/health", include_in_schema=False)
def api_health():
    primary_mongo_ok = False
    try:
        primary_mongo_ok = bool(_col.database.client.admin.command("ping").get("ok"))
    except Exception:
        primary_mongo_ok = False

    overall_ok = primary_mongo_ok
    status_code = 200 if overall_ok else 503
    payload = {
        "status": "ok" if overall_ok else "degraded",
        "mongo_primary": "ok" if primary_mongo_ok else "error",
    }
    return JSONResponse(status_code=status_code, content=payload)

@app.middleware("http")
async def catch_all_errors(request: Request, call_next):
    req_id = str(uuid.uuid4())[:8]
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        # Log full traceback to stdout (docker logs / journalctl)
        print(f"[ERR {req_id}] {request.method} {request.url}\n{tb}", flush=True)
        # Return JSON instead of plain text
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(e).__name__}: {e}", "req_id": req_id}
        )

@router.post(
    "/login",
    tags=["Auth"],
    summary="Login and get a JWT access token",
    description=(
        "Use form fields `username` (email) and `password`.\n\n"
        "Returns a JWT for `Authorization: Bearer <token>`.\n\n"
        "Swagger example credentials:\n"
        "- `username`: `demo.publisher@example.org`\n"
        "- `password`: `correct-horse-battery-staple`"
    ),
    response_class=HTMLResponse
)
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    email_lower = form_data.username.strip().lower()
    user = db.query(User).filter(User.email == email_lower).first()
    if not user:
        # First login: check if allowed, then register
        allowed_emails = load_allowed_emails()
        if email_lower not in allowed_emails:
            return access_not_allowed_response(email_lower)
        hashed_password = pwd_context.hash(form_data.password)
        db_user = User(email=email_lower, hashed_password=hashed_password)
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        user = db_user
        _ensure_bootstrap_roles_for_user(db, user)
    elif not pwd_context.verify(form_data.password, user.hashed_password):
        raise HTTPException(status_code=400, detail=f"Incorrect password. If you have forgotten your password please contact the GreenDIGIT team: {ACCESS_CONTACT_EMAIL}.")
    else:
        _ensure_bootstrap_roles_for_user(db, user)
    now = int(time.time())
    token_data = {
        "sub": user.email,
        "iss": JWT_ISSUER,
        "iat": now,
        "nbf": now,
        "exp": now + ACCESS_TOKEN_EXPIRE_SECONDS,
    }
    token = jwt.encode(token_data, SECRET_KEY, algorithm=ALGORITHM)
    return f"""
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>API Token Generated</title>
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    padding: 20px;
                }}
                
                .container {{
                    background: white;
                    padding: 40px;
                    border-radius: 12px;
                    box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                    width: 100%;
                    max-width: 600px;
                    align-items: center;
                }}
                
                h1 {{
                    text-align: center;
                }}
                
                h2 {{
                    color: #333;
                    margin-bottom: 30px;
                    text-align: center;
                    font-size: 24px;
                    font-weight: 600;
                }}
                
                .token-section {{
                    margin-bottom: 30px;
                }}
                
                .token-label {{
                    font-weight: 600;
                    color: #333;
                    margin-bottom: 8px;
                    font-size: 14px;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                }}
                
                .token-container {{
                    position: relative;
                    background: #f8f9fa;
                    border: 2px solid #e1e5e9;
                    border-radius: 8px;
                    padding: 16px;
                    margin-bottom: 20px;
                }}
                
                .token-value {{
                    font-family: 'Courier New', monospace;
                    font-size: 14px;
                    color: #333;
                    word-break: break-all;
                    line-height: 1.5;
                    margin: 0;
                    padding-right: 50px;
                }}
                
                .copy-btn {{
                    position: absolute;
                    top: 12px;
                    right: 12px;
                    background: #667eea;
                    color: white;
                    border: none;
                    padding: 8px 12px;
                    border-radius: 6px;
                    font-size: 12px;
                    cursor: pointer;
                    transition: background-color 0.3s ease;
                }}
                
                .copy-btn:hover {{
                    background: #5a6fd8;
                }}
                
                .copy-btn.copied {{
                    background: #28a745;
                }}
                
                .success-banner {{
                    background: linear-gradient(90deg, #28a745 0%, #20c997 100%);
                    color: white;
                    padding: 16px;
                    border-radius: 8px;
                    text-align: center;
                    margin-bottom: 30px;
                    font-weight: 500;
                }}
                
                .warning {{
                    background: #fff3cd;
                    border: 1px solid #ffeaa7;
                    color: #856404;
                    padding: 16px;
                    border-radius: 8px;
                    font-size: 14px;
                    text-align: center;
                }}

                .dashboard-form {{
                    margin-top: 16px;
                    margin-bottom: 10px;
                    text-align: center;
                }}

                .dashboard-btn {{
                    display: inline-block;
                    background: #f97316;
                    color: #fff;
                    border: none;
                    border-radius: 8px;
                    padding: 12px 18px;
                    font-size: 14px;
                    font-weight: 600;
                    cursor: pointer;
                }}

                .dashboard-btn:hover {{
                    background: #ea580c;
                }}
                
                .back-link {{
                    display: inline-block;
                    margin-top: 20px;
                    color: #667eea;
                    text-decoration: none;
                    font-size: 14px;
                    transition: color 0.3s ease;
                }}
                
                .back-link:hover {{
                    color: #5a6fd8;
                    text-decoration: underline;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="success-banner">
                    ✓ Token Generated Successfully
                </div>
                
                <h2>Your API Token</h2>
                
                <div class="token-section">
                    <div class="token-label">Access Token</div>
                    <div class="token-container">
                        <div class="token-value" id="access-token">
                            {token}
                        </div>
                        <button class="copy-btn" onclick="copyToken('access-token', this)">Copy</button>
                    </div>
                </div>
                
                <div class="token-section">
                    <div class="token-label">Token Type</div>
                    <div class="token-container">
                        <div class="token-value" id="token-type">
                            bearer
                        </div>
                        <button class="copy-btn" onclick="copyToken('token-type', this)">Copy</button>
                    </div>
                </div>
                
                <div class="warning">
                    ⚠️ This token expires in 24 hours. Store it securely and do not share it.
                </div>

                <form class="dashboard-form" method="post" action="/metricsdb-dashboard/v1/charts/auth/sso">
                    <input type="hidden" name="token" value="{token}">
                    <input type="hidden" name="next" value="/metricsdb-dashboard/v1/charts/">
                    <button class="dashboard-btn" type="submit">Login to Dashboard</button>
                </form>
            </div>
            
            <script>
                function copyToken(elementId, button) {{
                    const tokenElement = document.getElementById(elementId);
                    const tokenText = tokenElement.textContent.trim();
                    
                    navigator.clipboard.writeText(tokenText).then(function() {{
                        button.textContent = 'Copied!';
                        button.classList.add('copied');
                        
                        setTimeout(function() {{
                            button.textContent = 'Copy';
                            button.classList.remove('copied');
                        }}, 2000);
                    }});
                }}
                
                // You can populate the actual token values like this:
                // document.getElementById('access-token').textContent = json.access_token;
                // document.getElementById('token-type').textContent = json.token_type;
            </script>
        </body>
        </html>
    """

@router.get(
    "/token-ui",
    tags=["Auth"],
    summary="Simple HTML login to manually obtain a token",
    description="Convenience page that POSTs to `/v1/login`.",
    response_class=HTMLResponse
)
def token_ui(request: Request):
    gd_logo = embedded_png_data_url("cropped-GD_logo.png")
    eu_logo = embedded_png_data_url("EN-Funded-by-the-EU-POS-2.png")

    return f"""
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>API Token Generator</title>
            <style>
                * {{
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }}
                
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    padding: 20px;
                }}
                
                .container {{
                    display: flex;
                    flex-direction: column;
                    justify-content: center;
                    background: white;
                    padding: 40px;
                    border-radius: 12px;
                    box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                    width: 100%;
                    max-width: 500px;
                }}
                
                h2 {{
                    color: #333;
                    margin-bottom: 30px;
                    text-align: center;
                    font-size: 24px;
                    font-weight: 600;
                }}
                
                form {{
                    margin-bottom: 30px;
                }}
                
                input {{
                    width: 100%;
                    padding: 12px 16px;
                    margin-bottom: 16px;
                    border: 2px solid #e1e5e9;
                    border-radius: 8px;
                    font-size: 16px;
                    transition: border-color 0.3s ease;
                }}
                
                input:focus {{
                    outline: none;
                    border-color: #667eea;
                }}
                
                button {{
                    width: 100%;
                    padding: 14px;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    border: none;
                    border-radius: 8px;
                    font-size: 16px;
                    font-weight: 600;
                    cursor: pointer;
                    transition: transform 0.2s ease;
                }}
                
                button:hover {{
                    transform: translateY(-2px);
                }}

                .dashboard-btn {{
                    margin-top: 10px;
                    background: #f97316;
                }}

                .dashboard-btn:hover {{
                    background: #ea580c;
                    cursor: pointer;
                }}
                
                .info {{
                    background: #f8f9fa;
                    padding: 20px;
                    border-radius: 8px;
                    border-left: 4px solid #ffc107;
                    margin-bottom: 20px;
                }}
                
                .info p {{
                    color: #666;
                    font-size: 14px;
                    line-height: 1.5;
                    margin-bottom: 0;
                }}
                
                .contact {{
                    background: #f8f9fa;
                    padding: 20px;
                    border-radius: 8px;
                    border-left: 4px solid #17a2b8;
                    margin-bottom: 20px;
                    width: 100%;
                }}
                
                .contact p {{
                    color: #666;
                    font-size: 14px;
                    margin-bottom: 10px;
                }}
                
                .contact ul {{
                    list-style: none;
                    margin: 0;
                    padding: 0;
                }}
                
                .contact li {{
                    color: #667eea;
                    font-size: 14px;
                    margin-bottom: 5px;
                }}
                
                .contact li:last-child {{
                    margin-bottom: 0;
                }}

                /* Footer style */
                .footer {{
                    font-size: 12px;
                    color: #555;
                    text-align: center;
                    margin-top: 30px;
                    line-height: 1.5;
                }}

                .footer a {{
                    color: #667eea;
                    text-decoration: none;
                }}

                .footer a:hover {{
                    text-decoration: underline;
                }}

                .footer-logos {{
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    gap: 20px;
                    margin-top: 15px;
                }}

                .footer-logos img {{
                    max-height: 50px;
                    object-fit: contain;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>GreenDIGIT WP6 CIM API</h1>
                <h2 style="margin-top:15px;">Login to generate token</h2>
                <form id="token-form" action="login" method="post">
                    <input id="token-username" name="username" type="email" placeholder="Email" required>
                    <input id="token-password" name="password" type="password" placeholder="Password" required>
                    <button type="submit">Get Token</button>
                    <button class="dashboard-btn" type="button" onclick="loginDashboard()">Login to Dashboard</button>
                </form>
                
                <div class="info">
                    <p>The token is only valid for 1 day. You must regenerate in order to access.</p>
                </div>
                
                <div class="contact">
                    <p>If you have problems logging in, or if you need access to dashboards and/or metric submission, please contact:</p>
                    <ul>
                        <li>{ACCESS_CONTACT_EMAIL}</li>
                    </ul>
                </div>

                <div class="footer">
                    This work is funded from the European Union’s Horizon Europe research and innovation programme through the 
                    <a href="https://greendigit-project.eu/" target="_blank">GreenDIGIT project</a>, under the grant agreement No. 
                    <a href="https://cordis.europa.eu/project/id/101131207" target="_blank">101131207</a>.
                    
                    <div class="footer-logos">
                        <img src="{gd_logo}" alt="GreenDIGIT logo">
                        <img src="{eu_logo}" alt="Funded by the EU">
                    </div>
                </div>
            </div>
            <script>
                function loginDashboard() {{
                    const username = document.getElementById('token-username').value.trim();
                    const password = document.getElementById('token-password').value;
                    if (!username || !password) {{
                        alert('Please fill in email and password first.');
                        return;
                    }}

                    const f = document.createElement('form');
                    f.method = 'post';
                    f.action = '/metricsdb-dashboard/v1/charts/auth/login';

                    const emailInput = document.createElement('input');
                    emailInput.type = 'hidden';
                    emailInput.name = 'email';
                    emailInput.value = username;
                    f.appendChild(emailInput);

                    const passInput = document.createElement('input');
                    passInput.type = 'hidden';
                    passInput.name = 'password';
                    passInput.value = password;
                    f.appendChild(passInput);

                    const nextInput = document.createElement('input');
                    nextInput.type = 'hidden';
                    nextInput.name = 'next';
                    nextInput.value = '/metricsdb-dashboard/v1/charts/';
                    f.appendChild(nextInput);

                    document.body.appendChild(f);
                    f.submit();
                }}
            </script>
        </body>
        </html>
    """

@router.post(
    "/submit",
    tags=["Metrics"],
    summary="Submit a metrics JSON payload",
    description=(
        "Stores an arbitrary JSON document as a metric entry.\n\n"
        "**Requires:** `Authorization: Bearer <token>` and the `submit` role.\n\n"
        "The `publisher_email` is derived from the token’s `sub` claim.\n\n"
        "Example header:\n"
        "- `Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.demo.signature`"
    ),
    responses={
        200: {"description": "Stored successfully"},
        400: {"description": "Invalid JSON body"},
        401: {"description": "Missing/invalid Bearer token"},
        403: {"description": "Authenticated user is missing the submit role"},
        500: {"description": "Database error"},
    },
)
async def submit(
    request: Request,
    publisher_email: str = Depends(require_role("submit")),
    _example: Any = Body(
        default=None,
        examples={
            "sample": {
                "summary": "Example metric payload",
                "value": {
                    "cpu_watts": 11.2,
                    "mem_bytes": 734003200,
                    "labels": {"node": "compute-0", "job_id": "abc123"}
                },
            }
        },
    ),
):
    body = await request.json()
    ack = store_metric(publisher_email=publisher_email, body=body)
    if not ack.get("ok"):
        raise HTTPException(status_code=500, detail=f"DB error: {ack.get('error')}")
    return {"stored": ack}


@router.post(
    "/submit-cim",
    tags=["Metrics"],
    summary="Replay stored metrics through CIM conversion (enrich + forward to SQL adapter).",
    description=(
        "Loads previously stored metric payload(s) from MongoDB and forwards the embedded `body` to the CIM service.\n\n"
        "- Provide a `start`/`end` time window (filters on MongoDB field `timestamp`), OR provide `entry_id`.\n"
        "- By default, the authenticated user can replay their own metrics.\n"
        "- To replay other publishers, set `ADMIN_EMAILS` to include your email.\n\n"
        "**Requires:** `Authorization: Bearer <token>` and the `publish` role.\n\n"
        "Example request body values are derived from `scripts/example_requests/example-request-cim.sh`, with fake documentation-only values."
    ),
    responses={
        200: {"description": "Forwarded to CIM successfully"},
        400: {"description": "Invalid request"},
        401: {"description": "Missing/invalid Bearer token"},
        403: {"description": "Authenticated user is missing the publish role, or is not allowed to replay the requested publisher_email"},
        404: {"description": "No matching stored metrics found"},
        502: {"description": "CIM call failed"},
    },
)
async def submit_cim(
    request: Request,
    payload: SubmitCIMRequest = Body(
        ...,
        examples=SubmitCIMRequest.Config.schema_extra["examples"],
    ),
    caller_email: str = Depends(require_role("publish")),
):
    caller = caller_email.strip().lower()
    publisher_email = payload.publisher_email.strip().lower()
    if caller != publisher_email and caller not in ADMIN_EMAILS:
        raise HTTPException(status_code=403, detail="Not allowed to replay metrics for this publisher_email")

    docs: list[dict] = []
    if payload.entry_id:
        oid = _coerce_object_id(payload.entry_id)
        doc = _col.find_one({"_id": oid})
        if not doc:
            raise HTTPException(status_code=404, detail="No stored entry found for entry_id")
        if str(doc.get("publisher_email", "")).strip().lower() != publisher_email:
            raise HTTPException(status_code=404, detail="entry_id does not match publisher_email")
        docs = [doc]
    else:
        if payload.start is None or payload.end is None:
            raise HTTPException(status_code=400, detail="Provide start and end when entry_id is not set")
        start = _ensure_utc(payload.start)
        end = _ensure_utc(payload.end)
        if start > end:
            raise HTTPException(status_code=400, detail="start must be <= end")
        start_iso = _iso_utc_micro(start)
        end_iso = _iso_utc_micro(end)

        after_iso = None
        after_oid = None
        if payload.after_timestamp is not None:
            after_iso = _iso_utc_micro(payload.after_timestamp)
        if payload.after_id is not None:
            after_oid = _coerce_object_id(payload.after_id)
        if after_oid is not None and after_iso is None:
            raise HTTPException(status_code=400, detail="after_id requires after_timestamp")

        # Build the time range constraint.
        time_range: Dict[str, Any] = {"$gte": start_iso}
        if payload.end_inclusive:
            time_range["$lte"] = end_iso
        else:
            time_range["$lt"] = end_iso

        query: Dict[str, Any] = {
            "publisher_email": publisher_email,
            "timestamp": time_range,
        }
        if after_iso is not None:
            # Timestamp is stored as ISO string; lexicographic order matches chronological order for our format.
            if after_oid is not None:
                query["$or"] = [
                    {"timestamp": {"$gt": after_iso}},
                    {"timestamp": after_iso, "_id": {"$gt": after_oid}},
                ]
            else:
                query["timestamp"]["$gt"] = after_iso

        cursor = (
            _col.find(query)
            .sort([("timestamp", 1), ("_id", 1)])
            .limit(int(payload.limit_docs))
        )
        docs = list(cursor)
        if not docs:
            raise HTTPException(status_code=404, detail="No stored metrics found for publisher_email in the given time window")

    # Flatten stored bodies into a list of metric entries acceptable by CIM (dict or list[dict]).
    entries: list[dict] = []
    for d in docs:
        body = d.get("body")
        if isinstance(body, list):
            entries.extend([x for x in body if isinstance(x, dict)])
            continue
        if isinstance(body, dict):
            # Handle odd "object of numeric indices" encodings.
            if body and all(str(k).isdigit() for k in body.keys()) and all(isinstance(v, dict) for v in body.values()):
                for k in sorted(body.keys(), key=lambda s: int(str(s))):
                    entries.append(body[k])
            else:
                entries.append(body)
            continue

    if not entries:
        raise HTTPException(status_code=400, detail="Stored documents contained no CIM-compatible entries in body")

    auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if auth_header:
        headers["Authorization"] = auth_header
    # Preserve who is replaying / where these records came from.
    headers["X-Publisher-Email"] = publisher_email
    headers["X-Caller-Email"] = caller

    try:
        # Large pages can take several minutes (Mongo load + CIM enrichment + per-entry SQL forwards).
        r = requests.post(
            CIM_INTERNAL_ENDPOINT,
            json=entries,
            headers=headers,
            timeout=(10, CIM_SUBMIT_TIMEOUT_SECONDS),
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to call CIM service: {exc}")

    try:
        cim_payload = r.json()
    except Exception:
        cim_payload = {"raw": (r.text or "")[:2000]}

    if not r.ok:
        raise HTTPException(status_code=r.status_code, detail={"cim_endpoint": CIM_INTERNAL_ENDPOINT, "cim_response": cim_payload})

    next_after_timestamp = None
    next_after_id = None
    if docs:
        last = docs[-1]
        next_after_timestamp = last.get("timestamp")
        next_after_id = str(last.get("_id")) if last.get("_id") is not None else None

    return {
        "publisher_email": publisher_email,
        "docs_loaded": len(docs),
        "entries_forwarded": len(entries),
        "next_after_timestamp": next_after_timestamp,
        "next_after_id": next_after_id,
        "cim_endpoint": CIM_INTERNAL_ENDPOINT,
        "cim_response": cim_payload,
        "mongo_ids": [str(d.get("_id")) for d in docs[:20]],
    }

@router.get(
    "/cim-records",
    tags=["Metrics"],
    summary="List my stored Mongo/CIM records",
    description=(
        "Returns records stored in the local MongoDB for the authenticated user. "
        "Optional filters: `filter_key` (repeatable `key=value`), `start`, `end`, `limit`, `offset`, `page`.\n\n"
        "Example: `GET /v1/cim-records?filter_key=SiteName=EGI.SARA.nl&filter_key=Owner=DIRAC&start=2026-03-01T00:00:00Z&end=2026-03-31T23:59:59Z&limit=20`"
    ),
)
def get_cim_records(
    filter_key: Optional[List[str]] = Query(default=None, description="Repeatable recursive filter in key=value form.", example=["SiteName=EGI.SARA.nl", "Owner=DIRAC"]),
    start: Optional[datetime] = Query(default=None, description="Inclusive start timestamp (UTC).", example="2026-03-01T00:00:00Z"),
    end: Optional[datetime] = Query(default=None, description="Inclusive end timestamp (UTC).", example="2026-03-31T23:59:59Z"),
    limit: Optional[int] = Query(default=None, ge=1, description=f"Max docs to return; capped at {RECORDS_MAX_LIMIT}.", example=20),
    offset: Optional[int] = Query(default=0, ge=0, description="Row offset.", example=0),
    page: Optional[int] = Query(default=None, ge=1, description="Optional 1-based page number; overrides offset.", example=2),
    publisher_email: str = Depends(verify_token),
):
    if (start is None) != (end is None):
        raise HTTPException(status_code=400, detail="Provide both start and end, or neither")

    effective_limit, effective_offset = _resolve_limit_offset_page(limit, offset, page, RECORDS_MAX_LIMIT)
    filters = _parse_filter_exprs(filter_key)

    query: dict[str, Any] = {"publisher_email": publisher_email}
    start_dt = None
    end_dt = None
    if start is not None and end is not None:
        start_dt = _ensure_utc(start)
        end_dt = _ensure_utc(end)
        if start_dt > end_dt:
            raise HTTPException(status_code=400, detail="start must be <= end")

    records: list[dict[str, Any]] = []
    matched_seen = 0
    cursor = _col.find(query).sort("timestamp", -1)
    for doc in cursor:
        if start_dt is not None and end_dt is not None and not _doc_matches_time_window(doc, start_dt, end_dt):
            continue
        if filters and not _doc_matches_all_filter_exprs(doc, filters):
            continue
        if matched_seen < effective_offset:
            matched_seen += 1
            continue
        records.append(_serialise_mongo_doc(doc))
        matched_seen += 1
        if len(records) >= effective_limit:
            break

    return {
        "ok": True,
        "publisher_email": publisher_email,
        "limit": effective_limit,
        "offset": effective_offset,
        "page": page,
        "returned": len(records),
        "filters": [f"{k}={v}" for k, v in filters],
        "records": records,
    }


@router.get(
    "/cim-records/count",
    tags=["Metrics"],
    summary="Count my stored Mongo/CIM records",
    description=(
        "Counts internal MongoDB records belonging to the authenticated user after applying the optional "
        "recursive `filter_key` filters and optional inclusive `start`/`end` time window."
    ),
)
def get_cim_records_count(
    filter_key: Optional[List[str]] = Query(default=None, description="Repeatable recursive filter in key=value form.", example=["SiteName=EGI.SARA.nl"]),
    start: Optional[datetime] = Query(default=None, description="Inclusive start timestamp (UTC).", example="2026-03-01T00:00:00Z"),
    end: Optional[datetime] = Query(default=None, description="Inclusive end timestamp (UTC).", example="2026-03-31T23:59:59Z"),
    publisher_email: str = Depends(verify_token),
):
    if (start is None) != (end is None):
        raise HTTPException(status_code=400, detail="Provide both start and end, or neither")

    filters = _parse_filter_exprs(filter_key)
    query: dict[str, Any] = {"publisher_email": publisher_email}
    start_dt = None
    end_dt = None
    if start is not None and end is not None:
        start_dt = _ensure_utc(start)
        end_dt = _ensure_utc(end)
        if start_dt > end_dt:
            raise HTTPException(status_code=400, detail="start must be <= end")

    count = 0
    cursor = _col.find(query, {"_id": 1, "body": 1, "timestamp": 1})
    for doc in cursor:
        if start_dt is not None and end_dt is not None and not _doc_matches_time_window(doc, start_dt, end_dt):
            continue
        if filters and not _doc_matches_all_filter_exprs(doc, filters):
            continue
        count += 1

    return {
        "ok": True,
        "publisher_email": publisher_email,
        "count": count,
        "filters": [f"{k}={v}" for k, v in filters],
    }


@router.post(
    "/cim-db/delete",
    tags=["Metrics"],
    summary="Delete my stored Mongo/CIM records",
    description=(
        "Deletes internal MongoDB records for the authenticated user filtered by `filter_key[]` and `start`/`end`.\n\n"
        "The response includes `unmatched_filters`, `deleted_count`, and `time_window_candidates` so callers can "
        "distinguish between empty-result, partial-delete, and full-match cases."
    ),
)
def delete_cim_records(
    payload: CIMDeleteRequest = Body(
        ...,
        examples=CIMDeleteRequest.Config.schema_extra["examples"],
    ),
    publisher_email: str = Depends(verify_token),
):
    start_dt = _ensure_utc(payload.start)
    end_dt = _ensure_utc(payload.end)
    if start_dt > end_dt:
        raise HTTPException(status_code=400, detail="start must be <= end")

    filters = _parse_filter_exprs(payload.filter_key)
    base_query: dict[str, Any] = {
        "publisher_email": publisher_email,
    }

    try:
        candidates = list(_col.find(base_query, {"_id": 1, "body": 1, "timestamp": 1, "publisher_email": 1}))
        time_window_candidates = [d for d in candidates if _doc_matches_time_window(d, start_dt, end_dt)]
        unmatched_filters = _find_unmatched_filter_exprs(time_window_candidates, filters)
        to_delete_ids = [
            d["_id"]
            for d in time_window_candidates
            if _doc_matches_all_filter_exprs(d, filters)
        ]
        deleted_count = 0
        if to_delete_ids:
            res = _col.delete_many({"publisher_email": publisher_email, "_id": {"$in": to_delete_ids}})
            deleted_count = int(getattr(res, "deleted_count", 0))
        remaining_count = int(_col.count_documents({"publisher_email": publisher_email}))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Mongo delete failed: {exc}")

    return {
        "ok": True,
        "publisher_email": publisher_email,
        "start": _iso_utc_micro(start_dt),
        "end": _iso_utc_micro(end_dt),
        "requested_filters": [f"{k}={v}" for k, v in filters],
        "unmatched_filters": unmatched_filters,
        "deleted_count": deleted_count,
        "time_window_candidates": len(time_window_candidates),
        "remaining_count": remaining_count,
    }


@router.get(
    "/cnr-records",
    tags=["Metrics"],
    summary="List my CNR SQL records",
    description=(
        "Lists CNR SQL records filtered by optional `site_id`, `vo`, `activity`, and inclusive `start`/`end`, "
        "with pagination via `limit`, `offset`, and `page`."
    ),
)
def get_cnr_records(
    site_id: Optional[int] = Query(default=None, example=123),
    vo: Optional[str] = Query(default=None, example="DIRAC"),
    activity: Optional[str] = Query(default=None, example="grid"),
    start: Optional[datetime] = Query(default=None, example="2026-03-01T00:00:00Z"),
    end: Optional[datetime] = Query(default=None, example="2026-03-31T23:59:59Z"),
    limit: Optional[int] = Query(default=None, ge=1, description=f"Max rows to return; capped at {RECORDS_MAX_LIMIT}.", example=20),
    offset: Optional[int] = Query(default=0, ge=0, example=0),
    page: Optional[int] = Query(default=None, ge=1, example=2),
    publisher_email: str = Depends(verify_token),
):
    effective_limit, effective_offset = _resolve_limit_offset_page(limit, offset, page, RECORDS_MAX_LIMIT)
    params: dict[str, Any] = {
        "site_id": site_id,
        "vo": vo,
        "activity": activity,
        "limit": effective_limit,
        "offset": effective_offset,
    }
    if start is not None:
        params["start"] = _iso_utc_micro(_ensure_utc(start))
    if end is not None:
        params["end"] = _iso_utc_micro(_ensure_utc(end))
    return _forward_sql_adapter("GET", "/cnr-db/records", params=params)


@router.get(
    "/cnr-records/count",
    tags=["Metrics"],
    summary="Count my CNR SQL records",
    description="Counts CNR SQL records matching the optional `site_id`, `vo`, `activity`, and inclusive `start`/`end` filters.",
)
def get_cnr_records_count(
    site_id: Optional[int] = Query(default=None, example=123),
    vo: Optional[str] = Query(default=None, example="DIRAC"),
    activity: Optional[str] = Query(default=None, example="grid"),
    start: Optional[datetime] = Query(default=None, example="2026-03-01T00:00:00Z"),
    end: Optional[datetime] = Query(default=None, example="2026-03-31T23:59:59Z"),
    publisher_email: str = Depends(verify_token),
):
    params: dict[str, Any] = {
        "site_id": site_id,
        "vo": vo,
        "activity": activity,
    }
    if start is not None:
        params["start"] = _iso_utc_micro(_ensure_utc(start))
    if end is not None:
        params["end"] = _iso_utc_micro(_ensure_utc(end))
    return _forward_sql_adapter("GET", "/cnr-db/records/count", params=params)


# @router.post(
#     "/cnr-db/delete",
#     tags=["Metrics"],
#     summary="Delete my CNR SQL records",
#     description=(
#         "Deletes CNR SQL records matching the provided `site_id`, `vo`, `activity`, and inclusive `start`/`end` filters.\n\n"
#         "Note: current filtering is based on the supplied SQL dimensions and time window."
#     ),
# )
# def delete_cnr_records(
#     payload: CNRDeleteRequest = Body(
#         ...,
#         example=CNRDeleteRequest.Config.schema_extra["example"],
#     ),
#     publisher_email: str = Depends(verify_token),
# ):
#     body = {
#         "site_id": payload.site_id,
#         "vo": payload.vo,
#         "activity": payload.activity,
#         "start": _iso_utc_micro(_ensure_utc(payload.start)),
#         "end": _iso_utc_micro(_ensure_utc(payload.end)),
#     }
#     return _forward_sql_adapter("POST", "/cnr-db/delete", json_body=body)



class PasswordResetRequest(BaseModel):
    new_password: str

    class Config:
        schema_extra = {
            "example": {
                "new_password": "new-demo-password-123",
            }
        }

@router.post(
    "/reset-password",
    tags=["Auth"],
    summary="Reset my password",
    description="Reset the password for the authenticated user.\n\nExample body: `{ \"new_password\": \"new-demo-password-123\" }`",
)
def reset_password(
    data: PasswordResetRequest = Body(..., example=PasswordResetRequest.Config.schema_extra["example"]),
    publisher_email: str = Depends(verify_token),
    db: Session = Depends(get_db)
):
    """
    Reset the password for the currently logged-in user.
    Requires a valid Authorization: Bearer <token>.
    """
    user = db.query(User).filter(User.email == publisher_email).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.hashed_password = pwd_context.hash(data.new_password)
    db.commit()
    return {"msg": "Password updated successfully"}

@router.get(
    "/verify-token",
    tags=["Auth"],
    summary="Validate GreenDIGIT JWT token and optionally require a role",
    description=(
        "Validates the Bearer token and returns the authenticated email plus current database roles.\n\n"
        "Pass `required_role=submit`, `required_role=publish`, or `required_role=dashboards` to require a specific role. "
        "The endpoint returns `403` when the token is valid but the role is missing."
    ),
    responses={
        200: {"description": "Token is valid and role requirement, if supplied, is satisfied"},
        401: {"description": "Missing/invalid Bearer token"},
        403: {"description": "Token is valid, but required_role is missing"},
        400: {"description": "Unknown required_role"},
    },
)
def verify_token_endpoint(
    required_role: Optional[str] = Query(default=None, description="Optional required role: submit, publish, or dashboards."),
    email: str = Depends(verify_token),
    db: Session = Depends(get_db),
):
    roles = get_user_roles(email, db)
    payload = {"valid": True, "sub": email, "roles": roles}
    if required_role:
        role = _normalise_role(required_role)
        if role not in roles:
            raise HTTPException(status_code=403, detail=f"Missing required role: {role}")
        payload["required_role"] = role
    return payload


@router.get(
    "/token",
    tags=["Auth"],
    summary="Get JWT via query string (email and password).",
    description="Returns JSON: {access_token, token_type, expires_in}. Accepts `email` and `password` as query parameters."
)
def get_token(
    email: str = Query(..., description="User email", example="demo.publisher@example.org"),
    password: str = Query(..., description="User password", example="correct-horse-battery-staple"),
    db: Session = Depends(get_db)
):
    email_lower = email.strip().lower()
    user = db.query(User).filter(User.email == email_lower).first()
    if not user:
        allowed_emails = load_allowed_emails()
        if email_lower not in allowed_emails:
            raise HTTPException(
                status_code=403,
                detail=(
                    "Email not allowed. To request access to dashboards and/or permission "
                    f"to submit metrics, contact {ACCESS_CONTACT_EMAIL}."
                ),
            )
        hashed_password = pwd_context.hash(password)
        user = User(email=email_lower, hashed_password=hashed_password)
        db.add(user); db.commit(); db.refresh(user)
        _ensure_bootstrap_roles_for_user(db, user)
    elif not pwd_context.verify(password, user.hashed_password):
        raise HTTPException(status_code=400, detail=f"Incorrect password. If you have forgotten your password please contact the GreenDIGIT team: {ACCESS_CONTACT_EMAIL}.")
    else:
        _ensure_bootstrap_roles_for_user(db, user)

    now = int(time.time())
    token_data = {
        "sub": user.email,
        "iss": JWT_ISSUER,
        "iat": now,
        "nbf": now,
        "exp": now + ACCESS_TOKEN_EXPIRE_SECONDS,
    }
    token = jwt.encode(token_data, SECRET_KEY, algorithm=ALGORITHM)
    return {"access_token": token, "token_type": "bearer", "expires_in": ACCESS_TOKEN_EXPIRE_SECONDS}

@router.post(
    "/cim-json",
    tags=["Metrics"],
    summary="Submit JSON metrics for conversion to SQL.",
    description="Converts JSON metrics with CFP calculated into namespaces to be submitted to SQL-compatible endpoint Databases."
)
def digest_cim_json(body: PostCimJsonRequest):
    # For now just print for debugging
    print("Received /cim-json submission:")
    print("Publisher:", body.publisher_email)
    print("Job ID:", body.job_id)
    for m in body.metrics:
        print(f"  - Metric {m.metric} @ {m.timestamp}: {m.value} (node={m.node})")
        print("    CFP:", m.cfp_ci_service)

    # Mock SQL mapping (to later adapt cnr_db_connect.py)
    mock_sql = [
        {
            "table": "metrics_table",
            "publisher_email": body.publisher_email,
            "job_id": body.job_id,
            "metric": m.metric,
            "value": m.value,
            "timestamp": m.timestamp,
            "cfp": m.cfp_ci_service.get("cfp_g")
        }
        for m in body.metrics
    ]

    print("Mock SQL mapping:")
    for row in mock_sql:
        print(row)

    return {"ok": True, "rows_prepared": len(mock_sql)}

app.include_router(router)
