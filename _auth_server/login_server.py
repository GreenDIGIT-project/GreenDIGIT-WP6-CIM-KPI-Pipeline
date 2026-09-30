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
from sqlalchemy import create_engine, Column, String, Integer, ForeignKey, UniqueConstraint, text
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime, timezone
import traceback, uuid
from pathlib import Path
import base64
import requests
from bson import ObjectId
from html import escape
from auth_db import (
    DEFAULT_GROUP, GREENDIGIT_GROUP, VALID_ROLES, bootstrap as bootstrap_auth_db,
    OIDC_PASSWORD_DISABLED, ensure_schema as ensure_auth_schema, normalise_group,
)




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
    "- Access is role-based. `publish` is required to submit metrics for nightly publication, and `dashboards_view` is required for private Grafana dashboards.\n\n"
    "**Metrics read/delete endpoints**\n\n"
    "- `GET /v1/cim-records` and `GET /v1/cim-records/count` list/count raw records stored in the internal MongoDB for the authenticated user.\n"
    "- `POST /v1/cim-db/delete` deletes internal MongoDB records for the authenticated user within a time window and filtered by repeatable `filter_key` expressions.\n"
    "- `POST /v1/submit` stores metrics for authenticated users with the `publish` role. Stored metrics are published to CNR by the nightly batch export.\n"
    "- `GET /v1/cnr-records` and `GET /v1/cnr-records/count` query CNR SQL records by `site_id`, `vo`, `activity`, and time window.\n"
    "- `POST /v1/cnr-db/delete` is disabled.\n"
    "- Example request snippets are available in `scripts/example-edit-metrics.sh` and `scripts/example_requests/example-request-metrics.sh`.\n\n"
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
METRICS_ME_MAX_LIMIT = int(os.getenv("METRICS_ME_MAX_LIMIT", "1000"))
MONGO_SERVER_SELECTION_TIMEOUT_MS = int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS", "5000"))
MONGO_CONNECT_TIMEOUT_MS = int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "5000"))

RECORDS_MAX_LIMIT = int(os.getenv("RECORDS_MAX_LIMIT", "500"))
CNR_SQL_API_BASE = os.getenv("CNR_SQL_API_BASE", "http://sql-adapter:8033")
CNR_INTERNAL_TOKEN = os.getenv("CNR_INTERNAL_TOKEN", os.getenv("JWT_TOKEN", ""))

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

class Group(Base):
    __tablename__ = "groups"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False, index=True)
    display_name = Column(String, nullable=True)
    created_at = Column(String, nullable=False, server_default=text("CURRENT_TIMESTAMP"))

class UserGroup(Base):
    __tablename__ = "user_groups"
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    group_id = Column(Integer, ForeignKey("groups.id", ondelete="CASCADE"), primary_key=True)
    is_super = Column(Integer, nullable=False, default=0)
    source = Column(String, nullable=False, default="manual")
    created_at = Column(String, nullable=False, server_default=text("CURRENT_TIMESTAMP"))

class AccessRequest(Base):
    __tablename__ = "access_requests"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    request_type = Column(String, nullable=False)
    requested_value = Column(String, nullable=False)
    status = Column(String, nullable=False, default="pending")
    created_at = Column(String, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    decided_at = Column(String)
    decided_by_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"))

class AdminAudit(Base):
    __tablename__ = "admin_audit"
    id = Column(Integer, primary_key=True)
    actor_email = Column(String, nullable=False)
    action = Column(String, nullable=False)
    target_email = Column(String)
    group_name = Column(String)
    outcome = Column(String, nullable=False)
    detail = Column(String)
    created_at = Column(String, nullable=False, server_default=text("CURRENT_TIMESTAMP"))

Base.metadata.create_all(bind=engine)

_schema_conn = engine.raw_connection()
try:
    ensure_auth_schema(_schema_conn)
finally:
    _schema_conn.close()

def migrate_legacy_roles() -> None:
    with engine.begin() as conn:
        conn.execute(text(
            """
            INSERT OR IGNORE INTO user_roles (user_id, role)
            SELECT user_id, 'publish'
            FROM user_roles
            WHERE role = 'submit'
            """
        ))
        conn.execute(text(
            """
            INSERT OR IGNORE INTO user_roles (user_id, role)
            SELECT user_id, 'dashboards_view'
            FROM user_roles
            WHERE role = 'dashboards'
            """
        ))
        conn.execute(text("DELETE FROM user_roles WHERE role IN ('submit', 'dashboards')"))

migrate_legacy_roles()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
PASSWORD_RESET_MARKER = "!RESET_REQUIRED!"

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


def _store_metric_in_col(*, col, publisher_email: str, group: str, body: Any) -> Dict[str, Any]:
    doc = {
        "publisher_email": str(publisher_email).strip().lower(),
        "group": normalise_group(group),
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
        headers = {"X-CNR-Internal-Token": CNR_INTERNAL_TOKEN} if CNR_INTERNAL_TOKEN else {}
        response = requests.request(method, url, params=params, json=json_body, headers=headers, timeout=(10, 120))
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

def _load_email_file(filename: str) -> set[str]:
    path = Path(__file__).resolve().parent / filename
    if not path.exists():
        path = PROJECT_ROOT / filename
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8") as f:
        return {
            line.strip().lower()
            for line in f
            if line.strip() and not line.lstrip().startswith("#")
        }

def load_access_emails():
    return _load_email_file("dashboards_emails.txt") | _load_email_file("submit_emails.txt")

def _normalise_role(role: str) -> str:
    role = (role or "").strip().lower()
    if role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail=f"Unknown role: {role}")
    return role

def get_user_groups(email: str, db: Session, *, supervised_only: bool = False) -> list[str]:
    query = (
        db.query(Group.name)
        .join(UserGroup, UserGroup.group_id == Group.id)
        .join(User, User.id == UserGroup.user_id)
        .filter(User.email == email.strip().lower())
    )
    if supervised_only:
        query = query.filter(UserGroup.is_super == 1)
    return [row[0] for row in query.order_by(Group.name).all()]

def add_default_membership(db: Session, user: User) -> None:
    if db.query(UserGroup).filter(UserGroup.user_id == user.id).first():
        return
    group = db.query(Group).filter(Group.name == DEFAULT_GROUP).first()
    if group is None:
        group = Group(name=DEFAULT_GROUP, display_name="Public")
        db.add(group); db.flush()
    db.add(UserGroup(user_id=user.id, group_id=group.id, is_super=0, source="bootstrap"))
    db.commit()

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
    db.commit()
    raw = engine.raw_connection()
    try:
        email_root = Path(__file__).resolve().parent
        if not (email_root / "dashboards_emails.txt").exists():
            email_root = PROJECT_ROOT
        counts = bootstrap_auth_db(raw, email_root)
    finally:
        raw.close()
    db.expire_all()
    return sum(counts.values())

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
    if email in _load_email_file("dashboards_emails.txt"):
        changed = grant_user_role(db, user, "dashboards_view") or changed
    if email in _load_email_file("submit_emails.txt"):
        changed = grant_user_role(db, user, "publish") or changed
    if changed:
        db.commit()
    # Group authorization comes from persisted approvals, never from a live
    # role allowlist. This keeps future submit-list additions out of the legacy
    # GreenDIGIT compatibility cohort.
    approved_group_ids = [
        row[0] for row in db.execute(
            text("SELECT group_id FROM group_email_approvals WHERE lower(email)=:email"),
            {"email": email},
        ).all()
    ]
    for group_id in approved_group_ids:
        db.execute(
            UserGroup.__table__.insert().prefix_with("OR IGNORE").values(
                user_id=user.id, group_id=group_id, is_super=0, source="bootstrap"
            )
        )
    if approved_group_ids:
        db.commit()
    add_default_membership(db, user)

with SessionLocal() as _bootstrap_db:
    bootstrap_roles_from_files(_bootstrap_db)

def access_not_allowed_response(email: str) -> HTMLResponse:
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
        <p>This account is not currently allowed to register for this service.</p>
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

@app.middleware("http")
async def audit_admin_mutations(request: Request, call_next):
    response = await call_next(request)
    if request.method in {"POST", "PUT", "PATCH", "DELETE"} and request.url.path.startswith("/v1/admin"):
        actor = "unknown"
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            try:
                claims = jwt.decode(auth.split(" ", 1)[1], SECRET_KEY, algorithms=[ALGORITHM], issuer=JWT_ISSUER)
                actor = str(claims.get("sub") or "unknown").strip().lower()
            except Exception:
                pass
        try:
            with SessionLocal() as audit_db:
                audit_db.add(AdminAudit(actor_email=actor, action=f"http.{request.method.lower()} {request.url.path}",
                    outcome="success" if response.status_code < 400 else "denied", detail=f"status={response.status_code}"))
                audit_db.commit()
        except Exception:
            pass
    return response

@router.post(
    "/login",
    tags=["Auth"],
    summary="Login and get a JWT access token",
    description=(
        "Use form fields `username` (email) and `password`.\n\n"
        "Returns a JWT for `Authorization: Bearer <token>`.\n\n"
        "Example request:\n\n"
        "```bash\n"
        "curl -sS -X POST \"https://greendigit-cim.sztaki.hu/gd-cim-api/v1/login\" \\\n"
        "  -H \"Content-Type: application/x-www-form-urlencoded\" \\\n"
        "  --data-urlencode \"username=demo.publisher@example.org\" \\\n"
        "  --data-urlencode \"password=correct-horse-battery-staple\"\n"
        "```\n\n"
        "Swagger example credentials:\n"
        "- `username`: `demo.publisher@example.org`\n"
        "- `password`: `correct-horse-battery-staple`"
    ),
    response_class=HTMLResponse,
    openapi_extra={
        "requestBody": {
            "content": {
                "application/x-www-form-urlencoded": {
                    "examples": {
                        "demo_credentials": {
                            "summary": "Demo credentials",
                            "value": {
                                "username": "demo.publisher@example.org",
                                "password": "correct-horse-battery-staple",
                            },
                        }
                    }
                }
            }
        }
    },
)
def login(request: Request, form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    email_lower = form_data.username.strip().lower()
    user = db.query(User).filter(User.email == email_lower).first()
    if not user:
        # First login: check if allowed, then register
        access_emails = load_access_emails()
        if email_lower not in access_emails:
            return access_not_allowed_response(email_lower)
        hashed_password = pwd_context.hash(form_data.password)
        db_user = User(email=email_lower, hashed_password=hashed_password)
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        user = db_user
        _ensure_bootstrap_roles_for_user(db, user)
    elif user.hashed_password == OIDC_PASSWORD_DISABLED:
        raise HTTPException(
            status_code=401,
            detail="Local sign-in is unavailable or the credentials are invalid.",
        )
    elif user.hashed_password == PASSWORD_RESET_MARKER:
        user.hashed_password = pwd_context.hash(form_data.password)
        db.commit()
        _ensure_bootstrap_roles_for_user(db, user)
    elif not pwd_context.verify(form_data.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Local sign-in is unavailable or the credentials are invalid.")
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
    if "application/json" in request.headers.get("accept", ""):
        return JSONResponse({"access_token": token, "token_type": "bearer", "expires_in": ACCESS_TOKEN_EXPIRE_SECONDS})
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

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Sign in | GreenDIGIT</title>
  <style>
    * {{ box-sizing:border-box }}
    body {{ margin:0; font:16px/1.5 system-ui,sans-serif; color:#24332a; background:#f3f7f3 }}
    main {{ width:min(920px,calc(100% - 32px)); margin:40px auto }}
    header,.card {{ background:#fff; border:1px solid #d8e3da; border-radius:12px; padding:28px }}
    header {{ display:flex; align-items:center; gap:22px; margin-bottom:20px }}
    header img {{ width:110px; height:auto }}
    h1,h2 {{ color:#185b31; margin-top:0 }}
    .grid {{ display:grid; grid-template-columns:1.15fr 1fr; gap:20px }}
    .primary {{ display:block; padding:14px 18px; border-radius:7px; color:#fff; background:#176b39;
      text-decoration:none; text-align:center; font-weight:700 }}
    label {{ display:block; margin:12px 0 5px; font-weight:650 }}
    input {{ width:100%; padding:11px; border:2px solid #b8c7bc; border-radius:6px; font:inherit }}
    button {{ width:100%; margin-top:16px; padding:12px; border:0; border-radius:6px; color:#fff;
      background:#345342; font:inherit; font-weight:700; cursor:pointer }}
    a:focus,input:focus,button:focus {{ outline:3px solid #f2a900; outline-offset:2px }}
    .links {{ display:flex; flex-wrap:wrap; gap:16px; margin-top:22px }}
    .links a {{ color:#155d8b; font-weight:650 }}
    .note {{ color:#56645b; font-size:.93rem }}
    footer {{ text-align:center; color:#56645b; margin-top:22px }}
    footer img {{ height:42px; width:auto; margin:10px }}
    @media(max-width:700px) {{ .grid {{ grid-template-columns:1fr }} header {{ align-items:flex-start }} }}
  </style>
</head>
<body><main>
  <header><img src="{gd_logo}" alt="GreenDIGIT logo"><div><h1>GreenDIGIT access</h1>
    <p>Sign in to private dashboards or obtain an API token. Public dashboards remain open.</p></div></header>
  <div class="grid">
    <section class="card" aria-labelledby="egi-title"><h2 id="egi-title">Institutional sign-in</h2>
      <p>Use your institutional identity through EGI Check-in. This confirms identity only; local roles and group memberships still control access.</p>
      <a class="primary" href="/auth/login">Sign in with EGI Check-in</a>
      <p class="note">A valid identity without dashboard permission is directed to the access-request page.</p>
    </section>
    <section class="card" aria-labelledby="local-title"><h2 id="local-title">Local fallback</h2>
      <form action="login" method="post">
        <label for="token-username">Email</label><input id="token-username" name="username" type="email" autocomplete="username" required>
        <label for="token-password">Password</label><input id="token-password" name="password" type="password" autocomplete="current-password" required>
        <button type="submit">Generate API token</button>
      </form>
      <p class="note">Local accounts are intended for API publishers and recovery access. Dashboard access additionally requires <code>dashboards_view</code>.</p>
    </section>
  </div>
  <nav class="links" aria-label="Related links">
    <a href="/public-dashboards/">Public dashboards</a>
    <a href="/gd-cim-api/v1/documentation">Documentation</a>
    <a href="/gd-cim-api/v1/request-access">Request access</a>
  </nav>
  <footer><p>Support: <a href="mailto:{ACCESS_CONTACT_EMAIL}">{ACCESS_CONTACT_EMAIL}</a></p>
    <img src="{eu_logo}" alt="Funded by the European Union"></footer>
</main></body></html>"""

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

@router.get("/request-access", response_class=HTMLResponse, include_in_schema=False)
def request_access_page(reason: str = ""):
    messages = {
        "missing_dashboard_role": "Your EGI identity was confirmed, but this account does not have the dashboards_view role.",
        "no_private_group": "You are signed in, but do not belong to a private metric group.",
    }
    explanation = messages.get(reason, "Request a role or private-group membership from the GreenDIGIT support team.")
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Request access | GreenDIGIT</title>
<style>body{{font:16px/1.55 system-ui,sans-serif;background:#f3f7f3;color:#24332a;margin:0;padding:30px}}
main{{max-width:680px;margin:auto;background:#fff;border:1px solid #d8e3da;border-radius:10px;padding:30px}}
h1{{color:#185b31}}a{{color:#155d8b;font-weight:650}}a:focus{{outline:3px solid #f2a900}}</style></head>
<body><main><h1>Access request needed</h1><p>{escape(explanation)}</p>
<p>Roles control permitted operations. Group membership controls which metric data is visible or writable. EGI identity alone grants neither.</p>
<p>Contact <a href="mailto:{ACCESS_CONTACT_EMAIL}">{ACCESS_CONTACT_EMAIL}</a> and state whether you need <code>publish</code>, <code>dashboards_view</code>, or membership in a named group.</p>
<p><a href="/gd-cim-api/v1/token-ui">Return to sign in</a> · <a href="/gd-cim-api/v1/documentation">Documentation</a> · <a href="/public-dashboards/">Public dashboards</a></p>
</main></body></html>"""


@router.get("/documentation", response_class=HTMLResponse, include_in_schema=False)
def documentation_page():
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Documentation | GreenDIGIT</title>
<style>body{{font:16px/1.55 system-ui,sans-serif;background:#f3f7f3;color:#24332a;margin:0;padding:30px}}
main{{max-width:850px;margin:auto;background:#fff;border:1px solid #d8e3da;border-radius:10px;padding:32px}}
h1,h2{{color:#185b31}}code{{background:#edf2ee;padding:2px 5px}}a{{color:#155d8b}}table{{border-collapse:collapse;width:100%}}
th,td{{border:1px solid #cdd8cf;padding:8px;text-align:left;vertical-align:top}}a:focus{{outline:3px solid #f2a900}}</style></head>
<body><main><h1>GreenDIGIT access documentation</h1>
<p>The platform accepts environmental-impact metrics and presents public or group-scoped dashboards.</p>
<h2>Sign-in and authorization</h2><p>EGI Check-in is the primary institutional login. Local email/password login remains a fallback for existing accounts. Authentication confirms who you are; local roles and memberships determine what you may do.</p>
<ul><li><code>publish</code>: submit metrics to a group you belong to.</li><li><code>dashboards_view</code>: view private dashboards, filtered to current memberships.</li><li><code>admin</code>: administer platform roles and groups.</li></ul>
<p><code>public</code> is the fallback group. <code>greendigit</code> is private. A submission must include <code>"group": "greendigit"</code> (or another authorized group); the server derives the publisher identity.</p>
<h2>EGI configuration</h2><table><thead><tr><th>Variable</th><th>Source / sensitivity</th><th>Used by</th><th>Change</th></tr></thead><tbody>
<tr><td><code>EGI_OIDC_ISSUER</code></td><td>EGI; public</td><td>auth proxy</td><td>restart proxy</td></tr>
<tr><td><code>EGI_OIDC_CLIENT_ID</code></td><td>EGI registration; identifier</td><td>auth proxy</td><td>restart proxy</td></tr>
<tr><td><code>EGI_OIDC_CLIENT_SECRET</code></td><td>EGI registration; secret when issued</td><td>auth proxy</td><td>restart proxy</td></tr>
<tr><td><code>EGI_OIDC_REDIRECT_URI</code>, <code>EGI_OIDC_POST_LOGOUT_URI</code></td><td>registration/deployment; public</td><td>auth proxy</td><td>restart proxy</td></tr>
<tr><td><code>EGI_OIDC_SCOPE</code></td><td>deployment; public</td><td>auth proxy</td><td>restart proxy</td></tr>
<tr><td><code>EGI_GROUP_CLAIM</code>, <code>EGI_GROUP_MAPPINGS</code></td><td>confirmed EGI claim and JSON mapping; public authorization config</td><td>auth proxy</td><td>confirm first, then restart proxy</td></tr>
<tr><td><code>EGI_REQUIRED_ENTITLEMENT</code></td><td>optional confirmed EGI entitlement; public identifier</td><td>auth proxy</td><td>restart proxy</td></tr>
<tr><td><code>JWT_GEN_SEED_TOKEN</code></td><td>generated locally with a CSPRNG; secret</td><td>auth API and proxy</td><td>coordinated restart; invalidates sessions</td></tr>
</tbody></table>
<h2>Troubleshooting</h2><p>“Invalid identity response” means signature, issuer, audience, expiry, nonce, or subject validation failed. “Temporarily unavailable” indicates discovery, token, JWKS, or UserInfo could not be reached. A confirmed identity sent here for access lacks a local role; contact <a href="mailto:{ACCESS_CONTACT_EMAIL}">support</a>.</p>
<p>Recover an existing client in EGI client management: verify the exact callback URI and reuse the client ID. If the secret cannot be viewed, rotate it only through the authorized interface, store it only in the secret store or <code>.env</code>, test before revoking the old secret when overlap is supported, restart only the proxy, and record who rotated it and when—not the value.</p>
<p><a href="/gd-cim-api/v1/token-ui">Sign in</a> · <a href="/gd-cim-api/v1/request-access">Request access</a> · <a href="/gd-cim-api/v1/docs">OpenAPI</a></p>
</main></body></html>"""


@router.post(
    "/submit",
    tags=["Metrics"],
    summary="Submit a metrics JSON payload",
    description=(
        "Stores an arbitrary JSON document as a metric entry.\n\n"
        "**Requires:** `Authorization: Bearer <token>` and the `publish` role.\n\n"
        "The `publisher_email` is derived from the token’s `sub` claim.\n\n"
        "A top-level `group` string is required for new publishers and checked against current server-side membership. "
        "Legacy GreenDIGIT-cohort members may omit it; the server then assigns `greendigit`.\n\n"
        "Example header:\n"
        "- `Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.demo.signature`\n\n"
        "Generic workload example:\n\n"
        "```json\n"
        "{\n"
        '  "group": "greendigit",\n'
        '  "workload_id": "generic-workload-001",\n'
        '  "workload_type": "batch-job",\n'
        '  "site": "Example-Site",\n'
        '  "owner": "example.org",\n'
        '  "start_time": "2026-03-01T00:00:00Z",\n'
        '  "end_time": "2026-03-01T01:00:00Z",\n'
        '  "energy_wh": 1250.5,\n'
        '  "cpu_core_seconds": 14400,\n'
        '  "memory_gib_seconds": 7200,\n'
        '  "labels": {\n'
        '    "queue": "standard",\n'
        '    "project": "green-digit-demo"\n'
        "  }\n"
        "}\n"
        "```"
    ),
    responses={
        200: {"description": "Stored successfully"},
        400: {"description": "Invalid JSON body"},
        401: {"description": "Missing/invalid Bearer token"},
        403: {"description": "Authenticated user is missing the publish role"},
        500: {"description": "Database error"},
    },
)
async def submit(
    request: Request,
    publisher_email: str = Depends(require_role("publish")),
    db: Session = Depends(get_db),
    _example: Any = Body(
        default=None,
        examples={
            "sample": {
                "summary": "Example metric payload",
                "value": {
                    "group": "greendigit",
                    "cpu_watts": 11.2,
                    "mem_bytes": 734003200,
                    "labels": {"node": "compute-0", "job_id": "abc123"}
                },
            },
            "generic_workload": {
                "summary": "Generic workload payload",
                "description": "A generic workload record with energy and runtime measurements.",
                "value": {
                    "group": "greendigit",
                    "workload_id": "generic-workload-001",
                    "workload_type": "batch-job",
                    "site": "Example-Site",
                    "owner": "example.org",
                    "start_time": "2026-03-01T00:00:00Z",
                    "end_time": "2026-03-01T01:00:00Z",
                    "energy_wh": 1250.5,
                    "cpu_core_seconds": 14400,
                    "memory_gib_seconds": 7200,
                    "labels": {
                        "queue": "standard",
                        "project": "green-digit-demo"
                    }
                },
            }
        },
    ),
):
    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON body") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Submission body must be a JSON object with a group field")
    raw_group = body.get("group")
    if not isinstance(raw_group, str) or not raw_group.strip():
        memberships = get_user_groups(publisher_email, db)
        if GREENDIGIT_GROUP not in memberships:
            raise HTTPException(status_code=400, detail="A non-empty group field is required")
        group_name = GREENDIGIT_GROUP
    else:
        try:
            group_name = normalise_group(raw_group)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    group = db.query(Group).filter(Group.name == group_name).first()
    if group is None:
        # Do not disclose whether a private group exists through alternate errors.
        raise HTTPException(status_code=404, detail="Group is not available")
    user = db.query(User).filter(User.email == publisher_email).first()
    membership = db.query(UserGroup).filter(
        UserGroup.user_id == user.id, UserGroup.group_id == group.id
    ).first() if user else None
    if membership is None:
        raise HTTPException(status_code=403, detail="You are not a member of the requested group")
    metric_body = dict(body)
    metric_body.pop("publisher_email", None)
    metric_body["group"] = group.name
    ack = _store_metric_in_col(col=_col, publisher_email=publisher_email, group=group.name, body=metric_body)
    if not ack.get("ok"):
        raise HTTPException(status_code=500, detail=f"DB error: {ack.get('error')}")
    return {"stored": ack, "group": group.name}


@router.post(
    "/submit-cim",
    include_in_schema=False,
)
async def submit_cim(
):
    raise HTTPException(
        status_code=410,
        detail="This legacy endpoint is disabled. Use POST /v1/submit; CNR publication runs through the nightly MetricsDB batch export.",
    )

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
    publisher_email: str = Depends(require_role("dashboards_view")),
    db: Session = Depends(get_db),
):
    if (start is None) != (end is None):
        raise HTTPException(status_code=400, detail="Provide both start and end, or neither")

    effective_limit, effective_offset = _resolve_limit_offset_page(limit, offset, page, RECORDS_MAX_LIMIT)
    filters = _parse_filter_exprs(filter_key)

    allowed_groups = get_user_groups(publisher_email, db)
    query: dict[str, Any] = {"group": {"$in": allowed_groups}}
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
        "groups": allowed_groups,
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
    publisher_email: str = Depends(require_role("dashboards_view")),
    db: Session = Depends(get_db),
):
    if (start is None) != (end is None):
        raise HTTPException(status_code=400, detail="Provide both start and end, or neither")

    filters = _parse_filter_exprs(filter_key)
    allowed_groups = get_user_groups(publisher_email, db)
    query: dict[str, Any] = {"group": {"$in": allowed_groups}}
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
        "groups": allowed_groups,
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
    publisher_email: str = Depends(require_role("dashboards_view")),
    db: Session = Depends(get_db),
):
    effective_limit, effective_offset = _resolve_limit_offset_page(limit, offset, page, RECORDS_MAX_LIMIT)
    params: dict[str, Any] = {
        "site_id": site_id,
        "vo": vo,
        "activity": activity,
        "limit": effective_limit,
        "offset": effective_offset,
        "groups": get_user_groups(publisher_email, db),
    }
    if start is not None:
        params["start"] = _iso_utc_micro(_ensure_utc(start))
    if end is not None:
        params["end"] = _iso_utc_micro(_ensure_utc(end))
    return _forward_sql_adapter("GET", "/cnr-db/records", params=params)


@router.post(
    "/dashboard-query",
    tags=["Metrics"],
    summary="Run a Grafana query within the authenticated user's groups",
    include_in_schema=False,
)
async def dashboard_query(
    request: Request,
    publisher_email: str = Depends(require_role("dashboards_view")),
    db: Session = Depends(get_db),
):
    try:
        grafana_request = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    if not isinstance(grafana_request, dict):
        raise HTTPException(status_code=400, detail="Invalid Grafana request")
    grafana_request = dict(grafana_request)
    grafana_request.pop("group", None)
    grafana_request.pop("groups", None)
    return _forward_sql_adapter(
        "POST",
        "/grafana-query",
        json_body={
            "groups": get_user_groups(publisher_email, db),
            "request": grafana_request,
        },
    )


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
    publisher_email: str = Depends(require_role("dashboards_view")),
    db: Session = Depends(get_db),
):
    params: dict[str, Any] = {
        "site_id": site_id,
        "vo": vo,
        "activity": activity,
        "groups": get_user_groups(publisher_email, db),
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

class GroupCreateRequest(BaseModel):
    name: str
    display_name: Optional[str] = None

class MembershipRequest(BaseModel):
    email: str

class RoleChangeRequest(BaseModel):
    email: str
    role: str

class AccessRequestCreate(BaseModel):
    request_type: str
    requested_value: str

class AccessDecision(BaseModel):
    decision: str

def _is_admin(email: str, db: Session) -> bool:
    return user_has_role(email, "admin", db)

def require_admin(email: str = Depends(verify_token), db: Session = Depends(get_db)) -> str:
    if not _is_admin(email, db):
        raise HTTPException(status_code=403, detail="Platform administrator role required")
    return email

def _audit(db: Session, actor: str, action: str, outcome: str, *, target=None, group=None, detail=None):
    db.add(AdminAudit(actor_email=actor, action=action, target_email=target,
                      group_name=group, outcome=outcome, detail=detail))

def _managed_group(actor: str, group_name: str, db: Session) -> Group:
    try: canonical = normalise_group(group_name)
    except ValueError as exc: raise HTTPException(status_code=400, detail=str(exc)) from exc
    group = db.query(Group).filter(Group.name == canonical).first()
    if not group: raise HTTPException(status_code=404, detail="Group not found")
    if _is_admin(actor, db): return group
    actor_user = db.query(User).filter(User.email == actor).first()
    membership = db.query(UserGroup).filter(UserGroup.user_id == actor_user.id,
        UserGroup.group_id == group.id, UserGroup.is_super == 1).first() if actor_user else None
    if not membership: raise HTTPException(status_code=403, detail="You do not supervise this group")
    return group

@router.get("/admin", tags=["Administration"], response_class=HTMLResponse)
def administration_page(actor: str = Depends(verify_token), db: Session = Depends(get_db)):
    supervised = get_user_groups(actor, db, supervised_only=True)
    if not _is_admin(actor, db) and not supervised:
        raise HTTPException(status_code=403, detail="Administrator or group super-user access required")
    state = administration_state(actor, db)
    safe = escape(json.dumps(state, indent=2))
    return HTMLResponse(f"""<!doctype html><html><head><title>GreenDIGIT administration</title>
<style>body{{font:15px system-ui;max-width:1100px;margin:2rem auto;padding:0 1rem}}pre{{background:#f4f6f4;padding:1rem;overflow:auto}}.ok{{color:#176b35}}</style></head>
<body><h1>GreenDIGIT access administration</h1><p class=ok>Authenticated as {escape(actor)}.</p>
<p>This page intentionally exposes no password hashes or tokens. Mutations use the documented Bearer-authenticated JSON API.</p><pre>{safe}</pre></body></html>""")

@router.get("/admin/state", tags=["Administration"])
def administration_state(actor: str = Depends(verify_token), db: Session = Depends(get_db)):
    admin = _is_admin(actor, db); supervised = get_user_groups(actor, db, supervised_only=True)
    if not admin and not supervised: raise HTTPException(status_code=403, detail="Administration access required")
    allowed = None if admin else set(supervised)
    users = []
    for user in db.query(User).order_by(User.email).all():
        memberships = db.query(Group.name, UserGroup.is_super).join(UserGroup, UserGroup.group_id==Group.id).filter(UserGroup.user_id==user.id).all()
        visible = memberships if allowed is None else [m for m in memberships if m[0] in allowed]
        if allowed is not None and not visible: continue
        users.append({"email": user.email, "roles": get_user_roles(user.email, db) if admin else [],
                      "groups": [{"name": n, "is_super": bool(s)} for n,s in visible]})
    requests_q = db.query(AccessRequest, User.email).join(User, User.id==AccessRequest.user_id).filter(AccessRequest.status=="pending")
    requests_out=[]
    for req,email in requests_q.all():
        if admin or (req.request_type=="group" and req.requested_value in supervised):
            requests_out.append({"id":req.id,"email":email,"type":req.request_type,"value":req.requested_value,"status":req.status})
    return {"actor":actor,"platform_admin":admin,"supervised_groups":supervised,"users":users,"pending_requests":requests_out}

@router.post("/admin/groups", tags=["Administration"])
def create_group(data: GroupCreateRequest, actor: str=Depends(require_admin), db: Session=Depends(get_db)):
    try: name=normalise_group(data.name)
    except ValueError as exc: raise HTTPException(status_code=400, detail=str(exc)) from exc
    group=db.query(Group).filter(Group.name==name).first()
    if not group: group=Group(name=name,display_name=data.display_name); db.add(group)
    _audit(db,actor,"group.create","success",group=name); db.commit()
    return {"ok":True,"group":name}

@router.delete("/admin/groups/{group_name}", tags=["Administration"])
def delete_group(group_name: str, actor: str=Depends(require_admin), db: Session=Depends(get_db)):
    try: name=normalise_group(group_name)
    except ValueError as exc: raise HTTPException(status_code=400,detail=str(exc)) from exc
    if name in {DEFAULT_GROUP,GREENDIGIT_GROUP}:
        raise HTTPException(status_code=400,detail="Built-in groups cannot be deleted")
    group=db.query(Group).filter(Group.name==name).first()
    if not group: return {"ok":True,"deleted":False,"group":name}
    db.delete(group); _audit(db,actor,"group.delete","success",group=name); db.commit()
    return {"ok":True,"deleted":True,"group":name}

@router.put("/admin/groups/{group_name}/members", tags=["Administration"])
def add_group_member(group_name: str, data: MembershipRequest, actor: str=Depends(verify_token), db: Session=Depends(get_db)):
    group=_managed_group(actor,group_name,db); user=db.query(User).filter(User.email==data.email.strip().lower()).first()
    if not user: raise HTTPException(status_code=404,detail="User not found")
    db.execute(UserGroup.__table__.insert().prefix_with("OR IGNORE").values(user_id=user.id,group_id=group.id,is_super=0))
    _audit(db,actor,"group.add-user","success",target=user.email,group=group.name); db.commit()
    return {"ok":True,"group":group.name,"email":user.email}

@router.delete("/admin/groups/{group_name}/members/{email}", tags=["Administration"])
def remove_group_member(group_name: str, email: str, actor: str=Depends(verify_token), db: Session=Depends(get_db)):
    group=_managed_group(actor,group_name,db); user=db.query(User).filter(User.email==email.strip().lower()).first()
    if not user: raise HTTPException(status_code=404,detail="User not found")
    db.query(UserGroup).filter(UserGroup.user_id==user.id,UserGroup.group_id==group.id).delete()
    _audit(db,actor,"group.remove-user","success",target=user.email,group=group.name); db.commit()
    return {"ok":True}

@router.put("/admin/groups/{group_name}/super/{email}", tags=["Administration"])
def promote_group_super(group_name: str,email: str,actor: str=Depends(require_admin),db: Session=Depends(get_db)):
    group=_managed_group(actor,group_name,db); user=db.query(User).filter(User.email==email.strip().lower()).first()
    if not user: raise HTTPException(status_code=404,detail="User not found")
    membership=db.query(UserGroup).filter(UserGroup.user_id==user.id,UserGroup.group_id==group.id).first()
    if membership: membership.is_super=1
    else: db.add(UserGroup(user_id=user.id,group_id=group.id,is_super=1))
    _audit(db,actor,"group.promote-super","success",target=user.email,group=group.name); db.commit(); return {"ok":True}

@router.delete("/admin/groups/{group_name}/super/{email}", tags=["Administration"])
def demote_group_super(group_name: str,email: str,actor: str=Depends(require_admin),db: Session=Depends(get_db)):
    group=_managed_group(actor,group_name,db); user=db.query(User).filter(User.email==email.strip().lower()).first()
    if not user: raise HTTPException(status_code=404,detail="User not found")
    db.query(UserGroup).filter(UserGroup.user_id==user.id,UserGroup.group_id==group.id).update({"is_super":0})
    _audit(db,actor,"group.demote-super","success",target=user.email,group=group.name); db.commit(); return {"ok":True}

@router.put("/admin/roles", tags=["Administration"])
def add_role(data: RoleChangeRequest,actor: str=Depends(require_admin),db: Session=Depends(get_db)):
    user=db.query(User).filter(User.email==data.email.strip().lower()).first()
    if not user: raise HTTPException(status_code=404,detail="User not found")
    role=_normalise_role(data.role); grant_user_role(db,user,role); _audit(db,actor,"role.add","success",target=user.email,detail=role); db.commit(); return {"ok":True}

@router.delete("/admin/roles/{role}/{email}", tags=["Administration"])
def revoke_role(role: str,email: str,actor: str=Depends(require_admin),db: Session=Depends(get_db)):
    role=_normalise_role(role); user=db.query(User).filter(User.email==email.strip().lower()).first()
    if not user: raise HTTPException(status_code=404,detail="User not found")
    db.query(UserRole).filter(UserRole.user_id==user.id,UserRole.role==role).delete(); _audit(db,actor,"role.remove","success",target=user.email,detail=role); db.commit(); return {"ok":True}

@router.post("/access-requests", tags=["Administration"])
def request_access(data: AccessRequestCreate,email: str=Depends(verify_token),db: Session=Depends(get_db)):
    kind=data.request_type.strip().lower()
    if kind=="role": value=_normalise_role(data.requested_value)
    elif kind=="group":
        try: value=normalise_group(data.requested_value)
        except ValueError as exc: raise HTTPException(status_code=400,detail=str(exc)) from exc
        if not db.query(Group).filter(Group.name==value).first(): raise HTTPException(status_code=404,detail="Group not available")
    else: raise HTTPException(status_code=400,detail="request_type must be role or group")
    user=db.query(User).filter(User.email==email).first()
    existing=db.query(AccessRequest).filter(AccessRequest.user_id==user.id,AccessRequest.request_type==kind,AccessRequest.requested_value==value,AccessRequest.status=="pending").first()
    if existing: return {"ok":True,"request_id":existing.id,"status":"pending"}
    req=AccessRequest(user_id=user.id,request_type=kind,requested_value=value); db.add(req); db.commit(); db.refresh(req)
    return {"ok":True,"request_id":req.id,"status":"pending"}

@router.post("/admin/access-requests/{request_id}", tags=["Administration"])
def decide_access(request_id:int,data:AccessDecision,actor:str=Depends(verify_token),db:Session=Depends(get_db)):
    req=db.query(AccessRequest).filter(AccessRequest.id==request_id,AccessRequest.status=="pending").first()
    if not req: raise HTTPException(status_code=404,detail="Pending request not found")
    decision=data.decision.strip().lower()
    if decision not in {"approved","rejected"}: raise HTTPException(status_code=400,detail="decision must be approved or rejected")
    target=db.query(User).filter(User.id==req.user_id).first(); admin=_is_admin(actor,db)
    if req.request_type=="role" and not admin: raise HTTPException(status_code=403,detail="Only platform administrators approve roles")
    if req.request_type=="group": group=_managed_group(actor,req.requested_value,db)
    if decision=="approved":
        if req.request_type=="role": grant_user_role(db,target,req.requested_value)
        else: db.execute(UserGroup.__table__.insert().prefix_with("OR IGNORE").values(user_id=target.id,group_id=group.id,is_super=0))
    req.status=decision; req.decided_at=datetime.now(timezone.utc).isoformat(); req.decided_by_user_id=db.query(User).filter(User.email==actor).first().id
    _audit(db,actor,"request.decide","success",target=target.email,group=req.requested_value if req.request_type=="group" else None,detail=decision); db.commit(); return {"ok":True,"status":decision}

@router.get(
    "/verify-token",
    tags=["Auth"],
    summary="Validate GreenDIGIT JWT token and optionally require a role",
    description=(
        "Validates the Bearer token and returns the authenticated email plus current database roles.\n\n"
        "Pass `required_role=publish` or `required_role=dashboards_view` to require a specific role. "
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
    required_role: Optional[str] = Query(default=None, description="Optional required role: publish or dashboards_view."),
    email: str = Depends(verify_token),
    db: Session = Depends(get_db),
):
    roles = get_user_roles(email, db)
    payload = {"valid": True, "sub": email, "roles": roles, "groups": get_user_groups(email, db)}
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
    description=(
        "Returns JSON: `{access_token, token_type, expires_in}`. "
        "Accepts `email` and `password` as query parameters.\n\n"
        "Example request:\n\n"
        "```bash\n"
        "curl -sS -G \"https://greendigit-cim.sztaki.hu/gd-cim-api/v1/token\" \\\n"
        "  --data-urlencode \"email=demo.publisher@example.org\" \\\n"
        "  --data-urlencode \"password=correct-horse-battery-staple\"\n"
        "```"
    ),
)
def get_token(
    email: str = Query(..., description="User email", example="demo.publisher@example.org"),
    password: str = Query(..., description="User password", example="correct-horse-battery-staple"),
    db: Session = Depends(get_db)
):
    email_lower = email.strip().lower()
    user = db.query(User).filter(User.email == email_lower).first()
    if not user:
        access_emails = load_access_emails()
        if email_lower not in access_emails:
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
    elif user.hashed_password == OIDC_PASSWORD_DISABLED:
        raise HTTPException(status_code=401, detail="Local sign-in is unavailable or the credentials are invalid.")
    elif user.hashed_password == PASSWORD_RESET_MARKER:
        user.hashed_password = pwd_context.hash(password)
        db.commit()
        _ensure_bootstrap_roles_for_user(db, user)
    elif not pwd_context.verify(password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Local sign-in is unavailable or the credentials are invalid.")
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

@router.post("/cim-json", include_in_schema=False)
def digest_cim_json():
    raise HTTPException(
        status_code=410,
        detail="Legacy client-supplied publisher endpoint disabled; use POST /v1/submit",
    )

app.include_router(router)
