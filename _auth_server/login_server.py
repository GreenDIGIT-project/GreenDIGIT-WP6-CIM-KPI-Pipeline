from fastapi import FastAPI, Depends, HTTPException, status, Request, Body, Query, APIRouter
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
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime, timezone
import traceback, uuid
from pathlib import Path
import requests
from bson import ObjectId




load_dotenv()  # loads from .env in the current folder by default

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
    root_path="/gd-cim-api",
    docs_url="/v1/docs",
    openapi_url="/v1/openapi.json",
)
router = APIRouter(prefix="/v1")
prefix = app.root_path or ""
app.description = (
    "API for publishing metrics for GreenDIGIT WP6 partners (IFcA, DIRAC, and UTH).\n\n"
    "**Authentication**\n\n"
    "- Obtain a token via **POST /v1/login** using form fields `email` and `password`, "
    "or via **GET /v1/token** with query parameters `email` and `password`. "
    "Your email must be registered beforehand. If it fails (wrong password/unknown), "
    "please contact goncalo.ferreira@student.uva.nl or a.tahir2@uva.nl.\n"
    "- Then include `Authorization: Bearer <token>` on all protected requests.\n"
    "- Tokens expire after 1 day — regenerate when needed.\n\n"
    "### Funding and acknowledgements\n"
    "This work is funded from the European Union’s Horizon Europe research and innovation programme "
    "through the [GreenDIGIT project](https://greendigit-project.eu/), under the grant agreement "
    "No. [101131207](https://cordis.europa.eu/project/id/101131207).\n\n"
    # GitHub badge (Markdown)
    "[![GitHub Repo](https://img.shields.io/badge/github-GreenDIGIT--AuthServer-blue?logo=github)]"
    "(https://github.com/g-uva/GreenDIGIT-AuthServer)\n\n"
    # Logos (HTML so we can size them)
    f'<p><img src="{prefix}/static/EN-Funded-by-the-EU-POS-2.png" alt="Funded by the EU" width="160"> '
    f'<img src="{prefix}/static/cropped-GD_logo.png" alt="GreenDIGIT" width="120"></p>'
)

STATIC_DIR = Path(__file__).parent / "static"
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
CIM_SUBMIT_TIMEOUT_SECONDS = int(os.getenv("CIM_SUBMIT_TIMEOUT_SECONDS", "900"))
MONGO_URI_DIRAC = os.getenv("MONGO_URI_DIRAC", os.getenv("MONGO_URI", "mongodb://localhost:27017"))
DB_NAME_DIRAC = os.getenv("DB_NAME_DIRAC", os.getenv("DB_NAME", "metricsdb"))
COLL_NAME_DIRAC = os.getenv("COLL_NAME_DIRAC", os.getenv("COLL_NAME", "metrics"))
MONGO_SERVER_SELECTION_TIMEOUT_MS = int(os.getenv("MONGO_SERVER_SELECTION_TIMEOUT_MS", "5000"))
MONGO_CONNECT_TIMEOUT_MS = int(os.getenv("MONGO_CONNECT_TIMEOUT_MS", "5000"))

_dirac_client = MongoClient(
    MONGO_URI_DIRAC,
    serverSelectionTimeoutMS=MONGO_SERVER_SELECTION_TIMEOUT_MS,
    connectTimeoutMS=MONGO_CONNECT_TIMEOUT_MS,
)
_dirac_db = _dirac_client[DB_NAME_DIRAC]
_dirac_col = _dirac_db[COLL_NAME_DIRAC]

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

Base.metadata.create_all(bind=engine)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class SubmitData(BaseModel):
    field1: str
    field2: int

class GetTokenRequest(BaseModel):
    email: str
    password: str
    
class MetricItem(BaseModel):
    node: str
    metric: str
    value: float
    timestamp: str
    cfp_ci_service: Dict[str, Any] = Field(..., description="Embedded CI service response")

class PostCimJsonRequest(BaseModel):
    publisher_email: str
    job_id: str
    metrics: List[MetricItem]

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
        return set(line.strip().lower() for line in f if line.strip())

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
        "Returns a JWT for `Authorization: Bearer <token>`."
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
            raise HTTPException(status_code=403, detail="Email not allowed")
        hashed_password = pwd_context.hash(form_data.password)
        db_user = User(email=email_lower, hashed_password=hashed_password)
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        user = db_user
    elif not pwd_context.verify(form_data.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect password. \n If you have forgotten your password please contact the GreenDIGIT team: goncalo.ferreira@student.uva.nl.")
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

def static_url(request: Request, filename: str) -> str:
    # Prefer proxy header; fall back to ASGI root_path; finally no prefix
    prefix = request.headers.get("x-forwarded-prefix") or request.scope.get("root_path") or ""
    if prefix.endswith("/"):
        prefix = prefix[:-1]
    return f"{prefix}/static/{filename}"

@router.get(
    "/token-ui",
    tags=["Auth"],
    summary="Simple HTML login to manually obtain a token",
    description="Convenience page that POSTs to `/v1/login`.",
    response_class=HTMLResponse
)
def token_ui(request: Request):
    gd_logo = static_url(request, "cropped-GD_logo.png")
    eu_logo = static_url(request, "EN-Funded-by-the-EU-POS-2.png")

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
                <form action="login" method="post">
                    <input name="username" type="email" placeholder="Email" required>
                    <input name="password" type="password" placeholder="Password" required>
                    <button type="submit">Get Token</button>
                </form>
                
                <div class="info">
                    <p>The token is only valid for 1 day. You must regenerate in order to access.</p>
                </div>
                
                <div class="contact">
                    <p>If you have problems logging in, please contact:</p>
                    <ul>
                        <li>goncalo.ferreira@student.uva.nl</li>
                        <li>a.tahir2@uva.nl</li>
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
        </body>
        </html>
    """

@router.post(
    "/submit",
    tags=["Metrics"],
    summary="Submit a metrics JSON payload",
    description=(
        "Stores an arbitrary JSON document as a metric entry.\n\n"
        "**Requires:** `Authorization: Bearer <token>`.\n\n"
        "The `publisher_email` is derived from the token’s `sub` claim."
    ),
    responses={
        200: {"description": "Stored successfully"},
        400: {"description": "Invalid JSON body"},
        401: {"description": "Missing/invalid Bearer token"},
        500: {"description": "Database error"},
    },
)
async def submit(
    request: Request,
    publisher_email: str = Depends(verify_token),
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
    "/submit-dirac",
    tags=["Metrics"],
    summary="Submit a metrics JSON payload to the DIRAC metrics DB",
    description=(
        "Stores an arbitrary JSON document as a metric entry in the DIRAC MongoDB.\n\n"
        "**Requires:** `Authorization: Bearer <token>`.\n\n"
        "The `publisher_email` is derived from the token’s `sub` claim."
    ),
    responses={
        200: {"description": "Stored successfully"},
        400: {"description": "Invalid JSON body"},
        401: {"description": "Missing/invalid Bearer token"},
        500: {"description": "Database error"},
    },
)
async def submit_dirac(
    request: Request,
    publisher_email: str = Depends(verify_token),
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
    ack = _store_metric_in_col(col=_dirac_col, publisher_email=publisher_email, body=body)
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
        "**Requires:** `Authorization: Bearer <token>`."
    ),
    responses={
        200: {"description": "Forwarded to CIM successfully"},
        400: {"description": "Invalid request"},
        401: {"description": "Missing/invalid Bearer token"},
        404: {"description": "No matching stored metrics found"},
        502: {"description": "CIM call failed"},
    },
    include_in_schema=False,
)
async def submit_cim(
    payload: SubmitCIMRequest,
    request: Request,
    caller_email: str = Depends(verify_token),
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
    "/metrics/me",
    tags=["Metrics"],
    summary="List my published metrics",
    description=(
        "Returns all metrics published by the authenticated user.\n\n"
        "**Requires:** `Authorization: Bearer <token>`."
    ),
    responses={
        200: {"description": "List of metrics"},
        401: {"description": "Missing/invalid Bearer token"},
    },
)
def get_my_metrics(publisher_email: str = Depends(verify_token)):
    # Query all documents for this publisher
    docs = list(_col.find({"publisher_email": publisher_email}).sort("timestamp", -1))
    # Convert ObjectId and datetime to strings
    for d in docs:
        d["_id"] = str(d["_id"])
        if "timestamp" in d and not isinstance(d["timestamp"], str):
            d["timestamp"] = str(d["timestamp"])
    return docs


@router.delete(
    "/delete-dirac/{site}/{start_end}",
    tags=["Metrics"],
    summary="Delete my stored metrics for a site and time window",
    description=(
        "Deletes MongoDB metric documents owned by the authenticated user (`publisher_email` from JWT) "
        "filtered by `site` and an inclusive time window.\n\n"
        "`start_end` must be two ISO-8601 timestamps separated by `--` (recommended) or `_`, for example:\n"
        "- `2025-01-01T00:00:00Z--2025-01-02T00:00:00Z`\n"
        "- `2025-01-01T00:00:00Z_2025-01-02T00:00:00Z`\n\n"
        "Note: if your client struggles with `:` in the URL path, URL-encode timestamps (e.g. `%3A`)."
    ),
    responses={
        200: {"description": "Delete result"},
        400: {"description": "Invalid parameters"},
        401: {"description": "Missing/invalid Bearer token"},
        500: {"description": "Database error"},
    },
)
def delete_my_metrics_for_site(
    site: str,
    start_end: str,
    publisher_email: str = Depends(verify_token),
):
    start_raw, end_raw = _split_start_end(start_end)
    start_dt = _parse_iso_dt_or_400(start_raw, "start")
    end_dt = _parse_iso_dt_or_400(end_raw, "end")
    if start_dt > end_dt:
        raise HTTPException(status_code=400, detail="start must be <= end")

    # `timestamp` is typically stored as ISO string; keep a datetime fallback in case older docs used BSON Date.
    start_iso = _iso_utc_micro(start_dt)
    end_iso = _iso_utc_micro(end_dt)

    base_query: dict[str, Any] = {"publisher_email": publisher_email}

    try:
        candidates = list(_dirac_col.find(base_query, {"_id": 1, "site": 1, "body": 1}))
        to_delete_ids = [
            d["_id"]
            for d in candidates
            if _doc_matches_site(d, site) and _doc_matches_time_window(d, start_dt, end_dt)
        ]
        if to_delete_ids:
            res = _dirac_col.delete_many({"publisher_email": publisher_email, "_id": {"$in": to_delete_ids}})
            deleted_count = int(getattr(res, "deleted_count", 0))
        else:
            deleted_count = 0
        remaining_count = _dirac_col.count_documents({"publisher_email": publisher_email})
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Mongo delete failed: {exc}")

    return {
        "ok": True,
        "publisher_email": publisher_email,
        "site": site,
        "start": start_iso,
        "end": end_iso,
        "deleted_count": deleted_count,
        "time_window_candidates": int(len(candidates)),
        "remaining_count": int(remaining_count),
    }


class PasswordResetRequest(BaseModel):
    new_password: str

@router.post("/reset-password", tags=["Auth"], summary="Reset my password")
def reset_password(
    data: PasswordResetRequest,
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

@router.get("/verify-token", tags=["Auth"], summary=["Validate GreenDIGIT JWT based token."])
def verify_token_endpoint(email: str = Depends(verify_token)):
    return { "valid": True, "sub": email }


@router.get(
    "/token",
    tags=["Auth"],
    summary="Get JWT via query string (email and password).",
    description="Returns JSON: {access_token, token_type, expires_in}. Accepts `email` and `password` as query parameters."
)
def get_token(
    email: str = Query(..., description="User email"),
    password: str = Query(..., description="User password"),
    db: Session = Depends(get_db)
):
    email_lower = email.strip().lower()
    user = db.query(User).filter(User.email == email_lower).first()
    if not user:
        allowed_emails = load_allowed_emails()
        if email_lower not in allowed_emails:
            raise HTTPException(status_code=403, detail="Email not allowed")
        hashed_password = pwd_context.hash(password)
        user = User(email=email_lower, hashed_password=hashed_password)
        db.add(user); db.commit(); db.refresh(user)
    elif not pwd_context.verify(password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect password. \n If you have forgotten your password please contact the GreenDIGIT team: goncalo.ferreira@student.uva.nl.")

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
