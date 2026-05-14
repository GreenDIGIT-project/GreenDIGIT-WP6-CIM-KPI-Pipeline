import os
import time
import json
import hmac
import base64
import hashlib
import secrets
from html import escape
from urllib.parse import quote, urlencode

import requests
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

app = FastAPI(title="Grafana Auth Proxy", version="0.1.0")

GRAFANA_SUBPATH = os.getenv("GRAFANA_SUBPATH", "/metricsdb-dashboard/v1/charts").rstrip("/")
BASE_DASH_PATH = os.getenv("BASE_DASH_PATH", "/metricsdb-dashboard").rstrip("/")
LEGACY_DASH_PATH = os.getenv("LEGACY_DASH_PATH", "/metricsdb-dashboards").rstrip("/")
TOKEN_UI_PATH = os.getenv("TOKEN_UI_PATH", "/gd-cim-api/v1/token-ui")
PUBLIC_DASHBOARD_PATH = os.getenv("PUBLIC_DASHBOARD_PATH", "/public-dashboards").rstrip("/")
GRAFANA_UPSTREAM = os.getenv("GRAFANA_UPSTREAM", "http://grafana:3000").rstrip("/")
PUBLIC_GRAFANA_UPSTREAM = os.getenv("PUBLIC_GRAFANA_UPSTREAM", "http://grafana-public:3000").rstrip("/")
AUTH_VERIFY_URL = os.getenv("AUTH_VERIFY_URL", "http://cim-fastapi:8000/v1/verify-token")
AUTH_TOKEN_URL = os.getenv("AUTH_TOKEN_URL", "http://cim-fastapi:8000/v1/token")
DASHBOARD_REQUIRED_ROLE = os.getenv("DASHBOARD_REQUIRED_ROLE", "dashboards")
COOKIE_NAME = os.getenv("GRAFANA_AUTH_COOKIE_NAME", "gd_access_token")
COOKIE_SECURE = os.getenv("GRAFANA_AUTH_COOKIE_SECURE", "false").lower() == "true"
AUTH_VERIFY_CACHE_TTL_S = int(os.getenv("AUTH_VERIFY_CACHE_TTL_S", "120"))
LOCAL_JWT_VERIFY_ENABLED = os.getenv("LOCAL_JWT_VERIFY_ENABLED", "true").lower() == "true"
JWT_SECRET = os.getenv("JWT_GEN_SEED_TOKEN", "")
JWT_ISSUER = os.getenv("JWT_ISSUER", "greendigit-login-uva")
EGI_OIDC_ISSUER = os.getenv("EGI_OIDC_ISSUER", "https://aai.egi.eu/auth/realms/egi").rstrip("/")
EGI_OIDC_CLIENT_ID = os.getenv("EGI_OIDC_CLIENT_ID", "")
EGI_OIDC_CLIENT_SECRET = os.getenv("EGI_OIDC_CLIENT_SECRET", "")
EGI_OIDC_REDIRECT_URI = os.getenv("EGI_OIDC_REDIRECT_URI", "")
EGI_OIDC_SCOPE = os.getenv("EGI_OIDC_SCOPE", "openid email profile")
OIDC_STATE_COOKIE = os.getenv("EGI_OIDC_STATE_COOKIE", "gd_oidc_state")
OIDC_VERIFIER_COOKIE = os.getenv("EGI_OIDC_VERIFIER_COOKIE", "gd_oidc_verifier")
OIDC_NEXT_COOKIE = os.getenv("EGI_OIDC_NEXT_COOKIE", "gd_oidc_next")
DEFAULT_PUBLIC_DASHBOARD_URL = PUBLIC_DASHBOARD_PATH
DEFAULT_EGI_FEDERATION_REGISTRY_URL = "https://aai.egi.eu/auth/realms/id/account/#/enroll?groupPath=/vo.greendigit.egi.eu"
DEFAULT_METRICS_FORM_URL = "https://forms.gle/uYvEBGPvaiGW1rDDA"

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
}

http = requests.Session()
_verify_cache: dict[tuple[str, str | None], tuple[str, float]] = {}
_oidc_config: tuple[dict[str, str], float] | None = None


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(raw: str) -> bytes:
    padded = raw + "=" * (-len(raw) % 4)
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def _sha256_b64url(raw: str) -> str:
    return _b64url_encode(hashlib.sha256(raw.encode("ascii")).digest())


def _create_local_access_token(email: str) -> str:
    if not JWT_SECRET:
        raise RuntimeError("JWT_GEN_SEED_TOKEN is required for OIDC dashboard login")
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": email.strip().lower(),
        "iss": JWT_ISSUER,
        "iat": now,
        "nbf": now,
        "exp": now + 86400,
    }
    header_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    sig_b64 = _b64url_encode(
        hmac.new(JWT_SECRET.encode("utf-8"), signing_input, hashlib.sha256).digest()
    )
    return f"{header_b64}.{payload_b64}.{sig_b64}"


def _safe_next(next_path: str | None) -> str:
    if isinstance(next_path, str) and next_path.startswith("/") and not next_path.startswith("//"):
        return next_path
    return f"{GRAFANA_SUBPATH}/"


def _oidc_metadata() -> dict[str, str]:
    global _oidc_config
    now = time.time()
    if _oidc_config and _oidc_config[1] > now:
        return _oidc_config[0]

    resp = http.get(f"{EGI_OIDC_ISSUER}/.well-known/openid-configuration", timeout=10)
    resp.raise_for_status()
    metadata = resp.json()
    _oidc_config = (metadata, now + 3600)
    return metadata


def _oidc_redirect_uri(request: Request) -> str:
    if EGI_OIDC_REDIRECT_URI:
        return EGI_OIDC_REDIRECT_URI
    return str(request.url_for("oidc_callback"))


def _extract_oidc_email(token_payload: dict, userinfo: dict) -> str | None:
    for source in (userinfo, token_payload):
        for key in ("email", "sub"):
            value = str(source.get(key, "")).strip().lower()
            if "@" in value:
                return value
    return None


def _local_verify_user_email(token: str) -> str | None:
    if not LOCAL_JWT_VERIFY_ENABLED or not JWT_SECRET:
        return None
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header_b64, payload_b64, sig_b64 = parts

        header = json.loads(_b64url_decode(header_b64))
        if header.get("alg") != "HS256":
            return None

        signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
        expected_sig = hmac.new(
            JWT_SECRET.encode("utf-8"),
            signing_input,
            hashlib.sha256,
        ).digest()
        actual_sig = _b64url_decode(sig_b64)
        if not hmac.compare_digest(expected_sig, actual_sig):
            return None

        payload = json.loads(_b64url_decode(payload_b64))
        now = int(time.time())

        iss = payload.get("iss")
        sub = str(payload.get("sub", "")).strip().lower()
        exp = int(payload.get("exp", 0))
        nbf = int(payload.get("nbf", 0))
        iat = int(payload.get("iat", 0))

        if not sub:
            return None
        if iss != JWT_ISSUER:
            return None
        if exp <= now:
            return None
        if nbf and nbf > now:
            return None
        if iat and iat > now:
            return None
        return sub
    except Exception:
        return None


def _cache_get(token: str, required_role: str | None = None) -> str | None:
    item = _verify_cache.get((token, required_role))
    if not item:
        return None
    email, expires_at = item
    if time.time() >= expires_at:
        _verify_cache.pop((token, required_role), None)
        return None
    return email


def _cache_set(token: str, email: str, required_role: str | None = None) -> None:
    ttl = max(1, AUTH_VERIFY_CACHE_TTL_S)
    _verify_cache[(token, required_role)] = (email, time.time() + ttl)
    # Keep memory bounded in long-running processes.
    if len(_verify_cache) > 10000:
        now = time.time()
        expired = [k for k, (_, exp) in _verify_cache.items() if exp <= now]
        for key in expired:
            _verify_cache.pop(key, None)
        if len(_verify_cache) > 10000:
            _verify_cache.clear()


def _extract_token(request: Request) -> str | None:
    auth_header = request.headers.get("authorization", "")
    if auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()
        if token:
            return token
    token = request.cookies.get(COOKIE_NAME)
    if token:
        token = token.strip()
        if token:
            return token
    return None


def _verify_user_email(token: str, required_role: str | None = None) -> str | None:
    cached = _cache_get(token, required_role)
    if cached:
        return cached

    # Role checks must use the auth service because roles live in users.db.
    if required_role is None:
        local = _local_verify_user_email(token)
        if local:
            _cache_set(token, local)
            return local

    # Fallback: remote introspection via CIM endpoint.
    try:
        params = {"required_role": required_role} if required_role else None
        resp = http.get(
            AUTH_VERIFY_URL,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=5,
        )
    except requests.RequestException:
        return None

    if resp.status_code != 200:
        return None

    try:
        payload = resp.json()
    except ValueError:
        return None

    if payload.get("valid") is True and payload.get("sub"):
        email = str(payload["sub"]).strip().lower()
        if email:
            _cache_set(token, email, required_role)
            return email
    return None


def _verify_dashboard_user_email(token: str) -> str | None:
    return _verify_user_email(token, required_role=DASHBOARD_REQUIRED_ROLE)


def _login_redirect(request: Request) -> RedirectResponse:
    nxt = request.url.path
    if request.url.query:
        nxt = f"{nxt}?{request.url.query}"
    return RedirectResponse(url=f"{TOKEN_UI_PATH}?next={quote(nxt, safe='/%?=&')}", status_code=307)


def _issue_dashboard_session(token: str, next_path: str) -> RedirectResponse:
    safe_next = (
        next_path
        if isinstance(next_path, str) and next_path.startswith(f"{GRAFANA_SUBPATH}/")
        else f"{GRAFANA_SUBPATH}/"
    )
    response = RedirectResponse(url=safe_next, status_code=303)
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite="lax",
        path="/",
        max_age=86400,
    )
    return response


async def _forward_to_upstream(
    request: Request,
    upstream_base: str,
    user_email: str | None = None,
) -> Response:
    target = f"{upstream_base}{request.url.path}"
    if request.url.query:
        target = f"{target}?{request.url.query}"

    headers = {}
    cookie_parts: list[str] = []
    raw_cookie = request.headers.get("cookie", "")
    if raw_cookie:
        for part in raw_cookie.split(";"):
            piece = part.strip()
            if not piece:
                continue
            if piece.lower().startswith(f"{COOKIE_NAME.lower()}="):
                # Keep JWT cookie only at the proxy layer.
                continue
            cookie_parts.append(piece)

    for key, value in request.headers.items():
        k = key.lower()
        if k in {"host", "cookie", "authorization", "x-webauth-user", "x-webauth-email"}:
            continue
        headers[key] = value
    if cookie_parts:
        headers["Cookie"] = "; ".join(cookie_parts)
    if user_email:
        headers["X-WEBAUTH-USER"] = user_email
        headers["X-WEBAUTH-EMAIL"] = user_email

    body = await request.body()
    try:
        upstream = http.request(
            method=request.method,
            url=target,
            headers=headers,
            data=body if body else None,
            allow_redirects=False,
            timeout=120,
        )
    except requests.Timeout:
        return JSONResponse(
            {"detail": "Grafana upstream timed out"},
            status_code=504,
        )
    except requests.RequestException:
        return JSONResponse(
            {"detail": "Grafana upstream unavailable"},
            status_code=503,
        )

    response_headers = {
        k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS
    }
    return Response(content=upstream.content, status_code=upstream.status_code, headers=response_headers)


async def _forward(request: Request, user_email: str) -> Response:
    return await _forward_to_upstream(request, GRAFANA_UPSTREAM, user_email=user_email)


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/", include_in_schema=False)
def root() -> HTMLResponse:
    return landing_page()


@app.get("/landing", include_in_schema=False)
def landing() -> HTMLResponse:
    return landing_page()


def landing_page() -> HTMLResponse:
    login_url = escape(TOKEN_UI_PATH, quote=True)
    public_dashboard_url = escape(
        os.getenv("PUBLIC_DASHBOARD_URL", DEFAULT_PUBLIC_DASHBOARD_URL).strip()
        or DEFAULT_PUBLIC_DASHBOARD_URL,
        quote=True,
    )
    metrics_form_url = escape(
        os.getenv("METRICS_FORM_URL", DEFAULT_METRICS_FORM_URL).strip()
        or DEFAULT_METRICS_FORM_URL,
        quote=True,
    )

    return HTMLResponse(
        f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>EIMPS CIM Platform</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            line-height: 1.6;
            color: #25302b;
            background: #f5f7f4;
            min-height: 100vh;
        }}
        .container {{ max-width: 1040px; margin: 0 auto; padding: 40px 20px; }}
        header {{ margin-bottom: 32px; }}
        .logo {{ width: 112px; height: auto; margin-bottom: 20px; }}
        h1 {{ color: #215f32; font-size: 2.35rem; line-height: 1.15; font-weight: 700; margin-bottom: 14px; }}
        .subtitle {{ color: #4f5f55; font-size: 1.08rem; max-width: 820px; }}
        .actions {{
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 18px;
            margin: 32px 0;
        }}
        .action-card {{
            background: #fff;
            border: 1px solid #dfe7df;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06);
            padding: 24px;
            display: flex;
            flex-direction: column;
            min-height: 210px;
        }}
        .action-card h2 {{ color: #215f32; font-size: 1.14rem; margin-bottom: 10px; }}
        .action-card p {{ color: #59655d; font-size: 0.95rem; margin-bottom: 20px; }}
        .button {{
            margin-top: auto;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-height: 44px;
            border-radius: 6px;
            padding: 10px 14px;
            background: #215f32;
            color: #fff;
            text-decoration: none;
            font-weight: 650;
            text-align: center;
        }}
        .button.secondary {{ background: #1f5f8f; }}
        .button.tertiary {{ background: #38423b; }}
        .button-row {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 10px;
            margin-top: auto;
        }}
        .button-row .button {{ margin-top: 0; }}
        .button-row .button:first-child {{ grid-column: 1 / -1; }}
        .button.disabled,
        .button[aria-disabled="true"] {{
            background: #9aa39d;
            color: #f7faf7;
            cursor: not-allowed;
            pointer-events: none;
        }}
        .button:hover {{ filter: brightness(0.94); }}
        .section {{
            background: #fff;
            border: 1px solid #dfe7df;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06);
            padding: 28px;
            margin-bottom: 24px;
        }}
        .section h2 {{ color: #215f32; font-size: 1.28rem; margin-bottom: 16px; }}
        .steps {{ margin-left: 22px; color: #46534b; }}
        .steps li {{ margin-bottom: 10px; }}
        .funding p {{ color: #58645d; font-size: 0.95rem; }}
        .funding a {{ color: #1f5f8f; }}
        footer {{ text-align: center; padding: 24px 20px; color: #78827b; font-size: 0.85rem; }}
        @media (max-width: 800px) {{
            h1 {{ font-size: 1.9rem; }}
            .actions {{ grid-template-columns: 1fr; }}
            .action-card {{ min-height: auto; }}
            .button-row {{ grid-template-columns: 1fr; }}
            .button-row .button:first-child {{ grid-column: auto; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <img src="/gd-cim-api/static/cropped-GD_logo.png" alt="GreenDIGIT Logo" class="logo">
            <h1>EIMPS CIM Platform</h1>
            <p class="subtitle">EIMPS provides a common pipeline for collecting, harmonising, enriching, and publishing environmental impact metrics from distributed research infrastructures. The platform supports secure metric submission, KPI enrichment, and dashboard-based exploration of aggregated sustainability indicators.</p>
        </header>

        <main>
            <div class="actions" aria-label="Platform entry points">
                <article class="action-card">
                    <h2>Login</h2>
                    <p>Use the existing token login page to generate an API token or continue to the dashboards.</p>
                    <div class="button-row">
                        <a class="button" href="{login_url}">Login</a>
                        <a class="button secondary" href="{public_dashboard_url}">View Public Dashboards</a>
                        <a class="button tertiary" href="{metrics_form_url}" target="_blank" rel="noopener">Dashboard / Metrics Form</a>
                    </div>
                </article>

                <article class="action-card">
                    <h2>Request Access / Register Service</h2>
                    <p>EGI Federation Registry access requests are temporarily unavailable from this page.</p>
                    <span class="button secondary disabled" aria-disabled="true">Temporarily unavailable</span>
                </article>

                <article class="action-card">
                    <h2>Login / Sign up</h2>
                    <p>EGI Check-in / OIDC login is temporarily unavailable from this entry point.</p>
                    <span class="button tertiary disabled" aria-disabled="true">Temporarily unavailable</span>
                </article>
            </div>

            <section class="section">
                <h2>How it works</h2>
                <ol class="steps">
                    <li>Register or request access through EGI.</li>
                    <li>Login with your institutional or EGI identity.</li>
                    <li>Submit environmental metrics or access authorised dashboards depending on your role.</li>
                    <li>Explore anonymised public dashboards without authentication.</li>
                </ol>
            </section>

            <section class="section funding">
                <h2>Funding &amp; Acknowledgements</h2>
                <p>This work is funded from the European Union's Horizon Europe research and innovation programme through the <a href="https://greendigit-project.eu/" target="_blank" rel="noopener">GreenDIGIT project</a>, under Grant Agreement No. <a href="https://cordis.europa.eu/project/id/101131207" target="_blank" rel="noopener">101131207</a>.</p>
            </section>
        </main>

        <footer>
            <p>If you want to have access to the dashboards and/or submit your metrics, please contact <a href="mailto:g.j.teixeiradepinhoferreira@uva.nl">g.j.teixeiradepinhoferreira@uva.nl</a>.</p>
            <p>&copy; <span id="year"></span> GreenDIGIT Project. All rights reserved.</p>
        </footer>
    </div>
    <script>
        const currentYear = new Date().getFullYear();
        document.getElementById("year").textContent = `2024-${{currentYear >= 2027 ? 2027 : currentYear}}`;
    </script>
</body>
</html>"""
    )


@app.api_route(LEGACY_DASH_PATH, methods=["GET", "HEAD"], include_in_schema=False)
@app.api_route(f"{LEGACY_DASH_PATH}/", methods=["GET", "HEAD"], include_in_schema=False)
@app.api_route(f"{LEGACY_DASH_PATH}" + "/{path:path}", methods=["GET", "HEAD"], include_in_schema=False)
def legacy_dash_redirect(request: Request, path: str = "") -> RedirectResponse:
    token = _extract_token(request)
    if token and _verify_dashboard_user_email(token):
        return RedirectResponse(url=f"{GRAFANA_SUBPATH}/", status_code=307)
    return RedirectResponse(
        url=f"{TOKEN_UI_PATH}?next={quote(f'{GRAFANA_SUBPATH}/', safe='/%?=&')}",
        status_code=307,
    )


@app.get("/auth/login", include_in_schema=False)
@app.get(f"{GRAFANA_SUBPATH}/auth/login", include_in_schema=False)
def login_page(request: Request, next: str = f"{GRAFANA_SUBPATH}/") -> Response:
    if not EGI_OIDC_CLIENT_ID:
        return HTMLResponse(
            "EGI Check-in login is not configured. Set EGI_OIDC_CLIENT_ID, "
            "EGI_OIDC_CLIENT_SECRET if required, and EGI_OIDC_REDIRECT_URI.",
            status_code=503,
        )

    try:
        metadata = _oidc_metadata()
    except requests.RequestException:
        return Response("EGI Check-in metadata unavailable", status_code=502)

    state = secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(64)
    params = {
        "client_id": EGI_OIDC_CLIENT_ID,
        "redirect_uri": _oidc_redirect_uri(request),
        "response_type": "code",
        "scope": EGI_OIDC_SCOPE,
        "state": state,
        "code_challenge": _sha256_b64url(verifier),
        "code_challenge_method": "S256",
    }
    response = RedirectResponse(
        url=f"{metadata['authorization_endpoint']}?{urlencode(params)}",
        status_code=303,
    )
    cookie_kwargs = {
        "httponly": True,
        "secure": COOKIE_SECURE,
        "samesite": "lax",
        "path": "/",
        "max_age": 600,
    }
    response.set_cookie(OIDC_STATE_COOKIE, state, **cookie_kwargs)
    response.set_cookie(OIDC_VERIFIER_COOKIE, verifier, **cookie_kwargs)
    response.set_cookie(OIDC_NEXT_COOKIE, _safe_next(next), **cookie_kwargs)
    return response


@app.get("/auth/callback", name="oidc_callback", include_in_schema=False)
def oidc_callback(request: Request, code: str | None = None, state: str | None = None) -> Response:
    expected_state = request.cookies.get(OIDC_STATE_COOKIE)
    verifier = request.cookies.get(OIDC_VERIFIER_COOKIE)
    next_path = _safe_next(request.cookies.get(OIDC_NEXT_COOKIE))

    if not code or not state or not expected_state or not hmac.compare_digest(state, expected_state):
        return Response("Invalid EGI Check-in login state", status_code=400)
    if not verifier:
        return Response("Missing EGI Check-in verifier", status_code=400)

    try:
        metadata = _oidc_metadata()
        token_data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": _oidc_redirect_uri(request),
            "client_id": EGI_OIDC_CLIENT_ID,
            "code_verifier": verifier,
        }
        if EGI_OIDC_CLIENT_SECRET:
            token_data["client_secret"] = EGI_OIDC_CLIENT_SECRET
        token_resp = http.post(metadata["token_endpoint"], data=token_data, timeout=15)
        token_resp.raise_for_status()
        token_payload = token_resp.json()
        access_token = token_payload.get("access_token")
        if not access_token:
            return Response("EGI Check-in token response missing access token", status_code=502)

        userinfo_resp = http.get(
            metadata["userinfo_endpoint"],
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        )
        userinfo_resp.raise_for_status()
        userinfo = userinfo_resp.json()
    except requests.RequestException:
        return Response("EGI Check-in token exchange failed", status_code=502)
    except ValueError:
        return Response("Invalid EGI Check-in response", status_code=502)

    email = _extract_oidc_email(token_payload, userinfo)
    if not email:
        return Response("EGI Check-in did not return an email address", status_code=403)

    try:
        local_token = _create_local_access_token(email)
    except RuntimeError as exc:
        return Response(str(exc), status_code=503)
    if not _verify_dashboard_user_email(local_token):
        return Response("Dashboard access is not allowed for this user", status_code=403)

    response = _issue_dashboard_session(local_token, next_path)
    response.delete_cookie(OIDC_STATE_COOKIE, path="/")
    response.delete_cookie(OIDC_VERIFIER_COOKIE, path="/")
    response.delete_cookie(OIDC_NEXT_COOKIE, path="/")
    return response


@app.post("/auth/login", include_in_schema=False)
@app.post(f"{GRAFANA_SUBPATH}/auth/login", include_in_schema=False)
def login_submit(
    email: str = Form(...),
    password: str = Form(...),
    next: str = Form(f"{GRAFANA_SUBPATH}/"),
) -> Response:
    safe_next = next if isinstance(next, str) and next.startswith("/") else f"{GRAFANA_SUBPATH}/"
    try:
        resp = http.get(
            AUTH_TOKEN_URL,
            params={"email": email, "password": password},
            timeout=15,
        )
    except requests.RequestException:
        return Response("Auth service unavailable", status_code=502)

    if resp.status_code != 200:
        return Response("Invalid credentials", status_code=401)

    try:
        payload = resp.json()
    except ValueError:
        return Response("Invalid auth response", status_code=502)

    token = payload.get("access_token")
    if not token:
        return Response("Auth token missing", status_code=502)
    if not _verify_dashboard_user_email(token):
        return Response("Dashboard access is not allowed for this user", status_code=403)

    return _issue_dashboard_session(token, safe_next)


@app.get("/auth/logout", include_in_schema=False)
@app.get(f"{GRAFANA_SUBPATH}/auth/logout", include_in_schema=False)
def logout() -> RedirectResponse:
    response = RedirectResponse(url=TOKEN_UI_PATH, status_code=303)
    response.delete_cookie(COOKIE_NAME, path="/")
    return response


@app.post(f"{GRAFANA_SUBPATH}/auth/sso", include_in_schema=False)
def sso_login(
    token: str = Form(...),
    next: str = Form(f"{GRAFANA_SUBPATH}/"),
) -> Response:
    clean_token = (token or "").strip()
    if not clean_token:
        return Response("Missing token", status_code=400)
    if not _verify_dashboard_user_email(clean_token):
        return Response("Dashboard access is not allowed for this user", status_code=403)
    return _issue_dashboard_session(clean_token, next)


@app.get(f"{PUBLIC_DASHBOARD_PATH}", include_in_schema=False)
@app.head(f"{PUBLIC_DASHBOARD_PATH}", include_in_schema=False)
def public_dashboard_redirect() -> RedirectResponse:
    return RedirectResponse(url=f"{PUBLIC_DASHBOARD_PATH}/", status_code=307)


@app.api_route(
    f"{PUBLIC_DASHBOARD_PATH}" + "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    include_in_schema=False,
)
async def public_grafana_proxy(request: Request, path: str = "") -> Response:
    return await _forward_to_upstream(request, PUBLIC_GRAFANA_UPSTREAM)


@app.api_route(f"{GRAFANA_SUBPATH}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
@app.api_route(
    f"{GRAFANA_SUBPATH}" + "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
)
async def grafana_proxy(request: Request, path: str = "") -> Response:
    token = _extract_token(request)
    if not token:
        return _login_redirect(request)

    user_email = _verify_dashboard_user_email(token)
    if not user_email:
        response = _login_redirect(request)
        response.delete_cookie(COOKIE_NAME, path="/")
        return response

    # Grafana may aggressively rotate its own session token in a loop when
    # used behind auth proxy. We authenticate each request via JWT anyway,
    # so this endpoint can be treated as a harmless no-op.
    if request.url.path.endswith("/api/user/auth-tokens/rotate") and request.method == "POST":
        return JSONResponse({"message": "ok"}, status_code=200)

    return await _forward(request, user_email)


@app.api_route(BASE_DASH_PATH, methods=["GET", "HEAD"], include_in_schema=False)
@app.api_route(f"{BASE_DASH_PATH}/", methods=["GET", "HEAD"], include_in_schema=False)
@app.api_route(f"{BASE_DASH_PATH}" + "/{path:path}", methods=["GET", "HEAD"], include_in_schema=False)
def base_dash_redirect(request: Request, path: str = "") -> RedirectResponse:
    token = _extract_token(request)
    if token and _verify_dashboard_user_email(token):
        return RedirectResponse(url=f"{GRAFANA_SUBPATH}/", status_code=307)
    return RedirectResponse(
        url=f"{TOKEN_UI_PATH}?next={quote(f'{GRAFANA_SUBPATH}/', safe='/%?=&')}",
        status_code=307,
    )
