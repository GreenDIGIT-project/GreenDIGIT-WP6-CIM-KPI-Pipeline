import os
from urllib.parse import quote

import requests
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

app = FastAPI(title="Grafana Auth Proxy", version="0.1.0")

GRAFANA_SUBPATH = os.getenv("GRAFANA_SUBPATH", "/metricsdb-dashboard/v1/charts").rstrip("/")
BASE_DASH_PATH = os.getenv("BASE_DASH_PATH", "/metricsdb-dashboard").rstrip("/")
LEGACY_DASH_PATH = os.getenv("LEGACY_DASH_PATH", "/metricsdb-dashboards").rstrip("/")
TOKEN_UI_PATH = os.getenv("TOKEN_UI_PATH", "/gd-cim-api/v1/token-ui")
GRAFANA_UPSTREAM = os.getenv("GRAFANA_UPSTREAM", "http://grafana:3000").rstrip("/")
AUTH_VERIFY_URL = os.getenv("AUTH_VERIFY_URL", "http://cim-fastapi:8000/v1/verify-token")
AUTH_TOKEN_URL = os.getenv("AUTH_TOKEN_URL", "http://cim-fastapi:8000/v1/token")
COOKIE_NAME = os.getenv("GRAFANA_AUTH_COOKIE_NAME", "gd_access_token")
COOKIE_SECURE = os.getenv("GRAFANA_AUTH_COOKIE_SECURE", "false").lower() == "true"

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


def _verify_user_email(token: str) -> str | None:
    try:
        resp = http.get(
            AUTH_VERIFY_URL,
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
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
        return str(payload["sub"]).strip().lower()
    return None


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


async def _forward(request: Request, user_email: str) -> Response:
    target = f"{GRAFANA_UPSTREAM}{request.url.path}"
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
    headers["X-WEBAUTH-USER"] = user_email
    headers["X-WEBAUTH-EMAIL"] = user_email

    body = await request.body()
    upstream = http.request(
        method=request.method,
        url=target,
        headers=headers,
        data=body if body else None,
        allow_redirects=False,
        timeout=120,
    )

    response_headers = {
        k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS
    }
    return Response(content=upstream.content, status_code=upstream.status_code, headers=response_headers)


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url=f"{GRAFANA_SUBPATH}/", status_code=307)


@app.api_route(LEGACY_DASH_PATH, methods=["GET", "HEAD"], include_in_schema=False)
@app.api_route(f"{LEGACY_DASH_PATH}/", methods=["GET", "HEAD"], include_in_schema=False)
@app.api_route(f"{LEGACY_DASH_PATH}" + "/{path:path}", methods=["GET", "HEAD"], include_in_schema=False)
def legacy_dash_redirect(request: Request, path: str = "") -> RedirectResponse:
    token = _extract_token(request)
    if token and _verify_user_email(token):
        return RedirectResponse(url=f"{GRAFANA_SUBPATH}/", status_code=307)
    return RedirectResponse(
        url=f"{TOKEN_UI_PATH}?next={quote(f'{GRAFANA_SUBPATH}/', safe='/%?=&')}",
        status_code=307,
    )


@app.get("/auth/login", response_class=HTMLResponse, include_in_schema=False)
@app.get(f"{GRAFANA_SUBPATH}/auth/login", response_class=HTMLResponse, include_in_schema=False)
def login_page(next: str = f"{GRAFANA_SUBPATH}/") -> str:
    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>GreenDIGIT Grafana Login</title>
  <style>
    body {{ font-family: Arial, sans-serif; max-width: 420px; margin: 40px auto; padding: 0 12px; }}
    input, button {{ width: 100%; padding: 10px; margin-top: 8px; }}
    button {{ cursor: pointer; }}
  </style>
</head>
<body>
  <h2>Sign in to GreenDIGIT Grafana</h2>
  <form method="post" action="{GRAFANA_SUBPATH}/auth/login">
    <input type="hidden" name="next" value="{next}">
    <label>Email</label>
    <input type="email" name="email" required>
    <label>Password</label>
    <input type="password" name="password" required>
    <button type="submit">Sign in</button>
  </form>
</body>
</html>
"""


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
    if not _verify_user_email(clean_token):
        return Response("Invalid token", status_code=401)
    return _issue_dashboard_session(clean_token, next)


@app.api_route(f"{GRAFANA_SUBPATH}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
@app.api_route(
    f"{GRAFANA_SUBPATH}" + "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
)
async def grafana_proxy(request: Request, path: str = "") -> Response:
    token = _extract_token(request)
    if not token:
        return _login_redirect(request)

    user_email = _verify_user_email(token)
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
    if token and _verify_user_email(token):
        return RedirectResponse(url=f"{GRAFANA_SUBPATH}/", status_code=307)
    return RedirectResponse(
        url=f"{TOKEN_UI_PATH}?next={quote(f'{GRAFANA_SUBPATH}/', safe='/%?=&')}",
        status_code=307,
    )
