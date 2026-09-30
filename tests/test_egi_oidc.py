import base64
import importlib.util
import json
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jose")
pytest.importorskip("cryptography")
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from jose import JWTError, jwt

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_auth_server"))
from auth_db import ensure_schema, resolve_external_identity


def _b64int(value: int) -> str:
    raw = value.to_bytes((value.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _keypair(kid="test-key"):
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    numbers = private.public_key().public_numbers()
    public_jwk = {
        "kty": "RSA",
        "use": "sig",
        "alg": "RS256",
        "kid": kid,
        "n": _b64int(numbers.n),
        "e": _b64int(numbers.e),
    }
    return private, public_jwk


def _load_proxy(monkeypatch, tmp_path, **env):
    defaults = {
        "EGI_OIDC_ISSUER": "https://issuer.example",
        "EGI_OIDC_CLIENT_ID": "client-id",
        "EGI_OIDC_CLIENT_SECRET": "client-secret",
        "EGI_OIDC_REDIRECT_URI": "https://service.example/auth/callback",
        "EGI_REQUIRED_ENTITLEMENT": "",
        "EGI_REQUIRED_GROUP": "",
        "EGI_GROUP_CLAIM": "",
        "EGI_GROUP_MAPPINGS": "",
        "AUTH_DB_PATH": str(tmp_path / "users.db"),
        "JWT_GEN_SEED_TOKEN": "local-session-secret",
        "GRAFANA_AUTH_COOKIE_SECURE": "true",
    }
    defaults.update(env)
    for name, value in defaults.items():
        monkeypatch.setenv(name, value)
    spec = importlib.util.spec_from_file_location(
        f"egi_proxy_{time.time_ns()}", ROOT / "_grafana_auth_proxy/main.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with sqlite3.connect(defaults["AUTH_DB_PATH"]) as conn:
        ensure_schema(conn)
    return module


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_external_identity_uses_issuer_subject_and_defaults_to_public():
    conn = sqlite3.connect(":memory:")
    ensure_schema(conn)
    first = resolve_external_identity(
        conn,
        issuer="https://issuer.example/",
        subject="stable-subject",
        verified_email="first@example.org",
    )
    second = resolve_external_identity(
        conn,
        issuer="https://issuer.example",
        subject="stable-subject",
        verified_email="changed@example.org",
    )
    assert first["user_id"] == second["user_id"]
    assert first["groups"] == ["public"]
    assert first["roles"] == []
    assert second["email"] == "first@example.org"
    assert conn.execute("SELECT current_verified_email FROM external_identities").fetchone()[0] == "changed@example.org"


def test_confirmed_mapping_adds_group_but_never_roles():
    conn = sqlite3.connect(":memory:")
    ensure_schema(conn)
    identity = resolve_external_identity(
        conn,
        issuer="https://issuer.example",
        subject="subject",
        verified_email="member@example.org",
        mapped_groups={"greendigit"},
    )
    assert identity["groups"] == ["greendigit"]
    assert identity["roles"] == []
    assert conn.execute(
        "SELECT source FROM user_groups WHERE user_id = ?", (identity["user_id"],)
    ).fetchone()[0] == "egi"


def test_mapping_is_disabled_until_both_configuration_values_exist(monkeypatch, tmp_path):
    proxy = _load_proxy(monkeypatch, tmp_path, EGI_GROUP_CLAIM="eduperson_entitlement")
    with pytest.raises(ValueError):
        proxy._mapped_local_groups([{"eduperson_entitlement": ["example"]}])


def _install_oidc_responses(proxy, *, nonce, private_key, public_jwk, email="user@example.org"):
    metadata = {
        "issuer": proxy.EGI_OIDC_ISSUER,
        "authorization_endpoint": "https://issuer.example/authorize",
        "token_endpoint": "https://issuer.example/token",
        "jwks_uri": "https://issuer.example/jwks",
        "userinfo_endpoint": "https://issuer.example/userinfo",
        "id_token_signing_alg_values_supported": ["RS256"],
    }
    now = int(time.time())
    claims = {
        "iss": proxy.EGI_OIDC_ISSUER,
        "sub": "egi-subject",
        "aud": proxy.EGI_OIDC_CLIENT_ID,
        "iat": now,
        "exp": now + 300,
        "nonce": nonce,
        "email": email,
        "email_verified": True,
    }
    id_token = jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": public_jwk["kid"]})

    def get(url, **_kwargs):
        if url.endswith("openid-configuration"):
            return FakeResponse(metadata)
        if url.endswith("/jwks"):
            return FakeResponse({"keys": [public_jwk]})
        if url.endswith("/userinfo"):
            return FakeResponse({"sub": "egi-subject", "email": email, "email_verified": True})
        raise AssertionError(url)

    def post(url, **kwargs):
        assert url.endswith("/token")
        assert kwargs["data"]["code_verifier"]
        assert "client_secret" not in kwargs["data"]
        assert kwargs["auth"] == ("client-id", "client-secret")
        return FakeResponse({"access_token": "opaque-access-token", "id_token": id_token})

    proxy.http.get = get
    proxy.http.post = post


def test_oidc_login_uses_pkce_nonce_and_creates_public_identity(monkeypatch, tmp_path):
    proxy = _load_proxy(monkeypatch, tmp_path)
    private, public_jwk = _keypair()
    _install_oidc_responses(proxy, nonce="not-used-yet", private_key=private, public_jwk=public_jwk)
    client = TestClient(proxy.app, base_url="https://testserver")
    start = client.get("/auth/login", follow_redirects=False)
    assert start.status_code == 303
    query = parse_qs(urlparse(start.headers["location"]).query)
    assert query["code_challenge_method"] == ["S256"]
    assert query["nonce"] and query["state"] and query["code_challenge"]
    _install_oidc_responses(proxy, nonce=query["nonce"][0], private_key=private, public_jwk=public_jwk)
    callback = client.get(
        "/auth/callback",
        params={"code": "one-time-code", "state": query["state"][0]},
        follow_redirects=False,
    )
    assert callback.status_code == 303
    assert "reason=missing_dashboard_role" in callback.headers["location"]
    assert "gd_access_token=" in callback.headers.get("set-cookie", "")
    with sqlite3.connect(proxy.AUTH_DB_PATH) as conn:
        assert conn.execute("SELECT issuer, subject FROM external_identities").fetchone() == (
            "https://issuer.example",
            "egi-subject",
        )
        assert conn.execute(
            "SELECT g.name FROM user_groups ug JOIN groups g ON g.id=ug.group_id"
        ).fetchall() == [("public",)]
        assert conn.execute("SELECT count(*) FROM user_roles").fetchone()[0] == 0


def test_oidc_callback_rejects_nonce_mismatch_before_identity_creation(monkeypatch, tmp_path):
    proxy = _load_proxy(monkeypatch, tmp_path)
    private, public_jwk = _keypair()
    _install_oidc_responses(proxy, nonce="not-used-yet", private_key=private, public_jwk=public_jwk)
    client = TestClient(proxy.app, base_url="https://testserver")
    start = client.get("/auth/login", follow_redirects=False)
    query = parse_qs(urlparse(start.headers["location"]).query)
    _install_oidc_responses(proxy, nonce="wrong-nonce", private_key=private, public_jwk=public_jwk)
    callback = client.get(
        "/auth/callback",
        params={"code": "one-time-code", "state": query["state"][0]},
        follow_redirects=False,
    )
    assert callback.status_code == 401
    with sqlite3.connect(proxy.AUTH_DB_PATH) as conn:
        assert conn.execute("SELECT count(*) FROM external_identities").fetchone()[0] == 0


def test_mapping_claim_cannot_grant_dashboard_role(monkeypatch, tmp_path):
    external = "urn:mace:egi.eu:group:vo.example:role=member#aai.egi.eu"
    proxy = _load_proxy(
        monkeypatch,
        tmp_path,
        EGI_GROUP_CLAIM="eduperson_entitlement",
        EGI_GROUP_MAPPINGS=json.dumps({external: "greendigit"}),
    )
    assert proxy._mapped_local_groups([{"eduperson_entitlement": [external]}]) == {"greendigit"}
    with sqlite3.connect(proxy.AUTH_DB_PATH) as conn:
        identity = resolve_external_identity(
            conn,
            issuer=proxy.EGI_OIDC_ISSUER,
            subject="subject",
            verified_email="mapped@example.org",
            mapped_groups={"greendigit"},
        )
    assert identity["groups"] == ["greendigit"]
    assert identity["roles"] == []


@pytest.mark.parametrize("failure", ["audience", "signature"])
def test_id_token_rejects_invalid_audience_or_signature(monkeypatch, tmp_path, failure):
    proxy = _load_proxy(monkeypatch, tmp_path)
    trusted_private, trusted_public = _keypair("trusted")
    signing_key = trusted_private
    if failure == "signature":
        signing_key, _ = _keypair("untrusted")
    now = int(time.time())
    claims = {
        "iss": proxy.EGI_OIDC_ISSUER,
        "sub": "subject",
        "aud": "wrong-client" if failure == "audience" else proxy.EGI_OIDC_CLIENT_ID,
        "iat": now,
        "exp": now + 300,
        "nonce": "expected-nonce",
    }
    token = jwt.encode(claims, signing_key, algorithm="RS256", headers={"kid": "trusted"})
    metadata = {
        "issuer": proxy.EGI_OIDC_ISSUER,
        "jwks_uri": "https://issuer.example/jwks",
        "id_token_signing_alg_values_supported": ["RS256"],
    }
    proxy._oidc_jwks = ({"keys": [trusted_public]}, time.time() + 60)
    with pytest.raises(JWTError):
        proxy._validate_id_token(
            token,
            metadata=metadata,
            nonce="expected-nonce",
            access_token="opaque",
        )
