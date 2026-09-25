import asyncio, importlib, importlib.util, os, sys, types
from pathlib import Path
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("sqlalchemy")
from fastapi.testclient import TestClient
from starlette.requests import Request
from jose import jwt

ROOT=Path(__file__).resolve().parents[1]

class Cursor(list):
    def sort(self,*_args): return self
class Collection:
    def __init__(self): self.docs=[]
    def insert_one(self,doc):
        saved=dict(doc); saved["_id"]=len(self.docs)+1; self.docs.append(saved); return types.SimpleNamespace(inserted_id=saved["_id"])
    def find(self,query,projection=None):
        def match(d):
            return all(d.get(k) in v["$in"] if isinstance(v,dict) and "$in" in v else d.get(k)==v for k,v in query.items())
        return Cursor([dict(d) for d in self.docs if match(d)])
    def count_documents(self,query): return len(self.find(query))
    def delete_many(self,query): return types.SimpleNamespace(deleted_count=0)

@pytest.fixture()
def api(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path); monkeypatch.setenv("JWT_GEN_SEED_TOKEN","test-secret")
    col=Collection(); metrics=types.ModuleType("metrics_store"); metrics._col=col; metrics.store_metric=lambda **kw:{"ok":True}
    monkeypatch.setitem(sys.modules,"metrics_store",metrics); monkeypatch.syspath_prepend(str(ROOT/"_auth_server"))
    sys.modules.pop("login_server",None); mod=importlib.import_module("login_server")
    with mod.SessionLocal() as db:
        for email in ("gd@example.org","public@example.org","both@example.org","outsider@example.org","super@example.org","admin@example.org"):
            db.add(mod.User(email=email,hashed_password="x"))
        db.commit()
        users={u.email:u for u in db.query(mod.User).all()}; groups={g.name:g for g in db.query(mod.Group).all()}
        for email,role in (("gd@example.org","publish"),("gd@example.org","dashboards_view"),("public@example.org","publish"),("public@example.org","dashboards_view"),("both@example.org","dashboards_view"),("super@example.org","dashboards_view"),("admin@example.org","admin")):
            db.add(mod.UserRole(user_id=users[email].id,role=role))
        for email,names in {"gd@example.org":["greendigit"],"public@example.org":["public"],"both@example.org":["public","greendigit"],"super@example.org":["greendigit"],"admin@example.org":["public"]}.items():
            for name in names: db.add(mod.UserGroup(user_id=users[email].id,group_id=groups[name].id,is_super=int(email=="super@example.org")))
        db.commit()
    def token(email):
        import time; now=int(time.time()); return jwt.encode({"sub":email,"iss":mod.JWT_ISSUER,"iat":now,"nbf":now,"exp":now+600},"test-secret",algorithm="HS256")
    return mod,TestClient(mod.app),col,token

def auth(token): return {"Authorization":f"Bearer {token}"}

def test_submission_requires_authorized_group_and_canonical_storage(api):
    mod,client,col,token=api
    legacy=client.post("/v1/submit",headers=auth(token("gd@example.org")),json={"value":1})
    assert legacy.status_code==200 and legacy.json()["group"]=="greendigit"
    assert client.post("/v1/submit",headers=auth(token("public@example.org")),json={"value":1}).status_code==400
    assert client.post("/v1/submit",headers=auth(token("gd@example.org")),json={"group":"public","value":1}).status_code==403
    response=client.post("/v1/submit",headers=auth(token("gd@example.org")),json={"group":" GreenDigit ","publisher_email":"forged@example.org","value":1})
    assert response.status_code==200 and response.json()["group"]=="greendigit"
    assert col.docs[-1]["publisher_email"]=="gd@example.org" and col.docs[-1]["group"]=="greendigit"
    assert "forged@example.org" not in str(col.docs[-1])

def test_dashboard_union_and_membership_revocation(api):
    mod,client,col,token=api
    col.insert_one({"publisher_email":"x","group":"greendigit","timestamp":"1","body":{}}); col.insert_one({"publisher_email":"x","group":"public","timestamp":"2","body":{}}); col.insert_one({"publisher_email":"x","group":"private","timestamp":"3","body":{}})
    assert client.get("/v1/cim-records",headers=auth(token("gd@example.org"))).json()["returned"]==1
    assert client.get("/v1/cim-records",headers=auth(token("both@example.org"))).json()["returned"]==2
    with mod.SessionLocal() as db:
        user=db.query(mod.User).filter_by(email="gd@example.org").first(); group=db.query(mod.Group).filter_by(name="greendigit").first(); db.query(mod.UserGroup).filter_by(user_id=user.id,group_id=group.id).delete(); db.commit()
    assert client.get("/v1/cim-records",headers=auth(token("gd@example.org"))).json()["returned"]==0

def test_group_super_boundaries_and_admin_operations(api):
    mod,client,_col,token=api
    h=auth(token("super@example.org"))
    assert client.put("/v1/admin/groups/greendigit/members",headers=h,json={"email":"outsider@example.org"}).status_code==200
    assert client.put("/v1/admin/groups/public/members",headers=h,json={"email":"outsider@example.org"}).status_code==403
    assert client.put("/v1/admin/roles",headers=h,json={"email":"outsider@example.org","role":"admin"}).status_code==403
    assert client.put("/v1/admin/groups/greendigit/super/outsider@example.org",headers=h).status_code==403
    admin=auth(token("admin@example.org"))
    assert client.put("/v1/admin/roles",headers=admin,json={"email":"outsider@example.org","role":"publish"}).status_code==200
    assert client.post("/v1/admin/groups",headers=admin,json={"name":"Partner_A"}).json()["group"]=="partner_a"

def test_request_does_not_grant_until_authorized_approval(api):
    mod,client,_col,token=api
    outsider=auth(token("outsider@example.org"))
    response=client.post("/v1/access-requests",headers=outsider,json={"request_type":"group","requested_value":"greendigit"})
    assert response.status_code==200
    with mod.SessionLocal() as db: assert mod.get_user_groups("outsider@example.org",db)==[]
    request_id=response.json()["request_id"]
    assert client.post(f"/v1/admin/access-requests/{request_id}",headers=auth(token("super@example.org")),json={"decision":"approved"}).status_code==200
    with mod.SessionLocal() as db: assert mod.get_user_groups("outsider@example.org",db)==["greendigit"]

def test_proxy_strips_forged_identity_and_group_headers(monkeypatch):
    monkeypatch.setenv("JWT_GEN_SEED_TOKEN","test-secret")
    spec=importlib.util.spec_from_file_location("gd_proxy",ROOT/"_grafana_auth_proxy/main.py")
    proxy=importlib.util.module_from_spec(spec); spec.loader.exec_module(proxy)
    captured={}
    class Response:
        content=b"ok"; status_code=200; headers={}
    def request(**kwargs): captured.update(kwargs); return Response()
    proxy.http=types.SimpleNamespace(request=request)
    async def receive(): return {"type":"http.request","body":b"","more_body":False}
    request_obj=Request({"type":"http","method":"GET","path":"/x","query_string":b"","headers":[
        (b"x-webauth-user",b"forged@example.org"),(b"x-authorized-groups",b"private"),(b"x-groups",b"private")
    ]},receive)
    asyncio.run(proxy._forward_to_upstream(request_obj,"http://upstream",user_email="real@example.org"))
    lowered={k.lower():v for k,v in captured["headers"].items()}
    assert lowered["x-webauth-user"]=="real@example.org"
    assert "x-authorized-groups" not in lowered and "x-groups" not in lowered

def test_sql_filter_is_mandatory_and_empty_membership_denies(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT/"_sql_cnr"))
    spec=importlib.util.spec_from_file_location("gd_sql",ROOT/"_sql_cnr/main.py")
    sql=importlib.util.module_from_spec(spec); spec.loader.exec_module(sql)
    where,params=sql._build_filters(None,site_id=None,vo=None,activity=None,start=None,end=None,groups=["public","greendigit"])
    assert "f.group_name = ANY(%s)" in where and params==[["public","greendigit"]]
    denied,_=sql._build_filters(None,site_id=None,vo=None,activity=None,start=None,end=None,groups=[])
    assert "FALSE" in denied
