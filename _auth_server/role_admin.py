#!/usr/bin/env python3
"""Local, platform-administrator CLI for GreenDIGIT roles and groups."""
from __future__ import annotations
import argparse, sqlite3
from pathlib import Path
from auth_db import bootstrap as bootstrap_db, ensure_schema, normalise_group, normalise_role, snapshot_current_greendigit_cohort

def repo_root(): return Path(__file__).resolve().parent.parent
def default_db_path(): return repo_root() / "_auth_server" / "users.db"
def connect(path):
    conn = sqlite3.connect(str(path)); conn.row_factory = sqlite3.Row; ensure_schema(conn); return conn
def user_id(conn, email):
    row = conn.execute("SELECT id FROM users WHERE lower(email)=?", (email.strip().lower(),)).fetchone()
    if not row: raise SystemExit(f"User not found: {email}")
    return int(row[0])
def group_id(conn, name):
    try: name = normalise_group(name)
    except ValueError as exc: raise SystemExit(str(exc)) from exc
    row = conn.execute("SELECT id,name FROM groups WHERE name=?", (name,)).fetchone()
    if not row: raise SystemExit(f"Group not found: {name}")
    return int(row[0]), str(row[1])
def audit(conn, action, target=None, group=None, detail=None):
    conn.execute("INSERT INTO admin_audit(actor_email,action,target_email,group_name,outcome,detail) VALUES(?,?,?,?,?,?)", ("local-cli",action,target,group,"success",detail))
def role_change(conn, email, role, add):
    uid = user_id(conn,email)
    try: role = normalise_role(role)
    except ValueError as exc: raise SystemExit(str(exc)) from exc
    if add: conn.execute("INSERT OR IGNORE INTO user_roles(user_id,role) VALUES(?,?)", (uid,role))
    else: conn.execute("DELETE FROM user_roles WHERE user_id=? AND role=?", (uid,role))
    audit(conn, "role.add" if add else "role.remove", email.strip().lower(), detail=role); conn.commit()
def group_change(conn, name, email, op):
    gid, name = group_id(conn,name); uid = user_id(conn,email)
    if op == "add-user": conn.execute("INSERT OR IGNORE INTO user_groups(user_id,group_id) VALUES(?,?)",(uid,gid))
    elif op == "remove-user": conn.execute("DELETE FROM user_groups WHERE user_id=? AND group_id=?",(uid,gid))
    elif op == "promote-super": conn.execute("INSERT INTO user_groups(user_id,group_id,is_super) VALUES(?,?,1) ON CONFLICT(user_id,group_id) DO UPDATE SET is_super=1",(uid,gid))
    elif op == "demote-super": conn.execute("UPDATE user_groups SET is_super=0 WHERE user_id=? AND group_id=?",(uid,gid))
    audit(conn,f"group.{op}",email.strip().lower(),name); conn.commit()
def show_user(conn,email):
    uid=user_id(conn,email)
    roles=[r[0] for r in conn.execute("SELECT role FROM user_roles WHERE user_id=? ORDER BY role",(uid,))]
    groups=[f"{r[0]}{' (super)' if r[1] else ''}" for r in conn.execute("SELECT g.name,ug.is_super FROM user_groups ug JOIN groups g ON g.id=ug.group_id WHERE ug.user_id=? ORDER BY g.name",(uid,))]
    print(f"email: {email.strip().lower()}\nroles: {','.join(roles) or '-'}\ngroups: {','.join(groups) or '-'}")
def main():
    ap=argparse.ArgumentParser(description="Manage GreenDIGIT roles and private metric groups"); ap.add_argument("--db",type=Path,default=default_db_path()); top=ap.add_subparsers(dest="command",required=True)
    for c in ("add","remove"):
        p=top.add_parser(c); p.add_argument("email"); p.add_argument("role")
    p=top.add_parser("list"); p.add_argument("email",nargs="?")
    top.add_parser("bootstrap"); top.add_parser("bootstrap-greendigit"); top.add_parser("publish-emails")
    gp=top.add_parser("group").add_subparsers(dest="group_command",required=True)
    p=gp.add_parser("create"); p.add_argument("group"); p.add_argument("--display-name")
    gp.add_parser("list"); p=gp.add_parser("members"); p.add_argument("group")
    for c in ("add-user","remove-user","promote-super","demote-super"):
        p=gp.add_parser(c); p.add_argument("group"); p.add_argument("email")
    up=top.add_parser("user").add_subparsers(dest="user_command",required=True); p=up.add_parser("show"); p.add_argument("email")
    rp=top.add_parser("role").add_subparsers(dest="role_command",required=True)
    for c in ("add","remove"):
        p=rp.add_parser(c); p.add_argument("email"); p.add_argument("role")
    a=ap.parse_args()
    with connect(a.db) as conn:
        if a.command in {"add","remove"}: role_change(conn,a.email,a.role,a.command=="add")
        elif a.command=="role": role_change(conn,a.email,a.role,a.role_command=="add")
        elif a.command=="bootstrap": print("bootstrap complete: "+", ".join(f"{k}={v}" for k,v in bootstrap_db(conn,repo_root()).items()))
        elif a.command=="bootstrap-greendigit": print("GreenDIGIT cohort snapshot: "+", ".join(f"{k}={v}" for k,v in snapshot_current_greendigit_cohort(conn,repo_root()).items()))
        elif a.command=="publish-emails": print(",".join(r[0] for r in conn.execute("SELECT lower(u.email) FROM users u JOIN user_roles r ON r.user_id=u.id WHERE r.role='publish' ORDER BY 1")))
        elif a.command=="list":
            if a.email: show_user(conn,a.email)
            else:
                for r in conn.execute("SELECT email FROM users ORDER BY lower(email)"): show_user(conn,r[0])
        elif a.command=="user": show_user(conn,a.email)
        elif a.group_command=="create":
            try: name=normalise_group(a.group)
            except ValueError as exc: raise SystemExit(str(exc)) from exc
            conn.execute("INSERT OR IGNORE INTO groups(name,display_name) VALUES(?,?)",(name,a.display_name)); audit(conn,"group.create",group=name); conn.commit()
        elif a.group_command=="list":
            for r in conn.execute("SELECT name,COALESCE(display_name,'') FROM groups ORDER BY name"): print(f"{r[0]}{': '+r[1] if r[1] else ''}")
        elif a.group_command=="members":
            gid,_=group_id(conn,a.group)
            for r in conn.execute("SELECT u.email,ug.is_super FROM user_groups ug JOIN users u ON u.id=ug.user_id WHERE ug.group_id=? ORDER BY lower(u.email)",(gid,)): print(f"{r[0]}{' (super)' if r[1] else ''}")
        else: group_change(conn,a.group,a.email,a.group_command)
if __name__=="__main__": main()
