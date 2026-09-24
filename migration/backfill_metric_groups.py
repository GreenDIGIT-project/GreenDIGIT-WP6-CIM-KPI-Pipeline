#!/usr/bin/env python3
"""Dry-run-by-default, restartable group backfill for MongoDB and CNR PostgreSQL."""
from __future__ import annotations
import argparse, os, sqlite3
from collections import Counter
from pathlib import Path

def membership_map(db_path: Path) -> dict[str, set[str]]:
    with sqlite3.connect(db_path) as conn:
        rows=conn.execute("SELECT lower(u.email),g.name FROM users u JOIN user_groups ug ON ug.user_id=u.id JOIN groups g ON g.id=ug.group_id").fetchall()
    result: dict[str,set[str]]={}
    for email,group in rows: result.setdefault(email,set()).add(group)
    return result

def classify(email: str | None, memberships: dict[str,set[str]]) -> tuple[str | None,str]:
    groups=memberships.get((email or "").strip().lower(),set())
    if len(groups)>1: return None,"ambiguous_multi_group_publisher"
    if groups=={"greendigit"}: return "greendigit","greendigit_member"
    if not groups or groups=={"public"}: return "public","public_default"
    return None,"ambiguous_private_group"

def backfill_mongo(collection, memberships, apply=False):
    counts=Counter(); planned=[]
    for doc in collection.find({"$or":[{"group":{"$exists":False}},{"group":None},{"group":""}]},{"_id":1,"publisher_email":1}):
        group,reason=classify(doc.get("publisher_email"),memberships); counts[reason]+=1
        if group: planned.append((doc["_id"],group))
        else: print(f"AMBIGUOUS mongo _id={doc['_id']} publisher={doc.get('publisher_email')!r} reason={reason}")
    if apply:
        print("mongo pre-apply counts:",dict(counts))
        for doc_id,group in planned:
            collection.update_one({"_id":doc_id,"$or":[{"group":{"$exists":False}},{"group":None},{"group":""}]},{"$set":{"group":group,"body.group":group}})
            counts["updated"]+=1
    return counts

def main():
    root=Path(__file__).resolve().parent.parent
    ap=argparse.ArgumentParser(description=__doc__); ap.add_argument("--auth-db",type=Path,default=root/"_auth_server/users.db"); ap.add_argument("--apply",action="store_true"); ap.add_argument("--skip-mongo",action="store_true"); ap.add_argument("--skip-postgres",action="store_true"); a=ap.parse_args()
    memberships=membership_map(a.auth_db); mode="APPLY" if a.apply else "DRY RUN"; print(f"mode={mode}")
    if not a.skip_mongo:
        from pymongo import MongoClient
        client=MongoClient(os.getenv("MONGO_URI","mongodb://localhost:27017")); coll=client[os.getenv("MONGO_DB","metricsdb")][os.getenv("MONGO_COLLECTION","metrics")]
        print("mongo:",dict(backfill_mongo(coll,memberships,a.apply)))
    if not a.skip_postgres:
        import psycopg2
        conn=psycopg2.connect(host=os.environ["CNR_HOST"],port=os.getenv("CNR_POSTEGRESQL_PORT","5432"),user=os.environ["CNR_USER"],password=os.environ["CNR_POSTEGRESQL_PASSWORD"],dbname=os.environ["CNR_GD_DB"])
        counts=Counter()
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT event_id,publisher_email FROM monitoring.fact_site_event WHERE group_name IS NULL OR btrim(group_name)='' FOR UPDATE")
                planned=[]
                for event_id,email in cur.fetchall():
                    group,reason=classify(email,memberships); counts[reason]+=1
                    if group: planned.append((event_id,group))
                    else: print(f"AMBIGUOUS postgres event_id={event_id} publisher={email!r} reason={reason}")
                if a.apply:
                    print("postgres pre-apply counts:",dict(counts))
                    for event_id,group in planned:
                        cur.execute("UPDATE monitoring.fact_site_event SET group_name=%s WHERE event_id=%s AND (group_name IS NULL OR btrim(group_name)='')",(group,event_id)); counts["updated"]+=cur.rowcount
                if not a.apply: conn.rollback()
        conn.close(); print("postgres:",dict(counts))

if __name__=="__main__": main()
