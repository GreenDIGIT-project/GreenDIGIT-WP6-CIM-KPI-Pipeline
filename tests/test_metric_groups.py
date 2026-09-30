import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(ROOT / "_auth_server"))
from auth_db import bootstrap, ensure_schema, groups_for_user, normalise_group, snapshot_current_greendigit_cohort

spec = importlib.util.spec_from_file_location("backfill", ROOT / "migration/backfill_metric_groups.py")
backfill = importlib.util.module_from_spec(spec); spec.loader.exec_module(backfill)

class FakeCursor:
    def __init__(self, docs): self.docs=list(docs)
    def sort(self, key, direction):
        self.docs.sort(key=lambda doc: doc[key], reverse=direction < 0); return self
    def limit(self, size): self.docs=self.docs[:size]; return self
    def __iter__(self): return iter(self.docs)

class FakeCollection:
    def __init__(self, docs): self.docs=docs
    def find(self, query, *_args):
        before=None
        for clause in query.get("$and",[]):
            if "_id" in clause: before=clause["_id"]["$lt"]
        docs=[d for d in self.docs if not d.get("group") and (before is None or d["_id"] < before)]
        return FakeCursor(docs)
    def update_one(self, filt, update):
        doc=next(d for d in self.docs if d["_id"]==filt["_id"]); doc["group"]=update["$set"]["group"]
        doc.setdefault("body",{})["group"]=update["$set"]["body.group"]

class GroupTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.root=Path(self.tmp.name)
        self.db=sqlite3.connect(":memory:"); ensure_schema(self.db)
        self.db.executemany("INSERT INTO users(email,hashed_password) VALUES(?, 'x')", [("gd@example.org",),("plain@example.org",),("multi@example.org",)])
        self.db.execute("INSERT INTO user_roles(user_id,role) SELECT id,'publish' FROM users WHERE email='plain@example.org'"); self.db.commit()
        (self.root/"dashboards_emails.txt").write_text("GD@EXAMPLE.ORG\n",encoding="utf-8")
        (self.root/"submit_emails.txt").write_text("plain@example.org\n",encoding="utf-8")
    def tearDown(self): self.tmp.cleanup(); self.db.close()
    def test_schema_is_idempotent_and_defaults_exist(self):
        ensure_schema(self.db); ensure_schema(self.db)
        self.assertEqual([r[0] for r in self.db.execute("SELECT name FROM groups ORDER BY name")],["greendigit","public"])
    def test_group_normalisation_and_uniqueness(self):
        self.assertEqual(normalise_group(" GreenDigit "),"greendigit")
        with self.assertRaises(ValueError): normalise_group("bad group")
        self.db.execute("INSERT OR IGNORE INTO groups(name) VALUES(?)",(normalise_group("PUBLIC"),))
        self.assertEqual(self.db.execute("SELECT count(*) FROM groups WHERE name='public'").fetchone()[0],1)
    def test_bootstrap_is_idempotent_and_preserves_roles(self):
        snapshot=snapshot_current_greendigit_cohort(self.db,self.root)
        self.assertFalse(snapshot["already_snapshotted"])
        first=bootstrap(self.db,self.root); second=bootstrap(self.db,self.root)
        self.assertGreater(sum(first.values()),0); self.assertEqual(sum(second.values()),0)
        self.assertEqual(groups_for_user(self.db,"gd@example.org"),["greendigit"])
        self.assertEqual(groups_for_user(self.db,"plain@example.org"),["greendigit"])
        self.assertEqual(groups_for_user(self.db,"multi@example.org"),["public"])
        roles={r[0] for r in self.db.execute("SELECT role FROM user_roles r JOIN users u ON u.id=r.user_id WHERE u.email='plain@example.org'")}
        self.assertEqual(roles,{"publish"})
    def test_future_submit_allowlist_addition_is_not_grandfathered(self):
        snapshot_current_greendigit_cohort(self.db,self.root)
        self.db.execute("INSERT INTO users(email,hashed_password) VALUES('new@example.org','x')"); self.db.commit()
        (self.root/"submit_emails.txt").write_text("plain@example.org\nnew@example.org\n",encoding="utf-8")
        bootstrap(self.db,self.root)
        self.assertEqual(groups_for_user(self.db,"new@example.org"),["public"])
        self.assertEqual(snapshot_current_greendigit_cohort(self.db,self.root)["approvals_added"],0)
    def test_multi_group_classification_is_ambiguous(self):
        group,reason=backfill.classify("x@example.org",{"x@example.org":{"public","greendigit"}})
        self.assertIsNone(group); self.assertEqual(reason,"ambiguous_multi_group_publisher")
    def test_backfill_dry_run_and_repeat(self):
        docs=[{"_id":1,"publisher_email":"gd@example.org","body":{}},{"_id":2,"publisher_email":"other@example.org","body":{}},{"_id":3,"publisher_email":"multi@example.org","body":{}}]
        memberships={"gd@example.org":{"greendigit"},"multi@example.org":{"public","greendigit"}}
        coll=FakeCollection(docs); dry=backfill.backfill_mongo(coll,memberships,False,batch_size=2)
        self.assertNotIn("group",docs[0]); self.assertEqual(dry["ambiguous_multi_group_publisher"],1)
        applied=backfill.backfill_mongo(coll,memberships,True,batch_size=2); self.assertEqual(applied["updated"],2)
        self.assertEqual(docs[0]["group"],"greendigit"); self.assertEqual(docs[1]["group"],"public")
        self.assertEqual(backfill.backfill_mongo(coll,memberships,True)["updated"],0)
    def test_public_dashboard_sql_is_explicitly_public_only(self):
        sql=(ROOT/"_test_sql/public_dashboard_views.sql").read_text(encoding="utf-8")
        self.assertIn("m.group_name = 'public'",sql)

if __name__ == "__main__": unittest.main()
