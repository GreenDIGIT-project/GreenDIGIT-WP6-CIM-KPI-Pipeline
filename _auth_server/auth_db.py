"""Shared SQLite schema and helpers for GreenDIGIT roles and metric groups."""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

VALID_ROLES = {"admin", "dashboards_view", "publish"}
DEFAULT_GROUP = "public"
GREENDIGIT_GROUP = "greendigit"
OIDC_PASSWORD_DISABLED = "!OIDC_ONLY!"
_GROUP_RE = re.compile(r"^[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?$")


def normalise_email(value: str) -> str:
    email = (value or "").strip().lower()
    if not email or "@" not in email:
        raise ValueError("A valid email address is required")
    return email


def normalise_group(value: str) -> str:
    name = (value or "").strip().lower()
    if not _GROUP_RE.fullmatch(name):
        raise ValueError(
            "Group names must be 1-64 lowercase letters, numbers, dots, underscores or hyphens"
        )
    return name


def normalise_role(value: str) -> str:
    role = (value or "").strip().lower()
    if role not in VALID_ROLES:
        raise ValueError(f"Unknown role {role!r}; valid roles: {', '.join(sorted(VALID_ROLES))}")
    return role


def read_emails(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        line.strip().lower()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create all additive auth structures. Safe to run repeatedly."""
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER NOT NULL PRIMARY KEY,
            email VARCHAR NOT NULL UNIQUE,
            hashed_password VARCHAR NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_users_email ON users (email);

        CREATE TABLE IF NOT EXISTS user_roles (
            id INTEGER NOT NULL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            role VARCHAR NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE,
            CONSTRAINT uq_user_roles_user_id_role UNIQUE (user_id, role)
        );
        CREATE INDEX IF NOT EXISTS ix_user_roles_id ON user_roles (id);
        CREATE INDEX IF NOT EXISTS ix_user_roles_user_id ON user_roles (user_id);
        CREATE INDEX IF NOT EXISTS ix_user_roles_role ON user_roles (role);

        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER NOT NULL PRIMARY KEY,
            name VARCHAR NOT NULL COLLATE NOCASE,
            display_name VARCHAR,
            created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_groups_name UNIQUE (name)
        );
        CREATE INDEX IF NOT EXISTS ix_groups_name ON groups (name);

        CREATE TABLE IF NOT EXISTS user_groups (
            user_id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            is_super INTEGER NOT NULL DEFAULT 0 CHECK (is_super IN (0, 1)),
            source VARCHAR NOT NULL DEFAULT 'manual',
            created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, group_id),
            FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE,
            FOREIGN KEY(group_id) REFERENCES groups (id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_user_groups_group_id ON user_groups (group_id);

        CREATE TABLE IF NOT EXISTS external_identities (
            id INTEGER NOT NULL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            issuer VARCHAR NOT NULL,
            subject VARCHAR NOT NULL,
            current_verified_email VARCHAR NOT NULL,
            created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE,
            CONSTRAINT uq_external_identities_issuer_subject UNIQUE (issuer, subject)
        );
        CREATE INDEX IF NOT EXISTS ix_external_identities_user_id
          ON external_identities (user_id);

        CREATE TABLE IF NOT EXISTS group_email_approvals (
            email VARCHAR NOT NULL COLLATE NOCASE,
            group_id INTEGER NOT NULL,
            created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (email, group_id),
            FOREIGN KEY(group_id) REFERENCES groups (id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS migration_state (
            key VARCHAR NOT NULL PRIMARY KEY,
            completed_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS access_requests (
            id INTEGER NOT NULL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            request_type VARCHAR NOT NULL CHECK (request_type IN ('role', 'group')),
            requested_value VARCHAR NOT NULL,
            status VARCHAR NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'approved', 'rejected')),
            created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
            decided_at VARCHAR,
            decided_by_user_id INTEGER,
            FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE,
            FOREIGN KEY(decided_by_user_id) REFERENCES users (id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS admin_audit (
            id INTEGER NOT NULL PRIMARY KEY,
            actor_email VARCHAR NOT NULL,
            action VARCHAR NOT NULL,
            target_email VARCHAR,
            group_name VARCHAR,
            outcome VARCHAR NOT NULL,
            detail VARCHAR,
            created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS ix_admin_audit_created_at ON admin_audit (created_at);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_access_request_pending
          ON access_requests(user_id, request_type, requested_value)
          WHERE status = 'pending';
        """
    )
    # CREATE TABLE IF NOT EXISTS does not add columns to an existing table.
    user_group_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(user_groups)").fetchall()
    }
    if "source" not in user_group_columns:
        conn.execute(
            "ALTER TABLE user_groups ADD COLUMN source VARCHAR NOT NULL DEFAULT 'manual'"
        )
    conn.execute(
        """INSERT OR IGNORE INTO user_roles (user_id, role)
           SELECT user_id, 'publish' FROM user_roles WHERE role = 'submit'"""
    )
    conn.execute(
        """INSERT OR IGNORE INTO user_roles (user_id, role)
           SELECT user_id, 'dashboards_view' FROM user_roles WHERE role = 'dashboards'"""
    )
    conn.execute("DELETE FROM user_roles WHERE role IN ('submit', 'dashboards')")
    conn.execute("UPDATE groups SET name = lower(trim(name))")
    for name, display in ((DEFAULT_GROUP, "Public"), (GREENDIGIT_GROUP, "GreenDIGIT")):
        conn.execute(
            "INSERT OR IGNORE INTO groups (name, display_name) VALUES (?, ?)",
            (name, display),
        )
    conn.commit()


def resolve_external_identity(
    conn: sqlite3.Connection,
    *,
    issuer: str,
    subject: str,
    verified_email: str,
    mapped_groups: set[str] | None = None,
) -> dict[str, object]:
    """Resolve an OIDC identity without granting roles.

    A verified email may safely link a first login to an existing local user.
    Subsequent logins are resolved only through the stable issuer/subject pair.
    Group mappings are additive in this iteration; automatic removal is deferred
    so manually assigned memberships can never be removed accidentally.
    """
    ensure_schema(conn)
    canonical_issuer = (issuer or "").strip().rstrip("/")
    canonical_subject = (subject or "").strip()
    email = normalise_email(verified_email)
    if not canonical_issuer or not canonical_subject:
        raise ValueError("OIDC issuer and subject are required")

    requested_groups = {normalise_group(name) for name in (mapped_groups or set())}
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("BEGIN IMMEDIATE")
    try:
        identity = conn.execute(
            """SELECT user_id FROM external_identities
               WHERE issuer = ? AND subject = ?""",
            (canonical_issuer, canonical_subject),
        ).fetchone()
        created_user = False
        linked_existing = False
        if identity:
            user_id = int(identity[0])
            conn.execute(
                """UPDATE external_identities
                   SET current_verified_email = ?, updated_at = ?
                   WHERE issuer = ? AND subject = ?""",
                (email, now, canonical_issuer, canonical_subject),
            )
        else:
            user = conn.execute(
                "SELECT id FROM users WHERE lower(email) = ?", (email,)
            ).fetchone()
            if user:
                user_id = int(user[0])
                linked_existing = True
            else:
                cursor = conn.execute(
                    "INSERT INTO users(email, hashed_password) VALUES (?, ?)",
                    (email, OIDC_PASSWORD_DISABLED),
                )
                user_id = int(cursor.lastrowid)
                created_user = True
            conn.execute(
                """INSERT INTO external_identities
                   (user_id, issuer, subject, current_verified_email, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (user_id, canonical_issuer, canonical_subject, email, now, now),
            )

        # The local account email remains stable. A changed verified OIDC email
        # is retained on external_identities for administrator review.
        local_email = str(
            conn.execute("SELECT email FROM users WHERE id = ?", (user_id,)).fetchone()[0]
        ).strip().lower()

        group_rows = conn.execute(
            "SELECT id, name FROM groups WHERE name IN ({})".format(
                ",".join("?" for _ in requested_groups) or "NULL"
            ),
            tuple(sorted(requested_groups)),
        ).fetchall()
        found_groups = {str(row[1]) for row in group_rows}
        if found_groups != requested_groups:
            raise ValueError("An OIDC mapping references an unknown local group")
        for group_id, _name in group_rows:
            conn.execute(
                """INSERT OR IGNORE INTO user_groups(user_id, group_id, is_super, source)
                   VALUES (?, ?, 0, 'egi')""",
                (user_id, int(group_id)),
            )

        if not conn.execute(
            "SELECT 1 FROM user_groups WHERE user_id = ?", (user_id,)
        ).fetchone():
            public_id = conn.execute(
                "SELECT id FROM groups WHERE name = ?", (DEFAULT_GROUP,)
            ).fetchone()[0]
            conn.execute(
                """INSERT INTO user_groups(user_id, group_id, is_super, source)
                   VALUES (?, ?, 0, 'egi')""",
                (user_id, int(public_id)),
            )

        roles = [
            str(row[0])
            for row in conn.execute(
                "SELECT role FROM user_roles WHERE user_id = ? ORDER BY role", (user_id,)
            )
        ]
        groups = [
            str(row[0])
            for row in conn.execute(
                """SELECT g.name FROM groups g
                   JOIN user_groups ug ON ug.group_id = g.id
                   WHERE ug.user_id = ? ORDER BY g.name""",
                (user_id,),
            )
        ]
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "user_id": user_id,
        "email": local_email,
        "roles": roles,
        "groups": groups,
        "created_user": created_user,
        "linked_existing": linked_existing,
    }


def bootstrap(conn: sqlite3.Connection, root: Path) -> dict[str, int]:
    """Add configured roles/groups without removing any existing assignment."""
    ensure_schema(conn)
    counts = {"roles_added": 0, "greendigit_added": 0, "public_added": 0}
    dashboard_emails = read_emails(root / "dashboards_emails.txt")
    submit_emails = read_emails(root / "submit_emails.txt")
    public_id = conn.execute("SELECT id FROM groups WHERE name = ?", (DEFAULT_GROUP,)).fetchone()[0]

    for email, role in ((e, "dashboards_view") for e in dashboard_emails):
        row = conn.execute("SELECT id FROM users WHERE lower(email) = ?", (email,)).fetchone()
        if row:
            counts["roles_added"] += conn.execute(
                "INSERT OR IGNORE INTO user_roles (user_id, role) VALUES (?, ?)", (row[0], role)
            ).rowcount
    for email in submit_emails:
        row = conn.execute("SELECT id FROM users WHERE lower(email) = ?", (email,)).fetchone()
        if row:
            counts["roles_added"] += conn.execute(
                "INSERT OR IGNORE INTO user_roles (user_id, role) VALUES (?, 'publish')", (row[0],)
            ).rowcount

    # Apply only persisted, previously-approved group memberships. The live
    # role allowlists are deliberately not treated as group authorization.
    counts["greendigit_added"] += conn.execute(
        """INSERT OR IGNORE INTO user_groups (user_id, group_id, is_super, source)
           SELECT u.id, a.group_id, 0, 'bootstrap'
           FROM group_email_approvals a
           JOIN users u ON lower(u.email) = lower(a.email)"""
    ).rowcount

    counts["public_added"] += conn.execute(
        """INSERT OR IGNORE INTO user_groups (user_id, group_id, is_super, source)
           SELECT u.id, ?, 0, 'bootstrap' FROM users u
           WHERE NOT EXISTS (SELECT 1 FROM user_groups ug WHERE ug.user_id = u.id)""",
        (public_id,),
    ).rowcount
    conn.commit()
    return counts


def snapshot_current_greendigit_cohort(conn: sqlite3.Connection, root: Path) -> dict[str, int | bool]:
    """One-time snapshot of today's allowlists as the legacy GreenDIGIT cohort.

    Later changes to either role allowlist are intentionally ignored. The
    persisted approvals also cover cohort members who register after snapshot.
    """
    ensure_schema(conn)
    marker = "legacy_greendigit_cohort_v1"
    if conn.execute("SELECT 1 FROM migration_state WHERE key = ?", (marker,)).fetchone():
        return {"already_snapshotted": True, "approvals_added": 0, "memberships_added": 0}
    emails = read_emails(root / "dashboards_emails.txt") | read_emails(root / "submit_emails.txt")
    gd_id = conn.execute("SELECT id FROM groups WHERE name = ?", (GREENDIGIT_GROUP,)).fetchone()[0]
    approvals = 0
    for email in sorted(emails):
        approvals += conn.execute(
            "INSERT OR IGNORE INTO group_email_approvals(email, group_id) VALUES (?, ?)",
            (email, gd_id),
        ).rowcount
    memberships = conn.execute(
        """INSERT OR IGNORE INTO user_groups(user_id, group_id, is_super, source)
           SELECT u.id, a.group_id, 0, 'bootstrap' FROM group_email_approvals a
           JOIN users u ON lower(u.email) = lower(a.email)
           WHERE a.group_id = ?""",
        (gd_id,),
    ).rowcount
    conn.execute("INSERT INTO migration_state(key) VALUES (?)", (marker,))
    conn.commit()
    return {"already_snapshotted": False, "approvals_added": approvals, "memberships_added": memberships}


def groups_for_user(conn: sqlite3.Connection, email: str) -> list[str]:
    return [
        row[0]
        for row in conn.execute(
            """SELECT g.name FROM groups g
               JOIN user_groups ug ON ug.group_id = g.id
               JOIN users u ON u.id = ug.user_id
               WHERE lower(u.email) = ? ORDER BY g.name""",
            (email.strip().lower(),),
        )
    ]
