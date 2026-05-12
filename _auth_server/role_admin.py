#!/usr/bin/env python3
import argparse
import sqlite3
from pathlib import Path

VALID_ROLES = {"submit", "publish", "dashboards"}


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def default_db_path() -> Path:
    return repo_root() / "_auth_server" / "users.db"


def read_emails(path: Path) -> set[str]:
    if not path.exists():
        return set()
    emails = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip().lower()
        if line and not line.startswith("#"):
            emails.add(line)
    return emails


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    ensure_schema(conn)
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_roles (
            id INTEGER NOT NULL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            role VARCHAR NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE,
            CONSTRAINT uq_user_roles_user_id_role UNIQUE (user_id, role)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_user_roles_id ON user_roles (id)")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_user_roles_user_id ON user_roles (user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_user_roles_role ON user_roles (role)")
    conn.commit()


def user_id_for_email(conn: sqlite3.Connection, email: str) -> int:
    row = conn.execute("SELECT id FROM users WHERE lower(email) = ?", (email.lower(),)).fetchone()
    if not row:
        raise SystemExit(f"User not found in users.db: {email}")
    return int(row[0])


def validate_role(role: str) -> str:
    role = role.strip().lower()
    if role not in VALID_ROLES:
        raise SystemExit(f"Unknown role {role!r}. Valid roles: {', '.join(sorted(VALID_ROLES))}")
    return role


def add_role(conn: sqlite3.Connection, email: str, role: str) -> None:
    user_id = user_id_for_email(conn, email)
    role = validate_role(role)
    conn.execute(
        "INSERT OR IGNORE INTO user_roles (user_id, role) VALUES (?, ?)",
        (user_id, role),
    )
    conn.commit()


def remove_role(conn: sqlite3.Connection, email: str, role: str) -> None:
    user_id = user_id_for_email(conn, email)
    role = validate_role(role)
    conn.execute("DELETE FROM user_roles WHERE user_id = ? AND role = ?", (user_id, role))
    conn.commit()


def list_roles(conn: sqlite3.Connection, email: str | None) -> None:
    if email:
        rows = conn.execute(
            """
            SELECT users.email, user_roles.role
            FROM users
            LEFT JOIN user_roles ON user_roles.user_id = users.id
            WHERE lower(users.email) = ?
            ORDER BY user_roles.role
            """,
            (email.lower(),),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT users.email, user_roles.role
            FROM users
            LEFT JOIN user_roles ON user_roles.user_id = users.id
            ORDER BY users.email, user_roles.role
            """
        ).fetchall()

    current_email = None
    roles: list[str] = []
    for row_email, role in rows:
        if current_email is not None and row_email != current_email:
            print(f"{current_email}: {','.join(roles) if roles else '-'}")
            roles = []
        current_email = row_email
        if role:
            roles.append(role)
    if current_email is not None:
        print(f"{current_email}: {','.join(roles) if roles else '-'}")
    elif email:
        raise SystemExit(f"User not found in users.db: {email}")


def bootstrap(conn: sqlite3.Connection, root: Path) -> None:
    changed = 0
    assignments: dict[str, set[str]] = {}
    for email in read_emails(root / "allowed_emails.txt"):
        assignments.setdefault(email, set()).update({"submit", "dashboards"})
    for email in read_emails(root / "submit_emails.txt"):
        assignments.setdefault(email, set()).add("publish")

    for email, roles in sorted(assignments.items()):
        row = conn.execute("SELECT id FROM users WHERE lower(email) = ?", (email,)).fetchone()
        if not row:
            print(f"skip missing user: {email}")
            continue
        user_id = int(row[0])
        for role in sorted(roles):
            cur = conn.execute(
                "INSERT OR IGNORE INTO user_roles (user_id, role) VALUES (?, ?)",
                (user_id, role),
            )
            changed += cur.rowcount
    conn.commit()
    print(f"bootstrap complete: {changed} role assignment(s) added")


def publish_emails(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT lower(users.email)
        FROM users
        JOIN user_roles ON user_roles.user_id = users.id
        WHERE user_roles.role = 'publish'
        ORDER BY lower(users.email)
        """
    ).fetchall()
    for (email,) in rows:
        print(email)


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage GreenDIGIT auth user roles.")
    parser.add_argument("--db", type=Path, default=default_db_path(), help="Path to users.db")
    sub = parser.add_subparsers(dest="command", required=True)

    add_p = sub.add_parser("add", help="Grant a role to an existing user")
    add_p.add_argument("email")
    add_p.add_argument("role")

    remove_p = sub.add_parser("remove", help="Remove a role from a user")
    remove_p.add_argument("email")
    remove_p.add_argument("role")

    list_p = sub.add_parser("list", help="List roles")
    list_p.add_argument("email", nargs="?")

    sub.add_parser("bootstrap", help="Grant initial roles from allowed_emails.txt and submit_emails.txt")
    sub.add_parser("publish-emails", help="Print comma-separated publish-role emails")

    args = parser.parse_args()
    with connect(args.db) as conn:
        if args.command == "add":
            add_role(conn, args.email, args.role)
        elif args.command == "remove":
            remove_role(conn, args.email, args.role)
        elif args.command == "list":
            list_roles(conn, args.email)
        elif args.command == "bootstrap":
            bootstrap(conn, repo_root())
        elif args.command == "publish-emails":
            print(",".join(line for line in capture_publish_emails(conn)))


def capture_publish_emails(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT lower(users.email)
        FROM users
        JOIN user_roles ON user_roles.user_id = users.id
        WHERE user_roles.role = 'publish'
        ORDER BY lower(users.email)
        """
    ).fetchall()
    return [email for (email,) in rows]


if __name__ == "__main__":
    main()
