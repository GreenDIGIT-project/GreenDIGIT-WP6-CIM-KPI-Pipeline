#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname "$0")/.." && pwd)"
DB_PATH="${USERS_DB_PATH:-"$ROOT_DIR/_auth_server/users.db"}"

exec python3 "$ROOT_DIR/_auth_server/role_admin.py" --db "$DB_PATH" bootstrap
