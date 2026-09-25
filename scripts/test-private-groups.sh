#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
TEMP_VENV=""
cleanup() {
  if [[ -n "$TEMP_VENV" && -d "$TEMP_VENV" ]]; then
    rm -rf -- "$TEMP_VENV"
  fi
}
trap cleanup EXIT

if ! "$PYTHON_BIN" -c 'import fastapi, sqlalchemy, jose, passlib, pymongo, requests, pytest, psycopg2' >/dev/null 2>&1; then
  TEMP_VENV="$(mktemp -d /tmp/greendigit-private-groups-tests.XXXXXX)"
  "$PYTHON_BIN" -m venv "$TEMP_VENV"
  "$TEMP_VENV/bin/pip" install --quiet \
    fastapi 'pydantic<3' uvicorn python-jose 'passlib[bcrypt]' bcrypt==4.0.1 \
    python-multipart sqlalchemy python-dotenv pymongo requests httpx pytest psycopg2-binary
  PYTHON_BIN="$TEMP_VENV/bin/python"
fi

"$PYTHON_BIN" -m pytest -q --disable-warnings tests
"$PYTHON_BIN" -m py_compile \
  _auth_server/*.py _grafana_auth_proxy/main.py _sql_cnr/*.py \
  migration/backfill_metric_groups.py scripts/batch_submit_cnr/process_dump.py \
  scripts/batch_submit_cnr/load_envelopes_direct_cnr.py

bash -n scripts/manage-user-role.sh scripts/bootstrap-user-roles.sh scripts/test-private-groups.sh
CNR_INTERNAL_TOKEN=test-only docker compose config --quiet >/dev/null
git diff --check

echo "Private-group checks passed. No production database was modified."
