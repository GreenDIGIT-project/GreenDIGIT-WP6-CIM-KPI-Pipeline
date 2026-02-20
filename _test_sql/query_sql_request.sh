#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

set -a
source "$ROOT_DIR/.env"
set +a

export PGPASSWORD="${CNR_POSTEGRESQL_PASSWORD}"

CONNINFO="host=${CNR_HOST} port=${CNR_POSTEGRESQL_PORT:-5432} dbname=${CNR_GD_DB} user=${CNR_USER} sslmode=${CNR_POSTEGRESQL_SSLMODE:-disable}"

echo "Running pgbench against ${CNR_HOST}:${CNR_POSTEGRESQL_PORT:-5432}/${CNR_GD_DB} as ${CNR_USER}"

pgbench "$CONNINFO" -n -f "$SCRIPT_DIR/query.sql" -T 30 -c 1 -j 1
pgbench "$CONNINFO" -n -f "$SCRIPT_DIR/query.sql" -T 30 -c 10 -j 10
pgbench "$CONNINFO" -n -f "$SCRIPT_DIR/query.sql" -T 30 -c 20 -j 20
