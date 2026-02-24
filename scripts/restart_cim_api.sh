#!/usr/bin/env bash
set -euo pipefail

# Rolling restart for cim-fastapi-a/b with readiness waits between steps.
# This avoids restarting both backends before at least one is confirmed ready.

READINESS_PATH="${READINESS_PATH:-/v1/openapi.json}"
HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-180}"
HEALTH_INTERVAL_SECONDS="${HEALTH_INTERVAL_SECONDS:-2}"

wait_http_ready() {
    local name="$1"
    local port="$2"
    local deadline=$((SECONDS + HEALTH_TIMEOUT_SECONDS))
    local url="http://127.0.0.1:${port}${READINESS_PATH}"

    while (( SECONDS < deadline )); do
        if curl -fsS --max-time 2 "$url" >/dev/null; then
            echo "[ok] ${name} ready on ${url}"
            return 0
        fi
        sleep "${HEALTH_INTERVAL_SECONDS}"
    done

    echo "[error] ${name} did not become ready within ${HEALTH_TIMEOUT_SECONDS}s (${url})" >&2
    return 1
}

restart_and_wait() {
    local service="$1"
    local port="$2"
    echo "[info] restarting ${service}"
    docker compose down "${service}"
    docker compose up -d --build "${service}"
    wait_http_ready "${service}" "${port}"
}

echo "[info] checking peer backend before first restart"
wait_http_ready "cim-fastapi-b" 8001
restart_and_wait "cim-fastapi-a" 8000

echo "[info] checking peer backend before second restart"
wait_http_ready "cim-fastapi-a" 8000
restart_and_wait "cim-fastapi-b" 8001

echo "[info] rolling restart finished"
