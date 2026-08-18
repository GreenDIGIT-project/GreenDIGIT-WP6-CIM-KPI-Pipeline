#!/bin/bash
set -euo pipefail

usage() {
    echo "Usage:"
    echo "  $0 mark-reset <email>"
    echo "  $0 delete <email>"
    echo "  $0 set <email> <new_password>"
}

if [ "$#" -lt 2 ]; then
    usage
    exit 1
fi

MODE="$1"
EMAIL="$2"

case "$MODE" in
    mark-reset)
        docker compose exec cim-fastapi-a python user_service/reset_password_admin.py "$EMAIL" --mark-reset
        ;;
    delete)
        docker compose exec cim-fastapi-a python user_service/reset_password_admin.py "$EMAIL" --delete
        ;;
    set)
        if [ "$#" -ne 3 ]; then
            usage
            exit 1
        fi
        docker compose exec cim-fastapi-a python user_service/reset_password_admin.py "$EMAIL" --set "$3"
        ;;
    *)
        usage
        exit 1
        ;;
esac
