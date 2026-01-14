#!/bin/bash
# Usage: ./reset_user_password.sh <email> <new_password>

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <email> <new_password>"
    exit 1
fi

EMAIL="$1"
NEW_PASSWORD="$2"


# Set a new password immediately
docker compose exec cim-fastapi python user_service/reset_password_admin.py $EMAIL --set $NEW_PASSWORD

# Delete user entry (next login will trigger first-login flow)
docker compose exec cim-fastapi python user_service/reset_password_admin.py $EMAIL --delete

# Mark user for reset (keeps row but forces password set at next login)
docker compose exec cim-fastapi python user_service/reset_password_admin.py $EMAIL --mark-reset