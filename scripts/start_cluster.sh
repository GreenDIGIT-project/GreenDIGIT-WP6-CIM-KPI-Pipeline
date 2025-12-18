#!/bin/bash

# This script is necessary for the docker-compose as it generates the JWT token.
# Please write your `USER_EMAIL` and `USER_PASSWORD` into the .env file.

# Script for the .env
set -e
sudo apt install python3-venv -y
python3 -m venv . 2>/dev/null || true
. bin/activate

pip install -r requirements.txt

sudo ./bin/python tokens/get_bearer_token/get_bearer_token.py
sudo ./bin/python tokens/get_wattprint_token/get_wattnet_token.py

export USER=$(id -un) # This is necessary for the crontab.
echo "User is: $USER."
sudo chown -R $USER:$USER .
echo "Ownership changed in .env file."

# Reset ownership so future non-root runs can read freshly written secrets
# if [ -f .env ]; then
#   OWNER="${SUDO_USER:-$USER}"
#   if [ -n "$OWNER" ]; then
#     GROUP="$(id -gn "$OWNER")"
#     sudo chown "$OWNER:$GROUP" .env
#   fi
# fi

sudo docker compose up -d --force-recreate --no-deps
