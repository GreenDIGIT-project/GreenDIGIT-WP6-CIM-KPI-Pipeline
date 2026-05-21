#!/bin/bash

# This script is necessary for the docker-compose as it generates the JWT token.
# Please write your `USER_EMAIL` and `USER_PASSWORD` into the .env file.

change_ownership_env() {
  USER="${USER:-$(id -un)}"
  echo "User is: $USER."
  sudo chown "$USER:$USER" .env
  chmod 600 .env
  echo "Ownership changed in .env file."
}

# Script for the .env
set -e
sudo apt install python3-venv -y
python3 -m venv . 2>/dev/null || true
. bin/activate

pip install -r requirements.txt

# No need to run this unless there are some problems with ownership; it shouldn't be the case :)
# change_ownership_env

./bin/python tokens/get_jwt_token/main.py
./bin/python tokens/get_wattnet_token/main.py || {
  echo "Warning: WATTNET_TOKEN refresh failed; continuing restart without refreshing it." >&2
}

change_ownership_env

# Rollout of the services
./scripts/restart_compose_rollout.sh
