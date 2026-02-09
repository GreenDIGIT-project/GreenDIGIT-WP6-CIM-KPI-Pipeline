#!/bin/bash

# This script is necessary for the docker-compose as it generates the JWT token.
# Please write your `USER_EMAIL` and `USER_PASSWORD` into the .env file.

change_ownership_env() {
  # export USER=$(id -un) # This is necessary for the crontab.
  USER="ubuntu" # This has to be changed on the deployment
  echo "User is: $USER."
  sudo chown -R "$USER:$USER" .
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

sudo ./bin/python tokens/get_jwt_token/main.py
sudo ./bin/python tokens/get_wattnet_token/main.py

change_ownership_env

docker compose down -v && docker compose up -d --force-recreate --no-deps


