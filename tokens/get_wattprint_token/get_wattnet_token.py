import os
import requests
from dotenv import load_dotenv, set_key

# script_dir = os.path.dirname(os.path.abspath(__file__))
cmd_pwd_dir = os.getcwd()

# Load existing .env (same folder by default)
env_local = os.path.join(cmd_pwd_dir, ".env")

# If not found, check parent directory
if os.path.isfile(env_local):
    ENV_PATH = env_local
else:
    ENV_PATH = os.path.join(os.path.dirname(cmd_pwd_dir), ".env")

load_dotenv(ENV_PATH)

email = os.environ.get("WATTNET_EMAIL")
password = os.environ.get("WATTNET_PASSWORD")
if not email or not password:
    raise SystemExit("WATTNET_EMAIL and WATTNET_PASSWORD must be set in .env")

base = os.environ.get("WATTNET_API_BASE", "https://api.wattnet.eu")
url = f"{base.rstrip('/')}/token-request/get_token"

r = requests.post(url, json={"email": email, "password": password}, timeout=10)
r.raise_for_status()
token = r.json()["access_token"]
print(f"token is {token}")

# Write/replace WATTNET in the same .env
set_key(ENV_PATH, "WATTNET_TOKEN", token)
print("Updated WATTNET in", ENV_PATH)