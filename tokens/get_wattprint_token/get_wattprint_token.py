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

email = os.environ.get("WATTPRINT_EMAIL")
password = os.environ.get("WATTPRINT_PASSWORD")
if not email or not password:
    raise SystemExit("WATTPRINT_EMAIL and WATTPRINT_PASSWORD must be set in .env")

base = os.environ.get("WATTPRINT_API_BASE", "https://api.wattprint.eu")
url = f"{base.rstrip('/')}/token-request/get_token"

r = requests.post(url, json={"email": email, "password": password}, timeout=10)
r.raise_for_status()
token = r.json()["access_token"]

# Write/replace WATTPRINT_TOKEN in the same .env
set_key(ENV_PATH, "WATTPRINT_TOKEN", token)
print("Updated WATTPRINT_TOKEN in", ENV_PATH)