from __future__ import annotations

import json
from pathlib import Path

from dotenv import dotenv_values
from kiteconnect import KiteConnect


BROKER_DIR = Path(__file__).resolve().parent
ROOT_DIR = BROKER_DIR.parent
BROKER_ENV_PATH = BROKER_DIR / ".env"
ROOT_ENV_PATH = ROOT_DIR / ".env"
ENV_PATH = BROKER_ENV_PATH if BROKER_ENV_PATH.exists() else ROOT_ENV_PATH

vals = dotenv_values(ENV_PATH)
api_key = (vals.get("KITE_API_KEY") or "").strip()
api_secret = (vals.get("KITE_API_SECRET") or "").strip()

if not api_key or not api_secret:
    raise RuntimeError(f"Missing KITE_API_KEY or KITE_API_SECRET in {ENV_PATH}")

kite = KiteConnect(api_key=api_key)
token_file = BROKER_DIR / "access_token.txt"
session_file = BROKER_DIR / "kite_session.json"

print("Loaded .env from:", ENV_PATH)
print("API key prefix:", api_key[:4] + "..." if len(api_key) >= 4 else "(short)")
print("\nOpen this login URL in browser and complete login:")
print(kite.login_url())

request_token = input("\nPaste request_token here: ").strip()
if not request_token:
    raise RuntimeError("Empty request_token.")

data = kite.generate_session(request_token, api_secret=api_secret)
access_token = str(data["access_token"]).strip()

token_file.write_text(access_token, encoding="utf-8")
session_file.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")

print("\nAccess token generated successfully")
print("User ID   :", data.get("user_id"))
print("User Name :", data.get("user_name"))
print("Saved token to:", token_file)
print("Saved session to:", session_file)
