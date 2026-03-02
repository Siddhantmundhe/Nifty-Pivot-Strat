import json
from pathlib import Path

from dotenv import dotenv_values
from flask import Flask, request
from kiteconnect import KiteConnect


BROKER_DIR = Path(__file__).resolve().parent
ROOT_DIR = BROKER_DIR.parent
BROKER_ENV_PATH = BROKER_DIR / ".env"
ROOT_ENV_PATH = ROOT_DIR / ".env"

ENV_PATH = BROKER_ENV_PATH if BROKER_ENV_PATH.exists() else ROOT_ENV_PATH
vals = dotenv_values(ENV_PATH)

API_KEY = vals.get("KITE_API_KEY")
API_SECRET = vals.get("KITE_API_SECRET")

print("Loaded .env from:", ENV_PATH)
print("API key present:", bool(API_KEY))
print("API secret present:", bool(API_SECRET))

if not API_KEY or not API_SECRET:
    raise RuntimeError(f"Missing KITE_API_KEY or KITE_API_SECRET in {ENV_PATH}")

kite = KiteConnect(api_key=API_KEY)
app = Flask(__name__)


@app.get("/")
def home():
    return f"""
    <h3>Kite Connect Login</h3>
    <p>Redirect URL in Zerodha app must be exactly:</p>
    <pre>http://127.0.0.1:5000/callback</pre>
    <a href="{kite.login_url()}">Click here to login</a>
    """


@app.get("/callback")
def callback():
    request_token = request.args.get("request_token")
    if not request_token:
        return "No request_token found. Check Redirect URL in Zerodha app settings.", 400

    data = kite.generate_session(request_token, api_secret=API_SECRET)

    session_path = BROKER_DIR / "kite_session.json"
    token_path = BROKER_DIR / "access_token.txt"

    with open(session_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)

    with open(token_path, "w", encoding="utf-8") as f:
        f.write(str(data.get("access_token", "")).strip())

    return f"""
    <h3>Login success</h3>
    <p>Saved to <b>{session_path}</b></p>
    <p>Saved token to <b>{token_path}</b></p>
    <p><b>access_token</b>: {data.get("access_token")}</p>
    """


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
