import json
from pathlib import Path
from kiteconnect import KiteConnect
from dotenv import dotenv_values

BASE_DIR = Path(__file__).resolve().parent

def load_kite() -> KiteConnect:
    # Load API key/secret from .env
    vals = dotenv_values(BASE_DIR / ".env")
    api_key = vals.get("KITE_API_KEY")
    if not api_key:
        raise RuntimeError("KITE_API_KEY missing in .env")

    # Load access_token from kite_session.json
    session_path = BASE_DIR / "kite_session.json"
    if not session_path.exists():
        raise RuntimeError("kite_session.json not found. Run login_server.py and login once.")

    session = json.loads(session_path.read_text())
    access_token = session.get("access_token")
    if not access_token:
        raise RuntimeError("access_token missing in kite_session.json")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite