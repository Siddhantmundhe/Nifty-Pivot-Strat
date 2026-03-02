from pathlib import Path
from kiteconnect import KiteConnect
import os

proj_root = Path(__file__).resolve().parent.parent

API_KEY = "sudneg6y3zb2gxz7"   # <-- same app key
TOKEN_FILE = proj_root / "broker" / "access_token.txt"

def main():
    print("CWD:", os.getcwd())

    if not API_KEY:
        print("API key missing ❌")
        return
    print("API key present ✅")

    if not os.path.exists(TOKEN_FILE):
        print(f"Token file not found ❌ -> {TOKEN_FILE}")
        return

    with open(TOKEN_FILE, "r") as f:
        access_token = f.read().strip()

    if not access_token:
        print("Access token missing ❌")
        return
    print("Access token present ✅")

    # Debug
    print("DEBUG api_key repr:", repr(API_KEY))
    print("DEBUG token repr  :", repr(access_token))
    print("DEBUG token len   :", len(access_token))

    try:
        kite = KiteConnect(api_key=API_KEY)
        kite.set_access_token(access_token)

        profile = kite.profile()
        print("\n✅ Kite authentication successful")
        print("User ID   :", profile.get("user_id"))
        print("User Name :", profile.get("user_name"))
    except Exception as e:
        print("\n❌ Kite authentication failed")
        print("Reason:", str(e))
        print("\nCommon fixes:")
        print("1) Generate a fresh access token (Kite token expires daily)")
        print("2) Make sure API key matches the same app used for token generation")
        print("3) Remove extra spaces while copying token")

if __name__ == "__main__":
    main()