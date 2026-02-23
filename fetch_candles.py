from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from kite_client import load_kite

OUT = Path(__file__).resolve().parent / "nifty_fut_5m.csv"

# Keep under Zerodha's max range for 5m candles
CHUNK_DAYS = 90
LOOKBACK_DAYS = 180   # change this to 60 / 90 / 180 as needed

def get_near_nifty_fut_token(instruments_csv: str = "instruments.csv") -> int:
    df = pd.read_csv(instruments_csv)
    fut = df[
        (df["exchange"] == "NFO") &
        (df["segment"] == "NFO-FUT") &
        (df["name"] == "NIFTY")
    ].copy()
    fut["expiry"] = pd.to_datetime(fut["expiry"], errors="coerce")
    fut = fut[fut["expiry"].dt.date >= datetime.now().date()].sort_values("expiry")
    if fut.empty:
        raise RuntimeError("No active NIFTY futures found.")
    return int(fut.iloc[0]["instrument_token"])

def fetch_historical_in_chunks(kite, token: int, from_dt: datetime, to_dt: datetime, interval="5minute"):
    all_rows = []
    chunk_start = from_dt

    while chunk_start < to_dt:
        chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), to_dt)

        print(f"Fetching chunk: {chunk_start}  ->  {chunk_end}")
        candles = kite.historical_data(
            instrument_token=token,
            from_date=chunk_start,
            to_date=chunk_end,
            interval=interval,
            continuous=False,
            oi=False,
        )

        if candles:
            all_rows.extend(candles)
            print(f"  received rows: {len(candles)}")
        else:
            print("  received rows: 0")

        # move forward a little to avoid overlap duplicates edge-case
        chunk_start = chunk_end + timedelta(minutes=1)

    return all_rows

def main():
    kite = load_kite()
    token = get_near_nifty_fut_token()

    to_dt = datetime.now()
    from_dt = to_dt - timedelta(days=LOOKBACK_DAYS)

    print(f"Fetching 5m candles for token {token} from {from_dt} to {to_dt} (chunked)")

    candles = fetch_historical_in_chunks(kite, token, from_dt, to_dt, interval="5minute")

    if not candles:
        raise RuntimeError("No candles returned.")

    df = pd.DataFrame(candles)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)

    df.to_csv(OUT, index=False)
    print(f"\nSaved {len(df)} rows to {OUT}")
    print(df.tail(5))

if __name__ == "__main__":
    main()