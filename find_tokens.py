from pathlib import Path
from datetime import date
import pandas as pd

CSV = Path(__file__).resolve().parent / "instruments.csv"

def round_to_50(x: float) -> int:
    return int(round(x / 50.0) * 50)

def normalize_strike_series(s: pd.Series) -> pd.Series:
    """
    Zerodha instrument dump sometimes stores strikes as:
    - normal (22500.0)
    - scaled (2250000.0)  -> divide by 100
    This function auto-detects and normalizes.
    """
    s = pd.to_numeric(s, errors="coerce")
    non_null = s.dropna()
    if non_null.empty:
        return s

    median_val = non_null.median()

    # If strike values look too large (e.g., 2250000 instead of 22500), scale down by 100
    if median_val > 100000:
        s = s / 100.0

    return s

def main():
    if not CSV.exists():
        raise RuntimeError("instruments.csv not found. Run download_instruments.py first.")

    df = pd.read_csv(CSV)

    # --- 1) NIFTY near-month FUT ---
    fut = df[
        (df["exchange"] == "NFO") &
        (df["segment"] == "NFO-FUT") &
        (df["name"] == "NIFTY")
    ].copy()

    fut["expiry"] = pd.to_datetime(fut["expiry"], errors="coerce").dt.date
    fut = fut[fut["expiry"] >= date.today()].sort_values("expiry")

    if fut.empty:
        raise RuntimeError("No active NIFTY futures found.")

    fut_row = fut.iloc[0]
    print("NIFTY FUT (near):", fut_row["tradingsymbol"], "| token:", int(fut_row["instrument_token"]), "| expiry:", fut_row["expiry"])

    # --- 2) NIFTY options nearest expiry ---
    opt = df[
        (df["exchange"] == "NFO") &
        (df["segment"] == "NFO-OPT") &
        (df["name"] == "NIFTY")
    ].copy()

    if opt.empty:
        raise RuntimeError("No NIFTY option rows found in instruments.csv")

    opt["expiry"] = pd.to_datetime(opt["expiry"], errors="coerce").dt.date
    opt = opt[opt["expiry"] >= date.today()]

    if opt.empty:
        raise RuntimeError("No active NIFTY options found.")

    # Normalize strike
    opt["strike_norm"] = normalize_strike_series(opt["strike"])

    # Debug print (important)
    print("Sample raw strikes:", sorted(opt["strike"].dropna().unique().tolist())[:5])
    print("Sample normalized strikes:", sorted(opt["strike_norm"].dropna().unique().tolist())[:5])

    nearest_expiry = opt["expiry"].min()
    opt = opt[opt["expiry"] == nearest_expiry].copy()

    print("Nearest NIFTY option expiry:", nearest_expiry)

    # --- 3) Ask spot and choose ATM ---
    spot_price = float(input("Enter current NIFTY spot (example 22510): ").strip())
    strike = round_to_50(spot_price)

    # Match using normalized strike
    ce = opt[(opt["strike_norm"] == strike) & (opt["instrument_type"] == "CE")]
    pe = opt[(opt["strike_norm"] == strike) & (opt["instrument_type"] == "PE")]

    if ce.empty or pe.empty:
        available = sorted(opt["strike_norm"].dropna().unique().tolist())
        # show nearest 15 strikes
        near = sorted(available, key=lambda x: abs(x - strike))[:15]
        print(f"Requested ATM strike: {strike}")
        print("Nearest available normalized strikes:", near)
        raise RuntimeError(f"ATM options not found for strike {strike} on expiry {nearest_expiry}")

    ce_row = ce.iloc[0]
    pe_row = pe.iloc[0]

    print("\nATM strike:", strike)
    print("CE:", ce_row["tradingsymbol"], "| token:", int(ce_row["instrument_token"]), "| raw_strike:", ce_row["strike"], "| norm_strike:", ce_row["strike_norm"])
    print("PE:", pe_row["tradingsymbol"], "| token:", int(pe_row["instrument_token"]), "| raw_strike:", pe_row["strike"], "| norm_strike:", pe_row["strike_norm"])

if __name__ == "__main__":
    main()