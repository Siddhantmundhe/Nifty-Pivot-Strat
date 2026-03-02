# paper_option_backtest_scaleout.py
from __future__ import annotations

import os
import sys
import inspect
import argparse
from pathlib import Path
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional, Tuple

# Add project root to sys.path so 'core' module can be found
proj_root = Path(__file__).resolve().parent.parent
if str(proj_root) not in sys.path:
    sys.path.insert(0, str(proj_root))

from dotenv import load_dotenv
load_dotenv(proj_root / ".env")

import pandas as pd

try:
    from kiteconnect import KiteConnect
except Exception:
    print("❌ kiteconnect is not installed. Run: pip install kiteconnect")
    raise

# =========================
# USER CONFIG
# =========================
API_KEY = os.getenv("KITE_API_KEY", "").strip()
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "").strip()
ACCESS_TOKEN_FILE = proj_root / "broker/access_token.txt"  # fallback
OUTPUT_CSV_PATH = proj_root / "option_paper_backtest_scaleout.csv"

SLIPPAGE_PER_SIDE_RUPEES = 0.50
CHARGES_PER_LOT_ROUNDTRIP = 60.00
LOTS = 2
TARGET1_POINTS = 40.0

UNDERLYING_TRADINGSYMBOL = "NIFTY"   # edit if BANKNIFTY / FINNIFTY
ALLOW_LONG_PP_SIGNAL = False
ALLOW_SHORT_PP_SIGNAL = False
MIN_BODY_POINTS = 0.0
SIGNAL_ENTRY_CUTOFF_HHMM = None
PP_ENTRY_CUTOFF_HHMM = None
MAX_CANDLE_RANGE_POINTS = None
MAX_SL_DISTANCE_POINTS = None


def _parse_profile_arg(default: str = "NIFTY") -> str:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--profile", choices=["nifty", "banknifty"])
    args, _ = parser.parse_known_args()
    val = (args.profile or os.getenv("STRATEGY_PROFILE", default)).strip().upper()
    return "BANKNIFTY" if "BANK" in val else "NIFTY"


def apply_strategy_profile(profile: str) -> None:
    global OUTPUT_CSV_PATH, TARGET1_POINTS, UNDERLYING_TRADINGSYMBOL
    global ALLOW_LONG_PP_SIGNAL, ALLOW_SHORT_PP_SIGNAL
    global MIN_BODY_POINTS, SIGNAL_ENTRY_CUTOFF_HHMM, PP_ENTRY_CUTOFF_HHMM
    global MAX_CANDLE_RANGE_POINTS, MAX_SL_DISTANCE_POINTS
    if profile == "BANKNIFTY":
        OUTPUT_CSV_PATH = proj_root / "option_paper_backtest_scaleout_banknifty.csv"
        TARGET1_POINTS = 80.0
        UNDERLYING_TRADINGSYMBOL = "BANKNIFTY"
        ALLOW_LONG_PP_SIGNAL = True
        ALLOW_SHORT_PP_SIGNAL = True
        MIN_BODY_POINTS = 10.0
        SIGNAL_ENTRY_CUTOFF_HHMM = 1330
        PP_ENTRY_CUTOFF_HHMM = 1030
        MAX_CANDLE_RANGE_POINTS = 120.0
        MAX_SL_DISTANCE_POINTS = 60.0
    else:
        OUTPUT_CSV_PATH = proj_root / "option_paper_backtest_scaleout.csv"
        TARGET1_POINTS = 40.0
        UNDERLYING_TRADINGSYMBOL = "NIFTY"
        ALLOW_LONG_PP_SIGNAL = False
        ALLOW_SHORT_PP_SIGNAL = False
        MIN_BODY_POINTS = 0.0
        SIGNAL_ENTRY_CUTOFF_HHMM = None
        PP_ENTRY_CUTOFF_HHMM = None
        MAX_CANDLE_RANGE_POINTS = None
        MAX_SL_DISTANCE_POINTS = None

# =========================
# Signal compatibility layer
# =========================
def _sig_get(signal, *names):
    for name in names:
        if hasattr(signal, name):
            v = getattr(signal, name)
            if v is not None:
                return v
    if isinstance(signal, dict):
        for name in names:
            if name in signal and signal[name] is not None:
                return signal[name]
    if is_dataclass(signal):
        d = asdict(signal)
        for name in names:
            if name in d and d[name] is not None:
                return d[name]
    available = list(getattr(signal, "__dict__", {}).keys()) if hasattr(signal, "__dict__") else []
    raise AttributeError(f"Signal missing fields {names}. Available: {available}")


def sig_entry(signal) -> float:
    return float(_sig_get(signal, "entry", "fut_entry", "entry_price", "trigger_price", "price"))


def sig_sl(signal) -> float:
    return float(_sig_get(signal, "sl", "fut_sl", "sl_price", "stop_loss"))


def sig_side(signal) -> str:
    return str(_sig_get(signal, "side", "direction", "signal_type")).upper().strip()


def sig_time(signal):
    return pd.to_datetime(_sig_get(signal, "entry_time", "ts", "timestamp", "time", "datetime"), errors="coerce")


# =========================
# Kite auth / setup helpers
# =========================
def _read_access_token_fallback() -> str:
    if ACCESS_TOKEN:
        return ACCESS_TOKEN
    if os.path.exists(ACCESS_TOKEN_FILE):
        with open(ACCESS_TOKEN_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    return ""


def verify_kite_auth(kite):
    profile = kite.profile()
    user_name = profile.get("user_name") or profile.get("user_id") or "Unknown"
    print(f"Kite auth OK | {user_name}")
    return True


def save_empty_option_results(csv_path):
    cols = [
        "entry_time", "side", "opt_error",
        "opt_symbol", "opt_token",
        "opt_entry_price", "opt_exit1_price", "opt_exit2_price",
        "opt_net_total_pnl_rupees_2lots",
        "opt_gross_total_pnl_rupees_2lots",
        "opt_charges_total_rupees_2lots",
    ]
    pd.DataFrame(columns=cols).to_csv(csv_path, index=False)
    print(f"Saved empty placeholder option results to: {csv_path}")


# =========================
# Load signals from test_signals.py (monkey patch capture)
# =========================
def _looks_like_signal_list(x: Any) -> bool:
    return isinstance(x, list) and len(x) > 0


def _load_signals_from_test_signals():
    import core.test_signals as mod

    print("test_signals exports:", [n for n in dir(mod) if not n.startswith("_")])
    print("prepare_df signature:", inspect.signature(mod.prepare_df))
    print("generate_signals signature:", inspect.signature(mod.generate_signals))
    print("main signature:", inspect.signature(mod.main))

    captured = {"prepared": None, "signals": None}

    original_prepare_df = mod.prepare_df
    original_generate_signals = mod.generate_signals

    def wrapped_prepare_df(df, *args, **kwargs):
        out = original_prepare_df(df, *args, **kwargs)
        captured["prepared"] = out
        return out

    def wrapped_generate_signals(df, *args, **kwargs):
        kwargs = dict(kwargs)
        kwargs["target_points"] = TARGET1_POINTS
        kwargs["min_body_points"] = MIN_BODY_POINTS
        kwargs["allow_long_pp"] = ALLOW_LONG_PP_SIGNAL
        kwargs["allow_short_pp"] = ALLOW_SHORT_PP_SIGNAL
        kwargs["entry_cutoff_hhmm"] = SIGNAL_ENTRY_CUTOFF_HHMM
        kwargs["pp_entry_cutoff_hhmm"] = PP_ENTRY_CUTOFF_HHMM
        kwargs["max_candle_range_points"] = MAX_CANDLE_RANGE_POINTS
        kwargs["max_sl_distance_points"] = MAX_SL_DISTANCE_POINTS
        out = original_generate_signals(df, *args, **kwargs)
        captured["signals"] = out
        return out

    try:
        mod.prepare_df = wrapped_prepare_df
        mod.generate_signals = wrapped_generate_signals
        mod.main()  # your main prints and runs pipeline
    finally:
        mod.prepare_df = original_prepare_df
        mod.generate_signals = original_generate_signals

    if _looks_like_signal_list(captured["signals"]):
        print(f"Captured signals from test_signals.main(): {len(captured['signals'])}")
        return captured["signals"]

    raise RuntimeError(
        "Could not capture signals from test_signals.main().\n"
        "main() ran, but generate_signals(...) output was not captured as a non-empty list."
    )


# =========================
# Option mapping / pricing helpers
# =========================
def round_to_step(x: float, step: int) -> int:
    return int(round(x / step) * step)


def strike_step_for_underlying(symbol: str) -> int:
    symbol = symbol.upper()
    return 100 if "BANK" in symbol else 50


def pick_option_type_from_signal(side: str) -> str:
    return "CE" if side == "LONG" else "PE"


def get_instruments_df(kite: KiteConnect) -> pd.DataFrame:
    data = kite.instruments("NFO")
    df = pd.DataFrame(data)
    if df.empty:
        raise RuntimeError("kite.instruments('NFO') returned empty data.")
    return df


def find_option_contract(
    instruments_df: pd.DataFrame,
    signal_time: pd.Timestamp,
    underlying_symbol: str,
    side: str,
    fut_entry_price: float
) -> Tuple[Optional[Any], Optional[str]]:
    if pd.isna(signal_time):
        return None, "Invalid signal time"

    opt_type = pick_option_type_from_signal(side)
    step = strike_step_for_underlying(underlying_symbol)
    atm_strike = round_to_step(fut_entry_price, step)

    df = instruments_df.copy()
    req_cols = {"tradingsymbol", "instrument_token", "name", "strike", "instrument_type", "expiry"}
    missing = [c for c in req_cols if c not in df.columns]
    if missing:
        return None, f"Instrument master missing columns: {missing}"

    df = df[df["name"].astype(str).str.upper() == underlying_symbol.upper()].copy()
    df = df[df["instrument_type"].astype(str).str.upper() == opt_type].copy()

    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df = df[df["strike"] == float(atm_strike)].copy()
    if df.empty:
        return None, f"No {underlying_symbol} {opt_type} contract found for ATM strike {atm_strike}"

    df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")
    df = df.dropna(subset=["expiry"]).copy()

    trade_date = signal_time.date()
    df = df[df["expiry"].dt.date >= trade_date].copy()
    if df.empty:
        return None, f"No non-expired {underlying_symbol} {opt_type} contracts for strike {atm_strike}"

    df = df.sort_values(["expiry", "tradingsymbol"]).reset_index(drop=True)
    return df.iloc[0], None


def fetch_option_intraday_candles(
    kite: KiteConnect,
    instrument_token: int,
    signal_time: pd.Timestamp,
    interval: str = "5minute"
) -> pd.DataFrame:
    start_dt = signal_time.normalize()
    end_dt = start_dt + pd.Timedelta(days=1)

    candles = kite.historical_data(
        instrument_token=instrument_token,
        from_date=start_dt.to_pydatetime(),
        to_date=end_dt.to_pydatetime(),
        interval=interval,
        continuous=False,
        oi=False,
    )
    df = pd.DataFrame(candles)
    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def simulate_option_scaleout_from_fut_signal(
    option_candles: pd.DataFrame,
    signal_time: pd.Timestamp,
    target1_points_underlying: float,
    lots: int = LOTS,
    slippage_per_side_rupees: float = SLIPPAGE_PER_SIDE_RUPEES,
    charges_per_lot_roundtrip: float = CHARGES_PER_LOT_ROUNDTRIP,
) -> Dict[str, Any]:
    if option_candles.empty:
        return {"opt_error": "No option candles fetched for signal day"}
    if "date" not in option_candles.columns:
        return {"opt_error": "Option candle data missing 'date' column"}

    df = option_candles.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    trade_df = df[df["date"] >= signal_time].copy().reset_index(drop=True)
    if trade_df.empty:
        return {"opt_error": "No option candles after signal time"}

    entry_row = trade_df.iloc[0]
    opt_entry: float = float(entry_row["close"])
    opt_entry_time = entry_row["date"]

    assumed_delta = 0.50
    opt_target_points: float = max(1.0, float(target1_points_underlying) * assumed_delta)
    opt_t1: float = opt_entry + opt_target_points
    opt_sl: float = max(0.05, opt_entry - (opt_target_points * 0.5))
    opt_be: float = opt_entry

    t1_hit = False
    exit1_time = None
    exit1_price = None
    exit2_time = None
    exit2_price = None

    gross_points: float = 0.0
    half: float = 0.5
    rem: float = 0.5

    for _, row in trade_df.iterrows():
        t = row["date"]
        h: float = float(row["high"])
        l: float = float(row["low"])

        if not t1_hit:
            sl_hit = l <= opt_sl
            t1_reached = h >= opt_t1

            if sl_hit and t1_reached:
                exit1_time = t
                exit1_price = opt_sl
                exit2_time = t
                exit2_price = opt_sl
                gross_points = (opt_sl - opt_entry)
                break
            if sl_hit:
                exit1_time = t
                exit1_price = opt_sl
                exit2_time = t
                exit2_price = opt_sl
                gross_points = (opt_sl - opt_entry)
                break
            if t1_reached:
                t1_hit = True
                exit1_time = t
                exit1_price = opt_t1
                gross_points += (opt_t1 - opt_entry) * half
                continue
        else:
            be_hit = l <= opt_be
            if be_hit:
                exit2_time = t
                exit2_price = opt_be
                gross_points += (opt_be - opt_entry) * rem
                break

    if exit1_time is None and exit2_time is None:
        last = trade_df.iloc[-1]
        exit1_time = last["date"]
        exit1_price = float(last["close"])
        gross_points = (exit1_price - opt_entry)
    elif t1_hit and exit2_time is None:
        last = trade_df.iloc[-1]
        exit2_time = last["date"]
        exit2_price = float(last["close"])
        gross_points += (exit2_price - opt_entry) * rem

    assumed_lot_size = 75 if "NIFTY" in UNDERLYING_TRADINGSYMBOL.upper() else 15
    gross_total_rupees = gross_points * assumed_lot_size * lots

    slippage_total = slippage_per_side_rupees * 2 * lots
    charges_total = charges_per_lot_roundtrip * lots
    total_cost = slippage_total + charges_total
    net_total_rupees = gross_total_rupees - total_cost

    return {
        "opt_entry_time": opt_entry_time,
        "opt_entry_price": opt_entry,
        "opt_exit1_price": exit1_price,
        "opt_exit2_price": exit2_price,
        "opt_gross_total_pnl_rupees_2lots": gross_total_rupees,
        "opt_charges_total_rupees_2lots": total_cost,
        "opt_net_total_pnl_rupees_2lots": net_total_rupees,
        "opt_error": None,
    }


# =========================
# Main
# =========================
def main():
    global ACCESS_TOKEN
    profile = _parse_profile_arg()
    apply_strategy_profile(profile)
    print(
        f"Profile: {profile} | Underlying={UNDERLYING_TRADINGSYMBOL} | "
        f"TARGET1_POINTS={TARGET1_POINTS} | PP(long/short)=({ALLOW_LONG_PP_SIGNAL}/{ALLOW_SHORT_PP_SIGNAL}) | "
        f"body>={MIN_BODY_POINTS} range<={MAX_CANDLE_RANGE_POINTS} sldist<={MAX_SL_DISTANCE_POINTS} "
        f"cutoff={SIGNAL_ENTRY_CUTOFF_HHMM} pp_cutoff={PP_ENTRY_CUTOFF_HHMM}"
    )
    ACCESS_TOKEN = _read_access_token_fallback()

    if not API_KEY:
        print("❌ Missing KITE_API_KEY (env var).")
        save_empty_option_results(OUTPUT_CSV_PATH)
        sys.exit(1)

    if not ACCESS_TOKEN:
        print("❌ Missing KITE_ACCESS_TOKEN (env var or access_token.txt).")
        save_empty_option_results(OUTPUT_CSV_PATH)
        sys.exit(1)

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)

    try:
        verify_kite_auth(kite)
    except Exception as e:
        print(f"Kite authentication failed. Generate a fresh access token and rerun.\nOriginal error: {e}")
        save_empty_option_results(OUTPUT_CSV_PATH)
        sys.exit(1)

    try:
        signals = _load_signals_from_test_signals()
    except Exception as e:
        print("❌ Failed to load signals from project.")
        print(e)
        save_empty_option_results(OUTPUT_CSV_PATH)
        sys.exit(1)

    if not signals:
        print("No signals found.")
        save_empty_option_results(OUTPUT_CSV_PATH)
        sys.exit(0)

    try:
        instruments_df = get_instruments_df(kite)
    except Exception as e:
        print(f"❌ Failed to load NFO instruments: {e}")
        save_empty_option_results(OUTPUT_CSV_PATH)
        sys.exit(1)

    rows = []
    for s in signals:
        base = {
            "entry_time": sig_time(s),
            "side": None,
            "fut_entry": None,
            "fut_sl": None,
        }

        try:
            base["side"] = sig_side(s)
            base["fut_entry"] = sig_entry(s)
            base["fut_sl"] = sig_sl(s)

            signal_time = pd.to_datetime(base["entry_time"], errors="coerce")
            if pd.isna(signal_time):
                rows.append({**base, "opt_error": "Invalid signal time"})
                continue

            contract, err = find_option_contract(
                instruments_df=instruments_df,
                signal_time=signal_time,
                underlying_symbol=UNDERLYING_TRADINGSYMBOL,
                side=base["side"],
                fut_entry_price=float(base["fut_entry"]),
            )
            if err:
                rows.append({**base, "opt_error": err})
                continue

            token: int = int(contract["instrument_token"])
            tradingsymbol: str = str(contract["tradingsymbol"])

            opt_df = fetch_option_intraday_candles(kite, token, signal_time, interval="5minute")
            sim = simulate_option_scaleout_from_fut_signal(
                option_candles=opt_df,
                signal_time=signal_time,
                target1_points_underlying=TARGET1_POINTS,
                lots=LOTS,
            )

            row = {
                **base,
                "opt_symbol": tradingsymbol,
                "opt_token": token,
                "opt_expiry": contract.get("expiry"),
                "opt_strike": contract.get("strike"),
                "opt_type": contract.get("instrument_type"),
                "opt_lot_size": contract.get("lot_size"),
                **sim,
            }
            rows.append(row)

        except Exception as e:
            rows.append({**base, "opt_error": str(e)})

    out_df = pd.DataFrame(rows)
    out_df.to_csv(OUTPUT_CSV_PATH, index=False)

    print(f"Saved option scale-out results to: {OUTPUT_CSV_PATH}")
    print(
        f"Cost assumptions -> Slippage/side: Rs {SLIPPAGE_PER_SIDE_RUPEES:.2f}, "
        f"Charges/lot roundtrip: Rs {CHARGES_PER_LOT_ROUNDTRIP:.2f}"
    )

    if out_df.empty:
        print("No rows written.")
        return

    valid_mask = out_df["opt_error"].isna() if "opt_error" in out_df.columns else pd.Series([True] * len(out_df))
    valid_count = int(valid_mask.sum())

    if valid_count == 0:
        print("No valid mapped option trades.")
        preview_cols = [c for c in ["entry_time", "side", "opt_error"] if c in out_df.columns]
        print(out_df[preview_cols].head(30).to_string(index=False))
        return

    print(f"Valid mapped option trades: {valid_count}")
    if "opt_net_total_pnl_rupees_2lots" in out_df.columns:
        s = pd.to_numeric(out_df["opt_net_total_pnl_rupees_2lots"], errors="coerce")
        print(f"Net total PnL (sum): Rs {s.sum(skipna=True):,.2f}")


if __name__ == "__main__":
    main()
