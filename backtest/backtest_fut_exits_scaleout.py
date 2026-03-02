# backtest_fut_exits_scaleout.py
from __future__ import annotations

import sys
import inspect
import argparse
import os
from pathlib import Path

# Add project root to sys.path so 'core' module can be found
proj_root = Path(__file__).resolve().parent.parent
if str(proj_root) not in sys.path:
    sys.path.insert(0, str(proj_root))

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

TARGET1_POINTS = 40.0
PRINT_PREVIEW_ROWS = 10
OUTPUT_CSV = proj_root / "fut_backtest_scaleout_results.csv"
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
    global TARGET1_POINTS, OUTPUT_CSV, ALLOW_LONG_PP_SIGNAL, ALLOW_SHORT_PP_SIGNAL
    global MIN_BODY_POINTS, SIGNAL_ENTRY_CUTOFF_HHMM, PP_ENTRY_CUTOFF_HHMM
    global MAX_CANDLE_RANGE_POINTS, MAX_SL_DISTANCE_POINTS
    if profile == "BANKNIFTY":
        TARGET1_POINTS = 80.0
        OUTPUT_CSV = proj_root / "fut_backtest_scaleout_results_banknifty.csv"
        ALLOW_LONG_PP_SIGNAL = True
        ALLOW_SHORT_PP_SIGNAL = True
        MIN_BODY_POINTS = 10.0
        SIGNAL_ENTRY_CUTOFF_HHMM = 1330
        PP_ENTRY_CUTOFF_HHMM = 1030
        MAX_CANDLE_RANGE_POINTS = 120.0
        MAX_SL_DISTANCE_POINTS = 60.0
    else:
        TARGET1_POINTS = 40.0
        OUTPUT_CSV = proj_root / "fut_backtest_scaleout_results.csv"
        ALLOW_LONG_PP_SIGNAL = False
        ALLOW_SHORT_PP_SIGNAL = False
        MIN_BODY_POINTS = 0.0
        SIGNAL_ENTRY_CUTOFF_HHMM = None
        PP_ENTRY_CUTOFF_HHMM = None
        MAX_CANDLE_RANGE_POINTS = None
        MAX_SL_DISTANCE_POINTS = None

TIME_COL_CANDIDATES = ["datetime", "timestamp", "time", "ts", "date", "index"]
HIGH_COL_CANDIDATES = ["high", "High", "h"]
LOW_COL_CANDIDATES = ["low", "Low", "l"]
CLOSE_COL_CANDIDATES = ["close", "Close", "c"]


def _sig_get(signal, *names):
    for name in names:
        if hasattr(signal, name):
            value = getattr(signal, name)
            if value is not None:
                return value
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
    raise AttributeError(f"Signal missing fields {names}. Available fields: {available}")


def sig_entry(signal) -> float:
    return float(_sig_get(signal, "entry", "fut_entry", "entry_price", "trigger_price", "price", "tp")) if False else float(
        _sig_get(signal, "entry", "fut_entry", "entry_price", "trigger_price", "price")
    )


def sig_sl(signal) -> float:
    return float(_sig_get(signal, "sl", "fut_sl", "sl_price", "stop_loss"))


def sig_side(signal) -> str:
    return str(_sig_get(signal, "side", "direction", "signal_type")).upper().strip()


def sig_time(signal):
    return _sig_get(signal, "entry_time", "ts", "timestamp", "time", "datetime")


def sig_level_tag(signal) -> str:
    # Your Signal has level_name / level_value
    for name in ["level_tag", "level_name", "pivot_tag", "tag"]:
        try:
            return str(_sig_get(signal, name))
        except Exception:
            pass
    return ""


def sig_level_price(signal) -> Optional[float]:
    for name in ["level_price", "level_value", "pivot_price", "level"]:
        try:
            return float(_sig_get(signal, name))
        except Exception:
            pass
    return None


def _row_get(row: Any, candidates: List[str]):
    if isinstance(row, pd.Series):
        for c in candidates:
            if c in row.index:
                return row[c]
    if isinstance(row, dict):
        for c in candidates:
            if c in row:
                return row[c]
    for c in candidates:
        if hasattr(row, c):
            return getattr(row, c)
    raise KeyError(f"Missing row field. Tried: {candidates}")


def row_high(row) -> float:
    return float(_row_get(row, HIGH_COL_CANDIDATES))


def row_low(row) -> float:
    return float(_row_get(row, LOW_COL_CANDIDATES))


def row_close(row) -> float:
    return float(_row_get(row, CLOSE_COL_CANDIDATES))


def _ensure_df(prepared: Any) -> pd.DataFrame:
    if isinstance(prepared, pd.DataFrame):
        df = prepared.copy()
    elif isinstance(prepared, list):
        if len(prepared) == 0:
            return pd.DataFrame()
        if isinstance(prepared[0], dict):
            df = pd.DataFrame(prepared)
        else:
            rows = []
            for r in prepared:
                if isinstance(r, dict):
                    rows.append(r)
                elif is_dataclass(r):
                    rows.append(asdict(r))
                elif hasattr(r, "__dict__"):
                    rows.append(vars(r))
                else:
                    raise TypeError(f"Unsupported candle row type: {type(r)}")
            df = pd.DataFrame(rows)
    else:
        raise TypeError(f"Unsupported prepared type: {type(prepared)}")

    # KEY FIX: if time is in index, move it to a column
    if not df.empty:
        # If index is datetime-like, preserve it
        if isinstance(df.index, pd.DatetimeIndex):
            if "datetime" not in df.columns and "timestamp" not in df.columns and "time" not in df.columns and "ts" not in df.columns and "date" not in df.columns:
                df = df.reset_index()
                # rename index column to datetime (common after reset_index)
                first_col = df.columns[0]
                if str(first_col).lower() in ["index", "date", "datetime", "timestamp", "time", "ts"] or "unnamed" in str(first_col).lower():
                    df = df.rename(columns={first_col: "datetime"})
        else:
            # Sometimes datetime is in a generic unnamed index column after reset
            if all(c not in df.columns for c in ["datetime", "timestamp", "time", "ts", "date"]):
                # try resetting anyway if index looks meaningful
                idx_name = str(df.index.name) if df.index.name is not None else ""
                if idx_name or not isinstance(df.index, pd.RangeIndex):
                    df = df.reset_index()
                    first_col = df.columns[0]
                    if str(first_col).lower() == "index":
                        # we'll let time detector inspect values later
                        pass

    return df


def _looks_like_signal_list(x: Any) -> bool:
    return isinstance(x, list) and len(x) > 0


def _load_prepared_and_signals_from_test_signals():
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
        mod.main()
    finally:
        mod.prepare_df = original_prepare_df
        mod.generate_signals = original_generate_signals

    if captured["prepared"] is None or not _looks_like_signal_list(captured["signals"]):
        raise RuntimeError("Could not capture prepared dataframe + signals from test_signals.main().")

    print(f"Captured signals: {len(captured['signals'])}")
    return captured["prepared"], captured["signals"]


def _detect_time_col(df: pd.DataFrame) -> str:
    # 1) direct name match
    for c in TIME_COL_CANDIDATES:
        if c in df.columns:
            return c

    # 2) try to infer by parsing columns (first datetime-like column)
    for c in df.columns:
        sample = pd.to_datetime(df[c], errors="coerce")
        if sample.notna().sum() > 0 and sample.notna().mean() > 0.8:
            return c

    raise KeyError(f"No time column found in prepared candles. Tried names {TIME_COL_CANDIDATES}. Columns present: {list(df.columns)}")


def simulate_scaleout_trade(prepared_df: pd.DataFrame, signal, target1_points: float = TARGET1_POINTS) -> Dict[str, Any]:
    side = sig_side(signal)
    entry_price = sig_entry(signal)
    sl_price = sig_sl(signal)
    signal_ts = pd.to_datetime(sig_time(signal), errors="coerce")

    if pd.isna(signal_ts):
        raise ValueError("Invalid signal time")
    if side not in {"LONG", "SHORT"}:
        raise ValueError(f"Unsupported side: {side}")

    t1_price = entry_price + target1_points if side == "LONG" else entry_price - target1_points

    df = prepared_df.copy()
    time_col = _detect_time_col(df)

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col]).sort_values(time_col).reset_index(drop=True)

    trade_df = df[(df[time_col].dt.date == signal_ts.date()) & (df[time_col] >= signal_ts)].copy().reset_index(drop=True)
    if trade_df.empty:
        return {
            "entry_time": signal_ts, "side": side, "entry_price": entry_price, "sl_price": sl_price,
            "t1_price": t1_price, "status": "NO_CANDLES_AFTER_SIGNAL", "error": "No candles after signal timestamp",
            "realized_points_total": 0.0, "exit1_time": None, "exit1_price": None, "exit2_time": None, "exit2_price": None
        }

    half_qty, rem_qty = 0.5, 0.5
    t1_hit = False
    exit1_time = exit1_price = exit2_time = exit2_price = None
    realized_points_total = 0.0
    status = "OPEN"

    def points_from_move(exit_px: float) -> float:
        return (exit_px - entry_price) if side == "LONG" else (entry_price - exit_px)

    for _, row in trade_df.iterrows():
        t = row[time_col]
        h = row_high(row)
        l = row_low(row)

        if not t1_hit:
            if side == "LONG":
                sl_hit = l <= sl_price
                t1_reached = h >= t1_price
            else:
                sl_hit = h >= sl_price
                t1_reached = l <= t1_price

            if sl_hit and t1_reached:
                exit1_time = t
                exit1_price = sl_price
                exit2_time = t
                exit2_price = sl_price
                realized_points_total = points_from_move(sl_price)
                status = "SL_HIT_AMBIGUOUS_BEFORE_T1"
                break
            if sl_hit:
                exit1_time = t
                exit1_price = sl_price
                exit2_time = t
                exit2_price = sl_price
                realized_points_total = points_from_move(sl_price)
                status = "SL_HIT"
                break
            if t1_reached:
                t1_hit = True
                exit1_time = t
                exit1_price = t1_price
                realized_points_total += points_from_move(t1_price) * half_qty
                continue
        else:
            be_price = entry_price
            be_hit = (l <= be_price) if side == "LONG" else (h >= be_price)
            if be_hit:
                exit2_time = t
                exit2_price = be_price
                realized_points_total += points_from_move(be_price) * rem_qty
                status = "T1_THEN_BE"
                break

    if status == "OPEN":
        last_row = trade_df.iloc[-1]
        last_t = last_row[time_col]
        last_c = row_close(last_row)
        if not t1_hit:
            exit1_time = last_t
            exit1_price = last_c
            realized_points_total = points_from_move(last_c)
            status = "EOD_EXIT_FULL"
        else:
            exit2_time = last_t
            exit2_price = last_c
            realized_points_total += points_from_move(last_c) * rem_qty
            status = "T1_THEN_EOD"

    return {
        "entry_time": signal_ts,
        "side": side,
        "level_tag": sig_level_tag(signal),
        "level_price": sig_level_price(signal),
        "entry_price": entry_price,
        "sl_price": sl_price,
        "t1_price": t1_price,
        "exit1_time": exit1_time,
        "exit1_price": exit1_price,
        "exit2_time": exit2_time,
        "exit2_price": exit2_price,
        "status": status,
        "realized_points_total": realized_points_total,
        "t1_hit": t1_hit,
    }


def main():
    profile = _parse_profile_arg()
    apply_strategy_profile(profile)
    print(
        f"Profile: {profile} | TARGET1_POINTS={TARGET1_POINTS} | "
        f"PP(long/short)=({ALLOW_LONG_PP_SIGNAL}/{ALLOW_SHORT_PP_SIGNAL}) | "
        f"body>={MIN_BODY_POINTS} range<={MAX_CANDLE_RANGE_POINTS} sldist<={MAX_SL_DISTANCE_POINTS} "
        f"cutoff={SIGNAL_ENTRY_CUTOFF_HHMM} pp_cutoff={PP_ENTRY_CUTOFF_HHMM}"
    )
    try:
        prepared, signals = _load_prepared_and_signals_from_test_signals()
    except Exception as e:
        print("❌ Auto-import path failed.")
        print(e)
        sys.exit(1)

    prepared_df = _ensure_df(prepared)
    if prepared_df.empty:
        print("❌ Prepared candles are empty.")
        sys.exit(1)
    if not signals:
        print("❌ No signals found.")
        sys.exit(1)

    print("Prepared columns:", list(prepared_df.columns))
    print("Prepared index type:", type(prepared_df.index))
    print("First signal type:", type(signals[0]))
    print("First signal preview:", signals[0])

    results = []
    for s in signals:
        try:
            results.append(simulate_scaleout_trade(prepared_df, s, target1_points=TARGET1_POINTS))
        except Exception as e:
            try:
                side_val = sig_side(s)
            except Exception:
                side_val = None
            results.append({
                "entry_time": pd.to_datetime(sig_time(s), errors="coerce"),
                "side": side_val,
                "status": "SIM_ERROR",
                "error": str(e),
                "realized_points_total": 0.0,
            })

    res_df = pd.DataFrame(results)
    res_df.to_csv(OUTPUT_CSV, index=False)

    print(f"\nSaved futures scale-out backtest results to: {OUTPUT_CSV}")
    print(f"Total simulated rows: {len(res_df)}")
    if "status" in res_df.columns:
        print("\nStatus counts:")
        print(res_df["status"].value_counts(dropna=False).to_string())
    if "realized_points_total" in res_df.columns:
        pnl = pd.to_numeric(res_df["realized_points_total"], errors="coerce").fillna(0)
        print("\nSummary:")
        print(f"Gross points: {pnl.sum():.2f}")
        print(f"Average/trade: {pnl.mean():.2f}")

    print("\nPreview:")
    print(res_df.head(PRINT_PREVIEW_ROWS).to_string(index=False))


if __name__ == "__main__":
    main()
