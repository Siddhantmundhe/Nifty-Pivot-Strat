from dataclasses import dataclass
from typing import List, Optional
import pandas as pd
from indicators import add_ema, add_intraday_vwap, compute_daily_traditional_pivots

@dataclass
class Signal:
    signal_time: pd.Timestamp      # signal-1 candle close time
    entry_time: pd.Timestamp       # entry on 3rd candle open timestamp
    side: str                      # LONG / SHORT
    level_name: str                # R1/R2/S1/S2
    level_value: float
    fut_signal_close: float
    fut_entry: float
    fut_sl: float
    fut_tp: float
    signal1_idx: int
    signal2_idx: int
    entry_idx: int

def prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    out = out.sort_values("date").reset_index(drop=True)

    out = compute_daily_traditional_pivots(out)
    out["ema50"] = add_ema(out, 50)
    out["ema222"] = add_ema(out, 222)
    out["vwap"] = add_intraday_vwap(out)

    # time helpers
    out["trade_date"] = out["date"].dt.date
    out["time"] = out["date"].dt.time
    return out

def _is_after_930(ts: pd.Timestamp) -> bool:
    t = ts.time()
    return (t.hour > 9) or (t.hour == 9 and t.minute >= 30)

def _is_before_entry_cutoff(ts: pd.Timestamp, cutoff_h=14, cutoff_m=45) -> bool:
    t = ts.time()
    return (t.hour < cutoff_h) or (t.hour == cutoff_h and t.minute <= cutoff_m)

def _long_filter(row) -> bool:
    return (
        pd.notna(row["vwap"]) and pd.notna(row["ema50"]) and pd.notna(row["ema222"]) and
        row["close"] > row["vwap"] and
        row["close"] > row["ema50"] and
        row["ema50"] > row["ema222"]
    )

def _short_filter(row) -> bool:
    return (
        pd.notna(row["vwap"]) and pd.notna(row["ema50"]) and pd.notna(row["ema222"]) and
        row["close"] < row["vwap"] and
        row["close"] < row["ema50"] and
        row["ema50"] < row["ema222"]
    )

def generate_signals(df_raw: pd.DataFrame, target_points: float = 40.0) -> List[Signal]:
    """
    Nifty rules:
    - trade R1/R2 (LONG), S1/S2 (SHORT)
    - skip P
    - signal1 = break & close through level
    - signal2 = next candle must not touch level
    - entry = open of 3rd candle
    """
    df = prepare_df(df_raw)
    signals: List[Signal] = []

    # Need t, t+1, t+2 rows available
    for i in range(1, len(df) - 2):
        r1 = df.loc[i, "R1"]
        r2 = df.loc[i, "R2"]
        s1 = df.loc[i, "S1"]
        s2 = df.loc[i, "S2"]

        # skip rows where pivots unavailable (first day)
        if pd.isna(r1) or pd.isna(r2) or pd.isna(s1) or pd.isna(s2):
            continue

        row_t = df.loc[i]
        row_prev = df.loc[i - 1]
        row_t1 = df.loc[i + 1]   # signal-2 candle
        row_t2 = df.loc[i + 2]   # entry candle

        # Time filters on signal and entry
        if not _is_after_930(row_t["date"]):
            continue
        if not _is_before_entry_cutoff(row_t2["date"]):
            continue

        # avoid cross-day sequence (all 3 candles should be same date)
        if not (row_t["trade_date"] == row_t1["trade_date"] == row_t2["trade_date"]):
            continue

        # -------- LONG candidates on R1 / R2 --------
        for level_name in ["R1", "R2"]:
            L = float(row_t[level_name])

            signal1 = (row_t["close"] > L) and (row_prev["close"] <= L)
            signal2 = (row_t1["low"] > L)  # next candle doesn't touch level
            filt = _long_filter(row_t)

            if signal1 and signal2 and filt:
                fut_entry = float(row_t2["open"])
                fut_sl = float(row_t["low"])  # low of signal-1 candle
                fut_tp = fut_entry + float(target_points)

                # sanity: skip if SL >= entry (bad structure)
                if fut_sl >= fut_entry:
                    continue

                signals.append(
                    Signal(
                        signal_time=row_t["date"],
                        entry_time=row_t2["date"],
                        side="LONG",
                        level_name=level_name,
                        level_value=L,
                        fut_signal_close=float(row_t["close"]),
                        fut_entry=fut_entry,
                        fut_sl=fut_sl,
                        fut_tp=fut_tp,
                        signal1_idx=i,
                        signal2_idx=i + 1,
                        entry_idx=i + 2,
                    )
                )

        # -------- SHORT candidates on S1 / S2 --------
        for level_name in ["S1", "S2"]:
            L = float(row_t[level_name])

            signal1 = (row_t["close"] < L) and (row_prev["close"] >= L)
            signal2 = (row_t1["high"] < L)  # next candle doesn't touch level
            filt = _short_filter(row_t)

            if signal1 and signal2 and filt:
                fut_entry = float(row_t2["open"])
                fut_sl = float(row_t["high"])  # high of signal-1 candle
                fut_tp = fut_entry - float(target_points)

                # sanity: skip if SL <= entry (bad structure)
                if fut_sl <= fut_entry:
                    continue

                signals.append(
                    Signal(
                        signal_time=row_t["date"],
                        entry_time=row_t2["date"],
                        side="SHORT",
                        level_name=level_name,
                        level_value=L,
                        fut_signal_close=float(row_t["close"]),
                        fut_entry=fut_entry,
                        fut_sl=fut_sl,
                        fut_tp=fut_tp,
                        signal1_idx=i,
                        signal2_idx=i + 1,
                        entry_idx=i + 2,
                    )
                )

    return signals