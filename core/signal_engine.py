from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
import numpy as np


# =============================================================================
# Strategy v2 filters (derived from backtest variant analysis)
# =============================================================================
ENTRY_CUTOFF_HOUR = 14
ENTRY_CUTOFF_MINUTE = 0

ALLOW_LONG_R1 = True
ALLOW_LONG_R2 = True
ALLOW_SHORT_S1 = True
ALLOW_SHORT_S2 = False   # v2: disable SHORT S2
ALLOW_LONG_PP = False
ALLOW_SHORT_PP = False


# =============================================================================
# Config / constants
# =============================================================================
DEFAULT_TARGET_POINTS = 40.0
DEFAULT_TOUCH_TOLERANCE = 5.0          # points around pivot level to consider "touch"
DEFAULT_BREAK_CONFIRMATION = 0.0        # extra points beyond level to confirm breakout
DEFAULT_SL_BUFFER_POINTS = 12.0         # fallback SL buffer if candle extreme not suitable
MIN_BODY_POINTS = 0.0                   # can keep 0 for now


# =============================================================================
# Data structures
# =============================================================================
@dataclass
class Signal:
    entry_idx: int
    entry_time: pd.Timestamp
    side: str                 # "LONG" / "SHORT"
    level_name: str           # "PP" / "R1" / "R2" / "S1" / "S2"
    level_value: float

    entry: float
    sl: float
    tp: float

    # Optional metadata (nice to have / future use)
    trigger_open: Optional[float] = None
    trigger_high: Optional[float] = None
    trigger_low: Optional[float] = None
    trigger_close: Optional[float] = None

    def to_dict(self):
        return {
            "entry_idx": self.entry_idx,
            "entry_time": self.entry_time,
            "side": self.side,
            "level_name": self.level_name,
            "level_value": self.level_value,
            "entry": self.entry,
            "sl": self.sl,
            "tp": self.tp,
            "trigger_open": self.trigger_open,
            "trigger_high": self.trigger_high,
            "trigger_low": self.trigger_low,
            "trigger_close": self.trigger_close,
        }


# =============================================================================
# Helpers
# =============================================================================
def _ensure_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if isinstance(out.index, pd.DatetimeIndex):
        return out.sort_index()

    # Common timestamp column names
    for c in ["date", "datetime", "timestamp", "time"]:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")
            out = out.dropna(subset=[c]).sort_values(c).set_index(c)
            return out

    raise ValueError(
        "Could not find a DatetimeIndex or a datetime column "
        "(tried: date, datetime, timestamp, time)."
    )


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str:
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    raise ValueError(f"Required column not found. Tried: {candidates}")


def _normalize_ohlc_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    open_col = _find_col(out, ["open", "o"])
    high_col = _find_col(out, ["high", "h"])
    low_col = _find_col(out, ["low", "l"])
    close_col = _find_col(out, ["close", "c"])

    rename_map = {
        open_col: "open",
        high_col: "high",
        low_col: "low",
        close_col: "close",
    }
    out = out.rename(columns=rename_map)
    return out


def _daily_pivots(prev_day_high: float, prev_day_low: float, prev_day_close: float):
    pp = (prev_day_high + prev_day_low + prev_day_close) / 3.0
    r1 = 2 * pp - prev_day_low
    s1 = 2 * pp - prev_day_high
    r2 = pp + (prev_day_high - prev_day_low)
    s2 = pp - (prev_day_high - prev_day_low)
    return pp, r1, r2, s1, s2


def _compute_prev_day_pivot_levels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add pivot columns (PP, R1, R2, S1, S2) for each intraday row
    using PREVIOUS trading day's H/L/C.
    """
    out = df.copy()

    # local date buckets from index
    out["trade_date"] = out.index.date

    daily = (
        out.groupby("trade_date")
        .agg(day_high=("high", "max"), day_low=("low", "min"), day_close=("close", "last"))
        .sort_index()
    )

    # previous day values
    daily["prev_day_high"] = daily["day_high"].shift(1)
    daily["prev_day_low"] = daily["day_low"].shift(1)
    daily["prev_day_close"] = daily["day_close"].shift(1)

    pivots = daily.apply(
        lambda r: pd.Series(
            _daily_pivots(r["prev_day_high"], r["prev_day_low"], r["prev_day_close"])
        )
        if pd.notna(r["prev_day_high"]) and pd.notna(r["prev_day_low"]) and pd.notna(r["prev_day_close"])
        else pd.Series([np.nan, np.nan, np.nan, np.nan, np.nan]),
        axis=1,
    )
    pivots.columns = ["PP", "R1", "R2", "S1", "S2"]

    daily = pd.concat([daily, pivots], axis=1)

    # merge back to intraday rows by trade_date
    out = out.merge(
        daily[["PP", "R1", "R2", "S1", "S2"]],
        left_on="trade_date",
        right_index=True,
        how="left",
    )

    return out


def _within_entry_cutoff(ts: pd.Timestamp) -> bool:
    # inclusive cutoff
    cutoff_minutes = ENTRY_CUTOFF_HOUR * 60 + ENTRY_CUTOFF_MINUTE
    ts_minutes = ts.hour * 60 + ts.minute
    return ts_minutes <= cutoff_minutes


def _within_custom_cutoff(ts: pd.Timestamp, cutoff_hhmm: Optional[int]) -> bool:
    if cutoff_hhmm is None:
        return _within_entry_cutoff(ts)
    cutoff_h = int(cutoff_hhmm) // 100
    cutoff_m = int(cutoff_hhmm) % 100
    cutoff_minutes = cutoff_h * 60 + cutoff_m
    ts_minutes = ts.hour * 60 + ts.minute
    return ts_minutes <= cutoff_minutes


def _body_size(row: pd.Series) -> float:
    return abs(float(row["close"]) - float(row["open"]))


def _range_size(row: pd.Series) -> float:
    return abs(float(row["high"]) - float(row["low"]))


def _touched_level(row: pd.Series, level_value: float, tol: float) -> bool:
    low_ = float(row["low"])
    high_ = float(row["high"])
    return (low_ - tol) <= level_value <= (high_ + tol)


def _make_long_signal(
    i: int,
    ts: pd.Timestamp,
    row: pd.Series,
    level_name: str,
    level_value: float,
    target_points: float,
    sl_buffer_points: float,
) -> Signal:
    entry = float(row["close"])
    # keep SL below candle low with small buffer; fallback to fixed buffer if weird
    sl_candidate = float(row["low"]) - 0.1
    if not np.isfinite(sl_candidate) or sl_candidate >= entry:
        sl_candidate = entry - sl_buffer_points
    tp = entry + float(target_points)

    return Signal(
        entry_idx=i,
        entry_time=ts,
        side="LONG",
        level_name=level_name,
        level_value=float(level_value),
        entry=float(entry),
        sl=float(sl_candidate),
        tp=float(tp),
        trigger_open=float(row["open"]),
        trigger_high=float(row["high"]),
        trigger_low=float(row["low"]),
        trigger_close=float(row["close"]),
    )


def _make_short_signal(
    i: int,
    ts: pd.Timestamp,
    row: pd.Series,
    level_name: str,
    level_value: float,
    target_points: float,
    sl_buffer_points: float,
) -> Signal:
    entry = float(row["close"])
    sl_candidate = float(row["high"]) + 0.1
    if not np.isfinite(sl_candidate) or sl_candidate <= entry:
        sl_candidate = entry + sl_buffer_points
    tp = entry - float(target_points)

    return Signal(
        entry_idx=i,
        entry_time=ts,
        side="SHORT",
        level_name=level_name,
        level_value=float(level_value),
        entry=float(entry),
        sl=float(sl_candidate),
        tp=float(tp),
        trigger_open=float(row["open"]),
        trigger_high=float(row["high"]),
        trigger_low=float(row["low"]),
        trigger_close=float(row["close"]),
    )


# =============================================================================
# Public API
# =============================================================================
def prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare raw candle dataframe for signal generation.
    Expected input: intraday OHLC data with datetime index or a datetime column.
    Returns a dataframe with normalized OHLC + pivot levels.
    """
    out = _ensure_datetime_index(df)
    out = _normalize_ohlc_columns(out)

    # Keep only rows with valid OHLC
    out = out.dropna(subset=["open", "high", "low", "close"]).copy()

    # Ensure numeric
    for c in ["open", "high", "low", "close"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.dropna(subset=["open", "high", "low", "close"]).copy()

    # Basic sanity
    out = out[(out["high"] >= out["low"])].copy()

    # Add pivots
    out = _compute_prev_day_pivot_levels(out)

    # Optional convenience columns
    out["candle_body"] = (out["close"] - out["open"]).abs()
    out["candle_range"] = (out["high"] - out["low"]).abs()

    return out


def generate_signals(
    df: pd.DataFrame,
    target_points: float = DEFAULT_TARGET_POINTS,
    touch_tolerance: float = DEFAULT_TOUCH_TOLERANCE,
    break_confirmation: float = DEFAULT_BREAK_CONFIRMATION,
    sl_buffer_points: float = DEFAULT_SL_BUFFER_POINTS,
    min_body_points: float = MIN_BODY_POINTS,
    allow_long_pp: Optional[bool] = None,
    allow_short_pp: Optional[bool] = None,
    entry_cutoff_hhmm: Optional[int] = None,
    pp_entry_cutoff_hhmm: Optional[int] = None,
    max_candle_range_points: Optional[float] = None,
    max_sl_distance_points: Optional[float] = None,
) -> List[Signal]:
    """
    Generate pivot breakout signals.

    Compatibility notes:
    - accepts `target_points=` keyword (fixes your caller errors)
    - returns List[Signal] (fixes test_signals expectations)
    - Signal contains `entry_idx` (fixes backtest_fut_exits_scaleout expectations)
    """
    prepared = prepare_df(df) if "R1" not in df.columns else df.copy()
    allow_long_pp = ALLOW_LONG_PP if allow_long_pp is None else bool(allow_long_pp)
    allow_short_pp = ALLOW_SHORT_PP if allow_short_pp is None else bool(allow_short_pp)

    signals: List[Signal] = []

    # Iterate row by row
    for i, (ts, row) in enumerate(prepared.iterrows()):
        # Need pivot levels present
        if (
            pd.isna(row.get("PP"))
            or pd.isna(row.get("R1"))
            or pd.isna(row.get("R2"))
            or pd.isna(row.get("S1"))
            or pd.isna(row.get("S2"))
        ):
            continue

        # Entry time filter (v2)
        if not _within_custom_cutoff(ts, entry_cutoff_hhmm):
            continue

        # Minimal candle body filter (optional)
        if _body_size(row) < float(min_body_points):
            continue

        # Max candle range filter (optional)
        if max_candle_range_points is not None and _range_size(row) > float(max_candle_range_points):
            continue

        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        # ---- PIVOT (PP) logic ----
        if allow_long_pp:
            pp = float(row["PP"])
            if (
                _within_custom_cutoff(ts, pp_entry_cutoff_hhmm)
                and
                _touched_level(row, pp, touch_tolerance)
                and c > (pp + break_confirmation)
                and c >= o
            ):
                sig = _make_long_signal(i, ts, row, "PP", pp, target_points, sl_buffer_points)
                if (
                    max_sl_distance_points is None
                    or abs(float(sig.entry) - float(sig.sl)) <= float(max_sl_distance_points)
                ):
                    signals.append(sig)

        if allow_short_pp:
            pp = float(row["PP"])
            if (
                _within_custom_cutoff(ts, pp_entry_cutoff_hhmm)
                and
                _touched_level(row, pp, touch_tolerance)
                and c < (pp - break_confirmation)
                and c <= o
            ):
                sig = _make_short_signal(i, ts, row, "PP", pp, target_points, sl_buffer_points)
                if (
                    max_sl_distance_points is None
                    or abs(float(sig.entry) - float(sig.sl)) <= float(max_sl_distance_points)
                ):
                    signals.append(sig)

        # ---- LONG logic ----
        # R1 breakout: candle touches R1 and closes above (with optional confirmation)
        if ALLOW_LONG_R1:
            r1 = float(row["R1"])
            if (
                _touched_level(row, r1, touch_tolerance)
                and c > (r1 + break_confirmation)
                and c >= o  # bullish bias
            ):
                sig = _make_long_signal(i, ts, row, "R1", r1, target_points, sl_buffer_points)
                if (
                    max_sl_distance_points is None
                    or abs(float(sig.entry) - float(sig.sl)) <= float(max_sl_distance_points)
                ):
                    signals.append(sig)

        # R2 breakout
        if ALLOW_LONG_R2:
            r2 = float(row["R2"])
            if (
                _touched_level(row, r2, touch_tolerance)
                and c > (r2 + break_confirmation)
                and c >= o
            ):
                sig = _make_long_signal(i, ts, row, "R2", r2, target_points, sl_buffer_points)
                if (
                    max_sl_distance_points is None
                    or abs(float(sig.entry) - float(sig.sl)) <= float(max_sl_distance_points)
                ):
                    signals.append(sig)

        # ---- SHORT logic ----
        # S1 breakdown: candle touches S1 and closes below
        if ALLOW_SHORT_S1:
            s1 = float(row["S1"])
            if (
                _touched_level(row, s1, touch_tolerance)
                and c < (s1 - break_confirmation)
                and c <= o  # bearish bias
            ):
                sig = _make_short_signal(i, ts, row, "S1", s1, target_points, sl_buffer_points)
                if (
                    max_sl_distance_points is None
                    or abs(float(sig.entry) - float(sig.sl)) <= float(max_sl_distance_points)
                ):
                    signals.append(sig)

        # S2 breakdown (disabled in v2 by default)
        if ALLOW_SHORT_S2:
            s2 = float(row["S2"])
            if (
                _touched_level(row, s2, touch_tolerance)
                and c < (s2 - break_confirmation)
                and c <= o
            ):
                sig = _make_short_signal(i, ts, row, "S2", s2, target_points, sl_buffer_points)
                if (
                    max_sl_distance_points is None
                    or abs(float(sig.entry) - float(sig.sl)) <= float(max_sl_distance_points)
                ):
                    signals.append(sig)

    return signals


# Optional helper if any script wants a DataFrame view
def signals_to_df(signals: List[Signal]) -> pd.DataFrame:
    if not signals:
        return pd.DataFrame(
            columns=[
                "entry_idx", "entry_time", "side", "level_name", "level_value",
                "entry", "sl", "tp", "trigger_open", "trigger_high", "trigger_low", "trigger_close"
            ]
        )
    return pd.DataFrame([s.to_dict() for s in signals])
