import pandas as pd

def add_ema(df: pd.DataFrame, period: int, price_col: str = "close") -> pd.Series:
    return df[price_col].ewm(span=period, adjust=False).mean()

def add_intraday_vwap(df: pd.DataFrame) -> pd.Series:
    """
    VWAP resets each trading day.
    Assumes df has columns: date, high, low, close, volume
    """
    out = df.copy()
    out["session"] = out["date"].dt.date
    tp = (out["high"] + out["low"] + out["close"]) / 3.0
    pv = tp * out["volume"]

    out["_cum_pv"] = pv.groupby(out["session"]).cumsum()
    out["_cum_vol"] = out["volume"].groupby(out["session"]).cumsum()
    vwap = out["_cum_pv"] / out["_cum_vol"].replace(0, pd.NA)
    return vwap

def compute_daily_traditional_pivots(intraday_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute previous-day-based daily pivots and map them onto each intraday row.
    Returns columns added: P, R1, S1, R2, S2
    """
    df = intraday_df.copy()
    df["trade_date"] = df["date"].dt.date

    daily = (
        df.groupby("trade_date", as_index=False)
        .agg(day_high=("high", "max"),
             day_low=("low", "min"),
             day_close=("close", "last"))
        .sort_values("trade_date")
        .reset_index(drop=True)
    )

    # previous day's OHLC drives current day pivots
    daily["prev_high"] = daily["day_high"].shift(1)
    daily["prev_low"] = daily["day_low"].shift(1)
    daily["prev_close"] = daily["day_close"].shift(1)

    daily["P"] = (daily["prev_high"] + daily["prev_low"] + daily["prev_close"]) / 3.0
    daily["R1"] = 2 * daily["P"] - daily["prev_low"]
    daily["S1"] = 2 * daily["P"] - daily["prev_high"]
    daily["R2"] = daily["P"] + (daily["prev_high"] - daily["prev_low"])
    daily["S2"] = daily["P"] - (daily["prev_high"] - daily["prev_low"])

    piv = daily[["trade_date", "P", "R1", "S1", "R2", "S2"]]
    merged = df.merge(piv, on="trade_date", how="left")
    return merged