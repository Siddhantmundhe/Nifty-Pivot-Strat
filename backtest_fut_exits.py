from pathlib import Path
from dataclasses import asdict
import pandas as pd
from signal_engine import generate_signals, prepare_df

CSV = Path(__file__).resolve().parent / "nifty_fut_5m.csv"
OUT = Path(__file__).resolve().parent / "fut_backtest_results.csv"

def simulate_exit(df: pd.DataFrame, signal):
    """
    Simulate exits on FUT candles after entry:
    - LONG: TP if high >= tp, SL if low <= sl
    - SHORT: TP if low <= tp, SL if high >= sl
    If both hit in same candle, assume WORST CASE (SL first) for safety.
    Day-end fallback: exit at 15:25 candle close of same day.
    """
    entry_idx = signal.entry_idx
    entry_time = signal.entry_time
    side = signal.side
    tp = signal.fut_tp
    sl = signal.fut_sl

    trade_date = pd.Timestamp(entry_time).date()

    # Start checking from entry candle itself (entry is at open of this candle)
    scan = df.iloc[entry_idx:].copy()
    scan = scan[scan["date"].dt.date == trade_date]

    if scan.empty:
        return {
            "exit_time": None,
            "exit_price": None,
            "exit_reason": "NO_DATA",
            "pnl_points": None,
            "win": None,
        }

    for _, row in scan.iterrows():
        high_ = float(row["high"])
        low_ = float(row["low"])
        close_ = float(row["close"])
        ts = row["date"]

        if side == "LONG":
            hit_tp = high_ >= tp
            hit_sl = low_ <= sl

            if hit_tp and hit_sl:
                # conservative: assume SL first
                return {
                    "exit_time": ts,
                    "exit_price": sl,
                    "exit_reason": "SL_SAME_CANDLE",
                    "pnl_points": sl - signal.fut_entry,
                    "win": 0,
                }
            elif hit_sl:
                return {
                    "exit_time": ts,
                    "exit_price": sl,
                    "exit_reason": "SL",
                    "pnl_points": sl - signal.fut_entry,
                    "win": 0,
                }
            elif hit_tp:
                return {
                    "exit_time": ts,
                    "exit_price": tp,
                    "exit_reason": "TP",
                    "pnl_points": tp - signal.fut_entry,
                    "win": 1,
                }

        elif side == "SHORT":
            hit_tp = low_ <= tp
            hit_sl = high_ >= sl

            if hit_tp and hit_sl:
                # conservative: assume SL first
                return {
                    "exit_time": ts,
                    "exit_price": sl,
                    "exit_reason": "SL_SAME_CANDLE",
                    "pnl_points": signal.fut_entry - sl,
                    "win": 0,
                }
            elif hit_sl:
                return {
                    "exit_time": ts,
                    "exit_price": sl,
                    "exit_reason": "SL",
                    "pnl_points": signal.fut_entry - sl,
                    "win": 0,
                }
            elif hit_tp:
                return {
                    "exit_time": ts,
                    "exit_price": tp,
                    "exit_reason": "TP",
                    "pnl_points": signal.fut_entry - tp,
                    "win": 1,
                }

    # Day-end fallback (last available candle of same day)
    last_row = scan.iloc[-1]
    eod_close = float(last_row["close"])
    ts = last_row["date"]

    if side == "LONG":
        pnl = eod_close - signal.fut_entry
    else:
        pnl = signal.fut_entry - eod_close

    return {
        "exit_time": ts,
        "exit_price": eod_close,
        "exit_reason": "EOD",
        "pnl_points": pnl,
        "win": 1 if pnl > 0 else 0,
    }

def main():
    if not CSV.exists():
        raise RuntimeError("nifty_fut_5m.csv not found. Run fetch_candles.py first.")

    df = pd.read_csv(CSV)
    df["date"] = pd.to_datetime(df["date"])

    # Use prepared df so index positions match generate_signals internals
    prepared = prepare_df(df)
    signals = generate_signals(df, target_points=40.0)

    if not signals:
        print("No signals found.")
        return

    rows = []
    for s in signals:
        exit_info = simulate_exit(prepared, s)
        row = asdict(s)
        row.update(exit_info)

        # RR metrics
        risk_points = abs(s.fut_entry - s.fut_sl)
        row["risk_points"] = risk_points
        row["reward_points"] = abs(s.fut_tp - s.fut_entry)
        row["rr_planned"] = (row["reward_points"] / risk_points) if risk_points > 0 else None

        rows.append(row)

    res = pd.DataFrame(rows)
    res.to_csv(OUT, index=False)

    # summary
    total = len(res)
    wins = int(res["win"].fillna(0).sum())
    losses = int((res["win"] == 0).sum())
    win_rate = (wins / total * 100) if total else 0
    net_points = res["pnl_points"].fillna(0).sum()
    avg_points = res["pnl_points"].fillna(0).mean() if total else 0

    print(f"Saved results to: {OUT}")
    print(f"Total trades: {total}")
    print(f"Wins: {wins} | Losses: {losses} | Win rate: {win_rate:.2f}%")
    print(f"Net FUT points: {net_points:.2f} | Avg/trade: {avg_points:.2f}")
    print("\nLast trades:")
    cols = [
        "entry_time", "side", "level_name", "fut_entry", "fut_sl", "fut_tp",
        "exit_time", "exit_price", "exit_reason", "pnl_points"
    ]
    print(res[cols].tail(10).to_string(index=False))

if __name__ == "__main__":
    main()