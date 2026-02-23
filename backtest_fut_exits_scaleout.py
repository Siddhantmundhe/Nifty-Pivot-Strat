from pathlib import Path
from dataclasses import asdict
import pandas as pd
from signal_engine import generate_signals, prepare_df

CSV = Path(__file__).resolve().parent / "nifty_fut_5m.csv"
OUT = Path(__file__).resolve().parent / "fut_backtest_scaleout_results.csv"

TARGET1_POINTS = 40.0  # lot1 fixed target

def simulate_scaleout_trade(df: pd.DataFrame, signal, target1_points: float = TARGET1_POINTS):
    """
    Simulates 2-lot trade on FUT price:

    Lot 1:
      - fixed TP at +40 (LONG) / -40 (SHORT)
      - SL = signal-1 candle low/high (same as strategy)

    Lot 2:
      - initial SL same as lot1
      - after lot1 TP hit -> move SL to break-even, then trail:
          LONG: previous completed candle low
          SHORT: previous completed candle high
      - EOD exit fallback if still open

    Conservative rule:
      If TP and SL are both hit in same candle, assume SL first.
    """
    entry_idx = signal.entry_idx
    side = signal.side
    entry_time = pd.Timestamp(signal.entry_time)
    trade_date = entry_time.date()

    entry_price = float(signal.fut_entry)
    initial_sl = float(signal.fut_sl)

    # Lot1 target
    if side == "LONG":
        lot1_tp = entry_price + target1_points
    else:
        lot1_tp = entry_price - target1_points

    # State
    lot1_open = True
    lot2_open = True

    lot1_exit_price = None
    lot1_exit_time = None
    lot1_exit_reason = None

    lot2_exit_price = None
    lot2_exit_time = None
    lot2_exit_reason = None

    # trailing state for lot2
    lot2_sl = initial_sl
    lot1_done = False  # once TP/SL happens
    lot2_be_activated = False

    # MAE / MFE (in FUT points, per trade)
    max_favorable = 0.0
    max_adverse = 0.0

    # scan only same-day candles starting from entry candle
    scan = df.iloc[entry_idx:].copy()
    scan = scan[scan["date"].dt.date == trade_date].reset_index(drop=False)  # keep original index in "index"

    if scan.empty:
        return {
            "lot1_exit_time": None, "lot1_exit_price": None, "lot1_exit_reason": "NO_DATA", "lot1_pnl_points": None,
            "lot2_exit_time": None, "lot2_exit_price": None, "lot2_exit_reason": "NO_DATA", "lot2_pnl_points": None,
            "lot2_final_sl": None,
            "mfe_points": None, "mae_points": None,
            "total_points_2lots": None, "effective_points_per_lot": None,
        }

    for local_i, row in scan.iterrows():
        ts = row["date"]
        high_ = float(row["high"])
        low_ = float(row["low"])
        close_ = float(row["close"])

        # Track MFE/MAE on FUT (using candle extremes vs entry)
        if side == "LONG":
            favorable = high_ - entry_price
            adverse = entry_price - low_
        else:
            favorable = entry_price - low_
            adverse = high_ - entry_price

        max_favorable = max(max_favorable, favorable)
        max_adverse = max(max_adverse, adverse)

        # ---------------------------
        # LOT 1 (fixed TP40 / initial SL)
        # ---------------------------
        if lot1_open:
            if side == "LONG":
                hit_tp = high_ >= lot1_tp
                hit_sl = low_ <= initial_sl

                if hit_tp and hit_sl:
                    # conservative: SL first
                    lot1_open = False
                    lot1_done = True
                    lot1_exit_price = initial_sl
                    lot1_exit_time = ts
                    lot1_exit_reason = "SL_SAME_CANDLE"
                elif hit_sl:
                    lot1_open = False
                    lot1_done = True
                    lot1_exit_price = initial_sl
                    lot1_exit_time = ts
                    lot1_exit_reason = "SL"
                elif hit_tp:
                    lot1_open = False
                    lot1_done = True
                    lot1_exit_price = lot1_tp
                    lot1_exit_time = ts
                    lot1_exit_reason = "TP1"

            else:  # SHORT
                hit_tp = low_ <= lot1_tp
                hit_sl = high_ >= initial_sl

                if hit_tp and hit_sl:
                    lot1_open = False
                    lot1_done = True
                    lot1_exit_price = initial_sl
                    lot1_exit_time = ts
                    lot1_exit_reason = "SL_SAME_CANDLE"
                elif hit_sl:
                    lot1_open = False
                    lot1_done = True
                    lot1_exit_price = initial_sl
                    lot1_exit_time = ts
                    lot1_exit_reason = "SL"
                elif hit_tp:
                    lot1_open = False
                    lot1_done = True
                    lot1_exit_price = lot1_tp
                    lot1_exit_time = ts
                    lot1_exit_reason = "TP1"

            # If lot1 hit TP, activate BE for lot2 (immediately from this candle onward)
            if lot1_done and lot1_exit_reason == "TP1" and lot2_open:
                if side == "LONG":
                    lot2_sl = max(lot2_sl, entry_price)  # move to BE
                else:
                    lot2_sl = min(lot2_sl, entry_price)
                lot2_be_activated = True

        # ---------------------------
        # LOT 2 (runner with trailing)
        # ---------------------------
        if lot2_open:
            # Update trailing only AFTER lot1 TP has happened and from next candles onward
            # (using previous completed candle low/high)
            if lot2_be_activated and local_i >= 1:
                prev_row = scan.iloc[local_i - 1]
                prev_low = float(prev_row["low"])
                prev_high = float(prev_row["high"])

                if side == "LONG":
                    # trail upward only
                    lot2_sl = max(lot2_sl, prev_low)
                else:
                    # trail downward only (for shorts SL is above price)
                    lot2_sl = min(lot2_sl, prev_high)

            # Check lot2 SL hit (no fixed TP)
            if side == "LONG":
                hit_sl2 = low_ <= lot2_sl
                if hit_sl2:
                    lot2_open = False
                    lot2_exit_price = lot2_sl
                    lot2_exit_time = ts
                    lot2_exit_reason = "TRAIL_SL" if lot2_be_activated else "INITIAL_SL"
            else:
                hit_sl2 = high_ >= lot2_sl
                if hit_sl2:
                    lot2_open = False
                    lot2_exit_price = lot2_sl
                    lot2_exit_time = ts
                    lot2_exit_reason = "TRAIL_SL" if lot2_be_activated else "INITIAL_SL"

        # If both lots closed, stop
        if (not lot1_open) and (not lot2_open):
            break

    # EOD fallback exits for any open legs
    if lot1_open or lot2_open:
        last_row = scan.iloc[-1]
        eod_close = float(last_row["close"])
        eod_ts = last_row["date"]

        if lot1_open:
            lot1_open = False
            lot1_exit_price = eod_close
            lot1_exit_time = eod_ts
            lot1_exit_reason = "EOD"

        if lot2_open:
            lot2_open = False
            lot2_exit_price = eod_close
            lot2_exit_time = eod_ts
            lot2_exit_reason = "EOD"

    # PnL in FUT points
    if side == "LONG":
        lot1_pnl = lot1_exit_price - entry_price
        lot2_pnl = lot2_exit_price - entry_price
    else:
        lot1_pnl = entry_price - lot1_exit_price
        lot2_pnl = entry_price - lot2_exit_price

    total_points = lot1_pnl + lot2_pnl
    effective_per_lot = total_points / 2.0

    return {
        "lot1_exit_time": lot1_exit_time,
        "lot1_exit_price": lot1_exit_price,
        "lot1_exit_reason": lot1_exit_reason,
        "lot1_pnl_points": lot1_pnl,

        "lot2_exit_time": lot2_exit_time,
        "lot2_exit_price": lot2_exit_price,
        "lot2_exit_reason": lot2_exit_reason,
        "lot2_pnl_points": lot2_pnl,
        "lot2_final_sl": lot2_sl,

        "mfe_points": max_favorable,
        "mae_points": max_adverse,

        "total_points_2lots": total_points,
        "effective_points_per_lot": effective_per_lot,
    }

def main():
    if not CSV.exists():
        raise RuntimeError("nifty_fut_5m.csv not found. Run fetch_candles.py first.")

    raw = pd.read_csv(CSV)
    raw["date"] = pd.to_datetime(raw["date"])

    prepared = prepare_df(raw)
    signals = generate_signals(raw, target_points=40.0)  # signal generation unchanged

    if not signals:
        print("No signals found.")
        return

    rows = []
    for s in signals:
        sim = simulate_scaleout_trade(prepared, s, target1_points=TARGET1_POINTS)
        row = asdict(s)
        row.update(sim)

        # risk stats
        risk_points = abs(s.fut_entry - s.fut_sl)
        row["risk_points"] = risk_points
        row["tp1_points"] = TARGET1_POINTS
        row["tp1_rr"] = (TARGET1_POINTS / risk_points) if risk_points > 0 else None

        rows.append(row)

    res = pd.DataFrame(rows)
    res.to_csv(OUT, index=False)

    # Summary
    total = len(res)
    net_2lots = res["total_points_2lots"].fillna(0).sum()
    avg_2lots = res["total_points_2lots"].fillna(0).mean() if total else 0
    avg_eff = res["effective_points_per_lot"].fillna(0).mean() if total else 0

    # Define "win" if total 2-lot points > 0
    wins = int((res["total_points_2lots"] > 0).sum())
    losses = int((res["total_points_2lots"] <= 0).sum())
    win_rate = (wins / total * 100) if total else 0

    # How often TP1 got hit
    tp1_hits = int((res["lot1_exit_reason"] == "TP1").sum())
    tp1_hit_rate = (tp1_hits / total * 100) if total else 0

    print(f"Saved results to: {OUT}")
    print(f"Total trades: {total}")
    print(f"Wins (2-lot net > 0): {wins} | Losses: {losses} | Win rate: {win_rate:.2f}%")
    print(f"TP1 hit count: {tp1_hits}/{total} ({tp1_hit_rate:.2f}%)")
    print(f"Net FUT points (2 lots combined): {net_2lots:.2f}")
    print(f"Avg per trade (2 lots combined): {avg_2lots:.2f}")
    print(f"Effective avg points per lot: {avg_eff:.2f}")

    cols = [
        "entry_time", "side", "level_name", "fut_entry", "fut_sl",
        "lot1_exit_reason", "lot1_pnl_points",
        "lot2_exit_reason", "lot2_pnl_points",
        "total_points_2lots", "effective_points_per_lot"
    ]
    print("\nLast trades:")
    print(res[cols].tail(10).to_string(index=False))

if __name__ == "__main__":
    main()