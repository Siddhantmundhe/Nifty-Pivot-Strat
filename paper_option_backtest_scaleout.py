from pathlib import Path
import pandas as pd
from kite_client import load_kite

BASE = Path(__file__).resolve().parent
INSTR_CSV = BASE / "instruments.csv"
FUT_SCALEOUT_CSV = BASE / "fut_backtest_scaleout_results.csv"
OUT_CSV = BASE / "option_paper_backtest_scaleout.csv"

LOT_SIZE_DEFAULT = 75  # fallback only

# -----------------------------
# Cost model (tune these)
# -----------------------------
SLIPPAGE_PER_SIDE = 0.50  # ₹ per option unit, per transaction side (entry or exit)
CHARGES_PER_LOT_ROUNDTRIP = 60.0  # ₹ estimated charges for 1 lot buy+sell (approx)

def round_to_50(x: float) -> int:
    return int(round(float(x) / 50.0) * 50)

def load_instruments():
    df = pd.read_csv(INSTR_CSV)
    df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")
    return df

def normalize_strike_col(df: pd.DataFrame, col: str = "strike") -> pd.Series:
    s = pd.to_numeric(df[col], errors="coerce")
    non_null = s.dropna()
    if not non_null.empty and non_null.median() > 100000:
        s = s / 100.0
    return s

def get_nifty_option_for_trade(inst_df: pd.DataFrame, entry_ts, strike: int, opt_type: str):
    trade_date = pd.Timestamp(entry_ts).date()

    opt = inst_df[
        (inst_df["exchange"] == "NFO") &
        (inst_df["segment"] == "NFO-OPT") &
        (inst_df["name"] == "NIFTY") &
        (inst_df["instrument_type"] == opt_type)
    ].copy()

    if opt.empty:
        raise RuntimeError(f"No NIFTY {opt_type} options found in instruments.csv")

    opt["strike_norm"] = normalize_strike_col(opt, "strike")
    opt = opt[opt["expiry"].dt.date >= trade_date]

    if opt.empty:
        raise RuntimeError(f"No active NIFTY {opt_type} options on/after {trade_date}")

    nearest_expiry = opt["expiry"].dt.date.min()
    same_exp = opt[opt["expiry"].dt.date == nearest_expiry].copy()

    exact = same_exp[same_exp["strike_norm"] == strike]
    if exact.empty:
        same_exp["dist"] = (same_exp["strike_norm"] - strike).abs()
        same_exp = same_exp.sort_values("dist")
        if same_exp.empty:
            raise RuntimeError(f"No {opt_type} options found on expiry {nearest_expiry}")
        row = same_exp.iloc[0]
    else:
        row = exact.iloc[0]

    lot_size = row.get("lot_size", LOT_SIZE_DEFAULT)
    if pd.isna(lot_size):
        lot_size = LOT_SIZE_DEFAULT

    return {
        "opt_token": int(row["instrument_token"]),
        "opt_symbol": str(row["tradingsymbol"]),
        "opt_expiry": row["expiry"].date(),
        "opt_strike": float(row["strike_norm"]),
        "opt_type": str(row["instrument_type"]),
        "lot_size": int(lot_size),
    }

def fetch_option_day_5m(kite, instrument_token: int, any_ts) -> pd.DataFrame:
    ts = pd.Timestamp(any_ts)
    start = ts.normalize()
    end = start + pd.Timedelta(days=1)

    candles = kite.historical_data(
        instrument_token=instrument_token,
        from_date=start.to_pydatetime(),
        to_date=end.to_pydatetime(),
        interval="5minute",
        continuous=False,
        oi=False,
    )

    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)

def candle_at_or_after(df: pd.DataFrame, ts):
    if df.empty:
        return None
    ts = pd.Timestamp(ts)
    x = df[df["date"] >= ts]
    if x.empty:
        return None
    return x.iloc[0]

def apply_long_option_slippage(entry_price_raw: float, exit_price_raw: float):
    """
    Long option trade:
      - Buy entry gets worse => +slippage
      - Sell exit gets worse => -slippage
    """
    entry_exec = entry_price_raw + SLIPPAGE_PER_SIDE
    exit_exec = max(0.0, exit_price_raw - SLIPPAGE_PER_SIDE)
    return entry_exec, exit_exec

def main():
    if not FUT_SCALEOUT_CSV.exists():
        raise RuntimeError("fut_backtest_scaleout_results.csv not found. Run backtest_fut_exits_scaleout.py first.")

    kite = load_kite()
    inst_df = load_instruments()
    fut = pd.read_csv(FUT_SCALEOUT_CSV)

    # Parse times
    for c in ["entry_time", "lot1_exit_time", "lot2_exit_time"]:
        if c in fut.columns:
            fut[c] = pd.to_datetime(fut[c], errors="coerce")

    rows = []

    for _, tr in fut.iterrows():
        try:
            side = str(tr["side"]).upper()
            entry_time = pd.Timestamp(tr["entry_time"])
            fut_entry = float(tr["fut_entry"])

            if pd.isna(entry_time):
                raise RuntimeError("Missing entry_time")

            opt_type = "CE" if side == "LONG" else "PE"
            strike = round_to_50(fut_entry)

            meta = get_nifty_option_for_trade(inst_df, entry_time, strike, opt_type)

            opt_df = fetch_option_day_5m(kite, meta["opt_token"], entry_time)
            if opt_df.empty:
                raise RuntimeError("No option candles returned")

            c_entry = candle_at_or_after(opt_df, entry_time)
            if c_entry is None:
                raise RuntimeError("No option entry candle found at/after entry_time")

            lot1_exit_time = pd.Timestamp(tr["lot1_exit_time"]) if "lot1_exit_time" in tr else pd.NaT
            c_lot1 = candle_at_or_after(opt_df, lot1_exit_time) if pd.notna(lot1_exit_time) else None
            if c_lot1 is None:
                raise RuntimeError("No lot1 option exit candle found at/after lot1_exit_time")

            lot2_exit_time = pd.Timestamp(tr["lot2_exit_time"]) if "lot2_exit_time" in tr else pd.NaT
            c_lot2 = candle_at_or_after(opt_df, lot2_exit_time) if pd.notna(lot2_exit_time) else None
            if c_lot2 is None:
                raise RuntimeError("No lot2 option exit candle found at/after lot2_exit_time")

            # Raw proxy prices
            opt_entry_price_raw = float(c_entry["open"])   # both lots same entry proxy
            lot1_exit_price_raw = float(c_lot1["close"])
            lot2_exit_price_raw = float(c_lot2["close"])
            lot_size = int(meta["lot_size"])

            # Slippage-adjusted executable prices (long option trades)
            opt_entry_price_exec_l1, lot1_exit_price_exec = apply_long_option_slippage(
                opt_entry_price_raw, lot1_exit_price_raw
            )
            opt_entry_price_exec_l2, lot2_exit_price_exec = apply_long_option_slippage(
                opt_entry_price_raw, lot2_exit_price_raw
            )

            # Gross PnL (raw)
            lot1_gross_per_unit = lot1_exit_price_raw - opt_entry_price_raw
            lot2_gross_per_unit = lot2_exit_price_raw - opt_entry_price_raw
            lot1_gross_rupees = lot1_gross_per_unit * lot_size
            lot2_gross_rupees = lot2_gross_per_unit * lot_size
            gross_total_2lots = lot1_gross_rupees + lot2_gross_rupees

            # Slippage-adjusted PnL
            lot1_slip_per_unit = lot1_exit_price_exec - opt_entry_price_exec_l1
            lot2_slip_per_unit = lot2_exit_price_exec - opt_entry_price_exec_l2
            lot1_pnl_after_slippage = lot1_slip_per_unit * lot_size
            lot2_pnl_after_slippage = lot2_slip_per_unit * lot_size
            total_after_slippage_2lots = lot1_pnl_after_slippage + lot2_pnl_after_slippage

            # Charges (1 completed roundtrip per lot)
            lot1_charges = CHARGES_PER_LOT_ROUNDTRIP
            lot2_charges = CHARGES_PER_LOT_ROUNDTRIP
            total_charges_2lots = lot1_charges + lot2_charges

            # Net
            lot1_net = lot1_pnl_after_slippage - lot1_charges
            lot2_net = lot2_pnl_after_slippage - lot2_charges
            net_total_2lots = lot1_net + lot2_net
            net_effective_per_lot = net_total_2lots / 2.0

            rows.append({
                **tr.to_dict(),

                "opt_symbol": meta["opt_symbol"],
                "opt_token": meta["opt_token"],
                "opt_expiry": meta["opt_expiry"],
                "opt_strike": meta["opt_strike"],
                "opt_type": meta["opt_type"],
                "lot_size": lot_size,

                "opt_entry_time_used": c_entry["date"],
                "opt_entry_price_raw": opt_entry_price_raw,

                "opt_lot1_exit_time_used": c_lot1["date"],
                "opt_lot1_exit_price_raw": lot1_exit_price_raw,

                "opt_lot2_exit_time_used": c_lot2["date"],
                "opt_lot2_exit_price_raw": lot2_exit_price_raw,

                # executable prices after slippage
                "opt_lot1_entry_price_exec": opt_entry_price_exec_l1,
                "opt_lot1_exit_price_exec": lot1_exit_price_exec,
                "opt_lot2_entry_price_exec": opt_entry_price_exec_l2,
                "opt_lot2_exit_price_exec": lot2_exit_price_exec,

                # gross
                "opt_lot1_gross_pnl_rupees": lot1_gross_rupees,
                "opt_lot2_gross_pnl_rupees": lot2_gross_rupees,
                "opt_gross_total_pnl_rupees_2lots": gross_total_2lots,

                # after slippage
                "opt_lot1_pnl_after_slippage_rupees": lot1_pnl_after_slippage,
                "opt_lot2_pnl_after_slippage_rupees": lot2_pnl_after_slippage,
                "opt_total_pnl_after_slippage_rupees_2lots": total_after_slippage_2lots,

                # charges
                "opt_lot1_charges_rupees": lot1_charges,
                "opt_lot2_charges_rupees": lot2_charges,
                "opt_total_charges_rupees_2lots": total_charges_2lots,

                # net
                "opt_lot1_net_pnl_rupees": lot1_net,
                "opt_lot2_net_pnl_rupees": lot2_net,
                "opt_net_total_pnl_rupees_2lots": net_total_2lots,
                "opt_net_effective_pnl_per_lot_rupees": net_effective_per_lot,

                "opt_error": None,
            })

        except Exception as e:
            rows.append({
                **tr.to_dict(),
                "opt_error": str(e),
            })

    res = pd.DataFrame(rows)
    res.to_csv(OUT_CSV, index=False)

    print(f"Saved option scale-out results to: {OUT_CSV}")
    print(f"Cost assumptions -> Slippage/side: ₹{SLIPPAGE_PER_SIDE:.2f}, Charges/lot roundtrip: ₹{CHARGES_PER_LOT_ROUNDTRIP:.2f}")

    if "opt_error" not in res.columns:
        print("No opt_error column found unexpectedly.")
        return

    ok = res[res["opt_error"].isna()].copy()
    errs = res[res["opt_error"].notna()].copy()

    if ok.empty:
        print("No valid mapped option trades.")
        if not errs.empty:
            print(errs[["entry_time", "side", "opt_error"]].to_string(index=False))
        return

    total = len(ok)

    # Gross summary
    gross_col = "opt_gross_total_pnl_rupees_2lots"
    gross_net = ok[gross_col].sum()
    gross_avg = ok[gross_col].mean()
    gross_wins = int((ok[gross_col] > 0).sum())
    gross_losses = int((ok[gross_col] <= 0).sum())
    gross_win_rate = gross_wins / total * 100 if total else 0.0

    # Net summary
    net_col = "opt_net_total_pnl_rupees_2lots"
    net_total = ok[net_col].sum()
    net_avg = ok[net_col].mean()
    net_eff_avg = ok["opt_net_effective_pnl_per_lot_rupees"].mean()
    net_wins = int((ok[net_col] > 0).sum())
    net_losses = int((ok[net_col] <= 0).sum())
    net_win_rate = net_wins / total * 100 if total else 0.0

    total_charges = ok["opt_total_charges_rupees_2lots"].sum()
    total_slippage_impact = (ok["opt_gross_total_pnl_rupees_2lots"] - ok["opt_total_pnl_after_slippage_rupees_2lots"]).sum()

    # Date range for clarity
    min_dt = ok["entry_time"].min()
    max_dt = ok["entry_time"].max()

    print(f"\nDate range tested: {min_dt}  ->  {max_dt}")
    print(f"Valid option trades: {total}")

    print("\n--- Gross (before costs) ---")
    print(f"Wins: {gross_wins} | Losses: {gross_losses} | Win rate: {gross_win_rate:.2f}%")
    print(f"Net option PnL (2 lots total): ₹{gross_net:.2f}")
    print(f"Avg/trade (2 lots total): ₹{gross_avg:.2f}")

    print("\n--- Net (after slippage + charges) ---")
    print(f"Wins: {net_wins} | Losses: {net_losses} | Win rate: {net_win_rate:.2f}%")
    print(f"Net option PnL (2 lots total): ₹{net_total:.2f}")
    print(f"Avg/trade (2 lots total): ₹{net_avg:.2f}")
    print(f"Effective avg per-lot PnL: ₹{net_eff_avg:.2f}")

    print("\n--- Cost impact summary ---")
    print(f"Total slippage impact (2 lots aggregate): ₹{total_slippage_impact:.2f}")
    print(f"Total estimated charges (2 lots aggregate): ₹{total_charges:.2f}")
    print(f"Total cost impact: ₹{(total_slippage_impact + total_charges):.2f}")

    show_cols = [
        "entry_time", "side", "opt_symbol",
        "opt_gross_total_pnl_rupees_2lots",
        "opt_total_pnl_after_slippage_rupees_2lots",
        "opt_total_charges_rupees_2lots",
        "opt_net_total_pnl_rupees_2lots",
        "lot1_exit_reason", "lot2_exit_reason"
    ]
    show_cols = [c for c in show_cols if c in ok.columns]
    print("\nLast valid trades (gross -> net):")
    print(ok[show_cols].tail(10).to_string(index=False))

    if not errs.empty:
        print("\nTrades with option mapping/fetch errors:")
        err_cols = [c for c in ["entry_time", "side", "fut_entry", "lot1_exit_time", "lot2_exit_time", "opt_error"] if c in errs.columns]
        print(errs[err_cols].to_string(index=False))

if __name__ == "__main__":
    main()