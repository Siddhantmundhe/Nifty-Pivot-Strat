from pathlib import Path
import pandas as pd
import numpy as np

CSV = Path(__file__).resolve().parent / "option_paper_backtest_scaleout.csv"

PNL_COL = "opt_net_total_pnl_rupees_2lots"  # use net results
TIME_COL = "entry_time"

def add_time_bucket(ts: pd.Timestamp) -> str:
    if pd.isna(ts):
        return "UNKNOWN"
    t = ts.tz_convert("Asia/Kolkata") if ts.tzinfo is not None else ts
    hhmm = t.hour * 100 + t.minute
    if hhmm < 1100:
        return "09:15-11:00"
    elif hhmm < 1300:
        return "11:00-13:00"
    elif hhmm < 1430:
        return "13:00-14:30"
    else:
        return "14:30-15:30"

def summarize(df: pd.DataFrame, group_cols):
    if df.empty:
        return pd.DataFrame()

    g = df.groupby(group_cols, dropna=False)

    out = g.agg(
        trades=(PNL_COL, "count"),
        wins=(PNL_COL, lambda x: int((x > 0).sum())),
        losses=(PNL_COL, lambda x: int((x <= 0).sum())),
        net_pnl=(PNL_COL, "sum"),
        avg_pnl=(PNL_COL, "mean"),
        median_pnl=(PNL_COL, "median"),
        avg_win=(PNL_COL, lambda x: x[x > 0].mean() if (x > 0).any() else np.nan),
        avg_loss=(PNL_COL, lambda x: x[x <= 0].mean() if (x <= 0).any() else np.nan),
        max_win=(PNL_COL, "max"),
        max_loss=(PNL_COL, "min"),
    ).reset_index()

    out["win_rate_%"] = (out["wins"] / out["trades"] * 100).round(2)
    out["profit_factor"] = out.apply(
        lambda r: (abs(r["avg_win"] * r["wins"]) / abs(r["avg_loss"] * r["losses"]))
        if pd.notna(r["avg_win"]) and pd.notna(r["avg_loss"]) and r["avg_loss"] != 0 and r["losses"] > 0
        else np.nan,
        axis=1
    ).round(2)

    return out.sort_values(["net_pnl", "trades"], ascending=[False, False])

def main():
    if not CSV.exists():
        raise RuntimeError(f"File not found: {CSV}")

    df = pd.read_csv(CSV)

    if PNL_COL not in df.columns:
        raise RuntimeError(f"Expected column not found: {PNL_COL}")

    # parse datetimes
    for c in ["entry_time", "lot1_exit_time", "lot2_exit_time"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # valid trades only
    if "opt_error" in df.columns:
        df = df[df["opt_error"].isna()].copy()

    df["time_bucket"] = df[TIME_COL].apply(add_time_bucket)

    # helper combined exit pattern
    if "lot1_exit_reason" in df.columns and "lot2_exit_reason" in df.columns:
        df["exit_pattern"] = df["lot1_exit_reason"].astype(str) + " | " + df["lot2_exit_reason"].astype(str)

    total = len(df)
    net = df[PNL_COL].sum()
    wins = int((df[PNL_COL] > 0).sum())
    losses = int((df[PNL_COL] <= 0).sum())

    print(f"Valid trades: {total}")
    print(f"Net PnL (2 lots, net): ₹{net:.2f}")
    print(f"Wins: {wins} | Losses: {losses} | Win rate: {wins/total*100:.2f}%")

    print("\n=== By Side ===")
    by_side = summarize(df, ["side"])
    print(by_side.to_string(index=False))

    if "level_name" in df.columns:
        print("\n=== By Level ===")
        by_level = summarize(df, ["level_name"])
        print(by_level.to_string(index=False))

        print("\n=== By Side + Level ===")
        by_side_level = summarize(df, ["side", "level_name"])
        print(by_side_level.to_string(index=False))

    print("\n=== By Time Bucket ===")
    by_time = summarize(df, ["time_bucket"])
    print(by_time.to_string(index=False))

    if "exit_pattern" in df.columns:
        print("\n=== By Exit Pattern ===")
        by_exit = summarize(df, ["exit_pattern"])
        print(by_exit.to_string(index=False))

    # Optional: save reports
    out_dir = CSV.parent
    by_side.to_csv(out_dir / "report_by_side.csv", index=False)
    by_time.to_csv(out_dir / "report_by_time_bucket.csv", index=False)
    if "level_name" in df.columns:
        by_level.to_csv(out_dir / "report_by_level.csv", index=False)
        by_side_level.to_csv(out_dir / "report_by_side_level.csv", index=False)
    if "exit_pattern" in df.columns:
        by_exit.to_csv(out_dir / "report_by_exit_pattern.csv", index=False)

    print("\nSaved breakdown CSV reports in the same folder.")

if __name__ == "__main__":
    main()