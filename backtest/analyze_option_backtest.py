# analyze_option_backtest.py
from __future__ import annotations

import os
import argparse
from pathlib import Path
from typing import Optional

import pandas as pd

proj_root = Path(__file__).resolve().parent.parent
CSV_PATH = proj_root / "option_paper_backtest_scaleout.csv"


def _parse_profile_arg(default: str = "NIFTY") -> str:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--profile", choices=["nifty", "banknifty"])
    args, _ = parser.parse_known_args()
    val = (args.profile or os.getenv("STRATEGY_PROFILE", default)).strip().upper()
    return "BANKNIFTY" if "BANK" in val else "NIFTY"


def apply_strategy_profile(profile: str) -> None:
    global CSV_PATH
    if profile == "BANKNIFTY":
        CSV_PATH = proj_root / "option_paper_backtest_scaleout_banknifty.csv"
    else:
        CSV_PATH = proj_root / "option_paper_backtest_scaleout.csv"


def detect_pnl_column(df: pd.DataFrame) -> Optional[str]:
    candidates = [
        "opt_net_total_pnl_rupees_2lots",
        "opt_net_pnl_rupees_2lots",
        "opt_net_total_pnl_rupees",
        "opt_net_pnl_rupees",
        "net_pnl_rupees",
        "pnl",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None


def detect_time_column(df: pd.DataFrame) -> Optional[str]:
    for c in ["entry_time", "opt_entry_time", "timestamp", "time", "date"]:
        if c in df.columns:
            return c
    return None


def main():
    profile = _parse_profile_arg()
    apply_strategy_profile(profile)
    print(f"Profile: {profile} | Input CSV: {CSV_PATH}")
    if not os.path.exists(CSV_PATH):
        raise RuntimeError(f"CSV not found: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    print("Loaded rows:", len(df))
    print("Columns:", list(df.columns))

    if df.empty:
        raise RuntimeError(
            "Option backtest CSV is empty. Run paper_option_backtest_scaleout.py successfully first."
        )

    if "opt_error" in df.columns:
        err = df["opt_error"].astype(str).str.strip()
        nonempty_err = err.ne("") & err.ne("nan") & err.notna()
        if len(df) > 0 and nonempty_err.all():
            top_errors = err.value_counts(dropna=False).head(10)
            raise RuntimeError(
                "All rows are option mapping/backtest errors. Fix upstream issue first.\n"
                f"Top errors:\n{top_errors.to_string()}"
            )

    pnl_col = detect_pnl_column(df)
    if pnl_col is None:
        raise RuntimeError(
            "Expected PnL column not found.\n"
            f"Available columns: {list(df.columns)}"
        )

    print(f"Using PnL column: {pnl_col}")

    df[pnl_col] = pd.to_numeric(df[pnl_col], errors="coerce")
    valid = df.dropna(subset=[pnl_col]).copy()

    if valid.empty:
        raise RuntimeError(
            f"No valid PnL rows found in column '{pnl_col}'. "
            "Check upstream option mapping/backtest output."
        )

    time_col = detect_time_column(valid)
    if time_col and time_col in valid.columns:
        valid[time_col] = pd.to_datetime(valid[time_col], errors="coerce")
        valid = valid.sort_values(time_col).reset_index(drop=True)

    pnl = valid[pnl_col]

    total_trades = len(valid)
    wins = int((pnl > 0).sum()) # type: ignore
    losses = int((pnl < 0).sum()) # type: ignore
    flats = int((pnl == 0).sum()) # type: ignore

    valid["equity"] = pnl.cumsum()
    valid["peak"] = valid["equity"].cummax()
    valid["drawdown"] = valid["equity"] - valid["peak"]

    print("\n===== OPTION BACKTEST SUMMARY =====")
    print(f"Total valid trades : {total_trades}")
    print(f"Wins / Loss / Flat : {wins} / {losses} / {flats}")
    print(f"Win rate           : {(wins/total_trades*100 if total_trades else 0):.2f}%")
    print(f"Gross PnL          : Rs {pnl.sum():,.2f}")
    print(f"Average PnL/trade  : Rs {pnl.mean():,.2f}")
    print(f"Best trade         : Rs {pnl.max():,.2f}")
    print(f"Worst trade        : Rs {pnl.min():,.2f}")
    print(f"Max Drawdown       : Rs {valid['drawdown'].min():,.2f}")

    preview_cols = [c for c in ["entry_time", "opt_entry_time", "side", "opt_symbol", pnl_col, "opt_error"] if c in valid.columns]
    print("\nRecent valid trades:")
    print(valid[preview_cols].tail(10).to_string(index=False))

    analyzed_path = os.path.splitext(CSV_PATH)[0] + "_analyzed.csv"
    valid.to_csv(analyzed_path, index=False)
    print(f"\nSaved analyzed file to: {analyzed_path}")


if __name__ == "__main__":
    main()
