from pathlib import Path
import pandas as pd
import numpy as np

CSV = Path(__file__).resolve().parent / "option_paper_backtest_scaleout.csv"
PNL_COL = "opt_net_total_pnl_rupees_2lots"

def parse_dt_col(df: pd.DataFrame, col: str):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

def ist_time_parts(ts):
    if pd.isna(ts):
        return None, None
    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.tz_convert("Asia/Kolkata")
    return ts.hour, ts.minute

def hhmm_to_str(hhmm: int) -> str:
    h = hhmm // 100
    m = hhmm % 100
    return f"{h:02d}:{m:02d}"

def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    hours = []
    mins = []
    hhmm_vals = []
    for ts in df["entry_time"]:
        hm = ist_time_parts(ts)
        if hm == (None, None):
            hours.append(np.nan)
            mins.append(np.nan)
            hhmm_vals.append(np.nan)
        else:
            h, m = hm
            hours.append(h)
            mins.append(m)
            hhmm_vals.append(h * 100 + m)
    df["entry_hour"] = hours
    df["entry_minute"] = mins
    df["entry_hhmm"] = hhmm_vals
    return df

def summarize(df: pd.DataFrame, name: str) -> dict:
    if df.empty:
        return {
            "variant": name,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate_%": 0.0,
            "net_pnl": 0.0,
            "avg_pnl": 0.0,
            "median_pnl": 0.0,
            "avg_win": np.nan,
            "avg_loss": np.nan,
            "max_win": np.nan,
            "max_loss": np.nan,
            "profit_factor": np.nan,
        }

    pnl = df[PNL_COL].astype(float)
    wins_mask = pnl > 0
    losses_mask = ~wins_mask

    wins = int(wins_mask.sum())
    losses = int(losses_mask.sum())
    trades = len(df)

    gross_profit = pnl[wins_mask].sum()
    gross_loss_abs = abs(pnl[losses_mask].sum())

    pf = np.nan
    if gross_loss_abs > 0:
        pf = gross_profit / gross_loss_abs

    return {
        "variant": name,
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "win_rate_%": round((wins / trades) * 100, 2) if trades else 0.0,
        "net_pnl": round(float(pnl.sum()), 2),
        "avg_pnl": round(float(pnl.mean()), 2),
        "median_pnl": round(float(pnl.median()), 2),
        "avg_win": round(float(pnl[wins_mask].mean()), 2) if wins > 0 else np.nan,
        "avg_loss": round(float(pnl[losses_mask].mean()), 2) if losses > 0 else np.nan,
        "max_win": round(float(pnl.max()), 2),
        "max_loss": round(float(pnl.min()), 2),
        "profit_factor": round(float(pf), 2) if pd.notna(pf) else np.nan,
    }

def apply_cutoff(df: pd.DataFrame, cutoff_hhmm: int) -> pd.DataFrame:
    # keep entries <= cutoff
    return df[df["entry_hhmm"] <= cutoff_hhmm].copy()

def exclude_short_s2(df: pd.DataFrame) -> pd.DataFrame:
    if not {"side", "level_name"}.issubset(df.columns):
        return df.copy()
    mask = ~((df["side"].astype(str).str.upper() == "SHORT") & (df["level_name"].astype(str).str.upper() == "S2"))
    return df[mask].copy()

def only_short_s1(df: pd.DataFrame) -> pd.DataFrame:
    if not {"side", "level_name"}.issubset(df.columns):
        return df.iloc[0:0].copy()
    mask = (df["side"].astype(str).str.upper() == "SHORT") & (df["level_name"].astype(str).str.upper() == "S1")
    return df[mask].copy()

def only_longs(df: pd.DataFrame) -> pd.DataFrame:
    if "side" not in df.columns:
        return df.iloc[0:0].copy()
    return df[df["side"].astype(str).str.upper() == "LONG"].copy()

def only_shorts(df: pd.DataFrame) -> pd.DataFrame:
    if "side" not in df.columns:
        return df.iloc[0:0].copy()
    return df[df["side"].astype(str).str.upper() == "SHORT"].copy()

def print_variant_detail(df: pd.DataFrame, name: str, max_rows: int = 10):
    print(f"\n--- {name} (sample rows) ---")
    if df.empty:
        print("No trades.")
        return
    cols = [
        "entry_time", "side", "level_name",
        "lot1_exit_reason", "lot2_exit_reason",
        PNL_COL
    ]
    cols = [c for c in cols if c in df.columns]
    print(df[cols].tail(max_rows).to_string(index=False))

def main():
    if not CSV.exists():
        raise RuntimeError(f"File not found: {CSV}")

    df = pd.read_csv(CSV)
    df = parse_dt_col(df, "entry_time")

    if PNL_COL not in df.columns:
        raise RuntimeError(f"Missing required column: {PNL_COL}")

    if "opt_error" in df.columns:
        df = df[df["opt_error"].isna()].copy()

    df = add_time_features(df)

    # Build variants
    variants = []

    # Baseline
    variants.append(("Baseline", df.copy()))

    # Time cutoffs only
    for cutoff in [1400, 1415, 1430]:
        variants.append((f"Cutoff <= {hhmm_to_str(cutoff)}", apply_cutoff(df, cutoff)))

    # Filter only
    variants.append(("Exclude SHORT S2", exclude_short_s2(df)))
    variants.append(("Only SHORT S1", only_short_s1(df)))
    variants.append(("Only SHORTs", only_shorts(df)))
    variants.append(("Only LONGs", only_longs(df)))

    # Combined filters
    base_no_s2 = exclude_short_s2(df)
    for cutoff in [1400, 1415, 1430]:
        variants.append((f"Exclude SHORT S2 + Cutoff <= {hhmm_to_str(cutoff)}", apply_cutoff(base_no_s2, cutoff)))

    # S1-focused cutoff variants
    short_s1 = only_short_s1(df)
    for cutoff in [1400, 1415, 1430]:
        variants.append((f"Only SHORT S1 + Cutoff <= {hhmm_to_str(cutoff)}", apply_cutoff(short_s1, cutoff)))

    # Summaries
    summary_rows = [summarize(vdf, name) for name, vdf in variants]
    summary_df = pd.DataFrame(summary_rows)

    # Sort by net pnl then profit factor then trades
    summary_df = summary_df.sort_values(
        by=["net_pnl", "profit_factor", "trades"],
        ascending=[False, False, False]
    ).reset_index(drop=True)

    print("\n=== Filtered Variant Comparison (NET PnL, 2 lots) ===")
    print(summary_df.to_string(index=False))

    # Save summary
    out_path = CSV.parent / "report_filtered_variants.csv"
    summary_df.to_csv(out_path, index=False)
    print(f"\nSaved: {out_path}")

    # Also save top 3 variant trade lists for inspection
    top3 = summary_df.head(3)["variant"].tolist()
    name_to_df = {name: vdf for name, vdf in variants}

    for i, name in enumerate(top3, start=1):
        vdf = name_to_df[name].copy()
        safe_name = (
            name.replace(" ", "_")
                .replace(":", "")
                .replace("<=", "le")
                .replace("|", "_")
                .replace("/", "_")
        )
        detail_path = CSV.parent / f"top_variant_{i}_{safe_name}.csv"
        vdf.to_csv(detail_path, index=False)
        print(f"Saved top variant trades: {detail_path}")
        print_variant_detail(vdf, name)

    # Quick recommendation helper
    print("\n=== Quick takeaways ===")
    best = summary_df.iloc[0]
    print(
        f"Top variant by net PnL: {best['variant']} | "
        f"Trades={int(best['trades'])}, WinRate={best['win_rate_%']}%, "
        f"Net=₹{best['net_pnl']}, PF={best['profit_factor']}"
    )

    # Baseline comparison
    baseline = summary_df[summary_df["variant"] == "Baseline"]
    if not baseline.empty:
        b = baseline.iloc[0]
        delta = best["net_pnl"] - b["net_pnl"]
        print(f"Vs baseline net PnL delta: ₹{delta:.2f}")

if __name__ == "__main__":
    main()