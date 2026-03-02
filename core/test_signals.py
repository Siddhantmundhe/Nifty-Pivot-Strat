import sys
import os
import argparse
from pathlib import Path

# Add project root to sys.path so 'core' module can be found
proj_root = Path(__file__).resolve().parent.parent
if str(proj_root) not in sys.path:
    sys.path.insert(0, str(proj_root))

import pandas as pd
from core.signal_engine import generate_signals, prepare_df


def _parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--profile", choices=["nifty", "banknifty"])
    parser.add_argument("--csv")
    args, _ = parser.parse_known_args()
    return args


def _resolve_csv_path() -> Path:
    args = _parse_args()
    if args.csv:
        return Path(args.csv)

    env_csv = os.getenv("SIGNAL_CSV_PATH", "").strip()
    if env_csv:
        return Path(env_csv)

    profile = (args.profile or os.getenv("STRATEGY_PROFILE", "nifty")).strip().upper()
    if "BANK" in profile:
        return proj_root / "banknifty_fut_5m.csv"

    return proj_root / "nifty_fut_5m.csv"


def main():
    csv_path = _resolve_csv_path()

    if not csv_path.exists():
        print(f"Error: Could not find {csv_path}")
        return

    print(f"Using candles CSV: {csv_path}")
    df = pd.read_csv(csv_path)

    # Prepare dataframe (adds pivots etc.)
    prepared = prepare_df(df)

    # Generate signals (returns list[Signal])
    signals = generate_signals(prepared, target_points=40.0)

    print(f"Total signals found: {len(signals)}")
    print("-" * 100)

    # Print last 10 signals
    for s in signals[-10:]:
        print(
            f"{s.entry_time} | {s.side:<5} | {s.level_name} ({s.level_value:.2f}) | "
            f"Entry={s.entry:.2f} SL={s.sl:.2f} TP={s.tp:.2f}"
        )


if __name__ == "__main__":
    main()
