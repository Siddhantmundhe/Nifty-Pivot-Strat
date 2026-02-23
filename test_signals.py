from pathlib import Path
import pandas as pd
from signal_engine import generate_signals, prepare_df

CSV = Path(__file__).resolve().parent / "nifty_fut_5m.csv"

def main():
    if not CSV.exists():
        raise RuntimeError("nifty_fut_5m.csv not found. Run fetch_candles.py first.")

    df = pd.read_csv(CSV)
    df["date"] = pd.to_datetime(df["date"])

    # Generate signals
    signals = generate_signals(df, target_points=40.0)

    print(f"Total signals found: {len(signals)}")
    print("-" * 100)

    for s in signals[-10:]:  # last 10 signals
        print(
            f"{s.entry_time} | {s.side:<5} | {s.level_name} ({s.level_value:.2f}) | "
            f"Entry={s.fut_entry:.2f} SL={s.fut_sl:.2f} TP={s.fut_tp:.2f}"
        )

if __name__ == "__main__":
    main()