from pathlib import Path
import pandas as pd
from kite_client import load_kite

OUT = Path(__file__).resolve().parent / "instruments.csv"

def main():
    kite = load_kite()
    print("Downloading instruments…")
    inst = kite.instruments()  # list[dict]
    df = pd.DataFrame(inst)
    df.to_csv(OUT, index=False)
    print("Saved:", OUT, "rows:", len(df))

if __name__ == "__main__":
    main()