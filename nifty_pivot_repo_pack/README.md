# Nifty Pivot Strategy (NIFTY FUT -> Weekly ATM Options)

Signals are generated on **NIFTY Futures (5m candles)** and executed on **NIFTY weekly ATM options**.

## Current tested setup (v2 hypothesis)
- Keep LONG: `R1`, `R2`
- Keep SHORT: `S1`
- Disable SHORT: `S2`
- Entry cutoff: `14:00`
- Scale-out: 2 lots
  - Lot1 books at TP1 (+40 FUT points equivalent rule in your backtest flow)
  - Lot2 trails (BE after TP1)

## Quickstart

### 1) Create environment
```powershell
py -3.9 -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
pip install -r requirements.txt
```

### 2) Add secrets in `.env` (do NOT commit)
```env
KITE_API_KEY=your_key
KITE_API_SECRET=your_secret
```

### 3) Login and create session
```powershell
python login_server.py
```
Open `http://127.0.0.1:5000/` and finish Kite login.

### 4) Download instruments
```powershell
python download_instruments.py
```

### 5) Backtest pipeline
```powershell
python fetch_candles.py
python backtest_fut_exits_scaleout.py
python paper_option_backtest_scaleout.py
python analyze_option_backtest.py
python analyze_filtered_variants.py
```

## Security
- Never commit `.env`
- Never commit `kite_session.json`
- If a key/secret was exposed anywhere, regenerate it in Zerodha Developer Console immediately.

## Notes
This project is for education/research. Live deployment should only happen after paper testing, cost/slippage sensitivity testing, and strict risk management.
