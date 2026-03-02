# Pivot Point Options Lab (NIFTY / BANKNIFTY)

Signals are generated from index futures 5-minute candles and mapped to index weekly options for paper/live-forward execution.

## Repo layout
- `backtest/`: historical strategy simulation and analysis scripts.
- `broker/`: Zerodha auth/session utilities.
- `core/`: indicators and signal engine.
- `live/`: intraday paper/live runner.
- `scripts/`: data fetch + helper scripts.
- `docs/`: internal design/change notes.

## Strategy snapshot
- Underlying: NIFTY FUT / BANKNIFTY FUT (5m).
- Execution instrument: nearest weekly index options.
- Scale-out model: partial target booking + managed exit.
- Profile behavior:
  - `nifty`: tighter target model.
  - `banknifty`: wider target model, PP-enabled variants.

## Setup
1. Create and activate venv:
```powershell
py -3.9 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. Create `.env` from `.env.example`:
```powershell
Copy-Item .env.example .env
```
Then set:
```env
KITE_API_KEY=...
KITE_API_SECRET=...
```

3. Generate daily token/session:
```powershell
python broker/generate_kite_token.py
```

4. Verify auth:
```powershell
python broker/check_kite_auth.py
```

## Backtest workflow
```powershell
python scripts/download_instruments.py
python scripts/fetch_candles.py

python backtest/backtest_fut_exits_scaleout.py --profile nifty
python backtest/paper_option_backtest_scaleout.py --profile nifty
python backtest/analyze_option_backtest.py --profile nifty
python backtest/analyze_filtered_variants.py --profile nifty

python backtest/backtest_fut_exits_scaleout.py --profile banknifty
python backtest/paper_option_backtest_scaleout.py --profile banknifty
python backtest/analyze_option_backtest.py --profile banknifty
python backtest/analyze_filtered_variants.py --profile banknifty
```

## Live paper run
```powershell
python live/live_runner_zerodha_paper.py --profile nifty
python live/live_runner_zerodha_paper.py --profile banknifty
```

## Daily operating checklist
1. Refresh token (`broker/generate_kite_token.py`) before market open.
2. Validate auth (`broker/check_kite_auth.py`).
3. Ensure latest instruments/candles are available if your flow depends on local files.
4. Start only paper mode first.
5. Keep `STOP_TRADING.txt` ready for emergency manual kill-switch.
6. Review generated logs after market close.

## Security and repo hygiene
- Never commit secrets or session files.
- Rotate API key/secret immediately if exposed.
- Keep generated CSV/log/report outputs out of git.
- Use GitHub Actions CI (`.github/workflows/ci.yml`) to catch broken pushes.

## Collaboration
- Contribution process: `CONTRIBUTING.md`
- Change history: `CHANGELOG.md`
- Internal patch notes: `docs/PATCH_V2_FILTERS.md`

## Risk note
This code is for research and educational use. Options trading can cause significant loss. Validate with paper-forward testing before any live deployment.
