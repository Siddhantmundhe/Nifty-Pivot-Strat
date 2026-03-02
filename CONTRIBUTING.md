# Contributing Guide

## Branching
- Use short-lived feature branches from `main`.
- Keep commits focused and atomic.

## Commit style
- Use imperative commit messages.
- Examples:
  - `Add banknifty profile guardrails`
  - `Fix token load fallback`

## Local checks before push
```powershell
.\.venv\Scripts\python.exe -m py_compile broker\*.py
.\.venv\Scripts\python.exe -m py_compile core\*.py
.\.venv\Scripts\python.exe -m py_compile backtest\*.py
.\.venv\Scripts\python.exe -m py_compile live\*.py
.\.venv\Scripts\python.exe -m py_compile scripts\*.py
```

## Security rules
- Never commit `.env`.
- Never commit `broker/access_token.txt` or `broker/kite_session.json`.
- Never commit generated data/log/report artifacts.

## Pull request checklist
- Scope is clear and minimal.
- Strategy behavior changes are documented in `README.md`.
- Backtest/live-impacting changes include before/after notes.
