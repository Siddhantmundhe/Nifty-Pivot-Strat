$py = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $py)) {
    throw "Python venv not found at $py"
}

Start-Process powershell -ArgumentList "-NoExit", "-Command", "& $py live\live_runner_zerodha_paper.py --profile nifty"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& $py live\live_runner_zerodha_paper.py --profile banknifty"
