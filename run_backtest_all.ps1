Param(
    [ValidateSet("nifty", "banknifty", "both")]
    [string]$Profile = "both"
)

$py = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $py)) {
    throw "Python venv not found at $py"
}

& $py scripts\download_instruments.py
& $py scripts\fetch_candles.py

function Run-Profile([string]$p) {
    & $py backtest\backtest_fut_exits_scaleout.py --profile $p
    & $py backtest\paper_option_backtest_scaleout.py --profile $p
    & $py backtest\analyze_option_backtest.py --profile $p
    & $py backtest\analyze_filtered_variants.py --profile $p
}

if ($Profile -eq "both") {
    Run-Profile "nifty"
    Run-Profile "banknifty"
} else {
    Run-Profile $Profile
}
