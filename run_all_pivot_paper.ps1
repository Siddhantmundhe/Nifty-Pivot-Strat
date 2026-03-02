$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $root '.venv\Scripts\python.exe'

if (!(Test-Path $python)) {
    throw "Python not found at $python"
}

function Start-Runner {
    param(
        [string]$Title,
        [string]$Command
    )

    Start-Process powershell -ArgumentList @(
        '-NoExit',
        '-Command',
        "cd '$root'; `$Host.UI.RawUI.WindowTitle = '$Title'; $Command"
    ) | Out-Null
}

Start-Runner -Title 'Pivot NIFTY Paper' -Command "& '$python' '.\\live\\live_runner_zerodha_paper.py' --profile nifty --mode paper"
Start-Runner -Title 'Pivot BANKNIFTY Paper' -Command "& '$python' '.\\live\\live_runner_zerodha_paper.py' --profile banknifty --mode paper"

Write-Host 'Launched pivot paper runners in separate terminals.'
