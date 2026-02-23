param(
  [Parameter(Mandatory=$true)]
  [string]$RepoUrl
)

# Run from project root (e.g., C:\Users\siddh\kite-login)
if (-not (Test-Path .git)) {
  git init
}

git add .
git commit -m "Initial commit: NIFTY FUT to ATM options pivot strategy" 2>$null
if ($LASTEXITCODE -ne 0) {
  Write-Host "No commit created (maybe nothing changed). Continuing..."
}

# Ensure branch is main
try { git branch -M main } catch {}

# Add/update origin
$hasOrigin = git remote 2>$null | Select-String -Pattern '^origin$'
if (-not $hasOrigin) {
  git remote add origin $RepoUrl
} else {
  git remote set-url origin $RepoUrl
}

git push -u origin main
