$ROOT = Split-Path $PSScriptRoot -Parent
$PY   = Join-Path $ROOT ".venv\Scripts\python.exe"
$LOG  = Join-Path $ROOT "data\poly_5m_recorder.log"

Set-Location $ROOT
Write-Host "=== POLY RECORDER SUPERVISOR ===" -ForegroundColor Cyan
Write-Host "CSV  -> data\poly_5m_live.csv" -ForegroundColor Gray
Write-Host "Log  -> data\poly_5m_recorder.log" -ForegroundColor Gray
Write-Host "Press Ctrl+C to stop." -ForegroundColor Gray
Write-Host ""

while ($true) {
    $ts = (Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ" -AsUTC)
    Write-Host "[$ts] Starting recorder..." -ForegroundColor Green
    Add-Content $LOG "[$ts] recorder starting"

    & $PY -X utf8 poly_live_ticker.py --record-5m-csv data/poly_5m_live.csv --windows 6

    $ec  = $LASTEXITCODE
    $ts2 = (Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ" -AsUTC)
    Write-Host "[$ts2] Recorder exited (code $ec). Restarting in 3s..." -ForegroundColor Yellow
    Add-Content $LOG "[$ts2] recorder exited code=$ec, restarting in 3s"
    Start-Sleep 3
}
