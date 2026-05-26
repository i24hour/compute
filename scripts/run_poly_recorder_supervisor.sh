#!/bin/bash
# Auto-restart poly 5m CSV recorder (1 Hz → data/poly_5m_live.csv).
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
mkdir -p data

PY="$ROOT/dashboard_venv/bin/python"
if [ ! -x "$PY" ]; then
  PY="$ROOT/.venv/bin/python"
fi
if [ ! -x "$PY" ]; then
  PY="python3"
fi

LOG="$ROOT/data/poly_5m_recorder.log"
echo "[supervisor] started $(date -u +%Y-%m-%dT%H:%M:%SZ) py=$PY" >> "$LOG"

while true; do
  "$PY" "$ROOT/poly_live_ticker.py" --record-5m-csv "$ROOT/data/poly_5m_live.csv" --windows 6 >> "$LOG" 2>&1
  ec=$?
  echo "[supervisor] recorder exited code $ec at $(date -u +%Y-%m-%dT%H:%M:%SZ), restarting in 2s" >> "$LOG"
  sleep 2
done
