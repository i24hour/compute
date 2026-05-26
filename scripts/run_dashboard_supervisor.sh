#!/bin/bash
# Keep Flask dashboard (localhost:5050) running — auto-restart on crash.
# Run in its own terminal:
#   ./scripts/run_dashboard_supervisor.sh
# Or detached:
#   nohup ./scripts/run_dashboard_supervisor.sh >> data/dashboard_supervisor.log 2>&1 &
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/dashboard"
mkdir -p "$ROOT/data"

PY="$ROOT/dashboard_venv/bin/python"
if [ ! -x "$PY" ]; then
  PY="$ROOT/.venv/bin/python"
fi
if [ ! -x "$PY" ]; then
  PY="python3"
fi

LOG="$ROOT/data/dashboard.log"
echo "[dashboard-supervisor] started $(date -u +%Y-%m-%dT%H:%M:%SZ) py=$PY" >> "$LOG"

while true; do
  "$PY" app.py >> "$LOG" 2>&1
  ec=$?
  echo "[dashboard-supervisor] app.py exited code $ec at $(date -u +%Y-%m-%dT%H:%M:%SZ), restart in 3s" >> "$LOG"
  sleep 3
done
