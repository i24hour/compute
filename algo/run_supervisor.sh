#!/bin/bash
# Auto-restart 5m algo trader. Uses algo/.env if present.
cd "$(dirname "$0")"
mkdir -p ../data

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

LIVE="${ALGO_LIVE:-0}"
echo "[algo-supervisor] started $(date -u +%Y-%m-%dT%H:%M:%SZ) ALGO_LIVE=$LIVE" >> ../data/algo_trader.log

while true; do
  ALGO_LIVE="$LIVE" node trader.mjs >> ../data/algo_trader.log 2>&1
  ec=$?
  echo "[algo-supervisor] exited $ec at $(date -u +%Y-%m-%dT%H:%M:%SZ), restart in 3s" >> ../data/algo_trader.log
  sleep 3
done
