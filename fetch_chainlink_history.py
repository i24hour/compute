#!/usr/bin/env python3
"""
Fetch BTC/USD 1-minute OHLC from Chainlink's Data Streams Candlestick API only
(no Pyth Benchmarks).

Docs: https://docs.chain.link/data-streams/reference/candlestick-api
Mainnet: https://priceapi.dataengine.chain.link

Authentication: JWT from POST /api/v1/authorize (application/x-www-form-urlencoded
  login=user_id&password=api_key).

1m resolution only allowed when (to - from) is <= 24h, so this script pulls in
chunks (default 22h).

Environment (recommended):
  CHAINLINK_PRICEAPI_LOGIN       — Candlestick API user ID
  CHAINLINK_PRICEAPI_PASSWORD    — Candlestick API key
Optional:
  CHAINLINK_PRICEAPI_BASE_URL    — default https://priceapi.dataengine.chain.link
                                   (testnet: https://priceapi.testnet-dataengine.chain.link)

Output: data/btc_1m_chainlink_1year.csv
Columns: timestamp (UTC iso), open, high, low, close, volume

Usage:
    export CHAINLINK_PRICEAPI_LOGIN=...
    export CHAINLINK_PRICEAPI_PASSWORD=...
    python fetch_chainlink_history.py
    python fetch_chainlink_history.py --days 365 --out data/btc_1m.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

# Column-format prices in responses are denominated ~1e18 fixed-point USD (see Chainlink docs).
PRICE_SCALE = 1e18

DEFAULT_MAINNET = "https://priceapi.dataengine.chain.link"
SYMBOL_DEFAULT = "BTCUSD"
# Candlestick docs: 1m resolution requires query window <= 24h — stay safely under.
CHUNK_SECONDS = 22 * 3600
RATE_SLEEP = 0.35

OUT_DIR = Path(__file__).parent / "data"
OUT_FILE = OUT_DIR / "btc_1m_chainlink_1year.csv"


def _decode_price(x: float) -> float:
    if abs(float(x)) > 1e12:
        return float(x) / PRICE_SCALE
    return float(x)


@dataclass
class TokenBundle:
    access_token: str
    expires_at_unix: int

    def valid(self, now: float, skew_s: int = 120) -> bool:
        return now + skew_s < self.expires_at_unix


def authorize(base_url: str, login: str, password: str) -> TokenBundle:
    body = urllib.parse.urlencode({"login": login, "password": password}).encode()
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/v1/authorize",
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "fetch_chainlink_history/2.0",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        payload = json.loads(r.read())

    if payload.get("s") != "ok" or not isinstance(payload.get("d"), dict):
        raise RuntimeError(f"Candlestick authorize failed: {payload!r}")

    d = payload["d"]
    token = d.get("access_token")
    exp = int(d.get("expiration", 0))
    if not token or not exp:
        raise RuntimeError(f"Candlestick authorize missing token/exp: {payload!r}")
    return TokenBundle(access_token=str(token), expires_at_unix=exp)


def fetch_history_rows(
    base_url: str,
    token: str,
    *,
    symbol: str,
    from_ts: int,
    to_ts: int,
    resolution: str = "1m",
) -> list[dict]:
    q = urllib.parse.urlencode(
        {
            "symbol": symbol,
            "resolution": resolution,
            "from": str(from_ts),
            "to": str(to_ts),
        }
    )
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/v1/history/rows?{q}",
        headers={
            "Authorization": f"Bearer {token}",
            "User-Agent": "fetch_chainlink_history/2.0",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        data = json.loads(r.read())

    if data.get("s") != "ok":
        raise RuntimeError(f"history/rows returned non-ok: {data!r}")

    raw_candles = data.get("candles") or []

    rows: list[dict] = []
    for candle in raw_candles:
        if not isinstance(candle, list) or len(candle) < 5:
            continue
        ti = int(float(candle[0]))
        o, hi, lo, cl = (_decode_price(float(candle[j])) for j in range(1, 5))
        vol = float(candle[5]) if len(candle) > 5 else 0.0
        rows.append(
            {
                "_ts": ti,
                "timestamp": datetime.fromtimestamp(ti, tz=timezone.utc).isoformat(),
                "open": o,
                "high": hi,
                "low": lo,
                "close": cl,
                "volume": vol,
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch BTC 1m OHLC from Chainlink Candlestick API (authenticated)."
    )
    parser.add_argument("--days", type=int, default=365, help="Days of history (default: 365)")
    parser.add_argument("--symbol", type=str, default=SYMBOL_DEFAULT, help="Feed symbol e.g. BTCUSD")
    parser.add_argument("--out", type=str, default=str(OUT_FILE), help="Output CSV path")
    parser.add_argument(
        "--base-url",
        type=str,
        default=os.environ.get("CHAINLINK_PRICEAPI_BASE_URL", DEFAULT_MAINNET).rstrip("/"),
        help="Candlestick API base URL (or CHAINLINK_PRICEAPI_BASE_URL)",
    )
    parser.add_argument(
        "--login",
        type=str,
        default=os.environ.get("CHAINLINK_PRICEAPI_LOGIN", ""),
        help="User ID (or CHAINLINK_PRICEAPI_LOGIN)",
    )
    parser.add_argument(
        "--password",
        type=str,
        default=os.environ.get("CHAINLINK_PRICEAPI_PASSWORD", ""),
        help="API key (or CHAINLINK_PRICEAPI_PASSWORD)",
    )
    args = parser.parse_args()

    if not args.login or not args.password:
        print(
            "Missing Chainlink Candlestick API credentials.\n"
            "Set CHAINLINK_PRICEAPI_LOGIN and CHAINLINK_PRICEAPI_PASSWORD, or pass --login / --password.\n"
            "Access is issued through Chainlink Data Streams onboarding (often via Polymarket-sponsored flows).\n"
            "Reference: https://docs.chain.link/data-streams/reference/candlestick-api",
            file=sys.stderr,
        )
        sys.exit(1)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    end_ts = int(time.time())
    start_ts = end_ts - int(args.days) * 86400

    tb = authorize(args.base_url, args.login, args.password)

    by_ts: dict[int, dict] = {}
    cursor = start_ts
    chunk_i = 0
    chunks_est = max(1, int((end_ts - start_ts) / CHUNK_SECONDS) + 1)

    print(
        f"Fetching {args.days} days (~{args.days * 1440:,} bars) BTC/USD 1m OHLC "
        f"from Chainlink Candlestick API"
    )
    print(f"Base URL: {args.base_url}")
    print(f"Symbol  : {args.symbol}")
    print(f"Output  : {out_path}")
    print()

    while cursor <= end_ts:
        if not tb.valid(time.time()):
            tb = authorize(args.base_url, args.login, args.password)

        chunk_to = min(cursor + CHUNK_SECONDS, end_ts)
        try:
            rows = fetch_history_rows(
                args.base_url,
                tb.access_token,
                symbol=args.symbol,
                from_ts=cursor,
                to_ts=chunk_to,
            )
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            print(f"\n[!] HTTP {e.code}: {body[:800]}", file=sys.stderr)
            raise

        for r in rows:
            ts = int(r["_ts"])
            by_ts[ts] = r

        chunk_i += 1
        pct = chunk_i / chunks_est * 100
        dt = datetime.fromtimestamp(cursor, tz=timezone.utc).date()
        print(
            f"  {pct:5.1f}%  chunk {chunk_i}/{chunks_est}  {dt}  +{len(rows):5d} candles  cumulative={len(by_ts):8d}",
            end="\r",
        )

        cursor = chunk_to + 1
        time.sleep(RATE_SLEEP)

    print()

    sorted_ts = sorted(by_ts)
    rows_out = []
    for ts in sorted_ts:
        r = by_ts[ts]
        rows_out.append(
            {
                "timestamp": r["timestamp"],
                "open": round(r["open"], 4),
                "high": round(r["high"], 4),
                "low": round(r["low"], 4),
                "close": round(r["close"], 4),
                "volume": round(r["volume"], 6),
            }
        )

    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
        w.writeheader()
        w.writerows(rows_out)

    if not rows_out:
        print("\nNo rows returned — verify symbol/credentials/date range.", file=sys.stderr)
        sys.exit(2)

    print(f"\nDone — {len(rows_out):,} rows → {out_path}")
    print(f"Range : {rows_out[0]['timestamp'][:19]} → {rows_out[-1]['timestamp'][:19]} UTC")


if __name__ == "__main__":
    main()
