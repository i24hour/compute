#!/usr/bin/env python3
"""
Build ~1‑minute BTC/USD candles from Ethereum mainnet Chainlink **on-chain**
AggregatorV3 `getRoundData` history (no Pyth, no Chainlink Candlestick JWT).

Mechanics
---------
1. Read `decimals()` once from the feed proxy.
2. Call `latestRoundData()`, then repeatedly `getRoundData(roundId)` while
   decrementing `roundId` until `updatedAt` is before the requested window.
3. Each round yields one oracle price at `updatedAt` (Chainlink’s on-chain
   timestamp for that answer).
4. Bucket prices into UTC minutes: O/H/L/C from all ticks in that minute.
   Minutes with no fresh tick forward-fill **after** at least one tick is seen,
   matching the oracle’s last consensus price between updates.
   By default we synthesize ticks at `--days` boundary using the earliest in-window
   price (--no-anchor-start disables that extrapolation).

RPC
---
Set `ETH_RPC_URL` to any Ethereum mainnet JSON‑RPC (Alchemy, Infura, public
node, etc.). Free public RPCs may rate-limit long backfills.

Default feed
------------
Mainnet BTC/USD proxy (Chainlink data.chain.link “Standard” address):
  0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c

Override with `--feed` if you need another network or feed.

Output
------
CSV: timestamp (UTC ISO), open, high, low, close, volume
`volume` is always 0 (oracle rounds have no trade volume).

Usage:
  export ETH_RPC_URL=https://ethereum-rpc.publicnode.com
  python fetch_chainlink_onchain_btc_1m.py --days 365

  python fetch_chainlink_onchain_btc_1m.py --days 30 --out data/btc_1m_onchain.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Chainlink mainnet BTC/USD (Standard proxy) — verify on https://data.chain.link
DEFAULT_BTC_USD_FEED = "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c"

# Function selectors (Keccak-256 first 4 bytes)
SEL_DECIMALS = "313ce567"
SEL_LATEST = "feaf968c"
SEL_GET_ROUND = "9a6fc8f5"

OUT_DIR = Path(__file__).parent / "data"
OUT_DEFAULT = OUT_DIR / "btc_1m_onchain_chainlink_1year.csv"


@dataclass
class RoundRow:
    round_id: int
    answer: int
    started_at: int
    updated_at: int


def _rpc(provider: str, method: str, params: list[Any], timeout_s: float = 60.0) -> Any:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(
        provider,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "fetch_chainlink_onchain/1.0"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as r:
        payload = json.loads(r.read())

    if payload.get("error"):
        raise RuntimeError(str(payload["error"]))
    return payload["result"]


def _decode_hex_word(data: bytes, word_index: int) -> int:
    off = word_index * 32
    return int.from_bytes(data[off : off + 32], "big", signed=False)


def _decode_get_round(hex_result: str) -> RoundRow:
    raw = bytes.fromhex(hex_result.removeprefix("0x"))
    if len(raw) < 5 * 32:
        raise ValueError(f"Unexpected return length {len(raw)}")

    round_id = _decode_hex_word(raw, 0)
    ans_u = _decode_hex_word(raw, 1)
    if ans_u >= 2**255:
        answer = ans_u - 2**256
    else:
        answer = int(ans_u)

    started_at = _decode_hex_word(raw, 2)
    updated_at = _decode_hex_word(raw, 3)

    return RoundRow(round_id=round_id, answer=answer, started_at=int(started_at), updated_at=int(updated_at))


def _eth_call(provider: str, to: str, data_hex: str, block_tag: str = "latest") -> str:
    res = _rpc(provider, "eth_call", [{"to": to, "data": data_hex}, block_tag])
    if not isinstance(res, str):
        raise RuntimeError(f"Unexpected eth_call result: {res!r}")
    return res


def _decimals(provider: str, feed: str) -> int:
    out = _eth_call(provider, feed, "0x" + SEL_DECIMALS)
    if len(out) < 66:
        raise RuntimeError(f"decimals() returned {out}")
    return int(out[2:], 16)


def _latest_round(provider: str, feed: str) -> RoundRow:
    return _decode_get_round(_eth_call(provider, feed, "0x" + SEL_LATEST))


def _get_round(provider: str, feed: str, round_id: int) -> RoundRow:
    rd = hex(round_id)[2:].rjust(64, "0")
    return _decode_get_round(_eth_call(provider, feed, "0x" + SEL_GET_ROUND + rd))


def _price_from_answer(answer: int, decimals: int) -> float:
    return float(answer) / (10**decimals)


def _minute_bucket(ts: int) -> int:
    return ts - (ts % 60)


def _collect_rounds(
    provider: str,
    feed: str,
    start_ts: int,
    *,
    max_rounds: int,
    sleep_s: float,
    retries: int,
) -> list[RoundRow]:
    head = _latest_round(provider, feed)
    if head.updated_at < start_ts:
        print("Latest oracle update is already older than requested window.", file=sys.stderr)
        return []

    rows: list[RoundRow] = []
    # Reuse latestRoundData result as newest round — then walk downwards.
    if head.updated_at >= start_ts:
        rows.append(head)
    rd = head.round_id - 1

    iterations = 1 if rows else 0

    while iterations < max_rounds:
        row: Optional[RoundRow] = None
        last_err: Optional[BaseException] = None
        for attempt in range(retries):
            try:
                row = _get_round(provider, feed, rd)
                last_err = None
                break
            except (
                RuntimeError,
                urllib.error.HTTPError,
                urllib.error.URLError,
                TimeoutError,
                ValueError,
            ) as e:
                last_err = e
                time.sleep(min(30.0, 0.4 * (2**attempt) + random.random()))

        if row is None:
            print(f"[stop] round {rd}: {last_err}", file=sys.stderr)
            rows.sort(key=lambda r: r.updated_at)
            return rows

        iterations += 1

        if row.updated_at < start_ts:
            rows.sort(key=lambda r: r.updated_at)
            return rows

        rows.append(row)

        if iterations % 200 == 0:
            pct = 100 * (head.updated_at - row.updated_at) / max(head.updated_at - start_ts + 1, 1)
            print(f"  rounds={iterations}  round_id={rd}  updated_at={row.updated_at}  ~{pct:.1f}% of time span", flush=True)

        rd -= 1
        if sleep_s > 0:
            time.sleep(sleep_s)

    print(f"[warn] Hit --max-rounds={max_rounds}; output may truncate early.", file=sys.stderr)
    rows.sort(key=lambda r: r.updated_at)
    return rows


def _minute_ohlc_from_ticks(
    ticks: list[tuple[int, float]],
    start_min: int,
    end_min: int,
) -> list[dict[str, Any]]:
    """ticks: (updated_at unix, price), sorted by time."""
    by_min: dict[int, list[float]] = {}
    for ts, px in ticks:
        m = _minute_bucket(ts)
        by_min.setdefault(m, []).append(px)

    out: list[dict[str, Any]] = []
    last_close: Optional[float] = None

    m = start_min
    while m <= end_min:
        if m in by_min:
            series = by_min[m]
            o, h, lo, c = series[0], max(series), min(series), series[-1]
            last_close = c
        elif last_close is not None:
            o = h = lo = c = last_close
        else:
            m += 60
            continue

        out.append(
            {
                "timestamp": datetime.fromtimestamp(m, tz=timezone.utc).isoformat(),
                "open": round(o, 4),
                "high": round(h, 4),
                "low": round(lo, 4),
                "close": round(c, 4),
                "volume": 0.0,
            }
        )
        m += 60

    return out


def main() -> None:
    p = argparse.ArgumentParser(description="1m BTC/USD CSV from Chainlink on-chain rounds (AggregatorV3).")
    p.add_argument("--days", type=int, default=365, help="Lookback days from now (default 365)")
    p.add_argument("--feed", type=str, default=DEFAULT_BTC_USD_FEED, help="Aggregator proxy address (checksummed optional)")
    p.add_argument("--rpc", type=str, default=os.environ.get("ETH_RPC_URL", ""), help="Ethereum JSON-RPC URL (or ETH_RPC_URL)")
    p.add_argument("--out", type=str, default=str(OUT_DEFAULT))
    p.add_argument(
        "--max-rounds",
        type=int,
        default=750_000,
        help="Safety cap on backwards round steps (stop if exceeded)",
    )
    p.add_argument("--sleep", type=float, default=0.06, help="Seconds between RPC calls (rate limits)")
    p.add_argument("--retries", type=int, default=4, help="Retries per failing eth_call")

    p.add_argument(
        "--no-anchor-start",
        action="store_true",
        help="Do not back-extrapolate the earliest observed oracle price before the first on-chain updatedAt.",
    )

    args = p.parse_args()
    rpc_url = args.rpc.strip()
    if not rpc_url:
        print(
            "Set ETH_RPC_URL or pass --rpc to your Ethereum mainnet endpoint.\n"
            "Example: export ETH_RPC_URL=https://ethereum-rpc.publicnode.com",
            file=sys.stderr,
        )
        sys.exit(1)

    feed = args.feed.strip()
    if not feed.startswith("0x"):
        feed = "0x" + feed
    feed_lower = feed.lower()

    now = int(time.time())
    start_ts = now - args.days * 86400
    start_min = _minute_bucket(start_ts)
    end_min = _minute_bucket(now)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(
        "Chainlink BTC/USD — on-chain rounds → 1-minute CSV\n"
        f"  RPC           {rpc_url}\n"
        f"  Feed          {feed_lower}\n"
        f"  Window (unix) {start_ts} → {now}\n"
        f"  Output        {out_path}\n",
        flush=True,
    )

    rounds = _collect_rounds(
        rpc_url,
        feed_lower,
        start_ts,
        max_rounds=args.max_rounds,
        sleep_s=args.sleep,
        retries=args.retries,
    )
    decs = _decimals(rpc_url, feed_lower)
    ticks: list[tuple[int, float]] = []
    for r in rounds:
        if r.updated_at < start_ts:
            continue
        ticks.append((r.updated_at, _price_from_answer(r.answer, decs)))

    ticks.sort(key=lambda x: x[0])
    # Drop duplicate timestamps: keep latest price for identical updatedAt if any
    deduped: list[tuple[int, float]] = []
    for ts, px in ticks:
        if deduped and deduped[-1][0] == ts:
            deduped[-1] = (ts, px)
        else:
            deduped.append((ts, px))

    if deduped and not args.no_anchor_start:
        first_ts, first_px = deduped[0]
        if first_ts > start_ts:
            deduped = [(start_ts, first_px)] + deduped

    rows = _minute_ohlc_from_ticks(deduped, start_min, end_min)

    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
        w.writeheader()
        w.writerows(rows)

    if not rows:
        print("No CSV rows produced (no ticks in window or RPC issues).", file=sys.stderr)
        sys.exit(2)

    print(
        f"Done — {len(rounds)} rounds fetched, {len(rows):,} minute rows → {out_path}\n"
        f"Range: {rows[0]['timestamp'][:19]} → {rows[-1]['timestamp'][:19]} UTC",
        flush=True,
    )


if __name__ == "__main__":
    main()
