#!/usr/bin/env python3
"""
Kalshi BTC 15-Minute Live Terminal Ticker
==========================================
Connects to the Kalshi WebSocket and streams real-time price updates
for ALL currently active KXBTC15M markets in a clean terminal display.

Usage:
    python live_ticker.py --key-id YOUR_API_KEY_ID --key-file path/to/key.pem

Or set environment variables:
    export KALSHI_KEY_ID=your-api-key-id
    export KALSHI_KEY_FILE=/path/to/kalshi-key.key
    python live_ticker.py

How to get API keys:
    1. Go to https://kalshi.com  → Account & Security → API Keys
    2. Click "Create Key"
    3. Save the API Key ID (UUID) and download the .key file

Authentication:
    Kalshi WebSocket requires RSA-PSS SHA256 signed headers.
    The connection itself is authenticated; the 'ticker' channel
    is public (no extra auth needed after connection).
"""

import argparse
import asyncio
import base64
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# ── third-party (all in requirements.txt) ─────────────────────────────────
try:
    import aiohttp
    import websockets
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Run:  pip install websockets cryptography aiohttp")
    sys.exit(1)

# ── Kalshi endpoints ───────────────────────────────────────────────────────
WS_URL   = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
REST_URL = "https://api.elections.kalshi.com/trade-api/v2"

# ── ANSI colour helpers ────────────────────────────────────────────────────
class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    GREEN  = "\033[92m"
    RED    = "\033[91m"
    YELLOW = "\033[93m"
    CYAN   = "\033[96m"
    WHITE  = "\033[97m"
    BLUE   = "\033[94m"
    MAGENTA= "\033[95m"
    BG_DARK= "\033[40m"

def clr(text: str, *codes: str) -> str:
    return "".join(codes) + str(text) + C.RESET


# ── RSA-PSS signing ────────────────────────────────────────────────────────

def load_private_key(path: str):
    with open(path, "rb") as f:
        return serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )


def sign_pss(private_key, message: str) -> str:
    sig = private_key.sign(
        message.encode("utf-8"),
        asym_padding.PSS(
            mgf=asym_padding.MGF1(hashes.SHA256()),
            salt_length=asym_padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("utf-8")


def make_auth_headers(key_id: str, private_key, method: str, path: str) -> dict:
    ts = str(int(time.time() * 1000))
    clean_path = path.split("?")[0]
    sig = sign_pss(private_key, ts + method + clean_path)
    return {
        "KALSHI-ACCESS-KEY":       key_id,
        "KALSHI-ACCESS-SIGNATURE": sig,
        "KALSHI-ACCESS-TIMESTAMP": ts,
    }


# ── REST helper — fetch active KXBTC15M markets ───────────────────────────

async def get_active_markets() -> list[Dict[str, Any]]:
    """
    Fetches the currently OPEN or recently INITIALIZED KXBTC15M markets
    from the public REST API (no auth needed).
    Returns list sorted by close_time ascending.
    """
    markets: list[Dict] = []
    cursor: Optional[str] = None

    async with aiohttp.ClientSession() as session:
        while True:
            params: Dict[str, Any] = {
                "series_ticker": "KXBTC15M",
                "limit": 100,
                "status": "open",
            }
            if cursor:
                params["cursor"] = cursor

            async with session.get(f"{REST_URL}/markets", params=params) as resp:
                data = await resp.json()

            batch = data.get("markets", [])
            markets.extend(batch)
            cursor = data.get("cursor", "")
            if not cursor or not batch:
                break

    # Sort by close_time so the soonest-expiring market is first
    markets.sort(key=lambda m: m.get("close_time", ""))
    return markets


# ── Terminal rendering ────────────────────────────────────────────────────

def format_price(val: Optional[str], decimals: int = 2) -> str:
    if val is None:
        return clr("  --  ", C.DIM)
    try:
        f = float(val)
        pct = f * 100
        if pct >= 70:
            color = C.GREEN
        elif pct <= 30:
            color = C.RED
        else:
            color = C.YELLOW
        return clr(f"{pct:5.1f}%", color, C.BOLD)
    except ValueError:
        return val


def time_remaining(close_time_str: Optional[str]) -> str:
    if not close_time_str:
        return "??:??"
    try:
        ct = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = ct - now
        if delta.total_seconds() < 0:
            return clr("EXPIRED", C.DIM)
        mins, secs = divmod(int(delta.total_seconds()), 60)
        color = C.RED if mins < 3 else C.YELLOW if mins < 8 else C.GREEN
        return clr(f"{mins:02d}:{secs:02d}", color, C.BOLD)
    except Exception:
        return "??:??"


def render(
    market_meta: Dict[str, Dict],    # ticker → market REST info
    tickers: Dict[str, Dict],        # ticker → latest WS ticker msg
    last_update: str,
    msg_count: int,
) -> None:
    """Clears terminal and redraws the dashboard."""
    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

    # ── header ──────────────────────────────────────────────────────────────
    print("\033[H\033[J", end="")   # clear screen
    width = 106
    print(clr("━" * width, C.CYAN))
    print(clr(
        f"  🟡  KALSHI  ·  BTC 15-MIN LIVE TICKER  ·  {now_str}"
        f"  ·  msgs: {msg_count}",
        C.WHITE, C.BOLD,
    ))
    print(clr("━" * width, C.CYAN))

    col_fmt = (
        f"  {'TICKER':<30} {'TARGET $':>12} {'EXPIRES':>7}"
        f"  {'BID':>7} {'ASK':>7} {'MID':>7} {'LAST':>7}  {'VOLUME':>10}"
    )
    print(clr(col_fmt, C.DIM))
    print(clr("─" * width, C.DIM))

    # ── rows — show at most 15 markets (one screen) ──────────────────────
    sorted_tickers = sorted(
        market_meta.keys(),
        key=lambda t: market_meta[t].get("close_time", ""),
    )

    shown = 0
    for ticker in sorted_tickers:
        meta   = market_meta[ticker]
        ws_msg = tickers.get(ticker, {})

        target = meta.get("floor_strike") or meta.get("cap_strike")
        target_str = f"${float(target):,.2f}" if target is not None else " TBD "
        target_str = clr(f"{target_str:>12}", C.MAGENTA, C.BOLD)

        close_time = meta.get("close_time", "")
        tr = time_remaining(close_time)

        bid   = format_price(ws_msg.get("yes_bid_dollars"))
        ask   = format_price(ws_msg.get("yes_ask_dollars"))
        price = format_price(ws_msg.get("price_dollars"))

        # Mid = (bid + ask) / 2 if both present
        try:
            mid_val = (float(ws_msg["yes_bid_dollars"]) + float(ws_msg["yes_ask_dollars"])) / 2
            mid = clr(f"{mid_val*100:5.1f}%", C.WHITE, C.BOLD)
        except (KeyError, TypeError, ValueError):
            mid = clr("  -- ", C.DIM)

        vol_fp = ws_msg.get("volume_fp", "")
        try:
            vol = clr(f"{float(vol_fp):>10,.0f}", C.CYAN)
        except (ValueError, TypeError):
            vol = clr(f"{'--':>10}", C.DIM)

        # Shorten ticker for display
        short_ticker = ticker.replace("KXBTC15M-", "")

        print(
            f"  {clr(short_ticker, C.WHITE):<40}"
            f" {target_str}"
            f"  {tr}"
            f"  {bid} {ask} {mid} {price}"
            f"  {vol}"
        )
        shown += 1
        if shown >= 15:
            remaining = len(sorted_tickers) - shown
            if remaining > 0:
                print(clr(f"  … and {remaining} more markets", C.DIM))
            break

    # ── footer ───────────────────────────────────────────────────────────
    print(clr("━" * width, C.CYAN))
    print(clr(
        f"  BID = market's probability of YES  "
        f"·  MID = mid-price  "
        f"·  LAST = last trade price"
        f"  ·  last msg: {last_update}",
        C.DIM,
    ))
    print(clr("  Ctrl+C to quit", C.DIM))


# ── Main WebSocket loop ────────────────────────────────────────────────────

async def run(key_id: str, private_key) -> None:
    # Step 1 — fetch REST data for all open markets
    print("Fetching active KXBTC15M markets from REST API…")
    markets = await get_active_markets()
    if not markets:
        print("No open KXBTC15M markets found right now. The exchange may be between windows.")
        return

    market_meta: Dict[str, Dict] = {m["ticker"]: m for m in markets}
    tickers: Dict[str, Dict] = {}
    msg_count = 0
    last_update = "--"
    all_tickers = list(market_meta.keys())

    print(f"Found {len(all_tickers)} open markets. Connecting to WebSocket…\n")

    # Step 2 — build auth headers
    ws_path = "/trade-api/ws/v2"
    headers = make_auth_headers(key_id, private_key, "GET", ws_path)

    # Step 3 — connect and stream
    async with websockets.connect(
        WS_URL,
        additional_headers=headers,
        ping_interval=20,
        ping_timeout=20,
    ) as ws:

        # Subscribe to ticker for all KXBTC15M markets in batches of 100
        batch_size = 100
        for i in range(0, len(all_tickers), batch_size):
            batch = all_tickers[i : i + batch_size]
            await ws.send(json.dumps({
                "id": i // batch_size + 1,
                "cmd": "subscribe",
                "params": {
                    "channels": ["ticker"],
                    "market_tickers": batch,
                    "send_initial_snapshot": True,
                },
            }))

        # Consume messages
        async for raw in ws:
            msg = json.loads(raw)
            mtype = msg.get("type")

            if mtype == "ticker":
                payload = msg.get("msg", {})
                ticker_sym = payload.get("market_ticker")
                if ticker_sym:
                    tickers[ticker_sym] = payload
                    msg_count += 1
                    last_update = datetime.now(timezone.utc).strftime("%H:%M:%S")

            elif mtype == "error":
                code = msg.get("msg", {}).get("code")
                err  = msg.get("msg", {}).get("msg")
                print(f"\n[WS ERROR {code}]: {err}")
                continue

            # Re-render on every ticker update
            if mtype == "ticker":
                render(market_meta, tickers, last_update, msg_count)


# ── CLI ────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Live terminal ticker for Kalshi BTC 15-min markets.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--key-id",
        default=os.environ.get("KALSHI_KEY_ID", ""),
        metavar="UUID",
        help="Kalshi API Key ID (or set KALSHI_KEY_ID env var)",
    )
    p.add_argument(
        "--key-file",
        default=os.environ.get("KALSHI_KEY_FILE", ""),
        metavar="PATH",
        help="Path to RSA private key .pem/.key file (or set KALSHI_KEY_FILE env var)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Validate
    if not args.key_id:
        print(clr("ERROR: API Key ID is required.", C.RED, C.BOLD))
        print("  Pass --key-id YOUR_KEY_ID  or  export KALSHI_KEY_ID=...")
        print("\nGet your key at: https://kalshi.com → Account & Security → API Keys")
        sys.exit(1)

    if not args.key_file:
        print(clr("ERROR: Private key file path is required.", C.RED, C.BOLD))
        print("  Pass --key-file /path/to/key.pem  or  export KALSHI_KEY_FILE=...")
        sys.exit(1)

    if not os.path.exists(args.key_file):
        print(clr(f"ERROR: Key file not found: {args.key_file}", C.RED, C.BOLD))
        sys.exit(1)

    try:
        private_key = load_private_key(args.key_file)
    except Exception as e:
        print(clr(f"ERROR loading private key: {e}", C.RED, C.BOLD))
        sys.exit(1)

    print(clr("Kalshi BTC 15-Min Live Ticker", C.CYAN, C.BOLD))
    print(clr(f"Key ID : {args.key_id[:8]}…", C.DIM))
    print(clr(f"Key file: {args.key_file}", C.DIM))
    print()

    try:
        asyncio.run(run(args.key_id, private_key))
    except KeyboardInterrupt:
        print("\nExited.")
