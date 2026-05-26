#!/usr/bin/env python3
"""
Polymarket BTC Up/Down 15-Minute Live Terminal Ticker
=====================================================
Streams two live WebSocket feeds simultaneously:

  1. wss://ws-subscriptions-clob.polymarket.com/ws/market
     → Real-time orderbook (bids/asks), price changes, best bid/ask
       for all currently active BTC Up/Down 15-minute markets.

  2. wss://ws-live-data.polymarket.com  (RTDS)
     → Live BTC/USDT spot price from Binance (sub-second updates).

Both are PUBLIC channels — NO API key required.

Usage:
    python poly_live_ticker.py
    python poly_live_ticker.py --windows 4    # show 4 upcoming windows
    python poly_live_ticker.py --depth 8      # show 8 order book levels
    python poly_live_ticker.py --record-5m-csv data/poly_5m_live.csv --windows 6
"""

import argparse
import asyncio
import csv
import json
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import websockets

# ── API endpoints ───────────────────────────────────────────────────────────
GAMMA_API   = "https://gamma-api.polymarket.com"
MARKET_WS   = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RTDS_WS     = "wss://ws-live-data.polymarket.com"
CLOB_API    = "https://clob.polymarket.com"
PYTH_HERMES_URL = "https://hermes.pyth.network"
PYTH_BTC_USD_ID = "0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"
CHAINLINK_TICK_WINDOW_SEC = 25 * 60
# PTB source: Polymarket resolves against Chainlink BTC/USD — must use chainlink
PTB_SOURCE = "chainlink"

PRICE_SOURCE_PRIORITY = {
    None: 0,
    "pyth": 1,
    "rtds": 2,
    "chainlink": 3,
}
MAX_EMPTY_QUOTE_STREAK_ROWS = 10
RECORD_STARTUP_GRACE_SEC = 120

# ── ANSI colours ────────────────────────────────────────────────────────────
class C:
    R="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"
    GREEN="\033[92m"; RED="\033[91m"; YELLOW="\033[93m"
    CYAN="\033[96m"; WHITE="\033[97m"; BLUE="\033[94m"
    MAGENTA="\033[95m"; ORANGE="\033[33m"; BG="\033[40m"

def col(text, *codes): return "".join(codes) + str(text) + C.R

WIDTH = 110

CHAINLINK_TICKS: "deque[tuple[int, float]]" = deque()

# ── BTC price helpers ───────────────────────────────────────────────────────

def _set_btc_price(price: float, ts_ms: Optional[int], source: str):
    if price is None:
        return
    try:
        price_f = float(price)
    except (TypeError, ValueError):
        return
    if price_f <= 0:
        return
    current_source = state.get("btc_source")
    if PRICE_SOURCE_PRIORITY.get(source, 0) < PRICE_SOURCE_PRIORITY.get(current_source, 0):
        return
    state["btc_price"] = price_f
    state["btc_price_ts"] = ts_ms or int(time.time() * 1000)
    state["btc_source"] = source


def _record_chainlink_tick(ts_ms: Optional[int], price: Optional[float]):
    if ts_ms is None or price is None:
        return
    try:
        ts = int(ts_ms)
        val = float(price)
    except (TypeError, ValueError):
        return
    if val <= 0:
        return
    CHAINLINK_TICKS.append((ts, val))
    cutoff = ts - (CHAINLINK_TICK_WINDOW_SEC * 1000)
    while CHAINLINK_TICKS and CHAINLINK_TICKS[0][0] < cutoff:
        CHAINLINK_TICKS.popleft()


def _chainlink_tick_at_or_before(ts_ms: int) -> Optional[float]:
    for ts, val in reversed(CHAINLINK_TICKS):
        if ts <= ts_ms:
            return val
    return None


def _chainlink_tick_after(ts_ms: int) -> Optional[float]:
    for ts, val in CHAINLINK_TICKS:
        if ts >= ts_ms:
            return val
    return None


def _update_ptb_from_price(source: str, now_ts: Optional[float] = None):
    """Set PTB for windows when we first cross their start time."""
    btc = state.get("btc_price")
    if btc is None:
        return
    now_ts = now_ts or time.time()
    ptb_map = state.get("ptb_by_slug", {})
    ptb_src_map = state.get("ptb_source_by_slug", {})
    for mkt in state.get("markets", []):
        slug = mkt.get("slug") or ""
        if not slug:
            continue
        ws = mkt.get("window_start")
        if not isinstance(ws, int):
            continue
        if slug in ptb_map:
            if ptb_src_map.get(slug) != PTB_SOURCE and source == PTB_SOURCE:
                ptb_map[slug] = float(btc)
                ptb_src_map[slug] = PTB_SOURCE
            continue
        if now_ts >= ws:
            if source == PTB_SOURCE:
                ptb_map[slug] = float(btc)
                ptb_src_map[slug] = PTB_SOURCE


def _update_ptb_from_chainlink_ticks():
    """Use stored Chainlink ticks to pin PTB at window start."""
    if PTB_SOURCE != "chainlink":
        return
    ptb_map = state.get("ptb_by_slug", {})
    ptb_src_map = state.get("ptb_source_by_slug", {})
    now_ms = int(time.time() * 1000)
    for mkt in state.get("markets", []):
        slug = mkt.get("slug") or ""
        if not slug:
            continue
        ws = mkt.get("window_start")
        if not isinstance(ws, int):
            continue
        ws_ms = ws * 1000
        if now_ms < ws_ms:
            continue
        existing_src = ptb_src_map.get(slug)
        if existing_src == "chainlink":
            continue
        after = _chainlink_tick_after(ws_ms)
        if after is not None:
            ptb_map[slug] = float(after)
            ptb_src_map[slug] = "chainlink"
            continue
        # Fallback: if we only have older ticks, use the last one before start
        before = _chainlink_tick_at_or_before(ws_ms)
        if before is not None and existing_src is None:
            ptb_map[slug] = float(before)
            ptb_src_map[slug] = "chainlink_pre"

# ── Shared state ─────────────────────────────────────────────────────────────
state: Dict[str, Any] = {
    "btc_price":     None,        # float
    "btc_price_ts":  0,           # ms timestamp
    "btc_source":    None,        # "rtds" | "pyth"
    "ptb_by_slug": {},            # event slug -> ptb price (5m + 15m can share same window_start)
    "ptb_source_by_slug": {},    # event slug -> source
    "markets":       [],          # list of market dicts from Gamma
    "books":         {},          # token_id → {"bids": {price: size}, "asks": {price: size}}
    "best":          {},          # token_id → {"bid": str, "ask": str, "spread": str}
    "last_trade":    {},          # token_id → {"price": str, "side": str, "size": str, "ts": int}
    "msg_count":     0,
    "last_render":   0.0,
}

# When True, skip terminal rendering (used by Flask dashboard background feed).
_dashboard_mode: bool = False
# When True, skip terminal clears (CSV recorder mode shares WS state but stays quiet).
_recording_mode: bool = False


# ── Market discovery ─────────────────────────────────────────────────────────

async def _discover_band(
    session: aiohttp.ClientSession,
    slug_prefix: str,
    span_sec: int,
    n_windows: int,
) -> List[Dict]:
    """
    Find active Polymarket BTC up/down events for one cadence.

    slug_prefix: 'btc-updown-5m' or 'btc-updown-15m'
    span_sec:    300 or 900 — length of each window in seconds
    """
    now = int(time.time())
    base = (now // span_sec) * span_sec
    candidates = [base + (i * span_sec) for i in range(-1, n_windows + 2)]

    tasks = [session.get(f"{GAMMA_API}/events?slug={slug_prefix}-{ts}") for ts in candidates]
    responses = await asyncio.gather(*tasks, return_exceptions=True)

    found = []
    interval_minutes = span_sec // 60
    for ts, resp in zip(candidates, responses):
        if isinstance(resp, Exception):
            continue
        try:
            data = await resp.json()
        except Exception:
            continue
        if not data:
            continue
        event = data[0]
        if not event.get("active"):
            continue
        mkts = event.get("markets", [])
        if not mkts:
            continue
        market = mkts[0]
        raw_tokens = market.get("clobTokenIds", [])
        # Gamma API returns clobTokenIds as a JSON-encoded string, not a list
        if isinstance(raw_tokens, str):
            try:
                raw_tokens = json.loads(raw_tokens)
            except (json.JSONDecodeError, TypeError):
                continue
        if len(raw_tokens) < 2:
            continue

        found.append({
            "slug":           event.get("slug", ""),
            "title":          event.get("title", ""),
            "window_start":   ts,
            "window_end":     ts + span_sec,
            "interval_minutes": interval_minutes,
            "condition_id":   market.get("conditionId", ""),
            "up_token":       raw_tokens[0],
            "down_token":     raw_tokens[1],
            "price_to_beat":  market.get("startingPrice") or market.get("outcomePrices", [None])[0],
        })

    if found:
        ptb_tasks = [fetch_price_to_beat(session, m["slug"]) for m in found]
        ptb_vals = await asyncio.gather(*ptb_tasks, return_exceptions=True)
        for m, ptb in zip(found, ptb_vals):
            if isinstance(ptb, Exception):
                continue
            if ptb is not None:
                m["price_to_beat"] = ptb

    found.sort(key=lambda m: m["window_start"])
    found = [m for m in found if m["window_end"] > now - 30]
    return found[:n_windows]


async def discover_markets(session: aiohttp.ClientSession, n_windows: int = 6) -> List[Dict]:
    """BTC 15m only (terminal CLI default)."""
    return await _discover_band(session, "btc-updown-15m", 900, n_windows)


async def discover_dashboard_markets(session: aiohttp.ClientSession, n_windows: int = 6) -> List[Dict]:
    """5m + 15m for Flask /live (matches Polymarket interval tabs)."""
    five, fifteen = await asyncio.gather(
        _discover_band(session, "btc-updown-5m", 300, n_windows),
        _discover_band(session, "btc-updown-15m", 900, n_windows),
    )
    merged = five + fifteen
    merged.sort(key=lambda m: (m["window_start"], m["interval_minutes"]))
    return merged


async def fetch_price_to_beat(session: aiohttp.ClientSession, slug: str) -> Optional[float]:
    """Fetches the price-to-beat for a BTC market from Polymarket."""
    try:
        url = f"https://polymarket.com/api/crypto/price-to-beat/{slug}"
        async with session.get(url) as r:
            if r.status == 200:
                d = await r.json()
                return float(d.get("price", 0)) or None
    except Exception:
        pass
    return None


# ── Order book helpers ────────────────────────────────────────────────────────

def apply_book_snapshot(token_id: str, bids: List, asks: List):
    book = state["books"].setdefault(token_id, {"bids": {}, "asks": {}})
    book["bids"] = {b["price"]: float(b["size"]) for b in bids}
    book["asks"] = {a["price"]: float(a["size"]) for a in asks}


def apply_price_change(token_id: str, price: str, size: str, side: str):
    book = state["books"].setdefault(token_id, {"bids": {}, "asks": {}})
    sz = float(size)
    bucket = "bids" if side == "BUY" else "asks"
    if sz == 0.0:
        book[bucket].pop(price, None)
    else:
        book[bucket][price] = sz


def top_book(token_id: str, depth: int = 5) -> Tuple[List, List]:
    book = state["books"].get(token_id, {"bids": {}, "asks": {}})
    bids = sorted(book["bids"].items(), key=lambda x: -float(x[0]))[:depth]
    asks = sorted(book["asks"].items(), key=lambda x:  float(x[0]))[:depth]
    return bids, asks


def token_latest_bid_ask(token_id: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Latest top-of-book bid/ask probability (0–1) for one outcome token.
    Prefers WS best-bid-ask cache and falls back to orderbook top level.
    """
    cached = state["best"].get(token_id, {})
    bid_raw = cached.get("bid")
    ask_raw = cached.get("ask")

    bid: Optional[float] = None
    ask: Optional[float] = None
    try:
        if bid_raw not in (None, ""):
            bid = float(bid_raw)
    except (TypeError, ValueError):
        bid = None
    try:
        if ask_raw not in (None, ""):
            ask = float(ask_raw)
    except (TypeError, ValueError):
        ask = None

    if bid is None or ask is None:
        bids, asks = top_book(token_id, 1)
        if bid is None and bids:
            try:
                bid = float(bids[0][0])
            except (TypeError, ValueError):
                bid = None
        if ask is None and asks:
            try:
                ask = float(asks[0][0])
            except (TypeError, ValueError):
                ask = None
    return bid, ask


# ── Terminal renderer ─────────────────────────────────────────────────────────

def fmt_pct(val: Optional[str], bold: bool = True) -> str:
    if val is None or val == "":
        return col(f"{'--':>7}", C.DIM)
    try:
        f = float(val) * 100
        color = C.GREEN if f >= 60 else C.RED if f <= 40 else C.YELLOW
        s = f"{f:5.1f}%"
        return col(s, color, C.BOLD if bold else "")
    except ValueError:
        return col(f"{val:>7}", C.DIM)


def fmt_size(sz: float) -> str:
    if sz >= 1000:
        return f"{sz/1000:.1f}k"
    return f"{sz:.0f}"


def time_remaining(window_end: int) -> str:
    delta = window_end - int(time.time())
    if delta < 0:
        return col("EXPIRED", C.DIM)
    m, s = divmod(delta, 60)
    clr = C.RED if m < 2 else C.YELLOW if m < 8 else C.GREEN
    return col(f"{m:02d}:{s:02d}", clr, C.BOLD)


def render(depth: int = 5):
    if _dashboard_mode or _recording_mode:
        return
    now_ts = time.time()
    if now_ts - state["last_render"] < 0.1:   # max 10 renders/sec
        return
    state["last_render"] = now_ts

    print("\033[H\033[J", end="")  # clear screen

    # ── Header ──
    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    btc_p = state["btc_price"]
    btc_str = col(f"${btc_p:,.2f}", C.YELLOW, C.BOLD) if btc_p else col("fetching…", C.DIM)
    btc_age = int((time.time() * 1000 - state["btc_price_ts"]) / 1000) if state["btc_price_ts"] else 99
    age_str = col(f"({btc_age}s ago)", C.DIM) if btc_age > 2 else ""

    print(col("━" * WIDTH, C.CYAN))
    print(col(
        f"  ₿  POLYMARKET · BTC UP/DOWN 15-MIN · {now_str}"
        f"   BTC LIVE: {btc_str} {age_str}"
        f"   msgs: {state['msg_count']}",
        C.WHITE, C.BOLD,
    ))
    print(col("━" * WIDTH, C.CYAN))

    markets = state["markets"]
    if not markets:
        print(col("  Discovering markets…", C.DIM))
        return

    for mkt in markets:
        up_tok   = mkt["up_token"]
        down_tok = mkt["down_token"]
        bup = state["best"].get(up_tok, {})
        bdn = state["best"].get(down_tok, {})
        lt  = state["last_trade"].get(up_tok, {})

        ptb = mkt.get("price_to_beat")
        try:
            ptb_str = col(f"${float(ptb):,.2f}", C.MAGENTA, C.BOLD) if ptb else col("TBD", C.DIM)
        except (TypeError, ValueError):
            ptb_str = col(str(ptb), C.MAGENTA, C.BOLD)

        title = mkt["title"].replace("Bitcoin Up or Down - ", "").strip()
        tr = time_remaining(mkt["window_end"])

        up_bid   = bup.get("bid", "")
        up_ask   = bup.get("ask", "")
        dn_bid   = bdn.get("bid", "")

        # Mid price for Up token = implied prob of BTC going up
        try:
            mid = (float(up_bid) + float(up_ask)) / 2
            mid_str = col(f"{mid*100:.1f}%", C.WHITE, C.BOLD)
        except (TypeError, ValueError):
            mid_str = col("  --  ", C.DIM)

        last_p = lt.get("price", "")
        last_str = fmt_pct(last_p, bold=False) if last_p else col("  --  ", C.DIM)

        spread = bup.get("spread", "")
        try:
            sp_str = col(f"{float(spread)*100:.1f}¢", C.DIM)
        except (TypeError, ValueError):
            sp_str = col("--", C.DIM)

        print(col("─" * WIDTH, C.DIM))
        print(
            f"  {col(title, C.WHITE, C.BOLD):<55}"
            f" PTB: {ptb_str}   expires: {tr}"
        )
        print(
            f"  {'':4}"
            f"  UP  bid {fmt_pct(up_bid)}  ask {fmt_pct(up_ask)}  mid {mid_str}  last {last_str}  spread {sp_str}"
            f"   DOWN bid {fmt_pct(dn_bid)}"
        )

        # ── Order book ──
        bids, asks = top_book(up_tok, depth)
        if bids or asks:
            # print side-by-side
            bids_p = [(f"{float(p)*100:.1f}%", fmt_size(s)) for p, s in bids]
            asks_p = [(f"{float(p)*100:.1f}%", fmt_size(s)) for p, s in asks]
            max_rows = max(len(bids_p), len(asks_p))
            # header
            print(f"  {'':4}  {col('── UP BIDS ──', C.GREEN)}{'':18}{col('── UP ASKS ──', C.RED)}")
            for i in range(min(max_rows, depth)):
                b = f"{col(bids_p[i][0], C.GREEN)} x {col(bids_p[i][1], C.DIM)}" if i < len(bids_p) else ""
                a = f"{col(asks_p[i][0], C.RED)}   x {col(asks_p[i][1], C.DIM)}" if i < len(asks_p) else ""
                print(f"  {'':8}  {b:<40}  {a}")

    print(col("━" * WIDTH, C.CYAN))
    print(col(
        "  Prices = implied probability of BTC going UP  ·  "
        "BTC price from Binance (RTDS)  ·  Ctrl+C to quit",
        C.DIM,
    ))


# ── WebSocket feed 1: Market orderbook ───────────────────────────────────────

async def _market_ws_session(token_ids: List[str], *, retry_delay: Dict[str, float]) -> None:
    async with websockets.connect(
        MARKET_WS,
        ping_interval=None,
        additional_headers={"Origin": "https://polymarket.com"},
    ) as ws:
        retry_delay["v"] = 1.0
        await ws.send(json.dumps({
            "assets_ids": token_ids,
            "type": "market",
            "custom_feature_enabled": True,
        }))

        async def heartbeat():
            while True:
                await asyncio.sleep(10)
                try:
                    await ws.send("PING")
                except Exception:
                    return

        asyncio.create_task(heartbeat())

        async for raw in ws:
            if raw == "PONG":
                continue
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msgs = parsed if isinstance(parsed, list) else [parsed]

            for msg in msgs:
                if not isinstance(msg, dict):
                    continue
                state["msg_count"] += 1
                etype = msg.get("event_type", "")

                if etype == "book":
                    tid = msg.get("asset_id", "")
                    apply_book_snapshot(tid, msg.get("bids", []), msg.get("asks", []))

                elif etype == "price_change":
                    for pc in msg.get("price_changes", []):
                        tid = pc.get("asset_id", "")
                        apply_price_change(tid, pc["price"], pc["size"], pc["side"])
                        bb = pc.get("best_bid")
                        ba = pc.get("best_ask")
                        if bb is not None or ba is not None:
                            entry = state["best"].setdefault(tid, {})
                            if bb is not None:
                                entry["bid"] = bb
                            if ba is not None:
                                entry["ask"] = ba
                            try:
                                entry["spread"] = str(round(float(ba) - float(bb), 4))
                            except Exception:
                                pass

                elif etype == "best_bid_ask":
                    tid = msg.get("asset_id", "")
                    state["best"][tid] = {
                        "bid": msg.get("best_bid", ""),
                        "ask": msg.get("best_ask", ""),
                        "spread": msg.get("spread", ""),
                    }

                elif etype == "last_trade_price":
                    tid = msg.get("asset_id", "")
                    state["last_trade"][tid] = {
                        "price": msg.get("price", ""),
                        "side": msg.get("side", ""),
                        "size": msg.get("size", ""),
                        "ts": msg.get("timestamp", 0),
                    }

            render(depth=ARGS.depth)


async def market_ws_loop(token_ids: List[str]):
    """Polymarket market WS — reconnect forever on drops until the whole run is cancelled."""
    retry_delay: Dict[str, float] = {"v": 1.0}
    max_delay = 60.0
    while True:
        try:
            await _market_ws_session(token_ids, retry_delay=retry_delay)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            d = retry_delay["v"]
            print(col(
                f"[market-ws] {type(exc).__name__}: {exc}; reconnecting in {d:.1f}s…",
                C.YELLOW,
            ))
            await asyncio.sleep(d)
            retry_delay["v"] = min(max_delay, d * 2)


# ── WebSocket feed 2: RTDS — live BTC price ───────────────────────────────────

async def rtds_ws_loop():
    """Connects to RTDS and streams live BTC/USDT price from Binance."""
    async with websockets.connect(
        RTDS_WS,
        ping_interval=None,
        additional_headers={"Origin": "https://polymarket.com"},
    ) as ws:
        await ws.send(json.dumps({
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices", "type": "update", "filters": "btcusdt"}
            ]
        }))

        async def heartbeat():
            while True:
                await asyncio.sleep(5)
                try:
                    await ws.send("PING")
                except Exception:
                    return

        asyncio.create_task(heartbeat())

        async for raw in ws:
            if raw == "PONG":
                continue
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if isinstance(msg, dict) and msg.get("topic") == "crypto_prices" and msg.get("type") == "update":
                payload = msg.get("payload", {})
                price = (
                    payload.get("value")
                    or payload.get("price")
                    or payload.get("last")
                    or payload.get("p")
                )
                ts = payload.get("timestamp") or payload.get("ts")
                _set_btc_price(price, ts, "rtds")
                _update_ptb_from_price("rtds")
                render(depth=ARGS.depth)


# ── WebSocket feed 3: Chainlink BTC/USD via RTDS ───────────────────────────

async def _chainlink_ws_session(*, retry_delay: Dict[str, float]) -> None:
    async with websockets.connect(
        RTDS_WS,
        ping_interval=None,
        additional_headers={"Origin": "https://polymarket.com"},
    ) as ws:
        retry_delay["v"] = 1.0
        await ws.send(json.dumps({
            "action": "subscribe",
            "subscriptions": [
                {"topic": "crypto_prices_chainlink", "type": "*", "filters": "{\"symbol\":\"btc/usd\"}"}
            ]
        }))

        async def heartbeat():
            while True:
                await asyncio.sleep(5)
                try:
                    await ws.send("PING")
                except Exception:
                    return

        asyncio.create_task(heartbeat())

        async def handle_payload(payload):
            if not isinstance(payload, dict):
                return
            data = payload.get("data")
            if isinstance(data, list) and data:
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    price = item.get("value") or item.get("price")
                    ts = item.get("timestamp") or item.get("ts")
                    _record_chainlink_tick(ts, price)
                payload = data[-1]
            price = payload.get("value") or payload.get("price")
            ts = payload.get("timestamp") or payload.get("ts")
            _record_chainlink_tick(ts, price)
            _set_btc_price(price, ts, "chainlink")
            _update_ptb_from_price("chainlink")
            _update_ptb_from_chainlink_ticks()

        async for raw in ws:
            if raw == "PONG":
                continue
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(msg, dict) and msg.get("topic") == "crypto_prices_chainlink":
                payload = msg.get("payload", {})
                await handle_payload(payload)
                render(depth=ARGS.depth)


async def chainlink_ws_loop():
    """RTDS Chainlink BTC/USD — reconnect forever on drops."""
    retry_delay: Dict[str, float] = {"v": 1.0}
    max_delay = 60.0
    while True:
        try:
            await _chainlink_ws_session(retry_delay=retry_delay)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            d = retry_delay["v"]
            print(col(
                f"[chainlink-ws] {type(exc).__name__}: {exc}; reconnecting in {d:.1f}s…",
                C.YELLOW,
            ))
            await asyncio.sleep(d)
            retry_delay["v"] = min(max_delay, d * 2)


# ── HTTP fallback: Pyth Hermes (BTC/USD) ───────────────────────────────────

async def pyth_poll_loop(interval: float = 2.0):
    """Poll Pyth Hermes for BTC/USD and update shared state."""
    url = f"{PYTH_HERMES_URL}/api/latest_price_feeds?ids[]={PYTH_BTC_USD_ID}"
    headers = {"User-Agent": "poly-live-ticker/1.0"}
    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            try:
                async with session.get(url, timeout=8) as r:
                    if r.status == 200:
                        data = await r.json()
                        if data:
                            price_info = data[0].get("price", {})
                            price = price_info.get("price")
                            expo = price_info.get("expo", 0)
                            ts = price_info.get("publish_time")
                            if price is not None:
                                btc = float(price) * (10 ** int(expo))
                                _set_btc_price(btc, int(ts) * 1000 if ts else None, "pyth")
                                _update_ptb_from_price("pyth")
            except Exception:
                pass
            await asyncio.sleep(interval)


# ── Market refresh loop ───────────────────────────────────────────────────────

async def market_refresh_loop(n_windows: int):
    """Re-discovers markets every 60s (15m CLI, or 5m+15m dashboard)."""
    headers = {"User-Agent": "poly-live-ticker/1.0"}
    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            if _dashboard_mode:
                markets = await discover_dashboard_markets(session, n_windows)
            else:
                markets = await discover_markets(session, n_windows)
            if markets:
                state["markets"] = markets
            await asyncio.sleep(60)


async def market_refresh_loop_5m_only(n_windows: int):
    """Gamma refresh for 5m BTC up/down markets only (CSV recorder)."""
    headers = {"User-Agent": "poly-live-ticker/1.0"}
    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            markets = await _discover_band(session, "btc-updown-5m", 300, n_windows)
            if markets:
                state["markets"] = markets
            await asyncio.sleep(60)


async def record_5m_csv_second_loop(csv_path: str, *, depth: int = 5, tick_s: float = 1.0) -> None:
    """
    Every ``tick_s`` seconds, append one CSV row for the **latest active 5m window**.

    UP/DOWN bid & ask columns store the latest best bid/ask probabilities (0–1).
    If any quote field stays empty for more than ``MAX_EMPTY_QUOTE_STREAK_ROWS``
    consecutive written rows, the recorder raises and exits (supervisor can restart it).
    """
    path = Path(csv_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    headers_row = (
        "timestamp_utc_iso",
        "slug",
        "window_end_unix",
        "seconds_left",
        "up_bid_prob",
        "up_ask_prob",
        "down_bid_prob",
        "down_ask_prob",
        "ptb_usd",
        "ptb_source",
        "btc_chainlink_usd",
    )
    header_needed = not path.exists() or path.stat().st_size == 0
    fh = path.open("a", newline="", encoding="utf-8")
    writer = csv.DictWriter(fh, fieldnames=list(headers_row))
    empty_streaks: Dict[str, int] = {
        "up_bid_prob": 0,
        "up_ask_prob": 0,
        "down_bid_prob": 0,
        "down_ask_prob": 0,
    }
    loop_started = time.time()
    try:
        if header_needed:
            writer.writeheader()
            fh.flush()
        print(col(f"[record] CSV 1 Hz → {path}", C.GREEN))

        while True:
            ts_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            now_i = int(time.time())
            btc_spot = state.get("btc_price")
            btc_str = ""
            try:
                if btc_spot is not None:
                    btc_str = f"{float(btc_spot):.2f}"
            except (TypeError, ValueError):
                btc_str = ""

            five_candidates = []
            for m in list(state.get("markets") or []):
                iv = int(m.get("interval_minutes") or 0)
                slug_guess = str(m.get("slug") or "")
                if iv == 5 or "btc-updown-5m" in slug_guess:
                    five_candidates.append(m)

            # Latest active = nearest-expiring non-expired 5m market.
            chosen = None
            if five_candidates:
                def _rank(m: Dict[str, Any]) -> Tuple[int, int]:
                    we_i = int(m.get("window_end") or 0)
                    sec = we_i - now_i
                    if sec >= 0:
                        return (0, sec)
                    return (1, abs(sec))
                chosen = min(five_candidates, key=_rank)

            if chosen is not None:
                slug_guess = str(chosen.get("slug") or "")
                up_tok = chosen.get("up_token")
                down_tok = chosen.get("down_token")
                if up_tok:
                    ub_b, ub_a = token_latest_bid_ask(str(up_tok))
                else:
                    ub_b, ub_a = (None, None)
                if down_tok:
                    db_b, db_a = token_latest_bid_ask(str(down_tok))
                else:
                    db_b, db_a = (None, None)

                ptb_raw = chosen.get("price_to_beat")
                ptb_f = None
                if ptb_raw is not None:
                    try:
                        ptb_f = float(ptb_raw)
                    except (TypeError, ValueError):
                        ptb_f = None
                if ptb_f is None and slug_guess:
                    ptb_f = state.get("ptb_by_slug", {}).get(slug_guess)

                slug = slug_guess
                ptb_src = state.get("ptb_source_by_slug", {}).get(slug) if slug else None
                we = int(chosen.get("window_end") or 0)
                sec_left = we - now_i if we else ""

                writer.writerow(
                    {
                        "timestamp_utc_iso": ts_iso,
                        "slug": slug,
                        "window_end_unix": str(we) if we else "",
                        "seconds_left": str(sec_left),
                        "up_bid_prob": "" if ub_b is None else f"{ub_b:.6f}",
                        "up_ask_prob": "" if ub_a is None else f"{ub_a:.6f}",
                        "down_bid_prob": "" if db_b is None else f"{db_b:.6f}",
                        "down_ask_prob": "" if db_a is None else f"{db_a:.6f}",
                        "ptb_usd": "" if ptb_f is None else f"{float(ptb_f):.4f}",
                        "ptb_source": ptb_src or "",
                        "btc_chainlink_usd": btc_str,
                    }
                )
                for col_name, val in (
                    ("up_bid_prob", ub_b),
                    ("up_ask_prob", ub_a),
                    ("down_bid_prob", db_b),
                    ("down_ask_prob", db_a),
                ):
                    if val is None:
                        empty_streaks[col_name] += 1
                    else:
                        empty_streaks[col_name] = 0
                breached = [
                    f"{k}={v}"
                    for k, v in empty_streaks.items()
                    if v > MAX_EMPTY_QUOTE_STREAK_ROWS
                ]
                if breached and (time.time() - loop_started) >= RECORD_STARTUP_GRACE_SEC:
                    msg = (
                        "[record] exiting: quote field empty for too many consecutive rows: "
                        + ", ".join(breached)
                    )
                    print(col(msg, C.RED))
                    raise RuntimeError(msg)

            fh.flush()
            await asyncio.sleep(max(0.05, tick_s))
    finally:
        fh.close()


# ── Main ─────────────────────────────────────────────────────────────────────

async def run(
    n_windows: int,
    *,
    dashboard_mode: bool = False,
    depth: int = 5,
    record_5m_csv: Optional[str] = None,
):
    global _dashboard_mode, _recording_mode

    csv_dest = (
        record_5m_csv.strip()
        if isinstance(record_5m_csv, str) and record_5m_csv.strip()
        else None
    )
    _recording_mode = csv_dest is not None
    # Recorder shares WS infra but disables terminal/dashboard render paths.
    _dashboard_mode = bool(dashboard_mode) and not _recording_mode

    # Initial market discovery
    headers = {"User-Agent": "poly-live-ticker/1.0"}
    async with aiohttp.ClientSession(headers=headers) as session:
        if csv_dest:
            markets = await _discover_band(session, "btc-updown-5m", 300, n_windows)
            label = "5m CSV"
        elif dashboard_mode:
            markets = await discover_dashboard_markets(session, n_windows)
            label = "5m + 15m"
        else:
            print(col("Discovering active BTC 15m markets…", C.CYAN))
            markets = await discover_markets(session, n_windows)
            label = "15m"

    if not markets:
        if csv_dest:
            print(col(f"No active BTC 5m markets for recording ({label}). Try again shortly.", C.RED))
        elif not dashboard_mode:
            print(col(f"No active BTC ({label}) markets found. Try again in a moment.", C.RED))
        _dashboard_mode = False
        _recording_mode = False
        return

    state["markets"] = markets

    # Collect all token IDs to subscribe to
    all_tokens = []
    for m in markets:
        all_tokens.extend([m["up_token"], m["down_token"]])

    if csv_dest:
        print(
            col(
                f"[record] {len(markets)}× 5m window(s) subscribed; CSV writes latest active 5m only each second…",
                C.GREEN,
            )
        )
        await asyncio.sleep(2.0)
    elif not dashboard_mode:
        print(col(f"Found {len(markets)} active windows ({label}). Connecting to WebSocket feeds…\n", C.GREEN))
        for m in markets:
            print(f"  {col(m['title'], C.WHITE)}  →  up={m['up_token'][:10]}…  down={m['down_token'][:10]}…")
        print()
        await asyncio.sleep(1)

    coros: List[Any] = [
        market_ws_loop(all_tokens),
        chainlink_ws_loop(),
    ]

    if csv_dest:
        coros.extend(
            [
                market_refresh_loop_5m_only(n_windows),
                pyth_poll_loop(),
                record_5m_csv_second_loop(csv_dest, depth=depth, tick_s=1.0),
            ]
        )
    elif dashboard_mode:
        coros.extend(
            [
                market_refresh_loop(n_windows),
                pyth_poll_loop(),
            ]
        )
    else:
        coros.extend(
            [
                market_refresh_loop(n_windows),
                rtds_ws_loop(),
            ]
        )

    try:
        await asyncio.gather(*coros)
    finally:
        _dashboard_mode = False
        _recording_mode = False


def live_snapshot(depth: int = 5) -> Dict[str, Any]:
    """JSON-serializable view of shared `state` for HTTP dashboards."""
    now_ts = time.time()
    btc_age_s = None
    ts_ms = state.get("btc_price_ts") or 0
    if ts_ms:
        btc_age_s = max(0, int((now_ts * 1000 - ts_ms) / 1000))

    rows: List[Dict[str, Any]] = []
    markets = list(state.get("markets") or [])

    for mkt in markets:
        up_tok = mkt["up_token"]
        down_tok = mkt["down_token"]
        bup = dict(state["best"].get(up_tok, {}))
        bdn = dict(state["best"].get(down_tok, {}))
        lt = dict(state["last_trade"].get(up_tok, {}))

        we = int(mkt["window_end"])
        delta = we - int(now_ts)

        bids, asks = top_book(up_tok, depth)

        def _lvl(pairs):
            out = []
            for p, s in pairs:
                try:
                    out.append({"price": float(p), "size": float(s)})
                except (TypeError, ValueError):
                    continue
            return out

        ptb = mkt.get("price_to_beat")
        ptb_f = None
        if ptb is not None:
            try:
                ptb_f = float(ptb)
            except (TypeError, ValueError):
                ptb_f = None

        slug = mkt.get("slug")
        if ptb_f is None and slug:
            ptb_f = state.get("ptb_by_slug", {}).get(slug)
        ptb_src = state.get("ptb_source_by_slug", {}).get(slug) if slug else None

        mid = None
        try:
            ub = float(bup["bid"])
            ua = float(bup["ask"])
            mid = (ub + ua) / 2
        except (KeyError, TypeError, ValueError):
            pass

        rows.append({
            "title": mkt.get("title", ""),
            "slug": mkt.get("slug", ""),
            "interval_minutes": mkt.get("interval_minutes"),
            "window_start": mkt.get("window_start"),
            "window_end": we,
            "seconds_left": delta,
            "price_to_beat": ptb_f,
            "ptb_source": ptb_src,
            "up_mid": mid,
            "up": bup,
            "down": bdn,
            "last_trade_up": lt,
            "book_bids": _lvl(bids),
            "book_asks": _lvl(asks),
        })

    btc_source = state.get("btc_source")
    ptb_note = f"PTB via {PTB_SOURCE}"
    if btc_source == "chainlink":
        feed_note = f"BTC spot via Chainlink (RTDS); {ptb_note}; books via CLOB WS"
    elif btc_source == "pyth":
        feed_note = f"BTC spot via Pyth Hermes; {ptb_note}; books via CLOB WS"
    elif btc_source == "rtds":
        feed_note = f"BTC spot via Polymarket RTDS (Binance); {ptb_note}; books via CLOB WS"
    else:
        feed_note = f"BTC spot pending; {ptb_note}; books via CLOB WS"

    return {
        "btc_usd": state.get("btc_price"),
        "btc_age_seconds": btc_age_s,
        "btc_source": btc_source,
        "msg_count": int(state.get("msg_count", 0)),
        "markets": rows,
        "server_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "feed_note": feed_note,
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Live Polymarket BTC Up/Down 15-min terminal ticker.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--windows", type=int, default=3,
                   help="Number of upcoming windows to subscribe to (same param for recorder).")
    p.add_argument("--depth",   type=int, default=5,
                   help="Order book depth used for averages + snapshots.")
    p.add_argument(
        "--record-5m-csv",
        metavar="PATH",
        default="",
        help="Append 1/sec rows for BTC 5m markets (avg top-N UP bid / UP ask probs, midpoint, PTB). "
             "Runs headless; uses Chainlink+Pyth parity with dashboard for PTB.",
    )
    return p.parse_args()


if __name__ == "__main__":
    ARGS = parse_args()
    rec = getattr(ARGS, "record_5m_csv", "") or ""
    rec_clean = rec.strip() or None
    try:
        asyncio.run(run(ARGS.windows, dashboard_mode=False, depth=ARGS.depth, record_5m_csv=rec_clean))
    except KeyboardInterrupt:
        print("\nExited.")
else:
    # Allow import without running
    import argparse as _ap
    ARGS = _ap.Namespace(windows=3, depth=5, record_5m_csv="")
