"""
Live paper livetest — Chainlink RTDS + Polymarket CLOB (no CSV).

Same strategy as livetest.py:
  5m windows · 2×1m closes vs PTB · ask≤80¢ · limit ask−1¢ · one fill per cent per side.
"""

from __future__ import annotations

import json
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from livetest import (
    STRATEGY_NOTE,
    _resolution,
    _signal_from_closes,
    format_livetest_response,
)

Tick = Tuple[int, float]  # unix seconds, price

MAX_ODDS = 0.80
TICK = 0.01
STATE_PATH = Path(__file__).resolve().parent.parent / "data" / "livetest_live_state.json"

_lock = threading.Lock()
_thread: threading.Thread | None = None
_started = False

_ticks: List[Tick] = []
_trades: List[Dict[str, Any]] = []
_windows: Dict[str, Dict[str, Any]] = {}
_last_snapshot: Dict[str, Any] = {}


def _ingest_chainlink_ticks() -> None:
    global _ticks
    try:
        import poly_live_ticker as poly
    except ImportError:
        return

    merged: Dict[int, float] = {ts: px for ts, px in _ticks}
    for ts_ms, px in list(poly.CHAINLINK_TICKS):
        try:
            merged[int(ts_ms) // 1000] = float(px)
        except (TypeError, ValueError):
            continue

    spot = poly.state.get("btc_price")
    src = poly.state.get("btc_source")
    ts_ms = poly.state.get("btc_price_ts") or 0
    if spot is not None and src == "chainlink" and ts_ms:
        try:
            merged[int(ts_ms) // 1000] = float(spot)
        except (TypeError, ValueError):
            pass

    _ticks = sorted(merged.items())[-50000:]


def _last_btc(t0: int, t1: int) -> Optional[float]:
    for ts, px in reversed(_ticks):
        if ts < t0:
            break
        if t0 <= ts < t1:
            return px
    return None


def _last_btc_before(limit_ts: int) -> Optional[float]:
    for ts, px in reversed(_ticks):
        if ts < limit_ts:
            return px
    return None


def _ptb_for_market(mkt: Dict[str, Any]) -> Optional[float]:
    slug = mkt.get("slug")
    raw = mkt.get("price_to_beat")
    if raw is not None:
        try:
            v = float(raw)
            if v > 0:
                return v
        except (TypeError, ValueError):
            pass
    try:
        import poly_live_ticker as poly

        if slug:
            v = poly.state.get("ptb_by_slug", {}).get(slug)
            if v is not None:
                return float(v)
    except ImportError:
        pass
    return None


def _ask_for_side(mkt: Dict[str, Any], side: str) -> Optional[float]:
    try:
        import poly_live_ticker as poly

        tok = mkt["up_token"] if side == "UP" else mkt["down_token"]
        _, ask = poly.token_latest_bid_ask(str(tok))
        return ask
    except ImportError:
        return None


def _five_m_markets() -> List[Dict[str, Any]]:
    try:
        import poly_live_ticker as poly
    except ImportError:
        return []
    out = []
    for m in list(poly.state.get("markets") or []):
        iv = int(m.get("interval_minutes") or 0)
        slug = str(m.get("slug") or "")
        if iv == 5 or "btc-updown-5m" in slug:
            out.append(m)
    return out


def _maybe_add_trade(
    slug: str,
    side: str,
    ask: float,
    *,
    now_i: int,
    window_end: int,
    ptb: Optional[float],
    recorded: Set[int],
) -> None:
    if ask > MAX_ODDS or ask <= 0:
        return
    limit_px = round(ask - TICK, 4)
    if limit_px < 0.01:
        return
    cent = int(round(limit_px * 100))
    if cent in recorded:
        return
    recorded.add(cent)
    iso = datetime.fromtimestamp(now_i, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _trades.append(
        {
            "slug": slug,
            "timestamp_utc_iso": iso,
            "ts_unix": now_i,
            "side": side,
            "ask_live": float(ask),
            "limit_px": float(limit_px),
            "odds_cents": cent,
            "odds_label": f"{cent}¢",
            "ptb_usd": ptb,
            "window_end_unix": window_end,
            "settled": False,
            "result": "OPEN",
        }
    )


def _settle_slug(slug: str, window_end: int, ptb: Optional[float]) -> None:
    settle = _last_btc_before(window_end)
    for t in _trades:
        if t.get("slug") != slug or t.get("settled"):
            continue
        if t.get("ts_unix", 0) >= window_end:
            continue
        side = t.get("side")
        px = float(t.get("limit_px") or 0)
        ptb_f = float(ptb) if ptb is not None else float("nan")
        won, reason = _resolution(ptb_f, settle, str(side))
        t["settled"] = True
        t["settlement_btc"] = settle
        t["resolution"] = reason
        if won is True:
            t["won"] = True
            t["result"] = "WIN"
            t["pnl_usd"] = round(1.0 - px, 4)
        elif won is False:
            t["won"] = False
            t["result"] = "LOSS"
            t["pnl_usd"] = round(-px, 4)
        else:
            t["won"] = None
            t["result"] = "PUSH"
            t["pnl_usd"] = 0.0


def _engine_step() -> None:
    global _windows
    _ingest_chainlink_ticks()
    now_i = int(time.time())

    active_slugs = set()
    for mkt in _five_m_markets():
        slug = str(mkt.get("slug") or "")
        if not slug:
            continue
        we = int(mkt.get("window_end") or 0)
        ws = int(mkt.get("window_start") or (we - 300))
        if we <= now_i:
            continue
        active_slugs.add(slug)

        ptb = _ptb_for_market(mkt)
        c1 = _last_btc(ws, ws + 60)
        c2 = _last_btc(ws + 60, ws + 120)
        sig = _signal_from_closes(c1, c2, float(ptb) if ptb else float("nan"))
        if now_i < ws + 120:
            sig = "PENDING"

        w = _windows.setdefault(
            slug,
            {
                "recorded_up": set(),
                "recorded_dn": set(),
                "window_start": ws,
                "window_end": we,
            },
        )
        w["window_start"] = ws
        w["window_end"] = we
        w["ptb_usd"] = ptb
        w["candle1_close_btc"] = c1
        w["candle2_close_btc"] = c2
        w["signal"] = sig

        if sig in ("UP", "DOWN") and now_i >= ws + 120 and now_i < we:
            ask = _ask_for_side(mkt, sig)
            if ask is not None:
                rec = w["recorded_up"] if sig == "UP" else w["recorded_dn"]
                _maybe_add_trade(
                    slug,
                    sig,
                    ask,
                    now_i=now_i,
                    window_end=we,
                    ptb=ptb,
                    recorded=rec,
                )

    # Settle expired windows
    for slug, w in list(_windows.items()):
        we = int(w.get("window_end") or 0)
        if we and now_i >= we:
            _settle_slug(slug, we, w.get("ptb_usd"))

    # Drop old window state (keep last 64 slugs)
    if len(_windows) > 64:
        for slug in sorted(_windows.keys(), key=lambda s: _windows[s].get("window_end", 0))[:-64]:
            if slug not in active_slugs:
                del _windows[slug]

    if len(_trades) > 5000:
        _trades[:] = _trades[-5000:]


def _build_windows_out(now_i: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for slug, w in sorted(
        _windows.items(),
        key=lambda x: int(x[1].get("window_end") or 0),
        reverse=True,
    )[:48]:
        we = int(w.get("window_end") or 0)
        ws = int(w.get("window_start") or 0)
        settle = _last_btc_before(we) if now_i >= we else _last_btc(now_i - 300, now_i + 1)
        rows.append(
            {
                "slug": slug,
                "window_start_unix": ws,
                "window_end_unix": we,
                "seconds_left_approx": max(0, we - now_i),
                "live_clock_unix": now_i,
                "ptb_usd": w.get("ptb_usd"),
                "candle1_close_btc": w.get("candle1_close_btc"),
                "candle2_close_btc": w.get("candle2_close_btc"),
                "signal": w.get("signal", "PENDING"),
                "settlement_btc_preview": settle,
                "settlement_label": "FINAL" if now_i >= we else "OPEN",
                "trades_in_window": sum(1 for t in _trades if t.get("slug") == slug),
                "last_iso": datetime.fromtimestamp(now_i, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
            }
        )
    return rows


def _persist_state(payload: Dict[str, Any]) -> None:
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    except OSError:
        pass


def _loop() -> None:
    while True:
        try:
            with _lock:
                _engine_step()
                now_i = int(time.time())
                payload = format_livetest_response(
                    list(_trades),
                    _build_windows_out(now_i),
                    clock_ts_unix=now_i,
                    data_source="live_chainlink_clob",
                )
                payload["live"] = True
                payload["data_source"] = "Chainlink RTDS + Polymarket CLOB (5m paper)"
                payload["engine_running"] = True
                payload["chainlink_tick_count"] = len(_ticks)
                global _last_snapshot
                _last_snapshot = payload
                _persist_state(payload)
        except Exception as exc:  # noqa: BLE001
            err = {
                "live": True,
                "engine_running": True,
                "error": str(exc),
                "strategy": {"description": STRATEGY_NOTE},
            }
            with _lock:
                _last_snapshot = err
        time.sleep(1.0)


def ensure_livetest_live_engine() -> None:
    global _thread, _started
    if _started and _thread and _thread.is_alive():
        return
    _started = True
    _thread = threading.Thread(target=_loop, daemon=True, name="livetest-live-engine")
    _thread.start()


def get_snapshot() -> Dict[str, Any]:
    ensure_livetest_live_engine()
    with _lock:
        if _last_snapshot:
            return dict(_last_snapshot)
    return {
        "live": True,
        "engine_running": _thread is not None and _thread.is_alive(),
        "strategy": {"description": STRATEGY_NOTE},
        "trades": [],
        "windows": [],
        "by_odds": {"UP": [], "DOWN": []},
        "summary": {"trade_count": 0, "pnl_usd": 0.0},
        "summary_side": {},
        "data_source": "Chainlink RTDS + Polymarket CLOB (starting…)",
    }
