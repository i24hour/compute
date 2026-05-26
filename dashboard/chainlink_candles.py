"""
Build 1-minute OHLC candles from Chainlink BTC/USD ticks.

Sources (merged):
  1. Live RTDS Chainlink ticks (poly_live_ticker.CHAINLINK_TICKS)
  2. Recorder CSV tail (btc_chainlink_usd @ ~1 Hz) for history when file exists
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

Tick = Tuple[int, float]  # (unix_seconds, price)

_csv_ticks_lock = threading.Lock()
_csv_ticks_cache: Dict[str, Any] = {"key": None, "ticks": []}


def _read_csv_tail_text(path: Path, n_data_rows: int) -> str:
    """Read header + last n_data_rows lines only."""
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        header = fh.readline()
        if not header:
            return ""
        from collections import deque

        tail = deque(maxlen=max(1, n_data_rows))
        for line in fh:
            if line.strip():
                tail.append(line)
    return header + "".join(tail)


def _parse_csv_ticks(path: Path, *, tail_rows: int = 50_000) -> List[Tick]:
    if not path.exists() or path.stat().st_size == 0:
        return []

    stat = path.stat()
    cache_key = (str(path.resolve()), int(stat.st_size), float(stat.st_mtime), int(tail_rows))
    with _csv_ticks_lock:
        if _csv_ticks_cache.get("key") == cache_key and _csv_ticks_cache.get("ticks") is not None:
            return list(_csv_ticks_cache["ticks"])

    try:
        text = _read_csv_tail_text(path, tail_rows if tail_rows > 0 else 50_000)
        if not text.strip():
            return []
        df = pd.read_csv(StringIO(text), low_memory=False)
    except Exception:
        return []
    if df.empty or "btc_chainlink_usd" not in df.columns:
        return []
    if "timestamp_utc_iso" not in df.columns:
        return []

    df = df[df["btc_chainlink_usd"].notna()].copy()
    if df.empty:
        return []

    df["ts"] = pd.to_datetime(df["timestamp_utc_iso"], utc=True, errors="coerce")
    df = df[df["ts"].notna()]
    if df.empty:
        return []

    epoch = pd.Timestamp("1970-01-01", tz="UTC")
    df["ts_unix"] = ((df["ts"] - epoch) / pd.Timedelta(seconds=1)).astype("float64").round().astype("int64")

    out: List[Tick] = []
    for tsu, px in zip(df["ts_unix"].tolist(), df["btc_chainlink_usd"].tolist()):
        try:
            out.append((int(tsu), float(px)))
        except (TypeError, ValueError):
            continue

    with _csv_ticks_lock:
        _csv_ticks_cache["key"] = cache_key
        _csv_ticks_cache["ticks"] = out
    return out


def _live_ticks_from_poly() -> List[Tick]:
    try:
        import poly_live_ticker as poly
    except ImportError:
        return []
    ticks_ms = list(poly.CHAINLINK_TICKS)
    out: List[Tick] = []
    for ts_ms, px in ticks_ms:
        try:
            out.append((int(ts_ms) // 1000, float(px)))
        except (TypeError, ValueError):
            continue
    return out


def _merge_ticks(*sources: List[Tick]) -> List[Tick]:
    merged: Dict[int, float] = {}
    for src in sources:
        for ts, px in src:
            merged[ts] = px
    return sorted(merged.items(), key=lambda x: x[0])


def ticks_to_candles(ticks: List[Tick]) -> List[Dict[str, Any]]:
    """1m buckets; each candle time = minute open (unix seconds UTC)."""
    if not ticks:
        return []

    buckets: Dict[int, Dict[str, Any]] = {}
    for ts, px in sorted(ticks, key=lambda x: x[0]):
        minute = (ts // 60) * 60
        if minute not in buckets:
            buckets[minute] = {
                "time": minute,
                "open": px,
                "high": px,
                "low": px,
                "close": px,
                "ticks": 1,
            }
        else:
            c = buckets[minute]
            c["high"] = max(c["high"], px)
            c["low"] = min(c["low"], px)
            c["close"] = px
            c["ticks"] += 1

    rows = [buckets[m] for m in sorted(buckets.keys())]
    for r in rows:
        r["open"] = round(r["open"], 2)
        r["high"] = round(r["high"], 2)
        r["low"] = round(r["low"], 2)
        r["close"] = round(r["close"], 2)
    return rows


def build_snapshot(
    *,
    csv_path: Optional[Path] = None,
    csv_tail_rows: int = 50_000,
    max_candles: int = 480,
) -> Dict[str, Any]:
    csv_ticks: List[Tick] = []
    if csv_path is not None:
        csv_ticks = _parse_csv_ticks(csv_path, tail_rows=csv_tail_rows)

    live_ticks = _live_ticks_from_poly()
    all_ticks = _merge_ticks(csv_ticks, live_ticks)
    candles = ticks_to_candles(all_ticks)

    if max_candles > 0 and len(candles) > max_candles:
        candles = candles[-max_candles:]

    now_s = int(time.time())
    current_minute = (now_s // 60) * 60
    forming = candles[-1] if candles and candles[-1]["time"] == current_minute else None
    closed = candles[:-1] if forming else candles

    last_px = None
    last_ts = None
    if all_ticks:
        last_ts, last_px = all_ticks[-1]
        last_px = round(last_px, 2)

    try:
        import poly_live_ticker as poly

        spot = poly.state.get("btc_price")
        src = poly.state.get("btc_source")
        if spot is not None and src == "chainlink":
            last_px = round(float(spot), 2)
    except ImportError:
        src = "chainlink" if live_ticks else "csv"

    sec_into_candle = now_s - current_minute
    sec_left = 60 - sec_into_candle

    return {
        "source": "chainlink",
        "server_time_unix": now_s,
        "server_time_iso": datetime.fromtimestamp(now_s, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "last_price": last_px,
        "last_tick_unix": last_ts,
        "tick_count": len(all_ticks),
        "csv_ticks": len(csv_ticks),
        "live_ticks": len(live_ticks),
        "current_minute_unix": current_minute,
        "seconds_into_candle": sec_into_candle,
        "seconds_until_close": sec_left,
        "forming_candle": forming,
        "candles_closed": closed,
        "candles_all": candles,
    }
