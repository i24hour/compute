"""
3×1m Chainlink candle closes vs PTB → 5m settlement probability.

If all three minute closes are above PTB → P(market closes UP at expiry).
If all three are below PTB → P(market closes DOWN at expiry).

Uses full poly_5m_live.csv (historical + live recorder append).
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from livetest import (
    _attach_csv_meta,
    _csv_file_key,
    _ffill_ptb,
    _last_btc_in_range,
    _load_csv_incremental,
    _settlement,
)

DESCRIPTION = (
    "First 3×1m Chainlink closes vs fixed PTB. "
    "All three above PTB → track P(5m settles UP). "
    "All three below PTB → track P(5m settles DOWN). "
    "Settlement = last Chainlink BTC before window_end."
)

_c3_lock = threading.Lock()
_c3_rebuild_lock = threading.Lock()
_c3_cache: Dict[str, Any] = {"key": None, "payload": None, "built_at": 0.0}
_c3_refresh_thread: threading.Thread | None = None
_c3_refresh_started = False


def _candle_closes(g: pd.DataFrame, window_start: int) -> tuple[Optional[float], Optional[float], Optional[float]]:
    c1 = _last_btc_in_range(g, window_start, window_start + 60)
    c2 = _last_btc_in_range(g, window_start + 60, window_start + 120)
    c3 = _last_btc_in_range(g, window_start + 120, window_start + 180)
    return c1, c2, c3


def _three_signal(
    c1: Optional[float],
    c2: Optional[float],
    c3: Optional[float],
    ptb: float,
) -> str:
    if c1 is None or c2 is None or c3 is None or ptb != ptb or ptb <= 0:
        return "INCOMPLETE"
    if c1 > ptb and c2 > ptb and c3 > ptb:
        return "THREE_POSITIVE"
    if c1 < ptb and c2 < ptb and c3 < ptb:
        return "THREE_NEGATIVE"
    return "MIXED"


def _outcome_label(settle: Optional[float], ptb: float) -> Optional[str]:
    if settle is None or ptb != ptb:
        return None
    if settle > ptb:
        return "UP"
    if settle < ptb:
        return "DOWN"
    return "PUSH"


def analyze_candle3_prob(
    df: pd.DataFrame,
    *,
    clock_ts_unix: Optional[int] = None,
) -> Dict[str, Any]:
    if df is None or df.empty:
        return {
            "description": DESCRIPTION,
            "windows_total": 0,
            "positive": {"windows": 0, "settled": 0, "hits": 0, "misses": 0, "pushes": 0, "probability_pct": None},
            "negative": {"windows": 0, "settled": 0, "hits": 0, "misses": 0, "pushes": 0, "probability_pct": None},
            "recent": [],
            "live_pending": [],
            "error": "No CSV rows yet.",
        }

    now_i = int(time.time()) if clock_ts_unix is None else int(clock_ts_unix)
    slug_groups = list(df.groupby("slug", sort=False))

    pos_settled = pos_hits = pos_misses = pos_pushes = 0
    neg_settled = neg_hits = neg_misses = neg_pushes = 0
    pos_total = neg_total = 0
    recent: List[Dict[str, Any]] = []
    live_pending: List[Dict[str, Any]] = []

    for slug, g in slug_groups:
        if slug is None or (isinstance(slug, float) and pd.isna(slug)) or slug == "":
            continue
        sk = str(slug)
        g = g.sort_values("ts_unix")
        ends = g["window_end_unix"].dropna()
        if ends.empty:
            continue
        window_end = int(ends.iloc[-1])
        window_start = window_end - 300
        ptb = _ffill_ptb(g["ptb_usd"])
        ptb_f = float(ptb) if ptb == ptb else float("nan")

        c1, c2, c3 = _candle_closes(g, window_start)
        sig = _three_signal(c1, c2, c3, ptb_f)
        if sig not in ("THREE_POSITIVE", "THREE_NEGATIVE"):
            continue

        is_final = now_i >= window_end
        settle_btc, settle_lab = _settlement(g, window_end, now_i if is_final else now_i)
        outcome = _outcome_label(settle_btc, ptb_f)

        row: Dict[str, Any] = {
            "slug": sk,
            "window_start_unix": window_start,
            "window_end_unix": window_end,
            "ptb_usd": ptb_f if ptb_f == ptb_f else None,
            "candle1_close_btc": c1,
            "candle2_close_btc": c2,
            "candle3_close_btc": c3,
            "signal": sig,
            "settlement_btc": settle_btc,
            "outcome": outcome,
            "settled": is_final and outcome is not None,
            "seconds_left": max(0, window_end - now_i) if not is_final else 0,
        }

        if sig == "THREE_POSITIVE":
            pos_total += 1
            if is_final and outcome is not None:
                pos_settled += 1
                if outcome == "UP":
                    pos_hits += 1
                    row["hit"] = True
                elif outcome == "DOWN":
                    pos_misses += 1
                    row["hit"] = False
                else:
                    pos_pushes += 1
                    row["hit"] = None
            elif not is_final and now_i >= window_start + 180:
                live_pending.append({**row, "awaiting": "UP"})
        else:
            neg_total += 1
            if is_final and outcome is not None:
                neg_settled += 1
                if outcome == "DOWN":
                    neg_hits += 1
                    row["hit"] = True
                elif outcome == "UP":
                    neg_misses += 1
                    row["hit"] = False
                else:
                    neg_pushes += 1
                    row["hit"] = None
            elif not is_final and now_i >= window_start + 180:
                live_pending.append({**row, "awaiting": "DOWN"})

        recent.append(row)

    recent.sort(key=lambda x: int(x.get("window_end_unix") or 0), reverse=True)

    def _prob(hits: int, misses: int) -> Optional[float]:
        dec = hits + misses
        return round(100.0 * hits / dec, 2) if dec else None

    pos_dec = pos_hits + pos_misses
    neg_dec = neg_hits + neg_misses

    return {
        "description": DESCRIPTION,
        "clock_unix": now_i,
        "clock_iso": datetime.fromtimestamp(now_i, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "windows_total": len(recent),
        "positive": {
            "label": "3 candles above PTB → market closes UP",
            "windows": pos_total,
            "settled": pos_settled,
            "hits": pos_hits,
            "misses": pos_misses,
            "pushes": pos_pushes,
            "probability_pct": _prob(pos_hits, pos_misses),
            "hit_rate_note": f"{pos_hits}/{pos_dec} decided" if pos_dec else None,
        },
        "negative": {
            "label": "3 candles below PTB → market closes DOWN",
            "windows": neg_total,
            "settled": neg_settled,
            "hits": neg_hits,
            "misses": neg_misses,
            "pushes": neg_pushes,
            "probability_pct": _prob(neg_hits, neg_misses),
            "hit_rate_note": f"{neg_hits}/{neg_dec} decided" if neg_dec else None,
        },
        "recent": recent[:64],
        "live_pending": sorted(live_pending, key=lambda x: x.get("window_end_unix", 0))[:8],
    }


def _rebuild(csv_path: Path) -> Dict[str, Any]:
    with _c3_rebuild_lock:
        df = _load_csv_incremental(csv_path)
        out = analyze_candle3_prob(df)
        return _attach_csv_meta(out, csv_path, df)


def _refresh_loop(csv_path: Path) -> None:
    while True:
        try:
            if csv_path.exists():
                key = _csv_file_key(csv_path)
                with _c3_lock:
                    stale = _c3_cache.get("key") != key
                if stale:
                    payload = _rebuild(csv_path)
                    with _c3_lock:
                        _c3_cache.update({"key": key, "payload": payload, "built_at": time.time()})
        except Exception:
            pass
        time.sleep(3.0)


def ensure_refresh(csv_path: Path) -> None:
    global _c3_refresh_thread, _c3_refresh_started
    if not _c3_refresh_started:
        _c3_refresh_started = True
        if csv_path.exists():
            key = _csv_file_key(csv_path)
            payload = _rebuild(csv_path)
            with _c3_lock:
                _c3_cache.update({"key": key, "payload": payload, "built_at": time.time()})
        _c3_refresh_thread = threading.Thread(
            target=_refresh_loop,
            args=(csv_path,),
            daemon=True,
            name="candle3-csv-refresh",
        )
        _c3_refresh_thread.start()
        return
    if _c3_refresh_thread is None or not _c3_refresh_thread.is_alive():
        _c3_refresh_thread = threading.Thread(
            target=_refresh_loop,
            args=(csv_path,),
            daemon=True,
            name="candle3-csv-refresh",
        )
        _c3_refresh_thread.start()


def get_snapshot(csv_path: Path) -> Dict[str, Any]:
    ensure_refresh(csv_path)
    if not csv_path.exists():
        return {**analyze_candle3_prob(pd.DataFrame()), "error": f"CSV not found: {csv_path}"}

    key = _csv_file_key(csv_path)
    with _c3_lock:
        cached = _c3_cache.get("payload")
        cached_key = _c3_cache.get("key")
        if cached and cached_key == key:
            out = dict(cached)
            out["cache_hit"] = True
            out["cache_age_seconds"] = round(time.time() - float(_c3_cache.get("built_at") or 0), 1)
            return out
        stale = dict(cached) if cached else None

    if stale is not None:
        stale["cache_hit"] = False
        stale["cache_stale"] = True
        stale["refresh_pending"] = True
        return stale

    payload = _rebuild(csv_path)
    with _c3_lock:
        _c3_cache.update({"key": key, "payload": payload, "built_at": time.time()})
    payload = dict(payload)
    payload["cache_hit"] = False
    return payload
