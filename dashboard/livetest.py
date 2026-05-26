"""
5m PTB live-strategy simulation from poly_5m_live.csv (1 Hz recorder).

Rules (as specified):
- PTB fixed per slug until that 5m window ends.
- Candle 1 / 2 = first two full minutes of the window; close = last Chainlink BTC print in that minute.
  - Both closes > PTB  -> signal UP (only trade UP side)
  - Both closes < PTB  -> signal DOWN (only trade DOWN side)
  - Else -> no trades
- Entries only when ask <= 80% (0.80). Limit price modeled as ask - $0.01 (one cent).
- At most one trade per distinct limit price (cent) per slug per side (captures 80, 75, 62, …).
- Settlement: last Chainlink BTC strictly before window_end_unix; UP wins if settle > PTB,
  DOWN wins if settle < PTB; equality = push (PnL 0 for that contract).
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

STRATEGY_NOTE = (
    "First 2×1m candles vs PTB define side; entries after min-2 until expiry; "
    "limit = best ask − 1¢; ask must be ≤80¢; one fill per distinct limit (cent) per window."
)

# Bump when API payload shape changes (forces in-memory cache invalidation).
LIVETEST_CACHE_SCHEMA = 2

_csv_df_lock = threading.Lock()
_csv_df_cache: Dict[str, Any] = {
    "path": None,
    "size": 0,
    "mtime": 0.0,
    "columns": None,
    "df": None,
}

_sim_lock = threading.Lock()
_rebuild_lock = threading.Lock()
_sim_cache: Dict[str, Any] = {"key": None, "payload": None, "built_at": 0.0}
_refresh_thread: threading.Thread | None = None
_refresh_started = False


def _read_csv_tail_text(path: Path, n_data_rows: int) -> str:
    """Read header + last ``n_data_rows`` lines only (fast for huge CSVs)."""
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        header = fh.readline()
        if not header:
            return ""
        tail = deque(maxlen=max(1, n_data_rows))
        for line in fh:
            if line.strip():
                tail.append(line)
    return header + "".join(tail)


def _normalize_csv_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # tolerate older schemas
    rename = {}
    if "up_bid_prob" not in df.columns and "up_bid_avg_top5_prob" in df.columns:
        rename.update(
            {
                "up_bid_avg_top5_prob": "up_bid_prob",
                "up_ask_avg_top5_prob": "up_ask_prob",
            }
        )
    if "down_bid_prob" not in df.columns and "down_bid_avg_top5_prob" in df.columns:
        rename.update(
            {
                "down_bid_avg_top5_prob": "down_bid_prob",
                "down_ask_avg_top5_prob": "down_ask_prob",
            }
        )
    if rename:
        df = df.rename(columns=rename)
    need = [
        "timestamp_utc_iso",
        "slug",
        "window_end_unix",
        "up_bid_prob",
        "up_ask_prob",
        "down_bid_prob",
        "down_ask_prob",
        "ptb_usd",
        "btc_chainlink_usd",
    ]
    for c in need:
        if c not in df.columns:
            df[c] = pd.NA

    df["ts"] = pd.to_datetime(df["timestamp_utc_iso"], utc=True)
    epoch = pd.Timestamp("1970-01-01", tz="UTC")
    df["ts_unix"] = ((df["ts"] - epoch) / pd.Timedelta(seconds=1)).astype("float64").round().astype("int64")
    df["window_end_unix"] = pd.to_numeric(df["window_end_unix"], errors="coerce")
    return df


def _parse_csv(path: Path, *, tail_rows: Optional[int] = None) -> pd.DataFrame:
    if tail_rows is not None and tail_rows > 0:
        text = _read_csv_tail_text(path, int(tail_rows))
        if not text.strip():
            return pd.DataFrame()
        df = pd.read_csv(StringIO(text), low_memory=False)
    else:
        df = pd.read_csv(path, low_memory=False)
    return _normalize_csv_df(df)


def _parse_csv_full(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    return _normalize_csv_df(df)


def _csv_file_key(path: Path) -> Tuple[str, int, float]:
    st = path.stat()
    return (str(path.resolve()), int(st.st_size), float(st.st_mtime))


def _load_csv_incremental(path: Path) -> pd.DataFrame:
    """Load full CSV once, then append only new bytes when the recorder grows the file."""
    stat = path.stat()
    path_key = str(path.resolve())
    with _csv_df_lock:
        cache = _csv_df_cache
        if (
            cache["df"] is None
            or cache["path"] != path_key
            or stat.st_size < int(cache["size"])
        ):
            df = _parse_csv(path, tail_rows=None)
            cache.update(
                {
                    "path": path_key,
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                    "columns": list(df.columns),
                    "df": df,
                }
            )
            return cache["df"]

        if stat.st_size > int(cache["size"]):
            cols = cache["columns"]
            with path.open("r", encoding="utf-8", errors="replace") as fh:
                fh.seek(int(cache["size"]))
                chunk = fh.read()
            if chunk.strip() and cols:
                extra = pd.read_csv(
                    StringIO(chunk),
                    header=None,
                    names=list(cols),
                    low_memory=False,
                )
                extra = _normalize_csv_df(extra)
                cache["df"] = pd.concat([cache["df"], extra], ignore_index=True)
            cache["size"] = stat.st_size
            cache["mtime"] = stat.st_mtime

        return cache["df"]


def _attach_csv_meta(out: Dict[str, Any], csv_path: Path, df: pd.DataFrame) -> Dict[str, Any]:
    out["csv_path"] = str(csv_path.resolve())
    out["csv_rows"] = len(df)
    out["csv_all_rows"] = True
    out["data_source"] = "csv_recorder"
    stat = csv_path.stat()
    out["csv_mtime_unix"] = stat.st_mtime
    out["csv_size_bytes"] = stat.st_size
    age_h = max(0, (datetime.now(timezone.utc).timestamp() - stat.st_mtime) / 3600)
    out["csv_age_hours"] = round(age_h, 1)
    out.pop("csv_stale_warning", None)
    if age_h > 0.25:
        out["csv_stale_warning"] = (
            f"Recorder CSV not updated in {out['csv_age_hours']}h — "
            "start scripts/run_poly_recorder_supervisor.sh for live rows."
        )
    return out


def _rebuild_simulation(csv_path: Path, **kwargs: Any) -> Dict[str, Any]:
    with _rebuild_lock:
        df = _load_csv_incremental(csv_path)
        out = simulate_livetest(df, **kwargs)
        out = _attach_csv_meta(out, csv_path, df)
        out["candle_probs"] = analyze_candle_settlement_probs(df)
        out["cache_schema"] = LIVETEST_CACHE_SCHEMA
        return out


def _refresh_loop(csv_path: Path) -> None:
    while True:
        try:
            if csv_path.exists():
                key = _csv_file_key(csv_path)
                with _sim_lock:
                    stale = _sim_cache.get("key") != key
                if stale:
                    payload = _rebuild_simulation(csv_path)
                    with _sim_lock:
                        _sim_cache.update(
                            {"key": key, "payload": payload, "built_at": time.time()}
                        )
        except Exception:
            pass
        time.sleep(3.0)


def ensure_livetest_csv_refresh(csv_path: Path) -> None:
    """Start background full-CSV refresh (never blocks the HTTP request path)."""
    global _refresh_thread, _refresh_started
    if _refresh_started:
        if _refresh_thread is None or not _refresh_thread.is_alive():
            _refresh_thread = threading.Thread(
                target=_refresh_loop,
                args=(csv_path,),
                daemon=True,
                name="livetest-csv-refresh",
            )
            _refresh_thread.start()
        return
    _refresh_started = True
    _refresh_thread = threading.Thread(
        target=_refresh_loop,
        args=(csv_path,),
        daemon=True,
        name="livetest-csv-refresh",
    )
    _refresh_thread.start()


def _fast_tail_snapshot(
    csv_path: Path,
    *,
    tail_rows: int,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Quick response from CSV tail while the full cache rebuilds in background."""
    try:
        df = _parse_csv(csv_path, tail_rows=tail_rows)
    except Exception as exc:  # noqa: BLE001
        return {
            **simulate_livetest(pd.DataFrame()),
            "error": f"CSV read failed: {exc}",
            "csv_path": str(csv_path),
            "data_source": "csv_recorder",
        }
    out = simulate_livetest(df, **kwargs)
    out = _attach_csv_meta(out, csv_path, df)
    out["candle_probs"] = analyze_candle_settlement_probs(df)
    out["fast_tail_rows"] = tail_rows
    out["full_refresh_pending"] = True
    out["cache_hit"] = False
    out["cache_schema"] = LIVETEST_CACHE_SCHEMA
    return out


def _cache_payload_fresh(payload: Optional[Dict[str, Any]]) -> bool:
    if not payload:
        return False
    if payload.get("cache_schema") != LIVETEST_CACHE_SCHEMA:
        return False
    probs = payload.get("candle_probs") or {}
    return "three_red" in probs and "three_green" in probs


def get_livetest_snapshot(
    csv_path: Path,
    *,
    tail_rows: int = 25_000,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Return cached full simulation, stale cache, or a fast tail snapshot."""
    ensure_livetest_csv_refresh(csv_path)
    if not csv_path.exists():
        return {
            **simulate_livetest(pd.DataFrame()),
            "candle_probs": analyze_candle_settlement_probs(pd.DataFrame()),
            "error": f"CSV not found: {csv_path}",
            "csv_path": str(csv_path),
            "data_source": "csv_recorder",
        }

    key = _csv_file_key(csv_path)
    with _sim_lock:
        cached = _sim_cache.get("payload")
        cached_key = _sim_cache.get("key")
        if cached and cached_key == key and _cache_payload_fresh(cached):
            out = dict(cached)
            out["cache_hit"] = True
            out["cache_age_seconds"] = round(time.time() - float(_sim_cache.get("built_at") or 0), 1)
            out.pop("full_refresh_pending", None)
            return out
        stale_payload = dict(cached) if cached and _cache_payload_fresh(cached) else None

    if stale_payload is not None:
        stale_payload["cache_hit"] = False
        stale_payload["cache_stale"] = True
        stale_payload["refresh_pending"] = True
        return stale_payload

    return _fast_tail_snapshot(csv_path, tail_rows=tail_rows, **kwargs)


def _ffill_ptb(series: pd.Series) -> float:
    for v in series:
        if pd.notna(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return float("nan")


def _last_btc_in_range(
    g: pd.DataFrame,
    t0: int,
    t1: int,
) -> Optional[float]:
    """Last non-null btc_chainlink_usd with t0 <= ts_unix < t1."""
    sub = g[(g["ts_unix"] >= t0) & (g["ts_unix"] < t1)]
    sub = sub[sub["btc_chainlink_usd"].notna()].copy()
    if sub.empty:
        return None
    try:
        return float(sub.iloc[-1]["btc_chainlink_usd"])
    except (TypeError, ValueError):
        return None


def _signal_from_closes(c1: Optional[float], c2: Optional[float], ptb: float) -> str:
    if c1 is None or c2 is None or ptb != ptb or ptb <= 0:
        return "PENDING"
    if c1 > ptb and c2 > ptb:
        return "UP"
    if c1 < ptb and c2 < ptb:
        return "DOWN"
    return "NONE"


def _settlement_outcome(settle: Optional[float], ptb: float) -> Optional[str]:
    if settle is None or ptb != ptb:
        return None
    if settle > ptb:
        return "UP"
    if settle < ptb:
        return "DOWN"
    return "PUSH"


def _prob_pct(hits: int, misses: int) -> Optional[float]:
    decided = hits + misses
    return round(100.0 * hits / decided, 2) if decided else None


def analyze_candle_settlement_probs(
    df: pd.DataFrame,
    *,
    clock_ts_unix: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Historical P(5m settles as predicted) from Chainlink 1m closes vs PTB.

    - three_red: minutes 1–3 all close < PTB → predict market closes DOWN
    - three_green: minutes 1–3 all close > PTB → predict market closes UP
    """
    empty_bucket = {
        "windows": 0,
        "settled": 0,
        "hits": 0,
        "misses": 0,
        "pushes": 0,
        "probability_pct": None,
        "hit_rate_note": None,
    }
    if df is None or df.empty:
        return {
            "description": (
                "Chainlink 1m closes vs fixed PTB → probability the 5m window settles "
                "in the predicted direction (last BTC before window end)."
            ),
            "three_red": {
                **empty_bucket,
                "label": "First 3 candles red (all closes < PTB) → 5m closes DOWN",
            },
            "three_green": {
                **empty_bucket,
                "label": "First 3 candles green (all closes > PTB) → 5m closes UP",
            },
            "recent_signals": [],
        }

    now_i = int(time.time()) if clock_ts_unix is None else int(clock_ts_unix)
    red = {"windows": 0, "settled": 0, "hits": 0, "misses": 0, "pushes": 0}
    grn = {"windows": 0, "settled": 0, "hits": 0, "misses": 0, "pushes": 0}
    recent: List[Dict[str, Any]] = []

    for slug, g in df.groupby("slug", sort=False):
        if slug is None or (isinstance(slug, float) and pd.isna(slug)) or slug == "":
            continue
        sk = str(slug)
        g = g.sort_values("ts_unix")
        ends = g["window_end_unix"].dropna()
        if ends.empty:
            continue
        window_end = int(ends.iloc[-1])
        window_start = window_end - 300
        ptb_f = float(_ffill_ptb(g["ptb_usd"]))
        if ptb_f != ptb_f or ptb_f <= 0:
            continue

        c1 = _last_btc_in_range(g, window_start, window_start + 60)
        c2 = _last_btc_in_range(g, window_start + 60, window_start + 120)
        c3 = _last_btc_in_range(g, window_start + 120, window_start + 180)

        is_final = now_i >= window_end
        settle_btc, _ = _settlement(g, window_end, now_i if is_final else now_i)
        outcome = _settlement_outcome(settle_btc, ptb_f)

        row_base: Dict[str, Any] = {
            "slug": sk,
            "window_end_unix": window_end,
            "ptb_usd": ptb_f,
            "candle1_close_btc": c1,
            "candle2_close_btc": c2,
            "candle3_close_btc": c3,
            "settlement_btc": settle_btc,
            "outcome": outcome,
            "settled": is_final and outcome is not None,
        }

        if (
            c1 is not None
            and c2 is not None
            and c3 is not None
            and c1 < ptb_f
            and c2 < ptb_f
            and c3 < ptb_f
        ):
            red["windows"] += 1
            hit: Optional[bool] = None
            if is_final and outcome is not None:
                red["settled"] += 1
                if outcome == "DOWN":
                    red["hits"] += 1
                    hit = True
                elif outcome == "UP":
                    red["misses"] += 1
                    hit = False
                else:
                    red["pushes"] += 1
            recent.append({**row_base, "pattern": "three_red", "predicts": "DOWN", "hit": hit})

        if (
            c1 is not None
            and c2 is not None
            and c3 is not None
            and c1 > ptb_f
            and c2 > ptb_f
            and c3 > ptb_f
        ):
            grn["windows"] += 1
            hit = None
            if is_final and outcome is not None:
                grn["settled"] += 1
                if outcome == "UP":
                    grn["hits"] += 1
                    hit = True
                elif outcome == "DOWN":
                    grn["misses"] += 1
                    hit = False
                else:
                    grn["pushes"] += 1
            recent.append({**row_base, "pattern": "three_green", "predicts": "UP", "hit": hit})

    recent.sort(key=lambda x: int(x.get("window_end_unix") or 0), reverse=True)

    def _finalize(stats: Dict[str, int], label: str) -> Dict[str, Any]:
        dec = stats["hits"] + stats["misses"]
        return {
            **stats,
            "label": label,
            "probability_pct": _prob_pct(stats["hits"], stats["misses"]),
            "hit_rate_note": f"{stats['hits']}/{dec} decided" if dec else None,
        }

    return {
        "description": (
            "Chainlink 1m closes vs fixed PTB → probability the 5m window settles "
            "in the predicted direction (last BTC before window end)."
        ),
        "three_red": _finalize(
            red,
            "First 3 candles red (all closes < PTB) → 5m closes DOWN",
        ),
        "three_green": _finalize(
            grn,
            "First 3 candles green (all closes > PTB) → 5m closes UP",
        ),
        "recent_signals": recent[:24],
    }


def _settlement(
    g: pd.DataFrame,
    window_end: int,
    clock_end: int,
) -> Tuple[Optional[float], str]:
    """
    Settlement reference: last BTC before window_end.
    If clock_end < window_end, window still open — return last BTC up to clock_end as preview.
    """
    if clock_end >= window_end:
        sub = g[(g["ts_unix"] < window_end) & (g["btc_chainlink_usd"].notna())]
    else:
        limit_t = min(clock_end, window_end - 1)
        sub = g[(g["ts_unix"] <= limit_t) & (g["btc_chainlink_usd"].notna())]
    if sub.empty:
        return None, "NO_BTC"
    try:
        bx = float(sub.iloc[-1]["btc_chainlink_usd"])
    except (TypeError, ValueError):
        return None, "NO_BTC"
    if clock_end >= window_end:
        return bx, "FINAL"
    return bx, "OPEN"


def _resolution(ptb: float, settle: Optional[float], side: str) -> Tuple[Optional[bool], str]:
    """True = win, False = loss, None = push/skip."""
    if settle is None or ptb != ptb:
        return None, "MISSING"
    if side == "UP":
        if settle > ptb:
            return True, "UP_WIN"
        if settle < ptb:
            return False, "UP_LOSS"
        return None, "PUSH"
    if side == "DOWN":
        if settle < ptb:
            return True, "DOWN_WIN"
        if settle > ptb:
            return False, "DOWN_LOSS"
        return None, "PUSH"
    return None, "NA"


def format_livetest_response(
    trades: List[Dict[str, Any]],
    windows_out: List[Dict[str, Any]],
    *,
    clock_ts_unix: Optional[int] = None,
    max_odds: float = 0.80,
    tick: float = 0.01,
    data_source: str = "csv",
) -> Dict[str, Any]:
    """Build dashboard JSON from trade + window lists (CSV replay or live engine)."""
    if clock_ts_unix is None:
        clock_ts_unix = int(time.time())

    finals: Dict[str, Dict[str, Any]] = {}
    for w in windows_out:
        slug = str(w.get("slug") or "")
        if not slug:
            continue
        we = int(w.get("window_end_unix") or 0)
        finals[slug] = {
            "window_end_unix": we,
            "ptb_usd": w.get("ptb_usd"),
            "settlement_btc": w.get("settlement_btc_preview"),
            "final": clock_ts_unix >= we or w.get("settlement_label") == "FINAL",
        }

    settled_n = wins = losses = pushes = 0
    pnl_total = 0.0
    for t in trades:
        if t.get("settled") is True and t.get("result") in ("WIN", "LOSS", "PUSH"):
            settled_n += 1
            pnl_total += float(t.get("pnl_usd") or 0.0)
            if t.get("result") == "WIN":
                wins += 1
            elif t.get("result") == "LOSS":
                losses += 1
            else:
                pushes += 1
            continue
        slug = t.get("slug")
        info = finals.get(str(slug)) if slug else None
        if not info or not info.get("final"):
            t["settled"] = False
            t["won"] = None
            t["pnl_usd"] = None
            t["result"] = "OPEN"
            continue
        settle = info.get("settlement_btc")
        ptb = info.get("ptb_usd")
        won, reason = _resolution(
            float(ptb) if ptb is not None else float("nan"),
            settle,
            str(t.get("side")),
        )
        px = float(t.get("limit_px") or 0)
        t["settled"] = True
        t["settlement_btc"] = settle
        t["resolution"] = reason
        settled_n += 1
        if won is True:
            wins += 1
            t["won"] = True
            t["result"] = "WIN"
            t["pnl_usd"] = round(1.0 - px, 4)
        elif won is False:
            losses += 1
            t["won"] = False
            t["result"] = "LOSS"
            t["pnl_usd"] = round(-px, 4)
        else:
            pushes += 1
            t["won"] = None
            t["result"] = "PUSH"
            t["pnl_usd"] = 0.0
        pnl_total += float(t.get("pnl_usd") or 0.0)

    by_odds: Dict[str, Dict[int, Dict[str, Any]]] = {
        "UP": defaultdict(
            lambda: {
                "side": "UP",
                "odds_cents": 0,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pushes": 0,
                "open": 0,
                "pnl_usd": 0.0,
            }
        ),
        "DOWN": defaultdict(
            lambda: {
                "side": "DOWN",
                "odds_cents": 0,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pushes": 0,
                "open": 0,
                "pnl_usd": 0.0,
            }
        ),
    }
    for t in trades:
        side = t.get("side")
        if side not in ("UP", "DOWN"):
            continue
        oc = int(t["odds_cents"])
        b = by_odds[side][oc]
        b["odds_cents"] = oc
        b["odds_label"] = f"{oc}¢"
        b["trades"] += 1
        if not t.get("settled"):
            b["open"] += 1
            continue
        if t.get("result") == "WIN":
            b["wins"] += 1
        elif t.get("result") == "LOSS":
            b["losses"] += 1
        else:
            b["pushes"] += 1
        b["pnl_usd"] += float(t.get("pnl_usd") or 0.0)

    def _finalize_bucket(side: str, oc: int, b: Dict[str, Any]) -> Dict[str, Any]:
        dec = b["wins"] + b["losses"]
        win_pct = round(100.0 * b["wins"] / dec, 2) if dec else None
        return {
            "side": side,
            "odds_cents": oc,
            "odds_label": f"{oc}¢",
            "trades": b["trades"],
            "wins": b["wins"],
            "losses": b["losses"],
            "pushes": b["pushes"],
            "open": b["open"],
            "win_pct_decided": win_pct,
            "pnl_usd": round(b["pnl_usd"], 4),
        }

    up_rows = [_finalize_bucket("UP", oc, dict(v)) for oc, v in sorted(by_odds["UP"].items())]
    dn_rows = [_finalize_bucket("DOWN", oc, dict(v)) for oc, v in sorted(by_odds["DOWN"].items())]

    def _side_summary(side_label: str) -> Dict[str, Any]:
        st = [x for x in trades if x.get("side") == side_label]
        op = wins_s = losses_s = pushes_s = pnl_sd = settled_s = 0
        for x in st:
            if x.get("settled"):
                settled_s += 1
                pnl_sd += float(x.get("pnl_usd") or 0.0)
                r = x.get("result")
                if r == "WIN":
                    wins_s += 1
                elif r == "LOSS":
                    losses_s += 1
                elif r == "PUSH":
                    pushes_s += 1
            elif x.get("result") == "OPEN":
                op += 1
        dec_s = wins_s + losses_s
        return {
            "side": side_label,
            "trades_total": len(st),
            "trades_open": op,
            "trades_settled": settled_s,
            "wins": wins_s,
            "losses": losses_s,
            "pushes": pushes_s,
            "pnl_usd_settled": round(pnl_sd, 4),
            "win_pct_decided": round(100.0 * wins_s / dec_s, 2) if dec_s else None,
        }

    return {
        "strategy": {
            "description": STRATEGY_NOTE,
            "max_ask_prob": max_odds,
            "limit_offset": tick,
            "clock_unix": clock_ts_unix,
            "clock_iso": datetime.fromtimestamp(clock_ts_unix, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        },
        "data_source": data_source,
        "windows": windows_out,
        "trades": sorted(trades, key=lambda x: (-x.get("ts_unix", 0), x.get("slug", "")))[:2500],
        "by_odds": {"UP": up_rows, "DOWN": dn_rows},
        "summary_side": {
            "UP": _side_summary("UP"),
            "DOWN": _side_summary("DOWN"),
        },
        "summary": {
            "trade_count": len(trades),
            "settled_count": settled_n,
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "pnl_usd": round(pnl_total, 4),
            "win_pct_decided": round(100.0 * wins / (wins + losses), 2)
            if (wins + losses)
            else None,
        },
    }


def simulate_livetest(
    df: pd.DataFrame,
    *,
    clock_ts_unix: Optional[int] = None,
    max_odds: float = 0.80,
    tick: float = 0.01,
) -> Dict[str, Any]:
    if df is None or df.empty:
        return {
            "strategy": {
                "description": STRATEGY_NOTE,
                "max_ask_prob": max_odds,
                "limit_offset": tick,
            },
            "windows": [],
            "trades": [],
            "by_odds": {"UP": [], "DOWN": []},
            "summary_side": {
                "UP": {
                    "side": "UP",
                    "trades_total": 0,
                    "trades_open": 0,
                    "trades_settled": 0,
                    "wins": 0,
                    "losses": 0,
                    "pushes": 0,
                    "pnl_usd_settled": 0.0,
                    "win_pct_decided": None,
                },
                "DOWN": {
                    "side": "DOWN",
                    "trades_total": 0,
                    "trades_open": 0,
                    "trades_settled": 0,
                    "wins": 0,
                    "losses": 0,
                    "pushes": 0,
                    "pnl_usd_settled": 0.0,
                    "win_pct_decided": None,
                },
            },
            "summary": {
                "trade_count": 0,
                "settled_count": 0,
                "wins": 0,
                "losses": 0,
                "pushes": 0,
                "pnl_usd": 0.0,
                "win_pct_decided": None,
            },
            "error": "No rows in CSV yet.",
        }

    # Always use real current time as the clock so historical windows are settled correctly.
    # CSV may have stopped recording hours ago, but those windows have since expired.
    now_real = int(time.time())
    if clock_ts_unix is None:
        clock_ts_unix = now_real

    trades: List[Dict[str, Any]] = []
    slug_groups = list(df.groupby("slug", sort=False))

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

        c1_close = _last_btc_in_range(g, window_start, window_start + 60)
        c2_close = _last_btc_in_range(g, window_start + 60, window_start + 120)

        sig = _signal_from_closes(c1_close, c2_close, ptb_f)

        recorded_up: set[int] = set()
        recorded_dn: set[int] = set()

        entry_start = window_start + 120
        if sig not in ("UP", "DOWN"):
            continue

        sub = g[
            (g["ts_unix"] >= entry_start)
            & (g["ts_unix"] <= clock_ts_unix)
            & (g["ts_unix"] < window_end)
        ]
        ask_col = "up_ask_prob" if sig == "UP" else "down_ask_prob"
        rec_local = recorded_up if sig == "UP" else recorded_dn
        iso_list = sub["timestamp_utc_iso"].astype(str).tolist()
        tsu_list = sub["ts_unix"].astype("int64").tolist()
        ask_list = sub[ask_col].tolist()

        # Settle immediately while we have full per-slug BTC data.
        # We use clock_ts_unix (real now) — if window_end < now, it's final.
        is_final = clock_ts_unix >= window_end
        settle_btc: Optional[float] = None
        settle_resolution: Optional[bool] = None
        settle_reason = "OPEN"
        if is_final:
            settle_btc, settle_label = _settlement(g, window_end, clock_ts_unix)
            settle_resolution, settle_reason = _resolution(ptb_f, settle_btc, sig)

        for iso, tsu_i, ask_raw in zip(iso_list, tsu_list, ask_list):
            tsu_v = int(tsu_i)
            if pd.isna(ask_raw):
                continue
            try:
                ask = float(ask_raw)
            except (TypeError, ValueError):
                continue
            if ask > max_odds or ask <= 0:
                continue
            limit_px_v = round(ask - tick, 4)
            if limit_px_v < 0.01:
                continue
            cent = int(round(limit_px_v * 100))
            if cent in rec_local:
                continue
            rec_local.add(cent)
            trade: Dict[str, Any] = {
                "slug": sk,
                "timestamp_utc_iso": str(iso),
                "ts_unix": tsu_v,
                "side": sig,
                "ask_live": float(ask),
                "limit_px": float(limit_px_v),
                "odds_cents": cent,
                "odds_label": f"{cent}¢",
                "ptb_usd": ptb_f if ptb_f == ptb_f else None,
                "window_end_unix": window_end,
            }
            if is_final:
                trade["settled"] = True
                trade["settlement_btc"] = settle_btc
                trade["resolution"] = settle_reason
                if settle_resolution is True:
                    trade["won"] = True
                    trade["result"] = "WIN"
                    trade["pnl_usd"] = round(1.0 - limit_px_v, 4)
                elif settle_resolution is False:
                    trade["won"] = False
                    trade["result"] = "LOSS"
                    trade["pnl_usd"] = round(-limit_px_v, 4)
                else:
                    trade["won"] = None
                    trade["result"] = "PUSH"
                    trade["pnl_usd"] = 0.0
            else:
                trade["settled"] = False
                trade["result"] = "OPEN"
            trades.append(trade)

    # Window cards (most recent first)
    windows_out: List[Dict[str, Any]] = []
    for slug, g in sorted(
        slug_groups,
        key=lambda x: (
            int(x[1]["window_end_unix"].iloc[-1])
            if len(x[1]["window_end_unix"].dropna())
            else 0
        ),
        reverse=True,
    )[:48]:
        slug_s, gg = slug, g.sort_values("ts_unix")
        ends = gg["window_end_unix"].dropna()
        if ends.empty:
            continue
        window_end = int(ends.iloc[-1])
        window_start = window_end - 300
        ptb = _ffill_ptb(gg["ptb_usd"])
        ptb_f = float(ptb) if ptb == ptb else float("nan")

        c1 = _last_btc_in_range(gg, window_start, window_start + 60)
        c2 = _last_btc_in_range(gg, window_start + 60, window_start + 120)
        sig = _signal_from_closes(c1, c2, ptb_f)
        if clock_ts_unix < window_start + 120:
            sig = "PENDING"
        settle_btc, settle_lab = _settlement(gg, window_end, clock_ts_unix)

        slug_key = str(slug_s)
        tw = sum(1 for tr in trades if tr["slug"] == slug_key)
        windows_out.append(
            {
                "slug": slug_key,
                "window_start_unix": window_start,
                "window_end_unix": window_end,
                "seconds_left_approx": max(0, window_end - clock_ts_unix),
                "live_clock_unix": clock_ts_unix,
                "ptb_usd": ptb_f if ptb_f == ptb_f else None,
                "candle1_close_btc": c1,
                "candle2_close_btc": c2,
                "signal": sig,
                "settlement_btc_preview": settle_btc,
                "settlement_label": settle_lab,
                "trades_in_window": tw,
                "last_iso": str(gg.iloc[-1]["timestamp_utc_iso"]),
            }
        )

    out = format_livetest_response(
        trades,
        windows_out,
        clock_ts_unix=clock_ts_unix,
        max_odds=max_odds,
        tick=tick,
        data_source="csv_recorder",
    )
    out["csv_rows"] = len(df)
    return out


def load_and_simulate(csv_path: Path, *, tail_rows: Optional[int] = None, **kwargs: Any) -> Dict[str, Any]:
    if tail_rows is not None and tail_rows > 0:
        if not csv_path.exists():
            return {
                **simulate_livetest(pd.DataFrame()),
                "error": f"CSV not found: {csv_path}",
                "csv_path": str(csv_path),
            }
        try:
            df = _parse_csv(csv_path, tail_rows=tail_rows)
        except Exception as exc:  # noqa: BLE001
            return {
                **simulate_livetest(pd.DataFrame()),
                "error": f"CSV read failed: {exc}",
                "csv_path": str(csv_path),
            }
        out = simulate_livetest(df, **kwargs)
        out = _attach_csv_meta(out, csv_path, df)
        out["candle_probs"] = analyze_candle_settlement_probs(df)
        out["cache_schema"] = LIVETEST_CACHE_SCHEMA
        return out

    return get_livetest_snapshot(csv_path, **kwargs)
