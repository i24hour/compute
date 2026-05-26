"""
Polymarket BTC Strategy Backtest Dashboard
Run: pip install -r dashboard/requirements.txt && ../dashboard_venv/bin/python app.py
Then open: http://localhost:5050/  ·  Live: /live  ·  Live test: /livetest  ·  Chainlink 1m: /candles
"""

import json
import sys
import threading
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from flask import Flask, jsonify, render_template, request, Response, make_response

sys.path.insert(0, str(Path(__file__).parent))

import btc_fetcher
import backtest as bt
import livetest
import candle3_prob as c3_prob
import chainlink_candles as cl_candles

import pandas as pd

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True

# ── Paths ──────────────────────────────────────────────────────────────────
DATA_DIR   = Path(__file__).parent.parent / "data"
MARKETS_CSV = DATA_DIR / "btc_15m_candlesticks_merged.csv"
POLY_5M_RECORD_CSV = DATA_DIR / "poly_5m_live.csv"
ALGO_STATE_JSON = DATA_DIR / "algo_state.json"

_livetest_cache_lock = threading.Lock()
_livetest_cache: dict = {"key": None, "payload": None, "ts": 0.0}

# ── Market data (loaded once at startup) ──────────────────────────────────
_market_df: pd.DataFrame | None = None
_market_lock = threading.Lock()

DATA_START = "2025-12-15T00:00:00Z"
DATA_END   = "2026-05-09T00:00:00Z"


def _load_markets():
    global _market_df
    with _market_lock:
        if _market_df is None:
            df = pd.read_csv(MARKETS_CSV, low_memory=False)
            df = df[df["target_price"].notna()].copy()
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            _market_df = df
    return _market_df


# ── Background BTC fetch ───────────────────────────────────────────────────
_fetch_thread: threading.Thread | None = None


def _ensure_btc_data():
    """Start background BTC spot fetch if data is not ready."""
    global _fetch_thread
    prog = btc_fetcher.get_progress()
    if prog["status"] == "running":
        return
    if btc_fetcher.is_data_ready(DATA_START, DATA_END):
        btc_fetcher._set_progress(status="done", pct=100)
        return
    if prog["status"] == "done":
        return

    _fetch_thread = threading.Thread(
        target=btc_fetcher.fetch_and_cache,
        args=(DATA_START, DATA_END),
        daemon=True,
    )
    _fetch_thread.start()


# ── Polymarket live WebSockets (same feeds as poly_live_ticker.py) ──────────
POLY_LIVE_WINDOWS = 4
_poly_live_thread: threading.Thread | None = None
_poly_live_thread_lock = threading.Lock()


def ensure_poly_live_feed():
    """Single daemon thread running asyncio WS loops for dashboard snapshots."""
    global _poly_live_thread
    with _poly_live_thread_lock:
        if _poly_live_thread is not None and _poly_live_thread.is_alive():
            return

        def _runner():
            import asyncio

            try:
                import poly_live_ticker as poly
            except ImportError as exc:
                print(f"[dashboard] poly_live_ticker unavailable: {exc}")
                return
            asyncio.run(poly.run(POLY_LIVE_WINDOWS, dashboard_mode=True))

        _poly_live_thread = threading.Thread(
            target=_runner,
            daemon=True,
            name="poly-live-feed",
        )
        _poly_live_thread.start()


# ── Chainlink-only feed (1m candle chart) ───────────────────────────────────
_chainlink_thread: threading.Thread | None = None
_chainlink_thread_lock = threading.Lock()


def ensure_chainlink_feed():
    """Lightweight RTDS Chainlink WS — no CLOB books."""
    global _chainlink_thread
    with _chainlink_thread_lock:
        if _chainlink_thread is not None and _chainlink_thread.is_alive():
            return

        def _runner():
            import asyncio

            try:
                import poly_live_ticker as poly
            except ImportError as exc:
                print(f"[dashboard] chainlink feed unavailable: {exc}")
                return
            poly._recording_mode = True

            async def _main():
                await poly.chainlink_ws_loop()

            asyncio.run(_main())

        _chainlink_thread = threading.Thread(
            target=_runner,
            daemon=True,
            name="chainlink-feed",
        )
        _chainlink_thread.start()


# ── Routes ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    _load_markets()
    _ensure_btc_data()
    html = render_template("index.html")
    resp = make_response(html)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.route("/live")
def live_page():
    """Dedicated live-only page (avoids homepage caching hiding the feed)."""
    ensure_poly_live_feed()
    resp = make_response(render_template("live.html"))
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.route("/api/status")
def api_status():
    prog = btc_fetcher.get_progress()
    return jsonify({
        "btc_fetch":     prog,
        "data_loaded":   _market_df is not None,
        "market_rows":   int(len(_market_df)) if _market_df is not None else 0,
    })


@app.route("/api/run", methods=["POST"])
def api_run():
    """Run the backtest and return results."""
    body = request.get_json(force=True, silent=True) or {}

    l1_pct       = float(body.get("l1_pct",       0.050))
    l1_limit     = float(body.get("l1_limit",     0.60))   # 60¢ entry limit
    l2_pct       = float(body.get("l2_pct",       0.100))
    l2_limit     = float(body.get("l2_limit",     0.80))   # 80¢ entry limit
    position_size = float(body.get("position_size", 100.0))
    start_date   = body.get("start_date") or None
    end_date     = body.get("end_date")   or None

    markets = _load_markets()
    if markets is None or markets.empty:
        return jsonify({"error": "Market data not loaded"}), 503

    prog = btc_fetcher.get_progress()
    if prog["status"] not in ("done", "idle") and not btc_fetcher.is_data_ready(
        start_date or DATA_START, end_date or DATA_END, min_coverage=0.5
    ):
        return jsonify({"error": "BTC price data still loading", "progress": prog}), 202

    btc = btc_fetcher.load_btc_df(
        start_date or DATA_START,
        end_date   or DATA_END,
    )
    if btc.empty:
        return jsonify({"error": "No BTC data available for this date range"}), 404

    trades, summary = bt.run_backtest(
        kalshi_df     = markets,
        btc_df        = btc,
        l1_pct        = l1_pct,
        l1_limit      = l1_limit,
        l2_pct        = l2_pct,
        l2_limit      = l2_limit,
        position_size = position_size,
        start_date    = start_date,
        end_date      = end_date,
    )

    trades_out = sorted(trades, key=lambda t: t["timestamp"], reverse=True)[:2000]
    return jsonify({"summary": summary, "trades": trades_out})


@app.route("/api/progress_stream")
def api_progress_stream():
    """SSE stream for BTC data download progress."""
    def generate():
        import time
        while True:
            prog = btc_fetcher.get_progress()
            yield f"data: {json.dumps(prog)}\n\n"
            if prog["status"] in ("done", "error", "idle"):
                break
            time.sleep(1)
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/candles")
def candles_page():
    """Chainlink BTC/USD 1m candles with live forming bar (TradingView-style)."""
    ensure_chainlink_feed()
    resp = make_response(render_template("candles.html"))
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.route("/api/chainlink_candles")
def api_chainlink_candles():
    ensure_chainlink_feed()
    try:
        minutes = int(request.args.get("minutes", "480"))
    except ValueError:
        minutes = 480
    minutes = max(30, min(minutes, 1440))
    try:
        csv_tail = int(request.args.get("csv_tail", "60000"))
    except ValueError:
        csv_tail = 60000
    csv_tail = max(0, min(csv_tail, 500_000))
    payload = cl_candles.build_snapshot(
        csv_path=POLY_5M_RECORD_CSV,
        csv_tail_rows=csv_tail,
        max_candles=minutes,
    )
    resp = jsonify(payload)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.route("/algo")
def algo_page():
    """Live 5m algo trader status (orders, signals, PTB vs candles)."""
    resp = make_response(render_template("algo.html"))
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.route("/api/algo")
def api_algo():
    if not ALGO_STATE_JSON.exists():
        return jsonify({
            "running": False,
            "dry_run": True,
            "live_trading": False,
            "strategy": {
                "market": "BTC Up/Down 5m only",
                "rule": "m1 & m2 Chainlink close > PTB → BUY UP @ 71¢",
                "limit_price": 0.71,
            },
            "signal": "TRADER_NOT_STARTED",
            "orders": [],
            "events": [],
            "error": "Start algo: cd algo && npm install && cp .env.example .env && npm start",
        })
    try:
        payload = json.loads(ALGO_STATE_JSON.read_text(encoding="utf-8"))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    resp = jsonify(payload)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.route("/livetest")
def livetest_page():
    """Paper execution + PnL from poly_5m_live.csv recorder (all rows)."""
    livetest.ensure_livetest_csv_refresh(POLY_5M_RECORD_CSV)
    resp = make_response(render_template("livetest.html"))
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.route("/api/livetest")
def api_livetest():
    """JSON payload for livetest dashboard — full CSV replay + live recorder append."""
    try:
        tail = int(request.args.get("tail", "25000"))
    except ValueError:
        tail = 25000
    tail = max(5000, min(tail, 200_000))
    payload = livetest.get_livetest_snapshot(POLY_5M_RECORD_CSV, tail_rows=tail)
    resp = jsonify(payload)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.route("/candle3")
def candle3_page():
    """3×1m candle close vs PTB → 5m settlement probability (full CSV)."""
    resp = make_response(render_template("candle3.html"))
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.route("/api/candle3_prob")
def api_candle3_prob():
    """Historical + live 3-candle signal probabilities from poly_5m_live.csv."""
    payload = c3_prob.get_snapshot(POLY_5M_RECORD_CSV)
    resp = jsonify(payload)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.route("/api/poly_live")
def api_poly_live():
    """Latest Polymarket BTC 5m + 15m order books + Chainlink BTC spot for dashboard."""
    ensure_poly_live_feed()
    raw_depth = request.args.get("depth", "5")
    try:
        depth = max(1, min(12, int(raw_depth)))
    except ValueError:
        depth = 5
    try:
        import poly_live_ticker as poly
        return jsonify(poly.live_snapshot(depth=depth))
    except Exception as exc:
        return jsonify({"error": str(exc), "markets": [], "btc_usd": None}), 503


@app.route("/api/date_range")
def api_date_range():
    mdf = _load_markets()
    if mdf is None or mdf.empty:
        return jsonify({"min": DATA_START[:10], "max": DATA_END[:10]})
    return jsonify({
        "min": mdf["timestamp"].min().strftime("%Y-%m-%d"),
        "max": mdf["timestamp"].max().strftime("%Y-%m-%d"),
    })


if __name__ == "__main__":
    livetest.ensure_livetest_csv_refresh(POLY_5M_RECORD_CSV)
    print("=" * 60)
    print("  Polymarket BTC Strategy Backtest Dashboard")
    print("  Backtest    http://localhost:5050/")
    print("  LIVE TAPE   http://localhost:5050/live")
    print("  LIVE TEST   http://localhost:5050/livetest")
    print("  3-CANDLE    http://localhost:5050/candle3")
    print("  CHAINLINK   http://localhost:5050/candles")
    print("  ALGO 5m     http://localhost:5050/algo")
    print("=" * 60)
    app.run(host="0.0.0.0", port=5050, debug=False, threaded=True)
