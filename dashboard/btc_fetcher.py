"""
BTC/USDT Spot Price Fetcher (Binance)
Downloads 1-minute OHLCV data and caches it in a local SQLite DB.
Used by the Polymarket BTC strategy backtester — no API key required.
"""

import sqlite3
import time
import urllib.request
import json
import threading
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "btc_cache.db"
BINANCE_URL = "https://api.binance.com/api/v3/klines"
# Fallback mirror for restricted regions
BINANCE_MIRROR = "https://api1.binance.com/api/v3/klines"

_progress = {"status": "idle", "pct": 0, "fetched": 0, "total": 0, "error": None}
_lock = threading.Lock()


def get_progress():
    with _lock:
        return dict(_progress)


def _set_progress(**kwargs):
    with _lock:
        _progress.update(kwargs)


def _init_db(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS btc_1m (
            ts INTEGER PRIMARY KEY,
            open REAL, high REAL, low REAL, close REAL, volume REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ts ON btc_1m(ts)")
    conn.commit()


def _fetch_chunk(start_ms: int, end_ms: int, retries: int = 3) -> list:
    """Fetch up to 1000 1-min candles from Binance."""
    params = f"symbol=BTCUSDT&interval=1m&startTime={start_ms}&endTime={end_ms}&limit=1000"
    for url_base in (BINANCE_URL, BINANCE_MIRROR):
        for attempt in range(retries):
            try:
                req = urllib.request.Request(
                    f"{url_base}?{params}",
                    headers={"User-Agent": "kalshi-backtester/1.0"}
                )
                with urllib.request.urlopen(req, timeout=15) as r:
                    return json.loads(r.read())
            except Exception as e:
                if attempt == retries - 1:
                    last_err = e
                time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"Binance fetch failed: {last_err}")


def fetch_and_cache(start_iso: str, end_iso: str):
    """
    Download BTC 1-min OHLCV for [start_iso, end_iso] and save to SQLite.
    Skips already-cached ranges. Runs in caller's thread (call in background thread).
    """
    start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    total_minutes = (end_ms - start_ms) // 60000
    _set_progress(status="running", pct=0, fetched=0, total=total_minutes, error=None)

    conn = sqlite3.connect(DB_PATH)
    _init_db(conn)

    # Find gaps (timestamps not yet in DB)
    existing = {row[0] for row in conn.execute(
        "SELECT ts FROM btc_1m WHERE ts >= ? AND ts <= ?", (start_ms, end_ms)
    )}

    current_ms = start_ms
    chunk_ms = 1000 * 60000  # 1000 minutes per chunk

    rows_fetched = len(existing)

    while current_ms < end_ms:
        chunk_end = min(current_ms + chunk_ms - 60000, end_ms)

        # Skip if entire chunk already cached
        expected = set(range(current_ms, chunk_end + 60000, 60000))
        missing = expected - existing
        if not missing:
            current_ms += chunk_ms
            rows_fetched += len(expected)
            pct = min(99, int(rows_fetched / max(total_minutes, 1) * 100))
            _set_progress(pct=pct, fetched=rows_fetched)
            continue

        try:
            candles = _fetch_chunk(current_ms, chunk_end)
        except Exception as e:
            _set_progress(status="error", error=str(e))
            conn.close()
            return

        if candles:
            conn.executemany(
                "INSERT OR IGNORE INTO btc_1m VALUES (?,?,?,?,?,?)",
                [(int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5]))
                 for c in candles]
            )
            conn.commit()
            rows_fetched += len(candles)

        pct = min(99, int(rows_fetched / max(total_minutes, 1) * 100))
        _set_progress(pct=pct, fetched=rows_fetched)

        current_ms += chunk_ms
        time.sleep(0.25)  # polite rate limit

    conn.close()
    _set_progress(status="done", pct=100)


def load_btc_df(start_iso: str, end_iso: str):
    """
    Load cached BTC data as a list of dicts: {ts_ms, open, high, low, close, volume}
    """
    import pandas as pd

    start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    if not DB_PATH.exists():
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT ts, open, high, low, close, volume FROM btc_1m WHERE ts >= ? AND ts <= ? ORDER BY ts",
        conn,
        params=(start_ms, end_ms),
    )
    conn.close()

    if df.empty:
        return df

    df["timestamp"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return df


def is_data_ready(start_iso: str, end_iso: str, min_coverage: float = 0.9) -> bool:
    """True if we have >= min_coverage of expected rows for the date range."""
    if not DB_PATH.exists():
        return False

    start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    conn = sqlite3.connect(DB_PATH)
    count = conn.execute(
        "SELECT COUNT(*) FROM btc_1m WHERE ts >= ? AND ts <= ?",
        (start_ms, end_ms)
    ).fetchone()[0]
    conn.close()

    expected = (end_ms - start_ms) // 60000
    return count / max(expected, 1) >= min_coverage
