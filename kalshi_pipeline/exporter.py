"""
CSV export, SQLite caching, and checkpointing.

Checkpointing
-------------
Each completed ticker is recorded in a lightweight SQLite table
(checkpoint.sqlite).  On restart the pipeline skips tickers that appear in
that table, enabling seamless resume after interruption.

SQLite cache
------------
When ENABLE_SQLITE_CACHE is True every row is also written to a local
SQLite database (candlesticks_cache.sqlite) that can be queried directly
with pandas, DuckDB, or any SQL client — useful for quick backtesting without
loading the full multi-GB merged CSV.

PostgreSQL / TimescaleDB (optional)
------------------------------------
Set the KALSHI_PG_DSN environment variable to a libpq connection string
(e.g. "postgresql://user:pass@localhost:5432/kalshi") to enable insertion
into a PostgreSQL table.  The table is created automatically.  TimescaleDB
hypertables on the ``timestamp`` column are supported if the extension is
already installed.
"""

import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .config import (
    CHECKPOINT_DB_PATH,
    CSV_COLUMNS,
    ENABLE_PER_TICKER_CSV,
    ENABLE_SQLITE_CACHE,
    MERGED_CSV_PATH,
    OUTPUT_DIR,
    PER_TICKER_DIR,
    SQLITE_CACHE_PATH,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Directory bootstrap
# ---------------------------------------------------------------------------


def _ensure_dirs() -> None:
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    Path(PER_TICKER_DIR).mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Checkpoint database
# ---------------------------------------------------------------------------

_CHECKPOINT_SCHEMA = """
CREATE TABLE IF NOT EXISTS completed_tickers (
    ticker         TEXT PRIMARY KEY,
    rows_fetched   INTEGER NOT NULL DEFAULT 0,
    completed_at   TEXT NOT NULL
);
"""


def _checkpoint_conn() -> sqlite3.Connection:
    _ensure_dirs()
    conn = sqlite3.connect(CHECKPOINT_DB_PATH, check_same_thread=False)
    conn.execute(_CHECKPOINT_SCHEMA)
    conn.commit()
    return conn


def load_completed_tickers() -> set:
    """Returns the set of ticker strings already processed in prior runs."""
    conn = _checkpoint_conn()
    rows = conn.execute("SELECT ticker FROM completed_tickers").fetchall()
    conn.close()
    return {r[0] for r in rows}


def mark_ticker_complete(ticker: str, rows_fetched: int) -> None:
    """Persists a completed ticker so future runs skip it."""
    conn = _checkpoint_conn()
    conn.execute(
        "INSERT OR REPLACE INTO completed_tickers (ticker, rows_fetched, completed_at) "
        "VALUES (?, ?, ?)",
        (ticker, rows_fetched, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()


def checkpoint_summary() -> Dict[str, Any]:
    """Returns stats from the checkpoint DB for display."""
    conn = _checkpoint_conn()
    total = conn.execute("SELECT COUNT(*) FROM completed_tickers").fetchone()[0]
    total_rows = conn.execute(
        "SELECT COALESCE(SUM(rows_fetched), 0) FROM completed_tickers"
    ).fetchone()[0]
    conn.close()
    return {"completed_tickers": total, "total_rows_fetched": total_rows}


# ---------------------------------------------------------------------------
# SQLite candlestick cache
# ---------------------------------------------------------------------------

# Column type map — TEXT for string fields, REAL for numeric
_SQLITE_TEXT_COLS = frozenset(
    {"market_ticker", "series_ticker", "expiration_time", "timestamp"}
)

_CACHE_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS candlesticks (\n"
    + ",\n".join(
        f"    {c} TEXT" if c in _SQLITE_TEXT_COLS else f"    {c} REAL"
        for c in CSV_COLUMNS
    )
    + ",\n    PRIMARY KEY (market_ticker, timestamp)\n);"
)

_CACHE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_cs_ticker ON candlesticks (market_ticker);",
    "CREATE INDEX IF NOT EXISTS idx_cs_ts     ON candlesticks (timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_cs_series ON candlesticks (series_ticker);",
]


def _cache_conn() -> sqlite3.Connection:
    _ensure_dirs()
    conn = sqlite3.connect(SQLITE_CACHE_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(_CACHE_SCHEMA)
    for idx in _CACHE_INDEXES:
        conn.execute(idx)
    conn.commit()
    return conn


def insert_rows_sqlite(rows: List[Dict[str, Any]]) -> None:
    """
    Batch-inserts candlestick rows into the local SQLite cache.
    Duplicate (market_ticker, timestamp) pairs are silently ignored.
    """
    if not rows:
        return
    placeholders = ", ".join("?" * len(CSV_COLUMNS))
    sql = (
        f"INSERT OR IGNORE INTO candlesticks ({', '.join(CSV_COLUMNS)}) "
        f"VALUES ({placeholders})"
    )
    data = [[r.get(col) for col in CSV_COLUMNS] for r in rows]
    conn = _cache_conn()
    conn.executemany(sql, data)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Per-ticker CSV
# ---------------------------------------------------------------------------


def save_ticker_csv(ticker: str, rows: List[Dict[str, Any]]) -> None:
    """
    Writes rows for a single market ticker to ``data/per_ticker/{ticker}.csv``.
    Sorted by timestamp ascending.
    """
    if not rows:
        return
    _ensure_dirs()
    path = os.path.join(PER_TICKER_DIR, f"{ticker}.csv")
    df = pd.DataFrame(rows, columns=CSV_COLUMNS)
    df.sort_values("timestamp", inplace=True)
    df.to_csv(path, index=False)
    logger.debug("Per-ticker CSV: %d rows → %s", len(rows), path)


# ---------------------------------------------------------------------------
# Merged CSV (append-while-running + final dedup/sort)
# ---------------------------------------------------------------------------


def append_to_merged_csv(rows: List[Dict[str, Any]]) -> None:
    """
    Appends rows to the merged CSV file.
    Creates the file with a header if it does not yet exist.

    Called after each ticker completes so that partial progress is preserved
    even if the pipeline is interrupted mid-run.
    """
    if not rows:
        return
    _ensure_dirs()
    df = pd.DataFrame(rows, columns=CSV_COLUMNS)
    write_header = not os.path.exists(MERGED_CSV_PATH)
    df.to_csv(MERGED_CSV_PATH, mode="a", header=write_header, index=False)


def finalize_merged_csv() -> Optional[str]:
    """
    De-duplicates and sorts the merged CSV by (market_ticker, timestamp).

    Should be called once at the end of a complete pipeline run.
    Returns the path to the finalised file, or None if no file exists.
    """
    if not os.path.exists(MERGED_CSV_PATH):
        logger.warning("No merged CSV found at %s — nothing to finalise.", MERGED_CSV_PATH)
        return None

    logger.info("Finalising merged CSV: de-dup + sort…")
    df = pd.read_csv(MERGED_CSV_PATH)
    before = len(df)
    df.drop_duplicates(subset=["market_ticker", "timestamp"], inplace=True)
    # Sort purely by candle timestamp (ISO string sorts correctly as UTC datetime).
    # Do NOT sort by market_ticker alphabetically — month abbreviations like APR/FEB/JAN
    # sort incorrectly alphabetically (APR < FEB < JAN instead of JAN < FEB < APR).
    df.sort_values("timestamp", inplace=True)
    df.to_csv(MERGED_CSV_PATH, index=False)
    logger.info(
        "Merged CSV finalised: %d → %d rows  (%s)",
        before,
        len(df),
        MERGED_CSV_PATH,
    )
    return MERGED_CSV_PATH


# ---------------------------------------------------------------------------
# Optional PostgreSQL / TimescaleDB insertion
# ---------------------------------------------------------------------------

_PG_DSN: Optional[str] = os.environ.get("KALSHI_PG_DSN")

_PG_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS btc_15m_candlesticks (
    market_ticker   TEXT        NOT NULL,
    series_ticker   TEXT,
    expiration_time TIMESTAMPTZ,
    target_price    DOUBLE PRECISION,
    timestamp       TIMESTAMPTZ NOT NULL,
    yes_bid_open    DOUBLE PRECISION,
    yes_bid_high    DOUBLE PRECISION,
    yes_bid_low     DOUBLE PRECISION,
    yes_bid_close   DOUBLE PRECISION,
    yes_ask_open    DOUBLE PRECISION,
    yes_ask_high    DOUBLE PRECISION,
    yes_ask_low     DOUBLE PRECISION,
    yes_ask_close   DOUBLE PRECISION,
    price_open      DOUBLE PRECISION,
    price_high      DOUBLE PRECISION,
    price_low       DOUBLE PRECISION,
    price_close     DOUBLE PRECISION,
    volume          DOUBLE PRECISION,
    open_interest   DOUBLE PRECISION,
    PRIMARY KEY (market_ticker, timestamp)
);
"""

_PG_TIMESCALE = """
SELECT create_hypertable(
    'btc_15m_candlesticks', 'timestamp',
    if_not_exists => TRUE
);
"""


def insert_rows_postgres(rows: List[Dict[str, Any]]) -> None:
    """
    Inserts rows into a PostgreSQL table (btc_15m_candlesticks).

    Requires the ``psycopg2`` package and KALSHI_PG_DSN environment variable.
    TimescaleDB is used automatically when the extension is installed.

    This function is a no-op if KALSHI_PG_DSN is not set.
    """
    if not _PG_DSN or not rows:
        return

    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        logger.warning(
            "psycopg2 not installed — skipping PostgreSQL insertion. "
            "Run: pip install psycopg2-binary"
        )
        return

    conn = psycopg2.connect(_PG_DSN)
    cur = conn.cursor()

    # Create table and attempt TimescaleDB hypertable (silent if unavailable)
    cur.execute(_PG_CREATE_TABLE)
    try:
        cur.execute(_PG_TIMESCALE)
    except Exception:
        conn.rollback()
        cur = conn.cursor()

    cols = ", ".join(CSV_COLUMNS)
    placeholders = ", ".join(f"%({c})s" for c in CSV_COLUMNS)
    sql = (
        f"INSERT INTO btc_15m_candlesticks ({cols}) VALUES ({placeholders}) "
        "ON CONFLICT (market_ticker, timestamp) DO NOTHING"
    )
    psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
    conn.commit()
    cur.close()
    conn.close()
    logger.debug("PostgreSQL: inserted %d rows.", len(rows))
