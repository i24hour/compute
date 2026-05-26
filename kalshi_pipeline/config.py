"""
Pipeline configuration — edit this section before running.

All tunable knobs live here so the rest of the code stays constant.
"""

import os
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Kalshi API base URLs
# Both point to the same production Trade API v2. Either may be used;
# the alternate URL is handy if the primary is unreachable.
# ---------------------------------------------------------------------------
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_API_BASE_ALT = "https://external-api.kalshi.com/trade-api/v2"

# ---------------------------------------------------------------------------
# BTC 15-minute series tickers
#
# Kalshi organises markets in a hierarchy:  Series → Events → Markets.
# All BTC 15-minute UP/DOWN markets share the same series_ticker.
#
# Verified via the Kalshi API (May 2026):
#   KXBTC15M  — "Bitcoin price up down" (frequency: fifteen_min)  ← primary
#   KXBTC     — "Bitcoin range"         (frequency: hourly)       ← optional
#
# A typical KXBTC15M market ticker looks like:
#   KXBTC15M-26MAY082345-45
#   └─ series ─┘└─ YY+MON+DD+HHMM ─┘└─ minute slot ─┘
#
# Each 15-minute window generates exactly ONE market (1 event : 1 market).
# The strike price (floor_strike) is set dynamically at market open based on
# the live BTC spot price. expiration_value stores the final settlement price.
# ---------------------------------------------------------------------------
BTC_SERIES_TICKERS: list[str] = [
    "KXBTC15M",   # primary BTC 15-minute series
    # "KXBTC",    # uncomment to also collect hourly BTC markets
]

# ---------------------------------------------------------------------------
# Date range for collection (UTC)
#
# Default window matches the research brief (2025-01-01 → 2026-05-08).
# KXBTC15M only has markets from ~2025-12-10 onward; earlier dates yield no
# rows for this series (harmless).
# ---------------------------------------------------------------------------
PIPELINE_START_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)
PIPELINE_END_DATE = datetime(2026, 5, 8, 23, 59, 59, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# Candlestick settings
#
# Kalshi supports period_interval values of 1 (minute), 60 (hour), 1440 (day).
# We default to 1-minute candles for maximum granularity.
#
# CANDLESTICK_FETCH_WINDOW_DAYS: each API call covers at most this many days.
# Splitting into smaller windows avoids hitting any server-side response size
# limits. 7 days × 1440 minutes/day = 10,080 candles per request — well within
# observed limits.
# ---------------------------------------------------------------------------
CANDLESTICK_PERIOD_INTERVAL = 1          # minutes per candle
CANDLESTICK_FETCH_WINDOW_DAYS = 7        # days per single API request

# ---------------------------------------------------------------------------
# Rate-limiting and concurrency
#
# Kalshi's unauthenticated endpoints allow roughly 10 req/s. We stay
# conservative: 5 concurrent requests + 150 ms inter-request delay ≈ 6 req/s.
# Increase MAX_CONCURRENT_REQUESTS (and decrease REQUEST_DELAY_SECONDS) if
# you have an API key with higher limits.
# ---------------------------------------------------------------------------
MAX_CONCURRENT_REQUESTS = 5
REQUEST_DELAY_SECONDS = 0.15
RATE_LIMIT_BACKOFF_SECONDS = 30.0   # pause after receiving HTTP 429

# ---------------------------------------------------------------------------
# Retry settings (exponential backoff with jitter)
# ---------------------------------------------------------------------------
MAX_RETRIES = 5
RETRY_BASE_DELAY = 1.0     # seconds for the first retry
RETRY_MAX_DELAY = 60.0     # cap on the computed delay
RETRY_JITTER = 0.3         # ±30 % randomisation of each delay

# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------
MARKETS_PAGE_SIZE = 1000   # max allowed per /markets page

# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------
OUTPUT_DIR = os.environ.get("KALSHI_OUTPUT_DIR", "./data")
MERGED_CSV_PATH = os.path.join(OUTPUT_DIR, "btc_15m_candlesticks_merged.csv")
PER_TICKER_DIR = os.path.join(OUTPUT_DIR, "per_ticker")
CHECKPOINT_DB_PATH = os.path.join(OUTPUT_DIR, "checkpoint.sqlite")
SQLITE_CACHE_PATH = os.path.join(OUTPUT_DIR, "candlesticks_cache.sqlite")

# ---------------------------------------------------------------------------
# Output CSV column schema (order matters)
# ---------------------------------------------------------------------------
CSV_COLUMNS: list[str] = [
    "market_ticker",
    "series_ticker",
    "expiration_time",
    "target_price",
    "timestamp",           # UTC ISO-8601 end of 1-minute candle
    "yes_bid_open",
    "yes_bid_high",
    "yes_bid_low",
    "yes_bid_close",
    "yes_ask_open",
    "yes_ask_high",
    "yes_ask_low",
    "yes_ask_close",
    "price_open",          # first trade price of the minute (null if no trades)
    "price_high",
    "price_low",
    "price_close",         # last trade price of the minute
    "volume",              # contracts traded during this candle
    "open_interest",       # cumulative open contracts at candle close
]

# ---------------------------------------------------------------------------
# Feature flags
# ---------------------------------------------------------------------------
ENABLE_PER_TICKER_CSV = True    # write one CSV file per market ticker
ENABLE_SQLITE_CACHE = True      # write rows into a local SQLite database
ENABLE_PARALLEL_FETCH = True    # use asyncio concurrency for fetching
