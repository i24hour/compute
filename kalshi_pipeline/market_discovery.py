"""
Market discovery for Kalshi BTC 15-minute UP/DOWN markets.

How Kalshi market tickers work
-------------------------------
Kalshi organises prediction markets in a three-level hierarchy:

  Series  →  Event  →  Market

Example BTC 15-minute market:

  Series  : KXBTC-15M
  Event   : KXBTC-15M-25JAN0100T          (BTC on Jan 1 2025, 00:00–00:15 UTC)
  Market  : KXBTC-15M-25JAN0100T-T97000   (will BTC be above $97,000 at 00:15?)

Each 15-minute window generates *one market per strike price* on the order
book, so a single event typically has many markets (different thresholds).
Over the Jan 2025 – May 2026 date range there can be tens of thousands of
individual market tickers.

Discovery strategy
------------------
1. Call GET /historical/cutoff to find the partition boundary.
2. Paginate GET /historical/markets?series_ticker=KXBTC-15M to collect
   all settled/archived markets within our date window.
3. Paginate GET /markets?series_ticker=KXBTC-15M to collect live markets
   (still within the 3-month live-data window).
4. Merge and deduplicate by ticker.
5. Tag each market with is_historical=True/False so the fetcher knows which
   candlestick endpoint to use.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .api_client import KalshiAPIClient
from .config import (
    BTC_SERIES_TICKERS,
    MARKETS_PAGE_SIZE,
    PIPELINE_END_DATE,
    PIPELINE_START_DATE,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    """Parses an ISO-8601 timestamp string to a timezone-aware UTC datetime."""
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _ts_to_unix(dt: Optional[datetime]) -> int:
    """Converts a datetime to a Unix timestamp integer, or 0 if None."""
    return int(dt.timestamp()) if dt else 0


def _market_overlaps_range(
    market: Dict[str, Any], start: datetime, end: datetime
) -> bool:
    """
    Returns True when the market's active window intersects [start, end].

    We check open_time and close_time/latest_expiration_time.  A market
    that opened before ``end`` and closed after ``start`` overlaps.
    """
    open_time = _parse_iso(market.get("open_time") or market.get("created_time"))
    close_time = _parse_iso(
        market.get("close_time") or market.get("latest_expiration_time")
    )

    if close_time and close_time < start:
        return False  # market closed before our window
    if open_time and open_time > end:
        return False  # market opened after our window
    return True


# ---------------------------------------------------------------------------
# Pagination helper
# ---------------------------------------------------------------------------


async def _paginate_markets(
    client: KalshiAPIClient,
    fetch_fn,  # either client.get_markets or client.get_historical_markets
    series_ticker: str,
    start: datetime,
    end: datetime,
    label: str,
) -> List[Dict[str, Any]]:
    """
    Pages through a markets endpoint for the given series ticker.
    Filters for markets whose windows overlap [start, end].
    Returns a flat list of market dicts.
    """
    results: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    page = 0
    total_seen = 0

    while True:
        params: Dict[str, Any] = {
            "series_ticker": series_ticker,
            "limit": MARKETS_PAGE_SIZE,
        }
        if cursor:
            params["cursor"] = cursor

        data = await fetch_fn(**params)
        batch: List[Dict[str, Any]] = data.get("markets", [])
        total_seen += len(batch)

        for market in batch:
            if _market_overlaps_range(market, start, end):
                market.setdefault("series_ticker", series_ticker)
                results.append(market)

        cursor = data.get("cursor") or ""
        page += 1

        logger.debug(
            "[%s / %s] Page %d: %d markets returned, %d match range (running total: %d)",
            series_ticker,
            label,
            page,
            len(batch),
            len(results),
            total_seen,
        )

        if not batch or not cursor:
            break

    return results


# ---------------------------------------------------------------------------
# Public discovery function
# ---------------------------------------------------------------------------


async def discover_btc_markets(
    client: KalshiAPIClient,
    start: datetime = PIPELINE_START_DATE,
    end: datetime = PIPELINE_END_DATE,
    series_tickers: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Discovers all BTC 15-minute markets whose windows overlap [start, end].

    Parameters
    ----------
    client:
        An open KalshiAPIClient instance.
    start, end:
        UTC datetime bounds for collection.
    series_tickers:
        Override the default BTC_SERIES_TICKERS list from config.

    Returns
    -------
    (markets, cutoff_ts) where:
      - markets: list of market dicts sorted by open_time, each with an
        added ``is_historical`` boolean flag.
      - cutoff_ts: Unix timestamp of the live/historical partition boundary.
        0 if the cutoff endpoint returned an unexpected structure.
    """
    if series_tickers is None:
        series_tickers = BTC_SERIES_TICKERS

    # ------------------------------------------------------------------
    # Step 1 — Fetch the live/historical partition boundary
    # ------------------------------------------------------------------
    cutoff_ts = 0
    try:
        raw = await client.get_historical_cutoff()
        # The API may return the cutoff either at the top level or nested
        # under a "cutoff" key depending on the API version.
        cutoff_block: Dict[str, Any] = raw.get("cutoff") or raw
        cutoff_str = (
            cutoff_block.get("market_settled_ts")
            or cutoff_block.get("markets_settled_ts")
            or ""
        )
        cutoff_dt = _parse_iso(cutoff_str)
        cutoff_ts = _ts_to_unix(cutoff_dt)
        logger.info(
            "Historical cutoff: %s  (unix=%d)", cutoff_str or "unknown", cutoff_ts
        )
    except Exception as exc:
        logger.warning(
            "Could not fetch historical cutoff: %s. "
            "All markets will be treated as historical.",
            exc,
        )

    # ------------------------------------------------------------------
    # Step 2 — Collect markets from all configured series
    # ------------------------------------------------------------------
    all_markets: Dict[str, Dict[str, Any]] = {}  # ticker → market (dedup)

    for series_ticker in series_tickers:
        # Historical markets (settled before cutoff, archived)
        logger.info(
            "Fetching HISTORICAL markets for series '%s'…", series_ticker
        )
        hist = await _paginate_markets(
            client,
            client.get_historical_markets,
            series_ticker,
            start,
            end,
            label="historical",
        )
        for m in hist:
            m["is_historical"] = True
            all_markets[m["ticker"]] = m
        logger.info(
            "  → %d historical markets in range for '%s'", len(hist), series_ticker
        )

        # Live markets (settled after cutoff or still open)
        logger.info(
            "Fetching LIVE markets for series '%s'…", series_ticker
        )
        live = await _paginate_markets(
            client,
            client.get_markets,
            series_ticker,
            start,
            end,
            label="live",
        )
        for m in live:
            ticker = m["ticker"]
            if ticker in all_markets:
                # Already found via the historical endpoint — trust that flag
                continue
            # Determine which candlestick endpoint to use for this market.
            # If the market settled before the cutoff it must use /historical.
            settled_str = m.get("settlement_ts") or m.get("close_time") or ""
            settled_dt = _parse_iso(settled_str)
            settled_unix = _ts_to_unix(settled_dt)
            m["is_historical"] = bool(
                cutoff_ts and settled_unix and settled_unix < cutoff_ts
            )
            all_markets[ticker] = m
        logger.info(
            "  → %d live markets in range for '%s'", len(live), series_ticker
        )

    # ------------------------------------------------------------------
    # Step 3 — Sort by open_time for deterministic processing order
    # ------------------------------------------------------------------
    def _sort_key(m: Dict[str, Any]) -> datetime:
        dt = _parse_iso(m.get("open_time") or m.get("created_time"))
        return dt or datetime.min.replace(tzinfo=timezone.utc)

    sorted_markets = sorted(all_markets.values(), key=_sort_key)

    hist_count = sum(1 for m in sorted_markets if m["is_historical"])
    live_count = len(sorted_markets) - hist_count
    logger.info(
        "Discovery complete: %d markets total  (%d historical, %d live).",
        len(sorted_markets),
        hist_count,
        live_count,
    )

    return sorted_markets, cutoff_ts
