"""
Candlestick fetcher — retrieves and normalises 1-minute OHLCV data.

Live vs historical field-name differences
------------------------------------------
The two Kalshi candlestick endpoints return structurally identical data but
use different JSON field names:

LIVE endpoint  (/series/{series}/markets/{ticker}/candlesticks)
  yes_bid  → { open_dollars, high_dollars, low_dollars, close_dollars }
  yes_ask  → { open_dollars, high_dollars, low_dollars, close_dollars }
  price    → { open_dollars, high_dollars, low_dollars, close_dollars, … }
  volume_fp          (FixedPoint string, e.g. "12.00")
  open_interest_fp

HISTORICAL endpoint  (/historical/markets/{ticker}/candlesticks)
  yes_bid  → { open, high, low, close }        ← no _dollars suffix
  yes_ask  → { open, high, low, close }
  price    → { open, high, low, close, mean, previous }
  volume
  open_interest

This module normalises both into the unified CSV_COLUMNS row schema.

How candlestick intervals work
--------------------------------
period_interval = 1 means one candle covers exactly 1 minute.
end_period_ts is the INCLUSIVE end of that minute in Unix seconds.

For a 15-minute BTC market (e.g. 00:00–00:15 UTC), you will receive up to
15 one-minute candles: end_period_ts = 00:01, 00:02, … 00:15.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .api_client import KalshiAPIClient
from .config import (
    CANDLESTICK_FETCH_WINDOW_DAYS,
    CANDLESTICK_PERIOD_INTERVAL,
    PIPELINE_END_DATE,
    PIPELINE_START_DATE,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Low-level field parsers
# ---------------------------------------------------------------------------


def _fp(val: Optional[str]) -> Optional[float]:
    """Converts a Kalshi fixed-point string (dollar or count) to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _extract_target_price(market: Dict[str, Any]) -> Optional[float]:
    """
    Extracts the BTC USD price threshold (strike) from a market dict.

    For an ABOVE market the strike lives in floor_strike; for a BELOW market
    it lives in cap_strike.  We try floor_strike first, then cap_strike.
    """
    for key in ("floor_strike", "cap_strike"):
        val = market.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return None


def _get_expiration(market: Dict[str, Any]) -> Optional[str]:
    """
    Returns the time when this market **stops trading** (end of the 15m window).

    Kalshi exposes several timestamps; do **not** use ``latest_expiration_time``
    first — that field is the *latest possible* settlement bound (often days
    later) and does not match the 15-minute window. For backtests, prefer the
    trading close.
    """
    return (
        market.get("close_time")
        or market.get("expected_expiration_time")
        or market.get("expiration_time")
        or market.get("latest_expiration_time")
    )


# ---------------------------------------------------------------------------
# Row normalisers
# ---------------------------------------------------------------------------


def _normalise_live(candle: Dict[str, Any], market: Dict[str, Any]) -> Dict[str, Any]:
    """Normalises a candlestick from the LIVE endpoint."""
    yb = candle.get("yes_bid") or {}
    ya = candle.get("yes_ask") or {}
    pr = candle.get("price") or {}

    return {
        "market_ticker":  market["ticker"],
        "series_ticker":  market.get("series_ticker", ""),
        "expiration_time": _get_expiration(market),
        "target_price":   _extract_target_price(market),
        "timestamp": datetime.fromtimestamp(
            candle["end_period_ts"], tz=timezone.utc
        ).isoformat(),
        # YES bid OHLC
        "yes_bid_open":   _fp(yb.get("open_dollars")),
        "yes_bid_high":   _fp(yb.get("high_dollars")),
        "yes_bid_low":    _fp(yb.get("low_dollars")),
        "yes_bid_close":  _fp(yb.get("close_dollars")),
        # YES ask OHLC
        "yes_ask_open":   _fp(ya.get("open_dollars")),
        "yes_ask_high":   _fp(ya.get("high_dollars")),
        "yes_ask_low":    _fp(ya.get("low_dollars")),
        "yes_ask_close":  _fp(ya.get("close_dollars")),
        # Last-trade price OHLC (null when no trades occurred in the minute)
        "price_open":     _fp(pr.get("open_dollars")),
        "price_high":     _fp(pr.get("high_dollars")),
        "price_low":      _fp(pr.get("low_dollars")),
        "price_close":    _fp(pr.get("close_dollars")),
        # Volume & open interest
        "volume":         _fp(candle.get("volume_fp")),
        "open_interest":  _fp(candle.get("open_interest_fp")),
    }


def _normalise_historical(
    candle: Dict[str, Any], market: Dict[str, Any]
) -> Dict[str, Any]:
    """Normalises a candlestick from the HISTORICAL endpoint."""
    yb = candle.get("yes_bid") or {}
    ya = candle.get("yes_ask") or {}
    pr = candle.get("price") or {}

    return {
        "market_ticker":  market["ticker"],
        "series_ticker":  market.get("series_ticker", ""),
        "expiration_time": _get_expiration(market),
        "target_price":   _extract_target_price(market),
        "timestamp": datetime.fromtimestamp(
            candle["end_period_ts"], tz=timezone.utc
        ).isoformat(),
        # YES bid OHLC (no _dollars suffix in historical responses)
        "yes_bid_open":   _fp(yb.get("open")),
        "yes_bid_high":   _fp(yb.get("high")),
        "yes_bid_low":    _fp(yb.get("low")),
        "yes_bid_close":  _fp(yb.get("close")),
        # YES ask OHLC
        "yes_ask_open":   _fp(ya.get("open")),
        "yes_ask_high":   _fp(ya.get("high")),
        "yes_ask_low":    _fp(ya.get("low")),
        "yes_ask_close":  _fp(ya.get("close")),
        # Last-trade price OHLC
        "price_open":     _fp(pr.get("open")),
        "price_high":     _fp(pr.get("high")),
        "price_low":      _fp(pr.get("low")),
        "price_close":    _fp(pr.get("close")),
        # Volume & open interest
        "volume":         _fp(candle.get("volume")),
        "open_interest":  _fp(candle.get("open_interest")),
    }


# ---------------------------------------------------------------------------
# Time-window splitting
# ---------------------------------------------------------------------------


def _time_windows(
    market_open: datetime,
    market_close: datetime,
    global_start: datetime,
    global_end: datetime,
    window_days: int,
) -> List[Tuple[int, int]]:
    """
    Splits the effective fetch range into batches of ``window_days`` each.

    The effective range is the intersection of the market's own window
    [market_open, market_close] with the global [global_start, global_end].

    Returns a list of (start_unix, end_unix) integer pairs.
    """
    start = max(market_open, global_start)
    end = min(market_close, global_end)

    if start >= end:
        return []

    windows: List[Tuple[int, int]] = []
    current = start
    delta = timedelta(days=window_days)

    while current < end:
        window_end = min(current + delta, end)
        windows.append((int(current.timestamp()), int(window_end.timestamp())))
        current = window_end

    return windows


# ---------------------------------------------------------------------------
# Public fetcher
# ---------------------------------------------------------------------------


def _parse_dt(ts: Optional[str], fallback: datetime) -> datetime:
    """Parses an ISO timestamp, returning ``fallback`` on failure."""
    if not ts:
        return fallback
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return fallback


async def fetch_market_candlesticks(
    client: KalshiAPIClient,
    market: Dict[str, Any],
    global_start: datetime = PIPELINE_START_DATE,
    global_end: datetime = PIPELINE_END_DATE,
    period_interval: int = CANDLESTICK_PERIOD_INTERVAL,
    window_days: int = CANDLESTICK_FETCH_WINDOW_DAYS,
) -> List[Dict[str, Any]]:
    """
    Fetches all 1-minute candlesticks for a single market within the global
    date range, splitting the request into weekly batches.

    The market dict must contain the ``is_historical`` flag set by
    ``market_discovery.discover_btc_markets``.

    Returns a list of normalised row dicts matching the CSV_COLUMNS schema.
    """
    ticker = market["ticker"]
    series = market.get("series_ticker", "")
    is_historical: bool = market.get("is_historical", True)

    market_open = _parse_dt(
        market.get("open_time") or market.get("created_time"), global_start
    )
    market_close = _parse_dt(
        market.get("close_time") or market.get("latest_expiration_time"), global_end
    )

    windows = _time_windows(market_open, market_close, global_start, global_end, window_days)
    if not windows:
        logger.debug(
            "No fetch windows for %s (open=%s, close=%s).", ticker, market_open, market_close
        )
        return []

    rows: List[Dict[str, Any]] = []
    endpoint_label = "historical" if is_historical else "live"

    for start_ts, end_ts in windows:
        start_label = datetime.fromtimestamp(start_ts, tz=timezone.utc).date()
        end_label = datetime.fromtimestamp(end_ts, tz=timezone.utc).date()

        try:
            if is_historical:
                data = await client.get_candlesticks_historical(
                    market_ticker=ticker,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    period_interval=period_interval,
                )
                candles = data.get("candlesticks", [])
                batch = [_normalise_historical(c, market) for c in candles]
            else:
                if not series:
                    # series_ticker is required by the live endpoint; fall back
                    # to historical if we don't have it.
                    logger.debug(
                        "%s: no series_ticker, falling back to historical endpoint.",
                        ticker,
                    )
                    data = await client.get_candlesticks_historical(
                        market_ticker=ticker,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        period_interval=period_interval,
                    )
                    candles = data.get("candlesticks", [])
                    batch = [_normalise_historical(c, market) for c in candles]
                else:
                    data = await client.get_candlesticks_live(
                        series_ticker=series,
                        market_ticker=ticker,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        period_interval=period_interval,
                    )
                    candles = data.get("candlesticks", [])
                    batch = [_normalise_live(c, market) for c in candles]

            rows.extend(batch)
            logger.debug(
                "  %s [%s→%s] %s: %d candles",
                ticker, start_label, end_label, endpoint_label, len(batch),
            )

        except Exception as exc:
            logger.warning(
                "Failed to fetch candles for %s [%s→%s]: %s",
                ticker, start_label, end_label, exc,
            )

    return rows
