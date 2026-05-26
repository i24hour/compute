"""
Async HTTP client for the Kalshi Trade API v2.

All endpoints used here are public (no authentication required).

Live vs Historical routing
--------------------------
Kalshi partitions exchange data at a rolling cutoff (~3 months ago):

  • Markets settled AFTER the cutoff → still in the "live" dataset.
    Candlesticks: GET /series/{series_ticker}/markets/{ticker}/candlesticks
    Field names use the ``_dollars`` suffix: open_dollars, high_dollars, …

  • Markets settled BEFORE the cutoff → moved to "historical" dataset.
    Candlesticks: GET /historical/markets/{ticker}/candlesticks
    Field names have NO suffix: open, high, low, close (plain strings).

The cutoff timestamp is available at GET /historical/cutoff and advances
forward over time (target window is 3 months of live data).

This client exposes both endpoints transparently; the market_discovery and
candlestick_fetcher modules decide which one to call.
"""

import asyncio
import logging
from typing import Any, Dict, Optional

import aiohttp

from .config import (
    KALSHI_API_BASE,
    MAX_CONCURRENT_REQUESTS,
    REQUEST_DELAY_SECONDS,
)
from .retry_utils import HTTPError, RateLimitError, async_retry

logger = logging.getLogger(__name__)


class KalshiAPIClient:
    """
    Async Kalshi API client with connection pooling, rate-limit enforcement,
    and automatic retry via ``async_retry``.

    Always use as an async context manager::

        async with KalshiAPIClient() as client:
            data = await client.get_historical_cutoff()
    """

    def __init__(
        self,
        base_url: str = KALSHI_API_BASE,
        max_concurrent: int = MAX_CONCURRENT_REQUESTS,
        request_delay: float = REQUEST_DELAY_SECONDS,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._request_delay = request_delay
        self._session: Optional[aiohttp.ClientSession] = None

    # ------------------------------------------------------------------
    # Context-manager lifecycle
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "KalshiAPIClient":
        connector = aiohttp.TCPConnector(
            limit=MAX_CONCURRENT_REQUESTS * 2,
            limit_per_host=MAX_CONCURRENT_REQUESTS,
        )
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                "Accept": "application/json",
                "User-Agent": "kalshi-btc-pipeline/1.0 (+github.com/your-repo)",
            },
        )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Core request
    # ------------------------------------------------------------------

    async def _get(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Performs a single GET request.  Raises ``RateLimitError`` on 429,
        returns an empty dict on 404, and raises ``HTTPError`` for other
        non-200 responses.
        """
        assert self._session is not None, (
            "KalshiAPIClient must be used as an async context manager."
        )
        url = f"{self.base_url}{path}"

        async with self._semaphore:
            # Throttle all outgoing requests to stay within rate limits
            await asyncio.sleep(self._request_delay)

            async with self._session.get(url, params=params) as resp:
                if resp.status == 429:
                    raise RateLimitError("HTTP 429 — rate limit exceeded")

                if resp.status == 404:
                    # Not found is expected for some archived/missing tickers
                    logger.debug("404 for %s — skipping.", url)
                    return {}

                if resp.status != 200:
                    body = await resp.text()
                    raise HTTPError(resp.status, body[:300])

                return await resp.json()

    async def get(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Public GET with automatic retry on transient errors."""
        return await async_retry(self._get, path, params)

    # ------------------------------------------------------------------
    # Historical cutoff
    # ------------------------------------------------------------------

    async def get_historical_cutoff(self) -> Dict[str, Any]:
        """
        GET /historical/cutoff

        Returns the current partition timestamps.  Key field:
          cutoff.market_settled_ts — ISO-8601 timestamp; markets settled
          before this value are only accessible via /historical/markets.
        """
        return await self.get("/historical/cutoff")

    # ------------------------------------------------------------------
    # Market discovery
    # ------------------------------------------------------------------

    async def get_markets(self, **params: Any) -> Dict[str, Any]:
        """
        GET /markets

        Returns live/open/recently-settled markets.  Supports filters:
          series_ticker, status, limit, cursor
        """
        return await self.get("/markets", params=params)

    async def get_historical_markets(self, **params: Any) -> Dict[str, Any]:
        """
        GET /historical/markets

        Returns markets archived to the historical dataset.  Supports:
          series_ticker, event_ticker, tickers, limit, cursor
        """
        return await self.get("/historical/markets", params=params)

    # ------------------------------------------------------------------
    # Candlestick endpoints
    # ------------------------------------------------------------------

    async def get_candlesticks_live(
        self,
        series_ticker: str,
        market_ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int = 1,
    ) -> Dict[str, Any]:
        """
        GET /series/{series_ticker}/markets/{ticker}/candlesticks

        For markets still within the live data window.

        Response field names use the ``_dollars`` suffix:
          yes_bid.open_dollars, yes_bid.high_dollars, …
          volume_fp, open_interest_fp
        """
        path = f"/series/{series_ticker}/markets/{market_ticker}/candlesticks"
        return await self.get(
            path,
            params={
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": period_interval,
            },
        )

    async def get_candlesticks_historical(
        self,
        market_ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int = 1,
    ) -> Dict[str, Any]:
        """
        GET /historical/markets/{ticker}/candlesticks

        For markets that have been archived (settled before the cutoff).

        Response field names have NO suffix:
          yes_bid.open, yes_bid.high, …
          volume, open_interest
        """
        path = f"/historical/markets/{market_ticker}/candlesticks"
        return await self.get(
            path,
            params={
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": period_interval,
            },
        )
