"""
Retry utilities — exponential backoff with jitter for async API calls.

Usage:
    result = await async_retry(client._get, "/some/path", params={"k": "v"})

The decorator handles:
  - HTTP 429 (rate limit): waits RATE_LIMIT_BACKOFF_SECONDS
  - HTTP 5xx (server errors): exponential backoff
  - aiohttp network errors: exponential backoff
"""

import asyncio
import logging
import random
from typing import Any, Callable, Tuple, Type

import aiohttp

from .config import (
    MAX_RETRIES,
    RATE_LIMIT_BACKOFF_SECONDS,
    RETRY_BASE_DELAY,
    RETRY_JITTER,
    RETRY_MAX_DELAY,
)

logger = logging.getLogger(__name__)

# HTTP status codes that warrant a retry (excludes 429 which is handled separately)
RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({500, 502, 503, 504})


class RateLimitError(Exception):
    """Raised when the Kalshi API returns HTTP 429 Too Many Requests."""


class HTTPError(Exception):
    """Raised for non-retryable HTTP errors (4xx except 404/429, etc.)."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}: {message}")


def _compute_backoff(attempt: int, base: float, cap: float, jitter: float) -> float:
    """Returns exponential backoff delay with additive random jitter."""
    delay = min(base * (2**attempt), cap)
    return delay + delay * jitter * random.random()


async def async_retry(
    coro_fn: Callable[..., Any],
    *args: Any,
    max_retries: int = MAX_RETRIES,
    base_delay: float = RETRY_BASE_DELAY,
    max_delay: float = RETRY_MAX_DELAY,
    jitter: float = RETRY_JITTER,
    retryable_network_exceptions: Tuple[Type[Exception], ...] = (
        aiohttp.ClientError,
        asyncio.TimeoutError,
        ConnectionResetError,
    ),
    **kwargs: Any,
) -> Any:
    """
    Calls ``coro_fn(*args, **kwargs)`` up to ``max_retries + 1`` times.

    Retry triggers:
    * ``RateLimitError``  → sleep ``RATE_LIMIT_BACKOFF_SECONDS`` then retry.
    * ``HTTPError`` with a retryable status code → exponential backoff.
    * Network-layer exceptions (aiohttp.ClientError, TimeoutError) → backoff.

    Raises the last exception if all attempts are exhausted.
    """
    last_exc: Exception = RuntimeError("No attempts made")

    for attempt in range(max_retries + 1):
        try:
            return await coro_fn(*args, **kwargs)

        except RateLimitError as exc:
            if attempt == max_retries:
                raise
            wait = RATE_LIMIT_BACKOFF_SECONDS
            logger.warning(
                "Rate-limited (429). Waiting %.0fs before retry %d/%d.",
                wait,
                attempt + 1,
                max_retries,
            )
            await asyncio.sleep(wait)
            last_exc = exc

        except HTTPError as exc:
            if exc.status_code not in RETRYABLE_STATUS_CODES or attempt == max_retries:
                raise
            delay = _compute_backoff(attempt, base_delay, max_delay, jitter)
            logger.warning(
                "HTTP %d error. Retrying in %.1fs (attempt %d/%d).",
                exc.status_code,
                delay,
                attempt + 1,
                max_retries,
            )
            await asyncio.sleep(delay)
            last_exc = exc

        except retryable_network_exceptions as exc:
            if attempt == max_retries:
                raise
            delay = _compute_backoff(attempt, base_delay, max_delay, jitter)
            logger.warning(
                "Network error: %s. Retrying in %.1fs (attempt %d/%d).",
                exc,
                delay,
                attempt + 1,
                max_retries,
            )
            await asyncio.sleep(delay)
            last_exc = exc

    raise last_exc
