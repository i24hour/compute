#!/usr/bin/env python3
"""
Kalshi BTC 15-minute Candlestick Pipeline — main entry point.

Run:
    python pipeline.py
    python pipeline.py --start 2025-01-01 --end 2026-05-08
    python pipeline.py --series KXBTC-15M --workers 8
    python pipeline.py --dry-run        # discover markets only, no fetch
    python pipeline.py --log-level DEBUG

The pipeline runs in four phases:
  1. Discover   — find all BTC 15-minute market tickers (live + historical)
  2. Checkpoint — skip tickers already completed in a previous run
  3. Fetch      — download 1-minute candlesticks concurrently
  4. Finalise   — de-duplicate and sort the merged CSV
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import tqdm as tqdm_module

from kalshi_pipeline.api_client import KalshiAPIClient
from kalshi_pipeline.candlestick_fetcher import fetch_market_candlesticks
from kalshi_pipeline.config import (
    BTC_SERIES_TICKERS,
    ENABLE_PARALLEL_FETCH,
    ENABLE_PER_TICKER_CSV,
    ENABLE_SQLITE_CACHE,
    MAX_CONCURRENT_REQUESTS,
    PIPELINE_END_DATE,
    PIPELINE_START_DATE,
)
from kalshi_pipeline.exporter import (
    append_to_merged_csv,
    checkpoint_summary,
    finalize_merged_csv,
    insert_rows_postgres,
    insert_rows_sqlite,
    load_completed_tickers,
    mark_ticker_complete,
    save_ticker_csv,
)
from kalshi_pipeline.market_discovery import discover_btc_markets

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------


def _setup_logging(level: str) -> None:
    log_level = getattr(logging, level.upper(), logging.INFO)
    fmt = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s"
    logging.basicConfig(
        level=log_level,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("pipeline.log", encoding="utf-8"),
        ],
    )
    # Quieten noisy libraries
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


logger = logging.getLogger("kalshi_pipeline.main")

# ---------------------------------------------------------------------------
# Per-market worker
# ---------------------------------------------------------------------------


async def _process_market(
    client: KalshiAPIClient,
    market: Dict[str, Any],
    global_start: datetime,
    global_end: datetime,
    enable_per_ticker: bool,
    enable_sqlite: bool,
    enable_postgres: bool,
    semaphore: asyncio.Semaphore,
) -> int:
    """
    Fetches candles for one market and persists the results.
    Returns the number of rows collected.
    The semaphore limits total concurrent in-flight fetches.
    """
    ticker = market["ticker"]
    async with semaphore:
        rows = await fetch_market_candlesticks(
            client=client,
            market=market,
            global_start=global_start,
            global_end=global_end,
        )

    if rows:
        append_to_merged_csv(rows)

        if enable_per_ticker:
            save_ticker_csv(ticker, rows)

        if enable_sqlite:
            insert_rows_sqlite(rows)

        if enable_postgres:
            insert_rows_postgres(rows)

    mark_ticker_complete(ticker, len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------


async def run_pipeline(
    start: datetime = PIPELINE_START_DATE,
    end: datetime = PIPELINE_END_DATE,
    series_tickers: Optional[List[str]] = None,
    max_workers: int = MAX_CONCURRENT_REQUESTS,
    enable_per_ticker: bool = ENABLE_PER_TICKER_CSV,
    enable_sqlite: bool = ENABLE_SQLITE_CACHE,
    enable_postgres: bool = False,
    dry_run: bool = False,
) -> None:
    """
    Runs the full BTC 15-minute candlestick collection pipeline.

    Parameters
    ----------
    start, end:
        UTC datetime bounds for the collection window.
    series_tickers:
        Override BTC_SERIES_TICKERS from config.
    max_workers:
        Number of concurrent market-fetch coroutines.
    enable_per_ticker:
        If True, save one CSV per market ticker.
    enable_sqlite:
        If True, write rows to local SQLite cache.
    enable_postgres:
        If True, insert rows into PostgreSQL (requires KALSHI_PG_DSN env var).
    dry_run:
        If True, discover markets and print a summary without fetching data.
    """
    _banner(start, end, series_tickers or BTC_SERIES_TICKERS, max_workers, dry_run)

    async with KalshiAPIClient() as client:

        # ---------------------------------------------------------------
        # Phase 1 — Market discovery
        # ---------------------------------------------------------------
        logger.info("Phase 1/4 — Discovering BTC 15-minute markets…")
        markets, cutoff_ts = await discover_btc_markets(
            client=client,
            start=start,
            end=end,
            series_tickers=series_tickers,
        )

        if not markets:
            logger.warning(
                "No markets found.  Check BTC_SERIES_TICKERS in config.py "
                "and verify the series ticker against the Kalshi API."
            )
            return

        logger.info(
            "Phase 1/4 complete: %d total markets discovered.", len(markets)
        )

        if dry_run:
            _print_market_summary(markets, cutoff_ts)
            return

        # ---------------------------------------------------------------
        # Phase 2 — Checkpoint (skip already-completed tickers)
        # ---------------------------------------------------------------
        logger.info("Phase 2/4 — Loading checkpoint…")
        completed = load_completed_tickers()
        pending = [m for m in markets if m["ticker"] not in completed]
        stats = checkpoint_summary()
        logger.info(
            "Phase 2/4 complete: %d already done, %d pending.  "
            "(Prior total: %d rows across %d tickers.)",
            len(completed),
            len(pending),
            stats["total_rows_fetched"],
            stats["completed_tickers"],
        )

        if not pending:
            logger.info("Nothing to do — all markets already fetched.")
            finalize_merged_csv()
            return

        # ---------------------------------------------------------------
        # Phase 3 — Concurrent candlestick fetch
        # ---------------------------------------------------------------
        logger.info(
            "Phase 3/4 — Fetching candlesticks for %d markets (workers=%d)…",
            len(pending),
            max_workers,
        )
        semaphore = asyncio.Semaphore(max_workers)
        total_rows = 0
        errors: List[str] = []

        tasks = [
            _process_market(
                client=client,
                market=m,
                global_start=start,
                global_end=end,
                enable_per_ticker=enable_per_ticker,
                enable_sqlite=enable_sqlite,
                enable_postgres=enable_postgres,
                semaphore=semaphore,
            )
            for m in pending
        ]

        with tqdm_module.tqdm(
            total=len(tasks),
            desc="Markets",
            unit="market",
            dynamic_ncols=True,
            smoothing=0.05,
        ) as pbar:
            for coro in asyncio.as_completed(tasks):
                try:
                    n = await coro
                    total_rows += n
                    pbar.set_postfix(rows=f"{total_rows:,}", refresh=False)
                except Exception as exc:
                    logger.error("Market error: %s", exc)
                    errors.append(str(exc))
                finally:
                    pbar.update(1)

        logger.info(
            "Phase 3/4 complete: %d rows collected.  %d errors.",
            total_rows,
            len(errors),
        )
        if errors:
            for e in errors[:10]:
                logger.warning("  error: %s", e)

        # ---------------------------------------------------------------
        # Phase 4 — Finalise merged CSV
        # ---------------------------------------------------------------
        logger.info("Phase 4/4 — Finalising merged CSV…")
        out = finalize_merged_csv()
        if out:
            logger.info("Phase 4/4 complete: %s", out)
        logger.info("Pipeline finished successfully.")


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------


def _banner(
    start: datetime,
    end: datetime,
    series: List[str],
    workers: int,
    dry_run: bool,
) -> None:
    sep = "─" * 60
    logger.info(sep)
    logger.info("  Kalshi BTC 15-Minute Candlestick Pipeline")
    logger.info("  Date range : %s → %s", start.date(), end.date())
    logger.info("  Series     : %s", ", ".join(series))
    logger.info("  Workers    : %d", workers)
    if dry_run:
        logger.info("  Mode       : DRY RUN (discovery only)")
    logger.info(sep)


def _print_market_summary(markets: List[Dict[str, Any]], cutoff_ts: int) -> None:
    from datetime import datetime as DT

    hist = [m for m in markets if m["is_historical"]]
    live = [m for m in markets if not m["is_historical"]]
    print(f"\n{'='*60}")
    print(f"  DRY RUN — Market Discovery Summary")
    print(f"  Cutoff timestamp : {cutoff_ts}  "
          f"({DT.fromtimestamp(cutoff_ts, tz=timezone.utc).isoformat() if cutoff_ts else 'unknown'})")
    print(f"  Total markets    : {len(markets)}")
    print(f"  Historical       : {len(hist)}")
    print(f"  Live             : {len(live)}")
    print(f"{'='*60}")
    print("\nFirst 10 market tickers:")
    for m in markets[:10]:
        flag = "[H]" if m["is_historical"] else "[L]"
        print(f"  {flag} {m['ticker']}")
    if len(markets) > 10:
        print(f"  … and {len(markets) - 10} more.")


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="pipeline.py",
        description="Download Kalshi BTC 15-minute candlestick data to CSV.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--start",
        default="2025-01-01",
        metavar="YYYY-MM-DD",
        help="Collection start date (UTC).",
    )
    p.add_argument(
        "--end",
        default="2026-05-08",
        metavar="YYYY-MM-DD",
        help="Collection end date (UTC, inclusive).",
    )
    p.add_argument(
        "--series",
        nargs="+",
        default=None,
        metavar="TICKER",
        help="One or more Kalshi series tickers. Default: config.BTC_SERIES_TICKERS.",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=MAX_CONCURRENT_REQUESTS,
        metavar="N",
        help="Max concurrent market-fetch coroutines.",
    )
    p.add_argument(
        "--no-per-ticker",
        action="store_true",
        help="Disable writing per-ticker CSV files.",
    )
    p.add_argument(
        "--no-sqlite",
        action="store_true",
        help="Disable the local SQLite candlestick cache.",
    )
    p.add_argument(
        "--postgres",
        action="store_true",
        help="Insert rows into PostgreSQL (requires KALSHI_PG_DSN env var).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover markets only; do not fetch any candlestick data.",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    args = _parse_args()
    _setup_logging(args.log_level)

    start_dt = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end_dt = (
        datetime.fromisoformat(args.end)
        .replace(hour=23, minute=59, second=59)
        .replace(tzinfo=timezone.utc)
    )

    asyncio.run(
        run_pipeline(
            start=start_dt,
            end=end_dt,
            series_tickers=args.series,
            max_workers=args.workers,
            enable_per_ticker=not args.no_per_ticker,
            enable_sqlite=not args.no_sqlite,
            enable_postgres=args.postgres,
            dry_run=args.dry_run,
        )
    )
