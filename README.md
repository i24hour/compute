# Kalshi BTC 15-Minute Candlestick Pipeline

A production-quality Python pipeline that downloads **all Kalshi BTC 15-minute UP/DOWN market** 1-minute candlestick data (odds + trade prices + volume) and saves it to CSV for quantitative research and backtesting.

---

## Table of Contents

1. [Setup](#setup)
2. [How to Run](#how-to-run)
3. [How the Kalshi API Works](#how-the-kalshi-api-works)
4. [CSV Schema](#csv-schema)
5. [Architecture](#architecture)
6. [Performance Notes](#performance-notes)
7. [Data Limitations](#data-limitations)
8. [Optional: PostgreSQL / TimescaleDB](#optional-postgresql--timescaledb)

---

## Setup

### Prerequisites

- Python 3.11+
- No API key required — all endpoints used are public and unauthenticated.

### Install

```bash
cd /path/to/kalshi
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## How to Run

### Full collection (Dec 2025 → May 2026)

```bash
python pipeline.py
```

> **Note:** The `KXBTC15M` series launched on **December 10, 2025**.
> Running with `--start 2025-01-01` is valid but will yield zero results
> before that date. Use `--start 2025-12-10` for efficiency.

### Custom date range

```bash
python pipeline.py --start 2025-06-01 --end 2025-12-31
```

### Dry run — discover markets without downloading any data

```bash
python pipeline.py --dry-run
```

This prints how many markets were found, their tickers, and whether each is live or historical. Use this first to verify that the `series_ticker` in `config.py` is correct.

### Increase concurrency (faster, uses more API quota)

```bash
python pipeline.py --workers 10
```

### Custom series tickers

```bash
python pipeline.py --series KXBTC-15M KXBTC-15MIN
```

### Disable per-ticker CSV files (saves disk I/O)

```bash
python pipeline.py --no-per-ticker
```

### Enable PostgreSQL insertion

```bash
export KALSHI_PG_DSN="postgresql://user:pass@localhost:5432/kalshi"
python pipeline.py --postgres
```

### Resume after interruption

Just re-run `python pipeline.py`. The checkpoint database (`data/checkpoint.sqlite`) records every completed ticker. Already-finished tickers are skipped automatically.

### All CLI options

```
python pipeline.py --help

options:
  --start YYYY-MM-DD    Collection start date (UTC). Default: 2025-01-01
  --end   YYYY-MM-DD    Collection end date   (UTC). Default: 2026-05-08
  --series TICKER [...]  One or more Kalshi series tickers
  --workers N            Concurrent fetch coroutines. Default: 5
  --no-per-ticker        Disable per-ticker CSV files
  --no-sqlite            Disable local SQLite cache
  --postgres             Insert into PostgreSQL (requires KALSHI_PG_DSN)
  --dry-run              Discover markets only; no data fetch
  --log-level            DEBUG | INFO | WARNING | ERROR (default: INFO)
```

---

## How the Kalshi API Works

### Market hierarchy

```
Series  →  Event  →  Market
KXBTC-15M  KXBTC-15M-25JAN0100T  KXBTC-15M-25JAN0100T-T97000
```

- **Series** (`KXBTC-15M`): the product line — all BTC 15-minute UP/DOWN markets.
- **Event** (`KXBTC-15M-25JAN0100T`): one 15-minute window on BTC price.
- **Market** (`KXBTC-15M-25JAN0100T-T97000`): will BTC be **above** $97,000 at 00:15 UTC on Jan 1 2025?

A single 15-minute event typically spawns **many markets** — one per strike price on the order book. Over Jan 2025–May 2026 (~490 days × 96 windows/day) there may be **tens of thousands** of individual market tickers.

### Live vs Historical split

Kalshi partitions exchange data at a rolling **cutoff** (~3 months ago):

| Data age | Market status | Correct endpoint |
|---|---|---|
| < ~3 months (recent) | Settled but in live dataset | `GET /series/{series}/markets/{ticker}/candlesticks` |
| > ~3 months (archived) | Moved to historical dataset | `GET /historical/markets/{ticker}/candlesticks` |

Retrieve the current cutoff with:
```
GET https://api.elections.kalshi.com/trade-api/v2/historical/cutoff
```

The pipeline calls this automatically on startup and routes each market to the correct endpoint.

### Field-name differences between endpoints

| Field | Live endpoint | Historical endpoint |
|---|---|---|
| YES bid open | `yes_bid.open_dollars` | `yes_bid.open` |
| Volume | `volume_fp` | `volume` |
| Open interest | `open_interest_fp` | `open_interest` |

The pipeline normalises both into the same CSV schema transparently.

### Candlestick interval

`period_interval=1` returns one candle per **minute**. For a 15-minute market you receive up to 15 candles. `end_period_ts` is the inclusive Unix timestamp for the end of each minute.

### Pagination

Market listing endpoints use cursor-based pagination. The pipeline handles this automatically, fetching pages of 1,000 markets until the cursor is exhausted.

---

## CSV Schema

### Merged file: `data/btc_15m_candlesticks_merged.csv`

| Column | Type | Description |
|---|---|---|
| `market_ticker` | string | Unique Kalshi market identifier |
| `series_ticker` | string | Parent series (e.g. `KXBTC-15M`) |
| `expiration_time` | ISO-8601 | When the market settled (UTC) |
| `target_price` | float | BTC USD strike price for this market |
| `timestamp` | ISO-8601 | End of 1-minute candle (UTC) |
| `yes_bid_open` | float | YES bid price at candle open (dollars) |
| `yes_bid_high` | float | Highest YES bid during candle |
| `yes_bid_low` | float | Lowest YES bid during candle |
| `yes_bid_close` | float | YES bid price at candle close |
| `yes_ask_open` | float | YES ask price at candle open |
| `yes_ask_high` | float | Highest YES ask during candle |
| `yes_ask_low` | float | Lowest YES ask during candle |
| `yes_ask_close` | float | YES ask price at candle close |
| `price_open` | float | First trade price in the minute (null = no trades) |
| `price_high` | float | Highest trade price in the minute |
| `price_low` | float | Lowest trade price in the minute |
| `price_close` | float | Last trade price in the minute |
| `volume` | float | Contracts traded during the candle |
| `open_interest` | float | Cumulative open contracts at candle close |

### Notes on prices

All prices are expressed as **dollars** in [0.00, 1.00]. A `yes_bid_close` of `0.65` means the market was pricing a 65 % probability that BTC would close above the strike.

`price_*` fields reflect **actual trades**. If no trades occurred during a minute they are `null` / `NaN`. `yes_bid_*` and `yes_ask_*` always have values because they reflect the order book.

### Per-ticker files: `data/per_ticker/{MARKET_TICKER}.csv`

Same schema, one file per market, sorted by `timestamp`.

---

## Architecture

```
kalshi/
├── pipeline.py                   ← CLI entry point & orchestrator
├── requirements.txt
├── README.md
└── kalshi_pipeline/
    ├── __init__.py
    ├── config.py                 ← All tunable constants
    ├── retry_utils.py            ← Exponential backoff, RateLimitError, HTTPError
    ├── api_client.py             ← Async aiohttp client (live + historical routing)
    ├── market_discovery.py       ← Paginate /markets + /historical/markets
    ├── candlestick_fetcher.py    ← Fetch + normalise candles per market
    └── exporter.py               ← CSV append, SQLite cache, checkpoint, PostgreSQL
```

### Data flow

```
pipeline.py
  │
  ├─ discover_btc_markets()
  │     ├─ GET /historical/cutoff           (determine partition boundary)
  │     ├─ GET /historical/markets?series_ticker=KXBTC-15M  (all pages)
  │     └─ GET /markets?series_ticker=KXBTC-15M             (all pages)
  │
  ├─ load_completed_tickers()              (resume support)
  │
  ├─ asyncio.as_completed(tasks)           (parallel fetch, semaphore-limited)
  │     └─ fetch_market_candlesticks()
  │           ├─ split into weekly windows
  │           ├─ GET /historical/markets/{ticker}/candlesticks  OR
  │           └─ GET /series/{series}/markets/{ticker}/candlesticks
  │
  └─ finalize_merged_csv()                 (dedup + sort)
```

---

## Performance Notes

### Scale

The Jan 2025–May 2026 window spans ~490 days. With one 15-minute event per window and typically 5–20 markets per event:

- **Events**: ~490 × 96 = ~47,000
- **Markets**: likely 100,000–300,000+ depending on the number of strikes

At 1 req/market and 6 req/s effective throughput, expect **4–13 hours** of wall-clock time for a full run. Increase `--workers` to speed up (the public API is fairly generous).

### Checkpoint and resume

Every completed ticker is recorded immediately after its data is saved. A `Ctrl+C` mid-run loses at most the current in-flight batch (~5 markets). Re-running resumes from where it stopped.

### SQLite cache

`data/candlesticks_cache.sqlite` has indexes on `market_ticker` and `timestamp`. Query with:

```python
import pandas as pd, sqlite3
conn = sqlite3.connect("data/candlesticks_cache.sqlite")
df = pd.read_sql(
    "SELECT * FROM candlesticks WHERE series_ticker = 'KXBTC-15M' "
    "AND timestamp >= '2025-03-01'",
    conn
)
```

### Disk usage (estimates)

| Output | Estimated size (full range) |
|---|---|
| Merged CSV | 2–10 GB |
| Per-ticker CSVs | 2–10 GB |
| SQLite cache | 3–15 GB |

Use `--no-per-ticker` to halve disk writes. Use `--no-sqlite` if you only need the merged CSV.

---

## Data Limitations

### No real-time orderbook snapshots

Kalshi does **not** expose historical level-2 orderbook snapshots via their public API. The `yes_bid` and `yes_ask` OHLC fields capture the **best bid/ask** at each minute boundary — they represent the top of book, not the full depth.

For full depth-of-book reconstruction you would need:
1. The WebSocket live feed (requires authentication, must be recorded in real time).
2. Kalshi's institutional data product (not publicly documented).

### Candlestick granularity

The finest available granularity is **1 minute** (`period_interval=1`). Sub-minute tick data is not available through any public Kalshi endpoint.

### Historical cutoff

Markets settled more than ~3 months ago are served from the historical dataset. Kalshi guarantees the data exists but the cutoff may occasionally be refreshed, briefly making some markets unreachable from the live endpoint. The pipeline handles this by checking the cutoff at startup and falling back to the historical endpoint.

### Market availability — KXBTC15M launch date

**The KXBTC15M series launched on December 10, 2025.** No data exists before this date regardless of what `--start` value you pass. Confirmed via the Kalshi public API (May 2026):

| Period | Status |
|---|---|
| Jan 2025 – Dec 9, 2025 | No KXBTC15M markets (series not yet launched) |
| Dec 10, 2025 – Mar 9, 2026 | Historical dataset (~7,970 markets) |
| Mar 9, 2026 – present | Live dataset (~6,425 markets as of May 2026) |

Total available: **~14,400 markets** × 15 candles each ≈ **~216,000 rows** of 1-minute candlestick data.

If you need BTC price data before December 2025, consider the **hourly** `KXBTC` series (started earlier). Set `--series KXBTC` to collect hourly BTC price markets instead.

### Price precision

All prices are fixed-point strings in the API, converted to `float64` in the CSV. Kalshi supports up to 6 decimal places; for binary markets prices are typically 2–4 decimal places (cents).

---

## Optional: PostgreSQL / TimescaleDB

```bash
# 1. Install psycopg2
pip install psycopg2-binary

# 2. Uncomment in requirements.txt:
#    psycopg2-binary>=2.9.0

# 3. Set connection string
export KALSHI_PG_DSN="postgresql://user:pass@localhost:5432/kalshi"

# 4. Run with --postgres flag
python pipeline.py --postgres
```

The pipeline creates the `btc_15m_candlesticks` table automatically. If TimescaleDB is installed, it converts the table to a hypertable on the `timestamp` column for efficient time-range queries.
