"""
Polymarket BTC Up/Down Strategy Backtester
===========================================
TWO-STAGE LOGIC:
  Stage 1 — BTC trigger:
    • When BTC spot touches +L1% above price-to-beat → L1_UP is "armed"
    • When BTC spot touches -L1% below price-to-beat → L1_DOWN is "armed"
    • When BTC spot touches +L2%                     → L2_UP is "armed"
    • When BTC spot touches -L2%                     → L2_DOWN is "armed"

  Stage 2 — Exact limit only (order book):
    After a trigger is armed, watch subsequent minutes.
    Entry only if quote is EXACTLY at the limit (no better or worse price).
      • L1 UP    → enter only if UP ask  == l1_limit (e.g. 60¢ exactly)
      • L1 DOWN → enter only if DOWN ask == l1_limit (1 - yes_bid == 60¢)
      • L2 UP    → enter only if UP ask  == l2_limit (e.g. 80¢)
      • L2 DOWN → enter only if DOWN ask == l2_limit
    If the level is never quoted at that exact price, skip (no trade).

  Touch rule: BTC trigger fires once the level is TOUCHED (uses intraday high/low).
  Entry: happens at the FIRST subsequent minute the contract price meets the limit.
  ONE entry per level per market window — L1 fires once (UP or DOWN, whichever comes
  first), L2 fires once. Maximum 2 trades per 15-minute window.

P&L:
  UP   entry at `ask`:      win = pos × (1-ask)/ask,   loss = -pos
  DOWN entry at `down_ask`: win = pos × (1-down_ask)/down_ask, loss = -pos
  (down_ask = 1 - yes_bid)

Settlement: inferred from last candle price_close:
  > 0.5 → UP settled  (BTC ended above price-to-beat)
  ≤ 0.5 → DOWN settled
"""

from __future__ import annotations
import pandas as pd
import numpy as np


def run_backtest(
    kalshi_df: pd.DataFrame,
    btc_df: pd.DataFrame,
    l1_pct: float = 0.050,      # % BTC deviation to arm L1 trigger
    l1_limit: float = 0.60,     # exact entry price for L1 (e.g. 0.60 = 60¢ only)
    l2_pct: float = 0.100,      # % BTC deviation to arm L2 trigger
    l2_limit: float = 0.80,     # exact entry price for L2 (e.g. 0.80 = 80¢ only)
    position_size: float = 100.0,  # $ risked per trade
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[list[dict], dict]:
    """
    Run the two-stage strategy backtest.

    Returns
    -------
    trades  : list of trade dicts
    summary : aggregate stats dict
    """
    if kalshi_df.empty or btc_df.empty:
        return [], _empty_summary()

    # ── Date filter ──────────────────────────────────────────────────────────
    kalshi = kalshi_df.copy()
    kalshi["timestamp"] = pd.to_datetime(kalshi["timestamp"], utc=True)

    if start_date:
        kalshi = kalshi[kalshi["timestamp"] >= pd.Timestamp(start_date, tz="UTC")]
    if end_date:
        kalshi = kalshi[kalshi["timestamp"] <= pd.Timestamp(end_date, tz="UTC")]

    kalshi = kalshi[kalshi["target_price"].notna()].copy()
    if kalshi.empty:
        return [], _empty_summary()

    # ── BTC data indexed by minute ───────────────────────────────────────────
    btc = btc_df.copy()
    btc["timestamp"] = pd.to_datetime(btc["timestamp"], utc=True)
    btc = btc.set_index("timestamp").sort_index()

    kalshi = kalshi.set_index("timestamp").sort_index()

    trades: list[dict] = []

    for ticker, grp in kalshi.groupby("market_ticker"):
        grp = grp.sort_index()
        target = float(grp["target_price"].iloc[0])
        if target <= 0:
            continue

        t_start = grp.index.min()
        t_end   = grp.index.max()

        btc_window = btc.loc[(btc.index >= t_start) & (btc.index <= t_end)]
        if btc_window.empty:
            continue

        # Settlement: UP wins if last price_close > 0.5
        up_wins = _up_settled(grp.reset_index())

        # BTC threshold prices
        l1_up_price   = target * (1 + l1_pct / 100)
        l1_down_price = target * (1 - l1_pct / 100)
        l2_up_price   = target * (1 + l2_pct / 100)
        l2_down_price = target * (1 - l2_pct / 100)

        # Stage 1: has BTC touched each level? (track UP and DOWN separately for direction)
        armed_l1_up   = False
        armed_l1_down = False
        armed_l2_up   = False
        armed_l2_down = False

        # Stage 2: ONE entry per level per market (not per direction)
        # Once L1 is entered (UP or DOWN), skip any further L1 attempts this window
        entered_l1 = False
        entered_l2 = False

        for ts, btc_row in btc_window.iterrows():
            if entered_l1 and entered_l2:
                break  # both levels done, nothing left to check

            high  = float(btc_row.get("high",  btc_row["close"]))
            low   = float(btc_row.get("low",   btc_row["close"]))

            # ── Stage 1: arm triggers based on BTC intraday high/low ────────
            if high >= l1_up_price:   armed_l1_up   = True
            if low  <= l1_down_price: armed_l1_down = True
            if high >= l2_up_price:   armed_l2_up   = True
            if low  <= l2_down_price: armed_l2_down = True

            # ── Stage 2: first valid contract-price match wins ───────────────
            k_row = _nearest_kalshi_row(grp, ts)
            if k_row is None:
                continue

            yes_ask = _safe_float(k_row, "yes_ask_close")
            yes_bid = _safe_float(k_row, "yes_bid_close")

            # ── L1 — only if not yet entered this window ─────────────────────
            if not entered_l1:
                # L1 UP: best ask for UP must equal l1_limit exactly (e.g. 60¢ only)
                if armed_l1_up and yes_ask is not None and _exact_cents(yes_ask, l1_limit):
                    entered_l1 = True
                    dev = (high - target) / target * 100
                    px = float(l1_limit)
                    pnl = (position_size * (1 - px) / px) if up_wins else -position_size
                    trades.append(_trade(ticker, ts, target, high, dev, "L1_UP",
                                        "UP", position_size, px, l1_limit, pnl, up_wins))

                # L1 DOWN: DOWN ask = 1 - yes_bid must equal l1_limit exactly
                elif armed_l1_down and yes_bid is not None:
                    down_ask = 1.0 - yes_bid
                    if _exact_cents(down_ask, l1_limit):
                        entered_l1 = True
                        dev = (low - target) / target * 100
                        px = float(l1_limit)
                        pnl = (position_size * (1 - px) / px) if not up_wins else -position_size
                        trades.append(_trade(ticker, ts, target, low, dev, "L1_DOWN",
                                            "DOWN", position_size, px, l1_limit, pnl, not up_wins))

            # ── L2 — only if not yet entered this window ─────────────────────
            if not entered_l2:
                if armed_l2_up and yes_ask is not None and _exact_cents(yes_ask, l2_limit):
                    entered_l2 = True
                    dev = (high - target) / target * 100
                    px = float(l2_limit)
                    pnl = (position_size * (1 - px) / px) if up_wins else -position_size
                    trades.append(_trade(ticker, ts, target, high, dev, "L2_UP",
                                        "UP", position_size, px, l2_limit, pnl, up_wins))

                elif armed_l2_down and yes_bid is not None:
                    down_ask = 1.0 - yes_bid
                    if _exact_cents(down_ask, l2_limit):
                        entered_l2 = True
                        dev = (low - target) / target * 100
                        px = float(l2_limit)
                        pnl = (position_size * (1 - px) / px) if not up_wins else -position_size
                        trades.append(_trade(ticker, ts, target, low, dev, "L2_DOWN",
                                            "DOWN", position_size, px, l2_limit, pnl, not up_wins))

    summary = _calc_summary(trades)
    return trades, summary


# ── Helpers ──────────────────────────────────────────────────────────────────

def _up_settled(group: pd.DataFrame) -> bool:
    """True if UP (YES) settled — inferred from last candle."""
    last = group.iloc[-1]
    pc = last.get("price_close")
    if pd.notna(pc):
        return float(pc) > 0.5
    bid = last.get("yes_bid_close", 0)
    ask = last.get("yes_ask_close", 1)
    mid = (float(bid if pd.notna(bid) else 0) + float(ask if pd.notna(ask) else 1)) / 2
    return mid > 0.5


def _exact_cents(price: float, limit: float) -> bool:
    """True if `price` matches `limit` to the cent (order-book tick style)."""
    return round(price, 2) == round(float(limit), 2)


def _nearest_kalshi_row(grp: pd.DataFrame, ts: pd.Timestamp):
    """Return the Kalshi row closest in time to ts, within 2 minutes."""
    secs = np.abs((grp.index - ts).total_seconds().to_numpy())
    pos = int(np.argmin(secs))
    if secs[pos] > 120:
        return None
    return grp.iloc[pos]


def _safe_float(row, col: str):
    v = row.get(col)
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    return float(v)


def _trade(ticker, ts, target, btc_price, dev, trigger, direction,
           position_size, entry_price, limit_price, pnl, won):
    return {
        "ticker":       ticker,
        "timestamp":    ts.isoformat(),
        "target_price": round(target, 2),
        "btc_price":    round(btc_price, 2),
        "deviation":    round(dev, 4),
        "trigger":      trigger,
        "direction":    direction,
        "position":     position_size,
        "entry_price":  round(entry_price, 4),
        "limit_price":  round(limit_price, 2),
        "pnl":          round(pnl, 2),
        "outcome":      "WIN" if won else "LOSS",
    }


def _calc_summary(trades: list[dict]) -> dict:
    if not trades:
        return _empty_summary()

    df = pd.DataFrame(trades)
    wins   = df[df["outcome"] == "WIN"]
    losses = df[df["outcome"] == "LOSS"]

    cum = (
        df.sort_values("timestamp")
        .assign(date=lambda d: pd.to_datetime(d["timestamp"]).dt.date.astype(str))
        .groupby("date")["pnl"]
        .sum()
        .cumsum()
        .reset_index()
        .rename(columns={"date": "d", "pnl": "cum_pnl"})
    )

    by_level = (
        df.groupby("trigger")
        .agg(trades=("pnl", "count"), pnl=("pnl", "sum"),
             wins=("outcome", lambda x: (x == "WIN").sum()))
        .reset_index()
        .to_dict("records")
    )

    return {
        "total_trades":   int(len(df)),
        "wins":           int(len(wins)),
        "losses":         int(len(losses)),
        "win_rate":       round(len(wins) / len(df) * 100, 1),
        "total_pnl":      round(df["pnl"].sum(), 2),
        "avg_pnl":        round(df["pnl"].mean(), 2),
        "max_win":        round(df["pnl"].max(), 2),
        "max_loss":       round(df["pnl"].min(), 2),
        "total_risked":   round(df["position"].sum(), 2),
        "roi_pct":        round(df["pnl"].sum() / df["position"].sum() * 100, 2)
                          if df["position"].sum() else 0,
        "cum_pnl_series": cum.to_dict("records"),
        "by_level":       by_level,
    }


def _empty_summary() -> dict:
    return {
        "total_trades": 0, "wins": 0, "losses": 0, "win_rate": 0,
        "total_pnl": 0, "avg_pnl": 0, "max_win": 0, "max_loss": 0,
        "total_risked": 0, "roi_pct": 0,
        "cum_pnl_series": [], "by_level": [],
    }
