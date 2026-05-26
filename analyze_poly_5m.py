"""Analyze Polymarket BTC 5m up/down live tick data."""
import csv
from collections import defaultdict
from pathlib import Path

CSV_PATH = Path(__file__).parent / "data" / "poly_5m_live.csv"


def fnum(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except ValueError:
        return None


def main():
    # Per-window state: slug -> list of ticks (we only need last tick at seconds_left==0)
    windows = {}  # slug -> dict with ticks keyed by seconds_left we care about

    row_count = 0
    with CSV_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_count += 1
            slug = row["slug"]
            sec = int(row["seconds_left"]) if row["seconds_left"] else None
            if sec is None:
                continue

            btc = fnum(row["btc_chainlink_usd"])
            ptb = fnum(row["ptb_usd"])
            up_mid = None
            up_bid = fnum(row["up_bid_prob"])
            up_ask = fnum(row["up_ask_prob"])
            down_bid = fnum(row["down_bid_prob"])
            down_ask = fnum(row["down_ask_prob"])
            if up_bid is not None and up_ask is not None:
                up_mid = (up_bid + up_ask) / 2

            if slug not in windows:
                windows[slug] = {
                    "window_end_unix": int(row["window_end_unix"]),
                    "ptb": ptb,
                    "ticks": {},
                }
            w = windows[slug]
            if ptb is not None:
                w["ptb"] = ptb

            tick = {
                "sec": sec,
                "btc": btc,
                "ptb": ptb,
                "up_mid": up_mid,
                "up_bid": up_bid,
                "up_ask": up_ask,
                "down_bid": down_bid,
                "down_ask": down_ask,
            }
            # Keep best tick near each checkpoint (within +/- 3s)
            for checkpoint in (0, 1, 5, 10, 30, 60, 120, 180, 240, 300):
                prev = w["ticks"].get(checkpoint)
                if prev is None or abs(sec - checkpoint) < abs(prev["sec"] - checkpoint):
                    if abs(sec - checkpoint) <= 3:
                        w["ticks"][checkpoint] = tick

    print(f"Total rows: {row_count:,}")
    print(f"Unique 5m windows: {len(windows):,}")

    # Date range
    first_ts = last_ts = None
    with CSV_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i == 0:
                first_ts = row["timestamp_utc_iso"]
            last_ts = row["timestamp_utc_iso"]
    print(f"Date range: {first_ts} to {last_ts}")

    # Build resolved outcomes from seconds_left==0 tick
    resolved = []
    for slug, w in windows.items():
        t0 = w["ticks"].get(0)
        if not t0 or t0["btc"] is None or w["ptb"] is None:
            continue
        outcome_up = 1 if t0["btc"] >= w["ptb"] else 0
        resolved.append(
            {
                "slug": slug,
                "ptb": w["ptb"],
                "final_btc": t0["btc"],
                "delta_usd": t0["btc"] - w["ptb"],
                "outcome_up": outcome_up,
                "ticks": w["ticks"],
            }
        )

    n = len(resolved)
    up_wins = sum(r["outcome_up"] for r in resolved)
    print(f"\n=== RESOLVED OUTCOMES (n={n:,}) ===")
    print(f"UP wins:   {up_wins:,} ({100*up_wins/n:.2f}%)")
    print(f"DOWN wins: {n-up_wins:,} ({100*(n-up_wins)/n:.2f}%)")

    # Delta distribution at resolution
    deltas = [r["delta_usd"] for r in resolved]
    deltas.sort()
    print(f"\nFinal BTC vs PTB (USD) at window end:")
    for p in [0, 5, 10, 25, 50, 75, 90, 95, 100]:
        idx = min(int(n * p / 100), n - 1)
        print(f"  p{p:>3}: {deltas[idx]:+.2f}")

    # Calibration: at various checkpoints, when up_mid says X, what % actually go UP?
    checkpoints = [240, 180, 120, 60, 30, 10, 5, 1]
    print("\n=== MARKET CALIBRATION (UP mid-price vs actual UP win rate) ===")
    for cp in checkpoints:
        buckets = defaultdict(lambda: {"n": 0, "wins": 0})
        for r in resolved:
            t = r["ticks"].get(cp)
            if not t or t["up_mid"] is None or t["btc"] is None or t["ptb"] is None:
                continue
            # bucket by up_mid rounded to nearest 5%
            b = round(t["up_mid"] * 20) / 20  # 0.05 steps
            buckets[b]["n"] += 1
            buckets[b]["wins"] += r["outcome_up"]

        total_with_data = sum(v["n"] for v in buckets.values())
        if total_with_data < 50:
            continue
        print(f"\n--- {cp}s left ({total_with_data:,} windows with data) ---")
        for b in sorted(buckets.keys()):
            v = buckets[b]
            if v["n"] < 20:
                continue
            actual = v["wins"] / v["n"]
            edge = actual - b
            flag = " *** EDGE" if abs(edge) >= 0.05 else ""
            print(
                f"  up_mid={b:.2f}  n={v['n']:4d}  actual_up={actual:.3f}  "
                f"edge={edge:+.3f}{flag}"
            )

    # BTC distance from PTB vs outcome probability
    print("\n=== BTC DISTANCE FROM PTB -> ACTUAL UP PROB (at checkpoint) ===")
    for cp in [180, 60, 30, 10]:
        dist_buckets = defaultdict(lambda: {"n": 0, "wins": 0, "up_mids": []})
        for r in resolved:
            t = r["ticks"].get(cp)
            if not t or t["btc"] is None or t["ptb"] is None:
                continue
            d = t["btc"] - t["ptb"]
            # bucket by $5 bands
            if abs(d) < 2.5:
                band = "+/-0-2.5"
            elif abs(d) < 10:
                band = f"{'+' if d>0 else '-'}{int(abs(d)//5*5)}-{int(abs(d)//5*5)+5}"
            elif abs(d) < 50:
                band = f"{'+' if d>0 else '-'}{int(abs(d)//10*10)}-{int(abs(d)//10*10)+10}"
            else:
                band = f"{'+' if d>0 else '-'}50+"
            dist_buckets[band]["n"] += 1
            dist_buckets[band]["wins"] += r["outcome_up"]
            if t["up_mid"] is not None:
                dist_buckets[band]["up_mids"].append(t["up_mid"])

        print(f"\n--- {cp}s left ---")
        order = sorted(
            dist_buckets.keys(),
            key=lambda x: (
                -1 if x.startswith("-") else (0 if "+/-" in x else 1),
                x,
            ),
        )
        for band in order:
            v = dist_buckets[band]
            if v["n"] < 15:
                continue
            actual = v["wins"] / v["n"]
            avg_mid = sum(v["up_mids"]) / len(v["up_mids"]) if v["up_mids"] else 0
            print(
                f"  {band:>12}  n={v['n']:4d}  actual_up={actual:.3f}  "
                f"avg_market_up_mid={avg_mid:.3f}"
            )

    # Reversal patterns: BTC on wrong side mid-window but wins at end
    print("\n=== LATE REVERSALS (BTC wrong side at T, but opposite side wins) ===")
    for cp in [120, 60, 30]:
        wrong_side_up_lost = 0
        wrong_side_down_lost = 0
        total_cp = 0
        for r in resolved:
            t = r["ticks"].get(cp)
            if not t or t["btc"] is None or t["ptb"] is None:
                continue
            total_cp += 1
            btc_above = t["btc"] >= t["ptb"]
            if btc_above and r["outcome_up"] == 0:
                wrong_side_up_lost += 1
            if not btc_above and r["outcome_up"] == 1:
                wrong_side_down_lost += 1
        print(
            f"  {cp}s left: BTC above PTB but DOWN wins: "
            f"{wrong_side_up_lost}/{total_cp} ({100*wrong_side_up_lost/total_cp:.1f}%)"
        )
        print(
            f"  {cp}s left: BTC below PTB but UP wins:   "
            f"{wrong_side_down_lost}/{total_cp} ({100*wrong_side_down_lost/total_cp:.1f}%)"
        )

    # Simple betting sim: buy UP at ask when btc > ptb by $X at 60s left
    print("\n=== SIMPLE STRATEGY BACKTESTS (buy at ask, hold to resolution) ===")
    for side in ("UP", "DOWN"):
        for cp in [180, 120, 60, 30, 10]:
            for min_dist in [0, 5, 10, 20]:
                trades = []
                for r in resolved:
                    t = r["ticks"].get(cp)
                    if not t or t["btc"] is None or t["ptb"] is None:
                        continue
                    d = t["btc"] - t["ptb"]
                    if side == "UP":
                        if d < min_dist:
                            continue
                        price = t["up_ask"]
                        win = r["outcome_up"]
                    else:
                        if d > -min_dist:
                            continue
                        price = t["down_ask"]
                        win = 1 - r["outcome_up"]
                    if price is None or price <= 0 or price >= 1:
                        continue
                    pnl = (1 - price) if win else (-price)
                    trades.append(pnl)
                if len(trades) >= 30:
                    avg_pnl = sum(trades) / len(trades)
                    win_rate = sum(1 for p in trades if p > 0) / len(trades)
                    if avg_pnl > 0.01 or (side == "UP" and min_dist >= 10):
                        print(
                            f"  BUY {side} @ {cp}s, btc dist>={min_dist}$: "
                            f"n={len(trades):4d} win={win_rate:.1%} avg_pnl={avg_pnl:+.4f}/share"
                        )

    # Momentum: early window direction predicts outcome?
    print("\n=== EARLY MOMENTUM (240s left BTC vs PTB -> final outcome) ===")
    for cp in [240, 180]:
        above_wins = below_wins = above_n = below_n = 0
        for r in resolved:
            t = r["ticks"].get(cp)
            if not t or t["btc"] is None or t["ptb"] is None:
                continue
            if t["btc"] >= t["ptb"]:
                above_n += 1
                above_wins += r["outcome_up"]
            else:
                below_n += 1
                below_wins += 1 - r["outcome_up"]
        if above_n:
            print(
                f"  {cp}s: BTC above PTB -> UP wins {above_wins}/{above_n} "
                f"({100*above_wins/above_n:.1f}%)"
            )
        if below_n:
            print(
                f"  {cp}s: BTC below PTB -> DOWN wins {below_n-below_wins}/{below_n} "
                f"({100*(below_n-below_wins)/below_n:.1f}%)"
            )

    # Spread / mispricing when market near 50/50 at open
    print("\n=== OPEN WINDOW (300s / ~start): market vs BTC position ===")
    near50_up = near50_down = 0
    for r in resolved:
        t = r["ticks"].get(300) or r["ticks"].get(299) or r["ticks"].get(298)
        if not t or t["up_mid"] is None or t["btc"] is None or t["ptb"] is None:
            continue
        if 0.45 <= t["up_mid"] <= 0.55:
            d = t["btc"] - t["ptb"]
            if d > 0:
                near50_up += r["outcome_up"]
            else:
                near50_down += 1 - r["outcome_up"]
    print(f"  (limited sample at exact 300s checkpoint)")


if __name__ == "__main__":
    main()




