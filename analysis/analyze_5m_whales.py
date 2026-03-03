#!/usr/bin/env python3
"""
Polymarket 5-Minute BTC Market — Whale & Alpha Wallet Analysis

Uses YOUR existing boundaries.csv (1599 windows) as the source of truth:
  1. Parse boundaries.csv → extract window_start timestamps + actual_direction
  2. Construct slugs → fetch condition_ids from Gamma API
  3. Fetch ALL public trades per market from Data API (no auth needed)
  4. Aggregate by wallet → find alpha wallets
  5. Reverse-engineer their strategies (timing, sizing, price, direction bias)

Saves progress incrementally so you can resume if interrupted.

Usage:
    python analysis/analyze_5m_whales.py                    # all 1599 windows
    python analysis/analyze_5m_whales.py --max-windows 100  # first 100
    python analysis/analyze_5m_whales.py --resume            # continue from cache
    python analysis/analyze_5m_whales.py --analyze-only      # skip fetching, just report
"""

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

# ─── Config ──────────────────────────────────────────────────────
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API  = "https://data-api.polymarket.com"
DATA_DIR  = Path(__file__).resolve().parent.parent / "data"
BOUNDARIES = DATA_DIR / "boundaries.csv"
CACHE_FILE = DATA_DIR / "whale_cache.json"
OUTPUT_FILE = DATA_DIR / "whale_analysis.json"

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})


# ─── Step 1: Parse boundaries.csv ────────────────────────────────

def load_boundaries():
    """Load all windows from boundaries.csv, return list of dicts with unix timestamps."""
    windows = []
    with open(BOUNDARIES, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                start_str = row["window_start_utc"]
                end_str = row["window_end_utc"]
                direction = (row.get("actual_direction") or "").strip().lower()

                # Parse ISO timestamps → unix
                start_dt = datetime.fromisoformat(start_str)
                end_dt = datetime.fromisoformat(end_str)
                start_ts = int(start_dt.timestamp())
                end_ts = int(end_dt.timestamp())

                windows.append({
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "start_utc": start_str,
                    "end_utc": end_str,
                    "direction": direction,
                    "slug": f"btc-updown-5m-{start_ts}",
                    "cl_start": float(row.get("chainlink_start", 0)),
                    "cl_end": float(row.get("chainlink_end", 0)),
                })
            except (ValueError, KeyError) as e:
                continue

    return windows


# ─── Step 2: Fetch condition_id + token_ids from Gamma ───────────

def fetch_market_info(slug, retries=2):
    """Fetch market metadata from Gamma API by slug."""
    url = f"{GAMMA_API}/events?slug={slug}"
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=12)
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            if resp.status_code != 200:
                return None
            data = resp.json()
            if not data:
                return None

            event = data[0]
            markets = event.get("markets", [])
            if not markets:
                return None

            m = markets[0]
            condition_id = m.get("conditionId", "")
            outcomes_raw = m.get("outcomes", "[]")
            tokens_raw = m.get("clobTokenIds", "[]")

            outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw

            if len(outcomes) < 2 or len(tokens) < 2:
                return None

            token_map = {}
            for i, out in enumerate(outcomes):
                token_map[tokens[i]] = out.lower()

            # Resolution from outcome prices
            resolution = None
            op_raw = m.get("outcomePrices", "[]")
            try:
                op = json.loads(op_raw) if isinstance(op_raw, str) else op_raw
                for i, p in enumerate(op):
                    if float(p) >= 0.99:
                        resolution = outcomes[i].lower()
            except (ValueError, IndexError):
                pass

            return {
                "condition_id": condition_id,
                "token_map": token_map,
                "resolution": resolution,
                "slug": slug,
            }
        except Exception:
            if attempt < retries - 1:
                time.sleep(1)
    return None


# ─── Step 3: Fetch all public trades for a market ────────────────

def fetch_trades(condition_id, max_pages=30):
    """Fetch all public trades for a condition_id from the Data API."""
    all_trades = []
    offset = 0
    limit = 1000

    for _ in range(max_pages):
        try:
            resp = SESSION.get(
                f"{DATA_API}/trades",
                params={"market": condition_id, "limit": limit, "offset": offset},
                timeout=15,
            )
            if resp.status_code == 429:
                time.sleep(2)
                continue
            if resp.status_code != 200:
                break
            batch = resp.json()
            if not batch:
                break
            all_trades.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
            time.sleep(0.1)
        except Exception:
            break

    return all_trades


# ─── Step 2+3 combined: fetch with caching ───────────────────────

def load_cache():
    """Load previously fetched market+trade data."""
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    """Persist cache to disk."""
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)


def fetch_all_windows(windows, max_windows=None, workers=5):
    """
    For each window from boundaries.csv:
      1. Look up condition_id via Gamma API (slug → market)
      2. Fetch all public trades via Data API
      3. Cache results incrementally
    """
    cache = load_cache()
    to_fetch = []

    for w in windows:
        slug = w["slug"]
        if slug in cache:
            continue
        to_fetch.append(w)

    if max_windows:
        to_fetch = to_fetch[:max_windows]

    already = len(cache)
    total = len(to_fetch)
    print(f"[*] {already} windows already cached, {total} to fetch")

    if total == 0:
        print("[+] All windows cached. Use --analyze-only to skip fetching.\n")
        return cache

    fetched = 0
    errors = 0
    total_trades = 0

    def process_window(w):
        slug = w["slug"]
        minfo = fetch_market_info(slug)
        if not minfo or not minfo["condition_id"]:
            return slug, None, []

        trades = fetch_trades(minfo["condition_id"])
        return slug, minfo, trades

    # Use thread pool for concurrent fetching (I/O bound)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        batch_size = 50  # save cache every N windows

        for i, w in enumerate(to_fetch):
            fut = pool.submit(process_window, w)
            futures[fut] = (i, w)

        for fut in as_completed(futures):
            idx, w = futures[fut]
            try:
                slug, minfo, trades = fut.result()
                fetched += 1

                if minfo:
                    cache[slug] = {
                        "condition_id": minfo["condition_id"],
                        "token_map": minfo["token_map"],
                        "resolution": minfo["resolution"] or w["direction"],
                        "trades": trades,
                        "direction_from_boundaries": w["direction"],
                    }
                    total_trades += len(trades)
                    status = f"{len(trades):>4} trades"
                else:
                    cache[slug] = {"error": "not_found", "trades": []}
                    errors += 1
                    status = "NOT FOUND"

                if fetched % 10 == 0 or fetched == total:
                    pct = fetched / total * 100
                    print(f"  [{fetched:>4}/{total}] ({pct:>5.1f}%) {slug} → {status} "
                          f"| cumulative: {total_trades} trades, {errors} errors")

                # Save cache periodically
                if fetched % batch_size == 0:
                    save_cache(cache)

            except Exception as e:
                errors += 1
                fetched += 1

    save_cache(cache)
    print(f"\n[+] Fetching complete: {fetched} windows, {total_trades} trades, {errors} errors")
    print(f"    Cache saved to {CACHE_FILE}\n")
    return cache


# ─── Step 4: Analyze — aggregate by wallet ───────────────────────

def analyze(cache, min_trades=5):
    """Aggregate all trades by wallet, compute PnL, identify alpha wallets."""

    wallet_stats = defaultdict(lambda: {
        "trades": 0, "wins": 0, "losses": 0,
        "wagered": 0.0, "pnl": 0.0,
        "best_pnl": 0.0, "worst_pnl": 0.0,
        "markets": set(), "sizes": [], "prices": [],
        "directions": {"up": 0, "down": 0},
        "win_streak": 0, "max_streak": 0,
        "pseudonym": "",
        "detail": [],
        "timing": [],  # seconds into the 5-min window when they traded
    })

    n_markets = 0
    n_trades = 0

    for slug, entry in cache.items():
        if not isinstance(entry, dict) or "error" in entry:
            continue
        trades = entry.get("trades", [])
        token_map = entry.get("token_map", {})
        resolution = entry.get("resolution", "") or entry.get("direction_from_boundaries", "")

        if not resolution or not trades:
            continue

        n_markets += 1

        # Parse window start time from slug for timing analysis
        try:
            window_start_ts = int(slug.split("-")[-1])
        except ValueError:
            window_start_ts = None

        for t in trades:
            wallet = t.get("proxyWallet", t.get("maker", ""))
            if not wallet:
                continue

            side = (t.get("side") or "").upper()
            asset = t.get("asset", "")
            size = float(t.get("size", 0) or 0)
            price = float(t.get("price", 0) or 0)
            ts_str = t.get("timestamp", "")
            pseudo = t.get("pseudonym") or t.get("name") or ""

            if price <= 0 or size <= 0:
                continue

            direction = token_map.get(asset, "unknown")
            if direction == "unknown":
                continue

            n_trades += 1

            # PnL calc
            if side == "BUY":
                is_win = (direction == resolution)
                if is_win:
                    shares = size / price
                    pnl = shares - size
                else:
                    pnl = -size
            else:  # SELL
                is_win = (direction != resolution)
                if is_win:
                    pnl = size  # sold tokens that became worthless
                else:
                    shares = size / price
                    pnl = size - shares  # sold winners cheap

            ws = wallet_stats[wallet]
            ws["trades"] += 1
            ws["wagered"] += size
            ws["pnl"] += pnl
            ws["best_pnl"] = max(ws["best_pnl"], pnl)
            ws["worst_pnl"] = min(ws["worst_pnl"], pnl)
            ws["markets"].add(slug)
            ws["sizes"].append(size)
            ws["prices"].append(price)
            ws["directions"][direction] = ws["directions"].get(direction, 0) + 1
            if pseudo:
                ws["pseudonym"] = pseudo

            if is_win:
                ws["wins"] += 1
                ws["win_streak"] += 1
                ws["max_streak"] = max(ws["max_streak"], ws["win_streak"])
            else:
                ws["losses"] += 1
                ws["win_streak"] = 0

            # Timing analysis
            if window_start_ts and ts_str:
                try:
                    if isinstance(ts_str, (int, float)):
                        trade_ts = float(ts_str)
                    else:
                        trade_dt = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
                        trade_ts = trade_dt.timestamp()
                    secs_into_window = trade_ts - window_start_ts
                    if 0 <= secs_into_window <= 300:
                        ws["timing"].append(secs_into_window)
                except (ValueError, TypeError):
                    pass

            ws["detail"].append({
                "slug": slug, "side": side, "dir": direction,
                "res": resolution, "size": size, "price": price,
                "pnl": round(pnl, 4), "win": is_win, "ts": ts_str,
            })

    print(f"[+] Analyzed {n_trades:,} trades across {n_markets:,} markets")
    return wallet_stats


# ─── Step 5: Report ──────────────────────────────────────────────

def report(wallet_stats, min_trades=5, top_n=30):
    qualified = {w: s for w, s in wallet_stats.items() if s["trades"] >= min_trades}
    by_pnl = sorted(qualified.items(), key=lambda x: x[1]["pnl"], reverse=True)

    W = 130
    print("\n" + "=" * W)
    print("  POLYMARKET 5-MINUTE BTC — ALPHA WALLET ANALYSIS (from your 1599 windows)")
    print("=" * W)
    print(f"  Total unique wallets: {len(wallet_stats):,}")
    print(f"  Qualified (>={min_trades} trades): {len(qualified):,}")
    total_vol = sum(s["wagered"] for s in wallet_stats.values())
    print(f"  Total volume across all wallets: ${total_vol:,.2f}")

    # ─── Leaderboard ─────────────────────────────────────────
    print(f"\n{'─'*W}")
    print(f"  TOP {top_n} PROFITABLE WALLETS")
    print(f"{'─'*W}")
    hdr = (f"{'#':>3} {'Wallet':>14} {'Name':<18} {'Trades':>6} {'WR%':>6} "
           f"{'PnL':>11} {'Wagered':>11} {'ROI%':>7} {'AvgSz':>8} "
           f"{'AvgPx':>6} {'Streak':>6} {'Mkts':>5} {'AvgTim':>6}")
    print(hdr)
    print(f"{'─'*W}")

    for i, (wallet, s) in enumerate(by_pnl[:top_n]):
        wr = s["wins"] / s["trades"] * 100
        roi = s["pnl"] / max(s["wagered"], 0.01) * 100
        avg_sz = s["wagered"] / s["trades"]
        avg_px = sum(s["prices"]) / len(s["prices"]) if s["prices"] else 0
        short_w = wallet[:6] + ".." + wallet[-4:] if len(wallet) > 12 else wallet
        name = (s["pseudonym"] or "")[:17]
        avg_timing = sum(s["timing"]) / len(s["timing"]) if s["timing"] else -1
        timing_str = f"{avg_timing:.0f}s" if avg_timing >= 0 else "?"

        print(f"{i+1:>3} {short_w:>14} {name:<18} {s['trades']:>6} {wr:>5.1f}% "
              f"${s['pnl']:>+10.2f} ${s['wagered']:>10.2f} {roi:>+6.1f}% "
              f"${avg_sz:>7.2f} {avg_px:>5.3f} {s['max_streak']:>6} "
              f"{len(s['markets']):>5} {timing_str:>6}")

    # ─── Bottom 10 ───────────────────────────────────────────
    print(f"\n{'─'*W}")
    print(f"  BOTTOM 10 (BIGGEST LOSERS)")
    print(f"{'─'*W}")
    for i, (wallet, s) in enumerate(reversed(by_pnl[-10:])):
        wr = s["wins"] / s["trades"] * 100
        roi = s["pnl"] / max(s["wagered"], 0.01) * 100
        short_w = wallet[:6] + ".." + wallet[-4:] if len(wallet) > 12 else wallet
        name = (s["pseudonym"] or "")[:17]
        print(f"{i+1:>3} {short_w:>14} {name:<18} {s['trades']:>6} {wr:>5.1f}% "
              f"${s['pnl']:>+10.2f} ${s['wagered']:>10.2f} {roi:>+6.1f}%")

    # ─── Deep Dive: Top 10 Alpha Wallets ─────────────────────
    print(f"\n{'='*W}")
    print("  REVERSE ENGINEERING: TOP 10 ALPHA WALLETS")
    print(f"{'='*W}")

    for i, (wallet, s) in enumerate(by_pnl[:10]):
        wr = s["wins"] / s["trades"] * 100
        roi = s["pnl"] / max(s["wagered"], 0.01) * 100
        short_w = wallet[:6] + ".." + wallet[-4:]
        name = s["pseudonym"] or short_w
        prices = s["prices"]
        sizes = s["sizes"]
        timing = s["timing"]

        print(f"\n{'─'*90}")
        print(f"  #{i+1}: {name}  |  {wallet}")
        print(f"{'─'*90}")
        print(f"  PnL: ${s['pnl']:+,.2f} | Wagered: ${s['wagered']:,.2f} | ROI: {roi:+.1f}%")
        print(f"  Record: {s['wins']}W-{s['losses']}L ({wr:.1f}%) | Max streak: {s['max_streak']} | Markets: {len(s['markets'])}")
        print(f"  Best trade: ${s['best_pnl']:+.2f} | Worst: ${s['worst_pnl']:+.2f}")

        # Direction bias
        up = s["directions"].get("up", 0)
        dn = s["directions"].get("down", 0)
        total = up + dn
        print(f"  Direction: Up {up}/{total} ({up/max(total,1)*100:.0f}%) | Down {dn}/{total} ({dn/max(total,1)*100:.0f}%)")

        # Price analysis
        if prices:
            avg_p = sum(prices) / len(prices)
            cheap = sum(1 for p in prices if p <= 0.40)
            mid = sum(1 for p in prices if 0.40 < p <= 0.60)
            expensive = sum(1 for p in prices if p > 0.60)
            print(f"  Entry Prices: avg={avg_p:.3f} | <0.40: {cheap} | 0.40-0.60: {mid} | >0.60: {expensive}")
            print(f"                min={min(prices):.3f} | max={max(prices):.3f}")

        # Size analysis
        if sizes:
            avg_s = sum(sizes) / len(sizes)
            print(f"  Trade Sizes: avg=${avg_s:.2f} | min=${min(sizes):.2f} | max=${max(sizes):.2f} | total=${sum(sizes):.2f}")

        # Timing analysis (when in the 5-min window do they trade?)
        if timing:
            avg_t = sum(timing) / len(timing)
            early = sum(1 for t in timing if t < 60)
            mid_t = sum(1 for t in timing if 60 <= t < 240)
            late = sum(1 for t in timing if t >= 240)
            last30 = sum(1 for t in timing if t >= 270)
            print(f"  Entry Timing: avg={avg_t:.0f}s into window | <60s: {early} | 60-240s: {mid_t} | >240s: {late} | last30s: {last30}")

        # Strategy classification
        print(f"  --- Strategy Inference ---")
        avg_p = sum(prices) / len(prices) if prices else 0.5
        avg_t = sum(timing) / len(timing) if timing else 150
        avg_s = sum(sizes) / len(sizes) if sizes else 0

        if wr >= 92 and s["trades"] >= 50:
            print(f"    LATENCY ARB BOT: {wr:.0f}% WR across {s['trades']} trades = almost certainly exploiting exchange→oracle lag")
        elif wr >= 85 and s["trades"] >= 20:
            print(f"    HIGH-EDGE SIGNAL BOT: {wr:.0f}% WR = strong multi-source signal, likely similar to your DSP approach")
        elif wr >= 75:
            print(f"    MODERATE EDGE: {wr:.0f}% WR = consistent edge, likely filtered entries")
        elif roi > 30:
            print(f"    ASYMMETRIC BETTOR: {wr:.0f}% WR but {roi:+.0f}% ROI = buying cheap + letting winners ride")

        if avg_t >= 250:
            print(f"    LATE ENTRY (avg {avg_t:.0f}s): waits until last ~50s → trades on confirmed momentum")
        elif avg_t >= 200:
            print(f"    MID-LATE ENTRY (avg {avg_t:.0f}s): enters with 1-2 min left → balances info vs. price")
        elif avg_t < 60:
            print(f"    EARLY ENTRY (avg {avg_t:.0f}s): enters in first minute → likely market making or limit orders")

        if avg_p < 0.35:
            print(f"    CHEAP HUNTER: avg price {avg_p:.2f} → buys massive underdogs for huge payoff")
        elif avg_p < 0.50:
            print(f"    VALUE BUYER: avg price {avg_p:.2f} → buys below 50/50 odds")
        elif avg_p > 0.65:
            print(f"    CONFIRMATION BUYER: avg price {avg_p:.2f} → pays premium for high-confidence direction")

        if avg_s > 1000:
            print(f"    WHALE SIZING: ${avg_s:.0f}/trade → high conviction + large bankroll")
        elif avg_s > 100:
            print(f"    MID SIZING: ${avg_s:.0f}/trade")

        # Show 8 most recent trades
        recent = sorted(s["detail"], key=lambda x: x.get("ts", ""), reverse=True)[:8]
        if recent:
            print(f"  Recent trades:")
            for t in recent:
                mark = "W" if t["win"] else "L"
                print(f"    [{mark}] {t['side']:>4} {t['dir']:>4} @{t['price']:.3f} "
                      f"${t['size']:.2f} → ${t['pnl']:+.2f}  |  {t['slug']}")

    # ─── Aggregate Stats ─────────────────────────────────────
    print(f"\n{'='*W}")
    print("  AGGREGATE STATISTICS")
    print(f"{'='*W}")

    all_w = wallet_stats.values()
    n_total = len(wallet_stats)
    n_profit = sum(1 for s in all_w if s["pnl"] > 0)
    n_loss = sum(1 for s in all_w if s["pnl"] < 0)
    vol = sum(s["wagered"] for s in wallet_stats.values())
    pnl_win = sum(s["pnl"] for s in wallet_stats.values() if s["pnl"] > 0)
    pnl_lose = sum(s["pnl"] for s in wallet_stats.values() if s["pnl"] < 0)

    print(f"  Wallets: {n_total:,} total | {n_profit:,} profitable ({n_profit/max(n_total,1)*100:.1f}%) | {n_loss:,} losing")
    print(f"  Volume:  ${vol:,.2f}")
    print(f"  Winners PnL: ${pnl_win:+,.2f} | Losers PnL: ${pnl_lose:+,.2f} | Net: ${pnl_win+pnl_lose:+,.2f}")

    # Win rate distribution
    print(f"\n  Win Rate Distribution (>={min_trades} trades):")
    wr_b = defaultdict(int)
    for s in qualified.values():
        b = int(s["wins"] / s["trades"] * 10) * 10
        wr_b[min(b, 100)] += 1
    for b in sorted(wr_b.keys()):
        bar = "#" * min(wr_b[b], 80)
        print(f"    {b:>3}-{min(b+10,100):<3}%: {wr_b[b]:>5} {bar}")

    # ─── Actionable Insights ─────────────────────────────────
    print(f"\n{'='*W}")
    print("  ACTIONABLE INSIGHTS FOR YOUR BOT")
    print(f"{'='*W}")

    # What do the top 10 do differently?
    top10 = by_pnl[:10]
    if top10:
        avg_wr = sum(s["wins"]/s["trades"] for _, s in top10) / len(top10) * 100
        avg_px = sum(sum(s["prices"])/len(s["prices"]) for _, s in top10 if s["prices"]) / len(top10)
        avg_sz = sum(s["wagered"]/s["trades"] for _, s in top10) / len(top10)
        all_timing = []
        for _, s in top10:
            all_timing.extend(s["timing"])
        avg_tm = sum(all_timing) / len(all_timing) if all_timing else -1

        print(f"  Top 10 alpha wallets profile:")
        print(f"    Avg WR:         {avg_wr:.1f}%")
        print(f"    Avg entry price: {avg_px:.3f}")
        print(f"    Avg trade size:  ${avg_sz:.2f}")
        if avg_tm >= 0:
            print(f"    Avg entry time:  {avg_tm:.0f}s into 5-min window ({300-avg_tm:.0f}s before close)")
        print()
        print(f"  YOUR BOT comparison:")
        print(f"    Live WR: 82.2% | Backtest WR: 90.9%")
        print(f"    Entry: last 40s of window")
        print(f"    Avg bet: ~$4.52")
        print()

        if avg_tm >= 240:
            print(f"  FINDING: Alpha wallets trade LATE ({avg_tm:.0f}s avg) — similar to your approach")
            print(f"           They wait for confirmed price momentum before entering.")
        elif avg_tm < 120:
            print(f"  FINDING: Alpha wallets trade EARLY ({avg_tm:.0f}s avg) — different from your 40s window!")
            print(f"           Consider testing earlier entry with loose filters.")

        if avg_px < 0.45:
            print(f"  FINDING: Alpha wallets buy CHEAP ({avg_px:.3f} avg) — they get better risk/reward")
            print(f"           Your ob_ask threshold may be too conservative.")
        elif avg_px > 0.55:
            print(f"  FINDING: Alpha wallets pay a PREMIUM ({avg_px:.3f} avg) — they prioritize certainty")

        if avg_wr > 90:
            print(f"  FINDING: {avg_wr:.0f}% WR suggests LATENCY ARBITRAGE dominates profits")
            print(f"           These bots exploit exchange→Polymarket price lag, not prediction.")

    return by_pnl


def save_results(by_pnl, path):
    results = []
    for wallet, s in by_pnl[:200]:
        results.append({
            "wallet": wallet,
            "pseudonym": s["pseudonym"],
            "trades": s["trades"],
            "wins": s["wins"],
            "losses": s["losses"],
            "win_rate": round(s["wins"] / max(s["trades"], 1), 4),
            "pnl": round(s["pnl"], 4),
            "wagered": round(s["wagered"], 4),
            "roi_pct": round(s["pnl"] / max(s["wagered"], 0.01) * 100, 2),
            "avg_size": round(s["wagered"] / max(s["trades"], 1), 4),
            "avg_price": round(sum(s["prices"]) / max(len(s["prices"]), 1), 4),
            "max_streak": s["max_streak"],
            "best_trade": round(s["best_pnl"], 4),
            "worst_trade": round(s["worst_pnl"], 4),
            "markets_count": len(s["markets"]),
            "direction_bias": s["directions"],
            "avg_timing": round(sum(s["timing"]) / max(len(s["timing"]), 1), 1) if s["timing"] else None,
            "recent_trades": s["detail"][-30:],
        })

    with open(path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n[+] Saved top 200 wallets to {path}")


# ─── Main ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Analyze alpha wallets on Polymarket 5-min BTC markets")
    parser.add_argument("--max-windows", type=int, default=None,
                        help="Max windows to fetch (default: all)")
    parser.add_argument("--min-trades", type=int, default=5,
                        help="Min trades to qualify wallet (default: 5)")
    parser.add_argument("--top", type=int, default=30,
                        help="Top N wallets to display (default: 30)")
    parser.add_argument("--workers", type=int, default=5,
                        help="Concurrent fetch workers (default: 5)")
    parser.add_argument("--resume", action="store_true",
                        help="Continue from existing cache")
    parser.add_argument("--analyze-only", action="store_true",
                        help="Skip fetching, analyze cached data only")
    parser.add_argument("--output", type=str, default=str(OUTPUT_FILE),
                        help="Output JSON path")
    args = parser.parse_args()

    print("+" + "=" * 68 + "+")
    print("|  POLYMARKET 5-MIN BTC — ALPHA WALLET REVERSE ENGINEERING          |")
    print("|  Using YOUR 1599 windows from boundaries.csv                      |")
    print("+" + "=" * 68 + "+\n")

    # Load boundaries
    windows = load_boundaries()
    print(f"[+] Loaded {len(windows)} windows from {BOUNDARIES}")
    if windows:
        print(f"    Range: {windows[0]['start_utc']} → {windows[-1]['start_utc']}\n")

    if args.analyze_only:
        cache = load_cache()
        print(f"[+] Loaded {len(cache)} cached windows\n")
    else:
        cache = fetch_all_windows(
            windows,
            max_windows=args.max_windows,
            workers=args.workers,
        )

    # Analyze
    wallet_stats = analyze(cache, min_trades=args.min_trades)

    # Report
    by_pnl = report(wallet_stats, min_trades=args.min_trades, top_n=args.top)

    # Save
    save_results(by_pnl, args.output)


if __name__ == "__main__":
    main()
