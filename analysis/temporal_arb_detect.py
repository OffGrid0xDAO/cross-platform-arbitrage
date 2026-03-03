#!/usr/bin/env python3
"""
Temporal Arbitrage Detection: Polymarket ↔ Kalshi ↔ Binance
============================================================
Instead of linking wallets, detect arbitrage through:
1. Temporal symmetries — simultaneous trades on both platforms
2. Amount matching — same $ sizes within seconds
3. Price divergence exploitation — trades at moments of max spread
4. Volume correlation — synchronized activity bursts
5. Latency arb — Polymarket trades that follow Binance price moves
"""

import json, csv, os, sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import statistics

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

def load_all():
    print("[*] Loading Polymarket trades...")
    with open(os.path.join(DATA_DIR, "whale_cache.json")) as f:
        poly_data = json.load(f)

    print("[*] Loading Kalshi trades...")
    with open(os.path.join(DATA_DIR, "kalshi_btc_trades.json")) as f:
        kalshi_data = json.load(f)

    print("[*] Loading boundaries...")
    boundaries = {}
    with open(os.path.join(DATA_DIR, "boundaries.csv")) as f:
        for row in csv.DictReader(f):
            try:
                dt = datetime.fromisoformat(row["window_start_utc"])
                ts = int(dt.timestamp())
                boundaries[ts] = {
                    "bn_start": float(row.get("binance_start") or 0),
                    "bn_end": float(row.get("binance_end") or 0),
                    "direction": row["actual_direction"],
                }
            except:
                continue

    return poly_data, kalshi_data, boundaries


def parse_kalshi_window(market):
    """Parse Kalshi market open/close times to unix timestamps."""
    try:
        open_t = datetime.fromisoformat(market["open_time"].replace("Z", "+00:00"))
        close_t = datetime.fromisoformat(market["close_time"].replace("Z", "+00:00"))
        return int(open_t.timestamp()), int(close_t.timestamp())
    except:
        return None, None


def parse_poly_slug_ts(slug):
    return int(slug.split("-")[-1])


def determine_poly_direction(trade, market_data):
    """Determine if a Polymarket trade is betting UP or DOWN."""
    token_map = market_data.get("token_map", {})
    asset = trade.get("asset", "")
    side = trade.get("side", "").upper()

    # Token map: token_id → direction (e.g., "12345...": "up")
    trade_token_dir = token_map.get(str(asset), "").lower()
    if not trade_token_dir:
        # Try reverse mapping in case format varies
        for token_id, direction in token_map.items():
            if str(token_id) == str(asset):
                trade_token_dir = direction.lower()
                break

    if not trade_token_dir:
        return None

    # BUY UP token = betting UP, SELL UP token = betting DOWN
    if side == "BUY":
        return trade_token_dir
    else:
        return "down" if trade_token_dir == "up" else "up"


# ═══════════════════════════════════════════════════════════════════
#  ANALYSIS 1: Second-by-Second Volume Correlation
# ═══════════════════════════════════════════════════════════════════

def volume_correlation(poly_data, kalshi_data, boundaries):
    print("\n" + "="*80)
    print("  ANALYSIS 1: SECOND-BY-SECOND VOLUME CORRELATION")
    print("="*80)

    # Build Kalshi per-second volume map
    kalshi_markets = {m["ticker"]: m for m in kalshi_data["markets"]}
    kalshi_second = defaultdict(lambda: {"count": 0, "volume": 0, "yes_vol": 0, "no_vol": 0})

    for ticker, trades in kalshi_data["trades_by_ticker"].items():
        market = kalshi_markets.get(ticker, {})
        for trade in trades:
            try:
                ts = int(datetime.fromisoformat(
                    trade["created_time"].replace("Z", "+00:00")
                ).timestamp())
                count = float(trade.get("count", trade.get("count_fp", 1)))
                price = float(trade.get("price", 0.5))
                taker = trade.get("taker_side", "")

                kalshi_second[ts]["count"] += 1
                kalshi_second[ts]["volume"] += count * price
                if taker == "yes":
                    kalshi_second[ts]["yes_vol"] += count
                else:
                    kalshi_second[ts]["no_vol"] += count
            except:
                continue

    print(f"  Kalshi: {sum(v['count'] for v in kalshi_second.values()):,} trades across {len(kalshi_second):,} seconds")

    # Build Polymarket per-second volume map
    poly_second = defaultdict(lambda: {"count": 0, "volume": 0, "up_vol": 0, "down_vol": 0})

    for slug, market in poly_data.items():
        for trade in market.get("trades", []):
            ts = int(trade.get("timestamp", 0))
            size = float(trade.get("size", 0))
            price = float(trade.get("price", 0.5))
            side = trade.get("side", "").upper()

            if not ts or not size:
                continue

            direction = determine_poly_direction(trade, market)

            poly_second[ts]["count"] += 1
            poly_second[ts]["volume"] += size * price if side == "BUY" else size * (1 - price)
            if direction == "up":
                poly_second[ts]["up_vol"] += size
            elif direction == "down":
                poly_second[ts]["down_vol"] += size

    print(f"  Polymarket: {sum(v['count'] for v in poly_second.values()):,} trades across {len(poly_second):,} seconds")

    # Find overlapping time range
    k_times = set(kalshi_second.keys())
    p_times = set(poly_second.keys())
    overlap = k_times & p_times

    print(f"  Seconds with activity on BOTH platforms: {len(overlap):,}")

    if not overlap:
        # Check time ranges
        if k_times and p_times:
            k_min, k_max = min(k_times), max(k_times)
            p_min, p_max = min(p_times), max(p_times)
            print(f"  Kalshi range:     {datetime.fromtimestamp(k_min, tz=timezone.utc)} to {datetime.fromtimestamp(k_max, tz=timezone.utc)}")
            print(f"  Polymarket range: {datetime.fromtimestamp(p_min, tz=timezone.utc)} to {datetime.fromtimestamp(p_max, tz=timezone.utc)}")

            # Expand to ±5 second windows for matching
            near_overlap = 0
            for ps in p_times:
                for offset in range(-5, 6):
                    if ps + offset in k_times:
                        near_overlap += 1
                        break
            print(f"  Seconds with activity within ±5s on both: {near_overlap:,}")
        return

    # Compute correlation on overlapping seconds
    # Bucket into 10-second windows for better signal
    bucket_size = 10
    k_buckets = defaultdict(float)
    p_buckets = defaultdict(float)

    for ts in k_times:
        bucket = ts // bucket_size * bucket_size
        k_buckets[bucket] += kalshi_second[ts]["volume"]
    for ts in p_times:
        bucket = ts // bucket_size * bucket_size
        p_buckets[bucket] += poly_second[ts]["volume"]

    common_buckets = set(k_buckets.keys()) & set(p_buckets.keys())
    if len(common_buckets) > 10:
        k_vals = [k_buckets[b] for b in sorted(common_buckets)]
        p_vals = [p_buckets[b] for b in sorted(common_buckets)]

        # Pearson correlation
        n = len(k_vals)
        k_mean = sum(k_vals) / n
        p_mean = sum(p_vals) / n
        cov = sum((k_vals[i] - k_mean) * (p_vals[i] - p_mean) for i in range(n))
        k_std = (sum((v - k_mean)**2 for v in k_vals) / n) ** 0.5
        p_std = (sum((v - p_mean)**2 for v in p_vals) / n) ** 0.5

        if k_std > 0 and p_std > 0:
            corr = cov / (n * k_std * p_std)
            print(f"\n  Volume correlation (10s buckets): {corr:.4f}")
            if corr > 0.3:
                print(f"  *** SIGNIFICANT CORRELATION — platforms have synchronized volume ***")
            elif corr > 0.1:
                print(f"  Moderate correlation — some volume sync")
        else:
            print(f"  Cannot compute correlation (zero variance)")

    return kalshi_second, poly_second


# ═══════════════════════════════════════════════════════════════════
#  ANALYSIS 2: Price Divergence Windows
# ═══════════════════════════════════════════════════════════════════

def price_divergence_analysis(poly_data, kalshi_data, boundaries):
    print("\n" + "="*80)
    print("  ANALYSIS 2: PRICE DIVERGENCE EXPLOITATION")
    print("="*80)

    # For each Kalshi 15-min window, find overlapping Polymarket 5-min windows
    kalshi_markets = {m["ticker"]: m for m in kalshi_data["markets"]}

    matched_windows = []

    for ticker, k_market in kalshi_markets.items():
        k_open, k_close = parse_kalshi_window(k_market)
        if not k_open:
            continue

        k_result = k_market.get("result", "")
        k_trades = kalshi_data["trades_by_ticker"].get(ticker, [])
        if not k_trades:
            continue

        # Kalshi avg price
        k_prices = [float(t.get("price", 0.5)) for t in k_trades if t.get("price")]
        k_avg = sum(k_prices) / len(k_prices) if k_prices else 0.5

        # Find overlapping Polymarket 5-min windows
        for p_slug, p_market in poly_data.items():
            p_ts = parse_poly_slug_ts(p_slug)
            p_end = p_ts + 300

            # Check overlap with Kalshi window
            if p_ts >= k_open and p_ts < k_close:
                p_trades = p_market.get("trades", [])
                if not p_trades:
                    continue

                p_prices = [float(t.get("price", 0.5)) for t in p_trades
                           if t.get("side", "").upper() == "BUY"]
                p_avg = sum(p_prices) / len(p_prices) if p_prices else 0.5

                # Price spread
                spread = abs(k_avg - p_avg)
                p_dir = p_market.get("direction_from_boundaries", boundaries.get(p_ts, {}).get("direction", ""))

                matched_windows.append({
                    "k_ticker": ticker,
                    "p_slug": p_slug,
                    "k_open": k_open,
                    "p_ts": p_ts,
                    "k_avg_price": round(k_avg, 4),
                    "p_avg_price": round(p_avg, 4),
                    "spread": round(spread, 4),
                    "k_trades": len(k_trades),
                    "p_trades": len(p_trades),
                    "k_result": k_result,
                    "p_direction": p_dir,
                })

    matched_windows.sort(key=lambda x: x["spread"], reverse=True)

    print(f"\n  Matched Kalshi-Polymarket windows: {len(matched_windows)}")

    if matched_windows:
        print(f"\n  Top 20 price divergences:")
        print(f"  {'Kalshi Ticker':<35} {'Poly Slug':<30} {'K_Avg':>6} {'P_Avg':>6} {'Spread':>7} {'K_Res':>5} {'P_Dir':>5}")
        print("  " + "-"*100)
        for w in matched_windows[:20]:
            print(f"  {w['k_ticker']:<35} {w['p_slug']:<30} {w['k_avg_price']:>6.2f} {w['p_avg_price']:>6.2f} {w['spread']:>7.4f} {w['k_result']:>5} {w['p_direction']:>5}")

        # Compute statistics
        spreads = [w["spread"] for w in matched_windows]
        print(f"\n  Spread statistics:")
        print(f"    Mean:   {sum(spreads)/len(spreads):.4f}")
        print(f"    Median: {sorted(spreads)[len(spreads)//2]:.4f}")
        print(f"    Max:    {max(spreads):.4f}")
        print(f"    >0.10:  {sum(1 for s in spreads if s > 0.10)} windows ({sum(1 for s in spreads if s > 0.10)/len(spreads)*100:.1f}%)")
        print(f"    >0.20:  {sum(1 for s in spreads if s > 0.20)} windows ({sum(1 for s in spreads if s > 0.20)/len(spreads)*100:.1f}%)")

    return matched_windows


# ═══════════════════════════════════════════════════════════════════
#  ANALYSIS 3: Latency Arb — Polymarket Wallets vs Binance
# ═══════════════════════════════════════════════════════════════════

def latency_arb_wallets(poly_data, boundaries):
    print("\n" + "="*80)
    print("  ANALYSIS 3: LATENCY ARB — POLYMARKET WALLETS vs BINANCE")
    print("="*80)

    wallet_stats = defaultdict(lambda: {
        "total": 0, "wins": 0, "volume": 0, "pnl": 0,
        "late_total": 0, "late_wins": 0, "late_vol": 0, "late_pnl": 0,
        "aligned": 0, "aligned_wins": 0,
        "big_move_total": 0, "big_move_wins": 0, "big_move_vol": 0,
    })

    for slug, market in poly_data.items():
        window_ts = parse_poly_slug_ts(slug)
        bd = boundaries.get(window_ts)
        if not bd or not bd["bn_start"] or not bd["bn_end"]:
            continue

        actual_dir = bd["direction"]
        bn_move = (bd["bn_end"] - bd["bn_start"]) / bd["bn_start"] * 100
        big_move = abs(bn_move) > 0.05

        for trade in market.get("trades", []):
            wallet = trade.get("proxyWallet", "").lower()
            if not wallet:
                continue

            side = trade.get("side", "").upper()
            size = float(trade.get("size", 0))
            price = float(trade.get("price", 0.5))
            ts = int(trade.get("timestamp", 0))

            if not ts or not size:
                continue

            direction = determine_poly_direction(trade, market)
            if not direction:
                continue

            entry_offset = ts - window_ts
            is_win = (direction == actual_dir)

            usdc_val = size * price if side == "BUY" else size * (1 - price)
            pnl = (size - usdc_val) if is_win else -usdc_val

            ws = wallet_stats[wallet]
            ws["total"] += 1
            ws["volume"] += usdc_val
            ws["pnl"] += pnl
            if is_win:
                ws["wins"] += 1

            # Late entry (last 60s)
            if entry_offset >= 240:
                ws["late_total"] += 1
                ws["late_vol"] += usdc_val
                ws["late_pnl"] += pnl
                if is_win:
                    ws["late_wins"] += 1

            # Aligned with Binance
            if (bn_move > 0 and direction == "up") or (bn_move < 0 and direction == "down"):
                ws["aligned"] += 1
                if is_win:
                    ws["aligned_wins"] += 1

            # Big BTC moves
            if big_move:
                ws["big_move_total"] += 1
                ws["big_move_vol"] += usdc_val
                if is_win:
                    ws["big_move_wins"] += 1

    # Rank by PnL and filter
    results = []
    for wallet, s in wallet_stats.items():
        if s["total"] < 20 or s["volume"] < 100:
            continue

        wr = s["wins"] / s["total"]
        late_wr = s["late_wins"] / s["late_total"] if s["late_total"] > 5 else 0
        aligned_wr = s["aligned_wins"] / s["aligned"] if s["aligned"] > 10 else 0
        big_wr = s["big_move_wins"] / s["big_move_total"] if s["big_move_total"] > 5 else 0

        results.append({
            "wallet": wallet,
            "trades": s["total"], "wr": round(wr, 4),
            "volume": round(s["volume"], 2), "pnl": round(s["pnl"], 2),
            "late_trades": s["late_total"], "late_wr": round(late_wr, 4),
            "late_pnl": round(s["late_pnl"], 2),
            "aligned": s["aligned"], "aligned_wr": round(aligned_wr, 4),
            "big_move_trades": s["big_move_total"], "big_move_wr": round(big_wr, 4),
            "big_move_vol": round(s["big_move_vol"], 2),
        })

    results.sort(key=lambda x: x["pnl"], reverse=True)

    print(f"\n  Total wallets analyzed: {len(results):,}")
    print(f"\n  TOP 30 BY PNL:")
    print(f"  {'Wallet':<44} {'Trades':>6} {'WR':>6} {'Volume':>10} {'PnL':>10} {'LateWR':>7} {'AlignWR':>8} {'BigMvWR':>8}")
    print("  " + "-"*108)
    for r in results[:30]:
        flag = ""
        if r["late_wr"] > 0.65 and r["late_trades"] > 10:
            flag += " [LATE-ARB]"
        if r["aligned_wr"] > 0.60 and r["aligned"] > 20:
            flag += " [BN-ALIGN]"
        if r["big_move_wr"] > 0.65 and r["big_move_trades"] > 10:
            flag += " [BIG-MOVE]"
        print(f"  {r['wallet']:<44} {r['trades']:>6} {r['wr']:>6.1%} ${r['volume']:>9,.0f} ${r['pnl']:>9,.0f} {r['late_wr']:>7.1%} {r['aligned_wr']:>8.1%} {r['big_move_wr']:>8.1%}{flag}")

    # Specific arb suspects: high late WR + aligned with Binance
    suspects = [r for r in results if
                (r["late_wr"] > 0.60 and r["late_trades"] > 10) or
                (r["aligned_wr"] > 0.58 and r["aligned"] > 30) or
                (r["big_move_wr"] > 0.60 and r["big_move_trades"] > 15 and r["pnl"] > 500)]

    suspects.sort(key=lambda x: x["pnl"], reverse=True)

    print(f"\n  ARB SUSPECTS (late WR>60% OR aligned WR>58% OR big-move WR>60%):")
    print(f"  Found: {len(suspects)}")
    print(f"  {'Wallet':<44} {'Trades':>6} {'WR':>6} {'PnL':>10} {'LateWR':>7} {'Late#':>5} {'AlignWR':>8} {'BigMvWR':>8} {'BigMvVol':>10}")
    print("  " + "-"*118)
    for r in suspects[:40]:
        print(f"  {r['wallet']:<44} {r['trades']:>6} {r['wr']:>6.1%} ${r['pnl']:>9,.0f} {r['late_wr']:>7.1%} {r['late_trades']:>5} {r['aligned_wr']:>8.1%} {r['big_move_wr']:>8.1%} ${r['big_move_vol']:>9,.0f}")

    return results, suspects


# ═══════════════════════════════════════════════════════════════════
#  ANALYSIS 4: Temporal Trade Burst Matching
# ═══════════════════════════════════════════════════════════════════

def temporal_burst_matching(poly_data, kalshi_data):
    print("\n" + "="*80)
    print("  ANALYSIS 4: TEMPORAL TRADE BURST MATCHING (Polymarket ↔ Kalshi)")
    print("="*80)

    # Build per-minute trade counts for both platforms
    poly_minute = defaultdict(lambda: {"count": 0, "volume": 0})
    kalshi_minute = defaultdict(lambda: {"count": 0, "volume": 0})

    for slug, market in poly_data.items():
        for trade in market.get("trades", []):
            ts = int(trade.get("timestamp", 0))
            size = float(trade.get("size", 0))
            price = float(trade.get("price", 0.5))
            if ts and size:
                minute = ts // 60 * 60
                poly_minute[minute]["count"] += 1
                poly_minute[minute]["volume"] += size * price

    for ticker, trades in kalshi_data["trades_by_ticker"].items():
        for trade in trades:
            try:
                ts = int(datetime.fromisoformat(
                    trade["created_time"].replace("Z", "+00:00")).timestamp())
                count = float(trade.get("count", 1))
                price = float(trade.get("price", 0.5))
                minute = ts // 60 * 60
                kalshi_minute[minute]["count"] += 1
                kalshi_minute[minute]["volume"] += count * price
            except:
                continue

    # Find overlap
    common_minutes = set(poly_minute.keys()) & set(kalshi_minute.keys())
    print(f"  Polymarket active minutes: {len(poly_minute):,}")
    print(f"  Kalshi active minutes:     {len(kalshi_minute):,}")
    print(f"  Overlapping minutes:       {len(common_minutes):,}")

    if not common_minutes:
        k_min = min(kalshi_minute.keys()) if kalshi_minute else 0
        k_max = max(kalshi_minute.keys()) if kalshi_minute else 0
        p_min = min(poly_minute.keys()) if poly_minute else 0
        p_max = max(poly_minute.keys()) if poly_minute else 0
        print(f"  Kalshi range:     {datetime.fromtimestamp(k_min, tz=timezone.utc)} to {datetime.fromtimestamp(k_max, tz=timezone.utc)}")
        print(f"  Polymarket range: {datetime.fromtimestamp(p_min, tz=timezone.utc)} to {datetime.fromtimestamp(p_max, tz=timezone.utc)}")
        return

    # Correlation
    k_vols = [kalshi_minute[m]["volume"] for m in sorted(common_minutes)]
    p_vols = [poly_minute[m]["volume"] for m in sorted(common_minutes)]

    n = len(k_vols)
    k_mean = sum(k_vols) / n
    p_mean = sum(p_vols) / n
    cov = sum((k_vols[i] - k_mean) * (p_vols[i] - p_mean) for i in range(n))
    k_std = max((sum((v - k_mean)**2 for v in k_vols) / n) ** 0.5, 1e-10)
    p_std = max((sum((v - p_mean)**2 for v in p_vols) / n) ** 0.5, 1e-10)
    corr = cov / (n * k_std * p_std)

    print(f"\n  Volume correlation (per-minute): {corr:.4f}")
    if abs(corr) > 0.3:
        print(f"  *** SIGNIFICANT — platforms have correlated volume patterns ***")

    # Trade count correlation
    k_counts = [kalshi_minute[m]["count"] for m in sorted(common_minutes)]
    p_counts = [poly_minute[m]["count"] for m in sorted(common_minutes)]
    kc_mean = sum(k_counts) / n
    pc_mean = sum(p_counts) / n
    cov_c = sum((k_counts[i] - kc_mean) * (p_counts[i] - pc_mean) for i in range(n))
    kc_std = max((sum((v - kc_mean)**2 for v in k_counts) / n) ** 0.5, 1e-10)
    pc_std = max((sum((v - pc_mean)**2 for v in p_counts) / n) ** 0.5, 1e-10)
    corr_c = cov_c / (n * kc_std * pc_std)
    print(f"  Trade count correlation:        {corr_c:.4f}")

    # Find synchronized bursts (both platforms >2x their average in same minute)
    k_avg_vol = sum(kalshi_minute[m]["volume"] for m in common_minutes) / len(common_minutes)
    p_avg_vol = sum(poly_minute[m]["volume"] for m in common_minutes) / len(common_minutes)

    sync_bursts = []
    for m in sorted(common_minutes):
        k_v = kalshi_minute[m]["volume"]
        p_v = poly_minute[m]["volume"]
        if k_v > k_avg_vol * 2 and p_v > p_avg_vol * 2:
            sync_bursts.append({
                "minute": m,
                "time": datetime.fromtimestamp(m, tz=timezone.utc).strftime("%m-%d %H:%M"),
                "k_vol": round(k_v, 2),
                "p_vol": round(p_v, 2),
                "k_trades": kalshi_minute[m]["count"],
                "p_trades": poly_minute[m]["count"],
                "k_ratio": round(k_v / k_avg_vol, 1),
                "p_ratio": round(p_v / p_avg_vol, 1),
            })

    print(f"\n  Synchronized volume bursts (both >2x average): {len(sync_bursts)}")
    if sync_bursts:
        print(f"  {'Time':<14} {'K_Vol':>10} {'P_Vol':>10} {'K_Trades':>9} {'P_Trades':>9} {'K_Ratio':>8} {'P_Ratio':>8}")
        print("  " + "-"*70)
        for b in sync_bursts[:25]:
            print(f"  {b['time']:<14} ${b['k_vol']:>9,.0f} ${b['p_vol']:>9,.0f} {b['k_trades']:>9} {b['p_trades']:>9} {b['k_ratio']:>7.1f}x {b['p_ratio']:>7.1f}x")

    return corr, corr_c, sync_bursts


# ═══════════════════════════════════════════════════════════════════

def main():
    print("="*80)
    print("  TEMPORAL ARBITRAGE DETECTION: Polymarket ↔ Kalshi ↔ Binance")
    print("="*80)

    poly_data, kalshi_data, boundaries = load_all()
    print(f"  Polymarket markets: {len(poly_data)}")
    print(f"  Kalshi markets: {len(kalshi_data['markets'])}")
    print(f"  Boundaries: {len(boundaries)} windows")

    # Run all analyses
    vol_result = volume_correlation(poly_data, kalshi_data, boundaries)
    divergence = price_divergence_analysis(poly_data, kalshi_data, boundaries)
    wallet_results, suspects = latency_arb_wallets(poly_data, boundaries)
    burst_result = temporal_burst_matching(poly_data, kalshi_data)

    # Save
    output = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "matched_windows": len(divergence) if divergence else 0,
        "top_divergences": divergence[:50] if divergence else [],
        "top_wallets_by_pnl": wallet_results[:50],
        "arb_suspects": suspects[:50],
        "sync_bursts": burst_result[2] if burst_result and len(burst_result) > 2 else [],
        "volume_correlation": burst_result[0] if burst_result else None,
        "count_correlation": burst_result[1] if burst_result else None,
    }

    out_path = os.path.join(DATA_DIR, "temporal_arb_results.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Results saved to {out_path}")


if __name__ == "__main__":
    main()
