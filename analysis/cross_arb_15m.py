#!/usr/bin/env python3
"""
Cross-Platform Arbitrage: Polymarket 15m ↔ Kalshi 15m
=====================================================
Direct window-to-window comparison:
1. Price divergence per matching window
2. Temporal trade symmetries (same-second trades)
3. Amount matching ($ sizes within ±5s)
4. Volume burst synchronization
5. Wallet-level timing patterns on Polymarket side
"""

import json, os, glob
from collections import defaultdict
from datetime import datetime, timezone

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
POLY15M_DIR = os.path.join(DATA_DIR, "poly15m")

def load_poly_15m():
    """Load all Polymarket 15-min market files."""
    markets = {}
    files = glob.glob(os.path.join(POLY15M_DIR, "*.json"))
    print(f"[*] Loading {len(files)} Polymarket 15-min markets...")
    for fp in files:
        slug = os.path.basename(fp).replace(".json", "")
        ts = int(slug.split("-")[-1])
        with open(fp) as f:
            data = json.load(f)
        markets[ts] = data
    return markets

def load_kalshi():
    print("[*] Loading Kalshi trades...")
    with open(os.path.join(DATA_DIR, "kalshi_btc_trades.json")) as f:
        data = json.load(f)
    # Build dict: open_timestamp → {market, trades}
    kalshi = {}
    for market in data["markets"]:
        ticker = market["ticker"]
        try:
            open_t = datetime.fromisoformat(market["open_time"].replace("Z", "+00:00"))
            close_t = datetime.fromisoformat(market["close_time"].replace("Z", "+00:00"))
            ts = int(open_t.timestamp())
        except:
            continue
        trades = data["trades_by_ticker"].get(ticker, [])
        kalshi[ts] = {
            "ticker": ticker,
            "result": market.get("result", ""),
            "volume": market.get("volume", 0),
            "open_ts": ts,
            "close_ts": int(close_t.timestamp()),
            "trades": trades,
        }
    return kalshi

def poly_direction(trade, token_map):
    asset = str(trade.get("asset", ""))
    side = trade.get("side", "").upper()
    token_dir = token_map.get(asset, "").lower()
    if not token_dir:
        return None
    return token_dir if side == "BUY" else ("down" if token_dir == "up" else "up")


# ═══════════════════════════════════════════════════════════════
#  MATCH WINDOWS: Polymarket 15m ↔ Kalshi 15m
# ═══════════════════════════════════════════════════════════════

def match_windows(poly, kalshi):
    """Match Polymarket and Kalshi 15-min windows by timestamp."""
    print("\n" + "="*80)
    print("  WINDOW MATCHING: Polymarket 15m ↔ Kalshi 15m")
    print("="*80)

    # Kalshi windows are on :00, :15, :30, :45 boundaries
    # Polymarket windows start at their unix timestamps
    # We need to find pairs where the windows overlap

    matched = []
    unmatched_poly = 0
    tolerance = 120  # seconds tolerance for matching

    for p_ts, p_data in poly.items():
        best_k = None
        best_diff = float('inf')
        for k_ts, k_data in kalshi.items():
            diff = abs(p_ts - k_ts)
            if diff < best_diff and diff <= tolerance:
                best_diff = diff
                best_k = k_ts

        if best_k is not None:
            matched.append((p_ts, best_k, best_diff))
        else:
            unmatched_poly += 1

    print(f"  Polymarket markets: {len(poly)}")
    print(f"  Kalshi markets:     {len(kalshi)}")
    print(f"  Matched pairs:      {len(matched)}")
    print(f"  Unmatched Poly:     {unmatched_poly}")

    if matched:
        offsets = [m[2] for m in matched]
        print(f"  Match offsets: avg={sum(offsets)/len(offsets):.1f}s, max={max(offsets)}s")

    return matched


# ═══════════════════════════════════════════════════════════════
#  ANALYSIS 1: Per-Window Price Divergence
# ═══════════════════════════════════════════════════════════════

def price_divergence(poly, kalshi, matched):
    print("\n" + "="*80)
    print("  ANALYSIS 1: PRICE DIVERGENCE PER MATCHED WINDOW")
    print("="*80)

    results = []
    for p_ts, k_ts, offset in matched:
        p_data = poly[p_ts]
        k_data = kalshi[k_ts]

        p_trades = p_data.get("trades", [])
        k_trades = k_data["trades"]
        token_map = p_data.get("token_map", {})

        if not p_trades or not k_trades:
            continue

        # Polymarket: compute avg YES (up) price from BUY UP trades
        p_up_prices = []
        p_down_prices = []
        for t in p_trades:
            d = poly_direction(t, token_map)
            price = float(t.get("price", 0.5))
            if t.get("side", "").upper() == "BUY":
                if d == "up":
                    p_up_prices.append(price)
                elif d == "down":
                    p_down_prices.append(price)

        # Kalshi: compute avg YES price
        k_yes_prices = [float(t.get("price", 0.5)) for t in k_trades]

        if not p_up_prices or not k_yes_prices:
            continue

        p_yes_avg = sum(p_up_prices) / len(p_up_prices)
        k_yes_avg = sum(k_yes_prices) / len(k_yes_prices)
        spread = p_yes_avg - k_yes_avg

        p_resolution = p_data.get("resolution", "")
        k_result = k_data.get("result", "")

        # Same outcome check
        p_outcome_yes = (p_resolution == "up")
        k_outcome_yes = (k_result == "yes")

        results.append({
            "p_ts": p_ts,
            "k_ts": k_ts,
            "time": datetime.fromtimestamp(p_ts, tz=timezone.utc).strftime("%m-%d %H:%M"),
            "p_yes_avg": round(p_yes_avg, 4),
            "k_yes_avg": round(k_yes_avg, 4),
            "spread": round(spread, 4),
            "abs_spread": round(abs(spread), 4),
            "p_trades": len(p_trades),
            "k_trades": len(k_trades),
            "p_result": p_resolution,
            "k_result": k_result,
            "same_outcome": p_outcome_yes == k_outcome_yes,
            "arb_profitable": (spread > 0.05 and not k_outcome_yes) or (spread < -0.05 and k_outcome_yes),
        })

    results.sort(key=lambda x: x["abs_spread"], reverse=True)

    if results:
        spreads = [r["abs_spread"] for r in results]
        same_outcome_pct = sum(1 for r in results if r["same_outcome"]) / len(results) * 100
        arb_pct = sum(1 for r in results if r["arb_profitable"]) / len(results) * 100

        print(f"\n  Matched windows with price data: {len(results)}")
        print(f"  Same outcome (both YES or both NO): {same_outcome_pct:.1f}%")
        print(f"  Arb profitable (spread + correct side): {arb_pct:.1f}%")
        print(f"\n  Spread stats:")
        print(f"    Mean:   {sum(spreads)/len(spreads):.4f}")
        print(f"    Median: {sorted(spreads)[len(spreads)//2]:.4f}")
        print(f"    Max:    {max(spreads):.4f}")
        print(f"    >5c:    {sum(1 for s in spreads if s > 0.05)} ({sum(1 for s in spreads if s > 0.05)/len(spreads)*100:.1f}%)")
        print(f"    >10c:   {sum(1 for s in spreads if s > 0.10)} ({sum(1 for s in spreads if s > 0.10)/len(spreads)*100:.1f}%)")
        print(f"    >20c:   {sum(1 for s in spreads if s > 0.20)} ({sum(1 for s in spreads if s > 0.20)/len(spreads)*100:.1f}%)")

        print(f"\n  Top 20 divergences:")
        print(f"  {'Time':<14} {'P_YES':>6} {'K_YES':>6} {'Spread':>7} {'P_Res':>5} {'K_Res':>5} {'Same':>5} {'ArbProf':>7}")
        print("  " + "-"*65)
        for r in results[:20]:
            print(f"  {r['time']:<14} {r['p_yes_avg']:>6.2f} {r['k_yes_avg']:>6.2f} {r['spread']:>+7.4f} {r['p_result']:>5} {r['k_result']:>5} {'Y' if r['same_outcome'] else 'N':>5} {'YES' if r['arb_profitable'] else '':>7}")

    return results


# ═══════════════════════════════════════════════════════════════
#  ANALYSIS 2: Second-Level Trade Synchronization
# ═══════════════════════════════════════════════════════════════

def trade_synchronization(poly, kalshi, matched):
    print("\n" + "="*80)
    print("  ANALYSIS 2: SECOND-LEVEL TRADE SYNCHRONIZATION")
    print("="*80)

    total_sync = 0
    total_near_sync = 0
    window_syncs = []

    for p_ts, k_ts, offset in matched:
        p_data = poly[p_ts]
        k_data = kalshi[k_ts]

        p_trades = p_data.get("trades", [])
        k_trades = k_data["trades"]

        # Build second-level maps
        p_seconds = defaultdict(list)
        for t in p_trades:
            ts = int(t.get("timestamp", 0))
            if ts:
                p_seconds[ts].append(t)

        k_seconds = defaultdict(list)
        for t in k_trades:
            try:
                ts = int(datetime.fromisoformat(
                    t["created_time"].replace("Z", "+00:00")).timestamp())
                k_seconds[ts].append(t)
            except:
                continue

        # Exact same-second matches
        exact = len(set(p_seconds.keys()) & set(k_seconds.keys()))

        # Within ±3 seconds
        near = 0
        for ps in p_seconds:
            for off in range(-3, 4):
                if ps + off in k_seconds:
                    near += 1
                    break

        if exact > 0 or near > 5:
            window_syncs.append({
                "time": datetime.fromtimestamp(p_ts, tz=timezone.utc).strftime("%m-%d %H:%M"),
                "p_ts": p_ts,
                "exact_sync": exact,
                "near_sync": near,
                "p_active_secs": len(p_seconds),
                "k_active_secs": len(k_seconds),
                "sync_pct": round(exact / max(len(p_seconds), 1) * 100, 1),
            })

        total_sync += exact
        total_near_sync += near

    print(f"  Total exact same-second matches: {total_sync:,}")
    print(f"  Total within ±3s matches:        {total_near_sync:,}")

    window_syncs.sort(key=lambda x: x["exact_sync"], reverse=True)
    if window_syncs:
        print(f"\n  Top 20 synchronized windows:")
        print(f"  {'Time':<14} {'Exact':>6} {'±3s':>6} {'P_Secs':>7} {'K_Secs':>7} {'Sync%':>6}")
        print("  " + "-"*50)
        for w in window_syncs[:20]:
            print(f"  {w['time']:<14} {w['exact_sync']:>6} {w['near_sync']:>6} {w['p_active_secs']:>7} {w['k_active_secs']:>7} {w['sync_pct']:>5.1f}%")

    return window_syncs


# ═══════════════════════════════════════════════════════════════
#  ANALYSIS 3: Amount Matching ($ within ±5s)
# ═══════════════════════════════════════════════════════════════

def amount_matching(poly, kalshi, matched):
    print("\n" + "="*80)
    print("  ANALYSIS 3: AMOUNT MATCHING (same $ ±5s)")
    print("="*80)

    # For each matched window, find trades with similar $ amounts within ±5 seconds
    total_matches = 0
    amount_matches = []

    for p_ts, k_ts, offset in matched:
        p_data = poly[p_ts]
        k_data = kalshi[k_ts]
        p_trades = p_data.get("trades", [])
        k_trades = k_data["trades"]
        token_map = p_data.get("token_map", {})

        # Build timestamped dollar amount lists
        p_amounts = []
        for t in p_trades:
            ts = int(t.get("timestamp", 0))
            size = float(t.get("size", 0))
            price = float(t.get("price", 0.5))
            side = t.get("side", "").upper()
            usd = size * price if side == "BUY" else size * (1 - price)
            direction = poly_direction(t, token_map)
            if ts and usd > 1:
                p_amounts.append({"ts": ts, "usd": usd, "dir": direction, "wallet": t.get("proxyWallet", "")})

        k_amounts = []
        for t in k_trades:
            try:
                ts = int(datetime.fromisoformat(t["created_time"].replace("Z", "+00:00")).timestamp())
                count = float(t.get("count", 1))
                price = float(t.get("price", 0.5))
                usd = count * price
                taker = t.get("taker_side", "")
                if usd > 1:
                    k_amounts.append({"ts": ts, "usd": usd, "side": taker})
            except:
                continue

        # Find matches: same $ (within 10%) and within ±5 seconds
        window_matches = 0
        for pa in p_amounts:
            for ka in k_amounts:
                if abs(pa["ts"] - ka["ts"]) <= 5:
                    ratio = pa["usd"] / ka["usd"] if ka["usd"] > 0 else 999
                    if 0.8 <= ratio <= 1.2:  # within 20%
                        window_matches += 1
                        if window_matches <= 3:  # Store first 3 per window
                            amount_matches.append({
                                "time": datetime.fromtimestamp(p_ts, tz=timezone.utc).strftime("%m-%d %H:%M"),
                                "p_usd": round(pa["usd"], 2),
                                "k_usd": round(ka["usd"], 2),
                                "time_diff": pa["ts"] - ka["ts"],
                                "p_dir": pa["dir"],
                                "k_side": ka["side"],
                                "wallet": pa["wallet"][:12] + "..." if pa["wallet"] else "",
                            })
                        break  # One match per poly trade

        total_matches += window_matches

    print(f"  Total amount matches (same $ ±20%, ±5s): {total_matches:,}")

    if amount_matches:
        # Check for opposite-side matching (arb signal)
        opposite = sum(1 for m in amount_matches
                      if (m["p_dir"] == "up" and m["k_side"] == "no") or
                         (m["p_dir"] == "down" and m["k_side"] == "yes"))

        print(f"  Opposite-side matches (arb pattern): {opposite} / {len(amount_matches)}")

        print(f"\n  Sample amount matches:")
        print(f"  {'Window':<14} {'P_USD':>8} {'K_USD':>8} {'TimeDiff':>8} {'P_Dir':>6} {'K_Side':>7} {'Wallet':<15}")
        print("  " + "-"*70)
        for m in amount_matches[:30]:
            arb = " ARB!" if (m["p_dir"] == "up" and m["k_side"] == "no") or \
                             (m["p_dir"] == "down" and m["k_side"] == "yes") else ""
            print(f"  {m['time']:<14} ${m['p_usd']:>7.2f} ${m['k_usd']:>7.2f} {m['time_diff']:>+7d}s {m['p_dir'] or '?':>6} {m['k_side']:>7} {m['wallet']:<15}{arb}")

    return amount_matches


# ═══════════════════════════════════════════════════════════════
#  ANALYSIS 4: Polymarket Wallet Behavior on 15-min
# ═══════════════════════════════════════════════════════════════

def wallet_analysis_15m(poly, kalshi, matched):
    print("\n" + "="*80)
    print("  ANALYSIS 4: POLYMARKET 15-MIN WALLET PERFORMANCE")
    print("="*80)

    wallet_stats = defaultdict(lambda: {
        "trades": 0, "wins": 0, "volume": 0, "pnl": 0,
        "buy_up": 0, "sell_down": 0, "aligned_with_kalshi": 0,
    })

    for p_ts, k_ts, offset in matched:
        p_data = poly[p_ts]
        k_data = kalshi[k_ts]
        p_resolution = p_data.get("resolution", "").lower()
        k_result = k_data.get("result", "").lower()
        token_map = p_data.get("token_map", {})

        for t in p_data.get("trades", []):
            wallet = t.get("proxyWallet", "").lower()
            if not wallet:
                continue

            side = t.get("side", "").upper()
            size = float(t.get("size", 0))
            price = float(t.get("price", 0.5))
            direction = poly_direction(t, token_map)

            if not direction or not size:
                continue

            is_win = (direction == p_resolution)
            usd = size * price if side == "BUY" else size * (1 - price)
            pnl = (size - usd) if is_win else -usd

            ws = wallet_stats[wallet]
            ws["trades"] += 1
            ws["volume"] += usd
            ws["pnl"] += pnl
            if is_win:
                ws["wins"] += 1

            if side == "BUY" and direction == "up":
                ws["buy_up"] += 1
            if side == "SELL" and direction == "down":
                ws["sell_down"] += 1

            # Does this Poly trade align with Kalshi outcome?
            if k_result == "yes" and direction == "up":
                ws["aligned_with_kalshi"] += 1
            elif k_result == "no" and direction == "down":
                ws["aligned_with_kalshi"] += 1

    # Rank
    results = []
    for wallet, s in wallet_stats.items():
        if s["trades"] < 20 or s["volume"] < 50:
            continue
        wr = s["wins"] / s["trades"]
        kalshi_align = s["aligned_with_kalshi"] / s["trades"]
        results.append({
            "wallet": wallet,
            "trades": s["trades"],
            "wr": round(wr, 4),
            "volume": round(s["volume"], 2),
            "pnl": round(s["pnl"], 2),
            "kalshi_align": round(kalshi_align, 4),
        })

    results.sort(key=lambda x: x["pnl"], reverse=True)

    print(f"\n  Wallets analyzed: {len(results):,}")
    print(f"\n  TOP 30 BY PNL (15-min markets):")
    print(f"  {'Wallet':<44} {'Trades':>6} {'WR':>6} {'Volume':>10} {'PnL':>10} {'K_Align':>8}")
    print("  " + "-"*90)
    for r in results[:30]:
        flag = " [KALSHI-SYNC]" if r["kalshi_align"] > 0.60 and r["trades"] > 50 else ""
        print(f"  {r['wallet']:<44} {r['trades']:>6} {r['wr']:>6.1%} ${r['volume']:>9,.0f} ${r['pnl']:>9,.0f} {r['kalshi_align']:>8.1%}{flag}")

    # Wallets that seem to know Kalshi outcome
    suspects = [r for r in results if r["kalshi_align"] > 0.58 and r["trades"] > 30 and r["pnl"] > 0]
    suspects.sort(key=lambda x: x["kalshi_align"], reverse=True)

    print(f"\n  WALLETS WITH >58% KALSHI ALIGNMENT (potential cross-platform informed):")
    print(f"  Found: {len(suspects)}")
    for r in suspects[:20]:
        print(f"    {r['wallet']} trades={r['trades']} WR={r['wr']:.1%} PnL=${r['pnl']:,.0f} K_align={r['kalshi_align']:.1%}")

    return results, suspects


# ═══════════════════════════════════════════════════════════════

def main():
    print("="*80)
    print("  CROSS-PLATFORM ARB: Polymarket 15m ↔ Kalshi 15m")
    print("="*80)

    poly = load_poly_15m()
    kalshi = load_kalshi()

    matched = match_windows(poly, kalshi)

    divergence = price_divergence(poly, kalshi, matched)
    sync = trade_synchronization(poly, kalshi, matched)
    amounts = amount_matching(poly, kalshi, matched)
    wallets, suspects = wallet_analysis_15m(poly, kalshi, matched)

    # Save
    output = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "matched_windows": len(matched),
        "price_divergence": divergence[:100],
        "trade_sync": sync[:50],
        "amount_matches": amounts[:200],
        "top_wallets": wallets[:100],
        "kalshi_aligned_suspects": suspects[:50],
    }

    out_path = os.path.join(DATA_DIR, "cross_arb_15m_results.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Results saved to {out_path}")

if __name__ == "__main__":
    main()
