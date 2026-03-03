#!/usr/bin/env python3
"""
Wallet Link Analysis: Find connections between arb wallets
============================================================
1. Behavioral clustering — same trade sizes, timing, directions
2. Co-trading analysis — wallets active in same markets at same times
3. Shared counterparty detection — wallets trading against same maker
4. On-chain funding trace — Etherscan V2 API for USDC transfers
5. Bridge tracing — Wormhole/Relay/deBridge for cross-chain links
"""

import json, os, glob, sys
from collections import defaultdict, Counter
from datetime import datetime, timezone
import itertools

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
POLY15M_DIR = os.path.join(DATA_DIR, "poly15m")

# Top arb wallets from cross_arb_15m results (100% Kalshi-aligned + high-value)
TOP_ARB_WALLETS = [
    "0xf2eda2eca20eea27bdb0038bc3fc559e2cbb98a8",
    "0x91176db78ea5d0d1566ee028120137e1bfaececa",
    "0xf3408ba8b1427564005fb5720c495f430958e55b",
    "0x4602411d7a9ca3004813da8fec7b46d27d72b0f9",
    "0x69786a14667cf010a87ce31ad4ea278b6a9bed38",
    "0x10e052124fdfe8a8b66811a4e0c4c19ffa26b38a",
    "0x4396378a91ba74be2b448f83c586574871fc332a",
    "0x9e51207b5fbb6cfe64d656ca73a4d8f2278b691c",
    "0xa07a9fd88723a49e32761eef7b4677aaa3299832",
    "0x398f2971ca851ef4560d172b2eee46be2eb38264",
    "0x583a7e4e2ec324ccdb4811e66eb8168d9aa2aff6",
    "0x33fa1a226618ead17e49717867cf242b3fe597f4",
    "0xc0629caf9a88dc6ce0b4980c393ef600949bea25",
    "0xa3d043b2da34f58045c6485d3f89b798b2b0ec04",
    "0x537494c54dee9162534675712f2e625c9713042e",
    "0x03c3b0236c5a01051381482e77f2210349073a1d",
    "0xf0078630935f985f1d03f7d2b3cd4bfb76d810b4",
    "0xd8fbd2007b6d7cd1ca19eceba1f1f28a64c1716a",
    "0x70488a2e4635e21bff01fa084f8854e3d5e12fbc",
    "0xabb4affae8fef5eadaa3ec9ffd3e9bae18215ff4",
    # High PnL wallets
    "0x1979ae6b7e6534de9c4539d0c205e582ca637c9d",
    "0x1d0034134e339a309700ff2d34e99fa2d48b0313",
    "0xbd7d453564deca2bd3b3d51091105e94270dd7d1",
    "0x63ce342161250d705dc0b16df89036c8e5f9ba9a",
    "0xd0d6053c3c37e727402d84c14069780d360993aa",
    "0x0ea574f3204c5c9c0cdead90392ea0990f4d17e4",
    "0xe041d09715148a9a4a7a881a5580da2c0701f2e5",
    # 5-min top arb wallets
    "0x571c285a83eba5322b5f916ba681669dc368a61f",
    "0x2b41a72490ca2af2709e34368e6697cdd32fe0d3",
]

def load_all_15m_trades():
    """Load all trades from 15-min markets, indexed by wallet."""
    wallet_trades = defaultdict(list)
    market_wallets = defaultdict(set)
    files = glob.glob(os.path.join(POLY15M_DIR, "*.json"))
    print(f"[*] Loading {len(files)} Polymarket 15-min markets...")

    for fp in files:
        slug = os.path.basename(fp).replace(".json", "")
        ts = int(slug.split("-")[-1])
        with open(fp) as f:
            data = json.load(f)

        token_map = data.get("token_map", {})
        resolution = data.get("resolution", "")

        for trade in data.get("trades", []):
            wallet = trade.get("proxyWallet", "").lower()
            if not wallet:
                continue

            asset = str(trade.get("asset", ""))
            side = trade.get("side", "").upper()
            token_dir = token_map.get(asset, "").lower()

            if not token_dir:
                for tid, d in token_map.items():
                    if str(tid) == asset:
                        token_dir = d.lower()
                        break

            # Determine bet direction
            if token_dir:
                if side == "BUY":
                    bet_dir = token_dir
                else:
                    bet_dir = "down" if token_dir == "up" else "up"
            else:
                bet_dir = None

            trade_ts = trade.get("timestamp", 0)
            if isinstance(trade_ts, str):
                try:
                    trade_ts = int(datetime.fromisoformat(trade_ts.replace("Z", "+00:00")).timestamp())
                except:
                    trade_ts = 0

            size = float(trade.get("size", 0))
            price = float(trade.get("price", 0))
            usd = size * price

            trade_rec = {
                "market_ts": ts,
                "slug": slug,
                "trade_ts": trade_ts,
                "side": side,
                "bet_dir": bet_dir,
                "size": size,
                "price": price,
                "usd": usd,
                "resolution": resolution,
            }
            wallet_trades[wallet].append(trade_rec)
            market_wallets[ts].add(wallet)

    return wallet_trades, market_wallets


def load_5m_trades():
    """Load 5-min trades from whale_cache."""
    print("[*] Loading 5-min whale_cache...")
    with open(os.path.join(DATA_DIR, "whale_cache.json")) as f:
        poly_data = json.load(f)

    wallet_trades = defaultdict(list)
    market_wallets = defaultdict(set)

    for slug, market in poly_data.items():
        ts = int(slug.split("-")[-1])
        token_map = market.get("token_map", {})
        resolution = market.get("resolution", "")

        for trade in market.get("trades", []):
            wallet = trade.get("proxyWallet", "").lower()
            if not wallet:
                continue

            asset = str(trade.get("asset", ""))
            side = trade.get("side", "").upper()
            token_dir = token_map.get(asset, "").lower()
            if not token_dir:
                for tid, d in token_map.items():
                    if str(tid) == asset:
                        token_dir = d.lower()
                        break

            if token_dir:
                bet_dir = token_dir if side == "BUY" else ("down" if token_dir == "up" else "up")
            else:
                bet_dir = None

            trade_ts = trade.get("timestamp", 0)
            if isinstance(trade_ts, str):
                try:
                    trade_ts = int(datetime.fromisoformat(trade_ts.replace("Z", "+00:00")).timestamp())
                except:
                    trade_ts = 0

            size = float(trade.get("size", 0))
            price = float(trade.get("price", 0))

            trade_rec = {
                "market_ts": ts,
                "slug": slug,
                "trade_ts": trade_ts,
                "side": side,
                "bet_dir": bet_dir,
                "size": size,
                "price": price,
                "usd": size * price,
                "resolution": resolution,
            }
            wallet_trades[wallet].append(trade_rec)
            market_wallets[ts].add(wallet)

    return wallet_trades, market_wallets


def analyze_co_trading(wallet_trades, market_wallets, target_wallets):
    """Find wallets that co-trade in the same markets at the same times as target wallets."""
    print("\n" + "="*80)
    print("  ANALYSIS 1: CO-TRADING PATTERNS")
    print("="*80)

    # For each target wallet, find which markets they trade in
    target_markets = {}
    for w in target_wallets:
        w_lower = w.lower()
        if w_lower in wallet_trades:
            markets = set(t["market_ts"] for t in wallet_trades[w_lower])
            target_markets[w_lower] = markets

    print(f"  Target wallets with trades: {len(target_markets)}")

    # Find co-occurrence: which target wallets share the most markets
    co_occurrence = defaultdict(int)
    same_side_co = defaultdict(int)

    for w1, w2 in itertools.combinations(target_markets.keys(), 2):
        shared = target_markets[w1] & target_markets[w2]
        if shared:
            co_occurrence[(w1, w2)] = len(shared)

            # Check if they trade same direction in shared markets
            w1_dirs = {}
            for t in wallet_trades[w1]:
                if t["market_ts"] in shared and t["bet_dir"]:
                    w1_dirs.setdefault(t["market_ts"], []).append(t["bet_dir"])

            w2_dirs = {}
            for t in wallet_trades[w2]:
                if t["market_ts"] in shared and t["bet_dir"]:
                    w2_dirs.setdefault(t["market_ts"], []).append(t["bet_dir"])

            same_side = 0
            total_compared = 0
            for mkt in shared:
                if mkt in w1_dirs and mkt in w2_dirs:
                    # Most common direction for each
                    d1 = Counter(w1_dirs[mkt]).most_common(1)[0][0]
                    d2 = Counter(w2_dirs[mkt]).most_common(1)[0][0]
                    total_compared += 1
                    if d1 == d2:
                        same_side += 1

            if total_compared > 0:
                same_side_co[(w1, w2)] = (same_side, total_compared)

    # Print top co-occurring pairs
    top_pairs = sorted(co_occurrence.items(), key=lambda x: -x[1])[:30]

    print(f"\n  Top 30 co-trading wallet pairs:")
    print(f"  {'Wallet 1':>14s}  {'Wallet 2':>14s}  Shared  Same%  SameSide/Total")
    print(f"  {'-'*75}")

    results = []
    for (w1, w2), count in top_pairs:
        ss_data = same_side_co.get((w1, w2), (0, 0))
        ss_pct = (ss_data[0] / ss_data[1] * 100) if ss_data[1] > 0 else 0
        print(f"  {w1[:14]:>14s}  {w2[:14]:>14s}  {count:>6d}  {ss_pct:5.1f}%  {ss_data[0]}/{ss_data[1]}")
        results.append({
            "wallet1": w1,
            "wallet2": w2,
            "shared_markets": count,
            "same_side_pct": round(ss_pct, 1),
            "same_side": ss_data[0],
            "compared": ss_data[1],
        })

    return results


def analyze_timing_fingerprints(wallet_trades, target_wallets):
    """Compare timing patterns to find wallets with identical entry profiles."""
    print("\n" + "="*80)
    print("  ANALYSIS 2: TIMING FINGERPRINTS")
    print("="*80)

    fingerprints = {}
    for w in target_wallets:
        w_lower = w.lower()
        trades = wallet_trades.get(w_lower, [])
        if len(trades) < 10:
            continue

        # Compute timing within each market window
        offsets = []
        for t in trades:
            offset = t["trade_ts"] - t["market_ts"]
            if 0 <= offset <= 900:  # within 15-min window
                offsets.append(offset)

        if len(offsets) < 10:
            continue

        avg_offset = sum(offsets) / len(offsets)

        # Size distribution
        sizes = [t["usd"] for t in trades if t["usd"] > 0]
        avg_size = sum(sizes) / len(sizes) if sizes else 0

        # Buy/sell ratio
        buys = sum(1 for t in trades if t["side"] == "BUY")
        buy_pct = buys / len(trades) * 100

        # Round size %
        round_pct = sum(1 for s in sizes if abs(s - round(s)) < 0.01 or abs(s % 5) < 0.01) / len(sizes) * 100 if sizes else 0

        fingerprints[w_lower] = {
            "trades": len(trades),
            "avg_offset": round(avg_offset, 1),
            "avg_size": round(avg_size, 2),
            "buy_pct": round(buy_pct, 1),
            "round_pct": round(round_pct, 1),
        }

    # Find similar fingerprints
    print(f"\n  Wallet fingerprints ({len(fingerprints)} wallets):")
    print(f"  {'Wallet':>14s}  Trades  AvgOff  AvgSize  Buy%  Round%")
    print(f"  {'-'*65}")

    for w, fp in sorted(fingerprints.items(), key=lambda x: -x[1]["trades"]):
        print(f"  {w[:14]:>14s}  {fp['trades']:>6d}  {fp['avg_offset']:>6.0f}s  ${fp['avg_size']:>7.2f}  {fp['buy_pct']:>4.1f}%  {fp['round_pct']:>5.1f}%")

    # Cluster by similar profiles
    print("\n  SIMILAR FINGERPRINT CLUSTERS:")
    clusters = []
    wallets = list(fingerprints.keys())

    for i, w1 in enumerate(wallets):
        for w2 in wallets[i+1:]:
            fp1 = fingerprints[w1]
            fp2 = fingerprints[w2]

            # Check similarity
            offset_diff = abs(fp1["avg_offset"] - fp2["avg_offset"])
            size_ratio = min(fp1["avg_size"], fp2["avg_size"]) / max(fp1["avg_size"], fp2["avg_size"]) if max(fp1["avg_size"], fp2["avg_size"]) > 0 else 0
            buy_diff = abs(fp1["buy_pct"] - fp2["buy_pct"])

            # Similar if: timing within 30s, size within 50%, buy% within 15%
            if offset_diff < 30 and size_ratio > 0.5 and buy_diff < 15:
                clusters.append((w1, w2, offset_diff, size_ratio, buy_diff))

    clusters.sort(key=lambda x: x[2] + (1 - x[3]) * 100 + x[4])

    print(f"\n  Found {len(clusters)} similar pairs:")
    for w1, w2, od, sr, bd in clusters[:20]:
        print(f"    {w1[:14]} ↔ {w2[:14]}  offset_diff={od:.0f}s  size_ratio={sr:.2f}  buy_diff={bd:.1f}%")

    return fingerprints, clusters


def analyze_shared_counterparties(wallet_trades, market_wallets, target_wallets):
    """Find if target wallets trade against the same counterparty wallets."""
    print("\n" + "="*80)
    print("  ANALYSIS 3: SHARED COUNTERPARTIES")
    print("="*80)

    # For each target wallet, find wallets they frequently share markets with
    target_counterparties = {}

    for w in target_wallets:
        w_lower = w.lower()
        if w_lower not in wallet_trades:
            continue

        trades = wallet_trades[w_lower]
        my_markets = set(t["market_ts"] for t in trades)

        # Find all wallets in those markets
        counterparty_count = Counter()
        for mkt in my_markets:
            for other_w in market_wallets.get(mkt, set()):
                if other_w != w_lower:
                    counterparty_count[other_w] += 1

        target_counterparties[w_lower] = counterparty_count

    # Find counterparties shared across multiple target wallets
    all_counterparties = Counter()
    for w, cp_counts in target_counterparties.items():
        for cp in cp_counts:
            all_counterparties[cp] += 1  # How many target wallets share this counterparty

    # Top shared counterparties (appear with many target wallets)
    top_shared = [(cp, count) for cp, count in all_counterparties.most_common(50)
                  if count >= 3 and cp.lower() not in [w.lower() for w in target_wallets]]

    print(f"\n  Counterparties shared by 3+ target wallets:")
    print(f"  {'Counterparty':>14s}  #Targets  Total shared markets")
    print(f"  {'-'*55}")

    results = []
    for cp, count in top_shared[:30]:
        total_mkts = sum(target_counterparties[tw].get(cp, 0) for tw in target_counterparties)
        is_target = cp.lower() in [w.lower() for w in target_wallets]
        tag = " [TARGET]" if is_target else ""
        print(f"  {cp[:14]:>14s}  {count:>8d}  {total_mkts:>20d}{tag}")
        results.append({
            "counterparty": cp,
            "shared_with_n_targets": count,
            "total_shared_markets": total_mkts,
        })

    return results


def analyze_same_second_pairs(wallet_trades, target_wallets):
    """Find target wallets that trade within the same second in the same market."""
    print("\n" + "="*80)
    print("  ANALYSIS 4: SAME-SECOND TRADING (within target group)")
    print("="*80)

    # Build index: (market_ts, trade_second) -> [(wallet, trade)]
    second_index = defaultdict(list)

    for w in target_wallets:
        w_lower = w.lower()
        for t in wallet_trades.get(w_lower, []):
            key = (t["market_ts"], t["trade_ts"])
            second_index[key].append((w_lower, t))

    # Find seconds with 2+ target wallets
    sync_pairs = defaultdict(int)
    sync_same_dir = defaultdict(int)

    for key, entries in second_index.items():
        wallets_in_second = set(e[0] for e in entries)
        if len(wallets_in_second) < 2:
            continue

        for w1, w2 in itertools.combinations(wallets_in_second, 2):
            sync_pairs[(w1, w2)] += 1

            # Check if same direction
            w1_dirs = set(e[1]["bet_dir"] for e in entries if e[0] == w1 and e[1]["bet_dir"])
            w2_dirs = set(e[1]["bet_dir"] for e in entries if e[0] == w2 and e[1]["bet_dir"])
            if w1_dirs & w2_dirs:  # They share at least one direction
                sync_same_dir[(w1, w2)] += 1

    top_sync = sorted(sync_pairs.items(), key=lambda x: -x[1])[:30]

    print(f"\n  Target wallet pairs trading in same second:")
    print(f"  {'Wallet 1':>14s}  {'Wallet 2':>14s}  SameSecond  SameDir  Dir%")
    print(f"  {'-'*70}")

    results = []
    for (w1, w2), count in top_sync:
        sd = sync_same_dir.get((w1, w2), 0)
        pct = sd / count * 100 if count > 0 else 0
        print(f"  {w1[:14]:>14s}  {w2[:14]:>14s}  {count:>10d}  {sd:>7d}  {pct:5.1f}%")
        results.append({
            "wallet1": w1,
            "wallet2": w2,
            "same_second_count": count,
            "same_dir_count": sd,
            "same_dir_pct": round(pct, 1),
        })

    return results


def analyze_withdrawal_destinations(wallet_trades, target_wallets):
    """Check if wallets share withdrawal destination patterns from trade data."""
    print("\n" + "="*80)
    print("  ANALYSIS 5: TRADE HASH EXTRACTION (for on-chain tracing)")
    print("="*80)

    # We can't directly trace on-chain without API calls, but we can extract
    # transaction hashes for manual/automated tracing

    # Also check: do any target wallets appear as counterparties to each other?
    target_set = set(w.lower() for w in target_wallets)

    internal_trades = defaultdict(list)
    for w in target_wallets:
        w_lower = w.lower()
        for t in wallet_trades.get(w_lower, []):
            # Check if any other target wallet is in the same market at same time
            pass  # Would need tx hash data

    # Aggregate stats for on-chain tracing
    print(f"\n  Wallet trade statistics for on-chain tracing:")
    print(f"  {'Wallet':>14s}  Trades  Markets  FirstTrade  LastTrade   AvgUSD")
    print(f"  {'-'*80}")

    wallet_stats = []
    for w in target_wallets:
        w_lower = w.lower()
        trades = wallet_trades.get(w_lower, [])
        if not trades:
            continue

        markets = set(t["market_ts"] for t in trades)
        first_ts = min(t["trade_ts"] for t in trades if t["trade_ts"] > 0)
        last_ts = max(t["trade_ts"] for t in trades if t["trade_ts"] > 0)
        avg_usd = sum(t["usd"] for t in trades) / len(trades)

        first_dt = datetime.fromtimestamp(first_ts, tz=timezone.utc).strftime("%m-%d %H:%M")
        last_dt = datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime("%m-%d %H:%M")

        print(f"  {w_lower[:14]:>14s}  {len(trades):>6d}  {len(markets):>7d}  {first_dt}  {last_dt}  ${avg_usd:>8.2f}")

        wallet_stats.append({
            "wallet": w_lower,
            "trades": len(trades),
            "markets": len(markets),
            "first_trade_ts": first_ts,
            "last_trade_ts": last_ts,
            "avg_usd": round(avg_usd, 2),
        })

    return wallet_stats


def find_extended_arb_network(wallet_trades, market_wallets, target_wallets):
    """Starting from target wallets, find the extended network using pre-indexed trades."""
    print("\n" + "="*80)
    print("  ANALYSIS 6: EXTENDED ARB NETWORK DISCOVERY")
    print("="*80)

    target_set = set(w.lower() for w in target_wallets)

    # Pre-index ALL trades by (market_ts, second) for O(1) lookup
    print("  Building trade index...")
    second_index = defaultdict(list)  # (market_ts, second) -> [(wallet, bet_dir)]
    for w, trades in wallet_trades.items():
        if w in target_set:
            continue
        for t in trades:
            if t["bet_dir"] and t["trade_ts"] > 0:
                key = (t["market_ts"], t["trade_ts"])
                second_index[key].append((w, t["bet_dir"]))

    print(f"  Indexed {len(second_index)} unique (market, second) keys")

    # For each target trade, look up ±3s window in the index
    network_candidates = Counter()
    network_same_dir = Counter()

    for w in target_wallets:
        w_lower = w.lower()
        for t in wallet_trades.get(w_lower, []):
            if not t["bet_dir"] or t["trade_ts"] <= 0:
                continue

            mkt = t["market_ts"]
            ts = t["trade_ts"]

            for offset in range(-3, 4):
                for other_w, other_dir in second_index.get((mkt, ts + offset), []):
                    network_candidates[other_w] += 1
                    if other_dir == t["bet_dir"]:
                        network_same_dir[other_w] += 1

    # Filter: wallets with 10+ co-trades and >65% same direction
    extended = []
    for w, count in network_candidates.most_common(200):
        same_dir = network_same_dir.get(w, 0)
        if count >= 10 and (same_dir / count) > 0.65:
            extended.append({
                "wallet": w,
                "co_trades": count,
                "same_dir": same_dir,
                "same_dir_pct": round(same_dir / count * 100, 1),
            })

    print(f"\n  Extended network: wallets co-trading with targets (same dir >65%, 10+ times):")
    print(f"  Found: {len(extended)}")
    print(f"  {'Wallet':>14s}  CoTrades  SameDir  Dir%")
    print(f"  {'-'*50}")

    for item in extended[:30]:
        print(f"  {item['wallet'][:14]:>14s}  {item['co_trades']:>8d}  {item['same_dir']:>7d}  {item['same_dir_pct']:5.1f}%")

    return extended


def main():
    print("="*80)
    print("  WALLET LINK ANALYSIS: Finding Connections Between Arb Wallets")
    print("="*80)

    # Load 15m data (primary for cross-platform analysis)
    wallet_trades_15m, market_wallets_15m = load_all_15m_trades()

    all_wallet_trades = wallet_trades_15m
    all_market_wallets = market_wallets_15m

    print(f"\n  Total wallets: {len(all_wallet_trades)}")
    print(f"  Total markets: {len(all_market_wallets)}")
    print(f"  Target wallets: {len(TOP_ARB_WALLETS)}")

    # Run analyses
    co_trading = analyze_co_trading(all_wallet_trades, all_market_wallets, TOP_ARB_WALLETS)
    fingerprints, clusters = analyze_timing_fingerprints(all_wallet_trades, TOP_ARB_WALLETS)
    counterparties = analyze_shared_counterparties(all_wallet_trades, all_market_wallets, TOP_ARB_WALLETS)
    sync_pairs = analyze_same_second_pairs(all_wallet_trades, TOP_ARB_WALLETS)
    wallet_stats = analyze_withdrawal_destinations(all_wallet_trades, TOP_ARB_WALLETS)
    extended_network = find_extended_arb_network(wallet_trades_15m, market_wallets_15m, TOP_ARB_WALLETS)

    # Save results
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "target_wallets": TOP_ARB_WALLETS,
        "co_trading_pairs": co_trading,
        "timing_fingerprints": fingerprints,
        "similar_clusters": [{"w1": c[0], "w2": c[1], "offset_diff": c[2], "size_ratio": c[3]} for c in clusters[:50]],
        "shared_counterparties": counterparties,
        "same_second_pairs": sync_pairs,
        "wallet_stats": wallet_stats,
        "extended_network": extended_network[:50],
    }

    out_path = os.path.join(DATA_DIR, "wallet_links.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\n  Results saved to {out_path}")


if __name__ == "__main__":
    main()
