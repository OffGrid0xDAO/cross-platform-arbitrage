#!/usr/bin/env python3
"""
Cross-Platform Arbitrage Detection Analysis
============================================

Hypotheses tested:
1. LATENCY ARB (Exchange → Polymarket): wallets that trade after BTC direction
   is already known from Binance, achieving >90% win rate in last 30s
2. INFORMED TIMING: wallets whose entry times correlate with BTC moves on Binance
3. SELL-CLUSTER ARBITRAGE: wallets that systematically sell the loser token
   (possible NegRisk minting arb or cross-platform hedge)
4. VOLUME SPIKE CORRELATION: wallets whose trade volume spikes in sync with
   Binance volatility (suggesting exchange-linked strategy)
5. CROSS-PLATFORM SIGNATURES: wallets that show patterns consistent with
   hedging on another platform (e.g., always trade one side, high WR, round sizes)
6. DEPOSIT/WITHDRAWAL TIMING: wallets with on-chain USDC flow patterns that
   suggest capital rotation between platforms

Data: whale_cache.json (2.18M trades, 26K wallets, 642 markets)
      boundaries.csv (1644 windows with Binance/Chainlink prices)
"""

import json
import csv
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
import statistics

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
WHALE_CACHE = os.path.join(DATA_DIR, "whale_cache.json")
BOUNDARIES = os.path.join(DATA_DIR, "boundaries.csv")

# ─── Load Data ─────────────────────────────────────────────────────

def load_boundaries():
    """Load boundaries.csv into a dict keyed by window_start unix timestamp."""
    boundaries = {}
    with open(BOUNDARIES) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dt = datetime.fromisoformat(row["window_start_utc"])
                ts = int(dt.timestamp())
                cl_s = row.get("chainlink_start", "")
                cl_e = row.get("chainlink_end", "")
                bn_s = row.get("binance_start", "")
                bn_e = row.get("binance_end", "")
                if not cl_s or not cl_e or not bn_s or not bn_e:
                    continue
                boundaries[ts] = {
                    "cl_start": float(cl_s),
                    "cl_end": float(cl_e),
                    "bn_start": float(bn_s),
                    "bn_end": float(bn_e),
                    "bn_momentum": float(row.get("binance_momentum_60s") or 0),
                    "divergence": float(row.get("divergence_at_end") or 0),
                    "direction": row["actual_direction"],
                    "window_start": ts,
                    "window_end": ts + 300,
                }
            except (ValueError, KeyError):
                continue
    return boundaries

def load_whale_cache():
    """Load whale_cache.json."""
    print("[*] Loading whale_cache.json (1.6GB)...")
    with open(WHALE_CACHE) as f:
        data = json.load(f)
    print(f"    Loaded {len(data)} markets")
    return data

def parse_slug_timestamp(slug):
    """Extract window_start unix timestamp from slug like 'btc-updown-5m-1771588500'."""
    parts = slug.split("-")
    return int(parts[-1])

# ─── Hypothesis 1: Latency Arbitrage ──────────────────────────────

def analyze_latency_arb(whale_data, boundaries):
    """
    HYPOTHESIS: Some wallets exploit the lag between Binance price moves
    and Polymarket oracle updates. They see BTC move on Binance, then
    buy the winning side on Polymarket before the book adjusts.

    SIGNATURE:
    - Trade in last 30-60s of window (direction already known from exchanges)
    - Very high win rate (>85%)
    - Consistent volume (not random)
    - Entry aligns with Binance direction
    """
    print("\n" + "="*80)
    print("HYPOTHESIS 1: LATENCY ARBITRAGE (Exchange → Polymarket)")
    print("="*80)

    wallet_stats = defaultdict(lambda: {
        "trades": 0, "wins": 0, "losses": 0,
        "buy_trades": 0, "sell_trades": 0,
        "late_trades": 0, "late_wins": 0,  # last 30s
        "very_late_trades": 0, "very_late_wins": 0,  # last 15s
        "last_60_trades": 0, "last_60_wins": 0,
        "total_volume": 0.0,
        "entry_times": [],
        "directions_correct": [],
        "pseudonym": "",
        "sizes": [],
        "prices": [],
        "bn_move_at_entry": [],  # Binance move magnitude at time of trade
    })

    for slug, market in whale_data.items():
        w_start = parse_slug_timestamp(slug)
        w_end = w_start + 300
        resolution = market.get("resolution") or market.get("direction_from_boundaries")
        if not resolution:
            continue

        token_map = market.get("token_map", {})
        # Reverse map: token_id → direction
        token_to_dir = {}
        for tok_id, direction in token_map.items():
            token_to_dir[tok_id] = direction

        # Get boundary data for Binance move context
        boundary = boundaries.get(w_start)
        bn_move = None
        if boundary:
            bn_move = boundary["bn_end"] - boundary["bn_start"]

        for trade in market["trades"]:
            wallet = trade["proxyWallet"]
            ts = trade["timestamp"]
            secs_into_window = ts - w_start
            side = trade["side"]  # BUY or SELL
            size = trade["size"]
            price = trade["price"]
            asset = trade["asset"]

            # Determine what direction this trade bets on
            trade_dir = token_to_dir.get(asset)
            if not trade_dir:
                continue

            # For BUY: you want this token to win
            # For SELL: you want this token to lose
            if side == "BUY":
                bet_direction = trade_dir
            else:
                bet_direction = "down" if trade_dir == "up" else "up"

            won = bet_direction == resolution
            volume = size * price

            ws = wallet_stats[wallet]
            ws["trades"] += 1
            ws["wins"] += int(won)
            ws["losses"] += int(not won)
            ws["total_volume"] += volume
            ws["entry_times"].append(secs_into_window)
            ws["directions_correct"].append(won)
            ws["pseudonym"] = trade.get("pseudonym", "")
            ws["sizes"].append(size)
            ws["prices"].append(price)

            if side == "BUY":
                ws["buy_trades"] += 1
            else:
                ws["sell_trades"] += 1

            # Track Binance move at entry time (interpolated)
            if bn_move is not None:
                # Approximate: linear interpolation of BN move by time fraction
                time_frac = min(max(secs_into_window / 300.0, 0), 1)
                approx_bn_at_entry = bn_move * time_frac
                ws["bn_move_at_entry"].append(approx_bn_at_entry)

            # Late-window buckets
            if secs_into_window >= 270:  # last 30s
                ws["late_trades"] += 1
                ws["late_wins"] += int(won)
            if secs_into_window >= 285:  # last 15s
                ws["very_late_trades"] += 1
                ws["very_late_wins"] += int(won)
            if secs_into_window >= 240:  # last 60s
                ws["last_60_trades"] += 1
                ws["last_60_wins"] += int(won)

    # ─── Score and rank wallets ───────────────────────────────────
    latency_arb_candidates = []

    for wallet, s in wallet_stats.items():
        if s["trades"] < 20:
            continue

        wr = s["wins"] / s["trades"] if s["trades"] > 0 else 0
        late_wr = s["late_wins"] / s["late_trades"] if s["late_trades"] > 0 else 0
        vlate_wr = s["very_late_wins"] / s["very_late_trades"] if s["very_late_trades"] > 0 else 0
        l60_wr = s["last_60_wins"] / s["last_60_trades"] if s["last_60_trades"] > 0 else 0

        avg_timing = statistics.mean(s["entry_times"]) if s["entry_times"] else 0
        timing_std = statistics.stdev(s["entry_times"]) if len(s["entry_times"]) > 1 else 0

        # Latency arb score: high WR + late entry + consistency
        late_fraction = s["late_trades"] / s["trades"] if s["trades"] > 0 else 0
        arb_score = 0

        # Strong signal: >85% WR in last 30s with >10 trades
        if s["late_trades"] >= 10 and late_wr >= 0.85:
            arb_score += 3
        elif s["late_trades"] >= 5 and late_wr >= 0.80:
            arb_score += 2

        # Very strong: >90% WR in last 15s
        if s["very_late_trades"] >= 5 and vlate_wr >= 0.90:
            arb_score += 3

        # Bonus: overall high WR with many trades
        if s["trades"] >= 50 and wr >= 0.80:
            arb_score += 2
        if s["trades"] >= 100 and wr >= 0.75:
            arb_score += 1

        # Bonus: primarily late trader (>50% of trades in last 60s)
        if s["last_60_trades"] / s["trades"] > 0.5:
            arb_score += 1

        # Bonus: consistent sizing (arb bots use fixed sizes)
        if s["sizes"]:
            size_cv = statistics.stdev(s["sizes"]) / statistics.mean(s["sizes"]) if len(s["sizes"]) > 1 and statistics.mean(s["sizes"]) > 0 else 99
            if size_cv < 0.3:
                arb_score += 1

        if arb_score >= 2:
            latency_arb_candidates.append({
                "wallet": wallet,
                "pseudonym": s["pseudonym"],
                "arb_score": arb_score,
                "trades": s["trades"],
                "wr": wr,
                "late_trades": s["late_trades"],
                "late_wr": late_wr,
                "vlate_trades": s["very_late_trades"],
                "vlate_wr": vlate_wr,
                "avg_timing": avg_timing,
                "timing_std": timing_std,
                "volume": s["total_volume"],
                "buy_pct": s["buy_trades"] / s["trades"],
                "sell_pct": s["sell_trades"] / s["trades"],
                "late_fraction": late_fraction,
                "avg_size": statistics.mean(s["sizes"]),
                "size_cv": statistics.stdev(s["sizes"]) / statistics.mean(s["sizes"]) if len(s["sizes"]) > 1 and statistics.mean(s["sizes"]) > 0 else 0,
            })

    latency_arb_candidates.sort(key=lambda x: x["arb_score"], reverse=True)

    print(f"\n  Wallets analyzed: {len(wallet_stats):,}")
    print(f"  Latency arb candidates (score >= 2): {len(latency_arb_candidates)}")

    print(f"\n  {'─'*110}")
    print(f"  {'Pseudonym':<25} {'Score':>5} {'Trades':>7} {'WR':>6} {'Late30':>7} {'LateWR':>7} {'VLate':>6} {'VLWR':>6} {'AvgT':>6} {'Vol$':>10} {'Buy%':>5} {'SizCV':>5}")
    print(f"  {'─'*110}")

    for c in latency_arb_candidates[:30]:
        print(f"  {c['pseudonym'][:24]:<25} {c['arb_score']:>5} {c['trades']:>7} {c['wr']:>5.1%} {c['late_trades']:>7} {c['late_wr']:>6.1%} {c['vlate_trades']:>6} {c['vlate_wr']:>5.1%} {c['avg_timing']:>5.0f}s ${c['volume']:>9,.0f} {c['buy_pct']:>4.0%} {c['size_cv']:>5.2f}")

    return wallet_stats, latency_arb_candidates


# ─── Hypothesis 2: Sell-Cluster Arbitrage ─────────────────────────

def analyze_sell_cluster(whale_data, boundaries):
    """
    HYPOTHESIS: Some wallets systematically sell the losing side token
    to enter positions (NegRisk minting). This could indicate:
    - Cross-platform hedging (sell loser on Polymarket, buy winner on Kalshi)
    - Superior execution via thinner loser-side books
    - Arbitrage between Polymarket's implied probability and exchange spot

    SIGNATURE:
    - High sell percentage (>60% of trades are SELL)
    - High win rate (selling the loser = predicting the winner correctly)
    - Trade sizes consistent with minting (size ≈ collateral / (1 - sell_price))
    """
    print("\n" + "="*80)
    print("HYPOTHESIS 2: SELL-CLUSTER ARBITRAGE (Sell-the-Loser)")
    print("="*80)

    wallet_sell = defaultdict(lambda: {
        "total_trades": 0, "sells": 0, "buys": 0,
        "sell_wins": 0, "buy_wins": 0,
        "sell_volume": 0.0, "buy_volume": 0.0,
        "sell_prices": [], "sell_sizes": [],
        "pseudonym": "", "markets_traded": set(),
    })

    for slug, market in whale_data.items():
        resolution = market.get("resolution") or market.get("direction_from_boundaries")
        if not resolution:
            continue

        token_map = market.get("token_map", {})
        token_to_dir = {tok: d for tok, d in token_map.items()}

        for trade in market["trades"]:
            wallet = trade["proxyWallet"]
            side = trade["side"]
            asset = trade["asset"]
            trade_dir = token_to_dir.get(asset)
            if not trade_dir:
                continue

            if side == "BUY":
                bet_direction = trade_dir
            else:
                bet_direction = "down" if trade_dir == "up" else "up"

            won = bet_direction == resolution
            ws = wallet_sell[wallet]
            ws["total_trades"] += 1
            ws["pseudonym"] = trade.get("pseudonym", "")
            ws["markets_traded"].add(slug)

            if side == "SELL":
                ws["sells"] += 1
                ws["sell_wins"] += int(won)
                ws["sell_volume"] += trade["size"] * trade["price"]
                ws["sell_prices"].append(trade["price"])
                ws["sell_sizes"].append(trade["size"])
            else:
                ws["buys"] += 1
                ws["buy_wins"] += int(won)
                ws["buy_volume"] += trade["size"] * trade["price"]

    # Filter to wallets with significant sell activity
    sell_heavy = []
    for wallet, s in wallet_sell.items():
        if s["total_trades"] < 20 or s["sells"] < 10:
            continue
        sell_pct = s["sells"] / s["total_trades"]
        if sell_pct < 0.40:
            continue

        sell_wr = s["sell_wins"] / s["sells"] if s["sells"] > 0 else 0
        buy_wr = s["buy_wins"] / s["buys"] if s["buys"] > 0 else 0
        avg_sell_price = statistics.mean(s["sell_prices"]) if s["sell_prices"] else 0
        pnl_estimate = (s["sell_wins"] * 1.0 - s["sells"]) * statistics.mean(s["sell_sizes"]) if s["sell_sizes"] else 0

        sell_heavy.append({
            "wallet": wallet,
            "pseudonym": s["pseudonym"],
            "trades": s["total_trades"],
            "sells": s["sells"],
            "buys": s["buys"],
            "sell_pct": sell_pct,
            "sell_wr": sell_wr,
            "buy_wr": buy_wr,
            "sell_volume": s["sell_volume"],
            "buy_volume": s["buy_volume"],
            "avg_sell_price": avg_sell_price,
            "markets": len(s["markets_traded"]),
        })

    sell_heavy.sort(key=lambda x: x["sell_volume"], reverse=True)

    print(f"\n  Sell-heavy wallets (>40% sells, ≥10 sells): {len(sell_heavy)}")
    print(f"\n  {'─'*120}")
    print(f"  {'Pseudonym':<25} {'Trades':>7} {'Sells':>6} {'S%':>5} {'SellWR':>7} {'BuyWR':>7} {'SellVol':>10} {'BuyVol':>10} {'AvgSP':>6} {'Mkts':>5}")
    print(f"  {'─'*120}")

    for c in sell_heavy[:25]:
        print(f"  {c['pseudonym'][:24]:<25} {c['trades']:>7} {c['sells']:>6} {c['sell_pct']:>4.0%} {c['sell_wr']:>6.1%} {c['buy_wr']:>6.1%} ${c['sell_volume']:>9,.0f} ${c['buy_volume']:>9,.0f} {c['avg_sell_price']:>5.2f} {c['markets']:>5}")

    return sell_heavy


# ─── Hypothesis 3: Binance-Move Timing Correlation ────────────────

def analyze_binance_correlation(whale_data, boundaries):
    """
    HYPOTHESIS: Some wallets only trade after Binance has already moved
    significantly. They wait for the exchange signal, then bet on Polymarket.

    SIGNATURE:
    - High win rate when Binance move at boundary is large (>$50)
    - Trade timing clusters right after large Binance moves
    - Win rate increases with BN move magnitude (they only trade when certain)
    """
    print("\n" + "="*80)
    print("HYPOTHESIS 3: BINANCE-MOVE TIMING CORRELATION")
    print("="*80)

    # For each market, compute what Binance said at various time points
    wallet_bn_corr = defaultdict(lambda: {
        "trades_small_move": 0, "wins_small_move": 0,
        "trades_medium_move": 0, "wins_medium_move": 0,
        "trades_large_move": 0, "wins_large_move": 0,
        "trades": 0, "wins": 0,
        "pseudonym": "",
        "selectivity": [],  # list of (bn_move_abs, traded_bool) per window
    })

    # Track per-market Binance move magnitudes
    market_bn_moves = {}
    for w_start, b in boundaries.items():
        bn_move = abs(b["bn_end"] - b["bn_start"])
        market_bn_moves[w_start] = bn_move

    # Track which wallets trade in which markets
    wallet_market_activity = defaultdict(lambda: defaultdict(list))

    for slug, market in whale_data.items():
        w_start = parse_slug_timestamp(slug)
        resolution = market.get("resolution") or market.get("direction_from_boundaries")
        if not resolution:
            continue

        boundary = boundaries.get(w_start)
        if not boundary:
            continue

        bn_move = abs(boundary["bn_end"] - boundary["bn_start"])
        token_map = market.get("token_map", {})
        token_to_dir = {tok: d for tok, d in token_map.items()}

        for trade in market["trades"]:
            wallet = trade["proxyWallet"]
            side = trade["side"]
            asset = trade["asset"]
            trade_dir = token_to_dir.get(asset)
            if not trade_dir:
                continue

            if side == "BUY":
                bet_direction = trade_dir
            else:
                bet_direction = "down" if trade_dir == "up" else "up"

            won = bet_direction == resolution
            ws = wallet_bn_corr[wallet]
            ws["trades"] += 1
            ws["wins"] += int(won)
            ws["pseudonym"] = trade.get("pseudonym", "")

            if bn_move < 30:
                ws["trades_small_move"] += 1
                ws["wins_small_move"] += int(won)
            elif bn_move < 80:
                ws["trades_medium_move"] += 1
                ws["wins_medium_move"] += int(won)
            else:
                ws["trades_large_move"] += 1
                ws["wins_large_move"] += int(won)

            wallet_market_activity[wallet][w_start].append(won)

    # Find wallets that are selective (skip low-vol windows, trade high-vol)
    selective_wallets = []
    for wallet, s in wallet_bn_corr.items():
        if s["trades"] < 30:
            continue

        wr = s["wins"] / s["trades"]
        wr_small = s["wins_small_move"] / s["trades_small_move"] if s["trades_small_move"] > 0 else 0
        wr_med = s["wins_medium_move"] / s["trades_medium_move"] if s["trades_medium_move"] > 0 else 0
        wr_large = s["wins_large_move"] / s["trades_large_move"] if s["trades_large_move"] > 0 else 0

        # Check selectivity: do they skip small moves?
        activity = wallet_market_activity[wallet]
        markets_traded = set(activity.keys())
        all_markets = set(boundaries.keys())
        markets_available = markets_traded  # markets within their date range

        selectivity_score = 0

        # Higher WR on large moves → informed about volatility
        if s["trades_large_move"] >= 5 and wr_large > wr + 0.10:
            selectivity_score += 2
        if s["trades_large_move"] >= 5 and wr_large >= 0.80:
            selectivity_score += 2

        # Overall high WR
        if wr >= 0.70 and s["trades"] >= 50:
            selectivity_score += 2

        # Mostly trade high-vol windows
        if s["trades_large_move"] + s["trades_medium_move"] > 0:
            high_vol_frac = (s["trades_large_move"] + s["trades_medium_move"]) / s["trades"]
            if high_vol_frac > 0.6:
                selectivity_score += 1

        if selectivity_score >= 2:
            selective_wallets.append({
                "wallet": wallet,
                "pseudonym": s["pseudonym"],
                "score": selectivity_score,
                "trades": s["trades"],
                "wr": wr,
                "t_small": s["trades_small_move"],
                "wr_small": wr_small,
                "t_med": s["trades_medium_move"],
                "wr_med": wr_med,
                "t_large": s["trades_large_move"],
                "wr_large": wr_large,
            })

    selective_wallets.sort(key=lambda x: x["score"], reverse=True)

    print(f"\n  Binance-correlated wallets (score >= 2): {len(selective_wallets)}")
    print(f"\n  BN move bins: small <$30, medium $30-80, large >$80")
    print(f"\n  {'─'*115}")
    print(f"  {'Pseudonym':<25} {'Score':>5} {'Trades':>7} {'WR':>6} {'T_sm':>5} {'WR_sm':>6} {'T_md':>5} {'WR_md':>6} {'T_lg':>5} {'WR_lg':>6}")
    print(f"  {'─'*115}")

    for c in selective_wallets[:25]:
        print(f"  {c['pseudonym'][:24]:<25} {c['score']:>5} {c['trades']:>7} {c['wr']:>5.1%} {c['t_small']:>5} {c['wr_small']:>5.1%} {c['t_med']:>5} {c['wr_med']:>5.1%} {c['t_large']:>5} {c['wr_large']:>5.1%}")

    return selective_wallets


# ─── Hypothesis 4: Cross-Platform Signature Detection ─────────────

def analyze_cross_platform_signatures(whale_data, boundaries):
    """
    HYPOTHESIS: Wallets doing cross-platform arb show distinctive patterns:
    - Round trade sizes ($5, $10, $20, $50) → automated bot
    - Always bet one side (directional) → hedged on other platform
    - Very consistent entry timing → scheduled execution
    - Win rate close to 50% but profitable → spread capture not direction

    Cross-platform arb between Polymarket and Kalshi:
    - If Polymarket UP costs $0.55 and Kalshi DOWN costs $0.40,
      buying both gives guaranteed $1 payout for $0.95 → $0.05 profit
    - These wallets would show: always buy, price sensitivity, small edge
    """
    print("\n" + "="*80)
    print("HYPOTHESIS 4: CROSS-PLATFORM ARBITRAGE SIGNATURES")
    print("="*80)

    wallet_patterns = defaultdict(lambda: {
        "trades": 0, "wins": 0,
        "sizes": [], "prices": [],
        "entry_times": [],
        "directions": {"up": 0, "down": 0},
        "dir_wins": {"up": 0, "down": 0},
        "buy_count": 0, "sell_count": 0,
        "pseudonym": "",
        "markets_traded": set(),
        "volumes_per_market": defaultdict(float),  # slug → total volume
    })

    for slug, market in whale_data.items():
        w_start = parse_slug_timestamp(slug)
        resolution = market.get("resolution") or market.get("direction_from_boundaries")
        if not resolution:
            continue

        token_map = market.get("token_map", {})
        token_to_dir = {tok: d for tok, d in token_map.items()}

        for trade in market["trades"]:
            wallet = trade["proxyWallet"]
            side = trade["side"]
            asset = trade["asset"]
            size = trade["size"]
            price = trade["price"]
            ts = trade["timestamp"]
            secs = ts - w_start

            trade_dir = token_to_dir.get(asset)
            if not trade_dir:
                continue

            if side == "BUY":
                bet_direction = trade_dir
            else:
                bet_direction = "down" if trade_dir == "up" else "up"

            won = bet_direction == resolution
            ws = wallet_patterns[wallet]
            ws["trades"] += 1
            ws["wins"] += int(won)
            ws["sizes"].append(size)
            ws["prices"].append(price)
            ws["entry_times"].append(secs)
            ws["directions"][bet_direction] += 1
            ws["dir_wins"][bet_direction] += int(won)
            ws["pseudonym"] = trade.get("pseudonym", "")
            ws["markets_traded"].add(slug)
            ws["volumes_per_market"][slug] += size * price
            if side == "BUY":
                ws["buy_count"] += 1
            else:
                ws["sell_count"] += 1

    # Detect patterns
    arb_signatures = []

    for wallet, s in wallet_patterns.items():
        if s["trades"] < 30:
            continue

        wr = s["wins"] / s["trades"]
        avg_size = statistics.mean(s["sizes"])
        avg_price = statistics.mean(s["prices"])
        avg_time = statistics.mean(s["entry_times"])

        # Check for round sizes (bot signature)
        round_sizes = sum(1 for sz in s["sizes"] if abs(sz - round(sz)) < 0.05 or abs(sz % 5) < 0.05)
        round_pct = round_sizes / len(s["sizes"])

        # Check for consistent timing
        time_std = statistics.stdev(s["entry_times"]) if len(s["entry_times"]) > 1 else 999

        # Direction bias
        up_pct = s["directions"]["up"] / s["trades"]
        bias = abs(up_pct - 0.5) * 2  # 0 = balanced, 1 = fully one-sided

        # Number of unique markets (high = systematic)
        market_count = len(s["markets_traded"])
        markets_per_trade = market_count / s["trades"]

        # Cross-platform arb score
        cp_score = 0

        # Bot-like behavior: round sizes + consistent timing
        if round_pct > 0.7 and time_std < 60:
            cp_score += 2

        # High WR + many markets + systematic
        if wr >= 0.65 and market_count >= 50:
            cp_score += 2

        # Narrow WR (50-60%) but very consistent = spread capture not direction
        if 0.50 <= wr <= 0.65 and s["trades"] >= 100 and time_std < 80:
            cp_score += 1

        # Always buys (never sells) + high WR = simple directional hedge
        if s["sell_count"] == 0 and wr >= 0.70 and s["trades"] >= 30:
            cp_score += 1

        # Mostly sells (>70%) → possible minting arb
        if s["sell_count"] / s["trades"] > 0.70 and s["trades"] >= 30:
            cp_score += 2

        # Very high market coverage (trades almost every window)
        if market_count >= 200:
            cp_score += 1

        if cp_score >= 2:
            arb_signatures.append({
                "wallet": wallet,
                "pseudonym": s["pseudonym"],
                "cp_score": cp_score,
                "trades": s["trades"],
                "wr": wr,
                "avg_size": avg_size,
                "avg_price": avg_price,
                "round_pct": round_pct,
                "time_std": time_std,
                "avg_time": avg_time,
                "up_pct": up_pct,
                "bias": bias,
                "buy_pct": s["buy_count"] / s["trades"],
                "markets": market_count,
                "total_vol": sum(s["volumes_per_market"].values()),
            })

    arb_signatures.sort(key=lambda x: x["cp_score"], reverse=True)

    print(f"\n  Cross-platform candidates (score >= 2): {len(arb_signatures)}")
    print(f"\n  {'─'*130}")
    print(f"  {'Pseudonym':<25} {'Score':>5} {'Trades':>7} {'WR':>6} {'AvgSz':>7} {'RndPct':>6} {'TStd':>6} {'AvgT':>6} {'Bias':>5} {'Buy%':>5} {'Mkts':>5} {'TotVol':>10}")
    print(f"  {'─'*130}")

    for c in arb_signatures[:30]:
        print(f"  {c['pseudonym'][:24]:<25} {c['cp_score']:>5} {c['trades']:>7} {c['wr']:>5.1%} {c['avg_size']:>6.1f} {c['round_pct']:>5.0%} {c['time_std']:>5.0f}s {c['avg_time']:>5.0f}s {c['bias']:>4.0%} {c['buy_pct']:>4.0%} {c['markets']:>5} ${c['total_vol']:>9,.0f}")

    return arb_signatures


# ─── Hypothesis 5: Two-Sided Trading (Market Making) ─────────────

def analyze_market_makers(whale_data, boundaries):
    """
    HYPOTHESIS: Some wallets act as market makers — they trade both sides
    in the same window, capturing the spread. They may also be hedging
    cross-platform (buy UP on Polymarket, buy DOWN on Kalshi).

    SIGNATURE:
    - Trades both UP and DOWN in the same window
    - Total payout close to break-even (spread capture)
    - Very high trade frequency per window
    - Low net directional exposure
    """
    print("\n" + "="*80)
    print("HYPOTHESIS 5: MARKET MAKERS & TWO-SIDED TRADERS")
    print("="*80)

    wallet_mm = defaultdict(lambda: {
        "total_windows": 0,
        "both_side_windows": 0,
        "trades": 0,
        "total_buy_vol": 0.0,
        "total_sell_vol": 0.0,
        "pseudonym": "",
        "window_details": [],  # list of (up_buys, down_buys, up_sells, down_sells) per window
    })

    for slug, market in whale_data.items():
        resolution = market.get("resolution") or market.get("direction_from_boundaries")
        if not resolution:
            continue

        token_map = market.get("token_map", {})
        token_to_dir = {tok: d for tok, d in token_map.items()}

        # Group trades by wallet within this window
        window_wallets = defaultdict(lambda: {
            "up_buys": 0, "down_buys": 0,
            "up_sells": 0, "down_sells": 0,
            "up_buy_vol": 0.0, "down_buy_vol": 0.0,
            "up_sell_vol": 0.0, "down_sell_vol": 0.0,
            "pseudonym": "",
        })

        for trade in market["trades"]:
            wallet = trade["proxyWallet"]
            side = trade["side"]
            asset = trade["asset"]
            trade_dir = token_to_dir.get(asset)
            if not trade_dir:
                continue

            vol = trade["size"] * trade["price"]
            ww = window_wallets[wallet]
            ww["pseudonym"] = trade.get("pseudonym", "")

            if side == "BUY" and trade_dir == "up":
                ww["up_buys"] += 1
                ww["up_buy_vol"] += vol
            elif side == "BUY" and trade_dir == "down":
                ww["down_buys"] += 1
                ww["down_buy_vol"] += vol
            elif side == "SELL" and trade_dir == "up":
                ww["up_sells"] += 1
                ww["up_sell_vol"] += vol
            elif side == "SELL" and trade_dir == "down":
                ww["down_sells"] += 1
                ww["down_sell_vol"] += vol

        for wallet, ww in window_wallets.items():
            ws = wallet_mm[wallet]
            ws["total_windows"] += 1
            ws["pseudonym"] = ww["pseudonym"]
            ws["trades"] += ww["up_buys"] + ww["down_buys"] + ww["up_sells"] + ww["down_sells"]
            ws["total_buy_vol"] += ww["up_buy_vol"] + ww["down_buy_vol"]
            ws["total_sell_vol"] += ww["up_sell_vol"] + ww["down_sell_vol"]

            # Check if traded both sides
            has_up = (ww["up_buys"] > 0 or ww["up_sells"] > 0)
            has_down = (ww["down_buys"] > 0 or ww["down_sells"] > 0)
            if has_up and has_down:
                ws["both_side_windows"] += 1
                ws["window_details"].append(ww)

    mm_candidates = []
    for wallet, s in wallet_mm.items():
        if s["total_windows"] < 10 or s["both_side_windows"] < 3:
            continue

        both_pct = s["both_side_windows"] / s["total_windows"]
        if both_pct < 0.05:
            continue

        mm_candidates.append({
            "wallet": wallet,
            "pseudonym": s["pseudonym"],
            "windows": s["total_windows"],
            "both_windows": s["both_side_windows"],
            "both_pct": both_pct,
            "trades": s["trades"],
            "buy_vol": s["total_buy_vol"],
            "sell_vol": s["total_sell_vol"],
            "net_vol": s["total_buy_vol"] - s["total_sell_vol"],
        })

    mm_candidates.sort(key=lambda x: x["both_pct"], reverse=True)

    print(f"\n  Two-sided traders (≥3 both-side windows): {len(mm_candidates)}")
    print(f"\n  {'─'*115}")
    print(f"  {'Pseudonym':<25} {'Wins':>6} {'Both':>5} {'B%':>5} {'Trades':>7} {'BuyVol':>10} {'SellVol':>10} {'NetVol':>10}")
    print(f"  {'─'*115}")

    for c in mm_candidates[:25]:
        print(f"  {c['pseudonym'][:24]:<25} {c['windows']:>6} {c['both_windows']:>5} {c['both_pct']:>4.0%} {c['trades']:>7} ${c['buy_vol']:>9,.0f} ${c['sell_vol']:>9,.0f} ${c['net_vol']:>+9,.0f}")

    return mm_candidates


# ─── Hypothesis 6: Polymarket-Binance Price Discrepancy ───────────

def analyze_price_discrepancy(whale_data, boundaries):
    """
    HYPOTHESIS: When Polymarket UP token price diverges from Binance-implied
    probability, arbitrageurs step in. If Binance shows BTC clearly up but
    Polymarket UP token is still cheap, that's a buy signal.

    We check: do certain wallets trade preferentially when the
    Polymarket-Binance divergence is large?
    """
    print("\n" + "="*80)
    print("HYPOTHESIS 6: POLYMARKET vs BINANCE PRICE DISCREPANCY")
    print("="*80)

    # For each boundary, compute the divergence
    divergence_stats = []
    for w_start, b in boundaries.items():
        bn_move = b["bn_end"] - b["bn_start"]
        cl_move = b["cl_end"] - b["cl_start"]
        divergence = abs(b["divergence"])
        divergence_stats.append({
            "w_start": w_start,
            "bn_move": bn_move,
            "cl_move": cl_move,
            "divergence": divergence,
            "direction": b["direction"],
        })

    if divergence_stats:
        divs = [d["divergence"] for d in divergence_stats]
        print(f"\n  Binance-Chainlink divergence at boundary:")
        print(f"    Mean: ${statistics.mean(divs):.2f}")
        print(f"    Median: ${statistics.median(divs):.2f}")
        print(f"    Stdev: ${statistics.stdev(divs):.2f}")
        print(f"    Max: ${max(divs):.2f}")
        print(f"    >$20 divergence: {sum(1 for d in divs if d > 20)} windows ({sum(1 for d in divs if d > 20)/len(divs):.1%})")
        print(f"    >$50 divergence: {sum(1 for d in divs if d > 50)} windows ({sum(1 for d in divs if d > 50)/len(divs):.1%})")

    # Track wallets that preferentially trade high-divergence windows
    wallet_div = defaultdict(lambda: {
        "high_div_trades": 0, "high_div_wins": 0,
        "low_div_trades": 0, "low_div_wins": 0,
        "pseudonym": "", "trades": 0, "wins": 0,
    })

    for slug, market in whale_data.items():
        w_start = parse_slug_timestamp(slug)
        boundary = boundaries.get(w_start)
        if not boundary:
            continue

        divergence = abs(boundary["divergence"])
        resolution = market.get("resolution") or market.get("direction_from_boundaries")
        if not resolution:
            continue

        token_map = market.get("token_map", {})
        token_to_dir = {tok: d for tok, d in token_map.items()}

        for trade in market["trades"]:
            wallet = trade["proxyWallet"]
            side = trade["side"]
            asset = trade["asset"]
            trade_dir = token_to_dir.get(asset)
            if not trade_dir:
                continue

            if side == "BUY":
                bet_direction = trade_dir
            else:
                bet_direction = "down" if trade_dir == "up" else "up"

            won = bet_direction == resolution
            ws = wallet_div[wallet]
            ws["pseudonym"] = trade.get("pseudonym", "")
            ws["trades"] += 1
            ws["wins"] += int(won)

            if divergence > 20:
                ws["high_div_trades"] += 1
                ws["high_div_wins"] += int(won)
            else:
                ws["low_div_trades"] += 1
                ws["low_div_wins"] += int(won)

    # Find wallets that perform better on high-divergence windows
    div_exploiters = []
    for wallet, s in wallet_div.items():
        if s["trades"] < 30 or s["high_div_trades"] < 5:
            continue

        wr_all = s["wins"] / s["trades"]
        wr_high = s["high_div_wins"] / s["high_div_trades"] if s["high_div_trades"] > 0 else 0
        wr_low = s["low_div_wins"] / s["low_div_trades"] if s["low_div_trades"] > 0 else 0

        # Do they outperform on high-divergence windows?
        if wr_high > wr_low + 0.05 and wr_high >= 0.65:
            div_exploiters.append({
                "wallet": wallet,
                "pseudonym": s["pseudonym"],
                "trades": s["trades"],
                "wr_all": wr_all,
                "t_high": s["high_div_trades"],
                "wr_high": wr_high,
                "t_low": s["low_div_trades"],
                "wr_low": wr_low,
                "wr_delta": wr_high - wr_low,
            })

    div_exploiters.sort(key=lambda x: x["wr_delta"], reverse=True)

    print(f"\n  Divergence exploiters (better WR on high-div windows): {len(div_exploiters)}")
    print(f"\n  {'─'*100}")
    print(f"  {'Pseudonym':<25} {'Trades':>7} {'WR_all':>7} {'T_hi':>5} {'WR_hi':>6} {'T_lo':>5} {'WR_lo':>6} {'Delta':>6}")
    print(f"  {'─'*100}")

    for c in div_exploiters[:20]:
        print(f"  {c['pseudonym'][:24]:<25} {c['trades']:>7} {c['wr_all']:>6.1%} {c['t_high']:>5} {c['wr_high']:>5.1%} {c['t_low']:>5} {c['wr_low']:>5.1%} {c['wr_delta']:>+5.1%}")

    return div_exploiters


# ─── Master Summary: Cross-Reference All Hypotheses ──────────────

def cross_reference(latency_candidates, sell_heavy, bn_corr, cp_sigs, mm_cands, div_exploit):
    """Find wallets that appear across multiple hypotheses — strongest arb signal."""
    print("\n" + "="*80)
    print("CROSS-REFERENCE: WALLETS APPEARING IN MULTIPLE HYPOTHESES")
    print("="*80)

    all_wallets = defaultdict(lambda: {
        "hypotheses": [],
        "pseudonym": "",
        "total_score": 0,
    })

    for c in latency_candidates:
        w = all_wallets[c["wallet"]]
        w["hypotheses"].append(f"H1:LatencyArb(score={c['arb_score']},wr={c['wr']:.0%},late={c['late_trades']})")
        w["pseudonym"] = c["pseudonym"]
        w["total_score"] += c["arb_score"]

    for c in sell_heavy:
        if c["sell_wr"] >= 0.60:
            w = all_wallets[c["wallet"]]
            w["hypotheses"].append(f"H2:SellCluster(sells={c['sells']},wr={c['sell_wr']:.0%})")
            w["pseudonym"] = c["pseudonym"]
            w["total_score"] += 2

    for c in bn_corr:
        w = all_wallets[c["wallet"]]
        w["hypotheses"].append(f"H3:BNCorr(score={c['score']},wr_lg={c['wr_large']:.0%})")
        w["pseudonym"] = c["pseudonym"]
        w["total_score"] += c["score"]

    for c in cp_sigs:
        w = all_wallets[c["wallet"]]
        w["hypotheses"].append(f"H4:CrossPlat(score={c['cp_score']},wr={c['wr']:.0%},mkts={c['markets']})")
        w["pseudonym"] = c["pseudonym"]
        w["total_score"] += c["cp_score"]

    for c in mm_cands:
        if c["both_pct"] >= 0.10:
            w = all_wallets[c["wallet"]]
            w["hypotheses"].append(f"H5:MM(both={c['both_pct']:.0%},vol=${c['buy_vol']+c['sell_vol']:,.0f})")
            w["pseudonym"] = c["pseudonym"]
            w["total_score"] += 2

    for c in div_exploit:
        if c["wr_delta"] >= 0.10:
            w = all_wallets[c["wallet"]]
            w["hypotheses"].append(f"H6:DivExploit(delta={c['wr_delta']:+.0%},wr_hi={c['wr_high']:.0%})")
            w["pseudonym"] = c["pseudonym"]
            w["total_score"] += 2

    # Sort by number of hypotheses matched, then total score
    multi_hypothesis = [(wallet, info) for wallet, info in all_wallets.items() if len(info["hypotheses"]) >= 2]
    multi_hypothesis.sort(key=lambda x: (len(x[1]["hypotheses"]), x[1]["total_score"]), reverse=True)

    print(f"\n  Wallets matching ≥2 hypotheses: {len(multi_hypothesis)}")

    for wallet, info in multi_hypothesis[:20]:
        print(f"\n  🔍 {info['pseudonym']} ({wallet[:10]}...)")
        print(f"     Score: {info['total_score']} | Hypotheses: {len(info['hypotheses'])}")
        for h in info["hypotheses"]:
            print(f"       → {h}")

    return multi_hypothesis


# ─── Main ─────────────────────────────────────────────────────────

def main():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  CROSS-PLATFORM ARBITRAGE DETECTION ANALYSIS               ║")
    print("║  Data: 2.18M trades, 26K wallets, 642 5-min BTC markets    ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    boundaries = load_boundaries()
    print(f"  Loaded {len(boundaries)} boundary records")

    whale_data = load_whale_cache()

    # Run all hypotheses
    wallet_stats, latency_candidates = analyze_latency_arb(whale_data, boundaries)
    sell_heavy = analyze_sell_cluster(whale_data, boundaries)
    bn_corr = analyze_binance_correlation(whale_data, boundaries)
    cp_sigs = analyze_cross_platform_signatures(whale_data, boundaries)
    mm_cands = analyze_market_makers(whale_data, boundaries)
    div_exploit = analyze_price_discrepancy(whale_data, boundaries)

    # Cross-reference
    multi = cross_reference(latency_candidates, sell_heavy, bn_corr, cp_sigs, mm_cands, div_exploit)

    # Summary statistics
    print("\n" + "="*80)
    print("FINAL SUMMARY")
    print("="*80)
    print(f"  H1 Latency Arb candidates:     {len(latency_candidates)}")
    print(f"  H2 Sell-cluster arbitrageurs:   {len([s for s in sell_heavy if s['sell_wr'] >= 0.60])}")
    print(f"  H3 Binance-correlated:          {len(bn_corr)}")
    print(f"  H4 Cross-platform signatures:   {len(cp_sigs)}")
    print(f"  H5 Market makers (two-sided):   {len(mm_cands)}")
    print(f"  H6 Divergence exploiters:       {len(div_exploit)}")
    print(f"  MULTI-HYPOTHESIS matches (≥2):  {len(multi)}")
    print()


if __name__ == "__main__":
    main()
