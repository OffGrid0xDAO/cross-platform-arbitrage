#!/usr/bin/env python3
"""
Arbitrage Wallet Tracing & Profiling
=====================================

Traces on-chain activity for the top 10 arbitrage suspect wallets identified
by detect_arbitrage.py. For each wallet, computes:
  - Total USDC volume, unique markets, average trade size
  - First/last trade timestamps
  - Transaction hashes (for Polygonscan lookup)
  - Entry timing distribution
  - Cross-wallet interaction detection (coordinated arb)
  - Shared timestamp / pseudonym pattern analysis

Output: analysis/arb_wallet_profiles.txt
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
WHALE_CACHE = os.path.join(DATA_DIR, "whale_cache.json")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "arb_wallet_profiles.txt")

# ─── Top 10 Suspect Wallets ──────────────────────────────────────────────────

SUSPECTS = [
    {
        "label": "#1",
        "address": "0x35c1d496413bc02fdce6ba5ab0e57800b5184e14",
        "pseudonym": "(no pseudonym)",
        "notes": "Score 18, all 6 hypotheses, 87% sell WR, 96% WR on large BN moves",
    },
    {
        "label": "#2",
        "address": "0xe20e72235e820b826fd0fef0b1b13bfab73c0c19",
        "pseudonym": "(no pseudonym)",
        "notes": "Score 13, all 6 hypotheses, 100% sell WR",
    },
    {
        "label": "#3",
        "address": "0xb6893804807f3cc6cc607e53b6dca8be5f8f84f9",
        "pseudonym": "Numb-Biopsy",
        "notes": "Score 19, 5 hypotheses, 94% WR large moves",
    },
    {
        "label": "#4",
        "address": "0x4b52448e9020f4b41cdd2c8384f8175ea47f29da",
        "pseudonym": "Meek-Plum",
        "notes": "Score 18, 5 hyp, 96% WR large moves",
    },
    {
        "label": "#5",
        "address": "0x0d4a2e5e9cb2c6ac42192cd63b4b9b1e88e88470",
        "pseudonym": "Whirlwind-Witchhunt",
        "notes": "Score 18, 5 hyp, 100% WR large moves",
    },
    {
        "label": "#6",
        "address": "0x2ca0b438daf84b00d9ce2ced735bd400f63b08d0",
        "pseudonym": "Plaintive-Forgery",
        "notes": "304 trades, 99% WR, 100% in last 15s",
    },
    {
        "label": "#7",
        "address": "0x894f4562628cd6b60e16cf6d4770e528ccd09ebe",
        "pseudonym": "Nippy-Styling",
        "notes": "141 trades, 98.6% WR, trades at second 299",
    },
    {
        "label": "#8",
        "address": "0xe0332306753f8c3188a2cb82b884c5fc75e8047c",
        "pseudonym": "Ambitious-Birch",
        "notes": "132 trades, 100% WR, trades at second 293",
    },
    {
        "label": "#9",
        "address": "0x92240c88a1c6c9afadca6973608e5d43ad621d3d",
        "pseudonym": "Impeccable-Venison",
        "notes": "1571 trades, 96.5% WR in last 15s",
    },
    {
        "label": "#10",
        "address": "0xb1a99c1fd9d53d45b7587a663b4e6a2d4ed543ba",
        "pseudonym": "Capital-Potty",
        "notes": "26816 trades, 79.2% sell WR",
    },
]

SUSPECT_ADDRESSES = set(s["address"] for s in SUSPECTS)

# Timing buckets for entry distribution
TIMING_BUCKETS = [
    (0, 60, "0-60s"),
    (60, 120, "60-120s"),
    (120, 180, "120-180s"),
    (180, 240, "180-240s"),
    (240, 270, "240-270s"),
    (270, 285, "270-285s"),
    (285, 300, "285-300s"),
]


def parse_slug_timestamp(slug):
    """Extract window_start unix timestamp from slug like 'btc-updown-5m-1771588500'."""
    parts = slug.split("-")
    return int(parts[-1])


def ts_to_str(ts):
    """Convert unix timestamp to human-readable UTC string."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def load_whale_cache():
    print("[*] Loading whale_cache.json ...")
    with open(WHALE_CACHE) as f:
        data = json.load(f)
    print(f"    Loaded {len(data)} markets")
    return data


# ─── Step 1-3: Build per-wallet profiles ─────────────────────────────────────

def build_wallet_profiles(whale_data):
    """For each suspect wallet, extract comprehensive trade profile."""

    profiles = {}
    for s in SUSPECTS:
        profiles[s["address"]] = {
            "label": s["label"],
            "address": s["address"],
            "pseudonym": s["pseudonym"],
            "notes": s["notes"],
            "total_buy_volume": 0.0,
            "total_sell_volume": 0.0,
            "total_volume": 0.0,
            "trade_count": 0,
            "buy_count": 0,
            "sell_count": 0,
            "markets_traded": set(),
            "sizes": [],
            "timestamps": [],
            "entry_seconds": [],
            "first_trade_ts": None,
            "last_trade_ts": None,
            "tx_hashes": [],
            "tx_hash_set": set(),
            "timing_buckets": {b[2]: 0 for b in TIMING_BUCKETS},
            # For cross-wallet analysis
            "trades_by_market": defaultdict(list),  # slug -> list of trade dicts
        }

    for slug, market in whale_data.items():
        w_start = parse_slug_timestamp(slug)

        for trade in market["trades"]:
            wallet = trade["proxyWallet"]
            if wallet not in profiles:
                continue

            p = profiles[wallet]
            ts = trade["timestamp"]
            side = trade["side"]
            size = trade["size"]
            price = trade["price"]
            volume = size * price
            tx_hash = trade.get("transactionHash", "")
            secs_into_window = ts - w_start

            # Update pseudonym if we find one (some have empty pseudonym in our list)
            if trade.get("pseudonym") and p["pseudonym"] == "(no pseudonym)":
                p["pseudonym"] = trade["pseudonym"]

            p["trade_count"] += 1
            p["total_volume"] += volume
            p["sizes"].append(size)
            p["timestamps"].append(ts)
            p["entry_seconds"].append(secs_into_window)
            p["markets_traded"].add(slug)

            if side == "BUY":
                p["buy_count"] += 1
                p["total_buy_volume"] += volume
            else:
                p["sell_count"] += 1
                p["total_sell_volume"] += volume

            if p["first_trade_ts"] is None or ts < p["first_trade_ts"]:
                p["first_trade_ts"] = ts
            if p["last_trade_ts"] is None or ts > p["last_trade_ts"]:
                p["last_trade_ts"] = ts

            if tx_hash and tx_hash not in p["tx_hash_set"]:
                p["tx_hashes"].append(tx_hash)
                p["tx_hash_set"].add(tx_hash)

            # Timing bucket
            for lo, hi, label in TIMING_BUCKETS:
                if lo <= secs_into_window < hi:
                    p["timing_buckets"][label] += 1
                    break
            else:
                # Overflow (>=300 or <0) — edge case
                if secs_into_window >= 285:
                    p["timing_buckets"]["285-300s"] += 1

            # Store for cross-wallet analysis
            p["trades_by_market"][slug].append({
                "timestamp": ts,
                "side": side,
                "asset": trade["asset"],
                "size": size,
                "price": price,
                "secs": secs_into_window,
                "tx_hash": tx_hash,
            })

    return profiles


# ─── Step 4: Cross-wallet interaction detection ──────────────────────────────

def detect_cross_wallet_interactions(profiles):
    """
    Check if any suspect wallets interact with EACH OTHER in the same market,
    on opposite sides, within 10 seconds. This would indicate coordinated arb.
    """
    interactions = []

    address_list = list(profiles.keys())

    for i in range(len(address_list)):
        for j in range(i + 1, len(address_list)):
            w1 = address_list[i]
            w2 = address_list[j]
            p1 = profiles[w1]
            p2 = profiles[w2]

            # Find common markets
            common_markets = p1["markets_traded"] & p2["markets_traded"]

            for slug in common_markets:
                trades1 = p1["trades_by_market"][slug]
                trades2 = p2["trades_by_market"][slug]

                for t1 in trades1:
                    for t2 in trades2:
                        time_diff = abs(t1["timestamp"] - t2["timestamp"])
                        if time_diff <= 10:
                            # Check opposite sides
                            opposite = (t1["side"] != t2["side"]) or (t1["asset"] != t2["asset"])
                            interactions.append({
                                "wallet1": w1,
                                "wallet1_pseudo": p1["pseudonym"],
                                "wallet2": w2,
                                "wallet2_pseudo": p2["pseudonym"],
                                "market": slug,
                                "time_diff": time_diff,
                                "t1_side": t1["side"],
                                "t2_side": t2["side"],
                                "t1_asset_suffix": t1["asset"][-8:],
                                "t2_asset_suffix": t2["asset"][-8:],
                                "t1_ts": t1["timestamp"],
                                "t2_ts": t2["timestamp"],
                                "t1_size": t1["size"],
                                "t2_size": t2["size"],
                                "opposite_sides": opposite,
                            })

    return interactions


# ─── Step 5: Shared pseudonym/timestamp patterns ─────────────────────────────

def detect_shared_patterns(profiles):
    """
    Check for:
    1. Similar pseudonym patterns (shared words/affixes)
    2. Identical timestamps across different wallets & markets
    """
    results = {"pseudonym_clusters": [], "timestamp_overlaps": []}

    # Pseudonym analysis
    pseudonyms = {}
    for addr, p in profiles.items():
        ps = p["pseudonym"]
        if ps and ps != "(no pseudonym)":
            pseudonyms[addr] = ps

    # Check for shared words in pseudonym pairs
    for a1, ps1 in pseudonyms.items():
        words1 = set(ps1.lower().replace("-", " ").split())
        for a2, ps2 in pseudonyms.items():
            if a1 >= a2:
                continue
            words2 = set(ps2.lower().replace("-", " ").split())
            shared = words1 & words2
            if shared:
                results["pseudonym_clusters"].append({
                    "wallet1": a1,
                    "pseudo1": ps1,
                    "wallet2": a2,
                    "pseudo2": ps2,
                    "shared_words": shared,
                })

    # Check for pseudonym structural patterns (both Adjective-Noun format, etc.)
    # Polymarket auto-generates "Adjective-Noun" pseudonyms; no info gained here
    # but look for identical first or last parts
    for a1, ps1 in pseudonyms.items():
        parts1 = ps1.split("-")
        if len(parts1) != 2:
            continue
        for a2, ps2 in pseudonyms.items():
            if a1 >= a2:
                continue
            parts2 = ps2.split("-")
            if len(parts2) != 2:
                continue
            if parts1[0] == parts2[0] or parts1[1] == parts2[1]:
                results["pseudonym_clusters"].append({
                    "wallet1": a1,
                    "pseudo1": ps1,
                    "wallet2": a2,
                    "pseudo2": ps2,
                    "shared_part": parts1[0] if parts1[0] == parts2[0] else parts1[1],
                })

    # Timestamp overlap analysis: find exact same second trades across wallets
    ts_map = defaultdict(list)  # timestamp -> [(wallet, market)]
    for addr, p in profiles.items():
        for slug, trades in p["trades_by_market"].items():
            for t in trades:
                ts_map[t["timestamp"]].append({
                    "wallet": addr,
                    "pseudo": p["pseudonym"],
                    "market": slug,
                    "side": t["side"],
                    "size": t["size"],
                })

    for ts, entries in ts_map.items():
        wallets_at_ts = set(e["wallet"] for e in entries)
        if len(wallets_at_ts) >= 2:
            results["timestamp_overlaps"].append({
                "timestamp": ts,
                "ts_human": ts_to_str(ts),
                "entries": entries,
            })

    return results


# ─── Output Report ────────────────────────────────────────────────────────────

def generate_report(profiles, interactions, patterns):
    """Build the full text report."""
    lines = []

    def w(text=""):
        lines.append(text)

    w("=" * 100)
    w("  ARBITRAGE WALLET TRACING & ON-CHAIN ACTIVITY REPORT")
    w("  Generated: " + datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
    w("=" * 100)
    w()

    # ─── Section 1: Individual Wallet Profiles ────────────────────────────
    w("=" * 100)
    w("  SECTION 1: INDIVIDUAL WALLET PROFILES")
    w("=" * 100)

    for s in SUSPECTS:
        addr = s["address"]
        p = profiles[addr]

        w()
        w("-" * 100)
        w(f"  SUSPECT {p['label']}: {p['pseudonym']}")
        w(f"  Wallet: {p['address']}")
        w(f"  Detection: {p['notes']}")
        w("-" * 100)

        if p["trade_count"] == 0:
            w("  *** No trades found for this wallet ***")
            continue

        avg_size = sum(p["sizes"]) / len(p["sizes"]) if p["sizes"] else 0
        median_size = sorted(p["sizes"])[len(p["sizes"]) // 2] if p["sizes"] else 0

        w(f"  Total Trades:        {p['trade_count']:,}")
        w(f"  Buy Trades:          {p['buy_count']:,} ({p['buy_count']/p['trade_count']:.1%})")
        w(f"  Sell Trades:         {p['sell_count']:,} ({p['sell_count']/p['trade_count']:.1%})")
        w(f"  Total USDC Volume:   ${p['total_volume']:,.2f}")
        w(f"    Buy Volume:        ${p['total_buy_volume']:,.2f}")
        w(f"    Sell Volume:       ${p['total_sell_volume']:,.2f}")
        w(f"  Unique Markets:      {len(p['markets_traded']):,}")
        w(f"  Average Trade Size:  {avg_size:,.2f} shares")
        w(f"  Median Trade Size:   {median_size:,.2f} shares")
        w(f"  First Trade:         {ts_to_str(p['first_trade_ts'])} (unix: {p['first_trade_ts']})")
        w(f"  Last Trade:          {ts_to_str(p['last_trade_ts'])} (unix: {p['last_trade_ts']})")

        # Active period
        if p["first_trade_ts"] and p["last_trade_ts"]:
            span_hours = (p["last_trade_ts"] - p["first_trade_ts"]) / 3600
            span_days = span_hours / 24
            w(f"  Active Period:       {span_days:.1f} days ({span_hours:.0f} hours)")
            if span_hours > 0:
                w(f"  Trade Frequency:     {p['trade_count'] / max(span_hours, 1):.1f} trades/hour")

        # Entry timing distribution
        w()
        w("  Entry Timing Distribution:")
        max_bucket = max(p["timing_buckets"].values()) if any(p["timing_buckets"].values()) else 1
        for bucket_label in [b[2] for b in TIMING_BUCKETS]:
            count = p["timing_buckets"][bucket_label]
            pct = count / p["trade_count"] if p["trade_count"] > 0 else 0
            bar_len = int(40 * count / max_bucket) if max_bucket > 0 else 0
            bar = "#" * bar_len
            w(f"    {bucket_label:>8s}: {count:>6,} ({pct:>5.1%}) |{bar}")

        # Timing stats
        if p["entry_seconds"]:
            avg_entry = sum(p["entry_seconds"]) / len(p["entry_seconds"])
            sorted_entries = sorted(p["entry_seconds"])
            p10 = sorted_entries[int(len(sorted_entries) * 0.10)]
            p50 = sorted_entries[int(len(sorted_entries) * 0.50)]
            p90 = sorted_entries[int(len(sorted_entries) * 0.90)]
            min_entry = sorted_entries[0]
            max_entry = sorted_entries[-1]
            w(f"    Average entry:   {avg_entry:.1f}s into window")
            w(f"    Min/P10/P50/P90/Max: {min_entry:.0f}s / {p10:.0f}s / {p50:.0f}s / {p90:.0f}s / {max_entry:.0f}s")

        # Transaction hashes
        w()
        w(f"  Transaction Hashes ({len(p['tx_hashes'])} unique):")
        w(f"  Polygonscan URL pattern: https://polygonscan.com/tx/{{hash}}")
        # Show first 15 and last 5 if many
        if len(p["tx_hashes"]) <= 25:
            for tx in p["tx_hashes"]:
                w(f"    {tx}")
        else:
            w(f"    First 15:")
            for tx in p["tx_hashes"][:15]:
                w(f"      {tx}")
            w(f"    ... ({len(p['tx_hashes']) - 20} more) ...")
            w(f"    Last 5:")
            for tx in p["tx_hashes"][-5:]:
                w(f"      {tx}")

        w()

    # ─── Section 2: Cross-Wallet Interactions ─────────────────────────────
    w()
    w("=" * 100)
    w("  SECTION 2: CROSS-WALLET INTERACTIONS (same market, within 10 seconds)")
    w("=" * 100)
    w()

    if not interactions:
        w("  No cross-wallet interactions detected within 10-second window.")
    else:
        # Summarize by wallet pair
        pair_counts = defaultdict(lambda: {"count": 0, "opposite": 0, "same": 0, "markets": set()})
        for ix in interactions:
            key = tuple(sorted([ix["wallet1"], ix["wallet2"]]))
            pair_counts[key]["count"] += 1
            pair_counts[key]["markets"].add(ix["market"])
            if ix["opposite_sides"]:
                pair_counts[key]["opposite"] += 1
            else:
                pair_counts[key]["same"] += 1

        w(f"  Total interactions found: {len(interactions)}")
        w(f"  Unique wallet pairs with interactions: {len(pair_counts)}")
        w()

        # Sort by count descending
        sorted_pairs = sorted(pair_counts.items(), key=lambda x: x[1]["count"], reverse=True)

        for (w1, w2), info in sorted_pairs[:30]:
            p1 = profiles[w1]["pseudonym"]
            p2 = profiles[w2]["pseudonym"]
            w(f"  {p1} <-> {p2}")
            w(f"    Interactions: {info['count']} (opposite sides: {info['opposite']}, same side: {info['same']})")
            w(f"    Shared markets: {len(info['markets'])}")
            w()

        # Show detailed examples of opposite-side interactions (strongest arb signal)
        opposite_ixs = [ix for ix in interactions if ix["opposite_sides"]]
        if opposite_ixs:
            w(f"  OPPOSITE-SIDE INTERACTIONS (strongest coordinated arb signal):")
            w(f"  Found {len(opposite_ixs)} opposite-side interactions")
            w()
            for ix in opposite_ixs[:20]:
                w(f"    Market: {ix['market']}")
                w(f"      {ix['wallet1_pseudo']:>25s} ({ix['wallet1'][:10]}...) {ix['t1_side']:>4s} {ix['t1_size']:>8.2f} @ t={ix['t1_ts']}")
                w(f"      {ix['wallet2_pseudo']:>25s} ({ix['wallet2'][:10]}...) {ix['t2_side']:>4s} {ix['t2_size']:>8.2f} @ t={ix['t2_ts']}")
                w(f"      Time diff: {ix['time_diff']}s | Assets differ: {ix['t1_asset_suffix'] != ix['t2_asset_suffix']}")
                w()

    # ─── Section 3: Shared Patterns ───────────────────────────────────────
    w()
    w("=" * 100)
    w("  SECTION 3: SHARED PSEUDONYM & TIMESTAMP PATTERNS")
    w("=" * 100)
    w()

    # Pseudonym clusters
    if patterns["pseudonym_clusters"]:
        w("  Pseudonym Similarities:")
        seen = set()
        for pc in patterns["pseudonym_clusters"]:
            key = tuple(sorted([pc["wallet1"], pc["wallet2"]]))
            detail = pc.get("shared_words") or pc.get("shared_part", "")
            sig = (key, str(detail))
            if sig in seen:
                continue
            seen.add(sig)
            w(f"    {pc['pseudo1']} <-> {pc['pseudo2']}")
            if "shared_words" in pc:
                w(f"      Shared words: {pc['shared_words']}")
            if "shared_part" in pc:
                w(f"      Shared part: {pc['shared_part']}")
        w()
    else:
        w("  No pseudonym similarities detected.")
        w()

    # Timestamp overlaps
    ts_overlaps = patterns["timestamp_overlaps"]
    if ts_overlaps:
        w(f"  Exact-Second Timestamp Overlaps: {len(ts_overlaps)} instances")
        w(f"  (Multiple suspect wallets trading at the exact same second)")
        w()

        # Group by wallet pairs
        pair_ts_counts = defaultdict(int)
        for ov in ts_overlaps:
            wallets_in = sorted(set(e["wallet"] for e in ov["entries"]))
            for i in range(len(wallets_in)):
                for j in range(i + 1, len(wallets_in)):
                    pair_ts_counts[(wallets_in[i], wallets_in[j])] += 1

        w("  Wallet Pair Timestamp Overlap Counts:")
        for (w1, w2), count in sorted(pair_ts_counts.items(), key=lambda x: x[1], reverse=True)[:20]:
            p1 = profiles[w1]["pseudonym"]
            p2 = profiles[w2]["pseudonym"]
            w(f"    {p1:>25s} <-> {p2:<25s}: {count} shared timestamps")
        w()

        # Show some examples
        w("  Sample timestamp overlaps (first 15):")
        for ov in ts_overlaps[:15]:
            w(f"    {ov['ts_human']} (unix {ov['timestamp']}):")
            for e in ov["entries"]:
                w(f"      {e['pseudo']:>25s} ({e['wallet'][:10]}...) {e['side']:>4s} {e['size']:>8.2f} in {e['market']}")
            w()
    else:
        w("  No exact-second timestamp overlaps detected among suspect wallets.")
        w()

    # ─── Section 4: Summary Table ─────────────────────────────────────────
    w()
    w("=" * 100)
    w("  SECTION 4: SUMMARY TABLE")
    w("=" * 100)
    w()

    header = f"  {'#':>3s} {'Pseudonym':<25s} {'Trades':>8s} {'Volume':>14s} {'AvgSize':>10s} {'Markets':>8s} {'Buy%':>6s} {'LateTrade%':>10s} {'ActiveDays':>10s}"
    w(header)
    w("  " + "-" * (len(header) - 2))

    for s in SUSPECTS:
        p = profiles[s["address"]]
        if p["trade_count"] == 0:
            w(f"  {s['label']:>3s} {p['pseudonym']:<25s} {'N/A':>8s}")
            continue

        buy_pct = p["buy_count"] / p["trade_count"]
        avg_sz = sum(p["sizes"]) / len(p["sizes"])
        late_trades = p["timing_buckets"]["270-285s"] + p["timing_buckets"]["285-300s"]
        late_pct = late_trades / p["trade_count"]
        span_days = (p["last_trade_ts"] - p["first_trade_ts"]) / 86400 if p["first_trade_ts"] and p["last_trade_ts"] else 0

        w(f"  {s['label']:>3s} {p['pseudonym']:<25s} {p['trade_count']:>8,} ${p['total_volume']:>12,.0f} {avg_sz:>10.1f} {len(p['markets_traded']):>8,} {buy_pct:>5.0%} {late_pct:>9.1%} {span_days:>10.1f}")

    w()

    # ─── Section 5: Transaction Hash Index ────────────────────────────────
    w()
    w("=" * 100)
    w("  SECTION 5: TRANSACTION HASH INDEX FOR POLYGONSCAN TRACING")
    w("=" * 100)
    w()
    w("  Use these hashes to trace USDC flows to/from other platforms on Polygonscan.")
    w("  Look for: USDC transfers from/to exchange hot wallets, bridge contracts, or other arb wallets.")
    w()

    for s in SUSPECTS:
        p = profiles[s["address"]]
        if not p["tx_hashes"]:
            continue
        w(f"  {p['label']} {p['pseudonym']} ({p['address']}):")
        w(f"    Total unique tx hashes: {len(p['tx_hashes'])}")
        # First 5 for quick lookup
        for tx in p["tx_hashes"][:5]:
            w(f"    https://polygonscan.com/tx/{tx}")
        if len(p["tx_hashes"]) > 5:
            w(f"    ... and {len(p['tx_hashes']) - 5} more")
        w()

    # ─── Section 6: Coordinated Arb Assessment ────────────────────────────
    w()
    w("=" * 100)
    w("  SECTION 6: COORDINATED ARBITRAGE ASSESSMENT")
    w("=" * 100)
    w()

    # Analyze which wallets might be controlled by the same entity
    w("  Indicators of same-entity control:")
    w()

    # Check for wallets with very similar timing profiles
    w("  A) Timing Profile Similarity (average entry second into window):")
    timing_profiles = []
    for s in SUSPECTS:
        p = profiles[s["address"]]
        if p["entry_seconds"]:
            avg = sum(p["entry_seconds"]) / len(p["entry_seconds"])
            timing_profiles.append((s["address"], p["pseudonym"], avg, p["trade_count"]))

    timing_profiles.sort(key=lambda x: x[2])
    for addr, pseudo, avg_t, tc in timing_profiles:
        w(f"    {pseudo:<25s}: avg entry = {avg_t:>6.1f}s ({tc:>6,} trades)")

    # Check for wallets active in same time range
    w()
    w("  B) Overlapping Active Periods:")
    for i in range(len(SUSPECTS)):
        for j in range(i + 1, len(SUSPECTS)):
            p1 = profiles[SUSPECTS[i]["address"]]
            p2 = profiles[SUSPECTS[j]["address"]]
            if not p1["first_trade_ts"] or not p2["first_trade_ts"]:
                continue
            # Calculate overlap
            start = max(p1["first_trade_ts"], p2["first_trade_ts"])
            end = min(p1["last_trade_ts"], p2["last_trade_ts"])
            if end > start:
                overlap_hours = (end - start) / 3600
                span1 = (p1["last_trade_ts"] - p1["first_trade_ts"]) / 3600
                span2 = (p2["last_trade_ts"] - p2["first_trade_ts"]) / 3600
                min_span = min(span1, span2) if min(span1, span2) > 0 else 1
                overlap_pct = overlap_hours / min_span
                if overlap_pct > 0.5:
                    w(f"    {p1['pseudonym']:<25s} <-> {p2['pseudonym']:<25s}: {overlap_hours:.0f}h overlap ({overlap_pct:.0%} of shorter span)")

    # Check for wallets that share markets
    w()
    w("  C) Shared Market Counts:")
    for i in range(len(SUSPECTS)):
        for j in range(i + 1, len(SUSPECTS)):
            p1 = profiles[SUSPECTS[i]["address"]]
            p2 = profiles[SUSPECTS[j]["address"]]
            shared = p1["markets_traded"] & p2["markets_traded"]
            if len(shared) >= 5:
                total_markets = len(p1["markets_traded"] | p2["markets_traded"])
                w(f"    {p1['pseudonym']:<25s} <-> {p2['pseudonym']:<25s}: {len(shared):>4} shared markets (of {total_markets} combined)")

    w()
    w("=" * 100)
    w("  END OF REPORT")
    w("=" * 100)

    return "\n".join(lines)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 80)
    print("  ARBITRAGE WALLET TRACING")
    print("  Top 10 suspect wallets from detect_arbitrage.py")
    print("=" * 80)
    print()

    whale_data = load_whale_cache()

    print("[*] Building wallet profiles ...")
    profiles = build_wallet_profiles(whale_data)

    # Quick summary to console
    for s in SUSPECTS:
        p = profiles[s["address"]]
        print(f"  {s['label']:>3s} {p['pseudonym']:<25s}: {p['trade_count']:>6,} trades, ${p['total_volume']:>12,.0f} vol, {len(p['markets_traded']):>4} mkts, {len(p['tx_hashes']):>5} txs")

    print()
    print("[*] Detecting cross-wallet interactions (same market, <=10s apart) ...")
    interactions = detect_cross_wallet_interactions(profiles)
    print(f"    Found {len(interactions)} interactions")

    # Count opposite-side interactions
    opposite = [ix for ix in interactions if ix["opposite_sides"]]
    print(f"    Opposite-side interactions: {len(opposite)}")

    print()
    print("[*] Analyzing shared patterns (pseudonyms, timestamps) ...")
    patterns = detect_shared_patterns(profiles)
    print(f"    Pseudonym clusters: {len(patterns['pseudonym_clusters'])}")
    print(f"    Timestamp overlaps: {len(patterns['timestamp_overlaps'])}")

    print()
    print("[*] Generating report ...")
    report = generate_report(profiles, interactions, patterns)

    # Write report
    with open(OUTPUT_FILE, "w") as f:
        f.write(report)
    print(f"    Saved to: {OUTPUT_FILE}")

    # Also print report to stdout
    print()
    print(report)


if __name__ == "__main__":
    main()
