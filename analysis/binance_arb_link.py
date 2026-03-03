#!/usr/bin/env python3
"""
Binance ↔ Polymarket Arbitrage Link Detection
==============================================
1. Find top Polymarket traders whose timing correlates with Binance BTC moves
2. Check those wallets for Binance hot wallet USDC flows (on-chain proof)
3. Detect latency arb: wallets that trade AFTER BTC direction is known
4. Cross-ref with Kalshi trade timing for 3-platform arb detection
"""

import json
import csv
import os
import sys
import time
import requests
from collections import defaultdict
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
WHALE_CACHE = os.path.join(DATA_DIR, "whale_cache.json")
BOUNDARIES = os.path.join(DATA_DIR, "boundaries.csv")
KALSHI_TRADES = os.path.join(DATA_DIR, "kalshi_btc_trades.json")
WHALE_ANALYSIS = os.path.join(DATA_DIR, "whale_analysis.json")

ETHERSCAN_KEY = os.getenv("ETHERSCAN_API_KEY", "WIGMWJF6D5GA6BSDMAP15F3HS37YGYMSME")

# Known Binance hot wallets on Polygon
BINANCE_WALLETS = {
    "0xe7804c37c13166ff0b37f5ae0bb07a3aebb6e245": "Binance Hot Wallet (Polygon)",
    "0xf977814e90da44bfa03b6295a0616a897441acec": "Binance 8",
    "0x28c6c06298d514db089934071355e5743bf21d60": "Binance 14",
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549": "Binance 15",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "Binance 16",
    "0x56eddb7aa87536c09ccc2793473599fd21a8b17f": "Binance 17",
    "0x9696f59e4d72e237be84ffd425dcad154bf96976": "Binance 18",
    "0x4976a4a02f38326660d17bf34b431dc6e2eb2327": "Binance 19",
    "0xd551234ae421e3bcba99a0da6d736074f22192ff": "Binance 20",
    "0x835678a611b28684005a5e2233695fb6cbbb0007": "Binance Deposit",
}

# Known exchange/bridge addresses on Polygon
EXCHANGES = {
    **BINANCE_WALLETS,
    "0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23": "Coinbase",
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "OKX",
    "0xf89d7b9c864f589bbf53a82105107622b35eaa40": "Bybit",
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "OKX Hot Wallet 2",
}

USDC_POLYGON = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"  # USDC.e
USDC_NATIVE = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"

# ─── Load Data ─────────────────────────────────────────────────────

def load_boundaries():
    boundaries = {}
    with open(BOUNDARIES) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dt = datetime.fromisoformat(row["window_start_utc"])
                ts = int(dt.timestamp())
                bn_s = float(row.get("binance_start") or 0)
                bn_e = float(row.get("binance_end") or 0)
                if not bn_s or not bn_e:
                    continue
                boundaries[ts] = {
                    "bn_start": bn_s,
                    "bn_end": bn_e,
                    "bn_move_pct": (bn_e - bn_s) / bn_s * 100,
                    "bn_momentum": float(row.get("binance_momentum_60s") or 0),
                    "direction": row["actual_direction"],
                }
            except (ValueError, KeyError):
                continue
    return boundaries


def load_whale_cache():
    print("[*] Loading whale_cache.json (~1.6GB)...")
    with open(WHALE_CACHE) as f:
        data = json.load(f)
    print(f"    {len(data)} markets loaded")
    return data


def load_kalshi_trades():
    if not os.path.exists(KALSHI_TRADES):
        print("[!] kalshi_btc_trades.json not found, skipping Kalshi analysis")
        return None
    print("[*] Loading kalshi_btc_trades.json...")
    with open(KALSHI_TRADES) as f:
        data = json.load(f)
    print(f"    {len(data)} Kalshi markets loaded")
    return data


def load_whale_analysis():
    with open(WHALE_ANALYSIS) as f:
        return json.load(f)


def parse_slug_ts(slug):
    parts = slug.split("-")
    return int(parts[-1])


# ─── Analysis 1: Latency Arb Detection ────────────────────────────

def detect_latency_arb(whale_cache, boundaries):
    """Find wallets that trade AFTER BTC direction is already determined on Binance.

    Key signal: trades in last 60s of window (BTC direction clear) with >70% WR
    AND trades that align with Binance momentum direction.
    """
    print("\n" + "="*80)
    print("  ANALYSIS 1: LATENCY ARBITRAGE (Binance → Polymarket)")
    print("="*80)

    # For each wallet, track: trades in last 60s, alignment with BN direction
    wallet_stats = defaultdict(lambda: {
        "total": 0, "wins": 0, "late_trades": 0, "late_wins": 0,
        "aligned_trades": 0, "aligned_wins": 0,
        "volume": 0.0, "late_volume": 0.0,
        "pnl": 0.0, "late_pnl": 0.0,
        "big_move_trades": 0, "big_move_wins": 0,
        "entry_times": [],
    })

    for slug, market in whale_cache.items():
        window_ts = parse_slug_ts(slug)
        bd = boundaries.get(window_ts)
        if not bd:
            continue

        actual_dir = bd["direction"]
        bn_move = bd["bn_move_pct"]
        big_move = abs(bn_move) > 0.05  # >0.05% BTC move

        token_map = market.get("token_map", {})

        for trade in market.get("trades", []):
            wallet = trade.get("proxyWallet", "").lower()
            if not wallet:
                continue

            side = trade.get("side", "").upper()
            asset = str(trade.get("asset", ""))
            size = float(trade.get("size", 0))
            price = float(trade.get("price", 0))
            ts = trade.get("timestamp", 0)

            if isinstance(ts, str):
                try:
                    ts = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
                except:
                    ts = 0
            ts = int(ts)

            if not ts or not size:
                continue

            # Determine bet direction from token_map
            token_dir = token_map.get(asset, "").lower()
            if not token_dir:
                for tid, d in token_map.items():
                    if str(tid) == asset:
                        token_dir = d.lower()
                        break
            if not token_dir:
                continue

            bet_dir = token_dir if side == "BUY" else ("down" if token_dir == "up" else "up")

            entry_offset = ts - window_ts
            is_win = (bet_dir == actual_dir)

            usdc_val = size * price
            pnl = size * (1 - price) if is_win else -usdc_val

            ws = wallet_stats[wallet]
            ws["total"] += 1
            ws["volume"] += usdc_val
            ws["pnl"] += pnl
            ws["entry_times"].append(entry_offset)
            if is_win:
                ws["wins"] += 1

            # Late trades (last 60s of window = entry_offset > 240s)
            if entry_offset > 240:
                ws["late_trades"] += 1
                ws["late_volume"] += usdc_val
                ws["late_pnl"] += pnl
                if is_win:
                    ws["late_wins"] += 1

            # Aligned with Binance direction
            if (bn_move > 0 and bet_dir == "up") or (bn_move < 0 and bet_dir == "down"):
                ws["aligned_trades"] += 1
                if is_win:
                    ws["aligned_wins"] += 1

            # Big BTC moves
            if big_move:
                ws["big_move_trades"] += 1
                if is_win:
                    ws["big_move_wins"] += 1

    # Filter: wallets with significant activity
    arb_suspects = []
    for wallet, s in wallet_stats.items():
        if s["total"] < 50:
            continue

        wr = s["wins"] / s["total"] if s["total"] > 0 else 0
        late_wr = s["late_wins"] / s["late_trades"] if s["late_trades"] > 5 else 0
        aligned_wr = s["aligned_wins"] / s["aligned_trades"] if s["aligned_trades"] > 10 else 0
        big_move_wr = s["big_move_wins"] / s["big_move_trades"] if s["big_move_trades"] > 10 else 0

        # Arb score: high late WR + high aligned WR + profitable
        arb_score = 0
        if late_wr > 0.65 and s["late_trades"] > 10:
            arb_score += 3
        if aligned_wr > 0.60 and s["aligned_trades"] > 20:
            arb_score += 2
        if big_move_wr > 0.65 and s["big_move_trades"] > 10:
            arb_score += 2
        if wr > 0.55 and s["volume"] > 500:
            arb_score += 1
        if s["pnl"] > 1000:
            arb_score += 1
        if s["late_pnl"] > 200:
            arb_score += 1

        if arb_score >= 3 or (s["pnl"] > 5000 and wr > 0.55):
            arb_suspects.append({
                "wallet": wallet,
                "trades": s["total"],
                "wins": s["wins"],
                "wr": round(wr, 4),
                "volume": round(s["volume"], 2),
                "pnl": round(s["pnl"], 2),
                "late_trades": s["late_trades"],
                "late_wr": round(late_wr, 4),
                "late_pnl": round(s["late_pnl"], 2),
                "aligned_trades": s["aligned_trades"],
                "aligned_wr": round(aligned_wr, 4),
                "big_move_trades": s["big_move_trades"],
                "big_move_wr": round(big_move_wr, 4),
                "avg_entry": round(sum(s["entry_times"]) / len(s["entry_times"]), 1) if s["entry_times"] else 0,
                "arb_score": arb_score,
            })

    arb_suspects.sort(key=lambda x: x["pnl"], reverse=True)

    print(f"\n  Found {len(arb_suspects)} wallets with arb signals (score >= 3 or PnL > $5K)")
    print(f"  {'Wallet':<44} {'Trades':>6} {'WR':>6} {'PnL':>10} {'Late WR':>8} {'Align WR':>9} {'BigMv WR':>9} {'AvgEntry':>8} {'Score':>5}")
    print("  " + "-"*105)

    for s in arb_suspects[:30]:
        print(f"  {s['wallet']:<44} {s['trades']:>6} {s['wr']:>6.1%} ${s['pnl']:>9,.0f} {s['late_wr']:>8.1%} {s['aligned_wr']:>9.1%} {s['big_move_wr']:>9.1%} {s['avg_entry']:>7.0f}s {s['arb_score']:>5}")

    return arb_suspects


# ─── Analysis 2: Binance On-Chain Flow Check ──────────────────────

def check_binance_flows(wallets_to_check, max_wallets=20):
    """Check if top arb suspects have on-chain USDC flows to/from Binance hot wallets."""
    print("\n" + "="*80)
    print("  ANALYSIS 2: BINANCE ON-CHAIN FLOW CHECK")
    print("="*80)

    binance_set = set(k.lower() for k in BINANCE_WALLETS.keys())
    exchange_set = set(k.lower() for k in EXCHANGES.keys())

    results = []
    checked = 0

    for w in wallets_to_check[:max_wallets]:
        wallet = w["wallet"]
        checked += 1
        print(f"\n  [{checked}/{min(max_wallets, len(wallets_to_check))}] Checking {wallet[:10]}... (PnL: ${w['pnl']:,.0f})")

        # Query Polygon token transfers
        url = (f"https://api.etherscan.io/v2/api?chainid=137&module=account"
               f"&action=tokentx&address={wallet}"
               f"&startblock=0&endblock=99999999&page=1&offset=100&sort=desc"
               f"&apikey={ETHERSCAN_KEY}")

        try:
            resp = requests.get(url, timeout=15)
            data = resp.json()
            time.sleep(0.25)  # Rate limit
        except Exception as e:
            print(f"    Error: {e}")
            continue

        if data.get("status") != "1" or not data.get("result"):
            print(f"    No token transfers found")
            continue

        transfers = data["result"]
        binance_in = []
        binance_out = []
        exchange_in = []
        exchange_out = []
        relay_in = []
        unique_senders = set()
        unique_receivers = set()
        total_in = 0
        total_out = 0

        for tx in transfers:
            frm = tx.get("from", "").lower()
            to = tx.get("to", "").lower()
            token = tx.get("tokenSymbol", "")
            value = int(tx.get("value", 0)) / (10 ** int(tx.get("tokenDecimal", 6)))

            if token not in ("USDC", "USDC.e", "(PoS) USD Coin", "USD Coin"):
                continue

            if frm == wallet:
                # Outgoing
                total_out += value
                unique_receivers.add(to)
                if to in binance_set:
                    binance_out.append({"to": to, "value": value, "label": BINANCE_WALLETS.get(to, "Binance")})
                elif to in exchange_set:
                    exchange_out.append({"to": to, "value": value, "label": EXCHANGES.get(to, "Exchange")})
            elif to == wallet:
                # Incoming
                total_in += value
                unique_senders.add(frm)
                if frm in binance_set:
                    binance_in.append({"from": frm, "value": value, "label": BINANCE_WALLETS.get(frm, "Binance")})
                elif frm in exchange_set:
                    exchange_in.append({"from": frm, "value": value, "label": EXCHANGES.get(frm, "Exchange")})
                elif frm == "0xf70da97812cb96acdf810712aa562db8dfa3dbef":
                    relay_in.append({"from": frm, "value": value, "label": "Relay Solver"})

        result = {
            "wallet": wallet,
            "pnl": w["pnl"],
            "wr": w["wr"],
            "trades": w["trades"],
            "total_usdc_in": round(total_in, 2),
            "total_usdc_out": round(total_out, 2),
            "unique_senders": len(unique_senders),
            "unique_receivers": len(unique_receivers),
            "binance_deposits": binance_in,
            "binance_withdrawals": binance_out,
            "exchange_deposits": exchange_in,
            "exchange_withdrawals": exchange_out,
            "relay_deposits": relay_in,
        }
        results.append(result)

        if binance_in or binance_out:
            b_in_total = sum(x["value"] for x in binance_in)
            b_out_total = sum(x["value"] for x in binance_out)
            print(f"    *** BINANCE LINK FOUND ***")
            print(f"    Binance → wallet: ${b_in_total:,.2f} ({len(binance_in)} txs)")
            print(f"    Wallet → Binance: ${b_out_total:,.2f} ({len(binance_out)} txs)")
        elif exchange_in or exchange_out:
            e_in_total = sum(x["value"] for x in exchange_in)
            e_out_total = sum(x["value"] for x in exchange_out)
            print(f"    Exchange link: IN ${e_in_total:,.2f}, OUT ${e_out_total:,.2f}")
        else:
            funding_sources = []
            if relay_in:
                funding_sources.append(f"Relay: ${sum(x['value'] for x in relay_in):,.2f}")
            print(f"    USDC in: ${total_in:,.2f} from {len(unique_senders)} sources. {', '.join(funding_sources) or 'No exchange links'}")

    # Summary
    binance_linked = [r for r in results if r["binance_deposits"] or r["binance_withdrawals"]]
    exchange_linked = [r for r in results if r["exchange_deposits"] or r["exchange_withdrawals"]]

    print(f"\n  ─── SUMMARY ───")
    print(f"  Checked: {len(results)} wallets")
    print(f"  Binance-linked: {len(binance_linked)}")
    print(f"  Exchange-linked: {len(exchange_linked)}")

    if binance_linked:
        print(f"\n  BINANCE-LINKED ARB WALLETS:")
        for r in binance_linked:
            b_in = sum(x["value"] for x in r["binance_deposits"])
            b_out = sum(x["value"] for x in r["binance_withdrawals"])
            print(f"    {r['wallet'][:12]}... PnL=${r['pnl']:>8,.0f} WR={r['wr']:.1%} Trades={r['trades']} BN_IN=${b_in:,.0f} BN_OUT=${b_out:,.0f}")

    return results


# ─── Analysis 3: Kalshi Timing Correlation ─────────────────────────

def kalshi_timing_analysis(kalshi_data, boundaries, arb_suspects):
    """Check if Polymarket arb wallets' trade times correlate with Kalshi trade bursts."""
    if not kalshi_data:
        print("\n  [!] Skipping Kalshi timing analysis (no data)")
        return

    print("\n" + "="*80)
    print("  ANALYSIS 3: KALSHI TIMING CORRELATION")
    print("="*80)

    # Build Kalshi trade-per-second map
    kalshi_tps = defaultdict(int)  # unix_second → trade count
    kalshi_volume_ts = defaultdict(float)
    total_kalshi = 0

    trades_by_ticker = kalshi_data.get("trades_by_ticker", {})
    for ticker, trades in trades_by_ticker.items():
        for trade in trades:
            ts = trade.get("created_time") or trade.get("ts") or trade.get("timestamp")
            if not ts:
                continue
            if isinstance(ts, str):
                try:
                    ts = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
                except:
                    continue
            ts = int(ts)
            kalshi_tps[ts] += 1
            kalshi_volume_ts[ts] += float(trade.get("count", trade.get("size", 1)))
            total_kalshi += 1

    print(f"  Kalshi trades indexed: {total_kalshi:,}")
    print(f"  Kalshi active seconds: {len(kalshi_tps):,}")

    # For each window, compute Kalshi activity in first 60s vs last 60s
    window_kalshi = {}
    for ts in boundaries:
        early = sum(kalshi_tps.get(ts + s, 0) for s in range(60))
        late = sum(kalshi_tps.get(ts + 240 + s, 0) for s in range(60))
        mid = sum(kalshi_tps.get(ts + 120 + s, 0) for s in range(60))
        window_kalshi[ts] = {"early": early, "mid": mid, "late": late, "total": early + mid + late}

    # Correlation: windows where Kalshi is very active AND our arb wallets trade
    print(f"\n  Windows with high Kalshi activity (top 20):")
    sorted_windows = sorted(window_kalshi.items(), key=lambda x: x[1]["total"], reverse=True)[:20]
    for ts, k in sorted_windows:
        bd = boundaries.get(ts, {})
        print(f"    {datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%m-%d %H:%M')} "
              f"K_early={k['early']:>3} K_late={k['late']:>3} "
              f"BN_move={bd.get('bn_move_pct', 0):>+.3f}% dir={bd.get('direction', '?')}")

    return window_kalshi


# ─── Analysis 4: Volume-Weighted Arb Detection ────────────────────

def volume_weighted_arb(whale_cache, boundaries):
    """Find wallets that concentrate volume on high-Binance-move windows."""
    print("\n" + "="*80)
    print("  ANALYSIS 4: VOLUME CONCENTRATION ON BIG BINANCE MOVES")
    print("="*80)

    wallet_window_vol = defaultdict(lambda: {"big_move_vol": 0, "small_move_vol": 0,
                                              "big_move_wins": 0, "big_move_total": 0,
                                              "total_vol": 0, "total_trades": 0})

    for slug, market in whale_cache.items():
        window_ts = parse_slug_ts(slug)
        bd = boundaries.get(window_ts)
        if not bd:
            continue

        big_move = abs(bd["bn_move_pct"]) > 0.05

        token_map = market.get("token_map", {})

        for trade in market.get("trades", []):
            wallet = trade.get("proxyWallet", "").lower()
            size = float(trade.get("size", 0))
            price = float(trade.get("price", 0))
            side = trade.get("side", "").upper()
            asset = str(trade.get("asset", ""))

            if not wallet or not size:
                continue

            token_dir = token_map.get(asset, "").lower()
            if not token_dir:
                for tid, d in token_map.items():
                    if str(tid) == asset:
                        token_dir = d.lower()
                        break
            if not token_dir:
                continue

            bet_dir = token_dir if side == "BUY" else ("down" if token_dir == "up" else "up")
            usdc_val = size * price
            is_win = (bet_dir == bd["direction"])

            ws = wallet_window_vol[wallet]
            ws["total_vol"] += usdc_val
            ws["total_trades"] += 1

            if big_move:
                ws["big_move_vol"] += usdc_val
                ws["big_move_total"] += 1
                if is_win:
                    ws["big_move_wins"] += 1
            else:
                ws["small_move_vol"] += usdc_val

    # Find wallets that disproportionately trade on big-move windows with high WR
    vol_arb = []
    for wallet, s in wallet_window_vol.items():
        if s["total_trades"] < 50 or s["total_vol"] < 500:
            continue

        big_pct = s["big_move_vol"] / s["total_vol"] if s["total_vol"] > 0 else 0
        big_wr = s["big_move_wins"] / s["big_move_total"] if s["big_move_total"] > 10 else 0

        # Suspicious: >60% volume on big moves AND >60% WR on those
        if big_pct > 0.5 and big_wr > 0.58 and s["big_move_vol"] > 500:
            vol_arb.append({
                "wallet": wallet,
                "total_vol": round(s["total_vol"], 2),
                "big_move_vol": round(s["big_move_vol"], 2),
                "big_pct": round(big_pct, 4),
                "big_move_wr": round(big_wr, 4),
                "big_move_trades": s["big_move_total"],
            })

    vol_arb.sort(key=lambda x: x["big_move_vol"], reverse=True)

    print(f"\n  Found {len(vol_arb)} wallets concentrating volume on big BTC moves")
    print(f"  {'Wallet':<44} {'TotalVol':>10} {'BigMvVol':>10} {'BigMv%':>7} {'BigMvWR':>8} {'BigTrades':>9}")
    print("  " + "-"*95)
    for s in vol_arb[:25]:
        print(f"  {s['wallet']:<44} ${s['total_vol']:>9,.0f} ${s['big_move_vol']:>9,.0f} {s['big_pct']:>6.1%} {s['big_move_wr']:>8.1%} {s['big_move_trades']:>9}")

    return vol_arb


# ─── Main ──────────────────────────────────────────────────────────

def main():
    print("="*80)
    print("  BINANCE ↔ POLYMARKET ARBITRAGE LINK DETECTION")
    print("="*80)

    boundaries = load_boundaries()
    print(f"  Boundaries: {len(boundaries)} windows")

    whale_cache = load_whale_cache()

    # Analysis 1: Latency arb detection
    arb_suspects = detect_latency_arb(whale_cache, boundaries)

    # Analysis 4: Volume concentration on big BTC moves
    vol_arb = volume_weighted_arb(whale_cache, boundaries)

    # Merge suspects (union of both analyses, sorted by PnL)
    all_suspects = {}
    for s in arb_suspects:
        all_suspects[s["wallet"]] = s
    for s in vol_arb:
        if s["wallet"] not in all_suspects:
            all_suspects[s["wallet"]] = {
                "wallet": s["wallet"], "pnl": 0, "wr": 0, "trades": s.get("big_move_trades", 0),
                "volume": s["total_vol"],
            }

    merged = sorted(all_suspects.values(), key=lambda x: x.get("pnl", 0), reverse=True)

    # Analysis 2: Check top suspects for Binance on-chain flows
    print(f"\n  Total unique arb suspects: {len(merged)}")
    top_for_check = [s for s in merged if s.get("pnl", 0) > 1000 or s.get("volume", 0) > 5000][:25]

    binance_results = check_binance_flows(top_for_check, max_wallets=25)

    # Analysis 3: Kalshi timing
    kalshi_data = load_kalshi_trades()
    kalshi_timing = kalshi_timing_analysis(kalshi_data, boundaries, arb_suspects)

    # ─── Save Results ─────────────────────────────────────────────
    output = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "windows_analyzed": len(boundaries),
            "latency_arb_suspects": len(arb_suspects),
            "volume_concentration_suspects": len(vol_arb),
            "binance_linked_wallets": len([r for r in binance_results if r.get("binance_deposits") or r.get("binance_withdrawals")]),
            "exchange_linked_wallets": len([r for r in binance_results if r.get("exchange_deposits") or r.get("exchange_withdrawals")]),
        },
        "top_arb_suspects": arb_suspects[:50],
        "volume_concentration": vol_arb[:50],
        "binance_flow_results": binance_results,
    }

    out_path = os.path.join(DATA_DIR, "binance_arb_link.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Results saved to {out_path}")

    # ─── Final Summary ────────────────────────────────────────────
    print("\n" + "="*80)
    print("  FINAL SUMMARY")
    print("="*80)

    bn_linked = [r for r in binance_results if r.get("binance_deposits") or r.get("binance_withdrawals")]
    if bn_linked:
        print(f"\n  *** {len(bn_linked)} WALLETS WITH PROVEN BINANCE LINK ***")
        for r in bn_linked:
            b_in = sum(x["value"] for x in r["binance_deposits"])
            b_out = sum(x["value"] for x in r["binance_withdrawals"])
            print(f"    {r['wallet']}")
            print(f"      PnL: ${r['pnl']:>10,.2f} | WR: {r['wr']:.1%} | Trades: {r['trades']}")
            print(f"      Binance IN:  ${b_in:>10,.2f} ({len(r['binance_deposits'])} deposits)")
            print(f"      Binance OUT: ${b_out:>10,.2f} ({len(r['binance_withdrawals'])} withdrawals)")
    else:
        print(f"\n  No direct Binance links found in top {len(binance_results)} wallets")
        print(f"  This may mean arb operators use intermediate wallets or different CEXes")

    ex_linked = [r for r in binance_results if r.get("exchange_deposits") or r.get("exchange_withdrawals")]
    if ex_linked:
        print(f"\n  {len(ex_linked)} wallets with OTHER exchange links:")
        for r in ex_linked:
            for d in r["exchange_deposits"]:
                print(f"    {r['wallet'][:12]}... ← ${d['value']:,.2f} from {d['label']}")
            for d in r["exchange_withdrawals"]:
                print(f"    {r['wallet'][:12]}... → ${d['value']:,.2f} to {d['label']}")


if __name__ == "__main__":
    main()
