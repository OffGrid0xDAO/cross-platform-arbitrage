#!/usr/bin/env python3
"""
P&L Decomposition, Temporal Stationarity, and Loss Distribution
================================================================

Three analyses for the cross-platform arbitrage paper:

1. P&L DECOMPOSITION BY STRATEGY
2. TEMPORAL STATIONARITY (day-by-day)
3. LOSS DISTRIBUTION (who pays?)

Data sources:
  - poly15m/  (672 JSON files, ~2.5M Polymarket 15-min trades)
  - kalshi_btc_trades.json (1.68M Kalshi trades)
  - boundaries.csv (5-min window Binance/Chainlink prices)
  - whale_analysis.json (top 200 wallets, 5-min aggregated stats)
"""

import json
import csv
import os
import sys
import glob
from collections import defaultdict
from datetime import datetime, timezone
import statistics
import math

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
POLY15M_DIR = os.path.join(DATA_DIR, "poly15m")
KALSHI_FILE = os.path.join(DATA_DIR, "kalshi_btc_trades.json")
BOUNDARIES = os.path.join(DATA_DIR, "boundaries.csv")
WHALE_ANALYSIS = os.path.join(DATA_DIR, "whale_analysis.json")

POLYGON_GAS_PER_TX = 0.01  # ~$0.01 per transaction on Polygon L2
WINDOW_DURATION = 900       # 15 minutes in seconds


# ═══════════════════════════════════════════════════════════════════
#  DATA LOADING
# ═══════════════════════════════════════════════════════════════════

def load_poly_15m():
    """Load all Polymarket 15-min market files from poly15m/."""
    files = sorted(glob.glob(os.path.join(POLY15M_DIR, "*.json")))
    print(f"[*] Loading {len(files)} Polymarket 15-min markets...")
    markets = {}
    total_trades = 0
    for fp in files:
        slug = os.path.basename(fp).replace(".json", "")
        ts = int(slug.split("-")[-1])
        with open(fp) as f:
            data = json.load(f)
        markets[ts] = data
        total_trades += len(data.get("trades", []))
    print(f"    Loaded {len(markets)} markets, {total_trades:,} trades")
    return markets


def load_kalshi():
    """Load Kalshi trade data keyed by open timestamp."""
    print("[*] Loading Kalshi trades...")
    with open(KALSHI_FILE) as f:
        data = json.load(f)
    kalshi = {}
    total_trades = 0
    for market in data["markets"]:
        ticker = market["ticker"]
        try:
            open_t = datetime.fromisoformat(market["open_time"].replace("Z", "+00:00"))
            ts = int(open_t.timestamp())
        except:
            continue
        trades = data["trades_by_ticker"].get(ticker, [])
        kalshi[ts] = {
            "ticker": ticker,
            "result": market.get("result", ""),
            "volume": market.get("volume", 0),
            "trades": trades,
        }
        total_trades += len(trades)
    print(f"    Loaded {len(kalshi)} markets, {total_trades:,} trades")
    return kalshi


def load_boundaries():
    """Load boundaries.csv for 5-min market context."""
    boundaries = {}
    with open(BOUNDARIES) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dt = datetime.fromisoformat(row["window_start_utc"])
                ts = int(dt.timestamp())
                boundaries[ts] = {
                    "bn_start": float(row.get("binance_start") or 0),
                    "bn_end": float(row.get("binance_end") or 0),
                    "direction": row["actual_direction"],
                    "date": dt.strftime("%Y-%m-%d"),
                }
            except (ValueError, KeyError):
                continue
    return boundaries


def load_whale_analysis():
    """Load whale_analysis.json (top 200 wallets, 5-min aggregated)."""
    with open(WHALE_ANALYSIS) as f:
        data = json.load(f)
    print(f"[*] Loaded whale_analysis.json: {len(data)} wallets (5-min summary)")
    return {w["wallet"]: w for w in data}


def poly_direction(trade, token_map):
    """Determine bet direction for a Polymarket trade."""
    asset = str(trade.get("asset", ""))
    side = trade.get("side", "").upper()
    token_dir = token_map.get(asset, "").lower()
    if not token_dir:
        return None
    return token_dir if side == "BUY" else ("down" if token_dir == "up" else "up")


# ═══════════════════════════════════════════════════════════════════
#  PHASE 1: PROCESS ALL TRADES AND CLASSIFY WALLETS
# ═══════════════════════════════════════════════════════════════════

def process_and_classify(poly_markets, kalshi_markets):
    """
    Process all 15-min trades, compute per-wallet stats, classify into strategies.
    Also match windows to Kalshi for alignment scoring.
    """
    print("\n" + "=" * 80)
    print("  PHASE 1: PROCESSING 15-MIN TRADES & CLASSIFYING WALLETS")
    print("=" * 80)

    # Match Polymarket → Kalshi windows
    kalshi_by_ts = {}
    for k_ts, k_data in kalshi_markets.items():
        kalshi_by_ts[k_ts] = k_data

    wallet_stats = defaultdict(lambda: {
        "trades": 0, "wins": 0, "volume": 0.0, "pnl": 0.0,
        "buy_trades": 0, "sell_trades": 0,
        "sell_wins": 0, "buy_wins": 0,
        "late_trades": 0, "late_wins": 0,      # last 120s of 900s window
        "vlate_trades": 0, "vlate_wins": 0,    # last 60s
        "entry_times": [],
        "sizes": [], "prices": [],
        "pseudonym": "",
        "windows_up": set(), "windows_down": set(),
        "windows_traded": set(),
        "per_trade": [],
        "first_seen": None,
        "kalshi_aligned": 0,  # trades where direction matches Kalshi outcome
        "kalshi_total": 0,    # trades in windows with Kalshi data
    })

    # Per-window trade lists for counterparty analysis
    window_trades = defaultdict(list)
    total_processed = 0
    skipped = 0

    for p_ts, p_data in poly_markets.items():
        resolution = p_data.get("resolution", "").lower()
        if not resolution or resolution not in ("up", "down"):
            skipped += 1
            continue

        token_map = p_data.get("token_map", {})
        trades = p_data.get("trades", [])
        date_str = datetime.fromtimestamp(p_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        w_end = p_ts + WINDOW_DURATION

        # Get Kalshi result for this window (match by timestamp, ±120s tolerance)
        k_result = None
        for offset in range(0, 121, 1):
            for sign in [0, 1, -1]:
                check_ts = p_ts + sign * offset
                if check_ts in kalshi_by_ts:
                    k_result = kalshi_by_ts[check_ts].get("result", "").lower()
                    break
            if k_result:
                break

        for trade in trades:
            wallet = trade["proxyWallet"]
            ts = trade.get("timestamp", 0)
            if not ts:
                continue
            secs = ts - p_ts
            side = trade.get("side", "").upper()
            size = float(trade.get("size", 0))
            price = float(trade.get("price", 0.5))
            direction = poly_direction(trade, token_map)
            if not direction or size <= 0:
                continue

            won = (direction == resolution)

            # P&L
            if side == "BUY":
                usd_cost = size * price
                trade_pnl = (size - usd_cost) if won else -usd_cost
            else:
                usd_cost = size * (1 - price)
                trade_pnl = (size - usd_cost) if won else -usd_cost

            ws = wallet_stats[wallet]
            ws["trades"] += 1
            ws["wins"] += int(won)
            ws["volume"] += usd_cost
            ws["pnl"] += trade_pnl
            ws["pseudonym"] = trade.get("pseudonym", "")
            ws["entry_times"].append(secs)
            ws["sizes"].append(size)
            ws["prices"].append(price)
            ws["windows_traded"].add(p_ts)

            if side == "BUY":
                ws["buy_trades"] += 1
                ws["buy_wins"] += int(won)
            else:
                ws["sell_trades"] += 1
                ws["sell_wins"] += int(won)

            if secs >= (WINDOW_DURATION - 120):  # last 2 min
                ws["late_trades"] += 1
                ws["late_wins"] += int(won)
            if secs >= (WINDOW_DURATION - 60):   # last 1 min
                ws["vlate_trades"] += 1
                ws["vlate_wins"] += int(won)

            if direction == "up":
                ws["windows_up"].add(p_ts)
            else:
                ws["windows_down"].add(p_ts)

            ws["per_trade"].append({
                "date": date_str,
                "pnl": trade_pnl,
                "volume": usd_cost,
                "won": won,
                "side": side,
                "secs": secs,
                "price": price,
                "size": size,
                "window": p_ts,
                "direction": direction,
            })

            if ws["first_seen"] is None or date_str < ws["first_seen"]:
                ws["first_seen"] = date_str

            # Kalshi alignment
            if k_result:
                ws["kalshi_total"] += 1
                k_yes = (k_result == "yes")
                if (direction == "up" and k_yes) or (direction == "down" and not k_yes):
                    ws["kalshi_aligned"] += 1

            window_trades[p_ts].append({
                "wallet": wallet,
                "side": side,
                "bet_dir": direction,
                "size": size,
                "price": price,
                "won": won,
                "secs": secs,
                "usd_cost": usd_cost,
                "pnl": trade_pnl,
            })

            total_processed += 1

    print(f"  Processed {total_processed:,} trades across {len(poly_markets) - skipped} windows")
    print(f"  Unique wallets: {len(wallet_stats):,}")

    # ── Classify wallets ──
    classifications = {}
    strategy_counts = defaultdict(int)

    for wallet, s in wallet_stats.items():
        if s["trades"] < 5:
            classifications[wallet] = "inactive"
            strategy_counts["inactive"] += 1
            continue

        wr = s["wins"] / s["trades"]
        late_frac = s["late_trades"] / s["trades"]
        sell_frac = s["sell_trades"] / s["trades"]
        sell_wr = s["sell_wins"] / s["sell_trades"] if s["sell_trades"] > 0 else 0

        total_windows = len(s["windows_traded"])
        both_windows = len(s["windows_up"] & s["windows_down"])
        both_frac = both_windows / total_windows if total_windows > 0 else 0

        # Kalshi alignment
        k_align = s["kalshi_aligned"] / s["kalshi_total"] if s["kalshi_total"] > 0 else 0.5

        # Classification (priority order)
        if (s["trades"] >= 15 and wr >= 0.80 and late_frac >= 0.40 and
                s["vlate_trades"] >= 3):
            classifications[wallet] = "latency_arb"
        elif (s["sell_trades"] >= 10 and sell_frac >= 0.50 and sell_wr >= 0.60
              and s["trades"] >= 20):
            classifications[wallet] = "sell_cluster"
        elif (both_frac >= 0.20 and total_windows >= 15 and both_windows >= 5):
            classifications[wallet] = "market_maker"
        elif (wr >= 0.65 and s["trades"] >= 30 and k_align >= 0.60):
            classifications[wallet] = "informed"
        elif (wr >= 0.60 and s["trades"] >= 50):
            classifications[wallet] = "informed"
        else:
            classifications[wallet] = "retail"

        strategy_counts[classifications[wallet]] += 1

    print(f"\n  Wallet classifications:")
    for strat in ["latency_arb", "sell_cluster", "market_maker", "informed", "retail", "inactive"]:
        print(f"    {strat:<20} {strategy_counts[strat]:>6,}")

    return wallet_stats, classifications, window_trades


# ═══════════════════════════════════════════════════════════════════
#  ANALYSIS 1: P&L DECOMPOSITION BY STRATEGY
# ═══════════════════════════════════════════════════════════════════

def pnl_decomposition(wallet_stats, classifications):
    print("\n" + "=" * 80)
    print("  ANALYSIS 1: P&L DECOMPOSITION BY STRATEGY")
    print("=" * 80)

    strat_names = ["latency_arb", "sell_cluster", "market_maker", "informed", "retail"]
    strategies = {name: {
        "wallets": 0, "trades": 0, "wins": 0,
        "gross_pnl": 0.0, "volume": 0.0,
        "prices": [], "entry_times": [], "sizes": [],
    } for name in strat_names}

    for wallet, s in wallet_stats.items():
        strat = classifications.get(wallet, "retail")
        if strat == "inactive" or strat not in strategies:
            continue

        st = strategies[strat]
        st["wallets"] += 1
        st["trades"] += s["trades"]
        st["wins"] += s["wins"]
        st["gross_pnl"] += s["pnl"]
        st["volume"] += s["volume"]
        st["prices"].extend(s["prices"][:200])  # sample to avoid memory
        st["entry_times"].extend(s["entry_times"][:200])
        st["sizes"].extend(s["sizes"][:200])

    print(f"\n  {'Strategy':<16} {'Wallets':>8} {'Trades':>10} {'WR':>7} "
          f"{'Gross PnL':>12} {'Volume':>14} {'Gas':>10} {'Net PnL':>12} {'ROI':>7} {'AvgP':>6} {'AvgT':>6}")
    print(f"  {'─' * 125}")

    results = []
    for name in strat_names:
        st = strategies[name]
        wr = st["wins"] / st["trades"] if st["trades"] > 0 else 0
        gas = st["trades"] * POLYGON_GAS_PER_TX
        net_pnl = st["gross_pnl"] - gas
        roi = net_pnl / st["volume"] * 100 if st["volume"] > 0 else 0
        avg_p = statistics.mean(st["prices"]) if st["prices"] else 0
        avg_t = statistics.mean(st["entry_times"]) if st["entry_times"] else 0
        avg_sz = statistics.mean(st["sizes"]) if st["sizes"] else 0
        annualized = net_pnl * (365 / 5.6)

        print(f"  {name:<16} {st['wallets']:>8,} {st['trades']:>10,} {wr:>6.1%} "
              f"${st['gross_pnl']:>11,.0f} ${st['volume']:>13,.0f} ${gas:>9,.0f} "
              f"${net_pnl:>11,.0f} {roi:>6.1f}% {avg_p:>5.2f} {avg_t:>5.0f}s")

        results.append({
            "strategy": name,
            "wallets": st["wallets"],
            "trades": st["trades"],
            "wins": st["wins"],
            "wr": round(wr, 4),
            "gross_pnl": round(st["gross_pnl"], 2),
            "volume": round(st["volume"], 2),
            "gas_cost": round(gas, 2),
            "net_pnl": round(net_pnl, 2),
            "roi_pct": round(roi, 2),
            "avg_price": round(avg_p, 4),
            "avg_entry_secs": round(avg_t, 1),
            "avg_size": round(avg_sz, 2),
            "annualized_pnl": round(annualized, 0),
        })

    total_gross = sum(r["gross_pnl"] for r in results)
    total_vol = sum(r["volume"] for r in results)
    total_gas = sum(r["gas_cost"] for r in results)
    total_net = total_gross - total_gas
    print(f"  {'─' * 125}")
    print(f"  {'TOTAL':<16} {sum(r['wallets'] for r in results):>8,} "
          f"{sum(r['trades'] for r in results):>10,} {'':>7} "
          f"${total_gross:>11,.0f} ${total_vol:>13,.0f} ${total_gas:>9,.0f} ${total_net:>11,.0f}")

    # Per-trade economics
    print(f"\n  Per-Trade Economics:")
    print(f"  {'Strategy':<16} {'Edge/Trade':>12} {'AvgSize':>10} {'AvgPrice':>10} {'Net/Trade':>12}")
    print(f"  {'─' * 65}")
    for r in results:
        if r["trades"] > 0:
            edge = r["gross_pnl"] / r["trades"]
            net_per = r["net_pnl"] / r["trades"]
            print(f"  {r['strategy']:<16} ${edge:>11.4f} ${r['avg_size']:>9.2f} "
                  f"{r['avg_price']:>10.4f} ${net_per:>11.4f}")

    return results


# ═══════════════════════════════════════════════════════════════════
#  ANALYSIS 2: TEMPORAL STATIONARITY
# ═══════════════════════════════════════════════════════════════════

def temporal_stationarity(wallet_stats, classifications, poly_markets):
    print("\n" + "=" * 80)
    print("  ANALYSIS 2: TEMPORAL STATIONARITY")
    print("=" * 80)

    # Get dates from market timestamps
    all_dates = set()
    windows_per_date = defaultdict(int)
    for p_ts in poly_markets:
        d = datetime.fromtimestamp(p_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        all_dates.add(d)
        windows_per_date[d] += 1
    dates = sorted(all_dates)
    print(f"  Date range: {dates[0]} to {dates[-1]} ({len(dates)} days)")

    # Aggregate per-day per-strategy
    daily = defaultdict(lambda: defaultdict(lambda: {
        "trades": 0, "wins": 0, "pnl": 0.0, "volume": 0.0,
        "wallets_active": set(), "new_wallets": set(),
    }))

    wallet_first_day = {}
    for wallet, s in wallet_stats.items():
        if s["first_seen"]:
            wallet_first_day[wallet] = s["first_seen"]

    for wallet, s in wallet_stats.items():
        strat = classifications.get(wallet, "retail")
        if strat == "inactive":
            continue
        for t in s["per_trade"]:
            d = daily[t["date"]][strat]
            d["trades"] += 1
            d["wins"] += int(t["won"])
            d["pnl"] += t["pnl"]
            d["volume"] += t["volume"]
            d["wallets_active"].add(wallet)
            if wallet_first_day.get(wallet) == t["date"]:
                d["new_wallets"].add(wallet)

    # Print per-strategy daily breakdown
    for strat_name in ["latency_arb", "sell_cluster", "market_maker", "informed"]:
        print(f"\n  ── {strat_name.upper()} ──")
        print(f"  {'Date':<12} {'Trades':>8} {'WR':>7} {'PnL':>12} {'Volume':>12} {'Active':>8} {'New':>6}")
        print(f"  {'─' * 70}")
        for date in dates:
            d = daily[date][strat_name]
            wr = d["wins"] / d["trades"] if d["trades"] > 0 else 0
            print(f"  {date:<12} {d['trades']:>8,} {wr:>6.1%} ${d['pnl']:>11,.0f} "
                  f"${d['volume']:>11,.0f} {len(d['wallets_active']):>8} {len(d['new_wallets']):>6}")

    # Combined arb summary
    arb_strats = {"latency_arb", "sell_cluster", "informed"}
    print(f"\n  ── ALL ARB STRATEGIES COMBINED ──")
    print(f"  {'Date':<12} {'Windows':>8} {'ArbTrades':>10} {'WR':>7} {'PnL':>12} "
          f"{'PnL/Win':>10} {'ArbWallets':>11} {'NewArb':>8}")
    print(f"  {'─' * 90}")

    daily_results = []
    for date in dates:
        trades = sum(daily[date][s]["trades"] for s in arb_strats)
        wins = sum(daily[date][s]["wins"] for s in arb_strats)
        pnl = sum(daily[date][s]["pnl"] for s in arb_strats)
        volume = sum(daily[date][s]["volume"] for s in arb_strats)
        active = set()
        new = set()
        for s in arb_strats:
            active |= daily[date][s]["wallets_active"]
            new |= daily[date][s]["new_wallets"]

        windows = windows_per_date[date]
        wr = wins / trades if trades > 0 else 0
        pnl_per_w = pnl / windows if windows > 0 else 0

        print(f"  {date:<12} {windows:>8} {trades:>10,} {wr:>6.1%} ${pnl:>11,.0f} "
              f"${pnl_per_w:>9,.1f} {len(active):>11,} {len(new):>8}")

        daily_results.append({
            "date": date,
            "windows": windows,
            "trades": trades,
            "wins": wins,
            "wr": round(wr, 4),
            "pnl": round(pnl, 2),
            "volume": round(volume, 2),
            "arb_wallets_active": len(active),
            "new_arb_wallets": len(new),
            "pnl_per_window": round(pnl_per_w, 2),
        })

    # Stationarity: linear regression on PnL/window over days
    full_days = [d for d in daily_results if d["windows"] >= 40]  # skip partial days
    if len(full_days) >= 3:
        y_vals = [d["pnl_per_window"] for d in full_days]
        n = len(y_vals)
        x_mean = (n - 1) / 2
        y_mean = sum(y_vals) / n
        num = sum((i - x_mean) * (y - y_mean) for i, y in enumerate(y_vals))
        den = sum((i - x_mean) ** 2 for i in range(n))
        slope = num / den if den > 0 else 0

        print(f"\n  Stationarity (full days only, n={len(full_days)}):")
        print(f"    Mean PnL/window:           ${y_mean:,.2f}")
        print(f"    Trend (PnL/window/day):    ${slope:,.2f}")
        if abs(slope) < abs(y_mean) * 0.15:
            print(f"    → STABLE (slope < 15% of mean)")
        elif slope < 0:
            print(f"    → DECLINING opportunity")
        else:
            print(f"    → INCREASING opportunity")

        total_new = sum(d["new_arb_wallets"] for d in daily_results)
        print(f"    New arb wallets over period: {total_new}")

    # Retail daily for comparison
    print(f"\n  ── RETAIL ──")
    print(f"  {'Date':<12} {'Trades':>10} {'WR':>7} {'PnL':>12} {'Volume':>14} {'Active':>8}")
    print(f"  {'─' * 65}")
    for date in dates:
        d = daily[date]["retail"]
        wr = d["wins"] / d["trades"] if d["trades"] > 0 else 0
        print(f"  {date:<12} {d['trades']:>10,} {wr:>6.1%} ${d['pnl']:>11,.0f} "
              f"${d['volume']:>13,.0f} {len(d['wallets_active']):>8,}")

    return daily_results


# ═══════════════════════════════════════════════════════════════════
#  ANALYSIS 3: LOSS DISTRIBUTION (WHO LOSES?)
# ═══════════════════════════════════════════════════════════════════

def loss_distribution(wallet_stats, classifications, window_trades):
    print("\n" + "=" * 80)
    print("  ANALYSIS 3: LOSS DISTRIBUTION (WHO LOSES?)")
    print("=" * 80)

    arb_set = {w for w, c in classifications.items()
               if c in ("latency_arb", "sell_cluster", "informed")}
    mm_set = {w for w, c in classifications.items() if c == "market_maker"}

    # Track per-type losses and who funds arb profits
    loss_by_type = {
        "market_maker": {"gross_loss": 0.0, "losing_trades": 0, "volume": 0.0,
                         "arb_attributed": 0.0},
        "other_arb":    {"gross_loss": 0.0, "losing_trades": 0, "volume": 0.0,
                         "arb_attributed": 0.0},
        "retail":       {"gross_loss": 0.0, "losing_trades": 0, "volume": 0.0,
                         "arb_attributed": 0.0},
    }

    # Also track total arb profits per window for attribution
    total_arb_profit = 0.0
    total_arb_losing_trades = 0

    wallet_losses = defaultdict(lambda: {"loss": 0.0, "trades": 0, "volume": 0.0})

    for w_start, trades in window_trades.items():
        # Compute arb profit in this window
        arb_profit_this = 0.0
        arb_bet_dirs = defaultdict(float)  # direction → volume of arb winning trades

        # Losing volume by direction and type
        losing_by_dir_type = defaultdict(lambda: defaultdict(float))

        for t in trades:
            wallet = t["wallet"]
            strat = classifications.get(wallet, "retail")

            if strat in ("latency_arb", "sell_cluster", "informed") and t["won"]:
                arb_profit_this += t["pnl"]
                arb_bet_dirs[t["bet_dir"]] += t["usd_cost"]

            if not t["won"]:
                loss_amt = abs(t["pnl"])

                if strat == "market_maker":
                    ltype = "market_maker"
                elif strat in ("latency_arb", "sell_cluster", "informed"):
                    ltype = "other_arb"
                    total_arb_losing_trades += 1
                else:
                    ltype = "retail"

                loss_by_type[ltype]["gross_loss"] += loss_amt
                loss_by_type[ltype]["losing_trades"] += 1
                loss_by_type[ltype]["volume"] += t["usd_cost"]

                losing_by_dir_type[t["bet_dir"]][ltype] += loss_amt

                wallet_losses[wallet]["loss"] += loss_amt
                wallet_losses[wallet]["trades"] += 1
                wallet_losses[wallet]["volume"] += t["usd_cost"]

        total_arb_profit += max(arb_profit_this, 0)

        # Attribute arb profits to counterparty types
        for winning_dir, arb_vol in arb_bet_dirs.items():
            losing_dir = "down" if winning_dir == "up" else "up"
            losers = losing_by_dir_type.get(losing_dir, {})
            total_losing = sum(losers.values())
            if total_losing > 0:
                for ltype, loss_vol in losers.items():
                    fraction = loss_vol / total_losing
                    loss_by_type[ltype]["arb_attributed"] += arb_profit_this * fraction

    # Print results
    print(f"\n  Total arb wallet profits (gross): ${total_arb_profit:,.0f}")
    print(f"\n  {'Counterparty':<18} {'Gross Losses':>14} {'Losing Trades':>14} "
          f"{'Volume':>14} {'Arb-Attributed':>16} {'Avg Loss/Trade':>14}")
    print(f"  {'─' * 95}")

    total_gross_loss = sum(v["gross_loss"] for v in loss_by_type.values())
    total_arb_attr = sum(v["arb_attributed"] for v in loss_by_type.values())
    loss_results = []

    for ltype in ["market_maker", "retail", "other_arb"]:
        lt = loss_by_type[ltype]
        avg_loss = lt["gross_loss"] / lt["losing_trades"] if lt["losing_trades"] > 0 else 0
        pct = lt["arb_attributed"] / total_arb_attr * 100 if total_arb_attr > 0 else 0

        print(f"  {ltype:<18} ${lt['gross_loss']:>13,.0f} {lt['losing_trades']:>14,} "
              f"${lt['volume']:>13,.0f} ${lt['arb_attributed']:>15,.0f} ${avg_loss:>13.2f}")

        loss_results.append({
            "type": ltype,
            "gross_loss": round(lt["gross_loss"], 2),
            "losing_trades": lt["losing_trades"],
            "volume": round(lt["volume"], 2),
            "arb_attributed": round(lt["arb_attributed"], 2),
            "pct_of_arb_funding": round(pct, 1),
            "avg_loss_per_trade": round(avg_loss, 4),
        })

    print(f"  {'─' * 95}")
    print(f"  {'TOTAL':<18} ${total_gross_loss:>13,.0f} "
          f"{sum(loss_by_type[t]['losing_trades'] for t in loss_by_type):>14,} "
          f"{'':>14} ${total_arb_attr:>15,.0f}")

    # Arb profit funded by each type (percentage)
    print(f"\n  Who funds arb profits?")
    for r in loss_results:
        print(f"    {r['type']:<18} {r['pct_of_arb_funding']:>5.1f}% "
              f"(${r['arb_attributed']:>12,.0f})")

    # Impact rates
    for label, wallet_set, ltype in [
        ("Retail", {w for w, c in classifications.items() if c == "retail"}, "retail"),
        ("Market Maker", {w for w, c in classifications.items() if c == "market_maker"}, "market_maker"),
    ]:
        total_vol = sum(wallet_stats[w]["volume"] for w in wallet_set if w in wallet_stats)
        loss = loss_by_type[ltype]["arb_attributed"]
        rate = loss / total_vol * 100 if total_vol > 0 else 0
        total_pnl = sum(wallet_stats[w]["pnl"] for w in wallet_set if w in wallet_stats)
        print(f"\n  {label} impact:")
        print(f"    Total volume:           ${total_vol:>14,.0f}")
        print(f"    Aggregate PnL:          ${total_pnl:>14,.0f}")
        print(f"    Loss attributed to arb: ${loss:>14,.0f}")
        print(f"    Loss rate:              {rate:.3f}%  (cents per dollar traded)")

    # Top 15 losing wallets
    top_losers = sorted(wallet_losses.items(), key=lambda x: x[1]["loss"], reverse=True)[:15]
    print(f"\n  Top 15 losing wallets:")
    print(f"  {'Wallet':<44} {'Type':<16} {'Trades':>8} {'Loss':>12} {'Volume':>12}")
    print(f"  {'─' * 95}")
    for wallet, info in top_losers:
        strat = classifications.get(wallet, "retail")
        pseudo = wallet_stats.get(wallet, {}).get("pseudonym", "")
        label = pseudo[:42] if pseudo else wallet[:42]
        print(f"  {label:<44} {strat:<16} {info['trades']:>8,} "
              f"${info['loss']:>11,.0f} ${info['volume']:>11,.0f}")

    return loss_results


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  P&L DECOMPOSITION, STATIONARITY, AND LOSS DISTRIBUTION    ║")
    print("║  Data: Polymarket 15-min (2.5M trades) + Kalshi (1.7M)     ║")
    print("╚══════════════════════════════════════════════════════════════╝\n")

    poly = load_poly_15m()
    kalshi = load_kalshi()

    # Phase 1: Process and classify
    wallet_stats, classifications, window_trades = process_and_classify(poly, kalshi)

    # Analysis 1
    pnl_results = pnl_decomposition(wallet_stats, classifications)

    # Analysis 2
    daily_results = temporal_stationarity(wallet_stats, classifications, poly)

    # Analysis 3
    loss_results = loss_distribution(wallet_stats, classifications, window_trades)

    # Save
    output = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dataset": "Polymarket 15-min + Kalshi 15-min, Feb 19-25 2026",
        "pnl_decomposition": pnl_results,
        "temporal_stationarity": daily_results,
        "loss_distribution": loss_results,
    }

    out_path = os.path.join(DATA_DIR, "pnl_stationarity_losses.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Results saved to {out_path}")
    print("  Done.")


if __name__ == "__main__":
    main()
