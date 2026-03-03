#!/usr/bin/env python3
"""
Cross-Platform Arbitrage Detection: Polymarket vs Kalshi (15-min BTC markets)

Matches simultaneous 15-min windows between platforms and detects:
1. Price discrepancies (same window, different prices → arb opportunity)
2. Timing correlation (trades within seconds across platforms)
3. Volume surges at window boundaries (arb traders rushing in)
4. Directional divergence (one platform leads the other)
5. Behavioral fingerprinting (Polymarket wallets that trade in sync with Kalshi patterns)
"""
import json, os, sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# ─── Load Data ───────────────────────────────────────────────────

def load_kalshi():
    """Load Kalshi 15-min trade data."""
    path = DATA_DIR / "kalshi_btc_trades.json"
    if not path.exists():
        print("ERROR: kalshi_btc_trades.json not found. Run fetch_kalshi_data.py first.")
        sys.exit(1)
    with open(path) as f:
        data = json.load(f)

    markets = data.get("markets", [])
    trades_by_ticker = data.get("trades_by_ticker", {})

    print(f"  Kalshi: {len(markets)} markets, {len(trades_by_ticker)} with trades")

    # Parse each market's close time to get window boundaries
    # Ticker format: KXBTC15M-26FEB{DD}{HH}{MM}-{offset}
    # close_time is the end of the window
    parsed = {}
    for mkt in markets:
        ticker = mkt["ticker"]
        close_str = mkt.get("close_time", "")
        if not close_str:
            continue
        try:
            close_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
            open_dt = close_dt - timedelta(minutes=15)
            open_ts = int(open_dt.timestamp())
            close_ts = int(close_dt.timestamp())

            trades = trades_by_ticker.get(ticker, [])
            parsed[ticker] = {
                "open_ts": open_ts,
                "close_ts": close_ts,
                "open_utc": open_dt.isoformat(),
                "close_utc": close_dt.isoformat(),
                "volume": mkt.get("volume", 0),
                "result": mkt.get("result"),
                "trades": trades,
            }
        except Exception:
            continue

    return parsed


def load_polymarket():
    """Load Polymarket 15-min trade data from per-market files in data/poly15m/."""
    market_dir = DATA_DIR / "poly15m"

    # Also check old monolithic cache
    if not market_dir.exists() or not any(market_dir.iterdir()):
        old_cache = DATA_DIR / "polymarket_15m_cache.json"
        if old_cache.exists():
            print("  Loading from monolithic cache...")
            with open(old_cache) as f:
                data = json.load(f)
            existing = {k: v for k, v in data.items() if v.get("exists")}
        else:
            print("ERROR: No Polymarket 15m data found. Run fetch_polymarket_15m.py first.")
            sys.exit(1)
    else:
        # Load per-market files
        existing = {}
        for fpath in sorted(market_dir.glob("btc-updown-15m-*.json")):
            slug = fpath.stem
            with open(fpath) as f:
                info = json.load(f)
            if info.get("exists"):
                existing[slug] = info

    total_trades = sum(len(v.get("trades", [])) for v in existing.values())
    print(f"  Polymarket 15m: {len(existing)} markets, {total_trades:,} trades")

    # Parse slug to get window start/end
    parsed = {}
    for slug, info in existing.items():
        # slug = btc-updown-15m-{start_ts}
        try:
            start_ts = int(slug.split("-")[-1])
            end_ts = start_ts + 900  # 15 min
            parsed[slug] = {
                "open_ts": start_ts,
                "close_ts": end_ts,
                "resolution": info.get("resolution"),
                "trades": info.get("trades", []),
                "token_map": info.get("token_map", {}),
            }
        except (ValueError, IndexError):
            continue

    return parsed


def load_polymarket_5m():
    """Load Polymarket 5-min trade data from whale_cache.json for additional context."""
    path = DATA_DIR / "whale_cache.json"
    if not path.exists():
        return {}
    with open(path) as f:
        data = json.load(f)
    # Keys are slugs like btc-updown-5m-{start_ts}
    parsed = {}
    for slug, info in data.items():
        try:
            start_ts = int(slug.split("-")[-1])
            parsed[slug] = {
                "open_ts": start_ts,
                "close_ts": start_ts + 300,
                "resolution": info.get("resolution"),
                "direction": info.get("direction_from_boundaries"),
                "trades": info.get("trades", []),
            }
        except (ValueError, IndexError):
            continue
    return parsed


# ─── Window Matching ─────────────────────────────────────────────

def match_windows(kalshi, poly):
    """Match Kalshi and Polymarket windows by overlapping time periods."""
    matches = []

    # Build lookup by open_ts for both
    kalshi_by_ts = {}
    for tk, info in kalshi.items():
        kalshi_by_ts[info["open_ts"]] = (tk, info)

    poly_by_ts = {}
    for slug, info in poly.items():
        poly_by_ts[info["open_ts"]] = (slug, info)

    # Find exact matches (same 15-min window start)
    common_ts = set(kalshi_by_ts.keys()) & set(poly_by_ts.keys())

    for ts in sorted(common_ts):
        k_ticker, k_info = kalshi_by_ts[ts]
        p_slug, p_info = poly_by_ts[ts]
        matches.append({
            "open_ts": ts,
            "close_ts": ts + 900,
            "kalshi_ticker": k_ticker,
            "kalshi": k_info,
            "poly_slug": p_slug,
            "poly": p_info,
        })

    return matches


# ─── Analysis 1: Price Discrepancy ──────────────────────────────

def analyze_price_discrepancy(matches):
    """Compare yes/no prices between platforms for the same window."""
    print("\n" + "=" * 60)
    print("ANALYSIS 1: PRICE DISCREPANCY")
    print("=" * 60)

    discrepancies = []

    for m in matches:
        k_trades = m["kalshi"]["trades"]
        p_trades = m["poly"]["trades"]

        if not k_trades or not p_trades:
            continue

        # Kalshi prices are in cents (1-99), Polymarket in decimals (0.01-0.99)
        k_prices = []
        for t in k_trades:
            p = t.get("yes_price", t.get("price"))
            if p:
                k_prices.append(float(p) / 100 if float(p) > 1 else float(p))

        p_prices = []
        for t in p_trades:
            p = t.get("price")
            if p:
                p_prices.append(float(p))

        if not k_prices or not p_prices:
            continue

        k_avg = sum(k_prices) / len(k_prices)
        p_avg = sum(p_prices) / len(p_prices)
        spread = abs(k_avg - p_avg)

        discrepancies.append({
            "window_ts": m["open_ts"],
            "kalshi_avg": k_avg,
            "poly_avg": p_avg,
            "spread": spread,
            "k_trades": len(k_trades),
            "p_trades": len(p_trades),
            "kalshi_result": m["kalshi"].get("result"),
            "poly_result": m["poly"].get("resolution"),
        })

    if not discrepancies:
        print("  No matched windows with trades on both platforms.")
        return []

    discrepancies.sort(key=lambda x: -x["spread"])

    print(f"\n  Matched windows with trades on both: {len(discrepancies)}")

    avg_spread = sum(d["spread"] for d in discrepancies) / len(discrepancies)
    print(f"  Average price spread: {avg_spread:.4f} ({avg_spread*100:.2f}%)")

    max_spread = discrepancies[0]
    print(f"  Max spread: {max_spread['spread']:.4f} at ts={max_spread['window_ts']}")
    print(f"    Kalshi avg={max_spread['kalshi_avg']:.3f}, Poly avg={max_spread['poly_avg']:.3f}")

    # Distribution
    buckets = {"<1%": 0, "1-2%": 0, "2-5%": 0, "5-10%": 0, ">10%": 0}
    for d in discrepancies:
        s = d["spread"] * 100
        if s < 1: buckets["<1%"] += 1
        elif s < 2: buckets["1-2%"] += 1
        elif s < 5: buckets["2-5%"] += 1
        elif s < 10: buckets["5-10%"] += 1
        else: buckets[">10%"] += 1

    print(f"\n  Spread distribution:")
    for k, v in buckets.items():
        print(f"    {k}: {v} windows ({v/len(discrepancies)*100:.1f}%)")

    # Arb-exploitable windows (spread > 2%)
    exploitable = [d for d in discrepancies if d["spread"] > 0.02]
    print(f"\n  Exploitable (>2% spread): {len(exploitable)} windows")
    for d in exploitable[:10]:
        ts_str = datetime.fromtimestamp(d["window_ts"], tz=timezone.utc).strftime("%m/%d %H:%M")
        print(f"    {ts_str}: K={d['kalshi_avg']:.3f} P={d['poly_avg']:.3f} "
              f"spread={d['spread']:.3f} ({d['k_trades']}/{d['p_trades']} trades)")

    return discrepancies


# ─── Analysis 2: Timing Correlation ─────────────────────────────

def analyze_timing_correlation(matches):
    """Look for trades that happen within seconds on both platforms."""
    print("\n" + "=" * 60)
    print("ANALYSIS 2: TIMING CORRELATION")
    print("=" * 60)

    cross_trades = []
    WINDOW_SEC = 5  # trades within 5 seconds

    for m in matches:
        k_trades = m["kalshi"]["trades"]
        p_trades = m["poly"]["trades"]

        if not k_trades or not p_trades:
            continue

        # Parse timestamps
        k_times = []
        for t in k_trades:
            ts = t.get("created_time") or t.get("trade_id", "")
            try:
                if isinstance(ts, str) and "T" in ts:
                    k_times.append((datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp(), t))
                elif isinstance(ts, (int, float)):
                    k_times.append((float(ts), t))
            except:
                pass

        p_times = []
        for t in p_trades:
            ts = t.get("timestamp")
            try:
                if ts:
                    p_times.append((float(ts), t))
            except:
                pass

        if not k_times or not p_times:
            continue

        k_times.sort(key=lambda x: x[0])
        p_times.sort(key=lambda x: x[0])

        # Find pairs within WINDOW_SEC seconds
        ki, pi = 0, 0
        for k_ts, k_trade in k_times:
            for p_ts, p_trade in p_times:
                if abs(k_ts - p_ts) <= WINDOW_SEC:
                    cross_trades.append({
                        "window_ts": m["open_ts"],
                        "kalshi_ts": k_ts,
                        "poly_ts": p_ts,
                        "delta_sec": abs(k_ts - p_ts),
                        "kalshi_trade": k_trade,
                        "poly_trade": p_trade,
                    })

    print(f"\n  Cross-platform trades within {WINDOW_SEC}s: {len(cross_trades)}")

    if not cross_trades:
        return []

    # Sort by delta
    cross_trades.sort(key=lambda x: x["delta_sec"])

    # Stats
    deltas = [ct["delta_sec"] for ct in cross_trades]
    print(f"  Avg delta: {sum(deltas)/len(deltas):.2f}s")
    print(f"  Min delta: {min(deltas):.2f}s")

    # Distribution
    buckets = {"<1s": 0, "1-2s": 0, "2-3s": 0, "3-5s": 0}
    for d in deltas:
        if d < 1: buckets["<1s"] += 1
        elif d < 2: buckets["1-2s"] += 1
        elif d < 3: buckets["2-3s"] += 1
        else: buckets["3-5s"] += 1

    print(f"\n  Delta distribution:")
    for k, v in buckets.items():
        pct = v / len(deltas) * 100 if deltas else 0
        print(f"    {k}: {v} pairs ({pct:.1f}%)")

    # Windows with most cross-platform activity
    window_counts = defaultdict(int)
    for ct in cross_trades:
        window_counts[ct["window_ts"]] += 1

    top_windows = sorted(window_counts.items(), key=lambda x: -x[1])[:10]
    print(f"\n  Top windows by cross-platform pairs:")
    for ts, cnt in top_windows:
        ts_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%m/%d %H:%M")
        print(f"    {ts_str}: {cnt} synchronized trade pairs")

    # Polymarket wallets involved in cross-trades
    wallet_cross = defaultdict(int)
    for ct in cross_trades:
        w = ct["poly_trade"].get("proxyWallet", ct["poly_trade"].get("maker", ""))
        if w:
            wallet_cross[w] += 1

    top_wallets = sorted(wallet_cross.items(), key=lambda x: -x[1])[:20]
    print(f"\n  Top Polymarket wallets in cross-platform trades:")
    for w, cnt in top_wallets:
        name = ""
        # Try to find the name from any cross trade
        for ct in cross_trades:
            pt = ct["poly_trade"]
            if pt.get("proxyWallet") == w or pt.get("maker") == w:
                name = pt.get("name", pt.get("pseudonym", ""))
                break
        print(f"    {w[:12]}... ({name[:20]}): {cnt} cross-pairs")

    return cross_trades


# ─── Analysis 3: Volume Surge at Boundaries ─────────────────────

def analyze_boundary_surges(matches):
    """Check if trades cluster at window open/close on both platforms."""
    print("\n" + "=" * 60)
    print("ANALYSIS 3: BOUNDARY VOLUME SURGES")
    print("=" * 60)

    # For each matched window, bin trades into first/middle/last 5 minutes
    k_first5 = 0
    k_mid5 = 0
    k_last5 = 0
    p_first5 = 0
    p_mid5 = 0
    p_last5 = 0

    n_windows = 0
    for m in matches:
        open_ts = m["open_ts"]
        close_ts = m["close_ts"]
        mid1 = open_ts + 300
        mid2 = open_ts + 600

        k_trades = m["kalshi"]["trades"]
        p_trades = m["poly"]["trades"]

        if not k_trades or not p_trades:
            continue

        n_windows += 1

        # Kalshi timing
        for t in k_trades:
            ts = t.get("created_time", "")
            try:
                if isinstance(ts, str) and "T" in ts:
                    tts = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
                elif isinstance(ts, (int, float)):
                    tts = float(ts)
                else:
                    continue
                if tts < mid1:
                    k_first5 += 1
                elif tts < mid2:
                    k_mid5 += 1
                else:
                    k_last5 += 1
            except:
                pass

        # Polymarket timing
        for t in p_trades:
            ts = t.get("timestamp")
            try:
                tts = float(ts)
                if tts < mid1:
                    p_first5 += 1
                elif tts < mid2:
                    p_mid5 += 1
                else:
                    p_last5 += 1
            except:
                pass

    print(f"\n  Windows analyzed: {n_windows}")
    k_total = k_first5 + k_mid5 + k_last5
    p_total = p_first5 + p_mid5 + p_last5

    if k_total > 0:
        print(f"\n  Kalshi trade distribution (across 15-min window):")
        print(f"    First 5 min:  {k_first5:>8,} ({k_first5/k_total*100:.1f}%)")
        print(f"    Middle 5 min: {k_mid5:>8,} ({k_mid5/k_total*100:.1f}%)")
        print(f"    Last 5 min:   {k_last5:>8,} ({k_last5/k_total*100:.1f}%)")

    if p_total > 0:
        print(f"\n  Polymarket trade distribution (across 15-min window):")
        print(f"    First 5 min:  {p_first5:>8,} ({p_first5/p_total*100:.1f}%)")
        print(f"    Middle 5 min: {p_mid5:>8,} ({p_mid5/p_total*100:.1f}%)")
        print(f"    Last 5 min:   {p_last5:>8,} ({p_last5/p_total*100:.1f}%)")

    if k_total > 0 and p_total > 0:
        k_last_pct = k_last5 / k_total
        p_last_pct = p_last5 / p_total
        print(f"\n  Both platforms show {'SIMILAR' if abs(k_last_pct - p_last_pct) < 0.05 else 'DIFFERENT'} "
              f"late-window concentration")
        print(f"  This {'supports' if k_last_pct > 0.4 and p_last_pct > 0.4 else 'does not support'} "
              f"the latency arb hypothesis on both platforms")


# ─── Analysis 4: Directional Divergence ─────────────────────────

def analyze_directional_divergence(matches):
    """Check if prices diverge between platforms (one leads the other)."""
    print("\n" + "=" * 60)
    print("ANALYSIS 4: DIRECTIONAL DIVERGENCE / PRICE LEAD")
    print("=" * 60)

    lead_kalshi = 0
    lead_poly = 0
    same = 0
    analyzed = 0

    for m in matches:
        k_trades = m["kalshi"]["trades"]
        p_trades = m["poly"]["trades"]
        if not k_trades or not p_trades:
            continue

        # Get early-window prices (first 3 minutes) vs late-window
        open_ts = m["open_ts"]
        early_cutoff = open_ts + 180  # first 3 min

        def get_early_late_bias(trades, ts_key, price_key, is_kalshi=False):
            early_up = 0
            late_up = 0
            for t in trades:
                try:
                    if is_kalshi:
                        ts_val = t.get("created_time", "")
                        if isinstance(ts_val, str) and "T" in ts_val:
                            tts = datetime.fromisoformat(ts_val.replace("Z", "+00:00")).timestamp()
                        else:
                            continue
                        price = float(t.get("yes_price", 50))
                        if price > 1:
                            price = price / 100  # cents to decimal
                    else:
                        tts = float(t.get("timestamp", 0))
                        price = float(t.get("price", 0.5))

                    if tts < early_cutoff:
                        early_up += (price - 0.5)
                    else:
                        late_up += (price - 0.5)
                except:
                    pass
            return early_up, late_up

        k_early, k_late = get_early_late_bias(k_trades, "created_time", "yes_price", True)
        p_early, p_late = get_early_late_bias(p_trades, "timestamp", "price", False)

        # Which platform moved first?
        # If Kalshi early bias > Polymarket early bias, Kalshi leads
        k_signal = 1 if k_early > 0 else (-1 if k_early < 0 else 0)
        p_signal = 1 if p_early > 0 else (-1 if p_early < 0 else 0)

        analyzed += 1
        if k_signal != 0 and p_signal != 0:
            if k_signal == p_signal:
                same += 1
            elif abs(k_early) > abs(p_early):
                lead_kalshi += 1
            else:
                lead_poly += 1

    print(f"\n  Windows analyzed: {analyzed}")
    total = lead_kalshi + lead_poly + same
    if total > 0:
        print(f"  Kalshi leads:     {lead_kalshi} ({lead_kalshi/total*100:.1f}%)")
        print(f"  Polymarket leads: {lead_poly} ({lead_poly/total*100:.1f}%)")
        print(f"  Same direction:   {same} ({same/total*100:.1f}%)")

        if lead_kalshi > lead_poly * 1.5:
            print(f"\n  >>> FINDING: Kalshi appears to LEAD Polymarket — arb flows Kalshi → Polymarket")
        elif lead_poly > lead_kalshi * 1.5:
            print(f"\n  >>> FINDING: Polymarket appears to LEAD Kalshi — arb flows Polymarket → Kalshi")
        else:
            print(f"\n  >>> FINDING: No clear leader — prices converge simultaneously (efficient market)")


# ─── Analysis 5: Wallet Behavioral Profiling ─────────────────────

def analyze_wallet_profiles(matches, poly_5m):
    """Profile Polymarket wallets that consistently trade on the same windows as Kalshi."""
    print("\n" + "=" * 60)
    print("ANALYSIS 5: POLYMARKET WALLET BEHAVIORAL PROFILING")
    print("=" * 60)

    # Track which wallets trade on windows with high Kalshi activity
    wallet_windows = defaultdict(lambda: {"wins": 0, "total": 0, "sizes": [], "timings": []})

    for m in matches:
        k_trades = m["kalshi"]["trades"]
        p_trades = m["poly"]["trades"]
        if not k_trades or not p_trades:
            continue

        # What was the Kalshi consensus? (majority direction of Kalshi trades)
        k_yes = sum(1 for t in k_trades if t.get("taker_side") == "yes")
        k_no = len(k_trades) - k_yes
        kalshi_consensus = "up" if k_yes > k_no else "down"

        resolution = m["poly"].get("resolution", "")

        token_map = m["poly"].get("token_map", {})
        # Invert: token_id -> direction
        inv_map = {}
        for tid, direction in token_map.items():
            inv_map[tid] = direction.lower()

        for t in p_trades:
            wallet = t.get("proxyWallet", t.get("maker", ""))
            if not wallet:
                continue

            # Determine trade direction
            asset = t.get("asset", "")
            side = t.get("side", "").upper()
            outcome = (t.get("outcome", "") or "").lower()

            if outcome in ("up", "down"):
                direction = outcome if side == "BUY" else ("down" if outcome == "up" else "up")
            elif asset in inv_map:
                direction = inv_map[asset] if side == "BUY" else ("down" if inv_map[asset] == "up" else "up")
            else:
                continue

            won = (direction == resolution) if resolution else None

            size = float(t.get("size", 0))
            ts = float(t.get("timestamp", 0))
            time_in_window = ts - m["open_ts"]

            w = wallet_windows[wallet]
            if won is not None:
                w["total"] += 1
                if won:
                    w["wins"] += 1
            w["sizes"].append(size)
            w["timings"].append(time_in_window)

    # Filter to wallets with enough activity
    active = {w: d for w, d in wallet_windows.items() if d["total"] >= 20}

    print(f"\n  Total wallets on matched windows: {len(wallet_windows)}")
    print(f"  Wallets with 20+ trades: {len(active)}")

    if not active:
        return

    # Sort by win rate
    scored = []
    for w, d in active.items():
        wr = d["wins"] / d["total"] if d["total"] > 0 else 0
        avg_size = sum(d["sizes"]) / len(d["sizes"]) if d["sizes"] else 0
        avg_timing = sum(d["timings"]) / len(d["timings"]) if d["timings"] else 0
        late_trades = sum(1 for t in d["timings"] if t > 600)  # last 5 min
        late_pct = late_trades / len(d["timings"]) if d["timings"] else 0
        scored.append({
            "wallet": w,
            "wins": d["wins"],
            "total": d["total"],
            "wr": wr,
            "avg_size": avg_size,
            "avg_timing_sec": avg_timing,
            "late_pct": late_pct,
        })

    scored.sort(key=lambda x: -x["wr"])

    # Top arb suspects: high WR + late trading
    print(f"\n  TOP ARB SUSPECTS (high WR + active on Kalshi-matched windows):")
    print(f"  {'Wallet':<16} {'WR':>6} {'Trades':>7} {'Avg$':>7} {'AvgT':>6} {'Late%':>6}")
    print(f"  {'-'*16} {'-'*6} {'-'*7} {'-'*7} {'-'*6} {'-'*6}")

    for s in scored[:30]:
        print(f"  {s['wallet'][:16]} {s['wr']:>5.1%} {s['total']:>7} "
              f"${s['avg_size']:>5.0f} {s['avg_timing_sec']:>5.0f}s {s['late_pct']:>5.1%}")

    # Correlate: wallets that trade late AND have high WR = likely arb
    arb_suspects = [s for s in scored if s["wr"] > 0.60 and s["late_pct"] > 0.40 and s["total"] >= 30]
    print(f"\n  STRONG ARB SUSPECTS (WR>60%, late>40%, 30+ trades): {len(arb_suspects)}")
    for s in arb_suspects[:15]:
        print(f"    {s['wallet'][:16]} WR={s['wr']:.1%} trades={s['total']} "
              f"late={s['late_pct']:.0%} avg${s['avg_size']:.0f}")

    return scored


# ─── Analysis 6: Resolution Concordance ─────────────────────────

def analyze_resolution_concordance(matches):
    """Check if both platforms resolve the same way (they should — same BTC price)."""
    print("\n" + "=" * 60)
    print("ANALYSIS 6: RESOLUTION CONCORDANCE")
    print("=" * 60)

    agree = 0
    disagree = 0
    unknown = 0

    for m in matches:
        k_result = m["kalshi"].get("result", "")
        p_result = m["poly"].get("resolution", "")

        if not k_result or not p_result:
            unknown += 1
            continue

        # Normalize
        k_dir = "up" if k_result.lower() in ("yes", "up") else "down" if k_result.lower() in ("no", "down") else k_result.lower()
        p_dir = p_result.lower()

        if k_dir == p_dir:
            agree += 1
        else:
            disagree += 1

    total = agree + disagree
    print(f"\n  Matched windows: {agree + disagree + unknown}")
    print(f"  Both resolved:   {total}")
    print(f"  Agreement:       {agree} ({agree/total*100:.1f}%)" if total > 0 else "")
    print(f"  Disagreement:    {disagree} ({disagree/total*100:.1f}%)" if total > 0 else "")

    if disagree > 0 and total > 0:
        print(f"\n  >>> WARNING: {disagree} windows resolved DIFFERENTLY!")
        print(f"  This could be due to different price feeds, rounding, or window offset.")
        # Show disagreements
        for m in matches:
            k_result = (m["kalshi"].get("result", "") or "").lower()
            p_result = (m["poly"].get("resolution", "") or "").lower()
            if k_result and p_result:
                k_dir = "up" if k_result in ("yes", "up") else "down" if k_result in ("no", "down") else k_result
                if k_dir != p_result:
                    ts_str = datetime.fromtimestamp(m["open_ts"], tz=timezone.utc).strftime("%m/%d %H:%M")
                    print(f"    {ts_str}: Kalshi={k_result} Polymarket={p_result}")
                    if len([1 for mm in matches if mm]) > 10:
                        break  # limit output


# ─── Main ────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("CROSS-PLATFORM ARBITRAGE: POLYMARKET vs KALSHI")
    print("15-Minute BTC Up/Down Markets — Feb 19-25, 2026")
    print("=" * 60)

    print("\n[1] Loading data...")
    kalshi = load_kalshi()
    poly = load_polymarket()
    poly_5m = load_polymarket_5m()
    print(f"  Polymarket 5m (context): {len(poly_5m)} windows")

    print(f"\n[2] Matching windows...")
    matches = match_windows(kalshi, poly)
    print(f"  Matched windows: {len(matches)}")

    if not matches:
        print("\n  WARNING: No matching windows found!")
        print("  This could mean the window timestamps don't align.")
        # Debug: show first few timestamps from each
        k_ts = sorted([info["open_ts"] for info in kalshi.values()])[:5]
        p_ts = sorted([info["open_ts"] for info in poly.values()])[:5]
        print(f"  Kalshi first 5 open_ts: {k_ts}")
        print(f"  Poly first 5 open_ts:   {p_ts}")
        if k_ts and p_ts:
            offset = k_ts[0] - p_ts[0]
            print(f"  Offset: {offset}s = {offset/60:.1f} min")
            print(f"  Kalshi: {datetime.fromtimestamp(k_ts[0], tz=timezone.utc)}")
            print(f"  Poly:   {datetime.fromtimestamp(p_ts[0], tz=timezone.utc)}")
        return

    print(f"\n[3] Running analyses...")
    discrepancies = analyze_price_discrepancy(matches)
    cross_trades = analyze_timing_correlation(matches)
    analyze_boundary_surges(matches)
    analyze_directional_divergence(matches)
    wallet_scores = analyze_wallet_profiles(matches, poly_5m)
    analyze_resolution_concordance(matches)

    # ── Save results ─────────────────────────────────────────────
    results = {
        "summary": {
            "kalshi_markets": len(kalshi),
            "poly_markets": len(poly),
            "matched_windows": len(matches),
            "price_discrepancies": len(discrepancies),
            "cross_platform_pairs": len(cross_trades),
        },
        "top_discrepancies": discrepancies[:50] if discrepancies else [],
        "cross_trades_sample": [{
            "window_ts": ct["window_ts"],
            "delta_sec": ct["delta_sec"],
            "poly_wallet": ct["poly_trade"].get("proxyWallet", "")[:20],
        } for ct in (cross_trades[:100] if cross_trades else [])],
        "wallet_scores": (wallet_scores[:50] if wallet_scores else []),
    }

    out = DATA_DIR / "cross_platform_results.json"
    with open(out, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\n{'='*60}")
    print(f"RESULTS SAVED: {out}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
