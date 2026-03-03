#!/usr/bin/env python3
"""Fetch Kalshi BTC 15-min trade data for Feb 19-25, 2026.

Strategy:
1. Paginate all settled KXBTC15M markets
2. Filter to our date range (Feb 19-25)
3. Fetch trades per ticker (the only reliable way to get BTC trades)
"""
import json, os, sys, time
from datetime import datetime, timezone

try:
    import requests
except ImportError:
    print("Installing requests...")
    os.system(f"{sys.executable} -m pip install requests -q")
    import requests

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

BASE = "https://api.elections.kalshi.com/trade-api/v2"
HEADERS = {"Accept": "application/json", "User-Agent": "research/1.0"}

# Feb 19-25 2026 UTC
START_TS = 1771459200   # 2026-02-19 00:00:00 UTC
END_TS   = 1771977600   # 2026-02-25 00:00:00 UTC
END_TS_PLUS = END_TS + 86400  # include Feb 25 markets that close by Feb 26

def get(path, params=None):
    url = f"{BASE}{path}"
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            return r.json(), 200
        else:
            print(f"  HTTP {r.status_code}: {r.text[:200]}")
            return None, r.status_code
    except Exception as e:
        print(f"  Error: {e}")
        return None, 0

def paginate(path, params, key="trades", max_pages=100):
    all_items = []
    cursor = None
    for page in range(max_pages):
        p = dict(params)
        if cursor:
            p["cursor"] = cursor
        data, code = get(path, p)
        if not data:
            break
        items = data.get(key, [])
        all_items.extend(items)
        cursor = data.get("cursor")
        if not cursor or not items:
            break
        if page % 10 == 9:
            print(f"    ... page {page+1}, {len(all_items)} items so far")
        time.sleep(0.25)
    return all_items

print("=" * 60)
print("KALSHI BTC 15-MIN DATA FETCHER")
print(f"Period: Feb 19-25, 2026")
print("=" * 60)

result = {"markets": [], "trades_by_ticker": {}, "stats": {}}

# ── Step 1: Fetch ALL settled KXBTC15M markets ──────────────────
print("\n[1/3] Fetching settled KXBTC15M markets...")
all_markets = paginate(
    "/markets",
    {"series_ticker": "KXBTC15M", "status": "settled", "limit": "200"},
    key="markets",
    max_pages=200
)
print(f"  Total settled KXBTC15M markets: {len(all_markets)}")

# Filter to our date range
in_range = []
for m in all_markets:
    ct = m.get("close_time", "")
    if ct:
        try:
            ts = datetime.fromisoformat(ct.replace("Z", "+00:00")).timestamp()
            if START_TS <= ts <= END_TS_PLUS:
                in_range.append(m)
        except:
            pass

print(f"  In date range (Feb 19-25): {len(in_range)} markets")
if in_range:
    # Sort by close time
    in_range.sort(key=lambda m: m.get("close_time", ""))
    print(f"  First: {in_range[0].get('ticker')} close={in_range[0].get('close_time')}")
    print(f"  Last:  {in_range[-1].get('ticker')} close={in_range[-1].get('close_time')}")

    # Show volume stats
    vols = [m.get("volume", 0) for m in in_range]
    print(f"  Volume: total={sum(vols):,}, avg={sum(vols)/len(vols):,.0f}, max={max(vols):,}")

    # Store market metadata
    for m in in_range:
        result["markets"].append({
            "ticker": m.get("ticker"),
            "title": m.get("title"),
            "close_time": m.get("close_time"),
            "volume": m.get("volume", 0),
            "yes_bid": m.get("yes_bid"),
            "yes_ask": m.get("yes_ask"),
            "result": m.get("result"),
            "open_time": m.get("open_time"),
        })

# ── Step 2: Fetch trades for each ticker ────────────────────────
tickers = [m.get("ticker") for m in in_range if m.get("ticker")]
print(f"\n[2/3] Fetching trades for {len(tickers)} markets...")

total_trades = 0
for i, tk in enumerate(tickers):
    trades = paginate("/markets/trades", {"ticker": tk, "limit": "1000"})
    if trades:
        result["trades_by_ticker"][tk] = trades
        total_trades += len(trades)

    # Progress every 20 tickers
    if (i + 1) % 20 == 0 or i == len(tickers) - 1:
        print(f"  [{i+1}/{len(tickers)}] {tk}: {len(trades)} trades (cumulative: {total_trades:,})")

    time.sleep(0.2)

print(f"\n  Total BTC trades fetched: {total_trades:,}")

# ── Step 3: Compute stats ───────────────────────────────────────
print(f"\n[3/3] Computing statistics...")

trade_counts = []
for tk, trades in result["trades_by_ticker"].items():
    trade_counts.append((tk, len(trades)))

trade_counts.sort(key=lambda x: -x[1])

print(f"\n  Top 20 markets by trade count:")
for tk, cnt in trade_counts[:20]:
    # Find market info
    mkt = next((m for m in result["markets"] if m["ticker"] == tk), {})
    print(f"    {tk}: {cnt} trades, vol={mkt.get('volume',0):,}, result={mkt.get('result','?')}")

# Aggregate timing stats
all_trades_flat = []
for tk, trades in result["trades_by_ticker"].items():
    for t in trades:
        t["_ticker"] = tk
        all_trades_flat.append(t)

if all_trades_flat:
    # Sort by timestamp
    all_trades_flat.sort(key=lambda t: t.get("created_time", t.get("trade_id", "")))

    # Price distribution
    yes_prices = [t.get("yes_price", 0) for t in all_trades_flat if t.get("yes_price")]
    no_prices = [t.get("no_price", 0) for t in all_trades_flat if t.get("no_price")]

    result["stats"] = {
        "total_markets": len(in_range),
        "total_trades": total_trades,
        "markets_with_trades": len(result["trades_by_ticker"]),
        "top_10_by_trades": trade_counts[:10],
        "sample_trade": all_trades_flat[0] if all_trades_flat else None,
        "avg_yes_price": sum(yes_prices) / len(yes_prices) if yes_prices else 0,
        "trade_fields": list(all_trades_flat[0].keys()) if all_trades_flat else [],
    }

    print(f"\n  Stats:")
    print(f"    Markets with trades: {len(result['trades_by_ticker'])}/{len(in_range)}")
    print(f"    Avg yes_price: {result['stats']['avg_yes_price']:.0f} cents")
    print(f"    Trade fields: {result['stats']['trade_fields']}")
    print(f"    Sample trade: {json.dumps(all_trades_flat[0], indent=2)[:400]}")

# ── Save ─────────────────────────────────────────────────────────
out = os.path.join(DATA_DIR, "kalshi_btc_trades.json")
with open(out, "w") as f:
    json.dump(result, f, indent=2, default=str)

sz = os.path.getsize(out)
print(f"\n{'='*60}")
print(f"SAVED: {out}")
print(f"SIZE: {sz/1024:.1f} KB ({sz/1024/1024:.1f} MB)")
print(f"MARKETS: {len(in_range)}")
print(f"TRADES: {total_trades:,}")
print(f"{'='*60}")
