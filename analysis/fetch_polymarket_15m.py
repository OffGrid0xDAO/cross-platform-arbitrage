#!/usr/bin/env python3
"""
Fetch Polymarket 15-min BTC up/down trades for Feb 19-25, 2026.

Uses per-market JSON files in data/poly15m/ to avoid massive checkpoint writes.
Loads existing cache from polymarket_15m_cache.json if present, then switches
to per-file storage for remaining markets.
"""
import json, os, sys, time
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    os.system(f"{sys.executable} -m pip install requests -q")
    import requests

# ─── Config ──────────────────────────────────────────────────────
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API  = "https://data-api.polymarket.com"
DATA_DIR  = Path(__file__).resolve().parent.parent / "data"
CACHE_FILE = DATA_DIR / "polymarket_15m_cache.json"
MARKET_DIR = DATA_DIR / "poly15m"
MARKET_DIR.mkdir(exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})

# Feb 19-25, 2026 UTC
START = datetime(2026, 2, 19, 0, 0, 0, tzinfo=timezone.utc)
END   = datetime(2026, 2, 26, 0, 0, 0, tzinfo=timezone.utc)

# ─── Generate 15-min windows ────────────────────────────────────
def generate_windows():
    windows = []
    current = START
    while current < END:
        start_ts = int(current.timestamp())
        windows.append({
            "start_ts": start_ts,
            "end_ts": start_ts + 900,
            "start_utc": current.isoformat(),
            "slug": f"btc-updown-15m-{start_ts}",
        })
        current += timedelta(minutes=15)
    return windows


# ─── Gamma API: slug → condition_id + token_map ─────────────────
def fetch_market_info(slug, retries=2):
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
            }
        except Exception:
            if attempt < retries - 1:
                time.sleep(1)
    return None


# ─── Data API: condition_id → trades ─────────────────────────────
def fetch_trades(condition_id, max_pages=30):
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


# ─── Per-market file I/O ─────────────────────────────────────────
def market_file(slug):
    return MARKET_DIR / f"{slug}.json"


def is_cached(slug):
    return market_file(slug).exists()


def save_market(slug, data):
    with open(market_file(slug), "w") as f:
        json.dump(data, f)


def load_market(slug):
    p = market_file(slug)
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return None


# ─── Migrate old cache to per-file ──────────────────────────────
def migrate_old_cache():
    """If a big polymarket_15m_cache.json exists, split into per-market files."""
    if not CACHE_FILE.exists():
        return 0

    sz = CACHE_FILE.stat().st_size
    if sz < 100:
        return 0

    print(f"  Migrating old cache ({sz/1024/1024:.0f} MB) to per-market files...")
    try:
        with open(CACHE_FILE) as f:
            cache = json.load(f)
    except Exception as e:
        print(f"  Failed to load old cache: {e}")
        return 0

    migrated = 0
    for slug, data in cache.items():
        if not is_cached(slug):
            save_market(slug, data)
            migrated += 1

    print(f"  Migrated {migrated} markets to {MARKET_DIR}/")
    # Rename old cache to avoid re-migration
    CACHE_FILE.rename(CACHE_FILE.with_suffix(".json.bak"))
    return migrated


# ─── Main ────────────────────────────────────────────────────────
def main():
    windows = generate_windows()
    print(f"{'='*60}")
    print(f"POLYMARKET 15-MIN BTC TRADE FETCHER (v2 - per-file)")
    print(f"Period: Feb 19-25, 2026")
    print(f"Windows: {len(windows)} (every 15 min)")
    print(f"{'='*60}")

    # Migrate old cache if present
    migrate_old_cache()

    # Check what's already cached
    to_fetch = [w for w in windows if not is_cached(w["slug"])]
    already = len(windows) - len(to_fetch)
    print(f"\n[*] {already} windows cached, {len(to_fetch)} to fetch")

    if not to_fetch:
        print("[+] All windows already cached!")
        summarize(windows)
        return

    found = 0
    not_found = 0
    total_trades = 0

    for i, w in enumerate(to_fetch):
        slug = w["slug"]

        info = fetch_market_info(slug)
        if not info:
            save_market(slug, {"exists": False, "trades": []})
            not_found += 1
        else:
            trades = fetch_trades(info["condition_id"])
            save_market(slug, {
                "exists": True,
                "condition_id": info["condition_id"],
                "token_map": info["token_map"],
                "resolution": info["resolution"],
                "trades": trades,
            })
            total_trades += len(trades)
            found += 1

        if (i + 1) % 20 == 0 or i == len(to_fetch) - 1:
            print(f"  [{i+1}/{len(to_fetch)}] found={found} missing={not_found} "
                  f"new_trades={total_trades:,} | {slug}")

        time.sleep(0.15)

    print(f"\n[+] Done! {found} found, {not_found} missing, {total_trades:,} new trades")
    summarize(windows)


def summarize(windows):
    """Summarize all per-market files."""
    total_trades = 0
    markets_found = 0
    markets_missing = 0
    resolutions = {}
    trade_counts = []

    for w in windows:
        slug = w["slug"]
        data = load_market(slug)
        if not data:
            continue
        if data.get("exists"):
            markets_found += 1
            trades = data.get("trades", [])
            total_trades += len(trades)
            trade_counts.append((slug, len(trades)))
            res = data.get("resolution", "unknown")
            resolutions[res] = resolutions.get(res, 0) + 1
        else:
            markets_missing += 1

    trade_counts.sort(key=lambda x: -x[1])

    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"  Markets found:       {markets_found}")
    print(f"  Markets missing:     {markets_missing}")
    print(f"  Total trades:        {total_trades:,}")
    print(f"  Resolutions:         {resolutions}")

    if trade_counts:
        print(f"\n  Top 10 by trade count:")
        for slug, cnt in trade_counts[:10]:
            data = load_market(slug)
            res = data.get("resolution", "?") if data else "?"
            print(f"    {slug}: {cnt} trades, resolved={res}")

        if total_trades > 0:
            sample_slug = next((s for s, c in trade_counts if c > 0), None)
            if sample_slug:
                data = load_market(sample_slug)
                if data and data.get("trades"):
                    sample = data["trades"][0]
                    print(f"\n  Sample trade fields: {list(sample.keys())}")

    # Save summary
    summary = {
        "total_windows": len(windows),
        "markets_found": markets_found,
        "markets_missing": markets_missing,
        "total_trades": total_trades,
        "resolutions": resolutions,
    }
    summary_file = DATA_DIR / "polymarket_15m_summary.json"
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\n  Summary: {summary_file}")


if __name__ == "__main__":
    main()
