# Data

## Included Files

| File | Size | Description |
|------|------|-------------|
| `boundaries.csv` | 320 KB | 5-minute BTC window boundaries (1,599 windows) with open/close timestamps and Binance BTC reference prices |
| `cross_arb_15m_results.json` | 108 KB | Window-by-window cross-platform comparison results: price divergences, temporal symmetries, and amount-matched trade pairs across Polymarket and Kalshi 15-min BTC markets |
| `pnl_stationarity_losses.json` | 4.6 KB | P&L decomposition by strategy, temporal stationarity metrics, and loss distribution analysis |
| `whale_analysis.json` | 1.4 MB | Per-wallet analysis of 26,475 Polymarket 5-min BTC wallets: win rates, timing distributions, trade counts, strategy classifications, and alpha scores |
| `proxy_owners.json` | 3.7 KB | Mapping of Polymarket proxy wallet addresses to their underlying operator addresses via PolyProxy contract resolution |
| `multi_bridge_scan.json` | 31 KB | Cross-chain bridge transaction scan results for arbitrage suspect wallets across Wormhole, Relay, and deBridge protocols |
| `solana_links.json` | 90 KB | Solana-side wallet activity linked to Polymarket arbitrage wallets via bridge deposits and withdrawals |

## Large Files Not Included

These files are required to reproduce the full analysis from raw data but are too large for the repository:

| File | Size | Description | How to Reproduce |
|------|------|-------------|------------------|
| `whale_cache.json` | ~180 MB | Full trade-level dataset: 2,182,329 trades across 26,475 wallets in 642 5-min BTC windows | Run `analysis/analyze_5m_whales.py` (fetches from Polymarket APIs) |
| `poly15m/` (directory) | ~200 MB | Per-market JSON files for 672 Polymarket 15-min BTC markets totaling 2,536,367 trades | Run `analysis/fetch_polymarket_15m.py` |
| `kalshi_btc_trades.json` | ~120 MB | 1,680,127 Kalshi KXBTC15M trades across 672 matched windows | Run `analysis/fetch_kalshi_data.py` (requires Kalshi API credentials) |

## Date Range

All data covers **February 19--25, 2026** (one full week).

## Data Sources

- **Polymarket**: Public trade data via [Gamma Markets API](https://gamma-api.polymarket.com) and [Data API](https://data-api.polymarket.com)
- **Kalshi**: Trade data via [Kalshi API](https://trading-api.kalshi.com) (requires authentication)
- **Binance**: BTC/USDT spot prices via public klines API
- **On-chain**: Etherscan, PolygonScan, Solscan APIs for wallet and bridge tracing
