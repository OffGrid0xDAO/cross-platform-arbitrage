# Polymarket 5-Min BTC Direction Prediction Bot

Algorithmic trading system for Polymarket's 5-minute BTC price direction prediction markets. Built in Rust for low-latency execution with Chainlink Data Streams, digital signal processing, and compound position sizing.

## Strategy Overview

The bot trades binary prediction markets that resolve based on whether the Chainlink oracle price moves UP or DOWN within a 5-minute window. The edge comes from a continuous EMA ribbon with price-majority direction, filtered by a 51-feature DSP quality score, noise veto, and momentum veto.

### Signal Architecture

**Primary Strategy: EMA Ribbon (t=90-110s)**

Direction is determined by price position relative to 4 Fibonacci EMAs [55, 89, 144, 233 seconds]:

| Price vs EMAs | Direction | Logic |
|---------------|-----------|-------|
| Above 3-4 of 4 EMAs | **UP** | Price above majority = bullish |
| Above 0-1 of 4 EMAs | **DOWN** | Price below majority = bearish |
| Above exactly 2 | Alignment fallback | Use EMA ordering if \|alignment\| >= 0.50, else skip |

The ribbon is **continuous** — EMA state carries across all windows without reset, providing persistent trend context.

**Filter Stack (applied after direction):**

| Filter | Condition | Effect |
|--------|-----------|--------|
| CL Staleness Gate | CL Streams or RTDS fresh (< 5s / 60s) | Blocks all trading when data stale |
| Noise Veto | `cl_noise <= 0.3 AND bn_noise <= 0.3` | Removes noisy market conditions |
| Momentum Veto | `pve_aligned >= -0.13` | Blocks when exchange momentum opposes bet |
| Quality Score | `qs >= 0.35` (7-feature weighted z-score) | Ensures sufficient signal quality |
| Ask Price Bounds | `$0.05 <= ask <= $0.69` | Avoids extreme fills |

**Secondary Strategy: Sniper (t=285-298s, disabled)**

Late-window cheap share entry ($0.01-$0.20 asks) with 3 DSP confirmation filters. Available but currently disabled pending more data collection.

## Backtest Results

### Full Dataset (1496 windows, all filters active)

| Metric | Value |
|--------|-------|
| Trades | 770 |
| Wins | 585 |
| Losses | 185 |
| Win Rate | 76.0% |
| EV per trade (@ $0.55 fill) | +$0.210 |
| Total P&L (@ $0.55 fill) | +$161.50 |
| Break-even WR (@ $0.55) | 55.0% |

### Last 24h (105 trades)

| Metric | Value |
|--------|-------|
| Trades | 105 |
| Win Rate | 76.2% |
| EV per trade | +$0.212 |
| P&L (@ $0.55 fill) | +$22.20 |

### Quality Score Features (7 weighted z-scores)

| Feature | Sign | Weight (Cohen's d) |
|---------|------|---------------------|
| `exchange_direction_count` | +1 (dir-normalized) | 0.481 |
| `indicator_score` | +1 | 0.344 |
| `cl_noise_level` | -1 | 0.337 |
| `bollinger_width` | +1 | 0.328 |
| `bn_noise_level` | -1 | 0.326 |
| `trade_intensity` | +1 | 0.230 |
| `ob_spread_vol` | -1 | 0.196 |

## Architecture

```
src/
├── main.rs              # CLI entry, process locking, mode selection
├── bot.rs               # Central orchestrator: strategy loop, stop-loss, execution
├── ribbon.rs            # EMA ribbon direction, quality score, noise veto, progression rules
├── dsp.rs               # 51-feature DSP engine (Daubechies-4 wavelet, multi-timeframe EMA, FFT)
├── feeds.rs             # Price feed store: CL Streams, RTDS, Binance, Coinbase, Kraken
├── chainlink_streams.rs # Chainlink Data Streams WebSocket (primary oracle feed)
├── chainlink_rpc.rs     # On-chain Chainlink polling (resolution verification only)
├── executor.rs          # Polymarket CLOB: market discovery, order placement, redemption
├── strategy.rs          # Window start price capture, multi-source signal helpers
├── kelly.rs             # Kelly criterion position sizing with DD limiter
├── config.rs            # All strategy parameters with defaults
├── data_logger.rs       # CSV logging: prices, DSP ticks, boundaries, trades
├── reconcile.rs         # On-chain P&L reconciliation via CTF contract events
├── server.rs            # Axum HTTP dashboard API
├── types.rs             # Direction, Signal, PriceTick, BookSnapshot
├── cfee.rs              # Legacy: logistic regression model (disabled)
├── conviction.rs        # Legacy: conviction strategy (disabled)
└── copy_trade.rs        # Legacy: copy-trade strategy (disabled)

frontend/                # React TypeScript monitoring dashboard
data/                    # CSV logs: price_log, boundaries, dsp_ticks, book_log
```

## Data Sources

| Source | Transport | Role | Freshness |
|--------|-----------|------|-----------|
| Chainlink Data Streams | WebSocket (direct) | **Primary** oracle feed for ribbon + DSP | Sub-second |
| Chainlink RTDS | WebSocket (Polymarket relay) | Fallback oracle feed | 1-2s |
| Chainlink On-Chain | Polygon RPC | Resolution verification only (excluded from ribbon) | ~30s |
| Binance | WebSocket (aggTrade) | DSP cross-feed features + trade flow (VPIN) | ~100ms |
| Coinbase | WebSocket v2 | Exchange consensus voting | ~1s |
| Kraken | WebSocket v2 | Exchange consensus voting | ~1s |
| Polymarket CLOB | REST + WebSocket | Order book, market discovery, execution | Real-time |

### Feed Priority for Ribbon

Only Chainlink Streams and RTDS feed the ribbon EMAs. On-chain Chainlink (~30s step-function updates) is deliberately excluded to prevent coarse data from corrupting the continuous EMA state. When both Streams and RTDS are stale, the bot halts trading rather than falling back to on-chain.

## Execution

### Order Types

| Strategy | Order Type | Method |
|----------|-----------|--------|
| Ribbon | GTC limit (2-phase ladder) | Walk up asks until filled |
| Sniper | FOK with retry | Immediate fill or cancel, retry up to 3x |
| Stop-loss | FOK → GTC sell ladder | Bid FOK, 70% bid FOK, 40% bid FOK, penny GTC |

### Position Management

- **priceToBeat crossover**: If CL persistently crosses wrong side of oracle start price, sell early (~83% recovery)
- **Pre-boundary marginal exit**: If CL move marginal in last 10s, sell while liquidity exists
- **Exchange final check**: If CL ambiguous + exchanges disagree in last 5s, sell
- **Last-resort dump**: Sell at any price in last 1.5s if losing indicators
- **Post-boundary informed exit**: If CL confirms loss 2-10s after boundary, sell before resolution

### Bet Sizing

Compound sizing: `balance × 5% × kelly_mult × dd_limiter`

| Ask Price | Kelly Multiplier | Effective Size |
|-----------|-----------------|----------------|
| <= $0.40 | 2.5x | 12.5% of bankroll |
| <= $0.48 | 2.0x | 10.0% |
| <= $0.55 | 1.5x | 7.5% |
| <= $0.65 | 1.2x | 6.0% |
| > $0.65 | 1.0x | 5.0% |

DD limiter reduces size when balance < peak (drawdown protection).

## Usage

```bash
# Data collection only (no trading, just log prices/DSP)
cargo run -- --collect --asset btc

# Dry run (simulated orders, real data feeds)
cargo run -- --dry-run --asset btc

# Live trading
cargo run -- --live --asset btc

# Dashboard at http://localhost:9090
```

## Cross-Platform Arbitrage Research

Companion research paper analyzing ~10.5 million trades across Polymarket, Kalshi, and Binance over 11 days (Feb 19 - Mar 2, 2026).

### Dataset

| Source | Period | Trades | Markets |
|--------|--------|--------|---------|
| Polymarket 5-min | Feb 19-25 | 2,182,329 | 642 windows |
| Polymarket 15-min (Week 1) | Feb 19-25 | 2,536,367 | 672 markets |
| Kalshi KXBTC15M (Week 1) | Feb 19-25 | 1,680,127 | 673 markets |
| Polymarket 15-min (Week 2) | Feb 25-Mar 2 | 2,215,260 | 576 markets |
| Kalshi KXBTC15M (Week 2) | Feb 25-Mar 2 | 1,894,658 | 633 markets |
| **Grand total** | | **~10.5M** | |

### Key Findings

**Week 1 (Primary Analysis):**
- **161,577 same-second trade pairs** between Polymarket and Kalshi
- **1,368 opposite-side amount-matched trades** (textbook arb signature)
- 29 wallets with >98% win rate on 5-min markets (latency arb via Binance)
- 5.7% oracle disagreement rate makes cross-platform hedging non-risk-free

**Week 2 (Out-of-Sample Validation):**
- **148,142 same-second pairs** (structural synchronization persists)
- **357,724 opposite-side matches** across four tiered tolerance levels (exact ±5%/±2s through loose ±20%/±15s)
- 20 wallets with 100% Kalshi alignment, wallet persistence across both weeks
- **Declining PnL**: -$31/window/day trend; latency arb turns negative (-$0.31/trade vs +$0.49 in week 1)
- Market making alone remains stable (~$0.20/trade both weeks)

### Papers

- `paper/cross_platform_arbitrage.pdf` — Full research paper (25+ pages)
- `paper/short_results.pdf` — 2-page executive summary

## Requirements

- Polygon wallet with USDC + MATIC for gas
- Alchemy RPC endpoint (Polygon mainnet)
- Chainlink Data Streams API credentials (testnet key works)
- Stable network connection (< 100ms latency to exchanges)
