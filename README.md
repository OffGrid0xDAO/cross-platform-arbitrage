# Cross-Platform Arbitrage in Cryptocurrency Prediction Markets

An empirical analysis of arbitrage activity across Polymarket, Kalshi, and Binance, based on 6.4 million trades from February 19--25, 2026.

**Paper**: [Full paper (PDF)](https://github.com/OffGrid0xDAO/cross-platform-arbitrage/raw/master/paper/cross_platform_arbitrage.pdf) | [Summary (PDF)](https://github.com/OffGrid0xDAO/cross-platform-arbitrage/raw/master/paper/short_results.pdf)

## Key Findings

1. **Binance-to-Polymarket latency arbitrage** -- 29 wallets achieve >98% win rates by trading Polymarket 5-min BTC markets in the final 15 seconds of each window, using Binance spot price as a free information feed. Win rates reach 97--100% when BTC moves >$80 but drop to ~50% (random) on small moves.

2. **Cross-platform trade synchronization** -- 161,577 exact same-second trade pairs detected between Polymarket and Kalshi 15-min BTC markets. 1,368 opposite-side amount-matched trades (same dollar amount +/-20%, within +/-5 seconds) indicate coordinated hedging.

3. **Coordinated wallet networks** -- 6 core wallets with $4.7M combined volume and $248K profit. Some wallets achieve 100% win rate with 100% Kalshi outcome alignment across hundreds of trades (p < 10^-270).

4. **Oracle risk** -- 5.7% disagreement rate between Chainlink (Polymarket) and CF Benchmarks BRTI (Kalshi) makes cross-platform hedging non-risk-free, with a 1-in-18 chance of double loss.

5. **Market structure** -- Five distinct trader strategies identified: latency arbitrageurs (93.5% win rate), cross-platform arbitrageurs (72.4%), informed traders (58.1%), market makers (51.2%), and retail (43.2%, net -$215K).

## Dataset

| Source | Trades | Wallets/Accounts | Markets |
|--------|--------|-------------------|---------|
| Polymarket 5-min BTC | 2,182,329 | 26,475 | 642 |
| Polymarket 15-min BTC | 2,536,367 | 7,570 | 672 |
| Kalshi KXBTC15M | 1,680,127 | -- | 672 |
| **Total** | **6,398,823** | | |

## Repository Structure

```
paper/                  Research papers
  cross_platform_arbitrage.{tex,pdf}   Full 23-page paper
  short_results.{tex,pdf}              2-page summary of key results

analysis/               Analysis scripts
  detect_arbitrage.py          6-hypothesis arbitrage detection framework (5-min markets)
  cross_arb_15m.py             Cross-platform comparison: Polymarket vs Kalshi 15-min
  pnl_stationarity_losses.py   P&L decomposition, stationarity, and loss distribution
  fetch_polymarket_15m.py      Data fetcher for Polymarket 15-min BTC trades
  fetch_kalshi_data.py         Data fetcher for Kalshi KXBTC15M trades
  cross_platform_compare.py    Price discrepancy and timing correlation analysis
  binance_arb_link.py          Binance-Polymarket latency arbitrage detection
  analyze_5m_whales.py         Whale and alpha wallet analysis (5-min markets)
  trace_arb_wallets.py         On-chain profiling of arbitrage suspect wallets
  wallet_links.py              Cross-wallet behavioral clustering and funding traces
  temporal_arb_detect.py       Temporal arbitrage detection across all three platforms

data/                   Processed datasets (see data/README.md)
  boundaries.csv               5-min window boundaries with Binance prices
  cross_arb_15m_results.json   Cross-platform comparison results
  pnl_stationarity_losses.json P&L and stationarity metrics
  whale_analysis.json          Per-wallet analysis (26K wallets)
  proxy_owners.json            Proxy-to-owner wallet mapping
  multi_bridge_scan.json       Cross-chain bridge scan results
  solana_links.json            Solana-side wallet links
```

## Reproducing the Analysis

### Prerequisites

```
Python 3.10+
pip install requests numpy pandas scipy
```

### Step 1: Fetch raw data

```bash
# Polymarket 5-min BTC trades (~180 MB, takes several hours)
python analysis/analyze_5m_whales.py

# Polymarket 15-min BTC trades (~200 MB)
python analysis/fetch_polymarket_15m.py

# Kalshi trades (requires API credentials)
python analysis/fetch_kalshi_data.py
```

### Step 2: Run analyses

```bash
# Binance latency arbitrage detection
python analysis/detect_arbitrage.py

# Cross-platform Polymarket vs Kalshi comparison
python analysis/cross_arb_15m.py

# P&L decomposition and loss analysis
python analysis/pnl_stationarity_losses.py

# On-chain wallet tracing
python analysis/trace_arb_wallets.py
python analysis/wallet_links.py
```

### Step 3: Build paper

```bash
cd paper
pdflatex cross_platform_arbitrage.tex
pdflatex short_results.tex
```

## Methodology

The analysis uses a six-hypothesis testing framework:

- **H1**: Latency arbitrage -- wallets trading in final seconds correlate with exchange price moves
- **H2**: Sell-cluster arbitrage -- NegRisk minting arbitrage via simultaneous opposite-side sells
- **H3**: Binance correlation -- trade timing and direction linked to BTC/USDT spot movements
- **H4**: Coordinated market making -- synchronized liquidity provision across platforms
- **H5**: Oracle divergence exploitation -- profiting from Chainlink vs CF Benchmarks disagreements
- **H6**: Cross-platform hedging -- matched opposite-side positions on Polymarket and Kalshi

All six hypotheses are confirmed with statistical significance. See the [full paper](paper/cross_platform_arbitrage.pdf) for details.

## Citation

```bibtex
@article{chainsaw2026crossplatform,
  title={Cross-Platform Arbitrage in Cryptocurrency Prediction Markets: An Empirical Analysis},
  author={Chainsaw Research},
  year={2026},
  month={February}
}
```

## License

This research is provided for educational and academic purposes. The analysis scripts and data are released under the MIT License.
