# crypto-trading-engine

<!-- AUTO-STATS:START -->
![Lines of source](https://img.shields.io/badge/source-2880_lines-c9a24b)
![Tests](https://img.shields.io/badge/tests-15-4a8a5c)
<!-- AUTO-STATS:END -->

Crypto Alert & Paper-Trading Engine: scans BTC/ETH + tracked altcoins on a 15-minute cycle for
Donchian(20)/ATR(14)/RVOL(20) breakout setups, contextualized with Deribit gamma exposure (majors)
and Binance funding-rate/open-interest squeeze signals (alts), and manages paper trades through a
2-stage scale-out with a live drawdown circuit breaker - all controlled via a Telegram bot.

**Currently paper-trading only.** No live order execution against any exchange exists in this
codebase - `/paper_buy` sizes and logs virtual trades against a virtual balance. Moving to live
execution is a distinct, not-yet-started piece of work (order placement, slippage handling, API
key security, partial-fill handling). Because it's paper-only, the market-data sources
(Deribit / Binance public / CoinGecko) are all free and keyless - the only secret it needs is a
Telegram bot token.

## Architecture

```
main_2.py             Current entry point: Flask webhook + APScheduler orchestration, dynamic
                       parameter engine (risk/heat/rvol/sl/tp/fee/alts, mutable via Telegram),
                       portfolio heat cap, circuit-breaker-aware scan/alert gating.
main.py                Earlier/simpler entry point (v1 feature set) - kept for reference.
analytics_engine.py    Data ingestion (Deribit/Binance/CoinGecko, all free/keyless) + the Hybrid
                        Sniper Score signal logic (Donchian/ATR/RVOL breakout + gamma/funding/OI
                        context) + shared trade reconciliation logic (reconcile_trade_tick).
alert_dispatcher.py    Telegram message formatting + dispatch for every alert type.
database.py             SQLite persistence (WAL mode): market ticks, open signals, paper
                        portfolio, system_config (hot-reloadable strategy parameters).
ws_reconciler.py        Real-time Binance WebSocket price feed for sub-second SL/TP/scale-out
                        detection, backed up by main_2.py's 5-min REST reconciliation loop.
seed_sample_data.py     Populates a fresh crypto_engine.db with fabricated demo signals/trades
                        so /portfolio renders on a clean checkout. Not a real trade history.
backtest.py             Standalone CLI historical backtester for the breakout strategy.
backtest_analysis.py    Validation layer on top of backtest.py: walk-forward testing (catches
                        curve-fitting to one historical window), expectancy/R-multiple reporting
                        (the number that actually decides if a strategy is worth trading), and
                        Monte Carlo drawdown resampling (worst-plausible-case sizing, not just
                        the one historical trade ordering).
funding_logger.py       Read-only cross-venue perpetual funding-rate logger + spread reporter.
                        Measurement instrument only - no strategy, no orders, no API keys.
                        See "Cross-venue funding logger" below.
circuit_breaker.py      Live drawdown kill switch: halts new signal alerts and paper-trade
                        entries once realized drawdown from peak balance crosses a threshold you
                        set - does not close already-open positions, which keep running their
                        own stop-loss/take-profit unchanged. Manual /halt and /resume also
                        available independent of the drawdown threshold.
```

## Before trading this strategy with any real capital

Run the validation layer first, honestly:

```
pip install -r requirements.txt
python backtest_analysis.py BTCUSDT --days 365
python backtest_analysis.py ETHUSDT --days 365
```

Look at the walk-forward consistency, the expectancy-per-trade (not just win rate), and the
Monte Carlo drawdown percentiles before trusting the strategy at any position size. A single
backtest run on one historical window is the most common way a retail-built strategy fools its
own builder - this module exists specifically to catch that.

## Cross-venue funding logger

`funding_logger.py` **logs data. That is all it does.** It records the perpetual funding rate
for BTC and ETH on every venue that serves it publicly from this machine, every 8 hours, and
reports the distribution of the spread between venues.

**There is no funding-arbitrage strategy in this repo, and this is not one.** It places no
orders, holds no API keys, and reads only public unauthenticated endpoints. It exists to
gather evidence for a single question that cannot be answered by guessing:

> Is the funding spread between venues, at retail size, after fees, actually harvestable?

Read the output as a spread distribution, not as a return. The tool deliberately does not
print a projected APY or an expected profit - that inference is yours to make, and it has to
survive everything the logger does **not** measure:

- **Harvesting a spread means holding two simultaneous positions on two different venues** -
  long the venue paying you, short the venue charging you. None of that is built here.
- That requires **collateral split across both venues**, actively managed, plus the transfer
  latency and cost of moving margin between them when one leg moves against you.
- The short leg carries **liquidation risk**. A funding spread of 0.02%/day is wiped out many
  times over by one liquidation, and the spread being wide is often *itself* the signal that
  one side is stressed.
- Execution slippage at your actual size, maker/taker fees on four legs (open and close, both
  venues), and the plain fact that a spread visible in a snapshot is not necessarily a spread
  you could have filled.

**30 days of observations is a starting sample, not proof of anything.** At an 8-hour cadence
that is roughly 90 data points per venue - enough to see whether a spread persists or is
noise, not enough to establish an edge. The report prints the batch count and flags small
samples for exactly this reason.

Venue coverage is determined by what is reachable without authentication from wherever you run
it, which is a geography question, not a code question - run the reachability check and see.
Notably: `api.binance.com` returns HTTP 451 from a US IP, `api.binance.us` is spot-only with no
funding endpoint at all, and Bybit's public endpoints return HTTP 403 from a US IP. No
workaround is attempted for any of those, by design.

```
python funding_logger.py collect                    # one collection cycle
python funding_logger.py report                     # spread distribution so far
python funding_logger.py report --symbol BTC --thresholds 0.05,0.10,0.20
```

Once `main_2.py` is running, the collection job runs itself every 8 hours. It is strictly
additive - if it fails it logs and returns, and the scan and reconciliation jobs are unaffected.

## Setup

1. `pip install -r requirements.txt`
2. Set environment variables: `CRYPTO_TELEGRAM_BOT_TOKEN`, `CRYPTO_TELEGRAM_CHAT_ID`,
   `CRYPTO_TELEGRAM_WEBHOOK_SECRET` (recommended), `CRYPTO_ENGINE_PORT` (default 5001).
3. *(optional)* `python seed_sample_data.py` - fabricated demo positions so `/portfolio` isn't
   empty on a fresh install. Delete `crypto_engine.db` to undo.
4. Run: `python main_2.py`

`crypto_engine.db` is gitignored - the strategy parameters, trade log, and virtual balance
all live there, on your machine only. This repo is the engine, not the account.

### Telegram commands

`/scan`, `/portfolio`, `/paper_buy <PAIR> [size%]`, `/config`, `/status`, `/halt`, `/resume`, or a
bare parameter mutation like `risk = 1.5`, `alts + SUI`, `alts - ADA`.

## Circuit breaker

Set your own drawdown tolerance before you need it, in a calm moment - not as a reflex mid
losing-streak:

```
# via a parameter mutation in Telegram, or directly in system_config:
max_drawdown_pct = 15.0
```

`/status` shows current drawdown vs. threshold. A tripped breaker blocks new entries/alerts until
`/resume` is sent - deliberately manual, so a balance recovery doesn't silently resume trading
without you reviewing what happened first.
