"""Validation layer on top of backtest.py: walk-forward testing, expectancy/R-multiple
reporting, and Monte Carlo drawdown resampling.

backtest.py answers "how did this strategy do on this exact historical window with these exact
parameters" - a single number that is trivially easy to curve-fit by hand without realizing it.
This module answers the three harder questions that actually decide whether a strategy is safe
to trade: does it hold up out-of-sample (walk-forward), is its per-trade edge real or just a
high win rate masking a bad payoff ratio (expectancy), and how bad could its drawdown have been
under a differently-ordered but equally-likely sequence of the same trades (Monte Carlo).

Deliberately built as a thin layer over backtest.run_backtest() - no duplicated strategy logic,
so a change to the entry/exit rules in backtest.py is automatically reflected here.
"""
import argparse
import logging
import random
import statistics

from backtest import (
    DEFAULT_RISK_PCT,
    FEE_SLIPPAGE_PCT,
    RVOL_THRESHOLD,
    SL_ATR_MULT,
    STARTING_BALANCE,
    TP_RR_RATIO,
    fetch_historical_klines,
    run_backtest,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ---------------------------------------------------------------------------
# Walk-forward validation
# ---------------------------------------------------------------------------
def split_walk_forward_windows(candles: list[dict], n_windows: int) -> list[list[dict]]:
    """Splits candles into n_windows contiguous, non-overlapping chronological chunks.

    Contiguous (not shuffled) because walk-forward validation is specifically about whether a
    strategy holds up on data it has not seen yet, in the order it would actually be traded -
    shuffling would destroy that meaning and just become another Monte Carlo variant.
    """
    if n_windows < 2:
        raise ValueError("n_windows must be >= 2 (at least one in-sample + one out-of-sample window)")
    chunk_size = len(candles) // n_windows
    if chunk_size < 50:
        raise ValueError(
            f"Only {len(candles)} candles for {n_windows} windows ({chunk_size}/window) - "
            "too few to trust; use more history or fewer windows."
        )
    windows = [candles[i * chunk_size:(i + 1) * chunk_size] for i in range(n_windows - 1)]
    windows.append(candles[(n_windows - 1) * chunk_size:])  # last window absorbs the remainder
    return windows


def run_walk_forward(candles: list[dict], params: dict, n_windows: int = 6) -> dict:
    """Runs the same fixed params unchanged across n_windows sequential chunks of history and
    reports each window's result side by side. A strategy whose performance is consistent
    (same sign, same rough magnitude) across windows is far more trustworthy than one that
    looks great in aggregate but is carried entirely by one lucky window - that pattern is the
    single most common way a retail-built strategy fools its own builder.
    """
    windows = split_walk_forward_windows(candles, n_windows)
    window_results = []
    for idx, window in enumerate(windows, start=1):
        result = run_backtest(window, params)
        summary = _compute_summary_local(result, params["starting_balance"])
        window_results.append({"window": idx, "candle_count": len(window), **summary})

    profitable_windows = sum(1 for w in window_results if w["net_pnl_pct"] > 0)
    pnl_values = [w["net_pnl_pct"] for w in window_results]
    consistency = {
        "windows_profitable": profitable_windows,
        "windows_total": len(window_results),
        "consistency_pct": (profitable_windows / len(window_results) * 100.0) if window_results else 0.0,
        "pnl_stdev_pct": statistics.pstdev(pnl_values) if len(pnl_values) >= 2 else 0.0,
        "pnl_mean_pct": statistics.mean(pnl_values) if pnl_values else 0.0,
    }
    return {"windows": window_results, "consistency": consistency}


def _compute_summary_local(result: dict, starting_balance: float) -> dict:
    """Same shape as backtest.compute_summary() - reimplemented locally to avoid importing a
    private-feeling helper across module boundaries; kept in exact sync with backtest.py's logic."""
    from backtest import compute_summary
    return compute_summary(result, starting_balance)


def print_walk_forward_report(symbol: str, wf: dict) -> None:
    print("=" * 72)
    print(f"WALK-FORWARD VALIDATION: {symbol}")
    print("=" * 72)
    for w in wf["windows"]:
        print(
            f"  Window {w['window']} ({w['candle_count']} candles): "
            f"{w['total_trades']} trades, win rate {w['win_rate_pct']:.1f}%, "
            f"net PnL {w['net_pnl_pct']:+.2f}%, max DD {w['max_drawdown_pct']:.2f}%"
        )
    c = wf["consistency"]
    print("-" * 72)
    print(f"Profitable windows: {c['windows_profitable']}/{c['windows_total']} ({c['consistency_pct']:.0f}%)")
    print(f"Mean window PnL: {c['pnl_mean_pct']:+.2f}%  |  Stdev across windows: {c['pnl_stdev_pct']:.2f}%")
    if c["consistency_pct"] < 60:
        print("VERDICT: Inconsistent across windows - likely curve-fit to one period. Do not trust the aggregate number.")
    else:
        print("VERDICT: Reasonably consistent across windows - worth further validation (Monte Carlo, forward test).")
    print("=" * 72)


# ---------------------------------------------------------------------------
# Expectancy / R-multiple reporting
# ---------------------------------------------------------------------------
def compute_expectancy(trades: list[dict]) -> dict:
    """Expectancy in R (risk units): the average R-multiple per trade, which is what actually
    determines whether a strategy is worth trading long-run - a high win rate with a poor
    average win/loss ratio can still have negative expectancy, and a low win rate with a large
    average win can still be strongly profitable. Win rate alone hides this; expectancy doesn't.
    """
    if not trades:
        return {
            "total_trades": 0, "win_rate_pct": 0.0, "avg_win_r": 0.0, "avg_loss_r": 0.0,
            "expectancy_r": 0.0, "payoff_ratio": 0.0,
        }
    r_multiples = [t["r_multiple"] for t in trades]
    wins = [r for r in r_multiples if r > 0]
    losses = [r for r in r_multiples if r <= 0]
    total = len(trades)
    win_rate = len(wins) / total
    avg_win_r = statistics.mean(wins) if wins else 0.0
    avg_loss_r = statistics.mean(losses) if losses else 0.0  # negative or zero
    expectancy_r = statistics.mean(r_multiples)
    payoff_ratio = (avg_win_r / abs(avg_loss_r)) if avg_loss_r else float("inf") if avg_win_r > 0 else 0.0
    return {
        "total_trades": total,
        "win_rate_pct": win_rate * 100.0,
        "avg_win_r": avg_win_r,
        "avg_loss_r": avg_loss_r,
        "expectancy_r": expectancy_r,
        "payoff_ratio": payoff_ratio,
    }


def print_expectancy_report(symbol: str, expectancy: dict) -> None:
    print("=" * 60)
    print(f"EXPECTANCY REPORT: {symbol}")
    print("=" * 60)
    print(f"{'Total Trades:':<24}{expectancy['total_trades']}")
    print(f"{'Win Rate:':<24}{expectancy['win_rate_pct']:.2f}%")
    print(f"{'Avg Win:':<24}{expectancy['avg_win_r']:+.2f}R")
    print(f"{'Avg Loss:':<24}{expectancy['avg_loss_r']:+.2f}R")
    payoff_display = "inf" if expectancy["payoff_ratio"] == float("inf") else f"{expectancy['payoff_ratio']:.2f}"
    print(f"{'Payoff Ratio (W/L):':<24}{payoff_display}")
    print(f"{'Expectancy per Trade:':<24}{expectancy['expectancy_r']:+.3f}R")
    print("-" * 60)
    if expectancy["total_trades"] < 50:
        print(f"CAUTION: only {expectancy['total_trades']} trades - too few to trust this number statistically.")
    if expectancy["expectancy_r"] > 0:
        print("VERDICT: Positive expectancy - on average this strategy makes money per trade taken.")
    else:
        print("VERDICT: Negative or zero expectancy - this strategy loses money on average per trade, regardless of win rate.")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Monte Carlo drawdown resampling
# ---------------------------------------------------------------------------
def monte_carlo_drawdown(trades: list[dict], starting_balance: float, n_simulations: int = 1000, seed: int = None) -> dict:
    """Shuffles the same set of closed trades into n_simulations different orderings and
    recomputes max drawdown for each. The backtest only shows you the drawdown for the one
    order those trades happened to occur in historically - an unlucky ordering (e.g. several
    losses clustered early before any wins) can produce a much worse drawdown with the exact
    same trades and the exact same edge. Sizing decisions should respect the worst plausible
    ordering, not just the one that happened.
    """
    if len(trades) < 5:
        return {
            "n_simulations": 0, "n_trades": len(trades),
            "max_drawdown_pct_median": 0.0, "max_drawdown_pct_p95": 0.0, "max_drawdown_pct_worst": 0.0,
            "note": "too_few_trades",
        }
    rng = random.Random(seed)
    pnl_values = [t["pnl_usd"] for t in trades]
    drawdowns = []
    for _ in range(n_simulations):
        shuffled = pnl_values.copy()
        rng.shuffle(shuffled)
        balance = starting_balance
        peak = balance
        max_dd_pct = 0.0
        for pnl in shuffled:
            balance += pnl
            peak = max(peak, balance)
            if peak > 0:
                max_dd_pct = max(max_dd_pct, (peak - balance) / peak * 100.0)
        drawdowns.append(max_dd_pct)

    drawdowns.sort()
    p95_index = min(len(drawdowns) - 1, int(round(0.95 * (len(drawdowns) - 1))))
    return {
        "n_simulations": n_simulations,
        "n_trades": len(trades),
        "max_drawdown_pct_median": statistics.median(drawdowns),
        "max_drawdown_pct_p95": drawdowns[p95_index],
        "max_drawdown_pct_worst": drawdowns[-1],
    }


def print_monte_carlo_report(symbol: str, mc: dict) -> None:
    print("=" * 60)
    print(f"MONTE CARLO DRAWDOWN: {symbol} ({mc['n_simulations']} reshuffles of {mc['n_trades']} trades)")
    print("=" * 60)
    if mc.get("note") == "too_few_trades":
        print("CAUTION: too few closed trades to run a meaningful Monte Carlo resample.")
    else:
        print(f"{'Median Max Drawdown:':<28}{mc['max_drawdown_pct_median']:.2f}%")
        print(f"{'95th Percentile Drawdown:':<28}{mc['max_drawdown_pct_p95']:.2f}%")
        print(f"{'Worst-Case Drawdown Seen:':<28}{mc['max_drawdown_pct_worst']:.2f}%")
        print("-" * 60)
        print(
            "Size positions so the 95th-percentile drawdown above is still tolerable - the "
            "single historical backtest run only ever shows you one ordering of these outcomes."
        )
    print("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Walk-forward validation, expectancy, and Monte Carlo drawdown analysis for the breakout strategy."
    )
    parser.add_argument("symbol", help="Binance spot symbol, e.g. SOLUSDT")
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--interval", default="4h")
    parser.add_argument("--risk-pct", type=float, default=DEFAULT_RISK_PCT)
    parser.add_argument("--sl-mult", type=float, default=SL_ATR_MULT)
    parser.add_argument("--tp-rr", type=float, default=TP_RR_RATIO)
    parser.add_argument("--rvol-threshold", type=float, default=RVOL_THRESHOLD)
    parser.add_argument("--fee-pct", type=float, default=FEE_SLIPPAGE_PCT)
    parser.add_argument("--balance", type=float, default=STARTING_BALANCE)
    parser.add_argument("--windows", type=int, default=6, help="Number of walk-forward windows (default 6)")
    parser.add_argument("--mc-sims", type=int, default=1000, help="Monte Carlo reshuffles (default 1000)")
    parser.add_argument("--seed", type=int, default=None, help="Monte Carlo random seed (for reproducible runs)")
    args = parser.parse_args()

    symbol = args.symbol.upper()
    logging.info(f"Downloading {args.days}d of {args.interval} klines for {symbol}...")
    candles = fetch_historical_klines(symbol, args.interval, args.days)
    logging.info(f"Downloaded {len(candles)} candles.")

    params = {
        "risk_pct": args.risk_pct, "sl_atr_mult": args.sl_mult, "tp_rr_ratio": args.tp_rr,
        "rvol_threshold": args.rvol_threshold, "fee_pct": args.fee_pct, "starting_balance": args.balance,
    }

    wf = run_walk_forward(candles, params, n_windows=args.windows)
    print_walk_forward_report(symbol, wf)
    print()

    full_result = run_backtest(candles, params)
    expectancy = compute_expectancy(full_result["closed_trades"])
    print_expectancy_report(symbol, expectancy)
    print()

    mc = monte_carlo_drawdown(full_result["closed_trades"], args.balance, n_simulations=args.mc_sims, seed=args.seed)
    print_monte_carlo_report(symbol, mc)


if __name__ == "__main__":
    main()
