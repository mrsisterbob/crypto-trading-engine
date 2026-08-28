"""Standalone historical backtester for the Crypto Alert & Paper-Trading Engine's breakout
strategy: Donchian(20) + ATR(14) + RVOL(20) entries, a 1.5x ATR stop, a 2-stage scale-out
(50% off + stop-to-breakeven at 1R) and a final 3.0x-R:R target, with a 0.13% round-trip
fee/slippage haircut applied on both the entry and exit notional of every closed slice.

Deliberately self-contained (no import of analytics_engine/database) so it can be run in any
environment with just `requests` installed, and so backtest parameters are explicit CLI
arguments rather than silently pulled from the live bot's mutable system_config table.

Usage:
    python backtest.py SOLUSDT --days 365
    python backtest.py BTCUSDT --days 180 --sl-mult 2.0 --tp-rr 4.0
"""
import argparse
import logging
import statistics
import time

import requests

BINANCE_SPOT_BASE = "https://api.binance.com"

DONCHIAN_PERIOD = 20
ATR_PERIOD = 14
RVOL_PERIOD = 20
RVOL_THRESHOLD = 2.0
SL_ATR_MULT = 1.5
TP_RR_RATIO = 3.0
FEE_SLIPPAGE_PCT = 0.13
STARTING_BALANCE = 10000.0
DEFAULT_RISK_PCT = 2.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def fetch_historical_klines(symbol: str, interval: str, days: int) -> list[dict]:
    """Downloads the full requested date range via Binance's public klines endpoint, paginating
    in 1000-candle pages (Binance's per-request cap)."""
    end_time_ms = int(time.time() * 1000)
    start_time_ms = end_time_ms - int(days * 24 * 3600 * 1000)
    candles: list[dict] = []
    cursor = start_time_ms
    while cursor < end_time_ms:
        params = {"symbol": symbol, "interval": interval, "startTime": cursor, "limit": 1000}
        resp = requests.get(f"{BINANCE_SPOT_BASE}/api/v3/klines", params=params, timeout=15)
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            break
        for row in rows:
            candles.append({
                "open_time": row[0], "open": float(row[1]), "high": float(row[2]),
                "low": float(row[3]), "close": float(row[4]), "volume": float(row[5]),
                "close_time": row[6],
            })
        last_open_time = rows[-1][0]
        if last_open_time <= cursor:
            break
        cursor = last_open_time + 1
        if len(rows) < 1000:
            break
        time.sleep(0.25)  # stay well clear of Binance's public rate limits during pagination

    unique = {c["open_time"]: c for c in candles}
    sorted_candles = sorted(unique.values(), key=lambda c: c["open_time"])
    return [c for c in sorted_candles if c["open_time"] >= start_time_ms]


def calculate_donchian_high(candles: list[dict], period: int = DONCHIAN_PERIOD) -> float:
    closed = candles[-(period + 1):-1]  # exclude the still-forming current candle
    return max(c["high"] for c in closed)


def calculate_atr(candles: list[dict], period: int = ATR_PERIOD) -> float:
    """Wilder's smoothing, seeded with a simple average of the first `period` true ranges -
    identical formula to analytics_engine.calculate_atr for strategy parity."""
    true_ranges = []
    for i in range(1, len(candles)):
        high, low, prev_close = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    atr = sum(true_ranges[:period]) / period
    for tr in true_ranges[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def calculate_rvol_ratio(candles: list[dict], period: int = RVOL_PERIOD) -> float:
    current_volume = candles[-1]["volume"]
    prior_volumes = [c["volume"] for c in candles[-(period + 1):-1]]
    avg_volume = sum(prior_volumes) / period
    return (current_volume / avg_volume) if avg_volume > 0 else 0.0


def _fee_haircut(entry_price: float, exit_price: float, quantity: float, fee_pct: float) -> float:
    fee_decimal = fee_pct / 100.0
    gross_pnl = (exit_price - entry_price) * quantity
    return gross_pnl - (entry_price * quantity * fee_decimal) - (exit_price * quantity * fee_decimal)


def run_backtest(candles: list[dict], params: dict) -> dict:
    """Walks the candles chronologically, one position at a time. Intra-candle fill order is a
    conservative OHLC-only assumption: the stop-loss (candle low) is always checked BEFORE any
    take-profit level (candle high), so a single wide-range candle never over-credits the
    strategy for a favorable outcome it may not actually have gotten tick-by-tick."""
    balance = params["starting_balance"]
    closed_trades: list[dict] = []
    equity_curve = [balance]
    position: dict | None = None

    lookback = max(DONCHIAN_PERIOD, ATR_PERIOD, RVOL_PERIOD) + 1
    for i in range(lookback, len(candles)):
        window = candles[: i + 1]
        candle = candles[i]

        if position is not None:
            stop_price = position["stop_loss"]
            if candle["low"] <= stop_price:
                exit_price = min(candle["open"], stop_price)
                status = "CLOSED_BE" if position["tp1_hit"] else "CLOSED_SL"
                pnl = _fee_haircut(position["entry_price"], exit_price, position["quantity"], params["fee_pct"])
                total_pnl = position["realized_pnl_usd"] + pnl
                balance += pnl
                r_risk = position["initial_quantity"] * (position["entry_price"] - position["initial_stop_loss"])
                closed_trades.append({
                    "status": status, "entry_price": position["entry_price"], "exit_price": exit_price,
                    "pnl_usd": total_pnl, "r_multiple": (total_pnl / r_risk) if r_risk else 0.0,
                })
                position = None
                equity_curve.append(balance)
                continue

            if not position["tp1_hit"]:
                tp1_price = position["entry_price"] + (position["entry_price"] - position["initial_stop_loss"])
                if candle["high"] >= tp1_price:
                    close_qty = position["initial_quantity"] * 0.5
                    pnl = _fee_haircut(position["entry_price"], tp1_price, close_qty, params["fee_pct"])
                    position["quantity"] -= close_qty
                    position["realized_pnl_usd"] += pnl
                    position["stop_loss"] = position["entry_price"]
                    position["tp1_hit"] = True
                    balance += pnl
                    equity_curve.append(balance)
                    # fall through - a single wide bar could also reach TP2 in the same candle

            if position is not None and candle["high"] >= position["take_profit"]:
                exit_price = position["take_profit"]
                pnl = _fee_haircut(position["entry_price"], exit_price, position["quantity"], params["fee_pct"])
                total_pnl = position["realized_pnl_usd"] + pnl
                balance += pnl
                r_risk = position["initial_quantity"] * (position["entry_price"] - position["initial_stop_loss"])
                closed_trades.append({
                    "status": "CLOSED_TP", "entry_price": position["entry_price"], "exit_price": exit_price,
                    "pnl_usd": total_pnl, "r_multiple": (total_pnl / r_risk) if r_risk else 0.0,
                })
                position = None
                equity_curve.append(balance)
            continue  # a position was already open this bar - no new entries on the same bar

        donchian_high = calculate_donchian_high(window)
        if candle["close"] <= donchian_high:
            continue
        if calculate_rvol_ratio(window) < params["rvol_threshold"]:
            continue

        atr_14 = calculate_atr(window)
        entry_price = candle["close"]
        stop_loss = entry_price - (params["sl_atr_mult"] * atr_14)
        take_profit = entry_price + (params["tp_rr_ratio"] * (entry_price - stop_loss))
        risk_per_unit = entry_price - stop_loss
        if risk_per_unit <= 0:
            continue
        quantity = (balance * (params["risk_pct"] / 100.0)) / risk_per_unit

        position = {
            "entry_price": entry_price, "initial_stop_loss": stop_loss, "stop_loss": stop_loss,
            "take_profit": take_profit, "quantity": quantity, "initial_quantity": quantity,
            "tp1_hit": False, "realized_pnl_usd": 0.0,
        }

    return {"closed_trades": closed_trades, "equity_curve": equity_curve, "final_balance": balance}


def compute_summary(result: dict, starting_balance: float) -> dict:
    trades = result["closed_trades"]
    total_trades = len(trades)
    if total_trades == 0:
        return {
            "total_trades": 0, "win_rate_pct": 0.0, "profit_factor": None,
            "max_drawdown_pct": 0.0, "sharpe_ratio": 0.0, "net_pnl_pct": 0.0,
        }

    wins = [t for t in trades if t["pnl_usd"] > 0]
    losses = [t for t in trades if t["pnl_usd"] <= 0]
    win_rate_pct = len(wins) / total_trades * 100.0

    gross_profit = sum(t["pnl_usd"] for t in wins)
    gross_loss = abs(sum(t["pnl_usd"] for t in losses))
    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    else:
        profit_factor = float("inf") if gross_profit > 0 else 0.0

    peak = result["equity_curve"][0]
    max_dd_pct = 0.0
    for value in result["equity_curve"]:
        peak = max(peak, value)
        if peak > 0:
            max_dd_pct = max(max_dd_pct, (peak - value) / peak * 100.0)

    trade_returns = []
    running_balance = starting_balance
    for t in trades:
        if running_balance > 0:
            trade_returns.append(t["pnl_usd"] / running_balance)
        running_balance += t["pnl_usd"]
    if len(trade_returns) >= 2 and statistics.pstdev(trade_returns) > 0:
        sharpe_ratio = statistics.mean(trade_returns) / statistics.pstdev(trade_returns)
    else:
        sharpe_ratio = 0.0

    net_pnl_pct = (result["final_balance"] - starting_balance) / starting_balance * 100.0

    return {
        "total_trades": total_trades, "win_rate_pct": win_rate_pct, "profit_factor": profit_factor,
        "max_drawdown_pct": max_dd_pct, "sharpe_ratio": sharpe_ratio, "net_pnl_pct": net_pnl_pct,
    }


def print_summary(symbol: str, summary: dict, days: int) -> None:
    if summary["profit_factor"] is None:
        pf_display = "N/A"
    elif summary["profit_factor"] == float("inf"):
        pf_display = "inf (no losing trades)"
    else:
        pf_display = f"{summary['profit_factor']:.2f}"

    print("=" * 60)
    print(f"BACKTEST SUMMARY: {symbol} | {days}D | 4H Donchian(20)/ATR(14)/RVOL(20) Breakout")
    print("=" * 60)
    print(f"{'Total Trades:':<28}{summary['total_trades']}")
    print(f"{'Win Rate:':<28}{summary['win_rate_pct']:.2f}%")
    print(f"{'Profit Factor:':<28}{pf_display}")
    print(f"{'Max Drawdown:':<28}{summary['max_drawdown_pct']:.2f}%")
    print(f"{'Sharpe Ratio (per-trade):':<28}{summary['sharpe_ratio']:.3f}")
    print(f"{'Net PnL:':<28}{summary['net_pnl_pct']:+.2f}%")
    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description="Historical backtester for the Donchian/ATR/RVOL breakout strategy.")
    parser.add_argument("symbol", help="Binance spot symbol, e.g. SOLUSDT")
    parser.add_argument("--days", type=int, default=365, help="Lookback window in days (default 365)")
    parser.add_argument("--interval", default="4h", help="Kline interval (default 4h)")
    parser.add_argument("--risk-pct", type=float, default=DEFAULT_RISK_PCT)
    parser.add_argument("--sl-mult", type=float, default=SL_ATR_MULT)
    parser.add_argument("--tp-rr", type=float, default=TP_RR_RATIO)
    parser.add_argument("--rvol-threshold", type=float, default=RVOL_THRESHOLD)
    parser.add_argument("--fee-pct", type=float, default=FEE_SLIPPAGE_PCT)
    parser.add_argument("--balance", type=float, default=STARTING_BALANCE)
    args = parser.parse_args()

    symbol = args.symbol.upper()
    logging.info(f"Downloading {args.days}d of {args.interval} klines for {symbol}...")
    candles = fetch_historical_klines(symbol, args.interval, args.days)
    logging.info(f"Downloaded {len(candles)} candles.")

    min_required = max(DONCHIAN_PERIOD, ATR_PERIOD, RVOL_PERIOD) + 2
    if len(candles) < min_required:
        logging.error(f"Not enough historical data returned to run a backtest (need >= {min_required} candles).")
        return

    params = {
        "risk_pct": args.risk_pct, "sl_atr_mult": args.sl_mult, "tp_rr_ratio": args.tp_rr,
        "rvol_threshold": args.rvol_threshold, "fee_pct": args.fee_pct, "starting_balance": args.balance,
    }
    result = run_backtest(candles, params)
    summary = compute_summary(result, args.balance)
    print_summary(symbol, summary, args.days)


if __name__ == "__main__":
    main()
