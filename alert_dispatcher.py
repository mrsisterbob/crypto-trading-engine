"""Telegram alert formatting + dispatch for the Crypto Alert & Paper-Trading Engine.

All dynamic strings are html.escape()'d before insertion into HTML-parsed Telegram messages.
Outbound calls reuse analytics_engine's exponential backoff decorator for 429/5xx resilience.
"""
import html
import logging
import os

import requests

from analytics_engine import with_retry_backoff

TELEGRAM_BOT_TOKEN = os.environ.get("CRYPTO_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("CRYPTO_TELEGRAM_CHAT_ID")
TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


@with_retry_backoff(max_retries=3, base_delay=1.0)
def _post_telegram(method: str, payload: dict) -> requests.Response:
    return requests.post(f"{TELEGRAM_API_BASE}/{method}", json=payload, timeout=10)


def send_telegram_message(text: str, chat_id: str = None, parse_mode: str = "HTML") -> bool:
    if not TELEGRAM_BOT_TOKEN:
        logging.error("CRYPTO_TELEGRAM_BOT_TOKEN not set; cannot send Telegram message.")
        return False
    target_chat = chat_id or TELEGRAM_CHAT_ID
    if not target_chat:
        logging.error("No Telegram chat_id available (CRYPTO_TELEGRAM_CHAT_ID unset and none provided).")
        return False
    try:
        resp = _post_telegram("sendMessage", {
            "chat_id": target_chat, "text": text, "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        })
        return resp.status_code == 200
    except Exception as e:
        logging.error(f"send_telegram_message failed: {e}")
        return False


def format_signal_alert_card(signal: dict) -> str:
    symbol = html.escape(str(signal.get("symbol", "UNKNOWN")))
    context_summary = html.escape(str(signal.get("context_summary", "N/A")))
    lines = [
        f"🚨 <b>BREAKOUT SIGNAL: {symbol}</b>",
        "",
        f"🎯 <b>Confidence Score:</b> {signal.get('confidence_score', 0)}/100",
        f"🧊 <b>Squeeze Context:</b> {context_summary}",
        f"📊 <b>RVOL:</b> {signal.get('rvol_ratio', 0.0):.2f}x (200% gate)",
        f"💰 <b>Entry (approx):</b> {signal.get('current_price', 0.0):.6g}",
        f"🛑 <b>Stop-Loss:</b> {signal.get('stop_loss', 0.0):.6g} (1.5x ATR)",
        f"🎯 <b>Take-Profit:</b> {signal.get('take_profit', 0.0):.6g} (1:3 R/R)",
        "",
        "⚡ Tap to copy paper trade command:",
        f"<code>/paper_buy {symbol} 2%</code>",
    ]
    return "\n".join(lines)


def format_trade_closed_alert(trade: dict) -> str:
    symbol = html.escape(str(trade.get("symbol", "UNKNOWN")))
    status = str(trade.get("status", "CLOSED"))
    emoji_map = {"CLOSED_TP": "\u2705", "CLOSED_SL": "\U0001F6D1", "CLOSED_BE": "\U0001F7E1", "CLOSED_MANUAL": "\u26AA"}
    outcome_emoji = emoji_map.get(status, "\u26AA")
    lines = [
        f"{outcome_emoji} <b>PAPER TRADE CLOSED: {symbol}</b>",
        "",
        f"<b>Outcome:</b> {html.escape(status.replace('_', ' '))}",
        f"<b>Entry:</b> {trade.get('entry_price', 0.0):.6g}",
        f"<b>Exit:</b> {trade.get('close_price', 0.0):.6g}",
        f"<b>PnL:</b> {trade.get('pnl_pct', 0.0):+.2f}% ({trade.get('pnl_usd', 0.0):+.2f} USD)",
    ]
    return "\n".join(lines)


def format_tp1_scaleout_alert(trade: dict) -> str:
    symbol = html.escape(str(trade.get("symbol", "UNKNOWN")))
    lines = [
        f"\U0001F3AF <b>Scale-Out &amp; Stop to Breakeven: {symbol}</b>",
        "",
        f"<b>Closed 50% @:</b> {trade.get('tp1_exit_price', 0.0):.6g}",
        f"<b>Realized (this slice):</b> {trade.get('partial_pnl_usd', 0.0):+.2f} USD",
        f"<b>Remaining Qty:</b> {trade.get('quantity', 0.0):.6g}",
        f"<b>New Stop (Breakeven):</b> {trade.get('stop_loss', 0.0):.6g}",
        f"<b>Final Target:</b> {trade.get('take_profit', 0.0):.6g}",
    ]
    return "\n".join(lines)


def format_heat_cap_rejection(pair: str, requested_risk_pct: float, current_open_risk_pct: float, max_heat_pct: float) -> str:
    symbol = html.escape(str(pair))
    lines = [
        "\U0001F6AB <b>Trade Rejected: Portfolio Heat Cap</b>",
        "",
        f"<b>Pair:</b> <code>{symbol}</code>",
        f"<b>Requested Risk:</b> {requested_risk_pct:.2f}%",
        f"<b>Current Open Risk:</b> {current_open_risk_pct:.2f}%",
        f"<b>Max Portfolio Heat:</b> {max_heat_pct:.2f}%",
        "",
        f"Adding this trade would push total open risk to {(current_open_risk_pct + requested_risk_pct):.2f}%, exceeding the cap.",
    ]
    return "\n".join(lines)


def format_config_card(params: dict) -> str:
    display_order = [
        ("risk_pct", "Risk % / Trade"),
        ("max_portfolio_heat_pct", "Max Portfolio Heat %"),
        ("rvol_threshold", "RVOL Trigger Threshold"),
        ("sl_atr_mult", "Stop-Loss ATR Multiplier"),
        ("tp_rr_ratio", "Take-Profit R:R Ratio"),
        ("fee_slippage_pct", "Fee + Slippage %"),
        ("tracked_alts", "Tracked Altcoins"),
    ]
    body_lines = [f"{label:<24} {html.escape(str(params.get(key, 'N/A')))}" for key, label in display_order]
    lines = [
        "\u2699\uFE0F <b>Active Engine Parameters</b>",
        f"<pre>{chr(10).join(body_lines)}</pre>",
        "",
        "Tap to copy a mutation:",
        "<code>risk = 1.5</code>",
        "<code>alts + SUI</code>",
        "<code>alts - ADA</code>",
    ]
    return "\n".join(lines)


def format_paper_buy_confirmation(trade: dict) -> str:
    symbol = html.escape(str(trade.get("symbol", "?")))
    lines = [
        f"✅ <b>Paper Trade Opened: {symbol}</b>",
        "",
        f"<b>Entry:</b> {trade.get('entry_price', 0.0):.6g}",
        f"<b>Quantity:</b> {trade.get('quantity', 0.0):.6g}",
        f"<b>Risk:</b> {trade.get('risk_pct', 0.0):.1f}% of virtual balance",
        f"<b>Stop-Loss:</b> {trade.get('stop_loss', 0.0):.6g}",
        f"<b>Take-Profit:</b> {trade.get('take_profit', 0.0):.6g}",
        f"<b>Trade ID:</b> #{trade.get('id', '?')}",
    ]
    return "\n".join(lines)


def format_portfolio_summary_card(summary: dict) -> str:
    lines = [
        "📈 <b>Paper Portfolio Summary</b>",
        "",
        f"💵 <b>Virtual Balance:</b> {summary.get('virtual_balance_usd', 0.0):.2f} USD",
        f"📂 <b>Open Positions:</b> {summary.get('open_count', 0)}",
        f"📊 <b>Realized PnL (all-time):</b> {summary.get('realized_pnl_usd', 0.0):+.2f} USD",
        f"🏆 <b>Win Rate:</b> {summary.get('win_rate_pct', 0.0):.1f}% ({summary.get('wins', 0)}W / {summary.get('losses', 0)}L)",
        "",
    ]
    open_positions = summary.get("open_positions", [])
    if open_positions:
        lines.append("<b>Open Positions:</b>")
        for pos in open_positions:
            symbol = html.escape(str(pos.get("symbol", "?")))
            lines.append(
                f"• <code>{symbol}</code> @ {pos.get('entry_price', 0.0):.6g} "
                f"-> {pos.get('unrealized_pnl_pct', 0.0):+.2f}% "
                f"(SL {pos.get('stop_loss', 0.0):.6g} / TP {pos.get('take_profit', 0.0):.6g})"
            )
    else:
        lines.append("No open positions.")
    return "\n".join(lines)


def format_circuit_breaker_tripped_alert(event: dict) -> str:
    lines = [
        "\U0001F6D1 <b>CIRCUIT BREAKER TRIPPED</b>",
        "",
        f"<b>Drawdown:</b> {event.get('drawdown_pct', 0.0):.2f}%",
        f"<b>Threshold:</b> {event.get('threshold_pct', 0.0):.2f}%",
        "",
        "New signal alerts and paper-trade entries are now HALTED. Existing open trades still "
        "run their own stop-loss/take-profit unchanged.",
        "",
        "Review what happened before resuming. Send <code>/resume</code> to clear the halt "
        "once you've decided to continue.",
    ]
    return "\n".join(lines)


def format_circuit_breaker_status_card(status: dict) -> str:
    state = "\U0001F6D1 HALTED" if status["tripped"] else "✅ ACTIVE"
    lines = [
        f"⚙️ <b>Circuit Breaker: {state}</b>",
        "",
        f"<b>Current Drawdown:</b> {status['drawdown_pct']:.2f}%",
        f"<b>Threshold:</b> {status['max_drawdown_pct']:.2f}%",
        f"<b>Peak Balance:</b> {status['peak_balance_usd']:.2f} USD",
        f"<b>Current Balance:</b> {status['current_balance_usd']:.2f} USD",
        f"<b>Manual Halt:</b> {'Yes' if status['manual_halt'] else 'No'}",
    ]
    return "\n".join(lines)


def dispatch_signal_alert(signal: dict) -> bool:
    return send_telegram_message(format_signal_alert_card(signal))


def dispatch_trade_closed_alert(trade: dict) -> bool:
    return send_telegram_message(format_trade_closed_alert(trade))


def dispatch_reconcile_event(event: dict | None) -> None:
    """Shared alert dispatch for reconciliation outcomes produced by
    analytics_engine.reconcile_trade_tick() - used identically by the REST reconciliation loop
    and the real-time WebSocket reconciler so both paths notify the user consistently."""
    if not event:
        return
    event_type = event.get("type")
    if event_type == "TP1":
        send_telegram_message(format_tp1_scaleout_alert(event["trade"]))
    elif event_type == "TP1_AND_TP2":
        send_telegram_message(format_tp1_scaleout_alert(event["tp1_trade"]))
        send_telegram_message(format_trade_closed_alert(event["trade"]))
    else:
        send_telegram_message(format_trade_closed_alert(event["trade"]))
