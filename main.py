"""Flask webhook server + APScheduler orchestration for the Crypto Alert & Paper-Trading Engine.

Commands (Telegram): /scan, /portfolio, /paper_buy <PAIR> [size%]
Background jobs: a 15-min market scan cycle and a 5-min paper-trade reconciliation loop, both
run on APScheduler's own thread pool - fully decoupled from the Flask request thread.
"""
import html
import logging
import os
import re
import threading
import time

from flask import Flask, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler

import database
import analytics_engine
import alert_dispatcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = Flask(__name__)
APP_START_TIME = time.time()

CRYPTO_TELEGRAM_CHAT_ID = os.environ.get("CRYPTO_TELEGRAM_CHAT_ID")
CRYPTO_TELEGRAM_WEBHOOK_SECRET = os.environ.get("CRYPTO_TELEGRAM_WEBHOOK_SECRET")
CRYPTO_ENGINE_PORT = int(os.environ.get("CRYPTO_ENGINE_PORT", "5001"))

SIGNAL_DEDUP_WINDOW_MINUTES = 60
PAPER_BUY_RE = re.compile(r"^/paper_buy\s+([A-Za-z0-9]{2,20})(?:\s+(\d+(?:\.\d+)?)\s*%?)?\s*$", re.IGNORECASE)

database.init_db()


# ==============================================================================
# Scheduled jobs (APScheduler background thread pool, decoupled from Flask)
# ==============================================================================
def _process_scan_results(signals: list[dict], notify: bool) -> list[dict]:
    """Persists newly-triggered signals and dispatches alerts. Skips symbols that already
    have a fresh OPEN signal within SIGNAL_DEDUP_WINDOW_MINUTES to avoid alert spam when
    /scan is run manually shortly after a scheduled cycle."""
    newly_created = []
    for signal in signals:
        if not signal.get("triggered"):
            continue
        existing = database.get_latest_open_signal(signal["symbol"], max_age_minutes=SIGNAL_DEDUP_WINDOW_MINUTES)
        if existing:
            logging.info(f"Skipping duplicate signal for {signal['symbol']} (OPEN signal #{existing['id']} still fresh)")
            continue
        signal_id = database.create_open_signal(signal)
        signal["signal_id"] = signal_id
        newly_created.append(signal)
        if notify:
            alert_dispatcher.dispatch_signal_alert(signal)
    return newly_created


def scan_cycle_job() -> None:
    try:
        signals = analytics_engine.scan_all_symbols()
    except Exception as e:
        logging.error(f"scan_cycle_job failed: {e}", exc_info=True)
        return
    _process_scan_results(signals, notify=True)


def _close_trade(trade: dict, close_price: float, status: str) -> None:
    # database.close_paper_trade now computes fee-aware PnL internally (crypto_engine v2 schema).
    fee_pct = float(database.get_config("fee_slippage_pct", "0.13"))
    closed = database.close_paper_trade(trade["id"], close_price, status, fee_pct)
    if closed:
        alert_dispatcher.dispatch_trade_closed_alert(closed)


def reconcile_paper_trades_job() -> None:
    """Checks every OPEN paper trade against the current spot price and closes it if the
    stop-loss or take-profit has been hit, logging the final PnL."""
    try:
        open_trades = database.get_open_paper_trades()
        if not open_trades:
            return
        prices = {}
        for symbol in {t["symbol"] for t in open_trades}:
            try:
                prices[symbol] = analytics_engine.fetch_current_price(symbol)
            except Exception as e:
                logging.error(f"reconcile_paper_trades_job: price fetch failed for {symbol}: {e}")
        for trade in open_trades:
            price = prices.get(trade["symbol"])
            if price is None:
                continue
            if price <= trade["stop_loss"]:
                _close_trade(trade, price, "CLOSED_SL")
            elif price >= trade["take_profit"]:
                _close_trade(trade, price, "CLOSED_TP")
    except Exception as e:
        logging.error(f"reconcile_paper_trades_job failed: {e}", exc_info=True)


scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(scan_cycle_job, "interval", minutes=15, id="market_scan",
                   max_instances=1, coalesce=True, misfire_grace_time=120)
scheduler.add_job(reconcile_paper_trades_job, "interval", minutes=5, id="reconcile_paper_trades",
                   max_instances=1, coalesce=True, misfire_grace_time=60)
scheduler.start()


# ==============================================================================
# Telegram command handlers
# ==============================================================================
def _cmd_scan(chat_id: str) -> None:
    alert_dispatcher.send_telegram_message("🔍 Scanning markets (BTC/ETH gamma + 7 altcoins)...", chat_id=chat_id)
    try:
        signals = analytics_engine.scan_all_symbols()
    except Exception as e:
        logging.error(f"/scan failed: {e}", exc_info=True)
        alert_dispatcher.send_telegram_message(f"⚠️ Scan failed: {html.escape(str(e))}", chat_id=chat_id)
        return
    triggered = _process_scan_results(signals, notify=True)
    if triggered:
        return
    near_miss = sorted((s for s in signals if not s.get("error")), key=lambda s: s.get("confidence_score", 0), reverse=True)[:3]
    lines = ["✅ Scan complete. No breakout triggers this cycle.", "", "<b>Closest setups:</b>"]
    for s in near_miss:
        lines.append(f"• <code>{html.escape(s['symbol'])}</code> - {s.get('confidence_score', 0)}/100")
    alert_dispatcher.send_telegram_message("\n".join(lines), chat_id=chat_id)


def _build_portfolio_summary() -> dict:
    open_trades = database.get_open_paper_trades()
    closed_stats = database.get_closed_trade_stats()
    virtual_balance = float(database.get_config("virtual_balance_usd", "10000"))

    open_positions = []
    for trade in open_trades:
        try:
            current_price = analytics_engine.fetch_current_price(trade["symbol"])
            unrealized_pnl_pct = (current_price - trade["entry_price"]) / trade["entry_price"] * 100.0
        except Exception as e:
            logging.warning(f"Portfolio price fetch failed for {trade['symbol']}: {e}")
            unrealized_pnl_pct = 0.0
        open_positions.append({**trade, "unrealized_pnl_pct": unrealized_pnl_pct})

    return {
        "virtual_balance_usd": virtual_balance,
        "open_count": len(open_trades),
        "open_positions": open_positions,
        "realized_pnl_usd": closed_stats["realized_pnl_usd"],
        "win_rate_pct": closed_stats["win_rate_pct"],
        "wins": closed_stats["wins"],
        "losses": closed_stats["losses"],
    }


def _cmd_portfolio(chat_id: str) -> None:
    try:
        summary = _build_portfolio_summary()
        alert_dispatcher.send_telegram_message(alert_dispatcher.format_portfolio_summary_card(summary), chat_id=chat_id)
    except Exception as e:
        logging.error(f"/portfolio failed: {e}", exc_info=True)
        alert_dispatcher.send_telegram_message(f"⚠️ Failed to build portfolio summary: {html.escape(str(e))}", chat_id=chat_id)


def _cmd_paper_buy(text: str, chat_id: str) -> None:
    match = PAPER_BUY_RE.match(text)
    if not match:
        alert_dispatcher.send_telegram_message("⚠️ Invalid format. Usage: <code>/paper_buy BTCUSDT 2%</code>", chat_id=chat_id)
        return
    pair = match.group(1).upper()
    if pair not in analytics_engine.SUPPORTED_PAIRS:
        supported = ", ".join(sorted(analytics_engine.SUPPORTED_PAIRS))
        alert_dispatcher.send_telegram_message(f"⚠️ Unsupported pair '{html.escape(pair)}'. Supported: {supported}", chat_id=chat_id)
        return

    risk_pct_raw = match.group(2)
    risk_pct = float(risk_pct_raw) if risk_pct_raw else float(database.get_config("default_risk_pct", "2.0"))
    risk_pct = max(0.1, min(risk_pct, 10.0))

    try:
        signal = database.get_latest_open_signal(pair, max_age_minutes=SIGNAL_DEDUP_WINDOW_MINUTES)
        if signal:
            entry_price = signal["entry_price"]
            stop_loss = signal["stop_loss"]
            take_profit = signal["take_profit"]
            signal_id = signal["id"]
        else:
            entry_price = analytics_engine.fetch_current_price(pair)
            candles = analytics_engine.fetch_klines(pair, interval="4h", limit=30)
            atr_14 = analytics_engine.calculate_atr(candles)
            stop_loss, take_profit = analytics_engine.calculate_risk_levels(entry_price, atr_14)
            signal_id = None

        risk_per_unit = entry_price - stop_loss
        if risk_per_unit <= 0:
            raise ValueError("Computed stop-loss is not below entry price; refusing to size trade.")

        virtual_balance = float(database.get_config("virtual_balance_usd", "10000"))
        quantity = (virtual_balance * (risk_pct / 100.0)) / risk_per_unit

        trade_id = database.insert_paper_trade(
            symbol=pair, side="LONG", entry_price=entry_price, quantity=quantity,
            risk_pct=risk_pct, stop_loss=stop_loss, take_profit=take_profit, signal_id=signal_id,
        )
        alert_dispatcher.send_telegram_message(
            alert_dispatcher.format_paper_buy_confirmation({
                "id": trade_id, "symbol": pair, "entry_price": entry_price, "quantity": quantity,
                "risk_pct": risk_pct, "stop_loss": stop_loss, "take_profit": take_profit,
            }),
            chat_id=chat_id,
        )
    except Exception as e:
        logging.error(f"/paper_buy failed for {pair}: {e}", exc_info=True)
        alert_dispatcher.send_telegram_message(f"⚠️ Failed to open paper trade: {html.escape(str(e))}", chat_id=chat_id)


def _handle_command(text: str, chat_id: str) -> None:
    try:
        if text == "/scan":
            _cmd_scan(chat_id)
        elif text == "/portfolio":
            _cmd_portfolio(chat_id)
        elif text.lower().startswith("/paper_buy"):
            _cmd_paper_buy(text, chat_id)
        else:
            alert_dispatcher.send_telegram_message(
                "Unknown command. Available: /scan, /portfolio, /paper_buy <PAIR> [size%]", chat_id=chat_id,
            )
    except Exception as e:
        logging.error(f"_handle_command failed for '{text}': {e}", exc_info=True)
        alert_dispatcher.send_telegram_message(f"⚠️ Internal error: {html.escape(str(e))}", chat_id=chat_id)


# ==============================================================================
# Flask routes
# ==============================================================================
@app.route("/health", methods=["GET"])
def health():
    start = time.time()
    try:
        with database.get_db_conn() as conn:
            mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
        return jsonify({
            "status": "ok", "mode": mode,
            "elapsed_ms": round((time.time() - start) * 1000, 2),
            "uptime_s": round(time.time() - APP_START_TIME, 1),
        }), 200
    except Exception as e:
        logging.error(f"/health check failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    if CRYPTO_TELEGRAM_WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != CRYPTO_TELEGRAM_WEBHOOK_SECRET:
        return jsonify({"status": "error", "message": "forbidden"}), 403

    data = request.get_json(silent=True) or {}
    message = data.get("message") or data.get("edited_message")
    if not message:
        return jsonify({"status": "ignored"}), 200

    chat_id = str(message.get("chat", {}).get("id", ""))
    if CRYPTO_TELEGRAM_CHAT_ID and chat_id != str(CRYPTO_TELEGRAM_CHAT_ID):
        logging.warning(f"Rejected Telegram command from unauthorized chat_id={chat_id}")
        return jsonify({"status": "ignored"}), 200

    text = (message.get("text") or "").strip()
    if not text:
        return jsonify({"status": "ignored"}), 200

    threading.Thread(target=_handle_command, args=(text, chat_id), daemon=True).start()
    return jsonify({"status": "accepted"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=CRYPTO_ENGINE_PORT, threaded=True)
