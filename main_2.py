"""Flask webhook server + APScheduler orchestration for the Crypto Alert & Paper-Trading Engine
(v2): adds the dynamic parameter engine, portfolio heat cap, 2-stage TP scale-outs, and the
real-time WebSocket reconciler on top of main.py's original feature set.

Commands (Telegram): /scan, /portfolio, /paper_buy <PAIR> [size%], /config, and bare parameter
mutations like `risk = 1.5`, `alts + SUI`, `alts - ADA`, `rvol = 2.5`.
Background: 15-min market scan (APScheduler), 5-min REST reconciliation backstop (APScheduler),
and the real-time WebSocket reconciler (ws_reconciler.py) - all fully decoupled from the Flask
request thread.
"""
import html
import json
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
import circuit_breaker
import ws_reconciler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = Flask(__name__)
APP_START_TIME = time.time()

CRYPTO_TELEGRAM_CHAT_ID = os.environ.get("CRYPTO_TELEGRAM_CHAT_ID")
CRYPTO_TELEGRAM_WEBHOOK_SECRET = os.environ.get("CRYPTO_TELEGRAM_WEBHOOK_SECRET")
CRYPTO_ENGINE_PORT = int(os.environ.get("CRYPTO_ENGINE_PORT", "5001"))

SIGNAL_DEDUP_WINDOW_MINUTES = 60
PAPER_BUY_RE = re.compile(r"^/paper_buy\s+([A-Za-z0-9]{2,20})(?:\s+(\d+(?:\.\d+)?)\s*%?)?\s*$", re.IGNORECASE)

# ==============================================================================
# Dynamic Parameter Engine (mirrors the Job Outreach Engine's ALIAS_MAP/update_filter_param)
# ==============================================================================
ALIAS_MAP = {
    "risk": "risk_pct",
    "heat": "max_portfolio_heat_pct",
    "rvol": "rvol_threshold",
    "sl": "sl_atr_mult",
    "tp": "tp_rr_ratio",
    "fee": "fee_slippage_pct",
    "alts": "tracked_alts",
}
CONFIG_LIST_KEYS = {"tracked_alts"}
CONFIG_NUMERIC_KEYS = {"risk_pct", "max_portfolio_heat_pct", "rvol_threshold", "sl_atr_mult", "tp_rr_ratio", "fee_slippage_pct"}
CONFIG_NUMERIC_BOUNDS = {
    "risk_pct": (0.1, 10.0),
    "max_portfolio_heat_pct": (0.5, 50.0),
    "rvol_threshold": (1.0, 10.0),
    "sl_atr_mult": (0.1, 10.0),
    "tp_rr_ratio": (0.5, 10.0),
    "fee_slippage_pct": (0.0, 5.0),
}
CONFIG_MUTATION_RE = re.compile(r"^([A-Za-z_]+)\s*([=+\-])\s*(.+)$")

database.init_db()


def _resolve_config_key(raw_key: str) -> str | None:
    key = ALIAS_MAP.get(raw_key.lower().strip(), raw_key.lower().strip())
    return key if (key in CONFIG_LIST_KEYS or key in CONFIG_NUMERIC_KEYS) else None


def update_config_param(raw_key: str, op: str, raw_val: str) -> str:
    key = _resolve_config_key(raw_key)
    if not key:
        return f"❌ Unknown config parameter: <code>{html.escape(raw_key)}</code>"
    raw_val = raw_val.strip()

    if key in CONFIG_LIST_KEYS:
        try:
            current = json.loads(database.get_config(key, "[]"))
        except (json.JSONDecodeError, TypeError):
            current = []
        symbol = raw_val.upper().strip()
        if op == "+":
            if symbol and symbol not in current:
                current.append(symbol)
        elif op == "-":
            current = [s for s in current if s != symbol]
        else:
            current = [s.strip().upper() for s in raw_val.split(",") if s.strip()]
        database.set_config(key, json.dumps(current))
        return f"⚙️ <code>{key}</code> updated to: <code>{html.escape(json.dumps(current))}</code>"

    try:
        delta_or_new = float(raw_val)
    except ValueError:
        return f"❌ Invalid numeric value: <code>{html.escape(raw_val)}</code>"
    current_val = float(database.get_config(key, "0"))
    if op == "+":
        new_val = current_val + delta_or_new
    elif op == "-":
        new_val = current_val - delta_or_new
    else:
        new_val = delta_or_new
    bounds = CONFIG_NUMERIC_BOUNDS.get(key)
    if bounds:
        new_val = max(bounds[0], min(new_val, bounds[1]))
    database.set_config(key, str(new_val))
    return f"⚙️ <code>{key}</code> updated to <code>{new_val:g}</code>"


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
    if circuit_breaker.is_tripped():
        logging.info("scan_cycle_job: circuit breaker tripped, skipping scan.")
        return
    try:
        signals = analytics_engine.scan_all_symbols()
    except Exception as e:
        logging.error(f"scan_cycle_job failed: {e}", exc_info=True)
        return
    _process_scan_results(signals, notify=True)


def reconcile_paper_trades_job() -> None:
    """REST-based reconciliation backstop (5-min cadence). The real-time WebSocket reconciler
    (ws_reconciler.py) handles sub-second SL/TP/scale-out detection on the same shared decision
    logic (analytics_engine.reconcile_trade_tick); this loop exists purely in case the WS
    connection drops or misses a tick.

    Runs (and checks the circuit breaker) regardless of whether it's currently tripped - a
    tripped breaker blocks NEW entries and alerts, but existing open trades must keep being
    reconciled against their own stop-loss/take-profit unchanged."""
    try:
        open_trades = database.get_open_paper_trades()
        if open_trades:
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
                event = analytics_engine.reconcile_trade_tick(trade, price)
                if event:
                    circuit_breaker.update_peak_balance()
                alert_dispatcher.dispatch_reconcile_event(event)

        breach = circuit_breaker.check_and_latch()
        if breach:
            alert_dispatcher.send_telegram_message(alert_dispatcher.format_circuit_breaker_tripped_alert(breach))
    except Exception as e:
        logging.error(f"reconcile_paper_trades_job failed: {e}", exc_info=True)


scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(scan_cycle_job, "interval", minutes=15, id="market_scan", max_instances=1, coalesce=True, misfire_grace_time=120)
scheduler.add_job(reconcile_paper_trades_job, "interval", minutes=5, id="reconcile_paper_trades", max_instances=1, coalesce=True, misfire_grace_time=60)
scheduler.start()
ws_reconciler.start_ws_reconciler()


# ==============================================================================
# Telegram command handlers
# ==============================================================================
def _cmd_scan(chat_id: str) -> None:
    alert_dispatcher.send_telegram_message("🔍 Scanning markets (BTC/ETH gamma + tracked altcoins)...", chat_id=chat_id)
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


def _cmd_config(chat_id: str) -> None:
    try:
        tracked_alts = json.loads(database.get_config("tracked_alts", "[]"))
    except (json.JSONDecodeError, TypeError):
        tracked_alts = []
    params = {
        "risk_pct": database.get_config("risk_pct", "2.0"),
        "max_portfolio_heat_pct": database.get_config("max_portfolio_heat_pct", "6.0"),
        "rvol_threshold": database.get_config("rvol_threshold", "2.0"),
        "sl_atr_mult": database.get_config("sl_atr_mult", "1.5"),
        "tp_rr_ratio": database.get_config("tp_rr_ratio", "3.0"),
        "fee_slippage_pct": database.get_config("fee_slippage_pct", "0.13"),
        "tracked_alts": ", ".join(tracked_alts) if tracked_alts else "None",
    }
    alert_dispatcher.send_telegram_message(alert_dispatcher.format_config_card(params), chat_id=chat_id)


def _cmd_paper_buy(text: str, chat_id: str) -> None:
    if circuit_breaker.is_tripped():
        alert_dispatcher.send_telegram_message(
            "🛑 Circuit breaker is active - new entries are blocked. Send <code>/status</code> "
            "to review, or <code>/resume</code> once you've decided to continue.",
            chat_id=chat_id,
        )
        return
    match = PAPER_BUY_RE.match(text)
    if not match:
        alert_dispatcher.send_telegram_message("⚠️ Invalid format. Usage: <code>/paper_buy BTCUSDT 2%</code>", chat_id=chat_id)
        return
    pair = match.group(1).upper()
    supported_pairs = analytics_engine.get_supported_pairs()
    if pair not in supported_pairs:
        supported = ", ".join(sorted(supported_pairs))
        alert_dispatcher.send_telegram_message(f"⚠️ Unsupported pair '{html.escape(pair)}'. Supported: {supported}", chat_id=chat_id)
        return

    risk_pct_raw = match.group(2)
    risk_pct = float(risk_pct_raw) if risk_pct_raw else float(database.get_config("risk_pct", "2.0"))
    risk_pct = max(0.1, min(risk_pct, 10.0))

    max_heat = float(database.get_config("max_portfolio_heat_pct", "6.0"))
    current_open_risk = database.get_total_open_risk_pct()
    if current_open_risk + risk_pct > max_heat:
        alert_dispatcher.send_telegram_message(
            alert_dispatcher.format_heat_cap_rejection(pair, risk_pct, current_open_risk, max_heat),
            chat_id=chat_id,
        )
        return

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


def _cmd_status(chat_id: str) -> None:
    alert_dispatcher.send_telegram_message(
        alert_dispatcher.format_circuit_breaker_status_card(circuit_breaker.status()), chat_id=chat_id,
    )


def _cmd_halt(chat_id: str) -> None:
    circuit_breaker.halt_manual()
    alert_dispatcher.send_telegram_message(
        "🛑 Manually halted. New signal alerts and paper-trade entries are blocked until <code>/resume</code>.",
        chat_id=chat_id,
    )


def _cmd_resume(chat_id: str) -> None:
    circuit_breaker.reset()
    alert_dispatcher.send_telegram_message(
        "✅ Circuit breaker reset. Peak balance re-anchored to current balance - drawdown is now tracked fresh from here.",
        chat_id=chat_id,
    )


def _handle_command(text: str, chat_id: str) -> None:
    try:
        if text == "/scan":
            _cmd_scan(chat_id)
        elif text == "/portfolio":
            _cmd_portfolio(chat_id)
        elif text == "/config":
            _cmd_config(chat_id)
        elif text == "/status":
            _cmd_status(chat_id)
        elif text == "/halt":
            _cmd_halt(chat_id)
        elif text == "/resume":
            _cmd_resume(chat_id)
        elif text.lower().startswith("/paper_buy"):
            _cmd_paper_buy(text, chat_id)
        elif not text.startswith("/") and (m := CONFIG_MUTATION_RE.match(text)):
            reply = update_config_param(m.group(1), m.group(2), m.group(3))
            alert_dispatcher.send_telegram_message(reply, chat_id=chat_id)
        else:
            alert_dispatcher.send_telegram_message(
                "Unknown command. Available: /scan, /portfolio, /paper_buy <PAIR> [size%], /config, "
                "/status, /halt, /resume, or a parameter mutation like <code>risk = 1.5</code>.",
                chat_id=chat_id,
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
