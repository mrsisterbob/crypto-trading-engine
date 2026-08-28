"""Real-time Binance mini-ticker WebSocket price feed for sub-second paper-trade reconciliation.

Runs on an isolated background daemon thread (standard `threading`, not asyncio) via the
`websocket-client` library, connected to Binance's combined mini-ticker stream
(`!miniTicker@arr`, pushes ALL market symbols roughly once per second). This bypasses the
5-minute REST reconciliation cadence entirely - every open paper trade is evaluated against a
fresh price the instant a tick for its symbol arrives, using the exact same decision logic
(analytics_engine.reconcile_trade_tick) as the REST fallback loop in main_2.py.
"""
import json
import logging
import threading
import time

import websocket

import alert_dispatcher
import analytics_engine
import circuit_breaker
import database

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/!miniTicker@arr"
RECONNECT_DELAY_SECONDS = 5

_price_cache: dict[str, float] = {}
_price_cache_lock = threading.Lock()


def get_cached_price(symbol: str) -> float | None:
    """Last real-time price observed for symbol, or None if no tick has arrived yet."""
    with _price_cache_lock:
        return _price_cache.get(symbol)


def _on_message(ws, message: str) -> None:
    try:
        tickers = json.loads(message)
    except (json.JSONDecodeError, TypeError):
        return
    if not isinstance(tickers, list):
        return

    tracked = analytics_engine.get_supported_pairs()
    relevant_prices: dict[str, float] = {}
    for ticker in tickers:
        symbol = ticker.get("s")
        if not symbol or symbol not in tracked:
            continue
        close_price_raw = ticker.get("c")
        if close_price_raw is None:
            continue
        try:
            relevant_prices[symbol] = float(close_price_raw)
        except (TypeError, ValueError):
            continue
    if not relevant_prices:
        return

    with _price_cache_lock:
        _price_cache.update(relevant_prices)

    try:
        open_trades = database.get_open_paper_trades()
    except Exception as e:
        logging.error(f"ws_reconciler: failed to fetch open trades: {e}")
        return

    for trade in open_trades:
        price = relevant_prices.get(trade["symbol"])
        if price is None:
            continue
        try:
            event = analytics_engine.reconcile_trade_tick(trade, price)
            if event:
                circuit_breaker.update_peak_balance()
            alert_dispatcher.dispatch_reconcile_event(event)
        except Exception as e:
            logging.error(f"ws_reconciler: failed to reconcile trade #{trade.get('id')} ({trade['symbol']}) @ {price}: {e}")


def _on_error(ws, error) -> None:
    logging.error(f"ws_reconciler: WebSocket error: {error}")


def _on_close(ws, close_status_code, close_msg) -> None:
    logging.warning(f"ws_reconciler: WebSocket closed (code={close_status_code}, msg={close_msg})")


def _on_open(ws) -> None:
    logging.info("ws_reconciler: connected to Binance mini-ticker stream.")


def _run_forever_with_reconnect() -> None:
    """Keeps the WebSocket connection alive indefinitely, reconnecting with a fixed delay on any
    disconnect/error so a transient network blip never permanently kills real-time reconciliation
    (the REST reconciliation loop in main_2.py remains as a backstop regardless)."""
    while True:
        try:
            ws_app = websocket.WebSocketApp(
                BINANCE_WS_URL,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
                on_open=_on_open,
            )
            ws_app.run_forever(ping_interval=180, ping_timeout=10)
        except Exception as e:
            logging.error(f"ws_reconciler: run_forever crashed: {e}", exc_info=True)
        logging.warning(f"ws_reconciler: disconnected, reconnecting in {RECONNECT_DELAY_SECONDS}s...")
        time.sleep(RECONNECT_DELAY_SECONDS)


def start_ws_reconciler() -> None:
    """Spins up the WebSocket price feed + reconciler as a daemon thread."""
    threading.Thread(target=_run_forever_with_reconnect, daemon=True, name="ws_reconciler").start()
    logging.info("ws_reconciler: background thread started.")
