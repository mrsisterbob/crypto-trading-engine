"""Live drawdown circuit breaker: halts new signal alerts and paper-trade entries once realized
drawdown from the peak virtual balance crosses a threshold you set in system_config.

This exists because a backtest-validated strategy can still diverge badly from its historical
behavior once traded live (regime change, overfit parameters, plain bad luck) - and the failure
mode that actually blows up retail traders is usually not the losses themselves, it's continuing
to trade unchanged through a losing streak that has already invalidated the plan. The threshold
below is deliberately a config value, not a hardcoded constant: deciding what drawdown you're
willing to tolerate before stopping is a personal risk decision this module cannot make for you -
set it once, in a calm moment, before you need it.

Tripping the breaker does not close open positions - it only blocks NEW entries and signal
alerts until manually reset via /resume, so an in-progress trade's own stop-loss/take-profit
still governs its exit exactly as it would otherwise.
"""
import logging

import database

DEFAULT_MAX_DRAWDOWN_PCT = 15.0  # halts new entries once balance falls this far below its peak


def _get_peak_balance() -> float:
    peak = database.get_config("peak_virtual_balance_usd")
    current = float(database.get_config("virtual_balance_usd", "10000"))
    return float(peak) if peak else current


def update_peak_balance() -> float:
    """Call after every balance-changing event (trade close, partial close). Ratchets the
    stored peak upward only - never resets it downward except via manual breaker reset, since
    the peak must represent the high-water mark for drawdown to mean anything."""
    current = float(database.get_config("virtual_balance_usd", "10000"))
    peak = _get_peak_balance()
    new_peak = max(peak, current)
    if new_peak != peak:
        database.set_config("peak_virtual_balance_usd", str(new_peak))
    return new_peak


def get_current_drawdown_pct() -> float:
    peak = _get_peak_balance()
    current = float(database.get_config("virtual_balance_usd", "10000"))
    if peak <= 0:
        return 0.0
    return max(0.0, (peak - current) / peak * 100.0)


def is_tripped() -> bool:
    """True if the breaker is latched (either by drawdown breach or manual /halt), meaning new
    entries and alerts should be suppressed until an explicit /resume."""
    if database.get_config("circuit_breaker_manual_halt", "0") == "1":
        return True
    max_dd = float(database.get_config("max_drawdown_pct", str(DEFAULT_MAX_DRAWDOWN_PCT)))
    return get_current_drawdown_pct() >= max_dd


def check_and_latch() -> dict | None:
    """Call this once per scan/reconciliation cycle. If drawdown just crossed the threshold and
    the breaker wasn't already latched, latches it (persists past restarts) and returns an event
    dict for alerting; returns None if nothing changed this call, including when the breaker was
    already latched (so the same trip doesn't re-alert every cycle).
    """
    already_latched = database.get_config("circuit_breaker_drawdown_latch", "0") == "1"
    max_dd = float(database.get_config("max_drawdown_pct", str(DEFAULT_MAX_DRAWDOWN_PCT)))
    current_dd = get_current_drawdown_pct()

    if current_dd >= max_dd and not already_latched:
        database.set_config("circuit_breaker_drawdown_latch", "1")
        logging.critical(f"CIRCUIT BREAKER TRIPPED: drawdown {current_dd:.2f}% >= threshold {max_dd:.2f}%")
        return {"event": "TRIPPED", "drawdown_pct": current_dd, "threshold_pct": max_dd}

    if current_dd < max_dd and already_latched:
        # Balance recovered above the threshold on its own - still requires an explicit /resume
        # to actually clear the manual latch and resume trading; this just avoids double-alerting.
        return None

    return None


def reset(chat_id: str = None) -> str:
    """Manually clears both the drawdown latch and any manual halt, and re-anchors the peak to
    the current balance so drawdown is measured fresh from here forward. Use only after actually
    reviewing what happened - not as a reflex to get alerts flowing again."""
    database.set_config("circuit_breaker_drawdown_latch", "0")
    database.set_config("circuit_breaker_manual_halt", "0")
    current = float(database.get_config("virtual_balance_usd", "10000"))
    database.set_config("peak_virtual_balance_usd", str(current))
    logging.warning("Circuit breaker manually reset; peak balance re-anchored to current balance.")
    return "circuit_breaker_reset"


def halt_manual() -> str:
    """Manual kill switch, independent of drawdown - e.g. you want to stop trading for reasons
    the drawdown number can't see (news event, personal judgment, going on vacation)."""
    database.set_config("circuit_breaker_manual_halt", "1")
    logging.warning("Circuit breaker manually halted.")
    return "circuit_breaker_halted"


def status() -> dict:
    return {
        "tripped": is_tripped(),
        "drawdown_pct": get_current_drawdown_pct(),
        "max_drawdown_pct": float(database.get_config("max_drawdown_pct", str(DEFAULT_MAX_DRAWDOWN_PCT))),
        "peak_balance_usd": _get_peak_balance(),
        "current_balance_usd": float(database.get_config("virtual_balance_usd", "10000")),
        "manual_halt": database.get_config("circuit_breaker_manual_halt", "0") == "1",
    }
