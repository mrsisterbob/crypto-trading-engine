"""PnL / ledger correctness tests for database.close_paper_trade, database.partial_close_tp1,
and analytics_engine.reconcile_trade_tick.

These paths had zero coverage. They move real (virtual) money and are hit concurrently by the
5-min REST reconciliation loop and the real-time WebSocket reconciler, so the invariants that
matter are: a close is idempotent (no double-count), the fee haircut hits both entry and exit
notional, a single tick through both TP levels nets to one coherent close, and the balance
column stays correct.
"""
import threading

import pytest

import analytics_engine
import database

FEE_PCT = 0.13
START_BALANCE = 10000.0


def _balance() -> float:
    return float(database.get_config("virtual_balance_usd"))


def _open_trade(entry=100.0, qty=10.0, sl=90.0, tp=130.0, risk_pct=2.0) -> int:
    return database.insert_paper_trade(
        symbol="BTCUSDT", side="LONG", entry_price=entry, quantity=qty,
        risk_pct=risk_pct, stop_loss=sl, take_profit=tp,
    )


def _expected_haircut(entry, exit_, qty, fee_pct=FEE_PCT):
    """Independent re-derivation of the round-trip haircut: gross move minus a fee on the entry
    notional AND a fee on the exit notional."""
    fee = fee_pct / 100.0
    return (exit_ - entry) * qty - (entry * qty * fee) - (exit_ * qty * fee)


# ---------------------------------------------------------------------------
# Fee-haircut math: applied to BOTH entry and exit notional
# ---------------------------------------------------------------------------
def test_fee_haircut_charges_both_legs_even_with_zero_price_move():
    # Price unchanged => gross PnL is 0, so anything non-zero is pure round-trip cost.
    # Both legs at notional 100*5 => two charges of 100*5*0.0013 = 0.65 each.
    pnl = database._fee_haircut_pnl(100.0, 100.0, 5.0, FEE_PCT)
    assert pnl == pytest.approx(-(100.0 * 5.0 * 0.0013) * 2)
    assert pnl == pytest.approx(-1.30)


def test_fee_haircut_exit_leg_uses_exit_price_not_entry_price():
    # entry notional 100*5, exit notional 120*5 => the two fee charges differ, which can only
    # happen if the exit leg is charged against the exit price.
    pnl = database._fee_haircut_pnl(100.0, 120.0, 5.0, 0.5)
    gross = (120.0 - 100.0) * 5.0
    entry_fee = 100.0 * 5.0 * 0.005
    exit_fee = 120.0 * 5.0 * 0.005
    assert entry_fee != exit_fee
    assert pnl == pytest.approx(gross - entry_fee - exit_fee)
    assert pnl == pytest.approx(94.5)


def test_close_paper_trade_pnl_matches_independent_haircut():
    tid = _open_trade(entry=100.0, qty=10.0)
    closed = database.close_paper_trade(tid, 90.0, "CLOSED_SL", FEE_PCT)
    assert closed is not None
    assert closed["pnl_usd"] == pytest.approx(_expected_haircut(100.0, 90.0, 10.0))
    # pnl_pct is measured against full original cost basis (entry * initial_quantity).
    assert closed["pnl_pct"] == pytest.approx(closed["pnl_usd"] / (100.0 * 10.0) * 100.0)


# ---------------------------------------------------------------------------
# Double-close race: second close must return None and not move money again
# ---------------------------------------------------------------------------
def test_double_close_returns_none_and_does_not_double_count():
    tid = _open_trade(entry=100.0, qty=10.0)
    assert _balance() == pytest.approx(START_BALANCE)

    first = database.close_paper_trade(tid, 90.0, "CLOSED_SL", FEE_PCT)
    assert first is not None
    expected_pnl = _expected_haircut(100.0, 90.0, 10.0)
    balance_after_first = _balance()
    assert balance_after_first == pytest.approx(START_BALANCE + expected_pnl)

    second = database.close_paper_trade(tid, 90.0, "CLOSED_SL", FEE_PCT)
    assert second is None
    assert _balance() == pytest.approx(balance_after_first)  # unchanged, not charged twice
    assert database.get_open_paper_trades() == []


def test_double_close_race_under_threads_charges_once():
    tid = _open_trade(entry=100.0, qty=10.0)
    results = []
    barrier = threading.Barrier(6)

    def worker():
        barrier.wait()
        results.append(database.close_paper_trade(tid, 90.0, "CLOSED_SL", FEE_PCT))

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    assert _balance() == pytest.approx(START_BALANCE + _expected_haircut(100.0, 90.0, 10.0))


def test_close_rejects_unknown_status():
    tid = _open_trade()
    with pytest.raises(ValueError):
        database.close_paper_trade(tid, 100.0, "NOT_A_STATUS", FEE_PCT)


# ---------------------------------------------------------------------------
# partial_close_tp1: 50% scale-out, stop to breakeven, atomic balance
# ---------------------------------------------------------------------------
def test_partial_close_tp1_scales_out_and_moves_stop_to_breakeven():
    tid = _open_trade(entry=100.0, qty=10.0, sl=90.0, tp=130.0)
    updated = database.partial_close_tp1(tid, 5.0, 110.0, FEE_PCT)
    assert updated is not None

    slice_pnl = _expected_haircut(100.0, 110.0, 5.0)
    assert updated["partial_pnl_usd"] == pytest.approx(slice_pnl)
    assert updated["quantity"] == pytest.approx(5.0)
    assert updated["tp1_hit"] == 1
    assert updated["stop_loss"] == pytest.approx(100.0)  # breakeven == entry
    assert updated["realized_pnl_usd"] == pytest.approx(slice_pnl)
    assert _balance() == pytest.approx(START_BALANCE + slice_pnl)


def test_partial_close_tp1_is_not_repeatable():
    tid = _open_trade()
    assert database.partial_close_tp1(tid, 5.0, 110.0, FEE_PCT) is not None
    balance_after = _balance()
    # tp1_hit is now 1, so the WHERE ... AND tp1_hit = 0 guard rejects the second call.
    assert database.partial_close_tp1(tid, 5.0, 110.0, FEE_PCT) is None
    assert _balance() == pytest.approx(balance_after)


def test_partial_then_final_close_nets_to_one_coherent_trade():
    tid = _open_trade(entry=100.0, qty=10.0, sl=90.0, tp=130.0)
    database.partial_close_tp1(tid, 5.0, 110.0, FEE_PCT)
    closed = database.close_paper_trade(tid, 130.0, "CLOSED_TP", FEE_PCT)
    assert closed is not None

    tp1_slice = _expected_haircut(100.0, 110.0, 5.0)
    tp2_slice = _expected_haircut(100.0, 130.0, 5.0)
    total = tp1_slice + tp2_slice
    assert closed["pnl_usd"] == pytest.approx(total)
    assert closed["pnl_pct"] == pytest.approx(total / (100.0 * 10.0) * 100.0)
    assert _balance() == pytest.approx(START_BALANCE + total)


# ---------------------------------------------------------------------------
# reconcile_trade_tick: the TP1_AND_TP2 gap case
# ---------------------------------------------------------------------------
def _fresh_trade_dict(tid: int) -> dict:
    rows = [t for t in database.get_open_paper_trades() if t["id"] == tid]
    assert rows, f"trade {tid} not open"
    return rows[0]


def test_reconcile_single_tick_through_tp1_and_tp2_closes_once():
    tid = _open_trade(entry=100.0, qty=10.0, sl=90.0, tp=130.0)
    trade = _fresh_trade_dict(tid)

    event = analytics_engine.reconcile_trade_tick(trade, 140.0)
    assert event is not None
    assert event["type"] == "TP1_AND_TP2"
    assert event["trade"]["status"] == "CLOSED_TP"

    # Exactly one row, closed once, PnL == the 50% TP1 slice + the 50% final slice.
    with database.get_db_conn() as conn:
        rows = conn.execute("SELECT * FROM paper_portfolio WHERE id = ?", (tid,)).fetchall()
    assert len(rows) == 1
    row = dict(rows[0])
    assert row["status"] == "CLOSED_TP"

    tp1_slice = _expected_haircut(100.0, 140.0, 5.0)
    final_slice = _expected_haircut(100.0, 140.0, 5.0)
    total = tp1_slice + final_slice
    assert row["pnl_usd"] == pytest.approx(total)
    assert _balance() == pytest.approx(START_BALANCE + total)


def test_reconcile_second_pass_on_stale_trade_is_a_noop():
    tid = _open_trade(entry=100.0, qty=10.0, sl=90.0, tp=130.0)
    trade = _fresh_trade_dict(tid)

    analytics_engine.reconcile_trade_tick(trade, 140.0)
    balance_after = _balance()

    # The REST loop and the WS reconciler can both still hold this same pre-close dict.
    assert analytics_engine.reconcile_trade_tick(trade, 140.0) is None
    assert analytics_engine.reconcile_trade_tick(trade, 50.0) is None
    assert _balance() == pytest.approx(balance_after)


def test_reconcile_stop_loss_before_tp1():
    tid = _open_trade(entry=100.0, qty=10.0, sl=90.0, tp=130.0)
    trade = _fresh_trade_dict(tid)
    event = analytics_engine.reconcile_trade_tick(trade, 90.0)
    assert event is not None and event["type"] == "CLOSED_SL"
    assert _balance() == pytest.approx(START_BALANCE + _expected_haircut(100.0, 90.0, 10.0))


def test_reconcile_tp1_only_then_breakeven_stop():
    tid = _open_trade(entry=100.0, qty=10.0, sl=90.0, tp=130.0)
    # First tick is past the TP1 trigger (110) but short of TP2 (130). The scale-out fills at
    # the actual tick price (115), not at the 110 trigger level.
    event = analytics_engine.reconcile_trade_tick(_fresh_trade_dict(tid), 115.0)
    assert event is not None and event["type"] == "TP1"
    tp1_slice = _expected_haircut(100.0, 115.0, 5.0)
    assert _balance() == pytest.approx(START_BALANCE + tp1_slice)

    # Later tick drops back to the breakeven stop (now == entry 100).
    event2 = analytics_engine.reconcile_trade_tick(_fresh_trade_dict(tid), 100.0)
    assert event2 is not None and event2["type"] == "CLOSED_BE"
    be_slice = _expected_haircut(100.0, 100.0, 5.0)
    assert _balance() == pytest.approx(START_BALANCE + tp1_slice + be_slice)


# ---------------------------------------------------------------------------
# Balance column correctness after the atomic-statement change (Item 3)
# ---------------------------------------------------------------------------
def test_atomic_balance_increment_statement_accumulates():
    now = database.utcnow_iso()
    with database.get_db_conn(immediate=True) as conn:
        for _ in range(5):
            conn.execute(
                "UPDATE system_config SET value = CAST(value AS REAL) + ?, updated_at = ? "
                "WHERE key = 'virtual_balance_usd'",
                (3.5, now),
            )
    assert _balance() == pytest.approx(START_BALANCE + 5 * 3.5)


def test_balance_correct_after_many_concurrent_closes():
    n = 12
    tids = [_open_trade(entry=100.0, qty=1.0, sl=90.0, tp=130.0) for _ in range(n)]
    per_trade_pnl = _expected_haircut(100.0, 105.0, 1.0)
    barrier = threading.Barrier(n)

    def worker(tid):
        barrier.wait()
        database.close_paper_trade(tid, 105.0, "CLOSED_TP", FEE_PCT)

    threads = [threading.Thread(target=worker, args=(tid,)) for tid in tids]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    stats = database.get_closed_trade_stats()
    assert stats["total_closed"] == n
    assert _balance() == pytest.approx(START_BALANCE + n * per_trade_pnl)
