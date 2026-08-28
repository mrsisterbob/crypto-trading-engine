"""Populate a fresh crypto_engine.db with synthetic demo state so `/portfolio`, `/scan`
output formatting, and the alert dispatchers have something to render on a clean checkout.

Everything here is fabricated - not a real trade history, not a real signal. Run once:

    python seed_sample_data.py

Then start the engine (`python main_2.py`) and `/portfolio` will show the seeded state.
Delete crypto_engine.db to get back to an empty install.
"""
from __future__ import annotations

import database as db


SAMPLE_SIGNALS = [
    {
        "symbol": "BTCUSDT", "asset_class": "major", "confidence_score": 71,
        "current_price": 61240.0, "stop_loss": 59800.0, "take_profit": 64100.0,
        "rvol_ratio": 2.4,
        "context_summary": "Donchian(20) breakout, RVOL 2.4x, dealer gamma flips positive above 61k",
        "reasons": ["close > donchian_high", "rvol_ratio >= 2.0", "positive_gamma_above_spot"],
    },
    {
        "symbol": "SOLUSDT", "asset_class": "alt", "confidence_score": 64,
        "current_price": 148.20, "stop_loss": 141.50, "take_profit": 165.00,
        "rvol_ratio": 3.1,
        "context_summary": "Range-high breakout on 3.1x RVOL; funding still flat (no crowded long yet)",
        "reasons": ["close > donchian_high", "rvol_ratio >= 2.0", "funding_not_extended"],
    },
]

# (symbol, side, entry, qty, risk_pct, stop, take_profit)
SAMPLE_OPEN_TRADES = [
    ("BTCUSDT", "LONG", 61240.0, 0.0163, 2.0, 59800.0, 64100.0),
    ("ETHUSDT", "LONG", 2980.0, 0.34, 2.0, 2870.0, 3220.0),
]


def main() -> None:
    db.init_db()

    existing = db.get_open_paper_trades()
    if existing:
        print(f"crypto_engine.db already has {len(existing)} open paper trade(s) - not re-seeding.")
        print("Delete crypto_engine.db first if you want a clean seed.")
        return

    for sig in SAMPLE_SIGNALS:
        sid = db.create_open_signal(sig)
        print(f"  seeded signal #{sid}: {sig['symbol']} ({sig['confidence_score']}/100)")

    for symbol, side, entry, qty, risk, sl, tp in SAMPLE_OPEN_TRADES:
        tid = db.insert_paper_trade(symbol, side, entry, qty, risk, sl, tp)
        print(f"  seeded paper trade #{tid}: {side} {qty} {symbol} @ {entry}")

    db.set_config("virtual_balance_usd", "10412.00")  # as if a couple of scale-outs already banked
    print("\nSeed complete. Start the engine with `python main_2.py` and send /portfolio.")


if __name__ == "__main__":
    main()
