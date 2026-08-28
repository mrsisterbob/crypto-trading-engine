"""Data ingestion + Hybrid Sniper Score logic engine for the Crypto Alert & Paper-Trading Engine.

Zero-cost public REST endpoints only (Deribit, Binance, CoinGecko) via `requests` - no ccxt,
no API keys required. All outbound HTTP calls go through `_get`, decorated with exponential
backoff for HTTP 429/5xx responses and connection-level failures.
"""
import functools
import json
import logging
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests

import database

# ---------------------------------------------------------------------------
# Symbols & tunable thresholds
# ---------------------------------------------------------------------------
MAJOR_SYMBOLS = ["BTC", "ETH"]
ALT_SYMBOLS = ["SOL", "BNB", "XRP", "ADA", "AVAX", "LINK", "DOT"]
SUPPORTED_PAIRS = {f"{s}USDT" for s in MAJOR_SYMBOLS + ALT_SYMBOLS}

DONCHIAN_PERIOD = 20
ATR_PERIOD = 14
RVOL_PERIOD = 20
RVOL_TRIGGER_THRESHOLD = 2.0                # 200% of the 20-period average volume
SL_ATR_MULTIPLIER = 1.5
RISK_REWARD_RATIO = 3.0
FUNDING_DEEPLY_NEGATIVE_DAILY_PCT = -0.10   # daily-equivalent funding rate considered "deeply negative"
BTC_DOMINANCE_VOL_THRESHOLD_PCT = 1.5       # 24h high-low range on BTC.D considered "extreme"

GAMMA_NEGATIVE_CONTEXT_SCORE = 40
GAMMA_POSITIVE_CONTEXT_SCORE = 10
OI_FUNDING_FULL_CONTEXT_SCORE = 40
OI_FUNDING_PARTIAL_CONTEXT_SCORE = 20
OI_FUNDING_MIN_CONTEXT_SCORE = 5
PIN_CLUSTER_PROXIMITY_PCT = 3.0
PIN_CLUSTER_PENALTY = 10

DERIBIT_BASE_URL = "https://www.deribit.com/api/v2"
BINANCE_FAPI_BASE = "https://fapi.binance.com"
BINANCE_SPOT_BASE = "https://api.binance.com"
COINGECKO_GLOBAL_URL = "https://api.coingecko.com/api/v3/global"

_DERIBIT_INSTRUMENT_RE = re.compile(r"^[A-Z]+-(\d{1,2}[A-Z]{3}\d{2})-(\d+(?:\.\d+)?)-([CP])$")


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_tracked_alts() -> list[str]:
    """Live/dynamic altcoin watchlist, hot-reloaded from system_config.tracked_alts on every
    call (mutable via the /config CLI's 'alts + SUI' / 'alts - ADA' commands). Falls back to
    the ALT_SYMBOLS default if the config value is missing or unparsable."""
    raw = database.get_config("tracked_alts", json.dumps(ALT_SYMBOLS))
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list) and parsed:
            return [str(s).upper() for s in parsed]
    except (json.JSONDecodeError, TypeError):
        pass
    return list(ALT_SYMBOLS)


def get_supported_pairs() -> set[str]:
    """Dynamic counterpart to SUPPORTED_PAIRS, reflecting the live tracked_alts watchlist."""
    return {f"{s}USDT" for s in MAJOR_SYMBOLS + get_tracked_alts()}


# ---------------------------------------------------------------------------
# Resilience: exponential backoff decorator for HTTP 429 / 5xx
# ---------------------------------------------------------------------------
def with_retry_backoff(max_retries: int = 4, base_delay: float = 1.0, max_delay: float = 30.0):
    """Decorator for functions that return a requests.Response. Retries with exponential
    backoff on HTTP 429 (honors Retry-After) and 5xx responses, and on connection/timeout
    errors. Any other non-2xx status raises immediately via raise_for_status()."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_retries + 1):
                try:
                    resp = func(*args, **kwargs)
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                    if attempt == max_retries:
                        logging.error(f"{func.__name__}: network failure after {max_retries} attempts: {exc}")
                        raise
                    logging.warning(f"{func.__name__}: network error ({exc}), attempt {attempt}/{max_retries}, retrying in {delay:.1f}s")
                    time.sleep(delay)
                    delay = min(delay * 2, max_delay)
                    continue
                if resp.status_code == 429 or resp.status_code >= 500:
                    if attempt == max_retries:
                        resp.raise_for_status()
                        return resp
                    retry_after_header = resp.headers.get("Retry-After")
                    wait_s = float(retry_after_header) if retry_after_header else delay
                    logging.warning(f"{func.__name__}: HTTP {resp.status_code}, attempt {attempt}/{max_retries}, retrying in {wait_s:.1f}s")
                    time.sleep(min(wait_s, max_delay))
                    delay = min(delay * 2, max_delay)
                    continue
                resp.raise_for_status()
                return resp
            raise RuntimeError(f"{func.__name__}: exhausted {max_retries} retries")
        return wrapper
    return decorator


@with_retry_backoff()
def _get(url: str, params: dict = None, timeout: int = 10) -> requests.Response:
    return requests.get(url, params=params, timeout=timeout)


# ---------------------------------------------------------------------------
# Deribit: Dealer Gamma Exposure (GEX) + pin clusters
# ---------------------------------------------------------------------------
def fetch_deribit_option_chain(currency: str) -> list[dict]:
    resp = _get(f"{DERIBIT_BASE_URL}/public/get_book_summary_by_currency",
                params={"currency": currency, "kind": "option"})
    payload = resp.json()
    if "error" in payload:
        raise RuntimeError(f"Deribit API error for {currency}: {payload['error']}")
    return payload.get("result", []) or []


def _parse_deribit_instrument(name: str) -> dict | None:
    match = _DERIBIT_INSTRUMENT_RE.match(name)
    if not match:
        return None
    expiry_raw, strike_raw, opt_type = match.groups()
    try:
        expiry_date = datetime.strptime(expiry_raw, "%d%b%y").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return {"expiry": expiry_date, "strike": float(strike_raw), "option_type": "call" if opt_type == "C" else "put"}


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


def _black_scholes_gamma(spot: float, strike: float, t_years: float, iv: float, r: float = 0.0) -> float:
    """Standard Black-Scholes gamma. Risk-free rate defaults to 0 (negligible impact on
    short-dated crypto options gamma)."""
    if spot <= 0 or strike <= 0 or t_years <= 0 or iv <= 0:
        return 0.0
    d1 = (math.log(spot / strike) + (r + 0.5 * iv * iv) * t_years) / (iv * math.sqrt(t_years))
    return _norm_pdf(d1) / (spot * iv * math.sqrt(t_years))


def calculate_gamma_exposure(currency: str) -> dict:
    """Estimates aggregate Dealer Gamma Exposure (GEX) and heavy strike pin clusters from
    Deribit's public option chain. Convention: dealers are assumed net long calls / net short
    puts (the standard retail GEX heuristic), so call gamma adds to total_gex and put gamma
    subtracts. This is a directional estimate, not an exact dealer-positioning figure - Deribit
    does not publish actual market-maker inventory.
    """
    currency = currency.upper()
    chain = fetch_deribit_option_chain(currency)
    if not chain:
        return {
            "currency": currency, "spot_price": None, "total_gex": 0.0,
            "gex_regime": "UNKNOWN", "pin_clusters": [], "as_of": _utcnow_iso(), "error": "no_data",
        }

    spot_price = None
    total_gex = 0.0
    oi_by_strike: dict[float, float] = {}
    now = datetime.now(timezone.utc)

    for entry in chain:
        parsed = _parse_deribit_instrument(entry.get("instrument_name", ""))
        if not parsed:
            continue
        underlying_price = entry.get("underlying_price") or entry.get("mark_price")
        open_interest = entry.get("open_interest") or 0.0
        mark_iv_pct = entry.get("mark_iv")
        if not underlying_price or not open_interest or not mark_iv_pct:
            continue
        spot_price = underlying_price
        t_years = (parsed["expiry"] - now).total_seconds() / (365.0 * 24 * 3600)
        if t_years <= 0:
            continue
        gamma = _black_scholes_gamma(underlying_price, parsed["strike"], t_years, mark_iv_pct / 100.0)
        exposure = gamma * open_interest * (underlying_price ** 2) * 0.01
        total_gex += exposure if parsed["option_type"] == "call" else -exposure
        oi_by_strike[parsed["strike"]] = oi_by_strike.get(parsed["strike"], 0.0) + open_interest

    pin_clusters = []
    if spot_price:
        ranked = sorted(oi_by_strike.items(), key=lambda kv: kv[1], reverse=True)
        for strike, oi in ranked[:5]:
            distance_pct = (strike - spot_price) / spot_price * 100.0
            pin_clusters.append({"strike": strike, "total_open_interest": oi, "distance_pct": round(distance_pct, 2)})

    return {
        "currency": currency,
        "spot_price": spot_price,
        "total_gex": total_gex,
        "gex_regime": "NEGATIVE" if total_gex < 0 else "POSITIVE",
        "pin_clusters": pin_clusters,
        "as_of": _utcnow_iso(),
    }


# ---------------------------------------------------------------------------
# Binance USD-M Futures: funding rate (dynamic interval) + open interest trend
# ---------------------------------------------------------------------------
def fetch_premium_index(symbol: str) -> dict:
    return _get(f"{BINANCE_FAPI_BASE}/fapi/v1/premiumIndex", params={"symbol": symbol}).json()


def fetch_funding_intervals() -> dict[str, int]:
    """Returns {symbol: fundingIntervalHours}. Symbols absent here use Binance's 8h default."""
    rows = _get(f"{BINANCE_FAPI_BASE}/fapi/v1/fundingInfo").json()
    return {row["symbol"]: int(row.get("fundingIntervalHours", 8)) for row in rows}


def get_normalized_funding_rate(symbol: str, interval_hours: int = 8) -> dict:
    """Normalizes the funding rate to a daily-equivalent percentage so 4h and 8h interval
    symbols are directly comparable."""
    premium = fetch_premium_index(symbol)
    last_funding_rate = float(premium.get("lastFundingRate", 0.0))
    periods_per_day = 24.0 / interval_hours
    daily_equivalent_pct = last_funding_rate * 100.0 * periods_per_day
    return {
        "symbol": symbol,
        "funding_rate": last_funding_rate,
        "funding_interval_hours": interval_hours,
        "daily_equivalent_pct": daily_equivalent_pct,
        "mark_price": float(premium.get("markPrice", 0.0)),
        "next_funding_time": premium.get("nextFundingTime"),
    }


def fetch_open_interest_trend(symbol: str, period: str = "4h", lookback: int = 6) -> dict:
    rows = _get(f"{BINANCE_FAPI_BASE}/futures/data/openInterestHist",
                params={"symbol": symbol, "period": period, "limit": lookback}).json()
    if not rows:
        return {"symbol": symbol, "rising": False, "change_pct": 0.0, "latest_oi": 0.0, "samples": 0}
    latest_oi = float(rows[-1]["sumOpenInterest"])
    if len(rows) < 2:
        return {"symbol": symbol, "rising": False, "change_pct": 0.0, "latest_oi": latest_oi, "samples": len(rows)}
    oldest_oi = float(rows[0]["sumOpenInterest"])
    change_pct = ((latest_oi - oldest_oi) / oldest_oi * 100.0) if oldest_oi else 0.0
    return {"symbol": symbol, "rising": change_pct > 0, "change_pct": change_pct, "latest_oi": latest_oi, "samples": len(rows)}


# ---------------------------------------------------------------------------
# Binance Spot: OHLCV + current price
# ---------------------------------------------------------------------------
def fetch_klines(symbol: str, interval: str = "4h", limit: int = 100) -> list[dict]:
    raw = _get(f"{BINANCE_SPOT_BASE}/api/v3/klines",
               params={"symbol": symbol, "interval": interval, "limit": limit}).json()
    return [
        {
            "open_time": row[0], "open": float(row[1]), "high": float(row[2]),
            "low": float(row[3]), "close": float(row[4]), "volume": float(row[5]),
            "close_time": row[6],
        }
        for row in raw
    ]


def fetch_current_price(symbol: str) -> float:
    return float(_get(f"{BINANCE_SPOT_BASE}/api/v3/ticker/price", params={"symbol": symbol}).json()["price"])


# ---------------------------------------------------------------------------
# BTC Dominance (CoinGecko /global, free & keyless) - environment volatility halt
# ---------------------------------------------------------------------------
def fetch_btc_dominance() -> float:
    data = _get(COINGECKO_GLOBAL_URL).json()
    return float(data["data"]["market_cap_percentage"]["btc"])


def is_btc_dominance_volatile(threshold_pct: float = BTC_DOMINANCE_VOL_THRESHOLD_PCT, lookback_hours: int = 24) -> dict:
    """Compares the high-low range of BTC.D snapshots (persisted by scan_all_symbols on every
    15-min cycle) over the lookback window against threshold_pct. Fails open (not volatile)
    with 'insufficient_history' when fewer than 4 samples exist yet (e.g. right after startup),
    so a cold start never permanently locks out altcoin triggers.
    """
    since_iso = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
    snapshots = database.get_recent_ticks("BTC.D", "dominance_snapshot", since_iso=since_iso, limit=500)
    values = [s["close"] for s in snapshots if s.get("close") is not None]
    if len(values) < 4:
        return {"volatile": False, "reason": "insufficient_history", "samples": len(values)}
    lo, hi = min(values), max(values)
    range_pct = ((hi - lo) / lo * 100.0) if lo else 0.0
    return {"volatile": range_pct >= threshold_pct, "range_pct": range_pct, "samples": len(values), "low": lo, "high": hi}


# ---------------------------------------------------------------------------
# Indicators: Donchian(20), ATR(14), RVOL(20)
# ---------------------------------------------------------------------------
def calculate_donchian_channels(candles: list[dict], period: int = DONCHIAN_PERIOD) -> dict:
    if len(candles) < period + 1:
        raise ValueError(f"Need at least {period + 1} candles for Donchian({period}); got {len(candles)}")
    closed_candles = candles[-(period + 1):-1]  # exclude the still-forming current candle
    return {
        "donchian_high": max(c["high"] for c in closed_candles),
        "donchian_low": min(c["low"] for c in closed_candles),
        "period": period,
    }


def calculate_atr(candles: list[dict], period: int = ATR_PERIOD) -> float:
    if len(candles) < period + 1:
        raise ValueError(f"Need at least {period + 1} candles for ATR({period}); got {len(candles)}")
    true_ranges = []
    for i in range(1, len(candles)):
        high, low, prev_close = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        true_ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    atr = sum(true_ranges[:period]) / period  # seed with a simple average
    for tr in true_ranges[period:]:
        atr = (atr * (period - 1) + tr) / period  # Wilder's smoothing
    return atr


def calculate_rvol(candles: list[dict], period: int = RVOL_PERIOD) -> dict:
    if len(candles) < period + 1:
        raise ValueError(f"Need at least {period + 1} candles for RVOL({period}); got {len(candles)}")
    current_volume = candles[-1]["volume"]
    prior_volumes = [c["volume"] for c in candles[-(period + 1):-1]]
    avg_volume = sum(prior_volumes) / period
    return {
        "current_volume": current_volume, "avg_volume_20": avg_volume,
        "rvol_ratio": (current_volume / avg_volume) if avg_volume > 0 else 0.0,
    }


def calculate_risk_levels(entry_price: float, atr_14: float) -> tuple[float, float]:
    """sl_atr_mult / tp_rr_ratio are read fresh from system_config on every call (hot-reload),
    so a /config mutation takes effect on the very next signal without a restart."""
    sl_mult = float(database.get_config("sl_atr_mult", str(SL_ATR_MULTIPLIER)))
    rr_ratio = float(database.get_config("tp_rr_ratio", str(RISK_REWARD_RATIO)))
    stop_loss = entry_price - (sl_mult * atr_14)
    take_profit = entry_price + (rr_ratio * (entry_price - stop_loss))
    return stop_loss, take_profit


def calculate_tp1_price(entry_price: float, stop_loss: float) -> float:
    """TP1 (2-stage scale-out trigger) sits exactly 1R above entry, where R is THIS trade's own
    actual stop distance (entry - stop_loss) rather than a hardcoded ATR multiple - this keeps
    the scale-out mathematically correct even if sl_atr_mult is changed between trades."""
    return entry_price + (entry_price - stop_loss)


# ---------------------------------------------------------------------------
# The Logic Engine: Hybrid Sniper Score
# ---------------------------------------------------------------------------
def evaluate_breakout_signal(symbol: str, asset_class: str, candles: list[dict], context: dict) -> dict:
    """
    asset_class: 'major' (BTC/ETH, context requires 'gamma_data') or
                 'alt' (context requires 'funding_data', 'oi_trend', 'btc_dominance_halted').
    Returns a fully-populated signal dict; 'triggered' is False whenever the Donchian breakout
    or the 200% RVOL gate fails, or (alts only) the BTC.D volatility halt is active.
    """
    donchian = calculate_donchian_channels(candles)
    atr_14 = calculate_atr(candles)
    rvol = calculate_rvol(candles)
    current_price = candles[-1]["close"]

    reasons = []
    context_score = 0
    halted = False

    if asset_class == "major":
        gamma_data = context["gamma_data"]
        if gamma_data.get("gex_regime") == "NEGATIVE":
            context_score = GAMMA_NEGATIVE_CONTEXT_SCORE
            reasons.append(f"Negative Gamma Regime (GEX={gamma_data.get('total_gex', 0):.0f}) - dealers short gamma, volatility-amplifying")
        else:
            context_score = GAMMA_POSITIVE_CONTEXT_SCORE
            reasons.append(f"Positive/Unknown Gamma Regime (GEX={gamma_data.get('total_gex', 0):.0f}) - dealers long gamma, pinning risk")
        for cluster in gamma_data.get("pin_clusters", []):
            if abs(cluster["distance_pct"]) <= PIN_CLUSTER_PROXIMITY_PCT:
                context_score = max(0, context_score - PIN_CLUSTER_PENALTY)
                reasons.append(f"Near heavy OI pin cluster at strike {cluster['strike']:.0f} ({cluster['distance_pct']:+.1f}% away)")
                break
    elif asset_class == "alt":
        if context.get("btc_dominance_halted"):
            halted = True
            reasons.append("HALTED: BTC.D exhibiting extreme daily volatility")
        funding_data = context["funding_data"]
        oi_trend = context["oi_trend"]
        oi_rising = oi_trend.get("rising", False)
        funding_deeply_negative = funding_data.get("daily_equivalent_pct", 0.0) < FUNDING_DEEPLY_NEGATIVE_DAILY_PCT
        if oi_rising and funding_deeply_negative:
            context_score = OI_FUNDING_FULL_CONTEXT_SCORE
            reasons.append(f"Short Squeeze Setup: OI {oi_trend.get('change_pct', 0.0):+.1f}%, Funding {funding_data.get('daily_equivalent_pct', 0.0):.3f}%/day")
        elif oi_rising or funding_deeply_negative:
            context_score = OI_FUNDING_PARTIAL_CONTEXT_SCORE
            reasons.append(f"Partial Squeeze Context: OI rising={oi_rising}, Funding deeply negative={funding_deeply_negative}")
        else:
            context_score = OI_FUNDING_MIN_CONTEXT_SCORE
            reasons.append("No squeeze context: OI flat/falling and funding not deeply negative")
    else:
        raise ValueError(f"Unknown asset_class '{asset_class}', expected 'major' or 'alt'")

    breakout_pct = (current_price - donchian["donchian_high"]) / donchian["donchian_high"] * 100.0
    donchian_breached = breakout_pct > 0
    breakout_score = min(30.0, breakout_pct * 15.0) if donchian_breached else 0.0
    reasons.append(
        f"Donchian({DONCHIAN_PERIOD}) {'breakout' if donchian_breached else 'no breakout'}: "
        f"price {breakout_pct:+.2f}% vs prior high {donchian['donchian_high']:.6g}"
    )

    rvol_ratio = rvol["rvol_ratio"]
    rvol_threshold = float(database.get_config("rvol_threshold", str(RVOL_TRIGGER_THRESHOLD)))
    volume_gate_passed = rvol_ratio >= rvol_threshold
    volume_score = min(30.0, (rvol_ratio / rvol_threshold) * 15.0) if volume_gate_passed else 0.0
    reasons.append(f"RVOL {rvol_ratio:.2f}x vs {rvol_threshold * 100:.0f}% gate ({'PASS' if volume_gate_passed else 'FAIL'})")

    confidence_score = int(round(min(100.0, context_score + breakout_score + volume_score)))
    triggered = donchian_breached and volume_gate_passed and not halted
    stop_loss, take_profit = calculate_risk_levels(current_price, atr_14)

    return {
        "symbol": symbol,
        "asset_class": asset_class,
        "triggered": triggered,
        "confidence_score": confidence_score,
        "current_price": current_price,
        "donchian_high": donchian["donchian_high"],
        "donchian_low": donchian["donchian_low"],
        "atr_14": atr_14,
        "rvol_ratio": rvol_ratio,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "reasons": reasons,
        "context_summary": reasons[0] if reasons else "",
        "halted": halted,
        "evaluated_at": _utcnow_iso(),
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def _persist_tick_snapshot(symbol: str, last_candle: dict, signal: dict, funding_data: dict = None, oi_trend: dict = None) -> None:
    try:
        database.insert_market_tick(
            symbol=symbol, timeframe="4h",
            open_=last_candle["open"], high=last_candle["high"], low=last_candle["low"],
            close=last_candle["close"], volume=last_candle["volume"],
            donchian_high=signal.get("donchian_high"), donchian_low=signal.get("donchian_low"),
            atr_14=signal.get("atr_14"), rvol_ratio=signal.get("rvol_ratio"),
            open_interest=(oi_trend or {}).get("latest_oi"),
            funding_rate_daily_equiv=(funding_data or {}).get("daily_equivalent_pct"),
        )
    except Exception as e:
        logging.error(f"Failed to persist market tick for {symbol}: {e}")


def _evaluate_major(currency: str) -> dict:
    pair = f"{currency}USDT"
    candles = fetch_klines(pair, interval="4h", limit=100)
    gamma_data = calculate_gamma_exposure(currency)
    signal = evaluate_breakout_signal(pair, "major", candles, {"gamma_data": gamma_data})
    _persist_tick_snapshot(pair, candles[-1], signal)
    return signal


def _evaluate_alt(base_asset: str, interval_hours: int, btc_dominance_halted: bool) -> dict:
    pair = f"{base_asset}USDT"
    candles = fetch_klines(pair, interval="4h", limit=100)
    funding_data = get_normalized_funding_rate(pair, interval_hours)
    oi_trend = fetch_open_interest_trend(pair)
    signal = evaluate_breakout_signal(pair, "alt", candles, {
        "funding_data": funding_data, "oi_trend": oi_trend, "btc_dominance_halted": btc_dominance_halted,
    })
    _persist_tick_snapshot(pair, candles[-1], signal, funding_data=funding_data, oi_trend=oi_trend)
    return signal


def reconcile_trade_tick(trade: dict, price: float) -> dict | None:
    """Given one OPEN paper trade and a fresh price tick, decides + executes the correct action
    (breakeven scale-out, full TP close, or SL close) via database.py's atomic ledger functions.
    Returns an event dict describing what happened (for alert dispatch), or None if no action
    was warranted at this price. Shared by both the REST reconciliation loop (main_2.py) and the
    real-time WebSocket feed (ws_reconciler.py) so the exact-execution rules never diverge
    between the two paths.
    """
    fee_pct = float(database.get_config("fee_slippage_pct", "0.13"))

    if not trade["tp1_hit"]:
        if price <= trade["stop_loss"]:
            closed = database.close_paper_trade(trade["id"], price, "CLOSED_SL", fee_pct)
            return {"type": "CLOSED_SL", "trade": closed} if closed else None

        tp1_price = calculate_tp1_price(trade["entry_price"], trade["stop_loss"])
        if price >= tp1_price:
            close_qty = trade["initial_quantity"] * 0.5
            updated = database.partial_close_tp1(trade["id"], close_qty, price, fee_pct)
            if not updated:
                return None
            updated["tp1_exit_price"] = price
            # Gap risk: a single tick/candle may have already blown through the final TP2 too.
            if price >= updated["take_profit"]:
                closed = database.close_paper_trade(updated["id"], price, "CLOSED_TP", fee_pct)
                if closed:
                    return {"type": "TP1_AND_TP2", "tp1_trade": updated, "trade": closed}
            return {"type": "TP1", "trade": updated}
        return None

    # tp1 already hit - stop_loss now sits at breakeven (entry_price)
    if price <= trade["stop_loss"]:
        closed = database.close_paper_trade(trade["id"], price, "CLOSED_BE", fee_pct)
        return {"type": "CLOSED_BE", "trade": closed} if closed else None
    if price >= trade["take_profit"]:
        closed = database.close_paper_trade(trade["id"], price, "CLOSED_TP", fee_pct)
        return {"type": "CLOSED_TP", "trade": closed} if closed else None
    return None


def scan_all_symbols() -> list[dict]:
    """Runs one full scan cycle across BTC/ETH (gamma context) and the 7 tracked altcoins
    (OI/funding context), gated by a single BTC.D volatility check computed once per cycle.
    Never raises - a failure on any individual symbol is caught and represented as an error
    entry so one bad API call can't take down the whole cycle."""
    try:
        dominance_pct = fetch_btc_dominance()
        database.insert_market_tick(symbol="BTC.D", timeframe="dominance_snapshot", close=dominance_pct)
    except Exception as e:
        logging.error(f"BTC.D fetch failed: {e}")

    dominance_check = is_btc_dominance_volatile()
    if dominance_check.get("volatile"):
        logging.warning(f"BTC.D volatility halt ACTIVE: {dominance_check}")

    try:
        funding_intervals = fetch_funding_intervals()
    except Exception as e:
        logging.error(f"fetch_funding_intervals failed, defaulting all alts to 8h: {e}")
        funding_intervals = {}

    tracked_alts = get_tracked_alts()
    results = []
    with ThreadPoolExecutor(max_workers=len(MAJOR_SYMBOLS) + len(tracked_alts)) as executor:
        futures = {}
        for currency in MAJOR_SYMBOLS:
            futures[executor.submit(_evaluate_major, currency)] = f"{currency}USDT"
        for base_asset in tracked_alts:
            interval_hours = funding_intervals.get(f"{base_asset}USDT", 8)
            futures[executor.submit(_evaluate_alt, base_asset, interval_hours, dominance_check.get("volatile", False))] = f"{base_asset}USDT"

        for future in as_completed(futures):
            pair = futures[future]
            try:
                results.append(future.result(timeout=20))
            except Exception as e:
                logging.error(f"Evaluation failed for {pair}: {e}")
                results.append({"symbol": pair, "triggered": False, "error": str(e), "confidence_score": 0})
    return results
