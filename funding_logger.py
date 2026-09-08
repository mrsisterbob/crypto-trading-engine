"""Read-only cross-venue perpetual funding-rate logger.

WHAT THIS IS: a measurement instrument. It records what each venue's perp funding rate was,
every 8 hours, so that after 30+ days there is real observed data to answer one question:
"is the funding spread between venues, at retail size, after fees, actually harvestable?"

WHAT THIS IS NOT: a trading system, a strategy, or a signal source. Nothing here places,
sizes, or recommends a trade. It touches no exchange with an API key, holds no credentials,
and reads only public unauthenticated endpoints. Do not let it grow into a trading system
without that being a deliberate, separate decision - see the README section it ships with.

NORMALIZATION IS THE WHOLE GAME. Venues quote funding over different intervals: Hyperliquid
and dYdX v4 settle HOURLY, while OKX / Deribit / KuCoin / Gate.io settle every 8 HOURS.
Comparing raw quoted rates across venues is meaningless and off by 8x. Every observation is
therefore stored with BOTH the raw rate and its funding_interval_hours, normalized to a
daily-equivalent using the same formula as analytics_engine.get_normalized_funding_rate():

    periods_per_day = 24 / interval_hours
    daily_equivalent_pct = raw_rate * 100 * periods_per_day

The interval is derived from the venue's own payload wherever the venue publishes it (OKX
exposes fundingTime/prevFundingTime, KuCoin exposes granularity, Gate.io exposes
funding_interval) and hardcoded only for the two venues that do not publish it.

raw_json is stored verbatim for every observation so that a normalization bug discovered in
week 4 can be corrected retroactively across the whole sample instead of invalidating it.

CLI:
    python funding_logger.py collect          # one collection cycle, writes to crypto_engine.db
    python funding_logger.py report           # spread distribution over whatever has accumulated
    python funding_logger.py report --symbol BTC --thresholds 0.05,0.10,0.20
"""
import argparse
import json
import logging
import math
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import database
from analytics_engine import with_retry_backoff

# Canonical assets. Perps only, majors only - this is a measurement sample, not a screener.
ASSETS = ("BTC", "ETH")

# Funding intervals that the venue does NOT publish in its response payload, so they must be
# asserted here from venue documentation. Both of these settle HOURLY, which is the single
# most consequential fact in this module: getting either wrong scales that venue by 8x.
HYPERLIQUID_INTERVAL_HOURS = 1.0
DYDX_INTERVAL_HOURS = 1.0

# Deribit accrues funding continuously rather than at discrete epochs, but publishes the
# 8-hour-denominated figure as `funding_8h`. That is the directly comparable number, so 8h is
# the correct normalization denominator for it.
DERIBIT_INTERVAL_HOURS = 8.0

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS funding_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    funding_rate_raw REAL NOT NULL,
    funding_interval_hours REAL NOT NULL,
    daily_equivalent_pct REAL NOT NULL,
    annualized_pct REAL NOT NULL,
    mark_price REAL,
    raw_json TEXT NOT NULL,
    UNIQUE (ts_utc, venue, symbol)
);
CREATE INDEX IF NOT EXISTS idx_funding_obs_lookup
    ON funding_observations(symbol, ts_utc, venue);
"""


def ensure_schema() -> None:
    """Creates only the funding_observations table. Deliberately does not call
    database.init_db() and does not touch any pre-existing table."""
    with database.get_db_conn() as conn:
        conn.executescript(_SCHEMA_SQL)


# ---------------------------------------------------------------------------
# HTTP (reuses analytics_engine's 429/5xx exponential-backoff decorator)
# ---------------------------------------------------------------------------
@with_retry_backoff(max_retries=3, base_delay=1.0)
def _get(url: str, params: dict = None, timeout: int = 15) -> requests.Response:
    return requests.get(url, params=params, timeout=timeout)


@with_retry_backoff(max_retries=3, base_delay=1.0)
def _post(url: str, payload: dict, timeout: int = 15) -> requests.Response:
    return requests.post(url, json=payload, timeout=timeout)


def normalize_daily_equivalent_pct(funding_rate_raw: float, interval_hours: float) -> float:
    """Same normalization as analytics_engine.get_normalized_funding_rate(): a venue's raw rate
    is quoted per its own funding interval, so scale by the number of intervals in a day."""
    if interval_hours <= 0:
        raise ValueError(f"interval_hours must be positive, got {interval_hours}")
    periods_per_day = 24.0 / interval_hours
    return funding_rate_raw * 100.0 * periods_per_day


def _observation(venue, symbol, raw_rate, interval_hours, mark_price, endpoint, venue_symbol, payload) -> dict:
    daily = normalize_daily_equivalent_pct(raw_rate, interval_hours)
    return {
        "venue": venue,
        "symbol": symbol,
        "funding_rate_raw": float(raw_rate),
        "funding_interval_hours": float(interval_hours),
        "daily_equivalent_pct": daily,
        "annualized_pct": daily * 365.0,
        "mark_price": float(mark_price) if mark_price is not None else None,
        "raw_json": json.dumps(
            {"endpoint": endpoint, "venue_symbol": venue_symbol, "payload": payload},
            separators=(",", ":"), default=str,
        ),
    }


def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Venue adapters. Each returns a list of observation dicts (one per asset) and is allowed to
# raise - collect_once() isolates each venue so one outage never costs the others their sample.
# ---------------------------------------------------------------------------
def fetch_hyperliquid() -> list[dict]:
    """Hyperliquid settles funding HOURLY. The interval is not present in the payload."""
    url = "https://api.hyperliquid.xyz/info"
    body = _post(url, {"type": "metaAndAssetCtxs"}).json()
    meta, asset_ctxs = body[0], body[1]
    universe = meta["universe"]
    index_by_name = {u["name"]: i for i, u in enumerate(universe)}

    out = []
    for asset in ASSETS:
        idx = index_by_name.get(asset)
        if idx is None or idx >= len(asset_ctxs):
            raise KeyError(f"Hyperliquid universe has no context for {asset}")
        ctx = asset_ctxs[idx]
        out.append(_observation(
            "Hyperliquid", asset, float(ctx["funding"]), HYPERLIQUID_INTERVAL_HOURS,
            _safe_float(ctx.get("markPx")), url, asset, ctx,
        ))
    return out


def fetch_okx() -> list[dict]:
    """OKX publishes fundingTime and prevFundingTime, so the interval is derived, not assumed -
    OKX has moved some instruments off the 8h default and this survives that."""
    url = "https://www.okx.com/api/v5/public/funding-rate"
    instruments = {"BTC": "BTC-USDT-SWAP", "ETH": "ETH-USDT-SWAP"}
    out = []
    for asset, inst in instruments.items():
        data = _get(url, params={"instId": inst}).json()["data"][0]
        interval_hours = (int(data["fundingTime"]) - int(data["prevFundingTime"])) / 3_600_000.0
        if interval_hours <= 0:
            raise ValueError(f"OKX {inst}: nonsensical derived interval {interval_hours}h")
        mark_price = None
        try:  # best-effort context only; never lose the funding observation over it
            mark_price = _safe_float(
                _get("https://www.okx.com/api/v5/public/mark-price",
                     params={"instId": inst}).json()["data"][0]["markPx"]
            )
        except Exception as e:
            logging.debug(f"OKX mark-price lookup failed for {inst}: {e}")
        out.append(_observation(
            "OKX", asset, float(data["fundingRate"]), interval_hours, mark_price, url, inst, data,
        ))
    return out


def fetch_deribit() -> list[dict]:
    url = "https://www.deribit.com/api/v2/public/ticker"
    instruments = {"BTC": "BTC-PERPETUAL", "ETH": "ETH-PERPETUAL"}
    out = []
    for asset, inst in instruments.items():
        result = _get(url, params={"instrument_name": inst}).json()["result"]
        out.append(_observation(
            "Deribit", asset, float(result["funding_8h"]), DERIBIT_INTERVAL_HOURS,
            _safe_float(result.get("mark_price")), url, inst, result,
        ))
    return out


def fetch_kucoin_futures() -> list[dict]:
    """KuCoin publishes `granularity` in milliseconds, so the interval is read, not assumed."""
    instruments = {"BTC": "XBTUSDTM", "ETH": "ETHUSDTM"}
    out = []
    for asset, sym in instruments.items():
        url = f"https://api-futures.kucoin.com/api/v1/funding-rate/{sym}/current"
        data = _get(url).json()["data"]
        interval_hours = float(data["granularity"]) / 3_600_000.0
        if interval_hours <= 0:
            raise ValueError(f"KuCoin {sym}: nonsensical granularity {data['granularity']}")
        mark_price = None
        try:  # best-effort context only
            mark_price = _safe_float(
                _get(f"https://api-futures.kucoin.com/api/v1/mark-price/{sym}/current")
                .json()["data"]["value"]
            )
        except Exception as e:
            logging.debug(f"KuCoin mark-price lookup failed for {sym}: {e}")
        out.append(_observation(
            "KuCoinFutures", asset, float(data["value"]), interval_hours, mark_price, url, sym, data,
        ))
    return out


def fetch_gateio() -> list[dict]:
    """Gate.io publishes `funding_interval` in seconds, so the interval is read, not assumed."""
    instruments = {"BTC": "BTC_USDT", "ETH": "ETH_USDT"}
    out = []
    for asset, sym in instruments.items():
        url = f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{sym}"
        data = _get(url).json()
        interval_hours = float(data["funding_interval"]) / 3600.0
        if interval_hours <= 0:
            raise ValueError(f"Gate.io {sym}: nonsensical funding_interval {data['funding_interval']}")
        out.append(_observation(
            "Gate.io", asset, float(data["funding_rate"]), interval_hours,
            _safe_float(data.get("mark_price")), url, sym, data,
        ))
    return out


def fetch_dydx_v4() -> list[dict]:
    """dYdX v4 settles funding HOURLY (corroborated by the payload's own defaultFundingRate1H
    field). One request returns every market. NOTE: BTC-USD open interest here is thin relative
    to the CEX venues - read its quotes with that in mind, the logger records it either way."""
    url = "https://indexer.dydx.trade/v4/perpetualMarkets"
    markets = _get(url).json()["markets"]
    tickers = {"BTC": "BTC-USD", "ETH": "ETH-USD"}
    out = []
    for asset, ticker in tickers.items():
        market = markets[ticker]
        out.append(_observation(
            "dYdX-v4", asset, float(market["nextFundingRate"]), DYDX_INTERVAL_HOURS,
            _safe_float(market.get("oraclePrice")), url, ticker, market,
        ))
    return out


VENUE_ADAPTERS = {
    "Hyperliquid": fetch_hyperliquid,
    "OKX": fetch_okx,
    "Deribit": fetch_deribit,
    "KuCoinFutures": fetch_kucoin_futures,
    "Gate.io": fetch_gateio,
    "dYdX-v4": fetch_dydx_v4,
}

# Bybit is deliberately absent: from a US IP its public v5 market-data endpoints return
# HTTP 403 ("The Amazon CloudFront distribution is configured to block access from your
# country"). Binance is absent for the same class of reason - api.binance.com returns 451 and
# api.binance.us is spot-only with no futures/funding endpoint at all. No workaround is
# attempted for either, by design.


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------
def collect_once() -> dict:
    """Runs one collection cycle across every venue and persists the results.

    All rows from one cycle share a single ts_utc batch timestamp. That is what makes the
    pairwise spread analysis exact: venue A and venue B are compared at the same instant via
    an equality join, never by fuzzy time-bucketing.
    """
    ensure_schema()
    batch_ts = database.utcnow_iso()

    observations: list[dict] = []
    failures: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=len(VENUE_ADAPTERS)) as executor:
        futures = {executor.submit(fn): venue for venue, fn in VENUE_ADAPTERS.items()}
        for future in as_completed(futures):
            venue = futures[future]
            try:
                observations.extend(future.result(timeout=45))
            except Exception as e:
                failures[venue] = f"{type(e).__name__}: {e}"
                logging.error(f"funding_logger: {venue} collection failed: {e}")

    written = 0
    if observations:
        with database.get_db_conn(immediate=True) as conn:
            for obs in observations:
                cursor = conn.execute(
                    """INSERT OR IGNORE INTO funding_observations
                       (ts_utc, venue, symbol, funding_rate_raw, funding_interval_hours,
                        daily_equivalent_pct, annualized_pct, mark_price, raw_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (batch_ts, obs["venue"], obs["symbol"], obs["funding_rate_raw"],
                     obs["funding_interval_hours"], obs["daily_equivalent_pct"],
                     obs["annualized_pct"], obs["mark_price"], obs["raw_json"]),
                )
                written += cursor.rowcount

    logging.info(
        f"funding_logger: batch {batch_ts} - {written} observations written from "
        f"{len(VENUE_ADAPTERS) - len(failures)}/{len(VENUE_ADAPTERS)} venues"
        + (f"; failed: {', '.join(sorted(failures))}" if failures else "")
    )
    return {"ts_utc": batch_ts, "written": written, "observations": observations, "failures": failures}


def log_funding_job() -> None:
    """APScheduler entry point. Swallows every exception by design: this logger is strictly
    additive instrumentation and must never be able to disturb the scan or reconciliation
    jobs sharing the same scheduler."""
    try:
        collect_once()
    except Exception as e:
        logging.error(f"funding_logger: collection cycle failed: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# Reporting (read-only)
# ---------------------------------------------------------------------------
def _percentile(sorted_values: list[float], p: float) -> float | None:
    """Linear-interpolated percentile. p in [0, 1]."""
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * p
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return sorted_values[int(k)]
    return sorted_values[lo] * (hi - k) + sorted_values[hi] * (k - lo)


def load_observations(symbol: str = None) -> list[dict]:
    ensure_schema()
    with database.get_db_conn() as conn:
        if symbol:
            rows = conn.execute(
                """SELECT ts_utc, venue, symbol, daily_equivalent_pct
                   FROM funding_observations WHERE symbol = ? ORDER BY ts_utc ASC""",
                (symbol,),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT ts_utc, venue, symbol, daily_equivalent_pct
                   FROM funding_observations ORDER BY ts_utc ASC"""
            ).fetchall()
    return [dict(r) for r in rows]


def per_venue_stats(observations: list[dict]) -> dict:
    """Per (symbol, venue): mean / median / stdev of daily-equivalent funding, and the share of
    observations that were positive."""
    buckets: dict[tuple[str, str], list[float]] = {}
    for obs in observations:
        buckets.setdefault((obs["symbol"], obs["venue"]), []).append(obs["daily_equivalent_pct"])

    stats = {}
    for (symbol, venue), values in buckets.items():
        stats[(symbol, venue)] = {
            "n": len(values),
            "mean_pct": statistics.mean(values),
            "median_pct": statistics.median(values),
            "stdev_pct": statistics.pstdev(values) if len(values) >= 2 else 0.0,
            "pct_positive": sum(1 for v in values if v > 0) / len(values) * 100.0,
        }
    return stats


def pairwise_spreads(observations: list[dict], thresholds: list[float]) -> dict:
    """Per (symbol, venue-pair): the distribution of (venue A - venue B) daily-equivalent
    funding, paired by exact batch timestamp.

    Threshold exceedance uses the ABSOLUTE spread: the harvestable leg is whichever side is
    paying, so direction does not matter to whether a given round-trip cost is cleared.
    """
    by_ts: dict[tuple[str, str], dict[str, float]] = {}
    for obs in observations:
        by_ts.setdefault((obs["symbol"], obs["ts_utc"]), {})[obs["venue"]] = obs["daily_equivalent_pct"]

    spreads: dict[tuple[str, str, str], list[float]] = {}
    for (symbol, _ts), venue_values in by_ts.items():
        venues = sorted(venue_values)
        for i, venue_a in enumerate(venues):
            for venue_b in venues[i + 1:]:
                key = (symbol, venue_a, venue_b)
                spreads.setdefault(key, []).append(venue_values[venue_a] - venue_values[venue_b])

    report = {}
    for key, values in spreads.items():
        ordered = sorted(values)
        absolute = [abs(v) for v in values]
        report[key] = {
            "n": len(values),
            "median_pct": statistics.median(ordered),
            "p25_pct": _percentile(ordered, 0.25),
            "p75_pct": _percentile(ordered, 0.75),
            "median_abs_pct": statistics.median(sorted(absolute)),
            "exceedance": {
                t: sum(1 for a in absolute if a >= t) / len(absolute) * 100.0
                for t in thresholds
            },
        }
    return report


def print_report(symbol_filter: str = None, thresholds: list[float] = None) -> None:
    thresholds = thresholds or [0.05, 0.10, 0.20]
    observations = load_observations(symbol_filter)

    print("=" * 88)
    print("CROSS-VENUE PERPETUAL FUNDING - OBSERVED SPREAD DISTRIBUTION")
    print("=" * 88)
    if not observations:
        print("No observations recorded yet. Run `python funding_logger.py collect`, or start")
        print("main_2.py and let the 8-hourly job accumulate a sample.")
        print("=" * 88)
        return

    timestamps = sorted({o["ts_utc"] for o in observations})
    print(f"Observations: {len(observations)} rows across {len(timestamps)} collection batches")
    print(f"First batch:  {timestamps[0]}")
    print(f"Last batch:   {timestamps[-1]}")
    if len(timestamps) < 90:
        print(f"SAMPLE SIZE: {len(timestamps)} batches. 30 days at 8h cadence is ~90 batches. "
              "Treat everything below as preliminary.")
    print()

    print("-" * 88)
    print("PER VENUE - daily-equivalent funding (%/day)")
    print("-" * 88)
    print(f"{'Symbol':<8}{'Venue':<16}{'n':>5}{'mean':>11}{'median':>11}{'stdev':>11}{'% positive':>13}")
    stats = per_venue_stats(observations)
    for (symbol, venue) in sorted(stats):
        s = stats[(symbol, venue)]
        print(f"{symbol:<8}{venue:<16}{s['n']:>5}{s['mean_pct']:>11.5f}"
              f"{s['median_pct']:>11.5f}{s['stdev_pct']:>11.5f}{s['pct_positive']:>12.1f}%")
    print()

    print("-" * 88)
    print("PER VENUE PAIR - spread distribution, (A - B) in %/day, paired by exact batch")
    print("-" * 88)
    spreads = pairwise_spreads(observations, thresholds)
    if not spreads:
        print("No batch contained two or more venues - no pair is computable yet.")
        print("=" * 88)
        return

    header = f"{'Symbol':<8}{'Venue A':<16}{'Venue B':<16}{'n':>4}{'p25':>10}{'median':>10}{'p75':>10}"
    header += "".join(f"{'>=' + format(t, '.2f') + '%':>10}" for t in thresholds)
    print(header)
    for key in sorted(spreads):
        symbol, venue_a, venue_b = key
        r = spreads[key]
        line = (f"{symbol:<8}{venue_a:<16}{venue_b:<16}{r['n']:>4}"
                f"{r['p25_pct']:>10.5f}{r['median_pct']:>10.5f}{r['p75_pct']:>10.5f}")
        line += "".join(f"{r['exceedance'][t]:>9.1f}%" for t in thresholds)
        print(line)

    print()
    print("-" * 88)
    print("The >= columns are the share of observations where the ABSOLUTE spread cleared that")
    print("round-trip cost threshold, in %/day. That is the decision-relevant number.")
    print()
    print("This is a distribution of observed spreads. It is NOT a return forecast, and no")
    print("projected yield is computed anywhere in this tool - that inference is yours to make,")
    print("and it must account for what is NOT measured here: execution slippage at your size,")
    print("maker/taker fees, collateral split across two venues, margin and liquidation risk on")
    print("the short leg, transfer latency between venues, and the fact that a spread visible in")
    print("a snapshot is not necessarily a spread you could have filled.")
    print("=" * 88)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only cross-venue perpetual funding-rate logger and spread reporter."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("collect", help="Run one collection cycle and persist the observations.")

    report_parser = sub.add_parser("report", help="Report the observed spread distribution.")
    report_parser.add_argument("--symbol", choices=ASSETS, default=None,
                               help="Restrict the report to one asset (default: both).")
    report_parser.add_argument("--thresholds", default="0.05,0.10,0.20",
                               help="Comma-separated round-trip cost thresholds in %%/day "
                                    "(default: 0.05,0.10,0.20).")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if args.command == "collect":
        result = collect_once()
        print(f"Batch {result['ts_utc']}: {result['written']} observations written.")
        for obs in sorted(result["observations"], key=lambda o: (o["symbol"], o["venue"])):
            print(f"  {obs['symbol']:<4} {obs['venue']:<15} raw={obs['funding_rate_raw']:<+18.10g} "
                  f"interval={obs['funding_interval_hours']:g}h  "
                  f"daily={obs['daily_equivalent_pct']:+.5f}%")
        for venue, err in sorted(result["failures"].items()):
            print(f"  FAILED {venue}: {err}")
    elif args.command == "report":
        thresholds = sorted({float(t) for t in args.thresholds.split(",") if t.strip()})
        print_report(symbol_filter=args.symbol, thresholds=thresholds)


if __name__ == "__main__":
    main()
