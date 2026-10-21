#!/usr/bin/env python3
"""Historical pump study (read-only; never changes live scanner thresholds).

A positive label means the highest HIGH in the *next* 24 hours is >= 15%
above the current closed candle's close. Features use only candles at or before
the current candle. Binance's public API cannot enumerate every delisted symbol;
the universe therefore combines currently tradable USDT spot pairs with symbols
found in local scanner logs. See README for this limitation.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests

BASES = ["https://data-api.binance.vision", "https://api.binance.com"]
INTERVAL_MS = 5 * 60 * 1000
BARS_PER_DAY = 288
DEFAULT_DAYS = 30
PUMP_PCT = 15.0
STABLES = {
    "USDT", "USDC", "FDUSD", "TUSD", "USDP", "DAI", "USDE", "USD1",
    "RLUSD", "USDD", "EUR", "EURI", "USTC", "BUSD", "AEUR", "XUSD",
}
LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR", "2L", "2S", "3L", "3S", "5L", "5S")
ROOT = Path(__file__).resolve().parent
OUT = ROOT / "historical_analysis"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "v3-historical-pump-analyzer/1.0"})


def is_eligible_symbol(symbol: str) -> bool:
    """Basic spot USDT filter; avoids stablecoin bases and common leveraged tokens."""
    if not symbol.endswith("USDT") or len(symbol) <= 4:
        return False
    base = symbol[:-4]
    if base in STABLES:
        return False
    return not any(base.endswith(suffix) for suffix in LEVERAGED_SUFFIXES)


def forward_pump_labels(highs: list[float], closes: list[float], horizon: int = BARS_PER_DAY,
                        threshold_pct: float = PUMP_PCT) -> list[bool | None]:
    """Label each bar from future highs only; None means the full horizon is unavailable."""
    labels: list[bool | None] = []
    n = len(closes)
    for i, close in enumerate(closes):
        end = i + horizon
        if close <= 0 or end >= n:
            labels.append(None)
            continue
        future_high = max(highs[i + 1:end + 1])
        labels.append(future_high >= close * (1 + threshold_pct / 100))
    return labels


def features_at(bars: list[dict], i: int) -> dict[str, float | str]:
    """All feature windows end at i; no future candle is read."""
    b = bars[i]
    close = b["close"]
    def pct_back(k: int) -> float:
        if i < k or bars[i-k]["close"] <= 0:
            return float("nan")
        return (close / bars[i-k]["close"] - 1) * 100
    v15 = sum(x["quote_volume"] for x in bars[max(0, i-2):i+1])
    v1h = sum(x["quote_volume"] for x in bars[max(0, i-11):i+1])
    hist15 = [x["quote_volume"] for x in bars[max(0, i-146):max(0, i-2)]]
    hist1h = [x["quote_volume"] for x in bars[max(0, i-155):max(0, i-11)]]
    mean15 = sum(hist15) / len(hist15) if hist15 else 0
    mean1h = sum(hist1h) / len(hist1h) if hist1h else 0
    green = sum(1 for x in bars[max(1, i-4):i+1] if x["close"] > x["open"])
    prev_highs = [x["high"] for x in bars[max(0, i-48):i]]
    buy_pressure = b["taker_buy_quote"] / b["quote_volume"] if b["quote_volume"] > 0 else float("nan")
    span = b["high"] - b["low"]
    upper_wick = (b["high"] - max(b["open"], close)) / span if span > 0 else 0.0
    return {
        "timestamp": b["timestamp"],
        "close": close,
        "gain_5m_pct": pct_back(1),
        "gain_15m_pct": pct_back(3),
        "gain_1h_pct": pct_back(12),
        "gain_4h_pct": pct_back(48),
        "gain_24h_pct": pct_back(288),
        "volume_ratio_15m": v15 / (3 * mean15) if mean15 > 0 else float("nan"),
        "volume_ratio_1h": v1h / (12 * mean1h) if mean1h > 0 else float("nan"),
        "buy_pressure": buy_pressure,
        "green_candles_last5": green,
        "breakout_vs_prior_4h_pct": (close / max(prev_highs) - 1) * 100 if prev_highs else float("nan"),
        "upper_wick_ratio": upper_wick,
    }


def _get_json(path: str, params: dict | None = None):
    last_error = None
    for base in BASES:
        for attempt in range(3):
            try:
                response = SESSION.get(base + path, params=params, timeout=20)
                if response.status_code in (418, 429):
                    time.sleep(2 ** attempt)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
                time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"Binance request failed: {last_error}")


def _symbols_from_csv(path: Path) -> set[str]:
    found: set[str] = set()
    if not path.exists():
        return found
    try:
        with path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                for key, value in row.items():
                    if key and value and key.lower() in {"symbol", "ticker", "pair"}:
                        symbol = value.strip().upper()
                        if is_eligible_symbol(symbol):
                            found.add(symbol)
    except (OSError, csv.Error):
        pass
    return found


def discover_universe() -> list[str]:
    """Current eligible spot symbols + symbols recoverable from retained local logs."""
    info = _get_json("/api/v3/exchangeInfo")
    symbols = {
        row["symbol"] for row in info.get("symbols", [])
        if row.get("status") == "TRADING"
        and row.get("isSpotTradingAllowed", True)
        and row.get("quoteAsset") == "USDT"
        and row.get("quoteOrderQtyMarketAllowed", True) is not False
        and is_eligible_symbol(row.get("symbol", ""))
    }
    for name in ("early_pump_signal_results.csv", "early_scan_diagnostics.csv", "early_watch_log.csv"):
        symbols |= _symbols_from_csv(ROOT / name)
    return sorted(symbols)


def fetch_bars(symbol: str, start_ms: int, end_ms: int) -> list[dict]:
    rows = []
    cursor = start_ms
    while cursor < end_ms:
        data = _get_json("/api/v3/klines", {
            "symbol": symbol, "interval": "5m", "startTime": cursor,
            "endTime": end_ms, "limit": 1000,
        })
        if not data:
            break
        for r in data:
            if int(r[6]) < end_ms and int(r[6]) < int(time.time() * 1000):
                rows.append({
                    "timestamp": datetime.fromtimestamp(int(r[0]) / 1000, tz=timezone.utc).isoformat(),
                    "open_time": int(r[0]), "open": float(r[1]), "high": float(r[2]),
                    "low": float(r[3]), "close": float(r[4]), "quote_volume": float(r[7]),
                    "taker_buy_quote": float(r[10]),
                })
        next_cursor = int(data[-1][0]) + INTERVAL_MS
        if next_cursor <= cursor or len(data) < 1000:
            break
        cursor = next_cursor
        time.sleep(0.05)
    # Deduplicate and guarantee chronological order.
    return list({x["open_time"]: x for x in rows}.values()) if not rows else sorted(
        {x["open_time"]: x for x in rows}.values(), key=lambda x: x["open_time"])


def load_scanner_signal_times() -> dict[str, list[int]]:
    """Read known signal times when a log provides a recognizable symbol + timestamp."""
    result: dict[str, list[int]] = {}
    paths = ("early_pump_signal_results.csv", "early_scan_diagnostics.csv", "early_watch_log.csv")
    for filename in paths:
        path = ROOT / filename
        if not path.exists():
            continue
        try:
            with path.open(newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    lower = {str(k).lower(): v for k, v in row.items() if k}
                    symbol = next((str(lower[k]).strip().upper() for k in ("symbol", "ticker", "pair")
                                   if lower.get(k)), "")
                    raw_time = next((lower[k] for k in ("timestamp", "signal_time", "created_at", "time")
                                     if lower.get(k)), "")
                    if not is_eligible_symbol(symbol) or not raw_time:
                        continue
                    try:
                        stamp = int(float(raw_time))
                        if stamp > 10**12:
                            stamp //= 1000
                    except (ValueError, TypeError):
                        try:
                            stamp = int(datetime.fromisoformat(str(raw_time).replace("Z", "+00:00")).timestamp())
                        except (ValueError, TypeError):
                            continue
                    result.setdefault(symbol, []).append(stamp)
        except (OSError, csv.Error):
            continue
    return result


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("no_rows\n", encoding="utf-8")
        return
    keys = list(dict.fromkeys(k for row in rows for k in row))
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def summarize(features: list[dict]) -> list[dict]:
    keys = ("gain_5m_pct", "gain_15m_pct", "gain_1h_pct", "gain_4h_pct", "gain_24h_pct",
            "volume_ratio_15m", "volume_ratio_1h", "buy_pressure", "green_candles_last5",
            "breakout_vs_prior_4h_pct", "upper_wick_ratio")
    output = []
    for label, subset in (("pump", [r for r in features if r["pump_next_24h"] is True]),
                          ("no_pump", [r for r in features if r["pump_next_24h"] is False])):
        for key in keys:
            values = sorted(float(r[key]) for r in subset if _finite(r.get(key)))
            if not values:
                continue
            output.append({
                "group": label, "feature": key, "sample_count": len(values),
                "mean": round(sum(values) / len(values), 6),
                "median": round(values[len(values)//2], 6),
                "p25": round(values[int((len(values)-1)*0.25)], 6),
                "p75": round(values[int((len(values)-1)*0.75)], 6),
            })
    return output


def _finite(value) -> bool:
    try:
        import math
        return math.isfinite(float(value))
    except (ValueError, TypeError):
        return False


def analyze_symbol(symbol: str, bars: list[dict], signal_times: dict[str, list[int]]) -> tuple[list[dict], list[dict]]:
    if len(bars) <= BARS_PER_DAY + 288:
        return [], []
    highs = [b["high"] for b in bars]
    closes = [b["close"] for b in bars]
    labels = forward_pump_labels(highs, closes)
    samples, missed = [], []
    known_signals = signal_times.get(symbol, [])
    for i, label in enumerate(labels):
        if label is None:
            continue
        feat = features_at(bars, i)
        stamp = bars[i]["open_time"] // 1000
        row = {"symbol": symbol, **feat, "pump_next_24h": label,
               "label_threshold_pct": PUMP_PCT, "label_horizon_hours": 24}
        samples.append(row)
        if label:
            # Label the first candle crossing the threshold, not every candle afterward.
            previous = labels[i-1] if i > 0 else False
            if not previous:
                has_prior_signal = any(stamp - 24*3600 <= s <= stamp for s in known_signals)
                missed.append({
                    "symbol": symbol, "candidate_start_utc": bars[i]["timestamp"],
                    "reference_close": bars[i]["close"], "future_24h_max_high": max(highs[i+1:i+BARS_PER_DAY+1]),
                    "rise_from_reference_pct": round((max(highs[i+1:i+BARS_PER_DAY+1]) / bars[i]["close"] - 1)*100, 4),
                    "scanner_signal_in_prior_24h": has_prior_signal,
                    "gain_5m_pct": feat["gain_5m_pct"], "gain_15m_pct": feat["gain_15m_pct"],
                    "gain_1h_pct": feat["gain_1h_pct"], "gain_4h_pct": feat["gain_4h_pct"],
                    "gain_24h_pct": feat["gain_24h_pct"], "volume_ratio_15m": feat["volume_ratio_15m"],
                    "volume_ratio_1h": feat["volume_ratio_1h"], "buy_pressure": feat["buy_pressure"],
                    "green_candles_last5": feat["green_candles_last5"],
                })
    return samples, missed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--symbols", type=int, default=0, help="Optional cap for smoke tests; 0 means all discovered symbols")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--out-dir", default=str(OUT))
    parser.add_argument("--universe-file", help="Optional text file containing one symbol per line")
    args = parser.parse_args()
    if args.days < 2 or args.workers < 1 or args.symbols < 0:
        parser.error("--days must be >=2, --workers >=1 and --symbols >=0")
    try:
        universe = [x.strip().upper() for x in Path(args.universe_file).read_text(encoding="utf-8").splitlines()
                    if is_eligible_symbol(x.strip().upper())] if args.universe_file else discover_universe()
    except Exception as exc:
        raise SystemExit(f"Could not discover Binance universe: {exc}")
    universe = sorted(set(universe))
    if args.symbols:
        universe = universe[:args.symbols]
    if not universe:
        raise SystemExit("No eligible USDT spot symbols found.")
    end_ms = int(time.time() * 1000)
    end_ms -= end_ms % INTERVAL_MS
    start_ms = end_ms - (args.days + 1) * 86_400_000  # extra day for prior-only 24h features
    signals = load_scanner_signal_times()
    all_samples, all_missed, errors = [], [], []
    print(f"Universe: {len(universe)} symbols | history: {args.days} days | label: +{PUMP_PCT}% within next 24h")
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_bars, sym, start_ms, end_ms): sym for sym in universe}
        for number, future in enumerate(as_completed(futures), 1):
            symbol = futures[future]
            try:
                bars = future.result()
                samples, missed = analyze_symbol(symbol, bars, signals)
                all_samples.extend(samples)
                all_missed.extend(missed)
            except Exception as exc:
                errors.append({"symbol": symbol, "error": str(exc)})
            if number % 25 == 0 or number == len(universe):
                print(f"Processed {number}/{len(universe)} | samples={len(all_samples)} | errors={len(errors)}")
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    write_csv(out / "pump_feature_analysis.csv", summarize(all_samples))
    write_csv(out / "historical_pumps.csv", sorted(
        (r for r in all_missed), key=lambda r: (not r["scanner_signal_in_prior_24h"], -float(r["rise_from_reference_pct"]))))
    write_csv(out / "missed_pumps.csv", [r for r in all_missed if not r["scanner_signal_in_prior_24h"]])
    write_csv(out / "historical_labeled_samples.csv", all_samples)
    write_csv(out / "analysis_errors.csv", errors)
    metadata = {
        "created_utc": datetime.now(timezone.utc).isoformat(), "requested_days": args.days,
        "pump_threshold_pct": PUMP_PCT, "forward_horizon_hours": 24,
        "symbols_attempted": len(universe), "labeled_samples": len(all_samples),
        "pump_onsets": len(all_missed), "missed_pump_onsets": sum(not x["scanner_signal_in_prior_24h"] for x in all_missed),
        "symbols_with_errors": len(errors),
        "universe_limitation": "Binance public exchangeInfo lists current symbols only; delisted historical pairs can only be included if found in retained local logs or supplied universe file.",
        "no_live_thresholds_modified": True,
    }
    (out / "analysis_summary.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(json.dumps(metadata, indent=2))
    print(f"Reports saved to: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
