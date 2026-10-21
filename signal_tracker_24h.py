#!/usr/bin/env python3
"""Independent 24-hour signal tracker. Does not import or modify the scanner."""
from __future__ import annotations

import argparse
import csv
import html
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests

SOURCE_DEFAULT = "early_pump_signal_results.csv"
DB_DEFAULT = "signal_tracker_24h.sqlite3"
OUTPUT_DEFAULT = "signal_tracker_output"
BASE_URLS = ("https://data-api.binance.vision", "https://api.binance.com")
MILESTONES = (5, 15, 60, 240, 1440)
KLINE_LIMIT = 1000


def parse_utc(value: str) -> int:
    """Parse an ISO timestamp to Unix milliseconds."""
    text = value.strip()
    if not text:
        raise ValueError("empty timestamp")
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def percent_change(entry: float, price: float) -> float:
    if entry <= 0:
        raise ValueError("entry price must be positive")
    return (price / entry - 1.0) * 100.0


def connect_db(path: str | Path) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY,
            signal_time_ms INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            setup TEXT NOT NULL DEFAULT '',
            score TEXT NOT NULL DEFAULT '',
            entry_price REAL NOT NULL CHECK(entry_price > 0),
            source_key TEXT NOT NULL UNIQUE,
            last_candle_open_ms INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'TRACKING',
            max_price REAL,
            min_price REAL
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            signal_id INTEGER NOT NULL REFERENCES signals(id),
            open_time_ms INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            PRIMARY KEY(signal_id, open_time_ms)
        )
    """)
    db.commit()
    return db


def ingest_signals(db: sqlite3.Connection, csv_path: str | Path) -> int:
    """Import finalized scanner signals from early_pump_signal_results.csv."""
    added = 0
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        required = {"final_time_utc", "symbol", "entry_price"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise ValueError(f"source CSV must contain columns: {', '.join(sorted(required))}")
        for row in reader:
            symbol = (row.get("symbol") or "").strip().upper()
            stamp = (row.get("final_time_utc") or "").strip()
            try:
                entry = float(row.get("entry_price") or "")
                signal_ms = parse_utc(stamp)
            except (ValueError, TypeError, OverflowError):
                continue
            if not symbol or not symbol.isascii() or not symbol.isalnum() or entry <= 0:
                continue
            source_key = f"{stamp}|{symbol}|{entry:.16g}"
            cur = db.execute("""
                INSERT OR IGNORE INTO signals
                (signal_time_ms, symbol, setup, score, entry_price, source_key)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (signal_ms, symbol, row.get("setup", ""), row.get("score", ""), entry, source_key))
            added += cur.rowcount
    db.commit()
    return added


def fetch_klines(symbol: str, start_ms: int, end_ms: int,
                 session: requests.Session | None = None) -> list[tuple[int, float, float, float, float]]:
    """Fetch 1m OHLC candles with pagination; timestamps are candle open times."""
    if end_ms <= start_ms:
        return []
    client = session or requests.Session()
    last_error = None
    for base in BASE_URLS:
        out = []
        cursor = start_ms
        try:
            while cursor < end_ms:
                response = client.get(
                    base + "/api/v3/klines",
                    params={"symbol": symbol, "interval": "1m", "startTime": cursor,
                            "endTime": end_ms, "limit": KLINE_LIMIT},
                    timeout=15,
                )
                response.raise_for_status()
                payload = response.json()
                if not payload:
                    break
                for k in payload:
                    open_ms = int(k[0])
                    if open_ms >= end_ms:
                        continue
                    out.append((open_ms, float(k[1]), float(k[2]), float(k[3]), float(k[4])))
                next_cursor = int(payload[-1][0]) + 60_000
                if next_cursor <= cursor:
                    break
                cursor = next_cursor
                if len(payload) < KLINE_LIMIT:
                    break
                time.sleep(0.08)
            return out
        except (requests.RequestException, ValueError, TypeError) as exc:
            last_error = exc
    raise RuntimeError(f"Could not fetch candles for {symbol}: {last_error}")


def store_candles(db: sqlite3.Connection, signal_id: int,
                  candles: Iterable[tuple[int, float, float, float, float]]) -> int:
    rows = [(signal_id, *c) for c in candles]
    db.executemany("""
        INSERT OR IGNORE INTO candles(signal_id, open_time_ms, open, high, low, close)
        VALUES (?, ?, ?, ?, ?, ?)
    """, rows)
    db.commit()
    return len(rows)


def refresh_signal(db: sqlite3.Connection, signal: sqlite3.Row, now_ms: int,
                   fetcher=fetch_klines) -> int:
    signal_id = int(signal["id"])
    signal_ms = int(signal["signal_time_ms"])
    # End at the current minute boundary so only completed 1m candles are counted.
    end_ms = (now_ms // 60_000) * 60_000
    if end_ms <= signal_ms:
        return 0
    latest = db.execute(
        "SELECT MAX(open_time_ms) FROM candles WHERE signal_id=?", (signal_id,)
    ).fetchone()[0]
    start_ms = max(signal_ms // 60_000 * 60_000, int(latest) + 60_000 if latest is not None else 0)
    if start_ms < signal_ms // 60_000 * 60_000:
        start_ms = signal_ms // 60_000 * 60_000
    if start_ms >= end_ms:
        candles = []
    else:
        candles = fetcher(signal["symbol"], start_ms, end_ms)
    stored = store_candles(db, signal_id, candles)
    extrema = db.execute(
        "SELECT MAX(high), MIN(low), MAX(open_time_ms) FROM candles WHERE signal_id=?",
        (signal_id,),
    ).fetchone()
    max_price, min_price, latest_ms = extrema
    status = "COMPLETED" if now_ms >= signal_ms + 24 * 60 * 60_000 else "TRACKING"
    db.execute("""
        UPDATE signals SET max_price=?, min_price=?, last_candle_open_ms=?, status=? WHERE id=?
    """, (max_price, min_price, latest_ms or 0, status, signal_id))
    db.commit()
    return stored


def build_results(db: sqlite3.Connection) -> list[dict]:
    results = []
    signals = db.execute("SELECT * FROM signals ORDER BY signal_time_ms, symbol").fetchall()
    for s in signals:
        candles = db.execute(
            "SELECT open_time_ms, high, low, close FROM candles WHERE signal_id=? ORDER BY open_time_ms",
            (s["id"],),
        ).fetchall()
        entry = float(s["entry_price"])
        row = {
            "signal_time_utc": datetime.fromtimestamp(s["signal_time_ms"] / 1000, timezone.utc).isoformat(),
            "symbol": s["symbol"], "setup": s["setup"], "score": s["score"],
            "entry_price": entry, "status": s["status"],
            "max_price_24h": s["max_price"], "max_gain_pct_24h": None,
            "min_price_24h": s["min_price"], "max_drawdown_pct_24h": None,
            "candles_recorded": len(candles),
        }
        if s["max_price"] is not None:
            row["max_gain_pct_24h"] = round(percent_change(entry, float(s["max_price"])), 6)
            row["max_drawdown_pct_24h"] = round(percent_change(entry, float(s["min_price"])), 6)
        for minute in MILESTONES:
            eligible = [c for c in candles if c["open_time_ms"] + 60_000 <= s["signal_time_ms"] + minute * 60_000]
            if eligible:
                close = float(eligible[-1]["close"])
                row[f"close_{minute}m"] = close
                row[f"return_{minute}m_pct"] = round(percent_change(entry, close), 6)
            else:
                row[f"close_{minute}m"] = ""
                row[f"return_{minute}m_pct"] = ""
        results.append(row)
    return results


def write_results(results: list[dict], out_dir: str | Path) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    if not results:
        return
    fields = list(results[0].keys())
    with (out / "signal_tracker_results.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)


def write_svg_chart(db: sqlite3.Connection, signal: sqlite3.Row, out_dir: str | Path) -> Path | None:
    candles = db.execute(
        "SELECT open_time_ms, close FROM candles WHERE signal_id=? ORDER BY open_time_ms",
        (signal["id"],),
    ).fetchall()
    if not candles:
        return None
    entry = float(signal["entry_price"])
    changes = [percent_change(entry, float(c["close"])) for c in candles]
    width, height, pad = 900, 420, 55
    min_y = min(changes + [0.0]) - 1.0
    max_y = max(changes + [0.0]) + 1.0
    if max_y - min_y < 0.01:
        max_y, min_y = 1.0, -1.0
    points = []
    for i, val in enumerate(changes):
        x = pad + i * (width - 2 * pad) / max(1, len(changes) - 1)
        y = pad + (max_y - val) * (height - 2 * pad) / (max_y - min_y)
        points.append(f"{x:.2f},{y:.2f}")
    symbol = html.escape(signal["symbol"])
    title = f"{symbol} — 24h price change after signal"
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="white"/>
<text x="{pad}" y="28" font-size="20" font-family="Arial" fill="#222">{title}</text>
<text x="{pad}" y="47" font-size="12" font-family="Arial" fill="#555">Change vs signal entry (%) · {len(changes)} completed 1m candles</text>
<line x1="{pad}" y1="{height-pad}" x2="{width-pad}" y2="{height-pad}" stroke="#888"/>
<line x1="{pad}" y1="{pad}" x2="{pad}" y2="{height-pad}" stroke="#888"/>
<line x1="{pad}" y1="{pad + (max_y) * (height-2*pad)/(max_y-min_y):.2f}" x2="{width-pad}" y2="{pad + (max_y) * (height-2*pad)/(max_y-min_y):.2f}" stroke="#aaa" stroke-dasharray="5,4"/>
<polyline fill="none" stroke="#1769aa" stroke-width="2" points="{' '.join(points)}"/>
<text x="8" y="{pad+5}" font-size="12" font-family="Arial" fill="#555">{max_y:.2f}%</text>
<text x="8" y="{height-pad}" font-size="12" font-family="Arial" fill="#555">{min_y:.2f}%</text>
<text x="{pad}" y="{height-15}" font-size="12" font-family="Arial" fill="#555">Signal</text>
<text x="{width-pad-45}" y="{height-15}" font-size="12" font-family="Arial" fill="#555">24h</text>
</svg>'''
    out = Path(out_dir) / "charts"
    out.mkdir(parents=True, exist_ok=True)
    safe_symbol = "".join(ch for ch in signal["symbol"] if ch.isalnum() or ch in "-_")
    filename = f"{signal['signal_time_ms']}_{safe_symbol}.svg"
    path = out / filename
    path.write_text(svg, encoding="utf-8")
    return path


def run(source: str, db_path: str, out_dir: str) -> int:
    db = connect_db(db_path)
    try:
        added = ingest_signals(db, source)
        now_ms = int(time.time() * 1000)
        errors = 0
        signals = db.execute("SELECT * FROM signals WHERE status='TRACKING' ORDER BY signal_time_ms").fetchall()
        for signal in signals:
            try:
                refresh_signal(db, signal, now_ms)
            except Exception as exc:
                errors += 1
                print(f"WARNING {signal['symbol']} {signal['source_key']}: {exc}")
        all_signals = db.execute("SELECT * FROM signals ORDER BY signal_time_ms").fetchall()
        for signal in all_signals:
            try:
                write_svg_chart(db, signal, out_dir)
            except Exception as exc:
                errors += 1
                print(f"WARNING chart {signal['symbol']}: {exc}")
        write_results(build_results(db), out_dir)
        print(f"Imported {added} new signals; refreshed {len(signals)} active signals; errors={errors}")
        print(f"Persistent database: {db_path}; output: {out_dir}")
        return 1 if errors else 0
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Track scanner signals for 24 hours and generate CSV/SVG charts.")
    parser.add_argument("--source", default=SOURCE_DEFAULT, help="Scanner signal CSV (default: early_pump_signal_results.csv)")
    parser.add_argument("--db", default=DB_DEFAULT, help="Persistent SQLite database path")
    parser.add_argument("--out", default=OUTPUT_DEFAULT, help="Directory for results CSV and SVG charts")
    args = parser.parse_args()
    if not Path(args.source).is_file():
        parser.error(f"source CSV not found: {args.source}")
    return run(args.source, args.db, args.out)


if __name__ == "__main__":
    raise SystemExit(main())
