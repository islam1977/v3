import csv
import tempfile
import unittest
from pathlib import Path

from signal_tracker_24h import connect_db, ingest_signals, parse_utc, percent_change, refresh_signal, build_results


class SignalTrackerTests(unittest.TestCase):
    def test_percent_change(self):
        self.assertAlmostEqual(percent_change(100, 110), 10.0)
        self.assertAlmostEqual(percent_change(100, 90), -10.0)

    def test_parse_utc(self):
        self.assertEqual(parse_utc("2026-01-01T00:00:00Z"), parse_utc("2026-01-01T00:00:00+00:00"))

    def test_ingest_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "signals.csv"
            with source.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["final_time_utc", "symbol", "entry_price", "setup", "score"])
                writer.writeheader()
                writer.writerow({"final_time_utc": "2026-01-01T00:00:00Z", "symbol": "BTCUSDT", "entry_price": "100", "setup": "test", "score": "80"})
            db = connect_db(Path(tmp) / "tracker.db")
            try:
                self.assertEqual(ingest_signals(db, source), 1)
                self.assertEqual(ingest_signals(db, source), 0)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM signals").fetchone()[0], 1)
            finally:
                db.close()

    def test_refresh_stores_candles_and_completes_after_24h(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = connect_db(Path(tmp) / "tracker.db")
            try:
                db.execute("""INSERT INTO signals(signal_time_ms,symbol,entry_price,source_key)
                              VALUES(?,?,?,?,?)""".replace("VALUES(?,?,?,?,?)", "VALUES(?,?,?,?)"),
                           (1_000_000, "BTCUSDT", 100.0, "key"))
                db.commit()
                signal = db.execute("SELECT * FROM signals").fetchone()
                fake = lambda symbol, start, end: [(1_020_000, 100, 112, 98, 110)]
                refresh_signal(db, signal, 1_000_000 + 24*60*60_000, fetcher=fake)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM candles").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT status FROM signals").fetchone()[0], "COMPLETED")
                row = build_results(db)[0]
                self.assertAlmostEqual(row["max_gain_pct_24h"], 12.0)
            finally:
                db.close()


if __name__ == "__main__":
    unittest.main()
