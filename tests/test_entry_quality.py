import unittest
from entry_quality import confirm_one_minute_entry


def candle(open_, high, low, close):
    return [0, str(open_), str(high), str(low), str(close), "0", 59999, "0", 1, "0", "0", "0"]


class OneMinuteEntryConfirmationTests(unittest.TestCase):
    def test_accepts_bullish_strong_close_near_signal(self):
        rows = [candle(100, 101, 99, 100), candle(100, 101.2, 99.9, 101), candle(101, 102, 100, 101.5)]
        accepted, reason, metrics = confirm_one_minute_entry(rows, 101.2)
        self.assertTrue(accepted, reason)
        self.assertEqual(reason, "CONFIRMED")
        self.assertGreaterEqual(metrics["close_location"], 0.60)

    def test_rejects_bearish_closed_candle(self):
        rows = [candle(100, 101, 99, 100), candle(101, 101.2, 99.5, 100), candle(100, 101, 99, 100)]
        accepted, reason, _ = confirm_one_minute_entry(rows, 100)
        self.assertFalse(accepted)
        self.assertEqual(reason, "BEARISH_1M_CANDLE")

    def test_rejects_price_losing_signal_level(self):
        rows = [candle(100, 101, 99, 98.5), candle(98.9, 99.1, 97, 99), candle(99, 100, 98, 99)]
        accepted, reason, _ = confirm_one_minute_entry(rows, 100)
        self.assertFalse(accepted)
        self.assertEqual(reason, "PRICE_LOST_SIGNAL_LEVEL")

    def test_rejects_chasing_a_large_one_minute_move(self):
        rows = [candle(100, 101, 99, 100), candle(101, 103, 100.8, 102), candle(102, 103, 101, 102)]
        accepted, reason, _ = confirm_one_minute_entry(rows, 100)
        self.assertFalse(accepted)
        self.assertEqual(reason, "CHASED_1M_MOVE")

    def test_requires_enough_candles(self):
        accepted, reason, _ = confirm_one_minute_entry([], 100)
        self.assertFalse(accepted)
        self.assertEqual(reason, "INSUFFICIENT_1M_CANDLES")


if __name__ == "__main__":
    unittest.main()
