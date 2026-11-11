import unittest
from unittest.mock import patch

import binance_early_pump_v4 as scanner


def candle(close):
    # Binance kline fields used by the guard: close is index 4.
    row = [0] * 12
    row[4] = str(close)
    return row


class BtcMarketGuardTests(unittest.TestCase):
    def test_ignores_currently_forming_candle(self):
        # Seven closed candles are flat; only the eighth (forming) candle crashes.
        klines = [candle(100)] * 7 + [candle(80)]
        with patch.object(scanner, "get_json", return_value=klines):
            safe, move_15m, move_1h = scanner.check_btc_market_health()

        self.assertTrue(safe)
        self.assertAlmostEqual(move_15m, 0.0)
        self.assertAlmostEqual(move_1h, 0.0)

    def test_uses_closed_candle_for_crash_detection(self):
        # The last closed candle falls 2%; the forming candle recovers.
        klines = [candle(100)] * 6 + [candle(98), candle(101)]
        with patch.object(scanner, "get_json", return_value=klines):
            safe, move_15m, _ = scanner.check_btc_market_health()

        self.assertFalse(safe)
        self.assertAlmostEqual(move_15m, -2.0)


if __name__ == "__main__":
    unittest.main()
