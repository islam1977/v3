import unittest
from historical_pump_analyzer import forward_pump_labels, is_eligible_symbol


class HistoricalPumpAnalyzerTests(unittest.TestCase):
    def test_labels_only_from_future_highs(self):
        closes = [100.0, 100.0, 100.0, 100.0, 100.0]
        highs = [100.0, 110.0, 114.0, 115.0, 100.0]
        labels = forward_pump_labels(highs, closes, horizon=3, threshold_pct=15)
        self.assertEqual(labels[0], True)
        self.assertEqual(labels[1], False)
        self.assertIsNone(labels[2])
        self.assertIsNone(labels[3])
        self.assertIsNone(labels[4])

    def test_threshold_is_inclusive(self):
        labels = forward_pump_labels([100, 115, 100], [100, 100, 100], horizon=1, threshold_pct=15)
        self.assertEqual(labels[0], True)

    def test_stable_and_leveraged_pairs_excluded(self):
        self.assertFalse(is_eligible_symbol("USDCUSDT"))
        self.assertFalse(is_eligible_symbol("BTCUPUSDT"))
        self.assertFalse(is_eligible_symbol("ETHDOWNUSDT"))
        self.assertTrue(is_eligible_symbol("SOLUSDT"))

    def test_wrong_quote_excluded(self):
        self.assertFalse(is_eligible_symbol("SOLEUR"))


if __name__ == "__main__":
    unittest.main()
