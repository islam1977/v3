import unittest

from conservative_gate import conservative_gate_failures


class ConservativeGateTests(unittest.TestCase):
    def valid_candidate(self):
        return {
            "score": 80,
            "buy_pressure": 0.65,
            "volume_15m": 4.0,
            "volume_1h": 2.0,
            "trend_continuity": 5,
            "green_streak": 3,
            "15m": 2.0,
            "5m": 0.5,
            "24h": 12.0,
            "upper_wick_ratio": 0.25,
            "is_wash": False,
        }

    def test_accepts_candidate_meeting_all_thresholds(self):
        self.assertEqual(conservative_gate_failures(self.valid_candidate()), [])

    def test_rejects_weak_buy_pressure_and_low_volume(self):
        x = self.valid_candidate()
        x["buy_pressure"] = 0.59
        x["volume_15m"] = 2.99
        failures = conservative_gate_failures(x)
        self.assertIn("BUY_PRESSURE_BELOW_060", failures)
        self.assertIn("VOLUME_15M_BELOW_3X", failures)

    def test_rejects_wash_flag_and_overextended_move(self):
        x = self.valid_candidate()
        x["is_wash"] = True
        x["24h"] = 20.1
        failures = conservative_gate_failures(x)
        self.assertIn("WASH_FLAGGED", failures)
        self.assertIn("24H_GAIN_ABOVE_20", failures)

    def test_missing_features_fail_closed(self):
        self.assertTrue(conservative_gate_failures({}))


if __name__ == "__main__":
    unittest.main()
