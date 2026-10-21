import unittest

import numpy as np

from backtest_5m import sim_dyn, sim_dyn_conservative, sim_fixed


class ExitSimulationTests(unittest.TestCase):
    def test_fixed_exit_prefers_stop_when_stop_and_target_hit_same_candle(self):
        high = np.array([[106.0]])
        low = np.array([[94.0]])
        close = np.array([[100.0]])
        entry = np.array([100.0])

        result = sim_fixed(high, low, close, entry, sl=5, tp=5, n=1, slip=0.1)

        self.assertAlmostEqual(float(result[0]), -5.1)

    def test_conservative_break_even_stop_can_trigger_in_activation_candle(self):
        high = np.array([[103.0]])
        low = np.array([[99.0]])  # Above original 95 stop, below the newly raised 100 stop.
        close = np.array([[101.0]])
        entry = np.array([100.0])

        result = sim_dyn_conservative(
            high, low, close, entry, sl=5, tp=8, n=1, slip=0.1, be=2.5
        )

        self.assertAlmostEqual(float(result[0]), -0.1)

    def test_conservative_trailing_stop_can_trigger_in_activation_candle(self):
        high = np.array([[104.0]])
        low = np.array([[101.5]])  # Crosses the 102.44 trailing stop raised by this candle's high.
        close = np.array([[103.0]])
        entry = np.array([100.0])

        result = sim_dyn_conservative(
            high, low, close, entry, sl=5, tp=12, n=1, slip=0.1, tt=2.5, tr=1.5
        )

        self.assertAlmostEqual(float(result[0]), 104.0 * 0.985 - 100.0 - 0.1)

    def test_legacy_dynamic_model_does_not_retroactively_apply_new_stop(self):
        high = np.array([[103.0]])
        low = np.array([[99.0]])
        close = np.array([[101.0]])
        entry = np.array([100.0])

        result = sim_dyn(high, low, close, entry, sl=5, tp=8, n=1, slip=0.1, be=2.5)

        # Legacy model leaves the newly activated stop for the next candle.
        self.assertAlmostEqual(float(result[0]), 1.0)


if __name__ == "__main__":
    unittest.main()
