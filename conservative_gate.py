"""Opt-in conservative final-signal gate, kept separate for isolated testing.

Thresholds are deliberately strict starting points for research, not evidence of
predictive edge. Enable only after a time-ordered holdout evaluation.
"""

def conservative_gate_failures(x):
    """Return stable reason codes for conservative-gate failures."""
    checks = [
        ("SCORE_BELOW_75", x.get("score", 0) >= 75),
        ("BUY_PRESSURE_BELOW_060", x.get("buy_pressure", 0) >= 0.60),
        ("VOLUME_15M_BELOW_3X", x.get("volume_15m", 0) >= 3.0),
        ("VOLUME_1H_BELOW_175X", x.get("volume_1h", 0) >= 1.75),
        ("TREND_CONTINUITY_BELOW_4", x.get("trend_continuity", 0) >= 4),
        ("GREEN_STREAK_BELOW_3", x.get("green_streak", 0) >= 3),
        ("MOVE_15M_BELOW_150", x.get("15m", -999) >= 1.50),
        ("MOVE_5M_NEGATIVE", x.get("5m", -999) >= 0.0),
        ("24H_GAIN_ABOVE_20", x.get("24h", 999) <= 20.0),
        ("UPPER_WICK_ABOVE_040", x.get("upper_wick_ratio", 1.0) <= 0.40),
        ("WASH_FLAGGED", not bool(x.get("is_wash", False))),
    ]
    return [reason for reason, passed in checks if not passed]
