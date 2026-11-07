"""One-minute entry confirmation helpers for the Binance early-pump scanner.

The scanner requests the latest three 1m klines. Binance includes the currently
forming candle in that response, so this module deliberately ignores the final
row and evaluates only completed candles.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Tuple


def _number(value: Any) -> float:
    """Convert a Binance kline field to float, rejecting invalid values."""
    result = float(value)
    if result != result or result in (float("inf"), float("-inf")):
        raise ValueError("non-finite kline value")
    return result


def confirm_one_minute_entry(
    klines: Iterable[Iterable[Any]], reference_price: float
) -> Tuple[bool, str, Dict[str, float]]:
    """Return (accepted, reason, metrics) using completed 1m candles only.

    klines is expected to be the raw Binance array returned by
    GET /api/v3/klines with limit=3. The newest row is the live/forming candle
    and is excluded. At least two completed candles are required.
    """
    try:
        rows = list(klines or [])
        if len(rows) < 3:
            return False, "INSUFFICIENT_1M_CANDLES", {"candles_received": float(len(rows))}

        # Binance's final kline is still forming; never use it for confirmation.
        completed = rows[:-1]
        previous, latest = completed[-2], completed[-1]

        prev_close = _number(previous[4])
        open_price = _number(latest[1])
        high = _number(latest[2])
        low = _number(latest[3])
        close = _number(latest[4])
        price = _number(reference_price)

        if min(prev_close, open_price, high, low, close, price) <= 0:
            return False, "INVALID_1M_PRICE", {}

        candle_range = max(high - low, 0.0)
        body = close - open_price
        upper_wick = max(high - max(open_price, close), 0.0)
        close_position = (close - low) / candle_range if candle_range else 0.5
        upper_wick_ratio = upper_wick / candle_range if candle_range else 0.0
        price_vs_close_pct = (price / close - 1.0) * 100.0
        previous_change_pct = (close / prev_close - 1.0) * 100.0

        metrics = {
            "close_position": round(close_position, 4),
            "upper_wick_ratio": round(upper_wick_ratio, 4),
            "price_vs_close_pct": round(price_vs_close_pct, 4),
            "previous_change_pct": round(previous_change_pct, 4),
            "candle_body_pct": round((body / open_price) * 100.0, 4),
        }

        if body <= 0:
            return False, "LAST_COMPLETED_1M_NOT_BULLISH", metrics
        if close <= prev_close:
            return False, "1M_CLOSE_NOT_ABOVE_PREVIOUS_CLOSE", metrics
        if close_position < 0.55:
            return False, "1M_CLOSE_TOO_FAR_FROM_HIGH", metrics
        if upper_wick_ratio > 0.50:
            return False, "1M_UPPER_WICK_TOO_LARGE", metrics
        if price_vs_close_pct < -0.50:
            return False, "LIVE_PRICE_BELOW_CONFIRMED_CLOSE", metrics
        if price_vs_close_pct > 1.50:
            return False, "ENTRY_TOO_EXTENDED_FROM_1M_CLOSE", metrics

        return True, "1M_ENTRY_CONFIRMED", metrics

    except (TypeError, ValueError, IndexError, OverflowError):
        return False, "INVALID_1M_KLINE_DATA", {"error": 1.0}
