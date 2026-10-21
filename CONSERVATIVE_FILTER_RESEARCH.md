# Conservative Final Gate — Research Draft

## Status

Research-only branch. This gate is **not wired into the live scanner** and does not change live signal behavior.

## Proposed filters

A candidate passes only if all are true:

- score >= 75
- buy pressure >= 0.60
- 15-minute relative volume >= 3.0x
- 1-hour relative volume >= 1.75x
- trend continuity >= 4
- green streak >= 3
- 15-minute price move >= 1.50%
- 5-minute price move >= 0%
- 24-hour gain <= 20%
- upper-wick ratio <= 0.40
- wash-trading flag is false

Missing features fail closed. Each failed condition has a stable reason code for diagnostics.

## Why these are only candidate thresholds

The currently available diagnostic CSV contains scanner features and stages, but not future trade outcomes for each scan row. The signal-results CSV has only 31 signal records and 15 lack outcome labels. Therefore, the available data can estimate how many candidates would be filtered out, but cannot establish that these filters improve precision or profitability.

## Validation required before integration

1. Repair outcome tracking for future signals and keep historical unknown outcomes unknown.
2. Capture each candidate's timestamp, symbol, feature vector, gate decision, and later outcome.
3. Replay the proposed gate against a time-ordered dataset; keep a later holdout period untouched.
4. Report signal retention, SL rate, TP1 rate, timeout rate, realized exit return, and missed positive pump onsets.
5. Include fees/slippage and use conservative same-candle SL/TP ordering.
6. Enable the gate only after the holdout results demonstrate a meaningful reduction in false positives without unacceptable loss of valid signals.

## Unit tests

Run:

```bash
python -m unittest discover -s tests
```
