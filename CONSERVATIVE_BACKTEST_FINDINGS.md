# Conservative Gate Historical Data Audit

Date of audit: 2026-10-21

## Executive conclusion

This is a **screening audit, not a valid performance backtest**. The checked-in CSVs do not contain a complete, time-aligned feature snapshot plus subsequent trade outcome for every candidate, so they cannot establish that the conservative gate improves win rate or expectancy. The conservative gate remains research-only and is not connected to the live scanner.

## Data inspected

- `early_scan_diagnostics.csv`: 2,693 rows, spanning the recorded scan diagnostics.
- `early_pump_signal_results.csv`: 31 signal rows.
- `missed_pumps.csv`: 3,833 candidate pump-onset rows, all marked as having no scanner signal in the prior 24 hours.

## What the current files show

### 1. Conservative criteria applied to scan diagnostics

Applied the available conservative requirements to diagnostic rows: score >= 75, buy pressure >= 0.60, 15m volume ratio >= 3.0, 1h volume ratio >= 1.75, trend continuity >= 4, green streak >= 3, 15m gain >= 1.50%, 5m gain >= 0%, 24h gain <= 20%, and not marked as wash trading.

- All 10 available checks passed on **16 / 2,693** diagnostic rows (0.59%).
- Those 16 rows were all `WATCH_CANDIDATE` rows: **16 / 1,181** (1.35%).
- **0 / 176** `ANALYZE_FILTER` rows passed the full available-check subset.
- Upper-wick ratio was not present in this CSV, so these are not full conservative-gate passes. This count also does not include the scanner's later confirmation, cooldown, and entry-candle logic.
- This is a candidate-retention count only; diagnostics have no linked future outcome, so it says nothing about precision or profitability.

### 2. Signal outcomes currently logged

The signal-results file contains 31 rows:

- TP1: 9
- SL: 4
- TIMEOUT: 3
- Blank/unlabeled outcome: 15

Only 16 rows have a recognized outcome label. Among those labeled rows, 9 reached TP1, 4 hit SL, and 3 timed out. This is a small, incomplete sample; TP1 is not necessarily equivalent to a profitable net trade after fees/slippage. Across all 31 rows, recorded 24h return has mean **-3.53%** and median **-4.61%**; mean MFE is **+7.29%**, and mean MAE is **-7.07%**. These descriptive statistics do not compare current versus conservative filtering, because the file lacks several gate features such as buy pressure, trend continuity, green streak, upper wick, and wash flag.

### 3. Pump-onset recall audit using only overlapping features

Applied a limited proxy for the current final gate to `missed_pumps.csv` using the available 5m/15m gains, volume ratios, buy pressure, green-candle count, and 24h gain:

- Current-gate available-feature proxy passed: **1 / 3,833**.
- Conservative available-feature proxy passed: **0 / 3,833**.

This is **not a precision or profitability result**. The file is preselected for later price rises and represents pump onsets the scanner did not signal; it does not provide a representative population of all candidates or labeled trade outcomes. It also lacks score, trend continuity, upper wick, and wash features. The counts only warn that strict entry-time gates may miss early pump onsets.

## Why a true comparison cannot yet be claimed

The datasets do not have a complete join key connecting every diagnostic candidate to:
1. the exact features known at the candidate timestamp,
2. whether the current scanner would emit a final signal,
3. whether the conservative gate would pass the same candidate, and
4. the ordered future OHLC path needed to resolve stop-loss versus take-profit first, with fees and slippage.

The signal file also has 15 blank outcome labels. They must not be filled in by guessing from aggregate MFE/MAE or 24h returns because intratrade TP/SL ordering matters.

## Next steps for a defensible backtest

1. Export timestamped feature snapshots for every candidate immediately before the current final gate, including score, buy pressure, volume ratios, trend continuity, green streak, upper wick, wash flag, and the current lane/rejection reason.
2. Link each candidate to timestamped future OHLC candles and simulate the same entry, stop, take-profit, timeout, fee, and slippage rules for both filters.
3. Split chronologically: tune only on the older segment and report the newer holdout separately. Avoid randomly splitting overlapping symbols/times.
4. Report signal count, coverage/recall on defined pump events, TP/SL/timeout rates, net expectancy, drawdown, and uncertainty intervals. Keep the conservative gate disabled until the holdout comparison supports it.

## Reproducibility note

The counts above were calculated from the CSV files currently checked into the repository on 2026-10-21. The conservative feature count excludes upper-wick validation because that field is absent from scan diagnostics. No live scanner behavior was changed by this audit.
