# Historical Pump Analyzer

Standalone historical research utility for the V3 scanner. It does not import or alter live scanner logic and never writes learned thresholds.

## Default run

```bash
pip install -r requirements.txt
python historical_pump_analyzer.py --days 30
```

Optional small smoke run:

```bash
python historical_pump_analyzer.py --days 3 --symbols 5
```

The full run fetches 5-minute spot klines for all discovered eligible USDT pairs and can take a while due to public API limits. Use `--workers 2` or `--workers 4` to keep request load reasonable. An optional `--universe-file symbols.txt` accepts one symbol per line.

## Label and leakage controls

- At each closed 5-minute candle, the label is positive if the **highest high during the following 24 hours** reaches at least +15% over that candle's close.
- Features are computed only from candles at or before the evaluated candle.
- The final 24 hours do not receive labels because their complete future horizon is not available.
- `historical_labeled_samples.csv` preserves both positive and negative samples; `pump_feature_analysis.csv` compares feature distributions for both groups.
- `historical_pumps.csv` lists pump onsets; `missed_pumps.csv` lists onsets without a recognized scanner signal in the preceding 24 hours, when local logs contain parseable signal timestamps.
- `analysis_errors.csv` records unavailable symbols rather than silently treating missing history as a negative.

## Important limitations

Binance's public `exchangeInfo` endpoint lists currently trading symbols, not a complete historical list of delisted spot pairs. The tool also adds symbols it can recover from local scanner CSV logs. Thus the universe is broader than only current top-volume coins, but it **cannot guarantee every pair ever listed** without an externally maintained historical symbol catalogue. You can provide a supplementary `--universe-file`.

The current scanner logs may not contain compatible symbol/time columns. If so, missed-pump classification is provisional; inspect `analysis_errors.csv` and compare against actual log schemas before drawing conclusions. A label identifies a later rise, not an executable trade or guaranteed profit. This first version reports descriptive feature differences only; it does not automatically tune or deploy thresholds. Validate any future rules on a later, untouched time period with fees/slippage and conservative stop/target assumptions.

## Outputs

Written to `historical_analysis/` by default:
- `analysis_summary.json`
- `historical_pumps.csv`
- `missed_pumps.csv`
- `pump_feature_analysis.csv`
- `historical_labeled_samples.csv`
- `analysis_errors.csv`
