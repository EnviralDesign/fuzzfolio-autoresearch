# Score Lab v2.2 Corpus Analysis - 2026-05-01

## Raw Corpus Shape

- Full 36m attempts parsed: 441
- Deduped profile/cell shapes: 341

## Score Distribution

Attempt-weighted score quantiles: `{"min": 0.0432, "p10": 23.4612, "p25": 49.5421, "median": 64.3938, "p75": 75.0161, "p90": 78.8949, "max": 86.8414}`

Unique score quantiles: `{"min": 0.0432, "p10": 22.2607, "p25": 48.5535, "median": 63.789, "p75": 75.0161, "p90": 79.7531, "max": 86.8414}`

Attempt-weighted score bins: `{"0-10": 16, "10-20": 17, "20-30": 27, "30-40": 19, "40-50": 45, "50-60": 69, "60-70": 65, "70-80": 151, "80-90": 32}`

Unique score bins: `{"0-10": 13, "10-20": 15, "20-30": 20, "30-40": 17, "40-50": 34, "50-60": 52, "60-70": 54, "70-80": 106, "80-90": 30}`

## R Distribution

Attempt-weighted landed R bins: `{"12R+": 131, "10-11.5R": 104, "5-6.5R": 53, "9-9.5R": 43, "<3R": 40, "3-4.5R": 37, "7-8.5R": 33}`

Unique landed R bins: `{"12R+": 100, "10-11.5R": 80, "5-6.5R": 41, "3-4.5R": 34, "9-9.5R": 31, "<3R": 31, "7-8.5R": 24}`

## High Score Health

Score >=70 attempt metric quantiles: `{"reward_landed": {"min": 2.0, "p10": 6.0, "p25": 9.5, "median": 11.0, "p75": 12.0, "p90": 12.0, "max": 12.0}, "trade_count": {"min": 8.0, "p10": 18.2, "p25": 35.0, "median": 108.0, "p75": 177.0, "p90": 214.0, "max": 403.0}, "trades_per_month": {"min": 0.2226, "p10": 0.5064, "p25": 0.9738, "median": 3.005, "p75": 4.9249, "p90": 5.9544, "max": 11.2131}, "max_drawdown_r": {"min": 0.0, "p10": 0.868, "p25": 0.9744, "median": 1.7706, "p75": 2.1241, "p90": 2.4891, "max": 4.0521}, "max_consecutive_losses": {"min": 2.0, "p10": 6.0, "p25": 15.0, "median": 20.0, "p75": 27.0, "p90": 37.0, "max": 38.0}, "win_rate": {"min": 0.1114, "p10": 0.1224, "p25": 0.134, "median": 0.1525, "p75": 0.2, "p90": 0.2891, "max": 0.625}, "positive_cell_ratio": {"min": 0.1488, "p10": 0.3728, "p25": 0.4912, "median": 0.6992, "p75": 0.8464, "p90": 0.912, "max": 0.9984}, "signal_coverage_pct": {"min": 0.0, "p10": 0.01, "p25": 0.02, "median": 0.04, "p75": 0.07, "p90": 0.09, "max": 0.15}, "bars_per_signal": {"min": 660.8943, "p10": 1085.6749, "p25": 1488.2313, "median": 2302.7737, "p75": 6342.9916, "p90": 12153.8333, "max": 54689.9375}, "max_signal_run": {"min": 1.0, "p10": 1.0, "p25": 1.0, "median": 1.0, "p75": 2.0, "p90": 3.0, "max": 7.0}}`

Score >=70 axis quantiles: `{"proof": {"min": 0.5011, "p10": 0.7394, "p25": 0.8786, "median": 0.9843, "p75": 0.9997, "p90": 0.9999, "max": 1.0}, "edge": {"min": 0.515, "p10": 0.5548, "p25": 0.6383, "median": 0.7092, "p75": 0.8353, "p90": 0.943, "max": 0.9832}, "ride": {"min": 0.2961, "p10": 0.3181, "p25": 0.3966, "median": 0.4443, "p75": 0.5158, "p90": 0.6788, "max": 0.7566}, "stability": {"min": 0.553, "p10": 0.855, "p25": 0.8952, "median": 0.9168, "p75": 0.9659, "p90": 0.9682, "max": 0.9844}, "viability": {"min": 0.5965, "p10": 0.7471, "p25": 0.8415, "median": 0.9819, "p75": 0.9998, "p90": 0.9998, "max": 0.9999}}`

Score >=70 flags: `{"win_rate_lt_20": 122, "high_r_10_plus": 120, "weak_ride_lt_45": 95, "loss_streak_20_plus": 93, "low_cadence_lt_2pm": 68, "r_12_plus": 64, "thin_sample_lt_36": 48, "tiny_stop_le_0_06": 35}`

## Indicator Counts

Unique high-score indicator counts: `{"WICK_REJECTION": 32, "ATR_VOLATILITY_FILTER": 29, "STOCH_CROSSOVER": 28, "STOCHRSI_CROSSBACK": 27, "PRICE_RECLAIM_MA": 26, "MA_SLOPE_TREND": 24, "MA_SPREAD_TREND": 24, "OBV_TREND": 21, "RSI_CROSSBACK": 20, "AROON_TREND": 20, "MACD_CROSSOVER": 19, "MFI_MEAN_REVERSION": 17, "PLUS_DI_TREND": 17, "STOCHF_MEAN_REVERSION": 14, "MFI_TREND": 14, "CHAIKIN_AD_MEAN_REVERSION": 13, "MACD_HISTOGRAM_PIPS_TREND": 13, "PLUS_DI_MEAN_REVERSION": 12, "MA_SPREAD_MEAN_REVERSION": 11, "CANDLESTICK_PATTERNS": 10, "CCI_TREND": 9, "ADX": 8, "ULTOSC_TREND": 8, "STOCHF_TREND": 8, "MA_DISTANCE_MEAN_REVERSION": 7}`

Unique high-score trigger counts: `{"WICK_REJECTION": 32, "STOCH_CROSSOVER": 28, "STOCHRSI_CROSSBACK": 27, "PRICE_RECLAIM_MA": 26, "RSI_CROSSBACK": 20, "MACD_CROSSOVER": 19, "CANDLESTICK_PATTERNS": 10, "CHANNEL_REENTRY": 6}`

Unique high-score lookback counts: `{"1": 469, "3": 17, "5": 1}`


See `summary.json`, `examples.json`, `indicator_performance_unique.json`, and `full_36m_attempts_compact.csv` for detail.
