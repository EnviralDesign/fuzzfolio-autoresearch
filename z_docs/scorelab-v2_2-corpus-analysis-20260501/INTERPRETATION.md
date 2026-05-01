# ScoreLab v2.2 Corpus Interpretation

Generated from the rebuilt autoresearch corpus after ScoreLab v2.2 was promoted. Profile-drop image output was intentionally ignored.

## Corpus Shape

- 25 run directories were present.
- 519 attempts were cataloged and scored.
- 441 attempts had valid 36 month full-backtest data.
- A stricter effective-profile/cell dedupe collapsed those to 180 distinct profile/cell outcomes.

The strict dedupe matters because many attempts are repeated variants of the same effective indicator configuration and selected exit cell. Attempt-weighted summaries overstate how much independent evidence exists.

## Score Distribution

Strict unique score quantiles:

- min: 0.04
- p10: 14.85
- p25: 33.25
- median: 58.86
- p75: 71.01
- p90: 77.58
- max: 86.84

Strict unique high-score counts:

- >= 40: 130
- >= 50: 114
- >= 60: 83
- >= 70: 50
- >= 80: 11
- >= 85: 3

Interpretation: v2.2 is meaningfully stricter than the old score, especially against high-churn strategies, but 70+ does not yet mean "human-clean." The top band still includes a lot of high-R, low-win-rate, long-loss-streak behavior.

## Exit Geometry

Strict unique landed reward bins:

- 12R+: 45
- 10R to 11.5R: 35
- 9R to 9.5R: 16
- 7R to 8.5R: 14
- 5R to 6.5R: 24
- 3R to 4.5R: 20
- under 3R: 26

For strict unique outcomes scoring >= 70:

- median selected reward: 11R
- p75 selected reward: 12R
- p90 selected reward: 12R
- 29 of 50 are >= 10R
- 18 of 50 are >= 12R
- 13 of 50 have stop <= 0.06%

Interpretation: the 12R attractor is reduced but definitely not gone. ScoreLab v2.2 no longer blindly rewards every high-R artifact, but the highest-scoring zone still tolerates too much skew when other qualities look good.

## Ride Quality

For strict unique outcomes scoring >= 70:

- median trades: 69
- median trades/month: 1.92
- median max consecutive losses: 16.5
- p75 max consecutive losses: 21.75
- median win rate: 18.76%
- 26 of 50 have win rate under 20%
- 17 of 50 have max loss streak >= 20
- 18 of 50 have ride below 45%

Interpretation: the loss-streak resilience addition found real problems, but the current assembly still lets some psychologically rough strategies reach high headline scores. This is the main place I would tighten next.

The useful distinction is:

- Churn control is working: old-score high-trade artifacts are heavily demoted.
- Ride utility is only partially working: sparse but brutal loss-streak systems can still score high.

## Good Signs

The corpus contains real lower-R candidates:

- Score 83.59 at 3R, 0.12% stop, 31 trades, 7 max losses, 41.9% win rate.
- Score 78.07 at 3.5R, 0.06% stop, 128 trades, 27 max losses, 23.4% win rate.
- Score 77.35 at 3R, 0.04% stop, 13 trades, 2 max losses, 38.5% win rate.
- Score 76.78 at 5R, 0.24% stop, 189 trades, 17 max losses, 22.8% win rate.

Interpretation: the new score can surface non-12R strategies. They are not dominating yet, but they exist and are worth manual review in the replay viewer.

## Old Score Versus v2.2

ScoreLab v2.2 strongly penalizes high-churn artifacts that old scoring liked. Examples include:

- MACD_CROSSOVER: old 69.50 to new 18.85, 17,671 trades.
- MACD_HISTOGRAM_PIPS_MEAN_REVERSION / STOCH_CROSSOVER / MA_DISTANCE_MEAN_REVERSION: old 73.31 to new 25.45, 13,961 trades.
- MACD_CROSSOVER: old 66.80 to new 20.76, 17,253 trades.

Interpretation: v2.2 is directionally correct. It closes a major old-score exploit around producing huge trade volume with acceptable aggregate equity stats.

## Entry Indicator Findings

High-scoring strict unique profiles are still heavily concentrated around oscillator-plus-trigger families.

Top median performers by unique profile membership include:

- MFI_MEAN_REVERSION
- STOCHF_MEAN_REVERSION
- RSI_CROSSBACK
- ULTOSC_TREND
- OBV_MEAN_REVERSION
- CHAIKIN_AD_MEAN_REVERSION
- WICK_REJECTION
- MA_SLOPE_TREND
- AROON_TREND

Trigger-backed entries are present in high scorers:

- STOCHRSI_CROSSBACK
- WICK_REJECTION
- RSI_CROSSBACK
- MACD_CROSSOVER
- PRICE_RECLAIM_MA
- STOCH_CROSSOVER
- CANDLESTICK_PATTERNS
- CHANNEL_REENTRY

However, high-scoring configs still overwhelmingly use lookbackBars = 1. In the strict high-score group, lookbackBars usage was:

- 1 bar: 174 indicator configs
- 3 bars: 5 indicator configs
- 5 bars: 1 indicator config

Interpretation: the controller is not yet exploring signal persistence enough. That is probably holding back multi-indicator alignment. The corpus does not prove lookbackBars is weak; it proves it is under-sampled.

## Airtable Backburner Indicator Ideas

Best immediate entry candidates:

1. KST

KST is a multi-cycle momentum oscillator with a signal-line crossover. It should provide a cleaner, more deliberate entry trigger than simple one-window oscillators. It is a good candidate for `KST_CROSSOVER` and possibly `KST_TREND`.

2. PMO

PMO is a double-smoothed rate-of-change momentum oscillator with signal-line behavior. It is simpler than KST, entry-friendly, and likely useful as a trigger or confirmation signal.

3. Roofing Filter

The Roofing Filter is useful for cycle-band cleanup and could reduce noisy oscillator entries. Implementation complexity is higher than PMO/KST, but the concept is distinct.

Best context/filter candidates:

1. Random Walk Index

This is a strong trend-validity filter. It helps distinguish structured movement from random-looking movement. It is probably more useful as context than as a direct entry signal.

2. Volatility Quality Index

VQI could help separate directional volatility from noisy volatility. This may be valuable for filtering high-R artifacts that rely on rare extended moves.

3. TrendFlex

TrendFlex is another context candidate that may help distinguish durable trend from noisy impulse.

Defer for now:

- GARCH: useful but heavy and not entry-focused.
- Volume Profile and Wave Weis: volume semantics need more confidence first.
- Adaptive Structure ZigZag: useful, but repaint/right-edge semantics need explicit handling.
- HMA, McGinley, Laguerre, Predictive MA, Kalman: useful smoothers, but less directly aimed at the entry-quality gap.

## Recommended ScoreLab v2.3 Direction

1. Tighten Ride.

Ride should more strongly penalize long loss-streak burden and very low win-rate experiences, especially when paired with high R and tiny stops. This should remain symptom-based rather than a direct anti-12R rule.

2. Add or expose an entry-spacing / entry-clustering measure.

This should measure whether a strategy fires in bar-to-bar clusters on its lowest operating timeframe. It is distinct from trade cadence. Cadence can look sparse over 36 months while entries still occur in local bursts.

3. Increase controller pressure toward lookbackBars exploration.

The current corpus barely samples 2 to 5 bar persistence. Multi-indicator strategies need some temporal tolerance to align without forcing bar-perfect coincidences.

4. Implement one new entry primitive.

KST is the strongest first candidate. PMO is the simpler fallback. The goal is to add a cleaner momentum trigger so agents are not forced to squeeze precision out of blunt oscillators.

5. Keep old score as diagnostic only.

The old score is useful as a comparison column, but v2.2 is already better aligned with the desired behavior. Further iteration should happen inside the new scoring framework.

