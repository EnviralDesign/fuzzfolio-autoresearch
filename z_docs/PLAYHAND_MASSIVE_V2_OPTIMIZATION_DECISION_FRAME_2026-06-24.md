# PlayHand Massive v2 Optimization Decision Frame - 2026-06-24

This follows the throughput snapshot in `PLAYHAND_MASSIVE_V2_VAST_THROUGHPUT_SNAPSHOT_2026-06-24.md` and the local Pro bundle at `cgpt review/playhand-v2-optimization-20260624.zip`.

## Current Evidence

Snapshot grain:

- Campaign: `20260624T032549690316Z-playhand-lab-campaign-v1`
- Lane rows: 2,642
- Terminal lanes: 2,387
- Promoted lanes: 118
- Promotion rate: 4.94%
- 95% Wilson interval for terminal promotion rate: about 4.14% to 5.89%
- Active runs: 256
- Approx promoted lanes/hour: about 12.7
- Approx current compute cost: `$0.454/hr`
- Approx cost/promoted lane: `$0.035` to `$0.036`

Terminal outcomes:

- `early_exit_policy_enforced`: 1,508
- `validation_score_below_45`: 472
- `final_36mo_score_below_40`: 209
- `lab_stage_worker_failed`: 80

The campaign was still running when the bundle was generated, so nonterminal lanes are censored. Use terminal-only rates for promotion-rate calculations.

## Funnel Shape

Score-bearing lane counts:

- Baseline score: 2,510
- Lookback top 3mo score: 961
- Coarse probe top 3mo score: 897
- Coarse top 3mo score: 831
- Focused top 3mo score: 717
- Validation 12mo score: 855
- Final 36mo score: 327
- Promoted after final 36mo: 118

Task pressure by stage:

- `stage:coarse_probe`: 84,773 enqueued tasks
- `stage:baseline_3mo`: 66,055
- `stage:lookback_timing`: 14,587
- `stage:coarse_expand`: 2,938
- lane prepared baseline tasks: 2,643
- `stage:validation_12mo`: 2,168
- `stage:focused`: 782

Interpretation: most compute is spent before final validation, especially coarse probe and baseline. Speed optimization should instrument and attack those stages first, but the data does not yet include enough per-task runtime telemetry to choose exact code/runtime work.

## Apparent Discovery Signals

These are useful hypotheses, not final sampler policy.

Best exact combos with meaningful sample size:

| Combo | Terminal | Promoted | Promotion Rate | Wilson 95% | Promoted/Task |
| --- | ---: | ---: | ---: | ---: | ---: |
| `RSI_CROSSBACK+WILLR_MEAN_REVERSION` | 314 | 35 | 11.15% | 8.12%-15.11% | 0.00093 |
| `MFI_TREND+OBV_MEAN_REVERSION` | 106 | 13 | 12.26% | 7.31%-19.86% | 0.00320 |
| `CMO_MEAN_REVERSION+STOCHF_TREND` | 28 | 6 | 21.43% | 10.21%-39.54% | 0.00283 |
| `BBANDS_POSITION_TREND+MA_SPREAD_MEAN_REVERSION` | 184 | 8 | 4.35% | 2.22%-8.34% | 0.00048 |

Best individual indicators by promoted count:

- `RSI_CROSSBACK`: 41 promoted / 490 terminal, 8.37%
- `WILLR_MEAN_REVERSION`: 38 / 430, 8.84%
- `OBV_MEAN_REVERSION`: 18 / 290, 6.21%
- `MFI_TREND`: 15 / 245, 6.12%
- `TOBY_CRABEL_NARROW_RANGE`: 13 / 310, 4.19%

Best instruments by promoted count:

- `JP225`: 7 / 62, 11.29%
- `GBPCAD`: 7 / 47, 14.89%
- `UK100`: 6 / 50, 12.00%
- `GBPUSD`: 5 / 67, 7.46%
- `US30`: 5 / 55, 9.09%

Interpretation: the best-supported discovery-rate improvement is to refresh Atlas/priors from this corpus and run a controlled sampler comparison. The current campaign already has enough signal to propose sampler hypotheses, but not enough to permanently remove indicators or lock a narrow set of combos.

## Ranking The Three Lanes

### 1. B - Optimize Discovery Rate

Recommendation: first priority.

Why:

- The current sampler is already fast enough to produce useful promoted lanes.
- Promotion-rate lift compounds directly into cost/promoted and runs/hour.
- The bundle shows repeated combo-level signal, especially `RSI_CROSSBACK+WILLR_MEAN_REVERSION` and `MFI_TREND+OBV_MEAN_REVERSION`.
- We have enough current-campaign outcome data to build hypotheses and run an A/B sampler test.

Confidence: medium. The evidence is real but biased by the sampler that generated it.

Next action:

- Build a corpus-derived prior refresh that is additive/soft-weighted, not a hard allowlist.
- Run two finite campaigns with identical compute budget:
  - control: current sampler
  - treatment: updated priors
- Compare terminal promotion rate, promoted/hour, cost/promoted, and post-import quality.

### 2. A - Optimize Speed

Recommendation: second priority, but instrument immediately.

Why:

- Speed work is valuable, but the current bundle lacks per-task latency, worker host, artifact/cache state, CPU saturation, and failure timing.
- Stage pressure clearly points at baseline and coarse probe as the highest-volume targets.
- The recent lake zip delivery improvement already produced a large practical speedup, so further speed work should be measurement-led.

Confidence: medium for instrumentation need; low for any specific code optimization without more data.

Next action:

- Add/export task-level telemetry before doing more speed surgery.
- Minimum useful fields are listed below.

### 3. C - Indicator Curation

Recommendation: add-only for now; do not remove existing indicators from the available set based on this single campaign.

Why:

- Removing indicators harms corpus comparability.
- Apparent losers are heavily confounded by sampler exposure, instrument mix, stage censoring, and combo partners.
- Some zero/low performers may still be useful as context/filter ingredients in combos not yet sampled enough.

Confidence: high for "do not remove yet"; medium for "add-only exploration can be useful."

Next action:

- Add promising indicators behind an exploration share, then measure whether they earn promotion exposure.
- Only consider removal after multiple campaigns show persistently negative adjusted lift and no useful pair/combo role.

## Minimum Next Export

To make the next decision rigorous, export a task-level table for one or more campaigns:

Required fields:

- `campaign_id`
- `lane_id`
- `run_id`
- `task_id`
- `stage`
- `phase`
- `task_kind`
- `status`
- `worker_id`
- `worker_pool`
- `worker_host_id` or stable host fingerprint
- `worker_contract_hash`
- `queued_at`
- `claimed_at`
- `started_at`
- `finished_at`
- `acked_at`
- `queue_wait_seconds`
- `worker_runtime_seconds`
- `result_drain_seconds`
- `attempt_number`
- `failure_reason`
- `instrument`
- `timeframe`
- `lookback_months`
- `analysis_window_start`
- `analysis_window_end`
- `indicator_ids`
- `indicator_combo_key`
- `profile_ref`
- `score`
- `trade_count`
- `profit_factor`
- `expectancy_r`
- `max_drawdown`
- `cache_hit`
- `lake_download_seconds`
- `artifact_bytes_returned`
- `artifact_bytes_written`

Useful derived tables:

- Lane funnel table: one row per lane with first/last timestamps per stage, terminal reason, promoted flag, total tasks, total worker seconds, and final score.
- Sampler exposure table: one row per indicator/combo/instrument/timeframe with exposure count, terminal count, promoted count, tasks spent, worker seconds spent, and posterior promotion interval.
- Failure table: one row per failed task/lane with stage, worker host, instrument, error class, and retry count.

## Overfitting Traps

- Treating a single continuous campaign as an IID experiment.
- Ranking by raw promoted count without exposure and task-cost normalization.
- Penalizing indicators that were sampled mostly in weak combos.
- Rewarding indicators that were sampled by already-biased priors.
- Ignoring instrument/timeframe/anchor-window interactions.
- Comparing promoted lanes before downstream portfolio import, de-duplication, and account realism checks.
- Letting nonterminal lanes bias phase counts.

## Practical Next Step

Do not choose one lane exclusively. The best next move is:

1. Add task/lane telemetry export so speed decisions have data.
2. Build a prior-refresh analysis from the current campaign plus older campaigns.
3. Run a controlled finite A/B campaign: current sampler vs refreshed sampler.
4. Keep indicator curation add-only until the A/B and multi-campaign corpus agree.

This preserves the current productive system while moving the discovery engine toward data-driven improvement.
