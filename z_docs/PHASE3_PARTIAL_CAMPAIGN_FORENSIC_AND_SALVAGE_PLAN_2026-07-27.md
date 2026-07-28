# Phase 3 Partial Campaign Forensic and Salvage Plan

Date: 2026-07-27  
Campaign: `phase3-darwin-rich-ab-v3`

## Executive conclusion

The current Phase 3 campaign should not be discarded, and it should not be
described as a completed 10,000-strategy experiment.

It is a computationally clean but structurally truncated guided experiment:

- 1,991 guided candidates were actually evaluated.
- 62 passed the complete 3-month, 12-month, and 36-month funnel.
- Those 62 reduce to 43 exact unique final profile files.
- 2,620 additional guided lane assignments produced no strategy because the
  campaign's diversity caps had become impossible to satisfy.
- No uncertain or wild lane has been evaluated.
- No worker task failure is recorded in the authoritative lane state.

The campaign therefore contains useful evidence about a narrow, highly
concentrated part of the current fuzzy strategy language. It cannot answer
whether the full 60/25/15 campaign policy works, whether Atlas guidance beats
uncertain or wild sampling, or whether the fuzzy strategy language has been
searched broadly.

The least-wasteful next step is not another 10,000-lane campaign. It is a
small, preregistered fixed-profile and fixed-cell stability study of the
survivors, with matched rejected controls. This should require hundreds of
replays rather than tens of thousands of PlayHand tasks.

## Authoritative evidence surface

Current accounting comes from:

`runs/derived/play-hand-lab-campaigns/phase3-darwin-rich-ab-v3/play-hand-lab-state.json`

The campaign summary is stale and must not be used for current totals:

`runs/derived/play-hand-lab-campaigns/phase3-darwin-rich-ab-v3/play-hand-lab-campaign-summary.json`

The state was stable when this review completed. The 9.9 GB execution journal
and 1.1 GB event stream were deliberately not scanned. They are not needed for
the findings below.

One accounting seam remains to reconcile before producing a final immutable
campaign receipt: lane phase counts total 44,301 completed tasks while
`recorded_result_count` is 44,274, a difference of 27. There are no lane-level
task failures, so this is not evidence that 27 research computations failed.

## What actually ran

The frozen campaign plan requested:

| Lane | Planned |
|---|---:|
| Guided | 6,000 |
| Uncertain | 2,500 |
| Wild | 1,500 |
| Total | 10,000 |

The authoritative current state contains:

| State | Count |
|---|---:|
| Guided candidates actually evaluated | 1,991 |
| Guided lanes exhausted without a candidate | 2,620 |
| Uncertain candidates evaluated | 0 |
| Wild candidates evaluated | 0 |
| Promoted lanes | 62 |
| Exact unique promoted profile files | 43 |

The 62/1,991 promotion rate is 3.11%. The deduplicated profile rate is
43/1,991, or 2.16%. Dividing by the planned 10,000 lanes would be misleading
because most of those lanes never produced a strategy.

## Funnel results

| Funnel stage | Candidates | Result |
|---|---:|---|
| Baseline 3-month screen | 1,991 | 309 advanced; 1,682 early-exited |
| Coarse, timing, and focused search | 309 | completed without task failures |
| Validation 12-month gate | 309 | 206 scored at least 45; 103 failed |
| Final 36-month scrutiny | 206 | 62 scored at least 40; 144 failed |
| Promoted | 62 | 43 exact unique profile files |

Score distributions:

| Stage | N | Mean | Maximum |
|---|---:|---:|---:|
| Baseline 3-month | 1,991 | 7.90 | 70.96 |
| Coarse top 3-month | 289 | 71.60 | 83.50 |
| Lookback top 3-month | 309 | 66.01 | 80.98 |
| Validation 12-month | 309 | 42.38 | 80.50 |
| Final 36-month | 206 | 19.76 | 82.44 |
| Promoted final scores | 62 | 55.01 | 82.44 |

This is a harsh and useful funnel. It does not look like the old system simply
rubber-stamped short-window winners. Only 15.5% passed the baseline, 66.7% of
those passed validation, and 30.1% of validation survivors passed final
scrutiny.

## Why guided capacity failed

The Phase 3 seed plan is correctly bound to the new A/B rich-prior generation.
It was not accidentally built from an old Atlas artifact.

The seed plan contains:

- 6 recipes;
- 68 total pair families;
- 4 `positive_pair` families;
- 8 `near_miss_pair` families;
- 56 `low_pair` families.

The campaign policy allows guided sampling to consume only `positive_pair`
families. All four guided-positive pairs contain `RSI_MEAN_REVERSION`.
Pairless or single-indicator candidates are all assigned the same typed
`family=absent` value.

The campaign caps are:

- 500 per family;
- 1,500 per indicator;
- 1,000 per instrument;
- 3,000 per recipe;
- 6,000 per timeframe.

Consequently, guided capacity was approximately bounded by:

- 1,500 RSI-containing pair candidates; plus
- 500 pairless candidates in the shared absent-family bucket.

The observed 1,991 accepted candidates are effectively the mathematical
ceiling of that policy. The runtime did what the authority instructed. The
authority should never have been allowed to reserve 6,000 guided slots without
a deterministic capacity preflight.

This is primarily a Phase 3 policy and taxonomy defect:

1. The guided quota did not match the eligible menu capacity.
2. Pairless compositions were collapsed into one family.
3. Fixed lane quotas could not borrow unused capacity.
4. No authority preflight proved that the requested lane allocation was
   realizable.

Atlas may still be honestly reporting that only four A/B pairs belonged in the
strongest positive category. That narrowness is evidence, not necessarily an
Atlas bug.

## Concentration of evaluated candidates

Accepted guided candidates:

| Recipe | Count |
|---|---:|
| `BREAKOUT_COMPRESSION_RELEASE` | 500 |
| `DISCOVERED_RECIPE_001` | 500 |
| `DISCOVERED_RECIPE_002` | 500 |
| `MEAN_REVERSION_RECLAIM` | 491 |

Dominant indicator use:

| Indicator | Count |
|---|---:|
| `RSI_MEAN_REVERSION` | 1,500 |
| `THRUST_BAR_SIGNAL` | 1,011 |
| `KEY_REVERSAL_SIGNAL` | 499 |

The RSI count reached the hard campaign cap exactly.

## Concentration of promoted candidates

Promotions by source recipe:

| Recipe | Promotions |
|---|---:|
| `MEAN_REVERSION_RECLAIM` | 23 |
| `DISCOVERED_RECIPE_001` | 17 |
| `DISCOVERED_RECIPE_002` | 11 |
| `BREAKOUT_COMPRESSION_RELEASE` | 11 |

Other concentration:

- 52/62 promoted profiles contain `RSI_MEAN_REVERSION`.
- 28/62 use exactly `RSI_MEAN_REVERSION + THRUST_BAR_SIGNAL`.
- 12/62 use exactly `RSI_MEAN_REVERSION + KEY_REVERSAL_SIGNAL`.
- 40/62 promotions therefore come from only two indicator compositions.
- CADCHF accounts for 16 promotions.
- AUDCHF accounts for 15 promotions.
- CADCHF and AUDCHF together account for 31/62 promotions.
- The 62 promotions contain only 19 distinct indicator sets.
- Exact final-profile hashing reduces 62 promotions to 43 unique files.

This is not broad evidence for the fuzzy grammar. It is evidence that a narrow
mean-reversion-heavy sublanguage can produce historical survivors.

## Evidence that should not be dismissed

Some final results are substantive enough to deserve real holdout testing.

The strongest promoted lane, lane 830, is a discovered
`RSI_MEAN_REVERSION + THRUST_BAR_SIGNAL` profile on AUDCHF:

- final 36-month score: 82.44;
- 223 resolved trades;
- final equity: 102.36R;
- profit factor: 2.24;
- maximum drawdown: 9.76R;
- positive matrix cells: 76%;
- normal-R positive matrix cells: 76.6%.

This is not proof of a tradable strategy. The 36-month window shares its final
12 months with the instrument-selection and validation process, and the exit
cell was selected from the final replay surface. It is, however, sufficiently
strong historical evidence that discarding it without a frozen holdout test
would waste information.

The surviving cohort as a whole is less comfortable than the best example.
Among the locally resolved unique final-result subset, median drawdown was
about 22R and median time underwater was about 87%. Even when the score gate
passes, many candidates are not obviously deployment-quality.

## Relationship to Phase 1 and Phase 2

Phase 1 established that the legacy corpus was not reliable under the current
execution contract:

- 450 fixed cells replayed successfully;
- 204 were positive, 136 negative, and 110 flat;
- aggregate unweighted net result was -15,911.19R;
- older unseen evaluation strongly favored negative or no-signal outcomes;
- no stable portfolio champion survived the nested comparison.

Phase 2 established a robustness gradient:

- broad indicator and structural ranks were highly stable across cutoffs;
- exact pair replay rankings and top-N identities were unstable;
- recurring structural families existed, but exact historical winners did not;
- the defensible use of Atlas was therefore as an exploration bias, not a list
  of strategies to trust.

Phase 3 is consistent with both findings:

- Atlas supplied a useful structural bias;
- the PlayHand funnel found some historical survivors;
- those survivors concentrated into a few recipes, indicators, and markets;
- the experiment still lacks an independent control lane and untouched outer
  evaluation.

Nothing here justifies returning to old exact-prior selection. Nothing here
yet proves that fuzzy scoring is incapable of generalizing.

## What this campaign answers

The campaign provides evidence that:

1. The current funnel is materially stricter than the old promotion process.
2. A narrow rich-prior-guided fuzzy sublanguage can produce 36-month
   historical survivors.
3. The survivors are highly concentrated in mean reversion and a small number
   of instruments.
4. The current authority and diversity taxonomy cannot realize the intended
   6,000-lane guided allocation.
5. Additional guided candidates generated from the same four positive pairs
   are unlikely to change the architectural picture materially.

## What this campaign does not answer

It does not establish:

1. that the 62 promotions survive a genuinely frozen fixed-cell holdout;
2. that guided sampling beats uncertain or wild sampling;
3. that the broad fuzzy grammar was searched;
4. that non-mean-reversion fuzzy strategies lack edge;
5. that temporal/state-machine grammar would perform better;
6. that any current candidate is ready for Darwinex deployment.

## Recommended salvage experiment

Do not launch another full PlayHand campaign yet.

### Stage 1: freeze and deduplicate

Create an immutable comparison plan containing:

- the 43 exact unique promoted profiles;
- their source lane and final evidence receipts;
- the exact final profile hash;
- a frozen robust or recommended execution cell;
- no profile mutation;
- no cell reoptimization;
- no use of the reserved tail.

Select a preregistered matched control cohort from the 144 candidates that
reached final 36-month scrutiny but failed the score-40 gate. Match controls by
source family, instrument class, trade-count band, and final score proximity
where possible. Freeze their profiles and cells through the same mechanism.

The matched control is important. Without it, a positive result cannot tell us
whether PlayHand selection added value or whether many arbitrary fuzzy
profiles would also look acceptable somewhere in history.

### Stage 2: historical offset panel

Run promoted and control profiles over several disjoint historical windows
outside the Phase 3 construction interval. Derive exact windows from verified
lake coverage rather than assuming dates in code.

Recommended shape:

- three or four disjoint 6- or 12-month windows;
- identical frozen profile and cell in every window;
- typed no-signal and insufficient-sample outcomes;
- per-window trades, net R, expectancy, drawdown, and path-concentration
  metrics;
- aggregate stability and worst-window statistics;
- no selection or parameter changes after observing a window.

At 43 promoted profiles plus a similarly sized control cohort, four windows
would be roughly 344 fixed replays. That is small compared with another
PlayHand campaign.

Before execution, preregister the comparison rules. At minimum evaluate:

- positive-window count;
- aggregate and worst-window net R;
- minimum trade support;
- concentration of gains;
- drawdown and time-underwater behavior;
- promoted-versus-control lift.

### Stage 3: reserved outer tail

Use the untouched `[2026-01-14, 2026-07-14)` tail only after the historical
offset panel is complete and its survivor rule is frozen.

Run only the offset-panel survivors on the tail:

- exact frozen profile;
- exact frozen cell;
- no optimization;
- no fallback substitution;
- one immutable report;
- tail is considered spent after this test.

This is the decisive test of whether the current fuzzy machinery has produced
anything that deserves further investment.

## Decision rule after salvage

### If promoted profiles beat controls and several survive the tail

Do not replace fuzzy scoring wholesale. Keep it as a working strategy
language, improve the campaign taxonomy, and develop temporal/state-machine
grammar as a parallel expansion. The evidence would show that fuzzy
composition has some real selection value even if its vocabulary is narrow.

### If promoted profiles do not beat controls or collapse on the tail

Stop scaling the current PlayHand grammar. Preserve any isolated diagnostics,
but make the constrained temporal/state-machine strategy language the primary
research investment. More sampling from the same static weighted-indicator
grammar would be unlikely to solve the generalization problem.

### If results are mixed

Retain the few durable fuzzy archetypes as primitives or regime filters inside
the future temporal grammar. Pivot strategy discovery toward the new grammar
without discarding useful fuzzy components.

## Work required before any new large campaign

Regardless of the salvage outcome:

1. Add deterministic lane-capacity preflight to Phase 3 authority generation.
2. Reject an authority whose requested lane quotas are not realizable.
3. Give pairless strategies a real canonical family identity.
4. Report expected versus realizable capacity per lane and cap dimension.
5. Decide explicitly whether unused lane capacity is terminal or may be
   reassigned before authority creation.
6. Keep guided, uncertain, and wild results separately accountable.
7. Reconcile the 27-result current-state accounting difference.
8. Never interpret exhausted lane slots as tested strategies.

## Recommended immediate decision

Preserve the current campaign unchanged as a partial guided experiment. Do not
pay for another 10,000-lane run now. Build and run the small fixed-profile,
fixed-cell salvage comparison first.

That study is capable of answering the question that matters most:

> Did the current fuzzy PlayHand funnel identify candidates that generalize
> better than matched rejected candidates, or did it merely select attractive
> historical artifacts?

The answer determines whether the next major investment should improve the
existing fuzzy grammar or prioritize the constrained temporal strategy
language.
