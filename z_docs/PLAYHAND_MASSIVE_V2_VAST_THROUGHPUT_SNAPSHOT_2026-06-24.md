# PlayHand Massive v2 Vast Throughput Snapshot - 2026-06-24

This snapshot records the overnight/current Vast-backed PlayHand Massive v2 run so we can compare future speed, discovery-rate, and indicator-curation changes against a real baseline.

## Scope

- Repo: `C:\repos\fuzzfolio-autoresearch`
- Campaign: `20260624T032549690316Z-playhand-lab-campaign-v1`
- Mode: continuous
- Active runs: 256
- Instrument pool preset: all
- Pipeline: `play_hand_lab_pipeline_v3`
- Worker path: PlayHand Lab v2 gateway, not Redis/Appwrite replay queues
- Snapshot generated: `2026-06-24T12:42:33Z`

The campaign was still running while this was measured. Treat all counts as a moving snapshot, not final campaign totals.

## Throughput And Discovery

- Lane rows observed on disk: 2,642
- Terminal lanes: 2,387
- Promoted lanes: 118
- Tombstoned lanes: 2,269
- In-progress/nonterminal lanes: 255
- Promotion rate among terminal lanes: 4.94%
- Approx promoted lanes per hour: about 12.7/h
- Tasks enqueued in campaign metadata near snapshot: 174,108

Terminal outcomes:

- Early-exit policy enforced: 1,508
- 12 month validation score below 45: 472
- Final 36 month score below 40: 209
- Lab-stage worker failed: 80

Active phase mix at snapshot:

- Baseline: 84
- Lookback: 29
- Coarse probe: 27
- Coarse expand: 22
- Focused: 36
- Validation: 30
- Instrument scout: 12
- Scrutiny: 15

## Compute Cost

Vast instances at the measurement point:

| Instance | CPU | Effective cores | Hourly |
| --- | --- | ---: | ---: |
| `42309359` | AMD EPYC 7B12 64-Core | 42.7 | `$0.131/hr` |
| `42309361` | AMD EPYC 7V12 64-Core | 42.7 | `$0.176/hr` |
| `42312700` | AMD EPYC 7642 48-Core | 38.4 | `$0.147/hr` |

Combined current hourly cost: about `$0.454/hr`.

Using Vast CLI instance durations and hourly rates at the snapshot point:

- Estimated campaign compute spend: about `$4.12`
- Cost per promoted lane so far: about `$0.035`
- Marginal cost per promoted at current rate: about `$0.036`
- Cost per 1,000 enqueued tasks so far: about `$0.024`

This does not include local gateway/desktop power or previous pre-campaign instance idle time.

## Observed Drivers

Top indicator tokens by promoted count in this snapshot:

- `RSI_CROSSBACK`: 41 promoted / 490 terminal, 8.37%
- `WILLR_MEAN_REVERSION`: 38 promoted / 430 terminal, 8.84%
- `OBV_MEAN_REVERSION`: 18 promoted / 290 terminal, 6.21%
- `MFI_TREND`: 15 promoted / 245 terminal, 6.12%
- `TOBY_CRABEL_NARROW_RANGE`: 13 promoted / 310 terminal, 4.19%

Top exact indicator combos by promoted count:

- `RSI_CROSSBACK+WILLR_MEAN_REVERSION`: 35 promoted / 314 terminal, 11.15%
- `MFI_TREND+OBV_MEAN_REVERSION`: 13 promoted / 106 terminal, 12.26%
- `BBANDS_POSITION_TREND+MA_SPREAD_MEAN_REVERSION`: 8 promoted / 184 terminal, 4.35%
- `CMO_MEAN_REVERSION+STOCHF_TREND`: 6 promoted / 28 terminal, 21.43%

Top instruments by promoted count:

- `JP225`: 7 promoted / 62 terminal, 11.29%
- `GBPCAD`: 7 promoted / 47 terminal, 14.89%
- `UK100`: 6 promoted / 50 terminal, 12.00%
- `GBPUSD`: 5 promoted / 67 terminal, 7.46%
- `US30`: 5 promoted / 55 terminal, 9.09%

These are raw observational counts from a single running campaign. They are useful for hypothesis generation but not yet enough to remove indicators or permanently bias the sampler.

## Reliability Notes

- The lake/archive path appeared healthy during the run after the archive zip delivery change.
- The campaign event log recorded 4 gateway result-read timeouts near the snapshot. They were not fatal and stayed below the coordinator failure limit.
- The `lab_stage_worker_failed` bucket was 80 terminal lanes at snapshot time. Before acting on it, inspect whether these are concentrated in missing-data instruments, transient worker failures, or a replay bug.
- Gateway and workers were using the v2 lab path. FuzzFolio dev replay queues were not part of this campaign path.

## Analysis Bundle

Prepared for ChatGPT Pro:

- Directory: `cgpt review\playhand-v2-optimization-20260624`
- Zip: `cgpt review\playhand-v2-optimization-20260624.zip`
- Zip size: about 2.7 MB

Bundle contents:

- Campaign lane summary CSV/JSON
- Campaign event summary JSON and full campaign event JSONL
- Indicator, instrument, and indicator-combo outcome rollups
- Selected promoted and tombstoned lane examples without bulky replay payloads
- Current Atlas/prior artifacts relevant to sampling and indicator curation

Excluded:

- Secrets and local config
- Full raw run corpus
- Large replay task payloads
- Raw Vast/worker logs

## Decision Frame

The current baseline is strong enough that the next improvement should be data-driven, not speculative.

Potential lanes:

- A: Speed. Preserve the current roughly 5% terminal promotion rate while reducing task latency, result-drain pressure, cold lake cost, or CPU waste.
- B: Discovery rate. Rebuild or tune Atlas/priors using this new corpus and compare whether sampler changes lift promotion rate without just overfitting one overnight run.
- C: Indicator curation. Prefer add-only exploration first. Removing indicators is destructive to corpus comparability and should require stronger evidence than this single campaign snapshot.

Suggested near-term rule: treat B as the most promising quality lever, A as the safest infra lever, and C as add-only until multiple campaigns agree on persistent dead weight.
