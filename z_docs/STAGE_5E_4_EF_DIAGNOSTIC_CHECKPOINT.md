# Stage 5E-4 read-only E/F diagnostic checkpoint

## Outcome

The two authorized diagnostics completed against the already frozen Stage 5E-3
E/F result inventory. They created no candidate task, made no Gateway or market
data request, and did not access G/H or reserved evidence.

The primary diagnosis remains a generator/economic-shape failure. The evidence
does not support changing selector-v2 thresholds, the distributed execution
architecture, or the replay semantics.

```text
E/F results revalidated:       256 / 256
candidate tasks created:         0
confirmation tasks created:      0
reserved evidence accessed:  false
Gateway contacted:           false
G/H permitted:               false
status: stage5e4_read_only_diagnostics_complete_review_required
```

## Bound identities

```text
AutoResearch diagnostic implementation:
  e4218b060a4e77e3f1d4c5ac4e0c7d7161df2c45

source Stage 5E-3 midpoint:
  sha256:53b641f0066be9c11d0b83a4a6bcc5c51be53bd82372eadc02d8a64e660ed7b7

source Stage 5E-3 midpoint manifest:
  sha256:13275d61a5afe7351d5238425792a28eab8f6e8e4552156ead0abcf62e121a9e

source result inventory:
  sha256:26b95dd70956c530be5bdb3b081ca31ac24d982db054032fa936cbb84b9898a9

task matrix:
  sha256:d35e6e300652ffd9cfbc8981e0209a9690e730e5939c380d977fc77a705e93a2

worker contract:
  sha256:b69ecc83570dc1996a39d24f4e8d6d7650ab0306b15831320c5acdca40522ee9
```

The canonical external evidence package is:

```text
C:\repos\temporal-search-discovery-pilot\
  stage5e4-diagnostics-20260801T141546Z\

diagnostic checkpoint:
  sha256:1aed4a56ff556d9d4442beb6811e68372e78626891ac1222a51a1557c2dc3758

manifest:
  sha256:efc106221e0021adb68efb795cd54e679e566c3a4829ae08e85f7b27f2cbc3a5

manifested files: 4
audit: exact
```

## Break-even causal and runway audit

The earlier `5 / 40` activation result contained a diagnostic-attribution
error. Five candidates carry explicit trailing repairs. Each has lifecycle-
preserving clones of its break-even transition in the trailing-applied and
trailing-rejected continuation states. The prior analyzer assigned a trade's
break-even record to every authored transition in that candidate rather than
to the exact transition whose intent produced the execution effect.

Exact transition and position attribution gives:

```text
logical candidates with break-even:        30
authored break-even transition instances:  40
prior reported activated instances:         5
exact activated instances:                  3
closed trades with exact BE application:    3
closed by break-even stop:                   2
```

The 80 instance-window classifications are:

```text
activated:                                                        3
no opened position:                                              15
intrabar MFE below trigger, source occupied:                     40
intrabar MFE below trigger, source not occupied:                 17
intrabar MFE at/above trigger, source not occupied:               4
intrabar MFE at/above trigger and source occupied,
  but close-mark overlap not persisted:                           1
```

Only 6 of the 350 closed trades belonging to break-even candidates recorded
intrabar MFE at or above `0.5R`; 3 of those had an exact current-route
application. This is runway evidence, not a counterfactual activation claim.
`unrealized_r_at_least` reads the completed-bar close, while MFE records the
intrabar favorable high or low. The immutable result does not retain the clock
of first threshold crossing or a per-bar close-mark R series. Aggregate state
occupancy and MFE therefore cannot prove simultaneous source-state and
close-mark eligibility.

The unambiguous finding is stronger dormancy plus scarce favorable runway. The
evidence does **not** make a canonical post-fill break-even rewrite or its
economic effect unambiguous. Any such rewrite requires a separately admitted
paired replay; E/F must not be used to tune the rewrite.

## Cost carriage and pacing

Across all 3,632 closed trades:

```text
gross R:                       +55.3759
cost drag R:                   355.1000
conservative net R:           -299.7241

gross expectancy per trade:    +0.01525 R
cost drag per trade:             0.09777 R
conservative expectancy/trade:  -0.08252 R

gross-positive/net-positive candidates: 20
gross-positive/cost-dominated:          33
gross-nonpositive:                      75
```

The robust-envelope 27 carried 544 trades and `+26.1273R` gross, but costs
reduced them to `-20.5894R` conservative net. Passing the relative selector
envelope is therefore not evidence that the current population carries the
absolute cost hurdle.

Frequency bands show a clear association:

| Candidate-frequency quartile | Trades | Gross R/trade | Net R/trade |
| --- | ---: | ---: | ---: |
| Q1, 1–8 trades | 85 | +0.2428 | +0.1536 |
| Q2, 9–22 trades | 403 | +0.0756 | -0.0222 |
| Q3, 23–33 trades | 780 | +0.0166 | -0.0785 |
| Q4, 33–155 trades | 2,364 | -0.0037 | -0.1026 |

Candidate median holding time and gross expectancy have a `0.7892`
association. Closed-trade count and total cost drag have a `0.9951`
association. This is not an immediate re-entry storm: only 1 of 3,460 observed
post-close re-entry gaps was at most 3 bars, and the rapid-re-entry-share versus
gross-expectancy association is only `0.0674`.

The dominant close path is discretionary exit:

```text
discretionary exits: 3,311 trades (91.16%)
gross / cost / net per trade: +0.0490 / 0.0976 / -0.0486 R
median holding time: 24 bars

trailing-stop exits: 214 trades (5.89%)
gross / cost / net per trade: -0.1251 / 0.0997 / -0.2248 R

take-profit exits: 11 trades (0.30%)
gross / cost / net per trade: +1.2727 / 0.1000 / +1.1727 R
median holding time: 869 bars
```

The two largest entry routes were both gross-weak: `context_to_trend_entry`
produced 942 trades at `+0.0021R` gross per trade, and
`context_to_breakout_entry` produced 930 trades at `-0.0100R`. The strongest
populated routes were `stretched_to_entry` at `+0.0973R` gross per trade and
`retest_to_entry` at `+0.0589R`, but neither carried costs in aggregate.

These are associations from one authored path, not paired management
counterfactuals. They support a generator-v3 design objective of greater entry
selectivity and favorable runway, and of avoiding shapes that create many
gross-weak trades. They do not support adding an immediate post-close cooldown
as the specific repair, nor do they prove that discretionary or trailing
management caused the weak gross edge.

## Findings classified at the review boundary

### Unambiguous

- Break-even activation attribution must remain exact-transition and
  exact-position bound; the repository analyzer is corrected and regression
  covered.
- Break-even is more dormant than previously reported: 3 rather than 5 of 40
  authored instances activated.
- The population's principal economic problem is insufficient gross edge per
  trade relative to the frozen cost model.
- Higher-frequency candidate shapes are the weakest gross cohort. Immediate
  post-close re-entry is not the source of that frequency.
- Selector-v2 remains frozen. G/H, Gateway evaluation, and larger search remain
  blocked.

### Directionally supported, not yet an implementation prescription

- Generator v3 should concentrate its synthetic design on entry selectivity,
  lower weak-trade frequency, and more favorable runway rather than on broader
  mutation volume.
- The gross-positive `stretched_to_entry` and `retest_to_entry` route shapes
  deserve hypothesis preservation, while the two dominant context routes need
  stronger pre-entry discrimination.
- A canonical post-fill management lifecycle may still be structurally simpler,
  but this audit does not admit or economically validate that repair.

### Ambiguous and prohibited as a claim

- Whether a canonical post-fill break-even transition would have fired on the
  three MFE-only opportunity trades.
- Whether changing break-even would improve P&L.
- Whether discretionary exits or trailing stops causally shortened profitable
  holds.
- Whether any seed, mutation family, or management family is superior; group
  membership overlaps and E/F is not a tuning set.

## Verification

- Full local discovery and machine-local procman regression surface:
  `54 passed`.
- Hosted `Temporal search discovery controller` on exact implementation commit
  `e4218b060a4e77e3f1d4c5ac4e0c7d7161df2c45`: run `30703360113`, passed.
- Canonical four-file package re-audited with exact checkpoint, component,
  inventory, file-length, and manifest identities.
- The only remaining worktree item is the pre-existing untracked
  `temporal-stage5b-distributed-verification/` evidence directory; it was not
  modified or committed by this batch.

## Decision boundary

This is the requested deep checkpoint. The next operation is a joint design
decision, not another search launch. A generator-v3 repair may proceed only as
a synthetic, selector-v2-frozen admission batch after review chooses a bounded
selectivity/runway hypothesis. G/H, new market evaluation, candidate promotion,
and any substantial search remain prohibited.
