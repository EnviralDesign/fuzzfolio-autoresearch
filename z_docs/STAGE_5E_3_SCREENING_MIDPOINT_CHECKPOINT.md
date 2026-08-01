# Stage 5E-3 E/F screening midpoint checkpoint

## Outcome

The one explicitly authorized 256-task E/F Screening Fresh completed exactly
once. The selector failed closed and the campaign is stopped at the mandatory
deep-review boundary:

```text
primary outcome: generator_real_market_validation_failed
robust-envelope eligible: 27
minimum required: 32
thresholds relaxed: false
G/H authority frozen: false
G/H tasks/results: 0 / 0
reserved evidence accessed: false
large search permitted: false
```

This is a useful generator/policy result, not an execution, Gateway, worker, or
restart defect. No confirmation archive, stratified control, or promotion set
was created, so selected-versus-control lift and G/H generalization are not
testable from this campaign.

## Bound identities

```text
AutoResearch screening implementation/evidence commit:
  45984c9a147f3126cb03b21f5e03e3bdefeb4a47

AutoResearch midpoint analysis commit:
  e0728924012b5749cb40791cf3847c16fac62da9

FuzzFolio commit:
  8744c7dcc726100f91dca68ab4d5e0f2ee9c2b69

worker contract:
  sha256:b69ecc83570dc1996a39d24f4e8d6d7650ab0306b15831320c5acdca40522ee9

screening authority:
  sha256:f9a458309920bda7a42ac483edb474e53570af40d8c4d00ec101662cf4df5a22

task matrix:
  sha256:d35e6e300652ffd9cfbc8981e0209a9690e730e5939c380d977fc77a705e93a2
```

The final external evidence package is:

```text
C:\repos\temporal-search-discovery-pilot\
  stage5e3-screening-midpoint-20260801T052500Z\

midpoint:
  sha256:53b641f0066be9c11d0b83a4a6bcc5c51be53bd82372eadc02d8a64e660ed7b7

manifest:
  sha256:13275d61a5afe7351d5238425792a28eab8f6e8e4552156ead0abcf62e121a9e

files: 7
audit: exact
```

## Execution integrity

The frozen result inventory contains exactly 128 E and 128 F results. All 256
tasks were materialized, completed, checkpointed, acknowledged, and pruned.
The authenticated final Gateway snapshot recorded:

```text
tasks enqueued / claims / accepted completions / result acknowledgements:
  256 / 256 / 256 / 256

duplicate enqueues/completions: 0 / 0
final or requeued failures: 0 / 0
expired or stale lease requeues: 0 / 0
lost/dropped results: 0 / 0
incompatible claims: 0
queue / leases / result backlog / retained task set: empty
```

Fifteen exact-contract workers participated: six `sager-lan`, eight `mac-lan`,
and one `temporal-search-local`. They completed 120, 110, and 26 tasks,
respectively. Per-task wall time had a 28.33-second median, 30.79-second mean,
and 56.30-second maximum. Resume was not used. The Lab Gateway was stopped
after the final snapshot; the frozen local worker remains available but idle.

## Activity and selector envelope

Of 128 candidates, 113 traded at least once:

```text
active in E and F: 99
active only in E:   4
active only in F:  10
inactive in both:  15
```

The frozen robust envelope admitted 27 rather than the required 32. Failed
check counts are overlapping:

```text
total conservative R below active median:       56
worst-window conservative R below active median: 55
minimum one trade in every screening window:    29
drawdown above active P75:                       27
cost drag per trade above active P75:            16
```

The selector thresholds were not relaxed. The midpoint tooling exposes the
predeclared envelope without changing `select_policy_v2` admission semantics.
It also represents zero-trade derived rates as finite zero values so inactive
candidates can be included in canonical diagnostic JSON; the existing
minimum-trade check still rejects them.

## Economics and cost shape

Across all candidate/window evaluations there were 3,632 trades:

```text
gross and no-cost net R:  +55.3759
conservative net R:      -299.7241
cost drag R:              355.1000

positive before costs: 53 candidates
positive after costs:  20 candidates
cost-dominated:        33 candidates
```

Candidate conservative net R had a median of `-1.1594`, P75 of `0.0`, P90 of
`1.3930`, and maximum of `4.8770`. The active-candidate cost drag per trade had
a median of `0.1 R`. This population contains real gross signal in places, but
the current turnover/economic shape is broadly unable to carry the frozen
cost model.

## Management activation

Static reachability remained valid, and trailing-stop support clearly works in
the real replay path:

```text
trailing instances authored / activated: 87 / 46
immediate trailing:                     17 / 17
explicit trailing:                       6 / 12
explicit feasible opportunity observed: yes
explicit zero-activation defect:         no
```

Break-even management is the predeclared severe-dormancy finding:

```text
break-even instances authored / activated: 40 / 5
never activated:                            35 (87.5%)
feasible-opportunity instances:              8
```

Across all 127 authored management instances, deepest causal states were 27
`activated_and_changed_trade_closure`, 24 `activated_successfully`, 50
`guard_evaluated_but_never_true`, 5 `intent_never_scheduled`, and 21
`source_state_never_occupied`. No management effects were rejected.

## Cross-window stability and diversity

The population was not behaviorally collapsed. E/F conservative-R rank
correlation was `0.7856` across all candidates and `0.7862` among candidates
active in both windows. Sign agreed or both windows were zero for 79.69% of
candidates. Composite behavioral distance had a `0.5699` median, while the
reference largest-cluster share was `0.65625`, below the frozen 0.75 collapse
boundary.

The high E/F rank agreement is evidence that the screen is measuring a
repeatable population shape, not that the strategies are economically useful:
75 candidates were negative in both windows, only 12 were positive in both,
and 15 were zero in both.

## Verification and focused evidence-tool correction

- The full local discovery and machine-local procman surface passed:
  `50 passed`.
- The hosted `Temporal search discovery controller` passed on exact commit
  `e0728924012b5749cb40791cf3847c16fac62da9` in run `30685739012`.
- The seven-file midpoint package re-audited with exact file, manifest, report,
  authority, matrix, result-inventory, and worker-contract identities.
- A diagnostic-only finite-serialization defect for zero-trade selector rows
  was corrected before the final midpoint freeze. It did not change eligibility
  or relax any threshold.
- The machine-local Fresh/Resume watchdog was restored to the normal 900-second
  procman contract after the completed run.

## Decision boundary

Generator-v2 failed this first real-market validation because the robust
envelope was too small and break-even management was severely dormant. The
population was nevertheless active, behaviorally diverse, cross-window
consistent, and capable of exercising every trailing mode. That combination
points toward a focused generator/economic-shape review, not a distributed
execution redesign and not a larger search.

G/H confirmation, threshold changes, generator changes, broader search,
candidate promotion, and production use remain blocked pending deep review.
