# Temporal QD audit remediation checkpoint

Date: 2026-08-09

Status: implementation and local no-market verification complete. No economic
campaign or reserved evidence was used.

Dashboard dependency: `EnviralDesign/FuzzFolio` main at `1766e272` or later with
identical temporal source content.

## Outcome

This checkpoint repairs the evidence and selection defects found in the first
`4,000 -> 1,024 x 5` audit and introduces a fresh, explicitly versioned v5 search
authority. The v5 path is not a relabelled typed-fragment campaign: its mutable
genotype is an AutoResearch-owned evolvable module genome which compiles into the
existing Dashboard-owned v2 one-direction profile and canonical v3 bidirectional
program before execution.

Legacy candidates, archives, journals, and campaigns retain read/restart support
under their original authorities. They are not upgraded or rehashed in place.

## Correctness and evidence admission

- Canonical evidence identities distinguish raw authored profile, normalized
  snapshot, resolved program, panel, window, cost view, and observation stream.
  Sidecar and rotating-panel admission recompute the applicable identity rather
  than trusting a plausible SHA string.
- Completed scrutiny now consumes the deduplicated union of final archive
  incumbents and the final new-proposal cohort. Archive admission binds the exact
  generation, policy, version, geometry, and self-hash.
- Python and Rust validate all seven QD descriptor coordinates, exact cell IDs,
  unique cells/candidates, and corrected descriptor semantics. Historical v3
  archives remain readable; corrected v4 and direction-aware v5 are explicit.
- Native tail reduction now aggregates and validates realized behavior with the
  same identity as the Python oracle.
- Current v5 uses raw result provenance. Indexed tail artifacts fail closed until
  a separately versioned index carries exact direction behavior.

## Reproduction and direction truth

- Fresh accepted-quota scheduling declares exact offspring/immigrant targets,
  supports parent sampling with replacement, retains a 20% immigrant floor, and
  records scheduled, attempted, materialized, accepted-for-evaluation, and
  deficit counts without calling unevaluated candidates evaluated.
- Python and Rust agree for small widths, including targets 1 through 6. Frozen v1
  sparse schedules again receive their exact historical semantic validation.
- Per-side realized behavior includes activity, trades, economics, costs,
  drawdown, exposure, holding, terminal state, conflicts, and event/action
  liveness. Its canonical identity is verified before archive classification.
- Selection has separately named balanced, long-specialist, short-specialist,
  inactive/unsupported, and harmful-opposite-side outcomes. Specialists may
  survive; an inactive or harmful side may not silently subsidize breeding.
- Rotating evidence recomputes direction eligibility from cumulative required
  panels, including negative-frontier fallback, and is restart exact.

## Richer v5 search substrate

The fresh v5 genotype contains typed resources, an entry/setup DAG, prioritized
position management/exit regions, recovery, explicit budgets, and a closed
compiler/operator authority.

Searchable grains now include:

- evidence-group create/remove/split/merge;
- membership insertion/removal, normalized positive weights, and thresholds;
- catalog-authorized indicator insert/remove/substitute plus timeframe, lookback,
  TA-period, and numeric-range mutation;
- fresh directional event insert/remove/substitute;
- count, sequence, absence, time-since, condition-duration, cooldown, re-entry,
  UTC-session, and regime-change temporal facts;
- typed alternate-watch, confirmation/rejection, re-arm, and timeout motifs;
- graph add/remove/rewire operations with reachability, resource-closure, budget,
  and native admission checks;
- deterministic same-side crossover at compatible `entry_setup`,
  `management_hub`, and `exit_hub` ports, bound to ordered parents and an exact
  segment map.

Crossover is deliberately recipient-closed: it does not implicitly import or
remap missing donor resources. This is broad typed recombination, not arbitrary
unbounded graph synthesis.

A matched no-market management A/B retained identical economics and exact
split/restart parity while increasing selected management/exit regions from 3 of
6 in the serial cursor to all 6 in the shared-hub topology. One conflict was
resolved by deterministic priority; no concurrent position/runtime semantics were
introduced.

## Live v5 authority and observability

- The supported supervisor opens the base pair source authority, then reopens the
  evolvable authority and recomputes exact run-config, archive-policy,
  behavior-requirement, operator, and capacity bindings at freeze and restart.
- Ordinary incompatible evolvable operations are deterministic proposal
  rejections, not campaign-aborting exceptions.
- Candidate/window tasks bind required transition/action and fuzzy-member
  attribution through the Dashboard worker and controller trust boundary.
- Fresh-v5 capacity admission consumes a deterministic receipt for the actual
  evolvable factory. The receipt hash excludes timing telemetry and binds native
  v2 plus compiled-v3 admission, authority, catalog, compiler, registry, budgets,
  counts, rejection outcomes, and per-side diversity.
- Campaign freeze requires admitted and unique semantic capacity to cover the
  complete frozen construction demand, not merely the initial population width.

Current artifacts cannot honestly prove causal parent/operator contribution
across rotating panels because older funnel/retention records lack exact
per-candidate attribution, decomposed evidence, and external-parent identities.
Fresh v5 therefore writes an immutable `proposal-lineage-unavailable` marker with
explicit reasons and bound source artifacts. It does not fabricate a partial
causal report. The complete lineage materializer is implemented for future runs
whose source contract is satisfied.

## Verification

```text
AutoResearch full Python suite: 2,052 passed
Rust temporal-QD workspace:     all executed tests passed; one existing ignored
Focused independent review:     265 passed
Dashboard temporal/lake gate:   341 passed
Cross-repo task/result roundtrip: passed with tamper rejection
Runtime oracle fixture:         regenerated against Dashboard 1766e272,
                                temporal dirty provenance empty
Evolvable factory capacity:     8,192 previewed
                                8,192 native v2/v3 admitted
                                8,192 unique semantic pairs
```

The authoritative no-market capacity receipt is
`sha256:1a783f103fed96401aa8959a637347808f44475aeb8d069b8bbb119a1913b84f`
under evolvable authority
`sha256:7fbf45787ea91ef707f58c71a04c00fc275d8df0791ed190b1182d553130f164`.
It exceeds the frozen five-generation construction requirement of 8,096 by 96
fully admitted unique semantic pairs.

The first live two-generation checkpoint preflight then exposed one focused
admission defect before candidate construction: the evolvable factory policy
did not carry the collision-collapse tripwire required by the optimized pair
population generator. The policy now binds the existing 512-attempt / 25%
minimum acceptance threshold, focused authority/supervisor/generator tests pass,
and the full 8,192-pair no-market capacity admission above was rerun against the
repaired policy. No candidate evaluation or market-data task occurred in the
failed preflight.

Launch-ready immutable inputs are committed beside this record:

- `TEMPORAL_QD_EVOLVABLE_CAPACITY_ADMISSION_2026-08-09.json` — the semantic
  receipt and detailed diversity counts;
- `TEMPORAL_QD_EVOLVABLE_AUTHORITY_2026-08-09.json` — the closed v5 authority
  with that exact receipt attached, suitable for
  `--evolvable-module-authority-config`.

Independent final review found no unresolved P0/P1 correctness, security,
identity, or restart defect in the combined tree.

## Remaining explicit boundaries

- The authoritative capacity receipt proves the present factory/authority. Any
  compiler, registry, budget, catalog, source-authority, or factory change requires
  a new receipt; this receipt is not a claim about future mutated code.
- No live worker/lake/economic campaign was run in this remediation.
- Full two-generation semantics are covered by hermetic and synthetic integration
  tests, not one real distributed transaction.
- Indexed v5 tail support and complete causal proposal-lineage analytics remain
  deliberately unavailable.
- Detailed realized per-origin reproduction accounting is Python/finalizer-owned;
  Rust validates the frozen schedule and allocation authority.
- Multiple simultaneous positions, partial exits, scale-in, pending orders,
  portfolio graph state, and arbitrary hierarchy remain outside this stage.

The next authorized operation after the capacity receipt and checkpoint commit is
a fresh canonical two-generation learning checkpoint, followed by review. It is
not an automatic large economic campaign.
