# V37 disposition ledger and call-site audit

## Status

This is a read-only join of V37's frozen current-panel, proposal-funnel,
prefinalizer, cumulative, and parent-archive receipts. It does not replay
market data, change a historical archive, construct candidates, or run a
counterfactual.

The normalized state ledger has exactly 5,126 rows. Its key is an exact
candidate-generation evaluation state, not candidate ID alone:

- 5,120 `proposal_current_panel` states: the five 1,024-member proposal
  panels; and
- six `retained_parent_current_panel` states: three separately retained G2
  incumbent re-evaluation receipts and three G3 receipts.

Each state binds a canonical hash of its exact `evaluated-members` record and
a canonical hash of its retained panel-bundle coverage. Those are ledger
descriptors, not claimed native self-hashes. The separate
`candidate-lineage-rollup.jsonl` groups those immutable state keys without
collapsing repeated evaluation or coverage.

| Generation | Evaluation states | Proposal current-panel states | Retained-parent re-evaluation states |
| --- | ---: | ---: | ---: |
| G1 | 1,024 | 1,024 | 0 |
| G2 | 1,027 | 1,024 | 3 |
| G3 | 1,027 | 1,024 | 3 |
| G4 | 1,024 | 1,024 | 0 |
| G5 | 1,024 | 1,024 | 0 |
| **Total** | **5,126** | **5,120** | **6** |

## Observed disposition

| Generation | Current candidates | Prefinalizer newcomer-cap | Native unsupported lane | Parent archive admitted |
| --- | ---: | ---: | ---: | ---: |
| G1 | 1,024 | 896 | 125 | 3 |
| G2 | 1,024 | 896 | 125 | 3 |
| G3 | 1,024 | 896 | 128 | 0 |
| G4 | 1,024 | 896 | 128 | 0 |
| G5 | 1,024 | 896 | 128 | 0 |
| **Total** | **5,120** | **4,480** | **634** | **6** |

All 5,120 proposal states have an accepted proposal-funnel receipt, static
reachability, and native validation before current-panel evaluation. Retained
parent states are not proposals and consequently have no new-proposal funnel
row; their separate fast-prefinalizer receipt is preserved instead. The ledger
contains no `parent_archive_cell_capacity_excluded` state: the six archive
admissions did not displace another native quality/frontier candidate through
per-cell capacity in this history.

The `unsupported` terminal reason is deliberately exact-but-narrow:
the native cumulative archive emits the final lane and the raw cumulative
windows, but not a per-row first-failed predicate for support, direction, or
economics. The ledger calls this
`cumulative_native_unsupported_reason_unavailable`; it does not reconstruct a
more specific reason from a different implementation.

## Focused cohort

This section intentionally considers only the 5,120 proposal-current-panel
states. The six incumbent re-evaluations are excluded, so the focused results
are not inflated by repeated candidate states. The label sets overlap; the
counts below are separate predicates rather than a summed population.

| Cohort | Count | Parent archive | Native unsupported | Prefinalizer cap |
| --- | ---: | ---: | ---: | ---: |
| Finite-support and after-cost positive | 186 | 5 | 74 | 107 |
| Positive in every retained current-panel window | 8 | 0 | 6 | 2 |
| Positive structural offspring with finite support | 5 | 0 | 3 | 2 |

The named thin-habitat cases all reached native reduction but ended in the
unsupported lane:

- `qd_599aa34a2aef63c49d3b0601e5cc`: G1, +8.826R, 21 trades.
- `qd_a8338e2e3bc4113cc307208723df`: G1, +6.308R, 46 trades.
- `qd_4987663ed6cf86fa49ae66c4517e`: G4, +2.394R, 36 trades.
- `qd_f0075a48ced9d13932aadca62adb`: G3, +10.198R, 59 trades, but one
  retained window has two trades and it fails the current finite-support gate.

This is not evidence that those four candidates were archive-ready breeders.
It does establish that they were not silently lost at the 128-newcomer cap.
The retained output does not reveal their first cumulative failure predicate.

## Evidence-state and terminal-stage labels

Every ledger state records the exact panel bundles retained by that generation,
the required panel IDs at that point, and one of:

- `exact_retained_required_evidence`; or
- `would_require_additional_backfill_evaluation`.

The latter is the required label for the 896 cap-excluded proposal states in
each of G2–G5. Their exact current-panel bundle exists, but their required
prior-panel coverage was never retained, so this audit does not call them
archive-ready or economically rejected. G1 needs only its current first panel;
selected provisional states and the G2/G3 retained-parent states have their
then-required bundle coverage recorded.

Terminal stages are mutually exclusive. A row has
`firstTerminalStage`, `allReasonCodesAtThatStage`, and
`secondaryDiagnosticFailures` in addition to its historical terminal label.
For the native unsupported lane, the only exact stage reason is
`native_unsupported_lane`; `gate_by_gate_reason_not_emitted` is a secondary
evidence limitation, not an inferred failed predicate.

## Cross-generation control continuity

Before counterfactual variants, the generated
`cross-generation-control-continuity.json` checks each frozen handoff:

| Later source | Required identity match | Parent projection match |
| --- | --- | --- |
| G2 prior state ← G1 output | exact cumulative object and archive SHA | archive SHA, member count, cells, policy projection, and self-hash |
| G3 prior state ← G2 output | exact cumulative object and archive SHA | archive SHA, member count, cells, policy projection, and self-hash |
| G4 prior state ← G3 output | exact cumulative object and archive SHA | archive SHA, member count, cells, policy projection, and self-hash |
| G5 prior state ← G4 output | exact cumulative object and archive SHA | archive SHA, member count, cells, policy projection, and self-hash |

All four checks pass against the same historical outputs that the native V0
control replay reproduced. This is control continuity only: sequential
variants must replace their own preceding archive or memory state after first
divergence, not reuse these historical prior-state objects.

## Call-site answers

### Current panel and prefinalizer

`make_cohort` assigns proposal and retained-parent roles separately in
`rust/temporal-qd/crates/qd-rotating-prefinalizer/src/v5.rs:1231`.
`make_provisional` at `:1582` then:

- passes every retained parent that has a current-panel receipt straight into
  the provisional set;
- selects up to 128 newcomers by a cell-balanced ordering;
- does not preserve an incumbent that lacks a fresh current-panel receipt.

Therefore incumbents and newcomers do **not** share the same cap. G2 and G3
have three mandatory incumbent re-evaluations plus 128 selected newcomers.
G4/G5 have no incumbent to reevaluate, so all 1,024 candidates are newcomers.
The ordering inputs are the current-panel screen at `:1500` and its stable
ordering at `:1306`, not a growing cumulative archive score.

### Cumulative evidence and archive replacement

The fast-ephemeral finalizer opens the bound source and calls the exact
cumulative and parent projections in
`rust/temporal-qd/crates/qd-generation-finalizer/src/lib.rs:260`.
`build_cumulative_archive` at `:1505` opens only provisional candidates and
the required rotated panels. A current candidate omitted by the prefinalizer
has no full cumulative projection in that generation.

`classify` at `:1649` applies support, direction, cumulative economics, then
bounded Pareto/frontier selection (`:1747` and `:1806`). Unsupported rows are
preserved only as raw base rows with an `unsupported` lane; the detailed gate
trace is not published. `build_parent_archive` at `:1942` groups only native
quality/frontier IDs and projects a replacement archive. It does not retain an
incumbent merely because no challenger exists.

That explains G3 without a policy reconstruction: all three G2 parents were
freshly re-evaluated, made mandatory provisional members, and then no native
quality/frontier member existed. The replacement parent archive is therefore
empty. G4/G5 consequently have no parent-memory input.

### Descriptor, capacity, and parent plumbing

The prefinalizer's cell ID is taken from the fresh current-panel row, so an
incumbent's descriptor is recomputed rather than carried as a historical cell.
The parent archive's cell capacity applies only after native quality/frontier
selection; no retained V37 candidate was excluded at that capacity seam.

The source cohort carries `retainedParentEvaluationCandidateIds`; the finalizer
validates that partition as distinct from new proposals. The final parent
schedule is a supported-parent, with-replacement schedule. The completed
proposal histories also bind accepted offspring back to parent identity:
the G1 archive parents have 215 recorded parent references across 205 accepted
G2 offspring (the excess is multi-parent/crossover linkage), while G2 parents
have 512 recorded parent references across 512 accepted G3 offspring.

### Explicitly unavailable

The V37 fast-ephemeral retained outputs do not publish per-candidate
resolved-execution-deduplication provenance or a per-ID Pareto eviction trace.
The audit consequently does not claim that a candidate was deduplicated,
Pareto-evicted, or lost to a specific cumulative gate unless that fact is in a
retained native receipt.

## Implication for variants

The ledger supports studying two distinct seams, which must remain separate:

1. the prefinalizer's 128-newcomer reduction before complete cumulative
   evidence is available; and
2. the replace-mode cumulative archive, in which evaluated incumbents lose
   parent rights when the fresh growing conjunction yields no quality/frontier
   member.

It does not justify lowering support, treating the 186 positive candidates as
breeders, or inventing a missing per-candidate failure reason.
