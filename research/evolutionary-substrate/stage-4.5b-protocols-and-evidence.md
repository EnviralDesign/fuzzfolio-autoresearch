# Stage 4.5B — protocol, geometry, and evidence boundary

## What this stage is and is not

This stage adds an exact Git-object authority map, a compact retained
V37/V38 existing-construction fixture, source-derived substrate detail, and
no-market construction/admission evidence. It does not run bars, replay,
evaluation, workers, a gateway, Vast, a generation continuation, or an
archive update. It does not alter FuzzFolio or production policy.

The canonical inputs are recorded by path and content hash in
[`existing-construction-fixture-v2.json`](existing-construction-fixture-v2.json).
The fixture validates the retained frozen authority and proposal manifest
twice with the existing V5 validators, and current source reconstructs the
same native operator and pair-policy closures. It is a reference to immutable
local inputs, not a copy of raw proposal, result, or market data.

## No-market pilot arms

| Arm | Existing path | Authority state | Limit | Current state |
| --- | --- | --- | ---: | --- |
| Production prior | `build_pair_generation_config` → `run_native_v5_proposal_construction` | retained V37 parent archive/identity ledger + retained V38 pair/evolvable authority; source-generated 1/5 immigrant floor | 64 accepted targets, 4,000 attempts | executed separately twice; receipt-derived summaries are ignored local artifacts |
| Coverage-balanced | retained V38 `operatorFamilyMatrix` config | exact historical 100% offspring allocation | 800 target in retained artifact | not executable in current source: current V5 validation rejects its frozen `0/1` immigrant ratio when no explicit confidence freeze exists |

The coverage arm is an evidence-bearing incompatibility, not a reason to add a
new selector or relax the current validation. The precise failing predicate is
the V5 full-generation-config minimum immigrant rational check. The retained
manifest still validates as a self-authenticating historical artifact; it does
not make its historic construction rule a currently admissible one.

## One-step neighbourhood geometry

Every retained/current construction observation is classified from
authority-bound receipts only. It is not assigned economic meaning.

| Classification | Deterministic rule |
| --- | --- |
| `no_effect` | V5 disposition is `no_op` or the child identity equals the parent identity. |
| `static_dead_end` | Deterministic rejection or no admitted plan for the fixed parent/authority. |
| `small_local_edit` | Exactly one accepted named operator trace whose semantic delta names one functional region. |
| `coherent_module_edit` | Accepted trace changes multiple coordinated fields inside a single entry, management, exit, recovery, or resource region. |
| `large_blast_radius` | Accepted trace names more than one functional region or a two-parent crossover closure. |
| `behavior_unknown` | A child is constructed/admitted but no runtime/market behavior is authorized or inferred. |
| `invalid` | Authority, parent binding, content identity, or compiler receipt is rejected as stale/malformed. |

The minimum geometry projection is: operator family, source suboperation,
parent role/side, disposition/reason, canonical parent and child identities,
semantic trace hash, compiler/admission receipt hash, resource/state/action
delta counts, and the classification above. It deliberately excludes score,
trade, and market fields.

## Twelve integrated reference-organism probes

The language classes below are source-representable. A standalone arbitrary
organism compile is intentionally **not** claimed for any row: the accepted
constructor takes sealed parent genomes plus content-bound plans, not a public
fragment-to-full-pair materializer. Creating such a materializer would be a
replacement construction authority, which is out of scope. The exact missing
primitive is therefore the same in each row: `a sealed existing parent genome
and an authority-bound plan selecting the named grammar/topology site`.

| ID | Required integrated shape | Existing grammar/topology route | Status |
| --- | --- | --- | --- |
| R1 | level arm → level entry | `arm_level` + `enter_on_level` | source-representable; no standalone materializer |
| R2 | event arm → event entry | `arm_fresh_event` + `enter_on_event` | source-representable; no standalone materializer |
| R3 | delay/streak gate → entry | `gate_delay`, `gate_streak` | source-representable; no standalone materializer |
| R4 | predicate-edge confirmation | `gate_predicate_edge` | source-representable; falling-edge direction not a grammar choice |
| R5 | management request | `move_break_even` or `tighten_stop` | source-representable; no standalone materializer |
| R6 | time exit | `exit_on_age` | source-representable; no standalone materializer |
| R7 | event exit | `exit_on_signal` | source-representable; no standalone materializer |
| R8 | cooldown/recovery | `cooldown` | source-representable; explicit abstention/fallback is absent |
| R9 | entry-branch addition | `insert_entry_branch` topology operation | source-representable; no standalone materializer |
| R10 | confirmation/rejection/rearm | `insert_confirmation_rejection`, `insert_timeout_rearm` | source-representable; no persistent latch primitive |
| R11 | management/exit reshape | insert/remove/rewire topology operations | source-representable; no standalone materializer |
| R12 | catalog/timeframe variation | V5 resource operator under sealed side policy | source-representable; requires an eligible sealed parent site |

## Ground Zero protocol (design only)

1. **J1 — authority preflight.** Re-run exact blob/fixture validation and
   stop if a source, input, binary, parent, or policy identity drifts.
2. **J2 — no-market construction.** Run the existing constructor in isolated
   output roots with a predeclared cap, then validate receipt-last outputs
   twice. Stop on an unsealed result, drift, or any evaluation/market request.
3. **J3 — offline structural analysis.** Reduce only receipt and graph/plan
   metadata into the geometry projection. Stop if a requested field requires
   raw market/result data.
4. **J4 — separate authorization gate.** A future evaluation protocol must
   declare data, cost, workers, selection policy, archive handling, and
   rollback before any market or remote resource is touched.

No J4 action is authorized by this document.

## Thin single-Vast design (design only)

If and only if J4 is separately authorized, use one bounded Vast instance with
one immutable image digest, one explicitly pinned source/binary/fixture set,
one per-run output root, no shared writable archive, quota/cost cap, upload
only of compact checksummed receipts, and explicit teardown on both success and
failure. The instance must not receive credentials, production authority, or
an instruction to mutate local/run archives. This stage neither rents nor
contacts Vast.

## Fable / reviewer handoff questions

1. Is the explicit current-source rejection of the historical coverage matrix
   sufficient evidence to leave that arm unexecuted, or should a separately
   authorized historical-source execution be evaluated?
2. Does the fixture manifest include enough input identity to reproduce an
   existing evolved-construction invocation without copying raw payloads?
3. Are any geometry classifications claiming more than the receipt/identity
   evidence supports?
4. Is a future reference-organism materializer desirable enough to authorize
   as a distinct milestone, rather than smuggling it into this audit?
