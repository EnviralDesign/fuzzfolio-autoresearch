# Stage 5E7-v3 repair-validation harness

`stage5e7-v3-validation` builds small immutable validation panels. It is not a
search controller: it never starts workers, reserves evidence, regenerates lake
windows, or writes under this repository. Every generated artifact is placed in
`<external-output>/stage5e7-v3-validation-<version>/` and is content audited.

The prohibited interval starts at `2024-06-29T00:00:00Z`; reserved evidence is
always disallowed by the manifests.

## Repair panel

The repair builder reads one old Stage5E7 gen4 archive, the matching population,
and its complete result root. It treats the archive only as provenance: archive
lane, Pareto membership, retention reason, and old rank never grant promotion.
It requires a unique deterministic matching for exactly 64 candidates:

- `qd_538`, `qd_390`, `qd_339`, `qd_9db`, and `qd_de455`;
- resolved and unresolved both-positive candidates;
- concentrated positive, high-support negative, high-turnover, sparse/long-hold,
  short-direction, and flat/negative controls;
- four representative source origins, structural families, and descriptor cells.

The optional dossier CSV is deliberately narrow. It must use `candidateId` (or
`candidate_id`) and may supply only resolution evidence. Repair economics always
come from the identity-bound predecessor aggregate: genuine v3 inputs retain
their terminal-adjusted names, while legacy inputs are emitted explicitly as
legacy closed-trade proxy metrics with their supplying aggregate fields. Legacy
proxy values are for coverage stratification only and never promote a candidate
or impersonate v3 economics. Missing required coverage fails closed rather than
silently reducing the panel or treating a legacy rank as a replacement score.

```powershell
stage5e7-v3-validation --output-root D:\fuzzfolio-validation --version 20260801-r1 build repair `
  --old-archive <stage5e7-gen4-archive.json> `
  --old-population <stage5e7-gen4-population.json> `
  --old-results <stage5e7-gen4-corrected-results-root> `
  --candidate-dossiers <optional-candidate_dossiers.csv> --seed 2026080101
```

## Causal operator panel

The operator panel deterministically picks twelve tagged parents. It writes an
unchanged control for each and schedules single, depth-one structural siblings.
For every enabled operator that has an applicable parent, it reserves one
opportunity before filling the remaining capacity. The finite panel is capped at
64 candidates; inapplicable or cap-excluded pairs are recorded as suppressed
with a reason. No crossover or multi-operator child is possible.

The builder calls only the existing static profile validator given in the command
file so an admitted child has the normal canonical program and application
identities. It does not contact an evaluator, worker, gateway, or search
controller. The corresponding `parent-baselines.json` must later be evaluated
under the same corrected contract as the no-op controls; the analyzer rejects
any non-identical no-op result before calculating paired effects and uses a
family-wise adjusted interval, defaulting to `inconclusive`.

```powershell
stage5e7-v3-validation --output-root D:\fuzzfolio-validation --version 20260801-r1 build operator `
  --reference-root D:\fuzzfolio-validation\stage5e7-v3-validation-20260801-r1\repair `
  --catalog <canonical-indicator-catalog.json> `
  --validator-command-file <static-validator-command.json> --seed 2026080101

# Later, after the finite corrected evaluations are present:
stage5e7-v3-validation --output-root D:\fuzzfolio-validation --version 20260801-r1 analyze operator `
  --operator-root D:\fuzzfolio-validation\stage5e7-v3-validation-20260801-r1\operator `
  --corrected-results <operator-panel-corrected-results-root> `
  --parent-corrected-results <parent-baselines-corrected-results-root> `
  --operator-panel-bridge <operator-panel-bridge-root> `
  --parent-panel-bridge <parent-baselines-bridge-root>
```

## Policy A/B and generator-v3 canary

The A/B builder writes two policy configuration roots against one tagged panel.
A is the explicit v2-like corrected-evidence control; B is the v3 robust policy.
It writes one exact repaired 64-candidate population, not two policy-specific
populations. Neither policy launches a search, breeds candidates, or describes
generational outcomes: analysis reduces that same population against one exact
corrected Stage5E7-v3 terminal-adjusted result set.

Policy A is a finite-only v2-like archive reducer: per descriptor cell it uses
Pareto fronts over risk-adjusted terminal-adjusted return
(`totalTerminalAdjustedConservativeNetR / (1 + maxWindowDrawdownR)`), drawdown,
raw (uncapped) trade support, and structural complexity. Its support gate is
8 total trades and 2 per window, capacity is 8, and it has neither an
observational nor a negative-novelty lane.

Policy B calls the canonical v3 robust archive reducer unchanged: worst-window
terminal-adjusted R, drawdown, and complexity; support gate 8 total / 4 per
window with support capped at 20 only as its prescribed tie-break; capacity 4;
quality, observational, and at-most-one-negative-novelty lanes.

The analyzer records each frozen policy identity and causal composition changes:
retained count and cells, robust-positive count/share, negative retained share,
raw support, drawdown, diversity, and the exact retained-set overlap. It rejects
legacy/non-v3 results, a candidate-ID match whose window `programSha256` does
not exactly match the panel candidate, or any result root that does not cover
precisely the 64 shared candidates. Each analyzer is also given the immutable
bridge that authorized the panel: it verifies the bridge manifest,
source-population identity, evaluation identity, authority, and task matrix;
then requires the result root's `authority.json` and `task-manifest.json` to be
byte-for-byte equivalent in canonical JSON meaning. Every exact task must have
one result bound to its authority ID, task/job ID, candidate, evidence-plan ID,
lake-window identity, stream identity, and ordered development calendar. At
least two development windows are required for Stage5E7-v3 analysis; a partial,
extra, substituted-authority, or wrong-calendar result root is rejected.

```powershell
stage5e7-v3-validation --output-root D:\fuzzfolio-validation --version 20260801-r1 build policy-ab `
  --reference-root D:\fuzzfolio-validation\stage5e7-v3-validation-20260801-r1\repair --seed 2026080101

stage5e7-v3-validation --output-root D:\fuzzfolio-validation --version 20260801-r1 analyze policy-ab `
  --policy-root D:\fuzzfolio-validation\stage5e7-v3-validation-20260801-r1\policy-ab `
  --corrected-results <shared-corrected-v3-results-root> `
  --panel-bridge <repair-reference-bridge-root>

stage5e7-v3-validation --output-root D:\fuzzfolio-validation --version 20260801-r1 build canary-composition `
  --canary-root <already-built-generator-v3-reachability-canary-root>
```

The final command audits, but does not reimplement, the repository-only
`temporal-search-generator-v3-canary`. It records the later runtime gate fields
for fired/activation evidence.

## Audit and plan-only surfaces

```powershell
stage5e7-v3-validation --output-root D:\fuzzfolio-validation --version 20260801-r1 audit
stage5e7-v3-validation --output-root D:\fuzzfolio-validation --version 20260801-r1 plan freeze
stage5e7-v3-validation --output-root D:\fuzzfolio-validation --version 20260801-r1 plan temporal-search
stage5e7-v3-validation --output-root D:\fuzzfolio-validation --version 20260801-r1 plan qd-supervisor
```

The `plan` commands emit a command-shaped immutable plan only. They do not invoke
the existing freeze, temporal-search, or supervisor entry points.
