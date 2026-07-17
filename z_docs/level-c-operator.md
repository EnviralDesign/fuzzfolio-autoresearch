# Level C Operator

The Level C operator is a thin orchestration layer over the existing formal
Atlas, PlayHand, frozen-cohort, nested-evidence, and portfolio APIs. It does not
implement a second research or replay engine.

## Bootstrap

Bootstrap runs once against an empty configured `runs` root. It links the
already completed manual archive by hashing only three named files. It does not
inventory or hash the archived corpus recursively.

```powershell
uv run level-c-bootstrap `
  --active-runs-root C:\repos\fuzzfolio-autoresearch\runs `
  --archive-root <archived-runs-root> `
  --archived-attempt-catalog <archived-attempt-catalog> `
  --archived-attempt-catalog-sha256 sha256:<hash> `
  --legacy-controls <legacy-controls-json> `
  --legacy-controls-sha256 sha256:<hash> `
  --completed-nested-report <completed-nested-report-json> `
  --completed-nested-report-sha256 sha256:<hash> `
  --archive-id <archive-id> `
  --new-generation-id <generation-id> `
  --lake-semantic-sha256 sha256:<hash> `
  --source-snapshot-sha256 sha256:<hash> `
  --universe-id <universe-id> `
  --universe-manifest-sha256 sha256:<hash> `
  --worker-contract-id <contract-id> `
  --worker-contract-sha256 sha256:<hash> `
  --worker-image <immutable-image-reference> `
  --global-seed <integer> `
  --json
```

The archived nested report mechanically supplies all A/B/C/D dates. Bootstrap
accepts no cutoff dates. Exact existing bootstrap files are verified and
reused; byte drift, semantic drift, unknown files, or non-prefix partial state
fails closed.

## Run A Cutoff

```powershell
uv run level-c-run-cutoff `
  --active-runs-root C:\repos\fuzzfolio-autoresearch\runs `
  --cutoff A `
  --resume `
  --gateway-url http://127.0.0.1:8799 `
  --gateway-token <token> `
  --atlas-active-probes <capacity> `
  --playhand-active-runs <capacity> `
  --nested-max-workers <capacity> `
  --trading-dashboard-root C:\repos\Trading-Dashboard `
  --json
```

Replace `A` with `B`, `C`, or `D`. The command loads the authoritative plan and
has no independent research-semantic flags. The remaining flags control only
transport, capacity, timeouts inherited from the frozen executor plan, output,
or resume behavior.

Stages execute in this order:

1. Atlas
2. Finite PlayHand
3. Frozen cohort
4. Training materialization/evidence
5. Frozen execution cells
6. Train-only frozen portfolio
7. Selected-only outer evaluation
8. Final report

Each stage has a create-only, content-addressed receipt. `--resume` verifies all
completed receipts and their artifacts, then starts the first incomplete stage.
A typed zero-candidate or other supported non-promotable outcome terminates
cleanly without outer work. Infrastructure, contract, receipt, accounting, or
membership failures block.

Cutoffs C and D require the create-only development policy produced after both
A and B complete. That policy binds only A/B final receipts; C/D outcomes cannot
feed back into policy or selection.

## Audit

```powershell
uv run level-c-audit `
  --active-runs-root C:\repos\fuzzfolio-autoresearch\runs `
  --cutoff A `
  --json
```

Omit `--cutoff` to audit all four cutoffs. Audit is read-only. It revalidates
the archive linkage, generation, protocol, authority, authoritative plans,
runtime policy and profile-source locks, stage receipt prefix and artifacts,
cohort accounting, selected-only outer membership, and no-validation-feedback
development policy.

## Process Manager

No existing process-manager entry must change during code deployment. A later
operator update should add three manual, non-auto-restarting entries:

- `Level C Bootstrap` for the one-time command with finalized identities.
- `Level C Run Cutoff` with one entry or editable cutoff argument.
- `Level C Audit` for read-only verification.

The Lab Gateway remains a separate prerequisite. The runner is finite and
resumable; process-manager auto-restart should remain disabled so restart is an
explicit `--resume` decision.

## Required Operational Inputs

Before the real bootstrap, the operator must supply:

- active and archived runs roots;
- exact archived attempt-catalog path and SHA-256;
- exact legacy-controls path and SHA-256;
- exact completed nested-report path and SHA-256;
- archive and new-generation identifiers;
- lake semantic, source snapshot, universe, worker contract, and immutable
  worker image identities;
- one global seed.

Engine, scoring, cost-policy, and profile-model source locks are derived live
from named runtime/code surfaces and frozen into the protocol and execution
plans. A later runtime mismatch fails closed.
