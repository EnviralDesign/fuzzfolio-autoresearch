# Phase 3 Research and Operations Master Plan

Date: 2026-07-21

Status: Phase 3 authority/runtime implementation complete; raw Phase 2 storage
cleanup is prepared as an explicit operator handoff and remains unexecuted.

## Purpose

This document is the durable handoff from the five-year lake rebuild, legacy
fixed-cell retrospective, and four-cutoff Atlas study into the next PlayHand
generation. It replaces chat history as the operating reference. The two
forensic reports remain immutable evidence; this document reconciles them and
defines the next engineering, operational, and storage work.

The goal is not to preserve every historical artifact. The goal is to preserve
enough authoritative evidence to reproduce the decisions, avoid repeating the
old failure modes, and start a clean research generation with controls the
runtime actually enforces.

## Evidence Ledger

Primary reports:

- First Atlas forensic report:
  `runs/derived/phase2-atlas-forensics/20260721T184206Z-a-d-rich-priors/PHASE-2-ATLAS-FORENSIC-COMPARISON-REPORT.md`
  (`sha256:f0bd617962c0799eb72b39517a75ae7258441345e657fd0b658d57749b2b5952`).
- Independent second analysis:
  `runs/derived/phase2-atlas-forensics/20260721T2010Z-second-analysis-a-d-rich-priors/SECOND-ANALYSIS-ATLAS-FORENSIC-REVIEW-20260721.md`
  (`sha256:257a46c29a840b3aba359c76f4345776b465a01779e36d237ab11aa4f24d2d26`).
- Phase 1 fixed-cell legacy comparison:
  `C:/repos/Trading-Dashboard/.tmp/live-vs-backtest-audit-workspace/PHASE-1-FIXED-CELL-COMPARISON-FINDINGS-20260719.md`
  (`sha256:9076b236ee5d443ecc154111ddaf803810f89a91b074f172d38a03bed733114a`).

Authoritative Phase 2 Atlas roots:

- A: `runs/derived/atlas-runs/atlas-lc-a-e2ff10eedfa1084b`
- B: `runs/derived/atlas-runs/atlas-lc-b-ea7140af612cf191`
- C: `runs/derived/atlas-runs/atlas-lc-c-5c251563f9035fe9`
- D: `runs/derived/atlas-runs/atlas-lc-d-8af1d45d58d5bb40`

Shared authority:

- Universe: `fuzzfolio-development-darwinex-zero`
- Universe manifest:
  `sha256:72f1d550bf56163059afd40099c4c5b8e130f9fa0430d82bc54fb13cfcaeff4e`
- Lake:
  `sha256:d66caba7e3b7c04bd93db15a296c95f2940bd57b3c436b0497aac9858b972a90`
- Source snapshot:
  `sha256:a071efc264008072ca2e8ab78fb0619c69a9e1ff270530d842c96d2beffd74c2`
- Worker contract:
  `sha256:0f2e7284beedf34afc9463b242f562591b5840104b85629316f3fc715ec5fec3`
- Worker image: `lucasmorgan/fuzzfolio-replay-worker:vast-sha-656f43da9df0`
- Generation: `level-c-v3-phase2-rich-priors`
- Reserved untouched tail: `[2026-01-14, 2026-07-14)`

The four runs are separately executed but not statistically independent.
Adjacent 36-month windows overlap by about 30 months; A/D overlap by about 18
months. A/B are development cutoffs. C/D are validation cutoffs and must not be
used to choose individual Phase 3 candidates.

## Consolidated Research Conclusion

### What Atlas knows reliably

Atlas has real information at coarse resolution:

- The 88-indicator catalog and role taxonomy are stable.
- Signal density and relative firing frequency are nearly invariant across
  cutoffs.
- Indicator-level forward response is highly stable, with mean-reversion
  archetypes dominating the recurring top ranks.
- The pre-replay structural candidate surface is highly stable.
- Eleven unordered pair/timeframe families are positive in all four runs;
  eight are strong in at least three and five are strong in all four.
- A/B share 35 final-prior structural families, and 27 remain present in both
  C and D.

The second analysis compared non-adjacent cutoffs and found that replay
correlation does not simply decay with less shared history. It also found some
non-adjacent top-N recurrence. This means the repeated structural surface is
not adequately explained as a trivial overlap artifact.

The second analysis also calculated a large enrichment over a naive
independence baseline for all-four-positive families. The direction is useful,
but the literal p-values must not be treated as calibrated significance:
families are correlated, windows overlap, and the candidate surface is not a
collection of independent Bernoulli trials.

### What Atlas does not know reliably

Fine-grained empirical certainty is unstable:

- Exact replay pair rankings have low adjacent-cutoff correlation.
- Adjacent exact top-10 and top-25 sets do not overlap.
- Exact instrument/timeframe/direction/horizon response cells are unstable.
- Final empirical weights are highly cutoff-dependent and sometimes collapse
  around one or two favorable survivors.
- Negative-prior lists are local to a cutoff and do not support permanent bans.
- The old retained-36m prior families, rankings, weights, and selected
  memberships do not reproduce under current semantics.

This is not evidence that the indicator library or broad Atlas process is
worthless. It is evidence that the system previously assigned too much
certainty after coarse discovery.

### The biggest correction from the second analysis

The validation and scrutiny funnel is too narrow to support strong statements
about repeated survival. Each cutoff validates roughly 16 candidates and
scrutinizes only 2-8, selected independently at that cutoff. Most candidates
that look good in one cutoff were never tested in the other cutoffs.

Therefore:

- `never tested again` must not be reported as `tested and failed`;
- the absence of three- or four-cutoff scrutiny survivors is not decisive
  evidence that no survivor exists;
- future Atlas protocols need a preregistered repeated panel evaluated at every
  cutoff, in addition to the cutoff-local promotion funnel.

The highest-value future protocol change is to freeze a panel from development
cutoffs, add a random control panel, and run both through the same 12-month and
36-month tests at every validation cutoff. That makes survival, failure, and
missing coverage distinguishable.

### What the legacy comparison establishes

The old strategy corpus is not a seed source for the new generation. The
current-contract Phase 1 comparison is directionally severe: 450 strategies
completed, with 204 positive, 136 negative, 110 flat, and an aggregate
unweighted path sum of `-15,911.19R`. Older unseen history was worse.

That does not prove every old strategy was always bad. The experiment cannot
fully separate original selection bias, alpha decay, and semantic drift. It
does establish that old maturity labels, old cells, old weights, and old
portfolio memberships are not current authority.

The old-world failure should be remembered in a compact decision record, not
by keeping every historical file forever.

## Corrected Overfit Model

The main overfit seam is:

1. A useful structural prior nominates a broad neighborhood.
2. A short three-month discovery probe gives noisy exact rankings.
3. A narrow, cutoff-local promotion funnel validates only a small selected
   subset.
4. Sparse or favorable survivors receive concentrated weights and authority.
5. PlayHand and portfolio selection further optimize on closely related train
   evidence.
6. The historical human practice of republishing a newly optimized portfolio
   every week amplified recency selection.

There is no automatic weekly replacement loop in current optimizer code. The
weekly behavior was operational, not an internal algorithm. Current code risk
is train-window alignment plus repeated selection and portfolio
co-optimization on related evidence. Documentation and process controls should
say this precisely.

## Phase 3 Decision

Verdict: `ready_with_constraints`, after the readiness work below.

Do not launch the long-running campaign merely because Atlas completed. The
research result supports a fresh PlayHand generation, but the runtime and
operator surface must first be made honest about what it enforces.

### Policy authority

- Build policy from A/B only.
- Record C/D only as aggregate validation evidence.
- Do not select the five or seven best A/B families after viewing their C/D
  identities.
- Keep the eight all-four families and the B/C scrutiny repeat as
  comparison-only hypotheses for a future preregistered test.
- Never use the reserved tail for discovery, tuning, or portfolio selection.

### Sampling policy

The intended target is:

- 60% guided
- 25% uncertain
- 15% wild

The current runtime does not enforce three distinct deal-time lanes. It
enforces a guided fraction and sends the remainder through one role-balanced
exploration path. Until that is changed, the honest description is `60%
guided / 40% exploration`, not 60/25/15.

Formal Lab currently goes further and forces the guided fraction to 100%.
Launching the existing formal path unchanged would therefore defeat the
intended exploration policy entirely. That must be changed or explicitly
replaced with a policy-honest campaign mode before the continuous run.

Two acceptable choices exist:

1. Implement distinct uncertain and wild runtime lanes with auditable
   accounting before the campaign.
2. Explicitly adopt 60/40 for this generation and defer lane separation.

The recommended choice is the first if the implementation remains bounded.
If it expands into a large scheduler rewrite, use honest 60/40 and preserve
momentum.

### Diversity and negative evidence

Desired targets:

- maximum one timeframe share: 60%
- maximum one indicator share: 15%
- maximum one unordered pair-family share: 5%
- maximum one instrument share: 10%
- role balance preserved
- meaningful unconstrained exploration retained

Current runtime does not enforce all of these campaign-level caps. At minimum,
Phase 3 must emit live/post-hoc accounting and stop or warn on material drift.
Hard deal-time quotas are preferred only if they can be implemented without a
large scheduling redesign.

Negative-prior expiry is currently metadata rather than an enforced lifecycle.
Either consume the expiry during seed-plan rebuild/runtime filtering or remove
the claim that negatives expire automatically. Negative evidence must remain
local and temporary.

The code audit identified the exact boundary:

- `recipe_priors.py` writes guided/uncertain/wild metadata;
- `play_hand.py` consumes only `guided_prior_fraction` and has a binary
  guided/fallback dealer;
- `play_hand_lab.py` persists crash-safe lane indices, but those indices are
  not sampling-policy lanes;
- family caps are metadata/build-time weighting controls, not campaign quotas;
- `expires_after_atlas_runs` has no runtime consumer;
- Level C binds cutoff, policy/source locks, seed-plan hash, and outer geometry,
  while casual PlayHand has no equivalent reserved-tail authority.

### New Atlas protocol requirement

For the next Atlas generation, add a preregistered repeated scrutiny panel:

- select the panel from development evidence only;
- freeze orientation, timeframe, and membership before validation;
- include a random control sample;
- evaluate the same panel at every cutoff;
- record `passed`, `failed`, `nonviable`, and `not tested` distinctly;
- add a small discovery probe-noise test using perturbed windows;
- keep the final reserved tail outside this entire process.

This is a future Atlas improvement, not a prerequisite for starting the next
bounded PlayHand generation. It is a prerequisite before claiming broad
36-month maturity from a later prior.

## Readiness Work Before Continuous PlayHand

### Gate 1: repository and authority hygiene

- Resolve the current dirty working tree deliberately; do not discard unknown
  user changes.
- Resolve or document `.generation-cutover.lock`.
- Create one immutable Phase 3 authority/seed-plan artifact from A/B-only
  inputs.
- Pin lake, universe, source snapshot, engine/scoring/cost/profile locks,
  worker image, and worker contract.
- Record the operational interpretation: true 60/25/15 or honest 60/40.

### Gate 2: bounded PlayHand policy implementation

- Close or explicitly defer the uncertain/wild runtime-lane gap.
- Make diversity accounting visible and auditable.
- Align the family cap with the chosen policy.
- Make negative-prior lifecycle truthful.
- Add focused tests proving that C/D identities and the reserved tail cannot
  feed candidate selection.

### Gate 3: Process Manager normalization

The live and configured process surfaces must be audited before use. The target
operator experience is:

1. Start `Lab Gateway`.
2. Start one `Phase 3 PlayHand Coordinator` bound to the immutable authority
   plan.
3. Observe readiness and queued work.
4. Launch Vast separately through CLI lifecycle control.
5. Stop the coordinator without losing durable campaign state.
6. Resume the exact campaign explicitly.

The final `scripts/processes.json` should contain only current, tested commands
with clear names. Historical one-off A-D entries and obsolete experiments
should be removed or moved to documentation rather than left as operational
buttons.

Read-only audit snapshot:

- Procman is healthy with 31 entries: Lab Gateway running, 30 stopped.
- All 24 unique configured CLI command names remain registered.
- The current Lab Gateway command parses and is suitable for remote Vast
  workers on `0.0.0.0:8799`.
- There is no current entry for a policy-honest Phase 3 coordinator or for
  building the required Phase 2 outcome priors.
- Twenty-two entries rely on `uv run` because their direct virtual-environment
  wrappers are absent. The running gateway currently locks its executable,
  preventing `uv` from reconciling the environment. Normalize the environment
  at a controlled gateway stop before testing other entries.

Procman disposition:

| Entry/category | Decision | Reason |
| --- | --- | --- |
| Lab Gateway | Keep | Required by remote replay workers; command is current. |
| July Level C Bootstrap and A-D cutoff entries | Remove | Hard-pinned completed one-off generation. |
| Level C Audit | Move to advanced/manual or remove | Useful for historical evidence, not routine Phase 3 operation. |
| AutoResearch Dashboard | Keep | Current independent utility. |
| Latest Portfolio Report | Keep | Current independent reporting utility. |
| Old Darwin Master portfolio command | Investigate then likely retire | Old suite authority is not valid for the new generation. |
| Resume Latest portfolio command | Update | Missing explicit gateway and Trading Dashboard bindings. |
| Corpus Catchup/Rebuild | Separate from gateway group | Rebuild must not start with normal research controls. |
| Cleanup incomplete PlayHand runs | Remove or make dry-run by default | Current entry is destructively unsafe. |
| Atlas cleanup dry run | Keep | Safe preview operation. |
| Atlas cleanup apply | Remove | Destructive action should require an explicit terminal command and approval. |
| Manual Atlas stages 01-15 | Keep under an advanced group | Commands still exist; useful for diagnostics, noisy for normal operation. |
| Build Recipe Priors stage | Update | Must produce the new policy/PlayHand outcome inputs. |
| Phase 3 PlayHand fresh start | Add | Must bind the immutable new authority and policy. |
| Phase 3 PlayHand resume | Add separately | Must resume the exact campaign rather than silently starting fresh. |

Target normal group:

1. `Lab Gateway`
2. `Phase 3 PlayHand - Fresh Start`
3. `Phase 3 PlayHand - Resume`
4. `AutoResearch Dashboard`
5. `Phase 3 Research Status/Report`

Manual Atlas, destructive cleanup, legacy portfolio research, corpus rebuild,
and old Level C controls must not share this normal operating group.

### Gate 4: storage cleanup and retention reset

Storage work must use a manifest-first, staged process:

1. Stop writers and verify gateway/coordinator state.
2. Inventory and classify top-level roots.
3. Preserve compact decision records, authority plans, summaries, journals,
   reports, manifests, selected final artifacts, and necessary source hashes.
4. Move only genuinely useful cold evidence to
   `Y:/ED-BEAST/C/repos/fuzzfolio-autoresearch/runs_archive`.
5. Verify the destination by manifest/hash before deleting the local source.
6. Delete reproducible raw probe evidence, obsolete generations, stale
   quarantines, caches, and scratch outputs that no longer support a live
   authority or unresolved investigation.
7. Re-audit the compact retained set and free-space result.

Do not solve storage pressure with a symlink from active `runs/` into `Y:`.
Network storage is appropriate for cold evidence and compact reports, not
active journals, SQLite state, worker result materialization, or campaign
coordination.

Read-only inventory snapshot:

- C: free space: about 126 GiB.
- `C:/repos/fuzzfolio-autoresearch/runs`: 103.60 GiB, 296,769 files.
- `runs/derived/phase2-atlas-forensics`: 3.75 MiB.
- `runs/derived/level-c`: 47 KiB.
- The remainder of active `runs` is effectively the four Phase 2 Atlas roots.
- `C:/repos/Trading-Dashboard/.tmp`: 14.49 GiB, 10,141 files.
- Y: archive destination free space: about 1.72 TiB.
- `completed-level-c-v2-20260719` is already present on Y: with a prior
  verified transfer receipt for 100.60 GiB / 301,000 files.

The initial inventory did not descend further into local AutoResearch `.tmp`,
historical local `runs_archive`, or every Atlas subdirectory. Those require
small, bounded follow-up inventories; do not run an unbounded recursive text
search or a parallel per-file scan.

### Current storage disposition

| Path/category | Size | Proposed disposition |
| --- | ---: | --- |
| `runs/derived/phase2-atlas-forensics` | 3.75 MiB | Keep local; compact decision evidence. |
| `runs/derived/level-c` | 47 KiB | Keep local until Phase 3 authority supersedes it. |
| Four current Phase 2 Atlas roots | about 103.6 GiB combined | Build and verify a compact authority capsule, then remove local raw per-task evidence. Do not keep all raw solely because it was expensive. |
| Current Atlas plans, summaries, journals, priors, aggregate CSVs, and source manifests | small subset of above | Keep local in the compact capsule; copy capsule to Y: as second copy. |
| Old full `completed-level-c-v2-20260719` on Y: | 100.60 GiB | Review after the new capsule exists; likely reduce to its compact reports/incident evidence rather than retain forever. |
| `Trading-Dashboard/.tmp/tick-export-bench` | 3.15 GiB | Delete after retaining any benchmark conclusion still cited; reproducible scratch. |
| `Trading-Dashboard/.tmp/fuzzfolio-bars-20260515-084458.tar.zst` | 1.38 GiB | Move to Y: only if it is the sole useful snapshot; otherwise delete. |
| `Trading-Dashboard/.tmp/video-tutorials` | 6.56 GiB | Outside current research; leave untouched unless separately approved for Y:. |
| `Trading-Dashboard/.tmp/video-tutorials.zip` | 1.18 GiB | Verify whether it duplicates the directory; keep one representation at most. |
| `Trading-Dashboard/.tmp/git-history.mp4` | 216 MiB | Outside current research; move/delete only with separate approval. |
| Phase 1/2 care packages and reports | tens of MiB | Keep one canonical package and reports; delete duplicate package copies. |

### Phase 2 authority capsule

Before deleting the 103.6 GiB raw Atlas trees, create a deterministic capsule
containing, for every cutoff:

- execution plan and protocol/authority files;
- execution journal and Atlas summary;
- stage-level manifests and terminal receipts;
- indicator atlas tables;
- signal/forward-response aggregate tables used by the reports;
- discovery result tables used by the reports;
- validation and scrutiny result tables;
- final recipe priors, negative priors, and seed plan;
- a complete capsule file manifest with source relative path, size, and SHA-256;
- both forensic reports, the generated machine-readable comparison outputs,
  and this master plan.

The capsule does not need every worker response, rendered detail bundle,
per-task raw evidence directory, cache file, lock file, or temporary result
envelope. The capsule must be able to reproduce the published tables and
verify plan/journal lineage; it does not need to rerun every replay without the
lake.

After capsule verification:

1. Copy the capsule to Y: and verify its manifest there.
2. Confirm the Phase 3 authority builder uses capsule/priors paths rather than
   raw task directories.
3. Delete the local raw A-D task evidence in one cutoff at a time.
4. Re-run a read-only capsule audit after each cutoff removal.
5. Record bytes reclaimed and leave no symlink at the removed raw path.

### Raw Atlas cleanup handoff

The Phase 2 capsule and the final v2 Phase 3 authority are now the required
retained authority for removing the four local raw A-D task trees. The operator
handoff is `scripts/cleanup-phase2-atlas-raw-roots.ps1`, documented in
`z_docs/phase2-atlas-raw-cleanup-operator.md`.

It defaults to a no-write preview and has a fixed allowlist of the exact A-D
roots. `-Apply` is separately required. Before each deletion it verifies the
local capsule, the Y: archive capsule, the final Phase 3 authority, a fully
stopped Procman, a zero-Vast state (or an explicit recorded override), direct
child containment, and the absence of reparse points. It deletes one cutoff at
a time, re-audits capsule/authority after each, and writes a JSON receipt only
when applying.

The current Codex tool session was policy-blocked from performing recursive
deletion. The script is intentionally the reviewed PowerShell operator handoff
and has not been executed with `-Apply`.

## Retention Principles

Keep locally:

- active Phase 3 authority, state, journals, gateway receipts, and campaign
  artifacts;
- the master plan and compact Phase 1/Phase 2 reports;
- code-required configuration and current prior artifacts;
- small manifests needed to verify cold archives.

Move to `Y:`:

- compact authoritative evidence packages that may be needed for later audit;
- completed-generation summaries and manifests whose raw details are still
  worth retaining;
- forensic evidence for unresolved or consequential bugs.

Delete after verification:

- reproducible Atlas raw probe trees once compact summaries, plans, journals,
  and comparison tables are retained;
- superseded canaries and duplicate comparison workspaces;
- resolved `.corrupt-*` and forensic trees after retaining their incident
  summary and hashes;
- stale gateway payload exports after the incident they diagnose is closed;
- build caches, temporary rendered assets, and obsolete care-package copies;
- historical run generations that are neither authority nor a needed
  comparison source.

Never delete blindly:

- active campaign roots;
- current execution plans or policy/source locks;
- artifacts referenced by a live journal or immutable receipt;
- the only copy of a legacy membership/control manifest used by Phase 1;
- unresolved incident evidence.

## Execution Work Packages

Use Terra 5.6 agents for bounded implementation. Each packet requires an exact
write scope, focused tests, and independent review where noted.

### Packet A: Process Manager cleanup

Scope: `scripts/processes.json`, process-manager tests/docs only.

- Validate every command against current CLI help.
- Remove obsolete one-off controls.
- Add/update Lab Gateway and Phase 3 PlayHand entries.
- Add an explicit resume entry or flag; do not overload fresh start.
- Reload and smoke only after no important managed process is running.

### Packet B: PlayHand policy truthfulness

Scope: `recipe_priors.py`, `play_hand.py`, `play_hand_lab.py`, Lab CLI, formal
Level C binding, and focused tests.

- Define a versioned campaign policy manifest with lane quotas, diversity
  targets, and an explicit negative-expiry basis.
- Add a deterministic lane-aware dealer and persist planned/used counters by
  lane, family, recipe, and instrument in campaign state.
- Preserve deterministic resume and delivery idempotency.
- Reject expired negatives before applying penalties/exclusions and record the
  decision.
- Expose an explicit policy-manifest input for fresh campaigns and bind its
  digest into formal authority.
- If this becomes a broad scheduler rewrite, deliberately fall back to an
  honest 60/40 mode with monitoring rather than delaying the campaign
  indefinitely.
- Preserve authority-plan and reserved-tail fail-closed behavior.

### Packet C: Phase 3 authority builder

Scope: one plan builder/operator command and tests.

- Consume A/B development artifacts only.
- Bind the agreed sampling/diversity policy.
- Bind all authority hashes.
- Record C/D aggregate verdict without candidate-level feedback.
- Emit deterministic plan and dry-run/audit output.

### Packet D: storage migration and cleanup

Scope: filesystem operations only after an approved manifest.

- Execute one retention category at a time.
- Move before delete only where retention is justified.
- Verify destination content before removing local data.
- Report bytes reclaimed and retained authority paths.
- Avoid recursive text searches over large corpus trees.

Expected first-pass reclaim is roughly 100 GiB from compacting completed A-D
raw evidence, plus about 3-5 GiB of obvious current-project scratch. Additional
reclaim from local `runs_archive` and AutoResearch `.tmp` is intentionally
unknown until the bounded inventory packet runs.

### Packet D2: documentation consolidation

Scope: `z_docs` and references from README/operator docs; no runtime code.

- Keep this master plan as the current roadmap.
- Keep `level-c-operator.md` while Level C remains the formal authority layer.
- Review `MANAGER_CONTROLLER_ARCHITECTURE.md` and
  `PLAYHAND_LAB_GATEWAY_DESIGN_SPEC_2026-06-19.md` for current architecture;
  update or retain only if accurate.
- Move superseded dated plans, task lists, capacity snapshots, and incident
  notes to one `z_docs/archive/pre-phase3-20260721/` directory after extracting
  any still-valid rule into this master plan.
- Delete duplicate or purely transient notes rather than maintaining multiple
  competing roadmaps.
- Update README/operator links so a future agent lands on this master plan and
  the current operator guide first.

### Packet E: canary and continuous launch

Scope: operator actions, not new architecture.

- Start Lab Gateway through Process Manager.
- Start Phase 3 coordinator through Process Manager.
- Run a bounded canary and audit receipts, lane accounting, diversity, and tail
  exclusion.
- Fix only correctness blockers.
- Launch one Vast host, scale only while saturated, and tear down at idle.
- Promote to continuous operation only after the canary report is valid.

## Acceptance Criteria

Research:

- A/B-only selection is mechanically proven.
- C/D remain validation-only.
- The reserved tail remains untouched.
- The campaign policy is described exactly as the runtime executes it.

Operations:

- Process Manager has one tested Gateway start and one tested Phase 3 fresh
  start/resume path.
- A stopped coordinator resumes without duplicate or lost work.
- Vast lifecycle remains CLI-only and reaches zero at idle.

Storage:

- No active receipt or journal points to deleted/moved evidence.
- Every retained cold archive has a compact manifest and verified destination.
- Local space is materially reduced, not merely rearranged.
- The master decision record remains usable without mounting the full archive.

Campaign:

- Bounded canary passes authority, receipt, sampling, diversity, and tail
  checks.
- Continuous campaign starts from a clean generation with no legacy strategy
  seeds or weights.

## Immediate Order Of Operations

1. Finish the Process Manager, policy-gap, and storage inventories.
2. Approve the retention matrix before any filesystem mutation.
3. Execute storage cleanup in verified batches to recover comfortable local
   headroom.
4. Normalize Process Manager and implement the smallest required policy gaps.
5. Build and audit the immutable Phase 3 authority plan.
6. Run the bounded PlayHand canary.
7. Start the continuous campaign and Vast lifecycle.
8. Defer portfolio optimization and reserved-tail judgment until a meaningful
   fresh corpus exists.
