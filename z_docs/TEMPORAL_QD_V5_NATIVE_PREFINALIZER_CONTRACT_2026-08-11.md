# Temporal QD v5 native rotating prefinalizer contract

Status: implementation contract frozen on 2026-08-11. This contract replaces the
unchecked Python `finalizerContext`; it must not be extended or accepted by the
native v5 path.

## Authority split

The base manifest authenticates only immutable authorities and prior committed
state:

- frozen supervisor config and v5 generation-config identities;
- sanitized supervisor state basis;
- native proposal manifest/result/receipt/adapter chain;
- current proposal identity-ledger artifact through that proposal chain;
- previous parent archive descriptor;
- previous cumulative rotating archive descriptor or null;
- proposal-current-panel native campaign receipt;
- frozen native runtime authority;
- operational file bindings required to reopen these artifacts.

The Rust transaction derives, and never accepts from Python:

- previous-parent summary and archive-policy binding;
- proposal accounting and complete proposal-attempt funnel;
- retained-parent cohort, provisional selection, and selected rich members;
- required-panel coverage and admitted campaign ledger;
- artifact/publication layout and task accounting;
- generation-record/state-transition bases;
- finalizer source and finalizer manifest.

Archive policy comes from the single frozen `archivePolicyAuthority`. Identity
ledger comes only from the proposal receipt inventory. Campaigns come only from
admitted native campaign receipts. Cell capacity, breeder width, provisional
limit, thresholds, and next immigrant ordinal derive from frozen authorities and
journals. Current panel derives from absolute generation mapping. A parent also
present in the proposal is superseded only when candidate identity agrees.

## Base manifest

Exact schema: `temporal_qd_v5_rotating_prefinalizer_manifest_v1`.

Required keys:

```text
schemaVersion
contractVersion
operation = "prepare_native_v5_rotating_generation"
generationIndex
supervisorConfigBinding
stateBasis
proposalConstructionBinding
previousParentArchiveBinding
previousCumulativeArchiveBinding
proposalCampaignReceiptBinding
runtimeAuthoritySha256
semanticAuthoritySha256
manifestSha256
```

`semanticAuthoritySha256` hashes this projection:

```text
schemaVersion = "temporal_qd_v5_rotating_prefinalizer_semantic_authority_v1"
generationIndex
supervisorConfigSha256
generationConfigSha256
stateBasisSha256
proposalSemanticRoots
identityLedgerSha256
previousParentArchiveSha256
previousCumulativeArchiveSha256
proposalCampaignSemanticReceiptSha256
```

The full manifest hash additionally binds raw file identities and execution
paths. Paths, caps, timeouts, thread counts, and telemetry do not enter the
semantic authority hash.

State basis exact schema: `temporal_qd_v5_generation_state_basis_v1`.

```text
schemaVersion
configSha256
generationIndex
completedGenerationsSha256
uniqueCandidatesEvaluated
workerTasksCompleted
nextImmigrantContinuationOrdinal
uniqueIdentityCounts
duplicateCounters
proposalSlotCounters
stateBasisSha256
```

Exclude wall-clock timestamps, progress/status text, and other observations.

## Resume manifest

Exact schema: `temporal_qd_v5_rotating_prefinalizer_resume_manifest_v1`.

```text
schemaVersion
contractVersion
operation = "resume_native_v5_rotating_generation"
baseManifestBinding
roundIndex
previousResultBinding
newCampaignReceiptBindings
runtimeAuthoritySha256
manifestSha256
```

Each round uses a new fixed directory containing immutable `manifest.json` and
`result.json`. A committed awaiting result never changes; progress requires the
next chained resume manifest.

## Campaign receipt

Exact schema: `temporal_qd_v5_rotating_campaign_receipt_v2`.

```text
schemaVersion
contractVersion
generationIndex
campaignRole
panelId
rotatingEvidenceSha256
cohortSource
campaignFreeze
campaignSeal
evaluatedMembers
candidatePanelBundles
semanticReceiptSha256
runtimeAuthoritySha256
executionBindings
receiptSha256
```

Semantic fields:

```text
cohortSource:
  kind = proposal_evaluation_population | sealed_cohort_selection
  sourceSemanticSha256
  candidateCount
  selectionSha256?  # required for non-proposal roles

campaignFreeze:
  transactionSha256
  cohortPopulationSha256
  preparationSha256
  authorityId
  evaluationIdentitySha256
  campaignSha256
  taskMatrixSha256
  candidateCount
  windowCount
  taskCount

campaignSeal:
  directionalTailAuthoritySha256
  campaignSealSha256
  tailResultIndexSha256
  tailTransactionSha256

evaluatedMembers:
  rawSha256
  sizeBytes
  recordCount
  rowSchema = "temporal_qd_evaluated_member_v1"

candidatePanelBundles:
  rawSha256
  sizeBytes
  recordCount
  rowSchema = "temporal_qd_candidate_panel_evidence_bundle_v1"
```

Execution bindings contain reopenable path/raw descriptors for freeze
manifest/transaction, campaign, seal, v4 index, tail transaction,
evaluated-members JSONL, and bundle JSONL. They are excluded from
`semanticReceiptSha256` but included in `receiptSha256`.

The receipt must cross-check role/panel/generation/rotating identity, task matrix,
authority/evaluation/campaign identities, v4 directional runtime epoch, index ↔
tail transaction, evaluated-member descriptor, cohort/panel bundle membership,
and input identity. Gateway dispatch settings/telemetry are execution evidence,
not research-semantic authority.

## Task plan

Exact schema: `temporal_qd_v5_rotating_prefinalizer_task_plan_v2`.

```text
schemaVersion
contractVersion
semanticAuthoritySha256
generationIndex
roundIndex
phase
tasks
taskCount
taskPlanSha256
```

Each task has exact keys:

```text
taskOrdinal
campaignRole
panelId
cohortSelection
candidateCount
candidateSetSha256
taskSha256
```

`cohortSelection` describes native-created rich candidate JSONL. Python candidate
lists are forbidden.

## Result

Exact schema: `temporal_qd_v5_rotating_prefinalizer_result_v1`.

```text
schemaVersion
contractVersion
baseManifestSha256
manifestSha256
semanticAuthoritySha256
roundIndex
previousResultSha256
generationIndex
status
admittedCampaignLedger
cohort
provisional
panelCoverage
taskPlan
funnelReductionSource
selectedRichMembers
finalizerSource
finalizerManifest
resultSha256
```

Unavailable outputs are null. Allowed statuses are:

```text
awaiting_retained_parent_current_panel
awaiting_prior_panel_backfill
ready_for_finalizer
```

The campaign ledger rejects duplicate `(role, panel)` receipts and receipt reuse.
Panel coverage maps every provisional candidate to exactly one bundle per required
panel and is self-hashed.

Ready output artifacts are:

- `temporal_qd_native_funnel_reduction_source_v1`;
- selected rich-member JSONL descriptor;
- complete bundle snapshot;
- `temporal_qd_generation_finalization_source_v2`;
- `temporal_qd_generation_finalization_manifest_v2`.

The finalizer exact-key validates v2, derives publication/state bases, and uses
fixed relative semantic paths:

```text
archive.json
generation-funnel.json
evidence/generation-ledger.json
evidence/checkpoint.json
evidence/cumulative-archive.json
```

Absolute publication paths are execution-only.

## Transaction sequence

1. Validate proposal and runtime chains.
2. Reopen prior archives and derive summaries/policy.
3. Extract the full attempt stream in Rust from the durable fragment receipt.
4. Join attempts to proposal v4 tail and commit funnel source.
5. Scan proposal members and previous parents.
6. If required, emit retained-parent selection and an awaiting result.
7. Run native freeze, gateway dispatch, seal, and campaign receipt v2.
8. Commit an immutable resume manifest.
9. Admit receipts, derive cohort/provisional, and find missing prior panels.
10. Emit native backfill selections and another awaiting result if needed.
11. Repeat through chained resume manifests.
12. With exact coverage, stream selected members, derive finalizer source/manifest,
    and commit the prefinalizer result last.
13. Run generation finalizer.
14. Apply the state patch only if `stateBasisSha256` still matches; promote the
    proposal identity ledger only after generation commit.

## Upstream proposal accessor

Persist the exact core `temporal_qd_v5_evolved_publication_fragments_v2` receipt
as a core-owned content-addressed object and expose its authenticated descriptor
through evolved outer receipt/result/adapter. The attempt adapter commits a
self-hashed receipt binding:

```text
inputSha256
fragmentBundleSha256
evaluationPopulationSha256
attemptStream descriptor
receiptSha256
```

## Required gates

Reject previous archive/cumulative tamper, summary injection, archive-policy
alias, ledger/result/receipt mismatch, fragment ordinal/count/accounting drift,
campaign relabel, freeze/task/seal/index/tail mismatch, member/bundle tamper,
missing/extra/wrong-candidate bundles, genome mismatch, conflicting duplicate
identity, state drift, runtime role mismatch, unknown fields, path traversal,
symlink escape, and descriptor mismatch.

Prove operational path/cap/timing changes do not alter semantic authority while
semantic caps do. Crash after every sidecar must recover identically; divergent
write-once output fails; old-generation or duplicate receipt reuse fails; sealed
campaign restart performs zero raw-result reads; finalizer source/manifest without
result commits identically; runtime rotation uses a successor attempt root.

## G0 selected-attempt authority

G0 extraction must not emit a pre-campaign
`temporal_qd_native_funnel_reduction_source_v1`. The ordinary Rust funnel
assembler is the sole author of evaluated, quality, retention, and terminal facts
after the v4 tail exists.

The extractor first reconstructs each Core `evaluation_funnel_entry` from the
authenticated all-attempt row, retaining the original construction ordinal,
origin, candidate, proposal, funnel candidate, and accepted record identity. The
candidate-ID-ascending comma-fragment over those rows must exactly match
`g0FunnelFragments.evaluationPopulationFunnelEntries` in SHA, encoded byte count,
and row count.

It then emits candidate-ID-ascending LF JSONL rows with schema
`temporal_qd_v5_g0_selected_proposal_attempt_v1`:

```text
schemaVersion
proposalOrdinal                  # selected-local 0..N-1
constructionAttempt              # unchanged Core proposal-funnel row
g0BootstrapProof
  schemaVersion = temporal_qd_v5_g0_funnel_proof_v2
  selectedProjectionSha256
  nativeStaticProofSha256
  proofSha256
selectedAttemptSha256
```

`nativeStaticProofSha256` hashes `{static: funnelCandidate.staticReachability,
native: funnelCandidate.nativeValidation}`. The selected projection must match the
construction row on generation, construction ordinal, origin, candidate ID,
record SHA, candidate/pair identity, and raw compiled-pair/source-profile SHA.
Construction-wide rejected/no-op attempts remain in construction accounting, not
the selected stream.

Command:

```text
extract-g0-selected-attempts <chain-input> <attempts.jsonl> <receipt.json>
```

Receipt exact schema:
`temporal_qd_v5_g0_selected_attempt_stream_receipt_v1`.

```text
schemaVersion
contractVersion
generationIndex = 1
inputSha256
proposalManifestSha256
proposalResultSha256
proposalReceiptSha256
outputInventorySha256
g0FunnelFragmentsSha256
g0FunnelProjectionStreamReceiptSha256
selectedProjectionIndexSha256
ordering = candidate_id_ascending_v1
attemptStream
  relativePath = g0-selected-proposal-attempts.jsonl
  rowSchema = temporal_qd_v5_g0_selected_proposal_attempt_v1
  rawSha256
  sizeBytes
  recordCount
proposalAccounting
  proposalAttemptCount           # selected-local
  dispositionCounts
  originProposalCounts
  g0ConstructionProposalAccounting
    proposalAttemptCount
    acceptedCount
    selectedCount
    attemptJournalSha256
    acceptedPoolSha256
    selectionSha256
    campaignLedgerSha256
    compactIdentityLedgerSha256
receiptSha256
```

All counts and roots derive from authenticated Core receipts. Semantic identity
uses the fixed relative stream path; absolute paths and operational observations
are excluded. Restart validates receipt and stream without reopening the large
outer chain. An orphan stream may be adopted only after reauthenticating and
byte-comparing it, then writing the receipt last.

## Funnel assembly v2

Use exact input schema `temporal_qd_v5_native_funnel_reduction_input_v2`. Replace
independent stream/accounting inputs with:

```text
proposalAttemptAuthority
  kind = g0_selected | evolved
  receiptPath
  receiptFileSha256
  receiptSizeBytes
  receiptSha256
```

The assembler derives stream and accounting only from that receipt. For G0 it
uses wrapper-local proposal ordinals and the nested construction entry SHA as the
attempt identity, validates both wrapper/proof self-hashes, and otherwise runs the
same tail/quality/archive-preprojection/terminal logic as evolved attempts.
Missing canonical evidence is permitted only for a valid G0 selected-stream
receipt plus valid bootstrap proof; generation index alone is never proof.

## Direct Rust state application sidecar

Store the exact Rust `temporal_qd_generation_record_v2` directly in
`state.completedGenerations`. Do not wrap it in `nativeV5Construction`,
`nativeV5Invocation`, `nativeGenerationFinalization`, or a rebuilt legacy artifact
ledger.

The pinned Rust boundary authors exact schema
`temporal_qd_v5_generation_state_application_sidecar_v1`:

```text
schemaVersion
contractVersion
generationIndex
generationKind
configSha256
stateBasisSha256
completedGenerationsBeforeSha256
semanticAuthoritySha256
runtimeAuthoritySha256
finalization
  sourceSha256
  manifestSha256
  commitSha256
  generationRecordSha256
  statePatchSha256
proposalStateAuthority
  proposalManifestSha256
  proposalReceiptSha256
  generationJournalSha256
nextState
  stage = generation_proposal
  currentGenerationIndex
  uniqueCandidatesEvaluated
  workerTasksCompleted
  nextImmigrantContinuationOrdinal
  uniqueIdentityCounts
  duplicateCounters
  proposalSlotCounters
  completedGenerationsSha256
identityLedgerPromotion
  inputIdentityLedgerSha256       # null only for G0
  outputRelativePath = proposal/v5-native/identity-ledger.json
  outputIdentityLedgerSha256
  outputIdentityLedgerFileSha256
sidecarSha256
```

The builder validates commit/record/state-patch equality, shared generation/state
basis/runtime/semantic authorities, exact absolute counters/maps, exact prior
records plus Rust record, and proposal receipt ledger roots.

Python applies it mechanically:

1. Require current state basis and completed-generations-before root.
2. Save pending marker `{generationIndex, sidecarSha256, phase: ready}`.
3. Promote the exact identity ledger (G0 input absent; evolved input exact).
4. Append the exact Rust record and atomically assign all absolute next-state
   fields; remove the marker.
5. Restart accepts only the exact old/applied state and input/output ledger
   combinations described by the pending marker; every other combination fails.

Tests must cover sparse construction ordinals, candidate reordering, duplicate or
missing selection, projection/record mismatch, fragment mismatch, proof replay,
null-evidence bypass, tail outcomes, orphan-stream adoption, assemble-input replay,
and commit/record/patch/state/ledger substitutions plus crash points around ledger
promotion/state save.
