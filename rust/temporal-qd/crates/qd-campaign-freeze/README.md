# Native v5 campaign freeze

`temporal-qd-campaign-freeze` owns the immutable candidate/window task-matrix
publication boundary. Its first admitted operation is
`temporal_qd_native_campaign_task_matrix_manifest_v1`: an already sealed
authority is materialized as Python-byte-compatible `authority.json`,
`task-manifest.json`, and `checkpoint.json` without retaining the task array.

The production v5 operation being added is a separate sealed request, not an
implicit fallback to `temporal_qd_campaign.freeze_qd_screening_campaign`:

- proposal-current-panel accepts a native `evaluation-population` binding plus
  the frozen v5 generation/config/panel authority;
- retained-parent/backfill accept the prefinalizer's
  `temporal_qd_rotating_cohort_selection_v1` file;
- every request binds the raw source semantic identity, rotating evidence,
  generation, role, panel, catalog, template, worker contract, and execution
  commit before candidate rows are opened.

For current-v5 proposal and evolved publications, `evaluation-population.json`
is the exact qd-batch producer artifact: compact canonical JSON followed by a
single LF, bound by its raw SHA-256.  The freezer rejects a Python-pretty (or
otherwise semantically equivalent) rewrite rather than treating formatting as
a fallback.  Historical ladder inputs retain their separately fenced
Python-pretty ABI.

The rotating-materializer's `templatePreparationPath` and
`constructionCatalogPath` have a different sealed-input ABI. They are bound
by their manifest semantic SHA-256 and must be the exact two-space,
sort-keyed Python-pretty representation, with either CRLF (Windows producer)
or LF (cross-platform producer). The freezer preserves those producer bytes;
it rejects compact JSON and arbitrary whitespace rewrites. This format split
is deliberate: those documents are Python-materialized semantic authorities,
while `evaluation-population.json` is a Rust qd-batch byte-bound sidecar.

The bounded current-v5 commit artifacts `native-freeze-result.json`,
`native-freeze-transaction.json`, and receipt-last
`native-freeze-receipt.json` are compact canonical JSON plus LF. Their
inventory rows remain embedded in the compact transaction/receipt and bind
the raw bytes of the larger Python-compatible campaign artifacts. A restart
therefore validates the same compact receipt ecosystem that the v2 control
plane reads; historical v1 and ladder result/transaction/receipt formats are
not changed.

The full operation will emit the legacy-compatible cohort population,
preparation, authority, evaluation identity, campaign, and screening task
artifacts as one restartable receipt. Python remains only an explicitly named
oracle during parity testing; there is no production Python fallback in the
bridge.

## Evidence ladder v3

`temporal_qd_v5_native_evidence_ladder_freeze_manifest_v3` removes the v2
requirement for an external evaluation-population sidecar. Its
validation `archiveAuthority` is exactly `{kind,receiptPath,receiptSha256}` and
uses a self-hashed `generation_finalizer_commit`. Scrutiny uses a self-hashed
`qd_archive_reducer_result` plus the exact fields
`validationFreezeReceiptPath`, `validationFreezeReceiptSha256`,
`validationTailAuthorityPath`, and `validationTailAuthoritySha256`. Both reopen only the receipt's fixed
same-directory `archive.json`, reject symlinks/path traversal, and bind its
semantic SHA, raw SHA, and byte count.

Selected quality members must carry a rich nested candidate whose candidate,
program, identity, and source-profile hashes agree with the member. The Rust
freezer materializes its own durable `ladder-archive-population.json`, then
publishes the existing cohort and task artifacts. `ladder-freeze-receipt.json`
is receipt-last and permits a restart without reopening a deleted upstream
archive receipt; it inventories every locally derived output and binds the
source authority kind/receipt, archive semantic+raw+size, ladder authority,
stage/limit, selection/projection, cohort population, and task roots.

The v3 freezer now accepts only
`temporal_qd_v5_native_evidence_ladder_authority_v2`. Validation and scrutiny
use the distinct campaign roles `evidence_ladder_validation` and
`evidence_ladder_scrutiny`; the authority fixes their limits at 128 and 32 and
binds the exact 12-month and 36-month continuous `researchScrutiny` windows.
For scrutiny, Rust requires the archive-reducer receipt's
`tailAuthoritySha256` to equal the bound validation tail receipt, then requires
its task-matrix and population roots to equal the validation ladder-freeze
receipt. A generic reducer output cannot be relabelled as scrutiny input.

## Evidence-ladder authority v2 materialization

The same binary certifies and publishes the current-v5 ladder authority:

```powershell
temporal-qd-campaign-freeze --manifest C:\sealed\ladder-materialization-manifest.json
```

The manifest schema is
`temporal_qd_v5_native_evidence_ladder_materialization_manifest_v2` and its
exact fields are:

```text
schemaVersion, rotatingEvidenceContract, rotatingEvidenceMaterialization,
sourceFinalizerAuthority, panelTemplatePreparation, constructionCatalog,
stageTemplatePreparations, workerContractSha256, executionEngineCommit,
archivePolicyAuthority, behaviorAttributionRequirement, outputRoot,
manifestSha256
```

`rotatingEvidenceContract` is exactly `{path,rotatingEvidenceSha256}`;
`rotatingEvidenceMaterialization` is exactly `{path,materializationSha256}`;
`sourceFinalizerAuthority` is the existing exact
`{kind,receiptPath,receiptSha256}` finalizer descriptor;
`panelTemplatePreparation` and both entries in
`stageTemplatePreparations` are exactly
`{path,preparationSha256,authorityId}`; and `constructionCatalog` is exactly
`{path,catalogSha256}`. `stageTemplatePreparations` has exactly `validation`
and `scrutiny`.

The two stage preparations must already contain fresh remote Lake attestations
for the complete continuous windows. Lake resolution is an external operation;
the binary fails closed on a missing/null attestation instead of deriving one
from quarter bindings. Each preparation must contain exactly one candidate,
identical to the first candidate in the generation-mapped authorized panel
template. The master template and any final-candidate population are rejected.
The full-window request must cover every catalog-capability timeframe, and an
attestation identity already present on a rotating quarter is stale and is
rejected.

The published self-hashed `ladder-authority.json` has exactly:

```text
schemaVersion, rotatingEvidenceSha256, sourceGenerationIndex, panelId,
stageOrder, outerTail, workerContractSha256, executionEngineCommit, stages,
ladderAuthoritySha256
```

Each stage has exactly:

```text
stage, sourceArchiveAuthorityKind, campaignRole, window,
requestedHorizonMonths, evidenceRole, candidateLimit,
templatePreparationPath, templatePreparationSha256, templateAuthorityId,
constructionCatalogPath, constructionCatalogSha256, archivePolicyAuthority,
behaviorAttributionRequirement
```

Publication writes the two preparations and authorities, the ladder authority,
`materialization-result.json`, `materialization-transaction.json`, and finally
`materialization-receipt.json`. A completed restart validates only this local
receipt-last inventory, so removal of upstream source paths cannot turn a
completed materialization into a partial one.

## Memory model

The durable artifacts intentionally contain O(C×W) data. The native writer
streams that data through canonical hashing and same-directory staging files;
it must not construct a second task array or read a staging artifact into
memory for write-once comparison. Runtime telemetry is excluded from durable
identities.
