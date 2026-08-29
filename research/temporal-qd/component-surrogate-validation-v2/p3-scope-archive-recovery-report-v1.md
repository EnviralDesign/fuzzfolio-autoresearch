# P3 scope-archive recovery report (V1)

Status: **blocked before canary**. This is an evidence report, not a
semantic-only acceptance or a protocol amendment.

## Authority and bounded action

- Recovery plan: `scope-archive-recovery-plan-v1.json`
  (`planCanonicalPayloadSha256`:
  `sha256:6d517eb026a1d748a51370b6660d7b0cb02968ce5016fa3ccc98003d8d30dae5`).
- Target: V38 `panel-3-year-1-q3`, `bars`, `EURUSD`, `H1/M15/M5`, exact
  half-open data window `[2022-05-11T00:00:00Z, 2022-10-01T00:00:00Z)`.
- Frozen V38 semantic identity:
  `sha256:fce37ff4b2469a0cdc9eeca306e6e98667a8b074f9eee07771f201f4effcc478`.
- Frozen V38 task/window receipt identity:
  `sha256:69ccc2e8c89b1f50c01e42eaa1612be161ee98dca8520d0380f3f6870b391988`.

The lake was queried sequentially, one exact month/timeframe scope at a time,
with the plan's frozen request and expected semantic identity. The recovered
files were written only to the ignored isolated root
`.tmp/artifacts/component-surrogate-validation-v2/isolated-lake/panel-3-year-1-q3`.
No current lake files, policies, workers, strategy graphs, or trade simulation
were changed.

## Exact archive result

All fifteen required scope archives arrived without retry or throttle:

| Timeframe | May | Jun | Jul | Aug | Sep |
| --- | --- | --- | --- | --- | --- |
| H1 | `64c34a35` | `06a79de3` | `883d4d53` | `05bb5499` | `57a601d5` |
| M15 | `57d8c62e` | `faeb8742` | `ffe375a2` | `72fe581c` | `911184bf` |
| M5 | `0cf437fb` | `e6d3d538` | `2cb442d2` | `249fb90e` | `732e4286` |

Each archive response and its embedded archive manifest supplied the frozen
window semantic identity above. Filtering the restored monthly files to the
frozen half-open window produced 2,472 H1 rows, 9,888 M15 rows, and 29,615 M5
rows. Each timeframe was strictly monotonic with no duplicate timestamps; the
observed timestamp gaps were whole multiples of that timeframe's cadence.

## Receipt finding

`POST /api/lake/window-attestations/verify` returned `matches: true` and the
frozen semantic identity. It issued a *new current-lake receipt* instead of the
frozen V38 receipt:

| Field | Frozen V38 | Current verifier result |
| --- | --- | --- |
| `window_semantic_sha256` | `fce37f...effcc478` | `fce37f...effcc478` |
| `attestation_sha256` | `69ccc2...391988` | `5fdf48...c864a4` |
| global coverage | `3f33c1...56ca2f` | `fa1cc9...68c1ca2` |
| source coverage | `91b308...223019` | `812030...386bf96` |

This is material to the frozen acceptance rule. The historical engine's
`_verify_window_attestation_for_binding` verifies semantic equality and records
the returned receipt as observed metadata; it does **not** assert equality to
the original receipt hash. That runtime behavior cannot supersede the V38
scope-recovery requirement, which specifically asks to reproduce the frozen
task/window attestation before a component-only canary.

## Retention check

Read-only inspection of the `fuzzfolio-data-lake` container's mounted runtime
found the live manifest, promotion records, and deployment backups. An exact
recursive search of the retained lake control records and deployment backups
for both the frozen semantic hash and the frozen V38 receipt hash returned no
matches. The scope-archive manifests contain the semantic identity, not a
historical V38 receipt record.

## Decision required before the next execution phase

Do **not** launch the component-only canary yet. The exact archived market
data is available and internally consistent, but the frozen V38 receipt cannot
currently be reproduced or independently verified from durable retained
metadata. Proceed only after an explicit authority decision either supplies a
historical receipt source that proves `69ccc2...391988`, or formally revises
the recovery acceptance rule. No such revision is made by this report.
