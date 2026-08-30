# Component-surrogate validation V3: pre-bulk scientific gates

## Scope and stopping boundary

This branch seals only the gates required before a sterile component-feature
bulk extraction.  It does **not** recover the remaining 11 windows, calculate
any profile/strategy/market result, inspect outcome values, start workers, or
construct the bulk corpus.

The branch starts from accepted V2 commit `3cd14c41f20cf927cf255271decef651cce44f51`.
The historical engine authority is `EnviralDesign/FuzzFolio` commit
`2bd50ccb3af1700d286da88cbcaecb4aca24f1a2`.

## Frozen extraction census

`extraction-census-v3.json` is an outcome-value-free, self-hashed census:

- 41 directional component contexts and 19 immutable component identities.
- Context-derived coverage: 20 long contexts, 21 short contexts, 37 M5
  contexts, and 4 M15 contexts.
- Separately, the frozen outcome-reconciliation cohort declares 25 exact P3
  same-panel parent-comparable contexts, 17 realized phenotypes among those
  contexts, 11 children with P1/P2 backfill, and 9 exact P1/P2 parent-comparable
  cases.  Those are not side or timeframe counts.
- Canonical payload SHA-256:
  `sha256:05b7c538eb601eb9a97a363184503cb144ed8f131a154e1ec055ddf233754f3a`.
- The M5 and M15 canaries are selected lexicographically from that manifest,
  not hand-picked for a favourable result.

The source script rejects outcome/economics/quality/rank fields recursively and
reads the V38 evaluated-member material only to obtain the frozen candidate
structure and binding provenance.  It emits none of that source's outcome
material into the manifest.

## Historical event and clock contract

`historical-event-projection-and-clock-audit-v1.json` seals the relevant
source rule, with canonical payload SHA-256
`sha256:9c3261e460b5a202c4ff31ff5c390cd80e62efc358c669e7daabc637c3c82b52`:

- A graph event comes from the named raw `EventBinding` output, never from
  `instance.process()` long/short scores.
- The historical M15 non-forming availability shift onto M5 is 10 minutes:
  a source M15 bar starting at `T` first appears at M5 bar start `T+10`.
- Raw events use exact reindexing and are not forward-filled; processed scores
  are forward-filled only as separate diagnostic evidence.
- The visual alignment boundary converts an indicator warm-up `None`/`NaN`
  event sample to `false` before the observation adapter applies its strict
  Boolean/exact-0-or-1 event-value contract.

The canaries call this projected value `componentEvent`; they do not claim to
have constructed a TemporalGraph runtime `freshEvent`.

## Deterministic component canaries

Both canaries read only the checksum-verified isolated P3 archive and run the
pinned indicator implementation directly.  They import neither TemporalGraph
nor an outcome path, and execute no strategy/economic computation.

| Canary | Frozen actual insertion | Result |
| --- | --- | --- |
| M5 | `CHANNEL_REENTRY`, `long_evtind_83dede93edc5`, binding `long_evt_890841164d87` | 18,978 rows. Two runs produced byte-identical gzip projections: `sha256:ebe6b1507cb433fb9f6f421ac52e6802570b391aae2fc4c3773dabe45f9d2659`. Raw long/short event values equal their processed scores for every comparable row in this *lookbackBars=1* fixture; that equality is diagnostic, not a general substitution rule. |
| M15 → M5 | `MARKET_MODE_TRANSITION`, `short_evtind_99f9673af321`, binding `short_evt_e26ec83b4023` | 6,336 native M15 rows and 18,978 aligned M5 rows. Two final runs produced byte-identical gzip projections: `sha256:ed9619209c643ca54bc420dffa582f9544f7886ba6471bb755bbc2cb5533175d`. |

The M15 timing adversarial checks pass: 237 active native events had all three
test clocks available; zero were visible one M5 bar before M15 completion,
zero first-allowed-observation mismatches occurred, and synthetically moving
the M15 source by five minutes changed 474 aligned M5 rows.  The indicator had
40 long and 40 short pre-analysis warm-up missing samples; the sealed
historical visual-boundary rule converts those samples to `false` rather than
creating events.

## Next authorized step

Only a separately authorized, sterile bulk extractor may consume this census.
It must not open evaluated outcomes, must write to an isolated new artifact
root, and must preserve the native component and historical M5 decision clocks
as distinct feature domains.
