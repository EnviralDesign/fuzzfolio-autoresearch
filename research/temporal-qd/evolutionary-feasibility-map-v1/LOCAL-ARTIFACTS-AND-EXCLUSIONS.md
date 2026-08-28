# Local artifacts and exclusions

## Retained large tables

The reviewed compact files in this directory were generated from this ignored
local artifact root:

`C:\repos\fuzzfolio-autoresearch\.tmp\artifacts\evolutionary-feasibility-map-v1\regeneration-g`

`regeneration-h` is a byte-identical independent regeneration. The large rows
are deliberately not tracked:

| File | Rows | Bytes | SHA-256 |
| --- | ---: | ---: | --- |
| `normalized-candidate-windows.jsonl` | 22,656 | 36,726,213 | `1b9264f55642f1d0096bc7283739fd6021382574289605eb8519dc8141b66735` |
| `candidate-evaluations.jsonl` | 5,664 | 9,674,439 | `6bb9b024d2be63f12d2d510d0aaa4313bbd86704dc671dae84a00acf2fa4a9ae` |

The full file manifest is `checksums.sha256`. The normalizer and its focused
fixture tests are the authoritative regeneration path.

## Exclusions and boundaries

- `C:\repos\fuzzfolio-autoresearch\runs\topology-v2-5-launch-ready-20260825`
  is retained only as a topology-reference cohort. Its economics are synthetic
  no-market conformance values, so it is not used in a market-habitat count.
- `C:\repos\fuzzfolio-autoresearch\.tmp\artifacts\stage5e0-temporal-discovery-bundle`
  is an authority/source bundle, not a completed candidate/window economics
  corpus. It is recorded as unavailable rather than fabricated or pooled.
- The completed Stage5b parity replay is a single vocabulary-stress result, not
  a candidate-panel corpus, and is likewise excluded from the map.
- Raw gateway transport packs are intentionally not read. The map uses the
  sealed reduced member and candidate-window evidence only.
- V38 has twelve scored clone-control window rows without a matching proposal
  campaign bundle (three clone baselines × four windows). They remain explicit
  scored controls; the remaining 2,164 V38 rows and all 20,480 V37 rows
  independently reconcile to their paired reduced bundles.

## Source baseline

This research branch starts at `282c06eb4be3c66cb59b0348d41adb70c7d64c65`
(`codex/rust-canonical-temporal-authority`). Pro's earlier verified
`4eca95eef843665d67cc72446c307987ef2f72b1` is not an ancestor of this local
Rust-canonical base, so the provenance difference is explicit rather than
silently mixed.
