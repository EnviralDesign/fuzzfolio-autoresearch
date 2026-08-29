# V38 component-surrogate validation V2

## Current disposition

`insufficient_retrospective_evidence`

The authorized V2 reconstruction stopped before the component-only canary. The frozen V38 task bindings are recoverable, the historical Trading-Dashboard engine is pinned, and the pre-outcome feature protocol is sealed. The exact historical source bars needed to reproduce the component inputs are not available in the current local market-data lake, however. Substituting nearby bars, current partitions, or a fresh download would invalidate the frozen attestations, so no reconstruction was launched.

## What was established

- AutoResearch source: `51c2f9175f441166e7fc997109e939a9f9103b5d`.
- Historical Trading-Dashboard engine: `2bd50ccb3af1700d286da88cbcaecb4aca24f1a2`.
- V38 campaign: `temporal-qd-v5-fast-ephemeral-operator-family-matrix-20260820-v38`.
- The source boundary for an allowed component-only calculation is `backend/app/api/scoring/indicator_preview.py` in the pinned engine: it instantiates an indicator and calculates it from historical OHLCV without a strategy profile, graph, trade simulation, or economics.
- `autoresearch/signal_atlas.py` is not an extractor seam: its path invokes replay/simulation and writes a forward-event sidecar, so it was not used.
- The sealed numeric contract is `feature-protocol-v2.json`, commit `0a5a74d`, SHA-256 `b412209520f0a1b8ea7bacf0d1f0e0bef1eda508f0e8096fdb8e31bc8b8c04cc`.

## Exact blocker

The current remote-state manifest contains none of the twelve V38 market-data attestation hashes. Direct reads through the pinned historical engine showed that panel 3 has zero M5 rows for each of its four frozen windows. The two nonzero windows from panels 1 and 2 are still missing part of their frozen lookback interval. Therefore no primary-panel canary can be truthfully run.

The detailed attestation-versus-availability matrix, manifest identity, and task-date ranges are retained as ignored audit artifacts under `.tmp/artifacts/component-surrogate-validation-v2/` and in the Pro review packet. No strategy replay, child evaluation, worker/gateway/Vast operation, calibration, generation, or policy change occurred.

## Authorized resumption condition

Resume only after an immutable, versioned historical bar source is available whose identity validates the exact twelve frozen V38 task bindings and whose coverage includes each requested lookback through analysis window. Do not use nearest/current data as a substitute. Re-enter at the sealed protocol, run the primary-panel component-only canary, and keep all strategy/outcome imports out of the extraction process until its feature artifact has been sealed.
