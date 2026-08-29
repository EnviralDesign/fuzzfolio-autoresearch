# P3 component-only canary report v1

## Scope

This report seals one deterministic, feature-only projection for frozen V38 panel-3 year-1-q3. It used historical Trading-Dashboard commit `2bd50ccb3af1700d286da88cbcaecb4aca24f1a2`, the first frozen task in the task source whose SHA-256 is `32c4cd57b99cd00ee77da129b6299149d148a174cdef78e64bbe2b3dd21384b2`, and only the isolated archive root recorded in the authority ruling. It did not execute a temporal graph, strategy, trade simulation, economics, or outcome calculation.

## Bound component

- Task: `temporal-search-6667c9b783c2363417e6b38b4784e3d0`
- Candidate: `qd_28dba1f812d0cb5716ffe871a6ce`
- Frozen window semantic: `sha256:fce37ff4b2469a0cdc9eeca306e6e98667a8b074f9eee07771f201f4effcc478`
- Component: `PRICE_RECLAIM_MA`, instance `long_trend_trigger`, M5 completed bars, `lookbackBars=1`, EMA period 21, `matype=1`, `min_prior_displacement=0`.
- Direction binding: long `bullish`; short `bearish`.
- Analysis interval: `[2022-07-01T00:00:00Z, 2022-10-01T00:00:00Z)`.

## Double-run result

Each fresh process checked the five isolated M5 raw artifact hashes before calculating the component. The two runs produced byte-identical gzip projections and the same canonical identities:

- Projection rows: 18,978
- Projection canonical payload SHA-256: `sha256:d3067eb37d5a12bfd62c1212186771ebe3ddb6bfd461aa4909335b5262984f6c`
- Projection gzip SHA-256: `sha256:11ae00f07b13d08a1617a21471b9678fdca498d5e889f1d3ada2ed9746ca9aca`
- Result canonical payload SHA-256: `sha256:c784b2cf67adbf51eb07923fcd6d74aef036951271e7d21bef8c4e068c3f868c`

Long had 1,346 active bars and 1,346 event starts; short had 1,347 active bars and 1,347 event starts. Therefore `eventStartShareOfActiveBars` is 1.0 for both directions. `freshEventAvailability` is intentionally unavailable because this run did not execute the temporal graph runtime.

## Guard evidence

- All bar reads were under the isolated P3 root and all five archive raw hashes matched.
- The runner accepts no outcome-path argument, imports no temporal graph, and records `noStrategyOrEconomicExecution=true`.
- Its component selection is fixed by the frozen task-source hash and the first task line; it does not select from any child, forensic, outcome, or current-lake data.
- Exact runtime-source hashes are in the ignored canary result artifacts and Pro review packet.

## Review boundary

This is not a 12-window validation and makes no claim about strategy or economic outcomes. Stop here pending Pro review before opening any outcome evaluation or extending the canary.
