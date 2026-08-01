# Stage 5E-1 search-quality result

Stage 5E-1 is admitted as a complete and valid deterministic calibration of
the Stage 5E-0 discovery generator and selection policy.  This record binds
the repository to the exact native evidence reviewed at the admission
checkpoint; it does not copy, rewrite, or replace the immutable evidence.

## Frozen implementation and evidence

- AutoResearch implementation commit:
  `7dd4cb75ac8ba5f3a450baa23e0069edfdef050f`
- FuzzFolio implementation commit:
  `8744c7dcc726100f91dca68ab4d5e0f2ee9c2b69`
- Worker contract:
  `sha256:b69ecc83570dc1996a39d24f4e8d6d7650ab0306b15831320c5acdca40522ee9`
- Stage 5E-0 discovery root:
  `stage5e0-discovery-pilot-20260731T220000Z-r3`
- Stage 5E-1 calibration root:
  `stage5e1-search-quality-20260731T235022Z`
- Calibration binding:
  `sha256:aa178786ab1bdabacd57b4478512d4e0475b3b54fcb6465032b818b71391930c`
- Phase A report:
  `sha256:7e15453b5c550041349177216f66ebfb071a96459f94dceb941fbfd425eda06a`
- Control selection:
  `sha256:d5e2f9203b096e5133f3bd7d28fdff003cc5c8467b73e7bc66bdc9a3d9a55d59`
- Control authority:
  `sha256:b6e9b15ebc19c71b0e84014644fcc198ebb16ee17c2df6387fb17d6ca47d3efb`
- Control task matrix:
  `sha256:119576276c0693a3400b18601a42fd1fad01c15e2836ea1b4ec9f6be714a17fc`
- Control result set:
  `sha256:8a72bf047594967b9c005026fcfa534bbcb2d8bc3da283993f020df81a226dbc`
- Final report:
  `sha256:f250bd9eed02b721d17429ff41a7be28b17d78be83cb836058dd0f49254154b2`
- Final manifest:
  `sha256:3ba047ccf369bba25bb8fc9c50ae87560f1da9a9580e4665bfc3bceae1a1d0cf`

The final manifest contains 151 files and passed the repository-owned
Stage 5E-1 audit.

## Native control lifecycle

The deterministic control selected 64 of the 167 unselected candidates after
excluding all 89 selected candidates.  It used the admitted stratified-hash
procedure without reading screening economics, novelty fingerprints, profile
content, or confirmation outcomes.

Control Fresh was launched once, with an absent output root and an empty Lab
Gateway.  Exactly 128 B/D tasks completed:

- 64 candidates on window B;
- 64 candidates on window D;
- 128 materialized results;
- 128 checkpoint entries;
- 128 acknowledgements;
- zero terminal failures, requeues, expired leases, incompatible claims,
  identity or attestation mismatches, dropped results, lost results, or result
  backlog;
- no reserved evidence, execution-cell identity, or lake-manifest identity;
- 57 results from the Mac LAN pool, 58 from the Sager LAN pool, and 13 from
  the local containment worker.

The Gateway returned to zero queue, live tasks, leases, backlog, retained
tasks, and stale workers.  All 15 workers remained online with the exact
frozen worker contract.  Resume was not used.

## Frozen findings

Primary outcome: `management_activation_gap`.

- Break-even management: 62 authored instances, 48 never activated,
  `0.7741935483870968` dormant share.
- Trailing management: 190 authored instances, 88 never activated,
  `0.4631578947368421` dormant share.
- Median composite behavioral distance: `0.5530863791517296`.
- Reference largest-cluster share: `0.609375`.
- The generator was not behaviorally collapsed under the frozen rule.

The independent search-policy finding is adverse selection enrichment:

| Measure | Selected 89 | Control 64 | Selected minus control |
| --- | ---: | ---: | ---: |
| Median B/D conservative net R | -4.849867 | -1.896696 | -2.953171 |
| Median worst-window R | -3.126681 | -1.492637 | -1.634043 |
| Positive aggregate rate | 0.044944 | 0.109375 | -0.064431 |
| At least one positive window | 0.123596 | 0.171875 | -0.048279 |

The Cliff deltas were `-0.557409` for total conservative net R and
`-0.554249` for worst-window R.  Sixteen controls had no B/D trades, but the
conclusion is not an inactivity artifact: among the 48 active controls, median
total conservative net R was `-2.555159` and positive aggregate rate was
`0.145833`, both still better than the selected cohort.

All cohort medians were negative.  Windows A-D are therefore exhausted
development evidence for generator and selector design.  They may support
read-only diagnosis and retrospective reporting, but they cannot validate a
repaired policy.

