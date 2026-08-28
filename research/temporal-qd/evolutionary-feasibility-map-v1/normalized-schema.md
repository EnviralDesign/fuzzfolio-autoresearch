# Normalized schema / data dictionary

`normalized-candidate-windows.jsonl` has one row per retained candidate evaluation and complete market window.

| Field | Meaning |
| --- | --- |
| `cohort`, `protocolGroup`, `evidenceRole` | Explicit stratum and retained-evidence role; never a pooled fitness label. |
| `evaluationId` | Source-row identity, preserving candidate re-evaluations. |
| `candidateId`, `parentCandidateId`, `operatorFamily` | Identity and lineage metadata where retained. |
| `panelId`, `windowId` | Retained panel/window identity; `panelId` is null only when the scored source does not retain it. |
| `grossR` | Retained no-cost/gross result for the window. |
| `modeledCostR` | `noCostNetR - conservativeNetR`; no cost is imputed. |
| `conservativeNetR` | Retained after-cost result used by the historical current policy. |
| `closedTrades` | Closed trades in the retained window. |
| `grossExpectancyPerTrade`, `costPerTrade`, `netExpectancyPerTrade` | Window totals divided by `closedTrades`; null when no trades occurred. |
| `averageHoldingBars`, `medianHoldingBars`, `exposureRatio` | Actual retained phenotype fields; null means unavailable. |
| `managementActionCount`, `managementActionShare` | Derived from retained realized action counts, not authored graph structure. |
| `longConservativeNetR`, `shortConservativeNetR` | Retained side contribution when present. |
| `supportQualified`, `qualityQualified` | Historical source classifications, not recalculated or altered. |

`candidate-evaluations.jsonl` is a compact four-window roll-up for sensitivity analysis. It retains original support/quality classifications and labels all added bands as descriptive only.
