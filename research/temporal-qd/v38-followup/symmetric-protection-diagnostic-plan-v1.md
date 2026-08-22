# Symmetric-protection diagnostic plan v1

Diagnostic only. Not a production gate. Not a universal 1:1 requirement. Native semantics only; do not invent a second execution engine.

## What 1:1 does and does not isolate

A native-supported `reward_multiple = 1.0` target, with the real stop definition retained, asks whether entry/signal logic still has edge when payout skew is removed. It does **not** freeze the entry tape. The real state machine, management, re-entry, and timeouts still run, so a 1:1 child can take a different path than its parent even when the graph is unchanged.

That is the full-system probe: compare with the exact parent clone on the same frozen panel. It answers “does this whole machine still pay after we stop paying it with target skew?” It does not answer “were the original entries independently +EV?”

## Costs and break-even win rate

At 1:1 gross, round-trip cost `c` (in R) moves break-even win rate to `0.5 + c/2` if wins and losses are the same size. V38’s catastrophic 0.25% stop child paid about **52R** of cost drag on **-69R** net with 260 trades. A 1:1 probe that raises trade frequency will look worse even with unchanged signals. Report cost drag, trade count, and holding time beside net R.

## Detecting extreme RR-only candidates

A candidate that is good only under extreme payout shaping will:

- beat its parent at a large `reward_multiple` and collapse under a 1.0 target on the same panel;
- show target-hit mass in close reasons that disappears when the multiple is 1.0;
- fail independent-panel confirmation once skew is removed.

Genuinely asymmetric strategies remain eligible when they still generalize across panels with controlled tails after the 1:1 probe, or when their asymmetry is a documented locator/contract rather than an unmeasured payout accident.

## Two probes

1. **Full-system symmetric-protection probe** (feasible now)
   - Keep the native strategy and stop.
   - Use native `reward_multiple = 1.0` where that locator is valid.
   - Allow real exits, re-entry, and management.
   - Compare with the exact parent clone.
   - This is the same compiler/worker path as current initial-protection plans.

2. **Fixed-entry counterfactual probe** (not feasible from current retained artifacts)
   - Current `tradeSequence` stores fills (`entryClockIndex`, `exitClockIndex`, `closeReason`, `netR`), not skipped opportunities.
   - Changing protection changes later availability, so applying 1:1 only to historical fills is not a clean entry-quality test.
   - Required additional worker artifact: `temporal_qd_entry_opportunity_tape_v1` — per-clock entry-intent/opportunity mask, parent locators at fill, identical conservative cost model.
   - Until that tape exists, do not fake the counterfactual.

## Isolation

Do not add this diagnostic as an archive or breeding gate. Do not treat 1:1 as a production requirement. Do not launch it in this change.
