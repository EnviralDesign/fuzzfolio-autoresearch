# Stage 5E-3 modest policy-validation decision

Status: implemented through the immutable screening-prelaunch gate. Screening
execution requires a separate explicit admission. Confirmation and broader
search remain blocked.

## Purpose

Stage 5E-3 tests two policies on fresh development evidence:

1. activation-aware generator v2 should reduce unreachable, dormant, and
   rejected management behavior without collapsing behavioral diversity;
2. robust-envelope selector v2 should enrich untouched confirmation outcomes
   relative to its embedded deterministic control.

This is not a production-strategy search. No candidate may be promoted from this
campaign.

## Fresh evidence rule

The window selector reads promoted Lake coverage metadata only. It never reads
bars, volatility, candidate outcomes, economics, window semantic hashes, or
reserved evidence. It enumerates complete one-calendar-month blocks inside the
already-authorized Level-C interval:

```text
[2021-06-29T00:00:00Z, 2024-06-29T00:00:00Z)
```

It removes all blocks overlapping exhausted windows A-D and the protected
interval beginning `2024-06-29T00:00:00Z`. Remaining blocks are ranked by the
canonical SHA-256 of only:

```text
selection algorithm version
block start
block end
```

The first four ranks are assigned E, F, G, and H before candidate generation.
E/F are screening; G/H remain untouched confirmation evidence. Every selected
block must subsequently pass an independent `require_complete` Lake attestation
against one frozen data-availability cutoff.

## Population and task boundary

Generator v2 is unchanged. Its repository-admitted modest-campaign parameter
profile changes only the population ceiling:

```text
128 unique validated programs
64 broad_seed_mutation
64 seed_derived
```

Every accepted program still requires authoritative Fuzz validation, static
management reachability, no orphaned or dominated management action, positive
synthetic witnesses for every authored capability, and exact serialized restart.

Only the E/F authority may be frozen initially:

```text
128 candidates x 2 screening windows = 256 tasks
two fixed cost views per task
one shared observation stream per task
```

No G/H authority is created before the mandatory screening-result checkpoint.

## Screening stop boundary

The prelaunch package must prove the exact window identities and attestations,
128-program population and deterministic journal, complete witness coverage,
256/256 AutoResearch and native Fuzz task validation, independent matrix rehash,
successful hosted regressions, unchanged FuzzFolio and worker contract, stopped
Gateway, and an absent screening result root.

After explicit authorization, E/F Screening Fresh may start once. When it
completes, selector v2 freezes its archives and exactly 32 deterministic controls,
then the system stops for the user's deep midpoint review. G/H cannot start
automatically.

## Predeclared final criteria

Selector enrichment on G/H requires every condition below:

- selected-minus-control median total conservative R greater than zero;
- selected-minus-control median worst-window conservative R greater than zero;
- selected-minus-control any-positive-window participation greater than zero;
- Cliff's delta for total conservative R at least `0.147`;
- no material reversal in the active-only comparison.

Generator v2 fails real-market validation on fewer than 32 robust-envelope
eligible candidates, behavioral collapse under the Stage 5E-1 rule, severe
(`>=75%`) break-even or trailing dormancy, explicit trailing with zero activation
despite feasible opportunities, or any accepted static-reachability defect.
Identity, attestation, worker-contract, reserved-evidence, or matrix faults are
immediate operational stop conditions.

Even a passing modest campaign does not authorize candidate promotion or a large
search. The complete smaller-result shape must be reviewed first.
