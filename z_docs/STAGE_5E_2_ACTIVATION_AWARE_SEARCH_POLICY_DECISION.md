# Stage 5E-2 activation-aware search-policy decision

Status: implemented and admitted on repository-only synthetic evidence; awaiting
review before Stage 5E-3 may define fresh windows or launch a modest campaign.

## Decision

Stage 5E-0's worker, Gateway, finite-controller, checkpoint, and artifact
contracts remain admitted. Stage 5E-1's calibration method remains admitted.
Generator v1 remains byte-for-byte reproducible, but its `de_novo` label is now
recognized as inaccurate: both source modes mutate known seeds. Selector v1
remains retired from campaign use because it adversely enriched B/D outcomes.

Stage 5E-2 admits two explicitly versioned successors:

- `temporal_discovery_generator_v2_activation_aware`;
- `temporal_discovery_selector_v2_robust_envelope`.

This does not admit new trading evidence. It admits the proposal and selection
policy to be tested later on fresh, non-reserved development windows.

## Historical causal result

The causal audit consumed the exact 818 immutable Stage 5E-0/5E-1 task results:
512 A/C screening results, 178 selected B/D confirmation results, and 128
deterministic-control B/D results. It did not rescore candidates.

| Capability | Authored | Activated | Changed closure |
| --- | ---: | ---: | ---: |
| Break-even | 62 | 15 | 3 |
| Trailing stop | 190 | 110 | 52 |

The 47 dormant or rejected break-even instances split into 13 whose source state
was never occupied, 27 whose guard was evaluated but never true, and 7 whose
scheduled effect was rejected. The immutable traces contained 23
`stop_not_tightened` and 13 `stop_not_protective_at_open` rejections.

The corrected trailing interpretation matters. Immediate trailing is installed
atomically at entry, so 37 of the 46 immediate plans were active. The previous
trace-only summary undercounted those entry-atomic activations. In contrast, all
20 v1 explicit trailing plans were genuinely orphaned: none contained a reachable
activation action and none activated.

The frozen causal identities are:

```text
report:   sha256:48d10e30a1f7cbb0c58ec7045d052ff393876762ab31e19641aef892329b474e
manifest: sha256:d1c6313ed6c55189790b54397819c6724fdc1926a5cb45acf31c0859fe34babd
dossiers: sha256:5858cb60ee15f957dc24a2d294df4990117cd9af7b3702332fb073a1ac43aea2
```

## Generator v2

The honest source modes are now `broad_seed_mutation` and `seed_derived`, 128
programs each. The generator remains seed-based; it does not claim compositional
de-novo synthesis.

Every accepted candidate must pass both the AutoResearch static policy and the
unchanged FuzzFolio validator. The static policy requires:

- every entry route to resolve one real management plan;
- no orphan plans;
- a fill transition and a post-entry path to each management action;
- no unreachable or statically dominated management action;
- every explicit trailing plan to have a complete request/applied/rejected route;
- every break-even action to be reachable after entry;
- break-even at 0.5R before automatic or explicit trailing at 1.0R when both are
  authored;
- enough target runway for the authored activation threshold.

The real validator accepted 256 unique programs from 282 proposals. It rejected
23 fail-closed and identified 3 duplicate programs. The accepted population has
70 break-even capabilities, 170 trailing capabilities, and 25 explicit trailing
activation actions across 21 explicit plans. Static reachability issues are zero.

Each of the 240 authored capabilities has a positive native synthetic witness.
Break-even and trailing also have one negative rejection witness apiece. All 242
witnesses reproduced identical result and checkpoint hashes after a serialized
mid-stream restart.

```text
config:      sha256:7ddccbae1e77f21ed7367d29dd485bc89ca9dc1b713cadb4ab08b9bf4d53de07
population:  sha256:7e9adc0c543980bca81426231f10febdbcefc4413d7a58dc0fbe28af01ca72d1
journal:     sha256:312ff217e1d91c14c3071a1a02a8882c50331a82efd50b0fd8d4534d8b45587e
witnesses:   sha256:0a7d2c2f628f7b00a432d370ce28c0905740cec82a72e7c4489b315c8a3b2774
determinism: sha256:3ef2d30b6f913146f883e9cd37470d3af2f649baad120f28438cbfe311fc682e
```

An ordinary repeat and `PYTHONHASHSEED=1..5` reproduced the exact config,
population, journal, candidate count, and proposal count from the native
validation ledger.

## Selector v2

Selector v2 derives a screening-only robust envelope before Pareto or novelty
selection. A candidate must trade in every screening window and satisfy all four
active-population thresholds:

- total conservative R at least the active-population median;
- worst-window conservative R at least the active-population median;
- maximum drawdown no greater than active-population p75;
- cost drag per trade no greater than active-population p75.

If fewer than 32 candidates qualify, selection fails closed. It never relaxes a
threshold to fill an archive.

Within the envelope, selector v2 has a 32-candidate economic Pareto archive and
a 32-candidate admissible novelty archive. An unrestricted 32-candidate pure
novelty archive is diagnostic-only and cannot promote a candidate. Management
activation and rejected-intent measures are only deterministic tie-breakers;
neither is an eligibility gate.

The selected union is capped at 64. A separate exactly-32 candidate control uses
the admitted Stage 5E-1 stratified hash and reads only candidate ID, source mode,
and seed ID. It cannot read economics or source-profile content. Selected plus
control is capped at 96.

The 256-candidate synthetic admission produced 78 robust-eligible candidates, a
48-candidate selected union, a 32-candidate control, and an 80-candidate
confirmation union. Those numbers and thresholds are synthetic policy evidence,
not claims about market performance. Original order, reversed order, five shuffled
orders, and `PYTHONHASHSEED=1..5` were byte-identical.

```text
selection: sha256:4d8c258a4df69245ae4b1ae5e9c09d0c7b5f41211683c6df07201858d6de444c
report:    sha256:c9bece8b7124df11f1ab4a0c98b46336d8626e7bc7fefdc64d85fe3237c6f8e9
manifest:  sha256:30657a8abeed28bfc88fed474e3c49a589b9e0d9c56eed667373bcfc5c153068
```

## Permanent pre-scale boundary

Before any large distributed search, Codex must stop and give the user a deep
checkpoint covering:

- activity and inactivity;
- cohort and window distributions;
- management activation, dormancy, and rejection causes;
- cost drag and cost per trade;
- screening-to-confirmation stability;
- generalization signals;
- the exact hypotheses, risks, and stop conditions of scaling.

Stage 5E-2 defined no fresh window, evidence plan, authority, Gateway task, or
distributed campaign. Stage 5E-3 remains blocked pending explicit review.
