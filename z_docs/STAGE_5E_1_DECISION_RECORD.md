# Stage 5E-1 decision record

## Admission decision

- Stage 5E-0 distributed search machinery: **admitted**.
- Stage 5E-1 deterministic calibration and control method: **admitted**.
- Generator v1: behaviorally diverse but activation-deficient.
- Selector v1 (`pareto_economic_plus_greedy_novelty`): **retired from campaign
  use** because it produced adverse confirmation enrichment.
- FuzzFolio evaluator and worker contract: admitted and unchanged.
- Windows A-D: exhausted for repaired-policy validation.
- Broader search: blocked.

The frozen primary architecture finding is `management_activation_gap`.  The
independent policy finding is that selector v1 selected a cohort materially
worse than the deterministic control.  The classifier's precedence rule does
not weaken or excuse the selector failure.

## Stage 5E-2 repair boundary

Stage 5E-2 is repository-only work.  It may change AutoResearch.  FuzzFolio
must remain frozen at `8744c7dcc726100f91dca68ab4d5e0f2ee9c2b69` unless a
causal audit demonstrates a genuine evaluator defect; such a finding requires
an explicit stop before any FuzzFolio change.

Stage 5E-2 must:

1. derive an exact management-activation causal taxonomy from the existing
   immutable 818 task results (512 screening, 178 selected confirmation, and
   128 deterministic control results);
2. preserve generator and selector v1 for exact Stage 5E-0 reproduction;
3. replace the misleading v1 `de_novo` source-mode name with honest v2 source
   semantics, either `broad_seed_mutation` or true compositional generation;
4. require static reachability and bounded synthetic activation/rejection
   witnesses for every authored v2 management capability;
5. admit a deterministic 256-program no-market generator batch with unique
   program identities, no orphaned management resources, no statically
   unreachable actions, full witness coverage, and exact restart behavior;
6. introduce a transparent selector v2 with a deterministic robust eligibility
   envelope, separate promotion and diagnostic archives, and a stratified
   embedded unselected control;
7. prove selector output byte-identical under original, reversed, at least five
   shuffled input orders, and `PYTHONHASHSEED` values 1 through 5;
8. stop before defining fresh market windows or constructing a distributed
   execution authority.

## Permanent pre-scale review boundary

Before any later substantial distributed search, stop for a deep human review
of the smaller-run evidence shape.  At minimum the checkpoint must cover:

- candidate and per-window activity versus inactivity;
- cohort distributions rather than only medians;
- activation, dormancy, rejection, and reachability causes;
- trade counts, holding behavior, drawdown, cost drag, and cost sensitivity;
- screening-to-confirmation stability and known generalization signals;
- zero-trade and sparse-trade sensitivity;
- the precise hypotheses, expected information gain, failure conditions, and
  compute scale of the proposed run.

No large run is authorized merely because Stage 5E-2 code or synthetic proofs
pass.  Stage 5E-2 itself ends at an admission checkpoint for review.

