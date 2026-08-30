# Fable review handoff

## Review target

Review the Stage 4.5A claim boundary, source bindings, and next-experiment
design. Do not treat this packet as authorization to run market work or alter
production code/policy.

## Material assertions to audit

1. The ledger's 175 records remain source-bound and separate runtime support
   from grammar/seed/mutation reachability.
2. The 15 listed gaps correctly distinguish confirmed direct-route gaps from
   strong hypotheses and unavailable frozen-authority facts.
3. The historical coverage report uses `unavailable` rather than inferring
   authored/compiled/activated/reduced/selected states from source.
4. The reference-organism labels accurately match only the cited
   construct/compile contracts; no behavioral or economic conclusion leaks in.
5. The proposed J1/J2/J3 protocol is genuinely no-market and does not create
   a replacement constructor, learned prior, archive mutation, or worker
   topology.

## Concrete questions

- Is any claimed direct grammar route actually only runtime vocabulary, or the
  reverse?
- Are the proposed smallest missing primitives truly local, or do they hide a
  coordinated authority/validator/runtime dependency?
- Does the one-step-neighbourhood classification need one more non-economic
  observable before J2, while still avoiding component scoring?
- Which single missing semantic should be the first candidate for a future
  narrow change after the source-only reachability audit—if any?
- Is the frozen-authority fixture request sufficient to unblock the pilot, or
  should the pilot remain design-only?

## Review boundaries

Accepted V4 remains authoritative: no universal context-free component score;
parent/suppression roles are underidentified without route/site context. This
packet neither revisits that conclusion nor introduces a V5 cycle.

## Source pointers

- Grammar registry and compiled graph: `rust/temporal-qd/crates/qd-kernel/src/grammar.rs`
- V5 family selection/admission: `rust/temporal-qd/crates/qd-kernel/src/v5_operators.rs`
- V5 topology operations: `rust/temporal-qd/crates/qd-kernel/src/v5_topology_operators.rs`
- Frozen bridge/receipt seam: `autoresearch/temporal_qd_v5_native.py`
- Runtime guard/action/kernel/compiler: FuzzFolio
  `shared/python/fuzzfolio_core/fuzzfolio_core/temporal_graph/{guards,action_models,kernel,bidirectional_compiler}.py`
- Generated inventory: [capability-ledger.json](capability-ledger.json)
- Gaps and no-market designs: [gap-matrix.json](gap-matrix.json),
  [static-pilot-and-neighborhood-design.md](static-pilot-and-neighborhood-design.md),
  [ground-zero-delta-atlas.md](ground-zero-delta-atlas.md)
