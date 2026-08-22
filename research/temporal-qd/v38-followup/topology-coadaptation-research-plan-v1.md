# Topology co-adaptation research plan v1

Experiment-only. Do not launch in this change. Production rotating 4/5 breeding must omit `topologyCoadaptationMatrix`. The overlay is rejected on the front generation path.

## Why this experiment exists

V38 pooled 14 typed topology operations at family level. Accepted topology was not uniformly destructive:

- `insert_setup` median parent-relative net R **+1.45** (9 accepted).
- `insert_timeout_rearm` median **0.0** with 0 parent losses (8 accepted).
- `insert_exit_region` median **-25.7** (6 accepted).
- `insert_management_region` median **-0.35** and 6 parent losses (18 accepted).
- 70/160 topology slots collapsed to `duplicate_pair_genome` and have no fast-ephemeral plan body.

A raw topology mutation can break a co-adapted controller. That is the body/brain split: topology is morphology; resources, guards, timing, hold, and protection are controller. V38 judged topology-only children immediately, so this experiment asks whether a bounded deterministic resource/indicator-parameter settling phase recovers raw topology damage without inventing a second executor.

## Four arms per frozen parent × exact topology plan

1. Exact parent clone, re-evaluated on the frozen panel.
2. Topology-only child from one exact plan (no mixed operations).
3. Resource/parameter-only control using the same local adaptation budget (no topology change).
4. The same topology child, then at most `maxResourceSteps` deterministic resource plans.

Keep raw topology identity and post-settling identity both. Score on the identical development panel, then require independent-panel confirmation before any production conclusion.

## Measurements

- raw topology damage versus parent
- recovery after bounded settling
- improvement beyond the resource-only control
- worst-window risk
- behavioral novelty and route occupancy
- entry/exit liveness
- action mix and management loops
- complexity and duplicate rate
- cross-panel survival

Success is not “novel.” A topology lane is promising only if raw topology or topology-plus-settling has a repeatable positive parent-relative tail, does not systematically worsen worst-window risk, and survives independent evidence.

## Morphology nursery (side archive)

Minimum native validity and behavioral liveness. Novelty/coverage descriptors for stepping stones. Local competition among similar topology/behavior. No production breeding rights. Bounded lifetime and deterministic eviction. No retention merely to keep the archive full. Strictly separate from the production quality archive.

## Isolation already implemented

- Python: `autoresearch/temporal_qd_topology_coadaptation.py`
- Rust parse-only: `qd-kernel/src/topology_coadaptation_matrix.rs`
- Front path rejects the overlay: `generation.rs`
- Tests prove the overlay is inert when absent and cannot enable production archive writes

Do not wire this overlay into the live supervisor in this change.
