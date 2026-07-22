# PlayHand / Atlas speed & memory simplification

## Goal
Cut lab-campaign coordinator I/O and memory pressure (journal/state amplification) and remove dead parallel surfaces, without changing research scoring semantics. No commit/push; user reviews locally.

## Non-goals
- Changing Phase 3 seed-plan / authority semantics
- Deleting standalone atlas CLI builders (fleet still uses them)
- Migrating in-flight campaign journals in place (new schema; resume across versions rejected)
- Portfolio / dashboard UI work beyond catalog scan routing for efficiency report

## Current evidence
| Fact | Source | Confidence |
|---|---|---|
| Live campaign journal 250MB, state 124MB | `runs/derived/play-hand-lab-campaigns/phase3-darwin-rich-ab-v2/` | high |
| `apply_batch` full rewrite + fsync | `durable_execution.py:184-250` | high |
| Profile snapshots duplicated per shard | `play_hand_lab.py` task build ~4284+ | high |
| `play-hand-massive-v2` == lab alias | `play_hand_lab_cli.py` | high |
| v1 massive unused by fleet/gateway scripts | processes.json + start-loopback script | high |
| Forward stage re-parses raw signal JSON | `forward_response_atlas.py` + atlas scout | high |

## Frozen decisions
- Product behavior: scoring, stage advancement, receipt validation semantics unchanged.
- Architecture: journal becomes append-only JSONL (`autoresearch-durable-execution-v2`); old v1 journals fail closed on resume with clear error. State file omits bloated `task_specs` payloads / profile duplicates where rebuildable.
- Compatibility: no silent resume of v1 journals; operators start fresh or finish old campaigns on old code.
- Deletions: soft-deprecate `play-hand-massive` (v1) CLI → warn + refuse or redirect documented; prefer remove dead shims first; hard-delete v1 module only if tests can be retired cleanly without breaking lab aliases.
- Atlas: keep standalone CLIs; optimize shared durable stage receipts + forward path.

## Acceptance criteria
- [ ] Journal commit cost scales with batch size, not full journal size (unit tests prove append path)
- [ ] Lab campaign state/task payloads no longer embed N copies of the same profile per shard
- [ ] Lab result recording uses append (or single rewrite) not double full load+rewrite per result
- [ ] Interactive play-hand batches progress renders (dirty flag / interval) like lab
- [ ] Existing durable + play_hand_lab + atlas_lab unit tests pass (or updated for v2)
- [ ] No git commit/push

## Execution packages
| ID | Objective | Worker | Deps | Exclusive write set |
|---|---|---|---|---|
| P6a | Remove dead shims / soft-deprecate v1 massive CLI | bounded | — | `__main__.py`, `play_hand.py` (shim only), `pyproject.toml` (optional), docs not required |
| P3 | Append attempts + batched progress render | bounded | — | `play_hand.py`, `play_hand_lab.py` (attempts/progress only) |
| P2 | Content-addressed / single profile per lane shards | bounded | — | `play_hand_lab.py` (task build + state payload) |
| P1 | Append-only durable journal v2 | senior | P2 preferred | `durable_execution.py`, consumers in `play_hand_lab.py` / `atlas_lab.py` / tests |
| P4 | Forward events at signal time + trim raw from receipts | bounded | — | `atlas_lab.py`, `forward_response_atlas.py`, tests |
| P5 | Efficiency report via SQLite catalog | bounded | — | `playhand_efficiency.py`, tests |

## Verification
- `uv run pytest tests/test_durable_execution.py tests/test_play_hand_lab.py tests/test_play_hand.py tests/test_atlas_lab.py tests/test_forward_response_atlas.py tests/test_playhand_efficiency.py -q --tb=line`
- lean-verifier after P1+P2

## Risks and rollback
- Journal schema change breaks resume of live campaigns → fail closed; document.
- Soft-deprecate massive may surprise scripts → clear error message.
- Rollback: revert working tree files; no push.
