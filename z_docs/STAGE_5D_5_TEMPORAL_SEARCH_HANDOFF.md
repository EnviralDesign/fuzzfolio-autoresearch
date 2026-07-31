# Stage 5D-5 Temporal Search Handoff

`temporal-search-authority` freezes a finite, immutable candidate/window matrix.
It does not generate, mutate, or promote profiles, and it rejects any development
window that overlaps the explicitly listed protected or reserved evidence.

Each authority candidate supplies a content-bound source-profile snapshot and one
v2 replay-evidence plan for every declared development window.  The plan must
bind that exact candidate snapshot, the exact window, instrument/timeframe, and
Lake binding.  A plan cannot be reused across candidates merely because the Lake
window semantic identity is shared.

The only worker unit is one `temporal_graph_candidate_window` task for one
candidate and one development window. The worker contract itself fixes the two
cost views, `research_conservative` and `none`; the job cannot add, remove, or
rename them. The worker builds one shared completed-bar observation stream before
evaluating either. Result acceptance requires
`temporal_graph_candidate_window_result_v1` and the same exact
`observation_stream_sha256` in both keyed cost-view results.

The authority binds the exact raw candidate document with
`sourceProfileSha256`. The worker independently resolves and normalizes that
document and returns the core's semantic source-profile identity. Keeping those
identities separate prevents a controller-side approximation of catalog
hydration from becoming an accidental second profile implementation.

The controller alone owns task generation, validation, checkpoint/journal,
resume, deduplication, immutable materialization, and basic finite selection.
Workers only evaluate immutable jobs.  The authority explicitly disallows a
mutation engine and long economic search; this is a bounded admission handoff.

Required worker capabilities are:

- `temporal_graph_candidate_window_v1`
- `temporal_graph_replay_v1`
- `management.scalar.price_level.completed_bar`
- `management.scalar.price_distance.completed_bar`
- `management.initial.dynamic`
- `management.trailing.indicator`
- `management.action.dynamic`

The local Procman Normal Operations group contains the Lab Gateway, the existing
`start-local-lab-ws-worker.ps1` local worker, fresh/resume temporal-search
controls, the read-only authority audit, and the dashboard.  The old Phase 3
Fresh/Resume/Audit Procman entries were removed only from the local topology;
historical implementation and evidence remain untouched. The four obsolete
Phase 3 ephemeral-worker generator controls were also removed from Procman; they
targeted an authority and worker contract that are not interchangeable with this
temporal-search task.

For the canonical admission preflight, build exactly one ATR-management
candidate from an admitted Stage 5C v2 development request:

```text
temporal-search-prepare-preflight
  --source-task <admitted-development-request.json>
  --output-root <ignored-authority-preparation-root>
  --authority-label <label>
  --worker-contract-sha256 <exact-current-contract>
  --prohibited-window-start <reserved-start>
  --prohibited-window-end <reserved-end>
  --prohibited-reason <reason>
  --confirm-non-reserved-development-window
```

The builder adds one catalog-backed `ATR_VOLATILITY_FILTER.atr_raw`
price-distance binding, a two-ATR initial stop, a two-R target, and a completed-bar
two-ATR trailing distance. It preserves the admitted Lake binding, rotates the
v2 evidence plan onto the new raw profile identity, clears the legacy execution
cell identity, and refuses divergent immutable outputs.

Before a real bounded run, create a fresh authority from the verified candidate
snapshots and per-candidate evidence plans, run `temporal-search-authority
--audit`, then use the Procman fresh control.  Do not start it merely to test
configuration; `temporal-search --fresh --plan-only` is the no-gateway
materialization preflight.
