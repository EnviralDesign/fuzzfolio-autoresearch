"""No-market admission harness for temporal management topology changes.

This module is intentionally an AutoResearch-owned *diagnostic*, not a new
search authority.  It compares the legacy serial management cursor with the
new ``EvolvableModuleGenomeV1`` compiler's shared-position hub.  Both sides
delegate all graph, replay, cost, and attribution semantics to the Dashboard
native core through its public Python API.

The fixture is deliberately small and synthetic.  It proves the topology seam
can keep one position and one next-open effect while making separately authored
management regions live.  It is not market evidence and must never be used as
an economic promotion result.
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .evolvable_module_genome import (
    EffectKind,
    EvolvableModuleCompilerV1,
    EvolvableModuleGenomeV1,
    GenomeEdgeV1,
    GenomeNodeV1,
    ResourceKind,
    ResourcePoolV1,
    ResourceUse,
    Zone,
)
from .temporal_search import canonical_sha256


MANAGEMENT_TOPOLOGY_AB_SCHEMA = "temporal_management_topology_ab_v1"
MANAGEMENT_TOPOLOGY_FIXTURE_ID = "serial_vs_shared_hub_management_v1"
DEFAULT_DASHBOARD_CORE_PYTHON = Path(
    r"C:\repos\Trading-Dashboard\compute-service\.venv\Scripts\python.exe"
)


def _transition(
    transition_id: str,
    source: str,
    destination: str,
    guard: Mapping[str, Any],
    *,
    event_class: str = "decision",
    priority: int = 10,
    actions: list[dict[str, Any]] | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    return {
        "id": transition_id,
        "sourceStateId": source,
        "destinationStateId": destination,
        "eventClass": event_class,
        "priority": priority,
        "guard": dict(guard),
        "actions": actions or [],
        "reasonCode": reason or transition_id,
    }


def _clock_window(minute: int) -> dict[str, Any]:
    """A one-minute completed-bar decision window in the synthetic stream."""

    return {"kind": "utc_time_window", "startMinute": minute, "endMinute": minute + 1}


def _position_window(minute: int) -> dict[str, Any]:
    return {
        "kind": "all",
        "guards": [
            {"kind": "position_exists", "expected": True},
            _clock_window(minute),
        ],
    }


def _management_plan() -> dict[str, Any]:
    # No bar in the fixture reaches the protection price.  That lets the two
    # topologies share the same entry/exit economics while still exercising
    # stop, target, and explicit trailing mutations.
    return {
        "id": "fixture_plan",
        "initialStop": {"kind": "fixed_percent", "percent": 1.0},
        "initialTarget": {"kind": "reward_multiple", "multiple": 4.0},
        "trailingStop": {
            "anchor": {"kind": "bar_close"},
            "distance": {"kind": "fixed_initial_r", "multiple": 2.0},
            "activation": {"kind": "explicit"},
            "minimumStepInitialR": 0.0,
        },
    }


def _common_profile_shell(*, name: str, states: list[str], transitions: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "version": "v2",
        "name": name,
        "description": "Synthetic no-market management-topology admission fixture.",
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "indicators": [],
        "executionConfig": {
            "managementLibrary": {
                "version": "temporal_management_v1",
                "defaultPlanId": "fixture_plan",
                "plans": [_management_plan()],
            }
        },
        "graph": {
            "kind": "temporal_graph_v1",
            "semanticPolicy": "temporal_graph_semantics_v1",
            "eventSchema": "temporal_event_v1",
            "factLibrary": "temporal_market_facts_v1",
            "guardLibrary": "temporal_guards_v1",
            "actionLibrary": "temporal_market_actions_v1",
            "clockRequirement": "clock.completed_bar",
            "fidelityRequirements": ["data.completed_ohlc"],
            "initialStateId": "flat",
            "states": [{"id": state} for state in states],
            "evidenceGroups": [],
            "eventBindings": [],
            "transitions": transitions,
        },
    }


def build_serial_management_profile() -> dict[str, Any]:
    """Current-style one-shot cursor with the exact fixture actions/windows.

    Once a management request has completed, the cursor proceeds to the next
    serial stage.  This is valid and deterministic, but it cannot reconsider a
    prior exit/management region from a common live-position state.
    """

    transitions = [
        _transition(
            "serial_enter",
            "flat",
            "entry_pending",
            {
                "kind": "all",
                "guards": [
                    {"kind": "position_exists", "expected": False},
                    _clock_window(5),
                ],
            },
            actions=[{"kind": "enter_next_open", "managementPlanId": "fixture_plan"}],
        ),
        _transition("serial_entry_filled", "entry_pending", "serial_be", {"kind": "execution_status_is", "status": "filled"}, event_class="execution"),
        _transition("serial_request_be", "serial_be", "serial_be_pending", _position_window(10), actions=[{"kind": "move_stop_to_break_even_next_open"}]),
        # The legacy cursor has one successful protection route: after a BE
        # application it moves onto its exit-only continuation.  Its other
        # management stages are still structurally valid fallback routes when
        # a preceding request is rejected/canceled, but they are not live on
        # the successful management path exercised by this fixture.
        _transition("serial_be_applied", "serial_be_pending", "serial_exit", {"kind": "execution_status_is", "status": "applied"}, event_class="execution"),
        _transition("serial_be_rejected", "serial_be_pending", "serial_tighten", {"kind": "execution_status_is", "status": "rejected"}, event_class="execution", priority=20),
        _transition("serial_be_canceled", "serial_be_pending", "serial_tighten", {"kind": "execution_status_is", "status": "canceled"}, event_class="execution", priority=30),
        _transition("serial_request_tighten", "serial_tighten", "serial_tighten_pending", _position_window(15), actions=[{"kind": "tighten_stop_next_open", "stopLocator": {"kind": "initial_r_multiple", "multiple": 0.0}}]),
        _transition("serial_tighten_applied", "serial_tighten_pending", "serial_target", {"kind": "execution_status_is", "status": "applied"}, event_class="execution"),
        _transition("serial_tighten_rejected", "serial_tighten_pending", "serial_target", {"kind": "execution_status_is", "status": "rejected"}, event_class="execution", priority=20),
        _transition("serial_tighten_canceled", "serial_tighten_pending", "serial_target", {"kind": "execution_status_is", "status": "canceled"}, event_class="execution", priority=30),
        _transition("serial_request_target", "serial_target", "serial_target_pending", _position_window(20), actions=[{"kind": "set_target_next_open", "targetLocator": {"kind": "reward_multiple", "multiple": 1.5}}]),
        _transition("serial_target_applied", "serial_target_pending", "serial_trailing", {"kind": "execution_status_is", "status": "applied"}, event_class="execution"),
        _transition("serial_target_rejected", "serial_target_pending", "serial_trailing", {"kind": "execution_status_is", "status": "rejected"}, event_class="execution", priority=20),
        _transition("serial_target_canceled", "serial_target_pending", "serial_trailing", {"kind": "execution_status_is", "status": "canceled"}, event_class="execution", priority=30),
        _transition("serial_request_trailing", "serial_trailing", "serial_trailing_pending", _position_window(25), actions=[{"kind": "activate_trailing_stop_next_open"}]),
        _transition("serial_trailing_applied", "serial_trailing_pending", "serial_exit", {"kind": "execution_status_is", "status": "applied"}, event_class="execution"),
        _transition("serial_trailing_rejected", "serial_trailing_pending", "serial_exit", {"kind": "execution_status_is", "status": "rejected"}, event_class="execution", priority=20),
        _transition("serial_trailing_canceled", "serial_trailing_pending", "serial_exit", {"kind": "execution_status_is", "status": "canceled"}, event_class="execution", priority=30),
        _transition("serial_request_exit", "serial_exit", "exit_pending", _position_window(30), actions=[{"kind": "exit_next_open"}]),
        _transition("serial_exit_closed", "exit_pending", "done", {"kind": "execution_status_is", "status": "closed"}, event_class="execution"),
        _transition("serial_protective_closed_be", "serial_be", "done", {"kind": "execution_status_is", "status": "closed"}, event_class="execution", priority=1),
        _transition("serial_protective_closed_tighten", "serial_tighten", "done", {"kind": "execution_status_is", "status": "closed"}, event_class="execution", priority=1),
        _transition("serial_protective_closed_target", "serial_target", "done", {"kind": "execution_status_is", "status": "closed"}, event_class="execution", priority=1),
        _transition("serial_protective_closed_trailing", "serial_trailing", "done", {"kind": "execution_status_is", "status": "closed"}, event_class="execution", priority=1),
        _transition("serial_protective_closed_exit", "serial_exit", "done", {"kind": "execution_status_is", "status": "closed"}, event_class="execution", priority=1),
    ]
    states = [
        "flat", "entry_pending", "serial_be", "serial_be_pending", "serial_tighten",
        "serial_tighten_pending", "serial_target", "serial_target_pending", "serial_trailing",
        "serial_trailing_pending", "serial_exit", "exit_pending", "done",
    ]
    return _common_profile_shell(name="serial management topology fixture", states=states, transitions=transitions)


def build_shared_hub_management_genome() -> EvolvableModuleGenomeV1:
    """The same actions as the serial fixture through independent hub regions."""

    resources = ResourcePoolV1(management_refs=(_management_plan(),))
    use_plan = ResourceUse(ResourceKind.MANAGEMENT_REF, "fixture_plan")
    nodes = (
        GenomeNodeV1("start", Zone.ENTRY, "start", _clock_window(5)),
        GenomeNodeV1("entry", Zone.ENTRY, "entry", resources=(use_plan,)),
        GenomeNodeV1("hub", Zone.POSITION, "position_hub"),
        GenomeNodeV1("break_even", Zone.MANAGEMENT, "break_even", _clock_window(10)),
        GenomeNodeV1("tighten", Zone.MANAGEMENT, "tighten", _clock_window(15)),
        GenomeNodeV1("target", Zone.MANAGEMENT, "target", _clock_window(20)),
        GenomeNodeV1(
            "trailing",
            Zone.MANAGEMENT,
            "trailing",
            {"kind": "any", "guards": [_clock_window(25), _clock_window(30)]},
        ),
        # At minute 30 the independent fixture deliberately makes exit and a
        # stale trailing region true together.  Exit priority deterministically
        # wins; the observer records the shadowed trailing branch.
        GenomeNodeV1("exit", Zone.EXIT, "time_exit", _clock_window(30)),
    )
    edges = (
        GenomeEdgeV1("start_entry", "start", "entry", priority=10, effect=EffectKind.ENTER),
        GenomeEdgeV1("entry_hub", "entry", "hub", priority=10),
        GenomeEdgeV1("hub_break_even", "hub", "break_even", priority=20, effect=EffectKind.BREAK_EVEN),
        GenomeEdgeV1("hub_tighten", "hub", "tighten", priority=30, effect=EffectKind.TIGHTEN_STOP),
        GenomeEdgeV1("hub_target", "hub", "target", priority=40, effect=EffectKind.SET_TARGET),
        GenomeEdgeV1("hub_trailing", "hub", "trailing", priority=50, effect=EffectKind.ACTIVATE_TRAILING),
        GenomeEdgeV1("hub_exit", "hub", "exit", priority=10, effect=EffectKind.EXIT),
    )
    return EvolvableModuleGenomeV1("long", resources, nodes, edges)


def build_shared_hub_management_profile() -> dict[str, Any]:
    return EvolvableModuleCompilerV1().compile(
        build_shared_hub_management_genome(), candidate_id="management-topology-ab"
    )["profile"]


# This runs in the Dashboard compute-service environment, so it consumes the
# native public replay API rather than importing a second copy of core code into
# AutoResearch.  Keep it self-contained for the no-market admission test.
_NATIVE_RUNNER = r'''
import json, sys
from datetime import UTC, datetime, timedelta
from fuzzfolio_core.compute.deep_replay import DeepReplayCostModel
from fuzzfolio_core.temporal_graph.candidate_attribution import TemporalCandidateBehaviorAttributionObserver
from fuzzfolio_core.temporal_graph.identity import build_profile_snapshot_sha256, build_program_sha256, canonical_json
from fuzzfolio_core.temporal_graph.models import TemporalGraphProfile
from fuzzfolio_core.temporal_graph.observation_models import build_completed_bar_observation, build_observation_stream
from fuzzfolio_core.temporal_graph.sequential_replay import advance_temporal_replay, finish_temporal_replay, run_temporal_replay

payload = json.loads(sys.stdin.read())
bars = [
    (100.0, 100.2, 99.8, 100.0), # decision at 05 -> entry next open
    (100.0, 100.3, 99.8, 100.1),
    (100.4, 100.8, 100.1, 100.6), # 10 -> BE (entry filled at open)
    (100.6, 100.9, 100.6, 100.7), # 15 -> tighten; BE cannot be touched
    (100.7, 101.0, 100.6, 100.8), # 20 -> target
    (100.8, 101.1, 100.7, 100.9), # 25 -> explicit trailing
    (100.9, 101.2, 100.8, 101.0), # 30 -> exit wins / trailing shadowed
    (101.0, 101.2, 100.9, 101.0), # 35 -> exit applied
]
cost = DeepReplayCostModel(mode="research_conservative")

def stream_for(profile):
    profile_hash = build_profile_snapshot_sha256(profile)
    program_hash = build_program_sha256(profile)
    anchor = datetime(2026, 1, 5, tzinfo=UTC)
    rows = []
    for index, (open_, high, low, close) in enumerate(bars):
        start = anchor + timedelta(minutes=5 * index)
        end = start + timedelta(minutes=5)
        rows.append(build_completed_bar_observation(
            program_sha256=program_hash, instrument="EURUSD", timeframe="M5",
            bar_id=f"EURUSD:M5:{start.isoformat().replace('+00:00','Z')}",
            bar_start=start, bar_close=end, sequence=index, clock_index=index,
            open_price=open_, high_price=high, low_price=low, close_price=close,
            evidence_scores={}, fresh_events=(),
        ))
    return build_observation_stream(
        source_profile_sha256=profile_hash, resolved_profile_sha256=profile_hash,
        program_sha256=program_hash, instrument="EURUSD", base_timeframe="M5", observations=rows,
    )

def counts(observer_payload):
    transitions = observer_payload["transitionBehaviors"]
    actions = observer_payload["actionBehaviors"]
    # A compiled shared hub contains execution-status return routes in addition
    # to its authored decision regions.  Keep those two counts separate: raw
    # compiled size is useful cost/complexity evidence, while the six
    # action-bearing decision transitions are the matched liveness surface.
    authored = [row for row in transitions if row["actionKeys"]]
    return {
        "compiledTransitions": len(transitions),
        "compiledEvaluatedTransitions": sum(row["evaluatedCount"] for row in transitions),
        "compiledTrueTransitions": sum(row["trueCount"] for row in transitions),
        "compiledSelectedTransitions": sum(row["selectedCount"] for row in transitions),
        "compiledPriorityShadowedTransitions": sum(row["priorityShadowedCount"] for row in transitions),
        "authoredActionTransitions": len(authored),
        "authoredActionEvaluatedTransitions": sum(row["evaluatedCount"] for row in authored),
        "authoredActionTrueTransitions": sum(row["trueCount"] for row in authored),
        "authoredActionSelectedTransitions": sum(row["selectedCount"] for row in authored),
        "authoredActionDeadTransitions": sum(1 for row in authored if not row["selectedCount"]),
        "authoredActionPriorityShadowedTransitions": sum(row["priorityShadowedCount"] for row in authored),
        "authoredActions": len(actions),
        "scheduledActions": sum(row["scheduledCount"] for row in actions),
        "appliedActions": sum(row["appliedCount"] for row in actions),
        "rejectedActions": sum(row["rejectedCount"] for row in actions),
        "canceledActions": sum(row["canceledCount"] for row in actions),
    }

def run(label, raw):
    profile = TemporalGraphProfile.model_validate(raw)
    stream = stream_for(profile)
    full_observer = TemporalCandidateBehaviorAttributionObserver(profile, stream_sha256=stream.stream_sha256)
    full = run_temporal_replay(profile, stream, cost_model=cost, attribution_observer=full_observer)
    plain = run_temporal_replay(profile, stream, cost_model=cost)
    if canonical_json(full.model_dump(mode="json", by_alias=True)) != canonical_json(plain.model_dump(mode="json", by_alias=True)):
        raise AssertionError("attribution changed replay result")
    split_observer = TemporalCandidateBehaviorAttributionObserver(profile, stream_sha256=stream.stream_sha256)
    partial = advance_temporal_replay(profile, stream, cost_model=cost, max_observations=4, attribution_observer=split_observer)
    restored = TemporalCandidateBehaviorAttributionObserver.from_payload(profile, split_observer.payload(), stream_sha256=stream.stream_sha256, checkpoint=partial)
    resumed_checkpoint = advance_temporal_replay(profile, stream, cost_model=cost, checkpoint=partial, attribution_observer=restored)
    resumed = finish_temporal_replay(profile, stream, resumed_checkpoint, cost_model=cost)
    if canonical_json(full.model_dump(mode="json", by_alias=True)) != canonical_json(resumed.model_dump(mode="json", by_alias=True)):
        raise AssertionError("split/restart replay result mismatch")
    if full_observer.payload() != restored.payload():
        raise AssertionError("split/restart attribution mismatch")
    final_state = full.final_execution_state
    if final_state.position is not None or final_state.pending_effect is not None:
        raise AssertionError("fixture ended with an unresolved position or pending effect")
    if full.metrics.positions_opened != 1 or full.metrics.trades_closed != 1:
        raise AssertionError("fixture must preserve exactly one opened and closed position")
    if full.metrics.unresolved_position or full.metrics.unresolved_pending_effect:
        raise AssertionError("fixture must not invent terminal liquidation or leave unresolved work")
    return {
        "label": label,
        "profileSha256": build_profile_snapshot_sha256(profile),
        "programSha256": build_program_sha256(profile),
        "streamSha256": stream.stream_sha256,
        "result": full.model_dump(mode="json", by_alias=True),
        "resultCanonicalJson": canonical_json(full.model_dump(mode="json", by_alias=True)),
        "attribution": full_observer.payload(),
        "counts": counts(full_observer.payload()),
        "onePositionOneEffect": True,
        "splitRestartExact": True,
    }

serial = run("serial", payload["serial"])
shared = run("shared_hub", payload["sharedHub"])
if serial["streamSha256"] == shared["streamSha256"]:
    # The profile-bound stream identity is intentionally different.  The bar
    # geometry itself is matched below through the deterministic literals.
    raise AssertionError("profile-bound streams unexpectedly shared identity")
if serial["result"]["metrics"]["totalNetR"] != shared["result"]["metrics"]["totalNetR"]:
    raise AssertionError("matched entry/exit economics diverged")
if serial["result"]["metrics"]["totalGrossR"] != shared["result"]["metrics"]["totalGrossR"]:
    raise AssertionError("matched gross R diverged")
if shared["counts"]["appliedActions"] <= serial["counts"]["appliedActions"]:
    raise AssertionError("shared hub did not increase applied management/exit liveness")
if shared["counts"]["authoredActionDeadTransitions"] >= serial["counts"]["authoredActionDeadTransitions"]:
    raise AssertionError("shared hub did not reduce dead authored action regions")
if shared["counts"]["authoredActionPriorityShadowedTransitions"] < 1:
    raise AssertionError("shared hub conflict was not observable as priority-shadowed")
print(json.dumps({"serial": serial, "sharedHub": shared}, sort_keys=True, separators=(",", ":")))
'''


def run_management_topology_ab_native(
    *,
    dashboard_core_python: Path | str = DEFAULT_DASHBOARD_CORE_PYTHON,
) -> dict[str, Any]:
    """Execute the versioned, no-market A/B in the native Dashboard core.

    The returned payload is immutable in the sense relevant to this admission:
    it contains only synthetic observations and every report field is bound into
    ``reportSha256``.  Calling it again produces byte-identical output under an
    unchanged native core and compiler policy.
    """

    executable = Path(dashboard_core_python)
    if not executable.is_file():
        raise FileNotFoundError(f"Dashboard core Python is unavailable: {executable}")
    input_payload = {
        "fixtureId": MANAGEMENT_TOPOLOGY_FIXTURE_ID,
        "serial": build_serial_management_profile(),
        "sharedHub": build_shared_hub_management_profile(),
    }
    completed = subprocess.run(
        [str(executable), "-c", _NATIVE_RUNNER],
        input=json.dumps(input_payload, sort_keys=True, separators=(",", ":")),
        text=True,
        capture_output=True,
        check=False,
        timeout=120,
    )
    if completed.returncode:
        raise RuntimeError(
            "native management-topology admission failed:\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    try:
        native = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("native management-topology admission returned invalid JSON") from exc
    report = {
        "schemaVersion": MANAGEMENT_TOPOLOGY_AB_SCHEMA,
        "fixtureId": MANAGEMENT_TOPOLOGY_FIXTURE_ID,
        "evidence": {
            "kind": "synthetic_completed_bar_observations",
            "marketDataUsed": False,
            "barCount": 8,
            "matchedEntryExitEconomics": True,
            "costModel": "research_conservative",
        },
        "serial": native["serial"],
        "sharedHub": native["sharedHub"],
        "admission": {
            "onePositionOnePendingEffect": True,
            "deterministicConflictResolution": True,
            "byteExactReplayAndRestart": True,
            "noInventedLiquidation": True,
            "independentRegionsMateriallyIncreaseLiveness": True,
            "semanticContradictions": [],
        },
    }
    report["reportSha256"] = canonical_sha256(report)
    return report


def write_management_topology_ab_report(
    output_path: Path | str,
    *,
    dashboard_core_python: Path | str = DEFAULT_DASHBOARD_CORE_PYTHON,
) -> dict[str, Any]:
    """Materialize one immutable no-market report without overwriting evidence.

    Rewriting the exact same canonical payload is idempotent.  A different
    payload at an existing path is refused instead of being silently replaced.
    """

    target = Path(output_path)
    report = run_management_topology_ab_native(
        dashboard_core_python=dashboard_core_python
    )
    serialized = json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n"
    if target.exists():
        existing = target.read_text(encoding="utf-8")
        if existing != serialized:
            raise FileExistsError(
                "management-topology A/B report already exists with different immutable content"
            )
        return report
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(serialized, encoding="utf-8")
    temporary.replace(target)
    return report


__all__ = [
    "DEFAULT_DASHBOARD_CORE_PYTHON",
    "MANAGEMENT_TOPOLOGY_AB_SCHEMA",
    "MANAGEMENT_TOPOLOGY_FIXTURE_ID",
    "build_serial_management_profile",
    "build_shared_hub_management_genome",
    "build_shared_hub_management_profile",
    "run_management_topology_ab_native",
    "write_management_topology_ab_report",
]
