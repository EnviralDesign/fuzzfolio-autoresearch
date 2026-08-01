"""Run bounded Stage 5E-2 management witnesses in the FuzzFolio environment.

This script intentionally lives in AutoResearch but is executed with the
FuzzFolio core project.  It reads only the repository-generated population and
uses synthetic completed bars; it has no Gateway or market-data client.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from fuzzfolio_core.temporal_graph.identity import (
    build_profile_snapshot_sha256,
    build_program_sha256,
    canonical_sha256,
)
from fuzzfolio_core.temporal_graph.models import TemporalGraphProfile
from fuzzfolio_core.temporal_graph.observation_models import (
    build_completed_bar_observation,
    build_observation_stream,
)
from fuzzfolio_core.temporal_graph.replay_models import TemporalReplayCheckpoint
from fuzzfolio_core.temporal_graph.sequential_replay import (
    advance_temporal_replay,
    finish_temporal_replay,
    run_temporal_replay,
)


WITNESS_SET_SCHEMA = "temporal_policy_v2_management_witness_set_v1"
WITNESS_SCHEMA = "temporal_policy_v2_management_witness_v1"
WITNESS_MANIFEST_SCHEMA = "temporal_policy_v2_management_witness_manifest_v1"


def _clone(value: Any) -> Any:
    return json.loads(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    )


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return _clone(value)


def _encoded(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        indent=2,
        sort_keys=True,
        ensure_ascii=True,
        allow_nan=False,
    ) + "\n"


def _write_immutable(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = _encoded(value)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise ValueError(f"refusing to overwrite divergent witness: {path}")
    path.write_text(encoded, encoding="utf-8")


def _file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def _transition(
    transition_id: str,
    source: str,
    destination: str,
    event_class: str,
    guard: dict[str, Any],
    *,
    actions: list[dict[str, Any]] | None = None,
    priority: int = 10,
) -> dict[str, Any]:
    return {
        "id": transition_id,
        "sourceStateId": source,
        "destinationStateId": destination,
        "eventClass": event_class,
        "priority": priority,
        "guard": guard,
        "actions": actions or [],
        "reasonCode": transition_id,
    }


def _management_library(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return _clone(candidate["sourceProfile"]["executionConfig"]["managementLibrary"])


def _plan(
    candidate: Mapping[str, Any], *, plan_id: str | None = None
) -> dict[str, Any]:
    library = _management_library(candidate)
    selected = plan_id or library["defaultPlanId"]
    return _clone(next(item for item in library["plans"] if item["id"] == selected))


def _projected_library(
    candidate: Mapping[str, Any], plan: Mapping[str, Any]
) -> dict[str, Any]:
    source = _management_library(candidate)
    output = {
        "version": source["version"],
        "defaultPlanId": plan["id"],
        "plans": [_clone(plan)],
    }
    binding_ids = set()

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            if value.get("bindingId"):
                binding_ids.add(str(value["bindingId"]))
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(plan)
    bindings = [
        item
        for item in source.get("scalarBindings") or []
        if str(item.get("id") or "") in binding_ids
    ]
    if bindings:
        output["scalarBindings"] = _clone(bindings)
    return output


def _profile(
    candidate: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    management_type: str,
    break_even_r: float = 0.5,
    negative: bool = False,
) -> TemporalGraphProfile:
    states = [
        {"id": "flat"},
        {"id": "entry_requested"},
        {"id": "open"},
        {"id": "closed"},
    ]
    transitions = [
        _transition(
            "flat_to_entry",
            "flat",
            "entry_requested",
            "decision",
            {"kind": "position_exists", "expected": False},
            actions=[
                {"kind": "enter_next_open", "managementPlanId": plan["id"]}
            ],
        ),
        _transition(
            "entry_to_open",
            "entry_requested",
            "open",
            "execution",
            {"kind": "execution_status_is", "status": "filled"},
        ),
        _transition(
            "open_to_closed",
            "open",
            "closed",
            "execution",
            {"kind": "execution_status_is", "status": "closed"},
        ),
    ]
    if management_type == "break_even":
        states.extend(
            [
                {"id": "management_requested"},
                {"id": "management_applied"},
                {"id": "management_rejected"},
            ]
        )
        transitions.extend(
            [
                _transition(
                    "open_to_management",
                    "open",
                    "management_requested",
                    "decision",
                    {
                        "kind": "all",
                        "guards": [
                            {"kind": "position_exists", "expected": True},
                            {"kind": "unrealized_r_at_least", "value": break_even_r},
                        ],
                    },
                    actions=[{"kind": "move_stop_to_break_even_next_open"}],
                ),
                _transition(
                    "management_to_applied",
                    "management_requested",
                    "management_applied",
                    "execution",
                    {"kind": "execution_status_is", "status": "applied"},
                ),
                _transition(
                    "management_to_rejected",
                    "management_requested",
                    "management_rejected",
                    "execution",
                    {"kind": "execution_status_is", "status": "rejected"},
                    priority=20,
                ),
            ]
        )
    elif management_type == "trailing_stop":
        activation = (plan.get("trailingStop") or {}).get("activation") or {}
        if activation.get("kind") == "explicit" or negative:
            states.extend(
                [
                    {"id": "management_requested"},
                    {"id": "management_applied"},
                    {"id": "management_rejected"},
                ]
            )
            transitions.extend(
                [
                    _transition(
                        "open_to_management",
                        "open",
                        "management_requested",
                        "decision",
                        {
                            "kind": "all",
                            "guards": [
                                {"kind": "position_exists", "expected": True},
                                {"kind": "position_age_at_least", "events": 1},
                            ],
                        },
                        actions=[{"kind": "activate_trailing_stop_next_open"}],
                    ),
                    _transition(
                        "management_to_applied",
                        "management_requested",
                        "management_applied",
                        "execution",
                        {"kind": "execution_status_is", "status": "applied"},
                    ),
                    _transition(
                        "management_to_rejected",
                        "management_requested",
                        "management_rejected",
                        "execution",
                        {"kind": "execution_status_is", "status": "rejected"},
                        priority=20,
                    ),
                ]
            )
    else:
        raise ValueError(f"unknown management type {management_type!r}")

    source = candidate["sourceProfile"]
    return TemporalGraphProfile.model_validate(
        {
            "version": "v2",
            "name": "Stage 5E-2 management capability witness",
            "description": "Bounded synthetic projection of one authored capability.",
            "instruments": [source["instruments"][0]],
            "directionMode": source["directionMode"],
            "isActive": False,
            "indicators": [],
            "executionConfig": {
                "managementLibrary": _projected_library(candidate, plan)
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
                "states": states,
                "evidenceGroups": [],
                "eventBindings": [],
                "transitions": transitions,
            },
        }
    )


def _risk_r(plan: Mapping[str, Any]) -> float:
    stop = plan["initialStop"]
    if stop.get("kind") != "fixed_percent":
        raise ValueError("Stage 5E-2 witness supports admitted fixed-percent stops")
    return 100.0 * float(stop["percent"]) / 100.0


def _target_r(plan: Mapping[str, Any]) -> float | None:
    target = plan["initialTarget"]
    if target.get("kind") == "none":
        return None
    if target.get("kind") == "reward_multiple":
        return float(target["multiple"])
    if target.get("kind") == "fixed_percent":
        return float(target["percent"]) / float(plan["initialStop"]["percent"])
    return None


def _favorable_price(direction: str, *, r_value: float, risk: float) -> float:
    return 100.0 + (risk * r_value if direction == "long" else -risk * r_value)


def _bars(
    *,
    direction: str,
    plan: Mapping[str, Any],
    management_type: str,
    break_even_r: float,
    negative: bool,
) -> list[tuple[float, float, float, float]]:
    risk = _risk_r(plan)
    bars: list[tuple[float, float, float, float]] = [
        (100.0, 100.02, 99.98, 100.0),
        (100.0, 100.02, 99.98, 100.0),
    ]
    required_r = break_even_r
    age = 1
    if management_type == "trailing_stop":
        activation = (plan.get("trailingStop") or {}).get("activation") or {}
        kind = activation.get("kind")
        if kind in {"after_unrealized_r", "after_r_and_age"}:
            required_r = float(activation["value"])
        elif kind in {"immediate", "after_position_age", "explicit"}:
            required_r = 0.0
        if kind in {"after_position_age", "after_r_and_age"}:
            age = int(activation["bars"])
        elif kind == "explicit" or negative:
            age = 1
    target_r = _target_r(plan)
    extra = 0.25
    if target_r is not None and required_r > 0:
        extra = min(extra, max(0.05, (target_r - required_r) / 2.0))
    favorable_r = max(0.1, required_r + extra)
    for _ in range(max(1, age - 1)):
        bars.append((100.0, 100.02, 99.98, 100.0))
    favorable = _favorable_price(direction, r_value=favorable_r, risk=risk)
    low = min(100.0, favorable) - 0.01
    high = max(100.0, favorable) + 0.01
    bars.append((100.0, high, low, favorable))
    effect_bars = 3 if negative and management_type == "break_even" else 2
    for _ in range(effect_bars):
        bars.append((favorable, favorable + 0.01, favorable - 0.01, favorable))
    return bars


def _stream(
    profile: TemporalGraphProfile,
    bars: Iterable[tuple[float, float, float, float]],
):
    profile_sha = build_profile_snapshot_sha256(profile)
    program_sha = build_program_sha256(profile)
    anchor = datetime(2025, 8, 1, tzinfo=UTC)
    observations = []
    for index, (open_price, high, low, close) in enumerate(bars):
        start = anchor + timedelta(minutes=index * 5)
        end = start + timedelta(minutes=5)
        token = start.isoformat().replace("+00:00", "Z")
        observations.append(
            build_completed_bar_observation(
                program_sha256=program_sha,
                instrument=profile.instruments[0],
                timeframe="M5",
                bar_id=f"{profile.instruments[0]}:M5:{token}",
                bar_start=start,
                bar_close=end,
                sequence=index,
                clock_index=index,
                open_price=open_price,
                high_price=high,
                low_price=low,
                close_price=close,
                evidence_scores={},
                fresh_events=(),
                management_scalars={},
            )
        )
    return build_observation_stream(
        source_profile_sha256=profile_sha,
        resolved_profile_sha256=profile_sha,
        program_sha256=program_sha,
        instrument=profile.instruments[0],
        base_timeframe="M5",
        observations=observations,
    )


def _compact_trace(row: Any) -> dict[str, Any]:
    return row.model_dump(mode="json", by_alias=True, exclude_none=False)


def _run_witness(
    *,
    candidate: Mapping[str, Any],
    capability_id: str,
    management_type: str,
    authored_payload: Mapping[str, Any],
    plan: Mapping[str, Any],
    break_even_r: float = 0.5,
    negative: bool = False,
    expected_rejection: str | None = None,
) -> dict[str, Any]:
    profile = _profile(
        candidate,
        plan=plan,
        management_type=management_type,
        break_even_r=break_even_r,
        negative=negative,
    )
    stream = _stream(
        profile,
        _bars(
            direction=profile.direction_mode,
            plan=plan,
            management_type=management_type,
            break_even_r=break_even_r,
            negative=negative,
        ),
    )
    uninterrupted = run_temporal_replay(profile, stream)
    split = max(1, len(stream.observations) // 2)
    partial = advance_temporal_replay(profile, stream, max_observations=split)
    serialized = partial.model_dump(mode="json", by_alias=True, exclude_none=False)
    restored = TemporalReplayCheckpoint.model_validate(_clone(serialized))
    resumed = advance_temporal_replay(profile, stream, checkpoint=restored)
    restarted = finish_temporal_replay(profile, stream, resumed)
    restart_exact = (
        uninterrupted.result_sha256 == restarted.result_sha256
        and uninterrupted.final_checkpoint_sha256
        == restarted.final_checkpoint_sha256
        and uninterrupted.execution_traces == restarted.execution_traces
        and uninterrupted.trades == restarted.trades
    )
    execution = list(uninterrupted.execution_traces)
    graph = list(uninterrupted.graph_traces)
    final_position = uninterrupted.final_execution_state.position
    entry_scheduled = any(
        row.action_kind == "enter_next_open" and row.status == "scheduled"
        for row in execution
    )
    entry_filled = any(
        row.action_kind == "enter_next_open" and row.status == "filled"
        for row in execution
    )
    selected_management = any(
        row.transition_id == "open_to_management" for row in graph
    )
    rejection_rows = [row for row in execution if row.status == "rejected"]
    if management_type == "break_even":
        action_scheduled = sum(
            row.action_kind == "move_stop_to_break_even_next_open"
            and row.status == "scheduled"
            for row in execution
        ) >= 1
        if negative:
            effect_accepted = entry_filled
            position_changed = bool(
                final_position is not None
                and final_position.trailing is not None
                and final_position.trailing.active
            )
        else:
            effect_accepted = any(
                row.action_kind == "move_stop_to_break_even_next_open"
                and row.status == "applied"
                for row in execution
            )
            position_changed = bool(
                final_position is not None and final_position.break_even_applied
            )
        guard_true = selected_management
    else:
        activation_kind = plan["trailingStop"]["activation"]["kind"]
        explicit = activation_kind == "explicit" or negative
        action_scheduled = (
            any(
                row.action_kind == "activate_trailing_stop_next_open"
                and row.status == "scheduled"
                for row in execution
            )
            if explicit
            else entry_scheduled
        )
        automatic_activation = any(
            (
                row.action_kind == "trailing_activation"
                and row.status == "applied"
            )
            or (
                row.action_kind == "trailing_stop_schedule"
                and row.status == "scheduled"
            )
            for row in execution
        )
        if negative:
            effect_accepted = entry_filled
        elif explicit:
            effect_accepted = any(
                row.action_kind == "activate_trailing_stop_next_open"
                and row.status == "applied"
                for row in execution
            )
        else:
            effect_accepted = entry_filled and (
                activation_kind == "immediate" or automatic_activation
            )
        position_changed = bool(
            final_position is not None
            and final_position.trailing is not None
            and final_position.trailing.active
        )
        guard_true = selected_management if explicit else effect_accepted

    observed_rejection = (
        any(row.reason_code == expected_rejection for row in rejection_rows)
        if expected_rejection
        else not rejection_rows
    )
    checks = {
        "sourceStateEntered": entry_filled,
        "guardTrue": bool(guard_true),
        "actionScheduled": bool(action_scheduled),
        "effectAccepted": bool(effect_accepted),
        "positionChanged": bool(position_changed),
        "expectedRejectionObserved": bool(observed_rejection),
        "restartExact": restart_exact,
    }
    if not all(checks.values()):
        raise AssertionError(
            f"witness failed {candidate['candidateId']} {capability_id}: {checks!r}; "
            f"rejections={[row.reason_code for row in rejection_rows]!r}"
        )
    relevant_execution = [
        _compact_trace(row)
        for row in execution
        if row.action_kind
        in {
            "enter_next_open",
            "move_stop_to_break_even_next_open",
            "activate_trailing_stop_next_open",
            "trailing_activation",
            "trailing_stop_schedule",
            "trailing_stop_update",
        }
    ]
    relevant_graph = [
        _compact_trace(row)
        for row in graph
        if row.transition_id in {"entry_to_open", "open_to_management"}
    ]
    value = {
        "schemaVersion": WITNESS_SCHEMA,
        "candidateId": candidate["candidateId"],
        "sourceProfileSha256": candidate["sourceProfileSha256"],
        "sourceProgramSha256": candidate["programSha256"],
        "capabilityId": capability_id,
        "managementType": management_type,
        "witnessPolarity": "negative" if negative else "positive",
        "expectedRejectionReason": expected_rejection,
        "authoredPayloadSha256": canonical_sha256(authored_payload),
        "projectionProfileSha256": build_profile_snapshot_sha256(profile),
        "projectionProgramSha256": build_program_sha256(profile),
        "observationStreamSha256": stream.stream_sha256,
        "observationCount": len(stream.observations),
        "splitObservationCount": split,
        "serializedCheckpointSha256": partial.checkpoint_sha256,
        "uninterruptedResultSha256": uninterrupted.result_sha256,
        "restartedResultSha256": restarted.result_sha256,
        "uninterruptedFinalCheckpointSha256": uninterrupted.final_checkpoint_sha256,
        "restartedFinalCheckpointSha256": restarted.final_checkpoint_sha256,
        "checks": checks,
        "graphTraceEvidence": relevant_graph,
        "executionTraceEvidence": relevant_execution,
        "marketEvidenceRead": False,
        "gatewayContacted": False,
    }
    value["witnessSha256"] = canonical_sha256(value)
    return value


def _walk_guard(guard: Mapping[str, Any]):
    yield guard
    child = guard.get("guard")
    if isinstance(child, Mapping):
        yield from _walk_guard(child)
    for item in guard.get("guards") or []:
        if isinstance(item, Mapping):
            yield from _walk_guard(item)


def _break_even_r(transition: Mapping[str, Any]) -> float:
    values = [
        float(node["value"])
        for node in _walk_guard(transition.get("guard") or {})
        if node.get("kind") == "unrealized_r_at_least"
    ]
    return max(values) if values else 0.5


def _capabilities(candidate: Mapping[str, Any]):
    profile = candidate["sourceProfile"]
    library = _management_library(candidate)
    default_plan = _plan(candidate, plan_id=library["defaultPlanId"])
    for transition in profile["graph"]["transitions"]:
        for index, action in enumerate(transition.get("actions") or []):
            if action.get("kind") == "move_stop_to_break_even_next_open":
                identity = {
                    "candidateId": candidate["candidateId"],
                    "managementType": "break_even",
                    "transitionId": transition["id"],
                    "actionIndex": index,
                }
                yield {
                    "capabilityId": canonical_sha256(identity),
                    "managementType": "break_even",
                    "authoredPayload": {
                        "transition": transition,
                        "action": action,
                        "managementPlan": default_plan,
                    },
                    "plan": default_plan,
                    "breakEvenR": _break_even_r(transition),
                }
    for plan in library["plans"]:
        if not plan.get("trailingStop"):
            continue
        identity = {
            "candidateId": candidate["candidateId"],
            "managementType": "trailing_stop",
            "planId": plan["id"],
        }
        yield {
            "capabilityId": canonical_sha256(identity),
            "managementType": "trailing_stop",
            "authoredPayload": {"managementPlan": plan},
            "plan": plan,
            "breakEvenR": 0.5,
        }


def build_witness_set(population_path: Path, output_root: Path) -> dict[str, Any]:
    population = _read(population_path)
    candidates = list(population.get("candidates") or [])
    expected_count = int(population.get("targetUniquePrograms") or 0)
    if (
        expected_count not in {128, 256}
        or len(candidates) != expected_count
        or int(population.get("candidateCount") or 0) != expected_count
        or len({item.get("candidateId") for item in candidates}) != expected_count
    ):
        raise ValueError(
            "witness batch requires an admitted exact 128- or 256-candidate population"
        )
    summaries = []
    positive_counts: Counter[str] = Counter()
    for candidate in sorted(candidates, key=lambda item: item["candidateId"]):
        for capability in _capabilities(candidate):
            witness = _run_witness(
                candidate=candidate,
                capability_id=capability["capabilityId"],
                management_type=capability["managementType"],
                authored_payload=capability["authoredPayload"],
                plan=capability["plan"],
                break_even_r=capability["breakEvenR"],
            )
            name = capability["capabilityId"].removeprefix("sha256:")[:20]
            relative = Path("positive") / f"{candidate['candidateId']}-{name}.json"
            _write_immutable(output_root / relative, witness)
            summaries.append(
                {
                    "candidateId": candidate["candidateId"],
                    "capabilityId": capability["capabilityId"],
                    "managementType": capability["managementType"],
                    "witnessPolarity": "positive",
                    "relativePath": relative.as_posix(),
                    "witnessSha256": witness["witnessSha256"],
                }
            )
            positive_counts[capability["managementType"]] += 1

    break_even_source = next(
        (candidate, capability)
        for candidate in candidates
        for capability in _capabilities(candidate)
        if capability["managementType"] == "break_even"
    )
    trailing_source = next(
        (candidate, capability)
        for candidate in candidates
        for capability in _capabilities(candidate)
        if capability["managementType"] == "trailing_stop"
        and capability["plan"]["trailingStop"]["activation"]["kind"] == "immediate"
    )
    negative_specs = [
        (*break_even_source, "stop_not_tightened"),
        (*trailing_source, "trailing_already_active"),
    ]
    negative_counts: Counter[str] = Counter()
    for candidate, capability, reason in negative_specs:
        capability_id = canonical_sha256(
            {
                "sourceCapabilityId": capability["capabilityId"],
                "polarity": "negative",
                "expectedRejectionReason": reason,
            }
        )
        witness_plan = _clone(capability["plan"])
        if capability["managementType"] == "break_even":
            witness_plan["trailingStop"] = {
                "activation": {"kind": "immediate"},
                "anchor": {"kind": "bar_close"},
                "distance": {"kind": "fixed_initial_r", "multiple": 0.25},
                "minimumStepInitialR": 0.0,
            }
        witness = _run_witness(
            candidate=candidate,
            capability_id=capability_id,
            management_type=capability["managementType"],
            authored_payload=capability["authoredPayload"],
            plan=witness_plan,
            break_even_r=capability["breakEvenR"],
            negative=True,
            expected_rejection=reason,
        )
        relative = Path("negative") / f"{capability['managementType']}.json"
        _write_immutable(output_root / relative, witness)
        summaries.append(
            {
                "candidateId": candidate["candidateId"],
                "capabilityId": capability_id,
                "managementType": capability["managementType"],
                "witnessPolarity": "negative",
                "expectedRejectionReason": reason,
                "relativePath": relative.as_posix(),
                "witnessSha256": witness["witnessSha256"],
            }
        )
        negative_counts[capability["managementType"]] += 1

    report = {
        "schemaVersion": WITNESS_SET_SCHEMA,
        "populationSha256": population["populationSha256"],
        "candidateCount": len(candidates),
        "authoredCapabilityCount": sum(positive_counts.values()),
        "positiveWitnessCounts": dict(sorted(positive_counts.items())),
        "negativeWitnessCounts": dict(sorted(negative_counts.items())),
        "witnessCount": len(summaries),
        "allChecksPassed": True,
        "restartExactWitnessCount": len(summaries),
        "marketEvidenceRead": False,
        "gatewayContacted": False,
        "witnesses": sorted(
            summaries,
            key=lambda item: (
                item["witnessPolarity"],
                item["candidateId"],
                item["capabilityId"],
            ),
        ),
    }
    report["reportSha256"] = canonical_sha256(report)
    _write_immutable(output_root / "report.json", report)
    files = []
    for path in sorted(item for item in output_root.rglob("*") if item.is_file()):
        if path.name == "manifest.json":
            continue
        files.append(
            {
                "relativePath": path.relative_to(output_root).as_posix(),
                "length": path.stat().st_size,
                "sha256": _file_sha(path),
            }
        )
    manifest = {
        "schemaVersion": WITNESS_MANIFEST_SCHEMA,
        "reportSha256": report["reportSha256"],
        "fileCount": len(files),
        "files": files,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write_immutable(output_root / "manifest.json", manifest)
    return {
        "schemaVersion": "temporal_policy_v2_management_witness_result_v1",
        "reportSha256": report["reportSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "authoredCapabilityCount": report["authoredCapabilityCount"],
        "witnessCount": report["witnessCount"],
        "restartExactWitnessCount": report["restartExactWitnessCount"],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--population", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            build_witness_set(args.population, args.output_root),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
