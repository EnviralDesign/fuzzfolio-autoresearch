"""Run synthetic native lifecycle witnesses for confirmed-entry pairs.

Execute this repository-owned script with the FuzzFolio core environment.  It
uses only authored graph facts and synthetic events; there is no market-data,
Gateway, worker, or replay-fleet client in this file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fuzzfolio_core.temporal_graph import (
    TemporalEvent,
    TemporalGraphProfile,
    TemporalGraphRuntime,
    advance_graph,
    initialize_graph_runtime,
)
from fuzzfolio_core.temporal_graph.identity import (
    build_program_sha256,
    canonical_sha256,
)

WITNESS_SET_SCHEMA = "temporal_confirmed_entry_native_witness_set_v1"
WITNESS_SCHEMA = "temporal_confirmed_entry_native_witness_v1"
MANIFEST_SCHEMA = "temporal_confirmed_entry_native_witness_manifest_v1"


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
        raise TypeError(f"JSON root must be an object: {path}")
    return _clone(value)


def _write_immutable(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(
            dict(value), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False
        )
        + "\n"
    )
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise ValueError(f"refusing to overwrite divergent witness: {path}")
    path.write_text(encoded, encoding="utf-8")


def _file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def _walk_guard(value: Mapping[str, Any]):
    yield value
    child = value.get("guard")
    if isinstance(child, Mapping):
        yield from _walk_guard(child)
    for item in value.get("guards") or []:
        if isinstance(item, Mapping):
            yield from _walk_guard(item)


def _incoming_setup_source_transition(
    profile: Mapping[str, Any], source_state_id: str
) -> dict[str, Any]:
    graph = profile["graph"]
    initial = graph["initialStateId"]
    if initial == source_state_id:
        raise ValueError("witness corpus unexpectedly starts in the setup source")
    matches = [
        item
        for item in graph["transitions"]
        if item["sourceStateId"] == initial
        and item["destinationStateId"] == source_state_id
        and item["eventClass"] == "decision"
    ]
    if len(matches) != 1:
        raise ValueError("witness requires one direct initial-to-setup-source route")
    return matches[0]


def _preparation_parameters(guard: Mapping[str, Any]) -> tuple[float, int, int]:
    if guard.get("kind") == "any":
        # The admitted family always retains an invertible directional evidence
        # branch, so the upper endpoint supplies a one-event witness.
        minute = 720
        for node in _walk_guard(guard):
            if node.get("kind") == "utc_time_window":
                minute = (int(node["startMinute"]) + 30) % 1440
                break
        return 100.0, 1, minute

    lower = 0.0
    upper = 100.0
    events = 1
    minute = 720

    def collect(node: Mapping[str, Any], *, negated: bool = False) -> None:
        nonlocal lower, upper, events, minute
        kind = node.get("kind")
        if kind == "not" and isinstance(node.get("guard"), Mapping):
            collect(node["guard"], negated=not negated)
            return
        threshold = float(node.get("thresholdPercent") or 0.0)
        if kind == "condition_streak_at_least":
            events = max(events, int(node["events"]))
            at_least = node["comparison"] == "at_least"
            if at_least is not negated:
                lower = max(lower, threshold)
            else:
                upper = min(upper, threshold)
        elif kind == "evidence_at_least":
            if not negated:
                lower = max(lower, threshold)
            else:
                upper = min(upper, threshold)
        elif kind == "evidence_below":
            if not negated:
                upper = min(upper, threshold)
            else:
                lower = max(lower, threshold)
        elif kind == "utc_time_window":
            minute = (
                int(node["endMinute"])
                if negated
                else (int(node["startMinute"]) + 30) % 1440
            )
        child = node.get("guard")
        if isinstance(child, Mapping):
            collect(child, negated=negated)
        for item in node.get("guards") or []:
            if isinstance(item, Mapping):
                collect(item, negated=negated)

    collect(guard)
    if lower >= upper:
        raise ValueError("preparation evidence constraints have no witness interval")
    score = lower if lower == 100.0 else (lower + upper) / 2.0
    return score, events, minute


def _anchor_scores(
    transformed_profile: Mapping[str, Any], application: Mapping[str, Any]
) -> tuple[dict[str, float], dict[str, float]]:
    invalidation = application["delta"]["addedTransitions"][2]
    guard = invalidation["guard"]
    group_id = str(guard["groupId"])
    threshold = float(guard["thresholdPercent"])
    if guard["kind"] == "evidence_below":
        setup = min(100.0, threshold + max(1.0, (100.0 - threshold) / 2.0))
        invalid = max(0.0, threshold - max(1.0, threshold / 2.0))
    elif guard["kind"] == "evidence_at_least":
        setup = max(0.0, threshold - max(1.0, threshold / 2.0))
        invalid = min(100.0, threshold + max(1.0, (100.0 - threshold) / 2.0))
    else:
        raise ValueError("witness supports direct evidence invalidation guards")
    group_ids = [item["id"] for item in transformed_profile["graph"]["evidenceGroups"]]
    setup_scores = {value: 50.0 for value in group_ids}
    invalid_scores = dict(setup_scores)
    setup_scores[group_id] = setup
    invalid_scores[group_id] = invalid
    return setup_scores, invalid_scores


class _Cursor:
    def __init__(self, *, minute: int) -> None:
        base = datetime(2025, 8, 4, tzinfo=UTC)
        self.anchor = base + timedelta(minutes=minute)
        self.sequence = 0
        self.clock_index = 0

    def decision(
        self,
        *,
        label: str,
        evidence_scores: Mapping[str, float],
        fresh_events: tuple[str, ...] = (),
    ) -> TemporalEvent:
        bar_start = self.anchor + timedelta(minutes=5 * self.clock_index)
        event = TemporalEvent(
            eventId=f"{label}_{self.sequence}",
            eventClass="decision",
            sequence=self.sequence,
            clockId="clock.completed_bar",
            clockIndex=self.clock_index,
            barStart=bar_start,
            facts={
                "evidenceScores": dict(evidence_scores),
                "freshEvents": list(fresh_events),
                "position": {"exists": False},
            },
        )
        self.sequence += 1
        self.clock_index += 1
        return event

    def execution_fill(self, *, intent: Any) -> TemporalEvent:
        event = TemporalEvent(
            eventId=f"entry_fill_{self.sequence}",
            eventClass="execution",
            sequence=self.sequence,
            clockId="clock.completed_bar",
            clockIndex=self.clock_index,
            facts={
                "position": {
                    "exists": True,
                    "ageEvents": 0,
                    "unrealizedR": 0.0,
                },
                "execution": {
                    "intentId": intent.intent_id,
                    "actionKind": intent.action_kind,
                    "status": "filled",
                },
            },
        )
        self.sequence += 1
        self.clock_index += 1
        return event


def _prepare(
    profile: TemporalGraphProfile,
    source_payload: Mapping[str, Any],
    *,
    source_state_id: str,
) -> tuple[Any, _Cursor, dict[str, float]]:
    transition = _incoming_setup_source_transition(source_payload, source_state_id)
    score, required_events, minute = _preparation_parameters(transition["guard"])
    # The final streak-building event must land in the authored time window.
    # Earlier events may be outside it; fact-history streaks still advance and
    # the transition is evaluated at the exact final trigger instant.
    cursor = _Cursor(minute=(minute - 5 * (required_events - 1)) % 1440)
    runtime = initialize_graph_runtime(profile)
    group_ids = [item.id for item in profile.graph.evidence_groups]
    scores = {value: score for value in group_ids}
    last = None
    for _ in range(max(required_events + 2, 3)):
        last = advance_graph(
            profile,
            runtime,
            cursor.decision(label="prepare", evidence_scores=scores),
        )
        runtime = last.runtime
        if runtime.current_state_id == source_state_id:
            break
    if runtime.current_state_id != source_state_id:
        raise ValueError("synthetic preparation did not reach setup source state")
    if (
        last is None
        or last.trace is None
        or last.trace.transition_id != transition["id"]
    ):
        raise ValueError("synthetic preparation used an unexpected transition")
    return runtime, cursor, scores


def _roundtrip_runtime(runtime: TemporalGraphRuntime) -> TemporalGraphRuntime:
    encoded = runtime.model_dump_json(by_alias=True, exclude_none=False)
    restored = TemporalGraphRuntime.model_validate_json(encoded)
    if restored != runtime:
        raise ValueError("runtime JSON roundtrip diverged")
    return restored


def _step_payload(step: Any) -> dict[str, Any]:
    return step.model_dump(mode="json", by_alias=True, exclude_none=False)


def _run_transformed(pair: Mapping[str, Any]) -> dict[str, Any]:
    transformed_payload = pair["transformed"]["sourceProfile"]
    profile = TemporalGraphProfile.model_validate(transformed_payload)
    if build_program_sha256(profile) != pair["transformed"]["programSha256"]:
        raise ValueError("transformed native program identity mismatch")
    application = pair["operatorApplication"]
    added = application["delta"]["addedTransitions"]
    (
        setup_transition,
        confirmation_transition,
        invalidation_transition,
        expiry_transition,
    ) = added
    source_state = setup_transition["sourceStateId"]
    armed_state = setup_transition["destinationStateId"]
    event_id = str(pair["operatorPlan"]["confirmationBindingIdentity"]).split(":", 1)[1]
    setup_scores, invalid_scores = _anchor_scores(transformed_payload, application)

    # Setup and later confirmation, including the ordinary execution event.
    runtime, cursor, _ = _prepare(
        profile, transformed_payload, source_state_id=source_state
    )
    setup_event = cursor.decision(
        label="setup", evidence_scores=setup_scores, fresh_events=(event_id,)
    )
    setup = advance_graph(profile, runtime, setup_event)
    if (
        setup.runtime.current_state_id != armed_state
        or setup.trace is None
        or setup.trace.transition_id != setup_transition["id"]
        or setup.intents
    ):
        raise ValueError("setup witness did not arm without an intent")
    armed_checkpoint = _roundtrip_runtime(setup.runtime)
    confirmation_event = cursor.decision(
        label="confirmation", evidence_scores=setup_scores
    )
    confirmation = advance_graph(profile, setup.runtime, confirmation_event)
    confirmation_resumed = advance_graph(profile, armed_checkpoint, confirmation_event)
    if _step_payload(confirmation) != _step_payload(confirmation_resumed):
        raise ValueError("armed-before-confirmation restart diverged")
    if not confirmation.intents:
        # Fresh-only confirmations require one additional later event.
        confirmation_event = cursor.decision(
            label="confirmation_fresh",
            evidence_scores=setup_scores,
            fresh_events=(event_id,),
        )
        armed_checkpoint = _roundtrip_runtime(confirmation.runtime)
        confirmation = advance_graph(profile, confirmation.runtime, confirmation_event)
        confirmation_resumed = advance_graph(
            profile, armed_checkpoint, confirmation_event
        )
        if _step_payload(confirmation) != _step_payload(confirmation_resumed):
            raise ValueError("fresh confirmation restart diverged")
    if (
        confirmation.trace is None
        or confirmation.trace.transition_id != confirmation_transition["id"]
        or len(confirmation.intents) != 1
        or confirmation.intents[0].action_kind != "enter_next_open"
    ):
        raise ValueError("later confirmation did not schedule the original entry")
    pending_checkpoint = _roundtrip_runtime(confirmation.runtime)
    intent = confirmation.intents[0]
    fill_event = cursor.execution_fill(intent=intent)
    filled = advance_graph(profile, confirmation.runtime, fill_event)
    filled_resumed = advance_graph(profile, pending_checkpoint, fill_event)
    if _step_payload(filled) != _step_payload(filled_resumed):
        raise ValueError("pending-entry restart diverged")
    if filled.runtime.current_state_id == confirmation.runtime.current_state_id:
        raise ValueError("ordinary entry fill did not advance the authored graph")

    # Invalidation from an independently prepared state with no event priming.
    invalid_runtime, invalid_cursor, _ = _prepare(
        profile, transformed_payload, source_state_id=source_state
    )
    invalid_setup = advance_graph(
        profile,
        invalid_runtime,
        invalid_cursor.decision(label="setup", evidence_scores=setup_scores),
    )
    invalidated = advance_graph(
        profile,
        invalid_setup.runtime,
        invalid_cursor.decision(label="invalidation", evidence_scores=invalid_scores),
    )
    if (
        invalidated.trace is None
        or invalidated.trace.transition_id != invalidation_transition["id"]
        or invalidated.runtime.current_state_id != source_state
        or invalidated.intents
    ):
        raise ValueError("invalidation witness failed")

    # Finite expiry, with restart from the final armed state before expiry.
    expiry_runtime, expiry_cursor, _ = _prepare(
        profile, transformed_payload, source_state_id=source_state
    )
    expiry_setup = advance_graph(
        profile,
        expiry_runtime,
        expiry_cursor.decision(label="setup", evidence_scores=setup_scores),
    )
    current = expiry_setup
    for age in (1, 2):
        current = advance_graph(
            profile,
            current.runtime,
            expiry_cursor.decision(
                label=f"expiry_wait_{age}", evidence_scores=setup_scores
            ),
        )
        if current.transition_selected or current.intents:
            raise ValueError("absent confirmation did not remain armed before expiry")
    before_expiry = _roundtrip_runtime(current.runtime)
    expiry_event = expiry_cursor.decision(label="expiry", evidence_scores=setup_scores)
    expired = advance_graph(profile, current.runtime, expiry_event)
    expired_resumed = advance_graph(profile, before_expiry, expiry_event)
    if _step_payload(expired) != _step_payload(expired_resumed):
        raise ValueError("armed-before-expiry restart diverged")
    if (
        expired.trace is None
        or expired.trace.transition_id != expiry_transition["id"]
        or expired.runtime.current_state_id != source_state
        or expired.intents
    ):
        raise ValueError("finite expiry witness failed")

    return {
        "setupTransitionId": setup_transition["id"],
        "confirmationTransitionId": confirmation_transition["id"],
        "invalidationTransitionId": invalidation_transition["id"],
        "expiryTransitionId": expiry_transition["id"],
        "setupRuntimeSha256": setup.runtime.state_sha256,
        "confirmationRuntimeSha256": confirmation.runtime.state_sha256,
        "confirmationIntentId": intent.intent_id,
        "filledRuntimeSha256": filled.runtime.state_sha256,
        "invalidationRuntimeSha256": invalidated.runtime.state_sha256,
        "beforeExpiryRuntimeSha256": before_expiry.state_sha256,
        "expiredRuntimeSha256": expired.runtime.state_sha256,
        "armedBeforeConfirmationRestartExact": True,
        "armedBeforeExpiryRestartExact": True,
        "pendingEntryRestartExact": True,
        "ordinaryEntryFillObserved": True,
    }


def _run_control(pair: Mapping[str, Any]) -> dict[str, Any]:
    payload = pair["control"]["sourceProfile"]
    profile = TemporalGraphProfile.model_validate(payload)
    if build_program_sha256(profile) != pair["control"]["programSha256"]:
        raise ValueError("control native program identity mismatch")
    original = pair["operatorApplication"]["delta"]["removedTransitions"][0]
    source_state = original["sourceStateId"]
    event_id = str(pair["operatorPlan"]["confirmationBindingIdentity"]).split(":", 1)[1]
    setup_scores, _ = _anchor_scores(
        pair["transformed"]["sourceProfile"], pair["operatorApplication"]
    )
    runtime, cursor, _ = _prepare(profile, payload, source_state_id=source_state)
    direct = advance_graph(
        profile,
        runtime,
        cursor.decision(
            label="control_compound",
            evidence_scores=setup_scores,
            fresh_events=(event_id,),
        ),
    )
    if not direct.intents:
        direct = advance_graph(
            profile,
            direct.runtime,
            cursor.decision(
                label="control_compound_later", evidence_scores=setup_scores
            ),
        )
    if (
        direct.trace is None
        or direct.trace.transition_id != original["id"]
        or len(direct.intents) != 1
        or direct.intents[0].action_kind != "enter_next_open"
    ):
        raise ValueError("control did not enter under the original compound guard")
    return {
        "transitionId": original["id"],
        "runtimeSha256": direct.runtime.state_sha256,
        "intentId": direct.intents[0].intent_id,
        "directCompoundEntryObserved": True,
    }


def build_witnesses(*, population_path: Path, output_root: Path) -> dict[str, Any]:
    population = _read(population_path)
    pairs = sorted(population["pairs"], key=lambda item: item["pairId"])
    witnesses = []
    for pair in pairs:
        try:
            witness = {
                "schemaVersion": WITNESS_SCHEMA,
                "pairId": pair["pairId"],
                "controlProgramSha256": pair["control"]["programSha256"],
                "transformedProgramSha256": pair["transformed"]["programSha256"],
                "planSha256": pair["operatorPlan"]["planSha256"],
                "applicationSha256": pair["operatorApplication"]["applicationSha256"],
                "control": _run_control(pair),
                "transformed": _run_transformed(pair),
            }
        except Exception as exc:
            raise ValueError(
                f"native witness failed for {pair['pairId']}: {exc}"
            ) from exc
        witness["witnessSha256"] = canonical_sha256(witness)
        witnesses.append(witness)
    report = {
        "schemaVersion": WITNESS_SET_SCHEMA,
        "populationSha256": population["populationSha256"],
        "pairCount": len(pairs),
        "controlDirectEntryCount": len(witnesses),
        "setupCount": len(witnesses),
        "confirmationCount": len(witnesses),
        "invalidationCount": len(witnesses),
        "expiryCount": len(witnesses),
        "armedBeforeConfirmationRestartCount": len(witnesses),
        "armedBeforeExpiryRestartCount": len(witnesses),
        "pendingEntryRestartCount": len(witnesses),
        "witnesses": witnesses,
        "marketEvidenceRead": False,
        "gatewayContacted": False,
    }
    report["reportSha256"] = canonical_sha256(report)
    _write_immutable(output_root / "native-witnesses.json", report)
    files = [
        {
            "relativePath": "native-witnesses.json",
            "length": (output_root / "native-witnesses.json").stat().st_size,
            "sha256": _file_sha(output_root / "native-witnesses.json"),
        }
    ]
    manifest = {
        "schemaVersion": MANIFEST_SCHEMA,
        "reportSha256": report["reportSha256"],
        "fileCount": 1,
        "files": files,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write_immutable(output_root / "manifest.json", manifest)
    return {
        "schemaVersion": "temporal_confirmed_entry_native_witness_result_v1",
        "reportSha256": report["reportSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "pairCount": len(pairs),
        "allLifecycleWitnessesPassed": True,
        "allRestartWitnessesPassed": True,
        "marketEvidenceRead": False,
        "gatewayContacted": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--population", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            build_witnesses(
                population_path=args.population,
                output_root=args.output_root,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
