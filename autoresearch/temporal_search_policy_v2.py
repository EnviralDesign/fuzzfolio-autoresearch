"""Stage 5E-2 activation-aware generator and static admission policy.

Version 1 remains untouched and reproducible.  This module owns the explicitly
versioned successor used by the repository-only Stage 5E-2 admission batch.
It does not prepare windows, contact the Gateway, or read market evidence.
"""

from __future__ import annotations

from collections import Counter, deque
from collections.abc import Iterable, Mapping, Sequence
import copy
import hashlib
import json
from pathlib import Path
import random
from typing import Any

from .temporal_discovery_base import (
    CandidateValidatorProtocol,
    TemporalDiscoveryContractError,
    TemporalDiscoveryGenerationExhausted,
    _CANDIDATE,
    _clone,
    _ensure_explicit_management,
    _sha,
    _write_immutable,
    canonical_sha256,
)
from .temporal_discovery_mutation import _mutate_profile
from .temporal_discovery_validation import _normalize_preparation
from .temporal_search_activation import (
    DEPTH_RANK,
    audit_activation_causality,
)


GENERATOR_V2_VERSION = "temporal_discovery_generator_v2_activation_aware"
GENERATOR_V2_CONFIG_SCHEMA = "temporal_discovery_generator_v2_config_v1"
GENERATOR_V2_POPULATION_SCHEMA = "temporal_discovery_population_v2"
GENERATOR_V2_JOURNAL_SCHEMA = "temporal_discovery_generation_journal_v2"
GENERATOR_V2_REACHABILITY_SCHEMA = "temporal_management_reachability_v1"
GENERATOR_V2_MANIFEST_SCHEMA = "temporal_discovery_generator_v2_manifest_v1"

GENERATOR_V2_PARAMETERS: dict[str, Any] = {
    "version": GENERATOR_V2_VERSION,
    "seed": 20260801,
    "targetUniquePrograms": 256,
    "sourceModeCounts": {
        "broad_seed_mutation": 128,
        "seed_derived": 128,
    },
    "broadMutationCount": {"min": 3, "max": 6},
    "seedMutationCount": {"min": 1, "max": 2},
    "maxProposalAttempts": 8192,
    "breakEvenUnrealizedR": 0.5,
    "trailingAfterBreakEvenUnrealizedR": 1.0,
    "minimumRewardMultipleWithBreakEven": 1.5,
    "causalCoverageMinimum": 0.8,
}

# Stage 5E-3 reuses the admitted generator-v2 algorithm and repair policy with a
# smaller, predeclared campaign envelope.  Keeping the campaign profile beside
# the admission profile makes the only permitted parameter difference explicit
# and prevents callers from inventing ad-hoc generator settings.
GENERATOR_V2_STAGE5E3_PARAMETERS: dict[str, Any] = {
    **GENERATOR_V2_PARAMETERS,
    "targetUniquePrograms": 128,
    "sourceModeCounts": {
        "broad_seed_mutation": 64,
        "seed_derived": 64,
    },
}

GENERATOR_V2_PARAMETER_PROFILES: dict[str, dict[str, Any]] = {
    "stage5e2_synthetic_admission": GENERATOR_V2_PARAMETERS,
    "stage5e3_modest_policy_validation": GENERATOR_V2_STAGE5E3_PARAMETERS,
}


def generator_v2_parameter_profile(parameters: Mapping[str, Any]) -> str:
    """Return the repository-admitted profile name for exact parameters."""

    value = _clone(parameters, name="generator v2 parameters")
    for name, admitted in GENERATOR_V2_PARAMETER_PROFILES.items():
        if value == admitted:
            return name
    raise TemporalDiscoveryContractError(
        "generator v2 parameters do not match a repository-admitted profile"
    )


def _encoded(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value),
        indent=2,
        sort_keys=True,
        ensure_ascii=True,
        allow_nan=False,
    ) + "\n"


def _read(path: Path, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"{name} root must be an object")
    return _clone(value, name=name)


def _file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def _manifest(root: Path, *, population_sha256: str) -> dict[str, Any]:
    files = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.name == "manifest.json":
            continue
        files.append(
            {
                "relativePath": path.relative_to(root).as_posix(),
                "length": path.stat().st_size,
                "sha256": _file_sha(path),
            }
        )
    value = {
        "schemaVersion": GENERATOR_V2_MANIFEST_SCHEMA,
        "populationSha256": population_sha256,
        "fileCount": len(files),
        "files": files,
    }
    value["manifestSha256"] = canonical_sha256(value)
    _write_immutable(root / "manifest.json", value)
    return value


def _walk_guard(value: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
    yield value
    child = value.get("guard")
    if isinstance(child, Mapping):
        yield from _walk_guard(child)
    for item in value.get("guards") or []:
        if isinstance(item, Mapping):
            yield from _walk_guard(item)


def _guard_is_statically_true(value: Mapping[str, Any]) -> bool:
    kind = value.get("kind")
    if kind == "state_age_at_least" and int(value.get("events") or 0) <= 0:
        return True
    if kind == "position_exists" and value.get("expected") is True:
        return False
    if kind == "all":
        guards = [item for item in value.get("guards") or [] if isinstance(item, Mapping)]
        return bool(guards) and all(_guard_is_statically_true(item) for item in guards)
    if kind == "any":
        return any(
            _guard_is_statically_true(item)
            for item in value.get("guards") or []
            if isinstance(item, Mapping)
        )
    return False


def _graph(profile: Mapping[str, Any]) -> dict[str, Any]:
    value = profile.get("graph")
    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError("candidate graph must be an object")
    return dict(value)


def _transitions(profile: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(item)
        for item in _graph(profile).get("transitions") or []
        if isinstance(item, Mapping)
    ]


def _reachable_from(profile: Mapping[str, Any], origins: Iterable[str]) -> set[str]:
    reached = {str(item) for item in origins if str(item)}
    queue = deque(sorted(reached))
    transitions = _transitions(profile)
    while queue:
        source = queue.popleft()
        for transition in transitions:
            if str(transition.get("sourceStateId") or "") != source:
                continue
            destination = str(transition.get("destinationStateId") or "")
            if destination and destination not in reached:
                reached.add(destination)
                queue.append(destination)
    return reached


def _filled_destinations(profile: Mapping[str, Any]) -> set[str]:
    output = set()
    for transition in _transitions(profile):
        guard = transition.get("guard")
        if transition.get("eventClass") != "execution" or not isinstance(guard, Mapping):
            continue
        if any(
            node.get("kind") == "execution_status_is" and node.get("status") == "filled"
            for node in _walk_guard(guard)
        ):
            output.add(str(transition.get("destinationStateId") or ""))
    return {item for item in output if item}


def _action_transitions(
    profile: Mapping[str, Any], *, kinds: set[str] | None = None
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    output = []
    for transition in _transitions(profile):
        for action in transition.get("actions") or []:
            if not isinstance(action, Mapping):
                continue
            if kinds is None or str(action.get("kind") or "") in kinds:
                output.append((transition, dict(action)))
    return output


def _management(profile: Mapping[str, Any]) -> tuple[str | None, list[dict[str, Any]]]:
    library = (
        ((profile.get("executionConfig") or {}).get("managementLibrary") or {})
        if isinstance(profile.get("executionConfig"), Mapping)
        else {}
    )
    return (
        str(library.get("defaultPlanId") or "") or None,
        [dict(item) for item in library.get("plans") or [] if isinstance(item, Mapping)],
    )


def _transition_dominated(
    profile: Mapping[str, Any], target: Mapping[str, Any]
) -> bool:
    peers = sorted(
        (
            transition
            for transition in _transitions(profile)
            if transition.get("sourceStateId") == target.get("sourceStateId")
            and transition.get("eventClass") == target.get("eventClass")
        ),
        key=lambda item: (int(item.get("priority") or 0), str(item.get("id") or "")),
    )
    for peer in peers:
        if peer.get("id") == target.get("id"):
            return False
        guard = peer.get("guard")
        if isinstance(guard, Mapping) and _guard_is_statically_true(guard):
            return True
    return False


def _replace_break_even_thresholds(guard: Any, *, value: float) -> int:
    changed = 0
    if isinstance(guard, dict):
        if guard.get("kind") == "unrealized_r_at_least" and guard.get("value") != value:
            guard["value"] = value
            changed += 1
        child = guard.get("guard")
        if isinstance(child, dict):
            changed += _replace_break_even_thresholds(child, value=value)
        for item in guard.get("guards") or []:
            if isinstance(item, dict):
                changed += _replace_break_even_thresholds(item, value=value)
    return changed


def _unique_graph_id(existing: set[str], *, prefix: str, material: Any) -> str:
    suffix = canonical_sha256(material).removeprefix("sha256:")[:12]
    base = f"{prefix}_{suffix}"
    candidate = base
    ordinal = 1
    while candidate in existing:
        candidate = f"{base}_{ordinal}"
        ordinal += 1
    existing.add(candidate)
    return candidate


def _inject_explicit_trailing_routes(
    graph: dict[str, Any],
    *,
    repairs: list[dict[str, Any]],
    minimum_unrealized_r: float | None = None,
) -> None:
    transitions = graph.get("transitions") or []
    states = graph.get("states") or []
    existing_transition_ids = {
        str(item.get("id") or "") for item in transitions if isinstance(item, Mapping)
    }
    existing_state_ids = {
        str(item.get("id") or "") for item in states if isinstance(item, Mapping)
    }
    filled_destinations = []
    for transition in transitions:
        guard = transition.get("guard")
        if transition.get("eventClass") != "execution" or not isinstance(guard, Mapping):
            continue
        if any(
            node.get("kind") == "execution_status_is" and node.get("status") == "filled"
            for node in _walk_guard(guard)
        ):
            destination = str(transition.get("destinationStateId") or "")
            if destination:
                filled_destinations.append(destination)

    for source_state in sorted(set(filled_destinations)):
        original_outgoing = [
            copy.deepcopy(item)
            for item in transitions
            if str(item.get("sourceStateId") or "") == source_state
        ]
        if any(
            any(
                isinstance(action, Mapping)
                and action.get("kind") == "activate_trailing_stop_next_open"
                for action in item.get("actions") or []
            )
            for item in original_outgoing
        ):
            continue
        token_material = {
            "sourceStateId": source_state,
            "operation": "explicit_trailing_route",
        }
        requested = _unique_graph_id(
            existing_state_ids,
            prefix="v2_trail_requested",
            material={**token_material, "state": "requested"},
        )
        applied = _unique_graph_id(
            existing_state_ids,
            prefix="v2_trail_applied",
            material={**token_material, "state": "applied"},
        )
        rejected = _unique_graph_id(
            existing_state_ids,
            prefix="v2_trail_rejected",
            material={**token_material, "state": "rejected"},
        )
        states.extend([{"id": requested}, {"id": applied}, {"id": rejected}])
        decision_priorities = {
            int(item.get("priority") or 0)
            for item in original_outgoing
            if item.get("eventClass") == "decision"
        }
        activation_priority = 0
        while activation_priority in decision_priorities:
            activation_priority += 1
        activation_id = _unique_graph_id(
            existing_transition_ids,
            prefix="v2_activate_trailing",
            material=token_material,
        )
        activation_guards = [
            {"kind": "position_exists", "expected": True},
            {"kind": "position_age_at_least", "events": 1},
        ]
        if minimum_unrealized_r is not None:
            activation_guards.append(
                {"kind": "unrealized_r_at_least", "value": minimum_unrealized_r}
            )
        additions = [
            {
                "id": activation_id,
                "sourceStateId": source_state,
                "destinationStateId": requested,
                "eventClass": "decision",
                "priority": activation_priority,
                "guard": {
                    "kind": "all",
                    "guards": activation_guards,
                },
                "actions": [{"kind": "activate_trailing_stop_next_open"}],
                "reasonCode": "explicit_trailing_activation_requested",
            },
            {
                "id": _unique_graph_id(
                    existing_transition_ids,
                    prefix="v2_trail_applied",
                    material=token_material,
                ),
                "sourceStateId": requested,
                "destinationStateId": applied,
                "eventClass": "execution",
                "priority": 10,
                "guard": {"kind": "execution_status_is", "status": "applied"},
                "actions": [],
                "reasonCode": "explicit_trailing_activation_applied",
            },
            {
                "id": _unique_graph_id(
                    existing_transition_ids,
                    prefix="v2_trail_rejected",
                    material=token_material,
                ),
                "sourceStateId": requested,
                "destinationStateId": rejected,
                "eventClass": "execution",
                "priority": 20,
                "guard": {"kind": "execution_status_is", "status": "rejected"},
                "actions": [],
                "reasonCode": "explicit_trailing_activation_rejected",
            },
        ]
        for destination_state in (applied, rejected):
            for original in original_outgoing:
                clone = copy.deepcopy(original)
                clone["id"] = _unique_graph_id(
                    existing_transition_ids,
                    prefix="v2_trail_continuation",
                    material={
                        **token_material,
                        "destinationState": destination_state,
                        "transitionId": original.get("id"),
                    },
                )
                clone["sourceStateId"] = destination_state
                additions.append(clone)
        requested_execution_priorities = {10, 20}
        for original in original_outgoing:
            guard = original.get("guard")
            if original.get("eventClass") != "execution" or not isinstance(guard, Mapping):
                continue
            if not any(
                node.get("kind") == "execution_status_is" and node.get("status") == "closed"
                for node in _walk_guard(guard)
            ):
                continue
            clone = copy.deepcopy(original)
            clone["id"] = _unique_graph_id(
                existing_transition_ids,
                prefix="v2_trail_requested_close",
                material={**token_material, "transitionId": original.get("id")},
            )
            clone["sourceStateId"] = requested
            requested_priority = int(clone.get("priority") or 0)
            while requested_priority in requested_execution_priorities:
                requested_priority += 1
            clone["priority"] = requested_priority
            requested_execution_priorities.add(requested_priority)
            additions.append(clone)
        transitions.extend(additions)
        repairs.append(
            {
                "kind": "explicit_trailing_activation_route",
                "sourceStateId": source_state,
                "requestedStateId": requested,
                "appliedStateId": applied,
                "rejectedStateId": rejected,
                "activationTransitionId": activation_id,
                "minimumUnrealizedR": minimum_unrealized_r,
                "continuationTransitionCount": len(additions) - 3,
            }
        )


def _repair_profile(
    profile: Mapping[str, Any], *, parameters: Mapping[str, Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    output = _ensure_explicit_management(dict(profile))
    repairs: list[dict[str, Any]] = []
    graph = output["graph"]
    transitions = graph.get("transitions") or []
    break_even_transitions = []
    for transition in transitions:
        actions = transition.get("actions") or []
        if any(
            isinstance(action, Mapping)
            and action.get("kind") == "move_stop_to_break_even_next_open"
            for action in actions
        ):
            break_even_transitions.append(transition)
            count = _replace_break_even_thresholds(
                transition.get("guard") or {},
                value=float(parameters["breakEvenUnrealizedR"]),
            )
            if count:
                repairs.append(
                    {
                        "kind": "break_even_threshold",
                        "transitionId": transition.get("id"),
                        "replacement": float(parameters["breakEvenUnrealizedR"]),
                        "nodeCount": count,
                    }
                )

    library = output["executionConfig"]["managementLibrary"]
    plans = library.get("plans") or []
    explicit = False
    for plan in plans:
        trailing = plan.get("trailingStop")
        if not isinstance(trailing, dict):
            continue
        activation = trailing.get("activation")
        activation_kind = (
            activation.get("kind") if isinstance(activation, Mapping) else None
        )
        activation_r = (
            float(activation.get("value"))
            if activation_kind in {"after_unrealized_r", "after_r_and_age"}
            and activation.get("value") is not None
            else None
        )
        if activation_kind == "explicit":
            explicit = True
        if break_even_transitions and activation_kind != "explicit":
            replacement = None
            if activation_kind != "after_unrealized_r" or float(
                activation.get("value") or 0.0
            ) < float(parameters["trailingAfterBreakEvenUnrealizedR"]):
                replacement = {
                    "kind": "after_unrealized_r",
                    "value": float(parameters["trailingAfterBreakEvenUnrealizedR"]),
                }
            if replacement is not None:
                trailing["activation"] = replacement
                activation_r = float(replacement["value"])
                repairs.append(
                    {
                        "kind": "order_trailing_after_break_even",
                        "planId": plan.get("id"),
                        "replacement": replacement,
                    }
                )
        target = plan.get("initialTarget")
        if (
            break_even_transitions
            and isinstance(target, dict)
            and target.get("kind") == "reward_multiple"
            and float(target.get("multiple") or 0.0)
            < float(parameters["minimumRewardMultipleWithBreakEven"])
        ):
            target["multiple"] = float(parameters["minimumRewardMultipleWithBreakEven"])
            repairs.append(
                {
                    "kind": "preserve_break_even_runway",
                    "planId": plan.get("id"),
                    "replacement": float(parameters["minimumRewardMultipleWithBreakEven"]),
                }
            )
        if activation_r is not None and isinstance(target, dict):
            if (
                target.get("kind") == "reward_multiple"
                and float(target.get("multiple") or 0.0) <= activation_r
            ):
                replacement = max(activation_r + 0.5, 1.5)
                target["multiple"] = replacement
                repairs.append(
                    {
                        "kind": "preserve_trailing_activation_runway",
                        "planId": plan.get("id"),
                        "replacement": {"kind": "reward_multiple", "multiple": replacement},
                    }
                )
            stop = plan.get("initialStop")
            if (
                target.get("kind") == "fixed_percent"
                and isinstance(stop, Mapping)
                and stop.get("kind") == "fixed_percent"
                and float(target.get("percent") or 0.0)
                / float(stop.get("percent") or 1.0)
                <= activation_r
            ):
                replacement = float(stop["percent"]) * (activation_r + 0.5)
                if replacement >= 100.0:
                    raise TemporalDiscoveryContractError(
                        "activation-aware target runway exceeds fixed-percent schema"
                    )
                target["percent"] = replacement
                repairs.append(
                    {
                        "kind": "preserve_trailing_activation_runway",
                        "planId": plan.get("id"),
                        "replacement": {"kind": "fixed_percent", "percent": replacement},
                    }
                )

    if explicit:
        _inject_explicit_trailing_routes(
            graph,
            repairs=repairs,
            minimum_unrealized_r=(
                float(parameters["trailingAfterBreakEvenUnrealizedR"])
                if break_even_transitions
                else None
            ),
        )
    return _clone(output, name="activation-aware repaired profile"), repairs


def inspect_management_reachability(profile: Mapping[str, Any]) -> dict[str, Any]:
    graph = _graph(profile)
    transitions = _transitions(profile)
    initial = str(graph.get("initialStateId") or "")
    all_reached = _reachable_from(profile, {initial})
    filled_destinations = _filled_destinations(profile)
    post_entry_reached = _reachable_from(profile, filled_destinations)
    default_plan, plans = _management(profile)
    plan_ids = {str(item.get("id") or "") for item in plans}
    entry_routes = _action_transitions(profile, kinds={"enter_next_open"})
    referenced_plans = set()
    issues = []
    if not entry_routes:
        issues.append("no_entry_route")
    for transition, action in entry_routes:
        plan_id = str(action.get("managementPlanId") or default_plan or "")
        if not plan_id or plan_id not in plan_ids:
            issues.append("entry_route_unknown_management_plan")
        else:
            referenced_plans.add(plan_id)
        if str(transition.get("sourceStateId") or "") not in all_reached:
            issues.append("entry_route_unreachable")

    orphan_plans = sorted(plan_ids - referenced_plans)
    if orphan_plans:
        issues.append("orphan_management_plan")
    if not filled_destinations:
        issues.append("no_entry_fill_transition")

    action_rows = []
    management_kinds = {
        "move_stop_to_break_even_next_open",
        "tighten_stop_next_open",
        "set_target_next_open",
        "cancel_target_next_open",
        "activate_trailing_stop_next_open",
        "deactivate_trailing_stop_next_open",
    }
    for transition, action in _action_transitions(profile, kinds=management_kinds):
        source = str(transition.get("sourceStateId") or "")
        row = {
            "transitionId": transition.get("id"),
            "sourceStateId": source,
            "eventClass": transition.get("eventClass"),
            "actionKind": action.get("kind"),
            "reachableFromInitial": source in all_reached,
            "reachableAfterEntry": source in post_entry_reached,
            "staticallyDominated": _transition_dominated(profile, transition),
        }
        action_rows.append(row)
        if not row["reachableFromInitial"]:
            issues.append("management_action_unreachable")
        if action.get("kind") != "activate_trailing_stop_next_open" and not row[
            "reachableAfterEntry"
        ]:
            issues.append("management_action_not_post_entry")
        if row["staticallyDominated"]:
            issues.append("management_action_dominated")

    explicit_plan_ids = sorted(
        str(plan.get("id") or "")
        for plan in plans
        if isinstance(plan.get("trailingStop"), Mapping)
        and isinstance(plan["trailingStop"].get("activation"), Mapping)
        and plan["trailingStop"]["activation"].get("kind") == "explicit"
    )
    activation_rows = [
        row
        for row in action_rows
        if row["actionKind"] == "activate_trailing_stop_next_open"
    ]
    if explicit_plan_ids and not activation_rows:
        issues.append("explicit_trailing_missing_activation_action")

    break_even_rows = [
        row
        for row in action_rows
        if row["actionKind"] == "move_stop_to_break_even_next_open"
    ]
    for row in break_even_rows:
        if not row["reachableAfterEntry"]:
            issues.append("break_even_impossible_branch")

    issue_counts = Counter(issues)
    report = {
        "schemaVersion": GENERATOR_V2_REACHABILITY_SCHEMA,
        "generatorVersion": GENERATOR_V2_VERSION,
        "acceptable": not issue_counts,
        "initialStateId": initial,
        "reachableStates": sorted(all_reached),
        "entryFillDestinationStates": sorted(filled_destinations),
        "postEntryReachableStates": sorted(post_entry_reached),
        "managementPlanIds": sorted(plan_ids),
        "referencedManagementPlanIds": sorted(referenced_plans),
        "orphanManagementPlanIds": orphan_plans,
        "explicitTrailingPlanIds": explicit_plan_ids,
        "managementActions": sorted(
            action_rows,
            key=lambda item: (
                str(item["transitionId"]),
                str(item["actionKind"]),
            ),
        ),
        "issueCounts": dict(sorted(issue_counts.items())),
    }
    report["reachabilitySha256"] = canonical_sha256(report)
    return report


def _causal_coverage(causality_root: Path) -> dict[str, Any]:
    audit = audit_activation_causality(causality_root)
    report = _read(causality_root / "activation-causality.json", name="causality report")
    break_even = [
        item
        for item in report.get("instances") or []
        if item.get("managementType") == "break_even"
        and DEPTH_RANK[str(item.get("deepestReachedState"))]
        < DEPTH_RANK["activated_successfully"]
    ]
    addressed_states = {
        "source_state_never_occupied",
        "transition_never_evaluated",
        "guard_evaluated_but_never_true",
        "intent_scheduled_but_rejected",
    }
    addressed = [
        item for item in break_even if item.get("deepestReachedState") in addressed_states
    ]
    coverage = len(addressed) / len(break_even) if break_even else 1.0
    return {
        "causalityReportSha256": audit["reportSha256"],
        "causalityManifestSha256": audit["manifestSha256"],
        "dormantOrRejectedBreakEvenCount": len(break_even),
        "addressedBreakEvenCount": len(addressed),
        "addressedDeepestStates": sorted(addressed_states),
        "coverageRatio": coverage,
    }


def generate_policy_v2_population(
    source_preparation: Mapping[str, Any],
    *,
    validator: CandidateValidatorProtocol,
    causality_root: Path | str,
    output_root: Path | str,
    parameters: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = _normalize_preparation(source_preparation)
    config = _clone(
        parameters or GENERATOR_V2_PARAMETERS,
        name="generator v2 parameters",
    )
    parameter_profile = generator_v2_parameter_profile(config)
    coverage = _causal_coverage(Path(causality_root))
    if coverage["coverageRatio"] < float(config["causalCoverageMinimum"]):
        raise TemporalDiscoveryContractError(
            "generator v2 repair does not cover 80% of dormant/rejected break-even"
        )

    rng = random.Random(int(config["seed"]))
    targets = dict(config["sourceModeCounts"])
    accepted: list[dict[str, Any]] = []
    journal: list[dict[str, Any]] = []
    programs: set[str] = set()
    mode_counts = {key: 0 for key in targets}
    seeds = sorted(normalized["seeds"], key=lambda item: str(item["seedId"]))
    for ordinal in range(int(config["maxProposalAttempts"])):
        if len(accepted) == int(config["targetUniquePrograms"]):
            break
        source_mode = next(
            mode for mode in sorted(targets) if mode_counts[mode] < targets[mode]
        )
        if all(mode_counts[mode] < targets[mode] for mode in targets):
            source_mode = (
                "broad_seed_mutation" if ordinal % 2 == 0 else "seed_derived"
            )
        seed = seeds[rng.randrange(len(seeds))]
        count_key = (
            "broadMutationCount"
            if source_mode == "broad_seed_mutation"
            else "seedMutationCount"
        )
        count_range = config[count_key]
        mutation_count = rng.randint(int(count_range["min"]), int(count_range["max"]))
        profile, mutations = _mutate_profile(
            seed["sourceProfile"],
            rng=rng,
            source_mode=("de_novo" if source_mode == "broad_seed_mutation" else "seed_derived"),
            mutation_count=mutation_count,
            family_rotation=ordinal,
        )
        profile["description"] = (
            "Deterministically generated activation-aware temporal candidate; "
            f"sourceMode={source_mode}; mutationCount={len(mutations)}."
        )
        profile, repairs = _repair_profile(profile, parameters=config)
        reachability = inspect_management_reachability(profile)
        raw_sha = canonical_sha256(profile)
        journal_row: dict[str, Any] = {
            "proposalOrdinal": ordinal,
            "sourceMode": source_mode,
            "seedId": seed["seedId"],
            "rawSourceProfileSha256": raw_sha,
            "mutations": mutations,
            "activationAwareRepairs": repairs,
            "reachabilitySha256": reachability["reachabilitySha256"],
            "reachabilityIssueCounts": reachability["issueCounts"],
        }
        if reachability["acceptable"] is not True:
            journal_row["disposition"] = "static_reachability_rejected"
            journal.append(journal_row)
            continue

        provisional_id = "proposal_" + raw_sha.removeprefix("sha256:")[:24]
        validation = validator.validate(
            candidate_id=provisional_id,
            source_profile=profile,
            expected_raw_source_profile_sha256=raw_sha,
        )
        journal_row.update(
            {
                "candidateAcceptable": validation.get("candidateAcceptable"),
                "validationStatus": validation.get("status"),
                "validationReportSha256": validation.get("validationReportSha256"),
                "profileSnapshotSha256": validation.get("profileSnapshotSha256"),
                "validatedProgramSha256": validation.get("programSha256"),
                "issueCodes": sorted(
                    str(item.get("code"))
                    for item in validation.get("issues") or []
                    if isinstance(item, Mapping) and item.get("code")
                ),
            }
        )
        if validation.get("candidateAcceptable") is not True:
            journal_row["disposition"] = "fuzz_validator_rejected"
            journal.append(journal_row)
            continue
        program_sha = _sha(validation.get("programSha256"), name="program sha256")
        journal_row["programSha256"] = program_sha
        if program_sha in programs:
            journal_row["disposition"] = "duplicate_program"
            journal.append(journal_row)
            continue
        candidate_id = "td_" + program_sha.removeprefix("sha256:")[:28]
        if not _CANDIDATE.fullmatch(candidate_id):
            raise TemporalDiscoveryContractError("generated candidate ID is invalid")
        programs.add(program_sha)
        mode_counts[source_mode] += 1
        accepted.append(
            {
                "candidateId": candidate_id,
                "sourceMode": source_mode,
                "seedId": seed["seedId"],
                "proposalOrdinal": ordinal,
                "sourceProfile": profile,
                "sourceProfileSha256": raw_sha,
                "profileSnapshotSha256": _sha(
                    validation.get("profileSnapshotSha256"),
                    name="profile snapshot sha256",
                ),
                "programSha256": program_sha,
                "validationReportSha256": _sha(
                    validation.get("validationReportSha256"),
                    name="validation report sha256",
                ),
                "mutationTrace": mutations,
                "activationAwareRepairs": repairs,
                "managementReachability": reachability,
            }
        )
        journal_row["candidateId"] = candidate_id
        journal_row["disposition"] = "accepted"
        journal.append(journal_row)

    if len(accepted) != int(config["targetUniquePrograms"]):
        raise TemporalDiscoveryGenerationExhausted(
            f"generated {len(accepted)} unique valid programs; target was "
            f"{config['targetUniquePrograms']}"
        )
    if mode_counts != targets:
        raise TemporalDiscoveryGenerationExhausted(
            f"source-mode allocation mismatch: {mode_counts!r}"
        )
    accepted.sort(key=lambda item: item["candidateId"])
    config_artifact = {
        "schemaVersion": GENERATOR_V2_CONFIG_SCHEMA,
        "generatorVersion": GENERATOR_V2_VERSION,
        "sourcePreparationSha256": normalized["preparationSha256"],
        "sourceGeneratorVersion": normalized["generator"]["version"],
        "causalCoverage": coverage,
        "parameters": config,
        "parameterProfile": parameter_profile,
        "marketEvidenceRead": False,
        "gatewayContacted": False,
    }
    config_artifact["configSha256"] = canonical_sha256(config_artifact)
    population = {
        "schemaVersion": GENERATOR_V2_POPULATION_SCHEMA,
        "generatorVersion": GENERATOR_V2_VERSION,
        "configSha256": config_artifact["configSha256"],
        "targetUniquePrograms": config["targetUniquePrograms"],
        "sourceModeCounts": mode_counts,
        "candidateCount": len(accepted),
        "candidates": accepted,
    }
    population["populationSha256"] = canonical_sha256(population)
    generation_journal = {
        "schemaVersion": GENERATOR_V2_JOURNAL_SCHEMA,
        "generatorVersion": GENERATOR_V2_VERSION,
        "configSha256": config_artifact["configSha256"],
        "proposalCount": len(journal),
        "acceptedCount": len(accepted),
        "dispositionCounts": dict(
            sorted(Counter(item["disposition"] for item in journal).items())
        ),
        "entries": journal,
    }
    generation_journal["journalSha256"] = canonical_sha256(generation_journal)
    root = Path(output_root)
    _write_immutable(root / "config.json", config_artifact)
    _write_immutable(root / "population.json", population)
    _write_immutable(root / "generation-journal.json", generation_journal)
    manifest = _manifest(root, population_sha256=population["populationSha256"])
    return {
        "schemaVersion": "temporal_discovery_generator_v2_result_v1",
        "generatorVersion": GENERATOR_V2_VERSION,
        "configSha256": config_artifact["configSha256"],
        "populationSha256": population["populationSha256"],
        "journalSha256": generation_journal["journalSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "candidateCount": len(accepted),
        "proposalCount": len(journal),
        "sourceModeCounts": mode_counts,
        "marketEvidenceRead": False,
        "gatewayContacted": False,
    }


def audit_policy_v2_population(output_root: Path | str) -> dict[str, Any]:
    root = Path(output_root)
    config = _read(root / "config.json", name="generator v2 config")
    supplied_config = str(config.pop("configSha256", ""))
    if canonical_sha256(config) != supplied_config:
        raise TemporalDiscoveryContractError("generator v2 config identity mismatch")
    if config.get("schemaVersion") != GENERATOR_V2_CONFIG_SCHEMA:
        raise TemporalDiscoveryContractError("unknown generator v2 config schema")
    if config.get("generatorVersion") != GENERATOR_V2_VERSION:
        raise TemporalDiscoveryContractError("generator v2 version mismatch")
    parameters = _mapping(config.get("parameters"), name="generator v2 parameters")
    parameter_profile = generator_v2_parameter_profile(parameters)
    if config.get("parameterProfile") != parameter_profile:
        raise TemporalDiscoveryContractError("generator v2 parameter profile mismatch")
    population = _read(root / "population.json", name="generator v2 population")
    supplied_population = str(population.pop("populationSha256", ""))
    if canonical_sha256(population) != supplied_population:
        raise TemporalDiscoveryContractError("generator v2 population identity mismatch")
    population["populationSha256"] = supplied_population
    journal = _read(root / "generation-journal.json", name="generator v2 journal")
    supplied_journal = str(journal.pop("journalSha256", ""))
    if canonical_sha256(journal) != supplied_journal:
        raise TemporalDiscoveryContractError("generator v2 journal identity mismatch")
    manifest = _read(root / "manifest.json", name="generator v2 manifest")
    supplied_manifest = str(manifest.pop("manifestSha256", ""))
    if canonical_sha256(manifest) != supplied_manifest:
        raise TemporalDiscoveryContractError("generator v2 manifest identity mismatch")
    if manifest.get("populationSha256") != supplied_population:
        raise TemporalDiscoveryContractError("generator v2 manifest population mismatch")
    expected = set()
    for item in manifest.get("files") or []:
        path = root / str(item["relativePath"])
        expected.add(path.resolve())
        if (
            not path.is_file()
            or path.stat().st_size != int(item["length"])
            or _file_sha(path) != item["sha256"]
        ):
            raise TemporalDiscoveryContractError(f"generator v2 file mismatch: {path}")
    actual = {
        path.resolve()
        for path in root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    if actual != expected:
        raise TemporalDiscoveryContractError("generator v2 artifact inventory drift")
    candidates = population.get("candidates") or []
    target = int(parameters["targetUniquePrograms"])
    if (
        len(candidates) != target
        or len({item["programSha256"] for item in candidates}) != target
        or population.get("targetUniquePrograms") != target
        or population.get("candidateCount") != target
        or population.get("sourceModeCounts") != parameters["sourceModeCounts"]
    ):
        raise TemporalDiscoveryContractError(
            "generator v2 population does not match its admitted parameter profile"
        )
    if any(item["managementReachability"]["acceptable"] is not True for item in candidates):
        raise TemporalDiscoveryContractError("generator v2 population has reachability defects")
    return {
        "schemaVersion": "temporal_discovery_generator_v2_audit_v1",
        "ok": True,
        "configSha256": supplied_config,
        "populationSha256": supplied_population,
        "journalSha256": supplied_journal,
        "manifestSha256": supplied_manifest,
        "candidateCount": len(candidates),
        "parameterProfile": parameter_profile,
    }


def audit_management_witnesses(output_root: Path | str) -> dict[str, Any]:
    root = Path(output_root)
    report = _read(root / "report.json", name="management witness report")
    supplied_report = str(report.pop("reportSha256", ""))
    if canonical_sha256(report) != supplied_report:
        raise TemporalDiscoveryContractError("management witness report identity mismatch")
    manifest = _read(root / "manifest.json", name="management witness manifest")
    supplied_manifest = str(manifest.pop("manifestSha256", ""))
    if canonical_sha256(manifest) != supplied_manifest:
        raise TemporalDiscoveryContractError("management witness manifest identity mismatch")
    if manifest.get("reportSha256") != supplied_report:
        raise TemporalDiscoveryContractError("management witness report/manifest mismatch")
    expected = set()
    for item in manifest.get("files") or []:
        path = root / str(item["relativePath"])
        expected.add(path.resolve())
        if (
            not path.is_file()
            or path.stat().st_size != int(item["length"])
            or _file_sha(path) != item["sha256"]
        ):
            raise TemporalDiscoveryContractError(f"management witness mismatch: {path}")
    actual = {
        path.resolve()
        for path in root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    if actual != expected:
        raise TemporalDiscoveryContractError("management witness inventory drift")
    positive_counts = report.get("positiveWitnessCounts") or {}
    negative_counts = report.get("negativeWitnessCounts") or {}
    authored_count = int(report.get("authoredCapabilityCount") or 0)
    witness_count = int(report.get("witnessCount") or 0)
    if (
        report.get("allChecksPassed") is not True
        or sum(int(value) for value in positive_counts.values()) != authored_count
        or set(positive_counts) != {"break_even", "trailing_stop"}
        or negative_counts != {"break_even": 1, "trailing_stop": 1}
        or witness_count != authored_count + 2
        or report.get("restartExactWitnessCount") != witness_count
    ):
        raise TemporalDiscoveryContractError("management witness admission counts mismatch")
    return {
        "schemaVersion": "temporal_policy_v2_management_witness_audit_v1",
        "ok": True,
        "reportSha256": supplied_report,
        "manifestSha256": supplied_manifest,
        "authoredCapabilityCount": report["authoredCapabilityCount"],
        "witnessCount": report["witnessCount"],
        "restartExactWitnessCount": report["restartExactWitnessCount"],
    }


__all__ = [
    "GENERATOR_V2_PARAMETERS",
    "GENERATOR_V2_VERSION",
    "audit_management_witnesses",
    "audit_policy_v2_population",
    "generate_policy_v2_population",
    "inspect_management_reachability",
]
