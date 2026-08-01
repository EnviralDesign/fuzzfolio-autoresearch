"""Deterministic entry setup/confirmation structural operator.

This first operator is a representational admission probe, not an assertion
that delayed confirmation is economically superior.  It splits one compound
direct-entry transition into a setup, armed confirmation, invalidation, and
finite-expiry lifecycle without inventing indicators or event bindings.
"""

from __future__ import annotations

import copy
from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    _clone,
    _sha,
    canonical_sha256,
)
from .temporal_structural_operators import (
    finalize_application,
    finalize_audit,
    finalize_plan,
)

OPERATOR_ID = "split_entry_setup_confirmation_v1"
OPERATOR_VERSION = "1"
OPERATOR_SPEC_SCHEMA = "temporal_structural_operator_spec_v1"
APPLICABILITY_REPORT_SCHEMA = "temporal_confirmed_entry_applicability_v1"
MIN_CONFIRMATION_STATE_AGE_EVENTS = 1
EXPIRY_STATE_AGE_EVENTS = 3


def _canonical(value: Any) -> Any:
    return _clone(value, name="confirmed-entry operator value")


def _graph(profile: Mapping[str, Any]) -> dict[str, Any]:
    graph = profile.get("graph")
    if not isinstance(graph, Mapping):
        raise TemporalDiscoveryContractError("candidate graph must be an object")
    return dict(graph)


def _transitions(profile: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(value)
        for value in _graph(profile).get("transitions") or []
        if isinstance(value, Mapping)
    ]


def _walk_guard(
    value: Mapping[str, Any], path: tuple[str | int, ...] = ()
) -> Iterable[tuple[tuple[str | int, ...], Mapping[str, Any]]]:
    yield path, value
    child = value.get("guard")
    if isinstance(child, Mapping):
        yield from _walk_guard(child, (*path, "guard"))
    for index, item in enumerate(value.get("guards") or []):
        if isinstance(item, Mapping):
            yield from _walk_guard(item, (*path, "guards", index))


def _binding_identities(value: Mapping[str, Any]) -> set[str]:
    identities: set[str] = set()
    for _, guard in _walk_guard(value):
        kind = guard.get("kind")
        if kind in {"evidence_at_least", "evidence_below", "condition_streak_at_least"}:
            group_id = str(guard.get("groupId") or "")
            if group_id:
                identities.add(f"group:{group_id}")
        elif kind in {"fresh_event", "event_age_at_most"}:
            event_id = str(guard.get("eventId") or "")
            if event_id:
                identities.add(f"event:{event_id}")
    return identities


def _invertible_evidence_paths(
    value: Mapping[str, Any], path: tuple[str | int, ...]
) -> Iterable[tuple[tuple[str | int, ...], Mapping[str, Any]]]:
    kind = value.get("kind")
    if kind in {"evidence_at_least", "evidence_below"}:
        yield path, value
        return
    child = value.get("guard")
    if (
        kind == "not"
        and isinstance(child, Mapping)
        and child.get("kind") in {"evidence_at_least", "evidence_below"}
    ):
        # The outer not is the directional atom.  Do not also enumerate its
        # child, which would produce a semantically different duplicate.
        yield path, value
        return
    if isinstance(child, Mapping):
        yield from _invertible_evidence_paths(child, (*path, "guard"))
    for index, item in enumerate(value.get("guards") or []):
        if isinstance(item, Mapping):
            yield from _invertible_evidence_paths(item, (*path, "guards", index))


def _guard_at_path(
    root: Mapping[str, Any], path: Sequence[str | int]
) -> dict[str, Any]:
    current: Any = root
    for item in path:
        current = current[item]
    if not isinstance(current, Mapping):
        raise TemporalDiscoveryContractError("operator guard path is not an object")
    return _canonical(current)


def _inverse_guard(value: Mapping[str, Any]) -> dict[str, Any]:
    kind = value.get("kind")
    if kind == "evidence_at_least":
        inverse = _canonical(value)
        inverse["kind"] = "evidence_below"
        return inverse
    if kind == "evidence_below":
        inverse = _canonical(value)
        inverse["kind"] = "evidence_at_least"
        return inverse
    if (
        kind == "not"
        and isinstance(value.get("guard"), Mapping)
        and value["guard"].get("kind") in {"evidence_at_least", "evidence_below"}
    ):
        return _canonical(value["guard"])
    raise TemporalDiscoveryContractError("setup anchor is not directionally invertible")


def _operator_spec() -> dict[str, Any]:
    spec = {
        "schemaVersion": OPERATOR_SPEC_SCHEMA,
        "operatorId": OPERATOR_ID,
        "operatorVersion": OPERATOR_VERSION,
        "minimumConfirmationStateAgeEvents": MIN_CONFIRMATION_STATE_AGE_EVENTS,
        "expiryStateAgeEvents": EXPIRY_STATE_AGE_EVENTS,
        "applicabilityContract": {
            "decisionEntryTransitionCount": 1,
            "entryActionCount": 1,
            "unrelatedEntrySideEffectsAllowed": False,
            "minimumDistinctMarketBindings": 2,
            "requiresInvertibleSetupEvidence": True,
            "confirmationBindingMustBeDistinct": True,
            "confirmationMustPermitFiniteExpiry": True,
        },
    }
    spec["operatorSpecSha256"] = canonical_sha256(spec)
    return spec


OPERATOR_SPEC = _operator_spec()


def _event_polarities(
    value: Mapping[str, Any], *, negated: bool = False
) -> dict[str, set[str]]:
    output = {
        "positiveFresh": set(),
        "positiveAge": set(),
        "negativeFresh": set(),
        "negativeAge": set(),
    }
    kind = value.get("kind")
    if kind == "not" and isinstance(value.get("guard"), Mapping):
        return _event_polarities(value["guard"], negated=not negated)
    if kind in {"fresh_event", "event_age_at_most"}:
        event_id = str(value.get("eventId") or "")
        if event_id:
            key = ("negative" if negated else "positive") + (
                "Fresh" if kind == "fresh_event" else "Age"
            )
            output[key].add(event_id)
        return output
    child = value.get("guard")
    children = []
    if isinstance(child, Mapping):
        children.append(child)
    children.extend(
        item for item in value.get("guards") or [] if isinstance(item, Mapping)
    )
    for item in children:
        child_polarities = _event_polarities(item, negated=negated)
        for key, identities in child_polarities.items():
            output[key].update(identities)
    return output


def _confirmation_precludes_expiry(value: Mapping[str, Any]) -> bool:
    """Detect event-history tautologies that make the armed state unexpirable."""

    if value.get("kind") != "any":
        return False
    polarities = _event_polarities(value)
    return bool(
        polarities["negativeFresh"].intersection(polarities["positiveAge"])
        or polarities["negativeAge"].intersection(polarities["positiveFresh"])
    )


def _entry_transition(profile: Mapping[str, Any]) -> dict[str, Any] | None:
    matching: list[dict[str, Any]] = []
    for transition in _transitions(profile):
        if transition.get("eventClass") != "decision":
            continue
        actions = [
            action
            for action in transition.get("actions") or []
            if isinstance(action, Mapping)
        ]
        if any(action.get("kind") == "enter_next_open" for action in actions):
            matching.append(transition)
    if len(matching) != 1:
        return None
    actions = matching[0].get("actions") or []
    if len(actions) != 1 or actions[0].get("kind") != "enter_next_open":
        return None
    guard = matching[0].get("guard")
    if not isinstance(guard, Mapping) or guard.get("kind") != "all":
        return None
    if len(guard.get("guards") or []) < 2:
        return None
    return matching[0]


def enumerate_confirmed_entry_plans(
    profile: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Enumerate every canonical strict-v1 split plan in SHA order."""

    parent = _canonical(profile)
    transition = _entry_transition(parent)
    if transition is None:
        return []
    guard = transition["guard"]
    clauses = guard["guards"]
    all_bindings = set().union(
        *(
            _binding_identities(clause)
            for clause in clauses
            if isinstance(clause, Mapping)
        )
    )
    if len(all_bindings) < 2:
        return []

    plans: list[dict[str, Any]] = []
    for confirmation_index, confirmation_clause in enumerate(clauses):
        if not isinstance(confirmation_clause, Mapping):
            continue
        confirmation_bindings = _binding_identities(confirmation_clause)
        if len(confirmation_bindings) != 1:
            continue
        if _confirmation_precludes_expiry(confirmation_clause):
            continue
        retained = [
            clause
            for index, clause in enumerate(clauses)
            if index != confirmation_index
        ]
        retained_bindings = set().union(
            *(
                _binding_identities(clause)
                for clause in retained
                if isinstance(clause, Mapping)
            )
        )
        if not retained_bindings or confirmation_bindings & retained_bindings:
            continue
        position_absence_indices = [
            index
            for index, clause in enumerate(clauses)
            if index != confirmation_index
            and isinstance(clause, Mapping)
            and clause.get("kind") == "position_exists"
            and clause.get("expected") is False
        ]
        if not position_absence_indices:
            continue
        for clause_index, clause in enumerate(clauses):
            if clause_index == confirmation_index or not isinstance(clause, Mapping):
                continue
            for anchor_path, anchor in _invertible_evidence_paths(
                clause, ("guards", clause_index)
            ):
                anchor_bindings = _binding_identities(anchor)
                if not anchor_bindings or anchor_bindings & confirmation_bindings:
                    continue
                plan = finalize_plan(
                    {
                        "operatorId": OPERATOR_ID,
                        "operatorVersion": OPERATOR_VERSION,
                        "operatorSpecSha256": OPERATOR_SPEC["operatorSpecSha256"],
                        "parentSourceProfileSha256": canonical_sha256(parent),
                        "targetTransitionId": transition["id"],
                        "confirmationClauseIndex": confirmation_index,
                        "confirmationBindingIdentity": next(
                            iter(sorted(confirmation_bindings))
                        ),
                        "confirmationClauseSha256": canonical_sha256(
                            confirmation_clause
                        ),
                        "positionAbsenceClauseIndices": position_absence_indices,
                        "setupAnchorPath": list(anchor_path),
                        "setupAnchorSha256": canonical_sha256(anchor),
                        "minimumConfirmationStateAgeEvents": (
                            MIN_CONFIRMATION_STATE_AGE_EVENTS
                        ),
                        "expiryStateAgeEvents": EXPIRY_STATE_AGE_EVENTS,
                    }
                )
                plans.append(plan)
    unique = {plan["planSha256"]: plan for plan in plans}
    return [unique[key] for key in sorted(unique)]


def inspect_confirmed_entry_applicability(
    profile: Mapping[str, Any],
) -> dict[str, Any]:
    """Explain strict applicability without weakening plan enumeration."""

    parent = _canonical(profile)
    transition = _entry_transition(parent)
    plans = enumerate_confirmed_entry_plans(parent)
    issues: list[str] = []
    if transition is None:
        issues.append("entry_transition_contract_not_met")
    else:
        clauses = transition["guard"].get("guards") or []
        if any(
            isinstance(clause, Mapping) and _confirmation_precludes_expiry(clause)
            for clause in clauses
        ):
            issues.append("confirmation_tautological_over_event_age")
        if not plans and not issues:
            issues.append("no_strict_confirmation_split_plan")
    report = {
        "schemaVersion": APPLICABILITY_REPORT_SCHEMA,
        "operatorId": OPERATOR_ID,
        "operatorSpecSha256": OPERATOR_SPEC["operatorSpecSha256"],
        "sourceProfileSha256": canonical_sha256(parent),
        "applicable": bool(plans),
        "planCount": len(plans),
        "planSha256s": [plan["planSha256"] for plan in plans],
        "issueCodes": sorted(set(issues)),
    }
    report["reportSha256"] = canonical_sha256(report)
    return report


def _derived_id(prefix: str, plan_sha256: str) -> str:
    return f"{prefix}_{plan_sha256.removeprefix('sha256:')[:16]}"


def _preview(
    profile: Mapping[str, Any], plan: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    parent = _canonical(profile)
    selected = _canonical(plan)
    if selected not in enumerate_confirmed_entry_plans(parent):
        raise TemporalDiscoveryContractError(
            "confirmed-entry plan is not a canonical applicable plan for parent"
        )
    transition = _entry_transition(parent)
    if transition is None or transition["id"] != selected["targetTransitionId"]:
        raise TemporalDiscoveryContractError(
            "confirmed-entry target transition changed"
        )
    original_guard = transition["guard"]
    confirmation_index = int(selected["confirmationClauseIndex"])
    confirmation_clause = _canonical(original_guard["guards"][confirmation_index])
    position_absence_clauses = [
        _canonical(original_guard["guards"][int(index)])
        for index in selected["positionAbsenceClauseIndices"]
    ]
    anchor = _guard_at_path(original_guard, selected["setupAnchorPath"])
    if canonical_sha256(confirmation_clause) != selected["confirmationClauseSha256"]:
        raise TemporalDiscoveryContractError("confirmation clause identity changed")
    if canonical_sha256(anchor) != selected["setupAnchorSha256"]:
        raise TemporalDiscoveryContractError("setup anchor identity changed")

    armed_state_id = _derived_id("armed_confirmation", selected["planSha256"])
    setup_transition_id = _derived_id("setup", selected["planSha256"])
    confirmation_transition_id = _derived_id("confirm", selected["planSha256"])
    invalidation_transition_id = _derived_id("invalidate", selected["planSha256"])
    expiry_transition_id = _derived_id("expire", selected["planSha256"])
    retained_clauses = [
        _canonical(clause)
        for index, clause in enumerate(original_guard["guards"])
        if index != confirmation_index
    ]
    setup_guard = {**_canonical(original_guard), "guards": retained_clauses}

    setup = {
        **_canonical(transition),
        "id": setup_transition_id,
        "destinationStateId": armed_state_id,
        "guard": setup_guard,
        "actions": [],
        "reasonCode": "entry_setup_armed",
    }
    confirmation = {
        **_canonical(transition),
        "id": confirmation_transition_id,
        "sourceStateId": armed_state_id,
        "priority": 10,
        "guard": {
            "kind": "all",
            "guards": [
                {
                    "kind": "state_age_at_least",
                    "events": MIN_CONFIRMATION_STATE_AGE_EVENTS,
                },
                *position_absence_clauses,
                confirmation_clause,
            ],
        },
        "reasonCode": "entry_setup_confirmed",
    }
    invalidation = {
        "id": invalidation_transition_id,
        "sourceStateId": armed_state_id,
        "destinationStateId": transition["sourceStateId"],
        "eventClass": "decision",
        "priority": 20,
        "guard": _inverse_guard(anchor),
        "actions": [],
        "reasonCode": "entry_setup_invalidated",
    }
    expiry = {
        "id": expiry_transition_id,
        "sourceStateId": armed_state_id,
        "destinationStateId": transition["sourceStateId"],
        "eventClass": "decision",
        "priority": 30,
        "guard": {
            "kind": "state_age_at_least",
            "events": EXPIRY_STATE_AGE_EVENTS,
        },
        "actions": [],
        "reasonCode": "entry_setup_expired",
    }

    child = copy.deepcopy(parent)
    graph = child["graph"]
    graph["states"] = [
        *graph.get("states", []),
        {
            "id": armed_state_id,
            "label": "Entry confirmation armed",
            "description": (
                "Deterministic intermediate state inserted by " + OPERATOR_ID
            ),
        },
    ]
    transformed_transitions: list[dict[str, Any]] = []
    for item in graph.get("transitions") or []:
        if item.get("id") == transition["id"]:
            transformed_transitions.append(setup)
        else:
            transformed_transitions.append(item)
    transformed_transitions.extend([confirmation, invalidation, expiry])
    graph["transitions"] = transformed_transitions
    delta = {
        "removedStateIds": [],
        "addedStates": [_canonical(graph["states"][-1])],
        "removedTransitions": [_canonical(transition)],
        "addedTransitions": [setup, confirmation, invalidation, expiry],
        "removedActions": _canonical(transition.get("actions") or []),
        "addedActions": _canonical(confirmation.get("actions") or []),
    }
    return child, delta


def _reachable_states(profile: Mapping[str, Any]) -> set[str]:
    graph = _graph(profile)
    initial = str(graph.get("initialStateId") or "")
    reached = {initial}
    queue = deque([initial])
    transitions = _transitions(profile)
    while queue:
        source = queue.popleft()
        for transition in transitions:
            if transition.get("sourceStateId") != source:
                continue
            destination = str(transition.get("destinationStateId") or "")
            if destination and destination not in reached:
                reached.add(destination)
                queue.append(destination)
    return reached


def _guard_is_statically_true(value: Mapping[str, Any]) -> bool:
    kind = value.get("kind")
    if kind == "always":
        return True
    if kind == "state_age_at_least" and int(value.get("events") or 0) <= 0:
        return True
    if kind == "all":
        guards = [
            item for item in value.get("guards") or [] if isinstance(item, Mapping)
        ]
        return bool(guards) and all(_guard_is_statically_true(item) for item in guards)
    if kind == "any":
        return any(
            _guard_is_statically_true(item)
            for item in value.get("guards") or []
            if isinstance(item, Mapping)
        )
    return False


def _new_transitions_are_not_dominated(
    transformed: Mapping[str, Any], added_ids: set[str]
) -> bool:
    for target in _transitions(transformed):
        if target.get("id") not in added_ids:
            continue
        peers = sorted(
            (
                item
                for item in _transitions(transformed)
                if item.get("sourceStateId") == target.get("sourceStateId")
                and item.get("eventClass") == target.get("eventClass")
            ),
            key=lambda item: (
                int(item.get("priority") or 0),
                str(item.get("id") or ""),
            ),
        )
        for peer in peers:
            if peer.get("id") == target.get("id"):
                break
            guard = peer.get("guard")
            if isinstance(guard, Mapping) and _guard_is_statically_true(guard):
                return False
    return True


def _audit_checks(
    parent: Mapping[str, Any],
    transformed: Mapping[str, Any],
    plan: Mapping[str, Any],
    delta: Mapping[str, Any],
) -> dict[str, bool]:
    parent_graph = _graph(parent)
    child_graph = _graph(transformed)
    original = delta["removedTransitions"][0]
    added = delta["addedTransitions"]
    setup, confirmation, invalidation, expiry = added
    target_id = str(plan["targetTransitionId"])
    parent_other = [
        item
        for item in parent_graph.get("transitions") or []
        if item.get("id") != target_id
    ]
    child_other = [
        item
        for item in child_graph.get("transitions") or []
        if item.get("id") not in {entry["id"] for entry in added}
    ]
    original_state_ids = {str(item["id"]) for item in parent_graph.get("states") or []}
    child_state_ids = {str(item["id"]) for item in child_graph.get("states") or []}
    reachable = _reachable_states(transformed)
    child_entry_routes = [
        item
        for item in _transitions(transformed)
        if any(
            isinstance(action, Mapping) and action.get("kind") == "enter_next_open"
            for action in item.get("actions") or []
        )
    ]
    confirmation_bindings = _binding_identities(
        original["guard"]["guards"][int(plan["confirmationClauseIndex"])]
    )
    setup_bindings = _binding_identities(setup["guard"])
    graph_metadata_keys = sorted(set(parent_graph) - {"states", "transitions"})
    return {
        "parent_identity_bound": (
            canonical_sha256(parent) == plan["parentSourceProfileSha256"]
        ),
        "management_byte_identical": (
            _canonical(parent.get("executionConfig"))
            == _canonical(transformed.get("executionConfig"))
        ),
        "non_target_graph_metadata_byte_identical": all(
            _canonical(parent_graph.get(key)) == _canonical(child_graph.get(key))
            for key in graph_metadata_keys
        ),
        "non_target_transitions_byte_identical": (
            _canonical(parent_other) == _canonical(child_other)
        ),
        "original_states_byte_identical": (
            _canonical(parent_graph.get("states") or [])
            == _canonical((child_graph.get("states") or [])[:-1])
        ),
        "exactly_one_armed_state_added": (
            len(child_state_ids - original_state_ids) == 1
            and len(child_state_ids) == len(original_state_ids) + 1
        ),
        "all_states_reachable": child_state_ids == reachable,
        "no_statically_dominated_added_transition": _new_transitions_are_not_dominated(
            transformed, {str(item["id"]) for item in added}
        ),
        "confirmation_binding_distinct_from_setup": bool(confirmation_bindings)
        and not confirmation_bindings.intersection(setup_bindings),
        "confirmation_requires_later_event": (
            confirmation["guard"]["guards"][0]
            == {
                "kind": "state_age_at_least",
                "events": MIN_CONFIRMATION_STATE_AGE_EVENTS,
            }
        ),
        "expiry_is_finite_and_frozen": (
            expiry["guard"]
            == {
                "kind": "state_age_at_least",
                "events": EXPIRY_STATE_AGE_EVENTS,
            }
        ),
        "invalidation_and_expiry_emit_no_intents": (
            invalidation.get("actions") == [] and expiry.get("actions") == []
        ),
        "management_action_and_destination_preserved": (
            confirmation.get("actions") == original.get("actions")
            and confirmation.get("destinationStateId")
            == original.get("destinationStateId")
        ),
        "no_direct_entry_bypass": (
            len(child_entry_routes) == 1
            and child_entry_routes[0].get("id") == confirmation.get("id")
            and child_entry_routes[0].get("sourceStateId")
            == delta["addedStates"][0]["id"]
        ),
    }


def apply_confirmed_entry_plan(
    profile: Mapping[str, Any],
    plan: Mapping[str, Any],
    *,
    parent_validated_program_sha256: str,
    child_validated_program_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Apply one canonical plan and emit its content-bound application record."""

    parent_program_sha = _sha(
        parent_validated_program_sha256,
        name="parent validated program SHA-256",
    )
    child_program_sha = _sha(
        child_validated_program_sha256,
        name="child validated program SHA-256",
    )
    child, delta = _preview(profile, plan)
    checks = _audit_checks(profile, child, plan, delta)
    audit = finalize_audit(
        checks,
        operatorId=OPERATOR_ID,
        operatorVersion=OPERATOR_VERSION,
        planSha256=plan["planSha256"],
        parentSourceProfileSha256=canonical_sha256(profile),
        childSourceProfileSha256=canonical_sha256(child),
    )
    if not audit["allChecksPassed"]:
        failed = sorted(name for name, passed in checks.items() if not passed)
        raise TemporalDiscoveryContractError(
            "confirmed-entry transformation failed invariants: " + ", ".join(failed)
        )
    application = finalize_application(
        {
            "operatorId": OPERATOR_ID,
            "operatorVersion": OPERATOR_VERSION,
            "operatorSpecSha256": OPERATOR_SPEC["operatorSpecSha256"],
            "planSha256": plan["planSha256"],
            "plan": _canonical(plan),
            "parentSourceProfileSha256": canonical_sha256(profile),
            "parentValidatedProgramSha256": parent_program_sha,
            "childSourceProfileSha256": canonical_sha256(child),
            "childValidatedProgramSha256": child_program_sha,
            "delta": delta,
            "staticInvariantReport": audit,
        }
    )
    return child, application


def preview_confirmed_entry_plan(
    profile: Mapping[str, Any], plan: Mapping[str, Any]
) -> dict[str, Any]:
    """Materialize a child for authoritative validation before final binding."""

    child, delta = _preview(profile, plan)
    checks = _audit_checks(profile, child, plan, delta)
    if not all(checks.values()):
        failed = sorted(name for name, passed in checks.items() if not passed)
        raise TemporalDiscoveryContractError(
            "confirmed-entry preview failed invariants: " + ", ".join(failed)
        )
    return child


def audit_confirmed_entry_application(
    parent_profile: Mapping[str, Any],
    transformed_profile: Mapping[str, Any],
    application_record: Mapping[str, Any],
) -> dict[str, Any]:
    """Reconstruct and verify an application record without trusting its delta."""

    application = _canonical(application_record)
    plan = application.get("plan")
    if not isinstance(plan, Mapping):
        raise TemporalDiscoveryContractError("application plan must be an object")
    expected_child, delta = _preview(parent_profile, plan)
    checks = _audit_checks(parent_profile, expected_child, plan, delta)
    checks.update(
        {
            "transformed_profile_exact": (
                _canonical(transformed_profile) == expected_child
            ),
            "application_identity_exact": (
                canonical_sha256(
                    {
                        key: value
                        for key, value in application.items()
                        if key != "applicationSha256"
                    }
                )
                == application.get("applicationSha256")
            ),
            "application_delta_exact": application.get("delta") == delta,
            "application_parent_source_identity_exact": (
                application.get("parentSourceProfileSha256")
                == canonical_sha256(parent_profile)
            ),
            "application_child_source_identity_exact": (
                application.get("childSourceProfileSha256")
                == canonical_sha256(expected_child)
            ),
        }
    )
    return finalize_audit(
        checks,
        operatorId=OPERATOR_ID,
        operatorVersion=OPERATOR_VERSION,
        planSha256=plan["planSha256"],
        applicationSha256=application.get("applicationSha256"),
    )


class ConfirmedEntryStructuralOperator:
    operator_id = OPERATOR_ID
    operator_version = OPERATOR_VERSION
    specification = OPERATOR_SPEC

    def enumerate_plans(self, profile: Mapping[str, Any]) -> list[dict[str, Any]]:
        return enumerate_confirmed_entry_plans(profile)

    def apply(
        self,
        profile: Mapping[str, Any],
        plan: Mapping[str, Any],
        *,
        parent_validated_program_sha256: str,
        child_validated_program_sha256: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return apply_confirmed_entry_plan(
            profile,
            plan,
            parent_validated_program_sha256=parent_validated_program_sha256,
            child_validated_program_sha256=child_validated_program_sha256,
        )

    def audit(
        self,
        parent_profile: Mapping[str, Any],
        transformed_profile: Mapping[str, Any],
        application_record: Mapping[str, Any],
    ) -> dict[str, Any]:
        return audit_confirmed_entry_application(
            parent_profile, transformed_profile, application_record
        )


__all__ = [
    "EXPIRY_STATE_AGE_EVENTS",
    "MIN_CONFIRMATION_STATE_AGE_EVENTS",
    "OPERATOR_ID",
    "OPERATOR_SPEC",
    "ConfirmedEntryStructuralOperator",
    "apply_confirmed_entry_plan",
    "audit_confirmed_entry_application",
    "enumerate_confirmed_entry_plans",
    "inspect_confirmed_entry_applicability",
    "preview_confirmed_entry_plan",
]
