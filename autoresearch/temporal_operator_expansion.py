"""The seven bounded structural operators admitted after confirmed entry.

This module intentionally implements seven concrete transformations rather than
a general graph-rewrite language.  Enumeration is pure and exhaustive; policy
selection and randomness belong to the later QD engine.
"""

from __future__ import annotations

import copy
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

OPERATOR_VERSION = "1"
OPERATOR_SPEC_SCHEMA = "temporal_structural_operator_spec_v1"
MAX_GUARD_DEPTH = 4
MAX_GUARDS_PER_COMPOSITE = 8
MAX_GRAPH_STATES = 32
MAX_GRAPH_TRANSITIONS = 128

EDGE_TRIGGER_PREDICATE = "edge_trigger_predicate_v1"
EVENT_AGE_WINDOW = "event_age_window_v1"
REQUIRE_CONSECUTIVE_TRUE = "require_consecutive_true_v1"
SEQUENCE_ACTION_GATE = "sequence_action_gate_v1"
REPEAT_ACTION_COOLDOWN = "repeat_action_cooldown_v1"
MINIMUM_POSITION_AGE_GATE = "minimum_position_age_gate_v1"
MAXIMUM_POSITION_AGE_EXIT = "maximum_position_age_exit_v1"

EVENT_AGE_WINDOWS = ((0, 1), (0, 3), (1, 1), (1, 3), (2, 5))
CONSECUTIVE_COUNTS = (2, 3, 5)
COOLDOWN_COUNTS = (1, 3, 5)
MINIMUM_POSITION_AGES = (1, 3, 5, 8)
MAXIMUM_POSITION_AGES = (3, 5, 8, 13)

LEVEL_PREDICATE_KINDS = frozenset(
    {
        "evidence_at_least",
        "evidence_below",
        "utc_time_window",
        "state_age_at_least",
        "state_age_at_most",
        "position_exists",
        "position_age_at_least",
        "unrealized_r_at_least",
        "unrealized_r_at_most",
    }
)
MUTABLE_LEVEL_PREDICATE_KINDS = LEVEL_PREDICATE_KINDS - {"position_exists"}
POSITION_ACTION_KINDS = frozenset(
    {
        "exit_next_open",
        "move_stop_to_break_even_next_open",
        "tighten_stop_next_open",
        "set_target_next_open",
        "cancel_target_next_open",
        "activate_trailing_stop_next_open",
        "deactivate_trailing_stop_next_open",
    }
)
REPEATABLE_MANAGEMENT_ACTION_KINDS = POSITION_ACTION_KINDS - {"exit_next_open"}


def _canonical(value: Any) -> Any:
    return _clone(value, name="temporal operator expansion value")


def _graph(profile: Mapping[str, Any]) -> Mapping[str, Any]:
    graph = profile.get("graph")
    if not isinstance(graph, Mapping):
        raise TemporalDiscoveryContractError("candidate graph must be an object")
    return graph


def _transitions(profile: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(item)
        for item in _graph(profile).get("transitions") or []
        if isinstance(item, Mapping)
    ]


def _walk_guard_occurrences(
    guard: Mapping[str, Any], path: tuple[str | int, ...] = ()
) -> Iterable[tuple[tuple[str | int, ...], Mapping[str, Any]]]:
    yield path, guard
    child = guard.get("guard")
    if isinstance(child, Mapping):
        yield from _walk_guard_occurrences(child, (*path, "guard"))
    for index, item in enumerate(guard.get("guards") or []):
        if isinstance(item, Mapping):
            yield from _walk_guard_occurrences(item, (*path, "guards", index))


def _guard_at_path(
    root: Mapping[str, Any], path: Sequence[str | int]
) -> dict[str, Any]:
    value: Any = root
    for part in path:
        value = value[part]
    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError("guard occurrence path is not an object")
    return _canonical(value)


def _replace_at_path(
    root: dict[str, Any], path: Sequence[str | int], value: Any
) -> None:
    if not path:
        root.clear()
        root.update(_canonical(value))
        return
    parent: Any = root
    for part in path[:-1]:
        parent = parent[part]
    parent[path[-1]] = _canonical(value)


def _guard_depth(guard: Mapping[str, Any]) -> int:
    children: list[Mapping[str, Any]] = []
    child = guard.get("guard")
    if isinstance(child, Mapping):
        children.append(child)
    children.extend(
        item for item in guard.get("guards") or [] if isinstance(item, Mapping)
    )
    predicate = guard.get("predicate")
    if isinstance(predicate, Mapping):
        children.append(predicate)
    return 1 + max((_guard_depth(item) for item in children), default=0)


def _guard_shape_valid(guard: Mapping[str, Any]) -> bool:
    if _guard_depth(guard) > MAX_GUARD_DEPTH:
        return False
    for _, item in _walk_guard_occurrences(guard):
        if item.get("kind") in {"all", "any"}:
            children = item.get("guards") or []
            if not 1 <= len(children) <= MAX_GUARDS_PER_COMPOSITE:
                return False
    return True


def _contains_kind(guard: Mapping[str, Any], kinds: set[str] | frozenset[str]) -> bool:
    return any(item.get("kind") in kinds for _, item in _walk_guard_occurrences(guard))


def _known_event_tautology(guard: Mapping[str, Any]) -> bool:
    for _, item in _walk_guard_occurrences(guard):
        if item.get("kind") != "any":
            continue
        negative_fresh: set[str] = set()
        zero_minimum_age: set[str] = set()
        for child in item.get("guards") or []:
            if not isinstance(child, Mapping):
                continue
            if (
                child.get("kind") == "not"
                and isinstance(child.get("guard"), Mapping)
                and child["guard"].get("kind") == "fresh_event"
            ):
                negative_fresh.add(str(child["guard"].get("eventId") or ""))
            if child.get("kind") == "event_age_at_most":
                zero_minimum_age.add(str(child.get("eventId") or ""))
            if (
                child.get("kind") == "event_age_window"
                and int(child.get("minimumEvents") or 0) == 0
            ):
                zero_minimum_age.add(str(child.get("eventId") or ""))
        if negative_fresh.intersection(zero_minimum_age):
            return True
    return False


def _occurrence_sha256(
    *,
    operator_id: str,
    parent_sha256: str,
    transition_id: str,
    guard_path: Sequence[str | int],
    guard: Mapping[str, Any],
) -> str:
    return canonical_sha256(
        {
            "schemaVersion": "temporal_graph_occurrence_identity_v1",
            "operatorId": operator_id,
            "operatorVersion": OPERATOR_VERSION,
            "transitionId": transition_id,
            "sourceGuardSha256": canonical_sha256(guard),
        }
    )


def _occurrence_is_unique(
    transition: Mapping[str, Any], occurrence: Mapping[str, Any]
) -> bool:
    identity = canonical_sha256(occurrence)
    return (
        sum(
            canonical_sha256(item) == identity
            for _, item in _walk_guard_occurrences(transition["guard"])
        )
        == 1
    )


def _and_guard(original: Mapping[str, Any], added: Mapping[str, Any]) -> dict[str, Any]:
    if original.get("kind") == "all":
        return {
            **_canonical(original),
            "guards": [*_canonical(original["guards"]), _canonical(added)],
        }
    return {"kind": "all", "guards": [_canonical(original), _canonical(added)]}


def _or_guard(original: Mapping[str, Any], added: Mapping[str, Any]) -> dict[str, Any]:
    if original.get("kind") == "any":
        return {
            **_canonical(original),
            "guards": [*_canonical(original["guards"]), _canonical(added)],
        }
    return {"kind": "any", "guards": [_canonical(original), _canonical(added)]}


def _operator_spec(operator_id: str, contract: Mapping[str, Any]) -> dict[str, Any]:
    value = {
        "schemaVersion": OPERATOR_SPEC_SCHEMA,
        "operatorId": operator_id,
        "operatorVersion": OPERATOR_VERSION,
        "contract": _canonical(contract),
    }
    value["operatorSpecSha256"] = canonical_sha256(value)
    return value


class _GuardRewriteOperator:
    operator_id = ""
    operator_version = OPERATOR_VERSION
    specification: dict[str, Any]

    def _sites(
        self, profile: Mapping[str, Any]
    ) -> Iterable[tuple[dict[str, Any], tuple[str | int, ...], dict[str, Any]]]:
        for transition in _transitions(profile):
            if transition.get("eventClass") != "decision":
                continue
            guard = transition.get("guard")
            if not isinstance(guard, Mapping):
                continue
            for path, occurrence in _walk_guard_occurrences(guard):
                yield transition, path, dict(occurrence)

    def _replacements(
        self,
        *,
        parent_sha256: str,
        transition: Mapping[str, Any],
        path: tuple[str | int, ...],
        occurrence: Mapping[str, Any],
    ) -> Iterable[tuple[dict[str, Any], dict[str, Any]]]:
        raise NotImplementedError

    def enumerate_plans(self, profile: Mapping[str, Any]) -> list[dict[str, Any]]:
        parent = _canonical(profile)
        parent_sha = canonical_sha256(parent)
        plans: dict[str, dict[str, Any]] = {}
        for transition, path, occurrence in self._sites(parent):
            for replacement, parameters in self._replacements(
                parent_sha256=parent_sha,
                transition=transition,
                path=path,
                occurrence=occurrence,
            ):
                candidate_root = _canonical(transition["guard"])
                _replace_at_path(candidate_root, path, replacement)
                if not _guard_shape_valid(candidate_root) or _known_event_tautology(
                    candidate_root
                ):
                    continue
                plan = finalize_plan(
                    {
                        "operatorId": self.operator_id,
                        "operatorVersion": self.operator_version,
                        "operatorSpecSha256": self.specification["operatorSpecSha256"],
                        "parentSourceProfileSha256": parent_sha,
                        "targetTransitionId": transition["id"],
                        "targetGuardPath": list(path),
                        "sourceGuardSha256": canonical_sha256(occurrence),
                        "replacementGuard": replacement,
                        "replacementGuardSha256": canonical_sha256(replacement),
                        "parameters": parameters,
                    }
                )
                plans[plan["planSha256"]] = plan
        return [plans[key] for key in sorted(plans)]

    def _preview(
        self, profile: Mapping[str, Any], plan: Mapping[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        parent = _canonical(profile)
        selected = _canonical(plan)
        if selected not in self.enumerate_plans(parent):
            raise TemporalDiscoveryContractError(
                f"{self.operator_id} plan is not canonical and applicable"
            )
        child = copy.deepcopy(parent)
        target = None
        for transition in child["graph"]["transitions"]:
            if transition.get("id") == selected["targetTransitionId"]:
                target = transition
                break
        if target is None:
            raise TemporalDiscoveryContractError("target transition disappeared")
        before = _guard_at_path(target["guard"], selected["targetGuardPath"])
        if canonical_sha256(before) != selected["sourceGuardSha256"]:
            raise TemporalDiscoveryContractError("target guard occurrence changed")
        _replace_at_path(
            target["guard"],
            selected["targetGuardPath"],
            selected["replacementGuard"],
        )
        delta = {
            "targetTransitionId": selected["targetTransitionId"],
            "targetGuardPath": selected["targetGuardPath"],
            "removedGuard": before,
            "addedGuard": _canonical(selected["replacementGuard"]),
        }
        return child, delta

    def preview(
        self, profile: Mapping[str, Any], plan: Mapping[str, Any]
    ) -> dict[str, Any]:
        return self._preview(profile, plan)[0]

    def apply(
        self,
        profile: Mapping[str, Any],
        plan: Mapping[str, Any],
        *,
        parent_validated_program_sha256: str,
        child_validated_program_sha256: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        child, delta = self._preview(profile, plan)
        checks = {
            "parent_identity_bound": canonical_sha256(profile)
            == plan["parentSourceProfileSha256"],
            "single_occurrence_rewritten": True,
            "replacement_identity_exact": canonical_sha256(delta["addedGuard"])
            == plan["replacementGuardSha256"],
            "graph_shape_within_native_limits": _guard_shape_valid(
                next(
                    item["guard"]
                    for item in child["graph"]["transitions"]
                    if item["id"] == plan["targetTransitionId"]
                )
            ),
        }
        audit = finalize_audit(
            checks,
            operatorId=self.operator_id,
            operatorVersion=self.operator_version,
            planSha256=plan["planSha256"],
        )
        application = finalize_application(
            {
                "operatorId": self.operator_id,
                "operatorVersion": self.operator_version,
                "operatorSpecSha256": self.specification["operatorSpecSha256"],
                "planSha256": plan["planSha256"],
                "plan": _canonical(plan),
                "parentSourceProfileSha256": canonical_sha256(profile),
                "parentValidatedProgramSha256": _sha(
                    parent_validated_program_sha256,
                    name="parent validated program SHA-256",
                ),
                "childSourceProfileSha256": canonical_sha256(child),
                "childValidatedProgramSha256": _sha(
                    child_validated_program_sha256,
                    name="child validated program SHA-256",
                ),
                "delta": delta,
                "staticInvariantReport": audit,
            }
        )
        return child, application

    def audit(
        self,
        parent_profile: Mapping[str, Any],
        transformed_profile: Mapping[str, Any],
        application_record: Mapping[str, Any],
    ) -> dict[str, Any]:
        application = _canonical(application_record)
        plan = application.get("plan")
        if not isinstance(plan, Mapping):
            raise TemporalDiscoveryContractError("application plan must be an object")
        expected, delta = self._preview(parent_profile, plan)
        checks = {
            "transformed_profile_exact": _canonical(transformed_profile) == expected,
            "application_delta_exact": application.get("delta") == delta,
            "application_identity_exact": canonical_sha256(
                {
                    key: value
                    for key, value in application.items()
                    if key != "applicationSha256"
                }
            )
            == application.get("applicationSha256"),
        }
        return finalize_audit(
            checks,
            operatorId=self.operator_id,
            operatorVersion=self.operator_version,
            planSha256=plan["planSha256"],
        )


class EdgeTriggerPredicateOperator(_GuardRewriteOperator):
    operator_id = EDGE_TRIGGER_PREDICATE
    specification = _operator_spec(
        operator_id,
        {"directions": ["falling", "rising"], "firstObservation": "baseline_only"},
    )

    def _replacements(self, *, parent_sha256, transition, path, occurrence):
        if occurrence.get(
            "kind"
        ) not in MUTABLE_LEVEL_PREDICATE_KINDS or not _occurrence_is_unique(
            transition, occurrence
        ):
            return
        occurrence_sha = _occurrence_sha256(
            operator_id=self.operator_id,
            parent_sha256=parent_sha256,
            transition_id=transition["id"],
            guard_path=path,
            guard=occurrence,
        )
        for direction in ("falling", "rising"):
            yield (
                {
                    "kind": "predicate_edge",
                    "operatorId": self.operator_id,
                    "operatorVersion": self.operator_version,
                    "occurrenceSha256": occurrence_sha,
                    "direction": direction,
                    "predicate": _canonical(occurrence),
                },
                {"direction": direction, "occurrenceSha256": occurrence_sha},
            )


class EventAgeWindowOperator(_GuardRewriteOperator):
    operator_id = EVENT_AGE_WINDOW
    specification = _operator_spec(operator_id, {"inclusiveWindows": EVENT_AGE_WINDOWS})

    def _replacements(self, *, parent_sha256, transition, path, occurrence):
        if occurrence.get("kind") not in {"fresh_event", "event_age_at_most"}:
            return
        event_id = str(occurrence.get("eventId") or "")
        if not event_id:
            return
        for minimum, maximum in EVENT_AGE_WINDOWS:
            if (
                occurrence.get("kind") == "event_age_at_most"
                and minimum == 0
                and maximum == int(occurrence.get("events") or 0)
            ):
                continue
            yield (
                {
                    "kind": "event_age_window",
                    "eventId": event_id,
                    "minimumEvents": minimum,
                    "maximumEvents": maximum,
                },
                {"minimumEvents": minimum, "maximumEvents": maximum},
            )


class RequireConsecutiveTrueOperator(_GuardRewriteOperator):
    operator_id = REQUIRE_CONSECUTIVE_TRUE
    specification = _operator_spec(
        operator_id, {"evaluationCounts": CONSECUTIVE_COUNTS}
    )

    def _replacements(self, *, parent_sha256, transition, path, occurrence):
        if occurrence.get(
            "kind"
        ) not in MUTABLE_LEVEL_PREDICATE_KINDS or not _occurrence_is_unique(
            transition, occurrence
        ):
            return
        occurrence_sha = _occurrence_sha256(
            operator_id=self.operator_id,
            parent_sha256=parent_sha256,
            transition_id=transition["id"],
            guard_path=path,
            guard=occurrence,
        )
        for evaluations in CONSECUTIVE_COUNTS:
            yield (
                {
                    "kind": "consecutive_true",
                    "operatorId": self.operator_id,
                    "operatorVersion": self.operator_version,
                    "occurrenceSha256": occurrence_sha,
                    "predicate": _canonical(occurrence),
                    "evaluations": evaluations,
                },
                {"evaluations": evaluations, "occurrenceSha256": occurrence_sha},
            )


class RepeatActionCooldownOperator(_GuardRewriteOperator):
    operator_id = REPEAT_ACTION_COOLDOWN
    specification = _operator_spec(
        operator_id, {"cooldownEvaluations": COOLDOWN_COUNTS}
    )

    def _sites(self, profile):
        for transition in _transitions(profile):
            actions = transition.get("actions") or []
            if (
                transition.get("eventClass") != "decision"
                or transition.get("sourceStateId")
                != transition.get("destinationStateId")
                or len(actions) != 1
                or actions[0].get("kind") not in REPEATABLE_MANAGEMENT_ACTION_KINDS
                or _contains_kind(transition["guard"], {"action_cooldown_elapsed"})
            ):
                continue
            yield transition, (), _canonical(transition["guard"])

    def _replacements(self, *, parent_sha256, transition, path, occurrence):
        for evaluations in COOLDOWN_COUNTS:
            cooldown = {
                "kind": "action_cooldown_elapsed",
                "transitionId": transition["id"],
                "actionOrdinal": 0,
                "evaluations": evaluations,
            }
            yield _and_guard(occurrence, cooldown), {"evaluations": evaluations}


class MinimumPositionAgeGateOperator(_GuardRewriteOperator):
    operator_id = MINIMUM_POSITION_AGE_GATE
    specification = _operator_spec(
        operator_id, {"minimumPositionAges": MINIMUM_POSITION_AGES}
    )

    def _sites(self, profile):
        for transition in _transitions(profile):
            actions = transition.get("actions") or []
            if (
                transition.get("eventClass") != "decision"
                or len(actions) != 1
                or actions[0].get("kind") not in POSITION_ACTION_KINDS
                or _contains_kind(transition["guard"], {"position_age_at_least"})
            ):
                continue
            yield transition, (), _canonical(transition["guard"])

    def _replacements(self, *, parent_sha256, transition, path, occurrence):
        for events in MINIMUM_POSITION_AGES:
            yield (
                _and_guard(
                    occurrence,
                    {"kind": "position_age_at_least", "events": events},
                ),
                {"events": events},
            )


class MaximumPositionAgeExitOperator(_GuardRewriteOperator):
    operator_id = MAXIMUM_POSITION_AGE_EXIT
    specification = _operator_spec(
        operator_id, {"maximumPositionAges": MAXIMUM_POSITION_AGES}
    )

    def _sites(self, profile):
        for transition in _transitions(profile):
            actions = transition.get("actions") or []
            if (
                transition.get("eventClass") != "decision"
                or len(actions) != 1
                or actions[0].get("kind") != "exit_next_open"
                or _contains_kind(transition["guard"], {"position_age_at_least"})
            ):
                continue
            yield transition, (), _canonical(transition["guard"])

    def _replacements(self, *, parent_sha256, transition, path, occurrence):
        for events in MAXIMUM_POSITION_AGES:
            yield (
                _or_guard(
                    occurrence,
                    {"kind": "position_age_at_least", "events": events},
                ),
                {"events": events},
            )


def _derived_id(prefix: str, plan_sha256: str, suffix: str = "") -> str:
    digest = plan_sha256.removeprefix("sha256:")[:14]
    return f"{prefix}_{digest}{suffix}"


class SequenceActionGateOperator:
    operator_id = SEQUENCE_ACTION_GATE
    operator_version = OPERATOR_VERSION
    specification = _operator_spec(
        operator_id,
        {"minimumConfirmationAge": 1, "expiryAge": 3, "entryActionsExcluded": True},
    )

    def enumerate_plans(self, profile: Mapping[str, Any]) -> list[dict[str, Any]]:
        parent = _canonical(profile)
        parent_sha = canonical_sha256(parent)
        graph = _graph(parent)
        plans: dict[str, dict[str, Any]] = {}
        if len(graph.get("states") or []) >= MAX_GRAPH_STATES:
            return []
        for transition in _transitions(parent):
            actions = transition.get("actions") or []
            guard = transition.get("guard")
            if (
                transition.get("eventClass") != "decision"
                or len(actions) != 1
                or actions[0].get("kind") not in POSITION_ACTION_KINDS
                or not isinstance(guard, Mapping)
                or guard.get("kind") != "all"
                or len(guard.get("guards") or []) < 2
            ):
                continue
            clauses = guard["guards"]
            for index, clause in enumerate(clauses):
                if (
                    not isinstance(clause, Mapping)
                    or clause.get("kind") == "position_exists"
                ):
                    continue
                if not _occurrence_is_unique(transition, clause):
                    continue
                if clause.get("kind") in LEVEL_PREDICATE_KINDS:
                    setup_kind = "level_rising"
                elif clause.get("kind") == "fresh_event":
                    setup_kind = "native_event"
                else:
                    continue
                occurrence = _occurrence_sha256(
                    operator_id=self.operator_id,
                    parent_sha256=parent_sha,
                    transition_id=transition["id"],
                    guard_path=("guards", index),
                    guard=clause,
                )
                plan = finalize_plan(
                    {
                        "operatorId": self.operator_id,
                        "operatorVersion": self.operator_version,
                        "operatorSpecSha256": self.specification["operatorSpecSha256"],
                        "parentSourceProfileSha256": parent_sha,
                        "targetTransitionId": transition["id"],
                        "actionKind": actions[0]["kind"],
                        "setupClauseIndex": index,
                        "setupClauseSha256": canonical_sha256(clause),
                        "setupKind": setup_kind,
                        "setupOccurrenceSha256": occurrence,
                        "minimumConfirmationAge": 1,
                        "expiryAge": 3,
                    }
                )
                try:
                    child, _ = self._preview_unchecked(parent, plan)
                except TemporalDiscoveryContractError:
                    continue
                if len(child["graph"]["transitions"]) <= MAX_GRAPH_TRANSITIONS:
                    plans[plan["planSha256"]] = plan
        return [plans[key] for key in sorted(plans)]

    def _preview_unchecked(self, parent: Mapping[str, Any], plan: Mapping[str, Any]):
        child = copy.deepcopy(parent)
        transitions = child["graph"]["transitions"]
        target_index = next(
            (
                i
                for i, item in enumerate(transitions)
                if item.get("id") == plan["targetTransitionId"]
            ),
            None,
        )
        if target_index is None:
            raise TemporalDiscoveryContractError(
                "sequence target transition disappeared"
            )
        original = _canonical(transitions[target_index])
        clause_index = int(plan["setupClauseIndex"])
        clause = _canonical(original["guard"]["guards"][clause_index])
        if canonical_sha256(clause) != plan["setupClauseSha256"]:
            raise TemporalDiscoveryContractError("sequence setup occurrence changed")
        if plan["setupKind"] == "level_rising":
            setup_guard = {
                "kind": "predicate_edge",
                "operatorId": self.operator_id,
                "operatorVersion": OPERATOR_VERSION,
                "occurrenceSha256": plan["setupOccurrenceSha256"],
                "direction": "rising",
                "predicate": clause,
            }
        else:
            setup_guard = clause
        armed_id = _derived_id("armed_action", plan["planSha256"])
        setup = {
            **original,
            "id": _derived_id("arm_action", plan["planSha256"]),
            "destinationStateId": armed_id,
            "guard": setup_guard,
            "actions": [],
            "reasonCode": "action_setup_armed",
        }
        remaining = [
            _canonical(item)
            for index, item in enumerate(original["guard"]["guards"])
            if index != clause_index
        ]
        confirmation = {
            **original,
            "id": _derived_id("confirm_action", plan["planSha256"]),
            "sourceStateId": armed_id,
            "guard": {
                "kind": "all",
                "guards": [
                    {"kind": "state_age_at_least", "events": 1},
                    *remaining,
                ],
            },
            "reasonCode": "action_setup_confirmed",
        }
        if not _guard_shape_valid(confirmation["guard"]):
            raise TemporalDiscoveryContractError(
                "sequence confirmation exceeds guard limits"
            )

        source_id = original["sourceStateId"]
        outgoing = [
            _canonical(item)
            for item in transitions
            if item.get("sourceStateId") == source_id
            and item.get("id") != original["id"]
        ]
        decision_peers = [
            item for item in outgoing if item.get("eventClass") == "decision"
        ]
        execution_peers = [
            item for item in outgoing if item.get("eventClass") == "execution"
        ]
        ranked_decisions = [
            (int(original["priority"]), str(original["id"]), confirmation),
            *(
                (int(item["priority"]), str(item["id"]), item)
                for item in decision_peers
            ),
        ]
        cloned: list[dict[str, Any]] = []
        for rank, (_, _, item) in enumerate(sorted(ranked_decisions), start=1):
            if item is confirmation:
                confirmation["priority"] = rank * 10
                continue
            clone = {
                **item,
                "id": _derived_id("armed_route", plan["planSha256"], f"_{rank}"),
                "sourceStateId": armed_id,
                "priority": rank * 10,
            }
            cloned.append(clone)
        for index, item in enumerate(
            sorted(
                execution_peers,
                key=lambda value: (int(value["priority"]), str(value["id"])),
            )
        ):
            cloned.append(
                {
                    **item,
                    "id": _derived_id("armed_exec", plan["planSha256"], f"_{index}"),
                    "sourceStateId": armed_id,
                }
            )
        expiry_priority = 10 * (len(decision_peers) + 2)
        expiry = {
            "id": _derived_id("expire_action", plan["planSha256"]),
            "sourceStateId": armed_id,
            "destinationStateId": source_id,
            "eventClass": "decision",
            "priority": expiry_priority,
            "guard": {"kind": "state_age_at_least", "events": 3},
            "actions": [],
            "reasonCode": "action_setup_expired",
        }
        transitions[target_index] = setup
        transitions.extend([confirmation, *cloned, expiry])
        child["graph"]["states"].append(
            {
                "id": armed_id,
                "label": "Action confirmation armed",
                "description": (
                    "Bounded intermediate state inserted by sequence_action_gate_v1"
                ),
            }
        )
        delta = {
            "removedTransition": original,
            "addedState": _canonical(child["graph"]["states"][-1]),
            "addedTransitions": _canonical([setup, confirmation, *cloned, expiry]),
        }
        return child, delta

    def _preview(self, profile: Mapping[str, Any], plan: Mapping[str, Any]):
        parent = _canonical(profile)
        selected = _canonical(plan)
        if selected not in self.enumerate_plans(parent):
            raise TemporalDiscoveryContractError(
                "sequence action plan is not canonical and applicable"
            )
        return self._preview_unchecked(parent, selected)

    def preview(
        self, profile: Mapping[str, Any], plan: Mapping[str, Any]
    ) -> dict[str, Any]:
        return self._preview(profile, plan)[0]

    def apply(
        self,
        profile,
        plan,
        *,
        parent_validated_program_sha256,
        child_validated_program_sha256,
    ):
        child, delta = self._preview(profile, plan)
        audit = finalize_audit(
            {
                "parent_identity_bound": canonical_sha256(profile)
                == plan["parentSourceProfileSha256"],
                "one_armed_state_added": len(child["graph"]["states"])
                == len(profile["graph"]["states"]) + 1,
                "action_preserved_only_on_confirmation": sum(
                    1
                    for item in delta["addedTransitions"]
                    if item.get("actions") == delta["removedTransition"].get("actions")
                )
                == 1,
                "expiry_is_finite": delta["addedTransitions"][-1]["guard"]
                == {"kind": "state_age_at_least", "events": 3},
            },
            operatorId=self.operator_id,
            operatorVersion=self.operator_version,
            planSha256=plan["planSha256"],
        )
        application = finalize_application(
            {
                "operatorId": self.operator_id,
                "operatorVersion": self.operator_version,
                "operatorSpecSha256": self.specification["operatorSpecSha256"],
                "planSha256": plan["planSha256"],
                "plan": _canonical(plan),
                "parentSourceProfileSha256": canonical_sha256(profile),
                "parentValidatedProgramSha256": _sha(
                    parent_validated_program_sha256, name="parent program SHA-256"
                ),
                "childSourceProfileSha256": canonical_sha256(child),
                "childValidatedProgramSha256": _sha(
                    child_validated_program_sha256, name="child program SHA-256"
                ),
                "delta": delta,
                "staticInvariantReport": audit,
            }
        )
        return child, application

    def audit(self, parent_profile, transformed_profile, application_record):
        plan = application_record.get("plan")
        if not isinstance(plan, Mapping):
            raise TemporalDiscoveryContractError("application plan must be an object")
        expected, delta = self._preview(parent_profile, plan)
        return finalize_audit(
            {
                "transformed_profile_exact": _canonical(transformed_profile)
                == expected,
                "application_delta_exact": application_record.get("delta") == delta,
                "application_identity_exact": canonical_sha256(
                    {
                        key: value
                        for key, value in application_record.items()
                        if key != "applicationSha256"
                    }
                )
                == application_record.get("applicationSha256"),
            },
            operatorId=self.operator_id,
            operatorVersion=self.operator_version,
            planSha256=plan["planSha256"],
        )


def expanded_structural_operators() -> tuple[Any, ...]:
    return (
        EdgeTriggerPredicateOperator(),
        EventAgeWindowOperator(),
        RequireConsecutiveTrueOperator(),
        SequenceActionGateOperator(),
        RepeatActionCooldownOperator(),
        MinimumPositionAgeGateOperator(),
        MaximumPositionAgeExitOperator(),
    )


__all__ = [
    "CONSECUTIVE_COUNTS",
    "COOLDOWN_COUNTS",
    "EVENT_AGE_WINDOWS",
    "EdgeTriggerPredicateOperator",
    "EventAgeWindowOperator",
    "MaximumPositionAgeExitOperator",
    "MinimumPositionAgeGateOperator",
    "RepeatActionCooldownOperator",
    "RequireConsecutiveTrueOperator",
    "SequenceActionGateOperator",
    "expanded_structural_operators",
]
