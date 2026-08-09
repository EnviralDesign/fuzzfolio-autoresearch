"""Offline, content-addressed temporal guard mutations for module genomes.

This adapter deliberately does *not* register itself with a live QD authority.
It exposes the same enumerate/preview/apply/audit surface as the resource
operator layer, while using only the temporal guard spellings and finite grids
already admitted by :mod:`temporal_operator_expansion` and the Dashboard
temporal discovery contract.  Every candidate is reconstructed from the
parent, validated as a genome, and compiled before it is offered or applied.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from typing import Any

from .evolvable_module_genome import (
    EvolvableGenomeError,
    EvolvableModuleCompilerV1,
    EvolvableModuleGenomeV1,
    NativeModuleValidator,
)
from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    _EVENT_GRID,
    _POS_AGE_GRID,
    _TIME_WINDOWS,
    canonical_sha256,
)
from .temporal_operator_expansion import (
    CONSECUTIVE_COUNTS,
    COOLDOWN_COUNTS,
    EDGE_TRIGGER_PREDICATE,
    EVENT_AGE_WINDOWS,
    MUTABLE_LEVEL_PREDICATE_KINDS,
    REPEATABLE_MANAGEMENT_ACTION_KINDS,
    REQUIRE_CONSECUTIVE_TRUE,
)
from .temporal_structural_operators import (
    finalize_application,
    finalize_audit,
    finalize_plan,
)


TEMPORAL_GENOME_OPERATOR_VERSION = "evolvable_module_temporal_operators_v1"
TEMPORAL_GENOME_OPERATOR_SCHEMA = "evolvable_module_temporal_operator_plan_v1"


class GenomeTemporalOperatorError(EvolvableGenomeError):
    """A temporal guard mutation is not safe, typed, or replayable."""


def _clone(value: Any) -> Any:
    try:
        import json

        return json.loads(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise GenomeTemporalOperatorError("temporal mutation requires finite canonical JSON") from exc


def _guard_at(root: Mapping[str, Any], path: Sequence[str | int]) -> dict[str, Any]:
    value: Any = root
    for part in path:
        if isinstance(part, int):
            if not isinstance(value, list) or not 0 <= part < len(value):
                raise GenomeTemporalOperatorError("temporal guard path drift")
        elif not isinstance(value, Mapping) or part not in value:
            raise GenomeTemporalOperatorError("temporal guard path drift")
        value = value[part]
    if not isinstance(value, Mapping):
        raise GenomeTemporalOperatorError("temporal guard path is not an object")
    return _clone(dict(value))


def _replace_at(root: Mapping[str, Any], path: Sequence[str | int], replacement: Mapping[str, Any]) -> dict[str, Any]:
    value = _clone(dict(root))
    if not path:
        return _clone(dict(replacement))
    cursor: Any = value
    for part in path[:-1]:
        cursor = cursor[part]
    cursor[path[-1]] = _clone(dict(replacement))
    return value


def _walk(guard: Mapping[str, Any], path: tuple[str | int, ...] = ()) -> Iterable[tuple[tuple[str | int, ...], dict[str, Any]]]:
    current = _clone(dict(guard))
    yield path, current
    nested = current.get("predicate")
    if isinstance(nested, Mapping):
        yield from _walk(nested, (*path, "predicate"))
    nested = current.get("guard")
    if isinstance(nested, Mapping):
        yield from _walk(nested, (*path, "guard"))
    for index, item in enumerate(current.get("guards") or []):
        if isinstance(item, Mapping):
            yield from _walk(item, (*path, "guards", index))


def _all_with(guard: Mapping[str, Any], clause: Mapping[str, Any]) -> dict[str, Any]:
    original = _clone(dict(guard))
    if original.get("kind") == "all" and isinstance(original.get("guards"), list):
        return {"kind": "all", "guards": [*original["guards"], _clone(dict(clause))]}
    return {"kind": "all", "guards": [original, _clone(dict(clause))]}


class GenomeTemporalOperatorLayer:
    """Deterministic, compiler-validated temporal mutations for v1 genomes.

    ``native_validator`` is optional by design: this remains an offline
    adapter, yet callers with the Dashboard validation boundary can make every
    preview/apply fail closed through that authority as well.
    """

    def __init__(
        self,
        *,
        compiler: EvolvableModuleCompilerV1 | None = None,
        native_validator: NativeModuleValidator | None = None,
    ) -> None:
        self.compiler = compiler or EvolvableModuleCompilerV1()
        self.native_validator = native_validator
        self.specification = {
            "schemaVersion": TEMPORAL_GENOME_OPERATOR_SCHEMA,
            "operatorVersion": TEMPORAL_GENOME_OPERATOR_VERSION,
            "domains": {
                "utcSessionWindows": [list(value) for value in _TIME_WINDOWS],
                "eventAges": list(_EVENT_GRID),
                "positionAges": list(_POS_AGE_GRID),
                "eventAgeWindows": [list(value) for value in EVENT_AGE_WINDOWS],
                "consecutiveCounts": list(CONSECUTIVE_COUNTS),
                "cooldownCounts": list(COOLDOWN_COUNTS),
            },
            "guardFamilies": [
                "predicate_edge", "consecutive_true", "event_age_window",
                "fresh_event_absence", "state_or_position_age", "utc_session_window",
                "action_cooldown_elapsed",
            ],
            "compilerPolicySha256": self.compiler.policy.sha256,
            "nativeValidation": native_validator is not None,
        }
        self.specification["operatorSpecSha256"] = canonical_sha256(self.specification)

    def enumerate_plans(self, genome: EvolvableModuleGenomeV1) -> list[dict[str, Any]]:
        """Enumerate canonical plans whose full compiled child is admissible."""

        genome.validate()
        plans: dict[str, dict[str, Any]] = {}
        for construction in self._constructions(genome):
            try:
                child, _ = self._transform(genome, construction)
                self._validate_compilable(child)
            except (GenomeTemporalOperatorError, EvolvableGenomeError, TemporalDiscoveryContractError, KeyError, TypeError, ValueError):
                continue
            identity = {
                "schemaVersion": TEMPORAL_GENOME_OPERATOR_SCHEMA,
                "operatorVersion": TEMPORAL_GENOME_OPERATOR_VERSION,
                "operatorSpecSha256": self.specification["operatorSpecSha256"],
                "parentGenomeSha256": genome.identity_sha256,
                "construction": _clone(construction),
            }
            plan = finalize_plan({
                "operatorVersion": TEMPORAL_GENOME_OPERATOR_VERSION,
                "operatorSpecSha256": self.specification["operatorSpecSha256"],
                "parentGenomeSha256": genome.identity_sha256,
                "construction": _clone(construction),
                "constructionIdentitySha256": canonical_sha256(identity),
            })
            plans[plan["planSha256"]] = plan
        return [plans[key] for key in sorted(plans)]

    def preview(self, genome: EvolvableModuleGenomeV1, plan: Mapping[str, Any]) -> EvolvableModuleGenomeV1:
        return self._preview_with_trace(genome, plan)[0]

    def apply(self, genome: EvolvableModuleGenomeV1, plan: Mapping[str, Any]) -> tuple[EvolvableModuleGenomeV1, dict[str, Any]]:
        child, trace = self._preview_with_trace(genome, plan)
        report = self._static_report(genome, child, plan)
        if not report["allChecksPassed"]:
            raise GenomeTemporalOperatorError("temporal mutation invariant audit failed")
        application = finalize_application({
            "operatorVersion": TEMPORAL_GENOME_OPERATOR_VERSION,
            "operatorSpecSha256": self.specification["operatorSpecSha256"],
            "planSha256": plan["planSha256"],
            "constructionIdentitySha256": plan["constructionIdentitySha256"],
            "parentGenomeSha256": genome.identity_sha256,
            "childGenomeSha256": child.identity_sha256,
            "parentSemanticTopologySha256": genome.semantic_topology_signature(),
            "childSemanticTopologySha256": child.semantic_topology_signature(),
            "semanticDelta": trace,
            "staticInvariantReport": report,
        })
        return child, application

    def audit(self, parent: EvolvableModuleGenomeV1, child: EvolvableModuleGenomeV1, application: Mapping[str, Any]) -> dict[str, Any]:
        stored = _clone(application)
        identity = stored.pop("applicationSha256", None)
        plan = next((item for item in self.enumerate_plans(parent) if item["planSha256"] == stored.get("planSha256")), None)
        replay: EvolvableModuleGenomeV1 | None = None
        trace: list[dict[str, Any]] | None = None
        if plan is not None:
            try:
                replay, trace = self._preview_with_trace(parent, plan)
            except GenomeTemporalOperatorError:
                pass
        expected = self._static_report(parent, child, plan) if plan is not None else None
        return finalize_audit({
            "application_identity_exact": isinstance(identity, str) and canonical_sha256(stored) == identity,
            "plan_is_currently_applicable": plan is not None,
            "parent_identity_exact": stored.get("parentGenomeSha256") == parent.identity_sha256,
            "child_identity_exact": stored.get("childGenomeSha256") == child.identity_sha256,
            "replay_child_exact": replay is not None and replay.canonical() == child.canonical(),
            "semantic_delta_exact": trace is not None and stored.get("semanticDelta") == trace,
            "static_report_exact": stored.get("staticInvariantReport") == expected,
            "static_report_passing": isinstance(expected, Mapping) and expected.get("allChecksPassed") is True,
        }, operatorVersion=TEMPORAL_GENOME_OPERATOR_VERSION, applicationSha256=identity)

    def _validate_compilable(self, genome: EvolvableModuleGenomeV1) -> None:
        genome.validate()
        compiled = self.compiler.compile(
            genome,
            candidate_id="evolvable-temporal-operator",
            native_validator=self.native_validator,
        )
        if self.native_validator is not None:
            native = compiled.get("nativeValidation")
            if not isinstance(native, Mapping) or native.get("candidateAcceptable") is not True:
                raise GenomeTemporalOperatorError("native validator did not accept temporal genome mutation")

    def _preview_with_trace(self, genome: EvolvableModuleGenomeV1, plan: Mapping[str, Any]) -> tuple[EvolvableModuleGenomeV1, list[dict[str, Any]]]:
        value = _clone(plan)
        if value.get("parentGenomeSha256") != genome.identity_sha256 or value.get("operatorSpecSha256") != self.specification["operatorSpecSha256"]:
            raise GenomeTemporalOperatorError("temporal mutation plan parent or specification drift")
        current = {item["planSha256"]: item for item in self.enumerate_plans(genome)}
        if current.get(value.get("planSha256")) != value:
            raise GenomeTemporalOperatorError("temporal mutation plan is not canonical and applicable")
        child, trace = self._transform(genome, value["construction"])
        self._validate_compilable(child)
        return child, trace

    def _static_report(self, parent: EvolvableModuleGenomeV1, child: EvolvableModuleGenomeV1, plan: Mapping[str, Any] | None) -> dict[str, Any]:
        try:
            self._validate_compilable(parent)
            self._validate_compilable(child)
            checks = {
                "parent_valid_and_compilable": True,
                "child_valid_and_compilable": True,
                "resources_unchanged": parent.resources.canonical() == child.resources.canonical(),
                "budget_unchanged": parent.budget.canonical() == child.budget.canonical(),
                "node_and_edge_ownership_unchanged": (
                    [(item.node_id, item.zone.value, item.kind) for item in parent.nodes]
                    == [(item.node_id, item.zone.value, item.kind) for item in child.nodes]
                    and [(item.edge_id, item.source_id, item.target_id, item.effect.value if item.effect else None) for item in parent.edges]
                    == [(item.edge_id, item.source_id, item.target_id, item.effect.value if item.effect else None) for item in child.edges]
                ),
                "plan_binds_parent": plan is not None and plan.get("parentGenomeSha256") == parent.identity_sha256,
                "plan_binds_specification": plan is not None and plan.get("operatorSpecSha256") == self.specification["operatorSpecSha256"],
            }
        except (EvolvableGenomeError, GenomeTemporalOperatorError):
            checks = {"parent_valid_and_compilable": False, "child_valid_and_compilable": False}
        return finalize_audit(checks, operatorVersion=TEMPORAL_GENOME_OPERATOR_VERSION, planSha256=(plan or {}).get("planSha256"), childGenomeSha256=child.identity_sha256)

    def _constructions(self, genome: EvolvableModuleGenomeV1) -> Iterable[dict[str, Any]]:
        for site, guard in self._sites(genome):
            yield from self._site_constructions(site, guard)
        # Cooldowns are only meaningful for a repeatable management dispatch.
        for edge in sorted(genome.edges, key=lambda item: item.edge_id):
            if edge.effect is None or edge.effect.value not in REPEATABLE_MANAGEMENT_ACTION_KINDS:
                continue
            for evaluations in COOLDOWN_COUNTS:
                yield self._replacement_construction(
                    {"ownerKind": "edge", "ownerId": edge.edge_id, "guardPath": []},
                    edge.guard,
                    _all_with(edge.guard, {
                        "kind": "action_cooldown_elapsed",
                        "transitionId": f"e_{edge.edge_id}",
                        "actionOrdinal": 0,
                        "evaluations": evaluations,
                    }),
                    family="action_cooldown", parameters={"evaluations": evaluations},
                )

    def _sites(self, genome: EvolvableModuleGenomeV1) -> Iterable[tuple[dict[str, Any], dict[str, Any]]]:
        owners: list[tuple[str, str, Mapping[str, Any]]] = [
            *( ("node", node.node_id, node.guard) for node in sorted(genome.nodes, key=lambda item: item.node_id) ),
            *( ("edge", edge.edge_id, edge.guard) for edge in sorted(genome.edges, key=lambda item: item.edge_id) ),
        ]
        for owner_kind, owner_id, root in owners:
            if not root:
                continue
            for path, guard in _walk(root):
                yield {"ownerKind": owner_kind, "ownerId": owner_id, "guardPath": list(path)}, guard

    def _site_constructions(self, site: Mapping[str, Any], guard: Mapping[str, Any]) -> Iterable[dict[str, Any]]:
        kind = str(guard.get("kind") or "")
        # Direct scalar mutations preserve typed guard kind and resource names.
        if kind == "utc_time_window":
            current = (guard.get("startMinute"), guard.get("endMinute"))
            for start, end in _TIME_WINDOWS:
                if current != (start, end):
                    after = _clone(guard); after["startMinute"] = start; after["endMinute"] = end
                    yield self._replacement_construction(site, guard, after, family="utc_session_window", parameters={"startMinute": start, "endMinute": end})
        if kind in {"event_age_at_most", "state_age_at_least", "state_age_at_most", "condition_streak_at_least"}:
            domain = _EVENT_GRID[1:] if kind == "condition_streak_at_least" else _EVENT_GRID
            for events in domain:
                if guard.get("events") != events:
                    after = _clone(guard); after["events"] = events
                    yield self._replacement_construction(site, guard, after, family="state_or_condition_age", parameters={"events": events})
        if kind in {"position_age_at_least", "position_age_at_most"}:
            for events in _POS_AGE_GRID:
                if guard.get("events") != events:
                    after = _clone(guard); after["events"] = events
                    yield self._replacement_construction(site, guard, after, family="position_age", parameters={"events": events})
        if kind == "fresh_event":
            event_id = guard.get("eventId")
            if isinstance(event_id, str) and event_id:
                for minimum, maximum in EVENT_AGE_WINDOWS:
                    after = {"kind": "event_age_window", "eventId": event_id, "minimumEvents": minimum, "maximumEvents": maximum}
                    yield self._replacement_construction(site, guard, after, family="fresh_event_age_window", parameters={"minimumEvents": minimum, "maximumEvents": maximum})
                yield self._replacement_construction(site, guard, {"kind": "not", "guard": _clone(guard)}, family="fresh_event_absence", parameters={})
        # These two wrappers are the established operator-expansion spellings.
        if kind in MUTABLE_LEVEL_PREDICATE_KINDS:
            occurrence = canonical_sha256({"site": site, "guard": _clone(guard)})
            for direction in ("falling", "rising"):
                after = {"kind": "predicate_edge", "operatorId": EDGE_TRIGGER_PREDICATE, "operatorVersion": "1", "occurrenceSha256": occurrence, "direction": direction, "predicate": _clone(guard)}
                yield self._replacement_construction(site, guard, after, family="predicate_edge", parameters={"direction": direction})
            for evaluations in CONSECUTIVE_COUNTS:
                after = {"kind": "consecutive_true", "operatorId": REQUIRE_CONSECUTIVE_TRUE, "operatorVersion": "1", "occurrenceSha256": occurrence, "predicate": _clone(guard), "evaluations": evaluations}
                yield self._replacement_construction(site, guard, after, family="consecutive_true", parameters={"evaluations": evaluations})

    def _replacement_construction(self, site: Mapping[str, Any], before: Mapping[str, Any], after: Mapping[str, Any], *, family: str, parameters: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "kind": "typed_guard_replace",
            "family": family,
            "site": _clone(site),
            "beforeGuard": _clone(before),
            "afterGuard": _clone(after),
            "parameters": _clone(parameters),
        }

    def _transform(self, genome: EvolvableModuleGenomeV1, construction: Mapping[str, Any]) -> tuple[EvolvableModuleGenomeV1, list[dict[str, Any]]]:
        value = _clone(construction)
        if value.get("kind") != "typed_guard_replace" or not isinstance(value.get("site"), Mapping):
            raise GenomeTemporalOperatorError("unsupported temporal construction")
        site = value["site"]
        owner_kind, owner_id = str(site.get("ownerKind") or ""), str(site.get("ownerId") or "")
        path = site.get("guardPath")
        if owner_kind not in {"node", "edge"} or not isinstance(path, list) or not all(isinstance(item, (str, int)) and not isinstance(item, bool) for item in path):
            raise GenomeTemporalOperatorError("invalid typed temporal guard site")
        if not isinstance(value.get("beforeGuard"), Mapping) or not isinstance(value.get("afterGuard"), Mapping):
            raise GenomeTemporalOperatorError("temporal construction requires typed guard objects")
        if owner_kind == "node":
            owner = next((item for item in genome.nodes if item.node_id == owner_id), None)
            if owner is None or _guard_at(owner.guard, path) != value["beforeGuard"]:
                raise GenomeTemporalOperatorError("temporal node guard parent drift")
            changed = replace(owner, guard=_replace_at(owner.guard, path, value["afterGuard"]))
            child = replace(genome, nodes=tuple(changed if item.node_id == owner_id else item for item in genome.nodes))
        else:
            owner = next((item for item in genome.edges if item.edge_id == owner_id), None)
            if owner is None or _guard_at(owner.guard, path) != value["beforeGuard"]:
                raise GenomeTemporalOperatorError("temporal edge guard parent drift")
            changed = replace(owner, guard=_replace_at(owner.guard, path, value["afterGuard"]))
            child = replace(genome, edges=tuple(changed if item.edge_id == owner_id else item for item in genome.edges))
        trace = [{
            "family": value.get("family"), "ownerKind": owner_kind, "ownerId": owner_id,
            "guardPath": _clone(path), "beforeGuardSha256": canonical_sha256(value["beforeGuard"]),
            "afterGuardSha256": canonical_sha256(value["afterGuard"]), "parameters": _clone(value.get("parameters") or {}),
        }]
        return child, trace


__all__ = [
    "GenomeTemporalOperatorError", "GenomeTemporalOperatorLayer",
    "TEMPORAL_GENOME_OPERATOR_SCHEMA", "TEMPORAL_GENOME_OPERATOR_VERSION",
]
