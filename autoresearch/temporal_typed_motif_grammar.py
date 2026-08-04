"""Bounded, typed fragment grammar for reusable temporal v2 module genomes.

This module intentionally has no QD registry dependency.  It composes only
sealed production IDs, finite named choices, and catalog-resolved resources.
The result is a v2 module; economic candidates are always long/short v3 pairs
compiled by the Dashboard's canonical authority.
"""

from __future__ import annotations

from collections import Counter, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from enum import Enum
from itertools import product
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any, Protocol

from .temporal_discovery_base import TemporalDiscoveryContractError, _clone
from .temporal_search import canonical_sha256


GRAMMAR_SCHEMA = "temporal_typed_fragment_grammar_v2"
GRAMMAR_VERSION = "3"
MODULE_SCHEMA = "temporal_typed_fragment_module_v2"
WITNESS_SCHEMA = "temporal_typed_fragment_activation_recipe_v1"
DEFAULT_BUDGETS = {"states": 16, "transitions": 63, "groups": 4, "events": 8, "indicators": 16, "guardDepth": 4}
ENTRY_ROUTE_DECISION_INDICATOR_CAP = 3
ENTRY_ROUTE_DECISION_INDICATOR_POLICY_VERSION = "temporal_entry_route_decision_indicator_cap_v1"


class GrammarError(TemporalDiscoveryContractError):
    pass


class EntryRouteDecisionIndicatorCapError(GrammarError):
    """A candidate route conjunctively depends on more than the admitted cap."""

    pass


class Port(str, Enum):
    READY = "ready"
    WATCH = "watch"
    ENTRY_PENDING = "entry_pending"
    POSITION_IDLE = "position_idle"
    MANAGEMENT_PENDING = "management_pending"
    EXIT_PENDING = "exit_pending"
    RECOVERY = "recovery"


@dataclass(frozen=True)
class FragmentSpec:
    production_id: str
    family: str
    consumes: Port
    produces: Port
    resource_slots: tuple[str, ...]
    choice_domains: Mapping[str, tuple[Any, ...]]
    max_instances: int
    activation_recipe: Mapping[str, Any]


def _spec(
    production_id: str, family: str, consumes: Port, produces: Port, *,
    resources: tuple[str, ...] = (), choices: Mapping[str, tuple[Any, ...]] | None = None,
    maximum: int = 1, recipe: Mapping[str, Any] | None = None,
) -> FragmentSpec:
    return FragmentSpec(production_id, family, consumes, produces, resources, choices or {}, maximum, recipe or {})


# Definitions declare ports and semantics. GraphBuilder below only dispatches on
# family, never on archetype/seed names such as breakout or mean reversion.
REGISTRY: dict[str, FragmentSpec] = {
    "arm_level": _spec("arm_level", "arm", Port.READY, Port.WATCH, resources=("group",), choices={"threshold": (35.0, 50.0, 65.0, 75.0)}, recipe={"facts": ["position.absent", "evidence_at_least"], "outcome": "watch"}),
    "arm_fresh_event": _spec("arm_fresh_event", "arm", Port.READY, Port.WATCH, resources=("event",), recipe={"facts": ["position.absent", "fresh_event"], "outcome": "watch"}),
    "gate_level": _spec("gate_level", "gate", Port.WATCH, Port.WATCH, resources=("group",), choices={"threshold": (40.0, 55.0, 70.0, 85.0)}, maximum=4, recipe={"facts": ["evidence_at_least"], "outcome": "next_watch"}),
    "gate_below": _spec("gate_below", "gate", Port.WATCH, Port.WATCH, resources=("group",), choices={"threshold": (20.0, 35.0, 50.0, 65.0)}, maximum=4, recipe={"facts": ["evidence_below"], "outcome": "next_watch"}),
    "gate_fresh_event": _spec("gate_fresh_event", "gate", Port.WATCH, Port.WATCH, resources=("event",), maximum=4, recipe={"facts": ["fresh_event"], "outcome": "next_watch"}),
    "gate_event_window": _spec("gate_event_window", "gate", Port.WATCH, Port.WATCH, resources=("event",), choices={"age": (0, 1, 2, 3)}, maximum=4, recipe={"facts": ["event_age"], "outcome": "next_watch"}),
    "gate_delay": _spec("gate_delay", "gate", Port.WATCH, Port.WATCH, choices={"bars": (1, 2, 3, 5)}, maximum=4, recipe={"facts": ["state_age"], "outcome": "next_watch"}),
    "gate_streak": _spec("gate_streak", "gate", Port.WATCH, Port.WATCH, resources=("group",), choices={"threshold": (45.0, 60.0, 75.0), "bars": (2, 3, 5)}, maximum=4, recipe={"facts": ["condition_streak"], "outcome": "next_watch"}),
    "gate_predicate_edge": _spec("gate_predicate_edge", "gate", Port.WATCH, Port.WATCH, resources=("group",), choices={"threshold": (45.0, 60.0, 75.0)}, maximum=4, recipe={"facts": ["predicate_edge", "evidence_at_least"], "outcome": "next_watch"}),
    "enter_on_level": _spec("enter_on_level", "entry", Port.WATCH, Port.ENTRY_PENDING, resources=("group", "plan"), choices={"threshold": (45.0, 60.0, 75.0)}, recipe={"facts": ["position.absent", "evidence_at_least"], "outcome": "entry_intent"}),
    "enter_on_event": _spec("enter_on_event", "entry", Port.WATCH, Port.ENTRY_PENDING, resources=("event", "plan"), recipe={"facts": ["position.absent", "fresh_event"], "outcome": "entry_intent"}),
    "enter_on_level_and_event": _spec("enter_on_level_and_event", "entry", Port.WATCH, Port.ENTRY_PENDING, resources=("group", "event", "plan"), choices={"threshold": (45.0, 60.0, 75.0)}, recipe={"facts": ["position.absent", "evidence_at_least", "fresh_event"], "outcome": "entry_intent"}),
    "move_break_even": _spec("move_break_even", "management", Port.POSITION_IDLE, Port.MANAGEMENT_PENDING, choices={"r": (0.5, 1.0, 1.5)}, maximum=4, recipe={"facts": ["position.present", "unrealized_r"], "outcome": "management_intent"}),
    "tighten_stop": _spec("tighten_stop", "management", Port.POSITION_IDLE, Port.MANAGEMENT_PENDING, choices={"r": (0.5, 1.0, 1.5), "multiple": (-0.5, 0.0, 0.5)}, maximum=4, recipe={"facts": ["position.present", "unrealized_r"], "outcome": "management_intent"}),
    "set_target": _spec("set_target", "management", Port.POSITION_IDLE, Port.MANAGEMENT_PENDING, choices={"r": (0.5, 1.0, 1.5), "multiple": (1.0, 1.5, 2.0)}, maximum=4, recipe={"facts": ["position.present", "unrealized_r"], "outcome": "management_intent"}),
    "cancel_target": _spec("cancel_target", "management", Port.POSITION_IDLE, Port.MANAGEMENT_PENDING, choices={"r": (0.5, 1.0, 1.5)}, maximum=4, recipe={"facts": ["position.present", "unrealized_r"], "outcome": "management_intent"}),
    "activate_trailing": _spec("activate_trailing", "management", Port.POSITION_IDLE, Port.MANAGEMENT_PENDING, choices={"r": (0.5, 1.0, 1.5)}, maximum=4, recipe={"facts": ["position.present", "unrealized_r"], "outcome": "management_intent"}),
    "deactivate_trailing": _spec("deactivate_trailing", "management", Port.POSITION_IDLE, Port.MANAGEMENT_PENDING, choices={"bars": (2, 3, 5)}, maximum=4, recipe={"facts": ["position.present", "position_age"], "outcome": "management_intent"}),
    "exit_on_age": _spec("exit_on_age", "exit", Port.POSITION_IDLE, Port.EXIT_PENDING, choices={"bars": (5, 8, 13, 21)}, maximum=4, recipe={"facts": ["position.present", "position_age"], "outcome": "exit_intent"}),
    "exit_on_loss": _spec("exit_on_loss", "exit", Port.POSITION_IDLE, Port.EXIT_PENDING, choices={"r": (-1.5, -1.0, -0.5)}, maximum=4, recipe={"facts": ["position.present", "unrealized_r"], "outcome": "exit_intent"}),
    "exit_on_profit": _spec("exit_on_profit", "exit", Port.POSITION_IDLE, Port.EXIT_PENDING, choices={"r": (1.0, 1.5, 2.0)}, maximum=4, recipe={"facts": ["position.present", "unrealized_r"], "outcome": "exit_intent"}),
    "exit_on_signal": _spec("exit_on_signal", "exit", Port.POSITION_IDLE, Port.EXIT_PENDING, resources=("event",), maximum=4, recipe={"facts": ["position.present", "fresh_event"], "outcome": "exit_intent"}),
    "cooldown": _spec("cooldown", "recovery", Port.RECOVERY, Port.RECOVERY, choices={"bars": (1, 2, 3, 5)}, maximum=4, recipe={"facts": ["state_age"], "outcome": "recovery"}),
}


@dataclass(frozen=True)
class Fragment:
    uid: str
    production_id: str
    resources: Mapping[str, str]
    choices: Mapping[str, Any]

    def canonical(self) -> dict[str, Any]:
        # uid intentionally excluded from program identity.
        return {"productionId": self.production_id, "resources": dict(sorted(self.resources.items())), "choices": dict(sorted(self.choices.items()))}


@dataclass(frozen=True)
class ModuleProgram:
    direction: str
    fragments: tuple[Fragment, ...]
    lineage: tuple[Mapping[str, Any], ...] = ()

    def canonical(self) -> dict[str, Any]:
        return {"schemaVersion": GRAMMAR_SCHEMA, "grammarVersion": GRAMMAR_VERSION, "direction": self.direction, "fragments": [item.canonical() for item in self.fragments]}


@dataclass(frozen=True)
class GrammarContext:
    instrument: str
    indicators: tuple[Mapping[str, Any], ...]
    evidence_groups: tuple[Mapping[str, Any], ...]
    event_bindings: tuple[Mapping[str, Any], ...]
    execution_config: Mapping[str, Any]
    budgets: Mapping[str, int] | None = None

    def normalized(self) -> dict[str, Any]:
        indicators = [_clone(dict(item), name="indicator") for item in self.indicators]
        groups = [_clone(dict(item), name="evidence group") for item in self.evidence_groups]
        events = [_clone(dict(item), name="event binding") for item in self.event_bindings]
        execution = _clone(dict(self.execution_config), name="execution config")
        instrument = str(self.instrument).strip().upper()
        if not instrument:
            raise GrammarError("fragment context requires one instrument")
        indicator_ids = [str((item.get("meta") or {}).get("instanceId") or "") for item in indicators]
        group_ids = [str(item.get("id") or "") for item in groups]
        event_ids = [str(item.get("id") or "") for item in events]
        plans = ((execution.get("managementLibrary") or {}).get("plans") or [])
        plan_ids = [str(item.get("id") or "") for item in plans if isinstance(item, Mapping)]
        for label, values in (("indicator", indicator_ids), ("group", group_ids), ("event", event_ids), ("plan", plan_ids)):
            if not values or "" in values or len(values) != len(set(values)):
                raise GrammarError(f"fragment context {label} identities are missing or duplicate")
        if not all(set(item.get("indicatorInstanceIds") or []).issubset(set(indicator_ids)) for item in groups):
            raise GrammarError("evidence group has unknown indicator reference")
        if not all(str(item.get("indicatorInstanceId") or "") in indicator_ids for item in events):
            raise GrammarError("event binding has unknown indicator reference")
        bounds = dict(DEFAULT_BUDGETS)
        for key, value in (self.budgets or {}).items():
            if key not in bounds or isinstance(value, bool) or int(value) < 1:
                raise GrammarError("invalid fragment budget")
            bounds[key] = int(value)
        return {"instrument": instrument, "indicators": indicators, "groups": sorted(groups, key=lambda x: x["id"]), "events": sorted(events, key=lambda x: x["id"]), "executionConfig": execution, "plans": sorted(plan_ids), "budgets": bounds}


class NativeValidator(Protocol):
    def validate_v2(self, *, profile: Mapping[str, Any], candidate_id: str) -> Mapping[str, Any]: ...


class PairCompiler(Protocol):
    def compile_pair(self, *, long_profile: Mapping[str, Any], short_profile: Mapping[str, Any], candidate_id: str) -> Mapping[str, Any]: ...


class JsonlNativeAuthority(NativeValidator, PairCompiler, Protocol):
    """Protocol for a persistent Dashboard JSONL validation/compile transport."""
    def request_jsonl(self, request: Mapping[str, Any]) -> Mapping[str, Any]: ...


class DashboardNativeAuthority:
    """Compatibility adapter for the Dashboard authority environment.

    Production callers may instead implement ``NativeValidator`` over a
    persistent JSONL transport; this adapter deliberately makes no persistence
    claim and is retained for local proof runs.
    """
    def __init__(self, python_executable: Path | str) -> None:
        self.python_executable = Path(python_executable)
        if not self.python_executable.is_file():
            raise GrammarError("Dashboard native validator executable is unavailable")

    def _run(self, code: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix="typed-fragment-") as root:
            path = Path(root) / "payload.json"
            path.write_text(json.dumps(dict(payload), sort_keys=True), encoding="utf-8")
            result = subprocess.run([str(self.python_executable), "-c", code, str(path)], text=True, capture_output=True, check=False)
            if result.returncode != 0:
                raise GrammarError(f"Dashboard native authority failed: {result.stderr.strip()[:500]}")
            try:
                value = json.loads(result.stdout)
            except json.JSONDecodeError as exc:
                raise GrammarError("Dashboard native authority returned non-JSON") from exc
            if not isinstance(value, dict):
                raise GrammarError("Dashboard native authority returned non-object")
            return value

    def validate_v2(self, *, profile: Mapping[str, Any], candidate_id: str) -> Mapping[str, Any]:
        return self._run("""import json,sys
from fuzzfolio_core.temporal_graph.search_validation import validate_temporal_search_candidate
p=json.load(open(sys.argv[1],encoding='utf-8'))
print(json.dumps(validate_temporal_search_candidate(p,candidate_id=p.pop('_candidateId'))))
""", {**dict(profile), "_candidateId": candidate_id})

    def validate_many(self, *, items: Sequence[Mapping[str, Any]]) -> Sequence[Mapping[str, Any]]:
        result = self._run("""import json,sys
from fuzzfolio_core.temporal_graph.search_validation import validate_temporal_search_candidate
p=json.load(open(sys.argv[1],encoding='utf-8'))
print(json.dumps({'reports':[validate_temporal_search_candidate(item['profile'],candidate_id=item['candidateId']) for item in p['items']]}))
""", {"items": [dict(item) for item in items]})
        reports = result.get("reports")
        if not isinstance(reports, list): raise GrammarError("Dashboard batch authority returned no reports")
        return reports

    def compile_pair(self, *, long_profile: Mapping[str, Any], short_profile: Mapping[str, Any], candidate_id: str) -> Mapping[str, Any]:
        return self._run("""import json,sys
from fuzzfolio_core.temporal_graph.graph_models import TemporalGraphProfile
from fuzzfolio_core.temporal_graph.bidirectional_compiler import compile_bidirectional_profile
from fuzzfolio_core.temporal_graph.search_validation import validate_temporal_search_candidate
p=json.load(open(sys.argv[1],encoding='utf-8'))
v=compile_bidirectional_profile(TemporalGraphProfile.model_validate(p['long']),TemporalGraphProfile.model_validate(p['short']))
raw=v.model_dump(mode='json',by_alias=True,exclude_none=False)
report=validate_temporal_search_candidate(raw,candidate_id=p['candidateId'])
print(json.dumps({'profile':raw,'validation':report}))
""", {"long": dict(long_profile), "short": dict(short_profile), "candidateId": candidate_id})


@dataclass(frozen=True)
class CompiledModule:
    profile: Mapping[str, Any]
    program: Mapping[str, Any]
    lineage: tuple[Mapping[str, Any], ...]
    identities: Mapping[str, str]
    activation_witnesses: tuple[Mapping[str, Any], ...]
    native_report: Mapping[str, Any]


def _guard_depth(value: Mapping[str, Any]) -> int:
    if value.get("kind") in {"all", "any"}:
        return 1 + max((_guard_depth(item) for item in value.get("guards") or []), default=0)
    if value.get("kind") in {"predicate_edge", "consecutive_true"}:
        return 1 + _guard_depth(value["predicate"])
    return 1


def _deduplicate_indicator_paths(
    paths: Sequence[frozenset[str]],
) -> tuple[frozenset[str], ...]:
    """Canonicalize finite conjunctive decision-indicator alternatives."""

    return tuple(
        sorted(set(paths), key=lambda item: (len(item), tuple(sorted(item))))
    )


def _guard_decision_indicator_paths(
    guard: Mapping[str, Any],
    *,
    groups: Mapping[str, frozenset[str]],
    events: Mapping[str, str],
    known_indicator_ids: frozenset[str],
    negated: bool = False,
) -> tuple[frozenset[str], ...]:
    """Return each feasible conjunctive indicator set for a guard.

    ``all`` combines requirements; ``any`` preserves alternatives rather than
    accidentally treating every branch as simultaneously required.  This
    representation remains exact when a later ``all`` combines an ``any``
    branch with another predicate.
    """

    kind = str(guard.get("kind") or "")
    if kind == "not":
        nested = guard.get("guard")
        if not isinstance(nested, Mapping):
            raise GrammarError("not decision guard is not closed")
        return _guard_decision_indicator_paths(
            nested,
            groups=groups,
            events=events,
            known_indicator_ids=known_indicator_ids,
            negated=not negated,
        )
    if kind in {"all", "any"}:
        children = guard.get("guards")
        if not isinstance(children, list) or not all(
            isinstance(item, Mapping) for item in children
        ):
            raise GrammarError("compound decision guard is not closed")
        child_paths = [
            _guard_decision_indicator_paths(
                item,
                groups=groups,
                events=events,
                known_indicator_ids=known_indicator_ids,
                negated=negated,
            )
            for item in children
        ]
        effective_kind = kind if not negated else ("all" if kind == "any" else "any")
        if effective_kind == "any":
            return _deduplicate_indicator_paths(
                [path for paths in child_paths for path in paths]
            ) or (frozenset(),)
        paths: tuple[frozenset[str], ...] = (frozenset(),)
        for alternatives in child_paths:
            paths = _deduplicate_indicator_paths(
                [left | right for left in paths for right in alternatives]
            )
        return paths
    if kind in {"predicate_edge", "consecutive_true"}:
        nested = guard.get("predicate")
        if not isinstance(nested, Mapping):
            raise GrammarError("predicate-edge decision guard is not closed")
        return _guard_decision_indicator_paths(
            nested,
            groups=groups,
            events=events,
            known_indicator_ids=known_indicator_ids,
            negated=negated,
        )

    identifiers: set[str] = set()
    if "groupId" in guard:
        group_id = str(guard.get("groupId") or "")
        if group_id not in groups:
            raise GrammarError("decision guard references an unknown evidence group")
        identifiers.update(groups[group_id])
    if "eventId" in guard:
        event_id = str(guard.get("eventId") or "")
        if event_id not in events:
            raise GrammarError("decision guard references an unknown event binding")
        identifiers.add(events[event_id])
    if not identifiers:
        return (frozenset(),)
    if not identifiers.issubset(known_indicator_ids):
        raise GrammarError("decision guard indicator closure is incomplete")
    return (frozenset(identifiers),)


def entry_route_decision_indicator_report(profile: Mapping[str, Any]) -> dict[str, Any]:
    """Describe the per-entry-route decision-indicator cardinality.

    Only paths composed of decision transitions before an ``enter_next_open``
    action participate.  Position, economic, runtime, and management guards
    either lie outside that pre-entry route or carry no group/event binding and
    therefore do not consume this search-language budget.
    """

    graph = profile.get("graph")
    if not isinstance(graph, Mapping):
        raise GrammarError("entry route indicator cap requires a graph")
    transitions_raw = graph.get("transitions", [])
    if not isinstance(transitions_raw, list):
        raise GrammarError("entry route indicator cap transition set is malformed")
    transitions = [item for item in transitions_raw if isinstance(item, Mapping)]
    if len(transitions) != len(transitions_raw):
        raise GrammarError("entry route indicator cap transition is malformed")
    entry_transitions = [
        item
        for item in transitions
        if item.get("eventClass") == "decision"
        and any(
            isinstance(action, Mapping) and action.get("kind") == "enter_next_open"
            for action in (item.get("actions") or [])
        )
    ]
    if not entry_transitions:
        return {
            "schemaVersion": "temporal_entry_route_decision_indicator_report_v1",
            "policyVersion": ENTRY_ROUTE_DECISION_INDICATOR_POLICY_VERSION,
            "maxDistinctDecisionIndicatorInstances": ENTRY_ROUTE_DECISION_INDICATOR_CAP,
            "entryTransitions": [],
            "reachableStateIndicatorSetCount": 0,
            "observedMaximumDistinctDecisionIndicatorInstances": 0,
        }
    indicators = profile.get("indicators")
    groups_raw = graph.get("evidenceGroups")
    events_raw = graph.get("eventBindings")
    if not all(isinstance(value, list) for value in (indicators, groups_raw, events_raw)):
        raise GrammarError("entry route indicator cap requires closed graph resources")
    known_indicator_ids = frozenset(
        str((item.get("meta") or {}).get("instanceId") or "")
        for item in indicators
        if isinstance(item, Mapping)
    )
    if "" in known_indicator_ids or len(known_indicator_ids) != len(indicators):
        raise GrammarError("entry route indicator cap requires unique indicator instances")
    if len(known_indicator_ids) > DEFAULT_BUDGETS["indicators"]:
        raise GrammarError("entry route indicator cap exceeds the per-side resource budget")
    groups: dict[str, frozenset[str]] = {}
    for item in groups_raw:
        if not isinstance(item, Mapping):
            raise GrammarError("entry route indicator cap evidence group is malformed")
        group_id = str(item.get("id") or "")
        members = item.get("indicatorInstanceIds")
        if not group_id or group_id in groups or not isinstance(members, list) or not members:
            raise GrammarError("entry route indicator cap evidence group is malformed")
        groups[group_id] = frozenset(str(member) for member in members)
    events: dict[str, str] = {}
    for item in events_raw:
        if not isinstance(item, Mapping):
            raise GrammarError("entry route indicator cap event binding is malformed")
        event_id = str(item.get("id") or "")
        member = str(item.get("indicatorInstanceId") or "")
        if not event_id or event_id in events or not member:
            raise GrammarError("entry route indicator cap event binding is malformed")
        events[event_id] = member

    initial_state = str(graph.get("initialStateId") or "")
    if not initial_state:
        raise GrammarError("entry route indicator cap requires an initial state")
    entry_indexes = {
        index
        for index, item in enumerate(transitions)
        if item in entry_transitions
    }
    for entry in entry_transitions:
        source = str(entry.get("sourceStateId") or "")
        guard = entry.get("guard")
        if not source or not isinstance(guard, Mapping):
            raise GrammarError("entry decision route is not closed")

    # Restrict propagation to decision states that can still reach an entry.
    # This keeps unrelated position/exit branches outside the entry language.
    reverse_decision_edges: dict[str, set[str]] = {}
    for index, edge in enumerate(transitions):
        if index in entry_indexes or edge.get("eventClass") != "decision":
            continue
        source = str(edge.get("sourceStateId") or "")
        destination = str(edge.get("destinationStateId") or "")
        if source and destination:
            reverse_decision_edges.setdefault(destination, set()).add(source)
    entry_sources = {
        str(edge.get("sourceStateId") or "") for edge in entry_transitions
    }
    relevant_states = set(entry_sources)
    pending_states = deque(sorted(entry_sources))
    while pending_states:
        state = pending_states.popleft()
        for predecessor in sorted(reverse_decision_edges.get(state, ())):
            if predecessor not in relevant_states:
                relevant_states.add(predecessor)
                pending_states.append(predecessor)

    outgoing: dict[str, list[tuple[int, Mapping[str, Any]]]] = {}
    for index, edge in enumerate(transitions):
        if edge.get("eventClass") != "decision":
            continue
        source = str(edge.get("sourceStateId") or "")
        if source:
            outgoing.setdefault(source, []).append((index, edge))
    for edges in outgoing.values():
        edges.sort(key=lambda item: item[0])

    # The worklist has a finite fixed point: each side has at most sixteen
    # resource-closed indicators and only sets of up to the cap are retained.
    # It therefore avoids materializing exponentially many simple graph paths.
    worklist = deque([(initial_state, frozenset())])
    seen_state_sets = {(initial_state, frozenset())}
    entry_sets: dict[int, set[frozenset[str]]] = {
        index: set() for index in entry_indexes
    }
    while worklist:
        state, current = worklist.popleft()
        for index, edge in outgoing.get(state, ()):
            is_entry = index in entry_indexes
            destination = str(edge.get("destinationStateId") or "")
            if not is_entry and (not destination or destination not in relevant_states):
                continue
            edge_guard = edge.get("guard")
            if not isinstance(edge_guard, Mapping):
                raise GrammarError("entry decision route guard is not closed")
            for requirement in _guard_decision_indicator_paths(
                edge_guard,
                groups=groups,
                events=events,
                known_indicator_ids=known_indicator_ids,
            ):
                combined = current | requirement
                if len(combined) > ENTRY_ROUTE_DECISION_INDICATOR_CAP:
                    raise EntryRouteDecisionIndicatorCapError(
                        "entry decision route exceeds the distinct decision-indicator cap"
                    )
                if is_entry:
                    entry_sets[index].add(combined)
                    continue
                state_set = (destination, combined)
                if state_set not in seen_state_sets:
                    seen_state_sets.add(state_set)
                    worklist.append(state_set)

    entry_reports = []
    for index, entry in enumerate(transitions):
        if index not in entry_indexes:
            continue
        route_sets = entry_sets[index]
        entry_reports.append(
            {
                "transitionId": str(entry.get("id") or ""),
                "routeCount": len(route_sets),
                "routeDistinctDecisionIndicatorCounts": sorted(
                    {len(item) for item in route_sets}
                ),
                "maxDistinctDecisionIndicatorInstances": max(
                    (len(item) for item in route_sets), default=0
                ),
            }
        )
    report = {
        "schemaVersion": "temporal_entry_route_decision_indicator_report_v1",
        "policyVersion": ENTRY_ROUTE_DECISION_INDICATOR_POLICY_VERSION,
        "maxDistinctDecisionIndicatorInstances": ENTRY_ROUTE_DECISION_INDICATOR_CAP,
        "entryTransitions": entry_reports,
        "reachableStateIndicatorSetCount": len(seen_state_sets),
    }
    report["observedMaximumDistinctDecisionIndicatorInstances"] = max(
        (
            int(item["maxDistinctDecisionIndicatorInstances"])
            for item in entry_reports
        ),
        default=0,
    )
    return report


def validate_entry_route_decision_indicator_cap(profile: Mapping[str, Any]) -> dict[str, Any]:
    report = entry_route_decision_indicator_report(profile)
    if (
        report["observedMaximumDistinctDecisionIndicatorInstances"]
        > ENTRY_ROUTE_DECISION_INDICATOR_CAP
    ):
        raise EntryRouteDecisionIndicatorCapError(
            "entry decision route exceeds the distinct decision-indicator cap"
        )
    return report


def _binding_ids(value: Any) -> set[str]:
    if isinstance(value, Mapping):
        found = {str(value["bindingId"])} if value.get("bindingId") else set()
        for child in value.values(): found.update(_binding_ids(child))
        return found
    if isinstance(value, list):
        found: set[str] = set()
        for child in value: found.update(_binding_ids(child))
        return found
    return set()


def _guard_shape(guard: Mapping[str, Any]) -> Any:
    kind = str(guard.get("kind") or "")
    if kind in {"all", "any"}:
        children = [_guard_shape(item) for item in guard.get("guards") or [] if isinstance(item, Mapping)]
        return (kind, tuple(sorted(children, key=repr)))
    if kind in {"predicate_edge", "consecutive_true"}: return (kind, _guard_shape(guard.get("predicate") or {}))
    return kind


def compiled_graph_signature(profile: Mapping[str, Any]) -> str:
    """Structure only: excludes direction, identifiers, resource bindings, numbers."""
    graph = profile.get("graph") if isinstance(profile, Mapping) else {}
    transitions = graph.get("transitions") or []
    state_ids = [str(item.get("id") or "") for item in graph.get("states") or [] if isinstance(item, Mapping)]
    degrees = Counter()
    edges = []
    for item in transitions:
        if not isinstance(item, Mapping): continue
        degrees[str(item.get("sourceStateId") or "")] += 1
        edges.append((str(item.get("eventClass") or ""), _guard_shape(item.get("guard") or {}), tuple(sorted(str(action.get("kind") or "") for action in item.get("actions") or [] if isinstance(action, Mapping)))))
    return canonical_sha256({"stateCount": len(state_ids), "outDegreeHistogram": sorted(Counter(degrees.values()).items()), "edges": sorted(edges, key=repr)})


def _resource_closed_context(context: Mapping[str, Any], fragments: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    groups = {str(value) for fragment in fragments for slot, value in fragment["resources"].items() if slot == "group"}
    events = {str(value) for fragment in fragments for slot, value in fragment["resources"].items() if slot == "event"}
    plans = {str(value) for fragment in fragments for slot, value in fragment["resources"].items() if slot == "plan"}
    selected_groups = [item for item in context["groups"] if item["id"] in groups]
    selected_events = [item for item in context["events"] if item["id"] in events]
    indicator_ids = {identifier for item in selected_groups for identifier in item["indicatorInstanceIds"]}
    indicator_ids.update(item["indicatorInstanceId"] for item in selected_events)
    library = context["executionConfig"]["managementLibrary"]
    selected_plans = [item for item in library["plans"] if item["id"] in plans]
    if len(selected_plans) != len(plans): raise GrammarError("selected plan closure is incomplete")
    bindings = list(library.get("scalarBindings") or [])
    binding_ids = set().union(*(_binding_ids(item) for item in selected_plans)) if selected_plans else set()
    selected_bindings = [item for item in bindings if item.get("id") in binding_ids]
    if {item.get("id") for item in selected_bindings} != binding_ids: raise GrammarError("selected scalar binding closure is incomplete")
    indicator_ids.update(item["indicatorInstanceId"] for item in selected_bindings)
    selected_indicators = [item for item in context["indicators"] if item.get("meta", {}).get("instanceId") in indicator_ids]
    if len(selected_indicators) != len(indicator_ids): raise GrammarError("indicator closure is incomplete")
    execution = {key: _clone(value, name="execution field") for key, value in context["executionConfig"].items() if key != "managementLibrary"}
    execution["managementLibrary"] = {"version": library.get("version", "temporal_management_v1"), "defaultPlanId": sorted(plans)[0], "plans": selected_plans}
    if selected_bindings: execution["managementLibrary"]["scalarBindings"] = selected_bindings
    return {"indicators": selected_indicators, "groups": selected_groups, "events": selected_events, "executionConfig": execution}


class GraphBuilder:
    """Generic lifecycle builder. It has no seed/archetype-specific branches."""
    def __init__(self, context: Mapping[str, Any], program: Mapping[str, Any]) -> None:
        self.context, self.program = context, program
        self.states = [{"id": "ready"}, {"id": "entry_pending"}, {"id": "position_idle"}, {"id": "exit_pending"}]
        self.transitions: list[dict[str, Any]] = []
        self.current_watch = "ready"
        # A management fragment is a one-shot authored lifecycle stage.  Its
        # terminal execution routes advance this cursor, so a guard that stays
        # true cannot continually pre-empt a later management/exit fragment.
        self.position_state = "position_idle"
        self.position_states = ["position_idle"]
        self.expiring_watches: set[str] = set()
        self.recovery_ordinals = [index for index, item in enumerate(program["fragments"]) if REGISTRY[item["productionId"]].family == "recovery"]
        self.recovery_ids = [f"recovery_{index}" for index in self.recovery_ordinals]

    def transition(self, ident: str, source: str, destination: str, event_class: str, guard: Mapping[str, Any], actions: Sequence[Mapping[str, Any]], reason: str, priority: int = 10) -> None:
        self.transitions.append({"id": ident, "sourceStateId": source, "destinationStateId": destination, "eventClass": event_class, "priority": priority, "guard": _clone(dict(guard), name="fragment guard"), "actions": [_clone(dict(item), name="fragment action") for item in actions], "reasonCode": reason})

    @staticmethod
    def _all(*guards: Mapping[str, Any]) -> dict[str, Any]:
        return {"kind": "all", "guards": [dict(item) for item in guards]}

    def _predicate(self, fragment: Mapping[str, Any]) -> dict[str, Any]:
        production, resources, choices = fragment["productionId"], fragment["resources"], fragment["choices"]
        if production.endswith("level") or production == "gate_level": return {"kind": "evidence_at_least", "groupId": resources["group"], "thresholdPercent": choices["threshold"]}
        if production == "gate_below": return {"kind": "evidence_below", "groupId": resources["group"], "thresholdPercent": choices["threshold"]}
        if production.endswith("fresh_event") or production == "enter_on_event": return {"kind": "fresh_event", "eventId": resources["event"]}
        if production == "gate_event_window": return {"kind": "event_age_at_most", "eventId": resources["event"], "events": choices["age"]}
        if production == "gate_delay": return {"kind": "state_age_at_least", "events": choices["bars"]}
        if production == "gate_streak": return {"kind": "condition_streak_at_least", "groupId": resources["group"], "comparison": "at_least", "thresholdPercent": choices["threshold"], "events": choices["bars"]}
        if production == "gate_predicate_edge": return {"kind": "predicate_edge", "occurrenceSha256": canonical_sha256({"productionId": production, "resources": resources, "choices": choices}), "direction": "rising", "predicate": {"kind": "evidence_at_least", "groupId": resources["group"], "thresholdPercent": choices["threshold"]}}
        if production == "enter_on_level_and_event": return self._all({"kind": "evidence_at_least", "groupId": resources["group"], "thresholdPercent": choices["threshold"]}, {"kind": "fresh_event", "eventId": resources["event"]})
        raise GrammarError("fragment predicate production is unimplemented")

    def attach(self, ordinal: int, fragment: Mapping[str, Any]) -> None:
        spec = REGISTRY[fragment["productionId"]]
        tag = f"f{ordinal}_{spec.production_id}"
        if spec.family == "arm":
            target = f"watch_{ordinal}"; self.states.append({"id": target})
            self.transition(f"{tag}_arm", "ready", target, "decision", self._all({"kind": "position_exists", "expected": False}, self._predicate(fragment)), [], f"{spec.production_id}.armed")
            self.transition(f"{tag}_expire", target, "ready", "decision", {"kind": "state_age_at_least", "events": 8}, [], f"{spec.production_id}.expired", 90)
            self.expiring_watches.add(target)
            self.current_watch = target
        elif spec.family == "gate":
            target = f"watch_{ordinal}"; self.states.append({"id": target})
            self.transition(f"{tag}_gate", self.current_watch, target, "decision", self._predicate(fragment), [], f"{spec.production_id}.passed")
            if self.current_watch not in self.expiring_watches:
                self.transition(f"{tag}_abort", self.current_watch, "ready", "decision", {"kind": "state_age_at_least", "events": 8}, [], f"{spec.production_id}.aborted", 90)
                self.expiring_watches.add(self.current_watch)
            self.current_watch = target
        elif spec.family == "entry":
            self.transition(f"{tag}_entry", self.current_watch, "entry_pending", "decision", self._all({"kind": "position_exists", "expected": False}, self._predicate(fragment)), [{"kind": "enter_next_open", "managementPlanId": fragment["resources"]["plan"]}], f"{spec.production_id}.entered")
            if self.current_watch not in self.expiring_watches:
                self.transition(f"{tag}_expire", self.current_watch, "ready", "decision", {"kind": "state_age_at_least", "events": 8}, [], f"{spec.production_id}.expired", 90)
                self.expiring_watches.add(self.current_watch)
        elif spec.family == "management":
            state = f"management_{ordinal}"; after = f"position_after_management_{ordinal}"
            self.states.extend(({"id": state}, {"id": after}))
            r = fragment["choices"].get("r")
            base = {"kind": "position_age_at_least", "events": fragment["choices"].get("bars", 1)} if r is None else {"kind": "unrealized_r_at_least", "value": r}
            if spec.production_id == "move_break_even": action = {"kind": "move_stop_to_break_even_next_open"}
            elif spec.production_id == "tighten_stop": action = {"kind": "tighten_stop_next_open", "stopLocator": {"kind": "initial_r_multiple", "multiple": fragment["choices"]["multiple"]}}
            elif spec.production_id == "set_target": action = {"kind": "set_target_next_open", "targetLocator": {"kind": "reward_multiple", "multiple": fragment["choices"]["multiple"]}}
            elif spec.production_id == "cancel_target": action = {"kind": "cancel_target_next_open"}
            elif spec.production_id == "activate_trailing": action = {"kind": "activate_trailing_stop_next_open"}
            else: action = {"kind": "deactivate_trailing_stop_next_open"}
            self.transition(f"{tag}_request", self.position_state, state, "decision", self._all({"kind": "position_exists", "expected": True}, base), [action], f"{spec.production_id}.requested", 10 + ordinal)
            for status, priority in (("applied", 10), ("rejected", 20), ("canceled", 25)):
                self.transition(f"{tag}_{status}", state, after, "execution", {"kind": "execution_status_is", "status": status}, [], f"{spec.production_id}.{status}", priority)
            self.transition(f"{tag}_closed", state, self.recovery_entry(), "execution", {"kind": "execution_status_is", "status": "closed"}, [], f"{spec.production_id}.closed", 30)
            self.position_state = after; self.position_states.append(after)
        elif spec.family == "exit":
            choices = fragment["choices"]
            if spec.production_id == "exit_on_age": base = {"kind": "position_age_at_least", "events": choices["bars"]}
            elif spec.production_id == "exit_on_loss": base = {"kind": "unrealized_r_at_most", "value": choices["r"]}
            elif spec.production_id == "exit_on_profit": base = {"kind": "unrealized_r_at_least", "value": choices["r"]}
            else: base = {"kind": "fresh_event", "eventId": fragment["resources"]["event"]}
            self.transition(f"{tag}_exit", self.position_state, "exit_pending", "decision", self._all({"kind": "position_exists", "expected": True}, base), [{"kind": "exit_next_open"}], f"{spec.production_id}.requested", 10 + ordinal)
        elif spec.family == "recovery":
            target = f"recovery_{ordinal}"; self.states.append({"id": target})
            recovery_index = self.recovery_ids.index(target)
            destination = self.recovery_ids[recovery_index + 1] if recovery_index + 1 < len(self.recovery_ids) else "ready"
            self.transition(f"{tag}_cooldown", target, destination, "decision", {"kind": "state_age_at_least", "events": fragment["choices"]["bars"]}, [], "cooldown.elapsed")
        else:
            raise GrammarError("unknown sealed fragment family")

    def recovery_entry(self) -> str:
        return self.recovery_ids[0] if self.recovery_ids else "ready"

    def build(self) -> dict[str, Any]:
        ordered = self.program["fragments"]
        for index, fragment in enumerate(ordered): self.attach(index, fragment)
        recovery = self.recovery_entry()
        self.transition("entry_filled", "entry_pending", "position_idle", "execution", {"kind": "execution_status_is", "status": "filled"}, [], "entry.filled")
        self.transition("entry_rejected", "entry_pending", "ready", "execution", {"kind": "execution_status_is", "status": "rejected"}, [], "entry.rejected", 20)
        self.transition("entry_canceled", "entry_pending", "ready", "execution", {"kind": "execution_status_is", "status": "canceled"}, [], "entry.canceled", 30)
        for position_state in self.position_states:
            self.transition("protective_closed" if position_state == "position_idle" else "protective_closed_" + position_state, position_state, recovery, "execution", {"kind": "execution_status_is", "status": "closed"}, [], "position.protective_closed")
        self.transition("exit_closed", "exit_pending", recovery, "execution", {"kind": "execution_status_is", "status": "closed"}, [], "exit.closed")
        self.transition("exit_rejected", "exit_pending", self.position_state, "execution", {"kind": "execution_status_is", "status": "rejected"}, [], "exit.rejected", 20)
        self.transition("exit_canceled", "exit_pending", self.position_state, "execution", {"kind": "execution_status_is", "status": "canceled"}, [], "exit.canceled", 30)
        return {"states": self.states, "transitions": self.transitions}


class TypedFragmentGrammar:
    def __init__(self, context: GrammarContext, *, native_authority: NativeValidator) -> None:
        if native_authority is None: raise GrammarError("Dashboard native validator authority is mandatory")
        self.context, self.native_authority = context.normalized(), native_authority
        self.context_sha256 = canonical_sha256(self.context)

    def _fragment(self, production_id: str, *, uid: str, resources: Mapping[str, str] | None = None, choices: Mapping[str, Any] | None = None) -> Fragment:
        spec = REGISTRY.get(production_id)
        if spec is None: raise GrammarError("unsealed fragment production")
        return Fragment(uid, production_id, dict(resources or {}), dict(choices or {key: values[0] for key, values in spec.choice_domains.items()}))

    def seed(
        self,
        *,
        direction: str,
        name: str,
        group_id: str | None = None,
        event_id: str | None = None,
        plan_id: str | None = None,
    ) -> ModuleProgram:
        # Named legacy roots use the same registry, never a special graph path.
        recipes = {
            "mean_reversion": ("arm_level", "gate_event_window", "enter_on_level_and_event", "exit_on_age"),
            "breakout": ("arm_fresh_event", "gate_delay", "enter_on_level", "exit_on_profit"),
            "trend": ("arm_level", "gate_below", "enter_on_event", "exit_on_signal"),
        }
        if name not in recipes: raise GrammarError("unknown registry seed")
        group_ids = [str(item["id"]) for item in self.context["groups"]]
        event_ids = [str(item["id"]) for item in self.context["events"]]
        plan_ids = [str(item) for item in self.context["plans"]]
        groups = str(group_id) if group_id is not None else group_ids[0]
        events = str(event_id) if event_id is not None else event_ids[0]
        plan = str(plan_id) if plan_id is not None else plan_ids[0]
        if groups not in group_ids or events not in event_ids or plan not in plan_ids:
            raise GrammarError("registry seed resource binding is outside the frozen context")
        fragments = []
        for index, production in enumerate(recipes[name]):
            spec = REGISTRY[production]; resources = {slot: {"group": groups, "event": events, "plan": plan}[slot] for slot in spec.resource_slots}
            fragments.append(self._fragment(production, uid=f"seed_{index}", resources=resources))
        program = ModuleProgram(direction, tuple(fragments), ({"operation": "seed", "seed": name},))
        self.validate(program); return program

    def canonical_program(self, program: ModuleProgram) -> dict[str, Any]:
        self.validate(program); return program.canonical()

    def validate(self, program: ModuleProgram) -> None:
        if program.direction not in {"long", "short"}: raise GrammarError("module direction must be long or short")
        fragments = [item.canonical() for item in program.fragments]
        if not fragments: raise GrammarError("module has no fragments")
        counts = Counter(item["productionId"] for item in fragments)
        for production, count in counts.items():
            spec = REGISTRY.get(production)
            if spec is None or count > spec.max_instances: raise GrammarError("fragment production exceeds sealed budget")
        families = [REGISTRY[item["productionId"]].family for item in fragments]
        if families.count("arm") != 1 or families.count("entry") != 1 or not any(item == "exit" for item in families):
            raise GrammarError("module requires exactly one arm and entry plus at least one exit")
        order = {"arm": 0, "gate": 1, "entry": 2, "management": 3, "exit": 4, "recovery": 5}
        if [order[item] for item in families] != sorted(order[item] for item in families): raise GrammarError("fragment lifecycle order is incompatible")
        watch_port = Port.READY
        for item in fragments:
            spec = REGISTRY[item["productionId"]]
            if spec.family in {"arm", "gate", "entry"}:
                if spec.consumes != watch_port: raise GrammarError("fragment port consume is incompatible with entry lifecycle")
                watch_port = spec.produces
            elif spec.family in {"management", "exit"}:
                if spec.consumes != Port.POSITION_IDLE or spec.produces not in {Port.MANAGEMENT_PENDING, Port.EXIT_PENDING}:
                    raise GrammarError("fragment port consume is incompatible with position lifecycle")
            elif spec.family == "recovery" and (spec.consumes != Port.RECOVERY or spec.produces != Port.RECOVERY):
                raise GrammarError("fragment port consume is incompatible with recovery lifecycle")
        if watch_port != Port.ENTRY_PENDING: raise GrammarError("entry lifecycle does not terminate in entry pending")
        resource_ids = {"group": {item["id"] for item in self.context["groups"]}, "event": {item["id"] for item in self.context["events"]}, "plan": set(self.context["plans"])}
        for item in fragments:
            spec = REGISTRY[item["productionId"]]
            if set(item["resources"]) != set(spec.resource_slots): raise GrammarError("fragment resource closure is incomplete")
            if any(item["resources"][slot] not in resource_ids[slot] for slot in spec.resource_slots): raise GrammarError("fragment resource reference is unknown or incompatible")
            if set(item["choices"]) != set(spec.choice_domains): raise GrammarError("fragment choices must use exact named domains")
            if any(item["choices"][key] not in domain for key, domain in spec.choice_domains.items()): raise GrammarError("fragment choice is outside its named domain")
        # Per-side budget assumes a composite will namespace both modules.
        built = GraphBuilder(self.context, {"fragments": fragments}).build()
        if len(built["states"]) > self.context["budgets"]["states"] or len(built["transitions"]) > self.context["budgets"]["transitions"]: raise GrammarError("module exceeds per-side graph budget")
        if len(self.context["groups"]) > self.context["budgets"]["groups"] or len(self.context["events"]) > self.context["budgets"]["events"] or len(self.context["indicators"]) > self.context["budgets"]["indicators"]: raise GrammarError("context exceeds per-side resource budget")
        if any(_guard_depth(item["guard"]) > self.context["budgets"]["guardDepth"] for item in built["transitions"]): raise GrammarError("fragment guard depth exceeds budget")
        validate_entry_route_decision_indicator_cap(
            {
                "indicators": self.context["indicators"],
                "graph": {
                    "initialStateId": "ready",
                    "evidenceGroups": self.context["groups"],
                    "eventBindings": self.context["events"],
                    "transitions": built["transitions"],
                },
            }
        )

    def _profile_payload(self, program: ModuleProgram) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        self.validate(program)
        canonical = program.canonical(); built = GraphBuilder(self.context, canonical).build(); closure = _resource_closed_context(self.context, canonical["fragments"])
        profile = {"version": "v2", "name": f"typed fragment {program.direction} module", "description": "sealed typed fragment module; not an economic candidate", "instruments": [self.context["instrument"]], "directionMode": program.direction, "isActive": False, "indicators": _clone(closure["indicators"], name="module indicators"), "executionConfig": _clone(closure["executionConfig"], name="module execution"), "graph": {"kind": "temporal_graph_v1", "semanticPolicy": "temporal_graph_semantics_v1", "eventSchema": "temporal_event_v1", "factLibrary": "temporal_market_facts_v1", "guardLibrary": "temporal_guards_v1", "actionLibrary": "temporal_market_actions_v1", "clockRequirement": "clock.completed_bar", "fidelityRequirements": ["data.completed_ohlc"], "initialStateId": "ready", "states": built["states"], "evidenceGroups": _clone(closure["groups"], name="module groups"), "eventBindings": _clone(closure["events"], name="module events"), "transitions": built["transitions"]}}
        return canonical, built, profile

    def materialize_profile(self, program: ModuleProgram) -> dict[str, Any]:
        """Build the authored v2 profile without invoking native validation.

        Rich immigrant construction composes several already-sealed grammar
        and indicator operations before paying the native admission cost.  The
        resulting profile is still admitted exactly once before it can become
        a :class:`FrozenModule`; this method is only the pure construction
        half of that boundary.
        """

        _canonical, _built, profile = self._profile_payload(program)
        return _clone(profile, name="materialized typed fragment profile")

    def _compiled(self, program: ModuleProgram, canonical: Mapping[str, Any], profile: Mapping[str, Any], report: Mapping[str, Any], *, candidate_id: str) -> CompiledModule:
        report = _clone(dict(report), name="native validator report")
        raw_sha = canonical_sha256(profile)
        if report.get("schemaVersion") != "temporal_search_candidate_validation_v1" or report.get("candidateId") != candidate_id or report.get("rawSourceProfileSha256") != raw_sha or report.get("status") != "valid_evaluable" or not str(report.get("evaluatorId") or "") or report.get("candidateAcceptable") is not True or not str(report.get("programSha256") or "").startswith("sha256:") or not str(report.get("validationReportSha256") or "").startswith("sha256:"):
            raise GrammarError("Dashboard native validator rejected or failed to bind module identity")
        identities = {"contextSha256": self.context_sha256, "programSha256": canonical_sha256(canonical), "rawModuleSha256": raw_sha, "nativeProgramSha256": str(report["programSha256"]), "nativeValidationReportSha256": str(report["validationReportSha256"]), "compiledGraphStructureSha256": compiled_graph_signature(profile)}
        witnesses = tuple({"schemaVersion": WITNESS_SCHEMA, "productionId": item["productionId"], "recipe": REGISTRY[item["productionId"]].activation_recipe, "fragment": item} for item in canonical["fragments"])
        return CompiledModule(profile, canonical, tuple(_clone(dict(item), name="lineage") for item in program.lineage), identities, witnesses, report)

    def compile_module(self, program: ModuleProgram, *, candidate_id: str) -> CompiledModule:
        canonical, _built, profile = self._profile_payload(program)
        report = self.native_authority.validate_v2(profile=profile, candidate_id=candidate_id)
        return self._compiled(program, canonical, profile, report, candidate_id=candidate_id)

    def compile_generation_native(self, programs: Sequence[ModuleProgram], *, candidate_prefix: str = "typed_fragment_generation") -> list[CompiledModule]:
        """Batch-native admission for canary-scale finite generations."""
        validate_many = getattr(self.native_authority, "validate_many", None)
        if not callable(validate_many): raise GrammarError("efficient native batch authority is mandatory for generation admission")
        prepared = []
        for index, program in enumerate(programs):
            canonical, _built, profile = self._profile_payload(program)
            prepared.append((program, canonical, profile, f"{candidate_prefix}_{index:04d}"))
        reports = list(validate_many(items=[{"profile": item[2], "candidateId": item[3]} for item in prepared]))
        if len(reports) != len(prepared): raise GrammarError("native batch authority changed generation cardinality")
        return [self._compiled(program, canonical, profile, report, candidate_id=candidate_id) for (program, canonical, profile, candidate_id), report in zip(prepared, reports)]

    def compile_pair(self, long: CompiledModule, short: CompiledModule, *, candidate_id: str, pair_authority: PairCompiler | None = None) -> Mapping[str, Any]:
        if long.program["direction"] != "long" or short.program["direction"] != "short": raise GrammarError("economic candidates require one long and one short module")
        authority = pair_authority if pair_authority is not None else self.native_authority if hasattr(self.native_authority, "compile_pair") else None
        if authority is None: raise GrammarError("canonical Dashboard bidirectional compiler authority is mandatory")
        result = _clone(dict(authority.compile_pair(long_profile=long.profile, short_profile=short.profile, candidate_id=candidate_id)), name="pair authority result")
        profile, validation = result.get("profile"), result.get("validation")
        if not isinstance(profile, Mapping) or not isinstance(validation, Mapping) or validation.get("schemaVersion") != "temporal_search_candidate_validation_v1" or validation.get("candidateId") != candidate_id or validation.get("rawSourceProfileSha256") != canonical_sha256(profile) or validation.get("status") != "valid_evaluable" or not str(validation.get("evaluatorId") or "") or validation.get("candidateAcceptable") is not True or not str(validation.get("programSha256") or "").startswith("sha256:") or not str(validation.get("validationReportSha256") or "").startswith("sha256:") or profile.get("version") != "v3" or profile.get("directionMode") != "both": raise GrammarError("canonical Dashboard pair compiler rejected pair")
        manifests = ((profile.get("graph") or {}).get("entryArbitration") or {}).get("modules") or []
        native_snapshots = {item.get("direction"): item.get("sourceProfileSnapshotSha256") for item in manifests if isinstance(item, Mapping)}
        if native_snapshots.get("long") != long.native_report.get("profileSnapshotSha256") or native_snapshots.get("short") != short.native_report.get("profileSnapshotSha256"):
            raise GrammarError("canonical pair compiler did not bind both native module identities")
        return {"profile": dict(profile), "validation": dict(validation), "identities": {"longModuleSha256": long.identities["rawModuleSha256"], "shortModuleSha256": short.identities["rawModuleSha256"], "rawPairSha256": canonical_sha256(profile), "nativeProgramSha256": validation["programSha256"], "nativeValidationReportSha256": validation["validationReportSha256"]}}

    def _child(self, program: ModuleProgram, fragments: Sequence[Fragment], operation: str, details: Mapping[str, Any]) -> ModuleProgram:
        child = ModuleProgram(program.direction, tuple(fragments), (*program.lineage, {"operation": operation, "details": _clone(dict(details), name="operation details"), "parentProgramSha256": canonical_sha256(program.canonical())}))
        self.validate(child); return child

    def enumerate_operations(self, program: ModuleProgram) -> list[dict[str, Any]]:
        self.validate(program); rows = []; counts = Counter(item.production_id for item in program.fragments)
        for index, item in enumerate(program.fragments):
            spec = REGISTRY[item.production_id]
            for replacement in sorted(key for key, candidate in REGISTRY.items() if candidate.family == spec.family and key != item.production_id): rows.append({"operation": "substitute", "index": index, "productionId": replacement})
            for slot in spec.resource_slots:
                values = self.context[{"group": "groups", "event": "events", "plan": "plans"}[slot]]
                ids = [value["id"] if isinstance(value, Mapping) else value for value in values]
                for value in ids:
                    if value != item.resources[slot]: rows.append({"operation": "rebind", "index": index, "slot": slot, "value": value})
            for choice, domain in spec.choice_domains.items():
                for value in domain:
                    if value != item.choices[choice]: rows.append({"operation": "mutate_choice", "index": index, "choice": choice, "value": value})
            if spec.family in {"gate", "management", "exit", "recovery"} and counts[spec.production_id] < spec.max_instances:
                rows.append({"operation": "duplicate_specialize", "index": index})
        for index in range(len(program.fragments) - 1):
            if REGISTRY[program.fragments[index].production_id].family == REGISTRY[program.fragments[index + 1].production_id].family and program.fragments[index].canonical() != program.fragments[index + 1].canonical():
                rows.append({"operation": "move", "from": index, "to": index + 1})
        for production, spec in REGISTRY.items():
            if spec.family in {"gate", "management", "exit", "recovery"} and sum(item.production_id == production for item in program.fragments) < spec.max_instances: rows.append({"operation": "add_branch" if spec.family in {"management", "exit"} else "insert", "productionId": production})
        for index, item in enumerate(program.fragments):
            if REGISTRY[item.production_id].family in {"gate", "management", "exit", "recovery"}: rows.append({"operation": "remove_branch" if REGISTRY[item.production_id].family in {"management", "exit"} else "remove", "index": index})
        return sorted(rows, key=canonical_sha256)

    def apply(self, program: ModuleProgram, operation: Mapping[str, Any]) -> ModuleProgram:
        if dict(operation) not in self.enumerate_operations(program): raise GrammarError("operation is not canonical and applicable")
        op, items = operation["operation"], list(program.fragments)
        if op == "move":
            source, target = int(operation["from"]), int(operation["to"])
            item = items.pop(source); items.insert(target, item)
            return self._child(program, items, str(op), operation)
        if op in {"insert", "add_branch"}:
            spec = REGISTRY[operation["productionId"]]; resources = {slot: (self.context[{"group":"groups","event":"events","plan":"plans"}[slot]][0]["id"] if slot != "plan" else self.context["plans"][0]) for slot in spec.resource_slots}; items.append(self._fragment(spec.production_id, uid=f"edit_{len(items)}", resources=resources))
        else:
            index = int(operation["index"]); item = items[index]
            if op in {"remove", "remove_branch"}: items.pop(index)
            elif op == "substitute":
                target = REGISTRY[operation["productionId"]]; resources = {slot: item.resources[slot] for slot in target.resource_slots if slot in item.resources}
                for slot in target.resource_slots: resources.setdefault(slot, self.context[{"group":"groups","event":"events","plan":"plans"}[slot]][0]["id"] if slot != "plan" else self.context["plans"][0])
                items[index] = self._fragment(target.production_id, uid=item.uid, resources=resources)
            elif op == "rebind": items[index] = replace(item, resources={**item.resources, str(operation["slot"]): str(operation["value"])})
            elif op == "mutate_choice": items[index] = replace(item, choices={**item.choices, str(operation["choice"]): operation["value"]})
            elif op == "duplicate_specialize":
                spec = REGISTRY[item.production_id]
                choices = dict(item.choices)
                if spec.choice_domains:
                    key = sorted(spec.choice_domains)[0]; domain = spec.choice_domains[key]
                    choices[key] = domain[(domain.index(choices[key]) + 1) % len(domain)]
                items.append(Fragment(f"duplicate_{len(items)}", item.production_id, dict(item.resources), choices))
            else: raise GrammarError("unsupported canonical operation")
        # Normalized lifecycle order is part of the grammar, not a retry loop.
        rank = {"arm":0,"gate":1,"entry":2,"management":3,"exit":4,"recovery":5}; items.sort(key=lambda x: (rank[REGISTRY[x.production_id].family], x.production_id, canonical_sha256(x.canonical())))
        return self._child(program, items, str(op), operation)

    def crossover(self, left: ModuleProgram, right: ModuleProgram, *, direction: str) -> ModuleProgram:
        self.validate(left); self.validate(right)
        left_parts = [item for item in left.fragments if REGISTRY[item.production_id].family in {"arm", "gate", "entry"}]
        right_parts = [item for item in right.fragments if REGISTRY[item.production_id].family in {"management", "exit", "recovery"}]
        child = ModuleProgram(direction, tuple(left_parts + right_parts), ({"operation":"crossover","leftProgramSha256":canonical_sha256(left.canonical()),"rightProgramSha256":canonical_sha256(right.canonical())},))
        self.validate(child); return child

    def generate(self, *, count: int, seed: int) -> list[ModuleProgram]:
        if not 1 <= int(count) <= 4096: raise GrammarError("generation count must be 1..4096")
        # Finite product enumeration, ordered by a seeded canonical permutation;
        # no retry-until-valid behavior.
        roots = [(direction, name) for direction in ("long", "short") for name in ("mean_reversion", "breakout", "trend")]
        gates = tuple(key for key, spec in REGISTRY.items() if spec.family == "gate")
        managements = tuple(key for key, spec in REGISTRY.items() if spec.family == "management")
        exits = tuple(key for key, spec in REGISTRY.items() if spec.family == "exit")
        variants = list(product(roots, gates, managements, exits, range(5), range(4)))
        variants.sort(key=lambda item: canonical_sha256({"seed": seed, "variant": item}))
        output = []
        seen_programs: set[str] = set()
        quotas = {"long": count // 2 + count % 2, "short": count // 2}; used = Counter()
        for (direction, name), gate_kind, management_kind, exit_kind, cooldown_count, choice_index in variants:
            if len(output) >= count: break
            if used[direction] >= quotas[direction]: continue
            program = self.seed(direction=direction, name=name)
            for production in (gate_kind, management_kind, exit_kind):
                selected = next(item for item in self.enumerate_operations(program) if item.get("productionId") == production and item["operation"] in {"insert", "add_branch"})
                program = self.apply(program, selected)
            for _ in range(cooldown_count):
                choices = [item for item in self.enumerate_operations(program) if item["operation"] == "insert" and item["productionId"] == "cooldown"]
                if choices: program = self.apply(program, choices[0])
            mutations = [item for item in self.enumerate_operations(program) if item["operation"] == "mutate_choice"]
            if mutations: program = self.apply(program, mutations[choice_index % len(mutations)])
            # Duplicate fragments are canonically sorted and edit UIDs/lineage
            # are intentionally non-semantic.  Two index-addressed edits may
            # therefore enumerate the same program; admit that program once
            # while continuing the finite product traversal.
            program_sha256 = canonical_sha256(program.canonical())
            if program_sha256 in seen_programs:
                continue
            seen_programs.add(program_sha256)
            output.append(program); used[direction] += 1
        if len(output) != count: raise GrammarError("finite generator has insufficient valid variants")
        return output


def module_signatures(program: ModuleProgram) -> dict[str, str]:
    canonical = program.canonical()
    fragments = canonical["fragments"]
    shape = [{"family": REGISTRY[item["productionId"]].family, "productionId": item["productionId"], "ports": [REGISTRY[item["productionId"]].consumes.value, REGISTRY[item["productionId"]].produces.value]} for item in fragments]
    parameters = [{"productionId": item["productionId"], "choices": item["choices"]} for item in fragments]
    composition = [item["productionId"] for item in fragments]
    return {"programShapeSha256": canonical_sha256(shape), "parameterSha256": canonical_sha256(parameters), "motifCompositionSha256": canonical_sha256(composition), "directionSha256": canonical_sha256({"direction": canonical["direction"]})}


def inspect_module(profile: Mapping[str, Any]) -> dict[str, Any]:
    """Diagnostic only; native authority remains the admission decision."""
    graph = profile.get("graph") if isinstance(profile, Mapping) else None
    states = graph.get("states") if isinstance(graph, Mapping) else []
    transitions = graph.get("transitions") if isinstance(graph, Mapping) else []
    ids = {str(item.get("id")) for item in states if isinstance(item, Mapping)}
    refs_ok = all(isinstance(item, Mapping) and item.get("sourceStateId") in ids and item.get("destinationStateId") in ids for item in transitions)
    return {"schemaVersion": GRAMMAR_SCHEMA, "diagnosticOnly": True, "stateCount": len(states), "transitionCount": len(transitions), "referenceClosure": refs_ok, "profileSha256": canonical_sha256(profile)}


__all__ = ["DashboardNativeAuthority", "ENTRY_ROUTE_DECISION_INDICATOR_CAP", "ENTRY_ROUTE_DECISION_INDICATOR_POLICY_VERSION", "EntryRouteDecisionIndicatorCapError", "Fragment", "FragmentSpec", "GRAMMAR_SCHEMA", "GRAMMAR_VERSION", "GrammarContext", "GrammarError", "ModuleProgram", "NativeValidator", "PairCompiler", "Port", "REGISTRY", "TypedFragmentGrammar", "CompiledModule", "compiled_graph_signature", "entry_route_decision_indicator_report", "inspect_module", "module_signatures", "validate_entry_route_decision_indicator_cap"]
