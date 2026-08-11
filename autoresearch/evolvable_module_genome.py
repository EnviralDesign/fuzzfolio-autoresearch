"""Deterministic, typed foundation for evolvable temporal module genomes.

This is deliberately an AutoResearch-owned genotype, rather than another
Dashboard profile codec.  It compiles a sealed, one-direction program to the
Dashboard-owned v2 JSON shape, and leaves v3 pairing to the existing canonical
Dashboard pair compiler.  No search, mutation, or QD registration lives here.
"""

from __future__ import annotations

from collections import Counter, defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from .temporal_search import canonical_sha256


GENOME_SCHEMA = "evolvable_module_genome_v1"
PROGRAM_KIND = "evolvable_module_genome_v1"
GENOME_CODEC = "evolvable_module_genome_json_v1"
COMPILER_POLICY_SCHEMA = "evolvable_module_compiler_policy_v1"
# This schema has always promised an ID- and representation-independent graph
# signature.  Source/authority commits bind implementation fixes; do not bump
# the semantic schema merely to preserve an insertion-order bug in old lineage.
TOPOLOGY_SCHEMA = "evolvable_module_semantic_topology_v1"


class EvolvableGenomeError(ValueError):
    """Raised when a genome cannot safely be compiled as a v2 module."""


class Zone(str, Enum):
    ENTRY = "entry"
    SETUP = "setup"
    POSITION = "position"
    MANAGEMENT = "management"
    EXIT = "exit"
    RECOVERY = "recovery"


class ResourceKind(str, Enum):
    INDICATOR = "indicator"
    EVIDENCE_GROUP = "evidence_group"
    EVENT = "event"
    MANAGEMENT_REF = "management_ref"


class EffectKind(str, Enum):
    ENTER = "enter_next_open"
    EXIT = "exit_next_open"
    BREAK_EVEN = "move_stop_to_break_even_next_open"
    TIGHTEN_STOP = "tighten_stop_next_open"
    SET_TARGET = "set_target_next_open"
    CANCEL_TARGET = "cancel_target_next_open"
    ACTIVATE_TRAILING = "activate_trailing_stop_next_open"
    DEACTIVATE_TRAILING = "deactivate_trailing_stop_next_open"


_MANAGEMENT_EFFECTS = frozenset(
    {
        EffectKind.BREAK_EVEN,
        EffectKind.TIGHTEN_STOP,
        EffectKind.SET_TARGET,
        EffectKind.CANCEL_TARGET,
        EffectKind.ACTIVATE_TRAILING,
        EffectKind.DEACTIVATE_TRAILING,
    }
)


def _clone(value: Any, *, name: str) -> Any:
    """Canonical JSON clone used by the surrounding temporal contracts."""

    try:
        import json

        return json.loads(
            json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        )
    except (TypeError, ValueError) as exc:
        raise EvolvableGenomeError(f"{name} must be finite canonical JSON") from exc


def _token(value: Any, *, name: str) -> str:
    token = str(value or "").strip()
    if not token or len(token) > 240:
        raise EvolvableGenomeError(f"{name} must be a nonempty explicit identifier")
    return token


def _side(value: Any, *, name: str = "direction") -> str:
    token = _token(value, name=name).lower()
    if token not in {"long", "short"}:
        raise EvolvableGenomeError(f"{name} must be long or short")
    return token


def _guard_depth(value: Any) -> int:
    if not isinstance(value, Mapping):
        return 0
    children = value.get("guards")
    nested = value.get("predicate")
    negated = value.get("guard")
    depths = [_guard_depth(item) for item in children or ()]
    if nested is not None:
        depths.append(_guard_depth(nested))
    if negated is not None:
        depths.append(_guard_depth(negated))
    return 1 + max(depths, default=0)


def _guard_resource_uses(
    value: Mapping[str, Any],
    *,
    pools: Mapping[ResourceKind, Mapping[str, Mapping[str, Any]]],
    location: str,
) -> tuple[ResourceUse, ...]:
    """Validate composite shape and resolve every raw guard resource reference.

    Node/edge ``ResourceUse`` declarations remain optional *per guard*; the
    module pool and global closure are the authority.  This prevents an edge
    guard from silently pointing to an event/group owned by another program or
    to an object of the wrong resource family.
    """

    if not isinstance(value, Mapping):
        raise EvolvableGenomeError(f"{location} must be a guard object")
    if not value:
        # Empty guards are the existing authored spelling of implicit always.
        return ()
    raw_kind = value.get("kind")
    if not isinstance(raw_kind, str) or not raw_kind.strip():
        raise EvolvableGenomeError(f"{location} guard requires a nonempty kind")
    kind = raw_kind.strip()
    uses: list[ResourceUse] = []

    def resolve(field: str, resource_kind: ResourceKind) -> None:
        if field not in value:
            return
        raw_identifier = value[field]
        if not isinstance(raw_identifier, str):
            raise EvolvableGenomeError(f"{location} {field} must be an explicit string resource ID")
        identifier = _token(raw_identifier, name=f"{location} {field}")
        if identifier not in pools[resource_kind]:
            wrong_kind = next(
                (other.value for other, rows in pools.items() if other is not resource_kind and identifier in rows),
                None,
            )
            if wrong_kind is not None:
                raise EvolvableGenomeError(
                    f"{location} {field} references {wrong_kind}; expected {resource_kind.value}"
                )
            raise EvolvableGenomeError(f"{location} references a dangling {resource_kind.value}: {identifier}")
        uses.append(ResourceUse(resource_kind, identifier))

    # These aliases appear in frozen Dashboard material.  Treat them as the
    # same typed contracts rather than allowing an alternate spelling to evade
    # the module-pool boundary.
    resolve("groupId", ResourceKind.EVIDENCE_GROUP)
    resolve("evidenceGroupId", ResourceKind.EVIDENCE_GROUP)
    resolve("eventId", ResourceKind.EVENT)
    resolve("eventBindingId", ResourceKind.EVENT)

    if kind in {"all", "any"}:
        children = value.get("guards")
        if not isinstance(children, list) or not children:
            raise EvolvableGenomeError(f"{location} {kind} guard requires a nonempty guards list")
        for index, child in enumerate(children):
            if not isinstance(child, Mapping):
                raise EvolvableGenomeError(f"{location} {kind} guard child {index} must be an object")
            uses.extend(_guard_resource_uses(child, pools=pools, location=f"{location}.{kind}[{index}]"))
    elif kind == "not":
        child = value.get("guard")
        if not isinstance(child, Mapping) or not child:
            raise EvolvableGenomeError(f"{location} not guard requires one guard object")
        uses.extend(_guard_resource_uses(child, pools=pools, location=f"{location}.not"))
    elif kind in {"predicate_edge", "consecutive_true"}:
        child = value.get("predicate")
        if not isinstance(child, Mapping) or not child:
            raise EvolvableGenomeError(f"{location} {kind} guard requires one predicate object")
        uses.extend(_guard_resource_uses(child, pools=pools, location=f"{location}.{kind}"))
    return tuple(uses)


@dataclass(frozen=True)
class ResourceUse:
    kind: ResourceKind
    resource_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", ResourceKind(self.kind))
        object.__setattr__(self, "resource_id", _token(self.resource_id, name="resource ID"))

    def canonical(self) -> dict[str, str]:
        return {"kind": self.kind.value, "id": self.resource_id}


@dataclass(frozen=True)
class ResourcePoolV1:
    """Frozen catalog-resolved resources owned by one module genome."""

    indicators: tuple[Mapping[str, Any], ...] = ()
    evidence_groups: tuple[Mapping[str, Any], ...] = ()
    events: tuple[Mapping[str, Any], ...] = ()
    management_refs: tuple[Mapping[str, Any], ...] = ()

    def _rows(self, kind: ResourceKind) -> tuple[dict[str, Any], ...]:
        source = {
            ResourceKind.INDICATOR: self.indicators,
            ResourceKind.EVIDENCE_GROUP: self.evidence_groups,
            ResourceKind.EVENT: self.events,
            ResourceKind.MANAGEMENT_REF: self.management_refs,
        }[kind]
        rows: list[dict[str, Any]] = []
        for item in source:
            if not isinstance(item, Mapping):
                raise EvolvableGenomeError(f"{kind.value} resource must be an object")
            row = _clone(dict(item), name=f"{kind.value} resource")
            # Dashboard indicator IDs are nested; every other current resource
            # family carries its resource key at the top level.
            if kind is ResourceKind.INDICATOR:
                identifier = ((row.get("meta") or {}).get("instanceId"))
            else:
                identifier = row.get("id")
            _token(identifier, name=f"{kind.value} resource ID")
            rows.append(row)
        identifiers = [self.identifier(kind, row) for row in rows]
        if len(identifiers) != len(set(identifiers)):
            raise EvolvableGenomeError(f"duplicate {kind.value} resource ID")
        return tuple(sorted(rows, key=lambda row: self.identifier(kind, row)))

    @staticmethod
    def identifier(kind: ResourceKind, row: Mapping[str, Any]) -> str:
        if kind is ResourceKind.INDICATOR:
            return _token((row.get("meta") or {}).get("instanceId"), name="indicator resource ID")
        return _token(row.get("id"), name=f"{kind.value} resource ID")

    def mapping(self, kind: ResourceKind) -> dict[str, dict[str, Any]]:
        return {self.identifier(kind, row): row for row in self._rows(kind)}

    def canonical(self) -> dict[str, list[dict[str, Any]]]:
        return {
            "indicators": list(self._rows(ResourceKind.INDICATOR)),
            "evidenceGroups": list(self._rows(ResourceKind.EVIDENCE_GROUP)),
            "events": list(self._rows(ResourceKind.EVENT)),
            "managementRefs": list(self._rows(ResourceKind.MANAGEMENT_REF)),
        }


@dataclass(frozen=True)
class GenomeNodeV1:
    node_id: str
    zone: Zone
    kind: str
    guard: Mapping[str, Any] = field(default_factory=dict)
    resources: tuple[ResourceUse, ...] = ()
    timeout_bars: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "node_id", _token(self.node_id, name="node ID"))
        object.__setattr__(self, "zone", Zone(self.zone))
        object.__setattr__(self, "kind", _token(self.kind, name="node kind"))
        object.__setattr__(self, "guard", _clone(dict(self.guard), name="node guard"))
        object.__setattr__(self, "resources", tuple(ResourceUse(item.kind, item.resource_id) if isinstance(item, ResourceUse) else ResourceUse(**item) for item in self.resources))
        if self.timeout_bars is not None and (isinstance(self.timeout_bars, bool) or int(self.timeout_bars) < 1):
            raise EvolvableGenomeError("timeout bars must be a positive integer")

    def canonical(self, *, include_id: bool = True) -> dict[str, Any]:
        value: dict[str, Any] = {
            "zone": self.zone.value,
            "kind": self.kind,
            "guard": _clone(self.guard, name="node guard"),
            "resources": [item.canonical() for item in sorted(self.resources, key=lambda item: (item.kind.value, item.resource_id))],
            "timeoutBars": self.timeout_bars,
        }
        if include_id:
            value["id"] = self.node_id
        return value


@dataclass(frozen=True)
class GenomeEdgeV1:
    edge_id: str
    source_id: str
    target_id: str
    event_class: str = "decision"
    priority: int = 10
    guard: Mapping[str, Any] = field(default_factory=dict)
    effect: EffectKind | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "edge_id", _token(self.edge_id, name="edge ID"))
        object.__setattr__(self, "source_id", _token(self.source_id, name="edge source ID"))
        object.__setattr__(self, "target_id", _token(self.target_id, name="edge target ID"))
        event_class = _token(self.event_class, name="edge event class")
        if event_class != "decision":
            raise EvolvableGenomeError("authored genome edges must use completed-bar decision events")
        object.__setattr__(self, "event_class", event_class)
        if isinstance(self.priority, bool) or not 0 <= int(self.priority) <= 999:
            raise EvolvableGenomeError("edge priority must be an integer from 0 to 999")
        object.__setattr__(self, "priority", int(self.priority))
        object.__setattr__(self, "guard", _clone(dict(self.guard), name="edge guard"))
        object.__setattr__(self, "effect", None if self.effect is None else EffectKind(self.effect))

    def canonical(self, *, include_id: bool = True) -> dict[str, Any]:
        value: dict[str, Any] = {
            "source": self.source_id,
            "target": self.target_id,
            "eventClass": self.event_class,
            "priority": self.priority,
            "guard": _clone(self.guard, name="edge guard"),
            "effect": self.effect.value if self.effect else None,
        }
        if include_id:
            value["id"] = self.edge_id
        return value


@dataclass(frozen=True)
class BudgetContractV1:
    """Initial memo caps.  Lower caller-supplied caps are allowed, never higher."""

    max_states: int = 14
    max_transitions: int = 56
    max_evidence_groups: int = 4
    max_group_members: int = 3
    max_events: int = 4
    max_indicators: int = 12
    max_entry_branches: int = 3
    max_management_regions: int = 4
    max_exit_regions: int = 3
    max_recovery_regions: int = 3
    max_scc_nodes: int = 3
    max_timeout_bars: int = 64
    max_guard_depth: int = 4

    def __post_init__(self) -> None:
        for name, cap in self.canonical().items():
            if isinstance(cap, bool) or not isinstance(cap, int) or cap < 1:
                raise EvolvableGenomeError(f"budget {name} must be a positive integer")
        initial_caps = {
            "maxStates": 14, "maxTransitions": 56, "maxEvidenceGroups": 4,
            "maxGroupMembers": 3, "maxEvents": 4, "maxIndicators": 12,
            "maxEntryBranches": 3, "maxManagementRegions": 4,
            "maxExitRegions": 3, "maxRecoveryRegions": 3, "maxSccNodes": 3,
            "maxTimeoutBars": 64, "maxGuardDepth": 4,
        }
        for name, cap in self.canonical().items():
            if cap > initial_caps[name]:
                raise EvolvableGenomeError(f"budget {name} may not exceed the v1 initial cap")

    def canonical(self) -> dict[str, int]:
        return {
            "maxStates": self.max_states, "maxTransitions": self.max_transitions,
            "maxEvidenceGroups": self.max_evidence_groups, "maxGroupMembers": self.max_group_members,
            "maxEvents": self.max_events, "maxIndicators": self.max_indicators,
            "maxEntryBranches": self.max_entry_branches, "maxManagementRegions": self.max_management_regions,
            "maxExitRegions": self.max_exit_regions, "maxRecoveryRegions": self.max_recovery_regions,
            "maxSccNodes": self.max_scc_nodes, "maxTimeoutBars": self.max_timeout_bars,
            "maxGuardDepth": self.max_guard_depth,
        }


@dataclass(frozen=True)
class CompilerPolicyIdentityV1:
    compiler_id: str = "autoresearch.evolvable-module-v1-to-dashboard-v2"
    target_profile_version: str = "v2"
    execution_semantics: Mapping[str, Any] = field(default_factory=lambda: {
        "onePosition": True, "maxPendingNextOpenEffects": 1,
        "completedBarDecisions": True, "nextOpenApplication": True,
        "managementTopology": "shared_position_hub_deterministic_priority_v1",
    })

    def canonical(self) -> dict[str, Any]:
        value = {"schemaVersion": COMPILER_POLICY_SCHEMA, "compilerId": _token(self.compiler_id, name="compiler ID"), "targetProfileVersion": _token(self.target_profile_version, name="target profile version"), "executionSemantics": _clone(dict(self.execution_semantics), name="compiler execution semantics")}
        if value["targetProfileVersion"] != "v2":
            raise EvolvableGenomeError("v1 compiler only targets Dashboard v2 modules")
        expected = {"onePosition": True, "maxPendingNextOpenEffects": 1, "completedBarDecisions": True, "nextOpenApplication": True}
        if any(value["executionSemantics"].get(key) != expected_value for key, expected_value in expected.items()):
            raise EvolvableGenomeError("compiler policy must preserve one-position next-open execution semantics")
        return value

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.canonical())


class NativeModuleValidator(Protocol):
    def validate_v2(self, *, profile: Mapping[str, Any], candidate_id: str) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class EvolvableModuleGenomeV1:
    direction: str
    resources: ResourcePoolV1
    nodes: tuple[GenomeNodeV1, ...]
    edges: tuple[GenomeEdgeV1, ...]
    budget: BudgetContractV1 = field(default_factory=BudgetContractV1)
    program_kind: str = PROGRAM_KIND
    codec: str = GENOME_CODEC
    instrument: str = "EURUSD"

    def __post_init__(self) -> None:
        object.__setattr__(self, "direction", _side(self.direction))
        object.__setattr__(self, "instrument", _token(self.instrument, name="instrument").upper())
        object.__setattr__(self, "nodes", tuple(self.nodes))
        object.__setattr__(self, "edges", tuple(self.edges))
        if self.program_kind != PROGRAM_KIND or self.codec != GENOME_CODEC:
            raise EvolvableGenomeError("unsupported evolvable module program kind or codec")

    def canonical(self) -> dict[str, Any]:
        return {
            "schemaVersion": GENOME_SCHEMA, "programKind": self.program_kind, "codec": self.codec,
            "direction": self.direction, "instrument": self.instrument, "resources": self.resources.canonical(),
            "nodes": [node.canonical() for node in sorted(self.nodes, key=lambda node: node.node_id)],
            "edges": [edge.canonical() for edge in sorted(self.edges, key=lambda edge: edge.edge_id)],
            "budget": self.budget.canonical(),
        }

    @property
    def identity_sha256(self) -> str:
        return canonical_sha256(self.canonical())

    def semantic_topology_signature(self) -> str:
        """ID-independent topology identity; resource IDs and numeric knobs are omitted."""

        nodes = {node.node_id: node for node in self.nodes}
        labels = {
            identifier: canonical_sha256(
                node.canonical(include_id=False)
                | {
                    "guard": _guard_shape(node.guard),
                    # Resource IDs are intentionally omitted, but resource-kind
                    # multiplicity is topology.  Canonicalize the kinds just as
                    # ``GenomeNodeV1.canonical`` canonicalizes ResourceUse rows;
                    # otherwise an in-memory genome and its own codec round trip
                    # can acquire different topology identities.
                    "resources": sorted(item.kind.value for item in node.resources),
                    "timeoutBars": node.timeout_bars is not None,
                }
            )
            for identifier, node in nodes.items()
        }
        # Weisfeiler-Lehman style refinement makes ordinary ID renames irrelevant
        # while retaining directed topology and edge semantics.
        for _ in range(max(1, len(nodes))):
            updated = {}
            for identifier, node in nodes.items():
                outgoing = [(edge.event_class, edge.priority, edge.effect.value if edge.effect else None, _guard_shape(edge.guard), labels[edge.target_id]) for edge in self.edges if edge.source_id == identifier and edge.target_id in labels]
                incoming = [(edge.event_class, edge.priority, edge.effect.value if edge.effect else None, _guard_shape(edge.guard), labels[edge.source_id]) for edge in self.edges if edge.target_id == identifier and edge.source_id in labels]
                updated[identifier] = canonical_sha256({"node": labels[identifier], "out": sorted(outgoing, key=repr), "in": sorted(incoming, key=repr)})
            if updated == labels:
                break
            labels = updated
        return canonical_sha256({"schemaVersion": TOPOLOGY_SCHEMA, "nodes": sorted(labels.values()), "edges": sorted((labels[edge.source_id], labels[edge.target_id], edge.event_class, edge.priority, edge.effect.value if edge.effect else None, _guard_shape(edge.guard)) for edge in self.edges if edge.source_id in labels and edge.target_id in labels)})

    def validate(self) -> None:
        nodes = {node.node_id: node for node in self.nodes}
        if len(nodes) != len(self.nodes):
            raise EvolvableGenomeError("duplicate node ID")
        if len({edge.edge_id for edge in self.edges}) != len(self.edges):
            raise EvolvableGenomeError("duplicate edge ID")
        if not nodes or not self.edges:
            raise EvolvableGenomeError("genome requires nodes and edges")
        if any(edge.source_id not in nodes or edge.target_id not in nodes for edge in self.edges):
            raise EvolvableGenomeError("dangling graph edge")
        starts = [node for node in nodes.values() if node.zone is Zone.ENTRY and node.kind == "start"]
        hubs = [node for node in nodes.values() if node.zone is Zone.POSITION and node.kind == "position_hub"]
        if len(starts) != 1 or len(hubs) != 1:
            raise EvolvableGenomeError("genome requires exactly one entry start and shared position hub")
        if any(node.zone is Zone.POSITION and node is not hubs[0] for node in nodes.values()):
            raise EvolvableGenomeError("only the shared position hub may occupy the position zone")
        if any(_guard_depth(node.guard) > self.budget.max_guard_depth for node in nodes.values()) or any(_guard_depth(edge.guard) > self.budget.max_guard_depth for edge in self.edges):
            raise EvolvableGenomeError("guard exceeds bounded depth")
        self._validate_resources(nodes)
        self._validate_budgets(nodes)
        self._validate_edges(nodes, starts[0], hubs[0])
        self._validate_reachability(nodes, starts[0], hubs[0])
        self._validate_entry_setup_dag(nodes)
        self._validate_scc_and_timeouts(nodes)

    def _validate_resources(self, nodes: Mapping[str, GenomeNodeV1]) -> None:
        pools = {kind: self.resources.mapping(kind) for kind in ResourceKind}
        references = Counter(use for node in nodes.values() for use in node.resources)
        # Guard references participate in module-level closure even if an edge
        # does not redundantly declare its group/event on either endpoint.
        for node in nodes.values():
            references.update(
                _guard_resource_uses(
                    node.guard,
                    pools=pools,
                    location=f"node {node.node_id} guard",
                )
            )
        for edge in self.edges:
            references.update(
                _guard_resource_uses(
                    edge.guard,
                    pools=pools,
                    location=f"edge {edge.edge_id} guard",
                )
            )
        # Dynamic management locators bind a catalog indicator output without
        # making it fuzzy evidence or a raw event.  The binding is part of the
        # genotype-owned management resource, so it must participate in the
        # same closure/orphan checks as every other indicator use.
        for plan in pools[ResourceKind.MANAGEMENT_REF].values():
            for binding in plan.get("scalarBindings") or []:
                if not isinstance(binding, Mapping) or not isinstance(binding.get("indicatorInstanceId"), str):
                    raise EvolvableGenomeError("management scalar binding is malformed")
                references[ResourceUse(ResourceKind.INDICATOR, binding["indicatorInstanceId"])] += 1
        for use in references:
            if use.resource_id not in pools[use.kind]:
                raise EvolvableGenomeError(f"dangling {use.kind.value} resource reference")
        # Groups and events are typed resource owners: their indicator members
        # count as consumed even when a guard only names the group/event.
        for use in tuple(references):
            if use.kind is ResourceKind.EVIDENCE_GROUP and use.resource_id in pools[use.kind]:
                for identifier in pools[use.kind][use.resource_id].get("indicatorInstanceIds") or []:
                    references[ResourceUse(ResourceKind.INDICATOR, str(identifier))] += 1
            if use.kind is ResourceKind.EVENT and use.resource_id in pools[use.kind]:
                identifier = str(pools[use.kind][use.resource_id].get("indicatorInstanceId") or "")
                if identifier:
                    references[ResourceUse(ResourceKind.INDICATOR, identifier)] += 1
        for use in references:
            owner = str(pools[use.kind][use.resource_id].get("side") or pools[use.kind][use.resource_id].get("ownerSide") or "both").lower()
            if owner not in {"both", self.direction}:
                raise EvolvableGenomeError("cross-side resource reference")
        supplied = {ResourceUse(kind, identifier) for kind, rows in pools.items() for identifier in rows}
        unused = supplied - set(references)
        if unused:
            raise EvolvableGenomeError("orphan resource in module resource pool")
        group_map = pools[ResourceKind.EVIDENCE_GROUP]
        indicator_ids = set(pools[ResourceKind.INDICATOR])
        for group in group_map.values():
            members = group.get("indicatorInstanceIds") or []
            if not isinstance(members, list) or not members or len(members) > self.budget.max_group_members:
                raise EvolvableGenomeError("evidence group membership violates budget")
            if not set(map(str, members)).issubset(indicator_ids):
                raise EvolvableGenomeError("evidence group has dangling indicator member")
        for event in pools[ResourceKind.EVENT].values():
            if str(event.get("indicatorInstanceId") or "") not in indicator_ids:
                raise EvolvableGenomeError("event has dangling indicator reference")

    def _validate_budgets(self, nodes: Mapping[str, GenomeNodeV1]) -> None:
        if len(nodes) > self.budget.max_states or len(self.edges) > self.budget.max_transitions:
            raise EvolvableGenomeError("authored graph exceeds state or transition budget")
        counts = {ResourceKind.INDICATOR: len(self.resources.mapping(ResourceKind.INDICATOR)), ResourceKind.EVIDENCE_GROUP: len(self.resources.mapping(ResourceKind.EVIDENCE_GROUP)), ResourceKind.EVENT: len(self.resources.mapping(ResourceKind.EVENT))}
        if counts[ResourceKind.INDICATOR] > self.budget.max_indicators or counts[ResourceKind.EVIDENCE_GROUP] > self.budget.max_evidence_groups or counts[ResourceKind.EVENT] > self.budget.max_events:
            raise EvolvableGenomeError("resource pool exceeds initial v1 budget")
        if sum(node.zone is Zone.MANAGEMENT for node in nodes.values()) > self.budget.max_management_regions or sum(node.zone is Zone.EXIT for node in nodes.values()) > self.budget.max_exit_regions or sum(node.zone is Zone.RECOVERY for node in nodes.values()) > self.budget.max_recovery_regions:
            raise EvolvableGenomeError("region count exceeds initial v1 budget")

    def _validate_edges(self, nodes: Mapping[str, GenomeNodeV1], start: GenomeNodeV1, hub: GenomeNodeV1) -> None:
        entry_effects = 0
        priorities: set[tuple[str, str, int]] = set()
        for edge in self.edges:
            source, target = nodes[edge.source_id], nodes[edge.target_id]
            priority_key = (edge.source_id, edge.event_class, edge.priority)
            if priority_key in priorities:
                raise EvolvableGenomeError("priority conflict at one source/event class")
            priorities.add(priority_key)
            if edge.effect is not None and not isinstance(edge.effect, EffectKind):
                raise EvolvableGenomeError("edge may carry at most one known effect")
            if target.zone is Zone.ENTRY and target.kind == "entry":
                entry_effects += 1
                if source.zone not in {Zone.ENTRY, Zone.SETUP} or edge.effect is not EffectKind.ENTER:
                    raise EvolvableGenomeError("entry branches require one enter_next_open effect")
            elif target.zone is Zone.SETUP:
                if source.zone not in {Zone.ENTRY, Zone.SETUP} or edge.effect is not None:
                    raise EvolvableGenomeError("setup graph must be a side-effect-free entry/setup DAG")
            elif target.node_id == hub.node_id:
                if source.zone is not Zone.ENTRY or source.kind != "entry" or edge.effect is not None:
                    raise EvolvableGenomeError("entry result may only connect conceptually to the position hub")
            elif target.zone is Zone.MANAGEMENT:
                if source.node_id != hub.node_id or edge.effect not in _MANAGEMENT_EFFECTS:
                    raise EvolvableGenomeError("management regions must dispatch from the shared position hub")
            elif target.zone is Zone.EXIT:
                if source.node_id != hub.node_id or edge.effect is not EffectKind.EXIT:
                    raise EvolvableGenomeError("exit regions must dispatch from the shared position hub")
            elif target.zone is Zone.RECOVERY:
                # A pre-position rejection/cooldown path is a pure decision
                # route.  It cannot queue an effect or create a position; the
                # recovery compiler transition re-arms the entry start only
                # after this state has aged through its explicit bound.
                if source.zone not in {Zone.ENTRY, Zone.SETUP} or edge.effect is not None:
                    raise EvolvableGenomeError("recovery routes must be side-effect-free pre-position paths")
                if target.timeout_bars is None:
                    raise EvolvableGenomeError("pre-position recovery route requires a bounded timeout")
            else:
                raise EvolvableGenomeError("unsupported authored edge topology")
        if not 1 <= entry_effects <= self.budget.max_entry_branches:
            raise EvolvableGenomeError("entry branch count violates initial v1 budget")

    def _validate_reachability(self, nodes: Mapping[str, GenomeNodeV1], start: GenomeNodeV1, hub: GenomeNodeV1) -> None:
        adjacency: dict[str, set[str]] = defaultdict(set)
        for edge in self.edges:
            adjacency[edge.source_id].add(edge.target_id)
        # Entry effects and system protective close provide the semantic paths
        # that intentionally do not appear as authored decision edges.
        for node in nodes.values():
            if node.zone is Zone.ENTRY and node.kind == "entry":
                adjacency[node.node_id].add(hub.node_id)
            if node.zone is Zone.RECOVERY:
                adjacency[hub.node_id].add(node.node_id)
        seen, queue = {start.node_id}, deque([start.node_id])
        while queue:
            current = queue.popleft()
            for target in adjacency[current]:
                if target not in seen:
                    seen.add(target); queue.append(target)
        if set(nodes) - seen:
            raise EvolvableGenomeError("orphan or unreachable graph node")

    def _validate_entry_setup_dag(self, nodes: Mapping[str, GenomeNodeV1]) -> None:
        """The pre-position graph is intentionally a DAG, not a retry loop."""

        active = {node.node_id for node in nodes.values() if node.zone in {Zone.ENTRY, Zone.SETUP}}
        # The semantic entry -> hub connector is outside this pre-position DAG.
        adjacency: dict[str, set[str]] = defaultdict(set)
        for edge in self.edges:
            if edge.source_id in active and edge.target_id in active:
                adjacency[edge.source_id].add(edge.target_id)
        visiting: set[str] = set(); visited: set[str] = set()
        def visit(vertex: str) -> None:
            if vertex in visiting:
                raise EvolvableGenomeError("entry/setup graph must be acyclic")
            if vertex in visited:
                return
            visiting.add(vertex)
            for target in adjacency[vertex]:
                visit(target)
            visiting.remove(vertex); visited.add(vertex)
        for vertex in active:
            visit(vertex)

    def _validate_scc_and_timeouts(self, nodes: Mapping[str, GenomeNodeV1]) -> None:
        adjacency: dict[str, set[str]] = defaultdict(set)
        for edge in self.edges:
            adjacency[edge.source_id].add(edge.target_id)
        # Tarjan is compact and lets bounded loops be rejected before Dashboard.
        index = 0; indices: dict[str, int] = {}; low: dict[str, int] = {}; stack: list[str] = []; on_stack: set[str] = set()
        def visit(vertex: str) -> None:
            nonlocal index
            indices[vertex] = low[vertex] = index; index += 1; stack.append(vertex); on_stack.add(vertex)
            for target in adjacency[vertex]:
                if target not in indices:
                    visit(target); low[vertex] = min(low[vertex], low[target])
                elif target in on_stack:
                    low[vertex] = min(low[vertex], indices[target])
            if low[vertex] == indices[vertex]:
                component: list[str] = []
                while True:
                    item = stack.pop(); on_stack.remove(item); component.append(item)
                    if item == vertex: break
                cyclic = len(component) > 1 or vertex in adjacency[vertex]
                if cyclic:
                    if len(component) > self.budget.max_scc_nodes:
                        raise EvolvableGenomeError("SCC exceeds bounded loop budget")
                    if not any(nodes[item].timeout_bars is not None for item in component):
                        raise EvolvableGenomeError("cyclic topology requires a bounded timeout node")
        for vertex in nodes:
            if vertex not in indices:
                visit(vertex)
        for node in nodes.values():
            if node.zone is Zone.RECOVERY and node.timeout_bars is None:
                raise EvolvableGenomeError("recovery nodes require a bounded timeout")
            if node.timeout_bars is not None and node.timeout_bars > self.budget.max_timeout_bars:
                raise EvolvableGenomeError("timeout exceeds initial v1 budget")


def _guard_all(*guards: Mapping[str, Any]) -> dict[str, Any]:
    values = [dict(value) for value in guards if value]
    if not values:
        return {"kind": "always"}
    if len(values) == 1:
        return values[0]
    return {"kind": "all", "guards": values}


class EvolvableModuleCompilerV1:
    """Compiles valid genomes to Dashboard v2 without owning native semantics."""

    def __init__(self, policy: CompilerPolicyIdentityV1 | None = None) -> None:
        self.policy = policy or CompilerPolicyIdentityV1()
        self.policy.canonical()  # validate eagerly

    def compile(self, genome: EvolvableModuleGenomeV1, *, candidate_id: str, native_validator: NativeModuleValidator | None = None) -> dict[str, Any]:
        genome.validate()
        profile = self._profile(genome)
        # Compile-side caps include generated pending/status/protection states.
        graph = profile["graph"]
        if len(graph["states"]) > genome.budget.max_states or len(graph["transitions"]) > genome.budget.max_transitions:
            raise EvolvableGenomeError("compiled v2 graph exceeds initial v1 budget")
        result: dict[str, Any] = {
            "programKind": PROGRAM_KIND, "codec": GENOME_CODEC,
            "compilerPolicy": self.policy.canonical(), "compilerPolicySha256": self.policy.sha256,
            "genomeSha256": genome.identity_sha256, "semanticTopologySha256": genome.semantic_topology_signature(),
            "profile": profile, "profileSha256": canonical_sha256(profile),
        }
        if native_validator is not None:
            report = _clone(dict(native_validator.validate_v2(profile=profile, candidate_id=_token(candidate_id, name="candidate ID"))), name="native validation report")
            result["nativeValidation"] = report
            if report.get("candidateId") not in {None, candidate_id}:
                raise EvolvableGenomeError("native validator returned a mismatched candidate ID")
        return result

    def _profile(self, genome: EvolvableModuleGenomeV1) -> dict[str, Any]:
        nodes = {node.node_id: node for node in genome.nodes}; hub = next(node for node in nodes.values() if node.zone is Zone.POSITION)
        pool = {kind: genome.resources.mapping(kind) for kind in ResourceKind}
        # ``ownerSide``/``side`` are genotype ownership constraints, not
        # Dashboard v2 profile fields.  They must remain in the frozen genome
        # for cross-side mutation validation, while the compiler deliberately
        # emits only the native resource schema.
        def published_resource(row: Mapping[str, Any]) -> dict[str, Any]:
            return _clone(
                {key: value for key, value in row.items() if key not in {"ownerSide", "side"}},
                name="compiled native resource",
            )
        # The exit execution-status state is meaningful only when the genome
        # actually contains an authored exit region.  Emitting it unconditionally
        # creates an unreachable state in otherwise valid entry/management-only
        # genomes, which native v2 correctly rejects.
        has_exit_regions = any(node.zone is Zone.EXIT for node in nodes.values())
        states: list[dict[str, Any]] = [{"id": "entry_pending"}, {"id": "position_hub"}]
        if has_exit_regions:
            states.append({"id": "exit_pending"})
        node_state: dict[str, str] = {}
        for node in sorted(nodes.values(), key=lambda item: item.node_id):
            if node.node_id == hub.node_id or (node.zone is Zone.ENTRY and node.kind == "entry") or node.zone is Zone.EXIT:
                continue
            state = f"n_{node.node_id}"; node_state[node.node_id] = state; states.append({"id": state})
        transitions: list[dict[str, Any]] = []
        def emit(identifier: str, source: str, destination: str, *, priority: int, guard: Mapping[str, Any], actions: Sequence[Mapping[str, Any]], reason: str, event_class: str = "decision") -> None:
            transitions.append({"id": identifier, "sourceStateId": source, "destinationStateId": destination, "eventClass": event_class, "priority": priority, "guard": _clone(dict(guard), name="compiled guard"), "actions": [_clone(dict(action), name="compiled action") for action in actions], "reasonCode": reason})
        start = next(node for node in nodes.values() if node.zone is Zone.ENTRY and node.kind == "start")
        start_state = node_state[start.node_id]
        management = sorted((node for node in nodes.values() if node.zone is Zone.MANAGEMENT), key=lambda node: node.node_id)
        recovery = sorted((node for node in nodes.values() if node.zone is Zone.RECOVERY), key=lambda node: node.node_id)
        for edge in sorted(genome.edges, key=lambda item: item.edge_id):
            source, target = nodes[edge.source_id], nodes[edge.target_id]
            # The entry -> hub link is a semantic model edge.  The executable
            # entry result is the shared entry_pending execution-status route.
            if target.node_id == hub.node_id:
                continue
            source_state = "position_hub" if source.node_id == hub.node_id else node_state[source.node_id]
            guard = _guard_all(source.guard, edge.guard, target.guard)
            if target.zone is Zone.ENTRY and target.kind == "entry":
                plan = next((use.resource_id for use in target.resources if use.kind is ResourceKind.MANAGEMENT_REF), None)
                if plan is None: raise EvolvableGenomeError("entry node requires one management reference")
                emit(f"e_{edge.edge_id}", source_state, "entry_pending", priority=edge.priority, guard=_guard_all({"kind": "position_exists", "expected": False}, guard), actions=[{"kind": EffectKind.ENTER.value, "managementPlanId": plan}], reason="evolvable.entry.request")
            elif target.zone is Zone.SETUP:
                emit(f"e_{edge.edge_id}", source_state, node_state[target.node_id], priority=edge.priority, guard=guard, actions=[], reason="evolvable.setup.advance")
            elif target.zone is Zone.MANAGEMENT:
                pending = node_state[target.node_id]
                action = self._management_action(edge.effect, target)
                emit(f"e_{edge.edge_id}", "position_hub", pending, priority=edge.priority, guard=_guard_all({"kind": "position_exists", "expected": True}, guard), actions=[action], reason="evolvable.management.request")
                for status, priority in (("applied", 10), ("rejected", 20), ("canceled", 30)):
                    emit(f"e_{edge.edge_id}_{status}", pending, "position_hub", priority=priority, guard={"kind": "execution_status_is", "status": status}, actions=[], reason=f"evolvable.management.{status}", event_class="execution")
                emit(f"e_{edge.edge_id}_closed", pending, self._recovery_state(recovery, node_state, start_state), priority=40, guard={"kind": "execution_status_is", "status": "closed"}, actions=[], reason="evolvable.management.closed", event_class="execution")
            elif target.zone is Zone.EXIT:
                emit(f"e_{edge.edge_id}", "position_hub", "exit_pending", priority=edge.priority, guard=_guard_all({"kind": "position_exists", "expected": True}, guard), actions=[{"kind": EffectKind.EXIT.value}], reason="evolvable.exit.request")
            elif target.zone is Zone.RECOVERY:
                emit(
                    f"e_{edge.edge_id}",
                    source_state,
                    node_state[target.node_id],
                    priority=edge.priority,
                    guard=guard,
                    actions=[],
                    reason="evolvable.pre_position_recovery",
                )
        emit("entry_filled", "entry_pending", "position_hub", priority=10, guard={"kind": "execution_status_is", "status": "filled"}, actions=[], reason="evolvable.entry.filled", event_class="execution")
        emit("entry_rejected", "entry_pending", start_state, priority=20, guard={"kind": "execution_status_is", "status": "rejected"}, actions=[], reason="evolvable.entry.rejected", event_class="execution")
        emit("entry_canceled", "entry_pending", start_state, priority=30, guard={"kind": "execution_status_is", "status": "canceled"}, actions=[], reason="evolvable.entry.canceled", event_class="execution")
        recovery_state = self._recovery_state(recovery, node_state, start_state)
        emit("position_protective_closed", "position_hub", recovery_state, priority=10, guard={"kind": "execution_status_is", "status": "closed"}, actions=[], reason="position.protective_closed", event_class="execution")
        if has_exit_regions:
            emit("exit_closed", "exit_pending", recovery_state, priority=10, guard={"kind": "execution_status_is", "status": "closed"}, actions=[], reason="evolvable.exit.closed", event_class="execution")
            emit("exit_rejected", "exit_pending", "position_hub", priority=20, guard={"kind": "execution_status_is", "status": "rejected"}, actions=[], reason="evolvable.exit.rejected", event_class="execution")
            emit("exit_canceled", "exit_pending", "position_hub", priority=30, guard={"kind": "execution_status_is", "status": "canceled"}, actions=[], reason="evolvable.exit.canceled", event_class="execution")
        for index, node in enumerate(recovery):
            destination = node_state[recovery[index + 1].node_id] if index + 1 < len(recovery) else start_state
            emit(f"recovery_{index}", node_state[node.node_id], destination, priority=10, guard=_guard_all(node.guard, {"kind": "state_age_at_least", "events": node.timeout_bars}), actions=[], reason="evolvable.recovery.timeout")
        used_groups = {use.resource_id for node in nodes.values() for use in node.resources if use.kind is ResourceKind.EVIDENCE_GROUP}
        used_events = {use.resource_id for node in nodes.values() for use in node.resources if use.kind is ResourceKind.EVENT}
        # Resource closure cannot rely on the optional node ``resources``
        # annotation: raw guard references were validated against the same pool
        # above and must be emitted to Dashboard as actual graph resources.
        for label, guard in [
            *((f"node {node.node_id}", node.guard) for node in nodes.values()),
            *((f"edge {edge.edge_id}", edge.guard) for edge in genome.edges),
        ]:
            for use in _guard_resource_uses(guard, pools=pool, location=f"{label} guard"):
                if use.kind is ResourceKind.EVIDENCE_GROUP:
                    used_groups.add(use.resource_id)
                elif use.kind is ResourceKind.EVENT:
                    used_events.add(use.resource_id)
        used_indicators = {use.resource_id for node in nodes.values() for use in node.resources if use.kind is ResourceKind.INDICATOR}
        for group_id in used_groups:
            used_indicators.update(map(str, pool[ResourceKind.EVIDENCE_GROUP][group_id].get("indicatorInstanceIds") or []))
        for event_id in used_events:
            used_indicators.add(str(pool[ResourceKind.EVENT][event_id].get("indicatorInstanceId")))
        plans = [published_resource(pool[ResourceKind.MANAGEMENT_REF][use.resource_id]) for node in nodes.values() for use in node.resources if use.kind is ResourceKind.MANAGEMENT_REF]
        unique_plans = {str(plan["id"]): plan for plan in plans}
        scalar_bindings: dict[str, dict[str, Any]] = {}
        for plan in unique_plans.values():
            for binding in plan.pop("scalarBindings", []) or []:
                if not isinstance(binding, Mapping) or not isinstance(binding.get("id"), str):
                    raise EvolvableGenomeError("management scalar binding is malformed")
                scalar_bindings[str(binding["id"])] = published_resource(binding)
                used_indicators.add(str(binding.get("indicatorInstanceId") or ""))
        library = {"version": "temporal_management_v1", "defaultPlanId": sorted(unique_plans)[0], "plans": [unique_plans[identifier] for identifier in sorted(unique_plans)]}
        if scalar_bindings:
            library["scalarBindings"] = [scalar_bindings[identifier] for identifier in sorted(scalar_bindings)]
        return {"version": "v2", "name": f"evolvable v1 {genome.direction} module", "description": "AutoResearch evolvable module genotype; not an economic candidate", "instruments": [genome.instrument], "directionMode": genome.direction, "isActive": False, "indicators": [published_resource(pool[ResourceKind.INDICATOR][identifier]) for identifier in sorted(used_indicators)], "executionConfig": {"managementLibrary": library}, "graph": {"kind": "temporal_graph_v1", "semanticPolicy": "temporal_graph_semantics_v1", "eventSchema": "temporal_event_v1", "factLibrary": "temporal_market_facts_v1", "guardLibrary": "temporal_guards_v1", "actionLibrary": "temporal_market_actions_v1", "clockRequirement": "clock.completed_bar", "fidelityRequirements": ["data.completed_ohlc"], "initialStateId": start_state, "states": states, "evidenceGroups": [published_resource(pool[ResourceKind.EVIDENCE_GROUP][identifier]) for identifier in sorted(used_groups)], "eventBindings": [published_resource(pool[ResourceKind.EVENT][identifier]) for identifier in sorted(used_events)], "transitions": transitions}}

    @staticmethod
    def _recovery_state(recovery: Sequence[GenomeNodeV1], states: Mapping[str, str], fallback: str) -> str:
        return states[recovery[0].node_id] if recovery else fallback

    @staticmethod
    def _management_action(effect: EffectKind | None, node: GenomeNodeV1) -> dict[str, Any]:
        if effect is EffectKind.TIGHTEN_STOP:
            return {"kind": effect.value, "stopLocator": {"kind": "initial_r_multiple", "multiple": 0.0}}
        if effect is EffectKind.SET_TARGET:
            return {"kind": effect.value, "targetLocator": {"kind": "reward_multiple", "multiple": 1.5}}
        if effect in _MANAGEMENT_EFFECTS:
            return {"kind": effect.value}
        raise EvolvableGenomeError(f"unsupported management effect for {node.node_id}")


class EvolvableModuleGenomeCodecV1:
    """Program-kind/codec seam; future codecs register beside this class."""

    program_kind = PROGRAM_KIND
    codec = GENOME_CODEC

    def decode(self, payload: Mapping[str, Any]) -> EvolvableModuleGenomeV1:
        value = _clone(dict(payload), name="evolvable genome payload")
        if value.get("schemaVersion") != GENOME_SCHEMA or value.get("programKind") != self.program_kind or value.get("codec") != self.codec:
            raise EvolvableGenomeError("unsupported evolvable genome payload identity")
        resource = value.get("resources") or {}
        budget_raw = value.get("budget") or {}
        reverse_budget = {"max_states": budget_raw.get("maxStates"), "max_transitions": budget_raw.get("maxTransitions"), "max_evidence_groups": budget_raw.get("maxEvidenceGroups"), "max_group_members": budget_raw.get("maxGroupMembers"), "max_events": budget_raw.get("maxEvents"), "max_indicators": budget_raw.get("maxIndicators"), "max_entry_branches": budget_raw.get("maxEntryBranches"), "max_management_regions": budget_raw.get("maxManagementRegions"), "max_exit_regions": budget_raw.get("maxExitRegions"), "max_recovery_regions": budget_raw.get("maxRecoveryRegions"), "max_scc_nodes": budget_raw.get("maxSccNodes"), "max_timeout_bars": budget_raw.get("maxTimeoutBars"), "max_guard_depth": budget_raw.get("maxGuardDepth")}
        return EvolvableModuleGenomeV1(direction=value.get("direction"), resources=ResourcePoolV1(tuple(resource.get("indicators") or []), tuple(resource.get("evidenceGroups") or []), tuple(resource.get("events") or []), tuple(resource.get("managementRefs") or [])), nodes=tuple(GenomeNodeV1(node_id=item.get("id"), zone=item.get("zone"), kind=item.get("kind"), guard=item.get("guard") or {}, resources=tuple(ResourceUse(kind=use["kind"], resource_id=use["id"]) for use in item.get("resources") or []), timeout_bars=item.get("timeoutBars")) for item in value.get("nodes") or []), edges=tuple(GenomeEdgeV1(edge_id=item.get("id"), source_id=item.get("source"), target_id=item.get("target"), event_class=item.get("eventClass", "decision"), priority=item.get("priority", 10), guard=item.get("guard") or {}, effect=item.get("effect")) for item in value.get("edges") or []), budget=BudgetContractV1(**reverse_budget), program_kind=value.get("programKind"), codec=value.get("codec"), instrument=value.get("instrument", "EURUSD"))


_CODECS: dict[tuple[str, str], EvolvableModuleGenomeCodecV1] = {(PROGRAM_KIND, GENOME_CODEC): EvolvableModuleGenomeCodecV1()}


def decode_program(*, program_kind: str, codec: str, payload: Mapping[str, Any]) -> EvolvableModuleGenomeV1:
    try:
        return _CODECS[(program_kind, codec)].decode(payload)
    except KeyError as exc:
        raise EvolvableGenomeError("no codec registered for program kind") from exc


def compile_program(*, program_kind: str, codec: str, payload: Mapping[str, Any], candidate_id: str, compiler: EvolvableModuleCompilerV1 | None = None, native_validator: NativeModuleValidator | None = None) -> dict[str, Any]:
    genome = decode_program(program_kind=program_kind, codec=codec, payload=payload)
    return (compiler or EvolvableModuleCompilerV1()).compile(genome, candidate_id=candidate_id, native_validator=native_validator)


def evolvable_resource_fingerprint(genome: EvolvableModuleGenomeV1) -> str:
    """Canonical strategy-resource projection used by factory admission audits."""

    resources = genome.resources.canonical()
    management_effects = {
        EffectKind.BREAK_EVEN,
        EffectKind.TIGHTEN_STOP,
        EffectKind.ACTIVATE_TRAILING,
        EffectKind.DEACTIVATE_TRAILING,
        EffectKind.SET_TARGET,
        EffectKind.CANCEL_TARGET,
    }
    return canonical_sha256({
        "indicators": [
            (
                (row.get("meta") or {}).get("id"),
                (row.get("config") or {}).get("timeframe"),
            )
            for row in resources["indicators"]
        ],
        "groups": [
            row.get("indicatorInstanceIds")
            for row in resources["evidenceGroups"]
        ],
        "events": [
            row.get("indicatorInstanceId")
            for row in resources["events"]
        ],
        "management": [
            edge.effect.value
            for edge in genome.edges
            if edge.effect in management_effects
        ],
        "exits": sum(1 for edge in genome.edges if edge.effect is EffectKind.EXIT),
    })


def _guard_shape(value: Mapping[str, Any]) -> Any:
    kind = str(value.get("kind") or "")
    if kind in {"all", "any"}:
        return (kind, tuple(sorted((_guard_shape(item) for item in value.get("guards") or [] if isinstance(item, Mapping)), key=repr)))
    if kind in {"predicate_edge", "consecutive_true"}:
        return (kind, _guard_shape(value.get("predicate") or {}))
    return kind


__all__ = [
    "BudgetContractV1", "COMPILER_POLICY_SCHEMA", "CompilerPolicyIdentityV1", "EffectKind",
    "EvolvableGenomeError", "EvolvableModuleCompilerV1", "EvolvableModuleGenomeCodecV1",
    "EvolvableModuleGenomeV1", "GENOME_CODEC", "GENOME_SCHEMA", "GenomeEdgeV1", "GenomeNodeV1",
    "NativeModuleValidator", "PROGRAM_KIND", "ResourceKind", "ResourcePoolV1", "ResourceUse", "Zone",
    "compile_program", "decode_program", "evolvable_resource_fingerprint",
]
