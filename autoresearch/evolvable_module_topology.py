"""Deterministic topology operators for :mod:`evolvable_module_genome`.

This module owns *plans*, not a live mutation policy.  A plan is content bound
to one exact parent genome and is intentionally rejected when replayed against
another parent.  This is the small, auditable seam between a future search
operator and the typed genome compiler: all generated children are validated
and compiled before they are returned.

The v1 grammar deliberately has one shared position hub.  Consequently a
"rewire" of a management or exit region can only change its hub dispatch
attributes (guard/priority); it cannot introduce a second position flow.  A
re-arm is represented by a bounded recovery state, whose canonical compiler
route returns to the entry start after its timeout.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .evolvable_module_genome import (
    EffectKind,
    EvolvableGenomeError,
    EvolvableModuleCompilerV1,
    EvolvableModuleGenomeV1,
    GenomeEdgeV1,
    GenomeNodeV1,
    ResourceKind,
    ResourceUse,
    Zone,
    _guard_resource_uses,
)
from .temporal_search import canonical_sha256


TOPOLOGY_OPERATOR_SCHEMA = "evolvable_module_topology_operator_v1"
TOPOLOGY_PLAN_SCHEMA = "evolvable_module_topology_plan_v1"
TOPOLOGY_DELTA_SCHEMA = "evolvable_module_topology_delta_v1"
CROSSOVER_SCHEMA = "evolvable_module_motif_crossover_v1"

_MANAGEMENT = frozenset(
    {
        EffectKind.BREAK_EVEN,
        EffectKind.TIGHTEN_STOP,
        EffectKind.SET_TARGET,
        EffectKind.CANCEL_TARGET,
        EffectKind.ACTIVATE_TRAILING,
        EffectKind.DEACTIVATE_TRAILING,
    }
)


def _json(value: Any, *, field: str) -> Any:
    """Return finite canonical JSON, keeping plans data-only and reproducible."""

    try:
        import json

        return json.loads(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise EvolvableGenomeError(f"{field} must be finite canonical JSON") from exc


def _token(value: Any, *, field: str) -> str:
    token = str(value or "").strip()
    if not token or len(token) > 240:
        raise EvolvableGenomeError(f"{field} must be a nonempty explicit identifier")
    return token


def _as_effect(value: EffectKind | str | None, *, field: str) -> EffectKind | None:
    try:
        return None if value is None else EffectKind(value)
    except ValueError as exc:
        raise EvolvableGenomeError(f"{field} is not a supported effect") from exc


def _resource_use(value: Mapping[str, Any]) -> ResourceUse:
    """Accept the core's canonical ``{kind,id}`` form, never an implicit ID."""

    if not isinstance(value, Mapping):
        raise EvolvableGenomeError("planned resource use must be an object")
    return ResourceUse(
        kind=ResourceKind(value.get("kind")),
        resource_id=_token(value.get("id", value.get("resource_id")), field="planned resource ID"),
    )


def _genome(
    parent: EvolvableModuleGenomeV1,
    *,
    nodes: Sequence[GenomeNodeV1] | None = None,
    edges: Sequence[GenomeEdgeV1] | None = None,
) -> EvolvableModuleGenomeV1:
    return EvolvableModuleGenomeV1(
        direction=parent.direction,
        resources=parent.resources,
        nodes=tuple(parent.nodes if nodes is None else nodes),
        edges=tuple(parent.edges if edges is None else edges),
        budget=parent.budget,
        program_kind=parent.program_kind,
        codec=parent.codec,
        instrument=parent.instrument,
    )


def _node_map(genome: EvolvableModuleGenomeV1) -> dict[str, GenomeNodeV1]:
    return {node.node_id: node for node in genome.nodes}


def _edge_map(genome: EvolvableModuleGenomeV1) -> dict[str, GenomeEdgeV1]:
    return {edge.edge_id: edge for edge in genome.edges}


def _edge_with(
    edge: GenomeEdgeV1,
    *,
    edge_id: str | None = None,
    source_id: str | None = None,
    target_id: str | None = None,
    priority: int | None = None,
    guard: Mapping[str, Any] | None = None,
    effect: EffectKind | None | object = ...,  # ``None`` is a legal replacement.
) -> GenomeEdgeV1:
    return GenomeEdgeV1(
        edge_id=edge.edge_id if edge_id is None else edge_id,
        source_id=edge.source_id if source_id is None else source_id,
        target_id=edge.target_id if target_id is None else target_id,
        event_class=edge.event_class,
        priority=edge.priority if priority is None else priority,
        guard=edge.guard if guard is None else guard,
        effect=edge.effect if effect is ... else effect,  # type: ignore[arg-type]
    )


def _unique_priority(
    genome: EvolvableModuleGenomeV1,
    *,
    source_id: str,
    priority: int,
    except_edge_id: str | None = None,
) -> None:
    for edge in genome.edges:
        if edge.edge_id != except_edge_id and edge.source_id == source_id and edge.event_class == "decision" and edge.priority == priority:
            raise EvolvableGenomeError("operator priority conflicts at one source/event class")


def _id(plan: "TopologyPlanV1", role: str) -> str:
    # A short, stable suffix stays within the core's explicit-ID constraints.
    return f"{role}_{plan.identity_sha256.split(':', 1)[-1][:16]}"


def _compile_or_raise(genome: EvolvableModuleGenomeV1, *, candidate_id: str) -> None:
    """The core validation is authoritative; compiling catches generated caps too."""

    genome.validate()
    EvolvableModuleCompilerV1().compile(genome, candidate_id=candidate_id)


@dataclass(frozen=True)
class TopologyPlanV1:
    """One exact, immutable topology mutation request.

    ``arguments`` contains only declarative values.  It never includes a
    generated child ID, which avoids an identity circularity: application
    derives IDs from this plan's content hash.
    """

    operation: str
    source_genome_sha256: str
    arguments: Mapping[str, Any]
    schema_version: str = TOPOLOGY_PLAN_SCHEMA

    def __post_init__(self) -> None:
        object.__setattr__(self, "operation", _token(self.operation, field="operation"))
        source = _token(self.source_genome_sha256, field="source genome SHA-256")
        if not source.startswith("sha256:"):
            raise EvolvableGenomeError("source genome SHA-256 must be canonical")
        object.__setattr__(self, "source_genome_sha256", source)
        object.__setattr__(self, "arguments", _json(dict(self.arguments), field="plan arguments"))
        if self.schema_version != TOPOLOGY_PLAN_SCHEMA:
            raise EvolvableGenomeError("unsupported topology plan schema")

    def canonical(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "operatorSchema": TOPOLOGY_OPERATOR_SCHEMA,
            "operation": self.operation,
            "sourceGenomeSha256": self.source_genome_sha256,
            "arguments": _json(self.arguments, field="plan arguments"),
        }

    @property
    def identity_sha256(self) -> str:
        return canonical_sha256(self.canonical())


@dataclass(frozen=True)
class TopologySemanticDeltaV1:
    operation: str
    plan_sha256: str
    before_genome_sha256: str
    after_genome_sha256: str
    before_topology_sha256: str
    after_topology_sha256: str
    added_nodes: tuple[str, ...] = ()
    removed_nodes: tuple[str, ...] = ()
    added_edges: tuple[str, ...] = ()
    removed_edges: tuple[str, ...] = ()
    changed_edges: tuple[str, ...] = ()

    def canonical(self) -> dict[str, Any]:
        return {
            "schemaVersion": TOPOLOGY_DELTA_SCHEMA,
            "operation": self.operation,
            "planSha256": self.plan_sha256,
            "beforeGenomeSha256": self.before_genome_sha256,
            "afterGenomeSha256": self.after_genome_sha256,
            "beforeTopologySha256": self.before_topology_sha256,
            "afterTopologySha256": self.after_topology_sha256,
            "addedNodes": list(self.added_nodes),
            "removedNodes": list(self.removed_nodes),
            "addedEdges": list(self.added_edges),
            "removedEdges": list(self.removed_edges),
            "changedEdges": list(self.changed_edges),
        }


@dataclass(frozen=True)
class TopologyApplicationV1:
    plan: TopologyPlanV1
    genome: EvolvableModuleGenomeV1
    delta: TopologySemanticDeltaV1


def make_plan(genome: EvolvableModuleGenomeV1, *, operation: str, **arguments: Any) -> TopologyPlanV1:
    """Construct a content-bound plan.  Planning never changes a genome."""

    _compile_or_raise(genome, candidate_id="topology-plan-source")
    return TopologyPlanV1(operation=operation, source_genome_sha256=genome.identity_sha256, arguments=arguments)


def apply_plan(genome: EvolvableModuleGenomeV1, plan: TopologyPlanV1) -> TopologyApplicationV1:
    """Apply exactly once to the parent content the plan names; fail closed otherwise."""

    if genome.identity_sha256 != plan.source_genome_sha256:
        raise EvolvableGenomeError("stale topology plan does not bind this exact parent genome")
    handlers = {
        "insert_setup": _insert_setup,
        "remove_setup": _remove_setup,
        "rewire_entry_branch": _rewire_entry_branch,
        "insert_entry_branch": _insert_entry_branch,
        "remove_entry_branch": _remove_entry_branch,
        "insert_confirmation_rejection": _insert_confirmation_rejection,
        "insert_timeout_rearm": _insert_timeout_rearm,
        "remove_timeout_rearm": _remove_timeout_rearm,
        "insert_management_region": _insert_management_region,
        "remove_management_region": _remove_region,
        "rewire_management_region": _rewire_region,
        "insert_exit_region": _insert_exit_region,
        "remove_exit_region": _remove_region,
        "rewire_exit_region": _rewire_region,
    }
    try:
        child = handlers[plan.operation](genome, plan)
    except KeyError as exc:
        raise EvolvableGenomeError("unsupported topology operation") from exc
    _compile_or_raise(child, candidate_id=f"topology-{plan.identity_sha256[-12:]}")
    delta = _delta(genome, child, plan)
    return TopologyApplicationV1(plan=plan, genome=child, delta=delta)


def _delta(before: EvolvableModuleGenomeV1, after: EvolvableModuleGenomeV1, plan: TopologyPlanV1) -> TopologySemanticDeltaV1:
    before_nodes, after_nodes = _node_map(before), _node_map(after)
    before_edges, after_edges = _edge_map(before), _edge_map(after)
    return TopologySemanticDeltaV1(
        operation=plan.operation,
        plan_sha256=plan.identity_sha256,
        before_genome_sha256=before.identity_sha256,
        after_genome_sha256=after.identity_sha256,
        before_topology_sha256=before.semantic_topology_signature(),
        after_topology_sha256=after.semantic_topology_signature(),
        added_nodes=tuple(sorted(set(after_nodes) - set(before_nodes))),
        removed_nodes=tuple(sorted(set(before_nodes) - set(after_nodes))),
        added_edges=tuple(sorted(set(after_edges) - set(before_edges))),
        removed_edges=tuple(sorted(set(before_edges) - set(after_edges))),
        changed_edges=tuple(sorted(identifier for identifier in set(before_edges) & set(after_edges) if before_edges[identifier].canonical() != after_edges[identifier].canonical())),
    )


def _args(plan: TopologyPlanV1) -> Mapping[str, Any]:
    return plan.arguments


def _setup_source(nodes: Mapping[str, GenomeNodeV1], identifier: Any) -> GenomeNodeV1:
    node = nodes.get(_token(identifier, field="source node ID"))
    if node is None or node.zone not in {Zone.ENTRY, Zone.SETUP} or (
        node.zone is Zone.ENTRY and node.kind != "start"
    ):
        raise EvolvableGenomeError("entry/setup operator source must be an entry or setup node")
    return node


def _entry_node(nodes: Mapping[str, GenomeNodeV1], identifier: Any) -> GenomeNodeV1:
    node = nodes.get(_token(identifier, field="entry node ID"))
    if node is None or node.zone is not Zone.ENTRY or node.kind != "entry":
        raise EvolvableGenomeError("operation requires an entry node")
    return node


def _management_ref(node: GenomeNodeV1) -> ResourceUse:
    refs = [use for use in node.resources if use.kind is ResourceKind.MANAGEMENT_REF]
    if len(refs) != 1:
        raise EvolvableGenomeError("entry motif requires exactly one management reference")
    return refs[0]


def _semantic_hub_edge(genome: EvolvableModuleGenomeV1, entry_id: str) -> GenomeEdgeV1:
    nodes = _node_map(genome)
    matches = [edge for edge in genome.edges if edge.source_id == entry_id and nodes[edge.target_id].zone is Zone.POSITION]
    if len(matches) != 1:
        raise EvolvableGenomeError("entry motif requires exactly one semantic position-hub connector")
    return matches[0]


def _insert_setup(genome: EvolvableModuleGenomeV1, plan: TopologyPlanV1) -> EvolvableModuleGenomeV1:
    args, edges = _args(plan), _edge_map(genome)
    edge = edges.get(_token(args.get("edgeId"), field="edge ID"))
    nodes = _node_map(genome)
    if edge is None or nodes[edge.source_id].zone not in {Zone.ENTRY, Zone.SETUP} or nodes[edge.target_id].zone not in {Zone.ENTRY, Zone.SETUP}:
        raise EvolvableGenomeError("setup insertion requires a pre-position edge")
    if edge.effect is not None and not (nodes[edge.target_id].zone is Zone.ENTRY and nodes[edge.target_id].kind == "entry"):
        raise EvolvableGenomeError("setup insertion cannot preserve an unsupported effect")
    node_id, edge_id = _id(plan, "setup"), _id(plan, "setup_in")
    if node_id in nodes or edge_id in edges:
        raise EvolvableGenomeError("deterministic topology ID collision")
    node = GenomeNodeV1(node_id, Zone.SETUP, _token(args.get("kind", "setup"), field="setup kind"), _json(args.get("guard") or {}, field="setup guard"), tuple(_resource_use(value) for value in args.get("resources") or ()), args.get("timeoutBars"))
    # Keep the original guard on exactly one half of the split.  Carrying it
    # on both edges is usually logically equivalent, but destroys exact
    # semantic accounting and can alter trace attribution.
    first = _edge_with(edge, edge_id=edge_id, target_id=node_id, effect=None)
    second = _edge_with(edge, source_id=node_id, guard={})
    return _genome(genome, nodes=(*genome.nodes, node), edges=tuple(item for item in genome.edges if item.edge_id != edge.edge_id) + (first, second))


def _remove_setup(genome: EvolvableModuleGenomeV1, plan: TopologyPlanV1) -> EvolvableModuleGenomeV1:
    nodes, args = _node_map(genome), _args(plan)
    node_id = _token(args.get("nodeId"), field="setup node ID")
    node = nodes.get(node_id)
    if node is None or node.zone is not Zone.SETUP:
        raise EvolvableGenomeError("operation requires a setup node")
    incoming = [edge for edge in genome.edges if edge.target_id == node_id]
    outgoing = [edge for edge in genome.edges if edge.source_id == node_id]
    if len(incoming) != 1 or len(outgoing) != 1:
        raise EvolvableGenomeError("only a linear setup motif may be removed")
    before, after = incoming[0], outgoing[0]
    if before.effect is not None:
        raise EvolvableGenomeError("setup motif has an invalid incoming effect")
    _unique_priority(genome, source_id=before.source_id, priority=after.priority, except_edge_id=before.edge_id)
    # Preserve the downstream edge's identity: insertion retained it for the
    # second half of the split, so removing the deterministic node restores
    # the exact original graph rather than merely an isomorphic graph.
    replacement = _edge_with(after, edge_id=after.edge_id, source_id=before.source_id, guard=before.guard)
    return _genome(genome, nodes=tuple(item for item in genome.nodes if item.node_id != node_id), edges=tuple(item for item in genome.edges if item.edge_id not in {before.edge_id, after.edge_id}) + (replacement,))


def _rewire_entry_branch(genome: EvolvableModuleGenomeV1, plan: TopologyPlanV1) -> EvolvableModuleGenomeV1:
    args, nodes, edges = _args(plan), _node_map(genome), _edge_map(genome)
    edge = edges.get(_token(args.get("edgeId"), field="entry branch edge ID"))
    if edge is None or nodes[edge.target_id].zone is not Zone.ENTRY or nodes[edge.target_id].kind != "entry" or edge.effect is not EffectKind.ENTER:
        raise EvolvableGenomeError("operation requires an enter branch")
    source = _setup_source(nodes, args.get("sourceId"))
    _unique_priority(genome, source_id=source.node_id, priority=int(args.get("priority", edge.priority)), except_edge_id=edge.edge_id)
    replacement = _edge_with(edge, source_id=source.node_id, priority=int(args.get("priority", edge.priority)), guard=_json(args.get("guard", edge.guard), field="entry branch guard"))
    return _genome(genome, edges=tuple(replacement if item.edge_id == edge.edge_id else item for item in genome.edges))


def _insert_entry_branch(genome: EvolvableModuleGenomeV1, plan: TopologyPlanV1) -> EvolvableModuleGenomeV1:
    args, nodes = _args(plan), _node_map(genome)
    source = _setup_source(nodes, args.get("sourceId"))
    priority = int(args.get("priority"))
    _unique_priority(genome, source_id=source.node_id, priority=priority)
    ref = ResourceUse(ResourceKind.MANAGEMENT_REF, _token(args.get("managementRefId"), field="management reference ID"))
    if ref.resource_id not in genome.resources.mapping(ResourceKind.MANAGEMENT_REF):
        raise EvolvableGenomeError("entry branch references an unknown management plan")
    node_id, branch_id, hub_id = _id(plan, "entry"), _id(plan, "entry_branch"), _id(plan, "entry_hub")
    node = GenomeNodeV1(node_id, Zone.ENTRY, "entry", resources=(ref,))
    hub = next(node for node in genome.nodes if node.zone is Zone.POSITION)
    branch = GenomeEdgeV1(branch_id, source.node_id, node_id, priority=priority, guard=_json(args.get("guard") or {}, field="entry branch guard"), effect=EffectKind.ENTER)
    connector = GenomeEdgeV1(hub_id, node_id, hub.node_id, priority=int(args.get("hubPriority", 10)))
    return _genome(genome, nodes=(*genome.nodes, node), edges=(*genome.edges, branch, connector))


def _remove_entry_branch(genome: EvolvableModuleGenomeV1, plan: TopologyPlanV1) -> EvolvableModuleGenomeV1:
    nodes, args = _node_map(genome), _args(plan)
    entry = _entry_node(nodes, args.get("nodeId"))
    incoming = [edge for edge in genome.edges if edge.target_id == entry.node_id]
    connector = _semantic_hub_edge(genome, entry.node_id)
    if len(incoming) != 1 or incoming[0].effect is not EffectKind.ENTER:
        raise EvolvableGenomeError("only a single-port entry branch may be removed")
    branches = [edge for edge in genome.edges if nodes[edge.target_id].zone is Zone.ENTRY and nodes[edge.target_id].kind == "entry"]
    if len(branches) <= 1:
        raise EvolvableGenomeError("cannot remove the final entry branch")
    ref = _management_ref(entry)
    if sum(ref in node.resources for node in genome.nodes if node.node_id != entry.node_id) == 0:
        raise EvolvableGenomeError("cannot orphan a management plan while removing an entry branch")
    gone = {incoming[0].edge_id, connector.edge_id}
    return _genome(genome, nodes=tuple(node for node in genome.nodes if node.node_id != entry.node_id), edges=tuple(edge for edge in genome.edges if edge.edge_id not in gone))


def _insert_confirmation_rejection(genome: EvolvableModuleGenomeV1, plan: TopologyPlanV1) -> EvolvableModuleGenomeV1:
    """Replace one entry branch with explicit confirm/reject setup ports.

    Confirmation remains a normal next-open entry effect.  Rejection is a
    genuinely side-effect-free pre-position route to a bounded recovery state,
    whose compiler-owned timeout then re-arms the entry start.
    """

    args, nodes, edges = _args(plan), _node_map(genome), _edge_map(genome)
    branch = edges.get(_token(args.get("edgeId"), field="entry branch edge ID"))
    if branch is None or branch.effect is not EffectKind.ENTER:
        raise EvolvableGenomeError("confirmation motif requires an enter branch")
    source, entry = nodes[branch.source_id], nodes[branch.target_id]
    if source.zone not in {Zone.ENTRY, Zone.SETUP} or entry.zone is not Zone.ENTRY or entry.kind != "entry":
        raise EvolvableGenomeError("confirmation motif requires a pre-position entry branch")
    reject_priority = int(args.get("rejectPriority"))
    rejection_timeout = int(args.get("rejectionTimeoutBars"))
    if rejection_timeout < 1:
        raise EvolvableGenomeError("rejection timeout must be positive")
    _unique_priority(genome, source_id=source.node_id, priority=reject_priority)
    confirm_id, reject_id = _id(plan, "confirm"), _id(plan, "rejected_rearm")
    confirm_edge_id, reject_edge_id = _id(plan, "confirm_path"), _id(plan, "reject_path")
    confirm = GenomeNodeV1(confirm_id, Zone.SETUP, "confirmation", _json(args.get("confirmGuard") or {}, field="confirmation guard"))
    reject = GenomeNodeV1(
        reject_id,
        Zone.RECOVERY,
        "rejection_rearm",
        _json(args.get("rejectGuard") or {}, field="rejection guard"),
        timeout_bars=rejection_timeout,
    )
    # Reuse the original edge's priority/guard for the confirm selection. The
    # original entry node and its hub connector stay valid and reachable.
    source_confirm = _edge_with(branch, target_id=confirm_id, effect=None)
    source_reject = GenomeEdgeV1(reject_edge_id, source.node_id, reject_id, priority=reject_priority, guard=_json(args.get("sourceRejectGuard") or {}, field="source rejection guard"))
    confirm_enter = GenomeEdgeV1(confirm_edge_id, confirm_id, entry.node_id, priority=10, guard={}, effect=EffectKind.ENTER)
    return _genome(
        genome,
        nodes=(*genome.nodes, confirm, reject),
        edges=tuple(edge for edge in genome.edges if edge.edge_id != branch.edge_id)
        + (source_confirm, source_reject, confirm_enter),
    )


def _insert_timeout_rearm(genome: EvolvableModuleGenomeV1, plan: TopologyPlanV1) -> EvolvableModuleGenomeV1:
    args = _args(plan)
    timeout = int(args.get("timeoutBars"))
    if timeout < 1:
        raise EvolvableGenomeError("re-arm timeout must be positive")
    node = GenomeNodeV1(_id(plan, "rearm"), Zone.RECOVERY, "bounded_rearm", _json(args.get("guard") or {}, field="re-arm guard"), timeout_bars=timeout)
    return _genome(genome, nodes=(*genome.nodes, node))


def _remove_timeout_rearm(genome: EvolvableModuleGenomeV1, plan: TopologyPlanV1) -> EvolvableModuleGenomeV1:
    nodes, args = _node_map(genome), _args(plan)
    node_id = _token(args.get("nodeId"), field="re-arm node ID")
    node = nodes.get(node_id)
    if node is None or node.zone is not Zone.RECOVERY or node.kind != "bounded_rearm":
        raise EvolvableGenomeError("operation requires a bounded re-arm recovery node")
    return _genome(genome, nodes=tuple(item for item in genome.nodes if item.node_id != node_id))


def _insert_management_region(genome: EvolvableModuleGenomeV1, plan: TopologyPlanV1) -> EvolvableModuleGenomeV1:
    args = _args(plan)
    effect = _as_effect(args.get("effect"), field="management effect")
    if effect not in _MANAGEMENT:
        raise EvolvableGenomeError("management insertion requires a management effect")
    hub = next(node for node in genome.nodes if node.zone is Zone.POSITION)
    priority = int(args.get("priority"))
    _unique_priority(genome, source_id=hub.node_id, priority=priority)
    node = GenomeNodeV1(_id(plan, "management"), Zone.MANAGEMENT, _token(args.get("kind", effect.value), field="management kind"), _json(args.get("nodeGuard") or {}, field="management node guard"))
    edge = GenomeEdgeV1(_id(plan, "management_dispatch"), hub.node_id, node.node_id, priority=priority, guard=_json(args.get("guard") or {}, field="management dispatch guard"), effect=effect)
    return _genome(genome, nodes=(*genome.nodes, node), edges=(*genome.edges, edge))


def _insert_exit_region(genome: EvolvableModuleGenomeV1, plan: TopologyPlanV1) -> EvolvableModuleGenomeV1:
    args = _args(plan)
    hub = next(node for node in genome.nodes if node.zone is Zone.POSITION)
    priority = int(args.get("priority"))
    _unique_priority(genome, source_id=hub.node_id, priority=priority)
    node = GenomeNodeV1(_id(plan, "exit"), Zone.EXIT, _token(args.get("kind", "exit"), field="exit kind"), _json(args.get("nodeGuard") or {}, field="exit node guard"))
    edge = GenomeEdgeV1(_id(plan, "exit_dispatch"), hub.node_id, node.node_id, priority=priority, guard=_json(args.get("guard") or {}, field="exit dispatch guard"), effect=EffectKind.EXIT)
    return _genome(genome, nodes=(*genome.nodes, node), edges=(*genome.edges, edge))


def _region_target(genome: EvolvableModuleGenomeV1, node_id: str, *, zone: Zone) -> tuple[GenomeNodeV1, GenomeEdgeV1]:
    nodes = _node_map(genome); node = nodes.get(node_id)
    if node is None or node.zone is not zone:
        raise EvolvableGenomeError(f"operation requires a {zone.value} region")
    incoming = [edge for edge in genome.edges if edge.target_id == node_id]
    if len(incoming) != 1 or nodes[incoming[0].source_id].zone is not Zone.POSITION:
        raise EvolvableGenomeError("region must have exactly one shared-hub dispatch")
    return node, incoming[0]


def _remove_region(genome: EvolvableModuleGenomeV1, plan: TopologyPlanV1) -> EvolvableModuleGenomeV1:
    args = _args(plan); zone = Zone.MANAGEMENT if plan.operation == "remove_management_region" else Zone.EXIT
    node, edge = _region_target(genome, _token(args.get("nodeId"), field="region node ID"), zone=zone)
    return _genome(genome, nodes=tuple(item for item in genome.nodes if item.node_id != node.node_id), edges=tuple(item for item in genome.edges if item.edge_id != edge.edge_id))


def _rewire_region(genome: EvolvableModuleGenomeV1, plan: TopologyPlanV1) -> EvolvableModuleGenomeV1:
    args = _args(plan); zone = Zone.MANAGEMENT if plan.operation == "rewire_management_region" else Zone.EXIT
    node, edge = _region_target(genome, _token(args.get("nodeId"), field="region node ID"), zone=zone)
    priority = int(args.get("priority", edge.priority))
    _unique_priority(genome, source_id=edge.source_id, priority=priority, except_edge_id=edge.edge_id)
    effect = edge.effect
    if zone is Zone.MANAGEMENT and "effect" in args:
        effect = _as_effect(args["effect"], field="management effect")
        if effect not in _MANAGEMENT:
            raise EvolvableGenomeError("rewired management region requires a management effect")
    replacement = _edge_with(edge, priority=priority, guard=_json(args.get("guard", edge.guard), field="rewired region guard"), effect=effect)
    replacement_node = GenomeNodeV1(node.node_id, node.zone, _token(args.get("kind", node.kind), field="region kind"), _json(args.get("nodeGuard", node.guard), field="rewired region node guard"), node.resources, node.timeout_bars)
    return _genome(genome, nodes=tuple(replacement_node if item.node_id == node.node_id else item for item in genome.nodes), edges=tuple(replacement if item.edge_id == edge.edge_id else item for item in genome.edges))


def _motif_ports(genome: EvolvableModuleGenomeV1) -> dict[str, tuple[str, ...]]:
    """Return typed ports that can be safely exchanged under v1 semantics."""

    nodes = _node_map(genome)
    result: dict[str, list[str]] = defaultdict(list)
    for edge in genome.edges:
        target = nodes[edge.target_id]
        if target.zone is Zone.MANAGEMENT:
            result["management_hub"].append(edge.edge_id)
        elif target.zone is Zone.EXIT:
            result["exit_hub"].append(edge.edge_id)
        elif target.zone is Zone.SETUP:
            incoming = [item for item in genome.edges if item.target_id == target.node_id]
            outgoing = [item for item in genome.edges if item.source_id == target.node_id]
            # Entry/setup crossover exchanges one linear, pre-position segment
            # (incoming dispatch + setup node + outgoing continuation). It
            # never clones an arbitrary branching subgraph.
            if (
                len(incoming) == 1
                and len(outgoing) == 1
                and nodes[incoming[0].source_id].zone in {Zone.ENTRY, Zone.SETUP}
                and nodes[outgoing[0].target_id].zone in {Zone.ENTRY, Zone.SETUP}
            ):
                result["entry_setup"].append(edge.edge_id)
    return {kind: tuple(sorted(values)) for kind, values in sorted(result.items())}


def _motif_resource_closure(genome: EvolvableModuleGenomeV1, edge_id: str) -> set[ResourceUse]:
    """Typed resources a donor hub motif actually needs, including guards.

    Whole-pool equality was needlessly restrictive: independently evolved
    parents may have different unused-by-this-motif resources.  We exchange
    only a motif whose *closed* resource set already exists in the recipient;
    imports/remapping are intentionally not implicit in v1 because they could
    create a second ownership path.  This is a real compatible-port boundary,
    not an identity-only exemption.
    """

    edge = _edge_map(genome).get(edge_id)
    if edge is None:
        raise EvolvableGenomeError("motif closure names an unknown donor edge")
    nodes = _node_map(genome)
    target = nodes[edge.target_id]
    pools = {kind: genome.resources.mapping(kind) for kind in ResourceKind}
    result = set(target.resources)
    result.update(_guard_resource_uses(target.guard, pools=pools, location="donor motif node guard"))
    result.update(_guard_resource_uses(edge.guard, pools=pools, location="donor motif edge guard"))
    if target.zone is Zone.SETUP:
        outgoing = [item for item in genome.edges if item.source_id == target.node_id]
        if len(outgoing) != 1:
            raise EvolvableGenomeError("entry/setup donor motif is not linear")
        result.update(_guard_resource_uses(outgoing[0].guard, pools=pools, location="donor setup continuation guard"))
    expanded = set(result)
    for use in tuple(result):
        if use.kind is ResourceKind.EVIDENCE_GROUP:
            expanded.update(ResourceUse(ResourceKind.INDICATOR, str(value)) for value in pools[use.kind][use.resource_id].get("indicatorInstanceIds") or [])
        elif use.kind is ResourceKind.EVENT:
            expanded.add(ResourceUse(ResourceKind.INDICATOR, str(pools[use.kind][use.resource_id].get("indicatorInstanceId") or "")))
    return {use for use in expanded if use.resource_id}


def _recipient_can_supply(left: EvolvableModuleGenomeV1, closure: set[ResourceUse]) -> bool:
    pools = {kind: left.resources.mapping(kind) for kind in ResourceKind}
    return all(use.resource_id in pools[use.kind] for use in closure)


@dataclass(frozen=True)
class MotifCrossoverPlanV1:
    """Select donor hub motifs by typed port, binding both ordered parents."""

    left_genome_sha256: str
    right_genome_sha256: str
    segment_map: Mapping[str, Sequence[str]]
    schema_version: str = CROSSOVER_SCHEMA

    def __post_init__(self) -> None:
        for field in ("left_genome_sha256", "right_genome_sha256"):
            value = _token(getattr(self, field), field=field)
            if not value.startswith("sha256:"):
                raise EvolvableGenomeError(f"{field} must be canonical")
            object.__setattr__(self, field, value)
        object.__setattr__(self, "segment_map", _json({str(key): list(value) for key, value in self.segment_map.items()}, field="crossover segment map"))
        if self.schema_version != CROSSOVER_SCHEMA:
            raise EvolvableGenomeError("unsupported crossover schema")

    def canonical(self) -> dict[str, Any]:
        return {"schemaVersion": self.schema_version, "leftGenomeSha256": self.left_genome_sha256, "rightGenomeSha256": self.right_genome_sha256, "segmentMap": self.segment_map}

    @property
    def identity_sha256(self) -> str:
        return canonical_sha256(self.canonical())


@dataclass(frozen=True)
class MotifCrossoverApplicationV1:
    plan: MotifCrossoverPlanV1
    genome: EvolvableModuleGenomeV1
    semantic_delta: Mapping[str, Any]


def make_crossover_plan(
    left: EvolvableModuleGenomeV1,
    right: EvolvableModuleGenomeV1,
    *,
    segment_map: Mapping[str, Sequence[str]],
) -> MotifCrossoverPlanV1:
    _compile_or_raise(left, candidate_id="crossover-left")
    _compile_or_raise(right, candidate_id="crossover-right")
    if left.direction != right.direction or left.instrument != right.instrument:
        raise EvolvableGenomeError("same-side crossover requires matching direction and instrument")
    if left.budget.canonical() != right.budget.canonical():
        raise EvolvableGenomeError("crossover requires an identical budget contract")
    if not any(segment_map.values()):
        raise EvolvableGenomeError("crossover segment map must select at least one donor motif")
    left_ports, right_ports = _motif_ports(left), _motif_ports(right)
    for port, values in segment_map.items():
        if port not in left_ports or port not in right_ports:
            raise EvolvableGenomeError("crossover segment names an incompatible typed motif port")
        for edge_id in values:
            donor_edge_id = _token(edge_id, field="donor edge ID")
            if donor_edge_id not in right_ports[port]:
                raise EvolvableGenomeError("crossover segment names a nonexistent donor motif")
            if not _recipient_can_supply(left, _motif_resource_closure(right, donor_edge_id)):
                raise EvolvableGenomeError("crossover donor motif resource closure is incompatible with recipient")
    return MotifCrossoverPlanV1(left.identity_sha256, right.identity_sha256, segment_map)


def apply_crossover(
    left: EvolvableModuleGenomeV1,
    right: EvolvableModuleGenomeV1,
    plan: MotifCrossoverPlanV1,
) -> MotifCrossoverApplicationV1:
    if left.identity_sha256 != plan.left_genome_sha256 or right.identity_sha256 != plan.right_genome_sha256:
        raise EvolvableGenomeError("stale crossover plan does not bind its ordered parents")
    if left.direction != right.direction or left.instrument != right.instrument or left.budget.canonical() != right.budget.canonical():
        raise EvolvableGenomeError("crossover parents no longer satisfy typed compatibility")
    donor_ports, left_ports = _motif_ports(right), _motif_ports(left)
    for port, edge_ids in sorted(plan.segment_map.items()):
        if port not in donor_ports or port not in left_ports:
            raise EvolvableGenomeError("crossover segment names an incompatible typed motif port")
        for edge_id in edge_ids:
            edge_id = _token(edge_id, field="donor edge ID")
            if edge_id not in donor_ports[port]:
                raise EvolvableGenomeError("crossover segment names a nonexistent donor motif")
            if not _recipient_can_supply(left, _motif_resource_closure(right, edge_id)):
                raise EvolvableGenomeError("crossover donor motif resource closure is incompatible with recipient")
    # Shared-hub ports have no downstream authored edges.  Replace matching
    # left motifs by ordinal port position; donor IDs are remapped from the
    # ordered-parent plan, not copied, preventing collisions and making replay
    # exact.  A selected donor port must have an equal-sized left port family.
    left_nodes, right_nodes = _node_map(left), _node_map(right)
    nodes, edges = list(left.nodes), list(left.edges)
    replacements: list[tuple[str, str]] = []
    for port in sorted(plan.segment_map):
        chosen = list(plan.segment_map[port])
        if len(chosen) > len(left_ports[port]):
            raise EvolvableGenomeError("crossover requests more donor motifs than compatible recipient ports")
        for ordinal, donor_edge_id in enumerate(chosen):
            donor_edge = _edge_map(right)[donor_edge_id]
            recipient_edge_id = left_ports[port][ordinal]
            recipient_edge = _edge_map(left)[recipient_edge_id]
            donor_node = right_nodes[donor_edge.target_id]
            recipient_node = left_nodes[recipient_edge.target_id]
            # The port's zone/effect is the typed interface.  The new names
            # make child identity depend on ordered parents + segment map.
            if donor_node.zone is not recipient_node.zone or donor_edge.effect is not recipient_edge.effect:
                raise EvolvableGenomeError("donor motif violates its typed port contract")
            suffix = canonical_sha256({"plan": plan.identity_sha256, "port": port, "ordinal": ordinal}).split(":", 1)[-1][:12]
            new_node_id, new_edge_id = f"x_{port}_{suffix}", f"x_dispatch_{suffix}"
            new_node = GenomeNodeV1(new_node_id, donor_node.zone, donor_node.kind, donor_node.guard, donor_node.resources, donor_node.timeout_bars)
            new_edge = GenomeEdgeV1(new_edge_id, recipient_edge.source_id, new_node_id, priority=recipient_edge.priority, guard=donor_edge.guard, effect=donor_edge.effect)
            if port == "entry_setup":
                donor_outgoing = [item for item in right.edges if item.source_id == donor_node.node_id]
                recipient_outgoing = [item for item in left.edges if item.source_id == recipient_node.node_id]
                if len(donor_outgoing) != 1 or len(recipient_outgoing) != 1:
                    raise EvolvableGenomeError("entry/setup crossover requires linear donor and recipient segments")
                donor_continue, recipient_continue = donor_outgoing[0], recipient_outgoing[0]
                if donor_continue.effect is not recipient_continue.effect:
                    raise EvolvableGenomeError("entry/setup donor continuation violates typed port contract")
                continuation_id = f"x_continue_{suffix}"
                continuation = GenomeEdgeV1(
                    continuation_id,
                    new_node_id,
                    recipient_continue.target_id,
                    priority=recipient_continue.priority,
                    guard=donor_continue.guard,
                    effect=donor_continue.effect,
                )
                nodes = [item for item in nodes if item.node_id != recipient_node.node_id] + [new_node]
                edges = [
                    item
                    for item in edges
                    if item.edge_id not in {recipient_edge.edge_id, recipient_continue.edge_id}
                ] + [new_edge, continuation]
            else:
                nodes = [item for item in nodes if item.node_id != recipient_node.node_id] + [new_node]
                edges = [item for item in edges if item.edge_id != recipient_edge.edge_id] + [new_edge]
            replacements.append((recipient_edge_id, donor_edge_id))
    child = _genome(left, nodes=nodes, edges=edges)
    _compile_or_raise(child, candidate_id=f"crossover-{plan.identity_sha256[-12:]}")
    return MotifCrossoverApplicationV1(plan=plan, genome=child, semantic_delta={
        "schemaVersion": TOPOLOGY_DELTA_SCHEMA,
        "crossoverPlanSha256": plan.identity_sha256,
        "orderedParents": [left.identity_sha256, right.identity_sha256],
        "segmentMap": _json(plan.segment_map, field="crossover segment map"),
        "replacements": [{"recipientEdgeId": receiver, "donorEdgeId": donor} for receiver, donor in replacements],
        "beforeTopologySha256": left.semantic_topology_signature(),
        "afterTopologySha256": child.semantic_topology_signature(),
        "childGenomeSha256": child.identity_sha256,
    })


__all__ = [
    "CROSSOVER_SCHEMA", "MotifCrossoverApplicationV1", "MotifCrossoverPlanV1",
    "TOPOLOGY_DELTA_SCHEMA", "TOPOLOGY_OPERATOR_SCHEMA", "TOPOLOGY_PLAN_SCHEMA",
    "TopologyApplicationV1", "TopologyPlanV1", "TopologySemanticDeltaV1",
    "apply_crossover", "apply_plan", "make_crossover_plan", "make_plan",
]
