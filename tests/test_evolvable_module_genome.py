from __future__ import annotations

import copy
import subprocess
import sys
from pathlib import Path

import pytest

from autoresearch.evolvable_module_genome import (
    EffectKind,
    EvolvableGenomeError,
    EvolvableModuleCompilerV1,
    EvolvableModuleGenomeV1,
    GenomeEdgeV1,
    GenomeNodeV1,
    PROGRAM_KIND,
    GENOME_CODEC,
    ResourceKind,
    ResourcePoolV1,
    ResourceUse,
    Zone,
    compile_program,
    decode_program,
)


CORE_PYTHON = Path(r"C:\repos\Trading-Dashboard\compute-service\.venv\Scripts\python.exe")


def _use(kind: ResourceKind, identifier: str) -> ResourceUse:
    return ResourceUse(kind=kind, resource_id=identifier)


def _genome(*, renamed: bool = False) -> EvolvableModuleGenomeV1:
    suffix = "x" if renamed else ""
    resources = ResourcePoolV1(
        indicators=(
            {"meta": {"id": "I_RSI", "instanceId": "rsi"}, "config": {"isActive": True, "useFormingBar": False, "timeframe": "M5"}},
        ),
        evidence_groups=({"id": "g_rsi", "indicatorInstanceIds": ["rsi"]},),
        events=({"id": "e_rsi", "indicatorInstanceId": "rsi", "longOutput": "bullish", "shortOutput": "bearish"},),
        management_refs=({"id": "base", "initialStop": {"kind": "fixed_percent", "percent": 1.0}, "initialTarget": {"kind": "reward_multiple", "multiple": 2.0}},),
    )
    start, setup, entry, hub, manage, exit_, cooldown = (f"{name}{suffix}" for name in ("start", "setup", "entry", "hub", "manage", "exit", "cooldown"))
    nodes = (
        GenomeNodeV1(start, Zone.ENTRY, "start", {"kind": "position_exists", "expected": False}),
        GenomeNodeV1(setup, Zone.SETUP, "setup", {"kind": "all", "guards": [{"kind": "evidence_at_least", "groupId": "g_rsi", "thresholdPercent": 55.0}, {"kind": "fresh_event", "eventId": "e_rsi"}]}, (_use(ResourceKind.EVIDENCE_GROUP, "g_rsi"), _use(ResourceKind.EVENT, "e_rsi"))),
        GenomeNodeV1(entry, Zone.ENTRY, "entry", resources=(_use(ResourceKind.MANAGEMENT_REF, "base"),)),
        GenomeNodeV1(hub, Zone.POSITION, "position_hub"),
        GenomeNodeV1(manage, Zone.MANAGEMENT, "break_even", {"kind": "unrealized_r_at_least", "value": 1.0}),
        GenomeNodeV1(exit_, Zone.EXIT, "time_exit", {"kind": "position_age_at_least", "events": 8}),
        GenomeNodeV1(cooldown, Zone.RECOVERY, "cooldown", timeout_bars=3),
    )
    edges = (
        GenomeEdgeV1(f"start_setup{suffix}", start, setup, priority=10),
        GenomeEdgeV1(f"setup_entry{suffix}", setup, entry, priority=10, effect=EffectKind.ENTER),
        GenomeEdgeV1(f"entry_hub{suffix}", entry, hub, priority=10),
        GenomeEdgeV1(f"hub_manage{suffix}", hub, manage, priority=10, effect=EffectKind.BREAK_EVEN),
        GenomeEdgeV1(f"hub_exit{suffix}", hub, exit_, priority=20, effect=EffectKind.EXIT),
    )
    return EvolvableModuleGenomeV1("long", resources, nodes, edges)


class _Validator:
    def __init__(self) -> None:
        self.calls: list[tuple[dict, str]] = []

    def validate_v2(self, *, profile, candidate_id):
        self.calls.append((copy.deepcopy(profile), candidate_id))
        return {"candidateId": candidate_id, "candidateAcceptable": True, "status": "valid_evaluable"}


def test_golden_compile_uses_shared_position_hub_priority_and_native_boundary() -> None:
    validator = _Validator()
    result = EvolvableModuleCompilerV1().compile(_genome(), candidate_id="golden-evolvable", native_validator=validator)
    profile = result["profile"]
    assert result["programKind"] == PROGRAM_KIND
    assert result["codec"] == GENOME_CODEC
    assert validator.calls and validator.calls[0][1] == "golden-evolvable"
    assert profile["version"] == "v2" and profile["directionMode"] == "long"
    graph = profile["graph"]
    management = next(item for item in graph["transitions"] if item["id"] == "e_hub_manage")
    exit_ = next(item for item in graph["transitions"] if item["id"] == "e_hub_exit")
    assert management["sourceStateId"] == exit_["sourceStateId"] == "position_hub"
    assert management["priority"] < exit_["priority"]
    assert management["actions"] == [{"kind": "move_stop_to_break_even_next_open"}]
    assert any(item["id"] == "position_protective_closed" and item["sourceStateId"] == "position_hub" for item in graph["transitions"])
    assert all(len(item["actions"]) <= 1 for item in graph["transitions"])
    assert len(graph["states"]) <= 14 and len(graph["transitions"]) <= 56
    # This is deliberately a content golden: names/metadata must not drift
    # unnoticed while native validation remains an authority boundary.
    assert result["profileSha256"] == "sha256:c0792c21cfc53b81314a562fb79dbb9a4de278e16f5a0c99d44f25e0cc551100"


def test_codec_dispatch_round_trip_and_id_independent_topology_signature() -> None:
    original, renamed = _genome(), _genome(renamed=True)
    original.validate(); renamed.validate()
    assert original.semantic_topology_signature() == renamed.semantic_topology_signature()
    restored = decode_program(program_kind=PROGRAM_KIND, codec=GENOME_CODEC, payload=original.canonical())
    assert restored.canonical() == original.canonical()
    assert restored.semantic_topology_signature() == original.semantic_topology_signature()
    result = compile_program(program_kind=PROGRAM_KIND, codec=GENOME_CODEC, payload=original.canonical(), candidate_id="dispatch")
    assert result["genomeSha256"] == original.identity_sha256
    with pytest.raises(EvolvableGenomeError, match="no codec"):
        decode_program(program_kind="old_typed_fragment", codec=GENOME_CODEC, payload=original.canonical())


def test_topology_signature_is_independent_of_resource_use_order() -> None:
    original = _genome()
    setup_index = next(
        index for index, node in enumerate(original.nodes) if node.node_id == "setup"
    )
    setup = original.nodes[setup_index]
    reordered_setup = GenomeNodeV1(
        setup.node_id,
        setup.zone,
        setup.kind,
        setup.guard,
        tuple(reversed(setup.resources)),
        setup.timeout_bars,
    )
    reordered_nodes = list(original.nodes)
    reordered_nodes[setup_index] = reordered_setup
    reordered = EvolvableModuleGenomeV1(
        original.direction,
        original.resources,
        tuple(reordered_nodes),
        original.edges,
        original.budget,
        original.program_kind,
        original.codec,
        original.instrument,
    )

    assert reordered.canonical() == original.canonical()
    assert reordered.semantic_topology_signature() == original.semantic_topology_signature()


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda genome: EvolvableModuleGenomeV1(genome.direction, genome.resources, genome.nodes, (*genome.edges, GenomeEdgeV1("dangling", "missing", genome.nodes[0].node_id))), "dangling graph edge"),
        (lambda genome: EvolvableModuleGenomeV1(genome.direction, ResourcePoolV1(genome.resources.indicators, genome.resources.evidence_groups, genome.resources.events, (*genome.resources.management_refs, {"id": "unused", "initialStop": {"kind": "fixed_percent", "percent": 1.0}})), genome.nodes, genome.edges), "orphan resource"),
        (lambda genome: EvolvableModuleGenomeV1(genome.direction, ResourcePoolV1(({"meta": {"id": "I_RSI", "instanceId": "rsi"}, "side": "short"},), genome.resources.evidence_groups, genome.resources.events, genome.resources.management_refs), genome.nodes, genome.edges), "cross-side"),
        (lambda genome: EvolvableModuleGenomeV1(genome.direction, genome.resources, genome.nodes, (*genome.edges, GenomeEdgeV1("collision", genome.nodes[3].node_id, genome.nodes[5].node_id, priority=10, effect=EffectKind.EXIT))), "priority conflict"),
    ],
)
def test_structural_validation_rejects_dangling_orphan_cross_side_and_conflicts(mutate, message) -> None:
    with pytest.raises(EvolvableGenomeError, match=message):
        mutate(_genome()).validate()


def test_entry_setup_is_a_dag_and_recovery_is_compiled_as_timeout() -> None:
    genome = _genome()
    nodes = list(genome.nodes)
    profile = EvolvableModuleCompilerV1().compile(genome, candidate_id="bounded-loop")["profile"]
    assert any(item["id"] == "recovery_0" and item["guard"]["kind"] == "state_age_at_least" for item in profile["graph"]["transitions"])
    cyclic = EvolvableModuleGenomeV1(genome.direction, genome.resources, tuple(nodes), (*genome.edges, GenomeEdgeV1("setup_back", "setup", "setup", priority=20)))
    with pytest.raises(EvolvableGenomeError, match="entry/setup graph must be acyclic"):
        cyclic.validate()


def test_guard_references_are_module_bound_recursively_for_nodes_and_edges() -> None:
    genome = _genome()
    nodes = tuple(
        GenomeNodeV1(
            node.node_id,
            node.zone,
            node.kind,
            {"kind": "all", "guards": [{"kind": "evidence_at_least", "groupId": "missing_group", "thresholdPercent": 55.0}]}
            if node.node_id == "setup"
            else node.guard,
            node.resources,
            node.timeout_bars,
        )
        for node in genome.nodes
    )
    with pytest.raises(EvolvableGenomeError, match="dangling evidence_group"):
        EvolvableModuleGenomeV1(genome.direction, genome.resources, nodes, genome.edges).validate()

    edges = tuple(
        GenomeEdgeV1(
            edge.edge_id,
            edge.source_id,
            edge.target_id,
            priority=edge.priority,
            guard={"kind": "fresh_event", "eventId": "missing_event"}
            if edge.edge_id == "hub_exit"
            else edge.guard,
            effect=edge.effect,
        )
        for edge in genome.edges
    )
    with pytest.raises(EvolvableGenomeError, match="dangling event"):
        EvolvableModuleGenomeV1(genome.direction, genome.resources, genome.nodes, edges).validate()

    wrong_type_nodes = tuple(
        GenomeNodeV1(
            node.node_id,
            node.zone,
            node.kind,
            {"kind": "evidence_at_least", "groupId": "e_rsi", "thresholdPercent": 55.0}
            if node.node_id == "setup"
            else node.guard,
            node.resources,
            node.timeout_bars,
        )
        for node in genome.nodes
    )
    with pytest.raises(EvolvableGenomeError, match="references event; expected evidence_group"):
        EvolvableModuleGenomeV1(genome.direction, genome.resources, wrong_type_nodes, genome.edges).validate()

    malformed = tuple(
        GenomeEdgeV1(
            edge.edge_id,
            edge.source_id,
            edge.target_id,
            priority=edge.priority,
            guard={"kind": "all", "guards": "not-a-list"} if edge.edge_id == "hub_exit" else edge.guard,
            effect=edge.effect,
        )
        for edge in genome.edges
    )
    with pytest.raises(EvolvableGenomeError, match="nonempty guards list"):
        EvolvableModuleGenomeV1(genome.direction, genome.resources, genome.nodes, malformed).validate()

    # The event is globally owned by the pool/setup node; an unrelated edge may
    # reference it without redundantly declaring it as a resource use.
    globally_bound = tuple(
        GenomeEdgeV1(
            edge.edge_id,
            edge.source_id,
            edge.target_id,
            priority=edge.priority,
            guard={"kind": "fresh_event", "eventId": "e_rsi"} if edge.edge_id == "hub_exit" else edge.guard,
            effect=edge.effect,
        )
        for edge in genome.edges
    )
    EvolvableModuleGenomeV1(genome.direction, genome.resources, genome.nodes, globally_bound).validate()


def test_guard_only_resources_are_emitted_and_not_nesting_cannot_evict_depth_cap() -> None:
    genome = _genome()
    # The setup guard owns both resources even though the node no longer
    # repeats them in the optional structural annotation.
    nodes = tuple(
        GenomeNodeV1(
            node.node_id, node.zone, node.kind, node.guard,
            () if node.node_id == "setup" else node.resources,
            node.timeout_bars,
        )
        for node in genome.nodes
    )
    guarded = EvolvableModuleGenomeV1(genome.direction, genome.resources, nodes, genome.edges)
    guarded.validate()
    profile = EvolvableModuleCompilerV1().compile(guarded, candidate_id="guard-only")["profile"]
    assert [item["id"] for item in profile["graph"]["evidenceGroups"]] == ["g_rsi"]
    assert [item["id"] for item in profile["graph"]["eventBindings"]] == ["e_rsi"]

    deeply_not = {"kind": "always"}
    for _ in range(5):
        deeply_not = {"kind": "not", "guard": deeply_not}
    over_depth_nodes = tuple(
        GenomeNodeV1(
            node.node_id, node.zone, node.kind,
            deeply_not if node.node_id == "setup" else node.guard,
            node.resources, node.timeout_bars,
        )
        for node in genome.nodes
    )
    with pytest.raises(EvolvableGenomeError, match="guard exceeds"):
        EvolvableModuleGenomeV1(genome.direction, genome.resources, over_depth_nodes, genome.edges).validate()


def test_all_recovery_nodes_require_explicit_timeouts_before_compilation() -> None:
    genome = _genome()
    unbounded = tuple(
        GenomeNodeV1(node.node_id, node.zone, node.kind, node.guard, node.resources, None)
        if node.zone is Zone.RECOVERY else node
        for node in genome.nodes
    )
    with pytest.raises(EvolvableGenomeError, match="recovery nodes require"):
        EvolvableModuleGenomeV1(genome.direction, genome.resources, unbounded, genome.edges).validate()


@pytest.mark.skipif(not CORE_PYTHON.is_file(), reason="Dashboard native authority environment is unavailable")
def test_golden_compiled_module_is_accepted_by_dashboard_native_validator() -> None:
    class DashboardValidator:
        def validate_v2(self, *, profile, candidate_id):
            script = """import json,sys\nfrom fuzzfolio_core.temporal_graph.search_validation import validate_temporal_search_candidate\np=json.loads(sys.stdin.read())\nprint(json.dumps(validate_temporal_search_candidate(p,candidate_id=sys.argv[1])))"""
            completed = subprocess.run([str(CORE_PYTHON), "-c", script, candidate_id], input=__import__("json").dumps(profile), text=True, capture_output=True, check=True)
            return __import__("json").loads(completed.stdout)
    report = EvolvableModuleCompilerV1().compile(_genome(), candidate_id="native_evolvable", native_validator=DashboardValidator())["nativeValidation"]
    assert report["candidateAcceptable"] is True
