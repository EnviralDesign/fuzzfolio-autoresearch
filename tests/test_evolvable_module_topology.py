from __future__ import annotations

import copy

import pytest

from autoresearch.evolvable_module_genome import (
    EffectKind,
    EvolvableGenomeError,
    EvolvableModuleCompilerV1,
    EvolvableModuleGenomeV1,
    GenomeEdgeV1,
    GenomeNodeV1,
    Zone,
)
from autoresearch.evolvable_module_topology import (
    MotifCrossoverPlanV1,
    apply_crossover,
    apply_plan,
    make_crossover_plan,
    make_plan,
)
from test_evolvable_module_genome import _genome


def _apply(genome, operation: str, **kwargs):
    return apply_plan(genome, make_plan(genome, operation=operation, **kwargs))


def test_setup_entry_and_confirmation_motifs_are_content_bound_and_compilable() -> None:
    base = _genome()
    setup = _apply(base, "insert_setup", edgeId="start_setup", kind="context", guard={"kind": "always"})
    assert setup.delta.added_nodes and setup.genome.identity_sha256 != base.identity_sha256
    # Removing an inserted linear motif deterministically restores the parent
    # content (the plan-derived inbound ID is intentionally removed as well).
    removed = _apply(setup.genome, "remove_setup", nodeId=setup.delta.added_nodes[0])
    assert removed.genome.canonical() == base.canonical()

    motif = _apply(
        base,
        "insert_confirmation_rejection",
        edgeId="setup_entry",
        rejectPriority=20,
        rejectionTimeoutBars=4,
    )
    entries = [node for node in motif.genome.nodes if node.kind == "entry"]
    assert len(entries) == 1
    assert {node.kind for node in motif.genome.nodes if node.node_id in motif.delta.added_nodes} >= {
        "confirmation",
        "rejection_rearm",
    }
    compiled = EvolvableModuleCompilerV1().compile(motif.genome, candidate_id="confirmation-rejection")["profile"]
    transitions = compiled["graph"]["transitions"]
    rejection = next(item for item in transitions if item["id"].startswith("e_reject_path_"))
    confirmation = next(item for item in transitions if item["id"].startswith("e_confirm_path_"))
    assert rejection["actions"] == []
    assert rejection["reasonCode"] == "evolvable.pre_position_recovery"
    assert confirmation["actions"] == [{"kind": EffectKind.ENTER.value, "managementPlanId": "base"}]
    motif.genome.validate()


def test_entry_branch_add_rewire_remove_preserves_resource_closure() -> None:
    base = _genome()
    added = _apply(base, "insert_entry_branch", sourceId="setup", priority=20, managementRefId="base")
    branch = next(edge for edge in added.genome.edges if edge.edge_id in added.delta.added_edges and edge.effect is EffectKind.ENTER)
    rewired = _apply(added.genome, "rewire_entry_branch", edgeId=branch.edge_id, sourceId="start", priority=20, guard={"kind": "always"})
    edge = next(edge for edge in rewired.genome.edges if edge.edge_id == branch.edge_id)
    assert edge.source_id == "start" and edge.priority == 20
    child_entry = next(node for node in rewired.genome.nodes if node.node_id.startswith("entry_"))
    removed = _apply(rewired.genome, "remove_entry_branch", nodeId=child_entry.node_id)
    assert len([edge for edge in removed.genome.edges if edge.effect is EffectKind.ENTER]) == 1
    removed.genome.validate()


def test_management_exit_and_bounded_rearm_regions_respect_shared_hub() -> None:
    base = _genome()
    managed = _apply(base, "insert_management_region", effect=EffectKind.TIGHTEN_STOP.value, priority=30, kind="trail", guard={"kind": "always"})
    management = next(node for node in managed.genome.nodes if node.node_id in managed.delta.added_nodes)
    dispatch = next(edge for edge in managed.genome.edges if edge.target_id == management.node_id)
    assert dispatch.source_id == "hub" and dispatch.effect is EffectKind.TIGHTEN_STOP
    rewired = _apply(managed.genome, "rewire_management_region", nodeId=management.node_id, priority=35, effect=EffectKind.BREAK_EVEN.value)
    assert next(edge for edge in rewired.genome.edges if edge.target_id == management.node_id).effect is EffectKind.BREAK_EVEN

    exit_added = _apply(rewired.genome, "insert_exit_region", priority=40, kind="volatility_exit", guard={"kind": "always"})
    exit_node = next(node for node in exit_added.genome.nodes if node.node_id in exit_added.delta.added_nodes)
    assert next(edge for edge in exit_added.genome.edges if edge.target_id == exit_node.node_id).source_id == "hub"
    rearm = _apply(exit_added.genome, "insert_timeout_rearm", timeoutBars=5)
    rearm_node = next(node for node in rearm.genome.nodes if node.node_id in rearm.delta.added_nodes)
    assert rearm_node.timeout_bars == 5 and rearm_node.kind == "bounded_rearm"
    cleaned = _apply(rearm.genome, "remove_timeout_rearm", nodeId=rearm_node.node_id)
    cleaned = _apply(cleaned.genome, "remove_exit_region", nodeId=exit_node.node_id)
    cleaned = _apply(cleaned.genome, "remove_management_region", nodeId=management.node_id)
    cleaned.genome.validate()


def test_plans_are_exact_replayable_and_stale_plans_fail_closed() -> None:
    base = _genome()
    plan = make_plan(base, operation="insert_management_region", effect=EffectKind.TIGHTEN_STOP.value, priority=30)
    first = apply_plan(base, plan)
    second = apply_plan(base, plan)
    assert first.genome.canonical() == second.genome.canonical()
    assert first.delta.canonical() == second.delta.canonical()
    with pytest.raises(EvolvableGenomeError, match="stale topology plan"):
        apply_plan(first.genome, plan)

    stale = copy.deepcopy(plan)
    with pytest.raises(EvolvableGenomeError, match="stale topology plan"):
        apply_plan(_genome(renamed=True), stale)


def test_setup_split_and_remove_preserve_a_nonempty_original_guard_exactly() -> None:
    base = _genome()
    guarded_edges = tuple(
        GenomeEdgeV1(
            edge.edge_id,
            edge.source_id,
            edge.target_id,
            priority=edge.priority,
            guard={"kind": "always"} if edge.edge_id == "start_setup" else edge.guard,
            effect=edge.effect,
        )
        for edge in base.edges
    )
    guarded = EvolvableModuleGenomeV1(base.direction, base.resources, base.nodes, guarded_edges)
    inserted = _apply(guarded, "insert_setup", edgeId="start_setup", kind="context")
    first = next(edge for edge in inserted.genome.edges if edge.edge_id.startswith("setup_in_"))
    second = next(edge for edge in inserted.genome.edges if edge.edge_id == "start_setup")
    assert first.guard == {"kind": "always"} and second.guard == {}
    restored = _apply(inserted.genome, "remove_setup", nodeId=inserted.delta.added_nodes[0])
    assert restored.genome.canonical() == guarded.canonical()


def test_bad_topology_requests_fail_before_child_admission() -> None:
    base = _genome()
    with pytest.raises(EvolvableGenomeError, match="priority conflicts"):
        _apply(base, "insert_exit_region", priority=20)
    with pytest.raises(EvolvableGenomeError, match="final entry branch"):
        _apply(base, "remove_entry_branch", nodeId="entry")
    with pytest.raises(EvolvableGenomeError, match="positive"):
        _apply(base, "insert_timeout_rearm", timeoutBars=0)
    with pytest.raises(EvolvableGenomeError, match="incompatible typed motif port"):
        make_crossover_plan(base, base, segment_map={"not_a_port": ["anything"]})


def test_pre_position_recovery_is_side_effect_free_and_explicitly_bounded() -> None:
    base = _genome()
    with_effect = EvolvableModuleGenomeV1(
        base.direction,
        base.resources,
        base.nodes,
        (*base.edges, GenomeEdgeV1("setup_recovery", "setup", "cooldown", priority=20, effect=EffectKind.ENTER)),
    )
    with pytest.raises(EvolvableGenomeError, match="side-effect-free"):
        with_effect.validate()
    unbounded_nodes = tuple(
        GenomeNodeV1(node.node_id, node.zone, node.kind, node.guard, node.resources, None)
        if node.node_id == "cooldown"
        else node
        for node in base.nodes
    )
    unbounded = EvolvableModuleGenomeV1(
        base.direction,
        base.resources,
        unbounded_nodes,
        (*base.edges, GenomeEdgeV1("setup_recovery", "setup", "cooldown", priority=20)),
    )
    with pytest.raises(EvolvableGenomeError, match="bounded timeout"):
        unbounded.validate()


def test_same_side_crossover_binds_ordered_parents_and_segment_map() -> None:
    left = _genome()
    right = _apply(left, "insert_management_region", effect=EffectKind.BREAK_EVEN.value, priority=30, kind="alternate_break_even", guard={"kind": "always"}).genome
    donor = next(edge for edge in right.edges if edge.edge_id.startswith("management_dispatch_"))
    plan = make_crossover_plan(left, right, segment_map={"management_hub": [donor.edge_id]})
    result = apply_crossover(left, right, plan)
    assert result.semantic_delta["orderedParents"] == [left.identity_sha256, right.identity_sha256]
    assert result.semantic_delta["segmentMap"] == {"management_hub": [donor.edge_id]}
    assert result.genome.identity_sha256 != left.identity_sha256
    result.genome.validate()
    with pytest.raises(EvolvableGenomeError, match="stale crossover plan"):
        apply_crossover(right, left, plan)

    # Segment maps are part of the plan identity; a hand-mutated instance is a
    # different plan and must not silently share identity with the original.
    alternate = MotifCrossoverPlanV1(left.identity_sha256, right.identity_sha256, {"management_hub": []})
    assert alternate.identity_sha256 != plan.identity_sha256


def test_linear_entry_setup_crossover_is_native_compilable_and_resource_closed() -> None:
    left = _genome()
    right = _apply(left, "insert_setup", edgeId="start_setup", kind="donor_context", guard={"kind": "always"}).genome
    # The retained second half of the split targets the original setup, whose
    # continuation remains the typed enter edge on both parents.
    donor = next(edge for edge in right.edges if edge.edge_id == "start_setup")
    plan = make_crossover_plan(left, right, segment_map={"entry_setup": [donor.edge_id]})
    result = apply_crossover(left, right, plan)
    assert result.semantic_delta["segmentMap"] == {"entry_setup": [donor.edge_id]}
    assert EvolvableModuleCompilerV1().compile(result.genome, candidate_id="entry_setup_crossover")["profile"]["version"] == "v2"

    # An edge outside the typed linear setup port fails before any child/replay
    # identity is materialized.
    with pytest.raises(EvolvableGenomeError, match="incompatible typed motif port|nonexistent donor motif"):
        make_crossover_plan(left, right, segment_map={"entry_setup": ["entry_hub"]})
