from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path

import psutil
import pytest

from autoresearch.temporal_bidirectional_genome import FrozenModule, FrozenPair, IdentitySnapshot, canonical_json, canonical_sha256
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_g0_bootstrap import (
    DESCRIPTOR_AXES, build_accepted_pool, descriptor_vector, materialize_campaign_ledger,
    project_accepted_pair_entry, select_g0_bootstrap, verify_campaign_ledger, materialize_campaign_ledger,
    read_selected_entry_from_journal, verify_selected_reference_against_entry,
)
from autoresearch.temporal_qd_population_finalizer import ensure_rust_population_finalizer, finalize_population_with_rust
from autoresearch.temporal_qd_g0_bootstrap import _canonical_graph_topology
from autoresearch.temporal_qd_pair_generation import materialize_pair_candidate


POOL_ID = canonical_sha256({"g0": "verified-pair-pool"})


class _Native:
    def validate_v2(self, *, profile, candidate_id):
        raw = canonical_sha256(profile)
        return {"schemaVersion": "temporal_search_candidate_validation_v1", "candidateId": candidate_id, "rawSourceProfileSha256": raw, "profileSnapshotSha256": canonical_sha256({"snapshot": profile}), "programSha256": canonical_sha256({"native": profile}), "validationReportSha256": canonical_sha256({"validation": profile}), "status": "valid_evaluable", "candidateAcceptable": True}


class _PairCompiler:
    def __init__(self, *, mode: str = "live", initial_state_id: str = "flat") -> None:
        self.mode = mode
        self.initial_state_id = initial_state_id

    def compile_pair(self, *, long_profile, short_profile, candidate_id):
        def module(side, source):
            prefix = {"opaque_ids_a": "alpha", "opaque_ids_b": "beta"}.get(self.mode, "")
            token = f"{prefix}_" if prefix else ""
            states = [f"{token}{side}_watch", f"{token}{side}_pending", f"{token}{side}_open"]
            transitions = [f"{token}{side}_arm", f"{token}{side}_enter", f"{token}{side}_fill", f"{token}{side}_exit"]
            return {"direction": side, "id": f"opaque-{side}", "stateIds": states, "transitionIds": transitions, "indicatorIds": [item["meta"]["instanceId"] for item in source["indicators"]], "eventBindingIds": [f"{side}_event"], "evidenceGroupIds": [f"{side}_group"], "sourceProfileSnapshotSha256": canonical_sha256({"snapshot": source}), "sourceProgramSha256": canonical_sha256({"native": source})}
        modules = [module("long", long_profile), module("short", short_profile)]
        transitions = []
        for item in modules:
            side = item["direction"]; watch, pending, opened = item["stateIds"]; arm, enter, fill, exit_ = item["transitionIds"]
            transitions.extend([
                {"id": arm, "sourceStateId": self.initial_state_id, "destinationStateId": watch, "eventClass": "decision", "guard": {"kind": "fresh_event", "eventId": f"{side}_event"}, "actions": []},
                {"id": enter, "sourceStateId": watch if self.mode != f"{side}_unreachable" else f"{side}_dead", "destinationStateId": pending, "eventClass": "decision", "guard": {"kind": "evidence_at_least", "groupId": f"{side}_group", "thresholdPercent": 55}, "actions": [] if self.mode in {"no_entry", f"{side}_dead"} else [{"kind": "enter_next_open", "managementPlanId": f"{side}_base"}]},
                {"id": fill, "sourceStateId": pending, "destinationStateId": opened, "eventClass": "execution", "guard": {"kind": "execution_status_is", "status": "filled"}, "actions": []},
                {"id": exit_, "sourceStateId": watch if self.mode == "rewired" else opened, "destinationStateId": self.initial_state_id, "eventClass": "decision", "priority": 7, "guard": {"kind": "state_age_at_least", "events": 4}, "actions": [{"kind": "activate_trailing_stop_next_open"}] if self.mode == "trailing" else [{"kind": "exit_next_open"}]},
            ])
        plans = []
        for side, source in (("long", long_profile), ("short", short_profile)):
            base = copy.deepcopy(source["executionConfig"]["managementLibrary"]["plans"][0]); base["id"] = f"{side}_base"; plans.append(base)
        profile = {"version": "v3", "directionMode": "both", "name": candidate_id, "executionConfig": {"managementLibrary": {"defaultPlanId": "long_base", "plans": plans}}, "graph": {"initialStateId": "flat", "states": [{"id": "flat"}] + [{"id": state} for module_ in modules for state in module_["stateIds"]], "entryArbitration": {"modules": modules}, "transitions": transitions, "eventBindings": [{"id": "long_event", "indicatorInstanceId": long_profile["indicators"][1]["meta"]["instanceId"], "longOutput": "bullish", "shortOutput": "bearish"}, {"id": "short_event", "indicatorInstanceId": short_profile["indicators"][1]["meta"]["instanceId"], "longOutput": "bullish", "shortOutput": "bearish"}], "evidenceGroups": [{"id": "long_group", "indicatorInstanceIds": [long_profile["indicators"][0]["meta"]["instanceId"]]}, {"id": "short_group", "indicatorInstanceIds": [short_profile["indicators"][0]["meta"]["instanceId"]]}]}}
        profile["graph"]["initialStateId"] = self.initial_state_id
        profile["graph"]["states"][0]["id"] = self.initial_state_id
        raw = canonical_sha256(profile)
        return {"profile": profile, "validation": {"schemaVersion": "temporal_search_candidate_validation_v1", "rawSourceProfileSha256": raw, "profileSnapshotSha256": canonical_sha256({"snapshot": profile}), "programSha256": canonical_sha256({"native": profile}), "validationReportSha256": canonical_sha256({"validation": profile}), "status": "valid_evaluable", "candidateAcceptable": True}}


def _snapshot(kind: str, payload: dict) -> IdentitySnapshot:
    return IdentitySnapshot.create(kind=kind, schema_version=f"{kind}_v1", payload=payload)


def _source_profile(side: str, *, family: str, opaque: str, name_only_trailing: bool = False) -> dict:
    setup, trigger = f"{opaque}_{side}_setup", f"{opaque}_{side}_trigger"
    implementation = f"{family}_IMPL"
    return {"version": "v2", "directionMode": side, "name": "activate_trailing_only_in_name" if name_only_trailing else f"{opaque}_{side}", "indicators": [{"config": {}, "meta": {"instanceId": setup, "baseIndicatorId": family, "id": implementation}}, {"config": {}, "meta": {"instanceId": trigger, "id": f"{implementation}_EVENT"}}], "executionConfig": {"managementLibrary": {"defaultPlanId": "base", "plans": [{"id": "base", "initialStop": {"kind": "fixed_percent", "percent": 1.0}, "initialTarget": {"kind": "reward_multiple", "multiple": 2.0}, "holdPolicy": {"kind": "market_bars", "bars": 8}}]}}, "graph": {"states": [{"id": f"{opaque}_ready"}], "transitions": [], "eventBindings": [], "evidenceGroups": []}}


def _pair(*, ordinal: int, family: str = "RSI", opaque: str = "opaque", mode: str = "live", name_only_trailing: bool = False, initial_state_id: str = "flat") -> FrozenPair:
    primitive_ids = [f"{family}_IMPL", f"{family}_IMPL_EVENT"]
    catalog = _snapshot("catalog", {"catalog": {"indicators": [{"meta": {"id": identifier}, "implementation": identifier} for identifier in primitive_ids]}, "catalogSha256": canonical_sha256({"catalog": family})})
    compiler_identity_material = {"compiler": "g0-fixture", "mode": mode}
    if initial_state_id != "flat":
        compiler_identity_material["initialStateId"] = initial_state_id
    compiler_identity = _snapshot("pairCompiler", compiler_identity_material)
    native = _Native(); compiler = _PairCompiler(mode=mode, initial_state_id=initial_state_id)
    def module(side: str) -> FrozenModule:
        return FrozenModule.validate_native(program={"schemaVersion": "temporal_typed_fragment_grammar_v2", "grammarVersion": "3", "direction": side, "fragments": []}, profile=_source_profile(side, family=family, opaque=opaque, name_only_trailing=name_only_trailing), grammar_context=_snapshot("grammarContext", {"context": "fixture"}), catalog=catalog, policy=_snapshot("policy", {"policy": "fixture"}), native_authority_identity=_snapshot("nativeAuthority", {"authority": "fixture"}), native_validator=native, candidate_id=f"module-{ordinal}-{side}")
    return FrozenPair.compile(long=module("long"), short=module("short"), pair_compiler_identity=compiler_identity, pair_compiler=compiler, candidate_id=f"pair-{ordinal}")


def _entry(ordinal: int, **kwargs) -> dict:
    pair = _pair(ordinal=ordinal, **kwargs)
    policy = {"schemaVersion": "temporal_qd_bidirectional_pair_policy_v1", "enabled": True, "compilerAuthority": pair.pair_compiler.canonical_payload()}
    proposal = {"schemaVersion": "temporal_qd_pair_proposal_v2", "proposalSeed": f"seed-{ordinal}", "originKind": "random_immigrant", "side": "long", "factoryPair": pair.canonical_payload(), "pairIdentitySha256": pair.identity_sha256, "disposition": "materialized"}
    proposal["proposalSha256"] = canonical_sha256(proposal)
    candidate = materialize_pair_candidate(pair=pair, proposal=proposal, pair_policy=policy, generation_index=0, birth_ordinal=ordinal, proposal_ordinal=ordinal)
    entry = {"schemaVersion": "temporal_qd_proposal_entry_v3", "configSha256": canonical_sha256({"config": "fixture"}), "generationIndex": 0, "proposalOrdinal": ordinal, "originKind": "random_immigrant", "proposal": proposal, "operatorImplementationSha256": canonical_sha256({"operator": "fixture"}), "disposition": "accepted", "candidate": candidate}
    entry["entrySha256"] = canonical_sha256(entry)
    return entry


def _reference(ordinal: int, **kwargs) -> dict:
    return project_accepted_pair_entry(construction_pool_identity_sha256=POOL_ID, proposal_ordinal=ordinal, journal_path="fixtures/accepted-pairs.jsonl", accepted_pair_entry=_entry(ordinal, **kwargs))


def test_verified_pair_projection_is_order_invariant_and_binds_all_ledger_rows() -> None:
    refs = [_reference(i, family=("RSI", "ATR")[i % 2], opaque=f"opaque-{i}", mode="trailing" if i % 3 == 0 else "live") for i in range(8)]
    pool = build_accepted_pool(construction_pool_identity_sha256=POOL_ID, references=refs)
    reversed_pool = build_accepted_pool(construction_pool_identity_sha256=POOL_ID, references=list(reversed(refs)))
    result = select_g0_bootstrap(accepted_pool=pool, evaluation_width=4)
    assert result == select_g0_bootstrap(accepted_pool=reversed_pool, evaluation_width=4)
    assert result["marketEvidenceRead"] is False
    ledger = materialize_campaign_ledger(accepted_pool=pool)
    assert len(ledger["rows"]) == 8 and verify_campaign_ledger(ledger=ledger, accepted_pool=pool) == ledger


def test_compact_reference_lazy_hydrates_the_exact_selected_journal_entry(tmp_path) -> None:
    entry = _entry(9)
    reference = project_accepted_pair_entry(construction_pool_identity_sha256=POOL_ID, proposal_ordinal=9, journal_path="proposal-journal/00000009.json", accepted_pair_entry=entry)
    retained = json.dumps(reference, sort_keys=True)
    assert "bidirectionalGenome" not in retained and "pairProposal" not in retained
    assert "candidate" not in reference and "proposal" not in reference and "acceptedPairEntry" not in reference
    assert verify_selected_reference_against_entry(reference=reference, accepted_pair_entry=entry) == entry
    journal = tmp_path / "proposal-journal" / "00000009.json"
    journal.parent.mkdir(); journal.write_text(json.dumps(entry), encoding="utf-8")
    assert read_selected_entry_from_journal(reference=reference, journal_root=tmp_path) == entry


def test_semantics_ignore_opaque_native_ids_but_detect_catalog_indicator_family_change() -> None:
    same_a = descriptor_vector(_reference(0, family="RSI", opaque="wildly-different-a"))
    same_b = descriptor_vector(_reference(1, family="RSI", opaque="wildly-different-b"))
    changed = descriptor_vector(_reference(2, family="ATR", opaque="third"))
    assert same_a == same_b
    assert same_a["long.indicatorSemantics"] != changed["long.indicatorSemantics"]
    assert set(same_a) == set(DESCRIPTOR_AXES)


def test_pair_descriptor_binds_compiler_owned_shared_initial_state_id() -> None:
    descriptor = descriptor_vector(_reference(3, initial_state_id="flat_supervisor"))
    assert set(descriptor) == set(DESCRIPTOR_AXES)


def test_compiled_graph_topology_ignores_pure_id_renames_but_detects_rewiring() -> None:
    renamed_a = descriptor_vector(_reference(20, mode="opaque_ids_a"))
    renamed_b = descriptor_vector(_reference(21, mode="opaque_ids_b"))
    rewired = descriptor_vector(_reference(22, mode="rewired"))
    assert renamed_a["long.topology"] == renamed_b["long.topology"]
    assert renamed_a["short.topology"] == renamed_b["short.topology"]
    assert renamed_a["long.topology"] != rewired["long.topology"]
    assert renamed_a["short.topology"] != rewired["short.topology"]


def test_exact_topology_distinguishes_regular_graph_rewires_and_priority() -> None:
    def digest(edges, *, prefix: str, priority: int = 1):
        states = [{"id": f"{prefix}{index}"} for index in range(6)]
        transitions = [
            {"id": f"t{index}", "sourceStateId": f"{prefix}{source}", "destinationStateId": f"{prefix}{destination}", "eventClass": "decision", "priority": priority, "guard": {"kind": "always"}, "actions": []}
            for index, (source, destination) in enumerate(edges)
        ]
        return _canonical_graph_topology(states=states, transitions=transitions, event_map={}, group_map={}, indicator_map={}, plan_map={})
    six_cycle = [(index, (index + 1) % 6) for index in range(6)]
    two_triangles = [(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3)]
    assert digest(six_cycle, prefix="a") == digest(six_cycle, prefix="renamed_")
    assert digest(six_cycle, prefix="a") != digest(two_triangles, prefix="a")
    assert digest(six_cycle, prefix="a", priority=1) != digest(six_cycle, prefix="a", priority=2)


def test_declared_graph_order_and_transition_reference_targets_are_semantic() -> None:
    states = [{"id": "flat"}, {"id": "watch"}, {"id": "open"}]
    base = [
        {"id": "arm", "sourceStateId": "flat", "destinationStateId": "watch", "eventClass": "decision", "priority": 1, "guard": {"kind": "always"}, "actions": []},
        {"id": "enter", "sourceStateId": "watch", "destinationStateId": "open", "eventClass": "decision", "priority": 2, "guard": {"kind": "after_transition", "transitionId": "arm"}, "actions": []},
    ]
    renamed_states = [{"id": "x"}, {"id": "y"}, {"id": "z"}]
    renamed = [
        {**base[0], "id": "one", "sourceStateId": "x", "destinationStateId": "y"},
        {**base[1], "id": "two", "sourceStateId": "y", "destinationStateId": "z", "guard": {"kind": "after_transition", "transitionId": "one"}},
    ]
    digest = lambda rows, edges: _canonical_graph_topology(states=rows, transitions=edges, event_map={}, group_map={}, indicator_map={}, plan_map={})
    assert digest(states, base) == digest(renamed_states, renamed)
    altered_reference = copy.deepcopy(base); altered_reference[1]["guard"]["transitionId"] = "enter"
    assert digest(states, base) != digest(states, altered_reference)
    assert digest(states, base) != digest([states[0], states[2], states[1]], base)


def test_declared_topology_runtime_bounds_cover_realistic_8_16_and_maximum_graphs() -> None:
    def graph(count: int, transitions: int):
        states = [{"id": f"s{index}"} for index in range(count)]
        edges = [{"id": f"t{index}", "sourceStateId": f"s{index % count}", "destinationStateId": f"s{(index + 1) % count}", "eventClass": "decision", "priority": index % 3, "guard": {"kind": "always"}, "actions": []} for index in range(transitions)]
        return states, edges
    started = time.perf_counter()
    for count, transitions in ((8, 16), (16, 32), (32, 128)):
        states, edges = graph(count, transitions)
        assert _canonical_graph_topology(states=states, transitions=edges, event_map={}, group_map={}, indicator_map={}, plan_map={}).startswith("sha256:")
    assert time.perf_counter() - started < 1.0
    states, edges = graph(33, 1)
    with pytest.raises(TemporalDiscoveryContractError, match="runtime bound"):
        _canonical_graph_topology(states=states, transitions=edges, event_map={}, group_map={}, indicator_map={}, plan_map={})


def test_management_modes_are_closed_action_plan_semantics_not_names() -> None:
    named = descriptor_vector(_reference(0, name_only_trailing=True, mode="live"))
    action = descriptor_vector(_reference(1, mode="trailing"))
    assert named["long.graphManagementTrailingModes"] == "none"
    assert action["long.graphManagementTrailingModes"] == "activate_trailing_stop_next_open"


@pytest.mark.parametrize("mode", ["no_entry", "long_unreachable", "short_unreachable", "long_dead"])
def test_per_side_liveness_proof_rejects_no_transition_unreachable_or_dead_side(mode: str) -> None:
    with pytest.raises(TemporalDiscoveryContractError, match="reachability|liveness"):
        _reference(0, mode=mode)


def test_entry_pair_native_identity_and_closed_reference_schema_fail_closed() -> None:
    entry = _entry(0)
    entry["candidate"]["sourceProfile"]["name"] = "tampered"
    entry["entrySha256"] = canonical_sha256({key: value for key, value in entry.items() if key != "entrySha256"})
    with pytest.raises(TemporalDiscoveryContractError, match="frozen pair|compiled/native"):
        project_accepted_pair_entry(construction_pool_identity_sha256=POOL_ID, proposal_ordinal=0, journal_path="fixtures/accepted-pairs.jsonl", accepted_pair_entry=entry)
    ref = _reference(1)
    ref["EconomicScore"] = 1
    with pytest.raises(TemporalDiscoveryContractError, match="unexpected schema"):
        build_accepted_pool(construction_pool_identity_sha256=POOL_ID, references=[ref])
    ref = _reference(3)
    ref["descriptorProjection"]["EconomicScore"] = 1
    ref["descriptorProjection"]["descriptorProjectionSha256"] = canonical_sha256({key: value for key, value in ref["descriptorProjection"].items() if key != "descriptorProjectionSha256"})
    ref["descriptorProjectionSha256"] = ref["descriptorProjection"]["descriptorProjectionSha256"]
    ref["referenceSha256"] = canonical_sha256({key: value for key, value in ref.items() if key != "referenceSha256"})
    with pytest.raises(TemporalDiscoveryContractError, match="unexpected schema"):
        build_accepted_pool(construction_pool_identity_sha256=POOL_ID, references=[ref])
    missing = _reference(2); del missing["referenceSha256"]
    with pytest.raises(TemporalDiscoveryContractError, match="referenceSha256"):
        build_accepted_pool(construction_pool_identity_sha256=POOL_ID, references=[missing])


def _rebind_entry_proposal(entry: dict) -> None:
    proposal = entry["proposal"]
    proposal["proposalSha256"] = canonical_sha256({key: value for key, value in proposal.items() if key != "proposalSha256"})
    candidate = entry["candidate"]
    candidate["pairProposal"] = copy.deepcopy(proposal)
    candidate["pairProposalSha256"] = proposal["proposalSha256"]
    candidate["candidateIdentityMaterial"]["materializedPairProposalSha256"] = proposal["proposalSha256"]
    candidate["candidateIdentitySha256"] = canonical_sha256(candidate["candidateIdentityMaterial"])
    candidate["candidateId"] = "qd_" + candidate["candidateIdentitySha256"][7:35]
    candidate["lineage"]["candidateId"] = candidate["candidateId"]
    candidate["lineage"]["candidateIdentitySha256"] = candidate["candidateIdentitySha256"]
    entry["entrySha256"] = canonical_sha256({key: value for key, value in entry.items() if key != "entrySha256"})


def test_proposal_pair_provenance_must_match_candidate_frozen_genome() -> None:
    entry = _entry(35)
    different = _pair(ordinal=36, opaque="different-valid-pair")
    entry["proposal"]["factoryPair"] = different.canonical_payload()
    entry["proposal"]["pairIdentitySha256"] = different.identity_sha256
    _rebind_entry_proposal(entry)
    with pytest.raises(TemporalDiscoveryContractError, match="proposal frozen pair"):
        project_accepted_pair_entry(construction_pool_identity_sha256=POOL_ID, proposal_ordinal=35, journal_path="fixtures/accepted-pairs.jsonl", accepted_pair_entry=entry)
    entry = _entry(37)
    entry["proposal"]["pairIdentitySha256"] = different.identity_sha256
    _rebind_entry_proposal(entry)
    with pytest.raises(TemporalDiscoveryContractError, match="proposal frozen pair"):
        project_accepted_pair_entry(construction_pool_identity_sha256=POOL_ID, proposal_ordinal=37, journal_path="fixtures/accepted-pairs.jsonl", accepted_pair_entry=entry)


@pytest.mark.parametrize("target,key", [("entry", "economicScore"), ("candidate", "EconomicScore"), ("proposal", "ECONOMICSCORE")])
def test_journal_nested_schema_rejects_unknown_or_case_variant_economic_fields(target: str, key: str) -> None:
    entry = _entry(30)
    row = entry if target == "entry" else entry[target]
    row[key] = 1
    entry["entrySha256"] = canonical_sha256({field: value for field, value in entry.items() if field != "entrySha256"})
    with pytest.raises(TemporalDiscoveryContractError, match="unexpected schema"):
        project_accepted_pair_entry(construction_pool_identity_sha256=POOL_ID, proposal_ordinal=30, journal_path="fixtures/accepted-pairs.jsonl", accepted_pair_entry=entry)


@pytest.mark.parametrize(
    "owner,key,value",
    [
        ("entry", "identityChecks", {"candidateIdentity": False}),
        ("entry", "predeclaredLakeScope", {"acceptable": True}),
        ("candidate", "canonicalEvidenceIdentitySha256", "sha256:" + "f" * 64),
    ],
)
def test_g0_rejects_forged_evidence_only_working_fields(
    owner: str, key: str, value: object
) -> None:
    """G0 must not accept opaque scope/de-duplication attestations on restart."""
    entry = _entry(38)
    target = entry if owner == "entry" else entry[owner]
    target[key] = value
    entry["entrySha256"] = canonical_sha256(
        {field: row for field, row in entry.items() if field != "entrySha256"}
    )
    with pytest.raises(TemporalDiscoveryContractError, match="unexpected schema"):
        project_accepted_pair_entry(
            construction_pool_identity_sha256=POOL_ID,
            proposal_ordinal=38,
            journal_path="fixtures/accepted-pairs.jsonl",
            accepted_pair_entry=entry,
        )


def test_projector_rejects_ordinal_and_generation_lineage_mismatches() -> None:
    entry = _entry(31)
    with pytest.raises(TemporalDiscoveryContractError, match="proposal ordinal"):
        project_accepted_pair_entry(construction_pool_identity_sha256=POOL_ID, proposal_ordinal=32, journal_path="fixtures/accepted-pairs.jsonl", accepted_pair_entry=entry)
    entry = _entry(31)
    entry["candidate"]["generationIndex"] = 1
    entry["entrySha256"] = canonical_sha256({field: value for field, value in entry.items() if field != "entrySha256"})
    with pytest.raises(TemporalDiscoveryContractError, match="generation index"):
        project_accepted_pair_entry(construction_pool_identity_sha256=POOL_ID, proposal_ordinal=31, journal_path="fixtures/accepted-pairs.jsonl", accepted_pair_entry=entry)


@pytest.mark.parametrize("path", ["C:/proposal-journal/00000001.json", "C:\\proposal-journal\\00000001.json"])
def test_projector_rejects_drive_qualified_journal_locators(path: str) -> None:
    with pytest.raises(TemporalDiscoveryContractError, match="journal relative path"):
        project_accepted_pair_entry(construction_pool_identity_sha256=POOL_ID, proposal_ordinal=1, journal_path=path, accepted_pair_entry=_entry(1))


@pytest.mark.parametrize("path", [("candidate", "lineage"), ("candidate", "constructionEvidenceScope"), ("proposal", "factoryConstructionAudit"), ("entry", "funnelCandidate")])
def test_deep_journal_boundaries_fail_closed(path: tuple[str, str]) -> None:
    entry = _entry(33)
    owner, field = path
    target = entry if owner == "entry" else entry[owner]
    target[field] = {"EconomicScore": 1}
    entry["entrySha256"] = canonical_sha256({key: value for key, value in entry.items() if key != "entrySha256"})
    with pytest.raises(TemporalDiscoveryContractError):
        project_accepted_pair_entry(construction_pool_identity_sha256=POOL_ID, proposal_ordinal=33, journal_path="fixtures/accepted-pairs.jsonl", accepted_pair_entry=entry)


def test_random_immigrant_source_and_pool_birth_ordinals_are_authoritative() -> None:
    entry = _entry(34); entry["candidate"]["sourceMode"] = "qd_structural_offspring_bidirectional_pair"
    entry["entrySha256"] = canonical_sha256({key: value for key, value in entry.items() if key != "entrySha256"})
    with pytest.raises(TemporalDiscoveryContractError, match="source semantics"):
        project_accepted_pair_entry(construction_pool_identity_sha256=POOL_ID, proposal_ordinal=34, journal_path="fixtures/accepted-pairs.jsonl", accepted_pair_entry=entry)
    entry = _entry(34); entry["candidate"]["mutationTrace"] = [{"operation": "unexpected"}]
    entry["entrySha256"] = canonical_sha256({key: value for key, value in entry.items() if key != "entrySha256"})
    with pytest.raises(TemporalDiscoveryContractError, match="mutation or activation"):
        project_accepted_pair_entry(construction_pool_identity_sha256=POOL_ID, proposal_ordinal=34, journal_path="fixtures/accepted-pairs.jsonl", accepted_pair_entry=entry)
    refs = [_reference(0), _reference(1)]
    refs[1]["constructionLineage"]["birthOrdinal"] = 0
    refs[1]["constructionLineage"]["constructionLineageSha256"] = canonical_sha256({key: value for key, value in refs[1]["constructionLineage"].items() if key != "constructionLineageSha256"})
    refs[1]["referenceSha256"] = canonical_sha256({key: value for key, value in refs[1].items() if key != "referenceSha256"})
    with pytest.raises(TemporalDiscoveryContractError, match="birth ordinals"):
        build_accepted_pool(construction_pool_identity_sha256=POOL_ID, references=refs)


def test_ledger_drift_fails_closed() -> None:
    pool = build_accepted_pool(construction_pool_identity_sha256=POOL_ID, references=[_reference(i) for i in range(3)])
    ledger = materialize_campaign_ledger(accepted_pool=pool); ledger["rows"][0]["marketEvidenceRead"] = True
    with pytest.raises(TemporalDiscoveryContractError, match="identity drift"):
        verify_campaign_ledger(ledger=ledger, accepted_pool=pool)


def test_indexed_selector_4000_to_1024_scale_gate() -> None:
    # Each temporary entry is independently native-shaped and valid.  The
    # projector drops it immediately; only compact refs enter this 4k pool.
    refs = []
    for ordinal in range(4000):
        entry = _entry(ordinal, opaque=f"scale-{ordinal}")
        refs.append(project_accepted_pair_entry(construction_pool_identity_sha256=POOL_ID, proposal_ordinal=ordinal, journal_path="fixtures/accepted-pairs.jsonl", accepted_pair_entry=entry))
    pool = build_accepted_pool(construction_pool_identity_sha256=POOL_ID, references=refs)
    process = psutil.Process(); before = process.memory_info().rss; started = time.perf_counter(); result = select_g0_bootstrap(accepted_pool=pool, evaluation_width=1024); elapsed = time.perf_counter() - started; delta = process.memory_info().rss - before
    assert len(result["selected"]) == 1024 and elapsed < 20.0 and delta < 192 * 1024 * 1024


def test_64_to_32_closed_subset_rust_finalizer_matches_python_oracle(tmp_path) -> None:
    """All 64 canonical entries are authenticated; only 32 are materialized."""
    root = tmp_path / "generation"
    journal_root = root / "proposal-journal"
    journal_root.mkdir(parents=True)
    construction_identity = canonical_sha256({"fixture": "g0-64-to-32"})
    refs = []
    entries = []
    for ordinal in range(64):
        entry = _entry(ordinal, family=("RSI", "ATR")[ordinal % 2], opaque=f"g0-{ordinal}")
        (journal_root / f"{ordinal:08d}.json").write_text(json.dumps(entry, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
        entries.append(entry)
        refs.append(project_accepted_pair_entry(construction_pool_identity_sha256=construction_identity, proposal_ordinal=ordinal, journal_path=f"proposal-journal/{ordinal:08d}.json", accepted_pair_entry=entry))
    pool = build_accepted_pool(construction_pool_identity_sha256=construction_identity, references=refs)
    selection = select_g0_bootstrap(accepted_pool=pool, evaluation_width=32)
    ledger = materialize_campaign_ledger(accepted_pool=pool, selected_reference_sha256s=[row["referenceSha256"] for row in selection["selected"]])
    artifact_root = root / "g0-bootstrap"; artifact_root.mkdir()
    for name, value in (("accepted-pool.json", pool), ("selection.json", selection), ("campaign-construction-ledger.json", ledger)):
        (artifact_root / name).write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    selected_by_ordinal = {row["proposalOrdinal"]: row for row in selection["selected"]}
    selected_entries = [entries[ordinal]["candidate"] for ordinal in sorted(selected_by_ordinal, key=lambda ordinal: selected_by_ordinal[ordinal]["candidateId"])]
    shell = {
        "schemaVersion": "temporal_qd_generation_population_v3", "configSha256": entries[0]["configSha256"],
        "generationIndex": 0, "candidateCount": 32, "targetUniqueCandidates": 32,
        "candidates": selected_entries, "g0Bootstrap": {
            "constructionPoolIdentitySha256": construction_identity, "acceptedPoolSha256": pool["acceptedPoolSha256"],
            "selectionSha256": selection["selectionSha256"], "ledgerSha256": selection["campaignLedgerSha256"],
        },
    }
    oracle = dict(shell)
    oracle["populationSha256"] = canonical_sha256(oracle)
    result = finalize_population_with_rust(
        output_root=root, population_without_sha=shell,
        expected_entry_sha256s=[entry["entrySha256"] for entry in entries],
        accepted_candidates=[{"proposalOrdinal": ordinal, "candidateId": row["candidateId"], "candidateIdentitySha256": row["candidateIdentitySha256"]} for ordinal, row in selected_by_ordinal.items()],
        g0_bootstrap=shell["g0Bootstrap"],
    )
    assert result["candidateCount"] == 32
    assert json.loads((root / "population.json").read_text(encoding="utf-8")) == oracle
    # Reviewer regression: a self-rehashed native manifest cannot substitute
    # a valid but unselected construction ordinal for an authoritative pick.
    manifest_path = root / "performance" / "population-finalizer" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    selected_ordinals = {row["proposalOrdinal"] for row in selection["selected"]}
    replacement = next(ref for ref in pool["acceptedReferences"] if ref["proposalOrdinal"] not in selected_ordinals)
    manifest["acceptedCandidates"][0] = {
        "proposalOrdinal": replacement["proposalOrdinal"], "candidateId": replacement["candidateId"],
        "candidateIdentitySha256": replacement["candidateIdentitySha256"],
        "acceptedPairEntrySha256": replacement["acceptedPairEntrySha256"], "referenceSha256": replacement["referenceSha256"],
    }
    manifest["manifestSha256"] = canonical_sha256({key: value for key, value in manifest.items() if key != "manifestSha256"})
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    binary, _ = ensure_rust_population_finalizer()
    assert subprocess.run([str(binary), "--manifest", str(manifest_path)], capture_output=True, text=True).returncode != 0


def _write_self_hashed_manifest(path: Path, manifest: dict) -> None:
    manifest["manifestSha256"] = canonical_sha256(
        {key: value for key, value in manifest.items() if key != "manifestSha256"}
    )
    path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")


def _g0_finalizer_fixture(root: Path) -> tuple[Path, dict, dict, dict, dict]:
    """Materialize a small, complete G0 generation and return its authorities."""
    journal_root = root / "proposal-journal"
    journal_root.mkdir(parents=True)
    construction_identity = canonical_sha256({"fixture": "g0-finalizer-adversarial"})
    references, entries = [], []
    for ordinal in range(12):
        entry = _entry(
            ordinal,
            family=("RSI", "ATR")[ordinal % 2],
            opaque=f"g0-adversarial-{ordinal}",
        )
        (journal_root / f"{ordinal:08d}.json").write_text(
            canonical_json(entry) + "\n", encoding="utf-8"
        )
        entries.append(entry)
        references.append(
            project_accepted_pair_entry(
                construction_pool_identity_sha256=construction_identity,
                proposal_ordinal=ordinal,
                journal_path=f"proposal-journal/{ordinal:08d}.json",
                accepted_pair_entry=entry,
            )
        )
    pool = build_accepted_pool(
        construction_pool_identity_sha256=construction_identity, references=references
    )
    selection = select_g0_bootstrap(accepted_pool=pool, evaluation_width=6)
    ledger = materialize_campaign_ledger(
        accepted_pool=pool,
        selected_reference_sha256s=[row["referenceSha256"] for row in selection["selected"]],
    )
    artifact_root = root / "g0-bootstrap"
    artifact_root.mkdir()
    for name, value in (
        ("accepted-pool.json", pool),
        ("selection.json", selection),
        ("campaign-construction-ledger.json", ledger),
    ):
        (artifact_root / name).write_text(canonical_json(value) + "\n", encoding="utf-8")
    selected_by_ordinal = {row["proposalOrdinal"]: row for row in selection["selected"]}
    shell = {
        "schemaVersion": "temporal_qd_generation_population_v3",
        "configSha256": entries[0]["configSha256"],
        "generationIndex": 0,
        "candidateCount": len(selected_by_ordinal),
        "targetUniqueCandidates": len(selected_by_ordinal),
        "candidates": [
            entries[ordinal]["candidate"]
            for ordinal in sorted(
                selected_by_ordinal,
                key=lambda ordinal: selected_by_ordinal[ordinal]["candidateId"],
            )
        ],
        "g0Bootstrap": {
            "constructionPoolIdentitySha256": construction_identity,
            "acceptedPoolSha256": pool["acceptedPoolSha256"],
            "selectionSha256": selection["selectionSha256"],
            "ledgerSha256": ledger["ledgerSha256"],
        },
    }
    finalize_population_with_rust(
        output_root=root,
        population_without_sha=shell,
        expected_entry_sha256s=[entry["entrySha256"] for entry in entries],
        accepted_candidates=[
            {
                "proposalOrdinal": ordinal,
                "candidateId": row["candidateId"],
                "candidateIdentitySha256": row["candidateIdentitySha256"],
            }
            for ordinal, row in selected_by_ordinal.items()
        ],
        g0_bootstrap=shell["g0Bootstrap"],
    )
    manifest_path = root / "performance" / "population-finalizer" / "manifest.json"
    return manifest_path, pool, selection, ledger, shell


def _native_rejects(manifest_path: Path) -> None:
    binary, _ = ensure_rust_population_finalizer()
    completed = subprocess.run(
        [str(binary), "--manifest", str(manifest_path)], capture_output=True, text=True
    )
    assert completed.returncode != 0, completed.stdout


@pytest.mark.parametrize("kind", ["traversal", "absolute", "wrong_root"])
def test_g0_native_finalizer_rejects_manifest_artifact_path_redirects(
    tmp_path: Path, kind: str
) -> None:
    manifest_path, _pool, _selection, _ledger, _shell = _g0_finalizer_fixture(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    selection_path = tmp_path / "g0-bootstrap" / "selection.json"
    if kind == "traversal":
        manifest["g0Artifacts"]["selection"]["path"] = "../../g0-bootstrap/../g0-bootstrap/selection.json"
    elif kind == "absolute":
        manifest["g0Artifacts"]["selection"]["path"] = str(selection_path.resolve())
    else:
        wrong_root = tmp_path / "wrong-root"
        wrong_root.mkdir()
        redirected = wrong_root / "selection.json"
        redirected.write_bytes(selection_path.read_bytes())
        manifest["g0Artifacts"]["selection"] = {
            "path": "../../wrong-root/selection.json",
            "fileSha256": "sha256:" + hashlib.sha256(redirected.read_bytes()).hexdigest(),
        }
    _write_self_hashed_manifest(manifest_path, manifest)
    _native_rejects(manifest_path)


def test_g0_native_finalizer_rejects_artifact_symlink_escape(tmp_path: Path) -> None:
    manifest_path, _pool, _selection, _ledger, _shell = _g0_finalizer_fixture(tmp_path)
    target = tmp_path / "g0-bootstrap" / "selection.json"
    escaped = tmp_path / "escaped-selection.json"
    escaped.write_bytes(target.read_bytes())
    target.unlink()
    try:
        os.symlink(escaped, target)
    except OSError as exc:
        pytest.skip(f"symlink creation is unavailable in this test environment: {exc}")
    _native_rejects(manifest_path)


@pytest.mark.parametrize(
    ("artifact_name", "mutation"),
    [
        ("accepted-pool.json", "pool_identity"),
        ("selection.json", "selection_self_hash"),
        ("campaign-construction-ledger.json", "ledger_self_hash"),
        ("campaign-construction-ledger.json", "ledger_disposition"),
        ("campaign-construction-ledger.json", "ledger_market_evidence"),
    ],
)
def test_g0_native_finalizer_rejects_artifact_hash_and_ledger_tamper(
    tmp_path: Path, artifact_name: str, mutation: str
) -> None:
    manifest_path, _pool, selection, _ledger, _shell = _g0_finalizer_fixture(tmp_path)
    artifact_path = tmp_path / "g0-bootstrap" / artifact_name
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    if mutation == "pool_identity":
        # Rehash the enclosing manifest's raw-file checksum, but leave the
        # pool's embedded semantic identity stale.  The native finalizer must
        # not let a file checksum replace the pool's own hash commitment.
        selected_ordinals = {row["proposalOrdinal"] for row in selection["selected"]}
        unselected = next(
            row
            for row in artifact["acceptedReferences"]
            if row["proposalOrdinal"] not in selected_ordinals
        )
        unselected["candidateId"] = "tampered_pool_member"
    elif mutation == "selection_self_hash":
        artifact["marketEvidenceRead"] = True
    elif mutation == "ledger_self_hash":
        artifact["rows"][0]["candidateId"] = "tampered_ledger_member"
    elif mutation == "ledger_disposition":
        row = artifact["rows"][0]
        row["evaluationDisposition"] = (
            "bootstrap_diversity_not_selected"
            if row["evaluationDisposition"] == "selected_for_market_evaluation"
            else "selected_for_market_evaluation"
        )
    else:
        artifact["rows"][0]["marketEvidenceRead"] = True
    artifact_path.write_text(canonical_json(artifact) + "\n", encoding="utf-8")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    key = {
        "accepted-pool.json": "acceptedPool",
        "selection.json": "selection",
        "campaign-construction-ledger.json": "ledger",
    }[artifact_name]
    manifest["g0Artifacts"][key]["fileSha256"] = (
        "sha256:" + hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    )
    _write_self_hashed_manifest(manifest_path, manifest)
    _native_rejects(manifest_path)


@pytest.mark.parametrize("kind", ["unselected", "duplicate", "omitted", "bad_reference", "bad_entry", "bad_candidate_identity"])
def test_g0_native_finalizer_rejects_self_rehashed_manifest_selection_tamper(
    tmp_path: Path, kind: str
) -> None:
    manifest_path, pool, selection, _ledger, _shell = _g0_finalizer_fixture(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    selected_ordinals = {row["proposalOrdinal"] for row in selection["selected"]}
    replacement = next(
        row for row in pool["acceptedReferences"] if row["proposalOrdinal"] not in selected_ordinals
    )
    if kind == "unselected":
        manifest["acceptedCandidates"][0] = {
            "proposalOrdinal": replacement["proposalOrdinal"],
            "candidateId": replacement["candidateId"],
            "candidateIdentitySha256": replacement["candidateIdentitySha256"],
            "acceptedPairEntrySha256": replacement["acceptedPairEntrySha256"],
            "referenceSha256": replacement["referenceSha256"],
        }
    elif kind == "duplicate":
        manifest["acceptedCandidates"][1] = dict(manifest["acceptedCandidates"][0])
    elif kind == "omitted":
        manifest["acceptedCandidates"].pop()
        manifest["candidateCount"] -= 1
    elif kind == "bad_reference":
        manifest["acceptedCandidates"][0]["referenceSha256"] = canonical_sha256({"bad": "reference"})
    elif kind == "bad_entry":
        manifest["acceptedCandidates"][0]["acceptedPairEntrySha256"] = canonical_sha256({"bad": "entry"})
    else:
        manifest["acceptedCandidates"][0]["candidateIdentitySha256"] = canonical_sha256({"bad": "candidate"})
    _write_self_hashed_manifest(manifest_path, manifest)
    _native_rejects(manifest_path)
