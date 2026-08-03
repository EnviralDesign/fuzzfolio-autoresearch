from __future__ import annotations

import copy

import pytest

from autoresearch.temporal_bidirectional_genome import (
    BidirectionalGenomeError,
    FrozenModule,
    FrozenPair,
    HoldMutationPlan,
    IdentitySnapshot,
    apply_pair_hold_mutation,
    canonical_sha256,
    deterministic_same_side_crossover,
    proposal_side,
)
from autoresearch.temporal_qd_evolution import (
    _bidirectional_pair_policy,
    _require_bidirectional_candidate,
    materialize_bidirectional_qd_candidate,
)
from autoresearch.temporal_qd_pair_generation import (
    PairModuleOperator,
    TypedPairFactory,
    generate_pair_population,
    materialize_pair_candidate,
    propose_pair,
    propose_same_side_crossover,
    replay_pair_proposal,
)


class FakeNativeValidator:
    def validate_v2(self, *, profile, candidate_id):
        raw = canonical_sha256(profile)
        return {
            "schemaVersion": "temporal_search_candidate_validation_v1",
            "candidateId": candidate_id,
            "rawSourceProfileSha256": raw,
            "profileSnapshotSha256": raw,
            "programSha256": canonical_sha256({"nativeV2": profile}),
            "validationReportSha256": canonical_sha256({"nativeValidation": profile}),
            "status": "valid_evaluable",
            "candidateAcceptable": True,
        }


class FakePairCompiler:
    def compile_pair(self, *, long_profile, short_profile, candidate_id):
        profile = {
            "version": "v3",
            "directionMode": "both",
            "name": candidate_id,
            "graph": {
                "entryArbitration": {
                    "modules": [
                        {"direction": "long", "sourceProfileSnapshotSha256": canonical_sha256(long_profile)},
                        {"direction": "short", "sourceProfileSnapshotSha256": canonical_sha256(short_profile)},
                    ]
                }
            },
        }
        return {
            "profile": profile,
            "validation": {
                "schemaVersion": "temporal_search_candidate_validation_v1",
                "rawSourceProfileSha256": canonical_sha256(profile),
                "profileSnapshotSha256": canonical_sha256({"snapshot": profile}),
                "programSha256": canonical_sha256({"nativeV3": profile}),
                "validationReportSha256": canonical_sha256({"nativeV3Validation": profile}),
                "status": "valid_evaluable",
                "candidateAcceptable": True,
            },
        }


class FirstProgramCrossover:
    def crossover(self, left_program, right_program, *, direction, proposal_seed):
        assert left_program["direction"] == right_program["direction"] == direction
        return copy.deepcopy(left_program)


def _snapshot(kind: str, value: str) -> IdentitySnapshot:
    return IdentitySnapshot.create(kind=kind, schema_version=f"{kind}_v1", payload={"value": value})


def _program(side: str, marker: str = "base") -> dict:
    threshold = {"base": 35.0, "first": 50.0, "second": 65.0}.get(marker, 75.0)
    return {
        "schemaVersion": "temporal_typed_fragment_grammar_v2",
        "grammarVersion": "2",
        "direction": side,
        "fragments": [{"productionId": "arm_level", "resources": {"group": "g"}, "choices": {"threshold": threshold}}],
    }


def _profile(side: str, *, hold=None) -> dict:
    plan = {"id": "base", "initialStop": {"kind": "fixed_percent", "percent": 1.0}}
    if hold is not None:
        plan["hold"] = hold
    return {
        "version": "v2",
        "directionMode": side,
        "executionConfig": {"managementLibrary": {"plans": [plan]}},
        "graph": {"states": []},
    }


def _module(
    side: str,
    *,
    marker: str = "base",
    context: IdentitySnapshot | None = None,
    catalog: IdentitySnapshot | None = None,
    authority: IdentitySnapshot | None = None,
    lineage=(),
    hold=None,
    profile=None,
) -> FrozenModule:
    return FrozenModule.validate_native(
        program=_program(side, marker),
        profile=profile or _profile(side, hold=hold),
        grammar_context=context or _snapshot("grammarContext", "context-a"),
        catalog=catalog or _snapshot("catalog", "catalog-a"),
        policy=_snapshot("policy", "policy-a"),
        native_authority_identity=authority or _snapshot("nativeAuthority", "authority-a"),
        native_validator=FakeNativeValidator(),
        candidate_id=f"module_{side}_{marker}",
        lineage=lineage,
    )


def _pair(long: FrozenModule | None = None, short: FrozenModule | None = None, *, lineage=()) -> FrozenPair:
    return FrozenPair.compile(
        long=long or _module("long"),
        short=short or _module("short"),
        pair_compiler_identity=_snapshot("pairCompiler", "dashboard-v3-a"),
        pair_compiler=FakePairCompiler(),
        candidate_id="pair_test",
        side_targeted_lineage=lineage,
    )


def test_pair_identity_binds_both_modules_context_hold_catalog_authority_and_lineage() -> None:
    baseline = _pair()
    changed = [
        _pair(_module("long", marker="long-change"), _module("short")),
        _pair(_module("long"), _module("short", marker="short-change")),
        _pair(_module("long", context=_snapshot("grammarContext", "context-b")), _module("short")),
        _pair(_module("long", catalog=_snapshot("catalog", "catalog-b")), _module("short")),
        _pair(_module("long", authority=_snapshot("nativeAuthority", "authority-b")), _module("short")),
        _pair(_module("long", lineage=({"operation": "typed_edit", "side": "long"},)), _module("short")),
    ]
    hold_plan = HoldMutationPlan.create(baseline.long, plan_id="base", new_hold={"kind": "market_bars", "bars": 13})
    changed.append(apply_pair_hold_mutation(baseline, hold_plan, native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler(), candidate_id="pair_held"))
    assert all(pair.identity_sha256 != baseline.identity_sha256 for pair in changed)


def test_round_trip_is_exact_and_tampering_or_mutable_aliases_fail_closed() -> None:
    pair = _pair(lineage=({"operation": "proposal", "side": "long", "seed": "77"},))
    payload = pair.canonical_payload()
    restored = FrozenPair.from_payload(payload)
    assert restored.canonical_payload() == payload
    assert restored.identity_sha256 == pair.identity_sha256

    tampered = copy.deepcopy(payload)
    tampered["long"]["profile"]["directionMode"] = "short"
    with pytest.raises(BidirectionalGenomeError, match="matching v2 module|did not admit"):
        FrozenPair.from_payload(tampered)

    with pytest.raises(BidirectionalGenomeError, match="mutable alias"):
        IdentitySnapshot.create(kind="catalog", schema_version="catalog_v1", payload={"catalogAlias": "latest"})


def test_side_routing_and_same_side_crossover_are_deterministic_and_closed() -> None:
    assert proposal_side("proposal-42") == proposal_side("proposal-42")
    assert proposal_side(42) in {"long", "short"}
    first = _module("long", marker="first")
    second = _module("long", marker="second")
    child_a, record_a = deterministic_same_side_crossover(first, second, proposal_seed="11", crossover=FirstProgramCrossover())
    child_b, record_b = deterministic_same_side_crossover(second, first, proposal_seed="11", crossover=FirstProgramCrossover())
    assert child_a == child_b
    assert record_a == record_b
    assert child_a["direction"] == "long"
    with pytest.raises(BidirectionalGenomeError, match="opposite-side"):
        deterministic_same_side_crossover(first, _module("short"), proposal_seed="11", crossover=FirstProgramCrossover())


def test_v2_only_or_same_side_economic_candidates_are_rejected() -> None:
    long = _module("long")
    with pytest.raises(BidirectionalGenomeError, match="exactly one long and one short"):
        _pair(long, long)


def test_hold_operator_is_bounded_per_plan_and_preserves_asymmetric_modules_without_v3_top_level_hold() -> None:
    pair = _pair(_module("long", hold={"kind": "market_bars", "bars": 5}), _module("short", hold={"kind": "elapsed_calendar", "minutes": 30}))
    plan = HoldMutationPlan.create(pair.long, plan_id="base", new_hold={"kind": "none"})
    assert plan.old_hold_sha256 == canonical_sha256({"kind": "market_bars", "bars": 5})
    assert plan.new_hold_sha256 == canonical_sha256({"kind": "none"})
    assert HoldMutationPlan.from_payload(plan.canonical_payload()).canonical_payload() == plan.canonical_payload()
    changed = apply_pair_hold_mutation(pair, plan, native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler(), candidate_id="pair_asymmetric_hold")
    assert changed.long.profile["executionConfig"]["managementLibrary"]["plans"][0]["hold"] == {"kind": "none"}
    assert changed.short.profile["executionConfig"]["managementLibrary"]["plans"][0]["hold"] == {"kind": "elapsed_calendar", "minutes": 30}
    assert "hold" not in changed.profile
    with pytest.raises(BidirectionalGenomeError, match="1..512"):
        HoldMutationPlan.create(pair.long, plan_id="base", new_hold={"kind": "market_bars", "bars": 513})


def test_qd_economic_candidate_requires_exact_frozen_v3_pair_and_policy_binding() -> None:
    pair = _pair(lineage=({"operation": "typed_grammar", "side": "long", "proposalSeed": "1"},))
    raw_policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": pair.pair_compiler.canonical_payload(),
    }
    candidate = materialize_bidirectional_qd_candidate(
        pair=pair,
        pair_policy=raw_policy,
        origin_kind="structural_offspring",
        generation_index=1,
        birth_ordinal=0,
        proposal_ordinal=2,
    )
    policy = _bidirectional_pair_policy({"bidirectionalPairPolicy": raw_policy})
    assert policy is not None
    assert _require_bidirectional_candidate(candidate, policy).identity_sha256 == pair.identity_sha256
    assert candidate["sourceProfile"]["directionMode"] == "both"

    tampered = copy.deepcopy(candidate)
    tampered["sourceProfile"] = pair.long.canonical_payload()["profile"]
    with pytest.raises(Exception, match="v3/both|compiled identities|frozen v3"):
        _require_bidirectional_candidate(tampered, policy)


class _PairFactory:
    def create_pair(self, *, proposal_seed: str) -> FrozenPair:
        return _pair(lineage=({"operation": "factory", "side": "long", "proposalSeed": proposal_seed},))


class _DuplicatePairFactory:
    def create_pair(self, *, proposal_seed: str) -> FrozenPair:
        # Provenance differs, while both executable module profiles remain
        # identical.  Unique proposal slots must follow the latter.
        return _pair(lineage=({"operation": "factory", "side": "long", "proposalSeed": proposal_seed},))


class _PairOps:
    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled

    def grammar_plans(self, module: FrozenModule):
        return [{"op": "grammar", "side": module.direction}] if self.enabled else []

    def apply_grammar(self, module: FrozenModule, plan, *, candidate_id: str):
        changed = _module(module.direction, marker="first", lineage=({"operation": "grammar", "side": module.direction},))
        return changed, {"schemaVersion": "audit", "kind": "grammar", "candidateId": candidate_id}

    def indicator_plans(self, module: FrozenModule):
        return [{"op": "indicator", "side": module.direction}] if self.enabled else []

    def apply_indicator(self, module: FrozenModule, plan, *, candidate_id: str):
        changed = _module(module.direction, marker="second", lineage=({"operation": "indicator", "side": module.direction},))
        return changed, {"schemaVersion": "audit", "kind": "indicator", "candidateId": candidate_id}

    def crossover(self, left_program, right_program, *, direction: str, proposal_seed: str):
        assert left_program["direction"] == right_program["direction"] == direction
        return copy.deepcopy(left_program)

    def compile_program(self, template: FrozenModule, program, *, candidate_id: str):
        return _module(template.direction, marker="first", lineage=({"operation": "crossover", "side": template.direction},))


def test_pair_generation_immigrant_side_mutation_replay_and_no_eligible_are_exact() -> None:
    ops = _PairOps()
    immigrant, immigrant_entry = propose_pair(
        proposal_seed="seed-immigrant", parent=None, pair_factory=_PairFactory(), module_authority=ops,
        native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler(),
    )
    assert immigrant is not None and immigrant_entry["originKind"] == "random_immigrant"
    assert replay_pair_proposal(payload=immigrant_entry, module_authority=ops, native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler()).identity_sha256 == immigrant.identity_sha256

    parent = _pair()
    changed, entry = propose_pair(
        proposal_seed="seed-mutation", parent=parent, pair_factory=None, module_authority=ops,
        native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler(),
        replay_operation={"kind": "typed_grammar", "plan": {"op": "grammar", "side": proposal_side("seed-mutation")}},
    )
    assert changed is not None and entry["disposition"] == "materialized"
    assert entry["untouchedOppositeModuleIdentitySha256"] == (parent.short if proposal_side("seed-mutation") == "long" else parent.long).identity_sha256
    assert replay_pair_proposal(payload=entry, module_authority=ops, native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler()).canonical_payload() == changed.canonical_payload()
    tampered_entry = copy.deepcopy(entry)
    tampered_entry["operation"]["kind"] = "indicator_learning"
    with pytest.raises(Exception, match="identity mismatch|replay diverged"):
        replay_pair_proposal(payload=tampered_entry, module_authority=ops, native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler())
    policy = {"schemaVersion": "temporal_qd_bidirectional_pair_policy_v1", "enabled": True, "compilerAuthority": changed.pair_compiler.canonical_payload()}
    candidate = materialize_pair_candidate(pair=changed, proposal=entry, pair_policy=policy, generation_index=1, birth_ordinal=0, proposal_ordinal=0)
    assert candidate["sourceProfile"]["directionMode"] == "both"
    assert candidate["pairProposalSha256"] == entry["proposalSha256"]

    indicator_seed = "seed-indicator"
    indicator_side = proposal_side(indicator_seed)
    indicator_pair, indicator_entry = propose_pair(
        proposal_seed=indicator_seed, parent=parent, pair_factory=None, module_authority=ops,
        native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler(),
        replay_operation={"kind": "indicator_learning", "plan": {"op": "indicator", "side": indicator_side}},
    )
    assert indicator_pair is not None
    assert indicator_entry["operationAudit"]["kind"] == "indicator"

    hold_seed = "seed-hold"
    hold_side = proposal_side(hold_seed)
    hold_target = parent.long if hold_side == "long" else parent.short
    hold_bars = 1 + int(canonical_sha256({"module": hold_target.identity_sha256, "plan": "base"})[-2:], 16) % 32
    hold_pair, hold_entry = propose_pair(
        proposal_seed=hold_seed, parent=parent, pair_factory=None, module_authority=ops,
        native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler(),
        replay_operation={"kind": "hold", "planId": "base", "newHold": {"kind": "market_bars", "bars": hold_bars}},
    )
    assert hold_pair is not None and hold_entry["holdMutationPlan"]["side"] == hold_side
    changed_module = hold_pair.long if hold_side == "long" else hold_pair.short
    untouched_module = hold_pair.short if hold_side == "long" else hold_pair.long
    assert changed_module.profile["executionConfig"]["managementLibrary"]["plans"][0]["hold"] == {"kind": "market_bars", "bars": hold_bars}
    assert "hold" not in untouched_module.profile["executionConfig"]["managementLibrary"]["plans"][0]

    no_hold_parent = _pair(
        _module("long", profile={"version": "v2", "directionMode": "long", "graph": {}}),
        _module("short", profile={"version": "v2", "directionMode": "short", "graph": {}}),
    )
    none, no_entry = propose_pair(
        proposal_seed="seed-none", parent=no_hold_parent, pair_factory=None, module_authority=_PairOps(enabled=False),
        native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler(),
    )
    assert none is None and no_entry["disposition"] == "no_eligible_side_operation"


def test_pair_generation_same_side_crossover_never_crosses_modules() -> None:
    parent = _pair(_module("long", marker="first"), _module("short", marker="first"))
    mate = _pair(_module("long", marker="second"), _module("short", marker="second"))
    pair, audit = propose_same_side_crossover(
        proposal_seed="seed-cross", parent=parent, mate=mate, module_authority=_PairOps(), pair_compiler=FakePairCompiler(),
    )
    side = proposal_side("seed-cross")
    assert audit["sameSide"] is True and audit["side"] == side
    assert (pair.short if side == "long" else pair.long).identity_sha256 == (parent.short if side == "long" else parent.long).identity_sha256


def test_pair_population_journal_is_restart_safe_and_never_emits_v2_tasks(tmp_path) -> None:
    pair = _pair()
    policy = {"schemaVersion": "temporal_qd_bidirectional_pair_policy_v1", "enabled": True, "compilerAuthority": pair.pair_compiler.canonical_payload()}
    args = dict(output_root=tmp_path, generation_index=1, target_unique_candidates=1, run_config={"seed": "pair-run"}, pair_policy=policy, pair_factory=_PairFactory(), module_authority=_PairOps(), native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler(), operator_implementation_identity={"schemaVersion": "test_pair_operator_v1", "grammar": "frozen", "indicator": "frozen", "hold": "frozen"})
    first = generate_pair_population(**args)
    second = generate_pair_population(**args)
    assert first["completed"] is True and second["populationSha256"] == first["populationSha256"]
    import json
    population = json.loads((tmp_path / "population.json").read_text(encoding="utf-8"))
    assert population["candidates"][0]["sourceProfile"]["version"] == "v3"
    assert population["candidates"][0]["sourceProfile"]["directionMode"] == "both"


def test_pair_resume_rejects_provenance_distinct_duplicate_executable_semantics(tmp_path) -> None:
    pair = _pair()
    policy = {"schemaVersion": "temporal_qd_bidirectional_pair_policy_v1", "enabled": True, "compilerAuthority": pair.pair_compiler.canonical_payload()}
    args = dict(output_root=tmp_path, generation_index=1, target_unique_candidates=1, run_config={"seed": "duplicate-semantics"}, pair_policy=policy, pair_factory=_PairFactory(), module_authority=_PairOps(), native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler(), operator_implementation_identity={"schemaVersion": "test_pair_operator_v1", "grammar": "frozen", "indicator": "frozen", "hold": "frozen"})
    first = generate_pair_population(**args)
    assert first["candidateCount"] == 1
    import json
    original = json.loads((tmp_path / "proposal-journal" / "00000000.json").read_text(encoding="utf-8"))
    duplicate_pair, duplicate_proposal = propose_pair(proposal_seed="provenance-distinct", parent=None, pair_factory=_PairFactory(), module_authority=_PairOps(), native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler())
    assert duplicate_pair is not None
    duplicate_candidate = materialize_pair_candidate(pair=duplicate_pair, proposal=duplicate_proposal, pair_policy=policy, generation_index=1, birth_ordinal=1, proposal_ordinal=1)
    assert duplicate_candidate["candidateIdentitySha256"] != original["candidate"]["candidateIdentitySha256"]
    duplicate_entry = {"schemaVersion": "temporal_qd_proposal_entry_v3", "configSha256": original["configSha256"], "generationIndex": 1, "proposalOrdinal": 1, "originKind": "random_immigrant", "proposal": duplicate_proposal, "operatorImplementationSha256": original["operatorImplementationSha256"], "disposition": "accepted", "candidate": duplicate_candidate}
    duplicate_entry["entrySha256"] = canonical_sha256(duplicate_entry)
    (tmp_path / "proposal-journal" / "00000001.json").write_text(json.dumps(duplicate_entry, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    assert FrozenPair.from_payload(original["candidate"]["bidirectionalGenome"]).identity_sha256 != duplicate_pair.identity_sha256
    with pytest.raises(Exception, match="duplicate executable pair semantics"):
        generate_pair_population(**args)


def test_pair_population_does_not_spend_unique_slots_on_duplicate_genomes(tmp_path) -> None:
    pair = _pair()
    policy = {"schemaVersion": "temporal_qd_bidirectional_pair_policy_v1", "enabled": True, "compilerAuthority": pair.pair_compiler.canonical_payload()}
    result = generate_pair_population(
        output_root=tmp_path,
        generation_index=1,
        target_unique_candidates=2,
        run_config={"seed": "duplicate-pair-run"},
        pair_policy=policy,
        pair_factory=_DuplicatePairFactory(),
        module_authority=_PairOps(),
        native_validator=FakeNativeValidator(),
        pair_compiler=FakePairCompiler(),
        operator_implementation_identity={"schemaVersion": "test_pair_operator_v1", "grammar": "frozen", "indicator": "frozen", "hold": "frozen"},
        max_new_proposals=2,
    )
    assert result["completed"] is False
    entries = sorted((tmp_path / "proposal-journal").glob("*.json"))
    assert len(entries) == 2
    import json
    rows = [json.loads(path.read_text(encoding="utf-8")) for path in entries]
    assert [row["disposition"] for row in rows] == ["accepted", "duplicate_pair_genome"]
