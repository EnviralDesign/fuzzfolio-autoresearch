from __future__ import annotations

import ast
import copy
import json
from pathlib import Path

import pytest

from autoresearch.temporal_bidirectional_genome import (
    BidirectionalGenomeError,
    FrozenModule,
    FrozenPair,
    HoldMutationPlan,
    IdentitySnapshot,
    apply_hold_mutation,
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
    TypedGrammarPairOperator,
    TypedPairFactory,
    generate_pair_population,
    materialize_pair_candidate,
    propose_pair,
    propose_same_side_crossover,
    replay_pair_proposal,
)
from autoresearch.temporal_qd_pair_factory import default_hold_operator_policy
from autoresearch.temporal_qd_pair_generation import _hold_plans
from autoresearch.temporal_qd_pair_generation import _propose_crossover
from autoresearch.temporal_qd_pair_generation import _propose_pair_sequence
from autoresearch.temporal_qd_pair_generation import _mutation_depth_from_bucket
from autoresearch.temporal_qd_pair_generation import _explicit_parent_draw_count
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_discovery_validation import DashboardV2ModuleValidator, SubprocessCandidateValidator


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
    plan = {
        "id": "base",
        "initialStop": {"kind": "fixed_percent", "percent": 1.0},
        "initialTarget": {"kind": "reward_multiple", "multiple": 2.0},
    }
    if hold is not None:
        plan["holdPolicy"] = hold
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
    hold_plan = HoldMutationPlan.create(baseline.long, plan_id="base", new_hold={"kind": "market_bars", "bars": 13, "timeframe": "M5"})
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


def test_hold_operator_uses_native_hold_policy_and_preserves_asymmetric_modules_without_v3_top_level_hold() -> None:
    pair = _pair(_module("long", hold={"kind": "market_bars", "bars": 5, "timeframe": "M5"}), _module("short", hold={"kind": "elapsed_calendar", "hours": 0.5}))
    plan = HoldMutationPlan.create(pair.long, plan_id="base", new_hold={"kind": "none"})
    assert plan.old_hold_sha256 == canonical_sha256({"kind": "market_bars", "bars": 5, "timeframe": "M5"})
    assert plan.new_hold_sha256 == canonical_sha256({"kind": "none"})
    assert HoldMutationPlan.from_payload(plan.canonical_payload()).canonical_payload() == plan.canonical_payload()
    changed = apply_pair_hold_mutation(pair, plan, native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler(), candidate_id="pair_asymmetric_hold")
    assert "holdPolicy" not in changed.long.profile["executionConfig"]["managementLibrary"]["plans"][0]
    assert changed.short.profile["executionConfig"]["managementLibrary"]["plans"][0]["holdPolicy"] == {"kind": "elapsed_calendar", "hours": 0.5}
    assert "holdPolicy" not in changed.profile
    # The QD one-week cap is in its frozen choices, not a global cap on a
    # pre-existing Dashboard-native seed policy.
    assert HoldMutationPlan.create(pair.long, plan_id="base", new_hold={"kind": "market_bars", "bars": 2017, "timeframe": "M5"}).new_hold["bars"] == 2017


def test_hold_plans_are_frozen_finite_native_choices_and_skip_noops() -> None:
    ops = _PairOps()
    empty = _module("long")
    first = _hold_plans(empty, ops)
    assert first == _hold_plans(empty, ops)
    assert len(first) == len(default_hold_operator_policy()["choices"]) - 1
    assert {item["newHold"]["kind"] for item in first} == {"market_bars", "elapsed_calendar"}

    one_bar = _module("long", hold={"kind": "market_bars", "bars": 1, "timeframe": "M5"})
    choices = _hold_plans(one_bar, ops)
    assert {"kind": "hold", "planId": "base", "newHold": {"kind": "none"}} in choices
    assert {"kind": "hold", "planId": "base", "newHold": {"kind": "market_bars", "bars": 1, "timeframe": "M5"}} not in choices
    assert max(item["newHold"].get("bars", 0) for item in choices) == 2016
    assert max(item["newHold"].get("hours", 0.0) for item in choices) == 168.0


def test_hold_mutations_are_admitted_by_dashboard_native_management_validator() -> None:
    dashboard_root = Path("C:/repos/Trading-Dashboard")
    fixture = dashboard_root / "shared/python/fuzzfolio_core/tests/test_temporal_search_candidate_validation.py"
    tree = ast.parse(fixture.read_text(encoding="utf-8"))
    functions = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in {"_transition", "_candidate_profile"}
    ]
    namespace: dict[str, object] = {}
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(fixture), "exec"), namespace)
    source = FrozenModule.validate_native(
        program=_program("long"),
        profile=namespace["_candidate_profile"](),  # type: ignore[operator]
        grammar_context=_snapshot("grammarContext", "dashboard-hold-context"),
        catalog=_snapshot("catalog", "dashboard-hold-catalog"),
        policy=_snapshot("policy", "dashboard-hold-policy"),
        native_authority_identity=_snapshot("nativeAuthority", "dashboard-hold-authority"),
        native_validator=FakeNativeValidator(),
        candidate_id="dashboard_native_hold_source",
    )
    client = SubprocessCandidateValidator(
        [str(dashboard_root / "compute-service/.venv/Scripts/python.exe"), str(dashboard_root / "scripts/temporal_search_validate_candidate.py")],
        timeout_seconds=10,
    )
    native_validator = DashboardV2ModuleValidator(client)
    for hold in (
        {"kind": "market_bars", "bars": 12, "timeframe": "M5"},
        {"kind": "elapsed_calendar", "hours": 24.0},
    ):
        mutation = HoldMutationPlan.create(source, plan_id="core_plan", new_hold=hold)
        changed = apply_hold_mutation(source, mutation, native_validator=native_validator, candidate_id=f"dashboard_native_{hold['kind']}")
        report = native_validator.validate_v2(profile=changed.canonical_payload()["profile"], candidate_id=f"dashboard_recheck_{hold['kind']}")
        assert report["candidateAcceptable"] is True
        assert changed.canonical_payload()["profile"]["executionConfig"]["managementLibrary"]["plans"][0]["holdPolicy"] == hold


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


class _TripwireDuplicatePairFactory(_DuplicatePairFactory):
    construction_policy = {
        "schemaVersion": "test_rich_immigrant_policy_v1",
        "collisionTripwire": {
            "minimumImmigrantAttempts": 2,
            "minimumAcceptedRatio": 0.75,
        },
    }


class _UniquePairFactory:
    """Small deterministic authority used by old/new artifact equivalence."""

    def create_pair(self, *, proposal_seed: str) -> FrozenPair:
        def profile(side: str) -> dict:
            value = _profile(side)
            value["graph"]["proposalSeed"] = proposal_seed
            return value

        return _pair(
            _module("long", profile=profile("long")),
            _module("short", profile=profile("short")),
            lineage=(
                {
                    "operation": "factory",
                    "side": "long",
                    "proposalSeed": proposal_seed,
                },
            ),
        )


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

    def hold_policy_choices(self, module: FrozenModule):
        del module
        return copy.deepcopy(default_hold_operator_policy()["choices"])

    def apply_indicator(self, module: FrozenModule, plan, *, candidate_id: str):
        changed = _module(module.direction, marker="second", lineage=({"operation": "indicator", "side": module.direction},))
        return changed, {"schemaVersion": "audit", "kind": "indicator", "candidateId": candidate_id}

    def crossover(self, left_program, right_program, *, direction: str, proposal_seed: str):
        assert left_program["direction"] == right_program["direction"] == direction
        return copy.deepcopy(left_program)

    def compile_program(self, template: FrozenModule, program, *, candidate_id: str):
        return _module(template.direction, marker="first", lineage=({"operation": "crossover", "side": template.direction},))


class _RejectingCrossoverPairOps(_PairOps):
    """Reject only crossover compilation; ordinary side mutation remains valid."""

    def compile_program(self, template: FrozenModule, program, *, candidate_id: str):
        del template, program, candidate_id
        raise TemporalDiscoveryContractError("expected test crossover rejection")


class _MaterializingCrossoverPairOps(_PairOps):
    def compile_program(self, template: FrozenModule, program, *, candidate_id: str):
        del program, candidate_id
        return _module(
            template.direction,
            marker="crossover-materialized",
            profile={"version": "v2", "directionMode": template.direction, "graph": {"crossover": "materialized"}},
            lineage=({"operation": "crossover", "side": template.direction},),
        )


class _EarlyRejectingSequenceOps(_PairOps):
    """One eligible mutation which deterministically rejects at its first step."""

    def indicator_plans(self, module: FrozenModule):
        del module
        return []

    def hold_policy_choices(self, module: FrozenModule):
        del module
        return []

    def apply_grammar(self, module: FrozenModule, plan, *, candidate_id: str):
        del module, plan, candidate_id
        raise TemporalDiscoveryContractError("expected test sequence rejection")


class _EarlyNoopSequenceOps(_PairOps):
    """One eligible mutation which canonically leaves executable profiles unchanged."""

    def indicator_plans(self, module: FrozenModule):
        del module
        return []

    def hold_policy_choices(self, module: FrozenModule):
        del module
        return []


class _MaterializingSequenceOps(_EarlyNoopSequenceOps):
    def apply_grammar(self, module: FrozenModule, plan, *, candidate_id: str):
        del plan
        changed = _module(
            module.direction,
            marker="sequence-materialized",
            profile={"version": "v2", "directionMode": module.direction, "graph": {"candidateId": candidate_id}},
            lineage=({"operation": "grammar", "side": module.direction},),
        )
        return changed, {"schemaVersion": "audit", "kind": "grammar", "candidateId": candidate_id}


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
    hold = {"kind": "market_bars", "bars": 1, "timeframe": "M5"}
    hold_pair, hold_entry = propose_pair(
        proposal_seed=hold_seed, parent=parent, pair_factory=None, module_authority=ops,
        native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler(),
        replay_operation={"kind": "hold", "planId": "base", "newHold": hold},
    )
    assert hold_pair is not None and hold_entry["holdMutationPlan"]["side"] == hold_side
    changed_module = hold_pair.long if hold_side == "long" else hold_pair.short
    untouched_module = hold_pair.short if hold_side == "long" else hold_pair.long
    assert changed_module.profile["executionConfig"]["managementLibrary"]["plans"][0]["holdPolicy"] == hold
    assert "holdPolicy" not in untouched_module.profile["executionConfig"]["managementLibrary"]["plans"][0]

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


def test_crossover_materialized_and_noop_proposals_use_the_versioned_kind() -> None:
    first = _pair(_module("long", marker="first"), _module("short", marker="first"))
    second = _pair(_module("long", marker="second"), _module("short", marker="second"))
    common = dict(
        proposal_seed="seed-cross-disposition",
        pair_compiler=FakePairCompiler(),
        parent_selection=None,
        mate_selection=None,
        mate_selection_attempts=[],
    )
    materialized, materialized_proposal = _propose_crossover(parent=second, mate=first, module_authority=_MaterializingCrossoverPairOps(), **common)
    noop, noop_proposal = _propose_crossover(parent=first, mate=second, module_authority=_PairOps(), **common)

    assert materialized is not None and materialized_proposal["disposition"] == "materialized"
    assert noop is None and noop_proposal["disposition"] == "no_op_proposal"
    for proposal, expected, authority in ((materialized_proposal, materialized, _MaterializingCrossoverPairOps()), (noop_proposal, None, _PairOps())):
        assert proposal["proposalKind"] == "temporal_qd_same_side_crossover_v1"
        replayed = replay_pair_proposal(
            payload=proposal,
            module_authority=authority,
            native_validator=FakeNativeValidator(),
            pair_compiler=FakePairCompiler(),
        )
        assert (replayed.canonical_payload() if replayed is not None else None) == (expected.canonical_payload() if expected is not None else None)


def test_pair_multi_operation_mutation_is_replayable_and_records_each_step() -> None:
    parent = _pair()
    child, proposal = _propose_pair_sequence(
        proposal_seed="depth-two",
        parent=parent,
        mutation_depth=2,
        module_authority=_PairOps(),
        native_validator=FakeNativeValidator(),
        pair_compiler=FakePairCompiler(),
    )
    assert child is not None
    assert proposal["mutationDepth"] == 2
    assert len(proposal["mutationSteps"]) == 2
    assert replay_pair_proposal(
        payload=proposal,
        module_authority=_PairOps(),
        native_validator=FakeNativeValidator(),
        pair_compiler=FakePairCompiler(),
    ).canonical_payload() == child.canonical_payload()


@pytest.mark.parametrize(
    ("mutation_depth", "authority_type", "terminal_disposition"),
    [
        (2, _EarlyRejectingSequenceOps, "operation_rejected"),
        (3, _EarlyRejectingSequenceOps, "operation_rejected"),
        (2, _EarlyNoopSequenceOps, "no_op_proposal"),
        (3, _EarlyNoopSequenceOps, "no_op_proposal"),
    ],
)
def test_early_terminal_sequence_resume_matches_uninterrupted(
    tmp_path, monkeypatch, mutation_depth, authority_type, terminal_disposition,
) -> None:
    monkeypatch.setattr(
        "autoresearch.temporal_qd_pair_generation._mutation_depth_for_seed",
        lambda seed: mutation_depth,
    )
    parent = _pair()
    policy = {"schemaVersion": "temporal_qd_bidirectional_pair_policy_v1", "enabled": True, "compilerAuthority": parent.pair_compiler.canonical_payload()}
    common = dict(
        generation_index=1,
        target_unique_candidates=1,
        run_config={"seed": f"early-terminal-{mutation_depth}-{terminal_disposition}"},
        pair_policy=policy,
        parent_pairs=[parent],
        pair_factory=_PairFactory(),
        module_authority=authority_type(),
        native_validator=FakeNativeValidator(),
        pair_compiler=FakePairCompiler(),
        operator_implementation_identity={"schemaVersion": "test_pair_operator_v1", "grammar": "frozen", "indicator": "frozen", "hold": "frozen"},
    )

    generate_pair_population(output_root=tmp_path / "split", max_new_proposals=1, **common)
    generate_pair_population(output_root=tmp_path / "split", max_new_proposals=2, **common)
    generate_pair_population(output_root=tmp_path / "full", max_new_proposals=3, **common)

    import json

    split = [json.loads(path.read_text(encoding="utf-8")) for path in sorted((tmp_path / "split/proposal-journal").glob("*.json"))]
    full = [json.loads(path.read_text(encoding="utf-8")) for path in sorted((tmp_path / "full/proposal-journal").glob("*.json"))]
    assert [row["entrySha256"] for row in split] == [row["entrySha256"] for row in full]
    assert all(row["proposal"]["mutationDepth"] == mutation_depth for row in split)
    assert all(len(row["proposal"]["mutationSteps"]) == 1 for row in split)
    assert all(row["proposal"]["disposition"] == terminal_disposition for row in split)


def test_sequence_replay_rejects_truncated_or_tampered_terminal_prefixes() -> None:
    parent = _pair()
    materialized, complete = _propose_pair_sequence(
        proposal_seed="complete-materialized-sequence",
        parent=parent,
        mutation_depth=2,
        module_authority=_MaterializingSequenceOps(),
        native_validator=FakeNativeValidator(),
        pair_compiler=FakePairCompiler(),
    )
    assert materialized is not None and complete["disposition"] == "materialized"

    truncated = copy.deepcopy(complete)
    truncated["mutationSteps"] = truncated["mutationSteps"][:1]
    truncated.pop("proposalSha256")
    truncated["proposalSha256"] = canonical_sha256(truncated)
    with pytest.raises(TemporalDiscoveryContractError, match="truncated before a terminal disposition"):
        replay_pair_proposal(payload=truncated, module_authority=_MaterializingSequenceOps(), native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler())

    extra = copy.deepcopy(complete)
    extra["mutationSteps"].append(copy.deepcopy(extra["mutationSteps"][-1]))
    extra.pop("proposalSha256")
    extra["proposalSha256"] = canonical_sha256(extra)
    with pytest.raises(TemporalDiscoveryContractError, match="more steps than its planned mutation depth"):
        replay_pair_proposal(payload=extra, module_authority=_MaterializingSequenceOps(), native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler())

    _, rejected = _propose_pair_sequence(
        proposal_seed="early-rejected-sequence",
        parent=parent,
        mutation_depth=2,
        module_authority=_EarlyRejectingSequenceOps(),
        native_validator=FakeNativeValidator(),
        pair_compiler=FakePairCompiler(),
    )
    after_terminal = copy.deepcopy(rejected)
    after_terminal["mutationSteps"].append(copy.deepcopy(after_terminal["mutationSteps"][0]))
    after_terminal.pop("proposalSha256")
    after_terminal["proposalSha256"] = canonical_sha256(after_terminal)
    with pytest.raises(TemporalDiscoveryContractError, match="stored steps after a terminal disposition"):
        replay_pair_proposal(payload=after_terminal, module_authority=_EarlyRejectingSequenceOps(), native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler())

    tampered = copy.deepcopy(rejected)
    tampered["mutationSteps"][0]["operation"]["plan"]["op"] = "not-the-frozen-operation"
    tampered.pop("proposalSha256")
    tampered["proposalSha256"] = canonical_sha256(tampered)
    with pytest.raises(TemporalDiscoveryContractError, match="stored pair proposal operation is no longer exact/canonical"):
        replay_pair_proposal(payload=tampered, module_authority=_EarlyRejectingSequenceOps(), native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler())


def test_pair_mutation_depth_buckets_prove_exact_frozen_70_25_5_schedule() -> None:
    depths = [_mutation_depth_from_bucket(bucket) for bucket in range(20)]
    assert {depth: depths.count(depth) for depth in (1, 2, 3)} == {1: 14, 2: 5, 3: 1}


def test_archive_parent_selection_resume_matches_uninterrupted_across_crossover(tmp_path) -> None:
    first, second = _pair(_module("long", marker="first"), _module("short", marker="first")), _pair(_module("long", marker="second"), _module("short", marker="second"))
    def member(pair, candidate_id):
        return {"candidateId": candidate_id, "candidate": {"candidateId": candidate_id, "bidirectionalGenome": pair.canonical_payload()}, "archiveLane": "quality", "finiteDataValidity": {"isFiniteData": True, "passesSupportGate": True, "validForQuality": True}, "objectives": {"worstWindowConservativeNetR": 1.0, "maximumDrawdownR": 1.0, "structuralComplexity": 1.0}, "paretoFront": 0, "crowdingDistance": 1.0}
    archive = {"cells": [{"cellId": "cell-a", "members": [member(first, "archive-a")]}, {"cellId": "cell-b", "members": [member(second, "archive-b")]}]}
    policy = {"schemaVersion": "temporal_qd_bidirectional_pair_policy_v1", "enabled": True, "compilerAuthority": first.pair_compiler.canonical_payload()}
    common = dict(generation_index=1, target_unique_candidates=100, run_config={"seed": "archive-resume"}, pair_policy=policy, parent_archive=archive, pair_factory=_PairFactory(), module_authority=_PairOps(), native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler(), operator_implementation_identity={"schemaVersion": "test_pair_operator_v1", "grammar": "frozen", "indicator": "frozen", "hold": "frozen"})
    generate_pair_population(output_root=tmp_path / "split", max_new_proposals=7, **common)
    generate_pair_population(output_root=tmp_path / "split", max_new_proposals=7, **common)
    generate_pair_population(output_root=tmp_path / "full", max_new_proposals=14, **common)
    import json
    split = [json.loads(path.read_text(encoding="utf-8")) for path in sorted((tmp_path / "split/proposal-journal").glob("*.json"))]
    full = [json.loads(path.read_text(encoding="utf-8")) for path in sorted((tmp_path / "full/proposal-journal").glob("*.json"))]
    assert [row["entrySha256"] for row in split] == [row["entrySha256"] for row in full]
    assert split[6]["proposal"].get("crossoverAudit") is not None


def test_explicit_parent_ring_resume_matches_uninterrupted_after_twelve_proposals(tmp_path) -> None:
    """The non-archive parent cursor must survive the ordinal-12 split."""

    def source_pair(label: str) -> FrozenPair:
        return _pair(
            _module(
                "long",
                marker=f"{label}-long",
                profile={"version": "v2", "directionMode": "long", "graph": {"source": label}},
            ),
            _module(
                "short",
                marker=f"{label}-short",
                profile={"version": "v2", "directionMode": "short", "graph": {"source": label}},
            ),
        )

    first, second = source_pair("first"), source_pair("second")
    policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": first.pair_compiler.canonical_payload(),
    }
    common = dict(
        generation_index=1,
        target_unique_candidates=64,
        run_config={"seed": "explicit-parent-ring-resume"},
        pair_policy=policy,
        parent_pairs=[first, second],
        pair_factory=_PairFactory(),
        module_authority=_PairOps(),
        native_validator=FakeNativeValidator(),
        pair_compiler=FakePairCompiler(),
        operator_implementation_identity={
            "schemaVersion": "test_pair_operator_v1",
            "grammar": "frozen",
            "indicator": "frozen",
            "hold": "frozen",
        },
    )

    generate_pair_population(output_root=tmp_path / "split", max_new_proposals=12, **common)
    generate_pair_population(output_root=tmp_path / "split", max_new_proposals=1, **common)
    generate_pair_population(output_root=tmp_path / "full", max_new_proposals=13, **common)

    split = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((tmp_path / "split/proposal-journal").glob("*.json"))
    ]
    full = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((tmp_path / "full/proposal-journal").glob("*.json"))
    ]
    assert len(split) == len(full) == 13
    assert [row["entrySha256"] for row in split] == [row["entrySha256"] for row in full]
    assert split[6]["proposal"]["proposalKind"] == "temporal_qd_same_side_crossover_v1"
    assert split[12]["proposal"]["parentPairIdentitySha256"] == full[12]["proposal"]["parentPairIdentitySha256"]
    assert _explicit_parent_draw_count(split[:12]) == 11

    malformed = copy.deepcopy(split[:7])
    malformed[6]["proposal"]["mateSelectionAttempts"] = [{"unexpected": "archive-audit"}]
    with pytest.raises(TemporalDiscoveryContractError, match="mate retries"):
        _explicit_parent_draw_count(malformed)


def test_rejected_crossover_resume_matches_uninterrupted_and_replays_exactly(tmp_path) -> None:
    first = _pair(_module("long", marker="first"), _module("short", marker="first"))
    second = _pair(_module("long", marker="second"), _module("short", marker="second"))

    def member(pair, candidate_id):
        return {"candidateId": candidate_id, "candidate": {"candidateId": candidate_id, "bidirectionalGenome": pair.canonical_payload()}, "archiveLane": "quality", "finiteDataValidity": {"isFiniteData": True, "passesSupportGate": True, "validForQuality": True}, "objectives": {"worstWindowConservativeNetR": 1.0, "maximumDrawdownR": 1.0, "structuralComplexity": 1.0}, "paretoFront": 0, "crowdingDistance": 1.0}

    archive = {"cells": [{"cellId": "cell-a", "members": [member(first, "archive-a")]}, {"cellId": "cell-b", "members": [member(second, "archive-b")]}]}
    policy = {"schemaVersion": "temporal_qd_bidirectional_pair_policy_v1", "enabled": True, "compilerAuthority": first.pair_compiler.canonical_payload()}
    common = dict(generation_index=1, target_unique_candidates=100, run_config={"seed": "rejected-crossover-resume"}, pair_policy=policy, parent_archive=archive, pair_factory=_PairFactory(), module_authority=_RejectingCrossoverPairOps(), native_validator=FakeNativeValidator(), pair_compiler=FakePairCompiler(), operator_implementation_identity={"schemaVersion": "test_pair_operator_v1", "grammar": "frozen", "indicator": "frozen", "hold": "frozen"})

    generate_pair_population(output_root=tmp_path / "split", max_new_proposals=7, **common)
    generate_pair_population(output_root=tmp_path / "split", max_new_proposals=7, **common)
    generate_pair_population(output_root=tmp_path / "full", max_new_proposals=14, **common)

    import json

    split = [json.loads(path.read_text(encoding="utf-8")) for path in sorted((tmp_path / "split/proposal-journal").glob("*.json"))]
    full = [json.loads(path.read_text(encoding="utf-8")) for path in sorted((tmp_path / "full/proposal-journal").glob("*.json"))]
    rejected = split[6]["proposal"]
    assert rejected["proposalKind"] == "temporal_qd_same_side_crossover_v1"
    assert rejected["disposition"] == "operation_rejected"
    assert rejected["rejection"]["reasonCode"] == "crossover_rejected"
    tampered = copy.deepcopy(rejected)
    tampered["rejection"]["exceptionType"] = "ValueError"
    tampered.pop("proposalSha256")
    tampered["proposalSha256"] = canonical_sha256(tampered)
    with pytest.raises(TemporalDiscoveryContractError, match="crossover proposal replay diverged"):
        replay_pair_proposal(
            payload=tampered,
            module_authority=_RejectingCrossoverPairOps(),
            native_validator=FakeNativeValidator(),
            pair_compiler=FakePairCompiler(),
        )
    assert [row["entrySha256"] for row in split] == [row["entrySha256"] for row in full]


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


def test_optimized_pair_generation_is_exactly_equivalent_to_legacy_and_restart_safe(
    tmp_path,
) -> None:
    """The compact path is a storage implementation, not a new authority.

    Performance files deliberately differ because their clock/sampling spans
    describe the implementation.  Every semantic artifact below must remain
    byte-identical, including a resumed optimized run and the collision guard.
    """

    pair = _pair()
    policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": pair.pair_compiler.canonical_payload(),
    }
    common = {
        "generation_index": 1,
        "target_unique_candidates": 4,
        "run_config": {"seed": "old-new-equivalence"},
        "pair_policy": policy,
        "pair_factory": _UniquePairFactory(),
        "module_authority": _PairOps(),
        "native_validator": FakeNativeValidator(),
        "pair_compiler": FakePairCompiler(),
        "operator_implementation_identity": {
            "schemaVersion": "test_pair_operator_v1",
            "grammar": "frozen",
            "indicator": "frozen",
            "hold": "frozen",
        },
    }

    legacy_root = tmp_path / "legacy"
    optimized_root = tmp_path / "optimized"
    rust_root = tmp_path / "optimized-rust-finalizer"
    legacy = generate_pair_population(
        output_root=legacy_root,
        implementation="legacy",
        **common,
    )
    optimized = generate_pair_population(
        output_root=optimized_root,
        implementation="optimized",
        population_finalizer="python",
        **common,
    )
    rust_finalized = generate_pair_population(
        output_root=rust_root,
        implementation="optimized",
        population_finalizer="rust",
        **common,
    )
    assert optimized == legacy
    assert rust_finalized == legacy

    def semantic_files(root: Path) -> dict[str, bytes]:
        files = {
            "pair-config.json",
            "population.json",
            "generation-journal.json",
        }
        result = {name: (root / name).read_bytes() for name in files}
        result.update(
            {
                f"proposal-journal/{path.name}": path.read_bytes()
                for path in sorted((root / "proposal-journal").glob("*.json"))
            }
        )
        return result

    assert semantic_files(optimized_root) == semantic_files(legacy_root)
    assert semantic_files(rust_root) == semantic_files(legacy_root)

    # Either implementation can resume/verify the other's population.  The
    # finalizer selector is operational evidence, never semantic identity.
    assert generate_pair_population(
        output_root=optimized_root,
        implementation="optimized",
        population_finalizer="rust",
        **common,
    ) == legacy
    assert generate_pair_population(
        output_root=rust_root,
        implementation="optimized",
        population_finalizer="python",
        **common,
    ) == legacy
    assert semantic_files(optimized_root) == semantic_files(rust_root)
    legacy_rows = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((legacy_root / "proposal-journal").glob("*.json"))
    ]
    optimized_rows = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((optimized_root / "proposal-journal").glob("*.json"))
    ]
    assert [row["disposition"] for row in optimized_rows] == [
        row["disposition"] for row in legacy_rows
    ]
    assert [row["entrySha256"] for row in optimized_rows] == [
        row["entrySha256"] for row in legacy_rows
    ]
    assert [row["candidate"]["candidateId"] for row in optimized_rows] == [
        row["candidate"]["candidateId"] for row in legacy_rows
    ]
    assert [row["candidate"]["candidateIdentitySha256"] for row in optimized_rows] == [
        row["candidate"]["candidateIdentitySha256"] for row in legacy_rows
    ]
    assert [
        row["candidate"]["bidirectionalGenome"]["validation"]
        for row in optimized_rows
    ] == [
        row["candidate"]["bidirectionalGenome"]["validation"]
        for row in legacy_rows
    ]

    resumed_root = tmp_path / "optimized-resumed"
    progress = generate_pair_population(
        output_root=resumed_root,
        implementation="optimized",
        max_new_proposals=2,
        **common,
    )
    resumed = generate_pair_population(
        output_root=resumed_root,
        implementation="optimized",
        **common,
    )
    assert progress["completed"] is False
    assert resumed == legacy
    assert semantic_files(resumed_root) == semantic_files(legacy_root)

    cutover_root = tmp_path / "legacy-to-optimized-resumed"
    legacy_progress = generate_pair_population(
        output_root=cutover_root,
        implementation="legacy",
        max_new_proposals=2,
        **common,
    )
    cutover_resumed = generate_pair_population(
        output_root=cutover_root,
        implementation="optimized",
        **common,
    )
    assert legacy_progress["completed"] is False
    assert cutover_resumed == legacy
    assert semantic_files(cutover_root) == semantic_files(legacy_root)

    default_root = tmp_path / "optimized-default"
    default_result = generate_pair_population(
        output_root=default_root,
        **common,
    )
    assert default_result == optimized
    assert semantic_files(default_root) == semantic_files(optimized_root)
    assert (
        default_root / "performance" / "population-finalizer" / "authority.json"
    ).exists()

    tripwire_common = {
        **common,
        "target_unique_candidates": 2,
        "pair_factory": _TripwireDuplicatePairFactory(),
        "max_proposal_attempts": 3,
    }
    tripwire_roots = {}
    for implementation in ("legacy", "optimized"):
        root = tmp_path / f"tripwire-{implementation}"
        with pytest.raises(
            TemporalDiscoveryContractError, match="semantic acceptance collapsed"
        ):
            generate_pair_population(
                output_root=root,
                implementation=implementation,
                **tripwire_common,
            )
        tripwire_roots[implementation] = root
    for relative in (
        "pair-config.json",
        "proposal-journal/00000000.json",
        "proposal-journal/00000001.json",
        "immigrant-collision-tripwire.json",
    ):
        assert (tripwire_roots["optimized"] / relative).read_bytes() == (
            tripwire_roots["legacy"] / relative
        ).read_bytes()
    assert (
        tripwire_roots["optimized"] / "immigrant-collision-tripwire.json"
    ).read_bytes() == (
        tripwire_roots["legacy"] / "immigrant-collision-tripwire.json"
    ).read_bytes()


def test_optimized_pair_generation_matches_legacy_with_global_ledger(tmp_path) -> None:
    """Production pair generation always binds the campaign identity ledger."""

    pair = _pair()
    policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": pair.pair_compiler.canonical_payload(),
    }
    evidence_context = {
        "schemaVersion": "temporal_qd_predeclared_evidence_context_v3",
        "baseDecisionTimeframe": "M5",
        "orderedWindowPlanSemantic": [],
        "workerContractSha256": None,
        "constructionCatalog": None,
        "costViews": {
            "none": {
                "spreadBps": 0.0,
                "slippageBps": 0.0,
                "commissionBps": 0.0,
            },
            "research_conservative": {
                "spreadBps": 2.0,
                "slippageBps": 1.0,
                "commissionBps": 0.5,
            },
        },
    }
    common = {
        "generation_index": 1,
        "target_unique_candidates": 4,
        "run_config": {"seed": "old-new-ledger-equivalence"},
        "pair_policy": policy,
        "pair_factory": _UniquePairFactory(),
        "module_authority": _PairOps(),
        "native_validator": FakeNativeValidator(),
        "pair_compiler": FakePairCompiler(),
        "evidence_identity_context": evidence_context,
        "operator_implementation_identity": {
            "schemaVersion": "test_pair_operator_v1",
            "grammar": "frozen",
            "indicator": "frozen",
            "hold": "frozen",
        },
    }
    roots: dict[str, Path] = {}
    results: dict[str, dict] = {}
    for implementation in ("legacy", "optimized"):
        root = tmp_path / implementation
        roots[implementation] = root
        results[implementation] = generate_pair_population(
            output_root=root / "generation-1",
            identity_ledger_path=root / "identity-ledger.json",
            implementation=implementation,
            **common,
        )

    assert results["optimized"] == results["legacy"]
    for relative in (
        "generation-1/pair-config.json",
        "generation-1/population.json",
        "generation-1/generation-journal.json",
        "identity-ledger.json",
    ):
        assert (roots["optimized"] / relative).read_bytes() == (
            roots["legacy"] / relative
        ).read_bytes()
    legacy_entries = sorted(
        (roots["legacy"] / "generation-1/proposal-journal").glob("*.json")
    )
    optimized_entries = sorted(
        (roots["optimized"] / "generation-1/proposal-journal").glob("*.json")
    )
    assert [path.read_bytes() for path in optimized_entries] == [
        path.read_bytes() for path in legacy_entries
    ]


def test_optimized_parent_and_crossover_path_matches_legacy(tmp_path) -> None:
    """The compact loop must preserve evolved-generation proposal scheduling."""

    first = _pair(
        _module("long", marker="first"),
        _module("short", marker="first"),
    )
    second = _pair(
        _module("long", marker="second"),
        _module("short", marker="second"),
    )
    policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": first.pair_compiler.canonical_payload(),
    }
    common = {
        "generation_index": 2,
        "target_unique_candidates": 100,
        "run_config": {"seed": "old-new-parent-equivalence"},
        "pair_policy": policy,
        "parent_pairs": [first, second],
        "pair_factory": _UniquePairFactory(),
        "module_authority": _PairOps(),
        "native_validator": FakeNativeValidator(),
        "pair_compiler": FakePairCompiler(),
        "operator_implementation_identity": {
            "schemaVersion": "test_pair_operator_v1",
            "grammar": "frozen",
            "indicator": "frozen",
            "hold": "frozen",
        },
        "max_new_proposals": 7,
    }
    roots: dict[str, Path] = {}
    results: dict[str, dict] = {}
    for implementation in ("legacy", "optimized"):
        root = tmp_path / implementation
        roots[implementation] = root
        results[implementation] = generate_pair_population(
            output_root=root,
            implementation=implementation,
            **common,
        )

    assert results["optimized"] == results["legacy"]
    legacy_entries = sorted((roots["legacy"] / "proposal-journal").glob("*.json"))
    optimized_entries = sorted(
        (roots["optimized"] / "proposal-journal").glob("*.json")
    )
    assert [path.read_bytes() for path in optimized_entries] == [
        path.read_bytes() for path in legacy_entries
    ]
    crossover = json.loads(optimized_entries[6].read_text(encoding="utf-8"))
    assert crossover["proposal"]["proposalKind"] == (
        "temporal_qd_same_side_crossover_v1"
    )

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


def test_pair_population_stops_when_rich_immigrant_acceptance_collapses(tmp_path) -> None:
    pair = _pair()
    policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": pair.pair_compiler.canonical_payload(),
    }
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="semantic acceptance collapsed",
    ):
        generate_pair_population(
            output_root=tmp_path,
            generation_index=1,
            target_unique_candidates=2,
            run_config={"seed": "tripwire-pair-run"},
            pair_policy=policy,
            pair_factory=_TripwireDuplicatePairFactory(),
            module_authority=_PairOps(),
            native_validator=FakeNativeValidator(),
            pair_compiler=FakePairCompiler(),
            operator_implementation_identity={
                "schemaVersion": "test_pair_operator_v1"
            },
            max_proposal_attempts=3,
        )
    tripwire = json.loads(
        (tmp_path / "immigrant-collision-tripwire.json").read_text(
            encoding="utf-8"
        )
    )
    assert tripwire["immigrantAttempts"] == 2
    assert tripwire["immigrantAccepted"] == 1
    assert tripwire["acceptedRatio"] == 0.5
    assert tripwire["dispositionCounts"] == {
        "accepted": 1,
        "duplicate_pair_genome": 1,
    }


def test_pair_generation_global_ledger_blocks_prior_executable_semantics_and_cap_is_restart_safe(tmp_path) -> None:
    """A later generation must not spend a slot on an already-evaluated pair.

    Pair candidate identity includes frozen proposal provenance, so this checks
    the separate executable long/short semantic binding as well as the shared
    QD candidate ledger.  Split/restarted proposal production is byte-identical
    to uninterrupted production when the immutable attempt ceiling is reached.
    """

    pair = _pair()
    policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": pair.pair_compiler.canonical_payload(),
    }
    evidence_context = {
        "schemaVersion": "temporal_qd_predeclared_evidence_context_v3",
        "baseDecisionTimeframe": "M5",
        "orderedWindowPlanSemantic": [],
        "workerContractSha256": None,
        "constructionCatalog": None,
        "costViews": {
            "none": {"spreadBps": 0.0, "slippageBps": 0.0, "commissionBps": 0.0},
            "research_conservative": {
                "spreadBps": 2.0,
                "slippageBps": 1.0,
                "commissionBps": 0.5,
            },
        },
    }
    common = dict(
        target_unique_candidates=1,
        run_config={"seed": "global-semantic-ledger"},
        pair_policy=policy,
        pair_factory=_PairFactory(),
        module_authority=_PairOps(),
        native_validator=FakeNativeValidator(),
        pair_compiler=FakePairCompiler(),
        evidence_identity_context=evidence_context,
        operator_implementation_identity={
            "schemaVersion": "test_pair_operator_v1",
            "grammar": "frozen",
            "indicator": "frozen",
            "hold": "frozen",
        },
        max_proposal_attempts=2,
    )

    def seed_campaign(root: Path) -> None:
        first = generate_pair_population(
            output_root=root / "generation-1",
            generation_index=1,
            identity_ledger_path=root / "identity-ledger.json",
            **common,
        )
        assert first["completed"] is True

    split_root, full_root = tmp_path / "split", tmp_path / "full"
    seed_campaign(split_root)
    seed_campaign(full_root)
    split_args = dict(
        output_root=split_root / "generation-2",
        generation_index=2,
        identity_ledger_path=split_root / "identity-ledger.json",
        **common,
    )
    full_args = dict(
        output_root=full_root / "generation-2",
        generation_index=2,
        identity_ledger_path=full_root / "identity-ledger.json",
        **common,
    )
    first_split = generate_pair_population(max_new_proposals=1, **split_args)
    second_split = generate_pair_population(max_new_proposals=1, **split_args)
    full = generate_pair_population(**full_args)

    for result in (first_split, second_split, full):
        assert result["completed"] is False
    assert second_split["terminationReason"] == "max_proposal_attempts_reached"
    assert second_split["proposalCount"] == 2
    assert second_split["acceptedCount"] == 0
    assert second_split["maxProposalAttempts"] == 2

    split_rows = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((split_root / "generation-2" / "proposal-journal").glob("*.json"))
    ]
    full_rows = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((full_root / "generation-2" / "proposal-journal").glob("*.json"))
    ]
    assert [row["entrySha256"] for row in split_rows] == [row["entrySha256"] for row in full_rows]
    assert [row["disposition"] for row in split_rows] == [
        "duplicate_pair_genome_global",
        "duplicate_pair_genome_global",
    ]
    assert json.loads(
        (split_root / "generation-2" / "pair-config.json").read_text(encoding="utf-8")
    )["maxProposalAttempts"] == 2
    assert (split_root / "identity-ledger.json").read_bytes() == (
        full_root / "identity-ledger.json"
    ).read_bytes()
    ledger = json.loads((split_root / "identity-ledger.json").read_text(encoding="utf-8"))
    assert len(ledger["records"]) == 1
    assert len(ledger["pairExecutableSemantics"]) == 1
    assert ledger["pairExecutableSemanticDuplicateRejections"] == 2
    assert ledger["proposalSlotCounters"]["proposalsObserved"] == 3


def test_typed_pair_operator_rebinds_recursive_frozen_program_and_lineage() -> None:
    """Operator boundaries must thaw FrozenModule's mappingproxy/tuple state."""

    source = _module(
        "long",
        lineage=(
            {
                "operation": "typed_seed",
                "side": "long",
                "nested": {"immutable": ["program", "lineage"]},
            },
        ),
    )
    validator = FakeNativeValidator()

    class Grammar:
        context_sha256 = source.grammar_context.sha256

        def apply(self, program, plan):
            del plan
            return program

        def compile_module(self, program, *, candidate_id):
            profile = source.canonical_payload()["profile"]
            profile["graph"]["grammarRebound"] = candidate_id
            return type(
                "CompiledModule",
                (),
                {
                    "program": program.canonical(),
                    "profile": profile,
                    "native_report": validator.validate_v2(
                        profile=profile, candidate_id=candidate_id
                    ),
                },
            )()

    class Indicator:
        def preview(self, profile, plan):
            value = copy.deepcopy(profile)
            value["graph"]["indicatorRebound"] = plan["operatorId"]
            return value

        def apply(
            self,
            profile,
            plan,
            *,
            parent_validated_program_sha256,
            child_validated_program_sha256,
        ):
            del parent_validated_program_sha256, child_validated_program_sha256
            child = self.preview(profile, plan)
            application = {"applicationSha256": canonical_sha256({"plan": plan})}
            return child, application

    class Registry:
        def get(self, operator_id):
            assert operator_id == "portable_indicator_rebind"
            return Indicator()

    operator = TypedGrammarPairOperator(
        grammar_factory=lambda module: Grammar(),
        native_validator=validator,
        indicator_registry=Registry(),
        hold_operator_policy=default_hold_operator_policy(),
    )

    grammar_child, _ = operator.apply_grammar(
        source, {"kind": "portable_grammar_rebind"}, candidate_id="grammar_rebind"
    )
    indicator_child, _ = operator.apply_indicator(
        source,
        {
            "operatorId": "portable_indicator_rebind",
            "planSha256": canonical_sha256({"operator": "portable_indicator_rebind"}),
        },
        candidate_id="indicator_rebind",
    )

    for child in (grammar_child, indicator_child):
        payload = child.canonical_payload()
        assert FrozenModule.from_payload(payload).canonical_payload() == payload
        assert child.program["fragments"][0]["resources"]["group"] == "g"
        assert child.lineage[0]["nested"]["immutable"] == ("program", "lineage")
        assert len(child.lineage) == 2
