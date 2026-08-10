from __future__ import annotations

import copy
import json

import pytest

from autoresearch.evolvable_module_qd_authority import (
    build_evolvable_module_authority_config,
    capacity_probe,
    capacity_receipt,
    open_evolvable_module_pair_authority,
    validate_capacity_receipt,
)
from autoresearch.evolvable_module_genome import BudgetContractV1, ResourceKind
from autoresearch.temporal_bidirectional_genome import (
    IdentitySnapshot,
    canonical_sha256,
)
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_pair_generation import (
    materialize_pair_candidate,
    propose_pair,
    replay_pair_proposal,
)
from autoresearch.temporal_qd_g0_bootstrap import _verify_accepted_entry
from autoresearch.temporal_qd_initial_protection import default_initial_protection_policy


def _catalog_row(identifier: str, *, event: bool = False, scalar: bool = False) -> dict:
    meta = {
        "id": identifier, "strategyRole": "fixture", "signalRole": "trigger" if event else "context",
        "signalPersistence": "event-with-lookback" if event else "state",
        "valueRange": {"min": 0, "max": 100, "step": 1, "minRange": 5},
        "usesRangeConfiguration": not event, "inputs": ["close"], "requiredPaddingBars": 10,
        "talibMeta": [{"name": "timeperiod", "uiType": "integer_slider", "default": 14, "min": 2, "max": 30, "marks": [{"value": 5}, {"value": 14}]}],
        "familySubstitution": {
            "substitutionClass": "directional_event_v1" if event else "bounded_score_v1",
            "polarity": "bidirectional" if event else "symmetric",
            "scoreUnit": "binary_0_1" if event else "native_score",
            "rawUnit": "directional_boolean_outputs" if event else "index",
            "eventOutputSchema": {"kind": "directional_tokens", "longOutput": "bullish", "shortOutput": "bearish"} if event else {"kind": "none"},
            "persistenceCompatibility": "event-with-lookback" if event else "state",
        },
    }
    if scalar:
        meta["managementScalarOutputs"] = [{"outputKey": "level", "unit": "price", "valueKind": "price_level"}]
    return {"meta": meta, "config": {"isActive": True, "useFormingBar": False, "timeframe": "M5", "lookbackBars": 1, "weight": 1.0, "ranges": {"buy": [20, 40], "sell": [60, 80]}, "talibConfig": [{"name": "timeperiod", "value": 14}]}}


def _hermetic_pair_authority_source() -> dict:
    catalog = {"timeframes": {"M5": {}, "M15": {}, "H1": {}}, "indicators": [_catalog_row("STATE_A", scalar=True), _catalog_row("STATE_B"), _catalog_row("EVENT_A", event=True)]}
    indicators = copy.deepcopy(catalog["indicators"])
    for row, instance_id in zip(indicators, ("state_a", "state_b", "event_a"), strict=True):
        row["meta"]["instanceId"] = instance_id
    context = {
        "instrument": "EURUSD", "indicators": indicators,
        "events": [{"id": "event_a_binding", "indicatorInstanceId": "event_a", "longOutput": "bullish", "shortOutput": "bearish"}],
        "executionConfig": {"managementLibrary": {"plans": [{"id": "base", "initialStop": {"kind": "fixed_percent", "percent": 1.0}, "initialTarget": {"kind": "reward_multiple", "multiple": 2.0}}]}},
    }
    catalog_sha = canonical_sha256(catalog)
    return {"pairRunConfigSha256": canonical_sha256({"fixture": "evolvable-module-authority-v1"}), "longModule": {"catalogSha256": catalog_sha, "catalog": catalog, "context": context}, "shortModule": {"catalogSha256": catalog_sha, "catalog": copy.deepcopy(catalog), "context": copy.deepcopy(context)}, "holdOperatorPolicy": {"schemaVersion": "temporal_qd_pair_hold_operator_policy_v2", "enabled": True, "allowedKinds": ["none", "market_bars", "elapsed_calendar"], "choices": [{"kind": "none"}, {"kind": "market_bars", "bars": 3, "timeframe": "M5"}]}, "initialProtectionOperatorPolicy": default_initial_protection_policy()}


class _Validator:
    def __init__(self) -> None:
        self.snapshots: dict[str, str] = {}

    def validate_v2(self, *, profile, candidate_id):
        source_sha = canonical_sha256(profile)
        snapshot = canonical_sha256({"snapshot": source_sha})
        self.snapshots[source_sha] = snapshot
        return {
            "schemaVersion": "temporal_search_candidate_validation_v1",
            "candidateId": candidate_id,
            "rawSourceProfileSha256": source_sha,
            "status": "valid_evaluable",
            "candidateAcceptable": True,
            "profileSnapshotSha256": snapshot,
            "programSha256": canonical_sha256({"program": source_sha}),
            "validationReportSha256": canonical_sha256({"validation": source_sha}),
        }


class _PairCompiler:
    def __init__(self, validator: _Validator) -> None:
        self.validator = validator

    def compile_pair(self, *, long_profile, short_profile, candidate_id):
        long_sha, short_sha = canonical_sha256(long_profile), canonical_sha256(short_profile)
        profile = {
            "version": "v3",
            "directionMode": "both",
            "graph": {
                "entryArbitration": {
                    "modules": [
                        {"direction": "long", "sourceProfileSnapshotSha256": self.validator.snapshots[long_sha]},
                        {"direction": "short", "sourceProfileSnapshotSha256": self.validator.snapshots[short_sha]},
                    ]
                }
            },
        }
        source_sha = canonical_sha256(profile)
        return {
            "profile": profile,
            "validation": {
                "schemaVersion": "temporal_search_candidate_validation_v1",
                "candidateId": candidate_id,
                "rawSourceProfileSha256": source_sha,
                "status": "valid_evaluable",
                "candidateAcceptable": True,
                "profileSnapshotSha256": canonical_sha256({"pairSnapshot": source_sha}),
                "programSha256": canonical_sha256({"pairProgram": source_sha}),
                "validationReportSha256": canonical_sha256({"pairValidation": source_sha}),
            },
        }


class _Bundle:
    def __init__(self) -> None:
        self.config = _hermetic_pair_authority_source()
        self.validator = _Validator()
        self.compiler = _PairCompiler(self.validator)
        self.native_identity = IdentitySnapshot.create(
            kind="nativeAuthority", schema_version="fixture_native_v1", payload={"fixture": True}
        )
        self.compiler_identity = IdentitySnapshot.create(
            kind="pairCompiler", schema_version="fixture_pair_compiler_v1", payload={"fixture": True}
        )


@pytest.fixture()
def authority():
    bundle = _Bundle()
    config = build_evolvable_module_authority_config(
        pair_run_config_sha256=bundle.config["pairRunConfigSha256"],
        catalog_sha256=bundle.config["longModule"]["catalogSha256"],
    )
    return open_evolvable_module_pair_authority(bundle=bundle, config=config)


def test_opt_in_factory_compiles_symmetric_catalog_bound_modules_then_existing_v3_pair(authority) -> None:
    assert authority.factory.construction_policy["collisionTripwire"] == {
        "minimumImmigrantAttempts": 512,
        "minimumAcceptedRatio": 0.25,
    }
    pair = authority.factory.create_pair(proposal_seed="evolvable-fixture-seed")
    assert pair.profile["version"] == "v3"
    assert pair.long.canonical_payload()["program"]["programKind"] == "evolvable_module_genome_v1"
    audit = authority.factory.audit_pair(pair)
    assert audit["sides"]["long"]["semanticTopologySha256"]
    assert audit["sides"]["short"]["resourceFingerprintSha256"]
    assert all(item["codec"] == "evolvable_module_genome_json_v1" for item in pair.side_targeted_lineage)


def _evolvable_g0_entry(authority, *, audit_mutator=None) -> dict:
    pair, proposal = propose_pair(
        proposal_seed="evolvable-g0-audit-fixture",
        parent=None,
        pair_factory=authority.factory,
        module_authority=authority.operator,
        native_validator=authority.bundle.validator,
        pair_compiler=authority.bundle.compiler,
    )
    assert pair is not None
    if audit_mutator is not None:
        audit_mutator(proposal["factoryConstructionAudit"])
        proposal["factoryConstructionAudit"]["auditSha256"] = canonical_sha256({
            key: value
            for key, value in proposal["factoryConstructionAudit"].items()
            if key != "auditSha256"
        })
        proposal["proposalSha256"] = canonical_sha256({
            key: value for key, value in proposal.items() if key != "proposalSha256"
        })
    pair_policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": pair.pair_compiler.canonical_payload(),
    }
    candidate = materialize_pair_candidate(
        pair=pair,
        proposal=proposal,
        pair_policy=pair_policy,
        generation_index=0,
        birth_ordinal=0,
        proposal_ordinal=0,
    )
    entry = {
        "schemaVersion": "temporal_qd_proposal_entry_v3",
        "configSha256": canonical_sha256({"config": "evolvable-g0-fixture"}),
        "generationIndex": 0,
        "proposalOrdinal": 0,
        "originKind": "random_immigrant",
        "proposal": proposal,
        "operatorImplementationSha256": canonical_sha256({"operator": "evolvable-g0-fixture"}),
        "disposition": "accepted",
        "candidate": candidate,
    }
    entry["entrySha256"] = canonical_sha256(entry)
    return entry


def test_g0_accepts_and_binds_evolvable_factory_audit(authority) -> None:
    entry = _evolvable_g0_entry(authority)
    candidate, pair = _verify_accepted_entry(entry)
    assert candidate["candidateId"].startswith("qd_")
    assert pair.identity_sha256 == entry["proposal"]["pairIdentitySha256"]


@pytest.mark.parametrize(
    ("audit_mutator", "message"),
    [
        (
            lambda audit: audit.update({"authoritySha256": "sha256:" + "0" * 64}),
            "authority or side lineage drift",
        ),
        (
            lambda audit: audit["sides"]["long"].update(
                {"resourceFingerprintSha256": "sha256:" + "1" * 64}
            ),
            "diverged from frozen module program",
        ),
        (
            lambda audit: audit.update({"unexpected": True}),
            "unexpected schema",
        ),
    ],
)
def test_g0_rejects_rehashed_evolvable_factory_audit_drift(
    authority, audit_mutator, message
) -> None:
    entry = _evolvable_g0_entry(authority, audit_mutator=audit_mutator)
    with pytest.raises(TemporalDiscoveryContractError, match=message):
        _verify_accepted_entry(entry)


def test_resource_topology_replay_stale_plan_and_cross_side_drift_fail_closed(authority) -> None:
    pair = authority.factory.create_pair(proposal_seed="evolvable-replay-seed")
    target = pair.long
    plans = [*authority.operator.grammar_plans(target), *authority.operator.indicator_plans(target)]
    assert plans
    chosen = plans[0]
    apply = authority.operator.apply_grammar if chosen["operatorId"] == "evolvable_topology_v1" else authority.operator.apply_indicator
    child, audit = apply(target, chosen, candidate_id="fixture-child")
    assert child.identity_sha256 != target.identity_sha256
    assert audit["parentModuleIdentitySha256"] == target.identity_sha256
    with pytest.raises(TemporalDiscoveryContractError, match="stale|foreign|canonical"):
        apply(child, chosen, candidate_id="fixture-stale")
    with pytest.raises(TemporalDiscoveryContractError, match="foreign|policy authority drifted"):
        authority.operator.apply_grammar(pair.short, chosen, candidate_id="fixture-cross-side")


def test_pair_proposal_replays_exactly_and_rejects_authority_drift(authority) -> None:
    parent = authority.factory.create_pair(proposal_seed="evolvable-parent")
    child, proposal = propose_pair(
        proposal_seed="evolvable-offspring",
        parent=parent,
        pair_factory=None,
        module_authority=authority.operator,
        native_validator=authority.bundle.validator,
        pair_compiler=authority.bundle.compiler,
    )
    if child is None:
        pytest.skip("deterministic fixture selected an unavailable operation")
    replayed = replay_pair_proposal(
        payload=proposal,
        module_authority=authority.operator,
        native_validator=authority.bundle.validator,
        pair_compiler=authority.bundle.compiler,
    )
    assert replayed is not None and replayed.canonical_payload() == child.canonical_payload()
    tampered = dict(proposal)
    tampered["pairIdentitySha256"] = "sha256:" + "0" * 64
    with pytest.raises(TemporalDiscoveryContractError, match="identity mismatch"):
        replay_pair_proposal(
            payload=tampered,
            module_authority=authority.operator,
            native_validator=authority.bundle.validator,
            pair_compiler=authority.bundle.compiler,
        )


def test_capacity_probe_has_real_topology_resource_and_bidirectional_diversity(authority) -> None:
    # The production contract is 8,192 previews / 4,096 admitted candidates.
    # Keep unit execution bounded while exercising the same compiled v2 + v3
    # admission path and distinct resource/topology assertions.
    small_config = build_evolvable_module_authority_config(
        pair_run_config_sha256=authority.bundle.config["pairRunConfigSha256"],
        catalog_sha256=authority.bundle.config["longModule"]["catalogSha256"],
        capacity_contract={
            "schemaVersion": "temporal_qd_evolvable_module_capacity_contract_v1",
            "previewStreamSize": 128,
            "minimumUniquePairs": 64,
            "minimumUniqueTopologiesPerSide": 4,
            "minimumUniqueResourceFingerprintsPerSide": 8,
            "requiredDirections": ["long", "short"],
            "admission": "no_market_native_v2_and_compiled_v3_v1",
        },
    )
    small = open_evolvable_module_pair_authority(bundle=authority.bundle, config=small_config)
    result = capacity_probe(small)
    assert result["noMarket"] is True and result["passed"] is True
    assert result["compiledAdmittedCandidateCount"] >= 64
    assert all(result["perSide"][side]["uniqueSemanticTopologyCount"] >= 4 for side in ("long", "short"))
    assert all(result["perSide"][side]["uniqueResourceFingerprintCount"] >= 8 for side in ("long", "short"))
    receipt = capacity_receipt(small, result)
    assert validate_capacity_receipt(small, receipt)["semanticReceiptSha256"] == receipt["semanticReceiptSha256"]
    # Wall-clock telemetry is excluded from the frozen evidence identity.
    replay = json.loads(json.dumps(result)); replay["timing"]["totalSeconds"] += 1000
    assert capacity_receipt(small, replay)["semanticReceiptSha256"] == receipt["semanticReceiptSha256"]
    forged = dict(receipt); forged["uniqueSemanticPairCount"] = 1
    with pytest.raises(TemporalDiscoveryContractError, match="identity or authority drifted"):
        validate_capacity_receipt(small, forged)
    with pytest.raises(TemporalDiscoveryContractError, match="identity or authority drifted"):
        validate_capacity_receipt(authority, receipt)
    # A receipt is an exact admission witness, but deliberately does not
    # perturb the executable authority identity that it names.
    bound_config = build_evolvable_module_authority_config(
        pair_run_config_sha256=authority.bundle.config["pairRunConfigSha256"],
        catalog_sha256=authority.bundle.config["longModule"]["catalogSha256"],
        capacity_contract=small.config["capacityContract"],
        capacity_receipt=receipt,
    )
    assert bound_config["authoritySha256"] == small.config["authoritySha256"]
    bound = open_evolvable_module_pair_authority(bundle=authority.bundle, config=bound_config)
    bound_bindings = bound.generation_bindings({})
    assert bound_bindings["capacityReceipt"] == receipt
    assert (
        bound_bindings["operatorImplementation"]["capacityReceiptSha256"]
        == receipt["semanticReceiptSha256"]
    )
    recalculated_forgery = dict(receipt)
    recalculated_forgery["uniqueSemanticPairCount"] = 1
    recalculated_forgery["semanticReceiptSha256"] = canonical_sha256({
        key: value for key, value in recalculated_forgery.items()
        if key != "semanticReceiptSha256"
    })
    forged_config = build_evolvable_module_authority_config(
        pair_run_config_sha256=authority.bundle.config["pairRunConfigSha256"],
        catalog_sha256=authority.bundle.config["longModule"]["catalogSha256"],
        capacity_contract=small.config["capacityContract"],
        capacity_receipt=recalculated_forgery,
    )
    with pytest.raises(TemporalDiscoveryContractError, match="lacks required admitted pair diversity"):
        open_evolvable_module_pair_authority(bundle=authority.bundle, config=forged_config)
    wrong_factory_config = build_evolvable_module_authority_config(
        pair_run_config_sha256=authority.bundle.config["pairRunConfigSha256"],
        catalog_sha256=authority.bundle.config["longModule"]["catalogSha256"],
        budget=BudgetContractV1(max_states=small.config["budget"]["maxStates"] - 1),
        capacity_contract=small.config["capacityContract"],
        capacity_receipt=receipt,
    )
    with pytest.raises(TemporalDiscoveryContractError, match="identity or authority drifted"):
        open_evolvable_module_pair_authority(bundle=authority.bundle, config=wrong_factory_config)


def test_authority_config_rejects_legacy_or_wrong_archive_policy(authority) -> None:
    config = dict(authority.config)
    config["archivePolicyAuthority"] = {"policyName": "legacy"}
    config["authoritySha256"] = canonical_sha256({key: value for key, value in config.items() if key != "authoritySha256"})
    with pytest.raises(TemporalDiscoveryContractError, match="identity or policy drifted|v5"):
        open_evolvable_module_pair_authority(bundle=authority.bundle, config=config)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("compilerPolicy", "policyName"), "drift"),
        (("operatorRegistry", "topologyOperators"), "drift"),
    ],
)
def test_v5_authority_projection_rejects_compiler_and_registry_drift(authority, path, value) -> None:
    config = json.loads(json.dumps(authority.config))
    cursor = config
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = value
    config["authoritySha256"] = canonical_sha256(
        {key: item for key, item in config.items() if key != "authoritySha256"}
    )
    with pytest.raises(TemporalDiscoveryContractError, match="identity or policy drifted"):
        open_evolvable_module_pair_authority(bundle=authority.bundle, config=config)

def test_v5_generation_projection_rejects_a_different_valid_budget_authority(authority) -> None:
    raw = authority.config["budget"]
    budget = BudgetContractV1(
        max_states=raw["maxStates"] - 1,
        max_transitions=raw["maxTransitions"],
        max_evidence_groups=raw["maxEvidenceGroups"],
        max_group_members=raw["maxGroupMembers"],
        max_events=raw["maxEvents"],
        max_indicators=raw["maxIndicators"],
        max_entry_branches=raw["maxEntryBranches"],
        max_management_regions=raw["maxManagementRegions"],
        max_exit_regions=raw["maxExitRegions"],
        max_recovery_regions=raw["maxRecoveryRegions"],
        max_scc_nodes=raw["maxSccNodes"],
        max_timeout_bars=raw["maxTimeoutBars"],
        max_guard_depth=raw["maxGuardDepth"],
    )
    alternate = open_evolvable_module_pair_authority(
        bundle=authority.bundle,
        config=build_evolvable_module_authority_config(
            pair_run_config_sha256=authority.bundle.config["pairRunConfigSha256"],
            catalog_sha256=authority.bundle.config["longModule"]["catalogSha256"],
            budget=budget,
        ),
    ).generation_bindings({})
    with pytest.raises(TemporalDiscoveryContractError, match="operatorImplementation drifted"):
        authority.generation_bindings({"operatorImplementation": alternate["operatorImplementation"]})


def test_v5_generation_bindings_and_genome_backed_management_genes(authority) -> None:
    bindings = authority.generation_bindings({"runId": "fixture-v5"})
    assert bindings["runConfig"]["archivePolicyAuthority"] == authority.config["archivePolicyAuthority"]
    assert bindings["runConfig"]["behaviorAttributionRequirement"] == authority.config["behaviorAttributionRequirement"]
    assert bindings["operatorImplementation"]["authoritySha256"] == authority.config["authoritySha256"]
    assert bindings["operatorImplementation"]["compilerPolicySha256"] == authority.config["compilerPolicySha256"]
    with pytest.raises(TemporalDiscoveryContractError, match="behaviorAttributionRequirement drifted"):
        authority.generation_bindings({"behaviorAttributionRequirement": {"required": False}})
    with pytest.raises(TemporalDiscoveryContractError, match="operatorImplementation drifted"):
        authority.generation_bindings({"operatorImplementation": {"schemaVersion": "legacy"}})

    module = authority.factory.create_pair(proposal_seed="management-gene").long
    genome = authority.decode_module(module)
    plan_id = next(iter(genome.resources.mapping(ResourceKind.MANAGEMENT_REF)))
    holds = [item for item in authority.operator.hold_policy_choices(module) if item["kind"] != "none"]
    assert holds
    held, _ = authority.operator.apply_hold_policy(module, plan_id=plan_id, new_hold=holds[0], candidate_id="hold-child")
    assert authority.decode_module(held).resources.mapping(ResourceKind.MANAGEMENT_REF)[plan_id]["holdPolicy"] == holds[0]
    protection = authority.operator.initial_protection_plans(module)
    direct_protection = [item for item in protection if item.get("kind") != "dynamic_construction"]
    assert {item["site"] for item in direct_protection} == {"stop", "target"}
    protected, _ = authority.operator.apply_initial_protection(module, direct_protection[0], candidate_id="protection-child")
    assert authority.decode_module(protected).resources.mapping(ResourceKind.MANAGEMENT_REF)[plan_id]


def test_same_side_crossover_accepts_nonidentical_parent_pools_when_donor_motif_closure_is_compatible(authority) -> None:
    pairs = [authority.factory.create_pair(proposal_seed=f"crossover-{index}") for index in range(32)]
    found = None
    for left_pair in pairs:
        for right_pair in pairs:
            if left_pair is right_pair:
                continue
            left, right = authority.decode_module(left_pair.long), authority.decode_module(right_pair.long)
            if left.resources.canonical() == right.resources.canonical():
                continue
            try:
                program = authority.operator.crossover(
                    left_pair.long.canonical_payload()["program"],
                    right_pair.long.canonical_payload()["program"],
                    direction="long",
                    proposal_seed="crossover-probe",
                )
            except TemporalDiscoveryContractError:
                continue
            found = (left_pair, program)
            break
        if found:
            break
    assert found is not None, "expected a nonidentical-pool compatible same-side motif"
    child = authority.operator.compile_program(found[0].long, found[1], candidate_id="crossover-child")
    assert authority.decode_module(child).direction == "long"
