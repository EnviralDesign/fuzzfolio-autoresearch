from __future__ import annotations

import ast
import copy
from pathlib import Path
from types import SimpleNamespace

import pytest

from autoresearch.temporal_bidirectional_genome import canonical_sha256
from autoresearch.temporal_bidirectional_genome import FrozenModule, IdentitySnapshot
from autoresearch.temporal_discovery_validation import (
    DashboardV2ModuleValidator,
    SubprocessCandidateValidator,
)
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_initial_protection import (
    apply_immigrant_initial_protection,
    apply_initial_protection_plan,
    default_initial_protection_policy,
    enumerate_initial_protection_plans,
    immigrant_initial_protection_selector,
)
from autoresearch.temporal_qd_pair_factory import _Factory, default_hold_operator_policy
from autoresearch.temporal_qd_pair_generation import TypedGrammarPairOperator


class _FakeNativeValidator:
    def validate_v2(self, *, profile, candidate_id):
        raw = canonical_sha256(profile)
        return {
            "schemaVersion": "temporal_search_candidate_validation_v1",
            "candidateId": candidate_id,
            "rawSourceProfileSha256": raw,
            "profileSnapshotSha256": raw,
            "programSha256": canonical_sha256({"native": profile}),
            "validationReportSha256": canonical_sha256({"report": profile}),
            "status": "valid_evaluable",
            "candidateAcceptable": True,
        }


def _profile(*, scalar_binding: bool = False) -> dict:
    library: dict = {
        "version": "temporal_management_v1",
        "defaultPlanId": "core",
        "plans": [
            {
                "id": "core",
                "initialStop": {"kind": "fixed_percent", "percent": 1.0},
                "initialTarget": {"kind": "reward_multiple", "multiple": 2.0},
            }
        ],
    }
    if scalar_binding:
        library["scalarBindings"] = [
            {
                "id": "atr_distance",
                "indicatorInstanceId": "atr",
                "outputKey": "atr",
                "valueKind": "price_distance",
                "availability": "completed_bar",
            },
            {
                "id": "moving_average",
                "indicatorInstanceId": "ma",
                "outputKey": "value",
                "valueKind": "price_level",
                "availability": "completed_bar",
            },
        ]
    return {"version": "v2", "directionMode": "long", "executionConfig": {"managementLibrary": library}, "graph": {"states": []}}


def test_coupled_decoupled_and_existing_dynamic_protection_are_reachable() -> None:
    policy = default_initial_protection_policy()
    source = _profile(scalar_binding=True)
    plans = enumerate_initial_protection_plans(source, policy)

    coupled = next(
        item
        for item in plans
        if item["site"] == "target"
        and item["replacement"] == {"kind": "reward_multiple", "multiple": 0.25}
    )
    decoupled = next(
        item
        for item in plans
        if item["site"] == "target"
        and item["replacement"] == {"kind": "fixed_percent", "percent": 0.5}
    )
    dynamic = next(
        item
        for item in plans
        if item["site"] == "stop"
        and item["replacement"]
        == {"kind": "indicator_distance_multiple", "bindingId": "atr_distance", "multiple": 1.5}
    )
    price_level = next(
        item
        for item in plans
        if item["site"] == "target"
        and item["replacement"] == {"kind": "indicator_price_level", "bindingId": "moving_average"}
    )
    no_target = next(
        item
        for item in plans
        if item["site"] == "target" and item["replacement"] == {"kind": "none"}
    )

    for plan, expected_key, expected in (
        (coupled, "initialTarget", coupled["replacement"]),
        (decoupled, "initialTarget", decoupled["replacement"]),
        (dynamic, "initialStop", dynamic["replacement"]),
        (price_level, "initialTarget", price_level["replacement"]),
        (no_target, "initialTarget", no_target["replacement"]),
    ):
        child, audit = apply_initial_protection_plan(source, plan, policy)
        assert child["executionConfig"]["managementLibrary"]["plans"][0][expected_key] == expected
        assert audit["applicationSha256"].startswith("sha256:")

    dynamic_parent = _profile(scalar_binding=True)
    dynamic_parent["executionConfig"]["managementLibrary"]["plans"][0]["initialStop"] = {
        "kind": "indicator_distance_multiple", "bindingId": "atr_distance", "multiple": 1.0
    }
    dynamic_plans = enumerate_initial_protection_plans(dynamic_parent, policy)
    adjacent = next(item for item in dynamic_plans if item["site"] == "stop" and item["replacement"] == {"kind": "indicator_distance_multiple", "bindingId": "atr_distance", "multiple": 0.75})
    jump = next(item for item in dynamic_plans if item["site"] == "stop" and item["replacement"] == {"kind": "indicator_distance_multiple", "bindingId": "atr_distance", "multiple": 2.0})
    assert adjacent["mutationClass"] == "adjacent"
    assert jump["mutationClass"] == "jump"


def test_replacing_dynamic_locator_removes_only_unreferenced_scalar_bindings() -> None:
    policy = default_initial_protection_policy()
    source = _profile(scalar_binding=True)
    source["executionConfig"]["managementLibrary"]["plans"][0]["initialStop"] = {
        "kind": "indicator_distance_multiple", "bindingId": "atr_distance", "multiple": 1.0
    }
    replacement = next(item for item in enumerate_initial_protection_plans(source, policy) if item["site"] == "stop" and item["replacement"] == {"kind": "fixed_percent", "percent": 1.0})
    child, audit = apply_initial_protection_plan(source, replacement, policy)
    assert "scalarBindings" not in child["executionConfig"]["managementLibrary"]
    assert [item["id"] for item in audit["removedUnreferencedScalarBindings"]] == ["atr_distance", "moving_average"]
    expected = copy.deepcopy(source)
    expected["executionConfig"]["managementLibrary"]["plans"][0]["initialStop"] = {
        "kind": "fixed_percent", "percent": 1.0
    }
    expected["executionConfig"]["managementLibrary"].pop("scalarBindings")
    assert canonical_sha256(child) == canonical_sha256(expected)

    retained_source = copy.deepcopy(source)
    retained_source["graph"] = {"transitions": [{"actions": [{"kind": "set_target_next_open", "targetLocator": {"kind": "indicator_distance_multiple", "bindingId": "atr_distance", "multiple": 1.0}}]}]}
    retained, retained_audit = apply_initial_protection_plan(retained_source, replacement, policy)
    assert [item["id"] for item in retained["executionConfig"]["managementLibrary"]["scalarBindings"]] == ["atr_distance"]
    assert [item["id"] for item in retained_audit["removedUnreferencedScalarBindings"]] == ["moving_average"]
    assert canonical_sha256(retained) != canonical_sha256(child)


def test_initial_protection_application_and_g0_selector_are_deterministic() -> None:
    policy = default_initial_protection_policy()
    source = _profile()
    selector_a = immigrant_initial_protection_selector(
        policy=policy,
        choose=lambda seed, *, axis, values: values[
            int(canonical_sha256({"seed": seed, "axis": axis})[-2:], 16) % len(values)
        ],
        seed="restart-proof-seed",
    )
    selector_b = immigrant_initial_protection_selector(
        policy=policy,
        choose=lambda seed, *, axis, values: values[
            int(canonical_sha256({"seed": seed, "axis": axis})[-2:], 16) % len(values)
        ],
        seed="restart-proof-seed",
    )
    assert selector_a == selector_b
    left, left_audit = apply_immigrant_initial_protection(
        source, plan_id="core", selector=selector_a, policy=policy
    )
    right, right_audit = apply_immigrant_initial_protection(
        copy.deepcopy(source), plan_id="core", selector=selector_b, policy=policy
    )
    assert left == right
    assert left_audit == right_audit
    plan = left["executionConfig"]["managementLibrary"]["plans"][0]
    assert plan["initialStop"]["kind"] == "fixed_percent"
    assert plan["initialTarget"]["kind"] in {"reward_multiple", "fixed_percent", "none"}


def test_pair_operator_uses_existing_scalar_dynamic_construction_transaction() -> None:
    catalog = {
        "timeframes": {"M5": {"value": "M5"}},
        "indicators": [
            {
                "meta": {
                    "id": "ATR_VOLATILITY_FILTER",
                    "managementScalarOutputs": [
                        {
                            "outputKey": "atr_raw",
                            "valueKind": "price_distance",
                            "unit": "price_distance",
                        }
                    ],
                }
            }
        ],
    }
    profile = _profile()
    profile.update(
        {
            "name": "scalar fixture",
            "instruments": ["EURUSD"],
            "isActive": False,
            "indicators": [
                {
                    "meta": {
                        "id": "ATR_VOLATILITY_FILTER",
                        "instanceId": "atr",
                        "managementScalarOutputs": [
                            {
                                "outputKey": "atr_raw",
                                "valueKind": "price_distance",
                                "unit": "price_distance",
                            }
                        ],
                    },
                    "config": {"isActive": True, "useFormingBar": False, "timeframe": "M5"},
                }
            ],
            "graph": {
                "kind": "temporal_graph_v1",
                "semanticPolicy": "temporal_graph_semantics_v1",
                "eventSchema": "temporal_event_v1",
                "factLibrary": "temporal_market_facts_v1",
                "guardLibrary": "temporal_guards_v1",
                "actionLibrary": "temporal_market_actions_v1",
                "clockRequirement": "clock.completed_bar",
                "fidelityRequirements": ["data.completed_ohlc"],
                "initialStateId": "flat",
                "states": [{"id": "flat"}, {"id": "entry"}, {"id": "open"}],
                "evidenceGroups": [{"id": "context", "indicatorInstanceIds": ["atr"]}],
                "eventBindings": [],
                "transitions": [
                    {
                        "id": "enter",
                        "sourceStateId": "flat",
                        "destinationStateId": "entry",
                        "eventClass": "decision",
                        "priority": 10,
                        "guard": {"kind": "position_exists", "expected": False},
                        "actions": [{"kind": "enter_next_open", "managementPlanId": "core"}],
                        "reasonCode": "enter",
                    },
                    {
                        "id": "filled",
                        "sourceStateId": "entry",
                        "destinationStateId": "open",
                        "eventClass": "execution",
                        "priority": 10,
                        "guard": {"kind": "execution_status_is", "status": "filled"},
                        "actions": [],
                        "reasonCode": "filled",
                    },
                ],
            },
        }
    )
    validator = _FakeNativeValidator()
    module = FrozenModule.validate_native(
        program={
            "schemaVersion": "temporal_typed_fragment_grammar_v2",
            "grammarVersion": "3",
            "direction": "long",
            "fragments": [],
        },
        profile=profile,
        grammar_context=IdentitySnapshot.create(kind="grammarContext", schema_version="test", payload={}),
        catalog=IdentitySnapshot.create(kind="catalog", schema_version="test", payload={"catalog": catalog}),
        policy=IdentitySnapshot.create(kind="policy", schema_version="test", payload={}),
        native_authority_identity=IdentitySnapshot.create(kind="nativeAuthority", schema_version="test", payload={}),
        native_validator=validator,
        candidate_id="scalar_parent",
    )
    operator = TypedGrammarPairOperator(
        grammar_factory=lambda _: None,
        native_validator=validator,
        hold_operator_policy=default_hold_operator_policy(),
    )
    plan = next(
        item
        for item in operator.initial_protection_plans(module)
        if item["kind"] == "dynamic_construction"
        and item["constructionPlan"]["construction"]["site"] == "initial_stop"
    )
    child, _audit = operator.apply_initial_protection(
        module, plan, candidate_id="scalar_child"
    )
    management = child.canonical_payload()["profile"]["executionConfig"]["managementLibrary"]
    assert management["plans"][0]["initialStop"]["kind"] == "indicator_distance_multiple"
    assert management["scalarBindings"][0]["availability"] == "completed_bar"


def test_g0_dynamic_selector_materializes_catalog_authorized_transaction() -> None:
    catalog = {
        "timeframes": {"M5": {"value": "M5"}},
        "indicators": [
            {
                "meta": {
                    "id": "ATR_VOLATILITY_FILTER",
                    "managementScalarOutputs": [
                        {"outputKey": "atr_raw", "valueKind": "price_distance", "unit": "price_distance"}
                    ],
                }
            }
        ],
    }
    profile = _profile()
    profile["indicators"] = [{
        "meta": {"id": "ATR_VOLATILITY_FILTER", "instanceId": "atr", "managementScalarOutputs": catalog["indicators"][0]["meta"]["managementScalarOutputs"]},
        "config": {"isActive": True, "useFormingBar": False, "timeframe": "M5"},
    }]
    profile["graph"] = {
        "kind": "temporal_graph_v1", "semanticPolicy": "temporal_graph_semantics_v1",
        "eventSchema": "temporal_event_v1", "factLibrary": "temporal_market_facts_v1",
        "guardLibrary": "temporal_guards_v1", "actionLibrary": "temporal_market_actions_v1",
        "clockRequirement": "clock.completed_bar", "fidelityRequirements": ["data.completed_ohlc"],
        "initialStateId": "flat", "states": [{"id": "flat"}, {"id": "entry"}, {"id": "open"}],
        "evidenceGroups": [{"id": "context", "indicatorInstanceIds": ["atr"]}], "eventBindings": [],
        "transitions": [
            {"id": "enter", "sourceStateId": "flat", "destinationStateId": "entry", "eventClass": "decision", "priority": 10, "guard": {"kind": "position_exists", "expected": False}, "actions": [{"kind": "enter_next_open", "managementPlanId": "core"}], "reasonCode": "enter"},
            {"id": "filled", "sourceStateId": "entry", "destinationStateId": "open", "eventClass": "execution", "priority": 10, "guard": {"kind": "execution_status_is", "status": "filled"}, "actions": [], "reasonCode": "filled"},
        ],
    }
    factory = object.__new__(_Factory)
    factory.bundle = SimpleNamespace(
        validator=_FakeNativeValidator(),
        config={"initialProtectionOperatorPolicy": default_initial_protection_policy()},
    )
    multiples = set()
    for digit in range(1, 17):
        child, audit, report = factory._apply_dynamic_initial_protection(
            copy.deepcopy(profile),
            side={"catalog": catalog},
            side_seed="sha256:" + f"{digit:x}" * 64,
            selector={"planId": "core", "dynamicSite": "initial_stop"},
        )
        assert audit["dynamicDisposition"] == "materialized"
        assert report is not None
        locator = child["executionConfig"]["managementLibrary"]["plans"][0]["initialStop"]
        assert locator["kind"] == "indicator_distance_multiple"
        multiples.add(locator["multiple"])
    assert multiples <= set(default_initial_protection_policy()["distanceMultipleChoices"])
    assert len(multiples) > 1

    dynamic_selector = immigrant_initial_protection_selector(
        policy=default_initial_protection_policy(),
        choose=lambda _seed, *, axis, values: "dynamic_catalog_authorized" if axis == "initial_protection_mode" else values[0],
        seed="dynamic-selector",
    )
    assert set(dynamic_selector) == {"mode", "dynamicSite"}


def test_malformed_frozen_catalog_fails_closed_instead_of_hiding_dynamic_surface() -> None:
    validator = _FakeNativeValidator()
    module = FrozenModule.validate_native(
        program={"schemaVersion": "temporal_typed_fragment_grammar_v2", "grammarVersion": "3", "direction": "long", "fragments": []},
        profile=_profile(),
        grammar_context=IdentitySnapshot.create(kind="grammarContext", schema_version="test", payload={}),
        catalog=IdentitySnapshot.create(kind="catalog", schema_version="test", payload={"catalog": {}}),
        policy=IdentitySnapshot.create(kind="policy", schema_version="test", payload={}),
        native_authority_identity=IdentitySnapshot.create(kind="nativeAuthority", schema_version="test", payload={}),
        native_validator=validator,
        candidate_id="malformed_catalog_parent",
    )
    operator = TypedGrammarPairOperator(
        grammar_factory=lambda _: None,
        native_validator=validator,
        hold_operator_policy=default_hold_operator_policy(),
    )
    with pytest.raises(TemporalDiscoveryContractError, match="catalog requires timeframes"):
        operator.initial_protection_plans(module)


def test_coupled_and_decoupled_mutations_are_admitted_by_dashboard_validator() -> None:
    dashboard_root = Path("C:/repos/Trading-Dashboard")
    fixture = dashboard_root / "shared/python/fuzzfolio_core/tests/test_temporal_search_candidate_validation.py"
    tree = ast.parse(fixture.read_text(encoding="utf-8"))
    functions = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name in {"_transition", "_candidate_profile"}
    ]
    namespace: dict[str, object] = {}
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(fixture), "exec"), namespace)
    source = namespace["_candidate_profile"]()  # type: ignore[operator]
    policy = default_initial_protection_policy()
    plans = enumerate_initial_protection_plans(source, policy)
    client = SubprocessCandidateValidator(
        [
            str(dashboard_root / "compute-service/.venv/Scripts/python.exe"),
            str(dashboard_root / "scripts/temporal_search_validate_candidate.py"),
        ],
        timeout_seconds=10,
    )
    validator = DashboardV2ModuleValidator(client)
    try:
        for target in (
            {"kind": "reward_multiple", "multiple": 0.5},
            {"kind": "fixed_percent", "percent": 0.5},
            {"kind": "none"},
        ):
            plan = next(
                item
                for item in plans
                if item["site"] == "target" and item["replacement"] == target
            )
            child, _audit = apply_initial_protection_plan(source, plan, policy)
            report = validator.validate_v2(
                profile=child, candidate_id="dashboard_initial_protection_" + target["kind"]
            )
            assert report["candidateAcceptable"] is True
    finally:
        client.close()
