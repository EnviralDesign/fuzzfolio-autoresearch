from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from autoresearch.temporal_generator_v3_reachability_canary import (
    audit_generator_v3_reachability_canary,
    build_generator_v3_reachability_canary,
    run_generator_v3_reachability_canary,
)
from autoresearch.temporal_operator_construction_v3 import (
    DIRECTION_FLIP,
    GRAPH_BOUND_TIMEFRAME,
    INDICATOR_FAMILY_SUBSTITUTION,
    MANAGEMENT_PLAN,
    SCALAR_DYNAMIC_MANAGEMENT,
    GeneratorV3ConstructionRegistry,
    inspect_construction_reachability,
)


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "temporal_generator_v3_reachability"


def _fixture(name: str) -> dict:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _canary_profiles() -> list[dict]:
    # Ordering is deliberate: each construction selects the first applicable
    # validated fixture parent, so create/direction and delete remain separate
    # authored parents while scalar/timeframe use the scalar-authorized parent.
    return [
        _fixture("management-create-direction-profile.json"),
        _fixture("management-delete-profile.json"),
        _fixture("scalar-timeframe-profile.json"),
    ]


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _resign_validation_binding(binding: dict, validation: dict) -> None:
    """Model a malicious nested self-rehash, not a stale-file edit."""

    binding["validationPayloadSha256"] = canonical_sha256(validation)
    binding["programSha256"] = validation.get("programSha256")
    binding["validatorReportSha256"] = validation.get("validationReportSha256")
    binding.pop("validationBindingSha256", None)
    binding["validationBindingSha256"] = canonical_sha256(binding)


def _resign_canary_artifacts(root: Path) -> None:
    report_path = root / "generator-v3-reachability-canary.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report.pop("reportSha256", None)
    report["reportSha256"] = canonical_sha256(report)
    _write_json(report_path, report)

    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("manifestSha256", None)
    files = []
    for path in sorted(
        item for item in root.rglob("*") if item.is_file() and item.name != "manifest.json"
    ):
        files.append(
            {
                "relativePath": path.relative_to(root).as_posix(),
                "length": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest().upper(),
            }
        )
    manifest["fileCount"] = len(files)
    manifest["files"] = files
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write_json(manifest_path, manifest)


def _catalog() -> dict:
    return {
        "timeframes": {"M5": {"value": "M5"}, "M15": {"value": "M15"}},
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
            },
            {
                "meta": {
                    "id": "SAR_TREND",
                    "managementScalarOutputs": [
                        {"outputKey": "sar", "valueKind": "price_level", "unit": "price"}
                    ],
                }
            },
        ],
    }


def _profile() -> dict:
    return {
        "version": "v2",
        "name": "generator v3 reachability fixture",
        "description": "repository-only construction fixture",
        "instruments": ["EURUSD"],
        "directionMode": "long",
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
        "executionConfig": {
            "managementLibrary": {
                "version": "temporal_management_v1",
                "defaultPlanId": "base",
                "plans": [
                    {
                        "id": "base",
                        "initialStop": {"kind": "fixed_percent", "percent": 1.0},
                        "initialTarget": {"kind": "reward_multiple", "multiple": 2.0},
                    }
                ],
            }
        },
        "graph": {
            "kind": "temporal_graph_v1",
            "semanticPolicy": "temporal_graph_semantics_v1",
            "eventSchema": "temporal_event_v1",
            "factLibrary": "temporal_market_facts_v1",
            "guardLibrary": "temporal_guards_v1",
            "actionLibrary": "temporal_market_actions_v1",
            "clockRequirement": "clock.completed_bar",
            "fidelityRequirements": [],
            "initialStateId": "flat",
            "states": [{"id": "flat"}, {"id": "entry"}, {"id": "open"}],
            "evidenceGroups": [{"id": "atr_context", "indicatorInstanceIds": ["atr"]}],
            "eventBindings": [],
            "transitions": [
                {
                    "id": "enter",
                    "sourceStateId": "flat",
                    "destinationStateId": "entry",
                    "eventClass": "decision",
                    "priority": 10,
                    "guard": {"kind": "position_exists", "expected": False},
                    "actions": [{"kind": "enter_next_open", "managementPlanId": "base"}],
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
                {
                    "id": "closed",
                    "sourceStateId": "open",
                    "destinationStateId": "flat",
                    "eventClass": "execution",
                    "priority": 10,
                    "guard": {"kind": "execution_status_is", "status": "closed"},
                    "actions": [],
                    "reasonCode": "closed",
                },
            ],
        },
    }


class _Validator:
    def __init__(self, *, reject: str | None = None) -> None:
        self.reject = reject

    def validate(self, *, candidate_id: str, source_profile: dict, expected_raw_source_profile_sha256: str) -> dict:
        assert canonical_sha256(source_profile) == expected_raw_source_profile_sha256
        invalid = self.reject is not None and self.reject in candidate_id
        digest = hashlib.sha256(canonical_sha256(source_profile).encode("utf-8")).hexdigest()
        return {
            "schemaVersion": "temporal_search_candidate_validation_v1",
            "candidateId": candidate_id,
            "rawSourceProfileSha256": expected_raw_source_profile_sha256,
            "candidateAcceptable": not invalid,
            "status": "semantic_invalid" if invalid else "valid_evaluable",
            "validationReportSha256": "sha256:" + hashlib.sha256(candidate_id.encode()).hexdigest(),
            "profileSnapshotSha256": expected_raw_source_profile_sha256,
            "programSha256": "sha256:" + digest,
            "issues": [{"code": "semantic.test_rejection"}] if invalid else [],
        }


SHA_A = "sha256:" + "a" * 64
SHA_B = "sha256:" + "b" * 64


def _first(registry: GeneratorV3ConstructionRegistry, profile: dict, operator_id: str, *, kind: str | None = None) -> dict:
    plans = registry.get(operator_id).enumerate_plans(profile)
    if kind is not None:
        plans = [plan for plan in plans if plan["construction"]["kind"] == kind]
    assert plans
    return plans[0]


def test_scalar_binding_and_dynamic_locator_are_atomic_authorized_and_audited() -> None:
    profile = _profile()
    registry = GeneratorV3ConstructionRegistry(_catalog())
    operator = registry.get(SCALAR_DYNAMIC_MANAGEMENT)
    first = operator.enumerate_plans(profile)
    assert first == operator.enumerate_plans(copy.deepcopy(profile))
    child, application = operator.apply(profile, first[0], parent_validated_program_sha256=SHA_A, child_validated_program_sha256=SHA_B)

    library = child["executionConfig"]["managementLibrary"]
    assert library["scalarBindings"] == [
        {
            "id": "scalar_atr_atr_raw",
            "indicatorInstanceId": "atr",
            "outputKey": "atr_raw",
            "valueKind": "price_distance",
            "availability": "completed_bar",
        }
    ]
    assert child["executionConfig"]["managementLibrary"]["plans"][0]["initialStop"] == {
        "kind": "indicator_distance_multiple",
        "bindingId": "scalar_atr_atr_raw",
        "multiple": 1.0,
    }
    assert inspect_construction_reachability(child)["acceptable"] is True
    assert application["constructionIdentitySha256"] == first[0]["constructionIdentitySha256"]
    assert application["mutationTrace"][0]["operation"] == "replace_locator"
    assert operator.audit(profile, child, application)["allChecksPassed"] is True


def test_scalar_authorization_fails_closed_when_profile_omits_catalog_metadata() -> None:
    profile = _profile()
    profile["indicators"][0]["meta"].pop("managementScalarOutputs")
    registry = GeneratorV3ConstructionRegistry(_catalog())
    assert registry.get(SCALAR_DYNAMIC_MANAGEMENT).enumerate_plans(profile) == []


def test_management_plan_create_delete_preserves_default_and_reference_closure() -> None:
    profile = _profile()
    registry = GeneratorV3ConstructionRegistry(_catalog())
    operator = registry.get(MANAGEMENT_PLAN)
    create = _first(registry, profile, MANAGEMENT_PLAN, kind="create_plan")
    created, app = operator.apply(profile, create, parent_validated_program_sha256=SHA_A, child_validated_program_sha256=SHA_B)
    library = created["executionConfig"]["managementLibrary"]
    created_id = create["construction"]["plan"]["id"]
    assert library["defaultPlanId"] == "base"
    assert {item["id"] for item in library["plans"]} == {"base", created_id}
    assert created["graph"]["transitions"][0]["actions"][0]["managementPlanId"] == created_id
    assert inspect_construction_reachability(created)["acceptable"] is True
    assert app["staticInvariantReport"]["allChecksPassed"] is True

    delete = _first(registry, created, MANAGEMENT_PLAN, kind="delete_plan")
    deleted, _ = operator.apply(created, delete, parent_validated_program_sha256=SHA_B, child_validated_program_sha256=SHA_A)
    report = inspect_construction_reachability(deleted)
    assert report["acceptable"] is True
    assert report["orphanManagementPlanIds"] == []
    assert report["orphanScalarBindingIds"] == []


def test_direction_timeframe_and_deferred_family_contracts() -> None:
    profile = _profile()
    registry = GeneratorV3ConstructionRegistry(_catalog())
    direction = _first(registry, profile, DIRECTION_FLIP)
    flipped, _ = registry.get(DIRECTION_FLIP).apply(profile, direction, parent_validated_program_sha256=SHA_A, child_validated_program_sha256=SHA_B)
    assert flipped["directionMode"] == "short"
    timeframe = _first(registry, profile, GRAPH_BOUND_TIMEFRAME)
    changed, application = registry.get(GRAPH_BOUND_TIMEFRAME).apply(profile, timeframe, parent_validated_program_sha256=SHA_A, child_validated_program_sha256=SHA_B)
    assert changed["indicators"][0]["config"]["timeframe"] == "M15"
    assert application["evidenceScope"]["lakeScopeRegenerationRequired"] is True
    deferred = registry.deferred_operators[0]
    assert deferred.operator_id == INDICATOR_FAMILY_SUBSTITUTION
    assert deferred.enabled is False
    assert deferred.enumerate_plans(profile) == []
    with pytest.raises(TemporalDiscoveryContractError, match="deferred"):
        deferred.apply(profile, {}, parent_validated_program_sha256=SHA_A, child_validated_program_sha256=SHA_B)


def test_canary_is_deterministic_hash_bound_and_exactly_replayable(tmp_path) -> None:
    catalog = _fixture("catalog.json")
    profiles = _canary_profiles()
    first = run_generator_v3_reachability_canary(catalog=catalog, base_profiles=profiles, validator=_Validator())
    second = run_generator_v3_reachability_canary(catalog=copy.deepcopy(catalog), base_profiles=copy.deepcopy(profiles), validator=_Validator())
    assert first == second
    assert first["allEnabledFamiliesAdmitted"] is True
    assert first["baseProfileSha256"] is None
    assert len(first["baseProfileSha256s"]) == 3
    for counts in first["enabledFamilyCounts"].values():
        assert all(counts[key] > 0 for key in ("proposed", "staticReachable", "validatorValid", "admitted"))
    rows = {row["exampleId"]: row for row in first["records"]}
    assert rows["management_plan_create"]["baseProfileSha256"] == canonical_sha256(profiles[0])
    assert rows["direction_flip"]["baseProfileSha256"] == canonical_sha256(profiles[0])
    assert rows["management_plan_delete"]["baseProfileSha256"] == canonical_sha256(profiles[1])
    assert rows["scalar_dynamic_management"]["baseProfileSha256"] == canonical_sha256(profiles[2])
    assert rows["graph_bound_timeframe"]["baseProfileSha256"] == canonical_sha256(profiles[2])
    result = build_generator_v3_reachability_canary(catalog=catalog, base_profiles=profiles, validator=_Validator(), output_root=tmp_path)
    audit = audit_generator_v3_reachability_canary(tmp_path)
    assert audit["reportSha256"] == result["reportSha256"]


@pytest.mark.parametrize("role", ["parent", "child"])
def test_canary_audit_rejects_self_rehashed_validation_program_tampering(
    tmp_path: Path, role: str
) -> None:
    build_generator_v3_reachability_canary(
        catalog=_fixture("catalog.json"),
        base_profiles=_canary_profiles(),
        validator=_Validator(),
        output_root=tmp_path,
    )
    report_path = tmp_path / "generator-v3-reachability-canary.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    row = next(item for item in report["records"] if item["admitted"])
    replacement_program = canonical_sha256({"tamperedValidation": role})

    if role == "parent":
        base = next(
            item
            for item in report["baseProfileValidations"]
            if item["baseProfileSha256"] == row["baseProfileSha256"]
        )
        targets = [
            (row["parentValidation"], row["parentValidationBinding"]),
            (base["validation"], base["validationBinding"]),
        ]
    else:
        targets = [(row["validation"], row["childValidationBinding"])]
    for validation, binding in targets:
        validation["programSha256"] = replacement_program
        validation["validationReportSha256"] = canonical_sha256(
            {"selfRehashedValidatorReport": role, "programSha256": replacement_program}
        )
        _resign_validation_binding(binding, validation)
    _write_json(report_path, report)
    _resign_canary_artifacts(tmp_path)

    with pytest.raises(
        TemporalDiscoveryContractError,
        match=rf"canary {role} validation program binding mismatch",
    ):
        audit_generator_v3_reachability_canary(tmp_path)


def test_canary_audit_rejects_self_rehashed_parent_profile_tampering(tmp_path: Path) -> None:
    build_generator_v3_reachability_canary(
        catalog=_fixture("catalog.json"),
        base_profiles=_canary_profiles(),
        validator=_Validator(),
        output_root=tmp_path,
    )
    report_path = tmp_path / "generator-v3-reachability-canary.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    base = report["baseProfileValidations"][0]
    validation = base["validation"]
    binding = base["validationBinding"]
    validation["rawSourceProfileSha256"] = canonical_sha256({"tampered": "parent"})
    validation["validationReportSha256"] = canonical_sha256(
        {"selfRehashedValidatorReport": "parent-profile"}
    )
    _resign_validation_binding(binding, validation)
    _write_json(report_path, report)
    _resign_canary_artifacts(tmp_path)

    with pytest.raises(
        TemporalDiscoveryContractError,
        match="canary validation source-profile identity mismatch",
    ):
        audit_generator_v3_reachability_canary(tmp_path)


def test_canary_single_base_profile_interface_remains_supported() -> None:
    report = run_generator_v3_reachability_canary(
        catalog=_catalog(), base_profile=_profile(), validator=_Validator()
    )
    assert report["baseProfileSha256"] == canonical_sha256(_profile())
    assert report["baseProfileSha256s"] == [canonical_sha256(_profile())]


def test_canary_failure_taxonomy_records_validator_rejection_without_market_replay() -> None:
    report = run_generator_v3_reachability_canary(catalog=_catalog(), base_profile=_profile(), validator=_Validator(reject="direction_flip"))
    row = next(item for item in report["records"] if item["exampleId"] == "direction_flip")
    assert row["proposed"] is True
    assert row["staticReachable"] is True
    assert row["validatorValid"] is False
    assert row["admitted"] is False
    assert row["rejectionReasonCodes"] == ["validator_rejected", "semantic.test_rejection"]
    assert row["runtimeEvidence"]["fired"]["status"] == "unmeasured"
