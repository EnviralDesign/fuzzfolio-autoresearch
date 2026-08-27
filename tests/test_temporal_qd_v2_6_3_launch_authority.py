from __future__ import annotations

import copy

import pytest

from autoresearch.evidence_plan import canonical_sha256
from autoresearch.temporal_qd_v2_6_3_launch_authority import (
    CROSS_ROOT_SCHEMA,
    PREREGISTRATION_SCHEMA,
    _container_inspect,
    build_selection_result,
    validate_confirmation_preregistration,
    validate_cross_root_report,
)


WORKER = {
    "git_sha": "fuzzfolio-exact-commit",
    "image_digest": "sha256:image",
    "contract_hash": "sha256:contract",
}


def _preregistration() -> dict:
    value = {
        "schemaVersion": PREREGISTRATION_SCHEMA,
        "sourceCommit": "autoresearch-exact-commit",
        "status": "pending",
        "dispatchEnabled": False,
        "confirmationExecutionAuthorized": False,
        "productionConfirmed": False,
        "familyLevelInferencePermitted": False,
        "inspectedPanels": ["panel-1", "panel-2", "panel-3"],
        "windows": [
            {"windowId": "untouched-2027-q1"},
            {"windowId": "untouched-2027-q2"},
            {"windowId": "untouched-2027-q3"},
            {"windowId": "untouched-2027-q4"},
        ],
        "candidateBindings": [
            {"candidateId": f"candidate-{block}-{arm}", "blockId": block, "arm": arm}
            for block in ("block-a", "block-b", "block-c")
            for arm in ("P", "T", "E", "TE")
        ],
        "selectionRule": {
            "allowedProjectedTaskCounts": [0, 16, 32, 48],
            "manualSelectionPermitted": False,
            "candidateSubstitutionPermitted": False,
            "replacementBlockPermitted": False,
            "confirmationMayRescueInspectedFailure": False,
            "poolingPermitted": False,
            "majorityVotePermitted": False,
        },
        "workerPins": {
            "sourceCommit": WORKER["git_sha"],
            "imageDigest": WORKER["image_digest"],
            "contractSha256": WORKER["contract_hash"],
        },
    }
    value["preregistrationSha256"] = canonical_sha256(value)
    return value


def test_preregistration_rejects_result_derived_selection_fields() -> None:
    preregistration = _preregistration()
    validate_confirmation_preregistration(
        preregistration,
        expected_worker=WORKER,
        expected_source_commit="autoresearch-exact-commit",
    )
    invalid = copy.deepcopy(preregistration)
    invalid["analysisSha256"] = "sha256:synthetic-no-market-analysis"
    invalid["blocks"] = []
    invalid["preregistrationSha256"] = canonical_sha256(
        {key: value for key, value in invalid.items() if key != "preregistrationSha256"}
    )
    with pytest.raises(ValueError, match="result-derived"):
        validate_confirmation_preregistration(
            invalid,
            expected_worker=WORKER,
            expected_source_commit="autoresearch-exact-commit",
        )


def test_selection_result_refuses_no_market_analysis() -> None:
    with pytest.raises(ValueError, match="real inspected market"):
        build_selection_result(
            preregistration=_preregistration(),
            real_inspected_analysis={"analysisProvenance": "no_market_fixture"},
        )


def test_cross_root_report_requires_complete_portable_inventory() -> None:
    report = {
        "schemaVersion": CROSS_ROOT_SCHEMA,
        "artifactInventory": [
            {
                "logicalId": "worker-contract",
                "leftRawSha256": "sha256:left",
                "rightRawSha256": "sha256:right",
                "leftSizeBytes": 1,
                "rightSizeBytes": 1,
                "byteIdentical": True,
            }
        ],
        "allPortableArtifactsByteIdentical": True,
        "noAbsoluteHostRootInPortableAuthority": True,
        "rootBoundExcludedArtifacts": [],
    }
    report["crossRootReportSha256"] = canonical_sha256(report)
    with pytest.raises(ValueError, match="cross-root authority proof failed"):
        validate_cross_root_report(report)


def test_container_inspect_requires_a_single_raw_docker_object(tmp_path) -> None:
    path = tmp_path / "inspect.json"
    path.write_text('[{"Image":"sha256:image"}]', encoding="utf-8")
    assert _container_inspect(path) == {"Image": "sha256:image"}
    path.write_text('[]', encoding="utf-8")
    with pytest.raises(TypeError, match="exactly one Docker inspect"):
        _container_inspect(path)
