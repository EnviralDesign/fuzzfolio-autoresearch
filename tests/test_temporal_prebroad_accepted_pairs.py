from __future__ import annotations

from copy import deepcopy
from functools import cache
import json
from pathlib import Path
import subprocess

import pytest

from autoresearch.temporal_prebroad_accepted_pairs import build_accepted_pairs
from autoresearch.temporal_prebroad_control import (
    DEFAULT_DASHBOARD_PYTHON,
    WINDOWS,
    build_prebroad_authority,
)
from autoresearch.temporal_search import TemporalSearchContractError, canonical_sha256


SOURCE_WORKER_SHA256 = "sha256:" + "1" * 64
TARGET_WORKER_SHA256 = "sha256:" + "2" * 64
_DASHBOARD_CORE_CANDIDATE_TEST = (
    Path(r"C:\repos\Trading-Dashboard")
    / "shared"
    / "python"
    / "fuzzfolio_core"
    / "tests"
    / "test_temporal_search_candidate_validation.py"
)


def _profile(index: int) -> dict:
    del index
    return deepcopy(_current_catalog_backed_profile())


def _plan(window_id: str, start: str, end: str) -> dict:
    warmup_start = "2023-09-18T00:00:00Z" if start.startswith("2023-") else "2021-06-18T00:00:00Z"
    plan = {
        "schema_version": "fuzzfolio.replay-evidence-plan.v2",
        "analysis_window_start": start,
        "analysis_window_end": end,
        "campaign_plan_id": f"trusted:{window_id}",
        "coverage_policy": "require_complete",
        "data_availability_cutoff": "2026-01-01T00:00:00Z",
        "evidence_role": "development_parity",
        "execution_cell_sha256": None,
        "lake_manifest_sha256": None,
        "requested_horizon_months": 1,
        "selection_data_end": end,
        "profile_snapshot_sha256": "sha256:" + "a" * 64,
        "lake_window_binding": {
            "schema_version": "fuzzfolio.market-data-window-binding.v1",
            "semantic_contract_id": "fuzzfolio.canonical-bars.semantic-digest.v2",
            "request": {
                "schema_version": "fuzzfolio.market-data-window-request.v1",
                "dataset": "bars",
                "pairs": ["EURUSD"],
                "timeframes": ["M5", "M15"],
                # This is deliberate warmup, not an analysis-window equality.
                "data_start": warmup_start,
                "data_end": end,
                "coverage_policy": "require_complete",
            },
            "window_semantic_sha256": "sha256:" + "b" * 64,
            "attestation_sha256": "sha256:" + "c" * 64,
            "creation_global_coverage_sha256": "sha256:" + "d" * 64,
            "creation_source_coverage_sha256": "sha256:" + "e" * 64,
            "legacy_selection_manifest_sha256": None,
        },
    }
    identity = dict(plan)
    identity.pop("lake_manifest_sha256")
    plan["plan_id"] = canonical_sha256(identity)
    return plan


def _inputs() -> tuple[dict, dict, dict]:
    population = {"candidates": []}
    reports = {}
    for index in range(8):
        candidate_id = f"qd_candidate_{index}"
        profile = _profile(index)
        profile_sha = canonical_sha256(profile)
        report = {
            "candidateId": candidate_id,
            "rawSourceProfileSha256": profile_sha,
            "profileSnapshotSha256": "sha256:" + f"{index:x}" * 64,
            "programSha256": "sha256:" + "1" * 64,
            "validationReportSha256": "sha256:" + "2" * 64,
            "evaluatorId": "bar_bidirectional_single_position_execution_v2",
            "status": "valid_evaluable",
            "candidateAcceptable": True,
        }
        population["candidates"].append({"candidateId": candidate_id, "sourceProfile": profile, "sourceProfileSha256": profile_sha})
        reports[candidate_id] = report
    template_inputs = [{"windowId": window_id, "evidencePlan": _plan(window_id, start, end)} for window_id, start, end in WINDOWS]
    preparation = {
        "workerContract": {"workerContractSha256": SOURCE_WORKER_SHA256, "workerContractSchema": "replay-worker-contract-v1"},
        "developmentWindows": [{"windowId": window_id, "analysisWindowStart": start, "analysisWindowEnd": end} for window_id, start, end in WINDOWS],
        "candidates": [{"windowInputs": deepcopy(template_inputs)}],
    }
    return population, preparation, reports


@cache
def _current_catalog_backed_profile() -> dict:
    """Build a real v3/both profile with catalog-owned RSI instances.

    This intentionally crosses only the frozen Dashboard Python boundary used
    by admission.  It creates no evidence plan and reads no market data.
    """

    code = """import ast,json,sys
from pathlib import Path
from fuzzfolio_core.temporal_graph.bidirectional_compiler import compile_bidirectional_profile
from fuzzfolio_core.temporal_graph.graph_models import TemporalGraphProfile
tree=ast.parse(Path(sys.argv[1]).read_text(encoding='utf-8'))
namespace={}
nodes=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in {'_transition','_candidate_profile'}]
exec(compile(ast.Module(body=nodes,type_ignores=[]),sys.argv[1],'exec'),namespace)
def indicator(instance_id):
    return {'meta':{'id':'RSI_MEAN_REVERSION','instanceId':instance_id},'config':{'isActive':True,'timeframe':'M5','lookbackBars':1,'useFormingBar':False,'ranges':{'buy':[20.0,40.0],'sell':[60.0,80.0]},'talibConfig':[]}}
long=namespace['_candidate_profile']()
short=namespace['_candidate_profile']()
short['directionMode']='short'
long['indicators']=[indicator('catalog_rsi')]
short['indicators']=[indicator('catalog_rsi')]
profile=compile_bidirectional_profile(TemporalGraphProfile.model_validate(long),TemporalGraphProfile.model_validate(short),name='catalog admission fixture')
print(json.dumps(profile.model_dump(mode='json',by_alias=True,exclude_none=False),sort_keys=True,ensure_ascii=True,allow_nan=False))
"""
    completed = subprocess.run(
        [str(DEFAULT_DASHBOARD_PYTHON), "-c", code, str(_DASHBOARD_CORE_CANDIDATE_TEST)],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def _replace_profile(
    population: dict, reports: dict, *, candidate_id: str, profile: dict
) -> None:
    profile_sha = canonical_sha256(profile)
    candidate = next(
        item for item in population["candidates"] if item["candidateId"] == candidate_id
    )
    candidate["sourceProfile"] = profile
    candidate["sourceProfileSha256"] = profile_sha
    reports[candidate_id]["rawSourceProfileSha256"] = profile_sha


def test_adapter_preserves_attested_request_and_uses_raw_source_profile_hash() -> None:
    population, preparation, reports = _inputs()
    accepted = build_accepted_pairs(
        population,
        preparation,
        worker_contract_sha256=TARGET_WORKER_SHA256,
        native_reports=reports,
    )
    assert accepted["schemaVersion"] == "temporal_prebroad_accepted_pairs_v2"
    assert accepted["workerContract"] == {
        "workerContractSha256": TARGET_WORKER_SHA256,
        "workerContractSchema": "replay-worker-contract-v1",
    }
    assert [item["candidateId"] for item in accepted["pairs"]] == sorted(reports)
    assert all(item["timeframe"] == "M5" and item["barLimit"] == 5000 for item in accepted["pairs"])
    for pair in accepted["pairs"]:
        assert pair["profileSha256"] == canonical_sha256(pair["profile"])
        assert pair["profileSha256"] == pair["validation"]["rawSourceProfileSha256"]
        assert pair["profileSha256"] != pair["validation"]["profileSnapshotSha256"]
        resolution = pair["catalogResolution"]
        assert resolution["rawSourceProfileSha256"] == pair["profileSha256"]
        assert resolution["resolvedProfileSnapshotSha256"].startswith("sha256:")
        assert resolution["resolvedProgramSha256"].startswith("sha256:")
        assert resolution["indicatorCatalogSha256"].startswith("sha256:")
        resolution_material = dict(resolution)
        resolution_sha = resolution_material.pop("catalogResolutionSha256")
        assert resolution_sha == canonical_sha256(resolution_material)
        for original, rotated in zip(preparation["candidates"][0]["windowInputs"], pair["windowInputs"], strict=True):
            assert rotated["evidencePlan"]["lake_window_binding"] == original["evidencePlan"]["lake_window_binding"]
            assert rotated["evidencePlan"]["execution_cell_sha256"] is None
            assert rotated["evidencePlan"]["requested_horizon_months"] == 1
    assert build_prebroad_authority(accepted, native_reports=reports)["taskCount"] == 16


def test_adapter_rebinds_from_prior_control_preparation_without_lake_changes() -> None:
    population, preparation, reports = _inputs()
    preparation["pairs"] = preparation.pop("candidates")

    accepted = build_accepted_pairs(
        population,
        preparation,
        worker_contract_sha256=TARGET_WORKER_SHA256,
        native_reports=reports,
    )
    assert accepted["workerContract"]["workerContractSha256"] == TARGET_WORKER_SHA256
    assert all(
        pair["windowInputs"][0]["evidencePlan"]["lake_window_binding"]
        == preparation["pairs"][0]["windowInputs"][0]["evidencePlan"][
            "lake_window_binding"
        ]
        for pair in accepted["pairs"]
    )


def test_adapter_requires_current_catalog_hydration_before_evidence_rotation() -> None:
    population, preparation, reports = _inputs()
    candidate_id = "qd_candidate_0"
    concrete = _current_catalog_backed_profile()
    _replace_profile(
        population,
        reports,
        candidate_id=candidate_id,
        profile=concrete,
    )

    accepted = build_accepted_pairs(
        population,
        preparation,
        worker_contract_sha256=TARGET_WORKER_SHA256,
        # The catalog guard remains native even when the semantic report is
        # injected by this focused adapter test.
        native_reports=reports,
    )
    assert accepted["pairs"][0]["profile"]["indicators"][0]["meta"]["id"] == "RSI_MEAN_REVERSION"

    synthetic = deepcopy(concrete)
    synthetic["indicators"][0]["meta"]["id"] = "I_BREAKOUT"
    _replace_profile(
        population,
        reports,
        candidate_id=candidate_id,
        profile=synthetic,
    )
    with pytest.raises(
        TemporalSearchContractError,
        match=r"I_BREAKOUT.*absent from the current catalog",
    ):
        build_accepted_pairs(
            population,
            preparation,
            worker_contract_sha256=TARGET_WORKER_SHA256,
            native_reports=reports,
        )


def test_catalog_resolution_is_content_bound_into_prebroad_authority() -> None:
    population, preparation, reports = _inputs()
    accepted = build_accepted_pairs(
        population,
        preparation,
        worker_contract_sha256=TARGET_WORKER_SHA256,
        native_reports=reports,
    )
    authority = build_prebroad_authority(accepted, native_reports=reports)
    tampered = deepcopy(accepted)
    resolution = tampered["pairs"][0]["catalogResolution"]
    resolution["indicatorCatalogSha256"] = "sha256:" + "f" * 64
    assert canonical_sha256(tampered) != canonical_sha256(accepted)
    with pytest.raises(TemporalSearchContractError, match="catalog resolution identity mismatch"):
        build_prebroad_authority(tampered, native_reports=reports)
    assert authority["pairs"][0]["catalogResolution"] == accepted["pairs"][0]["catalogResolution"]

    rebound = deepcopy(accepted)
    rebound_resolution = rebound["pairs"][0]["catalogResolution"]
    rebound_resolution["indicatorCatalogSha256"] = "sha256:" + "e" * 64
    rebound_material = dict(rebound_resolution)
    rebound_material.pop("catalogResolutionSha256")
    rebound_resolution["catalogResolutionSha256"] = canonical_sha256(rebound_material)
    rebound_authority = build_prebroad_authority(rebound, native_reports=reports)
    assert rebound_authority["authorityId"] != authority["authorityId"]


def test_adapter_rejects_tampered_attested_binding_and_native_forgery() -> None:
    population, preparation, reports = _inputs()
    preparation["candidates"][0]["windowInputs"][0]["evidencePlan"]["lake_window_binding"]["request"]["data_start"] = "2023-10-02T00:00:00Z"
    with pytest.raises(TemporalSearchContractError, match="outside the immutable"):
        build_accepted_pairs(
            population,
            preparation,
            worker_contract_sha256=TARGET_WORKER_SHA256,
            native_reports=reports,
        )

    population, preparation, reports = _inputs()
    reports["qd_candidate_0"]["rawSourceProfileSha256"] = "sha256:" + "f" * 64
    with pytest.raises(TemporalSearchContractError, match="source profile identity drifted"):
        build_accepted_pairs(
            population,
            preparation,
            worker_contract_sha256=TARGET_WORKER_SHA256,
            native_reports=reports,
        )


def test_adapter_rejects_worker_contract_drift_and_protected_window_overlap() -> None:
    population, preparation, reports = _inputs()
    preparation["workerContract"]["workerContractSha256"] = "not-a-sha"
    with pytest.raises(TemporalSearchContractError, match="trusted preparation worker"):
        build_accepted_pairs(
            population,
            preparation,
            worker_contract_sha256=TARGET_WORKER_SHA256,
            native_reports=reports,
        )

    population, preparation, reports = _inputs()
    with pytest.raises(TemporalSearchContractError, match="required worker contract"):
        build_accepted_pairs(
            population,
            preparation,
            worker_contract_sha256="not-a-sha",
            native_reports=reports,
        )

    population, preparation, reports = _inputs()
    plan = preparation["candidates"][0]["windowInputs"][0]["evidencePlan"]
    plan["analysis_window_start"] = "2024-06-29T00:00:00Z"
    with pytest.raises(TemporalSearchContractError, match="fixed month"):
        build_accepted_pairs(
            population,
            preparation,
            worker_contract_sha256=TARGET_WORKER_SHA256,
            native_reports=reports,
        )
