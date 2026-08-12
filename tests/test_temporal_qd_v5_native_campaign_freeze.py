"""Oracle coverage for the closed v5 native campaign freezer."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

import pytest

import autoresearch.temporal_qd_campaign_native as campaign_native
from autoresearch.evolvable_module_qd_authority import (
    evolvable_behavior_attribution_requirement,
)
from autoresearch.result_codec import canonical_json_bytes
from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from autoresearch.temporal_qd_campaign_native import (
    freeze_qd_v5_campaign_native,
    freeze_qd_v5_campaign_oracle,
)
from autoresearch.temporal_qd_supervisor import _ladder_cohort
from autoresearch.temporal_qd_evaluation_population import build_rotating_cohort_population
from autoresearch.temporal_qd_evolution import directional_qd_archive_policy_authority
from autoresearch.temporal_qd_rotating_evidence import build_rotating_evidence_contract
from autoresearch.temporal_search import build_authority


def _prebuilt_native_freezer() -> Path:
    suffix = ".exe" if os.name == "nt" else ""
    binary = (
        Path(__file__).resolve().parents[1]
        / "rust"
        / "temporal-qd"
        / "target"
        / "debug"
        / f"temporal-qd-campaign-freeze{suffix}"
    )
    if not binary.is_file():
        pytest.skip("requires a prebuilt temporal-qd-campaign-freeze binary")
    return binary


@pytest.fixture(autouse=True)
def _pin_native_freezer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests may use an existing binary, but must never compile one on demand."""

    original = freeze_qd_v5_campaign_native

    def invoke(*args: object, **kwargs: object) -> dict:
        if "native_binary" in kwargs:
            raise AssertionError("fixture must be the only campaign-freeze binary source")
        return original(*args, native_binary=_prebuilt_native_freezer(), **kwargs)

    monkeypatch.setattr(
        sys.modules[__name__], "freeze_qd_v5_campaign_native", invoke
    )


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8"))


def _sealed_native_freeze_input_identities(
    *, evaluation: Path, template: Path, catalog: Path
) -> dict[str, str]:
    """Test-only stand-in for the compact receipt roots supplied in production."""

    def raw(path: Path) -> str:
        return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()

    return {
        "evaluation_population_raw_sha256": raw(evaluation),
        "template_preparation_sha256": canonical_sha256(
            json.loads(template.read_text(encoding="utf-8"))
        ),
        "construction_catalog_sha256": canonical_sha256(
            json.loads(catalog.read_text(encoding="utf-8"))
        ),
    }


def _fixture(root: Path, count: int) -> tuple[dict, dict, dict, Path, Path, Path, Path]:
    """Make a 4-window native-selection fixture without a Python task loop."""

    profile = {
        "version": "v3", "graph": {"states": [], "transitions": []},
        "instruments": ["EURUSD"], "directionMode": "both", "isActive": False,
        "executionConfig": {"exitPolicy": {"selectedCell": {
            "rewardMultiple": 2.0, "stopLossPercent": 0.5, "takeProfitPercent": 1.0,
        }}},
    }
    profile_sha = canonical_sha256(profile)
    years = [
        {"windowId": f"y{index}", "analysisWindowStart": f"{2019 + index}-01-01T00:00:00Z", "analysisWindowEnd": f"{2020 + index}-01-01T00:00:00Z"}
        for index in range(4)
    ]
    contract_input = {
        "schemaVersion": "temporal_qd_rotating_evidence_input_v1",
        "developmentYears": years,
        "validationWindow": {"windowId": "validation", "analysisWindowStart": "2024-01-01T00:00:00Z", "analysisWindowEnd": "2025-01-01T00:00:00Z"},
        "scrutinyWindow": {"windowId": "scrutiny", "analysisWindowStart": "2022-01-01T00:00:00Z", "analysisWindowEnd": "2025-01-01T00:00:00Z"},
        "outerTailStart": "2026-01-01T00:00:00Z", "provisionalSurvivorCount": count,
        "breederWidth": 1,
    }
    windows = build_rotating_evidence_contract(contract_input)["panels"][0]["windows"]
    inputs = []
    for window in windows:
        plan = {
            "schema_version": "fuzzfolio.replay-evidence-plan.v2",
            "profile_snapshot_sha256": profile_sha,
            "analysis_window_start": window["analysisWindowStart"],
            "analysis_window_end": window["analysisWindowEnd"],
            "execution_cell_sha256": canonical_sha256(profile["executionConfig"]["exitPolicy"]["selectedCell"]),
            "coverage_policy": "require_complete",
            "lake_window_binding": {"window_semantic_sha256": "sha256:" + hashlib.sha256(window["windowId"].encode()).hexdigest(), "request": {
                "data_start": window["analysisWindowStart"], "data_end": window["analysisWindowEnd"], "pairs": ["EURUSD"], "timeframes": ["M5"],
            }},
        }
        plan["plan_id"] = canonical_sha256(plan)
        inputs.append({"windowId": window["windowId"], "evidencePlan": plan})
    template = {
        "schemaVersion": "temporal_graph_candidate_window_preparation_v1", "authorityLabel": "v5-native-freeze-oracle",
        "workerContract": {"workerContractSha256": "sha256:" + "c" * 64, "workerContractSchema": "replay-worker-contract-v1"},
        "candidates": [{"candidateId": "template", "sourceProfile": profile, "sourceProfileSha256": profile_sha, "instrument": "EURUSD", "timeframe": "M5", "barLimit": 5000, "windowInputs": inputs}],
        "developmentWindows": [{key: window[key] for key in ("windowId", "analysisWindowStart", "analysisWindowEnd")} for window in windows],
        "prohibitedEvidence": [{"windowId": "reserved", "analysisWindowStart": "2025-01-01T00:00:00Z", "analysisWindowEnd": "2025-02-01T00:00:00Z", "reason": "reserved"}],
        "bounds": {"maxCandidates": count, "maxDevelopmentWindows": 4, "maxTasks": count * 4, "maxAttempts": 2, "deadlineSeconds": 60.0},
    }
    base_contract = build_rotating_evidence_contract(contract_input)
    contract_input["panelTemplates"] = {
        panel["panelId"]: {
            "path": f"{panel['panelId']}.json",
            "preparationSha256": canonical_sha256(template) if panel["panelId"] == "panel-1" else "sha256:" + "a" * 64,
            "authorityId": build_authority(template)["authorityId"] if panel["panelId"] == "panel-1" else "sha256:" + "b" * 64,
        }
        for panel in base_contract["panels"]
    }
    rotating = build_rotating_evidence_contract(contract_input)
    catalog = {"timeframes": {"M5": {}}, "indicators": []}
    common = root / "common"; template_path = common / "template.json"; catalog_path = common / "catalog.json"
    _write(template_path, template); _write(catalog_path, catalog)
    candidates = [{
        "candidateId": f"c{index:03d}",
        "candidateIdentitySha256": "sha256:" + hashlib.sha256(f"id-{index}".encode()).hexdigest(),
        "programSha256": "sha256:" + hashlib.sha256(f"program-{index}".encode()).hexdigest(),
        "sourceProfile": profile, "sourceProfileSha256": profile_sha,
    } for index in range(count)]
    oracle_population = build_rotating_cohort_population(candidates=candidates, generation_index=1, panel_id="panel-1", cohort_role="retained_parent_current_panel", rotating_evidence_sha256=rotating["rotatingEvidenceSha256"])
    oracle_population_path = root / "oracle-input" / "population.json"; _write(oracle_population_path, oracle_population)
    native_input = root / "native-input"; evaluation_path = native_input / "evaluation-population.json"
    evaluation = {"schemaVersion": "temporal_qd_evaluation_population_v1", "generationIndex": 1, "populationSha256": "sha256:" + "d" * 64, "candidates": candidates}
    evaluation["evaluationPopulationSha256"] = canonical_sha256(evaluation); _write(evaluation_path, evaluation)
    projection = native_input / "selected.jsonl"; rows = []
    for candidate in candidates:
        row = {"schemaVersion": "temporal_qd_rotating_candidate_projection_row_v1", "candidateId": candidate["candidateId"], "candidateIdentitySha256": candidate["candidateIdentitySha256"], "candidate": candidate}
        row["projectionRowSha256"] = canonical_sha256(row); rows.append(canonical_json_bytes(row))
    projection.write_bytes(b"\n".join(rows) + b"\n")
    selection = {"schemaVersion": "temporal_qd_rotating_cohort_selection_v1", "contractVersion": "temporal_qd_rotating_prefinalizer_v1", "generationIndex": 1, "campaignRole": "retained_parent_current_panel", "panelId": "panel-1", "rotatingEvidenceSha256": rotating["rotatingEvidenceSha256"], "candidateIds": [candidate["candidateId"] for candidate in candidates], "candidateProjection": {"relativePath": "selected.jsonl", "rawSha256": "sha256:" + hashlib.sha256(projection.read_bytes()).hexdigest(), "sizeBytes": projection.stat().st_size, "recordCount": count, "rowSchema": "temporal_qd_rotating_candidate_projection_row_v1"}, "sourceBindings": {}}
    selection["selectionSha256"] = canonical_sha256(selection)
    selection_path = native_input / "selection.json"; _write(selection_path, selection)
    return rotating, template, catalog, oracle_population_path, evaluation_path, selection_path, common


def test_native_v5_campaign_input_reader_rejects_oversized_checkpoint(
    tmp_path: Path,
) -> None:
    """Checkpoint validation is capped before it attempts JSON parsing."""

    (tmp_path / "campaign-input-checkpoint.json").write_bytes(
        b"x" * (campaign_native._CURRENT_V5_COMPACT_DOCUMENT_LIMIT_BYTES + 1)
    )
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="native v5 campaign-input checkpoint exceeds the control-document limit",
    ):
        campaign_native._validate_v5_campaign_input_checkpoint(tmp_path, {}, {})


def test_native_v5_freeze_rejects_bounded_pipe_overflow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The campaign bridge has no unbounded subprocess PIPE fallback."""

    evaluation = tmp_path / "evaluation.json"
    template = tmp_path / "template.json"
    catalog = tmp_path / "catalog.json"
    binary = tmp_path / "temporal-qd-campaign-freeze.exe"
    for path in (evaluation, template, catalog, binary):
        path.write_bytes(b"fixture")

    def overflow(*_args: object, **kwargs: object) -> object:
        assert (
            kwargs["stdout_limit_bytes"]
            == campaign_native._CURRENT_V5_COMPACT_STDOUT_LIMIT_BYTES
        )
        assert (
            kwargs["stderr_limit_bytes"]
            == campaign_native._CURRENT_V5_COMPACT_STDERR_LIMIT_BYTES
        )
        raise campaign_native.native.TemporalQDNativeError(
            "native Temporal QD command stderr exceeded its 262144 byte capture limit"
        )

    monkeypatch.setattr(campaign_native.native, "_run_checked", overflow)
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="stderr exceeded its 262144 byte capture limit",
    ):
        campaign_native.freeze_qd_v5_campaign_native(
            evaluation_population_path=evaluation,
            evaluation_population_raw_sha256="sha256:" + "a" * 64,
            template_preparation_path=template,
            template_preparation_sha256="sha256:" + "b" * 64,
            construction_catalog_path=catalog,
            construction_catalog_sha256="sha256:" + "c" * 64,
            output_root=tmp_path / "output",
            execution_engine_commit="d" * 40,
            worker_contract_sha256="sha256:" + "e" * 64,
            rotating_evidence={},
            archive_policy_authority={},
            behavior_attribution_requirement={},
            campaign_role="retained_parent_current_panel",
            panel_id="panel-1",
            native_binary=binary,
        )


@pytest.mark.parametrize("candidate_count", (2, 128))
def test_native_v5_freeze_matches_python_oracle_and_is_restart_safe(tmp_path: Path, candidate_count: int) -> None:
    rotating, _template, _catalog, population, evaluation, selection, common = _fixture(tmp_path, candidate_count)
    common_args = dict(
        execution_engine_commit="a" * 40, worker_contract_sha256="sha256:" + "c" * 64,
        construction_catalog_path=common / "catalog.json", rotating_evidence=rotating,
        campaign_role="retained_parent_current_panel", panel_id="panel-1",
        archive_policy_authority=directional_qd_archive_policy_authority(),
        behavior_attribution_requirement=evolvable_behavior_attribution_requirement(),
    )
    common_args.update(
        _sealed_native_freeze_input_identities(
            evaluation=evaluation,
            template=common / "template.json",
            catalog=common / "catalog.json",
        )
    )
    oracle_args = {
        key: value
        for key, value in common_args.items()
        if key
        not in {
            "evaluation_population_raw_sha256",
            "template_preparation_sha256",
            "construction_catalog_sha256",
        }
    }
    freeze_qd_v5_campaign_oracle(population_path=population, template_preparation_path=common / "template.json", output_root=tmp_path / "oracle", **oracle_args)
    native = freeze_qd_v5_campaign_native(evaluation_population_path=evaluation, template_preparation_path=common / "template.json", output_root=tmp_path / "native", cohort_selection_path=selection, **common_args)
    manifest = json.loads((tmp_path / "native" / ".native-v5-campaign-freeze-manifest.json").read_text(encoding="utf-8"))
    checkpoint = json.loads((tmp_path / "native" / "campaign-input-checkpoint.json").read_text(encoding="utf-8"))
    assert manifest["schemaVersion"] == "temporal_qd_v5_native_campaign_freeze_manifest_v2"
    assert manifest["manifestSha256"] == canonical_sha256({key: value for key, value in manifest.items() if key not in {"manifestSha256", "outputRoot"} and not key.endswith("Path")})
    assert checkpoint["schemaVersion"] == "temporal_qd_v5_campaign_input_checkpoint_v1"
    assert checkpoint["manifestSha256"] == manifest["manifestSha256"]
    assert checkpoint["nativeRuntimeAuthoritySha256"] == manifest["nativeRuntimeAuthoritySha256"]
    assert checkpoint["checkpointSha256"] == canonical_sha256(
        {key: value for key, value in checkpoint.items() if key != "checkpointSha256"}
    )
    assert native["checkpointSha256"] == checkpoint["checkpointSha256"]
    assert native["taskCount"] == candidate_count * 4
    assert native["telemetry"]["peakLiveTasks"] == 1
    assert native["telemetry"]["peakLiveCandidates"] == candidate_count
    for relative in ("cohort-population.json", "screening-run/task-manifest.json"):
        oracle = (tmp_path / "oracle" / relative).read_bytes().replace(b"\r\n", b"\n")
        assert (tmp_path / "native" / relative).read_bytes() == oracle
    retired = (
        "preparation.json",
        "authority.json",
        "evaluation-identity.json",
        "campaign.json",
        "screening-run/authority.json",
        "screening-run/checkpoint.json",
        "native-freeze-result.json",
        "native-freeze-transaction.json",
        "native-freeze-receipt.json",
    )
    assert all(not (tmp_path / "native" / relative).exists() for relative in retired)
    assert checkpoint["artifactMetrics"]["payloadFileCount"] == 2
    assert checkpoint["artifactMetrics"]["payloadBytes"] == (
        (tmp_path / "native" / "cohort-population.json").stat().st_size
        + (tmp_path / "native" / "screening-run" / "task-manifest.json").stat().st_size
    )
    restarted = freeze_qd_v5_campaign_native(evaluation_population_path=evaluation, template_preparation_path=common / "template.json", output_root=tmp_path / "native", cohort_selection_path=selection, **common_args)
    assert restarted["restart"] is True
    assert restarted["checkpointSha256"] == native["checkpointSha256"]
    payload = json.loads(selection.read_text(encoding="utf-8")); payload["candidateProjection"]["rawSha256"] = "sha256:" + "f" * 64; payload["selectionSha256"] = canonical_sha256({key: value for key, value in payload.items() if key != "selectionSha256"}); _write(tmp_path / "native-input" / "tampered-selection.json", payload)
    with pytest.raises(TemporalDiscoveryContractError, match="raw identity drifted"):
        freeze_qd_v5_campaign_native(evaluation_population_path=evaluation, template_preparation_path=common / "template.json", output_root=tmp_path / "tampered", cohort_selection_path=tmp_path / "native-input" / "tampered-selection.json", **common_args)


def test_native_v5_freeze_rejects_selection_candidate_identity_rebinding(tmp_path: Path) -> None:
    rotating, _template, _catalog, _population, evaluation, selection, common = _fixture(tmp_path, 4)
    common_args = dict(
        execution_engine_commit="a" * 40, worker_contract_sha256="sha256:" + "c" * 64,
        construction_catalog_path=common / "catalog.json", rotating_evidence=rotating,
        campaign_role="retained_parent_current_panel", panel_id="panel-1",
        archive_policy_authority=directional_qd_archive_policy_authority(),
        behavior_attribution_requirement=evolvable_behavior_attribution_requirement(),
    )
    common_args.update(
        _sealed_native_freeze_input_identities(
            evaluation=evaluation,
            template=common / "template.json",
            catalog=common / "catalog.json",
        )
    )
    projection = selection.parent / "selected.jsonl"
    rows = [json.loads(line) for line in projection.read_text(encoding="utf-8").splitlines()]
    rows[0]["candidate"]["programSha256"] = "sha256:" + "e" * 64
    rows[0]["projectionRowSha256"] = canonical_sha256({key: value for key, value in rows[0].items() if key != "projectionRowSha256"})
    encoded = b"\n".join(canonical_json_bytes(row) for row in rows) + b"\n"
    projection.write_bytes(encoded)
    payload = json.loads(selection.read_text(encoding="utf-8"))
    payload["candidateProjection"].update(rawSha256="sha256:" + hashlib.sha256(encoded).hexdigest(), sizeBytes=len(encoded))
    payload["selectionSha256"] = canonical_sha256({key: value for key, value in payload.items() if key != "selectionSha256"})
    _write(selection, payload)
    with pytest.raises(TemporalDiscoveryContractError, match="programSha256 binding drifted"):
        freeze_qd_v5_campaign_native(evaluation_population_path=evaluation, template_preparation_path=common / "template.json", output_root=tmp_path / "rebound", cohort_selection_path=selection, **common_args)


@pytest.mark.parametrize("candidate_count", (4, 128))
def test_native_v5_ladder_archive_round_robin_and_rebinding_parity(tmp_path: Path, candidate_count: int, monkeypatch: pytest.MonkeyPatch) -> None:
    rotating, _template, _catalog, population, evaluation, selection, common = _fixture(tmp_path, candidate_count)
    common_args = dict(execution_engine_commit="a" * 40, worker_contract_sha256="sha256:" + "c" * 64, construction_catalog_path=common / "catalog.json", rotating_evidence=rotating, campaign_role="retained_parent_current_panel", panel_id="panel-1", archive_policy_authority=directional_qd_archive_policy_authority(), behavior_attribution_requirement=evolvable_behavior_attribution_requirement())
    common_args.update(
        _sealed_native_freeze_input_identities(
            evaluation=evaluation,
            template=common / "template.json",
            catalog=common / "catalog.json",
        )
    )
    candidates = json.loads(evaluation.read_text(encoding="utf-8"))["candidates"]
    def archive_member(candidate: dict) -> dict:
        return {"archiveLane": "quality", "candidateId": candidate["candidateId"], "candidate": candidate, "finiteDataValidity": {"isFiniteData": True, "passesSupportGate": True, "validForQuality": True}, "objectives": {"worstWindowConservativeNetR": 1.0}, "robustObjectives": {"worstWindowConservativeNetR": 1.0, "drawdown": 1.0, "costDrag": 1.0, "novelty": 1.0}}
    # Deliberately unequal cells and all-tied robust objective values exercise
    # Python `_ladder_cohort`'s candidate-ID tie break and round-robin order.
    even = [archive_member(candidate) for candidate in reversed(candidates[::2])]
    odd = [archive_member(candidate) for candidate in reversed(candidates[1::2])]
    archive = {"schemaVersion": "temporal_qd_archive_v3", "cells": [
        {"cellId": "b", "members": odd + [{"archiveLane": "quality", "candidateId": "negative", "candidate": candidates[0], "finiteDataValidity": {"isFiniteData": True, "passesSupportGate": True, "validForQuality": True}, "objectives": {"worstWindowConservativeNetR": -1.0}, "robustObjectives": {"worstWindowConservativeNetR": -1.0, "drawdown": 1.0, "costDrag": 1.0, "novelty": 1.0}}]},
        {"cellId": "a", "members": even},
        {"cellId": "observational", "members": [{**archive_member(candidates[0]), "archiveLane": "observational"}]},
    ]}
    archive["archiveSha256"] = canonical_sha256(archive)
    archive_path = tmp_path / "native-input" / "archive.json"; _write(archive_path, archive)
    reduction_result = {
        "schemaVersion": "temporal_qd_native_archive_reduction_result_v1",
        "status": "completed",
        "archivePath": "archive.json",
        "archiveSha256": archive["archiveSha256"],
        "archiveRawSha256": "sha256:" + hashlib.sha256(archive_path.read_bytes()).hexdigest(),
        "archiveSizeBytes": archive_path.stat().st_size,
    }
    reduction_result["resultSha256"] = canonical_sha256(reduction_result)
    reduction_result_path = archive_path.parent / "archive-reduction-result.json"
    reduction_result_path.write_bytes(canonical_json_bytes(reduction_result) + b"\n")
    ladder_authority = {"schemaVersion": "temporal_qd_v5_native_evidence_ladder_authority_v1", "stageOrder": ["validation", "scrutiny"], "stages": {}}
    for stage in ("validation", "scrutiny"):
        ladder_authority["stages"][stage] = {"templatePreparationPath": str((common / "template.json").resolve()), "templatePreparationSha256": canonical_sha256(json.loads((common / "template.json").read_text(encoding="utf-8"))), "constructionCatalogPath": str((common / "catalog.json").resolve()), "constructionCatalogSha256": canonical_sha256(json.loads((common / "catalog.json").read_text(encoding="utf-8"))), "archivePolicyAuthority": common_args["archive_policy_authority"], "behaviorAttributionRequirement": common_args["behavior_attribution_requirement"], "candidateLimit": candidate_count}
    ladder_authority["ladderAuthoritySha256"] = canonical_sha256(ladder_authority)
    expected = [row["candidateId"] for row in _ladder_cohort(archive, limit=candidate_count)]
    assert expected[:4] == ["c000", "c001", "c002", "c003"]
    # Ladder selection is native-owned; a prebuilt external selection cannot
    # substitute an archive/stage-derived cohort.
    with pytest.raises(TemporalDiscoveryContractError, match="external selection is forbidden"):
        freeze_qd_v5_campaign_native(evaluation_population_path=evaluation, template_preparation_path=common / "template.json", output_root=tmp_path / "ladder-reject", cohort_selection_path=selection, final_archive_reduction_result_path=reduction_result_path, ladder_stage="validation", ladder_candidate_limit=candidate_count, ladder_authority=ladder_authority, **common_args)
    original_read_text = Path.read_text
    def forbid_bridge_archive_decode(path: Path, *args: object, **kwargs: object) -> str:
        if path == archive_path:
            raise AssertionError("ladder bridge must not decode archive bytes")
        return original_read_text(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", forbid_bridge_archive_decode)
    result = freeze_qd_v5_campaign_native(evaluation_population_path=evaluation, template_preparation_path=common / "template.json", output_root=tmp_path / "ladder", final_archive_reduction_result_path=reduction_result_path, ladder_stage="validation", ladder_candidate_limit=candidate_count, ladder_authority=ladder_authority, **common_args)
    monkeypatch.setattr(Path, "read_text", original_read_text)
    assert result["schemaVersion"] == "temporal_qd_v5_native_evidence_ladder_freeze_result_v1"
    assert result["candidateCount"] == candidate_count
    original_reduction_bytes = reduction_result_path.read_bytes()
    # The bounded reducer receipt is a hard root/path boundary.  A traversal
    # path, a bad self-hash, or a self-hashed substitution of archive identity
    # cannot make Rust consume an ambient archive.
    bad_reduction = dict(reduction_result)
    bad_reduction["archivePath"] = "../archive.json"
    bad_reduction["resultSha256"] = canonical_sha256({key: value for key, value in bad_reduction.items() if key != "resultSha256"})
    reduction_result_path.write_bytes(canonical_json_bytes(bad_reduction) + b"\n")
    with pytest.raises(TemporalDiscoveryContractError, match="archive-reduction result drifted"):
        freeze_qd_v5_campaign_native(evaluation_population_path=evaluation, template_preparation_path=common / "template.json", output_root=tmp_path / "ladder-path-tamper", final_archive_reduction_result_path=reduction_result_path, ladder_stage="validation", ladder_candidate_limit=candidate_count, ladder_authority=ladder_authority, **common_args)
    bad_reduction = dict(reduction_result)
    bad_reduction["resultSha256"] = "sha256:" + "0" * 64
    reduction_result_path.write_bytes(canonical_json_bytes(bad_reduction) + b"\n")
    with pytest.raises(TemporalDiscoveryContractError, match="archive-reduction result drifted"):
        freeze_qd_v5_campaign_native(evaluation_population_path=evaluation, template_preparation_path=common / "template.json", output_root=tmp_path / "ladder-receipt-tamper", final_archive_reduction_result_path=reduction_result_path, ladder_stage="validation", ladder_candidate_limit=candidate_count, ladder_authority=ladder_authority, **common_args)
    bad_reduction = dict(reduction_result)
    bad_reduction["archiveSha256"] = "sha256:" + "f" * 64
    bad_reduction["resultSha256"] = canonical_sha256({key: value for key, value in bad_reduction.items() if key != "resultSha256"})
    reduction_result_path.write_bytes(canonical_json_bytes(bad_reduction) + b"\n")
    with pytest.raises(TemporalDiscoveryContractError, match="sealed final archive identity drifted"):
        freeze_qd_v5_campaign_native(evaluation_population_path=evaluation, template_preparation_path=common / "template.json", output_root=tmp_path / "ladder-root-tamper", final_archive_reduction_result_path=reduction_result_path, ladder_stage="validation", ladder_candidate_limit=candidate_count, ladder_authority=ladder_authority, **common_args)
    reduction_result_path.write_bytes(original_reduction_bytes)
    selection_path = tmp_path / "ladder" / "cohort-selection.json"
    selection_bytes = selection_path.read_bytes()
    native_selected = [
        json.loads(line)["candidateId"]
        for line in (tmp_path / "ladder" / "cohort-selection.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert native_selected == expected
    # A substituted selection, even one with a recomputed local self-hash,
    # cannot satisfy the immutable selection receipt.
    substituted = json.loads(selection_bytes)
    substituted["candidateIds"] = list(reversed(substituted["candidateIds"]))
    substituted["selectionSha256"] = canonical_sha256({key: value for key, value in substituted.items() if key != "selectionSha256"})
    _write(selection_path, substituted)
    with pytest.raises(TemporalDiscoveryContractError, match="native ladder selection identity drifted"):
        freeze_qd_v5_campaign_native(evaluation_population_path=evaluation, template_preparation_path=common / "template.json", output_root=tmp_path / "ladder", final_archive_reduction_result_path=reduction_result_path, ladder_stage="validation", ladder_candidate_limit=candidate_count, ladder_authority=ladder_authority, **common_args)
    selection_path.write_bytes(selection_bytes)
    # Once receipt-last selection and freeze receipts exist, restart does not
    # reopen the archive source and preserves exact deterministic bytes.
    archive_path.unlink()
    assert freeze_qd_v5_campaign_native(evaluation_population_path=evaluation, template_preparation_path=common / "template.json", output_root=tmp_path / "ladder", final_archive_reduction_result_path=reduction_result_path, ladder_stage="validation", ladder_candidate_limit=candidate_count, ladder_authority=ladder_authority, **common_args)["campaignSha256"] == result["campaignSha256"]
    assert selection_path.read_bytes() == selection_bytes
    scrutiny_template = json.loads((common / "template.json").read_text(encoding="utf-8")); scrutiny_template["authorityLabel"] = "scrutiny-rebind"; scrutiny_path = common / "scrutiny.json"; _write(scrutiny_path, scrutiny_template)
    scrutiny_authority = json.loads(json.dumps(ladder_authority)); scrutiny_authority.pop("ladderAuthoritySha256"); scrutiny_authority["stages"]["scrutiny"]["templatePreparationPath"] = str(scrutiny_path.resolve()); scrutiny_authority["stages"]["scrutiny"]["templatePreparationSha256"] = canonical_sha256(scrutiny_template); scrutiny_authority["ladderAuthoritySha256"] = canonical_sha256(scrutiny_authority)
    _write(archive_path, archive)
    scrutiny_args = {
        **common_args,
        "template_preparation_sha256": canonical_sha256(
            json.loads(scrutiny_path.read_text(encoding="utf-8"))
        ),
    }
    scrutiny = freeze_qd_v5_campaign_native(evaluation_population_path=evaluation, template_preparation_path=scrutiny_path, output_root=tmp_path / "scrutiny", final_archive_reduction_result_path=reduction_result_path, ladder_stage="scrutiny", ladder_candidate_limit=candidate_count, ladder_authority=scrutiny_authority, **scrutiny_args)
    assert scrutiny["authorityId"] != result["authorityId"]
