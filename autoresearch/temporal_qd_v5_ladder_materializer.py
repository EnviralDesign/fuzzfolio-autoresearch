"""Externally attest the v5 validation/scrutiny ladder stage templates.

This deliberately sits beside (not inside) the rotating-development
materializer.  It opens one already-authorized panel exemplar and resolves two
fresh continuous lake windows.  It neither opens an archive nor enumerates a
candidate population.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Callable

from .evidence_plan import build_replay_evidence_plan, canonical_sha256
from .lake_window import LakeWindowBinding, LakeWindowRequest, lake_window_request_contains
from .lake_window_client import resolve_lake_window_binding
from .temporal_discovery_base import TemporalDiscoveryContractError, _clone
from .temporal_qd_evidence_ladder_materializer import (
    _attest, _catalog, _envelope_request, _execution_cell_sha, _external_root,
    _read, _worker_contract, _write_once,
)
from .temporal_qd_evolution import _resolve_archive_policy_authority
from .temporal_qd_rotating_evidence import OUTER_TAIL_START, validate_rotating_evidence_contract
from .temporal_search import TEMPORAL_SEARCH_PREPARATION_SCHEMA, _validated_behavior_attribution_requirement, build_authority


MATERIALIZATION_SCHEMA = "temporal_qd_v5_ladder_stage_materialization_v1"
RESULT_SCHEMA = "temporal_qd_v5_ladder_stage_materialization_result_v1"
INVENTORY_SCHEMA = "temporal_qd_v5_ladder_stage_inventory_v1"

_Attestor = Callable[..., LakeWindowBinding]


def _sha(value: object, *, name: str) -> str:
    if not isinstance(value, str) or len(value) != 71 or not value.startswith("sha256:") or any(ch not in "0123456789abcdef" for ch in value[7:]):
        raise TemporalDiscoveryContractError(f"{name} must be a canonical sha256 digest")
    return value


def _file_sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _descriptor(path: Path, payload: Mapping[str, Any], *, semantic_field: str) -> dict[str, Any]:
    return {"path": str(path), "fileSha256": _file_sha(path), "semanticSha256": _sha(payload.get(semantic_field), name=semantic_field)}


def _months(start: str, end: str) -> int:
    try:
        return (int(end[:4]) - int(start[:4])) * 12 + int(end[5:7]) - int(start[5:7])
    except (ValueError, IndexError) as exc:
        raise TemporalDiscoveryContractError("stage window timestamp is invalid") from exc


def _validate_materialization(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = _read(path, name="rotating evidence materialization")
    supplied = manifest.pop("materializationSha256", None)
    if manifest.get("schemaVersion") != "temporal_qd_rotating_evidence_materialization_v1" or _sha(supplied, name="rotating materialization identity") != canonical_sha256(manifest):
        raise TemporalDiscoveryContractError("rotating evidence materialization identity drifted")
    manifest["materializationSha256"] = supplied
    contract = manifest.get("rotatingEvidence")
    if not isinstance(contract, Mapping):
        raise TemporalDiscoveryContractError("rotating evidence materialization lacks contract")
    validated = validate_rotating_evidence_contract(contract)
    scrutiny = manifest.get("researchScrutiny")
    expected_scrutiny = contract.get("researchScrutiny")
    if (
        validated != contract
        or not isinstance(scrutiny, Mapping)
        or not isinstance(expected_scrutiny, Mapping)
        or any(
            scrutiny.get(stage, {}).get("window") != expected_scrutiny.get(stage, {}).get("window")
            for stage in ("validation", "scrutiny")
        )
    ):
        raise TemporalDiscoveryContractError("rotating evidence materialization contract binding drifted")
    return manifest, validated


def _authorized_exemplar(path: Path, manifest: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    template = _read(path, name="authorized rotating panel template")
    templates = manifest.get("templates")
    if not isinstance(templates, Mapping):
        raise TemporalDiscoveryContractError("rotating materialization has no template inventory")
    record = next((item for key, item in templates.items() if key.startswith("panel-") and isinstance(item, Mapping) and Path(str(item.get("path") or "")).resolve() == path), None)
    if not isinstance(record, Mapping) or canonical_sha256(template) != record.get("preparationSha256"):
        raise TemporalDiscoveryContractError("panel template is not authorized by rotating materialization")
    if build_authority(template).get("authorityId") != record.get("authorityId"):
        raise TemporalDiscoveryContractError("authorized panel template authority drifted")
    candidates = template.get("candidates")
    if not isinstance(candidates, list) or len(candidates) != 1 or not isinstance(candidates[0], Mapping):
        raise TemporalDiscoveryContractError("v5 ladder requires exactly one authorized panel exemplar")
    return template, dict(candidates[0])


def _capability_dependencies(catalog: Mapping[str, Any], envelope: Mapping[str, Any]) -> list[dict[str, Any]]:
    supplied = dict(envelope)
    supplied_sha = supplied.pop("capabilityEnvelopeSha256", None)
    if _sha(supplied_sha, name="catalog capability envelope identity") != canonical_sha256(supplied):
        raise TemporalDiscoveryContractError("catalog capability envelope identity drifted")
    frames = envelope.get("admittedTimeframes")
    bounds = envelope.get("lookbackBounds")
    if not isinstance(frames, list) or not frames or not isinstance(bounds, Mapping):
        raise TemporalDiscoveryContractError("catalog capability envelope is malformed")
    try:
        lookback = int(bounds["maxReachable"])
    except (KeyError, TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError("catalog capability envelope max lookback is invalid") from exc
    if lookback < 0:
        raise TemporalDiscoveryContractError("catalog capability envelope max lookback is invalid")
    result = []
    for raw in catalog.get("indicators") or []:
        meta = raw.get("meta") if isinstance(raw, Mapping) else None
        config = raw.get("config") if isinstance(raw, Mapping) else None
        if not isinstance(meta, Mapping) or not isinstance(config, Mapping) or config.get("isActive") is not True:
            continue
        indicator = str(meta.get("id") or "").strip()
        if not indicator:
            raise TemporalDiscoveryContractError("catalog capability indicator identity is invalid")
        for timeframe in frames:
            result.append({"dependencyKind": "catalog_capability", "indicatorId": indicator, "timeframe": str(timeframe), "lookbackBars": lookback, "sourceProfile": {"indicators": [{"meta": {"id": indicator}, "config": {"isActive": True, "timeframe": str(timeframe), "lookbackBars": lookback}}]}})
    if not result:
        raise TemporalDiscoveryContractError("catalog capability envelope has no active dependencies")
    return result


def _stage_preparation(*, stage: str, window: Mapping[str, str], exemplar: Mapping[str, Any], worker: Mapping[str, str], binding: LakeWindowBinding) -> dict[str, Any]:
    role, horizon = ("validation", 12) if stage == "validation" else ("scrutiny", 36)
    profile = _clone(exemplar.get("sourceProfile"), name="authorized panel exemplar profile")
    profile_sha = _sha(exemplar.get("sourceProfileSha256"), name="exemplar source profile identity")
    if canonical_sha256(profile) != profile_sha:
        raise TemporalDiscoveryContractError("authorized panel exemplar profile identity drifted")
    plan = build_replay_evidence_plan(
        evidence_role=role, selection_data_end=window["analysisWindowEnd"],
        analysis_window_start=window["analysisWindowStart"], analysis_window_end=window["analysisWindowEnd"],
        requested_horizon_months=horizon, profile_snapshot=profile,
        campaign_plan_id=f"temporal-qd-v5-ladder-{stage}-v1",
        execution_cell_sha256=_execution_cell_sha(profile), lake_window_binding=binding,
        data_availability_cutoff=window["analysisWindowEnd"], coverage_policy="require_complete",
    )
    preparation = {
        "schemaVersion": TEMPORAL_SEARCH_PREPARATION_SCHEMA,
        "authorityLabel": f"temporal-qd-v5-{stage}-{binding.window_semantic_sha256[-12:]}",
        "workerContract": dict(worker),
        "candidates": [{"candidateId": str(exemplar["candidateId"]), "sourceProfile": profile, "sourceProfileSha256": profile_sha, "instrument": exemplar["instrument"], "timeframe": exemplar["timeframe"], "barLimit": exemplar["barLimit"], "windowInputs": [{"windowId": window["windowId"], "evidencePlan": plan.model_dump(mode="json")}]}],
        "developmentWindows": [dict(window)],
        "prohibitedEvidence": [{"windowId": "untouched-outer-tail", "analysisWindowStart": OUTER_TAIL_START, "analysisWindowEnd": "9999-12-31T00:00:00Z", "reason": "sole untouched evidence"}],
        "bounds": {"maxCandidates": 1, "maxDevelopmentWindows": 1, "maxTasks": 1, "maxAttempts": 2, "deadlineSeconds": 86400.0},
    }
    build_authority(preparation)
    return preparation


def _restart(root: Path, input_sha: str) -> dict[str, Any] | None:
    receipt_path = root / "result.json"
    if not receipt_path.exists():
        return None
    result = _read(receipt_path, name="v5 ladder result")
    supplied = result.pop("resultSha256", None)
    if result.get("schemaVersion") != RESULT_SCHEMA or _sha(supplied, name="v5 ladder result identity") != canonical_sha256(result) or result.get("inputSha256") != input_sha:
        raise TemporalDiscoveryContractError("v5 ladder receipt identity or input binding drifted")
    inventory_path = root / "inventory.json"; inventory = _read(inventory_path, name="v5 ladder inventory")
    inventory_sha = inventory.pop("inventorySha256", None)
    if inventory.get("schemaVersion") != INVENTORY_SCHEMA or _sha(inventory_sha, name="v5 ladder inventory identity") != canonical_sha256(inventory) or inventory.get("inputSha256") != input_sha:
        raise TemporalDiscoveryContractError("v5 ladder inventory identity or input binding drifted")
    for row in inventory.get("stageTemplatePreparations") or []:
        if not isinstance(row, Mapping):
            raise TemporalDiscoveryContractError("v5 ladder inventory stage descriptor is malformed")
        path = Path(str(row.get("path") or ""))
        payload = _read(path, name="v5 ladder stage template")
        if _file_sha(path) != row.get("fileSha256") or canonical_sha256(payload) != row.get("preparationSha256") or build_authority(payload).get("authorityId") != row.get("authorityId"):
            raise TemporalDiscoveryContractError("v5 ladder committed stage template drifted")
    result["resultSha256"] = supplied
    return result


def materialize_qd_v5_ladder_stage_templates(*, rotating_materialization_path: Path | str, panel_template_preparation_path: Path | str, construction_catalog_path: Path | str, output_root: Path | str, worker_contract_sha256: str, worker_contract_schema: str, execution_engine_commit: str, archive_policy_authority: Mapping[str, Any], behavior_attribution_requirement: Mapping[str, Any], attestor: _Attestor = resolve_lake_window_binding) -> dict[str, Any]:
    """Create exactly the fresh 12m validation and 36m scrutiny templates."""
    materialization_path = Path(rotating_materialization_path).resolve()
    exemplar_path = Path(panel_template_preparation_path).resolve()
    catalog_path = Path(construction_catalog_path).resolve()
    root = _external_root(output_root)
    manifest, contract = _validate_materialization(materialization_path)
    template, exemplar = _authorized_exemplar(exemplar_path, manifest)
    catalog, frames = _catalog(_read(catalog_path, name="frozen construction catalog"))
    catalog_record = manifest.get("constructionCatalog")
    if not isinstance(catalog_record, Mapping) or canonical_sha256(catalog) != catalog_record.get("catalogSha256"):
        raise TemporalDiscoveryContractError("v5 ladder construction catalog binding drifted")
    worker = _worker_contract(worker_contract_sha256, worker_contract_schema)
    if worker != manifest.get("workerContract"):
        raise TemporalDiscoveryContractError("v5 ladder worker contract binding drifted")
    commit = str(execution_engine_commit or "").strip().lower()
    if len(commit) != 40 or any(char not in "0123456789abcdef" for char in commit):
        raise TemporalDiscoveryContractError("v5 ladder execution engine commit must be a full SHA")
    policy_name, policy_sha, _policy, directional = _resolve_archive_policy_authority(archive_policy_authority)
    if not directional:
        raise TemporalDiscoveryContractError("v5 ladder requires the direction-aware archive authority")
    behavior = _validated_behavior_attribution_requirement(behavior_attribution_requirement)
    envelope = manifest.get("catalogCapabilityEnvelope")
    if not isinstance(envelope, Mapping) or set(envelope.get("admittedTimeframes") or []) - set(frames):
        raise TemporalDiscoveryContractError("v5 ladder capability envelope drifted")
    dependencies = _capability_dependencies(catalog, envelope)
    if str(exemplar.get("timeframe") or "").upper() not in frames:
        raise TemporalDiscoveryContractError("v5 ladder exemplar timeframe is absent from catalog")
    profile = exemplar.get("sourceProfile")
    if not isinstance(profile, Mapping) or profile.get("instruments") != [exemplar.get("instrument")]:
        raise TemporalDiscoveryContractError("v5 ladder exemplar geometry is invalid")
    input_material = {"rotatingMaterializationSha256": manifest["materializationSha256"], "panelTemplatePreparationSha256": canonical_sha256(template), "constructionCatalogSha256": canonical_sha256(catalog), "workerContract": worker, "executionEngineCommit": commit, "archivePolicyName": policy_name, "archivePolicySha256": policy_sha, "behaviorAttributionRequirementSha256": behavior["requirementSha256"]}
    input_sha = canonical_sha256(input_material)
    restarted = _restart(root, input_sha)
    if restarted is not None:
        return restarted
    stages = {"validation": ("validation-12m", contract["researchScrutiny"]["validation"]["window"], 12), "scrutiny": ("scrutiny-36m", contract["researchScrutiny"]["scrutiny"]["window"], 36)}
    preparations: dict[str, dict[str, Any]] = {}
    stage_rows: list[dict[str, Any]] = []
    quarter_semantics = {row.get("remoteBinding", {}).get("window_semantic_sha256") for row in manifest.get("quarters") or [] if isinstance(row, Mapping)}
    for stage, (window_id, raw_window, horizon) in stages.items():
        if not isinstance(raw_window, Mapping):
            raise TemporalDiscoveryContractError("v5 ladder stage window is missing")
        window = {"windowId": window_id, "analysisWindowStart": str(raw_window.get("analysisWindowStart") or ""), "analysisWindowEnd": str(raw_window.get("analysisWindowEnd") or "")}
        if _months(window["analysisWindowStart"], window["analysisWindowEnd"]) != horizon or window["analysisWindowEnd"] > OUTER_TAIL_START:
            raise TemporalDiscoveryContractError("v5 ladder stage horizon or untouched-tail boundary drifted")
        request, reachable = _envelope_request(variants=[{"memberId": "panel-exemplar", "memberOrigin": "authorized_panel_template", "sourceProfileSha256": exemplar["sourceProfileSha256"], "variantId": "exact", "variantProfileSha256": exemplar["sourceProfileSha256"], "sourceProfile": profile}], instrument=str(exemplar["instrument"]), base_timeframe=str(exemplar["timeframe"]), window=window, catalog=catalog, capability_dependencies=dependencies)
        binding = _attest(request, attestor=attestor)
        if binding.window_semantic_sha256 in quarter_semantics or any(binding.request == LakeWindowBinding.model_validate(row["remoteBinding"]).request for row in manifest.get("quarters") or [] if isinstance(row, Mapping) and isinstance(row.get("remoteBinding"), Mapping)):
            raise TemporalDiscoveryContractError("v5 ladder must not reuse a rotating quarter binding")
        if not all(lake_window_request_contains(binding.request, row["request"]) for row in reachable):
            raise TemporalDiscoveryContractError("v5 ladder stage binding does not contain every capability dependency")
        preparation = _stage_preparation(stage=stage, window=window, exemplar=exemplar, worker=worker, binding=binding)
        preparations[stage] = preparation
        stage_rows.append({"stage": stage, "requestedHorizonMonths": horizon, "evidenceRole": stage, "window": window, "remoteBinding": binding.model_dump(mode="json"), "reachableRequests": reachable})
    paths = {stage: root / f"{stage}-template-preparation.json" for stage in preparations}
    for stage, preparation in preparations.items():
        _write_once(paths[stage], preparation); _write_once(root / f"{stage}-authority.json", build_authority(preparation))
    descriptors = [{"stage": stage, "path": str(paths[stage]), "preparationSha256": canonical_sha256(preparation), "authorityId": build_authority(preparation)["authorityId"], "fileSha256": _file_sha(paths[stage])} for stage, preparation in preparations.items()]
    inventory = {"schemaVersion": INVENTORY_SCHEMA, "inputSha256": input_sha, "stageTemplatePreparations": descriptors}
    inventory["inventorySha256"] = canonical_sha256(inventory)
    _write_once(root / "inventory.json", inventory)
    result = {"schemaVersion": RESULT_SCHEMA, "inputSha256": input_sha, "rotatingEvidenceSha256": contract["rotatingEvidenceSha256"], "stageTemplatePreparations": descriptors, "stages": stage_rows, "inventorySha256": inventory["inventorySha256"], "outputRoot": str(root)}
    result["resultSha256"] = canonical_sha256(result)
    _write_once(root / "result.json", result)
    return result


__all__ = ["MATERIALIZATION_SCHEMA", "RESULT_SCHEMA", "materialize_qd_v5_ladder_stage_templates"]
