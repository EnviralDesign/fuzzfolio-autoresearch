"""Execution bridge and explicit seed admission for Stage5E7-v3 validation panels.

This module intentionally does not search, mutate, or promote a validation
panel.  It only turns one already-frozen finite panel into the existing
``temporal-search`` authority/task contract.  The one exception is the
audited, post-evaluation conversion of the repaired 64-candidate reference
panel into a canonical QD-v3 generation-0 seed population.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .temporal_discovery_base import TemporalDiscoveryContractError, _clone, canonical_sha256
from .temporal_discovery_results import (
    _aggregate_candidate,
    _require_candidate_execution_binding,
    _result_set_sha256,
)
from .temporal_discovery_validation import (
    _rotate_evidence_plan,
    build_legacy_reference_admission_binding,
)
from .temporal_stage5e7_v3_validation import (
    _exact_task_manifest,
    load_authority_bound_panel_results,
)
from .temporal_qd_evolution import (
    QD_POLICY_NAME,
    QD_POLICY_SHA256,
    QD_POPULATION_SCHEMA,
    QD_VERSION,
    build_qd_archive,
    qd_canonical_evidence_identity,
    qd_predeclared_evidence_context,
)
from .temporal_search import (
    TEMPORAL_SEARCH_PREPARATION_SCHEMA,
    build_authority,
    materialize_plan,
    validate_authority,
)


BRIDGE_SCHEMA = "stage5e7_v3_finite_panel_execution_bridge_v1"
ADMISSION_SCHEMA = "stage5e7_v3_reference_seed_admission_v1"
REFERENCE_POPULATION_SCHEMA = "stage5e7_v3_reference_population_v1"
OPERATOR_PARENT_BASELINES_SCHEMA = "stage5e7_v3_operator_parent_baselines_v1"
_SHA = re.compile(r"^sha256:[0-9a-f]{64}$")


def _read(path: Path | str, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read {name}: {path}") from exc
    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError(f"{name} root must be an object")
    return _clone(value, name=name)


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    encoded = json.dumps(dict(value), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalDiscoveryContractError(f"refusing to overwrite divergent immutable file: {path}")
    if not path.exists():
        path.write_text(encoded, encoding="utf-8")


def _file_sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _external_root(output_root: Path | str) -> Path:
    root = Path(output_root).expanduser().resolve()
    repository = Path(__file__).resolve().parents[1]
    try:
        root.relative_to(repository)
    except ValueError:
        return root
    raise TemporalDiscoveryContractError("panel bridge output root must be outside the repository")


def _identity(payload: Mapping[str, Any], field: str, *, name: str) -> str:
    material = _clone(payload, name=name)
    supplied = material.pop(field, None)
    if not isinstance(supplied, str) or not _SHA.fullmatch(supplied) or canonical_sha256(material) != supplied:
        raise TemporalDiscoveryContractError(f"{name} {field} identity mismatch")
    return supplied


def _sha(value: Any, *, name: str) -> str:
    token = str(value or "")
    if not _SHA.fullmatch(token):
        raise TemporalDiscoveryContractError(f"{name} must be a canonical sha256 digest")
    return token


def _panel_population(path: Path | str) -> tuple[dict[str, Any], list[dict[str, Any]], str, str]:
    """Load only the three finite panel population contracts emitted by v3 validation."""
    population_path = Path(path)
    if population_path.is_dir():
        choices = [population_path / name for name in ("reference-population.json", "population.json", "parent-baselines.json")]
        existing = [item for item in choices if item.is_file()]
        if len(existing) != 1:
            raise TemporalDiscoveryContractError("panel population directory must contain exactly one known panel population")
        population_path = existing[0]
    payload = _read(population_path, name="finite validation panel population")
    population_sha = _identity(payload, "populationSha256", name="finite validation panel population")
    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or int(payload.get("candidateCount") or -1) != len(candidates):
        raise TemporalDiscoveryContractError("finite validation panel population count mismatch")
    if payload.get("referencePopulationSchema") == REFERENCE_POPULATION_SCHEMA:
        kind = "repair_reference"
        if payload.get("schemaVersion") != QD_POPULATION_SCHEMA or len(candidates) != 64:
            raise TemporalDiscoveryContractError("repair reference population contract mismatch")
        panel = _read(population_path.parent / "reference-panel.json", name="reference panel")
        panel_sha = _identity(panel, "referencePanelSha256", name="reference panel")
        if (
            panel.get("schemaVersion") != "stage5e7_v3_tagged_reference_panel_v1"
            or panel.get("referencePopulationSha256") != population_sha
        ):
            raise TemporalDiscoveryContractError("repair reference panel population contract mismatch")
        _ = panel_sha
    elif payload.get("schemaVersion") == OPERATOR_PARENT_BASELINES_SCHEMA:
        kind = "operator_parent_baselines"
        panel = _read(population_path.parent / "operator-panel.json", name="operator panel")
        panel_sha = _identity(panel, "operatorPanelSha256", name="operator panel")
        if (
            panel.get("schemaVersion") != "stage5e7_v3_operator_causal_panel_v1"
            or panel.get("parentBaselinePopulationSha256") != population_sha
        ):
            raise TemporalDiscoveryContractError("operator parent-baseline population contract mismatch")
        _ = panel_sha
    elif payload.get("schemaVersion") == QD_POPULATION_SCHEMA and payload.get("sourceReferencePopulationSha256"):
        kind = "operator"
        # The adjacent panel identity prevents an arbitrary QD population from
        # being presented as a finite validation panel.
        panel = _read(population_path.parent / "operator-panel.json", name="operator panel")
        panel_sha = _identity(panel, "operatorPanelSha256", name="operator panel")
        if panel.get("schemaVersion") != "stage5e7_v3_operator_causal_panel_v1" or panel.get("populationSha256") != population_sha:
            raise TemporalDiscoveryContractError("operator panel population contract mismatch")
        _ = panel_sha
    else:
        raise TemporalDiscoveryContractError("unknown finite validation panel population schema")
    copied = [_clone(item, name="finite panel candidate") for item in candidates]
    ids = [str(item.get("candidateId") or "") for item in copied]
    if not all(ids) or len(ids) != len(set(ids)):
        raise TemporalDiscoveryContractError("finite panel candidate identities must be non-empty and unique")
    for candidate in copied:
        profile = candidate.get("sourceProfile")
        source_sha = candidate.get("sourceProfileSha256")
        program_sha = candidate.get("programSha256")
        if not isinstance(profile, Mapping) or canonical_sha256(profile) != source_sha:
            raise TemporalDiscoveryContractError("finite panel candidate source-profile identity mismatch")
        if not isinstance(program_sha, str) or not _SHA.fullmatch(program_sha):
            raise TemporalDiscoveryContractError("finite panel candidate lacks canonical program identity")
        candidate_id = str(candidate.get("candidateId") or "")
        normalized_id = candidate_id.lower().replace("-", "_").replace(" ", "_")
        if candidate_id != normalized_id:
            raise TemporalDiscoveryContractError(
                "finite panel candidate ID must already match its temporal-search identity"
            )
    payload["populationSha256"] = population_sha
    return payload, copied, population_sha, kind


def _bridge_manifest(root: Path) -> dict[str, Any]:
    files = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != "manifest.json":
            files.append({"relativePath": path.relative_to(root).as_posix(), "length": path.stat().st_size, "sha256": _file_sha(path)})
    manifest = {"schemaVersion": "stage5e7_v3_finite_panel_bridge_manifest_v1", "fileCount": len(files), "files": files}
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write_once(root / "manifest.json", manifest)
    return manifest


def freeze_finite_panel_campaign(
    *,
    population_path: Path | str,
    template_preparation_path: Path | str,
    output_root: Path | str,
    worker_contract_sha256: str,
    construction_catalog_path: Path | str | None = None,
) -> dict[str, Any]:
    """Prepare one finite validation panel for the existing temporal-search CLI.

    Candidates stay validation-panel candidates; this function never supplies a
    QD policy, QD generation label, or QD archive input to the search surface.
    """
    population, candidates, population_sha, kind = _panel_population(population_path)
    frozen_construction_catalog = (
        _read(Path(construction_catalog_path), name="frozen panel construction catalog")
        if construction_catalog_path is not None
        else None
    )
    construction_catalog_identity = (
        {
            "path": str(Path(construction_catalog_path).resolve()),
            "catalogSha256": canonical_sha256(frozen_construction_catalog),
        }
        if frozen_construction_catalog is not None
        else None
    )
    if any(bool(candidate["sourceProfile"].get("indicators")) for candidate in candidates) and frozen_construction_catalog is None:
        raise TemporalDiscoveryContractError(
            "finite panel with indicators requires a frozen construction catalog for lake scope"
        )
    template = _read(template_preparation_path, name="frozen template preparation")
    template_authority = build_authority(template)
    effective_worker_contract = _clone(
        template_authority["workerContract"], name="template worker contract"
    )
    effective_worker_contract["workerContractSha256"] = _sha(
        worker_contract_sha256, name="--worker-contract-sha256"
    )
    exemplar = template_authority["candidates"][0]
    windows = template_authority["developmentWindows"]
    template_inputs = {item["windowId"]: item["evidencePlan"] for item in exemplar["windowInputs"]}
    if set(template_inputs) != {item["windowId"] for item in windows}:
        raise TemporalDiscoveryContractError("frozen template does not bind every development window")
    prepared_candidates = []
    for source in candidates:
        profile = _clone(source["sourceProfile"], name="panel candidate profile")
        source_sha = str(source["sourceProfileSha256"])
        prepared_candidates.append({
            "candidateId": str(source["candidateId"]),
            "sourceProfile": profile,
            "sourceProfileSha256": source_sha,
            "instrument": exemplar["instrument"],
            "timeframe": exemplar["timeframe"],
            "barLimit": exemplar["barLimit"],
            "windowInputs": [
                {
                    "windowId": window["windowId"],
                    "evidencePlan": _rotate_evidence_plan(
                        template_inputs[window["windowId"]],
                        raw_source_profile_sha256=source_sha,
                        source_profile=profile,
                        base_decision_timeframe=exemplar["timeframe"],
                        frozen_construction_catalog=frozen_construction_catalog,
                    ),
                }
                for window in windows
            ],
        })
    preparation = {
        "schemaVersion": TEMPORAL_SEARCH_PREPARATION_SCHEMA,
        "authorityLabel": str(template_authority["authorityLabel"]) + "-stage5e7-v3-panel-" + kind,
        "workerContract": effective_worker_contract,
        "candidates": prepared_candidates,
        "developmentWindows": _clone(windows, name="development windows"),
        "prohibitedEvidence": _clone(template_authority["prohibitedEvidence"], name="prohibited evidence"),
        "bounds": {
            "maxCandidates": len(prepared_candidates), "maxDevelopmentWindows": len(windows),
            "maxTasks": len(prepared_candidates) * len(windows),
            "maxAttempts": template_authority["bounds"]["maxAttempts"], "deadlineSeconds": template_authority["bounds"]["deadlineSeconds"],
        },
    }
    authority = build_authority(preparation)
    root = _external_root(output_root)
    evaluation = {
        "schemaVersion": "stage5e7_v3_finite_panel_evaluation_identity_v1",
        "panelKind": kind, "sourcePopulationSha256": population_sha,
        "constructionCatalog": construction_catalog_identity,
        "templatePreparationSha256": canonical_sha256(template),
        "preparationSha256": canonical_sha256(preparation),
        "authorityId": authority["authorityId"],
        "effectiveWorkerContract": _clone(authority["workerContract"], name="effective worker contract"),
        "candidateIds": [item["candidateId"] for item in prepared_candidates],
        "conversionToCanonicalQD": False, "reservedEvidencePermitted": False,
    }
    evaluation["evaluationIdentitySha256"] = canonical_sha256(evaluation)
    _write_once(root / "source-population.json", population)
    _write_once(root / "preparation.json", preparation)
    _write_once(root / "authority.json", authority)
    _write_once(root / "evaluation-identity.json", evaluation)
    task_manifest = materialize_plan(authority, root / "task-matrix")
    campaign = {
        "schemaVersion": BRIDGE_SCHEMA, "panelKind": kind, "sourcePopulationSha256": population_sha,
        "constructionCatalog": construction_catalog_identity,
        "templatePreparationSha256": canonical_sha256(template),
        "preparationSha256": canonical_sha256(preparation), "authorityId": authority["authorityId"],
        "effectiveWorkerContract": _clone(authority["workerContract"], name="effective worker contract"),
        "taskMatrixSha256": task_manifest["taskMatrixSha256"], "candidateCount": len(prepared_candidates),
        "windowCount": len(windows), "taskCount": len(prepared_candidates) * len(windows),
        "evaluationIdentitySha256": evaluation["evaluationIdentitySha256"], "canonicalQDConversion": "prohibited",
        "executeWith": ["temporal-search", "--fresh", "--authority-path", str((root / "authority.json").resolve()), "--output-root", "<external-temporal-search-result-root>", "--gateway-url", "<gateway-url>"],
    }
    campaign["campaignSha256"] = canonical_sha256(campaign)
    _write_once(root / "campaign.json", campaign)
    manifest = _bridge_manifest(root)
    return {"schemaVersion": "stage5e7_v3_finite_panel_bridge_result_v1", "outputRoot": str(root), "panelKind": kind, "campaignSha256": campaign["campaignSha256"], "authorityId": authority["authorityId"], "taskCount": campaign["taskCount"], "manifestSha256": manifest["manifestSha256"]}


def _load_repair_bridge_preparation(
    *, panel_preparation_path: Path | str, reference_population: Mapping[str, Any], source_candidates: list[dict[str, Any]], source_sha: str
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Fail closed unless this is the immutable repair bridge preparation."""
    preparation_path = Path(panel_preparation_path).resolve()
    if preparation_path.name != "preparation.json":
        raise TemporalDiscoveryContractError("--panel-preparation must name the bridge-produced preparation.json")
    root = preparation_path.parent
    preparation = _read(preparation_path, name="bridge panel preparation")
    authority = build_authority(preparation)
    stored_authority = validate_authority(_read(root / "authority.json", name="bridge authority"))
    if stored_authority != authority:
        raise TemporalDiscoveryContractError("bridge authority does not exactly bind panel preparation")
    campaign = _read(root / "campaign.json", name="bridge campaign")
    campaign_sha = _identity(campaign, "campaignSha256", name="bridge campaign")
    evaluation = _read(root / "evaluation-identity.json", name="bridge evaluation identity")
    evaluation_sha = _identity(evaluation, "evaluationIdentitySha256", name="bridge evaluation identity")
    stored_population = _read(root / "source-population.json", name="bridge source population")
    stored_population_sha = _identity(stored_population, "populationSha256", name="bridge source population")
    source_ids = [str(item["candidateId"]) for item in source_candidates]
    by_id = {str(item["candidateId"]): item for item in source_candidates}
    if (
        campaign.get("schemaVersion") != BRIDGE_SCHEMA
        or campaign.get("panelKind") != "repair_reference"
        or campaign.get("sourcePopulationSha256") != source_sha
        or campaign.get("preparationSha256") != canonical_sha256(preparation)
        or campaign.get("authorityId") != authority["authorityId"]
        or campaign.get("effectiveWorkerContract") != authority["workerContract"]
        or campaign.get("evaluationIdentitySha256") != evaluation_sha
        or stored_population_sha != source_sha
        or stored_population != reference_population
    ):
        raise TemporalDiscoveryContractError("bridge preparation is not bound to this repaired reference population")
    if (
        evaluation.get("panelKind") != "repair_reference"
        or evaluation.get("sourcePopulationSha256") != source_sha
        or evaluation.get("templatePreparationSha256") != campaign.get("templatePreparationSha256")
        or evaluation.get("preparationSha256") != canonical_sha256(preparation)
        or evaluation.get("authorityId") != authority["authorityId"]
        or evaluation.get("effectiveWorkerContract") != authority["workerContract"]
        or evaluation.get("candidateIds") != source_ids
    ):
        raise TemporalDiscoveryContractError("bridge evaluation identity is incomplete or mismatched")
    prepared = {str(item["candidateId"]): item for item in authority["candidates"]}
    if set(prepared) != set(by_id) or len(prepared) != 64:
        raise TemporalDiscoveryContractError("bridge preparation does not exactly cover the repaired reference candidates")
    for candidate_id, source in by_id.items():
        prepared_candidate = prepared[candidate_id]
        if (
            prepared_candidate["sourceProfile"] != source["sourceProfile"]
            or prepared_candidate["sourceProfileSha256"] != source["sourceProfileSha256"]
        ):
            raise TemporalDiscoveryContractError("bridge preparation source profile diverges from repaired reference population")
    campaign["campaignSha256"] = campaign_sha
    evaluation["evaluationIdentitySha256"] = evaluation_sha
    return preparation, authority, campaign, evaluation


def admit_repair_reference_seed(
    *, reference_population_path: Path | str, result_root: Path | str, panel_preparation_path: Path | str, output_root: Path | str
) -> dict[str, Any]:
    """The sole audited conversion of evaluated repair references to canonical QD-v3."""
    reference_population, source_candidates, source_sha, kind = _panel_population(reference_population_path)
    if kind != "repair_reference" or len(source_candidates) != 64:
        raise TemporalDiscoveryContractError("seed admission accepts only the repaired 64-candidate reference population")
    preparation, authority, bridge_campaign, evaluation = _load_repair_bridge_preparation(
        panel_preparation_path=panel_preparation_path,
        reference_population=reference_population,
        source_candidates=source_candidates,
        source_sha=source_sha,
    )
    expected_manifest = _exact_task_manifest(authority)
    if (
        bridge_campaign.get("taskMatrixSha256")
        != expected_manifest["taskMatrixSha256"]
    ):
        raise TemporalDiscoveryContractError(
            "bridge campaign task matrix diverges from repaired authority"
        )
    catalog_identity = evaluation.get("constructionCatalog")
    catalog_payload = None
    catalog_path = None
    if catalog_identity is not None:
        if not isinstance(catalog_identity, Mapping):
            raise TemporalDiscoveryContractError("bridge construction catalog identity is malformed")
        catalog_path = Path(str(catalog_identity.get("path") or ""))
        catalog_payload = _read(catalog_path, name="bridge frozen construction catalog")
        if canonical_sha256(catalog_payload) != catalog_identity.get("catalogSha256"):
            raise TemporalDiscoveryContractError("bridge frozen construction catalog drifted")
        if bridge_campaign.get("constructionCatalog") != catalog_identity:
            raise TemporalDiscoveryContractError("bridge campaign construction catalog diverged")
    context = qd_predeclared_evidence_context(
        preparation,
        worker_contract_sha256=authority["workerContract"]["workerContractSha256"],
        construction_catalog=catalog_payload,
        construction_catalog_path=catalog_path,
    )
    results = load_authority_bound_panel_results(
        authority=authority,
        expected_manifest=expected_manifest,
        result_root=result_root,
    )
    corrected_result_set_sha256 = _result_set_sha256(results)
    source_by_id = {str(item["candidateId"]): item for item in source_candidates}
    if set(results) != set(source_by_id):
        raise TemporalDiscoveryContractError("seed admission requires exact repaired-reference candidate result coverage")
    admitted = []
    aggregates = {}
    expected_windows = {
        (str(window["analysisWindowStart"]), str(window["analysisWindowEnd"]))
        for window in authority["developmentWindows"]
    }
    for candidate_id in sorted(source_by_id):
        source = source_by_id[candidate_id]
        candidate_windows = results[candidate_id]
        if (
            {(str(row.get("analysisWindowStart")), str(row.get("analysisWindowEnd"))) for row in candidate_windows} != expected_windows
            or len(candidate_windows) != len(expected_windows)
        ):
            raise TemporalDiscoveryContractError("corrected candidate results diverge from repaired reference/authority bindings")
        execution_binding = _require_candidate_execution_binding(
            source, candidate_windows
        )
        aggregate = _aggregate_candidate(source, candidate_windows)
        if (
            aggregate.get("v3Admissible") is not True
            or aggregate.get("authoredProgramSha256")
            != execution_binding["authoredProgramSha256"]
            or aggregate.get("sourceProfileSnapshotSha256")
            != execution_binding["sourceProfileSnapshotSha256"]
            or aggregate.get("resolvedProfileSnapshotSha256")
            != execution_binding["resolvedProfileSnapshotSha256"]
            or aggregate.get("resolvedProgramSha256")
            != execution_binding["resolvedProgramSha256"]
            or aggregate.get("programSha256")
            != execution_binding["resolvedProgramSha256"]
        ):
            raise TemporalDiscoveryContractError("seed admission requires v3Admissible corrected aggregates for every reference candidate")
        candidate = _clone(source, name="reference candidate for explicit QD admission")
        candidate.update({"sourceMode": "qd_stage5e7_v3_reference_seed_admitted", "generationIndex": 0, "birthOrdinal": len(admitted), "proposalOrdinal": len(admitted)})
        legacy_binding = build_legacy_reference_admission_binding(
            candidate=candidate,
            execution_binding=execution_binding,
            source_reference_population_sha256=source_sha,
            authority_id=authority["authorityId"],
            worker_contract_sha256=authority["workerContract"]["workerContractSha256"],
            corrected_result_set_sha256=corrected_result_set_sha256,
        )
        candidate["legacyReferenceAdmissionBindingSha256"] = legacy_binding.pop(
            "legacyReferenceAdmissionBindingSha256"
        )
        candidate["legacyReferenceAdmissionBinding"] = legacy_binding
        identity_material = {"schemaVersion": "stage5e7_v3_reference_seed_candidate_identity_v1", "referencePopulationSha256": source_sha, "sourceCandidateId": candidate_id, "sourceCandidateSha256": canonical_sha256(source), "programSha256": candidate["programSha256"], "predeclaredEvidenceContextSha256": context["predeclaredEvidenceContextSha256"], "authorityId": authority["authorityId"]}
        candidate["candidateIdentityMaterial"] = identity_material
        candidate["candidateIdentitySha256"] = canonical_sha256(identity_material)
        candidate["canonicalEvidenceIdentitySha256"] = qd_canonical_evidence_identity(candidate, context)
        admitted.append(candidate)
        aggregates[candidate_id] = aggregate
    seed_context = {
        "schemaVersion": "stage5e7_v3_reference_seed_admission_context_v1",
        "sourceReferencePopulationSha256": source_sha,
        "bridgePreparationSha256": canonical_sha256(preparation),
        "authorityId": authority["authorityId"],
        "effectiveWorkerContract": _clone(authority["workerContract"], name="effective worker contract"),
        "bridgeEvaluationIdentitySha256": evaluation["evaluationIdentitySha256"],
        "predeclaredEvidenceContextSha256": context["predeclaredEvidenceContextSha256"],
    }
    seed_context["seedAdmissionContextSha256"] = canonical_sha256(seed_context)
    seed_population = {
        "schemaVersion": QD_POPULATION_SCHEMA, "qdVersion": QD_VERSION, "policyName": QD_POLICY_NAME,
        "policySha256": QD_POLICY_SHA256, "generationIndex": 0, "targetUniqueCandidates": 64,
        "originCounts": {"stage5e7_v3_reference_seed_admission": 64}, "proposalOrderCandidateIds": [item["candidateId"] for item in admitted],
        "candidateCount": 64, "candidates": admitted, "predeclaredEvidenceContextSha256": context["predeclaredEvidenceContextSha256"],
        "stage5e7V3SeedAdmissionContext": seed_context,
        "legacyReferenceAdmissionBindingRequired": True,
        "proposalSlots": {"targetUniqueCandidates": 64, "acceptedUniqueCandidates": 64, "proposalAttempts": 64, "remainingUniqueCandidateSlots": 0},
    }
    seed_population["populationSha256"] = canonical_sha256(seed_population)
    root = _external_root(output_root)
    admission = {
        "schemaVersion": ADMISSION_SCHEMA, "sourceReferencePopulationSha256": source_sha,
        "resultRoot": str(Path(result_root).resolve()), "resultSetSha256": corrected_result_set_sha256,
        "candidateCount": 64, "exactResultCoverage": True, "allCorrectedAggregatesV3Admissible": True,
        "qdVersion": QD_VERSION, "policyName": QD_POLICY_NAME, "policySha256": QD_POLICY_SHA256,
        "predeclaredEvidenceContext": context, "predeclaredEvidenceContextSha256": context["predeclaredEvidenceContextSha256"],
        "panelPreparationSha256": canonical_sha256(preparation), "authorityId": authority["authorityId"],
        "effectiveWorkerContract": _clone(authority["workerContract"], name="effective worker contract"),
        "bridgeCampaignSha256": bridge_campaign["campaignSha256"], "seedAdmissionContext": seed_context,
        "v2ArchiveRanksUsed": False, "canonicalQDConversion": "explicit_post_evaluation_seed_admission_only", "aggregateSha256": canonical_sha256(aggregates),
    }
    admission["admissionSha256"] = canonical_sha256(admission)
    _write_once(root / "seed-population.json", seed_population)
    _write_once(root / "admission.json", admission)
    reduction = build_qd_archive(population_path=root / "seed-population.json", result_root=result_root, output_path=root / "generation-0-archive.json", generation_index=0)
    manifest = _bridge_manifest(root)
    return {"schemaVersion": "stage5e7_v3_reference_seed_admission_result_v1", "outputRoot": str(root), "admissionSha256": admission["admissionSha256"], "seedPopulationSha256": seed_population["populationSha256"], "archiveSha256": reduction["archiveSha256"], "manifestSha256": manifest["manifestSha256"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bridge Stage5E7-v3 finite validation panels to temporal-search; explicit repair seed admission only.")
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--population", type=Path, required=True)
    freeze.add_argument("--template-preparation", type=Path, required=True)
    freeze.add_argument("--worker-contract-sha256", required=True)
    freeze.add_argument("--construction-catalog", type=Path)
    freeze.add_argument("--output-root", type=Path, required=True)
    admit = commands.add_parser("admit-repair-seed")
    admit.add_argument("--reference-population", type=Path, required=True)
    admit.add_argument("--results", type=Path, required=True)
    admit.add_argument("--panel-preparation", type=Path, required=True)
    admit.add_argument("--output-root", type=Path, required=True)
    try:
        args = parser.parse_args(argv)
        result = freeze_finite_panel_campaign(population_path=args.population, template_preparation_path=args.template_preparation, worker_contract_sha256=args.worker_contract_sha256, output_root=args.output_root, construction_catalog_path=args.construction_catalog) if args.command == "freeze" else admit_repair_reference_seed(reference_population_path=args.reference_population, result_root=args.results, panel_preparation_path=args.panel_preparation, output_root=args.output_root)
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
        return 0
    except Exception as exc:
        print(json.dumps({"schemaVersion": "stage5e7_v3_panel_bridge_error_v1", "errorType": type(exc).__name__, "message": str(exc)}, indent=2, sort_keys=True), flush=True)
        return 1


__all__ = ["admit_repair_reference_seed", "freeze_finite_panel_campaign", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
