"""Freeze a QD generation into the existing immutable worker task contract."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    _clone,
    canonical_sha256,
)
from .temporal_discovery_validation import _rotate_evidence_plan
from .temporal_qd_evolution import (
    QD_POLICY_NAME,
    QD_POLICY_SHA256,
    QD_POPULATION_SCHEMA,
    _bidirectional_pair_policy,
    _load_population,
    _read,
    qd_canonical_evidence_identity,
    qd_predeclared_evidence_context,
)
from .temporal_qd_evidence_ladder import validate_template_discovery_windows
from .temporal_qd_evaluation_population import (
    evaluation_population_path,
    is_optimized_pair_population,
    load_evaluation_population,
)
from .temporal_search import (
    TEMPORAL_SEARCH_PREPARATION_SCHEMA,
    build_authority,
    materialize_plan,
)

QD_CAMPAIGN_SCHEMA = "temporal_qd_screening_campaign_v3"
INITIAL_POPULATION_SCHEMA = "temporal_discovery_population_v2"
_SMALL_POPULATION_FALLBACK_BYTES = 16 * 1024 * 1024


def _write_once(path: Path, value: Mapping[str, Any]) -> None:
    encoded = (
        json.dumps(
            dict(value), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False
        )
        + "\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalDiscoveryContractError(
            f"refusing to overwrite divergent QD campaign file: {path}"
        )
    if path.exists():
        return
    path.write_text(encoded, encoding="utf-8")


def freeze_qd_screening_campaign(
    *,
    population_path: Path | str,
    template_preparation_path: Path | str,
    output_root: Path | str,
    execution_engine_commit: str,
    worker_contract_sha256: str | None = None,
    construction_catalog_path: Path | str | None = None,
    evidence_ladder: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    population_file = Path(population_path)
    projection_file = evaluation_population_path(population_file)
    evaluation_population_sha256: str | None = None
    if projection_file.is_file():
        population_payload = load_evaluation_population(
            population_path=population_file,
            journal_path=population_file.with_name("generation-journal.json"),
        )
        population_schema = QD_POPULATION_SCHEMA
        candidates = list(population_payload["candidates"])
        population_sha = str(population_payload["populationSha256"])
        evaluation_population_sha256 = str(
            population_payload["evaluationPopulationSha256"]
        )
        bidirectional_policy = _bidirectional_pair_policy(
            {"bidirectionalPairPolicy": population_payload["bidirectionalPairPolicy"]}
        )
    else:
        if population_file.stat().st_size > _SMALL_POPULATION_FALLBACK_BYTES:
            raise TemporalDiscoveryContractError(
                "optimized pre-sidecar QD pair population requires a fresh truthful root"
            )
        population_payload = _read(population_file, name="QD generation population")
        population_schema = population_payload.get("schemaVersion")
        if population_schema not in {QD_POPULATION_SCHEMA, INITIAL_POPULATION_SCHEMA}:
            raise TemporalDiscoveryContractError("unknown QD generation population schema")
        if is_optimized_pair_population(population_payload):
            raise TemporalDiscoveryContractError(
                "optimized pre-sidecar QD pair population requires a fresh truthful root"
            )
        candidates, population_sha = _load_population(population_file)
        bidirectional_policy = _bidirectional_pair_policy(population_payload)
    frozen_construction_catalog = (
        _read(Path(construction_catalog_path), name="frozen QD construction catalog")
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
    if any(
        isinstance(candidate.get("sourceProfile"), Mapping)
        and bool(candidate["sourceProfile"].get("indicators"))
        for candidate in candidates
    ) and frozen_construction_catalog is None:
        raise TemporalDiscoveryContractError(
            "QD campaign with indicators requires a frozen construction catalog for lake scope"
        )
    normalized_commit = execution_engine_commit.strip().lower()
    if len(normalized_commit) != 40 or any(
        value not in "0123456789abcdef" for value in normalized_commit
    ):
        raise TemporalDiscoveryContractError(
            "QD execution engine commit must be a full 40-character Git SHA"
        )
    template = _read(
        Path(template_preparation_path), name="QD screening template preparation"
    )
    if template.get("schemaVersion") != TEMPORAL_SEARCH_PREPARATION_SCHEMA:
        raise TemporalDiscoveryContractError(
            "QD screening template is not a finite temporal preparation"
        )
    evidence_context = qd_predeclared_evidence_context(
        template,
        worker_contract_sha256=worker_contract_sha256,
        construction_catalog=frozen_construction_catalog,
        construction_catalog_path=construction_catalog_path,
    )
    if population_schema == QD_POPULATION_SCHEMA:
        if population_payload.get("policyName") != QD_POLICY_NAME or (
            population_payload.get("policySha256") != QD_POLICY_SHA256
        ):
            raise TemporalDiscoveryContractError("QD v3 population policy mismatch")
        if (
            population_payload.get("predeclaredEvidenceContextSha256")
            != evidence_context["predeclaredEvidenceContextSha256"]
        ):
            raise TemporalDiscoveryContractError(
                "QD population was generated for different predeclared evidence"
            )
    source_candidates = template.get("candidates") or []
    windows = template.get("developmentWindows") or []
    if not source_candidates or not windows:
        raise TemporalDiscoveryContractError(
            "QD screening template requires candidates and windows"
        )
    if evidence_ladder is not None:
        validate_template_discovery_windows(template, evidence_ladder)
    worker_contract = _clone(template["workerContract"], name="QD worker contract")
    if worker_contract_sha256 is not None:
        normalized_worker_contract = worker_contract_sha256.strip().lower()
        if (
            not normalized_worker_contract.startswith("sha256:")
            or len(normalized_worker_contract) != 71
            or any(
                value not in "0123456789abcdef"
                for value in normalized_worker_contract[7:]
            )
        ):
            raise TemporalDiscoveryContractError(
                "QD worker contract must be a canonical sha256 digest"
            )
        worker_contract["workerContractSha256"] = normalized_worker_contract
    exemplar = source_candidates[0]
    input_map = {
        str(item["windowId"]): item["evidencePlan"]
        for item in exemplar.get("windowInputs") or []
    }
    window_ids = [str(item["windowId"]) for item in windows]
    if set(input_map) != set(window_ids):
        raise TemporalDiscoveryContractError(
            "QD screening template does not bind every window"
        )
    finite_candidates = []
    for candidate in candidates:
        profile = candidate["sourceProfile"]
        if bidirectional_policy is not None and (
            profile.get("version") != "v3" or profile.get("directionMode") != "both"
        ):
            raise TemporalDiscoveryContractError(
                "QD bidirectional campaign refuses standalone v2 module tasks"
            )
        source_sha = candidate["sourceProfileSha256"]
        finite_candidates.append(
            {
                "candidateId": candidate["candidateId"],
                "sourceProfile": profile,
                "sourceProfileSha256": source_sha,
                "instrument": exemplar["instrument"],
                "timeframe": exemplar["timeframe"],
                "barLimit": exemplar["barLimit"],
                "windowInputs": [
                    {
                        "windowId": window_id,
                        "evidencePlan": _rotate_evidence_plan(
                            input_map[window_id],
                            raw_source_profile_sha256=source_sha,
                            source_profile=profile,
                            base_decision_timeframe=exemplar["timeframe"],
                            frozen_construction_catalog=frozen_construction_catalog,
                        ),
                    }
                    for window_id in window_ids
                ],
            }
        )
    task_count = len(finite_candidates) * len(windows)
    generation_index = (
        int(population_payload["generationIndex"])
        if population_schema == QD_POPULATION_SCHEMA
        else 0
    )
    preparation = {
        "schemaVersion": TEMPORAL_SEARCH_PREPARATION_SCHEMA,
        "authorityLabel": (
            str(template["authorityLabel"]) + f"-qd-generation-{generation_index}"
        ),
        "workerContract": worker_contract,
        "candidates": finite_candidates,
        "developmentWindows": _clone(windows, name="QD development windows"),
        "prohibitedEvidence": _clone(
            template["prohibitedEvidence"], name="QD prohibited evidence"
        ),
        "bounds": {
            "maxCandidates": len(finite_candidates),
            "maxDevelopmentWindows": len(windows),
            "maxTasks": task_count,
            "maxAttempts": int(template["bounds"]["maxAttempts"]),
            "deadlineSeconds": float(template["bounds"]["deadlineSeconds"]),
        },
    }
    authority = build_authority(preparation)
    candidate_identity_map = {
        str(item["candidateId"]): item.get("candidateIdentitySha256")
        for item in candidates
    }
    candidate_map = {str(item["candidateId"]): item for item in candidates}
    evaluation_candidates = []
    for candidate in finite_candidates:
        profile = candidate["sourceProfile"]
        plans = [item["evidencePlan"] for item in candidate["windowInputs"]]
        source_candidate = candidate_map[str(candidate["candidateId"])]
        canonical_evidence_identity = None
        if population_schema == QD_POPULATION_SCHEMA:
            canonical_evidence_identity = qd_canonical_evidence_identity(
                source_candidate, evidence_context
            )
            if (
                source_candidate.get("canonicalEvidenceIdentitySha256")
                != canonical_evidence_identity
            ):
                raise TemporalDiscoveryContractError(
                    "QD candidate canonical evidence identity diverged before evaluation"
                )
        evaluation_candidates.append(
            {
                "candidateId": candidate["candidateId"],
                "candidateIdentitySha256": candidate_identity_map[
                    str(candidate["candidateId"])
                ],
                "programSha256": source_candidate.get("programSha256"),
                "canonicalEvidenceIdentitySha256": canonical_evidence_identity,
                "sourceProfileSha256": candidate["sourceProfileSha256"],
                "canonicalGraphSha256": canonical_sha256(profile["graph"]),
                "executionConfigSha256": canonical_sha256(
                    profile.get("executionConfig") or {}
                ),
                "instrument": candidate["instrument"],
                "timeframe": candidate["timeframe"],
                "barLimit": candidate["barLimit"],
                "windowPlans": [
                    {
                        "planId": plan["plan_id"],
                        "analysisWindowStart": plan["analysis_window_start"],
                        "analysisWindowEnd": plan["analysis_window_end"],
                        "coveragePolicy": plan["coverage_policy"],
                        "windowSemanticSha256": plan["lake_window_binding"][
                            "window_semantic_sha256"
                        ],
                        "request": plan["lake_window_binding"]["request"],
                        "profileSnapshotSha256": plan["profile_snapshot_sha256"],
                    }
                    for plan in plans
                ],
            }
        )
    evaluation_identity = {
        "schemaVersion": "temporal_qd_evaluation_identity_v3",
        "policyName": QD_POLICY_NAME,
        "policySha256": QD_POLICY_SHA256,
        "populationSha256": population_sha,
        **({"evaluationPopulationSha256": evaluation_population_sha256} if evaluation_population_sha256 is not None else {}),
        "constructionCatalog": construction_catalog_identity,
        "templatePreparationSha256": canonical_sha256(template),
        "workerContract": _clone(worker_contract, name="evaluation worker contract"),
        "executionEngineCommit": normalized_commit,
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
        "predeclaredEvidenceContext": evidence_context,
        "predeclaredEvidenceContextSha256": evidence_context[
            "predeclaredEvidenceContextSha256"
        ],
        "warmupAndEligibilityPolicy": {
            "coveragePolicy": "require_complete",
            "barLimitBoundPerCandidate": True,
            "workerContractOwnsIndicatorWarmup": True,
            "reservedEvidencePermitted": False,
        },
        "evaluationSeeds": [],
        **({"evidenceLadder": _clone(evidence_ladder, name="QD evidence ladder")} if evidence_ladder is not None else {}),
        "candidates": evaluation_candidates,
        **({"bidirectionalPairPolicy": {key: value for key, value in bidirectional_policy.items() if key != "policySha256"}} if bidirectional_policy is not None else {}),
    }
    evaluation_identity["evaluationIdentitySha256"] = canonical_sha256(
        evaluation_identity
    )
    root = Path(output_root)
    _write_once(root / "preparation.json", preparation)
    _write_once(root / "authority.json", authority)
    _write_once(root / "evaluation-identity.json", evaluation_identity)
    manifest = materialize_plan(authority, root / "screening-run")
    campaign = {
        "schemaVersion": QD_CAMPAIGN_SCHEMA,
        "generationIndex": generation_index,
        "populationSha256": population_sha,
        **({"evaluationPopulationSha256": evaluation_population_sha256} if evaluation_population_sha256 is not None else {}),
        "constructionCatalog": construction_catalog_identity,
        "preparationSha256": canonical_sha256(preparation),
        "authorityId": authority["authorityId"],
        "taskMatrixSha256": manifest["taskMatrixSha256"],
        "candidateCount": len(finite_candidates),
        "windowCount": len(windows),
        "taskCount": task_count,
        **({"bidirectionalPairPolicy": {key: value for key, value in bidirectional_policy.items() if key != "policySha256"}} if bidirectional_policy is not None else {}),
        "evaluationIdentitySha256": evaluation_identity["evaluationIdentitySha256"],
        "marketEvidenceScope": "predeclared_development_windows_only",
        "reservedEvidencePermitted": False,
        **({"evidenceLadderSha256": evidence_ladder["evidenceLadderSha256"]} if evidence_ladder is not None else {}),
    }
    campaign["campaignSha256"] = canonical_sha256(campaign)
    _write_once(root / "campaign.json", campaign)
    return {
        "schemaVersion": "temporal_qd_screening_campaign_result_v3",
        "campaignSha256": campaign["campaignSha256"],
        "authorityId": authority["authorityId"],
        "taskMatrixSha256": manifest["taskMatrixSha256"],
        "candidateCount": len(finite_candidates),
        "windowCount": len(windows),
        "taskCount": task_count,
        "evaluationIdentitySha256": evaluation_identity["evaluationIdentitySha256"],
        **({"evaluationPopulationSha256": evaluation_population_sha256} if evaluation_population_sha256 is not None else {}),
        "outputRoot": str(root.resolve()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--population", type=Path, required=True)
    parser.add_argument("--template-preparation", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--execution-engine-commit", required=True)
    parser.add_argument("--worker-contract-sha256")
    parser.add_argument("--construction-catalog", type=Path)
    args = parser.parse_args()
    print(
        json.dumps(
            freeze_qd_screening_campaign(
                population_path=args.population,
                template_preparation_path=args.template_preparation,
                output_root=args.output_root,
                execution_engine_commit=args.execution_engine_commit,
                worker_contract_sha256=args.worker_contract_sha256,
                construction_catalog_path=args.construction_catalog,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()


__all__ = ["freeze_qd_screening_campaign"]
