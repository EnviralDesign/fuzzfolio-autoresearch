"""Freeze and audit the repository-only Stage 5E-2 admission checkpoint."""

from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping

from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from .temporal_search_activation import audit_activation_causality
from .temporal_search_policy_v2 import (
    audit_management_witnesses,
    audit_policy_v2_population,
)


CHECKPOINT_SCHEMA = "temporal_search_stage5e2_checkpoint_v1"
MANIFEST_SCHEMA = "temporal_search_stage5e2_manifest_v1"


def _read(path: Path, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"{name} root must be an object")
    return value


def _encoded(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False
    ) + "\n"


def _write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != value:
        raise TemporalDiscoveryContractError(f"refusing divergent checkpoint: {path}")
    path.write_text(value, encoding="utf-8")


def _file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def _verify_identity(path: Path, *, field: str, name: str) -> dict[str, Any]:
    value = _read(path, name=name)
    supplied = str(value.pop(field, ""))
    if canonical_sha256(value) != supplied:
        raise TemporalDiscoveryContractError(f"{name} identity mismatch")
    value[field] = supplied
    return value


def _verify_manifest(root: Path, *, report_sha256: str) -> dict[str, Any]:
    manifest = _read(root / "manifest.json", name=f"{root.name} manifest")
    supplied = str(manifest.pop("manifestSha256", ""))
    if canonical_sha256(manifest) != supplied:
        raise TemporalDiscoveryContractError(f"{root.name} manifest identity mismatch")
    if manifest.get("reportSha256") != report_sha256:
        raise TemporalDiscoveryContractError(f"{root.name} report/manifest mismatch")
    expected = set()
    for item in manifest.get("files") or []:
        path = root / str(item["relativePath"])
        expected.add(path.resolve())
        if (
            not path.is_file()
            or path.stat().st_size != int(item["length"])
            or _file_sha(path) != item["sha256"]
        ):
            raise TemporalDiscoveryContractError(f"{root.name} file mismatch: {path}")
    actual = {
        path.resolve()
        for path in root.rglob("*")
        if path.is_file() and not (path.name == "manifest.json" and path.parent == root)
    }
    if expected != actual:
        raise TemporalDiscoveryContractError(f"{root.name} inventory drift")
    manifest["manifestSha256"] = supplied
    return manifest


def _commit(value: str, *, name: str) -> str:
    token = str(value).strip().lower()
    if not re.fullmatch(r"[0-9a-f]{40}", token):
        raise TemporalDiscoveryContractError(f"{name} must be an exact commit SHA")
    return token


def _group(report: Mapping[str, Any], dimension: str, value: str) -> dict[str, Any]:
    return next(
        item
        for item in report["groupSummaries"][dimension]
        if item["value"] == value
    )


def _markdown(checkpoint: Mapping[str, Any]) -> str:
    historical = checkpoint["historicalCausality"]
    generated = checkpoint["generatorAdmission"]
    selector = checkpoint["selectorAdmission"]
    lines = [
        "# Stage 5E-2 repository-only search-policy checkpoint",
        "",
        "Status: **ready for review; fresh windows and distributed search remain blocked**.",
        "",
        "## Historical activation shape",
        "",
        f"- Break-even: {historical['breakEven']['authored']} authored, "
        f"{historical['breakEven']['activated']} activated, "
        f"{historical['breakEven']['changedClosure']} changed closure.",
        f"- Trailing: {historical['trailing']['authored']} authored, "
        f"{historical['trailing']['activated']} activated, "
        f"{historical['trailing']['changedClosure']} changed closure.",
        f"- Immediate trailing correction: {historical['immediateTrailing']['activated']} "
        f"of {historical['immediateTrailing']['authored']} were active; entry-atomic "
        "activation was previously undercounted by trace-only analysis.",
        f"- Explicit trailing: {historical['explicitTrailing']['activated']} of "
        f"{historical['explicitTrailing']['authored']} activated in v1.",
        "",
        "## Generator v2 synthetic admission",
        "",
        f"- {generated['candidateCount']} unique real-validator programs: "
        f"{generated['sourceModeCounts']['broad_seed_mutation']} broad seed mutations and "
        f"{generated['sourceModeCounts']['seed_derived']} seed-derived mutations.",
        f"- {generated['authoredCapabilityCount']} authored management capabilities and "
        f"{generated['witnessCount']} positive/negative witnesses; every witness restarted exactly.",
        f"- Proposal dispositions: {generated['proposalDispositionCounts']}.",
        "- Zero orphan plans, unreachable management actions, dominated management actions, "
        "or missing explicit-trailing routes in the accepted population.",
        "",
        "## Selector v2 synthetic admission",
        "",
        f"- Robust envelope: {selector['eligibleCandidateCount']} of "
        f"{selector['activePopulationCount']} active synthetic candidates.",
        f"- Selected union: {selector['selectedCandidateCount']}; stratified control: "
        f"{selector['stratifiedControlCount']}; confirmation ceiling use: "
        f"{selector['confirmationCandidateCount']} of 96.",
        "- Original, reversed, five shuffled orders, and PYTHONHASHSEED 1..5 were byte-identical.",
        "- This is policy admission only. Synthetic economics are not trading evidence.",
        "",
        "## Stop boundary",
        "",
        "No fresh development windows, evidence plan, authority, Gateway task, market-data read, "
        "or distributed search was defined or launched. Before any large run, the user receives "
        "a deep checkpoint on activity, cohort distributions, activation/rejection causes, costs, "
        "stability, generalization, and the hypotheses/risks of scaling.",
        "",
    ]
    return "\n".join(lines)


def freeze_stage5e2_checkpoint(
    *,
    root: Path | str,
    autoresearch_commit: str,
    fuzzfolio_commit: str,
    worker_contract_sha256: str,
) -> dict[str, Any]:
    base = Path(root)
    activation_audit = audit_activation_causality(base / "activation-causality")
    generator_audit = audit_policy_v2_population(base / "generator-v2")
    witness_audit = audit_management_witnesses(base / "management-witnesses")
    activation = _read(
        base / "activation-causality" / "activation-causality.json",
        name="activation causality",
    )
    population = _read(base / "generator-v2" / "population.json", name="population")
    journal = _read(
        base / "generator-v2" / "generation-journal.json", name="generation journal"
    )
    witnesses = _read(base / "management-witnesses" / "report.json", name="witnesses")
    determinism = _verify_identity(
        base / "generator-v2-determinism.json",
        field="reportSha256",
        name="generator determinism",
    )
    selector = _verify_identity(
        base / "selector-v2-synthetic" / "admission.json",
        field="reportSha256",
        name="selector admission",
    )
    selector_manifest = _verify_manifest(
        base / "selector-v2-synthetic", report_sha256=selector["reportSha256"]
    )
    if (
        determinism.get("allChecksPassed") is not True
        or determinism.get("repeatExact") is not True
        or not all(item.get("exact") for item in determinism.get("hashSeedResults") or [])
        or selector.get("allChecksPassed") is not True
        or not all(item.get("exact") for item in selector.get("orderDeterminism") or [])
        or not all(item.get("exact") for item in selector.get("hashSeedDeterminism") or [])
    ):
        raise TemporalDiscoveryContractError("Stage 5E-2 determinism admission is incomplete")
    if any(
        item["managementReachability"]["acceptable"] is not True
        for item in population["candidates"]
    ):
        raise TemporalDiscoveryContractError("accepted population has reachability defects")

    management_types = {
        item["managementType"]: item for item in activation["managementTypeSummary"]
    }
    immediate = _group(activation, "activationMode", "immediate")
    explicit = _group(activation, "activationMode", "explicit")
    action_counts: Counter[str] = Counter()
    repair_counts: Counter[str] = Counter()
    for candidate in population["candidates"]:
        for transition in candidate["sourceProfile"]["graph"]["transitions"]:
            action_counts.update(
                str(action.get("kind")) for action in transition.get("actions") or []
            )
        repair_counts.update(
            str(item["kind"]) for item in candidate.get("activationAwareRepairs") or []
        )
    checkpoint = {
        "schemaVersion": CHECKPOINT_SCHEMA,
        "status": "ready_for_review_fresh_search_blocked",
        "autoresearchImplementationCommit": _commit(
            autoresearch_commit, name="AutoResearch commit"
        ),
        "fuzzfolioCommit": _commit(fuzzfolio_commit, name="FuzzFolio commit"),
        "workerContractSha256": worker_contract_sha256,
        "sourceStage5e1BindingId": (
            "sha256:aa178786ab1bdabacd57b4478512d4e0475b3b54fcb6465032b818b71391930c"
        ),
        "historicalCausality": {
            "reportSha256": activation_audit["reportSha256"],
            "manifestSha256": activation_audit["manifestSha256"],
            "candidateCount": activation["candidateCount"],
            "taskResultCount": activation["taskResultCount"],
            "breakEven": {
                "authored": management_types["break_even"]["instanceCount"],
                "activated": management_types["break_even"]["activatedCount"],
                "changedClosure": management_types["break_even"]["changedClosureCount"],
            },
            "trailing": {
                "authored": management_types["trailing_stop"]["instanceCount"],
                "activated": management_types["trailing_stop"]["activatedCount"],
                "changedClosure": management_types["trailing_stop"]["changedClosureCount"],
            },
            "immediateTrailing": {
                "authored": immediate["instanceCount"],
                "activated": immediate["activatedCount"],
            },
            "explicitTrailing": {
                "authored": explicit["instanceCount"],
                "activated": explicit["activatedCount"],
            },
            "deepestStateSummary": activation["deepestStateSummary"],
            "rejectionReasonCounts": activation["rejectionReasonCounts"],
            "dossierSetSha256": activation["dossierSetSha256"],
        },
        "generatorAdmission": {
            "generatorVersion": population["generatorVersion"],
            "configSha256": generator_audit["configSha256"],
            "populationSha256": generator_audit["populationSha256"],
            "journalSha256": generator_audit["journalSha256"],
            "manifestSha256": generator_audit["manifestSha256"],
            "determinismReportSha256": determinism["reportSha256"],
            "candidateCount": population["candidateCount"],
            "sourceModeCounts": population["sourceModeCounts"],
            "proposalCount": journal["proposalCount"],
            "proposalDispositionCounts": journal["dispositionCounts"],
            "actionCounts": dict(sorted(action_counts.items())),
            "activationAwareRepairCounts": dict(sorted(repair_counts.items())),
            "authoredCapabilityCount": witnesses["authoredCapabilityCount"],
            "positiveWitnessCounts": witnesses["positiveWitnessCounts"],
            "negativeWitnessCounts": witnesses["negativeWitnessCounts"],
            "witnessCount": witness_audit["witnessCount"],
            "witnessReportSha256": witness_audit["reportSha256"],
            "witnessManifestSha256": witness_audit["manifestSha256"],
            "restartExactWitnessCount": witness_audit["restartExactWitnessCount"],
            "staticReachabilityIssueCount": 0,
        },
        "selectorAdmission": {
            "selectorVersion": selector["selectorVersion"],
            "parameters": selector["selectorParameters"],
            "reportSha256": selector["reportSha256"],
            "manifestSha256": selector_manifest["manifestSha256"],
            "selectionSha256": selector["selectionSha256"],
            "activePopulationCount": selector["activePopulationCount"],
            "eligibleCandidateCount": selector["eligibleCandidateCount"],
            "economicArchiveCount": selector["economicArchiveCount"],
            "admissibleNoveltyArchiveCount": selector[
                "admissibleNoveltyArchiveCount"
            ],
            "diagnosticPureNoveltyArchiveCount": selector[
                "diagnosticPureNoveltyArchiveCount"
            ],
            "selectedCandidateCount": selector["selectedCandidateCount"],
            "stratifiedControlCount": selector["stratifiedControlCount"],
            "confirmationCandidateCount": selector["confirmationCandidateCount"],
            "thresholds": selector["thresholds"],
        },
        "executionBoundary": {
            "marketEvidenceRead": False,
            "gatewayContacted": False,
            "freshWindowsDefined": False,
            "evidencePlanDefined": False,
            "authorityDefined": False,
            "distributedTasksLaunched": False,
            "fuzzfolioSourceModified": False,
        },
        "preScaleReviewRequired": {
            "required": True,
            "topics": [
                "activity_and_inactivity",
                "cohort_distributions",
                "activation_and_rejection_causes",
                "cost_drag_and_cost_per_trade",
                "screening_to_confirmation_stability",
                "generalization_signals",
                "scaled_run_hypotheses_and_risks",
            ],
        },
        "nextPermittedOperation": (
            "review Stage 5E-2; only after explicit admission may Stage 5E-3 freeze "
            "fresh non-reserved windows and a modest campaign"
        ),
    }
    checkpoint["checkpointSha256"] = canonical_sha256(checkpoint)
    _write(base / "checkpoint.json", _encoded(checkpoint))
    _write(base / "checkpoint.md", _markdown(checkpoint))
    files = []
    for path in sorted(item for item in base.rglob("*") if item.is_file()):
        if path.name == "manifest.json" and path.parent == base:
            continue
        files.append(
            {
                "relativePath": path.relative_to(base).as_posix(),
                "length": path.stat().st_size,
                "sha256": _file_sha(path),
            }
        )
    manifest = {
        "schemaVersion": MANIFEST_SCHEMA,
        "checkpointSha256": checkpoint["checkpointSha256"],
        "fileCount": len(files),
        "files": files,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write(base / "manifest.json", _encoded(manifest))
    return {
        "schemaVersion": "temporal_search_stage5e2_checkpoint_result_v1",
        "checkpointSha256": checkpoint["checkpointSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "fileCount": manifest["fileCount"],
        "status": checkpoint["status"],
    }


def audit_stage5e2_checkpoint(root: Path | str) -> dict[str, Any]:
    base = Path(root)
    checkpoint = _verify_identity(
        base / "checkpoint.json", field="checkpointSha256", name="Stage 5E-2 checkpoint"
    )
    manifest = _read(base / "manifest.json", name="Stage 5E-2 manifest")
    supplied_manifest = str(manifest.pop("manifestSha256", ""))
    if canonical_sha256(manifest) != supplied_manifest:
        raise TemporalDiscoveryContractError("Stage 5E-2 manifest identity mismatch")
    if manifest.get("checkpointSha256") != checkpoint["checkpointSha256"]:
        raise TemporalDiscoveryContractError("Stage 5E-2 checkpoint/manifest mismatch")
    expected = set()
    for item in manifest.get("files") or []:
        path = base / str(item["relativePath"])
        expected.add(path.resolve())
        if (
            not path.is_file()
            or path.stat().st_size != int(item["length"])
            or _file_sha(path) != item["sha256"]
        ):
            raise TemporalDiscoveryContractError(f"Stage 5E-2 file mismatch: {path}")
    actual = {
        path.resolve()
        for path in base.rglob("*")
        if path.is_file() and not (path.name == "manifest.json" and path.parent == base)
    }
    if actual != expected:
        raise TemporalDiscoveryContractError("Stage 5E-2 artifact inventory drift")
    return {
        "schemaVersion": "temporal_search_stage5e2_checkpoint_audit_v1",
        "ok": True,
        "checkpointSha256": checkpoint["checkpointSha256"],
        "manifestSha256": supplied_manifest,
        "fileCount": manifest["fileCount"],
        "status": checkpoint["status"],
    }


__all__ = ["audit_stage5e2_checkpoint", "freeze_stage5e2_checkpoint"]
