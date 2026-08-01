"""Stage 5E-3 modest fresh-window policy-validation prelaunch contracts.

This module may inspect promoted Lake *coverage metadata* and may freeze plans,
but it never reads bars, contacts the Lab Gateway, or submits work.  Screening
execution remains a separate, explicitly authorized operation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any

import httpx

from .lake_window_client import _lake_credentials
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_discovery_validation import _finite_preparation, _normalize_preparation
from .temporal_search import (
    build_authority,
    build_task_matrix,
    canonical_sha256,
    materialize_plan,
    validate_authority,
)
from .temporal_search_policy_v2 import (
    GENERATOR_V2_STAGE5E3_PARAMETERS,
    audit_management_witnesses,
    audit_policy_v2_population,
)


COVERAGE_SCHEMA = "temporal_search_stage5e3_coverage_metadata_v1"
WINDOW_SELECTION_SCHEMA = "temporal_search_stage5e3_window_selection_v1"
CAMPAIGN_SPEC_SCHEMA = "temporal_search_stage5e3_campaign_spec_v1"
MATRIX_REHASH_SCHEMA = "temporal_search_stage5e3_matrix_rehash_v1"
CHECKPOINT_SCHEMA = "temporal_search_stage5e3_prelaunch_checkpoint_v1"
MANIFEST_SCHEMA = "temporal_search_stage5e3_prelaunch_manifest_v1"

WINDOW_SELECTION_VERSION = "stage5e3_month_block_chronology_hash_v1"
LEVEL_C_START = "2021-06-29T00:00:00Z"
LEVEL_C_END = "2024-06-29T00:00:00Z"
BLOCK_MONTHS = 1
WINDOW_LABELS = ("E", "F", "G", "H")
SCREENING_LABELS = ("E", "F")
CONFIRMATION_LABELS = ("G", "H")
PAIR = "EURUSD"
TIMEFRAMES = ("H1", "M15", "M5")
BAR_LIMIT = 5000

EXHAUSTED_WINDOWS = (
    ("window_a_2021_08", "2021-08-01T00:00:00Z", "2022-02-01T00:00:00Z"),
    ("window_b_2022_05", "2022-05-01T00:00:00Z", "2022-11-01T00:00:00Z"),
    ("window_c_2023_02", "2023-02-01T00:00:00Z", "2023-08-01T00:00:00Z"),
    ("window_d_2023_11", "2023-11-01T00:00:00Z", "2024-05-01T00:00:00Z"),
)
PROHIBITED_WINDOWS = (
    (
        "reserved_protected_2024_06_29_forward",
        "2024-06-29T00:00:00Z",
        "2100-01-01T00:00:00Z",
        "Reserved unseen, scrutiny, survivor-tail, final-validation, and future evidence.",
    ),
)

_SHA40 = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")


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


def _write_immutable(path: Path, value: Mapping[str, Any]) -> None:
    encoded = _encoded(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalDiscoveryContractError(f"refusing divergent overwrite: {path}")
    path.write_text(encoded, encoding="utf-8")


def _file_sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _stamp(value: str) -> datetime:
    token = str(value or "").strip()
    if not token.endswith("Z"):
        raise TemporalDiscoveryContractError("timestamps must be explicit UTC values")
    try:
        return datetime.fromisoformat(token[:-1] + "+00:00")
    except ValueError as exc:
        raise TemporalDiscoveryContractError(f"invalid timestamp: {token}") from exc


def _format_stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _add_months(value: datetime, months: int) -> datetime:
    ordinal = value.year * 12 + (value.month - 1) + months
    return value.replace(year=ordinal // 12, month=ordinal % 12 + 1)


def _overlaps(start: str, end: str, other_start: str, other_end: str) -> bool:
    return _stamp(start) < _stamp(other_end) and _stamp(other_start) < _stamp(end)


def _identity(value: Mapping[str, Any], field: str, *, name: str) -> dict[str, Any]:
    payload = dict(value)
    supplied = str(payload.pop(field, ""))
    if not _SHA256.fullmatch(supplied) or canonical_sha256(payload) != supplied:
        raise TemporalDiscoveryContractError(f"{name} identity mismatch")
    payload[field] = supplied
    return payload


def build_coverage_metadata(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Redact the promoted Lake manifest to the selection-permitted metadata."""

    rows = []
    for raw in manifest.get("coverage") or []:
        if not isinstance(raw, Mapping):
            continue
        if (
            raw.get("dataset") != "bars"
            or str(raw.get("pair") or "").upper() != PAIR
            or str(raw.get("timeframe") or "").upper() not in TIMEFRAMES
        ):
            continue
        intervals = list(raw.get("attested_intervals") or [])
        buckets = list(raw.get("attested_bucket_intervals") or [])
        rows.append(
            {
                "dataset": "bars",
                "pair": PAIR,
                "timeframe": str(raw["timeframe"]).upper(),
                "availableFrom": str(raw.get("available_from") or ""),
                "availableTo": str(raw.get("available_to") or ""),
                "promotedThrough": str(raw.get("promoted_through") or ""),
                "status": str(raw.get("status") or ""),
                "source": str(raw.get("source") or ""),
                "calendarContractId": str(raw.get("calendar_contract_id") or ""),
                "marketStructureHash": str(raw.get("market_structure_hash") or ""),
                "attestedIntervalCount": len(intervals),
                "attestedIntervalsSha256": canonical_sha256(intervals),
                "attestedBucketIntervalCount": len(buckets),
                "attestedBucketIntervalsSha256": canonical_sha256(buckets),
                "coverageEntrySha256": canonical_sha256(dict(raw)),
            }
        )
    rows.sort(key=lambda item: item["timeframe"])
    if {item["timeframe"] for item in rows} != set(TIMEFRAMES) or len(rows) != 3:
        raise TemporalDiscoveryContractError(
            "promoted Lake metadata must contain exact EURUSD H1/M15/M5 coverage"
        )
    if any(item["status"] != "promoted" for item in rows):
        raise TemporalDiscoveryContractError("Stage 5E-3 requires promoted coverage")
    global_sha = str(manifest.get("coverage_sha256") or "")
    source_sha = str(manifest.get("source_coverage_sha256") or "")
    if not _SHA256.fullmatch(global_sha) or not _SHA256.fullmatch(source_sha):
        raise TemporalDiscoveryContractError("Lake coverage identities are missing")
    common_from = max(_stamp(item["availableFrom"]) for item in rows)
    common_through = min(_stamp(item["promotedThrough"]) for item in rows)
    value = {
        "schemaVersion": COVERAGE_SCHEMA,
        "selectionPermittedFieldsOnly": True,
        "priceBarsRead": False,
        "pair": PAIR,
        "timeframes": list(TIMEFRAMES),
        "lakeManifestUpdatedAt": str(manifest.get("updated_at") or ""),
        "lakeManifestPromotedAt": str(manifest.get("promoted_at") or ""),
        "coverageSha256": global_sha,
        "sourceCoverageSha256": source_sha,
        "commonAvailableFrom": _format_stamp(common_from),
        "commonPromotedThrough": _format_stamp(common_through),
        "dataAvailabilityCutoff": _format_stamp(common_through),
        "coverageEntries": rows,
    }
    value["coverageMetadataSha256"] = canonical_sha256(value)
    return value


def fetch_coverage_metadata(*, timeout_seconds: float = 120.0) -> dict[str, Any]:
    base_url, token = _lake_credentials()
    if not base_url or not token:
        raise TemporalDiscoveryContractError("Market Data Lake credentials are unavailable")
    response = httpx.get(
        f"{base_url}/api/lake/manifest",
        headers={"Authorization": f"Bearer {token}"},
        timeout=httpx.Timeout(timeout_seconds),
    )
    response.raise_for_status()
    manifest = response.json()
    if not isinstance(manifest, dict):
        raise TemporalDiscoveryContractError("Lake manifest response is not an object")
    return build_coverage_metadata(manifest)


def build_window_selection(coverage_metadata: Mapping[str, Any]) -> dict[str, Any]:
    coverage = _identity(
        coverage_metadata,
        "coverageMetadataSha256",
        name="Stage 5E-3 coverage metadata",
    )
    if coverage.get("schemaVersion") != COVERAGE_SCHEMA:
        raise TemporalDiscoveryContractError("unknown Stage 5E-3 coverage schema")
    common_from = _stamp(str(coverage["commonAvailableFrom"]))
    common_through = _stamp(str(coverage["commonPromotedThrough"]))
    start_bound = _stamp(LEVEL_C_START)
    end_bound = _stamp(LEVEL_C_END)
    cursor = start_bound.replace(day=1)
    if cursor < start_bound:
        cursor = _add_months(cursor, 1)
    eligible = []
    excluded = []
    while cursor < end_bound:
        end = _add_months(cursor, BLOCK_MONTHS)
        start_text = _format_stamp(cursor)
        end_text = _format_stamp(end)
        reasons = []
        if cursor < common_from or end > common_through:
            reasons.append("outside_common_promoted_coverage")
        for window_id, old_start, old_end in EXHAUSTED_WINDOWS:
            if _overlaps(start_text, end_text, old_start, old_end):
                reasons.append(f"overlaps_exhausted:{window_id}")
        for window_id, protected_start, protected_end, _reason in PROHIBITED_WINDOWS:
            if _overlaps(start_text, end_text, protected_start, protected_end):
                reasons.append(f"overlaps_prohibited:{window_id}")
        if end > end_bound:
            reasons.append("outside_authorized_development_interval")
        rank = canonical_sha256(
            {
                "selectionVersion": WINDOW_SELECTION_VERSION,
                "analysisWindowStart": start_text,
                "analysisWindowEnd": end_text,
            }
        )
        row = {
            "analysisWindowStart": start_text,
            "analysisWindowEnd": end_text,
            "blockMonths": BLOCK_MONTHS,
            "selectionRankSha256": rank,
        }
        if reasons:
            excluded.append({**row, "reasons": sorted(reasons)})
        else:
            eligible.append(row)
        cursor = end
    if len(eligible) < len(WINDOW_LABELS):
        raise TemporalDiscoveryContractError("fewer than four eligible fresh blocks")
    ranked = sorted(
        eligible,
        key=lambda item: (item["selectionRankSha256"], item["analysisWindowStart"]),
    )
    selected_rows = []
    for label, row in zip(WINDOW_LABELS, ranked[:4], strict=True):
        start = _stamp(row["analysisWindowStart"])
        window_id = f"window_{label.lower()}_{start.year:04d}_{start.month:02d}"
        selected_rows.append(
            {
                **row,
                "label": label,
                "windowId": window_id,
                "screeningStage": (
                    "screening" if label in SCREENING_LABELS else "confirmation"
                ),
            }
        )
    selected_chronology = sorted(
        selected_rows, key=lambda item: item["analysisWindowStart"]
    )
    for index, left in enumerate(selected_chronology):
        for right in selected_chronology[index + 1 :]:
            if _overlaps(
                left["analysisWindowStart"],
                left["analysisWindowEnd"],
                right["analysisWindowStart"],
                right["analysisWindowEnd"],
            ):
                raise TemporalDiscoveryContractError("selected windows overlap")
    value = {
        "schemaVersion": WINDOW_SELECTION_SCHEMA,
        "selectionVersion": WINDOW_SELECTION_VERSION,
        "coverageMetadataSha256": coverage["coverageMetadataSha256"],
        "coverageSha256": coverage["coverageSha256"],
        "sourceCoverageSha256": coverage["sourceCoverageSha256"],
        "dataAvailabilityCutoff": coverage["dataAvailabilityCutoff"],
        "authorizedDevelopmentInterval": {
            "start": LEVEL_C_START,
            "end": LEVEL_C_END,
        },
        "blockMonths": BLOCK_MONTHS,
        "rankingReads": ["selection_version", "block_start", "block_end"],
        "rankingProhibits": [
            "price_bars",
            "volatility",
            "candidate_outcomes",
            "economics",
            "window_semantic_hashes",
        ],
        "exhaustedWindows": [
            {"windowId": name, "start": start, "end": end}
            for name, start, end in EXHAUSTED_WINDOWS
        ],
        "prohibitedWindows": [
            {"windowId": name, "start": start, "end": end, "reason": reason}
            for name, start, end, reason in PROHIBITED_WINDOWS
        ],
        "eligibleBlockCount": len(eligible),
        "eligibleBlocks": sorted(eligible, key=lambda item: item["analysisWindowStart"]),
        "excludedBlocks": sorted(excluded, key=lambda item: item["analysisWindowStart"]),
        "selectedWindows": selected_rows,
        "priceBarsRead": False,
    }
    value["windowSelectionSha256"] = canonical_sha256(value)
    return value


def freeze_window_selection(output_root: Path | str) -> dict[str, Any]:
    root = Path(output_root)
    coverage = fetch_coverage_metadata()
    selection = build_window_selection(coverage)
    _write_immutable(root / "coverage-metadata.json", coverage)
    _write_immutable(root / "window-selection.json", selection)
    return {
        "schemaVersion": "temporal_search_stage5e3_window_freeze_result_v1",
        "coverageMetadataSha256": coverage["coverageMetadataSha256"],
        "windowSelectionSha256": selection["windowSelectionSha256"],
        "dataAvailabilityCutoff": selection["dataAvailabilityCutoff"],
        "eligibleBlockCount": selection["eligibleBlockCount"],
        "selectedWindows": selection["selectedWindows"],
        "priceBarsRead": False,
    }


def _campaign_spec(
    *,
    selection: Mapping[str, Any],
    population: Mapping[str, Any],
    generator_audit: Mapping[str, Any],
    source_preparation_sha256: str,
    autoresearch_implementation_commit: str,
    fuzzfolio_commit: str,
    worker_contract_sha256: str,
) -> dict[str, Any]:
    if not _SHA40.fullmatch(autoresearch_implementation_commit):
        raise TemporalDiscoveryContractError("AutoResearch commit must be exact")
    if not _SHA40.fullmatch(fuzzfolio_commit):
        raise TemporalDiscoveryContractError("FuzzFolio commit must be exact")
    value = {
        "schemaVersion": CAMPAIGN_SPEC_SCHEMA,
        "campaignId": "stage5e3-modest-policy-validation-v1",
        "purpose": "validate generator-v2 activation quality and selector-v2 enrichment; not production strategy search",
        "hypotheses": {
            "H1": "activation-aware generation reduces unreachable, dormant, and rejected management behavior without behavioral collapse",
            "H2": "the robust-envelope selected cohort generalizes better than its embedded deterministic control on untouched confirmation windows",
        },
        "autoresearchImplementationCommit": autoresearch_implementation_commit,
        "fuzzfolioCommit": fuzzfolio_commit,
        "workerContractSha256": worker_contract_sha256,
        "sourceStage5e2CheckpointSha256": "sha256:e9144bfa1e98d53a33382393f9fe294f29f36800a698538285bdd5b8ff90c4d1",
        "sourcePreparationSha256": source_preparation_sha256,
        "windowSelectionSha256": selection["windowSelectionSha256"],
        "coverageSha256": selection["coverageSha256"],
        "sourceCoverageSha256": selection["sourceCoverageSha256"],
        "dataAvailabilityCutoff": selection["dataAvailabilityCutoff"],
        "instrument": PAIR,
        "timeframe": "M5",
        "coveredTimeframes": list(TIMEFRAMES),
        "barLimit": BAR_LIMIT,
        "windows": list(selection["selectedWindows"]),
        "population": {
            "generatorVersion": population["generatorVersion"],
            "parameterProfile": generator_audit["parameterProfile"],
            "candidateCount": population["candidateCount"],
            "sourceModeCounts": population["sourceModeCounts"],
            "populationSha256": population["populationSha256"],
            "generatorManifestSha256": generator_audit["manifestSha256"],
        },
        "screening": {
            "windowLabels": list(SCREENING_LABELS),
            "candidateCount": 128,
            "taskCount": 256,
            "costViews": ["research_conservative", "none"],
        },
        "confirmation": {
            "windowLabels": list(CONFIRMATION_LABELS),
            "authorityFrozen": False,
            "taskLaunchPermitted": False,
            "selectedCandidateCap": 64,
            "deterministicControlCount": 32,
            "confirmationCandidateCap": 96,
            "taskCeiling": 192,
        },
        "predeclaredSelectorEnrichmentCriteria": {
            "medianTotalConservativeRDelta": "> 0",
            "medianWorstWindowConservativeRDelta": "> 0",
            "anyPositiveWindowParticipationDelta": "> 0",
            "cliffsDeltaTotalConservativeRMinimum": 0.147,
            "activeOnlyMaterialReversalPermitted": False,
        },
        "generatorFailureCriteria": {
            "minimumRobustEligibleCandidates": 32,
            "severeDormantShare": 0.75,
            "explicitTrailingZeroActivationWithFeasibleOpportunity": True,
            "acceptedStaticReachabilityDefect": True,
            "behavioralCollapseUsesStage5e1Thresholds": True,
        },
        "stopConditions": [
            "identity_or_attestation_fault",
            "worker_contract_mismatch",
            "reserved_evidence_overlap",
            "task_matrix_drift",
            "fewer_than_32_robust_eligible_candidates",
            "behavioral_collapse",
            "severe_management_dormancy",
        ],
        "executionBoundary": {
            "gatewayStartPermitted": False,
            "screeningTaskLaunchPermitted": False,
            "confirmationTaskLaunchPermitted": False,
            "largeSearchPermitted": False,
        },
    }
    value["campaignSpecSha256"] = canonical_sha256(value)
    return value


def prepare_screening_prelaunch(
    *,
    root: Path | str,
    source_preparation_path: Path | str,
    autoresearch_implementation_commit: str,
    fuzzfolio_commit: str,
    worker_contract_sha256: str,
) -> dict[str, Any]:
    base = Path(root)
    coverage = _identity(
        _read(base / "coverage-metadata.json", name="coverage metadata"),
        "coverageMetadataSha256",
        name="coverage metadata",
    )
    selection = _identity(
        _read(base / "window-selection.json", name="window selection"),
        "windowSelectionSha256",
        name="window selection",
    )
    if build_window_selection(coverage) != selection:
        raise TemporalDiscoveryContractError("window selection is not reproducible")
    generator_audit = audit_policy_v2_population(base / "generator-v2")
    if generator_audit["parameterProfile"] != "stage5e3_modest_policy_validation":
        raise TemporalDiscoveryContractError("Stage 5E-3 requires its 128-program profile")
    population = _read(base / "generator-v2" / "population.json", name="population")
    source = _normalize_preparation(
        _read(Path(source_preparation_path), name="source preparation")
    )
    if source["validation"]["fuzzfolioCommit"] != fuzzfolio_commit:
        raise TemporalDiscoveryContractError("FuzzFolio source preparation drift")
    if source["workerContract"]["workerContractSha256"] != worker_contract_sha256:
        raise TemporalDiscoveryContractError("worker contract drift")
    if source["instrument"] != PAIR or source["timeframe"] != "M5":
        raise TemporalDiscoveryContractError("Stage 5E-3 instrument/timeframe drift")
    window_rows = list(selection["selectedWindows"])
    templates = []
    development_windows = []
    for row in window_rows:
        plan = _read(
            base / "windows" / row["windowId"] / "evidence-plan.json",
            name=f"{row['label']} evidence plan",
        )
        if (
            plan.get("schema_version") != "fuzzfolio.replay-evidence-plan.v2"
            or plan.get("analysis_window_start") != row["analysisWindowStart"]
            or plan.get("analysis_window_end") != row["analysisWindowEnd"]
            or plan.get("data_availability_cutoff") != selection["dataAvailabilityCutoff"]
        ):
            raise TemporalDiscoveryContractError(f"window {row['label']} plan drift")
        binding = plan.get("lake_window_binding") or {}
        if (
            binding.get("creation_global_coverage_sha256") != selection["coverageSha256"]
            or binding.get("creation_source_coverage_sha256")
            != selection["sourceCoverageSha256"]
        ):
            raise TemporalDiscoveryContractError(f"window {row['label']} coverage drift")
        development_windows.append(
            {
                "windowId": row["windowId"],
                "analysisWindowStart": row["analysisWindowStart"],
                "analysisWindowEnd": row["analysisWindowEnd"],
            }
        )
        templates.append({"windowId": row["windowId"], "evidencePlan": plan})
    pseudo = {
        "authorityLabel": "stage5e3-modest-policy-validation-v1",
        "workerContract": source["workerContract"],
        "instrument": PAIR,
        "timeframe": "M5",
        "barLimit": BAR_LIMIT,
        "developmentWindows": development_windows,
        "evidencePlanTemplates": templates,
        "prohibitedEvidence": [
            {
                "windowId": name,
                "analysisWindowStart": start,
                "analysisWindowEnd": end,
                "reason": reason,
            }
            for name, start, end, reason in PROHIBITED_WINDOWS
        ],
        "bounds": {"maxAttempts": 2, "deadlineSeconds": 7200.0},
    }
    window_id_by_label = {row["label"]: row["windowId"] for row in window_rows}
    screening_preparation = _finite_preparation(
        pseudo,
        candidates=population["candidates"],
        window_ids=[window_id_by_label[label] for label in SCREENING_LABELS],
        label_suffix="screening",
        max_tasks=256,
    )
    authority = build_authority(screening_preparation)
    tasks = build_task_matrix(authority)
    if len(tasks) != 256:
        raise TemporalDiscoveryContractError("Stage 5E-3 screening matrix is not 256 tasks")
    if {task["payload"]["window_id"] for task in tasks} != {
        window_id_by_label[label] for label in SCREENING_LABELS
    }:
        raise TemporalDiscoveryContractError("G/H leaked into screening matrix")
    campaign = _campaign_spec(
        selection=selection,
        population=population,
        generator_audit=generator_audit,
        source_preparation_sha256=source["preparationSha256"],
        autoresearch_implementation_commit=autoresearch_implementation_commit,
        fuzzfolio_commit=fuzzfolio_commit,
        worker_contract_sha256=worker_contract_sha256,
    )
    _write_immutable(base / "campaign-spec.json", campaign)
    _write_immutable(base / "screening" / "preparation.json", screening_preparation)
    _write_immutable(base / "screening" / "authority.json", authority)
    plan_manifest = materialize_plan(authority, base / "screening-plan-only")
    rehash = {
        "schemaVersion": MATRIX_REHASH_SCHEMA,
        "authorityId": authority["authorityId"],
        "taskCount": len(tasks),
        "taskMatrixSha256": canonical_sha256(tasks),
        "manifestTaskMatrixSha256": plan_manifest["taskMatrixSha256"],
        "exact": canonical_sha256(tasks) == plan_manifest["taskMatrixSha256"],
        "windowTaskCounts": {
            window_id: sum(task["payload"]["window_id"] == window_id for task in tasks)
            for window_id in sorted({task["payload"]["window_id"] for task in tasks})
        },
        "confirmationWindowResultPathAccessible": False,
        "gatewayContacted": False,
    }
    rehash["matrixRehashSha256"] = canonical_sha256(rehash)
    _write_immutable(base / "screening" / "matrix-rehash.json", rehash)
    return {
        "schemaVersion": "temporal_search_stage5e3_screening_prelaunch_result_v1",
        "campaignSpecSha256": campaign["campaignSpecSha256"],
        "authorityId": authority["authorityId"],
        "taskMatrixSha256": rehash["taskMatrixSha256"],
        "taskCount": len(tasks),
        "gatewayContacted": False,
        "screeningStarted": False,
    }


def capture_process_state(
    *, procman_url: str = "http://127.0.0.1:47831"
) -> dict[str, Any]:
    health_response = httpx.get(f"{procman_url.rstrip('/')}/health", timeout=10.0)
    process_response = httpx.get(f"{procman_url.rstrip('/')}/processes", timeout=10.0)
    health_response.raise_for_status()
    process_response.raise_for_status()
    health = health_response.json()
    processes = process_response.json()
    if not isinstance(health, dict) or not isinstance(processes, list):
        raise TemporalDiscoveryContractError("procman response shape is invalid")
    gateway = next(
        (item for item in processes if item.get("name") == "Lab Gateway"), None
    )
    workers = sorted(
        [
            {
                "id": item.get("id"),
                "name": item.get("name"),
                "type": item.get("process_type"),
                "status": item.get("status"),
                "contractMentioned": (
                    "b69ecc83570dc1996a39d24f4e8d6d7650ab0306b15831320c5acdca40522ee9"
                    in str(item.get("command") or "")
                ),
            }
            for item in processes
            if "Temporal Graph" in str(item.get("name") or "")
        ],
        key=lambda item: str(item["name"]),
    )
    value = {
        "schemaVersion": "temporal_search_stage5e3_process_state_v1",
        "procmanHealthy": health.get("ok") is True,
        "gateway": {
            "id": gateway.get("id") if gateway else None,
            "status": gateway.get("status") if gateway else None,
            "startedForPrelaunch": False,
            "contactedForPrelaunch": False,
        },
        "configuredTemporalWorkers": workers,
        "runningTemporalWorkerCount": sum(item["status"] == "Running" for item in workers),
    }
    if value["gateway"]["status"] != "Stopped":
        raise TemporalDiscoveryContractError("Gateway must remain stopped at prelaunch")
    value["processStateSha256"] = canonical_sha256(value)
    return value


def freeze_prelaunch_checkpoint(
    *,
    root: Path | str,
    autoresearch_evidence_commit: str,
    workflow_run_id: str,
    workflow_url: str,
    workflow_conclusion: str,
) -> dict[str, Any]:
    base = Path(root)
    coverage = _identity(
        _read(base / "coverage-metadata.json", name="coverage metadata"),
        "coverageMetadataSha256",
        name="coverage metadata",
    )
    selection = _identity(
        _read(base / "window-selection.json", name="window selection"),
        "windowSelectionSha256",
        name="window selection",
    )
    if build_window_selection(coverage) != selection:
        raise TemporalDiscoveryContractError("window selection replay mismatch")
    generator = audit_policy_v2_population(base / "generator-v2")
    witnesses = audit_management_witnesses(base / "management-witnesses")
    determinism = _identity(
        _read(base / "generator-v2-determinism.json", name="generator determinism"),
        "reportSha256",
        name="generator determinism",
    )
    if (
        determinism.get("allChecksPassed") is not True
        or determinism.get("repeatExact") is not True
        or not all(item.get("exact") for item in determinism.get("hashSeedResults") or [])
    ):
        raise TemporalDiscoveryContractError("generator determinism is incomplete")
    campaign = _identity(
        _read(base / "campaign-spec.json", name="campaign spec"),
        "campaignSpecSha256",
        name="campaign spec",
    )
    authority = validate_authority(
        _read(base / "screening" / "authority.json", name="screening authority")
    )
    tasks = build_task_matrix(authority)
    plan_manifest = _read(
        base / "screening-plan-only" / "task-manifest.json", name="task manifest"
    )
    rehash = _identity(
        _read(base / "screening" / "matrix-rehash.json", name="matrix rehash"),
        "matrixRehashSha256",
        name="matrix rehash",
    )
    if (
        len(tasks) != 256
        or rehash.get("exact") is not True
        or rehash.get("taskMatrixSha256") != canonical_sha256(tasks)
        or plan_manifest.get("taskMatrixSha256") != canonical_sha256(tasks)
    ):
        raise TemporalDiscoveryContractError("screening matrix admission failed")
    native = _identity(
        _read(base / "screening" / "native-validation.json", name="native validation"),
        "reportSha256",
        name="native validation",
    )
    if (
        native.get("allChecksPassed") is not True
        or native.get("evidencePlanValidationCount") != 256
        or native.get("candidateWindowJobValidationCount") != 256
        or native.get("taskMatrixSha256") != canonical_sha256(tasks)
    ):
        raise TemporalDiscoveryContractError("native 256-task validation is incomplete")
    process_state = capture_process_state()
    _write_immutable(base / "process-state.json", process_state)
    if workflow_conclusion != "success" or not workflow_run_id or not workflow_url:
        raise TemporalDiscoveryContractError("hosted workflow must be successful")
    if not _SHA40.fullmatch(autoresearch_evidence_commit):
        raise TemporalDiscoveryContractError("evidence commit must be exact")
    checkpoint = {
        "schemaVersion": CHECKPOINT_SCHEMA,
        "status": "screening_prelaunch_ready_awaiting_explicit_authorization",
        "autoresearchImplementationCommit": campaign["autoresearchImplementationCommit"],
        "autoresearchEvidenceCommit": autoresearch_evidence_commit,
        "fuzzfolioCommit": campaign["fuzzfolioCommit"],
        "workerContractSha256": campaign["workerContractSha256"],
        "campaignSpecSha256": campaign["campaignSpecSha256"],
        "coverageMetadataSha256": coverage["coverageMetadataSha256"],
        "windowSelectionSha256": selection["windowSelectionSha256"],
        "selectedWindows": selection["selectedWindows"],
        "dataAvailabilityCutoff": selection["dataAvailabilityCutoff"],
        "generator": generator,
        "generatorDeterminismReportSha256": determinism["reportSha256"],
        "managementWitnesses": witnesses,
        "screeningAuthorityId": authority["authorityId"],
        "screeningTaskMatrixSha256": canonical_sha256(tasks),
        "screeningTaskCount": len(tasks),
        "nativeValidationReportSha256": native["reportSha256"],
        "processStateSha256": process_state["processStateSha256"],
        "hostedWorkflow": {
            "runId": workflow_run_id,
            "url": workflow_url,
            "conclusion": workflow_conclusion,
        },
        "executionBoundary": {
            "marketBarsReadByWindowSelection": False,
            "gatewayStarted": False,
            "gatewayContacted": False,
            "screeningStarted": False,
            "confirmationAuthorityFrozen": False,
            "confirmationStarted": False,
            "reservedEvidenceAccessed": False,
            "largeSearchPermitted": False,
        },
        "nextPermittedOperation": (
            "review and explicitly authorize the immutable 256-task E/F Screening Fresh"
        ),
    }
    checkpoint["checkpointSha256"] = canonical_sha256(checkpoint)
    _write_immutable(base / "checkpoint.json", checkpoint)
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
    _write_immutable(base / "manifest.json", manifest)
    return {
        "schemaVersion": "temporal_search_stage5e3_prelaunch_result_v1",
        "status": checkpoint["status"],
        "checkpointSha256": checkpoint["checkpointSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "fileCount": manifest["fileCount"],
        "screeningTaskCount": len(tasks),
        "screeningStarted": False,
    }


def audit_prelaunch_checkpoint(root: Path | str) -> dict[str, Any]:
    base = Path(root)
    checkpoint = _identity(
        _read(base / "checkpoint.json", name="prelaunch checkpoint"),
        "checkpointSha256",
        name="prelaunch checkpoint",
    )
    manifest = _read(base / "manifest.json", name="prelaunch manifest")
    supplied = str(manifest.pop("manifestSha256", ""))
    if canonical_sha256(manifest) != supplied:
        raise TemporalDiscoveryContractError("prelaunch manifest identity mismatch")
    if manifest.get("checkpointSha256") != checkpoint["checkpointSha256"]:
        raise TemporalDiscoveryContractError("prelaunch checkpoint/manifest mismatch")
    expected = set()
    for item in manifest.get("files") or []:
        path = base / str(item["relativePath"])
        expected.add(path.resolve())
        if (
            not path.is_file()
            or path.stat().st_size != int(item["length"])
            or _file_sha(path) != item["sha256"]
        ):
            raise TemporalDiscoveryContractError(f"prelaunch file mismatch: {path}")
    actual = {
        path.resolve()
        for path in base.rglob("*")
        if path.is_file() and not (path.name == "manifest.json" and path.parent == base)
    }
    if actual != expected:
        raise TemporalDiscoveryContractError("prelaunch artifact inventory drift")
    return {
        "schemaVersion": "temporal_search_stage5e3_prelaunch_audit_v1",
        "ok": True,
        "status": checkpoint["status"],
        "checkpointSha256": checkpoint["checkpointSha256"],
        "manifestSha256": supplied,
        "fileCount": manifest["fileCount"],
        "screeningTaskCount": checkpoint["screeningTaskCount"],
        "screeningStarted": checkpoint["executionBoundary"]["screeningStarted"],
    }


__all__ = [
    "audit_prelaunch_checkpoint",
    "build_coverage_metadata",
    "build_window_selection",
    "capture_process_state",
    "fetch_coverage_metadata",
    "freeze_prelaunch_checkpoint",
    "freeze_window_selection",
    "prepare_screening_prelaunch",
]
