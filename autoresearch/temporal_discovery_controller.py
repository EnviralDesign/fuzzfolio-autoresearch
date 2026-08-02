from __future__ import annotations

from collections.abc import Mapping, Sequence
import copy
from datetime import datetime
import json
import math
import os
from pathlib import Path
import random
import re
import subprocess
import tempfile
from typing import Any, Protocol

from .temporal_search import (
    TEMPORAL_SEARCH_PREPARATION_SCHEMA,
    TemporalSearchContractError,
    build_authority,
    canonical_sha256,
    validate_authority,
)

from .temporal_discovery_base import *
from .temporal_discovery_validation import *
from .temporal_discovery_generation import *
from .temporal_discovery_results import *
from .temporal_discovery_artifacts import *

def _population_map(discovery_root: Path) -> dict[str, dict[str, Any]]:
    population = _read_json(
        discovery_root / "population.json",
        name="discovery population",
    )
    if population.get("schemaVersion") != TEMPORAL_DISCOVERY_POPULATION_SCHEMA:
        raise TemporalDiscoveryContractError("unknown population schema")
    candidates = population.get("candidates") or []
    if population.get("authoredValidationBindingRequired") is True:
        for candidate in candidates:
            if not isinstance(candidate, Mapping):
                raise TemporalDiscoveryContractError("discovery population candidate must be an object")
            validate_authored_validation_binding(candidate)
    return {
        item["candidateId"]: item
        for item in candidates
    }


def select_confirmation_stage(
    discovery_root: Path | str,
    *,
    initial_result_root: Path | str,
) -> dict[str, Any]:
    root = Path(discovery_root)
    preparation = _read_json(root / "preparation.json", name="preparation")
    normalized = _normalize_preparation(preparation)
    authority = _read_json(
        root / "discovery-authority.json",
        name="discovery authority",
    )
    if authority.get("schemaVersion") != TEMPORAL_DISCOVERY_AUTHORITY_SCHEMA:
        raise TemporalDiscoveryContractError(
            "unknown discovery authority schema"
        )
    population = _population_map(root)
    results = load_stage_results(initial_result_root)
    expected_windows = len(normalized["screening"]["initialWindowIds"])
    if set(results) != set(population):
        raise TemporalDiscoveryContractError(
            "initial results do not exactly cover generated population"
        )
    aggregates = []
    for candidate_id in sorted(population):
        windows = results[candidate_id]
        if len(windows) != expected_windows:
            raise TemporalDiscoveryContractError(
                f"candidate {candidate_id} has incomplete initial-window results"
            )
        aggregates.append(
            _aggregate_candidate(population[candidate_id], windows)
        )

    unique_aggregates, resolved_duplicates = _deduplicate_resolved_programs(
        aggregates
    )
    economic = select_economic_archive(
        unique_aggregates,
        archive_size=normalized["screening"]["economicArchiveSize"],
        minimum_trades_per_window=normalized["screening"][
            "minimumTradesPerInitialWindowEconomic"
        ],
    )
    novelty = select_novelty_archive(
        unique_aggregates,
        archive_size=normalized["screening"]["noveltyArchiveSize"],
        minimum_total_trades=normalized["screening"][
            "minimumTotalTradesNovelty"
        ],
    )
    confirmation_ids = _confirmation_union(
        economic,
        novelty,
        cap=normalized["screening"]["confirmationCandidateCap"],
    )
    if not confirmation_ids:
        raise TemporalDiscoveryContractError(
            "screening produced no confirmation candidates"
        )
    selected_candidates = [
        population[candidate_id] for candidate_id in confirmation_ids
    ]
    confirmation_preparation = _finite_preparation(
        normalized,
        candidates=selected_candidates,
        window_ids=normalized["screening"]["confirmationWindowIds"],
        label_suffix="confirmation",
        max_tasks=normalized["bounds"]["maxConfirmationTasks"],
    )
    confirmation_authority = build_authority(confirmation_preparation)
    selection = {
        "schemaVersion": TEMPORAL_DISCOVERY_INITIAL_SELECTION_SCHEMA,
        "discoveryAuthorityId": authority["authorityId"],
        "initialResultSetSha256": _result_set_sha256(results),
        "candidateCount": len(aggregates),
        "resolvedUniqueProgramCount": len(unique_aggregates),
        "resolvedProgramDuplicates": resolved_duplicates,
        "economicArchive": [
            {
                "candidateId": item["candidateId"],
                "economicRank": item["economicRank"],
                "paretoFront": item["paretoFront"],
                "totalTrades": item["totalTrades"],
                "totalConservativeNetR": item["totalConservativeNetR"],
                "worstWindowConservativeNetR": item[
                    "worstWindowConservativeNetR"
                ],
                "maxWindowDrawdownR": item["maxWindowDrawdownR"],
                "costDragR": item["costDragR"],
            }
            for item in economic
        ],
        "noveltyArchive": [
            {
                "candidateId": item["candidateId"],
                "noveltyRank": item["noveltyRank"],
                "minimumArchiveDistance": item["minimumArchiveDistance"],
                "fingerprintSha256": item["fingerprintSha256"],
                "totalTrades": item["totalTrades"],
            }
            for item in novelty
        ],
        "confirmationCandidateIds": confirmation_ids,
        "confirmationCandidateCount": len(confirmation_ids),
        "confirmationAuthorityId": confirmation_authority["authorityId"],
        "aggregateSha256": canonical_sha256(aggregates),
    }
    selection["selectionSha256"] = canonical_sha256(selection)
    _write_immutable(root / "screening" / "initial-selection.json", selection)
    _write_immutable(
        root / "screening" / "initial-aggregates.json",
        {
            "schemaVersion": "temporal_graph_discovery_initial_aggregates_v1",
            "aggregates": aggregates,
            "resolvedUniqueAggregates": unique_aggregates,
            "resolvedProgramDuplicates": resolved_duplicates,
            "aggregatesSha256": canonical_sha256(aggregates),
        },
    )
    _write_immutable(
        root / "confirmation" / "preparation.json",
        confirmation_preparation,
    )
    _write_immutable(
        root / "confirmation" / "authority.json",
        confirmation_authority,
    )
    manifest = _refresh_manifest(root, authority["authorityId"])
    return {
        "schemaVersion": "temporal_graph_discovery_confirmation_freeze_result_v1",
        "discoveryAuthorityId": authority["authorityId"],
        "selectionSha256": selection["selectionSha256"],
        "confirmationCandidateCount": len(confirmation_ids),
        "confirmationTaskCount": (
            len(confirmation_ids)
            * len(normalized["screening"]["confirmationWindowIds"])
        ),
        "confirmationAuthorityId": confirmation_authority["authorityId"],
        "manifestSha256": manifest["manifestSha256"],
    }


def finalize_discovery(
    discovery_root: Path | str,
    *,
    initial_result_root: Path | str,
    confirmation_result_root: Path | str,
) -> dict[str, Any]:
    root = Path(discovery_root)
    normalized = _normalize_preparation(
        _read_json(root / "preparation.json", name="preparation")
    )
    authority = _read_json(
        root / "discovery-authority.json",
        name="discovery authority",
    )
    selection = _read_json(
        root / "screening" / "initial-selection.json",
        name="initial selection",
    )
    population = _population_map(root)
    initial = load_stage_results(initial_result_root)
    confirmation = load_stage_results(confirmation_result_root)
    selected_ids = list(selection["confirmationCandidateIds"])
    if set(confirmation) != set(selected_ids):
        raise TemporalDiscoveryContractError(
            "confirmation results do not exactly cover frozen survivor set"
        )
    expected_initial = len(normalized["screening"]["initialWindowIds"])
    expected_confirmation = len(
        normalized["screening"]["confirmationWindowIds"]
    )
    aggregates: list[dict[str, Any]] = []
    for candidate_id in selected_ids:
        initial_windows = initial.get(candidate_id)
        confirmation_windows = confirmation.get(candidate_id)
        if (
            initial_windows is None
            or len(initial_windows) != expected_initial
            or confirmation_windows is None
            or len(confirmation_windows) != expected_confirmation
        ):
            raise TemporalDiscoveryContractError(
                f"candidate {candidate_id} lacks complete progressive evidence"
            )
        aggregates.append(
            _aggregate_candidate(
                population[candidate_id],
                [*initial_windows, *confirmation_windows],
            )
        )
    economic = select_economic_archive(
        aggregates,
        archive_size=normalized["screening"]["finalEconomicArchiveSize"],
        minimum_trades_per_window=normalized["screening"][
            "minimumTradesPerInitialWindowEconomic"
        ],
    )
    novelty = select_novelty_archive(
        aggregates,
        archive_size=normalized["screening"]["finalNoveltyArchiveSize"],
        minimum_total_trades=normalized["screening"][
            "minimumTotalTradesNovelty"
        ],
    )
    funnel = {
        "proposalCount": _read_json(
            root / "generation-journal.json",
            name="generation journal",
        )["proposalCount"],
        "validUniqueProgramCount": len(population),
        "initialTaskCount": len(population) * expected_initial,
        "economicInitialArchiveCount": len(
            selection.get("economicArchive") or []
        ),
        "noveltyInitialArchiveCount": len(
            selection.get("noveltyArchive") or []
        ),
        "confirmationCandidateCount": len(selected_ids),
        "confirmationTaskCount": len(selected_ids) * expected_confirmation,
        "completeFourWindowCandidateCount": len(aggregates),
    }
    report = {
        "schemaVersion": TEMPORAL_DISCOVERY_FINAL_REPORT_SCHEMA,
        "discoveryAuthorityId": authority["authorityId"],
        "initialAuthorityId": authority["initialAuthorityId"],
        "confirmationAuthorityId": selection["confirmationAuthorityId"],
        "initialResultSetSha256": _result_set_sha256(initial),
        "confirmationResultSetSha256": _result_set_sha256(confirmation),
        "funnel": funnel,
        "economicArchive": [
            {
                "candidateId": item["candidateId"],
                "economicRank": item["economicRank"],
                "paretoFront": item["paretoFront"],
                "totalTrades": item["totalTrades"],
                "totalConservativeNetR": item["totalConservativeNetR"],
                "totalNoCostNetR": item["totalNoCostNetR"],
                "worstWindowConservativeNetR": item[
                    "worstWindowConservativeNetR"
                ],
                "profitableWindowCount": item["profitableWindowCount"],
                "maxWindowDrawdownR": item["maxWindowDrawdownR"],
                "costDragR": item["costDragR"],
                "sourceMode": item["sourceMode"],
                "seedId": item["seedId"],
            }
            for item in economic
        ],
        "noveltyArchive": [
            {
                "candidateId": item["candidateId"],
                "noveltyRank": item["noveltyRank"],
                "minimumArchiveDistance": item["minimumArchiveDistance"],
                "fingerprintSha256": item["fingerprintSha256"],
                "totalTrades": item["totalTrades"],
                "totalConservativeNetR": item["totalConservativeNetR"],
                "sourceMode": item["sourceMode"],
                "seedId": item["seedId"],
            }
            for item in novelty
        ],
        "candidateAggregates": aggregates,
        "interpretationBoundary": (
            "This bounded development pilot validates generation, screening, "
            "distribution, and evidence handling. It is not protected evidence "
            "and does not establish strategy profitability."
        ),
    }
    report["reportSha256"] = canonical_sha256(report)
    _write_immutable(root / "final" / "report.json", report)
    _write_immutable(
        root / "final" / "economic-archive.json",
        {
            "schemaVersion": "temporal_graph_discovery_economic_archive_v1",
            "discoveryAuthorityId": authority["authorityId"],
            "candidates": report["economicArchive"],
        },
    )
    _write_immutable(
        root / "final" / "novelty-archive.json",
        {
            "schemaVersion": "temporal_graph_discovery_novelty_archive_v1",
            "discoveryAuthorityId": authority["authorityId"],
            "candidates": report["noveltyArchive"],
        },
    )
    manifest = _refresh_manifest(root, authority["authorityId"])
    return {
        "schemaVersion": "temporal_graph_discovery_finalize_result_v1",
        "discoveryAuthorityId": authority["authorityId"],
        "reportSha256": report["reportSha256"],
        "completeCandidateCount": len(aggregates),
        "economicArchiveCount": len(economic),
        "noveltyArchiveCount": len(novelty),
        "manifestSha256": manifest["manifestSha256"],
    }



def audit_discovery(root: Path | str) -> dict[str, Any]:
    directory = Path(root)
    authority = _read_json(
        directory / "discovery-authority.json",
        name="discovery authority",
    )
    supplied_authority_id = _sha(
        authority.pop("authorityId", None),
        name="discovery authorityId",
    )
    if canonical_sha256(authority) != supplied_authority_id:
        raise TemporalDiscoveryContractError(
            "discovery authority identity mismatch"
        )
    authority["authorityId"] = supplied_authority_id
    population = _read_json(
        directory / "population.json",
        name="population",
    )
    supplied_population = _sha(
        population.pop("populationSha256", None),
        name="populationSha256",
    )
    if canonical_sha256(population) != supplied_population:
        raise TemporalDiscoveryContractError(
            "population identity mismatch"
        )
    if supplied_population != authority["populationSha256"]:
        raise TemporalDiscoveryContractError(
            "authority and population identities differ"
        )
    initial_authority = validate_authority(
        _read_json(
            directory / "initial" / "authority.json",
            name="initial authority",
        )
    )
    if initial_authority["authorityId"] != authority["initialAuthorityId"]:
        raise TemporalDiscoveryContractError(
            "initial authority identity mismatch"
        )
    manifest = _read_json(
        directory / "manifest.json",
        name="discovery manifest",
    )
    supplied_manifest = _sha(
        manifest.pop("manifestSha256", None),
        name="manifestSha256",
    )
    if canonical_sha256(manifest) != supplied_manifest:
        raise TemporalDiscoveryContractError(
            "manifest identity mismatch"
        )
    for entry in manifest.get("files") or []:
        path = directory / entry["relativePath"]
        if not path.exists():
            raise TemporalDiscoveryContractError(
                f"manifest file is missing: {entry['relativePath']}"
            )
        data = path.read_bytes()
        if (
            len(data) != int(entry["length"])
            or hashlib_sha256(data) != entry["sha256"]
        ):
            raise TemporalDiscoveryContractError(
                f"manifest file mismatch: {entry['relativePath']}"
            )
    confirmation_path = directory / "confirmation" / "authority.json"
    confirmation_authority_id = None
    if confirmation_path.exists():
        confirmation = validate_authority(
            _read_json(
                confirmation_path,
                name="confirmation authority",
            )
        )
        confirmation_authority_id = confirmation["authorityId"]
    return {
        "schemaVersion": "temporal_graph_discovery_audit_v1",
        "ok": True,
        "authorityId": supplied_authority_id,
        "populationSha256": supplied_population,
        "manifestSha256": supplied_manifest,
        "initialAuthorityId": initial_authority["authorityId"],
        "confirmationAuthorityId": confirmation_authority_id,
        "fileCount": manifest["fileCount"],
    }


__all__ = [
    "CandidateValidatorProtocol",
    "SubprocessCandidateValidator",
    "TEMPORAL_DISCOVERY_AUTHORITY_SCHEMA",
    "TEMPORAL_DISCOVERY_FINAL_REPORT_SCHEMA",
    "TEMPORAL_DISCOVERY_GENERATOR_VERSION",
    "TEMPORAL_DISCOVERY_INITIAL_SELECTION_SCHEMA",
    "TEMPORAL_DISCOVERY_MANIFEST_SCHEMA",
    "TEMPORAL_DISCOVERY_POPULATION_SCHEMA",
    "TEMPORAL_DISCOVERY_PREPARATION_SCHEMA",
    "TEMPORAL_DISCOVERY_SELECTION_VERSION",
    "TemporalDiscoveryContractError",
    "TemporalDiscoveryError",
    "TemporalDiscoveryGenerationExhausted",
    "audit_discovery",
    "fingerprint_distance",
    "finalize_discovery",
    "generate_discovery",
    "load_stage_results",
    "pareto_fronts",
    "select_confirmation_stage",
    "select_economic_archive",
    "select_novelty_archive",
]


__all__ = ['_population_map', 'select_confirmation_stage', 'finalize_discovery', '_refresh_manifest', 'hashlib_sha256', 'audit_discovery', '__all__']
