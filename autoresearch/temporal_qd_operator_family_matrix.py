"""Frozen-parent × operator-family matrix experiment contract.

This is research-harness scheduling only. It does not change production
rotating 4/5 breeding, archive gates, or family weights.

A matrix generation finishes after every construction slot is attempted
once. Rejects and no-ops stay on their slot; publication scores whatever
children exist. Empty slots are a scientific result, not a broken run.

Clone controls are parent genomes re-evaluated on the same frozen panel as
the one-change children. They are not admitted as new pair genomes.
Crossover is out of scope for this contract (`includeCrossover` must be false).
"""

from __future__ import annotations

import argparse
import json
import statistics
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_qd_generation_quality_audit import (
    _generation_root,
    _read_jsonl,
    _read_observational_object,
    _support_metrics,
    _window_metrics_from_evaluated,
)
from .temporal_qd_pair_generation import (
    PAIR_GENERATION_SCHEMA,
    REPRODUCTION_ALLOCATION_SCHEMA_ACCEPTED,
    _frozen_reproduction_allocation,
)

MATRIX_SCHEMA = "temporal_qd_operator_family_matrix_v1"
MATRIX_MODE = "frozen_parent_one_change_v1"
CLONE_CONTROL = "re_evaluate_parent_on_frozen_panel"
QUALIFICATION_REPORT_SCHEMA = "temporal_qd_operator_family_matrix_qualification_v1"
FAST_EPHEMERAL_COMPLETE_SCHEMA = "temporal_qd_v5_fast_ephemeral_complete_v1"
MATRIX_FAMILIES = (
    "hold",
    "resource",
    "topology",
    "temporal",
    "initial_protection",
)
PARENT_ROLES = ("archive", "inactive_control", "active_negative_control")
DEFAULT_CHILDREN_PER_FAMILY = 32
DEFAULT_MUTATION_DEPTH = 1


def is_matrix_family(family: str) -> bool:
    return family in MATRIX_FAMILIES


def operator_family_matrix_from_config(
    config: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(config, Mapping):
        return None
    matrix = config.get("operatorFamilyMatrix")
    if matrix is None:
        return None
    return validate_operator_family_matrix(matrix)


def require_exhausted_slot_grid(
    *,
    contract: Mapping[str, Any],
    attempt_count: int,
    accepted_count: int,
    requested_count: int,
    max_attempts: int,
) -> None:
    """A matrix run is complete after every slot is attempted once."""

    slots = construction_slot_count(contract)
    if requested_count != slots or max_attempts != slots:
        raise TemporalDiscoveryContractError(
            "operator-family matrix must bind requested count and max attempts to the slot grid"
        )
    if attempt_count != slots:
        raise TemporalDiscoveryContractError(
            "operator-family matrix must attempt every construction slot before publication"
        )
    if accepted_count > slots:
        raise TemporalDiscoveryContractError(
            "operator-family matrix accepted more children than construction slots"
        )


def evolved_fill_matches_manifest(
    *,
    generation_config: Mapping[str, Any],
    requested_count: int,
    max_attempts: int,
    declared_evaluation_population_size: int,
    accepted_count: int,
    attempt_count: int,
    evaluation_population_size: int,
) -> bool:
    """Return whether result/receipt fill is valid for this generation.

    Production still requires exact-width accepts. A matrix generation is
    complete after every slot is attempted; published evaluation size is the
    accepted-child count.
    """

    matrix = operator_family_matrix_from_config(generation_config)
    if matrix is None:
        return (
            accepted_count == requested_count
            and evaluation_population_size == declared_evaluation_population_size
            and requested_count <= attempt_count <= max_attempts
        )
    try:
        require_exhausted_slot_grid(
            contract=matrix,
            attempt_count=attempt_count,
            accepted_count=accepted_count,
            requested_count=requested_count,
            max_attempts=max_attempts,
        )
    except TemporalDiscoveryContractError:
        return False
    return (
        evaluation_population_size == accepted_count
        and evaluation_population_size <= declared_evaluation_population_size
    )


def validate_operator_family_matrix(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return a canonical copy of a matrix contract."""

    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError("operator-family matrix must be an object")
    if value.get("schemaVersion") != MATRIX_SCHEMA:
        raise TemporalDiscoveryContractError(
            "operator-family matrix schema is incompatible"
        )
    if value.get("mode") != MATRIX_MODE:
        raise TemporalDiscoveryContractError("operator-family matrix mode is incompatible")
    if value.get("includeCrossover") is not False:
        raise TemporalDiscoveryContractError(
            "operator-family matrix must keep crossover on a separate lane"
        )
    if value.get("cloneControl") != CLONE_CONTROL:
        raise TemporalDiscoveryContractError(
            "operator-family matrix clone control must re-evaluate parents on the frozen panel"
        )
    mutation_depth = value.get("mutationDepth")
    if mutation_depth != DEFAULT_MUTATION_DEPTH:
        raise TemporalDiscoveryContractError(
            "operator-family matrix mutation depth must be exactly 1"
        )
    children = value.get("childrenPerFamily")
    if (
        not isinstance(children, int)
        or isinstance(children, bool)
        or children < 1
    ):
        raise TemporalDiscoveryContractError(
            "operator-family matrix childrenPerFamily must be a positive integer"
        )
    families = value.get("families")
    if not isinstance(families, list) or not families:
        raise TemporalDiscoveryContractError(
            "operator-family matrix families must be a nonempty list"
        )
    if any(family not in MATRIX_FAMILIES for family in families):
        raise TemporalDiscoveryContractError(
            "operator-family matrix families contain an unsupported family"
        )
    if len(set(families)) != len(families):
        raise TemporalDiscoveryContractError(
            "operator-family matrix families must be unique"
        )
    parents = value.get("parents")
    if not isinstance(parents, list) or not parents:
        raise TemporalDiscoveryContractError(
            "operator-family matrix parents must be a nonempty list"
        )
    seen_ids: set[str] = set()
    canonical_parents: list[dict[str, Any]] = []
    for parent in parents:
        if not isinstance(parent, Mapping):
            raise TemporalDiscoveryContractError(
                "operator-family matrix parent must be an object"
            )
        candidate_id = parent.get("candidateId")
        role = parent.get("role")
        if not isinstance(candidate_id, str) or not candidate_id.strip():
            raise TemporalDiscoveryContractError(
                "operator-family matrix parent candidateId is invalid"
            )
        if role not in PARENT_ROLES:
            raise TemporalDiscoveryContractError(
                "operator-family matrix parent role is invalid"
            )
        if candidate_id in seen_ids:
            raise TemporalDiscoveryContractError(
                f"operator-family matrix repeats parent {candidate_id}"
            )
        seen_ids.add(candidate_id)
        canonical_parents.append({"candidateId": candidate_id, "role": role})
    roles = {parent["role"] for parent in canonical_parents}
    if "archive" not in roles:
        raise TemporalDiscoveryContractError(
            "operator-family matrix requires at least one archive parent"
        )
    contract = {
        "schemaVersion": MATRIX_SCHEMA,
        "mode": MATRIX_MODE,
        "includeCrossover": False,
        "cloneControl": CLONE_CONTROL,
        "mutationDepth": DEFAULT_MUTATION_DEPTH,
        "childrenPerFamily": int(children),
        "families": list(families),
        "parents": canonical_parents,
    }
    contract["constructionSlotCount"] = construction_slot_count(contract)
    contract["cloneParentCandidateIds"] = [
        parent["candidateId"] for parent in canonical_parents
    ]
    return contract


def construction_slot_count(contract: Mapping[str, Any]) -> int:
    parents = contract["parents"]
    families = contract["families"]
    children = int(contract["childrenPerFamily"])
    return len(parents) * len(families) * children


def slot_at(contract: Mapping[str, Any], ordinal: int) -> dict[str, Any] | None:
    """Map a proposal ordinal onto one declared one-change construction slot."""

    if not isinstance(ordinal, int) or isinstance(ordinal, bool) or ordinal < 0:
        raise TemporalDiscoveryContractError(
            "operator-family matrix ordinal must be a nonnegative integer"
        )
    parents = contract["parents"]
    families = contract["families"]
    children = int(contract["childrenPerFamily"])
    total = construction_slot_count(contract)
    if ordinal >= total:
        return None
    per_parent = len(families) * children
    parent_index, parent_offset = divmod(ordinal, per_parent)
    family_index, child_index = divmod(parent_offset, children)
    parent = parents[parent_index]
    return {
        "proposalOrdinal": ordinal,
        "kind": "one_change",
        "parentCandidateId": parent["candidateId"],
        "parentRole": parent["role"],
        "operatorFamily": families[family_index],
        "childIndex": child_index,
        "mutationDepth": DEFAULT_MUTATION_DEPTH,
    }


def iter_construction_slots(contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        slot
        for ordinal in range(construction_slot_count(contract))
        if (slot := slot_at(contract, ordinal)) is not None
    ]


def experiment_reproduction_allocation(*, target_accepted: int) -> dict[str, Any]:
    """Zero-immigrant accepted quota for matrix construction slots only."""

    return _frozen_reproduction_allocation(
        parent_schedule=None,
        target_unique_candidates=int(target_accepted),
        has_supported_parents=True,
        accepted_terminology=True,
        desired_offspring_count=int(target_accepted),
        desired_immigrant_count=0,
        minimum_immigrant_numerator=0,
        minimum_immigrant_denominator=1,
    )


def attach_operator_family_matrix(
    generation_config: Mapping[str, Any],
    matrix: Mapping[str, Any],
) -> dict[str, Any]:
    """Seal a generation config overlay. Production configs must omit this field."""

    if generation_config.get("schemaVersion") != PAIR_GENERATION_SCHEMA:
        raise TemporalDiscoveryContractError(
            "operator-family matrix overlay requires pair generation v2"
        )
    contract = validate_operator_family_matrix(matrix)
    slot_count = construction_slot_count(contract)
    config = {
        key: value
        for key, value in generation_config.items()
        if key
        not in {
            "configSha256",
            "parentSchedule",
            "breedingConfidencePolicy",
            "breedingConfidenceReceipt",
            "operatorFamilyMatrix",
            "reproductionAllocation",
        }
    }
    config["targetUniqueCandidates"] = slot_count
    config["maxProposalAttempts"] = slot_count
    config["mutationDepthProbabilities"] = {"1": 1.0}
    config["reproductionAllocation"] = experiment_reproduction_allocation(
        target_accepted=slot_count
    )
    config["operatorFamilyMatrix"] = contract
    if config.get("reproductionAllocation", {}).get("schemaVersion") != (
        REPRODUCTION_ALLOCATION_SCHEMA_ACCEPTED
    ):
        raise TemporalDiscoveryContractError(
            "operator-family matrix overlay must use accepted reproduction allocation v2"
        )
    config["configSha256"] = canonical_sha256(config)
    return config


def _archive_member_ids(archive: Mapping[str, Any]) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for cell in archive.get("cells") or []:
        if not isinstance(cell, Mapping):
            continue
        for member in cell.get("members") or []:
            if not isinstance(member, Mapping):
                continue
            candidate_id = member.get("candidateId")
            if isinstance(candidate_id, str) and candidate_id and candidate_id not in seen:
                seen.add(candidate_id)
                ids.append(candidate_id)
    return ids


def _evaluated_activity(row: Mapping[str, Any]) -> tuple[int, float, float]:
    windows = _window_metrics_from_evaluated(row)
    metrics = _support_metrics(windows, covered_months=max(len(windows), 1))
    trades = int(round(sum(float(item.get("closedTrades") or 0.0) for item in windows)))
    worst = min((float(item.get("conservativeNetR") or 0.0) for item in windows), default=0.0)
    return trades, float(metrics["cumulativeConservativeNetR"]), worst


def _pick_control_parent(
    evaluated: Sequence[Mapping[str, Any]],
    *,
    excluded: set[str],
    inactive: bool,
) -> str | None:
    ranked: list[tuple[str, int, float]] = []
    for row in evaluated:
        candidate_id = row.get("candidateId")
        if not isinstance(candidate_id, str) or candidate_id in excluded:
            continue
        trades, net_r, _worst = _evaluated_activity(row)
        if inactive:
            if trades != 0:
                continue
        elif trades <= 0 or net_r >= 0:
            continue
        ranked.append((candidate_id, trades, net_r))
    ranked.sort(key=lambda item: item[0])
    return ranked[0][0] if ranked else None


def freeze_operator_family_matrix_from_run(
    run_root: Path,
    *,
    source_generation: int,
    children_per_family: int = DEFAULT_CHILDREN_PER_FAMILY,
    families: Sequence[str] = MATRIX_FAMILIES,
    inactive_parent: str | None = None,
    active_negative_parent: str | None = None,
) -> dict[str, Any]:
    """Pin G-archive parents plus one inactive and one active-negative control."""

    if source_generation < 1:
        raise TemporalDiscoveryContractError(
            "operator-family matrix source generation must be positive"
        )
    generation_root = _generation_root(run_root, source_generation)
    archive_path = generation_root / "native-finalization" / "archive.json"
    evaluated_candidates = [
        generation_root
        / "campaign"
        / "proposal-current-panel"
        / "campaign-output"
        / "evaluated-members.jsonl",
        generation_root / "prefinalizer" / "evaluated-members.jsonl",
        generation_root / "proposal" / "evaluated-members.jsonl",
    ]
    evaluated_path = next((path for path in evaluated_candidates if path.is_file()), None)
    if evaluated_path is None:
        raise TemporalDiscoveryContractError(
            f"generation {source_generation} evaluated members are missing"
        )
    archive = _read_observational_object(archive_path, name="source archive")
    archive_ids = _archive_member_ids(archive)
    if not archive_ids:
        raise TemporalDiscoveryContractError(
            f"generation {source_generation} archive has no members to freeze"
        )
    evaluated = _read_jsonl(evaluated_path, name="source evaluated members")
    excluded = set(archive_ids)
    inactive_id = inactive_parent or _pick_control_parent(
        evaluated, excluded=excluded, inactive=True
    )
    if inactive_id is None:
        raise TemporalDiscoveryContractError(
            "could not auto-pick an inactive control parent; pass --inactive-parent"
        )
    excluded.add(inactive_id)
    active_id = active_negative_parent or _pick_control_parent(
        evaluated, excluded=excluded, inactive=False
    )
    if active_id is None:
        raise TemporalDiscoveryContractError(
            "could not auto-pick an active-negative control parent; pass --active-negative-parent"
        )
    parents = (
        [{"candidateId": candidate_id, "role": "archive"} for candidate_id in archive_ids]
        + [
            {"candidateId": inactive_id, "role": "inactive_control"},
            {"candidateId": active_id, "role": "active_negative_control"},
        ]
    )
    contract = validate_operator_family_matrix(
        {
            "schemaVersion": MATRIX_SCHEMA,
            "mode": MATRIX_MODE,
            "includeCrossover": False,
            "cloneControl": CLONE_CONTROL,
            "mutationDepth": DEFAULT_MUTATION_DEPTH,
            "childrenPerFamily": int(children_per_family),
            "families": list(families),
            "parents": parents,
        }
    )
    contract["sourceRunRoot"] = str(Path(run_root).resolve())
    contract["sourceGenerationIndex"] = int(source_generation)
    return contract


def freeze_spec_source_pin(matrix: Mapping[str, Any]) -> tuple[Path, int]:
    """Read the freeze-spec source run pin without hashing the identity ledger."""

    source_root = matrix.get("sourceRunRoot")
    source_generation = matrix.get("sourceGenerationIndex")
    if not isinstance(source_root, str) or not source_root.strip():
        raise TemporalDiscoveryContractError(
            "operator-family matrix freeze-spec lacks sourceRunRoot"
        )
    if (
        not isinstance(source_generation, int)
        or isinstance(source_generation, bool)
        or source_generation < 1
    ):
        raise TemporalDiscoveryContractError(
            "operator-family matrix freeze-spec lacks sourceGenerationIndex"
        )
    return Path(source_root).resolve(), int(source_generation)


def matrix_source_identity_ledger_descriptor(
    matrix: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind the source generation's sealed identity ledger without rehashing it."""

    source_root, source_generation = freeze_spec_source_pin(matrix)
    complete_path = (
        source_root
        / "generations"
        / f"generation-{source_generation:04d}"
        / "proposal"
        / "COMPLETE.json"
    )
    try:
        complete = json.loads(complete_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(
            "operator-family matrix source generation complete sidecar is unavailable"
        ) from exc
    if not isinstance(complete, Mapping):
        raise TemporalDiscoveryContractError(
            "operator-family matrix source generation complete sidecar is invalid"
        )
    if (
        complete.get("schemaVersion") != FAST_EPHEMERAL_COMPLETE_SCHEMA
        or complete.get("generationIndex") != source_generation
        or complete.get("executionMode") != "fast-ephemeral-v1"
    ):
        raise TemporalDiscoveryContractError(
            "operator-family matrix source identity ledger complete sidecar drifted"
        )
    artifacts = complete.get("artifacts")
    ledger = artifacts.get("identityLedger") if isinstance(artifacts, Mapping) else None
    if not isinstance(ledger, Mapping):
        raise TemporalDiscoveryContractError(
            "operator-family matrix source complete sidecar lacks identityLedger"
        )
    relative = ledger.get("relativePath")
    if relative != "identity-ledger.json":
        raise TemporalDiscoveryContractError(
            "operator-family matrix source identity ledger relative path drifted"
        )
    absolute = (complete_path.parent / relative).resolve()
    byte_length = ledger.get("byteLength")
    semantic = ledger.get("semanticSha256")
    file_sha = ledger.get("fileSha256")
    if (
        not absolute.is_file()
        or not isinstance(byte_length, int)
        or isinstance(byte_length, bool)
        or byte_length < 1
        or absolute.stat().st_size != byte_length
        or not isinstance(semantic, str)
        or not isinstance(file_sha, str)
    ):
        raise TemporalDiscoveryContractError(
            "operator-family matrix source identity ledger descriptor drifted"
        )
    return {
        "absolutePath": str(absolute),
        "semanticSha256": semantic,
        "fileSha256": file_sha,
        "byteLength": byte_length,
    }


def unique_evaluations_meet_plan(
    *,
    matrix: Mapping[str, Any] | None,
    unique_count: int,
    target_unique_evaluations: int,
) -> bool:
    """Exact-width production target, or an accepted-count deficit on a matrix run."""

    if (
        not isinstance(unique_count, int)
        or isinstance(unique_count, bool)
        or unique_count < 0
        or not isinstance(target_unique_evaluations, int)
        or isinstance(target_unique_evaluations, bool)
        or target_unique_evaluations < 1
    ):
        return False
    if matrix is None:
        return unique_count == target_unique_evaluations
    slot_count = construction_slot_count(validate_operator_family_matrix(matrix))
    return target_unique_evaluations == slot_count and unique_count <= slot_count


def _median(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return float(statistics.median(values))


def _mean(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _parent_relative_rows(
    children: Sequence[Mapping[str, Any]],
    parent_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    parent_net = parent_metrics.get("cumulativeConservativeNetR")
    parent_worst = parent_metrics.get("worstWindowConservativeNetR")
    deltas: list[float] = []
    worst_deltas: list[float] = []
    beat = 0
    worse_worst = 0
    for child in children:
        net = child.get("cumulativeConservativeNetR")
        worst = child.get("worstWindowConservativeNetR")
        if isinstance(net, (int, float)) and not isinstance(net, bool) and isinstance(
            parent_net, (int, float)
        ) and not isinstance(parent_net, bool):
            delta = float(net) - float(parent_net)
            deltas.append(delta)
            if delta > 0:
                beat += 1
        if isinstance(worst, (int, float)) and not isinstance(worst, bool) and isinstance(
            parent_worst, (int, float)
        ) and not isinstance(parent_worst, bool):
            worst_delta = float(worst) - float(parent_worst)
            worst_deltas.append(worst_delta)
            if worst_delta < 0:
                worse_worst += 1
    return {
        "comparisonCount": len(deltas),
        "meanParentRelativeConservativeNetR": _mean(deltas),
        "medianParentRelativeConservativeNetR": _median(deltas),
        "offspringBeatParentCount": beat,
        "offspringBeatParentRate": (beat / len(deltas)) if deltas else None,
        "meanParentRelativeWorstWindowConservativeNetR": _mean(worst_deltas),
        "medianParentRelativeWorstWindowConservativeNetR": _median(worst_deltas),
        "worseWorstWindowCount": worse_worst,
        "worseWorstWindowRate": (worse_worst / len(worst_deltas)) if worst_deltas else None,
    }


def _candidate_metrics(row: Mapping[str, Any]) -> dict[str, Any]:
    windows = _window_metrics_from_evaluated(row)
    support = _support_metrics(windows, covered_months=max(len(windows), 1))
    worst = min((float(item.get("conservativeNetR") or 0.0) for item in windows), default=None)
    return {
        "cumulativeConservativeNetR": support["cumulativeConservativeNetR"],
        "worstWindowConservativeNetR": worst,
        "medianWindowConservativeNetR": support["medianWindowConservativeNetR"],
        "activeWindowFraction": support["activeWindowFraction"],
    }


def score_operator_family_matrix(
    *,
    contract: Mapping[str, Any],
    evaluated: Mapping[str, Mapping[str, Any]],
    constructed_by_slot: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Same-panel parent-relative net R and worst-window risk by parent × family.

    `evaluated` must include clone/parent rows keyed by parent candidate id and
    children keyed by their constructed candidate ids. Slot rows may supply
    `candidateId` plus `operatorFamily` / `parentCandidateId` when construction
    telemetry is available.
    """

    matrix = validate_operator_family_matrix(contract)
    parent_baselines: dict[str, dict[str, Any]] = {}
    missing_clones: list[str] = []
    for parent in matrix["parents"]:
        parent_id = parent["candidateId"]
        row = evaluated.get(parent_id)
        if row is None:
            missing_clones.append(parent_id)
            continue
        parent_baselines[parent_id] = {
            "role": parent["role"],
            **_candidate_metrics(row),
        }

    children: list[dict[str, Any]] = []
    if constructed_by_slot:
        for slot in constructed_by_slot:
            candidate_id = slot.get("candidateId")
            parent_id = slot.get("parentCandidateId")
            family = slot.get("operatorFamily")
            if not isinstance(candidate_id, str) or candidate_id not in evaluated:
                continue
            if not isinstance(parent_id, str) or not isinstance(family, str):
                continue
            metrics = _candidate_metrics(evaluated[candidate_id])
            children.append(
                {
                    "candidateId": candidate_id,
                    "parentCandidateId": parent_id,
                    "operatorFamily": family,
                    **metrics,
                }
            )

    family_rows: list[dict[str, Any]] = []
    parent_family_rows: list[dict[str, Any]] = []
    for family in matrix["families"]:
        family_children = [row for row in children if row["operatorFamily"] == family]
        parent_relatives: list[dict[str, Any]] = []
        for parent in matrix["parents"]:
            parent_id = parent["candidateId"]
            subset = [
                row for row in family_children if row["parentCandidateId"] == parent_id
            ]
            baseline = parent_baselines.get(parent_id)
            relative = (
                _parent_relative_rows(subset, baseline)
                if baseline is not None
                else {
                    "comparisonCount": 0,
                    "meanParentRelativeConservativeNetR": None,
                    "medianParentRelativeConservativeNetR": None,
                    "offspringBeatParentCount": 0,
                    "offspringBeatParentRate": None,
                    "meanParentRelativeWorstWindowConservativeNetR": None,
                    "medianParentRelativeWorstWindowConservativeNetR": None,
                    "worseWorstWindowCount": 0,
                    "worseWorstWindowRate": None,
                }
            )
            parent_family_rows.append(
                {
                    "parentCandidateId": parent_id,
                    "parentRole": parent["role"],
                    "operatorFamily": family,
                    "evaluatedChildCount": len(subset),
                    "samePanelParentRelative": relative,
                }
            )
            parent_relatives.append(relative)
        nets = [
            float(row["cumulativeConservativeNetR"])
            for row in family_children
            if isinstance(row.get("cumulativeConservativeNetR"), (int, float))
            and not isinstance(row.get("cumulativeConservativeNetR"), bool)
        ]
        comparison_count = sum(int(item["comparisonCount"]) for item in parent_relatives)
        beat = sum(int(item["offspringBeatParentCount"]) for item in parent_relatives)
        median_relatives = [
            item["medianParentRelativeConservativeNetR"]
            for item in parent_relatives
            if item["medianParentRelativeConservativeNetR"] is not None
        ]
        median_worst = [
            item["medianParentRelativeWorstWindowConservativeNetR"]
            for item in parent_relatives
            if item["medianParentRelativeWorstWindowConservativeNetR"] is not None
        ]
        family_rows.append(
            {
                "operatorFamily": family,
                "evaluatedChildCount": len(family_children),
                "meanCurrentPanelConservativeNetR": _mean(nets),
                "medianCurrentPanelConservativeNetR": _median(nets),
                "samePanelParentRelative": {
                    "comparisonCount": comparison_count,
                    "medianParentRelativeConservativeNetR": _median(
                        [
                            float(value)
                            for value in median_relatives
                            if isinstance(value, (int, float))
                        ]
                    ),
                    "offspringBeatParentCount": beat,
                    "offspringBeatParentRate": (beat / comparison_count)
                    if comparison_count
                    else None,
                    "medianParentRelativeWorstWindowConservativeNetR": _median(
                        [
                            float(value)
                            for value in median_worst
                            if isinstance(value, (int, float))
                        ]
                    ),
                },
            }
        )

    passing_families = []
    for row in family_rows:
        relative = row["samePanelParentRelative"]
        median_delta = relative.get("medianParentRelativeConservativeNetR")
        median_worst = relative.get("medianParentRelativeWorstWindowConservativeNetR")
        beat_rate = relative.get("offspringBeatParentRate") or 0.0
        nonnegative_median = (
            isinstance(median_delta, (int, float)) and float(median_delta) >= 0
        )
        positive_tail = beat_rate > 0
        worse_worst = (
            isinstance(median_worst, (int, float)) and float(median_worst) < 0
        )
        if (nonnegative_median or positive_tail) and not worse_worst:
            passing_families.append(row["operatorFamily"])

    body: dict[str, Any] = {
        "schemaVersion": QUALIFICATION_REPORT_SCHEMA,
        "matrix": matrix,
        "cloneBaselines": parent_baselines,
        "missingCloneParentIds": missing_clones,
        "familyYield": family_rows,
        "parentFamilyYield": parent_family_rows,
        "passingFamilies": passing_families,
        "qualification": (
            "pass"
            if passing_families
            else "fail"
            if parent_baselines and children
            else "incomplete"
        ),
        "limitations": [
            "clone_control_is_parent_re_eval_on_frozen_panel",
            "crossover_is_out_of_scope",
            "one_declared_operator_family_per_child",
            "unfilled_slots_remain_on_their_declared_family",
        ],
    }
    body["reportSha256"] = canonical_sha256(body)
    return body


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    freeze = sub.add_parser("freeze-spec", help="Pin parents from a completed run")
    freeze.add_argument("--run-root", type=Path, required=True)
    freeze.add_argument("--source-generation", type=int, default=2)
    freeze.add_argument(
        "--children-per-family", type=int, default=DEFAULT_CHILDREN_PER_FAMILY
    )
    freeze.add_argument("--inactive-parent")
    freeze.add_argument("--active-negative-parent")
    freeze.add_argument("--output", type=Path, required=True)

    score = sub.add_parser("score", help="Score a completed matrix evaluation")
    score.add_argument("--matrix", type=Path, required=True)
    score.add_argument("--evaluated-jsonl", type=Path, required=True)
    score.add_argument("--slots-jsonl", type=Path)
    score.add_argument("--output", type=Path, required=True)

    args = parser.parse_args(argv)
    if args.command == "freeze-spec":
        contract = freeze_operator_family_matrix_from_run(
            args.run_root,
            source_generation=args.source_generation,
            children_per_family=args.children_per_family,
            inactive_parent=args.inactive_parent,
            active_negative_parent=args.active_negative_parent,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            canonical_json(contract) + "\n", encoding="utf-8", newline="\n"
        )
        print(args.output.resolve())
        return 0

    matrix = json.loads(Path(args.matrix).read_text(encoding="utf-8"))
    evaluated_rows = _read_jsonl(args.evaluated_jsonl, name="evaluated members")
    evaluated = {
        str(row["candidateId"]): row
        for row in evaluated_rows
        if isinstance(row.get("candidateId"), str)
    }
    slots = (
        _read_jsonl(args.slots_jsonl, name="matrix slots")
        if args.slots_jsonl is not None
        else None
    )
    report = score_operator_family_matrix(
        contract=matrix,
        evaluated=evaluated,
        constructed_by_slot=slots,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        canonical_json(report) + "\n", encoding="utf-8", newline="\n"
    )
    print(args.output.resolve())
    return 0


__all__ = [
    "CLONE_CONTROL",
    "DEFAULT_CHILDREN_PER_FAMILY",
    "MATRIX_FAMILIES",
    "MATRIX_MODE",
    "MATRIX_SCHEMA",
    "QUALIFICATION_REPORT_SCHEMA",
    "attach_operator_family_matrix",
    "construction_slot_count",
    "experiment_reproduction_allocation",
    "freeze_operator_family_matrix_from_run",
    "is_matrix_family",
    "iter_construction_slots",
    "main",
    "operator_family_matrix_from_config",
    "require_exhausted_slot_grid",
    "evolved_fill_matches_manifest",
    "freeze_spec_source_pin",
    "matrix_source_identity_ledger_descriptor",
    "score_operator_family_matrix",
    "slot_at",
    "unique_evaluations_meet_plan",
    "validate_operator_family_matrix",
]
