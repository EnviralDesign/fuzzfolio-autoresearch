"""Small, immutable Stage 5E7-v3 repair-validation panels.

This module is intentionally a *panel builder*, not another search controller.
It turns a frozen gen4 population/results set into a tagged reference panel,
creates finite one-operator causal siblings, and preregisters a v2-like versus
v3 policy pilot.  It never reserves evidence, starts a worker, or writes below
the repository.  The only optional subprocess is the already-owned static
profile validator needed to give a child its canonical program identity.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import statistics
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from statistics import NormalDist
from typing import Any, Protocol

from .strategy_identity import derive_strategy_identity
from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    _clone,
    _read_json,
    _write_immutable,
    canonical_sha256,
)
from .temporal_discovery_results import (
    _aggregate_candidate,
    _result_files,
    _result_set_sha256,
    load_stage_results,
)
from .temporal_operator_confirmed_entry import ConfirmedEntryStructuralOperator
from .temporal_operator_construction_v3 import (
    GeneratorV3ConstructionRegistry,
    inspect_construction_reachability,
)
from .temporal_operator_expansion import expanded_structural_operators
from .temporal_qd_evolution import (
    QD_POLICY,
    QD_POLICY_NAME,
    QD_POLICY_SHA256,
    QD_ARCHIVE_SCHEMA,
    QD_POPULATION_SCHEMA,
    _finite_data_validity,
    _objective_row,
    _load_population,
    qd_behavior_descriptor,
    select_qd_archive,
)
from .temporal_structural_operators import build_candidate_lineage
from .temporal_search import (
    TEMPORAL_SEARCH_MANIFEST_SCHEMA,
    TemporalSearchContractError,
    build_authority,
    build_task_matrix,
    validate_authority,
)


HARNESS_VERSION = "stage5e7-v3-validation-v1"
PROHIBITED_INTERVAL_START = "2024-06-29T00:00:00Z"
REFERENCE_PANEL_SCHEMA = "stage5e7_v3_tagged_reference_panel_v1"
REFERENCE_POPULATION_SCHEMA = "stage5e7_v3_reference_population_v1"
OPERATOR_PANEL_SCHEMA = "stage5e7_v3_operator_causal_panel_v1"
POLICY_AB_SCHEMA = "stage5e7_v3_policy_ab_preregistration_v2"
COMPONENT_MANIFEST_SCHEMA = "stage5e7_v3_validation_component_manifest_v1"
SAFE_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")
NAMED_REFERENCES = ("qd_538", "qd_390", "qd_339", "qd_9db", "qd_de455")

# The repair panel may read this one predecessor only as immutable provenance.
# These values intentionally do not extend the v3 QD archive/continuation
# contract in temporal_qd_evolution: that loader continues to accept v3 only.
FROZEN_STAGE5E7_V2_ARCHIVE_SCHEMA = "temporal_qd_archive_v2"
FROZEN_STAGE5E7_V2_QD_VERSION = "temporal_qd_evolution_v2"
FROZEN_STAGE5E7_V2_ARCHIVE_SHA256 = "sha256:affff47597c2acfa3f42ea8d45ee777d5b68c186cec14c978f83489df3083faf"
FROZEN_STAGE5E7_V2_POPULATION_SCHEMA = "temporal_qd_generation_population_v2"
FROZEN_STAGE5E7_V2_POPULATION_SHA256 = "sha256:38fcb3cde9828887d5f92ee91081548388a7362c993f99e1bf0ae29d10c96e86"

# The 64 slots deliberately make the required coverage mechanically auditable.
# These are coverage quotas, never performance ranks or promotion rights.
REFERENCE_QUOTAS = {
    # The identity-verified frozen union has four, not seven, candidates that
    # are both positive and explicitly resolved at the observation boundary.  Preserve every
    # available resolved case and keep the fixed 64-row panel by assigning the
    # remaining positive-evidence capacity to the explicitly unresolved peer
    # stratum; no unresolved state is manufactured or hidden.
    "both_positive_resolved": 4,
    "both_positive_unresolved": 10,
    "positive_single_trade_concentrated": 5,
    "high_support_negative": 5,
    "high_turnover": 4,
    "sparse_long_hold": 4,
    "short_direction": 4,
    "representative_origin": 4,
    "representative_family": 4,
    "representative_descriptor_cell": 4,
    "flat_negative_control": 11,
}
assert len(NAMED_REFERENCES) + sum(REFERENCE_QUOTAS.values()) == 64


class CandidateValidator(Protocol):
    def validate(
        self,
        *,
        candidate_id: str,
        source_profile: Mapping[str, Any],
        expected_raw_source_profile_sha256: str,
    ) -> dict[str, Any]: ...


def _read_object(path: Path | str, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"{name} root must be an object")
    return _clone(value, name=name)


def _file_sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _identity(value: Mapping[str, Any], field: str, *, name: str) -> str:
    material = _clone(value, name=name)
    supplied = material.pop(field, None)
    if not isinstance(supplied, str) or canonical_sha256(material) != supplied:
        raise TemporalDiscoveryContractError(f"{name} {field} identity mismatch")
    return supplied


def _external_component_root(output_root: Path | str, version: str, component: str) -> Path:
    if not SAFE_VERSION.fullmatch(version):
        raise TemporalDiscoveryContractError("version must be a short safe path token")
    parent = Path(output_root).expanduser().resolve()
    repository = Path(__file__).resolve().parents[1]
    try:
        parent.relative_to(repository)
    except ValueError:
        pass
    else:
        raise TemporalDiscoveryContractError(
            "validation output root must be outside the repository; evidence writes are prohibited"
        )
    if not component or "/" in component or "\\" in component:
        raise TemporalDiscoveryContractError("component name is invalid")
    return parent / f"stage5e7-v3-validation-{version}" / component


def _component_manifest(root: Path, *, component: str) -> dict[str, Any]:
    files = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name == "manifest.json":
            continue
        files.append(
            {
                "relativePath": path.relative_to(root).as_posix(),
                "length": path.stat().st_size,
                "sha256": _file_sha256(path),
            }
        )
    manifest = {
        "schemaVersion": COMPONENT_MANIFEST_SCHEMA,
        "harnessVersion": HARNESS_VERSION,
        "component": component,
        "fileCount": len(files),
        "files": files,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write_immutable(root / "manifest.json", manifest)
    return manifest


def _audit_component(root: Path) -> dict[str, Any]:
    manifest = _read_object(root / "manifest.json", name="validation component manifest")
    supplied = _identity(manifest, "manifestSha256", name="validation component manifest")
    if manifest.get("schemaVersion") != COMPONENT_MANIFEST_SCHEMA:
        raise TemporalDiscoveryContractError("unknown validation component manifest schema")
    expected: set[Path] = set()
    for row in manifest.get("files") or []:
        if not isinstance(row, Mapping):
            raise TemporalDiscoveryContractError("component manifest file row is invalid")
        relative = Path(str(row.get("relativePath") or ""))
        if not relative.parts or relative.is_absolute() or ".." in relative.parts:
            raise TemporalDiscoveryContractError("component manifest path escapes its root")
        path = root / relative
        expected.add(path.resolve())
        if (
            not path.is_file()
            or path.stat().st_size != int(row.get("length", -1))
            or _file_sha256(path) != row.get("sha256")
        ):
            raise TemporalDiscoveryContractError("validation component manifest file mismatch")
    actual = {
        path.resolve()
        for path in root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    if actual != expected:
        raise TemporalDiscoveryContractError("validation component artifact inventory drift")
    return {"component": manifest["component"], "manifestSha256": supplied, "ok": True}


def _load_dossiers(path: Path | str | None) -> tuple[dict[str, dict[str, str]], str | None]:
    if path is None:
        return {}, None
    file = Path(path)
    try:
        with file.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except OSError as exc:
        raise TemporalDiscoveryContractError(f"could not read candidate dossiers: {file}") from exc
    result: dict[str, dict[str, str]] = {}
    for raw in rows:
        row = {str(key): str(value or "").strip() for key, value in raw.items() if key}
        candidate_id = next(
            (row.get(key, "") for key in ("candidateId", "candidate_id", "id") if row.get(key, "")),
            "",
        )
        if not candidate_id:
            raise TemporalDiscoveryContractError("candidate dossiers require candidateId or candidate_id")
        if candidate_id in result:
            raise TemporalDiscoveryContractError("candidate dossiers contain duplicate candidate IDs")
        result[candidate_id] = row
    return result, _file_sha256(file)


def _load_repair_population(path: Path) -> tuple[list[dict[str, Any]], str, str]:
    payload = _read_object(path, name="old Stage5E7 population")
    population_schema = str(payload.get("schemaVersion") or "")
    population, population_sha = _load_population(path)
    if population_schema == FROZEN_STAGE5E7_V2_POPULATION_SCHEMA:
        if population_sha != FROZEN_STAGE5E7_V2_POPULATION_SHA256:
            raise TemporalDiscoveryContractError("old v2 population is not the frozen Stage5E7 population")
    elif population_schema != QD_POPULATION_SCHEMA:
        raise TemporalDiscoveryContractError("old population is not a canonical QD population")
    return population, population_sha, population_schema


def _verified_embedded_candidate_identity(candidate: Mapping[str, Any], *, name: str) -> tuple[str, str]:
    candidate_id = str(candidate.get("candidateId") or "")
    if not candidate_id:
        raise TemporalDiscoveryContractError(f"{name} candidateId is required")
    profile = candidate.get("sourceProfile")
    profile_sha = candidate.get("sourceProfileSha256")
    if not isinstance(profile, Mapping) or not isinstance(profile_sha, str) or canonical_sha256(profile) != profile_sha:
        raise TemporalDiscoveryContractError(f"{name} source profile identity mismatch")
    program_sha = str(candidate.get("programSha256") or "")
    snapshot_sha = str(candidate.get("profileSnapshotSha256") or "")
    if not program_sha or not snapshot_sha:
        raise TemporalDiscoveryContractError(f"{name} program and profile snapshot identities are required")
    material = candidate.get("candidateIdentityMaterial")
    supplied = candidate.get("candidateIdentitySha256")
    if material is not None or supplied is not None:
        if not isinstance(material, Mapping) or not isinstance(supplied, str) or canonical_sha256(material) != supplied:
            raise TemporalDiscoveryContractError(f"{name} candidate identity mismatch")
    return (
        candidate_id,
        canonical_sha256(
            {
                "schemaVersion": "stage5e7_v3_repair_carryover_identity_v1",
                "candidateId": candidate_id,
                "sourceProfileSha256": profile_sha,
                "profileSnapshotSha256": snapshot_sha,
                "programSha256": program_sha,
                "candidateIdentitySha256": supplied,
            }
        ),
    )


def _archive_context(
    archive_path: Path | str, population_sha: str, population_schema: str | None = None
) -> tuple[dict[str, dict[str, Any]], str, dict[str, dict[str, Any]]]:
    archive = _read_object(archive_path, name="old Stage5E7 archive")
    archive_sha = _identity(archive, "archiveSha256", name="old Stage5E7 archive")
    is_v3_archive = archive.get("schemaVersion") == QD_ARCHIVE_SCHEMA
    is_frozen_v2_provenance = (
        archive.get("schemaVersion") == FROZEN_STAGE5E7_V2_ARCHIVE_SCHEMA
        and archive.get("qdVersion") == FROZEN_STAGE5E7_V2_QD_VERSION
        and archive_sha == FROZEN_STAGE5E7_V2_ARCHIVE_SHA256
        and archive.get("populationSha256") == FROZEN_STAGE5E7_V2_POPULATION_SHA256
        and population_sha == FROZEN_STAGE5E7_V2_POPULATION_SHA256
        and population_schema == FROZEN_STAGE5E7_V2_POPULATION_SCHEMA
    )
    if not (is_v3_archive or is_frozen_v2_provenance):
        raise TemporalDiscoveryContractError("old archive is not a canonical QD archive")
    if archive.get("populationSha256") != population_sha:
        raise TemporalDiscoveryContractError("old archive and supplied population identity disagree")
    context: dict[str, dict[str, Any]] = {}
    carryovers: dict[str, dict[str, Any]] = {}
    for cell in archive.get("cells") or []:
        if not isinstance(cell, Mapping):
            continue
        for member in cell.get("members") or []:
            if not isinstance(member, Mapping) or not member.get("candidateId"):
                continue
            candidate_id = str(member["candidateId"])
            candidate = member.get("candidate")
            aggregate = member.get("aggregate")
            descriptor = member.get("descriptor")
            if not isinstance(candidate, Mapping) or not isinstance(aggregate, Mapping) or not isinstance(descriptor, Mapping):
                raise TemporalDiscoveryContractError("old archive member lacks embedded candidate, aggregate, or descriptor")
            embedded_id, identity = _verified_embedded_candidate_identity(candidate, name="old archive member")
            if embedded_id != candidate_id or str(aggregate.get("candidateId") or "") != candidate_id:
                raise TemporalDiscoveryContractError("old archive member candidate identity disagrees with its embedded records")
            if not str(descriptor.get("cellId") or ""):
                raise TemporalDiscoveryContractError("old archive member descriptor cell is required")
            member_context = {
                "oldArchiveCellId": cell.get("cellId"),
                "oldArchiveLane": member.get("archiveLane"),
                "oldArchiveRetentionReason": member.get("retentionReason"),
            }
            existing = carryovers.get(candidate_id)
            if existing is not None:
                if (
                    existing["candidateIdentitySha256"] != identity
                    or canonical_sha256(existing["candidate"]) != canonical_sha256(candidate)
                    or canonical_sha256(existing["aggregate"]) != canonical_sha256(aggregate)
                    or canonical_sha256(existing["descriptor"]) != canonical_sha256(descriptor)
                ):
                    raise TemporalDiscoveryContractError("old archive contains conflicting duplicate candidate records")
                continue
            context[candidate_id] = member_context
            carryovers[candidate_id] = {
                "candidate": _clone(candidate, name="old archive embedded candidate"),
                "aggregate": _clone(aggregate, name="old archive embedded aggregate"),
                "descriptor": _clone(descriptor, name="old archive embedded descriptor"),
                "candidateIdentitySha256": identity,
                "context": member_context,
            }
    return context, archive_sha, carryovers


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "resolved"}


def _number_from(mapping: Mapping[str, Any], *keys: str, default: float = 0.0) -> float:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, "") and not isinstance(value, bool):
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            if math.isfinite(number):
                return number
    return default


def _required_finite_value(
    mapping: Mapping[str, Any],
    *keys: str,
    name: str,
) -> tuple[float, str]:
    """Return one declared finite metric and the exact field that supplied it.

    Repair is allowed to use predecessor economics only for stratified coverage.
    It must therefore retain which predecessor field supplied a score instead of
    silently defaulting absent data to zero or relabelling an old score as v3.
    """

    for key in keys:
        value = mapping.get(key)
        if value in (None, "") or isinstance(value, bool):
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            return number, key
    raise TemporalDiscoveryContractError(f"{name} requires one finite declared aggregate metric")


def _text_from(mapping: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = str(mapping.get(key) or "").strip()
        if value:
            return value
    return ""


def _unresolved_flag(value: Any, *, name: str) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value in (0, 1):
            return bool(value)
        raise TemporalDiscoveryContractError(f"{name} must be a resolved/unresolved flag")
    normalized = str(value).strip().lower()
    if normalized in {"0", "false", "no", "n", "resolved", "closed", "none", "no_open_position"}:
        return False
    if normalized in {"1", "true", "yes", "y", "unresolved", "open", "position_open"}:
        return True
    return None


def _window_unresolved_flags(value: Any, *, name: str) -> list[bool]:
    if value is None or value == "":
        return []
    parsed = value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise TemporalDiscoveryContractError(f"{name} must be JSON encoded") from exc
    if not isinstance(parsed, Sequence) or isinstance(parsed, (str, bytes)):
        raise TemporalDiscoveryContractError(f"{name} must be a sequence of resolved/unresolved flags")
    flags = []
    for index, item in enumerate(parsed):
        flag = _unresolved_flag(item, name=f"{name}[{index}]")
        if flag is None:
            raise TemporalDiscoveryContractError(f"{name}[{index}] must be a resolved/unresolved flag")
        flags.append(flag)
    return flags


def _resolved_evidence(aggregate: Mapping[str, Any], dossier: Mapping[str, str], merged: Mapping[str, Any]) -> tuple[str, str]:
    evidence: list[tuple[str, bool]] = []

    def add(name: str, value: Any) -> None:
        flag = _unresolved_flag(value, name=name)
        if flag is not None:
            evidence.append((name, flag))

    for key in ("correctedEvidenceResolution", "corrected_evidence_resolution", "resolutionStatus", "resolution_status", "resolved"):
        add(key, merged.get(key))
    for key in ("any_unresolved_position", "anyUnresolvedPosition", "unresolved_position", "unresolvedPosition"):
        add(f"dossier.{key}", dossier.get(key))
        add(f"aggregate.{key}", aggregate.get(key))
    for key in ("window_unresolved_json", "windowUnresolvedJson", "windowUnresolved", "window_unresolved"):
        flags = _window_unresolved_flags(dossier.get(key), name=f"dossier.{key}")
        if flags:
            evidence.append((f"dossier.{key}.any", any(flags)))
    aggregate_window_evidence: list[bool] = []
    for index, window in enumerate(aggregate.get("windowRecords") or []):
        if not isinstance(window, Mapping):
            continue
        for key in ("unresolvedPosition", "unresolved_position", "conservativeUnresolvedPosition"):
            flag = _unresolved_flag(window.get(key), name=f"aggregate.windowRecords[{index}].{key}")
            if flag is not None:
                aggregate_window_evidence.append(flag)
        terminal = window.get("conservativeTerminal")
        if isinstance(terminal, Mapping):
            flag = _unresolved_flag(
                terminal.get("terminalPositionStatus"),
                name=f"aggregate.windowRecords[{index}].terminalPositionStatus",
            )
            if flag is not None:
                aggregate_window_evidence.append(flag)
    if aggregate_window_evidence:
        evidence.append(("aggregate.windowRecords.any", any(aggregate_window_evidence)))
    terminal_evidence: list[bool] = []
    for index, terminal in enumerate(aggregate.get("terminalEvidence") or []):
        if isinstance(terminal, Mapping):
            flag = _unresolved_flag(
                terminal.get("positionStatus"),
                name=f"aggregate.terminalEvidence[{index}].positionStatus",
            )
            if flag is not None:
                terminal_evidence.append(flag)
    if terminal_evidence:
        evidence.append(("aggregate.terminalEvidence.any", any(terminal_evidence)))
    if not evidence:
        return "unresolved", "no_explicit_unresolved_evidence"
    values = {flag for _name, flag in evidence}
    if len(values) != 1:
        sources = ", ".join(f"{name}={int(flag)}" for name, flag in evidence)
        raise TemporalDiscoveryContractError(f"candidate resolution evidence is contradictory: {sources}")
    source = ",".join(name for name, _flag in evidence)
    return ("unresolved" if values.pop() else "resolved"), source


def _candidate_annotations(
    candidate: Mapping[str, Any], aggregate: Mapping[str, Any], descriptor: Mapping[str, Any], dossier: Mapping[str, str]
) -> dict[str, Any]:
    profile = candidate.get("sourceProfile") or {}
    identity = derive_strategy_identity(candidate)
    merged: dict[str, Any] = {**aggregate, **candidate, **dossier}
    resolution, resolution_source = _resolved_evidence(aggregate, dossier, merged)
    v3_admissible = aggregate.get("v3Admissible") is True
    aggregate_basis = str(aggregate.get("economicsBasis") or "").strip()
    if v3_admissible:
        if aggregate_basis != "stage5e7_v3_terminal_adjusted":
            raise TemporalDiscoveryContractError(
                "v3 repair aggregate must declare the Stage5E7-v3 terminal-adjusted economics basis"
            )
        total_r, total_source = _required_finite_value(
            aggregate,
            "totalTerminalAdjustedConservativeNetR",
            name="v3 repair total terminal-adjusted R",
        )
        worst_r, worst_source = _required_finite_value(
            aggregate,
            "worstWindowTerminalAdjustedConservativeNetR",
            name="v3 repair worst-window terminal-adjusted R",
        )
        stratification = {
            "basis": "stage5e7_v3_terminal_adjusted",
            "v3Admissible": True,
            "totalNetR": total_r,
            "worstWindowNetR": worst_r,
            "totalNetRSource": f"aggregate.{total_source}",
            "worstWindowNetRSource": f"aggregate.{worst_source}",
        }
    else:
        if aggregate_basis and aggregate_basis != "legacy_closed_trade_v1_not_v3_admissible":
            raise TemporalDiscoveryContractError(
                "non-v3 repair aggregate must not claim a non-legacy economics basis"
            )
        total_r, total_source = _required_finite_value(
            aggregate,
            "totalRawClosedConservativeNetR",
            "totalConservativeNetR",
            name="legacy repair total closed-trade proxy R",
        )
        worst_r, worst_source = _required_finite_value(
            aggregate,
            "worstWindowRawClosedConservativeNetR",
            "worstWindowConservativeNetR",
            name="legacy repair worst-window closed-trade proxy R",
        )
        stratification = {
            "basis": "legacy_closed_trade_proxy",
            "v3Admissible": False,
            "totalNetR": total_r,
            "worstWindowNetR": worst_r,
            "totalNetRSource": f"aggregate.{total_source}",
            "worstWindowNetRSource": f"aggregate.{worst_source}",
            "predecessorEconomicsBasis": (
                aggregate_basis or "frozen_pre_v3_archive_embedded_aggregate"
            ),
            "selectionUse": "stratified_coverage_only_no_promotion",
        }
    record = {
        "candidateId": str(candidate["candidateId"]),
        "sourceMode": str(candidate.get("sourceMode") or "unknown"),
        "directionMode": str(profile.get("directionMode") or "both").lower(),
        "structuralFamilyId": str(identity.get("structural_family_id") or "unknown"),
        "structuralFamilySource": str(identity.get("structural_family_source") or "unknown"),
        "descriptorCellId": str(descriptor["cellId"]),
        "stratificationEconomics": stratification,
        "totalTrades": int(aggregate.get("totalTrades") or 0),
        "maxWindowTradeShare": (
            max(aggregate.get("tradeCountsByWindow") or [0]) / max(1, int(aggregate.get("totalTrades") or 0))
        ),
        "entryFrequencyPerThousand": _number_from(aggregate, "entryFrequencyPerThousand"),
        "medianHoldingBars": _number_from(aggregate, "medianHoldingBars"),
        "resolution": resolution,
        "resolutionEvidenceSource": resolution_source,
        "sourceCandidateSha256": canonical_sha256(candidate),
    }
    if v3_admissible:
        # These names are emitted only when they are backed by genuine v3
        # terminal economics.  Legacy panel rows intentionally never receive
        # a terminal-adjusted alias.
        record["terminalAdjustedR"] = total_r
        record["worstWindowTerminalAdjustedR"] = worst_r
    else:
        record["legacyClosedTradeProxyR"] = total_r
        record["worstWindowLegacyClosedTradeProxyR"] = worst_r
    return record


def _tag_predicates(record: Mapping[str, Any]) -> dict[str, bool]:
    economics = record.get("stratificationEconomics")
    if not isinstance(economics, Mapping):
        raise TemporalDiscoveryContractError("repair record stratification economics are required")
    worst = _number_from(economics, "worstWindowNetR")
    total = _number_from(economics, "totalNetR")
    positive = worst > 0.0 and total > 0.0
    return {
        "both_positive_resolved": positive and record["resolution"] == "resolved",
        "both_positive_unresolved": positive and record["resolution"] != "resolved",
        "positive_single_trade_concentrated": positive and record["maxWindowTradeShare"] >= 0.70,
        "high_support_negative": record["totalTrades"] >= 8 and worst < 0.0,
        "high_turnover": record["entryFrequencyPerThousand"] >= 12.0,
        "sparse_long_hold": record["entryFrequencyPerThousand"] <= 4.0 and record["medianHoldingBars"] >= 96.0,
        "short_direction": record["directionMode"] == "short",
        "flat_negative_control": abs(worst) <= 0.10 or total <= 0.0,
    }


def _tie_key(seed: int, candidate_id: str) -> tuple[str, str]:
    return canonical_sha256({"schema": "stage5e7_v3_validation_tiebreak_v1", "seed": int(seed), "candidateId": candidate_id}), candidate_id


def _matched_slots(
    slots: Sequence[tuple[str, Callable[[Mapping[str, Any]], bool]]], records: Mapping[str, Mapping[str, Any]], seed: int
) -> list[tuple[str, str]]:
    choices: dict[int, list[str]] = {}
    for index, (_label, predicate) in enumerate(slots):
        values = [candidate_id for candidate_id, row in records.items() if predicate(row)]
        values.sort(key=lambda item: _tie_key(seed, item))
        if not values:
            raise TemporalDiscoveryContractError(f"required repair coverage stratum cannot be filled: {slots[index][0]}")
        choices[index] = values
    # Deterministic bipartite matching makes overlapping strata work without
    # quietly reusing a candidate or relying on a historical archive rank.
    owner: dict[str, int] = {}

    def assign(slot: int, visited: set[str]) -> bool:
        for candidate_id in choices[slot]:
            if candidate_id in visited:
                continue
            visited.add(candidate_id)
            prior = owner.get(candidate_id)
            if prior is None or assign(prior, visited):
                owner[candidate_id] = slot
                return True
        return False

    for slot in sorted(range(len(slots)), key=lambda item: (len(choices[item]), slots[item][0])):
        if not assign(slot, set()):
            raise TemporalDiscoveryContractError(
                f"required repair coverage cannot be filled uniquely: {slots[slot][0]}"
            )
    by_slot = {slot: candidate_id for candidate_id, slot in owner.items()}
    return [(slots[index][0], by_slot[index]) for index in range(len(slots))]


def _reference_slots(records: Mapping[str, Mapping[str, Any]]) -> list[tuple[str, Callable[[Mapping[str, Any]], bool]]]:
    slots: list[tuple[str, Callable[[Mapping[str, Any]], bool]]] = []
    for named in NAMED_REFERENCES:
        slots.append((f"named:{named}", lambda row, named=named: row["candidateId"].startswith(named)))
    predicate_names = (
        "both_positive_resolved",
        "both_positive_unresolved",
        "positive_single_trade_concentrated",
        "high_support_negative",
        "high_turnover",
        "sparse_long_hold",
        "short_direction",
        "flat_negative_control",
    )
    for name in predicate_names:
        count = REFERENCE_QUOTAS[name]
        for ordinal in range(count):
            slots.append((f"{name}:{ordinal + 1}", lambda row, name=name: _tag_predicates(row)[name]))
    group_fields = (
        ("representative_origin", "sourceMode"),
        ("representative_family", "structuralFamilyId"),
        ("representative_descriptor_cell", "descriptorCellId"),
    )
    for stratum, field in group_fields:
        values = sorted({str(row[field]) for row in records.values() if str(row[field]) and str(row[field]) != "unknown"})
        quota = REFERENCE_QUOTAS[stratum]
        if stratum == "representative_origin":
            # Frozen gen4 uses two admitted provenance modes
            # (random immigrant and structural offspring), not four.  Keep the
            # fixed 64-row panel by allocating the four origin slots across
            # every actual origin deterministically.  More origins than slots
            # are a contract failure: omitting an admitted origin would make
            # the coverage claim false.
            if not values:
                raise TemporalDiscoveryContractError("required repair coverage has no representative_origin values")
            if len(values) > quota:
                raise TemporalDiscoveryContractError(
                    f"required repair coverage has {len(values)} representative_origin values but only {quota} slots"
                )
            repeats, remainder = divmod(quota, len(values))
            for index, value in enumerate(values):
                for ordinal in range(repeats + int(index < remainder)):
                    slots.append(
                        (
                            f"{stratum}:{value}:{ordinal + 1}",
                            lambda row, field=field, value=value: str(row[field]) == value,
                        )
                    )
            continue
        if len(values) < quota:
            raise TemporalDiscoveryContractError(f"required repair coverage has fewer than {quota} {stratum} values")
        for value in values[:quota]:
            slots.append((f"{stratum}:{value}", lambda row, field=field, value=value: str(row[field]) == value))
    if len(slots) != 64:
        raise AssertionError("reference panel slot accounting drift")
    return slots


def build_repair_panel(
    *,
    old_archive_path: Path | str,
    old_population_path: Path | str,
    old_result_root: Path | str,
    output_root: Path | str,
    version: str,
    seed: int,
    candidate_dossiers_path: Path | str | None = None,
) -> dict[str, Any]:
    """Build exactly 64 tagged candidates from frozen old inputs, without promotion."""

    population_path = Path(old_population_path)
    population, population_sha, population_schema = _load_repair_population(population_path)
    old_context, archive_sha, archive_members = _archive_context(old_archive_path, population_sha, population_schema)
    grouped = load_stage_results(old_result_root)
    if set(grouped) != {str(item["candidateId"]) for item in population}:
        raise TemporalDiscoveryContractError("old result root must cover exactly the supplied old population")
    dossiers, dossiers_sha = _load_dossiers(candidate_dossiers_path)
    records: dict[str, dict[str, Any]] = {}
    candidates: dict[str, dict[str, Any]] = {}
    candidate_identities: dict[str, str] = {}
    cohort_sources: dict[str, dict[str, Any]] = {}
    for candidate in population:
        candidate_id, identity = _verified_embedded_candidate_identity(candidate, name="old population")
        existing_identity = candidate_identities.get(candidate_id)
        if existing_identity is not None and existing_identity != identity:
            raise TemporalDiscoveryContractError("old population contains conflicting duplicate candidate identities")
        if existing_identity is None:
            candidates[candidate_id] = _clone(candidate, name="old population candidate")
            candidate_identities[candidate_id] = identity
    for candidate_id in sorted(candidates):
        candidate = candidates[candidate_id]
        aggregate = _aggregate_candidate(candidate, grouped[candidate_id])
        descriptor = qd_behavior_descriptor(candidate, aggregate)
        record = _candidate_annotations(candidate, aggregate, descriptor, dossiers.get(candidate_id, {}))
        record["aggregate"] = aggregate
        record["descriptor"] = descriptor
        record["dossier"] = dossiers.get(candidate_id, {})
        record["oldArchiveContext"] = old_context.get(candidate_id)
        record["repairCohortSource"] = "proposal_population_result"
        record["repairAggregateSource"] = "proposal_result_root"
        cohort_sources[candidate_id] = {
            "source": "proposal_population_result",
            "aggregateSource": "proposal_result_root",
        }
        records[candidate_id] = record
    archive_carryover_count = 0
    proposal_archive_overlap_count = 0
    for candidate_id, carried in sorted(archive_members.items()):
        candidate = carried["candidate"]
        identity = str(carried["candidateIdentitySha256"])
        if candidate_id in candidates:
            if candidate_identities[candidate_id] != identity:
                raise TemporalDiscoveryContractError("proposal population and old archive disagree on candidate identity")
            proposal_archive_overlap_count += 1
            record = records[candidate_id]
            record["oldArchiveContext"] = carried["context"]
            record["archiveAggregateSha256"] = canonical_sha256(carried["aggregate"])
            record["proposalAggregateSha256"] = canonical_sha256(record["aggregate"])
            record["repairAggregateSource"] = "proposal_result_root"
            record["repairAggregateAgreement"] = (
                "semantic_exact"
                if record["archiveAggregateSha256"] == record["proposalAggregateSha256"]
                else "proposal_result_root_preferred_archive_aggregate_recorded"
            )
            cohort_sources[candidate_id]["archiveContext"] = carried["context"]
            cohort_sources[candidate_id]["archiveAggregateSha256"] = record["archiveAggregateSha256"]
            continue
        candidates[candidate_id] = _clone(candidate, name="archive carryover candidate")
        candidate_identities[candidate_id] = identity
        aggregate = carried["aggregate"]
        descriptor = carried["descriptor"]
        record = _candidate_annotations(candidate, aggregate, descriptor, dossiers.get(candidate_id, {}))
        record["aggregate"] = aggregate
        record["descriptor"] = descriptor
        record["dossier"] = dossiers.get(candidate_id, {})
        record["oldArchiveContext"] = carried["context"]
        record["repairCohortSource"] = "archive_carryover"
        record["repairAggregateSource"] = "archive_embedded_aggregate"
        records[candidate_id] = record
        cohort_sources[candidate_id] = {
            "source": "archive_carryover",
            "aggregateSource": "archive_embedded_aggregate",
            "archiveContext": carried["context"],
        }
        archive_carryover_count += 1
    selected = _matched_slots(_reference_slots(records), records, seed)
    if len(selected) != 64 or len({candidate_id for _slot, candidate_id in selected}) != 64:
        raise TemporalDiscoveryContractError("repair panel must select exactly 64 unique candidates")
    selected_tags: dict[str, list[str]] = defaultdict(list)
    primary_reason: dict[str, str] = {}
    for slot, candidate_id in selected:
        selected_tags[candidate_id].append(slot)
        primary_reason.setdefault(candidate_id, slot)
    reference_candidates = []
    selection_rows = []
    for candidate_id in sorted(selected_tags):
        source = _clone(candidates[candidate_id], name="reference source candidate")
        source["referenceTags"] = sorted(selected_tags[candidate_id])
        source["referenceSelectionReason"] = primary_reason[candidate_id]
        source["referenceSelectionPolicy"] = "coverage_only_no_old_rank_or_promotion"
        source["referenceSourceCandidateSha256"] = records[candidate_id]["sourceCandidateSha256"]
        source["repairCohortSource"] = records[candidate_id]["repairCohortSource"]
        source["repairAggregateSource"] = records[candidate_id]["repairAggregateSource"]
        reference_candidates.append(source)
        row = {key: value for key, value in records[candidate_id].items() if key not in {"aggregate", "descriptor", "dossier"}}
        row["referenceTags"] = sorted(selected_tags[candidate_id])
        row["primarySelectionReason"] = primary_reason[candidate_id]
        selection_rows.append(row)
    reference_candidates.sort(key=lambda item: str(item["candidateId"]))
    reference_population = {
        "schemaVersion": QD_POPULATION_SCHEMA,
        "referencePopulationSchema": REFERENCE_POPULATION_SCHEMA,
        "harnessVersion": HARNESS_VERSION,
        "selectionPolicy": "coverage_only_no_old_rank_or_promotion",
        "candidateCount": len(reference_candidates),
        "candidates": reference_candidates,
    }
    reference_population["populationSha256"] = canonical_sha256(reference_population)
    cohort = {
        "schemaVersion": "stage5e7_v3_repair_selection_cohort_v1",
        "proposalPopulationCandidateCount": len(candidates) - archive_carryover_count,
        "proposalResultCandidateCount": len(grouped),
        "finalArchiveMemberCandidateCount": len(archive_members),
        "archiveOnlyCarryoverCandidateCount": archive_carryover_count,
        "proposalArchiveOverlapCandidateCount": proposal_archive_overlap_count,
        "selectionCandidateCount": len(candidates),
        "candidateSources": cohort_sources,
    }
    cohort["cohortSha256"] = canonical_sha256(cohort)
    panel = {
        "schemaVersion": REFERENCE_PANEL_SCHEMA,
        "harnessVersion": HARNESS_VERSION,
        "selectionPolicy": {
            "name": "stratified_repair_coverage_only",
            "oldArchiveRankPromotion": False,
            "selectionTieBreak": "seeded_canonical_candidate_identity",
            "exactCandidateCount": 64,
        },
        "seed": int(seed),
        "prohibitedEvidence": {"start": PROHIBITED_INTERVAL_START, "reservedEvidencePermitted": False},
        "source": {
            "oldArchivePath": str(Path(old_archive_path).resolve()),
            "oldArchiveSha256": archive_sha,
            "oldPopulationPath": str(population_path.resolve()),
            "oldPopulationSha256": population_sha,
            "oldResultRoot": str(Path(old_result_root).resolve()),
            "oldResultSetSha256": _result_set_sha256(grouped),
            "candidateDossiersPath": str(Path(candidate_dossiers_path).resolve()) if candidate_dossiers_path else None,
            "candidateDossiersSha256": dossiers_sha,
            "selectionCohort": cohort,
        },
        "referencePopulationSha256": reference_population["populationSha256"],
        "selectionRows": selection_rows,
    }
    panel["referencePanelSha256"] = canonical_sha256(panel)
    root = _external_component_root(output_root, version, "repair")
    _write_immutable(root / "reference-population.json", reference_population)
    _write_immutable(root / "reference-panel.json", panel)
    _write_immutable(
        root / "audit-command.json",
        {
            "schemaVersion": "stage5e7_v3_validation_audit_command_v1",
            "command": [
                "stage5e7-v3-validation",
                "--output-root",
                str(Path(output_root).resolve()),
                "--version",
                version,
                "audit",
            ],
            "writesEvidence": False,
        },
    )
    manifest = _component_manifest(root, component="repair")
    return {
        "schemaVersion": "stage5e7_v3_repair_panel_result_v1",
        "outputRoot": str(root),
        "referencePanelSha256": panel["referencePanelSha256"],
        "referencePopulationSha256": reference_population["populationSha256"],
        "candidateCount": 64,
        "manifestSha256": manifest["manifestSha256"],
    }


def _load_reference_panel(path: Path | str) -> tuple[dict[str, Any], dict[str, Any], Path]:
    root = Path(path)
    if root.is_file():
        root = root.parent
    panel = _read_object(root / "reference-panel.json", name="reference panel")
    panel_sha = _identity(panel, "referencePanelSha256", name="reference panel")
    population = _read_object(root / "reference-population.json", name="reference population")
    population_sha = _identity(population, "populationSha256", name="reference population")
    if (
        panel.get("schemaVersion") != REFERENCE_PANEL_SCHEMA
        or population.get("schemaVersion") != QD_POPULATION_SCHEMA
        or population.get("referencePopulationSchema") != REFERENCE_POPULATION_SCHEMA
        or panel.get("referencePopulationSha256") != population_sha
        or len(population.get("candidates") or []) != 64
    ):
        raise TemporalDiscoveryContractError("reference panel contract mismatch")
    panel["referencePanelSha256"] = panel_sha
    population["populationSha256"] = population_sha
    return panel, population, root


def _operator_parent_candidates(reference_candidates: Sequence[Mapping[str, Any]], seed: int) -> list[dict[str, Any]]:
    tags: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for candidate in reference_candidates:
        for tag in candidate.get("referenceTags") or []:
            tags[str(tag).split(":", 1)[0]].append(candidate)
    priorities = [
        "named", "both_positive_resolved", "both_positive_unresolved", "positive_single_trade_concentrated",
        "high_support_negative", "high_turnover", "sparse_long_hold", "short_direction",
        "representative_origin", "representative_family", "representative_descriptor_cell", "flat_negative_control",
    ]
    selected: list[dict[str, Any]] = []
    used: set[str] = set()
    for tag in priorities:
        options = sorted(tags.get(tag, []), key=lambda row: _tie_key(seed, str(row["candidateId"])))
        item = next((row for row in options if str(row["candidateId"]) not in used), None)
        if item is None:
            raise TemporalDiscoveryContractError(f"reference panel cannot supply causal parent stratum: {tag}")
        selected.append(_clone(item, name="operator panel parent"))
        used.add(str(item["candidateId"]))
    if not 12 <= len(selected) <= 16:
        raise TemporalDiscoveryContractError("operator panel must use 12–16 parents")
    return selected


def _operator_identity(
    *, parent: Mapping[str, Any], operator: Any, plan: Mapping[str, Any], child: Mapping[str, Any]
) -> str:
    return canonical_sha256(
        {
            "schemaVersion": "stage5e7_v3_planned_operator_application_v1",
            "parentCandidateId": parent["candidateId"],
            "parentSourceProfileSha256": parent["sourceProfileSha256"],
            "parentProgramSha256": parent["programSha256"],
            "operatorId": operator.operator_id,
            "operatorVersion": operator.operator_version,
            "planSha256": plan["planSha256"],
            "childSourceProfileSha256": canonical_sha256(child),
        }
    )


def _candidate_identity(payload: Mapping[str, Any]) -> str:
    return canonical_sha256({"schemaVersion": "stage5e7_v3_validation_candidate_identity_v1", **_clone(payload, name="candidate identity")})


def build_operator_panel(
    *,
    reference_root: Path | str,
    output_root: Path | str,
    version: str,
    seed: int,
    catalog: Mapping[str, Any],
    validator: CandidateValidator,
) -> dict[str, Any]:
    """Build a <=64 no-op/one-depth sibling panel with static validation only."""

    panel, reference_population, reference_path = _load_reference_panel(reference_root)
    parents = _operator_parent_candidates(reference_population["candidates"], seed)
    v3 = GeneratorV3ConstructionRegistry(catalog)
    operators = [*(v3.get(item) for item in v3.enabled_operator_ids), ConfirmedEntryStructuralOperator(), *expanded_structural_operators()]
    by_id: dict[str, Any] = {}
    for operator in operators:
        if operator.operator_id in by_id:
            raise TemporalDiscoveryContractError("duplicate enabled causal operator ID")
        by_id[operator.operator_id] = operator
    applications: list[dict[str, Any]] = []
    viable: list[dict[str, Any]] = []
    for parent in parents:
        for operator_id in sorted(by_id):
            operator = by_id[operator_id]
            plans = operator.enumerate_plans(parent["sourceProfile"])
            if not plans:
                applications.append({
                    "parentCandidateId": parent["candidateId"], "operatorId": operator_id,
                    "operatorVersion": operator.operator_version, "disposition": "suppressed",
                    "suppressionReason": "no_applicable_depth_1_plan",
                })
                continue
            plan = sorted(plans, key=lambda value: str(value["planSha256"]))[0]
            preview = operator.preview(parent["sourceProfile"], plan)
            if operator_id in v3.enabled_operator_ids:
                reachability = inspect_construction_reachability(preview)
                if reachability.get("acceptable") is not True:
                    applications.append({
                        "parentCandidateId": parent["candidateId"], "operatorId": operator_id,
                        "operatorVersion": operator.operator_version, "planSha256": plan["planSha256"],
                        "disposition": "suppressed", "suppressionReason": "static_construction_reachability_failed",
                        "reachabilityIssueCounts": reachability.get("issueCounts") or {},
                    })
                    continue
            application = {
                "parentCandidateId": parent["candidateId"], "operatorId": operator_id,
                "operatorVersion": operator.operator_version, "plan": plan,
                "planSha256": plan["planSha256"], "childSourceProfile": preview,
                "childSourceProfileSha256": canonical_sha256(preview),
                "plannedApplicationSha256": _operator_identity(parent=parent, operator=operator, plan=plan, child=preview),
                "disposition": "planned",
            }
            applications.append(application)
            viable.append(application)
    # A 64-candidate cap permits 12 no-ops + 52 siblings.  Give every enabled
    # applicable operator one opportunity first, then fill deterministically.
    selected: list[dict[str, Any]] = []
    selected_keys: set[tuple[str, str]] = set()
    for operator_id in sorted(by_id):
        options = [item for item in viable if item["operatorId"] == operator_id]
        if not options:
            continue
        choice = min(options, key=lambda item: _tie_key(seed, item["plannedApplicationSha256"]))
        selected.append(choice)
        selected_keys.add((str(choice["parentCandidateId"]), str(choice["operatorId"])))
    for item in sorted(viable, key=lambda value: _tie_key(seed, str(value["plannedApplicationSha256"]))):
        key = (str(item["parentCandidateId"]), str(item["operatorId"]))
        if len(selected) >= 64 - len(parents):
            break
        if key not in selected_keys:
            selected.append(item)
            selected_keys.add(key)
    selected_application_ids = {str(item["plannedApplicationSha256"]) for item in selected}
    for item in applications:
        if item.get("disposition") == "planned" and item["plannedApplicationSha256"] not in selected_application_ids:
            item["disposition"] = "suppressed"
            item["suppressionReason"] = "candidate_cap_after_balanced_operator_coverage"

    candidates: list[dict[str, Any]] = []
    pair_rows: list[dict[str, Any]] = []
    parent_by_id = {str(item["candidateId"]): item for item in parents}
    for ordinal, parent in enumerate(parents):
        parent_id = str(parent["candidateId"])
        control_id = "stage5e7v3_noop_" + canonical_sha256({"parent": parent_id, "seed": int(seed)})[7:35]
        control_identity = _candidate_identity({"kind": "no_op_control", "parentCandidateId": parent_id, "parentProgramSha256": parent["programSha256"]})
        control = {
            "candidateId": control_id,
            "sourceMode": "stage5e7_v3_noop_control",
            "seedId": str(parent.get("seedId") or "reference"),
            "sourceProfile": _clone(parent["sourceProfile"], name="no-op control profile"),
            "sourceProfileSha256": parent["sourceProfileSha256"],
            "profileSnapshotSha256": parent.get("profileSnapshotSha256") or parent["sourceProfileSha256"],
            "programSha256": parent["programSha256"],
            "candidateIdentitySha256": control_identity,
            "causalPair": {"parentCandidateId": parent_id, "role": "unchanged_no_op_control", "depth": 0},
        }
        candidates.append(control)
        pair_rows.append({"parentCandidateId": parent_id, "controlCandidateId": control_id, "parentOrdinal": ordinal})

    for birth_ordinal, item in enumerate(selected):
        parent = parent_by_id[str(item["parentCandidateId"])]
        operator = by_id[str(item["operatorId"])]
        child_profile = _clone(item["childSourceProfile"], name="operator preview profile")
        child_id = "stage5e7v3_op_" + str(item["plannedApplicationSha256"])[7:35]
        validation = validator.validate(
            candidate_id=child_id,
            source_profile=child_profile,
            expected_raw_source_profile_sha256=item["childSourceProfileSha256"],
        )
        if canonical_sha256(child_profile) != item["childSourceProfileSha256"]:
            raise TemporalDiscoveryContractError("operator child profile mutated during static validation")
        if validation.get("candidateAcceptable") is not True:
            item["disposition"] = "suppressed"
            item["suppressionReason"] = "static_validator_rejected"
            item["validatorIssueCodes"] = sorted(str(value.get("code")) for value in validation.get("issues") or [] if isinstance(value, Mapping) and value.get("code"))
            continue
        child_program = str(validation.get("programSha256") or "")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", child_program):
            raise TemporalDiscoveryContractError("accepted operator child lacks canonical program identity")
        rebound, application = operator.apply(
            parent["sourceProfile"], item["plan"],
            parent_validated_program_sha256=parent["programSha256"], child_validated_program_sha256=child_program,
        )
        audit = operator.audit(parent["sourceProfile"], rebound, application)
        if rebound != child_profile or audit.get("allChecksPassed") is not True:
            raise TemporalDiscoveryContractError("operator application replay/audit failed")
        candidate_identity = _candidate_identity({"kind": "one_depth_sibling", "plannedApplicationSha256": item["plannedApplicationSha256"], "applicationSha256": application["applicationSha256"]})
        lineage = build_candidate_lineage(
            candidate_id=child_id,
            candidate_source_profile_sha256=item["childSourceProfileSha256"],
            candidate_validated_program_sha256=child_program,
            generation_index=0,
            birth_ordinal=birth_ordinal,
            parent_candidate_ids=[str(parent["candidateId"])],
            parent_program_sha256s=[str(parent["programSha256"])],
            operator_id=operator.operator_id,
            operator_version=operator.operator_version,
            plan_sha256=item["planSha256"],
            application_sha256=application["applicationSha256"],
        )
        candidates.append({
            "candidateId": child_id, "sourceMode": "stage5e7_v3_one_depth_operator_sibling",
            "seedId": str(parent.get("seedId") or "reference"), "sourceProfile": rebound,
            "sourceProfileSha256": item["childSourceProfileSha256"],
            "profileSnapshotSha256": str(validation.get("profileSnapshotSha256") or item["childSourceProfileSha256"]),
            "programSha256": child_program, "validationReportSha256": validation.get("validationReportSha256"),
            "candidateIdentitySha256": candidate_identity, "structuralDepth": int(parent.get("structuralDepth") or 0) + 1,
            "lineage": lineage,
            "causalPair": {"parentCandidateId": parent["candidateId"], "role": "single_operator_sibling", "depth": 1, "operatorId": operator.operator_id, "plannedApplicationSha256": item["plannedApplicationSha256"], "applicationSha256": application["applicationSha256"]},
        })
        item["disposition"] = "admitted"
        item["application"] = application
        item["applicationAudit"] = audit
        item["childCandidateId"] = child_id
        item["childProgramSha256"] = child_program
        for pair in pair_rows:
            if pair["parentCandidateId"] == parent["candidateId"]:
                pair.setdefault("siblings", []).append(child_id)
                break
    if len(candidates) > 64:
        raise TemporalDiscoveryContractError("operator panel candidate cap exceeded")
    candidates.sort(key=lambda row: str(row["candidateId"]))
    parent_baselines = {
        "schemaVersion": "stage5e7_v3_operator_parent_baselines_v1",
        "candidateCount": len(parents),
        "candidates": [_clone(item, name="operator parent baseline") for item in sorted(parents, key=lambda row: str(row["candidateId"]))],
    }
    parent_baselines["populationSha256"] = canonical_sha256(parent_baselines)
    population = {
        "schemaVersion": QD_POPULATION_SCHEMA, "qdVersion": HARNESS_VERSION,
        "policyName": "stage5e7_v3_operator_causal_panel_not_promotion", "candidateCount": len(candidates),
        "candidates": candidates, "sourceReferencePopulationSha256": reference_population["populationSha256"],
    }
    population["populationSha256"] = canonical_sha256(population)
    output = {
        "schemaVersion": OPERATOR_PANEL_SCHEMA, "harnessVersion": HARNESS_VERSION, "seed": int(seed),
        "referencePanelSha256": panel["referencePanelSha256"], "referencePopulationSha256": reference_population["populationSha256"],
        "catalogSha256": v3.catalog.catalog_sha256, "generatorV3Policy": v3.policy,
        "deferredV3Operators": [{"operatorId": item.operator_id, "reason": item.deferred_reason} for item in v3.deferred_operators],
        "enabledOperatorIds": sorted(by_id), "parentCount": len(parents), "candidateCap": 64,
        "populationSha256": population["populationSha256"], "parentBaselinePopulationSha256": parent_baselines["populationSha256"], "pairs": pair_rows, "applications": applications,
        "prohibitedEvidence": {"start": PROHIBITED_INTERVAL_START, "reservedEvidencePermitted": False},
        "staticValidatorOnly": True,
    }
    output["operatorPanelSha256"] = canonical_sha256(output)
    root = _external_component_root(output_root, version, "operator")
    _write_immutable(root / "population.json", population)
    _write_immutable(root / "parent-baselines.json", parent_baselines)
    _write_immutable(root / "operator-panel.json", output)
    manifest = _component_manifest(root, component="operator")
    return {"schemaVersion": "stage5e7_v3_operator_panel_result_v1", "outputRoot": str(root), "operatorPanelSha256": output["operatorPanelSha256"], "populationSha256": population["populationSha256"], "parentCount": len(parents), "candidateCount": len(candidates), "manifestSha256": manifest["manifestSha256"]}


POLICY_A_NAME = "stage5e7_v2_like_corrected_archive_reducer"
POLICY_A = {
    "schemaVersion": "stage5e7_v2_like_archive_reducer_v1",
    "policyName": POLICY_A_NAME,
    "economicObjectives": [
        {
            "name": "riskAdjustedTerminalAdjustedReturn",
            "direction": "max",
            "definition": "totalTerminalAdjustedConservativeNetR / (1 + maxWindowDrawdownR)",
        },
        {"name": "maximumDrawdownR", "direction": "min"},
        {"name": "rawTradeSupport", "direction": "max", "capped": False},
        {"name": "structuralComplexity", "direction": "min"},
    ],
    "tradeSupport": {
        "minimumTotalTrades": 8,
        "minimumTradesPerWindow": 2,
        "capTrades": None,
        "role": "finite_eligibility_and_raw_pareto_objective",
    },
    "archive": {
        "cellCapacity": 8,
        "retention": "per_cell_pareto_fronts_then_deterministic_objective_order",
        "negativeNoveltyLane": False,
        "observationalLane": False,
    },
    "identity": {
        "candidatePopulation": "exact_repaired_64_candidate_population",
        "results": "exact_corrected_v3_admissible_result_set",
        "tieBreak": "candidateId",
    },
}
POLICY_A_SHA256 = canonical_sha256(POLICY_A)


def _policy_config(*, policy: Mapping[str, Any], reference_population: Mapping[str, Any], panel: Mapping[str, Any], seed: int) -> dict[str, Any]:
    config = {
        "schemaVersion": "stage5e7_v3_policy_ab_offline_reducer_config_v1",
        "harnessVersion": HARNESS_VERSION,
        "policyName": policy["policyName"],
        "frozenPolicy": _clone(policy, name="policy A/B frozen policy"),
        "policySha256": canonical_sha256(policy),
        # Kept solely as preregistration provenance.  Neither reducer uses RNG.
        "preregistrationSeed": int(seed),
        "referencePanelSha256": panel["referencePanelSha256"],
        "reducedPopulationSha256": reference_population["populationSha256"],
        "reducedCandidateCount": 64,
        "experiment": {
            "kind": "bounded_same_candidate_offline_archive_policy_comparison",
            "searchLaunched": False,
            "generations": 0,
            "breedingPerformed": False,
            "resultRootRequired": "one_exact_corrected_v3_admissible_result_set",
        },
        "correctedEvidence": {
            "prohibitedIntervalStart": PROHIBITED_INTERVAL_START,
            "reservedEvidencePermitted": False,
        },
        "oldArchive": {"reused": False, "allowedAsInput": False},
    }
    config["configSha256"] = canonical_sha256(config)
    return config


def build_policy_ab(
    *, reference_root: Path | str, output_root: Path | str, version: str, seed: int
) -> dict[str, Any]:
    """Preregister two deterministic reducers of one repaired 64-candidate panel."""

    panel, reference_population, _reference_path = _load_reference_panel(reference_root)
    policies = {
        "policy-a-v2-like-control": POLICY_A,
        "policy-b-v3-robust": QD_POLICY,
    }
    root = _external_component_root(output_root, version, "policy-ab")
    # There is deliberately one persisted population, with its original identity.
    # Policy labels must never change the candidate population being reduced.
    _write_immutable(root / "population.json", reference_population)
    policy_rows = []
    for directory, policy in policies.items():
        config = _policy_config(policy=policy, reference_population=reference_population, panel=panel, seed=seed)
        _write_immutable(root / directory / "config.json", config)
        policy_rows.append({"directory": directory, "policyName": policy["policyName"], "policySha256": config["policySha256"], "configSha256": config["configSha256"]})
    preregistration = {
        "schemaVersion": POLICY_AB_SCHEMA, "harnessVersion": HARNESS_VERSION, "seed": int(seed),
        "referencePanelSha256": panel["referencePanelSha256"], "reducedPopulationSha256": reference_population["populationSha256"], "reducedCandidateCount": 64, "policies": policy_rows,
        "comparisonMetrics": ["retained_count", "retained_cells", "robust_positive_retained_count", "robust_positive_retained_share", "negative_retained_share", "support", "drawdown", "diversity", "overlap"],
        "decisionRule": "descriptive_same_candidate_same_corrected_result_set_offline_archive_reduction_only",
        "prohibitedEvidence": {"start": PROHIBITED_INTERVAL_START, "reservedEvidencePermitted": False}, "launchAuthorized": False, "searchLaunched": False, "breedingPerformed": False,
    }
    preregistration["comparisonSha256"] = canonical_sha256(preregistration)
    _write_immutable(root / "comparison-manifest.json", preregistration)
    manifest = _component_manifest(root, component="policy-ab")
    return {"schemaVersion": "stage5e7_v3_policy_ab_result_v1", "outputRoot": str(root), "comparisonSha256": preregistration["comparisonSha256"], "manifestSha256": manifest["manifestSha256"], "launchAuthorized": False}


def build_canary_composition(*, canary_root: Path | str, output_root: Path | str, version: str) -> dict[str, Any]:
    """Reference, and audit, the repository-only generator-v3 canary without copying it."""

    from .temporal_generator_v3_reachability_canary import audit_generator_v3_reachability_canary

    audit = audit_generator_v3_reachability_canary(canary_root)
    composition = {
        "schemaVersion": "stage5e7_v3_generator_canary_composition_v1", "harnessVersion": HARNESS_VERSION,
        "repositoryOnlyCanary": {"module": "autoresearch.temporal_generator_v3_reachability_canary", "root": str(Path(canary_root).resolve()), "audit": audit, "duplicated": False},
        "runtimeActivationEvidence": {
            "requiredFields": ["candidateId", "programSha256", "operatorId", "applicationSha256", "windowId", "observationStreamSha256", "firedCount", "activationCount", "eligibleOpportunityCount", "runtimeResultSha256"],
            "gates": ["canary_audit_ok", "static_operator_application_audit_ok", "complete_corrected_window_coverage", "fired_count_positive_when_eligible", "activation_count_positive_when_claimed", "identity_bound_runtime_result"],
            "status": "deferred_until_later_runtime_evidence_campaign",
        },
        "marketEvidenceRead": False, "gatewayContacted": False,
    }
    composition["compositionSha256"] = canonical_sha256(composition)
    root = _external_component_root(output_root, version, "canary-composition")
    _write_immutable(root / "generator-v3-canary-composition.json", composition)
    manifest = _component_manifest(root, component="canary-composition")
    return {"schemaVersion": "stage5e7_v3_canary_composition_result_v1", "outputRoot": str(root), "compositionSha256": composition["compositionSha256"], "manifestSha256": manifest["manifestSha256"]}


def _load_operator_panel(path: Path | str) -> tuple[dict[str, Any], dict[str, Any], Path]:
    root = Path(path)
    if root.is_file(): root = root.parent
    panel = _read_object(root / "operator-panel.json", name="operator panel")
    panel_sha = _identity(panel, "operatorPanelSha256", name="operator panel")
    population = _read_object(root / "population.json", name="operator population")
    population_sha = _identity(population, "populationSha256", name="operator population")
    if panel.get("schemaVersion") != OPERATOR_PANEL_SCHEMA or panel.get("populationSha256") != population_sha:
        raise TemporalDiscoveryContractError("operator panel contract mismatch")
    panel["operatorPanelSha256"] = panel_sha; population["populationSha256"] = population_sha
    return panel, population, root


def _required_sha256(value: Any, *, name: str) -> str:
    digest = str(value or "")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise TemporalDiscoveryContractError(f"{name} must be a canonical sha256 digest")
    return digest


def _exact_task_manifest(authority: Mapping[str, Any]) -> dict[str, Any]:
    """Return the one closed task manifest derivable from an authority."""

    try:
        frozen = validate_authority(authority)
        tasks = build_task_matrix(frozen)
    except TemporalSearchContractError as exc:
        raise TemporalDiscoveryContractError(
            "invalid temporal-search authority used for Stage5E7-v3 panel analysis"
        ) from exc
    return {
        "schemaVersion": TEMPORAL_SEARCH_MANIFEST_SCHEMA,
        "authorityId": frozen["authorityId"],
        "taskCount": len(tasks),
        "tasks": tasks,
        "taskMatrixSha256": canonical_sha256(tasks),
    }


def _audit_bridge_manifest(root: Path) -> None:
    """Verify the bridge's immutable inventory before trusting its identities."""

    manifest = _read_object(root / "manifest.json", name="panel bridge manifest")
    supplied = _identity(manifest, "manifestSha256", name="panel bridge manifest")
    if manifest.get("schemaVersion") != "stage5e7_v3_finite_panel_bridge_manifest_v1":
        raise TemporalDiscoveryContractError("panel bridge manifest schema is not recognized")
    files = manifest.get("files")
    if not isinstance(files, list) or manifest.get("fileCount") != len(files):
        raise TemporalDiscoveryContractError("panel bridge manifest inventory is incomplete")
    expected: set[Path] = set()
    for index, row in enumerate(files):
        if not isinstance(row, Mapping):
            raise TemporalDiscoveryContractError("panel bridge manifest file entry must be an object")
        relative = Path(str(row.get("relativePath") or ""))
        if not relative.parts or relative.is_absolute() or ".." in relative.parts:
            raise TemporalDiscoveryContractError("panel bridge manifest path escapes its root")
        path = root / relative
        expected.add(path.resolve())
        if (
            not path.is_file()
            or path.stat().st_size != int(row.get("length", -1))
            or _file_sha256(path) != _required_sha256(row.get("sha256"), name=f"panel bridge manifest file {index} hash")
        ):
            raise TemporalDiscoveryContractError("panel bridge manifest file mismatch")
    actual = {
        path.resolve()
        for path in root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    if actual != expected or not supplied:
        raise TemporalDiscoveryContractError("panel bridge manifest inventory drift")


def _load_exact_panel_bridge(
    *,
    population: Mapping[str, Any],
    panel_bridge_root: Path | str,
    expected_panel_kind: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Load one immutable bridge and bind it to the exact panel population.

    The bridge is the frozen authority/calendar boundary for a finite panel.
    A result directory's self-reported authority is never sufficient: it must
    equal this content-audited bridge authority and its exact task matrix.
    """

    root = Path(panel_bridge_root)
    if root.is_file():
        root = root.parent
    root = root.resolve()
    _audit_bridge_manifest(root)

    expected_population = _clone(population, name="panel population for bridge binding")
    expected_population_sha = _identity(
        expected_population,
        "populationSha256",
        name="panel population for bridge binding",
    )
    expected_population["populationSha256"] = expected_population_sha
    source_population = _read_object(root / "source-population.json", name="panel bridge source population")
    source_population_sha = _identity(
        source_population,
        "populationSha256",
        name="panel bridge source population",
    )
    source_population["populationSha256"] = source_population_sha
    if source_population_sha != expected_population_sha or source_population != expected_population:
        raise TemporalDiscoveryContractError("panel bridge source population does not match the frozen analysis population")

    preparation = _read_object(root / "preparation.json", name="panel bridge preparation")
    stored_authority = _read_object(root / "authority.json", name="panel bridge authority")
    try:
        authority = validate_authority(stored_authority)
        rebuilt = build_authority(preparation)
    except TemporalSearchContractError as exc:
        raise TemporalDiscoveryContractError("panel bridge authority/preparation is invalid") from exc
    if authority != rebuilt:
        raise TemporalDiscoveryContractError("panel bridge authority does not exactly bind its preparation")
    source_candidates = source_population.get("candidates")
    if not isinstance(source_candidates, list) or not all(
        isinstance(item, Mapping) for item in source_candidates
    ):
        raise TemporalDiscoveryContractError("panel bridge source population candidates are missing")
    if [str(item.get("candidateId") or "") for item in source_candidates] != [
        str(item.get("candidateId") or "") for item in authority["candidates"]
    ]:
        raise TemporalDiscoveryContractError("panel bridge authority candidate order diverges from its source population")
    for source, candidate in zip(source_candidates, authority["candidates"], strict=True):
        if (
            not isinstance(source, Mapping)
            or candidate.get("sourceProfile") != source.get("sourceProfile")
            or candidate.get("sourceProfileSha256") != source.get("sourceProfileSha256")
        ):
            raise TemporalDiscoveryContractError("panel bridge authority profile diverges from its source population")

    expected_manifest = _exact_task_manifest(authority)
    bridge_matrix_authority = _read_object(
        root / "task-matrix" / "authority.json", name="panel bridge task-matrix authority"
    )
    bridge_manifest = _read_object(
        root / "task-matrix" / "task-manifest.json", name="panel bridge task manifest"
    )
    if bridge_matrix_authority != authority or bridge_manifest != expected_manifest:
        raise TemporalDiscoveryContractError("panel bridge task matrix does not exactly bind its authority")

    evaluation = _read_object(root / "evaluation-identity.json", name="panel bridge evaluation identity")
    evaluation_sha = _identity(
        evaluation, "evaluationIdentitySha256", name="panel bridge evaluation identity"
    )
    evaluation["evaluationIdentitySha256"] = evaluation_sha
    candidate_ids = [str(item.get("candidateId") or "") for item in authority["candidates"]]
    template_preparation_sha = _required_sha256(
        evaluation.get("templatePreparationSha256"),
        name="panel bridge template preparation identity",
    )
    if (
        evaluation.get("schemaVersion") != "stage5e7_v3_finite_panel_evaluation_identity_v1"
        or evaluation.get("panelKind") != expected_panel_kind
        or evaluation.get("sourcePopulationSha256") != expected_population_sha
        or evaluation.get("preparationSha256") != canonical_sha256(preparation)
        or evaluation.get("authorityId") != authority["authorityId"]
        or evaluation.get("effectiveWorkerContract") != authority["workerContract"]
        or evaluation.get("candidateIds") != candidate_ids
        or evaluation.get("conversionToCanonicalQD") is not False
        or evaluation.get("reservedEvidencePermitted") is not False
    ):
        raise TemporalDiscoveryContractError("panel bridge evaluation identity is not bound to the intended panel authority")

    campaign = _read_object(root / "campaign.json", name="panel bridge campaign")
    campaign_sha = _identity(campaign, "campaignSha256", name="panel bridge campaign")
    campaign["campaignSha256"] = campaign_sha
    if (
        campaign.get("schemaVersion") != "stage5e7_v3_finite_panel_execution_bridge_v1"
        or campaign.get("panelKind") != expected_panel_kind
        or campaign.get("sourcePopulationSha256") != expected_population_sha
        or campaign.get("templatePreparationSha256") != template_preparation_sha
        or campaign.get("preparationSha256") != canonical_sha256(preparation)
        or campaign.get("authorityId") != authority["authorityId"]
        or campaign.get("effectiveWorkerContract") != authority["workerContract"]
        or campaign.get("taskMatrixSha256") != expected_manifest["taskMatrixSha256"]
        or campaign.get("candidateCount") != len(candidate_ids)
        or campaign.get("windowCount") != len(authority["developmentWindows"])
        or campaign.get("taskCount") != len(expected_manifest["tasks"])
        or campaign.get("evaluationIdentitySha256") != evaluation_sha
        or campaign.get("canonicalQDConversion") != "prohibited"
    ):
        raise TemporalDiscoveryContractError("panel bridge campaign is not bound to the intended authority/task matrix")
    if len(authority["developmentWindows"]) < 2:
        raise TemporalDiscoveryContractError(
            "Stage5E7-v3 panel analysis requires at least two ordered development windows"
        )
    return authority, {
        "panelBridgeRoot": str(root),
        "panelKind": expected_panel_kind,
        "sourcePopulationSha256": expected_population_sha,
        "authorityId": authority["authorityId"],
        "taskMatrixSha256": expected_manifest["taskMatrixSha256"],
        "bridgeCampaignSha256": campaign_sha,
        "bridgeEvaluationIdentitySha256": evaluation_sha,
        "orderedDevelopmentWindows": _clone(
            authority["developmentWindows"], name="bound development windows"
        ),
        "taskCount": len(expected_manifest["tasks"]),
    }


def _load_bound_panel_results(
    *,
    population: Mapping[str, Any],
    result_root: Path | str,
    panel_bridge_root: Path | str,
    expected_panel_kind: str,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    """Verify result artifacts against every exact authority task before reading them."""

    authority, bridge_binding = _load_exact_panel_bridge(
        population=population,
        panel_bridge_root=panel_bridge_root,
        expected_panel_kind=expected_panel_kind,
    )
    expected_manifest = _exact_task_manifest(authority)
    root = Path(result_root).resolve()
    result_authority = _read_object(root / "authority.json", name="panel result authority")
    result_manifest = _read_object(root / "task-manifest.json", name="panel result task manifest")
    try:
        validated_result_authority = validate_authority(result_authority)
    except TemporalSearchContractError as exc:
        raise TemporalDiscoveryContractError("panel result authority is invalid") from exc
    if validated_result_authority != authority or result_manifest != expected_manifest:
        raise TemporalDiscoveryContractError(
            "panel results are not materialized from the exact frozen bridge authority/task matrix"
        )

    expected_by_task_id = {
        str(task["task_id"]): task for task in expected_manifest["tasks"]
    }
    raw_by_task_id: dict[str, dict[str, Any]] = {}
    for path in _result_files(root):
        material = _read_json(path, name="panel candidate/window result")
        task_id = str(material.get("job_id") or "")
        task = expected_by_task_id.get(task_id)
        if task is None or task_id in raw_by_task_id:
            raise TemporalDiscoveryContractError("panel results contain an unexpected or duplicate authority task")
        payload = task["payload"]
        required = {
            "task_kind": task["task_kind"],
            "job_id": payload["job_id"],
            "authority_id": payload["authority_id"],
            "candidate_id": payload["candidate_id"],
            "analysis_window_start": payload["analysis_window_start"],
            "analysis_window_end": payload["analysis_window_end"],
            "evidence_plan_id": payload["evidence_plan"]["plan_id"],
            "lake_window_semantic_sha256": payload["lake_window_semantic_sha256"],
            "shared_observation_stream_id": payload["shared_observation_stream_id"],
        }
        if any(material.get(key) != value for key, value in required.items()):
            raise TemporalDiscoveryContractError(
                "panel result does not bind to its exact frozen authority task"
            )
        raw_by_task_id[task_id] = material
    if set(raw_by_task_id) != set(expected_by_task_id):
        raise TemporalDiscoveryContractError(
            "panel results do not cover every intended candidate by ordered development window"
        )

    grouped = load_stage_results(root)
    expected_windows = [
        (str(item["analysisWindowStart"]), str(item["analysisWindowEnd"]))
        for item in authority["developmentWindows"]
    ]
    if set(grouped) != {str(item["candidateId"]) for item in authority["candidates"]}:
        raise TemporalDiscoveryContractError("panel result candidate coverage diverges from its exact authority")
    for candidate_id, windows in grouped.items():
        by_window = {
            (str(item.get("analysisWindowStart")), str(item.get("analysisWindowEnd"))): item
            for item in windows
        }
        if len(by_window) != len(windows) or set(by_window) != set(expected_windows):
            raise TemporalDiscoveryContractError(
                "panel result calendar coverage diverges from the frozen development windows: "
                + candidate_id
            )
        # ``load_stage_results`` sorts on timestamps for generic consumers.
        # Stage5E7-v3 policy support is bound to the frozen authority order, so
        # restore that order only after proving an exact one-to-one calendar map.
        grouped[candidate_id] = [by_window[key] for key in expected_windows]
    return grouped, bridge_binding


def _result_aggregate_by_candidate(
    population: Mapping[str, Any],
    result_root: Path | str,
    *,
    panel_bridge_root: Path | str,
    expected_panel_kind: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Load only exact v3 results from the intended frozen bridge task matrix."""

    grouped, bridge_binding = _load_bound_panel_results(
        population=population,
        result_root=result_root,
        panel_bridge_root=panel_bridge_root,
        expected_panel_kind=expected_panel_kind,
    )
    candidates: dict[str, Mapping[str, Any]] = {}
    for raw in population.get("candidates") or []:
        if not isinstance(raw, Mapping):
            raise TemporalDiscoveryContractError("panel population candidates must be objects")
        candidate_id = str(raw.get("candidateId") or "")
        if not candidate_id or candidate_id in candidates:
            raise TemporalDiscoveryContractError("panel population candidate identities must be unique and nonempty")
        _required_sha256(raw.get("programSha256"), name=f"panel candidate {candidate_id} program identity")
        candidates[candidate_id] = raw
    if set(grouped) != set(candidates):
        raise TemporalDiscoveryContractError("corrected result root must cover exactly the panel population")
    aggregates: dict[str, dict[str, Any]] = {}
    for candidate_id in sorted(candidates):
        candidate = candidates[candidate_id]
        expected_program = _required_sha256(
            candidate.get("programSha256"),
            name=f"panel candidate {candidate_id} program identity",
        )
        for index, window in enumerate(grouped[candidate_id]):
            if window.get("v3Admissible") is not True:
                raise TemporalDiscoveryContractError(
                    "operator/policy analysis requires v3-admissible corrected results for every candidate/window: "
                    f"{candidate_id}[{index}]"
                )
            observed_program = _required_sha256(
                window.get("programSha256"),
                name=f"corrected result {candidate_id}[{index}] program identity",
            )
            if observed_program != expected_program:
                raise TemporalDiscoveryContractError(
                    "corrected result program identity does not match its panel candidate: "
                    f"{candidate_id}[{index}]"
                )
        aggregate = _aggregate_candidate(candidate, grouped[candidate_id])
        if (
            aggregate.get("v3Admissible") is not True
            or aggregate.get("programSha256") != expected_program
        ):
            raise TemporalDiscoveryContractError(
                "corrected result aggregate diverges from its v3 panel program binding: "
                f"{candidate_id}"
            )
        aggregates[candidate_id] = aggregate
    return aggregates, bridge_binding


def _stripped_windows(aggregate: Mapping[str, Any]) -> list[dict[str, Any]]:
    result = []
    for raw in aggregate.get("windowRecords") or []:
        row = _clone(raw, name="no-op window record")
        row.pop("candidateId", None)
        result.append(row)
    return result


def _interval(values: Sequence[float], *, z: float) -> dict[str, float | None]:
    if not values:
        return {"mean": None, "lower": None, "upper": None, "n": 0}
    mean = statistics.fmean(values)
    if len(values) < 2:
        return {"mean": mean, "lower": None, "upper": None, "n": len(values)}
    radius = z * statistics.stdev(values) / math.sqrt(len(values))
    return {"mean": mean, "lower": mean - radius, "upper": mean + radius, "n": len(values)}


def analyze_operator_panel(
    *,
    operator_root: Path | str,
    corrected_result_root: Path | str,
    parent_corrected_result_root: Path | str,
    operator_panel_bridge_root: Path | str,
    parent_panel_bridge_root: Path | str,
    output_root: Path | str,
    version: str,
) -> dict[str, Any]:
    panel, population, _ = _load_operator_panel(operator_root)
    aggregates, operator_result_binding = _result_aggregate_by_candidate(
        population,
        corrected_result_root,
        panel_bridge_root=operator_panel_bridge_root,
        expected_panel_kind="operator",
    )
    parent_root = Path(operator_root)
    if parent_root.is_file(): parent_root = parent_root.parent
    parent_baselines = _read_object(parent_root / "parent-baselines.json", name="operator parent baselines")
    parent_sha = _identity(parent_baselines, "populationSha256", name="operator parent baselines")
    if panel.get("parentBaselinePopulationSha256") != parent_sha:
        raise TemporalDiscoveryContractError("operator panel parent baseline identity mismatch")
    parent_aggregates, parent_result_binding = _result_aggregate_by_candidate(
        parent_baselines,
        parent_corrected_result_root,
        panel_bridge_root=parent_panel_bridge_root,
        expected_panel_kind="operator_parent_baselines",
    )
    controls = {str(item["parentCandidateId"]): str(item["controlCandidateId"]) for item in panel.get("pairs") or []}
    candidates = {str(item["candidateId"]): item for item in population["candidates"]}
    failures = []
    deltas: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    parent_deltas: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    rows = []
    admitted = [item for item in panel.get("applications") or [] if item.get("disposition") == "admitted"]
    for item in admitted:
        parent_id, child_id = str(item["parentCandidateId"]), str(item["childCandidateId"])
        control_id = controls.get(parent_id)
        if not control_id:
            raise TemporalDiscoveryContractError("operator sibling has no paired no-op control")
        # Equality is enforced on the full reduced corrected result, excluding
        # the necessarily different candidate ID.  A mismatch invalidates causal comparison.
        parent_profile = next((row.get("sourceProfile") for row in parent_baselines["candidates"] if row.get("candidateId") == parent_id), None)
        if parent_profile is None or candidates[control_id]["sourceProfile"] != parent_profile:
            failures.append({"parentCandidateId": parent_id, "controlCandidateId": control_id, "reason": "control_source_profile_changed"})
        if _stripped_windows(aggregates[control_id]) != _stripped_windows(parent_aggregates[parent_id]):
            failures.append({"parentCandidateId": parent_id, "controlCandidateId": control_id, "reason": "no_op_result_equality_failed"})
        # Parent results are present only when the parent was independently in the
        # population.  The no-op's own canonical replay is the equality control;
        # it is compared to a duplicate source-profile control when available.
        for metric in ("worstWindowConservativeNetR", "totalConservativeNetR", "maxWindowDrawdownR", "totalTrades"):
            delta = float(aggregates[child_id][metric]) - float(aggregates[control_id][metric])
            deltas[str(item["operatorId"])][metric].append(delta)
            parent_deltas[parent_id][metric].append(delta)
        rows.append({"parentCandidateId": parent_id, "controlCandidateId": control_id, "childCandidateId": child_id, "operatorId": item["operatorId"], "plannedApplicationSha256": item["plannedApplicationSha256"], "childMinusControl": {metric: float(aggregates[child_id][metric]) - float(aggregates[control_id][metric]) for metric in ("worstWindowConservativeNetR", "totalConservativeNetR", "maxWindowDrawdownR", "totalTrades")}})
    if failures:
        raise TemporalDiscoveryContractError("no-op corrected-result equality failed: " + json.dumps(failures, sort_keys=True))
    m = max(1, len(deltas)); z = NormalDist().inv_cdf(1.0 - 0.05 / (2.0 * m))
    operator_rows = []
    for operator_id in sorted(deltas):
        metrics = {name: _interval(values, z=z) for name, values in deltas[operator_id].items()}
        robust = metrics["worstWindowConservativeNetR"]; total = metrics["totalConservativeNetR"]
        if robust["lower"] is not None and total["lower"] is not None and robust["lower"] > 0.0 and total["lower"] > 0.0:
            conclusion = "multiple_comparison_adjusted_positive_supported"
        elif robust["upper"] is not None and total["upper"] is not None and robust["upper"] < 0.0 and total["upper"] < 0.0:
            conclusion = "multiple_comparison_adjusted_negative_supported"
        else:
            conclusion = "inconclusive"
        operator_rows.append({"operatorId": operator_id, "intervals": metrics, "conclusion": conclusion})
    parent_rows = [
        {
            "parentCandidateId": parent_id,
            "intervals": {name: _interval(values, z=z) for name, values in sorted(metrics.items())},
        }
        for parent_id, metrics in sorted(parent_deltas.items())
    ]
    report = {"schemaVersion": "stage5e7_v3_operator_causal_analysis_v1", "operatorPanelSha256": panel["operatorPanelSha256"], "correctedResultRoot": str(Path(corrected_result_root).resolve()), "parentCorrectedResultRoot": str(Path(parent_corrected_result_root).resolve()), "operatorResultAuthorityBinding": operator_result_binding, "parentResultAuthorityBinding": parent_result_binding, "noOpResultEquality": "passed", "familywiseAlpha": 0.05, "comparisonCount": m, "normalCriticalValue": z, "pairedRows": rows, "parentClusters": parent_rows, "operatorClusters": operator_rows, "prohibitedEvidence": {"start": PROHIBITED_INTERVAL_START, "reservedEvidencePermitted": False}}
    report["analysisSha256"] = canonical_sha256(report)
    root = _external_component_root(output_root, version, "operator-analysis")
    _write_immutable(root / "operator-analysis.json", report)
    manifest = _component_manifest(root, component="operator-analysis")
    return {"schemaVersion": "stage5e7_v3_operator_analysis_result_v1", "outputRoot": str(root), "analysisSha256": report["analysisSha256"], "manifestSha256": manifest["manifestSha256"]}


def _require_v3_corrected_aggregates(aggregates: Mapping[str, Mapping[str, Any]]) -> None:
    invalid = sorted(candidate_id for candidate_id, row in aggregates.items() if row.get("v3Admissible") is not True)
    if invalid:
        raise TemporalDiscoveryContractError(
            "policy A/B requires corrected Stage5E7-v3 terminal-adjusted results for every candidate: "
            + ", ".join(invalid)
        )


def _policy_member(candidate: Mapping[str, Any], aggregate: Mapping[str, Any], *, minimum_total_trades: int, minimum_trades_per_window: int, cap_trades: int) -> dict[str, Any]:
    return {
        "candidateId": str(candidate["candidateId"]),
        "candidate": _clone(candidate, name="policy candidate"),
        "aggregate": _clone(aggregate, name="policy aggregate"),
        "descriptor": qd_behavior_descriptor(candidate, aggregate),
        "objectives": _objective_row(candidate, aggregate),
        "finiteDataValidity": _finite_data_validity(aggregate, minimum_total_trades=minimum_total_trades, minimum_trades_per_window=minimum_trades_per_window, cap_trades=cap_trades),
        "cappedTradeSupport": float(min(max(0, int(aggregate.get("totalTrades") or 0)), cap_trades)),
    }


def _a_is_eligible(member: Mapping[str, Any]) -> bool:
    validity = member["finiteDataValidity"]
    return bool(validity["isFiniteData"] and validity["passesSupportGate"] and validity["validForQuality"])


def _a_objectives(member: Mapping[str, Any]) -> tuple[float, float, float, float]:
    aggregate = member["aggregate"]
    drawdown = max(0.0, float(aggregate["maxWindowDrawdownR"]))
    terminal_return = float(aggregate["totalTerminalAdjustedConservativeNetR"])
    return (terminal_return / (1.0 + drawdown), drawdown, float(aggregate["totalTrades"]), float(member["objectives"]["structuralComplexity"]))


def _a_dominates(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_values, right_values = _a_objectives(left), _a_objectives(right)
    no_worse = left_values[0] >= right_values[0] and left_values[1] <= right_values[1] and left_values[2] >= right_values[2] and left_values[3] <= right_values[3]
    return no_worse and left_values != right_values


def _reduce_policy_a(members: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Old v2-like, finite-only archive reducer; it intentionally has no negative lane."""
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for member in members:
        if _a_is_eligible(member):
            grouped[str(member["descriptor"]["cellId"])].append(member)
    cells = []
    for cell_id in sorted(grouped):
        remaining = sorted(grouped[cell_id], key=lambda row: str(row["candidateId"]))
        retained: list[dict[str, Any]] = []
        front_index = 0
        while remaining and len(retained) < 8:
            front = [row for row in remaining if not any(other["candidateId"] != row["candidateId"] and _a_dominates(other, row) for other in remaining)]
            front.sort(key=lambda row: (-_a_objectives(row)[0], _a_objectives(row)[1], -_a_objectives(row)[2], _a_objectives(row)[3], str(row["candidateId"])))
            for row in front[: 8 - len(retained)]:
                retained.append({**_clone(row, name="policy A archive member"), "archiveLane": "finite_pareto", "paretoFront": front_index, "retentionReason": "v2_like_finite_pareto"})
            chosen = {str(row["candidateId"]) for row in front}
            remaining = [row for row in remaining if str(row["candidateId"]) not in chosen]
            front_index += 1
        cells.append({"cellId": cell_id, "descriptor": _clone(grouped[cell_id][0]["descriptor"], name="policy A descriptor"), "candidateCountBeforeCapacity": len(grouped[cell_id]), "members": sorted(retained, key=lambda row: str(row["candidateId"]))})
    return cells


def _reduce_policy_b(members: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    # This calls the canonical v3 reducer directly, preserving its quality,
    # observational, and max-one-negative-novelty lanes without reimplementation.
    return select_qd_archive([_clone(member, name="policy B archive member") for member in members], cell_capacity=4)


def _policy_summary(*, policy_name: str, policy_sha256: str, cells: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    retained = [member for cell in cells for member in cell.get("members") or []]
    candidate_ids = sorted(str(member["candidateId"]) for member in retained)
    robust_positive = [member for member in retained if float(member["aggregate"]["worstWindowConservativeNetR"]) > 0.0]
    negative = [member for member in retained if float(member["aggregate"]["worstWindowConservativeNetR"]) < 0.0]
    supports = [int(member["aggregate"]["totalTrades"]) for member in retained]
    drawdowns = [float(member["aggregate"]["maxWindowDrawdownR"]) for member in retained]
    behavior = {str(member["aggregate"]["fingerprintSha256"]) for member in retained}
    programs = {str(member["candidate"]["programSha256"]) for member in retained}
    evidence = {str(window.get("observationStreamSha256")) for member in retained for window in member["aggregate"].get("windowRecords") or []}
    count = len(retained)
    return {
        "policyIdentity": {"policyName": policy_name, "policySha256": policy_sha256},
        "retainedCandidateIds": candidate_ids,
        "retainedCount": count,
        "retainedCellCount": sum(bool(cell.get("members")) for cell in cells),
        "robustPositiveRetainedCount": len(robust_positive),
        "robustPositiveRetainedShare": len(robust_positive) / count if count else None,
        "negativeRetainedCount": len(negative),
        "negativeRetainedShare": len(negative) / count if count else None,
        "support": {"rawTotalTrades": sum(supports), "minimumRawTrades": min(supports, default=None), "medianRawTrades": statistics.median(supports) if supports else None},
        "drawdown": {"maximumR": max(drawdowns, default=None), "meanR": statistics.fmean(drawdowns) if drawdowns else None},
        "diversity": {"descriptorCellCount": len({str(member["descriptor"]["cellId"]) for member in retained}), "behaviorFingerprintCount": len(behavior), "programCount": len(programs), "evidenceCount": len(evidence)},
        "cells": _clone(list(cells), name="policy archive cells"),
    }


def analyze_policy_ab(
    *,
    policy_root: Path | str,
    corrected_result_root: Path | str,
    panel_bridge_root: Path | str,
    output_root: Path | str,
    version: str,
) -> dict[str, Any]:
    root = Path(policy_root)
    if root.is_file(): root = root.parent
    comparison = _read_object(root / "comparison-manifest.json", name="policy A/B comparison manifest")
    comparison_sha = _identity(comparison, "comparisonSha256", name="policy A/B comparison manifest")
    population = _read_object(root / "population.json", name="policy A/B reduced population")
    population_sha = _identity(population, "populationSha256", name="policy A/B reduced population")
    if population_sha != comparison.get("reducedPopulationSha256") or len(population.get("candidates") or []) != 64:
        raise TemporalDiscoveryContractError("policy A/B population must be the exact repaired 64-candidate population")
    manifest_policies = {str(row.get("directory")): row for row in comparison.get("policies") or [] if isinstance(row, Mapping)}
    for directory in ("policy-a-v2-like-control", "policy-b-v3-robust"):
        config = _read_object(root / directory / "config.json", name=f"{directory} config")
        config_sha = _identity(config, "configSha256", name=f"{directory} config")
        if config.get("reducedPopulationSha256") != population_sha:
            raise TemporalDiscoveryContractError("policy A/B config does not identify the shared reduced population")
        expected_policy, expected_sha = (
            (POLICY_A, POLICY_A_SHA256)
            if directory == "policy-a-v2-like-control"
            else (QD_POLICY, QD_POLICY_SHA256)
        )
        if (
            config.get("policyName") != expected_policy["policyName"]
            or config.get("policySha256") != expected_sha
            or config.get("frozenPolicy") != expected_policy
            or manifest_policies.get(directory, {}).get("configSha256") != config_sha
            or manifest_policies.get(directory, {}).get("policySha256") != expected_sha
        ):
            raise TemporalDiscoveryContractError("policy A/B config does not match its preregistered archive reducer")
    aggregates, result_binding = _result_aggregate_by_candidate(
        population,
        corrected_result_root,
        panel_bridge_root=panel_bridge_root,
        expected_panel_kind="repair_reference",
    )
    _require_v3_corrected_aggregates(aggregates)
    members_a = [_policy_member(candidate, aggregates[str(candidate["candidateId"])], minimum_total_trades=8, minimum_trades_per_window=2, cap_trades=max(1, int(aggregates[str(candidate["candidateId"])].get("totalTrades") or 0))) for candidate in population["candidates"]]
    members_b = [_policy_member(candidate, aggregates[str(candidate["candidateId"])], minimum_total_trades=8, minimum_trades_per_window=4, cap_trades=20) for candidate in population["candidates"]]
    a = _policy_summary(policy_name=POLICY_A_NAME, policy_sha256=POLICY_A_SHA256, cells=_reduce_policy_a(members_a))
    b = _policy_summary(policy_name=QD_POLICY_NAME, policy_sha256=QD_POLICY_SHA256, cells=_reduce_policy_b(members_b))
    a_ids, b_ids = set(a["retainedCandidateIds"]), set(b["retainedCandidateIds"])
    overlap = {"retainedIntersectionCount": len(a_ids & b_ids), "retainedUnionCount": len(a_ids | b_ids), "jaccardShare": len(a_ids & b_ids) / len(a_ids | b_ids) if a_ids | b_ids else None, "onlyPolicyA": sorted(a_ids - b_ids), "onlyPolicyB": sorted(b_ids - a_ids)}
    report = {"schemaVersion": "stage5e7_v3_policy_ab_analysis_v2", "comparisonSha256": comparison_sha, "reducedPopulationSha256": population_sha, "reducedCandidateCount": len(population["candidates"]), "correctedResultRoot": str(Path(corrected_result_root).resolve()), "correctedResultAuthorityBinding": result_binding, "correctedResultSetSha256": _result_set_sha256(load_stage_results(corrected_result_root)), "policyA": a, "policyB": b, "overlap": overlap, "differenceBMinusA": {"retainedCount": b["retainedCount"] - a["retainedCount"], "retainedCellCount": b["retainedCellCount"] - a["retainedCellCount"], "robustPositiveRetainedCount": b["robustPositiveRetainedCount"] - a["robustPositiveRetainedCount"], "robustPositiveRetainedShare": b["robustPositiveRetainedShare"] - a["robustPositiveRetainedShare"], "negativeRetainedShare": b["negativeRetainedShare"] - a["negativeRetainedShare"], "maximumDrawdownR": b["drawdown"]["maximumR"] - a["drawdown"]["maximumR"] if a["drawdown"]["maximumR"] is not None and b["drawdown"]["maximumR"] is not None else None}, "prohibitedEvidence": {"start": PROHIBITED_INTERVAL_START, "reservedEvidencePermitted": False}, "interpretation": "descriptive_same_candidate_same_corrected_result_set_offline_archive_policy_comparison_no_search_or_breeding"}
    report["analysisSha256"] = canonical_sha256(report)
    output = _external_component_root(output_root, version, "policy-ab-analysis")
    _write_immutable(output / "policy-ab-analysis.json", report)
    manifest = _component_manifest(output, component="policy-ab-analysis")
    return {"schemaVersion": "stage5e7_v3_policy_ab_analysis_result_v1", "outputRoot": str(output), "analysisSha256": report["analysisSha256"], "manifestSha256": manifest["manifestSha256"]}


def build_plan_only(*, surface: str, output_root: Path | str, version: str) -> dict[str, Any]:
    commands = {
        "freeze": ["temporal-graph-lab-freeze-evidence", "<existing-authority-and-output-arguments>"],
        "temporal-search": ["temporal-search", "<existing-preparation-and-output-arguments>"],
        "qd-supervisor": ["temporal-qd-supervisor", "<existing-config-and-output-arguments>"],
    }
    if surface not in commands: raise TemporalDiscoveryContractError("unknown plan-only surface")
    plan = {"schemaVersion": "stage5e7_v3_plan_only_v1", "surface": surface, "command": commands[surface], "executionPerformed": False, "workersStarted": False, "reservedEvidence": False, "prohibitedEvidence": {"start": PROHIBITED_INTERVAL_START}}
    plan["planSha256"] = canonical_sha256(plan)
    root = _external_component_root(output_root, version, f"plan-{surface}")
    _write_immutable(root / "plan-only.json", plan); manifest = _component_manifest(root, component=f"plan-{surface}")
    return {"schemaVersion": "stage5e7_v3_plan_only_result_v1", "outputRoot": str(root), "planSha256": plan["planSha256"], "manifestSha256": manifest["manifestSha256"]}


def audit_validation_root(*, output_root: Path | str, version: str) -> dict[str, Any]:
    parent = _external_component_root(output_root, version, "placeholder").parent
    if not parent.is_dir(): raise TemporalDiscoveryContractError("validation version root does not exist")
    rows = []
    for child in sorted(parent.iterdir()):
        if child.is_dir() and (child / "manifest.json").is_file(): rows.append(_audit_component(child))
    if not rows: raise TemporalDiscoveryContractError("validation version root has no component manifests")
    return {"schemaVersion": "stage5e7_v3_validation_root_audit_v1", "ok": True, "versionRoot": str(parent), "components": rows}


def _json_file(path: Path) -> dict[str, Any]: return _read_object(path, name="JSON input")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build/audit/analyze immutable Stage5E7-v3 validation panels; never starts search or workers.")
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--version", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build"); build_commands = build.add_subparsers(dest="build_command", required=True)
    repair = build_commands.add_parser("repair"); repair.add_argument("--old-archive", required=True, type=Path); repair.add_argument("--old-population", required=True, type=Path); repair.add_argument("--old-results", required=True, type=Path); repair.add_argument("--seed", required=True, type=int); repair.add_argument("--candidate-dossiers", type=Path)
    operator = build_commands.add_parser("operator"); operator.add_argument("--reference-root", required=True, type=Path); operator.add_argument("--catalog", required=True, type=Path); operator.add_argument("--validator-command-file", required=True, type=Path); operator.add_argument("--validator-timeout-seconds", default=60.0, type=float); operator.add_argument("--seed", required=True, type=int)
    policy = build_commands.add_parser("policy-ab"); policy.add_argument("--reference-root", required=True, type=Path); policy.add_argument("--seed", required=True, type=int)
    canary = build_commands.add_parser("canary-composition"); canary.add_argument("--canary-root", required=True, type=Path)
    audit = commands.add_parser("audit")
    analyze = commands.add_parser("analyze"); analyze_commands = analyze.add_subparsers(dest="analyze_command", required=True)
    op_analysis = analyze_commands.add_parser("operator"); op_analysis.add_argument("--operator-root", required=True, type=Path); op_analysis.add_argument("--corrected-results", required=True, type=Path); op_analysis.add_argument("--parent-corrected-results", required=True, type=Path); op_analysis.add_argument("--operator-panel-bridge", required=True, type=Path); op_analysis.add_argument("--parent-panel-bridge", required=True, type=Path)
    ab_analysis = analyze_commands.add_parser("policy-ab"); ab_analysis.add_argument("--policy-root", required=True, type=Path); ab_analysis.add_argument("--corrected-results", required=True, type=Path); ab_analysis.add_argument("--panel-bridge", required=True, type=Path)
    plan = commands.add_parser("plan"); plan.add_argument("surface", choices=("freeze", "temporal-search", "qd-supervisor"))
    args = parser.parse_args()
    if args.command == "audit": result = audit_validation_root(output_root=args.output_root, version=args.version)
    elif args.command == "plan": result = build_plan_only(surface=args.surface, output_root=args.output_root, version=args.version)
    elif args.command == "build" and args.build_command == "repair": result = build_repair_panel(old_archive_path=args.old_archive, old_population_path=args.old_population, old_result_root=args.old_results, output_root=args.output_root, version=args.version, seed=args.seed, candidate_dossiers_path=args.candidate_dossiers)
    elif args.command == "build" and args.build_command == "policy-ab": result = build_policy_ab(reference_root=args.reference_root, output_root=args.output_root, version=args.version, seed=args.seed)
    elif args.command == "build" and args.build_command == "canary-composition": result = build_canary_composition(canary_root=args.canary_root, output_root=args.output_root, version=args.version)
    elif args.command == "build":
        from .temporal_discovery_validation import SubprocessCandidateValidator
        command = json.loads(args.validator_command_file.read_text(encoding="utf-8"))
        if not isinstance(command, list) or not command or not all(isinstance(item, str) and item.strip() for item in command): raise TemporalDiscoveryContractError("validator command file must contain a non-empty string array")
        result = build_operator_panel(reference_root=args.reference_root, output_root=args.output_root, version=args.version, seed=args.seed, catalog=_json_file(args.catalog), validator=SubprocessCandidateValidator(command, timeout_seconds=args.validator_timeout_seconds))
    elif args.analyze_command == "operator": result = analyze_operator_panel(operator_root=args.operator_root, corrected_result_root=args.corrected_results, parent_corrected_result_root=args.parent_corrected_results, operator_panel_bridge_root=args.operator_panel_bridge, parent_panel_bridge_root=args.parent_panel_bridge, output_root=args.output_root, version=args.version)
    else: result = analyze_policy_ab(policy_root=args.policy_root, corrected_result_root=args.corrected_results, panel_bridge_root=args.panel_bridge, output_root=args.output_root, version=args.version)
    print(json.dumps(result, indent=2, sort_keys=True))


__all__ = ["analyze_operator_panel", "analyze_policy_ab", "audit_validation_root", "build_canary_composition", "build_operator_panel", "build_plan_only", "build_policy_ab", "build_repair_panel"]


if __name__ == "__main__":  # pragma: no cover
    main()
