"""Compact, immutable evaluation view of a rich temporal QD population."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .temporal_discovery_base import TemporalDiscoveryContractError, _clone, canonical_sha256


EVALUATION_POPULATION_SCHEMA = "temporal_qd_evaluation_population_v1"


def evaluation_population_path(population_path: Path | str) -> Path:
    return Path(population_path).with_name("evaluation-population.json")


def raw_file_sha256(path: Path | str) -> str:
    digest = hashlib.sha256()
    try:
        with Path(path).open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise TemporalDiscoveryContractError(
            f"could not read QD population bytes: {path}"
        ) from exc
    return "sha256:" + digest.hexdigest()


def is_optimized_pair_population(payload: Mapping[str, Any]) -> bool:
    """Recognize only the post-sidecar pair-population contract."""

    return (
        payload.get("schemaVersion") == "temporal_qd_generation_population_v3"
        and isinstance(payload.get("pairGenerationConfigSha256"), str)
        and payload.get("bidirectionalPairPolicy") is not None
    )


def _identity(payload: Mapping[str, Any], field: str, *, name: str) -> str:
    material = _clone(payload, name=name)
    supplied = material.pop(field, None)
    if not isinstance(supplied, str) or not supplied.startswith("sha256:"):
        raise TemporalDiscoveryContractError(f"{name} {field} is invalid")
    if canonical_sha256(material) != supplied:
        raise TemporalDiscoveryContractError(f"{name} identity mismatch")
    return supplied


def load_evaluation_population(
    *,
    population_path: Path | str,
    journal_path: Path | str | None = None,
) -> dict[str, Any]:
    """Load a compact sidecar and bind it to unchanged rich-population bytes.

    The raw file verification is intentionally streaming: callers can validate
    provenance without decoding the population-sized JSON document.
    """

    source = Path(population_path)
    sidecar = evaluation_population_path(source)
    try:
        payload = json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise TemporalDiscoveryContractError(
            f"could not read QD evaluation population: {sidecar}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("schemaVersion") != EVALUATION_POPULATION_SCHEMA:
        raise TemporalDiscoveryContractError("QD evaluation population schema is invalid")
    _identity(payload, "evaluationPopulationSha256", name="QD evaluation population")
    if payload.get("populationFileSha256") != raw_file_sha256(source):
        raise TemporalDiscoveryContractError(
            "QD evaluation population raw source file identity mismatch"
        )
    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or payload.get("candidateCount") != len(candidates):
        raise TemporalDiscoveryContractError("QD evaluation population candidate count mismatch")
    pair_policy = payload.get("bidirectionalPairPolicy")
    if not isinstance(pair_policy, Mapping) or payload.get("pairPolicySha256") != canonical_sha256(pair_policy):
        raise TemporalDiscoveryContractError("QD evaluation population pair policy identity mismatch")
    seen: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            raise TemporalDiscoveryContractError("QD evaluation population candidate is invalid")
        candidate_id = candidate.get("candidateId")
        profile = candidate.get("sourceProfile")
        if not isinstance(candidate_id, str) or not isinstance(profile, Mapping):
            raise TemporalDiscoveryContractError("QD evaluation population candidate lacks executable material")
        if candidate_id in seen:
            raise TemporalDiscoveryContractError("QD evaluation population candidate identities are not unique")
        seen.add(candidate_id)
        if canonical_sha256(profile) != candidate.get("sourceProfileSha256"):
            raise TemporalDiscoveryContractError("QD evaluation population profile identity mismatch")
        evidence_required = payload.get("predeclaredEvidenceContextSha256") is not None
        for field in (
            "candidateIdentitySha256",
            "programSha256",
            "sourceProfileSha256",
            "proposalEntrySha256",
        ):
            value = candidate.get(field)
            if not isinstance(value, str) or not value.startswith("sha256:"):
                raise TemporalDiscoveryContractError(
                    f"QD evaluation population candidate {field} is invalid"
                )
        evidence = candidate.get("canonicalEvidenceIdentitySha256")
        if evidence_required and (
            not isinstance(evidence, str) or not evidence.startswith("sha256:")
        ):
            raise TemporalDiscoveryContractError(
                "QD evaluation population candidate canonical evidence identity is invalid"
            )
    funnel_entries = payload.get("funnelEntries")
    if (
        not isinstance(funnel_entries, list)
        or payload.get("proposalAttempts") != len(funnel_entries)
    ):
        raise TemporalDiscoveryContractError("QD evaluation population proposal accounting mismatch")
    for ordinal, entry in enumerate(funnel_entries):
        if (
            not isinstance(entry, Mapping)
            or entry.get("proposalOrdinal") != ordinal
            or not isinstance(entry.get("entrySha256"), str)
            or not isinstance(entry.get("originKind"), str)
            or not isinstance(entry.get("disposition"), str)
        ):
            raise TemporalDiscoveryContractError("QD evaluation population funnel entry is invalid")
    if journal_path is not None:
        try:
            journal = json.loads(Path(journal_path).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise TemporalDiscoveryContractError(
                f"could not read QD generation journal: {journal_path}"
            ) from exc
        if not isinstance(journal, Mapping):
            raise TemporalDiscoveryContractError("QD generation journal is invalid")
        _identity(journal, "journalSha256", name="QD generation journal")
        for field in (
            "populationSha256",
            "configSha256",
            "policyName",
            "policySha256",
            "generationIndex",
            "evaluationPopulationSha256",
        ):
            projected = "pairGenerationConfigSha256" if field == "configSha256" else field
            if journal.get(field) != payload.get(projected):
                raise TemporalDiscoveryContractError(
                    f"QD evaluation population journal {field} binding mismatch"
                )
        if (
            payload.get("populationFileSha256") != journal.get("populationFileSha256")
            or not isinstance(journal.get("operatorImplementation"), Mapping)
            or payload.get("operatorImplementationSha256")
            != canonical_sha256(journal["operatorImplementation"])
            or payload.get("predeclaredEvidenceContextSha256")
            != journal.get("predeclaredEvidenceContextSha256")
        ):
            raise TemporalDiscoveryContractError(
                "QD evaluation population journal identity binding mismatch"
            )
        entries = journal.get("entrySha256s")
        if not isinstance(entries, list):
            raise TemporalDiscoveryContractError("QD generation journal entry identities are invalid")
        if (
            len(entries) != len(funnel_entries)
            or journal.get("proposalCount") != len(funnel_entries)
        ):
            raise TemporalDiscoveryContractError("QD evaluation population journal proposal count mismatch")
        for ordinal, entry in enumerate(funnel_entries):
            if entries[ordinal] != entry.get("entrySha256"):
                raise TemporalDiscoveryContractError(
                    "QD evaluation population journal funnel reference mismatch"
                )
        bindings = journal.get("evaluationCandidateBindings")
        if not isinstance(bindings, list) or journal.get("acceptedCount") != len(candidates):
            raise TemporalDiscoveryContractError("QD evaluation population journal accepted accounting mismatch")
        for candidate in candidates:
            ordinal = candidate.get("proposalOrdinal")
            if (
                isinstance(ordinal, bool)
                or not isinstance(ordinal, int)
                or ordinal < 0
                or ordinal >= len(funnel_entries)
            ):
                raise TemporalDiscoveryContractError(
                    "QD evaluation population candidate proposal ordinal is invalid"
                )
        expected_bindings = [
            {
                "candidateId": row["candidateId"],
                "proposalOrdinal": row["proposalOrdinal"],
                "proposalEntrySha256": row["proposalEntrySha256"],
                "candidateProjectionSha256": canonical_sha256(row),
            }
            for row in candidates
        ]
        if bindings != expected_bindings:
            raise TemporalDiscoveryContractError(
                "QD evaluation population journal candidate bindings mismatch"
            )
        accepted_ordinals: set[int] = set()
        for candidate, binding in zip(candidates, bindings, strict=True):
            ordinal = binding["proposalOrdinal"]
            if ordinal in accepted_ordinals:
                raise TemporalDiscoveryContractError("QD evaluation population accepted ordinals are not unique")
            accepted_ordinals.add(ordinal)
            funnel = funnel_entries[ordinal]
            funnel_candidate = funnel.get("candidate")
            if (
                funnel.get("disposition") != "accepted"
                or not isinstance(funnel_candidate, Mapping)
                or funnel_candidate.get("candidateId") != candidate["candidateId"]
                or funnel_candidate.get("sourceProfileSha256")
                != candidate["sourceProfileSha256"]
            ):
                raise TemporalDiscoveryContractError(
                    "QD evaluation population accepted funnel binding mismatch"
                )
        for candidate in candidates:
            ordinal = candidate.get("proposalOrdinal")
            if not isinstance(ordinal, int) or ordinal < 0 or ordinal >= len(entries) or entries[ordinal] != candidate.get("proposalEntrySha256"):
                raise TemporalDiscoveryContractError(
                    "QD evaluation population journal candidate reference mismatch"
                )
    return _clone(payload, name="QD evaluation population")


__all__ = [
    "EVALUATION_POPULATION_SCHEMA",
    "evaluation_population_path",
    "is_optimized_pair_population",
    "load_evaluation_population",
    "raw_file_sha256",
]
