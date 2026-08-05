"""Compact, immutable evaluation view of a rich temporal QD population."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .temporal_discovery_base import TemporalDiscoveryContractError, _clone, canonical_sha256
from .temporal_qd_g0_bootstrap import (
    verify_campaign_ledger,
    verify_g0_bootstrap_selection,
)


EVALUATION_POPULATION_SCHEMA = "temporal_qd_evaluation_population_v1"
ROTATING_COHORT_POPULATION_SCHEMA = "temporal_qd_rotating_cohort_population_v1"


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
        source_mode = candidate.get("sourceMode")
        seed_id = candidate.get("seedId")
        profile = candidate.get("sourceProfile")
        if not isinstance(candidate_id, str) or not candidate_id or not isinstance(profile, Mapping):
            raise TemporalDiscoveryContractError("QD evaluation population candidate lacks executable material")
        if not isinstance(source_mode, str) or not source_mode.strip():
            raise TemporalDiscoveryContractError(
                "QD evaluation population candidate sourceMode is invalid"
            )
        if not isinstance(seed_id, str) or not seed_id.strip():
            raise TemporalDiscoveryContractError(
                "QD evaluation population candidate seedId is invalid"
            )
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
    g0_bootstrap = payload.get("g0Bootstrap")
    g0_selected_ordinals: set[int] | None = None
    if g0_bootstrap is not None:
        if not isinstance(g0_bootstrap, Mapping) or set(g0_bootstrap) != {
            "constructionPoolIdentitySha256", "acceptedPoolSha256", "selectionSha256", "ledgerSha256"
        }:
            raise TemporalDiscoveryContractError("QD evaluation population G0 binding is invalid")
        base = source.parent / "g0-bootstrap"
        try:
            pool = json.loads((base / "accepted-pool.json").read_text(encoding="utf-8"))
            selection = json.loads((base / "selection.json").read_text(encoding="utf-8"))
            ledger = json.loads((base / "campaign-construction-ledger.json").read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise TemporalDiscoveryContractError("QD G0 bootstrap artifacts are unavailable") from exc
        verified_selection = verify_g0_bootstrap_selection(artifact=selection, accepted_pool=pool)
        verify_campaign_ledger(
            ledger=ledger,
            accepted_pool=pool,
            selected_reference_sha256s=[str(row["referenceSha256"]) for row in verified_selection["selected"]],
        )
        expected = {
            "constructionPoolIdentitySha256": pool.get("constructionPoolIdentitySha256"),
            "acceptedPoolSha256": pool.get("acceptedPoolSha256"),
            "selectionSha256": verified_selection.get("selectionSha256"),
            "ledgerSha256": ledger.get("ledgerSha256"),
        }
        if dict(g0_bootstrap) != expected:
            raise TemporalDiscoveryContractError("QD evaluation population G0 binding drift")
        g0_selected_ordinals = {int(row["proposalOrdinal"]) for row in verified_selection["selected"]}
    seen_funnel_ordinals: set[int] = set()
    for ordinal, entry in enumerate(funnel_entries):
        if (
            not isinstance(entry, Mapping)
            or isinstance(entry.get("proposalOrdinal"), bool)
            or not isinstance(entry.get("proposalOrdinal"), int)
            or entry.get("proposalOrdinal") in seen_funnel_ordinals
            or not isinstance(entry.get("entrySha256"), str)
            or not isinstance(entry.get("originKind"), str)
            or not isinstance(entry.get("disposition"), str)
        ):
            raise TemporalDiscoveryContractError("QD evaluation population funnel entry is invalid")
        seen_funnel_ordinals.add(int(entry["proposalOrdinal"]))
    if g0_selected_ordinals is not None and seen_funnel_ordinals != g0_selected_ordinals:
        raise TemporalDiscoveryContractError("QD evaluation population G0 funnel is not the selected subset")
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
        for entry_sha, entry in zip(entries, funnel_entries, strict=True):
            if entry_sha != entry.get("entrySha256"):
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
                or ordinal not in seen_funnel_ordinals
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
            funnel = next(row for row in funnel_entries if row["proposalOrdinal"] == ordinal)
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
            if not isinstance(ordinal, int) or not any(
                row["proposalOrdinal"] == ordinal and row["entrySha256"] == candidate.get("proposalEntrySha256")
                for row in funnel_entries
            ):
                raise TemporalDiscoveryContractError(
                    "QD evaluation population journal candidate reference mismatch"
                )
    return _clone(payload, name="QD evaluation population")


def hydrate_evaluation_candidate(
    candidate: Mapping[str, Any], *, proposal_root: Path | str
) -> dict[str, Any]:
    """Reopen the rich, immutable proposal behind one compact projection.

    Retained parents are already rich and pass through unchanged.  New pair
    proposals are hydrated from their own append-only proposal journal; this
    avoids decoding the population-sized source document during rotating
    cohort construction.
    """

    compact = _clone(candidate, name="QD evaluation candidate")
    if isinstance(compact.get("bidirectionalGenome"), Mapping):
        return compact
    ordinal = compact.get("proposalOrdinal")
    entry_sha = compact.get("proposalEntrySha256")
    if (
        isinstance(ordinal, bool)
        or not isinstance(ordinal, int)
        or ordinal < 0
        or not isinstance(entry_sha, str)
        or not entry_sha.startswith("sha256:")
    ):
        raise TemporalDiscoveryContractError(
            "QD compact candidate lacks an immutable proposal reference"
        )
    path = Path(proposal_root) / f"{ordinal:08d}.json"
    try:
        entry = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise TemporalDiscoveryContractError(
            f"could not read QD proposal journal entry: {path}"
        ) from exc
    if not isinstance(entry, Mapping):
        raise TemporalDiscoveryContractError("QD proposal journal entry is invalid")
    supplied = entry.get("entrySha256")
    material = {key: value for key, value in entry.items() if key != "entrySha256"}
    if supplied != entry_sha or canonical_sha256(material) != supplied:
        raise TemporalDiscoveryContractError("QD proposal journal entry identity mismatch")
    rich = entry.get("candidate")
    if not isinstance(rich, Mapping):
        raise TemporalDiscoveryContractError("QD accepted proposal lacks rich candidate material")
    for field in (
        "candidateId",
        "candidateIdentitySha256",
        "programSha256",
        "sourceProfileSha256",
    ):
        if rich.get(field) != compact.get(field):
            raise TemporalDiscoveryContractError(
                f"QD rich candidate {field} differs from its evaluation projection"
            )
    return _clone(rich, name="QD rich proposal candidate")


def build_rotating_cohort_population(
    *,
    candidates: Sequence[Mapping[str, Any]],
    generation_index: int,
    panel_id: str,
    cohort_role: str,
    rotating_evidence_sha256: str,
) -> dict[str, Any]:
    """Build a small executable population for parent/backfill campaigns.

    This schema is intentionally not a proposal population: it has no proposal
    ordinals or funnel entries, and therefore cannot inflate proposal counts.
    """

    if generation_index < 1 or not panel_id or not cohort_role:
        raise TemporalDiscoveryContractError("rotating cohort identity is invalid")
    rows = sorted(
        (_clone(row, name="rotating cohort candidate") for row in candidates),
        key=lambda row: str(row.get("candidateId")),
    )
    seen: set[str] = set()
    for row in rows:
        candidate_id = row.get("candidateId")
        profile = row.get("sourceProfile")
        if (
            not isinstance(candidate_id, str)
            or not candidate_id
            or candidate_id in seen
            or not isinstance(profile, Mapping)
            or canonical_sha256(profile) != row.get("sourceProfileSha256")
        ):
            raise TemporalDiscoveryContractError(
                "rotating cohort contains invalid or duplicate candidate material"
            )
        for field in ("candidateIdentitySha256", "programSha256"):
            value = row.get(field)
            if not isinstance(value, str) or not value.startswith("sha256:"):
                raise TemporalDiscoveryContractError(
                    f"rotating cohort candidate {field} is invalid"
                )
        seen.add(candidate_id)
    output = {
        "schemaVersion": ROTATING_COHORT_POPULATION_SCHEMA,
        "generationIndex": generation_index,
        "panelId": panel_id,
        "cohortRole": cohort_role,
        "rotatingEvidenceSha256": rotating_evidence_sha256,
        "candidateCount": len(rows),
        "candidates": rows,
        "proposalPopulation": False,
    }
    output["populationSha256"] = canonical_sha256(output)
    return output


__all__ = [
    "EVALUATION_POPULATION_SCHEMA",
    "ROTATING_COHORT_POPULATION_SCHEMA",
    "build_rotating_cohort_population",
    "evaluation_population_path",
    "hydrate_evaluation_candidate",
    "is_optimized_pair_population",
    "load_evaluation_population",
    "raw_file_sha256",
]
