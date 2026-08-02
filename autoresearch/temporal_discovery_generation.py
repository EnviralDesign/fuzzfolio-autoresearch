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
from .temporal_discovery_mutation import *
from .temporal_discovery_validation import *
from .temporal_discovery_artifacts import *

def generate_discovery(
    preparation: Mapping[str, Any],
    *,
    validator: CandidateValidatorProtocol,
    output_root: Path | str,
) -> dict[str, Any]:
    normalized = _normalize_preparation(preparation)
    target = normalized["generator"]["targetUniquePrograms"]
    de_novo_target = round(
        target * normalized["generator"]["deNovoFraction"]
    )
    seed_target = target - de_novo_target
    rng = random.Random(normalized["generator"]["seed"])
    seeds = normalized["seeds"]
    accepted: list[dict[str, Any]] = []
    journal: list[dict[str, Any]] = []
    programs: set[str] = set()
    mode_counts = {"de_novo": 0, "seed_derived": 0}
    authored_provenance = validator_provenance(
        validator,
        validation_contract=normalized["validation"],
    )

    for ordinal in range(normalized["generator"]["maxProposalAttempts"]):
        if len(accepted) >= target:
            break
        source_mode = (
            "de_novo"
            if mode_counts["de_novo"] < de_novo_target
            else "seed_derived"
        )
        if (
            source_mode == "de_novo"
            and mode_counts["seed_derived"] < seed_target
            and rng.random() > 0.7
        ):
            source_mode = "seed_derived"
        seed = seeds[rng.randrange(len(seeds))]
        count_range = normalized["generator"][
            "deNovoMutationCount"
            if source_mode == "de_novo"
            else "seedMutationCount"
        ]
        mutation_count = rng.randint(
            count_range["min"],
            count_range["max"],
        )
        profile, mutation_trace = _mutate_profile(
            seed["sourceProfile"],
            rng=rng,
            source_mode=source_mode,
            mutation_count=mutation_count,
            family_rotation=ordinal,
        )
        raw_sha = canonical_sha256(profile)
        provisional_id = (
            "proposal_"
            + raw_sha.removeprefix("sha256:")[:24]
        )
        report = validator.validate(
            candidate_id=provisional_id,
            source_profile=profile,
            expected_raw_source_profile_sha256=raw_sha,
        )
        entry: dict[str, Any] = {
            "proposalOrdinal": ordinal,
            "sourceMode": source_mode,
            "seedId": seed["seedId"],
            "rawSourceProfileSha256": raw_sha,
            "mutationCount": len(mutation_trace),
            "mutationFamilies": sorted(
                {item["family"] for item in mutation_trace}
            ),
            "mutations": mutation_trace,
            "validationStatus": report.get("status"),
            "validationReportSha256": report.get(
                "validationReportSha256"
            ),
            "issueCodes": sorted(
                {
                    str(item.get("code"))
                    for item in report.get("issues") or []
                    if isinstance(item, Mapping) and item.get("code")
                }
            ),
        }
        if report.get("candidateAcceptable") is not True:
            entry["disposition"] = "rejected"
            journal.append(entry)
            continue
        program_sha = _sha(
            report.get("programSha256"),
            name="validation programSha256",
        )
        entry["programSha256"] = program_sha
        if program_sha in programs:
            entry["disposition"] = "duplicate_program"
            journal.append(entry)
            continue
        candidate_id = (
            "td_"
            + program_sha.removeprefix("sha256:")[:28]
        )
        if not _CANDIDATE.fullmatch(candidate_id):
            raise TemporalDiscoveryContractError(
                "generated candidate ID is invalid"
            )
        programs.add(program_sha)
        mode_counts[source_mode] += 1
        candidate = {
            "candidateId": candidate_id,
            "sourceMode": source_mode,
            "seedId": seed["seedId"],
            "proposalOrdinal": ordinal,
            "sourceProfile": profile,
            "sourceProfileSha256": raw_sha,
            "profileSnapshotSha256": _sha(
                report.get("profileSnapshotSha256"),
                name="validation profileSnapshotSha256",
            ),
            "programSha256": program_sha,
            "validationReportSha256": _sha(
                report.get("validationReportSha256"),
                name="validation validationReportSha256",
            ),
            "mutationTrace": mutation_trace,
        }
        candidate["authoredValidationBinding"] = build_authored_validation_binding(
            raw_source_profile_sha256=raw_sha,
            validation=report,
            provenance=authored_provenance,
        )
        candidate["authoredValidationBindingSha256"] = candidate[
            "authoredValidationBinding"
        ]["authoredValidationBindingSha256"]
        candidate["authoredValidationBinding"].pop(
            "authoredValidationBindingSha256"
        )
        accepted.append(candidate)
        entry["authoredValidationBindingSha256"] = candidate[
            "authoredValidationBindingSha256"
        ]
        entry["candidateId"] = candidate_id
        entry["disposition"] = "accepted"
        journal.append(entry)

    if len(accepted) != target:
        raise TemporalDiscoveryGenerationExhausted(
            f"generated {len(accepted)} unique valid programs; target was {target}"
        )
    if mode_counts != {
        "de_novo": de_novo_target,
        "seed_derived": seed_target,
    }:
        raise TemporalDiscoveryGenerationExhausted(
            "generation did not satisfy the frozen source-mode allocation"
        )

    population = {
        "schemaVersion": TEMPORAL_DISCOVERY_POPULATION_SCHEMA,
        "preparationSha256": normalized["preparationSha256"],
        "generatorVersion": TEMPORAL_DISCOVERY_GENERATOR_VERSION,
        "targetUniquePrograms": target,
        "deNovoCount": mode_counts["de_novo"],
        "seedDerivedCount": mode_counts["seed_derived"],
        "candidateCount": len(accepted),
        "authoredValidationBindingRequired": True,
        "candidates": accepted,
    }
    population["populationSha256"] = canonical_sha256(population)
    generation_journal = {
        "schemaVersion": TEMPORAL_DISCOVERY_GENERATION_JOURNAL_SCHEMA,
        "preparationSha256": normalized["preparationSha256"],
        "proposalCount": len(journal),
        "acceptedCount": len(accepted),
        "rejectedCount": sum(
            item["disposition"] == "rejected" for item in journal
        ),
        "duplicateProgramCount": sum(
            item["disposition"] == "duplicate_program"
            for item in journal
        ),
        "entries": journal,
    }
    generation_journal["journalSha256"] = canonical_sha256(
        generation_journal
    )

    initial_ids = normalized["screening"]["initialWindowIds"]
    initial_preparation = _finite_preparation(
        normalized,
        candidates=accepted,
        window_ids=initial_ids,
        label_suffix="initial",
        max_tasks=normalized["bounds"]["maxInitialTasks"],
    )
    initial_authority = build_authority(initial_preparation)
    discovery_authority = {
        "schemaVersion": TEMPORAL_DISCOVERY_AUTHORITY_SCHEMA,
        "authorityLabel": normalized["authorityLabel"],
        "preparationSha256": normalized["preparationSha256"],
        "generator": normalized["generator"],
        "validation": normalized["validation"],
        "workerContract": normalized["workerContract"],
        "screening": normalized["screening"],
        "bounds": normalized["bounds"],
        "populationSha256": population["populationSha256"],
        "generationJournalSha256": generation_journal["journalSha256"],
        "initialAuthorityId": initial_authority["authorityId"],
        "executionPolicy": {
            "proposalGenerationPermitted": True,
            "workerMutationPermitted": False,
            "reservedEvidencePermitted": False,
            "selectionMode": "pareto_economic_plus_greedy_novelty",
            "progressiveWindowScreening": True,
        },
    }
    discovery_authority["authorityId"] = canonical_sha256(
        discovery_authority
    )

    root = Path(output_root)
    _write_immutable(
        root / "preparation.json",
        _clone(preparation, name="discovery preparation"),
    )
    _write_immutable(root / "population.json", population)
    _write_immutable(root / "generation-journal.json", generation_journal)
    _write_immutable(root / "discovery-authority.json", discovery_authority)
    _write_immutable(root / "initial" / "preparation.json", initial_preparation)
    _write_immutable(root / "initial" / "authority.json", initial_authority)
    manifest = _refresh_manifest(root, discovery_authority["authorityId"])
    return {
        "schemaVersion": "temporal_graph_discovery_generation_result_v1",
        "authorityId": discovery_authority["authorityId"],
        "populationSha256": population["populationSha256"],
        "candidateCount": len(accepted),
        "proposalCount": len(journal),
        "initialAuthorityId": initial_authority["authorityId"],
        "initialTaskCount": (
            len(accepted) * len(initial_ids)
        ),
        "manifestSha256": manifest["manifestSha256"],
    }




__all__ = ['generate_discovery']
