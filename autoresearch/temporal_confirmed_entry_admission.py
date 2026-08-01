"""Repository-only paired admission for the confirmed-entry operator.

The batch deliberately reads no market evidence.  It anchors every applicable
member of the admitted generator-v2 population, then resumes the exact RNG
stream after that population's final proposal and accepts the first additional
strictly applicable unique parents.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .temporal_discovery_base import (
    _CANDIDATE,
    TemporalDiscoveryContractError,
    TemporalDiscoveryGenerationExhausted,
    _clone,
    _sha,
    _write_immutable,
    canonical_sha256,
)
from .temporal_discovery_mutation import _mutate_profile
from .temporal_discovery_validation import (
    SubprocessCandidateValidator,
    _normalize_preparation,
)
from .temporal_operator_confirmed_entry import (
    OPERATOR_SPEC,
    ConfirmedEntryStructuralOperator,
    inspect_confirmed_entry_applicability,
    preview_confirmed_entry_plan,
)
from .temporal_search_generator_v2_admission import _LedgerValidator
from .temporal_search_policy_v2 import (
    GENERATOR_V2_VERSION,
    _repair_profile,
    inspect_management_reachability,
)
from .temporal_structural_operators import build_candidate_lineage

CONTINUATION_VERSION = "temporal_generator_v2_exact_rng_continuation_v1"
CONFIG_SCHEMA = "temporal_confirmed_entry_admission_config_v1"
JOURNAL_SCHEMA = "temporal_confirmed_entry_admission_journal_v1"
POPULATION_SCHEMA = "temporal_confirmed_entry_paired_population_v1"
SET_SCHEMA = "temporal_confirmed_entry_identity_set_v1"
MANIFEST_SCHEMA = "temporal_confirmed_entry_admission_manifest_v1"
RESULT_SCHEMA = "temporal_confirmed_entry_admission_result_v1"

TARGET_PAIR_COUNT = 128
TARGET_ANCHORED_COUNT = 34
TARGET_CONTINUATION_COUNT = 94
MAX_CONTINUATION_PROPOSALS = 32_768


def _read(path: Path, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"{name} root must be an object")
    return _clone(value, name=name)


def _file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def _write_manifest(root: Path, *, population_sha256: str) -> dict[str, Any]:
    files = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.name == "manifest.json":
            continue
        files.append(
            {
                "relativePath": path.relative_to(root).as_posix(),
                "length": path.stat().st_size,
                "sha256": _file_sha(path),
            }
        )
    manifest = {
        "schemaVersion": MANIFEST_SCHEMA,
        "populationSha256": population_sha256,
        "fileCount": len(files),
        "files": files,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write_immutable(root / "manifest.json", manifest)
    return manifest


def _audit_base_generator(root: Path) -> dict[str, Any]:
    """Audit the immutable admitted v2 artifact, including its legacy config shape."""

    config = _read(root / "config.json", name="base generator config")
    population = _read(root / "population.json", name="base generator population")
    journal = _read(root / "generation-journal.json", name="base generator journal")
    manifest = _read(root / "manifest.json", name="base generator manifest")
    identities: dict[str, str] = {}
    for name, payload, field in (
        ("config", config, "configSha256"),
        ("population", population, "populationSha256"),
        ("journal", journal, "journalSha256"),
        ("manifest", manifest, "manifestSha256"),
    ):
        supplied = str(payload.pop(field, ""))
        if canonical_sha256(payload) != supplied:
            raise TemporalDiscoveryContractError(
                f"base generator {name} identity mismatch"
            )
        payload[field] = supplied
        identities[field] = supplied
    if config.get("generatorVersion") != GENERATOR_V2_VERSION:
        raise TemporalDiscoveryContractError("base generator version mismatch")
    if (
        population.get("candidateCount") != 256
        or len(population.get("candidates") or []) != 256
    ):
        raise TemporalDiscoveryContractError(
            "base generator population must contain 256"
        )
    if len({item["programSha256"] for item in population["candidates"]}) != 256:
        raise TemporalDiscoveryContractError("base generator programs are not unique")
    if manifest.get("populationSha256") != identities["populationSha256"]:
        raise TemporalDiscoveryContractError(
            "base manifest population identity mismatch"
        )
    expected: set[Path] = set()
    for item in manifest.get("files") or []:
        path = root / str(item["relativePath"])
        expected.add(path.resolve())
        if (
            not path.is_file()
            or path.stat().st_size != int(item["length"])
            or _file_sha(path) != item["sha256"]
        ):
            raise TemporalDiscoveryContractError(
                f"base generator file mismatch: {path}"
            )
    actual = {
        path.resolve()
        for path in root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    if actual != expected:
        raise TemporalDiscoveryContractError("base generator artifact inventory drift")
    return {**identities, "candidateCount": 256}


def _proposal(
    *,
    rng: random.Random,
    ordinal: int,
    mode_counts: Mapping[str, int],
    targets: Mapping[str, int],
    seeds: Sequence[Mapping[str, Any]],
    parameters: Mapping[str, Any],
    continuation: bool,
) -> dict[str, Any]:
    incomplete = [mode for mode in sorted(targets) if mode_counts[mode] < targets[mode]]
    if incomplete:
        source_mode = incomplete[0]
        if len(incomplete) == len(targets):
            source_mode = "broad_seed_mutation" if ordinal % 2 == 0 else "seed_derived"
    elif continuation:
        source_mode = "broad_seed_mutation" if ordinal % 2 == 0 else "seed_derived"
    else:
        raise TemporalDiscoveryContractError(
            "base generator proposal requested after its allocation completed"
        )
    seed = seeds[rng.randrange(len(seeds))]
    count_key = (
        "broadMutationCount"
        if source_mode == "broad_seed_mutation"
        else "seedMutationCount"
    )
    count_range = parameters[count_key]
    mutation_count = rng.randint(int(count_range["min"]), int(count_range["max"]))
    profile, mutations = _mutate_profile(
        seed["sourceProfile"],
        rng=rng,
        source_mode=(
            "de_novo" if source_mode == "broad_seed_mutation" else "seed_derived"
        ),
        mutation_count=mutation_count,
        family_rotation=ordinal,
    )
    profile["description"] = (
        "Deterministically generated activation-aware temporal candidate; "
        f"sourceMode={source_mode}; mutationCount={len(mutations)}."
    )
    profile, repairs = _repair_profile(profile, parameters=parameters)
    reachability = inspect_management_reachability(profile)
    return {
        "proposalOrdinal": ordinal,
        "sourceMode": source_mode,
        "seedId": seed["seedId"],
        "profile": profile,
        "rawSourceProfileSha256": canonical_sha256(profile),
        "mutations": mutations,
        "activationAwareRepairs": repairs,
        "reachability": reachability,
    }


def _base_journal_row(proposal: Mapping[str, Any]) -> dict[str, Any]:
    reachability = proposal["reachability"]
    return {
        "proposalOrdinal": proposal["proposalOrdinal"],
        "sourceMode": proposal["sourceMode"],
        "seedId": proposal["seedId"],
        "rawSourceProfileSha256": proposal["rawSourceProfileSha256"],
        "mutations": proposal["mutations"],
        "activationAwareRepairs": proposal["activationAwareRepairs"],
        "reachabilitySha256": reachability["reachabilitySha256"],
        "reachabilityIssueCounts": reachability["issueCounts"],
    }


def _validation_fields(validation: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "candidateAcceptable": validation.get("candidateAcceptable"),
        "validationStatus": validation.get("status"),
        "validationReportSha256": validation.get("validationReportSha256"),
        "profileSnapshotSha256": validation.get("profileSnapshotSha256"),
        "validatedProgramSha256": validation.get("programSha256"),
        "issueCodes": sorted(
            str(item.get("code"))
            for item in validation.get("issues") or []
            if isinstance(item, Mapping) and item.get("code")
        ),
    }


def _accepted_candidate(
    proposal: Mapping[str, Any], validation: Mapping[str, Any]
) -> dict[str, Any]:
    program_sha = _sha(validation.get("programSha256"), name="program sha256")
    candidate_id = "td_" + program_sha.removeprefix("sha256:")[:28]
    if not _CANDIDATE.fullmatch(candidate_id):
        raise TemporalDiscoveryContractError("generated candidate ID is invalid")
    return {
        "candidateId": candidate_id,
        "sourceMode": proposal["sourceMode"],
        "seedId": proposal["seedId"],
        "proposalOrdinal": proposal["proposalOrdinal"],
        "sourceProfile": proposal["profile"],
        "sourceProfileSha256": proposal["rawSourceProfileSha256"],
        "profileSnapshotSha256": _sha(
            validation.get("profileSnapshotSha256"), name="profile snapshot sha256"
        ),
        "programSha256": program_sha,
        "validationReportSha256": _sha(
            validation.get("validationReportSha256"), name="validation report sha256"
        ),
        "mutationTrace": proposal["mutations"],
        "activationAwareRepairs": proposal["activationAwareRepairs"],
        "managementReachability": proposal["reachability"],
    }


def _validate_child(
    *,
    operator: ConfirmedEntryStructuralOperator,
    parent: Mapping[str, Any],
    plan: Mapping[str, Any],
    validator: SubprocessCandidateValidator,
    birth_ordinal: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    child = preview_confirmed_entry_plan(parent["sourceProfile"], plan)
    child_source_sha = canonical_sha256(child)
    provisional_id = f"confirmed_entry_birth_{birth_ordinal}"
    validation = validator.validate(
        candidate_id=provisional_id,
        source_profile=child,
        expected_raw_source_profile_sha256=child_source_sha,
    )
    return child, validation


def _pair(
    *,
    cohort: str,
    birth_ordinal: int,
    parent: Mapping[str, Any],
    plan: Mapping[str, Any],
    child: Mapping[str, Any],
    child_validation: Mapping[str, Any],
    operator: ConfirmedEntryStructuralOperator,
) -> dict[str, Any]:
    child_program_sha = _sha(
        child_validation.get("programSha256"), name="child program SHA-256"
    )
    transformed, application = operator.apply(
        parent["sourceProfile"],
        plan,
        parent_validated_program_sha256=parent["programSha256"],
        child_validated_program_sha256=child_program_sha,
    )
    if transformed != child:
        raise TemporalDiscoveryContractError(
            "validated child changed during final binding"
        )
    child_candidate_id = "ce_" + child_program_sha.removeprefix("sha256:")[:28]
    lineage = build_candidate_lineage(
        candidate_id=child_candidate_id,
        candidate_source_profile_sha256=canonical_sha256(child),
        candidate_validated_program_sha256=child_program_sha,
        generation_index=1,
        birth_ordinal=birth_ordinal,
        parent_candidate_ids=[str(parent["candidateId"])],
        parent_program_sha256s=[str(parent["programSha256"])],
        operator_id=operator.operator_id,
        operator_version=operator.operator_version,
        plan_sha256=plan["planSha256"],
        application_sha256=application["applicationSha256"],
    )
    pair_id = "pair_" + application["applicationSha256"].removeprefix("sha256:")[:28]
    return {
        "pairId": pair_id,
        "cohort": cohort,
        "birthOrdinal": birth_ordinal,
        "control": {
            "candidateId": parent["candidateId"],
            "sourceMode": parent["sourceMode"],
            "seedId": parent["seedId"],
            "proposalOrdinal": parent["proposalOrdinal"],
            "sourceProfile": parent["sourceProfile"],
            "sourceProfileSha256": parent["sourceProfileSha256"],
            "programSha256": parent["programSha256"],
            "profileSnapshotSha256": parent["profileSnapshotSha256"],
            "validationReportSha256": parent["validationReportSha256"],
        },
        "transformed": {
            "candidateId": child_candidate_id,
            "sourceProfile": child,
            "sourceProfileSha256": canonical_sha256(child),
            "programSha256": child_program_sha,
            "profileSnapshotSha256": _sha(
                child_validation.get("profileSnapshotSha256"),
                name="child profile snapshot SHA-256",
            ),
            "validationReportSha256": _sha(
                child_validation.get("validationReportSha256"),
                name="child validation report SHA-256",
            ),
        },
        "operatorPlan": plan,
        "operatorApplication": application,
        "lineage": lineage,
    }


def _identity_set(kind: str, values: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    payload = {
        "schemaVersion": SET_SCHEMA,
        "kind": kind,
        "count": len(values),
        "values": _clone(list(values), name=f"{kind} identity set"),
    }
    payload["setSha256"] = canonical_sha256(payload)
    return payload


def build_confirmed_entry_admission(
    *,
    source_preparation: Path | str,
    base_generator_root: Path | str,
    validator_command: Sequence[str],
    output_root: Path | str,
    validator_timeout_seconds: float = 60.0,
    target_pair_count: int = TARGET_PAIR_COUNT,
    target_continuation_count: int = TARGET_CONTINUATION_COUNT,
    max_continuation_proposals: int = MAX_CONTINUATION_PROPOSALS,
) -> dict[str, Any]:
    source_path = Path(source_preparation)
    base_root = Path(base_generator_root)
    root = Path(output_root)
    if target_pair_count != 128 or target_continuation_count != 94:
        raise TemporalDiscoveryContractError(
            "confirmed-entry admission counts are frozen"
        )
    if max_continuation_proposals != 32_768:
        raise TemporalDiscoveryContractError("continuation proposal ceiling is frozen")

    base_audit = _audit_base_generator(base_root)
    preparation = _normalize_preparation(_read(source_path, name="source preparation"))
    base_config = _read(base_root / "config.json", name="base generator config")
    base_population = _read(base_root / "population.json", name="base population")
    base_journal = _read(base_root / "generation-journal.json", name="base journal")
    if base_population["candidateCount"] != 256:
        raise TemporalDiscoveryContractError(
            "base generator population must contain 256"
        )

    parameters = _clone(base_config["parameters"], name="base generator parameters")
    targets = dict(parameters["sourceModeCounts"])
    seeds = sorted(preparation["seeds"], key=lambda item: str(item["seedId"]))
    rng = random.Random(int(parameters["seed"]))
    ledger = _LedgerValidator(base_journal)
    mode_counts = {key: 0 for key in targets}
    programs: set[str] = set()
    replayed_candidates: list[dict[str, Any]] = []
    replayed_journal: list[dict[str, Any]] = []

    # Replay the entire accepted generator prefix while retaining this exact RNG
    # object for the continuation proposal immediately after the last prefix row.
    for ordinal, expected in enumerate(base_journal["entries"]):
        proposal = _proposal(
            rng=rng,
            ordinal=ordinal,
            mode_counts=mode_counts,
            targets=targets,
            seeds=seeds,
            parameters=parameters,
            continuation=False,
        )
        row = _base_journal_row(proposal)
        if proposal["reachability"]["acceptable"] is not True:
            row["disposition"] = "static_reachability_rejected"
        else:
            validation = ledger.validate(
                candidate_id=(
                    "proposal_"
                    + proposal["rawSourceProfileSha256"].removeprefix("sha256:")[:24]
                ),
                source_profile=proposal["profile"],
                expected_raw_source_profile_sha256=proposal["rawSourceProfileSha256"],
            )
            row.update(_validation_fields(validation))
            if validation.get("candidateAcceptable") is not True:
                row["disposition"] = "fuzz_validator_rejected"
            else:
                program_sha = _sha(
                    validation.get("programSha256"), name="program sha256"
                )
                row["programSha256"] = program_sha
                if program_sha in programs:
                    row["disposition"] = "duplicate_program"
                else:
                    candidate = _accepted_candidate(proposal, validation)
                    programs.add(program_sha)
                    mode_counts[str(proposal["sourceMode"])] += 1
                    replayed_candidates.append(candidate)
                    row["candidateId"] = candidate["candidateId"]
                    row["disposition"] = "accepted"
        replayed_journal.append(row)
        if row != expected:
            raise TemporalDiscoveryContractError(
                f"base generator journal prefix diverged at proposal {ordinal}"
            )
    if (
        sorted(replayed_candidates, key=lambda item: item["candidateId"])
        != base_population["candidates"]
    ):
        raise TemporalDiscoveryContractError(
            "base generator population replay diverged"
        )
    if mode_counts != targets or len(programs) != 256:
        raise TemporalDiscoveryContractError("base generator prefix ended incompletely")

    operator = ConfirmedEntryStructuralOperator()
    validator = SubprocessCandidateValidator(
        validator_command, timeout_seconds=validator_timeout_seconds
    )
    pairs: list[dict[str, Any]] = []
    child_programs: set[str] = set()
    admission_rows: list[dict[str, Any]] = []
    anchored = [
        candidate
        for candidate in base_population["candidates"]
        if operator.enumerate_plans(candidate["sourceProfile"])
    ]
    if len(anchored) != TARGET_ANCHORED_COUNT:
        raise TemporalDiscoveryContractError(
            f"expected {TARGET_ANCHORED_COUNT} exact-population applicable parents, "
            f"observed {len(anchored)}"
        )
    for parent in sorted(
        base_population["candidates"], key=lambda item: item["candidateId"]
    ):
        applicability = inspect_confirmed_entry_applicability(parent["sourceProfile"])
        plans = operator.enumerate_plans(parent["sourceProfile"])
        if not plans:
            admission_rows.append(
                {
                    "cohort": "admitted_population_anchor",
                    "proposalOrdinal": parent["proposalOrdinal"],
                    "parentCandidateId": parent["candidateId"],
                    "parentProgramSha256": parent["programSha256"],
                    "planCount": 0,
                    "applicabilityIssueCodes": applicability["issueCodes"],
                    "applicabilityReportSha256": applicability["reportSha256"],
                    "disposition": "operator_inapplicable",
                }
            )
            continue
        plan = plans[0]
        child, validation = _validate_child(
            operator=operator,
            parent=parent,
            plan=plan,
            validator=validator,
            birth_ordinal=len(pairs),
        )
        if validation.get("candidateAcceptable") is not True:
            raise TemporalDiscoveryContractError(
                f"anchored child validator rejected {parent['candidateId']}"
            )
        child_program = _sha(
            validation.get("programSha256"), name="child program sha256"
        )
        if child_program in child_programs:
            raise TemporalDiscoveryContractError("anchored child program is duplicated")
        child_programs.add(child_program)
        pair = _pair(
            cohort="admitted_population_anchor",
            birth_ordinal=len(pairs),
            parent=parent,
            plan=plan,
            child=child,
            child_validation=validation,
            operator=operator,
        )
        pairs.append(pair)
        admission_rows.append(
            {
                "cohort": "admitted_population_anchor",
                "proposalOrdinal": parent["proposalOrdinal"],
                "parentCandidateId": parent["candidateId"],
                "parentProgramSha256": parent["programSha256"],
                "planCount": 1,
                "applicabilityIssueCodes": [],
                "applicabilityReportSha256": applicability["reportSha256"],
                "planSha256": plan["planSha256"],
                "childProgramSha256": child_program,
                "pairId": pair["pairId"],
                "disposition": "accepted_pair",
            }
        )

    continuation_start = len(base_journal["entries"])
    continuation_accepted = 0
    continuation_rows: list[dict[str, Any]] = []
    for offset in range(max_continuation_proposals):
        if continuation_accepted == target_continuation_count:
            break
        ordinal = continuation_start + offset
        proposal = _proposal(
            rng=rng,
            ordinal=ordinal,
            mode_counts=mode_counts,
            targets=targets,
            seeds=seeds,
            parameters=parameters,
            continuation=True,
        )
        row = {
            **_base_journal_row(proposal),
            "continuationOrdinal": offset,
        }
        if proposal["reachability"]["acceptable"] is not True:
            row["disposition"] = "static_reachability_rejected"
            continuation_rows.append(row)
            continue
        validation = validator.validate(
            candidate_id=(
                "continuation_"
                + proposal["rawSourceProfileSha256"].removeprefix("sha256:")[:24]
            ),
            source_profile=proposal["profile"],
            expected_raw_source_profile_sha256=proposal["rawSourceProfileSha256"],
        )
        row.update(_validation_fields(validation))
        if validation.get("candidateAcceptable") is not True:
            row["disposition"] = "validator_rejected"
            continuation_rows.append(row)
            continue
        parent = _accepted_candidate(proposal, validation)
        parent_program = parent["programSha256"]
        row["parentProgramSha256"] = parent_program
        if parent_program in programs:
            row["disposition"] = "duplicate_parent_program"
            continuation_rows.append(row)
            continue
        plans = operator.enumerate_plans(parent["sourceProfile"])
        applicability = inspect_confirmed_entry_applicability(parent["sourceProfile"])
        row["planCount"] = len(plans)
        row["applicabilityIssueCodes"] = applicability["issueCodes"]
        row["applicabilityReportSha256"] = applicability["reportSha256"]
        if not plans:
            row["disposition"] = "operator_inapplicable"
            continuation_rows.append(row)
            programs.add(parent_program)
            continue
        plan = plans[0]
        row["planSha256"] = plan["planSha256"]
        child, child_validation = _validate_child(
            operator=operator,
            parent=parent,
            plan=plan,
            validator=validator,
            birth_ordinal=len(pairs),
        )
        row["childValidation"] = _validation_fields(child_validation)
        if child_validation.get("candidateAcceptable") is not True:
            row["disposition"] = "child_validator_rejected"
            continuation_rows.append(row)
            programs.add(parent_program)
            continue
        child_program = _sha(
            child_validation.get("programSha256"), name="child program sha256"
        )
        row["childProgramSha256"] = child_program
        if child_program in child_programs:
            row["disposition"] = "duplicate_child_program"
            continuation_rows.append(row)
            programs.add(parent_program)
            continue
        pair = _pair(
            cohort="deterministic_continuation",
            birth_ordinal=len(pairs),
            parent=parent,
            plan=plan,
            child=child,
            child_validation=child_validation,
            operator=operator,
        )
        programs.add(parent_program)
        child_programs.add(child_program)
        pairs.append(pair)
        continuation_accepted += 1
        row["parentCandidateId"] = parent["candidateId"]
        row["pairId"] = pair["pairId"]
        row["disposition"] = "accepted_pair"
        continuation_rows.append(row)
    if continuation_accepted != target_continuation_count:
        raise TemporalDiscoveryGenerationExhausted(
            f"continuation found {continuation_accepted} additional applicable parents; "
            f"target was {target_continuation_count}"
        )
    if len(pairs) != target_pair_count:
        raise TemporalDiscoveryGenerationExhausted(
            f"paired corpus contains {len(pairs)} pairs; target was {target_pair_count}"
        )

    controls = [pair["control"] for pair in pairs]
    transformed = [pair["transformed"] for pair in pairs]
    applications = [pair["operatorApplication"] for pair in pairs]
    lineage = [pair["lineage"] for pair in pairs]
    plans = [pair["operatorPlan"] for pair in pairs]
    control_set = _identity_set("control_population", controls)
    transformed_set = _identity_set("transformed_population", transformed)
    application_set = _identity_set("operator_applications", applications)
    lineage_set = _identity_set("candidate_lineage", lineage)
    plan_set = _identity_set("operator_plans", plans)

    config = {
        "schemaVersion": CONFIG_SCHEMA,
        "continuationVersion": CONTINUATION_VERSION,
        "baseGeneratorVersion": GENERATOR_V2_VERSION,
        "baseConfigSha256": base_config["configSha256"],
        "basePopulationSha256": base_population["populationSha256"],
        "baseJournalSha256": base_journal["journalSha256"],
        "baseManifestSha256": base_audit["manifestSha256"],
        "sourcePreparationSha256": preparation["preparationSha256"],
        "continuationStartProposalOrdinal": continuation_start,
        "targetPairCount": target_pair_count,
        "anchoredPairCount": len(anchored),
        "targetAdditionalParentCount": target_continuation_count,
        "maximumContinuationProposals": max_continuation_proposals,
        "operatorSpecSha256": OPERATOR_SPEC["operatorSpecSha256"],
        "repairPolicySha256": canonical_sha256(
            {
                "generatorVersion": GENERATOR_V2_VERSION,
                "parameters": parameters,
                "repairPolicy": "activation_aware_repair_v2",
            }
        ),
        "validatorSchema": preparation["validation"]["validatorSchema"],
        "fuzzfolioCommit": preparation["validation"]["fuzzfolioCommit"],
        "workerContractSha256": preparation["workerContract"]["workerContractSha256"],
        "selectionInputs": ["strict_operator_applicability"],
        "marketEvidenceRead": False,
        "gatewayContacted": False,
    }
    config["configSha256"] = canonical_sha256(config)
    population = {
        "schemaVersion": POPULATION_SCHEMA,
        "configSha256": config["configSha256"],
        "pairCount": len(pairs),
        "controlCount": len(controls),
        "transformedCount": len(transformed),
        "cohortCounts": dict(sorted(Counter(pair["cohort"] for pair in pairs).items())),
        "controlPopulationSha256": control_set["setSha256"],
        "transformedPopulationSha256": transformed_set["setSha256"],
        "operatorPlanSetSha256": plan_set["setSha256"],
        "operatorApplicationSetSha256": application_set["setSha256"],
        "lineageSetSha256": lineage_set["setSha256"],
        "pairs": pairs,
    }
    population["populationSha256"] = canonical_sha256(population)
    all_rows = [*admission_rows, *continuation_rows]
    journal = {
        "schemaVersion": JOURNAL_SCHEMA,
        "configSha256": config["configSha256"],
        "basePrefixExact": True,
        "basePrefixProposalCount": len(replayed_journal),
        "basePrefixAcceptedCount": len(replayed_candidates),
        "continuationProposalCount": len(continuation_rows),
        "dispositionCounts": dict(
            sorted(Counter(row["disposition"] for row in all_rows).items())
        ),
        "entries": all_rows,
    }
    journal["journalSha256"] = canonical_sha256(journal)

    _write_immutable(root / "config.json", config)
    _write_immutable(root / "paired-population.json", population)
    _write_immutable(root / "control-population.json", control_set)
    _write_immutable(root / "transformed-population.json", transformed_set)
    _write_immutable(root / "operator-plans.json", plan_set)
    _write_immutable(root / "operator-applications.json", application_set)
    _write_immutable(root / "candidate-lineage.json", lineage_set)
    _write_immutable(root / "admission-journal.json", journal)
    manifest = _write_manifest(root, population_sha256=population["populationSha256"])
    return {
        "schemaVersion": RESULT_SCHEMA,
        "configSha256": config["configSha256"],
        "populationSha256": population["populationSha256"],
        "controlPopulationSha256": control_set["setSha256"],
        "transformedPopulationSha256": transformed_set["setSha256"],
        "operatorPlanSetSha256": plan_set["setSha256"],
        "operatorApplicationSetSha256": application_set["setSha256"],
        "lineageSetSha256": lineage_set["setSha256"],
        "journalSha256": journal["journalSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "pairCount": len(pairs),
        "anchoredPairCount": len(anchored),
        "continuationPairCount": continuation_accepted,
        "continuationProposalCount": len(continuation_rows),
        "marketEvidenceRead": False,
        "gatewayContacted": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-preparation", type=Path, required=True)
    parser.add_argument("--base-generator-root", type=Path, required=True)
    parser.add_argument("--validator-command-file", type=Path, required=True)
    parser.add_argument("--validator-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    command = json.loads(args.validator_command_file.read_text(encoding="utf-8"))
    if not isinstance(command, list) or not all(
        isinstance(value, str) for value in command
    ):
        raise TemporalDiscoveryContractError(
            "validator command file must be a string array"
        )
    print(
        json.dumps(
            build_confirmed_entry_admission(
                source_preparation=args.source_preparation,
                base_generator_root=args.base_generator_root,
                validator_command=command,
                output_root=args.output_root,
                validator_timeout_seconds=args.validator_timeout_seconds,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()


__all__ = ["build_confirmed_entry_admission"]
