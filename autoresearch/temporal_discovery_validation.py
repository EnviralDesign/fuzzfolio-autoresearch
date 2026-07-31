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

class SubprocessCandidateValidator:
    """Call the FuzzFolio-owned validator without duplicating its grammar."""

    def __init__(
        self,
        command: Sequence[str],
        *,
        timeout_seconds: float = 30.0,
    ) -> None:
        if not command or any(not str(part).strip() for part in command):
            raise TemporalDiscoveryContractError(
                "validator command must be a non-empty argument array"
            )
        self.command = tuple(str(part) for part in command)
        self.timeout_seconds = _number(
            timeout_seconds,
            name="validator timeout",
            minimum=1.0,
            maximum=300.0,
        )

    def validate(
        self,
        *,
        candidate_id: str,
        source_profile: Mapping[str, Any],
        expected_raw_source_profile_sha256: str,
    ) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(
            prefix="temporal-discovery-validation-"
        ) as temporary:
            profile_path = Path(temporary) / "profile.json"
            profile_path.write_text(
                json.dumps(
                    dict(source_profile),
                    indent=2,
                    sort_keys=True,
                    ensure_ascii=True,
                    allow_nan=False,
                )
                + "\n",
                encoding="utf-8",
            )
            command = [
                *self.command,
                "--profile",
                str(profile_path),
                "--candidate-id",
                candidate_id,
                "--expected-raw-sha256",
                expected_raw_source_profile_sha256,
            ]
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
                env=dict(os.environ),
            )
        try:
            report = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise TemporalDiscoveryContractError(
                "candidate validator did not return one JSON document; "
                f"exit={completed.returncode}; stderr={completed.stderr.strip()!r}"
            ) from exc
        report = _mapping(report, name="candidate validation report")
        if report.get("schemaVersion") != TEMPORAL_SEARCH_VALIDATION_SCHEMA:
            raise TemporalDiscoveryContractError(
                "candidate validator returned an unknown schema"
            )
        if completed.returncode not in {0, 2}:
            raise TemporalDiscoveryContractError(
                "candidate validator failed operationally; "
                f"exit={completed.returncode}; stderr={completed.stderr.strip()!r}"
            )
        if completed.returncode == 0 and report.get("candidateAcceptable") is not True:
            raise TemporalDiscoveryContractError(
                "validator exit code and acceptance disagree"
            )
        if completed.returncode == 2 and report.get("candidateAcceptable") is not False:
            raise TemporalDiscoveryContractError(
                "validator rejection exit code and report disagree"
            )
        return report


def _normalize_preparation(raw: Mapping[str, Any]) -> dict[str, Any]:
    payload = _mapping(raw, name="discovery preparation")
    required = {
        "schemaVersion",
        "authorityLabel",
        "generator",
        "validation",
        "workerContract",
        "instrument",
        "timeframe",
        "barLimit",
        "seeds",
        "developmentWindows",
        "evidencePlanTemplates",
        "prohibitedEvidence",
        "screening",
        "bounds",
    }
    if set(payload) != required:
        raise TemporalDiscoveryContractError(
            f"discovery preparation must contain exactly {sorted(required)!r}"
        )
    if payload["schemaVersion"] != TEMPORAL_DISCOVERY_PREPARATION_SCHEMA:
        raise TemporalDiscoveryContractError(
            "unknown discovery preparation schema"
        )

    generator = _mapping(payload["generator"], name="generator")
    generator_keys = {
        "seed",
        "targetUniquePrograms",
        "deNovoFraction",
        "maxProposalAttempts",
        "deNovoMutationCount",
        "seedMutationCount",
    }
    if set(generator) != generator_keys:
        raise TemporalDiscoveryContractError(
            "generator has a closed schema"
        )
    de_novo_count = _mapping(
        generator["deNovoMutationCount"],
        name="generator.deNovoMutationCount",
    )
    seed_count = _mapping(
        generator["seedMutationCount"],
        name="generator.seedMutationCount",
    )
    if set(de_novo_count) != {"min", "max"} or set(seed_count) != {"min", "max"}:
        raise TemporalDiscoveryContractError(
            "mutation-count ranges require min and max"
        )
    normalized_generator = {
        "version": TEMPORAL_DISCOVERY_GENERATOR_VERSION,
        "seed": _integer(
            generator["seed"],
            name="generator.seed",
            minimum=0,
            maximum=2**63 - 1,
        ),
        "targetUniquePrograms": _integer(
            generator["targetUniquePrograms"],
            name="generator.targetUniquePrograms",
            minimum=1,
            maximum=100_000,
        ),
        "deNovoFraction": _number(
            generator["deNovoFraction"],
            name="generator.deNovoFraction",
            minimum=0.0,
            maximum=1.0,
        ),
        "maxProposalAttempts": _integer(
            generator["maxProposalAttempts"],
            name="generator.maxProposalAttempts",
            minimum=1,
            maximum=1_000_000,
        ),
        "deNovoMutationCount": {
            "min": _integer(
                de_novo_count["min"],
                name="generator.deNovoMutationCount.min",
                minimum=1,
                maximum=32,
            ),
            "max": _integer(
                de_novo_count["max"],
                name="generator.deNovoMutationCount.max",
                minimum=1,
                maximum=32,
            ),
        },
        "seedMutationCount": {
            "min": _integer(
                seed_count["min"],
                name="generator.seedMutationCount.min",
                minimum=1,
                maximum=16,
            ),
            "max": _integer(
                seed_count["max"],
                name="generator.seedMutationCount.max",
                minimum=1,
                maximum=16,
            ),
        },
    }
    for key in ("deNovoMutationCount", "seedMutationCount"):
        if normalized_generator[key]["min"] > normalized_generator[key]["max"]:
            raise TemporalDiscoveryContractError(
                f"generator.{key}.min must not exceed max"
            )

    validation = _mapping(payload["validation"], name="validation")
    if set(validation) != {"validatorSchema", "fuzzfolioCommit"}:
        raise TemporalDiscoveryContractError(
            "validation must bind validatorSchema and fuzzfolioCommit"
        )
    normalized_validation = {
        "validatorSchema": _safe(
            validation["validatorSchema"],
            name="validation.validatorSchema",
        ),
        "fuzzfolioCommit": str(validation["fuzzfolioCommit"] or "").strip(),
    }
    if normalized_validation["validatorSchema"] != TEMPORAL_SEARCH_VALIDATION_SCHEMA:
        raise TemporalDiscoveryContractError(
            "discovery requires the admitted temporal search validator schema"
        )
    if not re.fullmatch(r"[0-9a-f]{40}", normalized_validation["fuzzfolioCommit"]):
        raise TemporalDiscoveryContractError(
            "validation.fuzzfolioCommit must be an exact commit SHA"
        )

    worker = _mapping(payload["workerContract"], name="workerContract")
    if set(worker) != {"workerContractSha256", "workerContractSchema"}:
        raise TemporalDiscoveryContractError(
            "workerContract has a closed schema"
        )
    normalized_worker = {
        "workerContractSha256": _sha(
            worker["workerContractSha256"],
            name="workerContract.workerContractSha256",
        ),
        "workerContractSchema": _safe(
            worker["workerContractSchema"],
            name="workerContract.workerContractSchema",
        ),
    }

    instrument = str(payload["instrument"] or "").strip().upper()
    timeframe = str(payload["timeframe"] or "").strip().upper()
    if not instrument or not timeframe:
        raise TemporalDiscoveryContractError(
            "instrument and timeframe are required"
        )
    bar_limit = _integer(
        payload["barLimit"],
        name="barLimit",
        minimum=10,
        maximum=1_000_000,
    )

    seeds_raw = payload["seeds"]
    if not isinstance(seeds_raw, list) or not seeds_raw:
        raise TemporalDiscoveryContractError("seeds must be non-empty")
    seeds: list[dict[str, Any]] = []
    for index, raw_seed in enumerate(seeds_raw):
        seed = _mapping(raw_seed, name=f"seeds[{index}]")
        if set(seed) != {"seedId", "sourceProfile"}:
            raise TemporalDiscoveryContractError(
                "seed entries require seedId and sourceProfile"
            )
        profile = _ensure_explicit_management(
            _mapping(seed["sourceProfile"], name=f"seeds[{index}].sourceProfile")
        )
        instruments = profile.get("instruments")
        if instruments != [instrument]:
            raise TemporalDiscoveryContractError(
                f"seed {seed['seedId']!r} does not bind the declared instrument"
            )
        seeds.append(
            {
                "seedId": _safe(seed["seedId"], name=f"seeds[{index}].seedId"),
                "sourceProfile": profile,
                "sourceProfileSha256": canonical_sha256(profile),
            }
        )
    if len({seed["seedId"] for seed in seeds}) != len(seeds):
        raise TemporalDiscoveryContractError("seed IDs must be unique")

    windows_raw = payload["developmentWindows"]
    if not isinstance(windows_raw, list) or len(windows_raw) < 2:
        raise TemporalDiscoveryContractError(
            "at least two development windows are required"
        )
    windows = [_clone(item, name="development window") for item in windows_raw]
    window_ids = [str(item.get("windowId") or "") for item in windows]
    if any(not token for token in window_ids) or len(set(window_ids)) != len(window_ids):
        raise TemporalDiscoveryContractError(
            "development window IDs must be non-empty and unique"
        )

    templates_raw = payload["evidencePlanTemplates"]
    if not isinstance(templates_raw, list):
        raise TemporalDiscoveryContractError(
            "evidencePlanTemplates must be a list"
        )
    templates: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(templates_raw):
        current = _mapping(item, name=f"evidencePlanTemplates[{index}]")
        if set(current) != {"windowId", "evidencePlan"}:
            raise TemporalDiscoveryContractError(
                "evidence plan templates require windowId and evidencePlan"
            )
        window_id = str(current["windowId"] or "")
        if window_id in templates:
            raise TemporalDiscoveryContractError(
                "evidence plan template window IDs must be unique"
            )
        plan = _mapping(
            current["evidencePlan"],
            name=f"evidencePlanTemplates[{index}].evidencePlan",
        )
        if plan.get("schema_version") != "fuzzfolio.replay-evidence-plan.v2":
            raise TemporalDiscoveryContractError(
                "discovery requires replay evidence plan v2 templates"
            )
        templates[window_id] = plan
    if set(templates) != set(window_ids):
        raise TemporalDiscoveryContractError(
            "evidence plan templates must exactly cover development windows"
        )

    screening = _mapping(payload["screening"], name="screening")
    screening_keys = {
        "initialWindowIds",
        "confirmationWindowIds",
        "economicArchiveSize",
        "noveltyArchiveSize",
        "confirmationCandidateCap",
        "minimumTradesPerInitialWindowEconomic",
        "minimumTotalTradesNovelty",
        "finalEconomicArchiveSize",
        "finalNoveltyArchiveSize",
    }
    if set(screening) != screening_keys:
        raise TemporalDiscoveryContractError(
            "screening has a closed schema"
        )
    initial_ids = [str(value) for value in screening["initialWindowIds"]]
    confirmation_ids = [str(value) for value in screening["confirmationWindowIds"]]
    if (
        not initial_ids
        or not confirmation_ids
        or set(initial_ids) & set(confirmation_ids)
        or set(initial_ids) | set(confirmation_ids) != set(window_ids)
    ):
        raise TemporalDiscoveryContractError(
            "initial and confirmation windows must be a disjoint full partition"
        )
    normalized_screening = {
        "version": TEMPORAL_DISCOVERY_SELECTION_VERSION,
        "initialWindowIds": initial_ids,
        "confirmationWindowIds": confirmation_ids,
        "economicArchiveSize": _integer(
            screening["economicArchiveSize"],
            name="screening.economicArchiveSize",
            minimum=1,
            maximum=100_000,
        ),
        "noveltyArchiveSize": _integer(
            screening["noveltyArchiveSize"],
            name="screening.noveltyArchiveSize",
            minimum=1,
            maximum=100_000,
        ),
        "confirmationCandidateCap": _integer(
            screening["confirmationCandidateCap"],
            name="screening.confirmationCandidateCap",
            minimum=1,
            maximum=100_000,
        ),
        "minimumTradesPerInitialWindowEconomic": _integer(
            screening["minimumTradesPerInitialWindowEconomic"],
            name="screening.minimumTradesPerInitialWindowEconomic",
            minimum=0,
            maximum=1_000_000,
        ),
        "minimumTotalTradesNovelty": _integer(
            screening["minimumTotalTradesNovelty"],
            name="screening.minimumTotalTradesNovelty",
            minimum=0,
            maximum=1_000_000,
        ),
        "finalEconomicArchiveSize": _integer(
            screening["finalEconomicArchiveSize"],
            name="screening.finalEconomicArchiveSize",
            minimum=1,
            maximum=100_000,
        ),
        "finalNoveltyArchiveSize": _integer(
            screening["finalNoveltyArchiveSize"],
            name="screening.finalNoveltyArchiveSize",
            minimum=1,
            maximum=100_000,
        ),
    }

    bounds = _mapping(payload["bounds"], name="bounds")
    bound_keys = {
        "maxCandidates",
        "maxInitialTasks",
        "maxConfirmationCandidates",
        "maxConfirmationTasks",
        "maxTotalTasks",
        "maxAttempts",
        "deadlineSeconds",
    }
    if set(bounds) != bound_keys:
        raise TemporalDiscoveryContractError("bounds has a closed schema")
    normalized_bounds = {
        "maxCandidates": _integer(
            bounds["maxCandidates"],
            name="bounds.maxCandidates",
            minimum=1,
            maximum=100_000,
        ),
        "maxInitialTasks": _integer(
            bounds["maxInitialTasks"],
            name="bounds.maxInitialTasks",
            minimum=1,
            maximum=1_000_000,
        ),
        "maxConfirmationCandidates": _integer(
            bounds["maxConfirmationCandidates"],
            name="bounds.maxConfirmationCandidates",
            minimum=1,
            maximum=100_000,
        ),
        "maxConfirmationTasks": _integer(
            bounds["maxConfirmationTasks"],
            name="bounds.maxConfirmationTasks",
            minimum=1,
            maximum=1_000_000,
        ),
        "maxTotalTasks": _integer(
            bounds["maxTotalTasks"],
            name="bounds.maxTotalTasks",
            minimum=1,
            maximum=1_000_000,
        ),
        "maxAttempts": _integer(
            bounds["maxAttempts"],
            name="bounds.maxAttempts",
            minimum=1,
            maximum=100,
        ),
        "deadlineSeconds": _number(
            bounds["deadlineSeconds"],
            name="bounds.deadlineSeconds",
            minimum=1.0,
            maximum=86_400.0,
        ),
    }
    target = normalized_generator["targetUniquePrograms"]
    initial_tasks = target * len(initial_ids)
    confirmation_tasks = (
        normalized_screening["confirmationCandidateCap"]
        * len(confirmation_ids)
    )
    if target > normalized_bounds["maxCandidates"]:
        raise TemporalDiscoveryContractError(
            "target unique programs exceeds maxCandidates"
        )
    if initial_tasks > normalized_bounds["maxInitialTasks"]:
        raise TemporalDiscoveryContractError(
            "initial task matrix exceeds maxInitialTasks"
        )
    if (
        normalized_screening["confirmationCandidateCap"]
        > normalized_bounds["maxConfirmationCandidates"]
        or confirmation_tasks > normalized_bounds["maxConfirmationTasks"]
    ):
        raise TemporalDiscoveryContractError(
            "confirmation stage exceeds authority bounds"
        )
    if initial_tasks + confirmation_tasks > normalized_bounds["maxTotalTasks"]:
        raise TemporalDiscoveryContractError(
            "progressive task ceiling exceeds maxTotalTasks"
        )

    normalized = {
        "schemaVersion": TEMPORAL_DISCOVERY_PREPARATION_SCHEMA,
        "authorityLabel": _safe(
            payload["authorityLabel"],
            name="authorityLabel",
        ),
        "generator": normalized_generator,
        "validation": normalized_validation,
        "workerContract": normalized_worker,
        "instrument": instrument,
        "timeframe": timeframe,
        "barLimit": bar_limit,
        "seeds": seeds,
        "developmentWindows": windows,
        "evidencePlanTemplates": [
            {"windowId": window_id, "evidencePlan": templates[window_id]}
            for window_id in window_ids
        ],
        "prohibitedEvidence": _clone(
            payload["prohibitedEvidence"],
            name="prohibitedEvidence",
        ),
        "screening": normalized_screening,
        "bounds": normalized_bounds,
    }
    normalized["preparationSha256"] = canonical_sha256(normalized)
    return normalized


def _rotate_evidence_plan(
    template: Mapping[str, Any],
    *,
    raw_source_profile_sha256: str,
    source_profile: Mapping[str, Any],
) -> dict[str, Any]:
    plan = _mapping(template, name="evidence plan template")
    profile = _mapping(source_profile, name="source profile")
    execution_config = _mapping(
        profile.get("executionConfig"),
        name="source profile executionConfig",
    )
    management_library = execution_config.get("managementLibrary")
    if management_library is not None:
        _mapping(
            management_library,
            name="source profile managementLibrary",
        )
        execution_cell_sha256 = None
    else:
        exit_policy = _mapping(
            execution_config.get("exitPolicy"),
            name="source profile exitPolicy",
        )
        selected_cell = _mapping(
            exit_policy.get("selectedCell"),
            name="source profile selectedCell",
        )
        execution_cell_sha256 = canonical_sha256(selected_cell)

    plan["profile_snapshot_sha256"] = raw_source_profile_sha256
    plan["execution_cell_sha256"] = execution_cell_sha256
    plan.pop("plan_id", None)
    identity = dict(plan)
    identity.pop("lake_manifest_sha256", None)
    plan["plan_id"] = canonical_sha256(identity)
    return plan


def _finite_preparation(
    preparation: Mapping[str, Any],
    *,
    candidates: Sequence[Mapping[str, Any]],
    window_ids: Sequence[str],
    label_suffix: str,
    max_tasks: int,
) -> dict[str, Any]:
    normalized = _mapping(preparation, name="normalized discovery preparation")
    window_map = {
        item["windowId"]: item for item in normalized["developmentWindows"]
    }
    template_map = {
        item["windowId"]: item["evidencePlan"]
        for item in normalized["evidencePlanTemplates"]
    }
    windows = [window_map[window_id] for window_id in window_ids]
    finite_candidates: list[dict[str, Any]] = []
    for candidate in candidates:
        source_profile = _mapping(
            candidate["sourceProfile"],
            name="candidate sourceProfile",
        )
        raw_sha = _sha(
            candidate["sourceProfileSha256"],
            name="candidate sourceProfileSha256",
        )
        finite_candidates.append(
            {
                "candidateId": candidate["candidateId"],
                "sourceProfile": source_profile,
                "sourceProfileSha256": raw_sha,
                "instrument": normalized["instrument"],
                "timeframe": normalized["timeframe"],
                "barLimit": normalized["barLimit"],
                "windowInputs": [
                    {
                        "windowId": window_id,
                        "evidencePlan": _rotate_evidence_plan(
                            template_map[window_id],
                            raw_source_profile_sha256=raw_sha,
                            source_profile=source_profile,
                        ),
                    }
                    for window_id in window_ids
                ],
            }
        )
    return {
        "schemaVersion": TEMPORAL_SEARCH_PREPARATION_SCHEMA,
        "authorityLabel": (
            normalized["authorityLabel"] + "-" + label_suffix
        ),
        "workerContract": normalized["workerContract"],
        "candidates": finite_candidates,
        "developmentWindows": windows,
        "prohibitedEvidence": normalized["prohibitedEvidence"],
        "bounds": {
            "maxCandidates": len(finite_candidates),
            "maxDevelopmentWindows": len(windows),
            "maxTasks": max_tasks,
            "maxAttempts": normalized["bounds"]["maxAttempts"],
            "deadlineSeconds": normalized["bounds"]["deadlineSeconds"],
        },
    }




__all__ = ['SubprocessCandidateValidator', '_normalize_preparation', '_rotate_evidence_plan', '_finite_preparation']
