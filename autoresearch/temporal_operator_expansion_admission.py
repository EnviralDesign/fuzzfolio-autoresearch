"""Deterministic, no-market admission for the seven structural operators.

The natural corpus is exhaustively enumerated, while native validation is
bounded to a deterministic coverage set.  A synthetic, validator-admitted
repeatable management self-loop supplies the cooldown lifecycle witness when
the natural corpus contains no such authored action site.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    _clone,
    _sha,
    _write_immutable,
    canonical_sha256,
)
from .temporal_discovery_validation import SubprocessCandidateValidator
from .temporal_operator_expansion import (
    EDGE_TRIGGER_PREDICATE,
    EVENT_AGE_WINDOW,
    MAXIMUM_POSITION_AGE_EXIT,
    MINIMUM_POSITION_AGE_GATE,
    REPEAT_ACTION_COOLDOWN,
    REQUIRE_CONSECUTIVE_TRUE,
    SEQUENCE_ACTION_GATE,
    expanded_structural_operators,
)

REPORT_SCHEMA = "temporal_operator_expansion_admission_v1"
MANIFEST_SCHEMA = "temporal_operator_expansion_admission_manifest_v1"
DEFAULT_ADMITTED_PER_OPERATOR = 16

_NO_SITE_REASON = {
    EDGE_TRIGGER_PREDICATE: "no_unique_level_predicate_site",
    EVENT_AGE_WINDOW: "no_native_event_occurrence_site",
    REQUIRE_CONSECUTIVE_TRUE: "no_unique_level_predicate_site",
    SEQUENCE_ACTION_GATE: "no_multiclause_non_entry_action_gate",
    REPEAT_ACTION_COOLDOWN: "no_repeatable_management_self_loop",
    MINIMUM_POSITION_AGE_GATE: "no_ungated_position_action",
    MAXIMUM_POSITION_AGE_EXIT: "no_ungated_unambiguous_full_exit",
}


def _read(path: Path, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"{name} root must be an object")
    return _clone(value, name=name)


def _audit_identity_set(population: Mapping[str, Any]) -> list[dict[str, Any]]:
    payload = _clone(population, name="operator admission population")
    supplied = _sha(payload.pop("setSha256", None), name="population set SHA-256")
    if canonical_sha256(payload) != supplied:
        raise TemporalDiscoveryContractError("population set identity mismatch")
    values = payload.get("values")
    if not isinstance(values, list) or int(payload.get("count", -1)) != len(values):
        raise TemporalDiscoveryContractError("population count does not match values")
    output = []
    for value in values:
        if not isinstance(value, Mapping):
            raise TemporalDiscoveryContractError(
                "population candidate must be an object"
            )
        profile = value.get("sourceProfile")
        if not isinstance(profile, Mapping):
            raise TemporalDiscoveryContractError("candidate source profile is required")
        if canonical_sha256(profile) != _sha(
            value.get("sourceProfileSha256"), name="candidate source profile SHA-256"
        ):
            raise TemporalDiscoveryContractError("candidate profile identity mismatch")
        output.append(_clone(value, name="operator admission candidate"))
    return sorted(output, key=lambda item: str(item["candidateId"]))


def _action_kind(profile: Mapping[str, Any], transition_id: str) -> str:
    for transition in profile["graph"]["transitions"]:
        if transition.get("id") != transition_id:
            continue
        actions = transition.get("actions") or []
        if not actions:
            return "none"
        return str(actions[0].get("kind") or "unknown")
    raise TemporalDiscoveryContractError("operator plan target transition disappeared")


def _coverage_tokens(reference: Mapping[str, Any]) -> set[str]:
    plan = reference["plan"]
    tokens = {f"action:{reference['actionKind']}"}
    for name, value in sorted((plan.get("parameters") or {}).items()):
        if name == "occurrenceSha256":
            continue
        tokens.add(
            "parameter:"
            + name
            + "="
            + json.dumps(value, sort_keys=True, separators=(",", ":"))
        )
    if plan.get("setupKind"):
        tokens.add(f"setup:{plan['setupKind']}")
    return tokens


def _select_coverage(
    references: Sequence[Mapping[str, Any]], *, limit: int
) -> list[dict[str, Any]]:
    ordered = sorted(references, key=lambda item: str(item["plan"]["planSha256"]))
    selected: list[dict[str, Any]] = []
    selected_shas: set[str] = set()
    covered: set[str] = set()
    for reference in ordered:
        tokens = _coverage_tokens(reference)
        if tokens - covered:
            selected.append(_clone(reference, name="coverage reference"))
            selected_shas.add(str(reference["plan"]["planSha256"]))
            covered.update(tokens)
            if len(selected) == limit:
                return selected
    for reference in ordered:
        plan_sha = str(reference["plan"]["planSha256"])
        if plan_sha in selected_shas:
            continue
        selected.append(_clone(reference, name="coverage reference"))
        if len(selected) == limit:
            break
    return selected


def _synthetic_cooldown_parent(
    candidates: Sequence[Mapping[str, Any]],
) -> tuple[str, dict[str, Any]]:
    for candidate in candidates:
        profile = _clone(candidate["sourceProfile"], name="cooldown synthetic parent")
        transitions = profile["graph"]["transitions"]
        for transition in transitions:
            actions = transition.get("actions") or []
            if (
                transition.get("eventClass") != "decision"
                or len(actions) != 1
                or actions[0].get("kind")
                not in {
                    "move_stop_to_break_even_next_open",
                    "tighten_stop_next_open",
                    "set_target_next_open",
                    "cancel_target_next_open",
                    "activate_trailing_stop_next_open",
                    "deactivate_trailing_stop_next_open",
                }
            ):
                continue
            source = str(transition["sourceStateId"])
            priorities = [
                int(item["priority"])
                for item in transitions
                if item.get("sourceStateId") == source
                and item.get("eventClass") == "decision"
            ]
            witness = _clone(transition, name="cooldown witness transition")
            witness["id"] = "synthetic_repeatable_management"
            witness["destinationStateId"] = source
            witness["priority"] = max(priorities, default=0) + 10
            witness["reasonCode"] = "synthetic_repeatable_management_witness"
            transitions.append(witness)
            profile["name"] = "Synthetic repeatable management admission witness"
            profile["description"] = (
                "Repository-only no-market structural admission fixture."
            )
            return str(candidate["candidateId"]), profile
    raise TemporalDiscoveryContractError(
        "natural corpus has no management action from which to build cooldown witness"
    )


def _validate_profile(
    validator: SubprocessCandidateValidator,
    *,
    candidate_id: str,
    profile: Mapping[str, Any],
) -> dict[str, Any]:
    source_sha = canonical_sha256(profile)
    report = validator.validate(
        candidate_id=candidate_id,
        source_profile=profile,
        expected_raw_source_profile_sha256=source_sha,
    )
    if report.get("candidateAcceptable") is not True:
        codes = sorted(
            str(item.get("code"))
            for item in report.get("issues") or []
            if isinstance(item, Mapping) and item.get("code")
        )
        raise TemporalDiscoveryContractError(
            f"native validator rejected {candidate_id}: {codes}"
        )
    return report


def _manifest(root: Path) -> dict[str, Any]:
    files = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.name == "manifest.json":
            continue
        files.append(
            {
                "relativePath": path.relative_to(root).as_posix(),
                "length": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest().upper(),
            }
        )
    manifest = {
        "schemaVersion": MANIFEST_SCHEMA,
        "fileCount": len(files),
        "files": files,
    }
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write_immutable(root / "manifest.json", manifest)
    return manifest


def build_operator_expansion_admission(
    *,
    population_path: Path | str,
    validator_command: Sequence[str],
    output_root: Path | str,
    admitted_per_operator: int = DEFAULT_ADMITTED_PER_OPERATOR,
    validator_timeout_seconds: float = 60.0,
) -> dict[str, Any]:
    if admitted_per_operator < 1 or admitted_per_operator > 64:
        raise TemporalDiscoveryContractError(
            "admitted plans per operator must be between 1 and 64"
        )
    population_file = Path(population_path)
    root = Path(output_root)
    candidates = _audit_identity_set(
        _read(population_file, name="operator admission population")
    )
    operators = {item.operator_id: item for item in expanded_structural_operators()}
    references_by_operator: dict[str, list[dict[str, Any]]] = {
        operator_id: [] for operator_id in sorted(operators)
    }
    parent_counts: Counter[str] = Counter()
    rejection_counts: dict[str, Counter[str]] = {
        operator_id: Counter() for operator_id in sorted(operators)
    }
    action_counts: dict[str, Counter[str]] = {
        operator_id: Counter() for operator_id in sorted(operators)
    }

    for candidate in candidates:
        profile = candidate["sourceProfile"]
        for operator_id, operator in sorted(operators.items()):
            plans = operator.enumerate_plans(profile)
            if not plans:
                rejection_counts[operator_id][_NO_SITE_REASON[operator_id]] += 1
                continue
            parent_counts[operator_id] += 1
            for plan in plans:
                action_kind = _action_kind(profile, str(plan["targetTransitionId"]))
                action_counts[operator_id][action_kind] += 1
                references_by_operator[operator_id].append(
                    {
                        "cohort": "natural",
                        "parentCandidateId": candidate["candidateId"],
                        "parentProgramSha256": candidate["programSha256"],
                        "parentSourceProfileSha256": candidate["sourceProfileSha256"],
                        "actionKind": action_kind,
                        "plan": plan,
                        "profile": profile,
                    }
                )

    validator = SubprocessCandidateValidator(
        validator_command, timeout_seconds=validator_timeout_seconds
    )
    synthetic_source_id, synthetic_profile = _synthetic_cooldown_parent(candidates)
    synthetic_validation = _validate_profile(
        validator,
        candidate_id="synthetic_cooldown_parent",
        profile=synthetic_profile,
    )
    cooldown_operator = operators[REPEAT_ACTION_COOLDOWN]
    cooldown_plans = cooldown_operator.enumerate_plans(synthetic_profile)
    if not cooldown_plans:
        raise TemporalDiscoveryContractError(
            "synthetic cooldown parent is inapplicable"
        )
    synthetic_parent_sha = canonical_sha256(synthetic_profile)
    for plan in cooldown_plans:
        references_by_operator[REPEAT_ACTION_COOLDOWN].append(
            {
                "cohort": "synthetic_cooldown_witness",
                "parentCandidateId": "synthetic_" + synthetic_source_id,
                "parentProgramSha256": synthetic_validation["programSha256"],
                "parentSourceProfileSha256": synthetic_parent_sha,
                "actionKind": _action_kind(
                    synthetic_profile, str(plan["targetTransitionId"])
                ),
                "plan": plan,
                "profile": synthetic_profile,
            }
        )

    admitted: list[dict[str, Any]] = []
    for operator_id, operator in sorted(operators.items()):
        selected = _select_coverage(
            references_by_operator[operator_id], limit=admitted_per_operator
        )
        if not selected:
            raise TemporalDiscoveryContractError(
                f"operator has no natural or synthetic admission plan: {operator_id}"
            )
        for ordinal, reference in enumerate(selected):
            plan = reference["plan"]
            child = operator.preview(reference["profile"], plan)
            child_validation = _validate_profile(
                validator,
                candidate_id=(
                    "structural_" + operator_id.removesuffix("_v1") + f"_{ordinal:02d}"
                ),
                profile=child,
            )
            rebound, application = operator.apply(
                reference["profile"],
                plan,
                parent_validated_program_sha256=reference["parentProgramSha256"],
                child_validated_program_sha256=child_validation["programSha256"],
            )
            replay_audit = operator.audit(reference["profile"], child, application)
            static_audit = application["staticInvariantReport"]
            if (
                rebound != child
                or static_audit["allChecksPassed"] is not True
                or replay_audit["allChecksPassed"] is not True
            ):
                raise TemporalDiscoveryContractError(
                    f"operator application audit failed: {operator_id}"
                )
            admitted.append(
                {
                    "operatorId": operator_id,
                    "operatorVersion": operator.operator_version,
                    "cohort": reference["cohort"],
                    "parentCandidateId": reference["parentCandidateId"],
                    "parentProgramSha256": reference["parentProgramSha256"],
                    "parentSourceProfileSha256": reference["parentSourceProfileSha256"],
                    "actionKind": reference["actionKind"],
                    "planSha256": plan["planSha256"],
                    "parameters": _clone(
                        plan.get("parameters") or {}, name="operator parameters"
                    ),
                    "targetTransitionId": plan["targetTransitionId"],
                    "childSourceProfileSha256": canonical_sha256(child),
                    "childProgramSha256": child_validation["programSha256"],
                    "validationReportSha256": child_validation[
                        "validationReportSha256"
                    ],
                    "applicationSha256": application["applicationSha256"],
                    "staticAuditSha256": static_audit["auditSha256"],
                    "replayAuditSha256": replay_audit["auditSha256"],
                    "boundary": {
                        "parentStateCount": len(
                            reference["profile"]["graph"]["states"]
                        ),
                        "childStateCount": len(child["graph"]["states"]),
                        "parentTransitionCount": len(
                            reference["profile"]["graph"]["transitions"]
                        ),
                        "childTransitionCount": len(child["graph"]["transitions"]),
                    },
                    "nativeValid": True,
                }
            )

    families = []
    for operator_id in sorted(operators):
        family_admitted = [row for row in admitted if row["operatorId"] == operator_id]
        natural = [
            row
            for row in references_by_operator[operator_id]
            if row["cohort"] == "natural"
        ]
        families.append(
            {
                "operatorId": operator_id,
                "operatorVersion": operators[operator_id].operator_version,
                "naturalApplicableParentCount": parent_counts[operator_id],
                "naturalPlanCount": len(natural),
                "naturalActionKindCounts": dict(
                    sorted(action_counts[operator_id].items())
                ),
                "naturalRejectionReasonCounts": dict(
                    sorted(rejection_counts[operator_id].items())
                ),
                "admittedPlanCount": len(family_admitted),
                "nativeValidCount": sum(row["nativeValid"] for row in family_admitted),
                "syntheticWitnessCount": sum(
                    row["cohort"] == "synthetic_cooldown_witness"
                    for row in family_admitted
                ),
            }
        )

    report = {
        "schemaVersion": REPORT_SCHEMA,
        "populationPath": str(population_file.resolve()),
        "populationCandidateCount": len(candidates),
        "operatorCount": len(operators),
        "admittedPerOperatorLimit": admitted_per_operator,
        "naturalPlanCount": sum(item["naturalPlanCount"] for item in families),
        "admittedPlanCount": len(admitted),
        "nativeValidCount": sum(row["nativeValid"] for row in admitted),
        "marketEvidenceRead": False,
        "gatewayContacted": False,
        "families": families,
        "admittedPlans": admitted,
    }
    report["reportSha256"] = canonical_sha256(report)
    _write_immutable(root / "operator-admission.json", report)
    manifest = _manifest(root)
    return {
        "schemaVersion": "temporal_operator_expansion_admission_result_v1",
        "reportSha256": report["reportSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "populationCandidateCount": len(candidates),
        "naturalPlanCount": report["naturalPlanCount"],
        "admittedPlanCount": len(admitted),
        "nativeValidCount": report["nativeValidCount"],
        "marketEvidenceRead": False,
        "gatewayContacted": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--population", type=Path, required=True)
    parser.add_argument("--validator-command-file", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--admitted-per-operator", type=int, default=DEFAULT_ADMITTED_PER_OPERATOR
    )
    parser.add_argument("--validator-timeout-seconds", type=float, default=60.0)
    args = parser.parse_args()
    command = json.loads(args.validator_command_file.read_text(encoding="utf-8"))
    if not isinstance(command, list) or not all(
        isinstance(value, str) for value in command
    ):
        raise TemporalDiscoveryContractError(
            "validator command file must contain a string array"
        )
    print(
        json.dumps(
            build_operator_expansion_admission(
                population_path=args.population,
                validator_command=command,
                output_root=args.output_root,
                admitted_per_operator=args.admitted_per_operator,
                validator_timeout_seconds=args.validator_timeout_seconds,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()


__all__ = ["build_operator_expansion_admission"]
