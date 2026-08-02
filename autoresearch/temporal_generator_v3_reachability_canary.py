"""Bounded, repository-only reachability canary for generator-v3 construction.

The canary never submits a replay or reads market evidence.  It proves only
that a fixed construction matrix can be statically reached, passed through the
canonical profile validator, and reconstructed byte-for-byte from its plan.
Runtime fired/activation evidence is explicitly left unmeasured for a later
evidence campaign.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    _clone,
    _write_immutable,
    canonical_sha256,
)
from .temporal_discovery_validation import SubprocessCandidateValidator
from .temporal_operator_construction_v3 import (
    DIRECTION_FLIP,
    GENERATOR_V3_VERSION,
    GRAPH_BOUND_TIMEFRAME,
    MANAGEMENT_PLAN,
    SCALAR_DYNAMIC_MANAGEMENT,
    GeneratorV3ConstructionRegistry,
    inspect_construction_reachability,
)


CANARY_SCHEMA = "temporal_generator_v3_reachability_canary_v1"
CANARY_MANIFEST_SCHEMA = "temporal_generator_v3_reachability_canary_manifest_v1"
CANARY_VALIDATION_BINDING_SCHEMA = (
    "temporal_generator_v3_reachability_validation_binding_v1"
)

_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")


class CandidateValidator(Protocol):
    def validate(
        self,
        *,
        candidate_id: str,
        source_profile: Mapping[str, Any],
        expected_raw_source_profile_sha256: str,
    ) -> dict[str, Any]: ...


def _validation_codes(validation: Mapping[str, Any]) -> list[str]:
    return sorted(
        str(item.get("code"))
        for item in validation.get("issues") or []
        if isinstance(item, Mapping) and item.get("code")
    )


def _validate(
    validator: CandidateValidator,
    *,
    candidate_id: str,
    profile: Mapping[str, Any],
) -> dict[str, Any]:
    source_sha = canonical_sha256(profile)
    result = validator.validate(
        candidate_id=candidate_id,
        source_profile=profile,
        expected_raw_source_profile_sha256=source_sha,
    )
    if canonical_sha256(profile) != source_sha:
        raise TemporalDiscoveryContractError("canary source profile changed during validation")
    return _clone(result, name="canary validation")


def _required_sha256(value: Any, *, name: str) -> str:
    token = str(value or "").strip()
    if not _SHA256.fullmatch(token):
        raise TemporalDiscoveryContractError(f"{name} must be an exact sha256 identity")
    return token


def _optional_sha256(value: Any, *, name: str) -> str | None:
    if value is None:
        return None
    return _required_sha256(value, name=name)


def _validation_binding(
    validation: Mapping[str, Any],
    *,
    role: str,
    candidate_id: str,
    source_profile_sha256: str,
) -> dict[str, Any]:
    """Bind one canonical validator response to its exact canary input.

    ``validationReportSha256`` is deliberately owned by the FuzzFolio
    validator: it identifies its core report and search-issue payload, not the
    outer transport response.  The canary therefore records a second,
    content-addressed binding over the complete returned response plus the
    exact candidate/profile input.  This makes the validator response
    independently auditable without pretending AutoResearch can recreate the
    validator's internal report hash.
    """

    if role not in {"parent", "child"}:
        raise TemporalDiscoveryContractError("canary validation binding role is invalid")
    report = _clone(validation, name="canary validation binding report")
    if report.get("schemaVersion") != "temporal_search_candidate_validation_v1":
        raise TemporalDiscoveryContractError("canary validation has an unknown schema")
    if str(report.get("candidateId") or "") != candidate_id:
        raise TemporalDiscoveryContractError("canary validation candidate identity mismatch")
    if report.get("rawSourceProfileSha256") != source_profile_sha256:
        raise TemporalDiscoveryContractError("canary validation source-profile identity mismatch")
    acceptable = report.get("candidateAcceptable")
    if not isinstance(acceptable, bool):
        raise TemporalDiscoveryContractError("canary validation acceptance must be boolean")
    status = str(report.get("status") or "").strip()
    if not status:
        raise TemporalDiscoveryContractError("canary validation status is required")
    if not isinstance(report.get("issues"), list):
        raise TemporalDiscoveryContractError("canary validation issues must be a list")
    validation_report_sha256 = _required_sha256(
        report.get("validationReportSha256"),
        name="canary validation report SHA-256",
    )
    profile_snapshot_sha256 = _optional_sha256(
        report.get("profileSnapshotSha256"),
        name="canary validation profile snapshot SHA-256",
    )
    program_sha256 = _optional_sha256(
        report.get("programSha256"),
        name="canary validation program SHA-256",
    )
    if acceptable and (
        status != "valid_evaluable"
        or profile_snapshot_sha256 is None
        or program_sha256 is None
    ):
        raise TemporalDiscoveryContractError(
            "accepted canary validation requires evaluable profile and program identities"
        )
    binding = {
        "schemaVersion": CANARY_VALIDATION_BINDING_SCHEMA,
        "role": role,
        "candidateId": candidate_id,
        "sourceProfileSha256": source_profile_sha256,
        "candidateAcceptable": acceptable,
        "status": status,
        "profileSnapshotSha256": profile_snapshot_sha256,
        "programSha256": program_sha256,
        "validatorReportSha256": validation_report_sha256,
        "validationPayloadSha256": canonical_sha256(report),
    }
    binding["validationBindingSha256"] = canonical_sha256(binding)
    return binding


def _audit_validation_binding(
    validation: Mapping[str, Any],
    binding: Mapping[str, Any],
    *,
    role: str,
    candidate_id: str,
    source_profile_sha256: str,
) -> dict[str, Any]:
    expected = _validation_binding(
        validation,
        role=role,
        candidate_id=candidate_id,
        source_profile_sha256=source_profile_sha256,
    )
    supplied = _clone(binding, name="canary validation binding")
    if supplied != expected:
        raise TemporalDiscoveryContractError("canary validation binding identity mismatch")
    return expected


def _plan_with_kind(
    operator: Any, profile: Mapping[str, Any], kind: str | None = None
) -> dict[str, Any] | None:
    plans = operator.enumerate_plans(profile)
    if kind is not None:
        plans = [item for item in plans if item.get("construction", {}).get("kind") == kind]
    return plans[0] if plans else None


_REQUIRED_EXAMPLES = (
    ("scalar_dynamic_management", SCALAR_DYNAMIC_MANAGEMENT, "scalar_dynamic_management"),
    ("management_plan_create", MANAGEMENT_PLAN, "create_plan"),
    ("management_plan_delete", MANAGEMENT_PLAN, "delete_plan"),
    ("direction_flip", DIRECTION_FLIP, "direction_flip"),
    ("graph_bound_timeframe", GRAPH_BOUND_TIMEFRAME, "graph_bound_timeframe_substitution"),
)


def _base_profiles(
    *,
    base_profile: Mapping[str, Any] | None,
    base_profiles: Sequence[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Normalize the backwards-compatible one-or-many fixture interface."""

    if base_profile is not None and base_profiles is not None:
        raise TemporalDiscoveryContractError(
            "supply either base_profile or base_profiles, not both"
        )
    supplied: Sequence[Mapping[str, Any]] | None = (
        base_profiles if base_profiles is not None else ([base_profile] if base_profile is not None else None)
    )
    if not supplied:
        raise TemporalDiscoveryContractError("generator-v3 canary requires at least one base profile")
    normalized = [_clone(item, name="generator-v3 canary base profile") for item in supplied]
    hashes = [canonical_sha256(item) for item in normalized]
    if len(hashes) != len(set(hashes)):
        raise TemporalDiscoveryContractError("generator-v3 canary base profiles must be unique")
    return normalized


def _examples(
    registry: GeneratorV3ConstructionRegistry,
    base_profiles: Sequence[Mapping[str, Any]],
    parent_validations: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Select each required construction from the first applicable fixture parent.

    The fixture matrix deliberately supplies fully-authored parents for both
    management-plan create and delete.  In particular, the delete proof must
    not be manufactured by applying the create construction during the canary:
    every record remains bound to one exact supplied base profile.
    """

    examples: list[dict[str, Any]] = []
    for example_id, operator_id, plan_kind in _REQUIRED_EXAMPLES:
        operator = registry.get(operator_id)
        selected: Mapping[str, Any] | None = None
        for profile in base_profiles:
            if parent_validations[canonical_sha256(profile)].get("candidateAcceptable") is not True:
                continue
            if _plan_with_kind(operator, profile, plan_kind) is not None:
                selected = profile
                break
        examples.append(
            {
                "exampleId": example_id,
                "operatorId": operator_id,
                "sourceProfile": selected,
                "planKind": plan_kind,
            }
        )
    return examples


def run_generator_v3_reachability_canary(
    *,
    catalog: Mapping[str, Any],
    validator: CandidateValidator,
    base_profile: Mapping[str, Any] | None = None,
    base_profiles: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run the finite construction matrix in memory without market activity."""

    registry = GeneratorV3ConstructionRegistry(catalog)
    normalized_base_profiles = _base_profiles(
        base_profile=base_profile, base_profiles=base_profiles
    )
    parent_validations: dict[str, dict[str, Any]] = {}
    parent_validation_bindings: dict[str, dict[str, Any]] = {}
    for profile in normalized_base_profiles:
        profile_sha256 = canonical_sha256(profile)
        candidate_id = (
            "generator_v3_canary_parent_"
            + profile_sha256.removeprefix("sha256:")[:20]
        )
        parent_validation = _validate(
            validator,
            candidate_id=candidate_id,
            profile=profile,
        )
        parent_validations[profile_sha256] = parent_validation
        parent_validation_bindings[profile_sha256] = _validation_binding(
            parent_validation,
            role="parent",
            candidate_id=candidate_id,
            source_profile_sha256=profile_sha256,
        )
    programs: set[str] = set()
    records: list[dict[str, Any]] = []
    for example in _examples(registry, normalized_base_profiles, parent_validations):
        source_profile = example["sourceProfile"]
        operator = registry.get(example["operatorId"])
        if source_profile is None:
            record = {
                "exampleId": example["exampleId"],
                "operatorId": example["operatorId"],
                "operatorVersion": operator.operator_version,
                "baseProfileSha256": None,
                "proposed": False,
                "staticReachable": False,
                "validatorValid": False,
                "duplicateProgram": False,
                "rejectionReasonCodes": ["no_applicable_construction_plan"],
                "admitted": False,
                "runtimeEvidence": {
                    "marketReplayRun": False,
                    "fired": {"status": "unmeasured", "reason": "repository_only_canary"},
                    "activation": {"status": "unmeasured", "reason": "repository_only_canary"},
                },
            }
            records.append(record)
            continue
        source = _clone(source_profile, name="canary example parent")
        base_sha = canonical_sha256(source)
        plan = _plan_with_kind(operator, source, example["planKind"])
        record: dict[str, Any] = {
            "exampleId": example["exampleId"],
            "operatorId": example["operatorId"],
            "operatorVersion": operator.operator_version,
            "baseProfileSha256": base_sha,
            "sourceProfile": source,
            "sourceProfileSha256": base_sha,
            "proposed": plan is not None,
            "staticReachable": False,
            "validatorValid": False,
            "duplicateProgram": False,
            "rejectionReasonCodes": [],
            "admitted": False,
            "runtimeEvidence": {
                "marketReplayRun": False,
                "fired": {"status": "unmeasured", "reason": "repository_only_canary"},
                "activation": {"status": "unmeasured", "reason": "repository_only_canary"},
            },
        }
        if plan is None:
            record["rejectionReasonCodes"] = ["no_applicable_construction_plan"]
            records.append(record)
            continue
        record["plan"] = plan
        child = operator.preview(source, plan)
        reachability = inspect_construction_reachability(child)
        record["childSourceProfile"] = child
        record["childSourceProfileSha256"] = canonical_sha256(child)
        record["staticReachability"] = reachability
        record["staticReachable"] = reachability["acceptable"] is True
        if not record["staticReachable"]:
            record["rejectionReasonCodes"] = sorted(reachability["issueCounts"])
            records.append(record)
            continue

        parent_sha = base_sha
        parent_validation = parent_validations[parent_sha]
        record["parentValidation"] = parent_validation
        record["parentValidationBinding"] = parent_validation_bindings[parent_sha]
        if parent_validation.get("candidateAcceptable") is not True:
            record["rejectionReasonCodes"] = ["parent_validator_rejected", *_validation_codes(parent_validation)]
            records.append(record)
            continue
        child_validation = _validate(
            validator,
            candidate_id="generator_v3_canary_" + record["exampleId"],
            profile=child,
        )
        record["validation"] = child_validation
        record["childValidationBinding"] = _validation_binding(
            child_validation,
            role="child",
            candidate_id="generator_v3_canary_" + record["exampleId"],
            source_profile_sha256=record["childSourceProfileSha256"],
        )
        record["validatorValid"] = child_validation.get("candidateAcceptable") is True
        if not record["validatorValid"]:
            record["rejectionReasonCodes"] = ["validator_rejected", *_validation_codes(child_validation)]
            records.append(record)
            continue
        child_program = str(child_validation.get("programSha256") or "")
        if not child_program:
            raise TemporalDiscoveryContractError("accepted canary validation requires programSha256")
        record["duplicateProgram"] = child_program in programs
        if record["duplicateProgram"]:
            record["rejectionReasonCodes"] = ["duplicate_program"]
            records.append(record)
            continue
        programs.add(child_program)
        rebound, application = operator.apply(
            source,
            plan,
            parent_validated_program_sha256=str(parent_validation.get("programSha256") or ""),
            child_validated_program_sha256=child_program,
        )
        audit = operator.audit(source, rebound, application)
        if rebound != child or audit["allChecksPassed"] is not True:
            raise TemporalDiscoveryContractError("canary construction application replay failed")
        record["application"] = application
        record["applicationAudit"] = audit
        record["admitted"] = True
        records.append(record)

    family_counts: dict[str, dict[str, int]] = {}
    for operator_id in registry.enabled_operator_ids:
        rows = [row for row in records if row["operatorId"] == operator_id]
        family_counts[operator_id] = {
            "proposed": sum(row["proposed"] for row in rows),
            "staticReachable": sum(row["staticReachable"] for row in rows),
            "validatorValid": sum(row["validatorValid"] for row in rows),
            "duplicateProgram": sum(row["duplicateProgram"] for row in rows),
            "admitted": sum(row["admitted"] for row in rows),
        }
    failure_taxonomy = Counter(
        code for row in records for code in row["rejectionReasonCodes"]
    )
    report = {
        "schemaVersion": CANARY_SCHEMA,
        "generatorVersion": GENERATOR_V3_VERSION,
        "policy": registry.policy,
        "catalog": registry.catalog.payload,
        "catalogSha256": registry.catalog.catalog_sha256,
        "baseProfileSha256": (
            canonical_sha256(normalized_base_profiles[0])
            if len(normalized_base_profiles) == 1
            else None
        ),
        "baseProfileSha256s": [canonical_sha256(profile) for profile in normalized_base_profiles],
        "baseProfileValidations": [
            {
                "baseProfileSha256": canonical_sha256(profile),
                "sourceProfile": profile,
                "sourceProfileSha256": canonical_sha256(profile),
                "candidateAcceptable": parent_validations[canonical_sha256(profile)].get("candidateAcceptable") is True,
                "validation": parent_validations[canonical_sha256(profile)],
                "validationBinding": parent_validation_bindings[canonical_sha256(profile)],
            }
            for profile in normalized_base_profiles
        ],
        "records": records,
        "enabledFamilyCounts": family_counts,
        "deferredOperators": [
            {"operatorId": item.operator_id, "reason": item.deferred_reason}
            for item in registry.deferred_operators
        ],
        "failureTaxonomy": dict(sorted(failure_taxonomy.items())),
        "marketEvidenceRead": False,
        "gatewayContacted": False,
        "runtimeEvidenceMeasured": False,
    }
    report["allEnabledFamiliesAdmitted"] = all(
        all(counts[key] > 0 for key in ("proposed", "staticReachable", "validatorValid", "admitted"))
        for counts in family_counts.values()
    )
    report["reportSha256"] = canonical_sha256(report)
    return report


def _manifest(root: Path) -> dict[str, Any]:
    files = []
    for path in sorted(item for item in root.rglob("*") if item.is_file() and item.name != "manifest.json"):
        files.append({
            "relativePath": path.relative_to(root).as_posix(),
            "length": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest().upper(),
        })
    manifest = {"schemaVersion": CANARY_MANIFEST_SCHEMA, "fileCount": len(files), "files": files}
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write_immutable(root / "manifest.json", manifest)
    return manifest


def build_generator_v3_reachability_canary(
    *,
    catalog: Mapping[str, Any],
    validator: CandidateValidator,
    output_root: Path | str,
    base_profile: Mapping[str, Any] | None = None,
    base_profiles: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    report = run_generator_v3_reachability_canary(
        catalog=catalog,
        base_profile=base_profile,
        base_profiles=base_profiles,
        validator=validator,
    )
    if report["allEnabledFamiliesAdmitted"] is not True:
        raise TemporalDiscoveryContractError("generator-v3 reachability canary has an unadmitted enabled family")
    root = Path(output_root)
    _write_immutable(root / "generator-v3-reachability-canary.json", report)
    manifest = _manifest(root)
    return {
        "schemaVersion": "temporal_generator_v3_reachability_canary_result_v1",
        "reportSha256": report["reportSha256"],
        "manifestSha256": manifest["manifestSha256"],
        "recordCount": len(report["records"]),
        "allEnabledFamiliesAdmitted": True,
        "marketEvidenceRead": False,
        "gatewayContacted": False,
    }


def audit_generator_v3_reachability_canary(output_root: Path | str) -> dict[str, Any]:
    root = Path(output_root)
    try:
        report = json.loads((root / "generator-v3-reachability-canary.json").read_text(encoding="utf-8"))
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError("could not read generator-v3 canary artifacts") from exc
    supplied_report = report.pop("reportSha256", None)
    if report.get("schemaVersion") != CANARY_SCHEMA or canonical_sha256(report) != supplied_report:
        raise TemporalDiscoveryContractError("generator-v3 canary report identity mismatch")
    report["reportSha256"] = supplied_report
    supplied_manifest = manifest.pop("manifestSha256", None)
    if manifest.get("schemaVersion") != CANARY_MANIFEST_SCHEMA or canonical_sha256(manifest) != supplied_manifest:
        raise TemporalDiscoveryContractError("generator-v3 canary manifest identity mismatch")
    expected = set()
    for entry in manifest.get("files") or []:
        path = root / str(entry.get("relativePath") or "")
        expected.add(path.resolve())
        if not path.is_file() or path.stat().st_size != int(entry.get("length", -1)) or hashlib.sha256(path.read_bytes()).hexdigest().upper() != entry.get("sha256"):
            raise TemporalDiscoveryContractError("generator-v3 canary manifest file mismatch")
    actual = {path.resolve() for path in root.rglob("*") if path.is_file() and path.name != "manifest.json"}
    if actual != expected:
        raise TemporalDiscoveryContractError("generator-v3 canary artifact inventory drift")
    registry = GeneratorV3ConstructionRegistry(report["catalog"])
    if registry.catalog.catalog_sha256 != report.get("catalogSha256") or registry.policy != report.get("policy"):
        raise TemporalDiscoveryContractError("generator-v3 canary policy/catalog binding mismatch")

    base_validation_rows = report.get("baseProfileValidations")
    if not isinstance(base_validation_rows, list) or not base_validation_rows:
        raise TemporalDiscoveryContractError("generator-v3 canary base validations are required")
    base_validations: dict[str, dict[str, Any]] = {}
    for item in base_validation_rows:
        if not isinstance(item, Mapping) or not isinstance(item.get("sourceProfile"), Mapping):
            raise TemporalDiscoveryContractError("generator-v3 canary base validation is malformed")
        source = _clone(item["sourceProfile"], name="canary base validation source profile")
        source_sha256 = canonical_sha256(source)
        if (
            item.get("baseProfileSha256") != source_sha256
            or item.get("sourceProfileSha256") != source_sha256
            or source_sha256 in base_validations
        ):
            raise TemporalDiscoveryContractError("generator-v3 canary base-profile identity mismatch")
        candidate_id = (
            "generator_v3_canary_parent_"
            + source_sha256.removeprefix("sha256:")[:20]
        )
        validation = item.get("validation")
        binding = item.get("validationBinding")
        if not isinstance(validation, Mapping) or not isinstance(binding, Mapping):
            raise TemporalDiscoveryContractError("generator-v3 canary parent validation is missing")
        validated = _audit_validation_binding(
            validation,
            binding,
            role="parent",
            candidate_id=candidate_id,
            source_profile_sha256=source_sha256,
        )
        if item.get("candidateAcceptable") is not validated["candidateAcceptable"]:
            raise TemporalDiscoveryContractError("generator-v3 canary parent acceptance mismatch")
        base_validations[source_sha256] = {
            "sourceProfile": source,
            "validation": _clone(validation, name="canary parent validation"),
            "binding": validated,
        }
    base_hashes = list(base_validations)
    if report.get("baseProfileSha256s") != base_hashes:
        raise TemporalDiscoveryContractError("generator-v3 canary base-profile order mismatch")
    if report.get("baseProfileSha256") != (
        base_hashes[0] if len(base_hashes) == 1 else None
    ):
        raise TemporalDiscoveryContractError("generator-v3 canary primary base-profile mismatch")

    admitted_programs: set[str] = set()
    for row in report.get("records") or []:
        if not isinstance(row, Mapping):
            raise TemporalDiscoveryContractError("generator-v3 canary record is malformed")
        source_value = row.get("sourceProfile")
        if not isinstance(source_value, Mapping):
            if row.get("admitted") or row.get("validatorValid"):
                raise TemporalDiscoveryContractError("generator-v3 canary record source profile is missing")
            continue
        source = _clone(source_value, name="canary record source profile")
        source_sha = canonical_sha256(source)
        if (
            row.get("baseProfileSha256") != source_sha
            or row.get("sourceProfileSha256") != source_sha
            or source_sha not in base_validations
        ):
            raise TemporalDiscoveryContractError("generator-v3 canary record base-profile binding mismatch")
        parent = base_validations[source_sha]
        parent_validation = row.get("parentValidation")
        parent_binding = row.get("parentValidationBinding")
        if parent_validation is not None or parent_binding is not None:
            if (
                not isinstance(parent_validation, Mapping)
                or not isinstance(parent_binding, Mapping)
                or _clone(parent_validation, name="canary record parent validation")
                != parent["validation"]
                or _clone(parent_binding, name="canary record parent validation binding")
                != parent["binding"]
            ):
                raise TemporalDiscoveryContractError("generator-v3 canary record parent validation mismatch")

        child_validation = row.get("validation")
        child_binding: dict[str, Any] | None = None
        if child_validation is not None or row.get("childValidationBinding") is not None:
            child_source = row.get("childSourceProfile")
            child_source_sha256 = row.get("childSourceProfileSha256")
            if (
                not isinstance(child_validation, Mapping)
                or not isinstance(row.get("childValidationBinding"), Mapping)
                or not isinstance(child_source, Mapping)
                or canonical_sha256(child_source) != child_source_sha256
            ):
                raise TemporalDiscoveryContractError("generator-v3 canary child validation is malformed")
            child_binding = _audit_validation_binding(
                child_validation,
                row["childValidationBinding"],
                role="child",
                candidate_id="generator_v3_canary_" + str(row.get("exampleId") or ""),
                source_profile_sha256=str(child_source_sha256),
            )
            if row.get("validatorValid") is not child_binding["candidateAcceptable"]:
                raise TemporalDiscoveryContractError("generator-v3 canary child acceptance mismatch")
        elif row.get("validatorValid"):
            raise TemporalDiscoveryContractError("generator-v3 canary child validation is missing")

        if not row.get("admitted"):
            continue
        if parent_validation is None or child_binding is None:
            raise TemporalDiscoveryContractError("admitted canary record lacks validated parent or child")
        if (
            parent["binding"]["candidateAcceptable"] is not True
            or child_binding["candidateAcceptable"] is not True
        ):
            raise TemporalDiscoveryContractError("admitted canary record has rejected validation")
        application_value = row.get("application")
        if not isinstance(application_value, Mapping):
            raise TemporalDiscoveryContractError("admitted canary record lacks application")
        if application_value.get("parentValidatedProgramSha256") != parent["binding"]["programSha256"]:
            raise TemporalDiscoveryContractError("canary parent validation program binding mismatch")
        if application_value.get("childValidatedProgramSha256") != child_binding["programSha256"]:
            raise TemporalDiscoveryContractError("canary child validation program binding mismatch")
        operator = registry.get(str(row["operatorId"]))
        plan = row["plan"]
        rebound, application = operator.apply(
            source,
            plan,
            parent_validated_program_sha256=parent["binding"]["programSha256"],
            child_validated_program_sha256=child_binding["programSha256"],
        )
        audit = operator.audit(source, rebound, application)
        if (
            rebound != row.get("childSourceProfile")
            or application != application_value
            or audit != row.get("applicationAudit")
        ):
            raise TemporalDiscoveryContractError("generator-v3 canary exact replay mismatch")
        child_program = child_binding["programSha256"]
        if child_program in admitted_programs:
            raise TemporalDiscoveryContractError("generator-v3 canary admitted child program is not globally unique")
        admitted_programs.add(child_program)
    recomputed_counts = {
        operator_id: {
            key: sum(
                row.get("operatorId") == operator_id and row.get(key) is True
                for row in report.get("records") or []
            )
            for key in ("proposed", "staticReachable", "validatorValid", "duplicateProgram", "admitted")
        }
        for operator_id in registry.enabled_operator_ids
    }
    if (
        report.get("enabledFamilyCounts") != recomputed_counts
        or report.get("allEnabledFamiliesAdmitted") is not True
        or not all(
            all(counts[key] > 0 for key in ("proposed", "staticReachable", "validatorValid", "admitted"))
            for counts in recomputed_counts.values()
        )
    ):
        raise TemporalDiscoveryContractError("generator-v3 canary enabled family admission failed")
    return {
        "schemaVersion": "temporal_generator_v3_reachability_canary_audit_v1",
        "ok": True,
        "reportSha256": supplied_report,
        "manifestSha256": supplied_manifest,
        "recordCount": len(report.get("records") or []),
    }


def _read_json(path: Path, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"{name} root must be an object")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Run or audit the no-market generator-v3 construction canary.")
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build")
    build.add_argument("--catalog", type=Path, required=True)
    build.add_argument(
        "--profile",
        type=Path,
        action="append",
        required=True,
        help="Base profile fixture; repeat to cover distinct construction parents (first applicable validated profile wins).",
    )
    build.add_argument("--validator-command-file", type=Path, required=True)
    build.add_argument("--output-root", type=Path, required=True)
    build.add_argument("--validator-timeout-seconds", type=float, default=60.0)
    audit = commands.add_parser("audit")
    audit.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "audit":
        result = audit_generator_v3_reachability_canary(args.output_root)
    else:
        command = json.loads(args.validator_command_file.read_text(encoding="utf-8"))
        if not isinstance(command, list) or not command or not all(isinstance(item, str) and item.strip() for item in command):
            raise TemporalDiscoveryContractError("validator command file must contain a non-empty string array")
        result = build_generator_v3_reachability_canary(
            catalog=_read_json(args.catalog, name="catalog"),
            base_profiles=[_read_json(path, name="profile") for path in args.profile],
            validator=SubprocessCandidateValidator(command, timeout_seconds=args.validator_timeout_seconds),
            output_root=args.output_root,
        )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()


__all__ = [
    "audit_generator_v3_reachability_canary",
    "build_generator_v3_reachability_canary",
    "run_generator_v3_reachability_canary",
]
