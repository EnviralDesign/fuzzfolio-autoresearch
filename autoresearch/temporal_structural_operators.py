"""Pure, deterministic whole-graph structural-operator contracts.

Structural operators are deliberately separate from proposal selection.  They
enumerate every canonical applicable plan and contain no random-number source;
an evolutionary policy may choose among those plans later using its own bound
seed, generation, and birth ordinal.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    _clone,
    _sha,
    canonical_sha256,
)

STRUCTURAL_OPERATOR_PLAN_SCHEMA = "temporal_structural_operator_plan_v1"
STRUCTURAL_OPERATOR_APPLICATION_SCHEMA = "temporal_structural_operator_application_v1"
STRUCTURAL_OPERATOR_AUDIT_SCHEMA = "temporal_structural_operator_audit_v1"
TEMPORAL_CANDIDATE_LINEAGE_SCHEMA = "temporal_candidate_lineage_v1"


class TemporalStructuralOperator(Protocol):
    """Protocol implemented by deterministic whole-graph operators."""

    operator_id: str
    operator_version: str

    def enumerate_plans(self, profile: Mapping[str, Any]) -> list[dict[str, Any]]: ...

    def apply(
        self,
        profile: Mapping[str, Any],
        plan: Mapping[str, Any],
        *,
        parent_validated_program_sha256: str,
        child_validated_program_sha256: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]: ...

    def audit(
        self,
        parent_profile: Mapping[str, Any],
        transformed_profile: Mapping[str, Any],
        application_record: Mapping[str, Any],
    ) -> dict[str, Any]: ...


class StructuralOperatorRegistry:
    """Canonical operator registry with explicit, collision-free identities."""

    def __init__(self, operators: Sequence[TemporalStructuralOperator] = ()) -> None:
        self._operators: dict[str, TemporalStructuralOperator] = {}
        for operator in operators:
            self.register(operator)

    def register(self, operator: TemporalStructuralOperator) -> None:
        operator_id = str(operator.operator_id)
        if not operator_id:
            raise TemporalDiscoveryContractError("structural operator ID is required")
        if operator_id in self._operators:
            raise TemporalDiscoveryContractError(
                f"duplicate structural operator ID: {operator_id}"
            )
        self._operators[operator_id] = operator

    def get(self, operator_id: str) -> TemporalStructuralOperator:
        try:
            return self._operators[operator_id]
        except KeyError as exc:
            raise TemporalDiscoveryContractError(
                f"unknown structural operator ID: {operator_id}"
            ) from exc

    def enumerate_plans(self, profile: Mapping[str, Any]) -> list[dict[str, Any]]:
        plans: list[dict[str, Any]] = []
        for operator_id in sorted(self._operators):
            plans.extend(self._operators[operator_id].enumerate_plans(profile))
        return sorted(plans, key=lambda value: str(value["planSha256"]))

    @property
    def operator_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._operators))


def finalize_plan(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a canonical plan with a content-derived identity."""

    plan = _clone(payload, name="structural operator plan")
    plan["schemaVersion"] = STRUCTURAL_OPERATOR_PLAN_SCHEMA
    plan.pop("planSha256", None)
    plan["planSha256"] = canonical_sha256(plan)
    return plan


def finalize_application(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return an immutable application record with a content-derived identity."""

    application = _clone(payload, name="structural operator application")
    application["schemaVersion"] = STRUCTURAL_OPERATOR_APPLICATION_SCHEMA
    application.pop("applicationSha256", None)
    application["applicationSha256"] = canonical_sha256(application)
    return application


def finalize_audit(checks: Mapping[str, bool], **context: Any) -> dict[str, Any]:
    """Create a canonical fail-closed invariant report."""

    normalized_checks = {
        str(name): bool(value) for name, value in sorted(checks.items())
    }
    report = {
        "schemaVersion": STRUCTURAL_OPERATOR_AUDIT_SCHEMA,
        **_clone(context, name="structural operator audit context"),
        "checks": normalized_checks,
        "allChecksPassed": all(normalized_checks.values()),
    }
    report["auditSha256"] = canonical_sha256(report)
    return report


def build_candidate_lineage(
    *,
    candidate_id: str,
    candidate_source_profile_sha256: str,
    candidate_validated_program_sha256: str,
    generation_index: int,
    birth_ordinal: int,
    parent_candidate_ids: Sequence[str],
    parent_program_sha256s: Sequence[str],
    operator_id: str,
    operator_version: str,
    plan_sha256: str,
    application_sha256: str,
) -> dict[str, Any]:
    """Build the durable lineage record consumed by later evolution stages."""

    if generation_index < 0 or birth_ordinal < 0:
        raise TemporalDiscoveryContractError(
            "generation index and birth ordinal must be non-negative"
        )
    if not parent_candidate_ids or len(parent_candidate_ids) != len(
        parent_program_sha256s
    ):
        raise TemporalDiscoveryContractError(
            "lineage parents require equally sized non-empty candidate/program arrays"
        )
    lineage = {
        "schemaVersion": TEMPORAL_CANDIDATE_LINEAGE_SCHEMA,
        "candidateId": str(candidate_id),
        "candidateSourceProfileSha256": _sha(
            candidate_source_profile_sha256,
            name="lineage candidate source profile SHA-256",
        ),
        "candidateValidatedProgramSha256": _sha(
            candidate_validated_program_sha256,
            name="lineage candidate validated program SHA-256",
        ),
        "generationIndex": int(generation_index),
        "birthOrdinal": int(birth_ordinal),
        "originKind": "structural_operator",
        "parentCandidateIds": [str(value) for value in parent_candidate_ids],
        "parentValidatedProgramSha256s": [
            _sha(value, name="lineage parent validated program SHA-256")
            for value in parent_program_sha256s
        ],
        "operatorId": str(operator_id),
        "operatorVersion": str(operator_version),
        "planSha256": _sha(plan_sha256, name="lineage plan SHA-256"),
        "applicationSha256": _sha(
            application_sha256, name="lineage application SHA-256"
        ),
    }
    lineage["lineageSha256"] = canonical_sha256(lineage)
    return lineage


__all__ = [
    "STRUCTURAL_OPERATOR_APPLICATION_SCHEMA",
    "STRUCTURAL_OPERATOR_AUDIT_SCHEMA",
    "STRUCTURAL_OPERATOR_PLAN_SCHEMA",
    "TEMPORAL_CANDIDATE_LINEAGE_SCHEMA",
    "StructuralOperatorRegistry",
    "TemporalStructuralOperator",
    "build_candidate_lineage",
    "finalize_application",
    "finalize_audit",
    "finalize_plan",
]
