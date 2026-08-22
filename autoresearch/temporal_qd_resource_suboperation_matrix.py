"""Balanced resource-suboperation matrix specification.

Experiment-only. This module does not schedule a generation, worker, or
market evaluation. Empty or ineligible cells remain explicit scientific
outcomes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .evidence_plan import canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError

RESOURCE_SUBOPERATION_MATRIX_SCHEMA = "temporal_qd_resource_suboperation_matrix_v1"
RESOURCE_SUBOPERATION_MODE = "frozen_parent_balanced_suboperation_v1"
CLONE_CONTROL = "re_evaluate_parent_on_frozen_panel"
LANES = (
    "directional_event_insert",
    "indicator_instance_insert",
    "indicator_period_mutate",
    "indicator_range_mutate",
    "indicator_timeframe_mutate",
    "indicator_lookback_mutate",
)
ROOT_KEYS = (
    "schemaVersion",
    "mode",
    "includeCrossover",
    "cloneControl",
    "productionArchiveWrite",
    "mutationDepth",
    "parents",
    "lanes",
    "childrenPerEligibleCell",
    "balanceRule",
    "emptyIneligibleCellsAreScientificOutcomes",
    "doNotSampleUniformlyFromFullPlanPool",
    "notAdmittedOnFrontGenerationPath",
    "contractSha256",
)
PARENT_KEYS = ("candidateId", "role")
PARENT_ROLES = ("archive", "inactive_control", "active_negative_control")
BALANCE_RULE = "eligible_parent_and_site_cells_only"


def _unexpected(label: str) -> TemporalDiscoveryContractError:
    return TemporalDiscoveryContractError(f"{label} has an unexpected schema")


def _exact_object(value: Any, required: Sequence[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(required):
        raise _unexpected(label)
    return dict(value)


def validate_resource_suboperation_matrix(contract: Mapping[str, Any]) -> dict[str, Any]:
    row = _exact_object(contract, ROOT_KEYS, "resource suboperation matrix")
    if row["schemaVersion"] != RESOURCE_SUBOPERATION_MATRIX_SCHEMA:
        raise TemporalDiscoveryContractError("resource suboperation matrix schema is incompatible")
    if row["mode"] != RESOURCE_SUBOPERATION_MODE:
        raise TemporalDiscoveryContractError("resource suboperation matrix mode is incompatible")
    if row["includeCrossover"] is not False:
        raise TemporalDiscoveryContractError("resource suboperation matrix must keep crossover out of this lane")
    if row["cloneControl"] != CLONE_CONTROL:
        raise TemporalDiscoveryContractError("resource suboperation matrix clone control must re-evaluate parents")
    if row["productionArchiveWrite"] is not False:
        raise TemporalDiscoveryContractError("resource suboperation matrix must not write the production archive")
    if row["mutationDepth"] != 1:
        raise TemporalDiscoveryContractError("resource suboperation matrix mutationDepth drifted")
    if not isinstance(row["childrenPerEligibleCell"], int) or isinstance(row["childrenPerEligibleCell"], bool):
        raise _unexpected("resource suboperation matrix")
    if row["childrenPerEligibleCell"] < 1:
        raise TemporalDiscoveryContractError("resource suboperation matrix childrenPerEligibleCell drifted")
    if row["balanceRule"] != BALANCE_RULE:
        raise TemporalDiscoveryContractError("resource suboperation matrix balanceRule drifted")
    if row["emptyIneligibleCellsAreScientificOutcomes"] is not True:
        raise TemporalDiscoveryContractError("resource suboperation matrix empty-cell rule drifted")
    if row["doNotSampleUniformlyFromFullPlanPool"] is not True:
        raise TemporalDiscoveryContractError("resource suboperation matrix sampling rule drifted")
    if row["notAdmittedOnFrontGenerationPath"] is not True:
        raise TemporalDiscoveryContractError("resource suboperation matrix must stay off the front generation path")
    if tuple(row["lanes"] or ()) != LANES:
        raise TemporalDiscoveryContractError("resource suboperation matrix lanes drifted")
    parents_value = row["parents"]
    if not isinstance(parents_value, list) or not parents_value:
        raise TemporalDiscoveryContractError("resource suboperation matrix requires frozen parents")
    parents: list[dict[str, str]] = []
    seen: set[str] = set()
    has_archive = False
    for item in parents_value:
        parent = _exact_object(item, PARENT_KEYS, "resource suboperation matrix parent")
        candidate_id = parent["candidateId"]
        role = parent["role"]
        if not isinstance(candidate_id, str) or not candidate_id.strip():
            raise _unexpected("resource suboperation matrix parent")
        if role not in PARENT_ROLES:
            raise TemporalDiscoveryContractError("resource suboperation matrix parent role is invalid")
        if candidate_id in seen:
            raise TemporalDiscoveryContractError(f"resource suboperation matrix repeats parent {candidate_id}")
        seen.add(candidate_id)
        if role == "archive":
            has_archive = True
        parents.append({"candidateId": candidate_id, "role": role})
    if not has_archive:
        raise TemporalDiscoveryContractError("resource suboperation matrix requires at least one archive parent")
    body = {
        "schemaVersion": RESOURCE_SUBOPERATION_MATRIX_SCHEMA,
        "mode": RESOURCE_SUBOPERATION_MODE,
        "includeCrossover": False,
        "cloneControl": CLONE_CONTROL,
        "productionArchiveWrite": False,
        "mutationDepth": 1,
        "parents": parents,
        "lanes": list(LANES),
        "childrenPerEligibleCell": row["childrenPerEligibleCell"],
        "balanceRule": BALANCE_RULE,
        "emptyIneligibleCellsAreScientificOutcomes": True,
        "doNotSampleUniformlyFromFullPlanPool": True,
        "notAdmittedOnFrontGenerationPath": True,
    }
    expected = canonical_sha256(body)
    if row["contractSha256"] != expected:
        raise TemporalDiscoveryContractError("resource suboperation matrix identity drift")
    body["contractSha256"] = expected
    return body


def build_resource_suboperation_matrix(
    *,
    parents: Sequence[Mapping[str, str]],
    children_per_eligible_cell: int = 8,
) -> dict[str, Any]:
    body = {
        "schemaVersion": RESOURCE_SUBOPERATION_MATRIX_SCHEMA,
        "mode": RESOURCE_SUBOPERATION_MODE,
        "includeCrossover": False,
        "cloneControl": CLONE_CONTROL,
        "productionArchiveWrite": False,
        "mutationDepth": 1,
        "parents": [
            {"candidateId": str(item["candidateId"]), "role": str(item["role"])} for item in parents
        ],
        "lanes": list(LANES),
        "childrenPerEligibleCell": children_per_eligible_cell,
        "balanceRule": BALANCE_RULE,
        "emptyIneligibleCellsAreScientificOutcomes": True,
        "doNotSampleUniformlyFromFullPlanPool": True,
        "notAdmittedOnFrontGenerationPath": True,
    }
    body["contractSha256"] = canonical_sha256(body)
    return validate_resource_suboperation_matrix(body)
