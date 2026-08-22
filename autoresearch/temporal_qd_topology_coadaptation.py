"""Experiment-only topology co-adaptation matrix contract.

Production rotating 4/5 breeding must omit `topologyCoadaptationMatrix`.
This overlay never launches a market evaluation by itself. The v1 schema is
rejected: it allowed a broad resource family to be labeled parameter-only,
omitted frozen parents from the emitted spec, and did not reseal config
identity.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .evidence_plan import canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError
from .temporal_qd_pair_generation import PAIR_GENERATION_SCHEMA

COADAPTATION_SCHEMA = "temporal_qd_topology_coadaptation_matrix_v2"
COADAPTATION_MODE = "frozen_parent_topology_then_matched_settling_v2"
CLONE_CONTROL = "re_evaluate_parent_on_frozen_panel"
ARMS = (
    "exact_parent_clone",
    "topology_only_child",
    "parameter_only_control",
    "resource_semantic_control",
    "topology_then_parameter_only_settling",
    "topology_then_resource_semantic_settling",
)
FIRST_EXPERIMENT_ARMS = (
    "exact_parent_clone",
    "topology_only_child",
    "resource_semantic_control",
    "topology_then_resource_semantic_settling",
)
PARAMETER_ONLY_KINDS = (
    "indicator_period_mutate",
    "indicator_range_mutate",
    "indicator_timeframe_mutate",
    "indicator_lookback_mutate",
)
RESOURCE_SEMANTIC_KINDS = ("directional_event_insert",)
RESOURCE_KIND_UNIVERSE = (
    "evidence_group_create",
    "evidence_group_remove",
    "evidence_group_split",
    "evidence_group_merge",
    "evidence_member_insert",
    "evidence_member_remove",
    "evidence_weight_mutate",
    "evidence_threshold_mutate",
    "indicator_instance_insert",
    "indicator_instance_remove",
    "indicator_substitute",
    "indicator_timeframe_mutate",
    "indicator_lookback_mutate",
    "indicator_period_mutate",
    "indicator_range_mutate",
    "directional_event_insert",
    "directional_event_remove",
    "directional_event_substitute",
)
FIRST_EXPERIMENT_CONTRAST = "resource_semantic_directional_event_insert"
FIRST_EXPERIMENT_JUSTIFICATION = (
    "V38's recovered positive resource tail was directional_event_insert around two archive parents, "
    "not period/range/lookback/timeframe mutation. The first experiment therefore matches topology "
    "children against a directional_event_insert settling lane and an identical resource-semantic "
    "control. Parameter-only settling remains specified but is not the first contrast because "
    "parameter learning was sparsely sampled rather than demonstrated."
)
SETTLING_ALGORITHM = "deterministic_matched_settling_v2"
PARENT_ROLES = ("archive", "inactive_control", "active_negative_control")
ADMITTED_TOPOLOGY_OPERATIONS = ("insert_setup", "insert_exit_region")
ROOT_KEYS = (
    "schemaVersion",
    "mode",
    "includeCrossover",
    "cloneControl",
    "productionArchiveWrite",
    "mutationDepth",
    "morphologyNurseryDeferred",
    "parents",
    "panelIdentities",
    "topologyPlans",
    "noMixedTopologyOperations",
    "preserveRawAndSettledIdentities",
    "independentConfirmationRequired",
    "firstExperimentContrast",
    "firstExperimentJustification",
    "contrasts",
    "arms",
    "firstExperimentArms",
    "settling",
    "slotBudget",
    "successRule",
    "notAdmittedOnFrontGenerationPath",
    "contractSha256",
)
PARENT_KEYS = ("candidateId", "role")
PANEL_KEYS = ("developmentPanelId", "confirmationPanelIds", "rotatingEvidenceSha256")
PLAN_KEYS = (
    "planId",
    "operation",
    "operatorSchema",
    "schemaVersion",
    "arguments",
    "v38ExampleOperatorPlanSha256",
)
CONTRAST_KEYS = ("contrastId", "settlingLane", "controlLane", "includedInFirstExperiment")
SETTLING_KEYS = (
    "algorithmId",
    "parameterOnly",
    "resourceSemantic",
    "developmentPanelUse",
    "independentPanelConfirmation",
)
LANE_KEYS = (
    "eligibleKinds",
    "forbiddenKinds",
    "maxSettlingPlans",
    "planSelection",
    "applicationMode",
    "evaluateIntermediatesOnDevelopmentPanel",
    "winnerSelection",
    "matchedControlBudget",
)
SLOT_KEYS = (
    "cloneCountPerParent",
    "topologyOnlyCountPerParentPerPlan",
    "parameterOnlyControlCountPerParent",
    "resourceSemanticControlCountPerParent",
    "topologyThenParameterSettlingCountPerParentPerPlan",
    "topologyThenSemanticSettlingCountPerParentPerPlan",
    "firstExperimentSlotCount",
)
SUCCESS_KEYS = (
    "noveltyIsNotQuality",
    "requireRepeatablePositiveParentRelativeTail",
    "forbidSystematicallyWorseWorstWindow",
    "requireIndependentPanelSurvival",
    "doNotPromoteOnDevelopmentPanelAlone",
)
ARGUMENT_KEYS_BY_OPERATION = {
    "insert_setup": ("edgeId", "guard", "kind"),
    "insert_exit_region": ("guard", "kind", "priority"),
}


def _unexpected(label: str) -> TemporalDiscoveryContractError:
    return TemporalDiscoveryContractError(f"{label} has an unexpected schema")


def _exact_object(value: Any, required: Sequence[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise _unexpected(label)
    if set(value) != set(required):
        raise _unexpected(label)
    return dict(value)


def _require_bool(value: Any, expected: bool, label: str) -> None:
    if value is not expected:
        raise TemporalDiscoveryContractError(f"{label} drifted")


def _require_int(value: Any, expected: int, label: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value != expected:
        raise TemporalDiscoveryContractError(f"{label} drifted")


def _require_text(value: Any, expected: str, label: str) -> str:
    if value != expected:
        raise TemporalDiscoveryContractError(f"{label} drifted")
    return expected


def _require_sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.startswith("sha256:") or len(value) != 71:
        raise TemporalDiscoveryContractError(f"{label} drifted")
    digest = value[7:]
    if not all(ch in "0123456789abcdef" for ch in digest):
        raise TemporalDiscoveryContractError(f"{label} drifted")
    return value


def _kinds_tuple(value: Any, expected: Sequence[str], label: str) -> list[str]:
    if not isinstance(value, list) or tuple(value) != tuple(expected):
        raise TemporalDiscoveryContractError(f"{label} drifted")
    return list(expected)


def _forbidden_for(eligible: Sequence[str]) -> list[str]:
    return [kind for kind in RESOURCE_KIND_UNIVERSE if kind not in eligible]


def _validate_parents(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list) or not value:
        raise TemporalDiscoveryContractError("topology coadaptation requires frozen parents")
    parents: list[dict[str, str]] = []
    seen: set[str] = set()
    has_archive = False
    for item in value:
        row = _exact_object(item, PARENT_KEYS, "topology coadaptation parent")
        candidate_id = row["candidateId"]
        role = row["role"]
        if not isinstance(candidate_id, str) or not candidate_id.strip():
            raise _unexpected("topology coadaptation parent")
        if role not in PARENT_ROLES:
            raise TemporalDiscoveryContractError("topology coadaptation parent role is invalid")
        if candidate_id in seen:
            raise TemporalDiscoveryContractError(f"topology coadaptation repeats parent {candidate_id}")
        seen.add(candidate_id)
        if role == "archive":
            has_archive = True
        parents.append({"candidateId": candidate_id, "role": role})
    if not has_archive:
        raise TemporalDiscoveryContractError("topology coadaptation requires at least one archive parent")
    return parents


def _validate_panels(value: Any) -> dict[str, Any]:
    row = _exact_object(value, PANEL_KEYS, "topology coadaptation panelIdentities")
    development = row["developmentPanelId"]
    confirmation = row["confirmationPanelIds"]
    if development != "panel-3":
        raise TemporalDiscoveryContractError("topology coadaptation panelIdentities drifted")
    if not isinstance(confirmation, list) or tuple(confirmation) != ("panel-1", "panel-2"):
        raise TemporalDiscoveryContractError("topology coadaptation panelIdentities drifted")
    rotating = _require_sha(row["rotatingEvidenceSha256"], "topology coadaptation panelIdentities")
    return {
        "developmentPanelId": "panel-3",
        "confirmationPanelIds": ["panel-1", "panel-2"],
        "rotatingEvidenceSha256": rotating,
    }


def _validate_arguments(operation: str, value: Any) -> dict[str, Any]:
    required = ARGUMENT_KEYS_BY_OPERATION.get(operation)
    if required is None:
        raise TemporalDiscoveryContractError("topology coadaptation topology plan operation drifted")
    return _exact_object(value, required, "topology coadaptation topology plan arguments")


def _validate_plans(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise TemporalDiscoveryContractError("topology coadaptation requires exact topology plans")
    plans: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_ops: set[str] = set()
    for item in value:
        row = _exact_object(item, PLAN_KEYS, "topology coadaptation topology plan")
        operation = row["operation"]
        if operation not in ADMITTED_TOPOLOGY_OPERATIONS:
            raise TemporalDiscoveryContractError("topology coadaptation topology plan operation drifted")
        plan_id = row["planId"]
        if not isinstance(plan_id, str) or not plan_id.strip() or plan_id in seen_ids:
            raise _unexpected("topology coadaptation topology plan")
        if operation in seen_ops:
            raise TemporalDiscoveryContractError("topology coadaptation mixed topology operations")
        seen_ids.add(plan_id)
        seen_ops.add(operation)
        if row["operatorSchema"] != "evolvable_module_topology_operator_v1":
            raise TemporalDiscoveryContractError("topology coadaptation topology plan drifted")
        if row["schemaVersion"] != "evolvable_module_topology_plan_v1":
            raise TemporalDiscoveryContractError("topology coadaptation topology plan drifted")
        plans.append(
            {
                "planId": plan_id,
                "operation": operation,
                "operatorSchema": "evolvable_module_topology_operator_v1",
                "schemaVersion": "evolvable_module_topology_plan_v1",
                "arguments": _validate_arguments(operation, row["arguments"]),
                "v38ExampleOperatorPlanSha256": _require_sha(
                    row["v38ExampleOperatorPlanSha256"],
                    "topology coadaptation topology plan",
                ),
            }
        )
    if set(seen_ops) != set(ADMITTED_TOPOLOGY_OPERATIONS):
        raise TemporalDiscoveryContractError("topology coadaptation topology plans drifted")
    return plans


def _validate_contrasts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) != 2:
        raise TemporalDiscoveryContractError("topology coadaptation contrasts drifted")
    expected = (
        {
            "contrastId": "topology_plus_parameter_only_vs_parameter_only_control",
            "settlingLane": "parameter_only",
            "controlLane": "parameter_only",
            "includedInFirstExperiment": False,
        },
        {
            "contrastId": "topology_plus_resource_semantic_vs_resource_semantic_control",
            "settlingLane": "resource_semantic",
            "controlLane": "resource_semantic",
            "includedInFirstExperiment": True,
        },
    )
    contrasts: list[dict[str, Any]] = []
    for item, want in zip(value, expected, strict=True):
        row = _exact_object(item, CONTRAST_KEYS, "topology coadaptation contrast")
        if row != want:
            raise TemporalDiscoveryContractError("topology coadaptation contrasts drifted")
        contrasts.append(dict(want))
    return contrasts


def _validate_lane(value: Any, *, eligible: Sequence[str], label: str) -> dict[str, Any]:
    row = _exact_object(value, LANE_KEYS, label)
    _kinds_tuple(row["eligibleKinds"], eligible, label)
    _kinds_tuple(row["forbiddenKinds"], _forbidden_for(eligible), label)
    if any(kind in eligible for kind in row["forbiddenKinds"]):
        raise TemporalDiscoveryContractError(f"{label} drifted")
    if "directional_event_insert" in eligible and eligible != RESOURCE_SEMANTIC_KINDS:
        raise TemporalDiscoveryContractError(f"{label} drifted")
    if tuple(eligible) == PARAMETER_ONLY_KINDS and "directional_event_insert" in row["eligibleKinds"]:
        raise TemporalDiscoveryContractError("parameter-only lane cannot admit broad resource kinds")
    _require_int(row["maxSettlingPlans"], 1, label)
    _require_text(row["planSelection"], "lexicographic_canonical_construction_identity", label)
    _require_text(row["applicationMode"], "sequential_single_step_from_pre_settling_genome", label)
    _require_bool(row["evaluateIntermediatesOnDevelopmentPanel"], True, label)
    _require_text(row["winnerSelection"], "only_candidate_when_maxSettlingPlans_is_1", label)
    _require_text(
        row["matchedControlBudget"],
        "identical_eligible_kind_set_order_and_maxSettlingPlans",
        label,
    )
    if "directional_event_insert" in row["eligibleKinds"] and tuple(eligible) == PARAMETER_ONLY_KINDS:
        raise TemporalDiscoveryContractError("parameter-only lane cannot admit directional_event_insert")
    return {
        "eligibleKinds": list(eligible),
        "forbiddenKinds": _forbidden_for(eligible),
        "maxSettlingPlans": 1,
        "planSelection": "lexicographic_canonical_construction_identity",
        "applicationMode": "sequential_single_step_from_pre_settling_genome",
        "evaluateIntermediatesOnDevelopmentPanel": True,
        "winnerSelection": "only_candidate_when_maxSettlingPlans_is_1",
        "matchedControlBudget": "identical_eligible_kind_set_order_and_maxSettlingPlans",
    }


def _validate_settling(value: Any) -> dict[str, Any]:
    row = _exact_object(value, SETTLING_KEYS, "topology coadaptation settling")
    _require_text(row["algorithmId"], SETTLING_ALGORITHM, "topology coadaptation settling")
    _require_text(
        row["developmentPanelUse"],
        "frozen_v38_development_panel_only",
        "topology coadaptation settling",
    )
    _require_text(
        row["independentPanelConfirmation"],
        "required_before_any_production_conclusion",
        "topology coadaptation settling",
    )
    parameter_only = _validate_lane(
        row["parameterOnly"],
        eligible=PARAMETER_ONLY_KINDS,
        label="topology coadaptation parameterOnly",
    )
    if "directional_event_insert" in parameter_only["eligibleKinds"]:
        raise TemporalDiscoveryContractError("parameter-only lane cannot admit directional_event_insert")
    if any(kind not in PARAMETER_ONLY_KINDS for kind in parameter_only["eligibleKinds"]):
        raise TemporalDiscoveryContractError("parameter-only lane cannot admit broad resource kinds")
    resource_semantic = _validate_lane(
        row["resourceSemantic"],
        eligible=RESOURCE_SEMANTIC_KINDS,
        label="topology coadaptation resourceSemantic",
    )
    return {
        "algorithmId": SETTLING_ALGORITHM,
        "parameterOnly": parameter_only,
        "resourceSemantic": resource_semantic,
        "developmentPanelUse": "frozen_v38_development_panel_only",
        "independentPanelConfirmation": "required_before_any_production_conclusion",
    }


def _validate_slot_budget(
    value: Any,
    *,
    parent_count: int,
    plan_count: int,
) -> dict[str, int]:
    row = _exact_object(value, SLOT_KEYS, "topology coadaptation slotBudget")
    expected = {
        "cloneCountPerParent": 1,
        "topologyOnlyCountPerParentPerPlan": 1,
        "parameterOnlyControlCountPerParent": 1,
        "resourceSemanticControlCountPerParent": 1,
        "topologyThenParameterSettlingCountPerParentPerPlan": 1,
        "topologyThenSemanticSettlingCountPerParentPerPlan": 1,
        "firstExperimentSlotCount": parent_count * (1 + plan_count + 1 + plan_count),
    }
    for key, want in expected.items():
        _require_int(row[key], want, "topology coadaptation slotBudget")
    return expected


def _validate_success(value: Any) -> dict[str, bool]:
    row = _exact_object(value, SUCCESS_KEYS, "topology coadaptation successRule")
    expected = {key: True for key in SUCCESS_KEYS}
    if row != expected:
        raise TemporalDiscoveryContractError("topology coadaptation successRule drifted")
    return expected


def validate_topology_coadaptation_matrix(contract: Mapping[str, Any]) -> dict[str, Any]:
    row = _exact_object(contract, ROOT_KEYS, "topology coadaptation")
    _require_text(row["schemaVersion"], COADAPTATION_SCHEMA, "topology coadaptation schema")
    _require_text(row["mode"], COADAPTATION_MODE, "topology coadaptation mode")
    _require_bool(row["includeCrossover"], False, "topology coadaptation includeCrossover")
    _require_text(row["cloneControl"], CLONE_CONTROL, "topology coadaptation cloneControl")
    if row["productionArchiveWrite"] is not False:
        raise TemporalDiscoveryContractError("topology coadaptation must not write the production archive")
    _require_int(row["mutationDepth"], 1, "topology coadaptation mutationDepth")
    _require_bool(row["morphologyNurseryDeferred"], True, "topology coadaptation morphologyNurseryDeferred")
    _require_bool(row["noMixedTopologyOperations"], True, "topology coadaptation noMixedTopologyOperations")
    _require_bool(row["preserveRawAndSettledIdentities"], True, "topology coadaptation preserveRawAndSettledIdentities")
    _require_bool(
        row["independentConfirmationRequired"],
        True,
        "topology coadaptation independentConfirmationRequired",
    )
    _require_text(
        row["firstExperimentContrast"],
        FIRST_EXPERIMENT_CONTRAST,
        "topology coadaptation firstExperimentContrast",
    )
    justification = _require_text(
        row["firstExperimentJustification"],
        FIRST_EXPERIMENT_JUSTIFICATION,
        "topology coadaptation firstExperimentJustification",
    )
    _kinds_tuple(row["arms"], ARMS, "topology coadaptation arms")
    _kinds_tuple(row["firstExperimentArms"], FIRST_EXPERIMENT_ARMS, "topology coadaptation firstExperimentArms")
    _require_bool(
        row["notAdmittedOnFrontGenerationPath"],
        True,
        "topology coadaptation notAdmittedOnFrontGenerationPath",
    )
    parents = _validate_parents(row["parents"])
    panel_identities = _validate_panels(row["panelIdentities"])
    topology_plans = _validate_plans(row["topologyPlans"])
    contrasts = _validate_contrasts(row["contrasts"])
    settling = _validate_settling(row["settling"])
    slot_budget = _validate_slot_budget(
        row["slotBudget"],
        parent_count=len(parents),
        plan_count=len(topology_plans),
    )
    success_rule = _validate_success(row["successRule"])
    body = {
        "schemaVersion": COADAPTATION_SCHEMA,
        "mode": COADAPTATION_MODE,
        "includeCrossover": False,
        "cloneControl": CLONE_CONTROL,
        "productionArchiveWrite": False,
        "mutationDepth": 1,
        "morphologyNurseryDeferred": True,
        "parents": parents,
        "panelIdentities": panel_identities,
        "topologyPlans": topology_plans,
        "noMixedTopologyOperations": True,
        "preserveRawAndSettledIdentities": True,
        "independentConfirmationRequired": True,
        "firstExperimentContrast": FIRST_EXPERIMENT_CONTRAST,
        "firstExperimentJustification": justification,
        "contrasts": contrasts,
        "arms": list(ARMS),
        "firstExperimentArms": list(FIRST_EXPERIMENT_ARMS),
        "settling": settling,
        "slotBudget": slot_budget,
        "successRule": success_rule,
        "notAdmittedOnFrontGenerationPath": True,
    }
    expected_hash = canonical_sha256(body)
    if row["contractSha256"] != expected_hash:
        raise TemporalDiscoveryContractError("topology coadaptation identity drift")
    body["contractSha256"] = expected_hash
    return body


def topology_coadaptation_from_config(
    config: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(config, Mapping):
        return None
    overlay = config.get("topologyCoadaptationMatrix")
    if overlay is None:
        return None
    return validate_topology_coadaptation_matrix(overlay)


def attach_topology_coadaptation_matrix(
    generation_config: Mapping[str, Any],
    matrix: Mapping[str, Any],
) -> dict[str, Any]:
    if generation_config.get("schemaVersion") != PAIR_GENERATION_SCHEMA:
        raise TemporalDiscoveryContractError("topology coadaptation overlay requires pair generation v2")
    if "topologyCoadaptationMatrix" in generation_config:
        raise TemporalDiscoveryContractError("topology coadaptation overlay was supplied twice")
    config = {
        key: value
        for key, value in generation_config.items()
        if key != "configSha256"
    }
    config["topologyCoadaptationMatrix"] = validate_topology_coadaptation_matrix(matrix)
    config["configSha256"] = canonical_sha256(config)
    return config


def _lane_body(eligible: Sequence[str]) -> dict[str, Any]:
    return {
        "eligibleKinds": list(eligible),
        "forbiddenKinds": _forbidden_for(eligible),
        "maxSettlingPlans": 1,
        "planSelection": "lexicographic_canonical_construction_identity",
        "applicationMode": "sequential_single_step_from_pre_settling_genome",
        "evaluateIntermediatesOnDevelopmentPanel": True,
        "winnerSelection": "only_candidate_when_maxSettlingPlans_is_1",
        "matchedControlBudget": "identical_eligible_kind_set_order_and_maxSettlingPlans",
    }


def build_topology_coadaptation_matrix(
    *,
    parents: Sequence[Mapping[str, str]],
    rotating_evidence_sha256: str,
    topology_plans: Sequence[Mapping[str, Any]],
    first_experiment_justification: str,
) -> dict[str, Any]:
    parent_rows = [
        {"candidateId": str(item["candidateId"]), "role": str(item["role"])} for item in parents
    ]
    plan_rows = [dict(item) for item in topology_plans]
    body = {
        "schemaVersion": COADAPTATION_SCHEMA,
        "mode": COADAPTATION_MODE,
        "includeCrossover": False,
        "cloneControl": CLONE_CONTROL,
        "productionArchiveWrite": False,
        "mutationDepth": 1,
        "morphologyNurseryDeferred": True,
        "parents": parent_rows,
        "panelIdentities": {
            "developmentPanelId": "panel-3",
            "confirmationPanelIds": ["panel-1", "panel-2"],
            "rotatingEvidenceSha256": rotating_evidence_sha256,
        },
        "topologyPlans": plan_rows,
        "noMixedTopologyOperations": True,
        "preserveRawAndSettledIdentities": True,
        "independentConfirmationRequired": True,
        "firstExperimentContrast": FIRST_EXPERIMENT_CONTRAST,
        "firstExperimentJustification": first_experiment_justification,
        "contrasts": [
            {
                "contrastId": "topology_plus_parameter_only_vs_parameter_only_control",
                "settlingLane": "parameter_only",
                "controlLane": "parameter_only",
                "includedInFirstExperiment": False,
            },
            {
                "contrastId": "topology_plus_resource_semantic_vs_resource_semantic_control",
                "settlingLane": "resource_semantic",
                "controlLane": "resource_semantic",
                "includedInFirstExperiment": True,
            },
        ],
        "arms": list(ARMS),
        "firstExperimentArms": list(FIRST_EXPERIMENT_ARMS),
        "settling": {
            "algorithmId": SETTLING_ALGORITHM,
            "parameterOnly": _lane_body(PARAMETER_ONLY_KINDS),
            "resourceSemantic": _lane_body(RESOURCE_SEMANTIC_KINDS),
            "developmentPanelUse": "frozen_v38_development_panel_only",
            "independentPanelConfirmation": "required_before_any_production_conclusion",
        },
        "slotBudget": {
            "cloneCountPerParent": 1,
            "topologyOnlyCountPerParentPerPlan": 1,
            "parameterOnlyControlCountPerParent": 1,
            "resourceSemanticControlCountPerParent": 1,
            "topologyThenParameterSettlingCountPerParentPerPlan": 1,
            "topologyThenSemanticSettlingCountPerParentPerPlan": 1,
            "firstExperimentSlotCount": len(parent_rows) * (1 + len(plan_rows) + 1 + len(plan_rows)),
        },
        "successRule": {key: True for key in SUCCESS_KEYS},
        "notAdmittedOnFrontGenerationPath": True,
    }
    body["contractSha256"] = canonical_sha256({key: value for key, value in body.items() if key != "contractSha256"})
    return validate_topology_coadaptation_matrix(body)
