"""Versioned TE-only production eligibility for the topology study."""

from __future__ import annotations

import math
from typing import Any, Mapping

from .evidence_plan import canonical_sha256
from .temporal_qd_evolution import (
    DIRECTIONAL_QD_POLICY_AUTHORITY,
    production_direction_selection,
    production_quality_member_eligible,
)
from .temporal_qd_topology_coadaptation_v7 import promising_coadaptation_observation

POLICY_SCHEMA = "temporal_qd_topology_panel_usefulness_policy_v2"
PANEL_SCHEMA = "temporal_qd_topology_panel_usefulness_v2"
SUPPORT_SCHEMA = "temporal_qd_topology_support_eligibility_v2"
QUALITY_SCHEMA = "temporal_qd_topology_quality_lane_eligibility_v2"
DIRECTION_SCHEMA = "temporal_qd_topology_direction_eligibility_v2"
ARM_PARITY_SCHEMA = "temporal_qd_topology_arm_eligibility_projection_v2"
REPLICATION_SCHEMA = "temporal_qd_topology_replication_survival_projection_v3"
ARMS = ("P", "T", "E", "TE")
MINIMUM_TOTAL_TRADES = 8
MINIMUM_TRADES_PER_WINDOW = 4


class PanelUsefulnessPolicyError(ValueError):
    """Policy evidence is incomplete or not the frozen production authority."""


def _finite(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PanelUsefulnessPolicyError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise PanelUsefulnessPolicyError(f"{label} must be finite")
    return result


def _boolean(value: Any, label: str) -> bool:
    if type(value) is not bool:
        raise PanelUsefulnessPolicyError(f"{label} must be Boolean")
    return value


def _greater(left: float, right: float) -> bool:
    return left - right > 1e-12


def _not_worse(left: float, right: float) -> bool:
    return left - right >= -1e-12


def verify_archive_policy_authority(authority: Mapping[str, Any]) -> dict[str, Any]:
    expected = DIRECTIONAL_QD_POLICY_AUTHORITY
    if dict(authority) != expected:
        raise PanelUsefulnessPolicyError("archive policy authority is not the exact frozen production policy")
    frozen = authority.get("frozenPolicy")
    if not isinstance(frozen, Mapping):
        raise PanelUsefulnessPolicyError("archive frozen policy is missing")
    support = frozen.get("tradeSupport")
    direction = frozen.get("directionSelection")
    lanes = frozen.get("archive", {}).get("lanes") if isinstance(frozen.get("archive"), Mapping) else None
    if (
        not isinstance(support, Mapping)
        or support.get("minimumTotalTrades") != MINIMUM_TOTAL_TRADES
        or support.get("minimumTradesPerWindow") != MINIMUM_TRADES_PER_WINDOW
        or not isinstance(direction, Mapping)
        or direction.get("selectionPolicySha256")
        != "sha256:2567175ff6ae6063baa485484c0faa0d742507af6814a593076020a68aef3ed1"
        or not isinstance(lanes, Mapping)
        or lanes.get("quality") != "finite_support_and_nonnegative_robust_return"
    ):
        raise PanelUsefulnessPolicyError("archive support/quality/direction material drifted")
    return dict(authority)


def support_eligibility(
    member: Mapping[str, Any], archive_policy_authority: Mapping[str, Any]
) -> dict[str, Any]:
    authority = verify_archive_policy_authority(archive_policy_authority)
    validity = member.get("finiteDataValidity")
    aggregate = member.get("aggregate")
    if not isinstance(validity, Mapping) or not isinstance(aggregate, Mapping):
        raise PanelUsefulnessPolicyError("member support evidence is incomplete")
    checks = validity.get("checks")
    if not isinstance(checks, Mapping):
        raise PanelUsefulnessPolicyError("member support checks are missing")
    counts = validity.get("tradeCountsByWindow")
    if (
        not isinstance(counts, list)
        or len(counts) != 4
        or any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in counts)
    ):
        raise PanelUsefulnessPolicyError("member support window counts are invalid")
    total = validity.get("totalTrades")
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        raise PanelUsefulnessPolicyError("member support totalTrades is invalid")
    if (
        validity.get("minimumTotalTrades") != MINIMUM_TOTAL_TRADES
        or validity.get("minimumTradesPerWindow") != MINIMUM_TRADES_PER_WINDOW
        or sum(counts) != total
        or aggregate.get("totalTrades") != total
        or aggregate.get("tradeCountsByWindow") != counts
    ):
        raise PanelUsefulnessPolicyError("member support threshold/count binding drifted")
    finite = _boolean(validity.get("isFiniteData"), "isFiniteData")
    passes = _boolean(validity.get("passesSupportGate"), "passesSupportGate")
    valid_quality = _boolean(validity.get("validForQuality"), "validForQuality")
    finite_check = _boolean(checks.get("finiteEconomicMetrics"), "finiteEconomicMetrics")
    total_check = _boolean(checks.get("minimumTotalTrades"), "minimumTotalTrades check")
    windows_check = _boolean(
        checks.get("minimumTradesEveryWindow"), "minimumTradesEveryWindow check"
    )
    positive_check = _boolean(
        checks.get("positiveObservationSupport"), "positiveObservationSupport check"
    )
    expected_total = total >= MINIMUM_TOTAL_TRADES
    expected_windows = all(value >= MINIMUM_TRADES_PER_WINDOW for value in counts)
    observations = aggregate.get("totalObservations")
    if isinstance(observations, bool) or not isinstance(observations, int) or observations < 0:
        raise PanelUsefulnessPolicyError("member totalObservations is invalid")
    expected_positive = observations > 0
    expected_finite = math.isfinite(
        _finite(aggregate.get("worstWindowConservativeNetR"), "aggregate worst window")
    ) and math.isfinite(_finite(aggregate.get("maxWindowDrawdownR"), "aggregate drawdown"))
    expected_passes = expected_total and expected_windows and expected_positive
    if (
        finite != expected_finite
        or finite_check != finite
        or total_check != expected_total
        or windows_check != expected_windows
        or positive_check != expected_positive
        or passes != expected_passes
        or valid_quality != (finite and passes)
    ):
        raise PanelUsefulnessPolicyError("member support Boolean derivation drifted")
    reasons: list[str] = []
    if not finite:
        reasons.append("nonfinite_data")
    if not expected_total:
        reasons.append("minimum_total_trades_failed")
    if not expected_windows:
        reasons.append("minimum_trades_per_window_failed")
    if not expected_positive:
        reasons.append("positive_observation_support_failed")
    if not valid_quality:
        reasons.append("finite_support_quality_validity_failed")
    eligible = finite and passes and valid_quality
    return {
        "schemaVersion": SUPPORT_SCHEMA,
        "eligible": eligible,
        "reasonCodes": ["eligible"] if eligible else reasons,
        "minimumTotalTrades": MINIMUM_TOTAL_TRADES,
        "minimumTradesPerWindow": MINIMUM_TRADES_PER_WINDOW,
        "archivePolicySha256": authority["policySha256"],
    }


def quality_lane_eligibility(
    member: Mapping[str, Any],
    support: Mapping[str, Any],
    archive_policy_authority: Mapping[str, Any],
) -> dict[str, Any]:
    authority = verify_archive_policy_authority(archive_policy_authority)
    if support.get("schemaVersion") != SUPPORT_SCHEMA or type(support.get("eligible")) is not bool:
        raise PanelUsefulnessPolicyError("support projection is incompatible")
    objectives = member.get("objectives")
    aggregate = member.get("aggregate")
    if not isinstance(objectives, Mapping) or not isinstance(aggregate, Mapping):
        raise PanelUsefulnessPolicyError("quality objectives are incomplete")
    worst = _finite(objectives.get("worstWindowConservativeNetR"), "quality worst window")
    if worst != _finite(aggregate.get("worstWindowConservativeNetR"), "aggregate worst window"):
        raise PanelUsefulnessPolicyError("quality worst-window binding drifted")
    eligible = production_quality_member_eligible(member)
    expected = bool(support["eligible"] and worst >= 0.0)
    if eligible != expected:
        raise PanelUsefulnessPolicyError("production quality helper disagrees with bound evidence")
    reason = "eligible" if eligible else (
        "support_ineligible" if not support["eligible"] else "negative_worst_window_robust_return"
    )
    return {
        "schemaVersion": QUALITY_SCHEMA,
        "eligible": eligible,
        "reasonCode": reason,
        "qualityLane": "finite_support_and_nonnegative_robust_return",
        "worstWindowConservativeNetR": worst,
        "archivePolicySha256": authority["policySha256"],
    }


def direction_eligibility(
    member: Mapping[str, Any], archive_policy_authority: Mapping[str, Any]
) -> dict[str, Any]:
    authority = verify_archive_policy_authority(archive_policy_authority)
    aggregate = member.get("aggregate")
    if not isinstance(aggregate, Mapping):
        raise PanelUsefulnessPolicyError("direction aggregate is missing")
    selection = production_direction_selection(aggregate)
    expected_sha = authority["frozenPolicy"]["directionSelection"]["selectionPolicySha256"]
    if selection.get("policyIdentitySha256") != expected_sha:
        raise PanelUsefulnessPolicyError("direction selection policy identity drifted")
    eligible = selection.get("selectionEligible")
    if type(eligible) is not bool:
        raise PanelUsefulnessPolicyError("direction selection eligibility is not Boolean")
    return {
        "schemaVersion": DIRECTION_SCHEMA,
        "eligible": eligible,
        "reasonCode": "eligible" if eligible else str(selection["lane"]),
        "selectionPolicySha256": expected_sha,
        "selection": selection,
    }


def arm_eligibility(
    member: Mapping[str, Any], archive_policy_authority: Mapping[str, Any]
) -> dict[str, Any]:
    support = support_eligibility(member, archive_policy_authority)
    return {
        "supportEligibility": support,
        "qualityLaneEligibility": quality_lane_eligibility(
            member, support, archive_policy_authority
        ),
        "directionSelection": direction_eligibility(member, archive_policy_authority),
    }


def arm_eligibility_parity_projection(
    member: Mapping[str, Any], archive_policy_authority: Mapping[str, Any]
) -> dict[str, Any]:
    """Exact compact Python/Rust projection from raw production evidence."""

    eligibility = arm_eligibility(member, archive_policy_authority)
    direction = eligibility["directionSelection"]
    selection = direction["selection"]
    result: dict[str, Any] = {
        "schemaVersion": ARM_PARITY_SCHEMA,
        "supportEligibility": eligibility["supportEligibility"],
        "qualityLaneEligibility": eligibility["qualityLaneEligibility"],
        "directionSelection": {
            "schemaVersion": DIRECTION_SCHEMA,
            "eligible": direction["eligible"],
            "reasonCode": direction["reasonCode"],
            "selectionPolicySha256": direction["selectionPolicySha256"],
            "realizedBehaviorIdentitySha256": selection["realizedBehaviorIdentitySha256"],
            "lane": selection["lane"],
            "specialistSide": selection["specialistSide"],
            "sides": {
                side: {
                    "supported": selection["sides"][side]["supported"],
                    "acceptable": selection["sides"][side]["acceptable"],
                    "materiallyHarmful": selection["sides"][side]["materiallyHarmful"],
                }
                for side in ("long", "short")
            },
        },
    }
    result["eligibilityProjectionSha256"] = canonical_sha256(result)
    return result


def evaluate_panel_usefulness_v2(arms: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    if set(arms) != set(ARMS):
        raise PanelUsefulnessPolicyError("panel must contain exact P/T/E/TE arms")
    normalized: dict[str, dict[str, Any]] = {}
    for arm in ARMS:
        row = arms[arm]
        support = row.get("supportEligibility")
        quality = row.get("qualityLaneEligibility")
        direction = row.get("directionSelection")
        if not all(isinstance(value, Mapping) for value in (support, quality, direction)):
            raise PanelUsefulnessPolicyError(f"{arm} eligibility evidence is incomplete")
        if (
            support.get("schemaVersion") != SUPPORT_SCHEMA
            or quality.get("schemaVersion") != QUALITY_SCHEMA
            or direction.get("schemaVersion") != DIRECTION_SCHEMA
            or any(type(value.get("eligible")) is not bool for value in (support, quality, direction))
        ):
            raise PanelUsefulnessPolicyError(f"{arm} eligibility projection is incompatible")
        trade_count = row.get("tradeCount")
        if isinstance(trade_count, bool) or not isinstance(trade_count, int) or trade_count < 0:
            raise PanelUsefulnessPolicyError(f"{arm} tradeCount is invalid")
        normalized[arm] = {
            "candidateId": str(row["candidateId"]),
            "conservativeNetR": _finite(row.get("conservativeNetR"), f"{arm} net"),
            "worstWindowConservativeNetR": _finite(
                row.get("worstWindowConservativeNetR"), f"{arm} worst window"
            ),
            "tradeCount": trade_count,
            "costDragR": _finite(row.get("costDragR"), f"{arm} cost drag"),
            "supportEligibility": dict(support),
            "qualityLaneEligibility": dict(quality),
            "directionSelection": dict(direction),
            "identity": dict(row["identity"]),
        }
    observation = promising_coadaptation_observation(
        parent_net=normalized["P"]["conservativeNetR"],
        topology_net=normalized["T"]["conservativeNetR"],
        event_net=normalized["E"]["conservativeNetR"],
        combined_net=normalized["TE"]["conservativeNetR"],
        parent_worst=normalized["P"]["worstWindowConservativeNetR"],
        topology_worst=normalized["T"]["worstWindowConservativeNetR"],
        event_worst=normalized["E"]["worstWindowConservativeNetR"],
        combined_worst=normalized["TE"]["worstWindowConservativeNetR"],
        metric_greater=_greater,
        metric_not_worse=_not_worse,
    )
    te = normalized["TE"]
    te_support = te["supportEligibility"]["eligible"]
    te_quality = te["qualityLaneEligibility"]["eligible"]
    te_direction = te["directionSelection"]["eligible"]
    useful = bool(
        observation["usefulProgressiveInnovation"]
        and te_support
        and te_quality
        and te_direction
    )
    result: dict[str, Any] = {
        "schemaVersion": PANEL_SCHEMA,
        "comparisonEvidenceComplete": True,
        "arms": normalized,
        "teMinusP": te["conservativeNetR"] - normalized["P"]["conservativeNetR"],
        "teMinusT": te["conservativeNetR"] - normalized["T"]["conservativeNetR"],
        "teMinusE": te["conservativeNetR"] - normalized["E"]["conservativeNetR"],
        "signedInteraction": observation["interactionNetR"],
        "combinedOutperformsParentAndSingles": bool(
            observation["teNetGreaterThanP"]
            and observation["teNetGreaterThanT"]
            and observation["teNetGreaterThanE"]
        ),
        "riskNonWorseThanParentAndSingles": bool(
            observation["teWorstWindowNotWorseThanP"]
            and observation["teWorstWindowNotWorseThanTAndE"]
        ),
        "teSupportEligible": te_support,
        "teQualityEligible": te_quality,
        "teDirectionEligible": te_direction,
        "allArmEligibilityDiagnostic": {
            arm: {
                "support": normalized[arm]["supportEligibility"]["eligible"],
                "quality": normalized[arm]["qualityLaneEligibility"]["eligible"],
                "direction": normalized[arm]["directionSelection"]["eligible"],
            }
            for arm in ARMS
        },
        "nonqualifyingRiskTradeoff": observation["nonqualifyingRiskTradeoff"],
        "usefulProgressiveInnovationV2": useful,
    }
    result["panelUsefulnessSha256"] = canonical_sha256(result)
    return result


def evaluate_replication_survival_v3(
    panel_useful: Mapping[str, bool | None], *, identities_valid: bool
) -> dict[str, Any]:
    """Name U_v2 explicitly while preserving the frozen strict-all operator."""

    panels = ("panel-3", "panel-1", "panel-2")
    values = {panel: panel_useful.get(panel) for panel in panels}
    complete = identities_valid and all(type(values[panel]) is bool for panel in panels)
    development = complete and values["panel-3"] is True
    replication = complete and values["panel-1"] is True and values["panel-2"] is True
    promising = development and replication
    if not complete:
        category = "incomplete_invalid"
    elif promising:
        category = "inspected_promising_pending_untouched_confirmation"
    elif development and values["panel-1"] is False and values["panel-2"] is False:
        category = "development_only_not_replicated"
    elif development:
        category = "mixed_panel_nonqualifying"
    elif values["panel-1"] is True or values["panel-2"] is True:
        category = "replication_only_discordant_not_promising"
    else:
        category = "complete_no_useful_panel"
    result: dict[str, Any] = {
        "schemaVersion": REPLICATION_SCHEMA,
        "panelLocalPredicate": "U_v2",
        "panelUsefulProgressiveInnovationV2": values,
        "evidenceCompleteAndIdentityValid": complete,
        "developmentQualified": development,
        "replicationSurviving": replication,
        "inspectedPromising": promising,
        "reportingCategory": category,
        "confirmationStatus": "pending",
        "confirmationPredicate": "U_v2_same_exact_block",
    }
    result["projectionSha256"] = canonical_sha256(result)
    return result


__all__ = [
    "ARMS",
    "ARM_PARITY_SCHEMA",
    "DIRECTION_SCHEMA",
    "PANEL_SCHEMA",
    "POLICY_SCHEMA",
    "PanelUsefulnessPolicyError",
    "QUALITY_SCHEMA",
    "REPLICATION_SCHEMA",
    "SUPPORT_SCHEMA",
    "arm_eligibility",
    "arm_eligibility_parity_projection",
    "direction_eligibility",
    "evaluate_panel_usefulness_v2",
    "evaluate_replication_survival_v3",
    "quality_lane_eligibility",
    "support_eligibility",
    "verify_archive_policy_authority",
]
