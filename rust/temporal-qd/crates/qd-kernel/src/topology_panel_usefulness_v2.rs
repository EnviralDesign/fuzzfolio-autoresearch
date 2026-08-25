//! Exact production eligibility projection for topology-study arms.

use serde_json::{Map, Value, json};
use temporal_qd_contract::{canonical_sha256, canonical_sha256_without_object_field};

pub const ARM_PROJECTION_SCHEMA: &str = "temporal_qd_topology_arm_eligibility_projection_v2";
pub const SUPPORT_SCHEMA: &str = "temporal_qd_topology_support_eligibility_v2";
pub const QUALITY_SCHEMA: &str = "temporal_qd_topology_quality_lane_eligibility_v2";
pub const DIRECTION_SCHEMA: &str = "temporal_qd_topology_direction_eligibility_v2";
pub const PANEL_SCHEMA: &str = "temporal_qd_topology_panel_usefulness_v2";
pub const REPLICATION_SCHEMA: &str = "temporal_qd_topology_replication_survival_projection_v3";
pub const ARCHIVE_POLICY_SHA256: &str =
    "sha256:c8ea30b0a9d2825844d4267be9e4ccf82f36dc43a741ac061d41508fe486c3da";
pub const ARCHIVE_AUTHORITY_SHA256: &str =
    "sha256:48c4f69bbe6fff7fa2b46b7783e95bd32f73d768047d87ce1dcf809445fef182";
pub const DIRECTION_POLICY_SHA256: &str =
    "sha256:2567175ff6ae6063baa485484c0faa0d742507af6814a593076020a68aef3ed1";
const MINIMUM_TOTAL_TRADES: u64 = 8;
const MINIMUM_TRADES_PER_WINDOW: u64 = 4;

fn invalid(message: impl Into<String>) -> String {
    message.into()
}

fn map<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>, String> {
    value
        .as_object()
        .ok_or_else(|| invalid(format!("{label} must be an object")))
}

fn field<'a>(source: &'a Map<String, Value>, key: &str, label: &str) -> Result<&'a Value, String> {
    source
        .get(key)
        .ok_or_else(|| invalid(format!("{label} lacks {key}")))
}

fn boolean(source: &Map<String, Value>, key: &str, label: &str) -> Result<bool, String> {
    field(source, key, label)?
        .as_bool()
        .ok_or_else(|| invalid(format!("{label} {key} must be Boolean")))
}

fn count(source: &Map<String, Value>, key: &str, label: &str) -> Result<u64, String> {
    field(source, key, label)?
        .as_u64()
        .ok_or_else(|| invalid(format!("{label} {key} must be a nonnegative integer")))
}

fn finite(source: &Map<String, Value>, key: &str, label: &str) -> Result<f64, String> {
    field(source, key, label)?
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| invalid(format!("{label} {key} must be finite")))
}

fn verify_archive_authority(authority: &Value) -> Result<(), String> {
    if canonical_sha256(authority).map_err(|error| invalid(error.to_string()))?
        != ARCHIVE_AUTHORITY_SHA256
    {
        return Err(invalid("archive policy authority is not exact"));
    }
    let authority = map(authority, "archive policy authority")?;
    if authority.get("policyName").and_then(Value::as_str)
        != Some("stage5e7_v5_direction_aware_breeding_archive")
        || authority.get("policySha256").and_then(Value::as_str) != Some(ARCHIVE_POLICY_SHA256)
    {
        return Err(invalid("archive policy identity drifted"));
    }
    let policy = map(
        field(authority, "frozenPolicy", "archive policy authority")?,
        "frozen archive policy",
    )?;
    let support = map(
        field(policy, "tradeSupport", "frozen archive policy")?,
        "archive support policy",
    )?;
    let direction = map(
        field(policy, "directionSelection", "frozen archive policy")?,
        "archive direction policy",
    )?;
    let lanes = map(
        field(
            map(
                field(policy, "archive", "frozen archive policy")?,
                "archive lanes parent",
            )?,
            "lanes",
            "archive lanes parent",
        )?,
        "archive lanes",
    )?;
    if count(support, "minimumTotalTrades", "archive support policy")? != MINIMUM_TOTAL_TRADES
        || count(support, "minimumTradesPerWindow", "archive support policy")?
            != MINIMUM_TRADES_PER_WINDOW
        || direction
            .get("selectionPolicySha256")
            .and_then(Value::as_str)
            != Some(DIRECTION_POLICY_SHA256)
        || lanes.get("quality").and_then(Value::as_str)
            != Some("finite_support_and_nonnegative_robust_return")
    {
        return Err(invalid(
            "archive support/quality/direction material drifted",
        ));
    }
    Ok(())
}

fn support_projection(member: &Map<String, Value>) -> Result<Value, String> {
    let validity = map(
        field(member, "finiteDataValidity", "member")?,
        "finite data validity",
    )?;
    let aggregate = map(field(member, "aggregate", "member")?, "member aggregate")?;
    let checks = map(
        field(validity, "checks", "finite data validity")?,
        "support checks",
    )?;
    let counts = field(validity, "tradeCountsByWindow", "finite data validity")?
        .as_array()
        .ok_or_else(|| invalid("support window counts must be an array"))?;
    if counts.len() != 4 {
        return Err(invalid("support requires four window counts"));
    }
    let counts = counts
        .iter()
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| invalid("support window count must be nonnegative"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let total = count(validity, "totalTrades", "finite data validity")?;
    if count(validity, "minimumTotalTrades", "finite data validity")? != MINIMUM_TOTAL_TRADES
        || count(validity, "minimumTradesPerWindow", "finite data validity")?
            != MINIMUM_TRADES_PER_WINDOW
        || counts.iter().sum::<u64>() != total
        || count(aggregate, "totalTrades", "member aggregate")? != total
        || aggregate.get("tradeCountsByWindow")
            != Some(&Value::Array(
                counts.iter().map(|value| json!(value)).collect(),
            ))
    {
        return Err(invalid("member support threshold/count binding drifted"));
    }
    let finite_data = boolean(validity, "isFiniteData", "finite data validity")?;
    let passes = boolean(validity, "passesSupportGate", "finite data validity")?;
    let valid_quality = boolean(validity, "validForQuality", "finite data validity")?;
    let total_ok = total >= MINIMUM_TOTAL_TRADES;
    let windows_ok = counts
        .iter()
        .all(|value| *value >= MINIMUM_TRADES_PER_WINDOW);
    let observations = count(aggregate, "totalObservations", "member aggregate")?;
    let positive = observations > 0;
    let expected_finite = finite(aggregate, "worstWindowConservativeNetR", "member aggregate")
        .is_ok()
        && finite(aggregate, "maxWindowDrawdownR", "member aggregate").is_ok();
    let expected_passes = total_ok && windows_ok && positive;
    if finite_data != expected_finite
        || boolean(checks, "finiteEconomicMetrics", "support checks")? != finite_data
        || boolean(checks, "minimumTotalTrades", "support checks")? != total_ok
        || boolean(checks, "minimumTradesEveryWindow", "support checks")? != windows_ok
        || boolean(checks, "positiveObservationSupport", "support checks")? != positive
        || passes != expected_passes
        || valid_quality != (finite_data && passes)
    {
        return Err(invalid("member support Boolean derivation drifted"));
    }
    let eligible = finite_data && passes && valid_quality;
    let reasons = if eligible {
        vec![Value::String("eligible".into())]
    } else {
        let mut values = Vec::new();
        if !finite_data {
            values.push(Value::String("nonfinite_data".into()));
        }
        if !total_ok {
            values.push(Value::String("minimum_total_trades_failed".into()));
        }
        if !windows_ok {
            values.push(Value::String("minimum_trades_per_window_failed".into()));
        }
        if !positive {
            values.push(Value::String("positive_observation_support_failed".into()));
        }
        if !valid_quality {
            values.push(Value::String(
                "finite_support_quality_validity_failed".into(),
            ));
        }
        values
    };
    Ok(json!({
        "schemaVersion": SUPPORT_SCHEMA,
        "eligible": eligible,
        "reasonCodes": reasons,
        "minimumTotalTrades": MINIMUM_TOTAL_TRADES,
        "minimumTradesPerWindow": MINIMUM_TRADES_PER_WINDOW,
        "archivePolicySha256": ARCHIVE_POLICY_SHA256,
    }))
}

fn quality_projection(member: &Map<String, Value>, support: &Value) -> Result<Value, String> {
    let support = map(support, "support projection")?;
    let support_eligible = boolean(support, "eligible", "support projection")?;
    let objectives = map(field(member, "objectives", "member")?, "member objectives")?;
    let aggregate = map(field(member, "aggregate", "member")?, "member aggregate")?;
    let worst = finite(
        objectives,
        "worstWindowConservativeNetR",
        "member objectives",
    )?;
    let aggregate_worst = finite(aggregate, "worstWindowConservativeNetR", "member aggregate")?;
    if worst != aggregate_worst {
        return Err(invalid("quality worst-window binding drifted"));
    }
    let eligible = support_eligible && worst >= 0.0;
    let reason = if eligible {
        "eligible"
    } else if !support_eligible {
        "support_ineligible"
    } else {
        "negative_worst_window_robust_return"
    };
    Ok(json!({
        "schemaVersion": QUALITY_SCHEMA,
        "eligible": eligible,
        "reasonCode": reason,
        "qualityLane": "finite_support_and_nonnegative_robust_return",
        "worstWindowConservativeNetR": worst,
        "archivePolicySha256": ARCHIVE_POLICY_SHA256,
    }))
}

fn direction_side(
    behavior: &Map<String, Value>,
    side: &str,
    window_count: u64,
) -> Result<(bool, bool, bool), String> {
    let sides = map(
        field(behavior, "sides", "realized behavior")?,
        "realized sides",
    )?;
    let row = map(field(sides, side, "realized sides")?, "realized side")?;
    let closed = count(row, "closedTrades", "realized side")?;
    let active_windows = count(row, "activeWindowCount", "realized side")?;
    let fraction = finite(row, "activeWindowFraction", "realized side")?;
    if active_windows > window_count
        || !(0.0..=1.0).contains(&fraction)
        || (fraction - active_windows as f64 / window_count as f64).abs() > 1e-12
    {
        return Err(invalid("direction active-window evidence is inconsistent"));
    }
    let gross = finite(row, "grossR", "realized side")?;
    let net = finite(row, "netR", "realized side")?;
    let cost = finite(row, "costR", "realized side")?;
    if ((gross - net) - cost).abs() > 1e-9 {
        return Err(invalid("direction gross/net/cost R does not reconcile"));
    }
    let terminal = count(row, "terminalDirectionCount", "realized side")?;
    if boolean(row, "active", "realized side")? != (closed > 0 || terminal > 0) {
        return Err(invalid("direction active flag is inconsistent"));
    }
    let supported = closed >= 1 && active_windows >= 1;
    Ok((
        supported,
        supported && net >= 0.0,
        supported && net <= -0.25,
    ))
}

fn direction_projection(member: &Map<String, Value>) -> Result<Value, String> {
    let aggregate = map(field(member, "aggregate", "member")?, "member aggregate")?;
    let behavior_value = field(aggregate, "realizedBehavior", "member aggregate")?;
    let behavior = map(behavior_value, "realized behavior")?;
    if behavior.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_realized_behavior_v1")
    {
        return Err(invalid("realized behavior schema is unsupported"));
    }
    let identity = field(behavior, "identityMaterial", "realized behavior")?;
    let identity_sha = behavior
        .get("identitySha256")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("realized behavior identity is missing"))?;
    if canonical_sha256(identity).map_err(|error| invalid(error.to_string()))? != identity_sha {
        return Err(invalid("realized behavior identity mismatch"));
    }
    let bound_sides = map(
        field(
            map(identity, "realized identity")?,
            "sides",
            "realized identity",
        )?,
        "bound sides",
    )?;
    let sides = map(
        field(behavior, "sides", "realized behavior")?,
        "realized sides",
    )?;
    const IDENTITY_FIELDS: [&str; 18] = [
        "closedTrades",
        "wins",
        "losses",
        "flatTrades",
        "grossR",
        "netR",
        "costR",
        "holdingBars",
        "holdingHours",
        "active",
        "activeWindowCount",
        "exposureProxy",
        "terminalDirectionCount",
        "conflictAbstentions",
        "closeReasonDistribution",
        "actionDistribution",
        "transitionDistribution",
        "terminalStatusCounts",
    ];
    for side in ["long", "short"] {
        let bound = map(field(bound_sides, side, "bound sides")?, "bound side")?;
        let observed = map(field(sides, side, "realized sides")?, "realized side")?;
        if IDENTITY_FIELDS
            .iter()
            .any(|key| bound.get(*key) != observed.get(*key))
        {
            return Err(invalid("realized behavior side identity drifted"));
        }
    }
    let window_count = count(behavior, "windowCount", "realized behavior")?;
    if window_count == 0 {
        return Err(invalid("direction behavior windowCount must be positive"));
    }
    let (long_supported, long_acceptable, long_harmful) =
        direction_side(behavior, "long", window_count)?;
    let (short_supported, short_acceptable, short_harmful) =
        direction_side(behavior, "short", window_count)?;
    let (lane, eligible, specialist): (&str, bool, Option<&str>) =
        if (long_acceptable && short_harmful) || (short_acceptable && long_harmful) {
            ("harmful_opposite_side", false, None)
        } else if long_acceptable && short_acceptable {
            ("balanced_bidirectional", true, None)
        } else if long_acceptable && !short_supported {
            ("long_specialist", true, Some("long"))
        } else if short_acceptable && !long_supported {
            ("short_specialist", true, Some("short"))
        } else {
            ("inactive_or_unsupported", false, None)
        };
    Ok(json!({
        "schemaVersion": DIRECTION_SCHEMA,
        "eligible": eligible,
        "reasonCode": if eligible { "eligible" } else { lane },
        "selectionPolicySha256": DIRECTION_POLICY_SHA256,
        "realizedBehaviorIdentitySha256": identity_sha,
        "lane": lane,
        "specialistSide": specialist,
        "sides": {
            "long": {
                "supported": long_supported,
                "acceptable": long_acceptable,
                "materiallyHarmful": long_harmful,
            },
            "short": {
                "supported": short_supported,
                "acceptable": short_acceptable,
                "materiallyHarmful": short_harmful,
            },
        },
    }))
}

pub fn arm_eligibility_projection(
    member: &Value,
    archive_policy_authority: &Value,
) -> Result<Value, String> {
    verify_archive_authority(archive_policy_authority)?;
    let member = map(member, "member")?;
    let support = support_projection(member)?;
    let quality = quality_projection(member, &support)?;
    let direction = direction_projection(member)?;
    let mut result = json!({
        "schemaVersion": ARM_PROJECTION_SCHEMA,
        "supportEligibility": support,
        "qualityLaneEligibility": quality,
        "directionSelection": direction,
    });
    let sha = canonical_sha256(&result).map_err(|error| invalid(error.to_string()))?;
    result
        .as_object_mut()
        .expect("arm projection is an object")
        .insert("eligibilityProjectionSha256".into(), Value::String(sha));
    Ok(result)
}

pub fn verify_arm_eligibility_projection(value: &Value) -> Result<(), String> {
    let source = map(value, "arm eligibility projection")?;
    if source.get("schemaVersion").and_then(Value::as_str) != Some(ARM_PROJECTION_SCHEMA) {
        return Err(invalid("arm eligibility projection schema drifted"));
    }
    let stored = source
        .get("eligibilityProjectionSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("arm eligibility projection identity is missing"))?;
    let actual = canonical_sha256_without_object_field(value, "eligibilityProjectionSha256")
        .map_err(|error| invalid(error.to_string()))?;
    if stored != actual {
        return Err(invalid("arm eligibility projection identity drifted"));
    }
    Ok(())
}

fn eligibility<'a>(
    arm: &'a Map<String, Value>,
    field_name: &str,
    schema: &str,
) -> Result<&'a Value, String> {
    let value = field(arm, field_name, "panel arm")?;
    let value_map = map(value, field_name)?;
    if value_map.get("schemaVersion").and_then(Value::as_str) != Some(schema) {
        return Err(invalid(format!("{field_name} schema drifted")));
    }
    boolean(value_map, "eligible", field_name)?;
    Ok(value)
}

fn metric(arms: &Map<String, Value>, arm: &str, field_name: &str) -> Result<f64, String> {
    finite(
        map(field(arms, arm, "panel arms")?, "panel arm")?,
        field_name,
        "panel arm",
    )
}

fn greater(left: f64, right: f64) -> bool {
    left - right > 1e-12
}

fn not_worse(left: f64, right: f64) -> bool {
    left - right >= -1e-12
}

/// Exact shared U_v2 projection. P/T/E eligibility is diagnostic only; TE is
/// the sole arm whose frozen production gates can veto useful innovation.
pub fn evaluate_panel_usefulness_v2(arms: &Value) -> Result<Value, String> {
    let arms = map(arms, "panel arms")?;
    if arms.len() != 4
        || ["P", "T", "E", "TE"]
            .iter()
            .any(|arm| !arms.contains_key(*arm))
    {
        return Err(invalid("panel must contain exact P/T/E/TE arms"));
    }
    let mut normalized = Map::new();
    for arm_name in ["P", "T", "E", "TE"] {
        let arm = map(field(arms, arm_name, "panel arms")?, "panel arm")?;
        let support = eligibility(arm, "supportEligibility", SUPPORT_SCHEMA)?;
        let quality = eligibility(arm, "qualityLaneEligibility", QUALITY_SCHEMA)?;
        let direction = eligibility(arm, "directionSelection", DIRECTION_SCHEMA)?;
        let trade_count = count(arm, "tradeCount", "panel arm")?;
        let identity = map(field(arm, "identity", "panel arm")?, "panel arm identity")?;
        normalized.insert(
            arm_name.into(),
            json!({
                "candidateId": field(arm, "candidateId", "panel arm")?,
                "conservativeNetR": finite(arm, "conservativeNetR", "panel arm")?,
                "worstWindowConservativeNetR": finite(arm, "worstWindowConservativeNetR", "panel arm")?,
                "tradeCount": trade_count,
                "costDragR": finite(arm, "costDragR", "panel arm")?,
                "supportEligibility": support,
                "qualityLaneEligibility": quality,
                "directionSelection": direction,
                "identity": identity,
            }),
        );
    }
    let p = metric(&normalized, "P", "conservativeNetR")?;
    let t = metric(&normalized, "T", "conservativeNetR")?;
    let e = metric(&normalized, "E", "conservativeNetR")?;
    let te = metric(&normalized, "TE", "conservativeNetR")?;
    let p_worst = metric(&normalized, "P", "worstWindowConservativeNetR")?;
    let t_worst = metric(&normalized, "T", "worstWindowConservativeNetR")?;
    let e_worst = metric(&normalized, "E", "worstWindowConservativeNetR")?;
    let te_worst = metric(&normalized, "TE", "worstWindowConservativeNetR")?;
    let te_gt_p = greater(te, p);
    let te_gt_t = greater(te, t);
    let te_gt_e = greater(te, e);
    let risk_p = not_worse(te_worst, p_worst);
    let risk_t = not_worse(te_worst, t_worst);
    let risk_e = not_worse(te_worst, e_worst);
    let te_arm = map(field(&normalized, "TE", "normalized arms")?, "TE arm")?;
    let te_support = boolean(
        map(field(te_arm, "supportEligibility", "TE arm")?, "TE support")?,
        "eligible",
        "TE support",
    )?;
    let te_quality = boolean(
        map(
            field(te_arm, "qualityLaneEligibility", "TE arm")?,
            "TE quality",
        )?,
        "eligible",
        "TE quality",
    )?;
    let te_direction = boolean(
        map(
            field(te_arm, "directionSelection", "TE arm")?,
            "TE direction",
        )?,
        "eligible",
        "TE direction",
    )?;
    let mut diagnostics = Map::new();
    for arm_name in ["P", "T", "E", "TE"] {
        let arm = map(
            field(&normalized, arm_name, "normalized arms")?,
            "panel arm",
        )?;
        let eligible = |name: &str| -> Result<bool, String> {
            boolean(map(field(arm, name, "panel arm")?, name)?, "eligible", name)
        };
        diagnostics.insert(
            arm_name.into(),
            json!({
                "support": eligible("supportEligibility")?,
                "quality": eligible("qualityLaneEligibility")?,
                "direction": eligible("directionSelection")?,
            }),
        );
    }
    let comparisons = te_gt_p && te_gt_t && te_gt_e;
    let risk = risk_p && risk_t && risk_e;
    let useful = comparisons && risk && te_support && te_quality && te_direction;
    let mut result = json!({
        "schemaVersion": PANEL_SCHEMA,
        "comparisonEvidenceComplete": true,
        "arms": normalized,
        "teMinusP": te - p,
        "teMinusT": te - t,
        "teMinusE": te - e,
        "signedInteraction": te - t - e + p,
        "combinedOutperformsParentAndSingles": comparisons,
        "riskNonWorseThanParentAndSingles": risk,
        "teSupportEligible": te_support,
        "teQualityEligible": te_quality,
        "teDirectionEligible": te_direction,
        "allArmEligibilityDiagnostic": diagnostics,
        "nonqualifyingRiskTradeoff": comparisons && risk_p && !(risk_t && risk_e),
        "usefulProgressiveInnovationV2": useful,
    });
    let sha = canonical_sha256(&result).map_err(|error| invalid(error.to_string()))?;
    result
        .as_object_mut()
        .expect("panel projection object")
        .insert("panelUsefulnessSha256".into(), Value::String(sha));
    Ok(result)
}

/// Versioned result authority naming the successor panel-local predicate while
/// preserving the original strict all-three-panel Boolean operator.
pub fn evaluate_replication_survival_v3(
    development_panel_3: Option<bool>,
    replication_panel_1: Option<bool>,
    replication_panel_2: Option<bool>,
    identities_valid: bool,
) -> Value {
    let complete = identities_valid
        && development_panel_3.is_some()
        && replication_panel_1.is_some()
        && replication_panel_2.is_some();
    let development = complete && development_panel_3 == Some(true);
    let replication =
        complete && replication_panel_1 == Some(true) && replication_panel_2 == Some(true);
    let promising = development && replication;
    let category = if !complete {
        "incomplete_invalid"
    } else if promising {
        "inspected_promising_pending_untouched_confirmation"
    } else if development
        && replication_panel_1 == Some(false)
        && replication_panel_2 == Some(false)
    {
        "development_only_not_replicated"
    } else if development {
        "mixed_panel_nonqualifying"
    } else if replication_panel_1 == Some(true) || replication_panel_2 == Some(true) {
        "replication_only_discordant_not_promising"
    } else {
        "complete_no_useful_panel"
    };
    let mut result = json!({
        "schemaVersion": REPLICATION_SCHEMA,
        "panelLocalPredicate": "U_v2",
        "panelUsefulProgressiveInnovationV2": {
            "panel-3": development_panel_3,
            "panel-1": replication_panel_1,
            "panel-2": replication_panel_2,
        },
        "evidenceCompleteAndIdentityValid": complete,
        "developmentQualified": development,
        "replicationSurviving": replication,
        "inspectedPromising": promising,
        "reportingCategory": category,
        "confirmationStatus": "pending",
        "confirmationPredicate": "U_v2_same_exact_block",
    });
    let sha = canonical_sha256(&result).expect("replication projection is canonical");
    result
        .as_object_mut()
        .expect("replication projection object")
        .insert("projectionSha256".into(), Value::String(sha));
    result
}
