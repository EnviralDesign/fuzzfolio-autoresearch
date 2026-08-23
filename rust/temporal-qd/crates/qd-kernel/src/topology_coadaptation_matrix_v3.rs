//! Experiment-only topology co-adaptation overlay v3.
//!
//! Production rotating 4/5 breeding omits `topologyCoadaptationMatrix`.
//! Parsing this contract never schedules market work.  Topology plans are
//! parent-bound `TopologyPlanV1` records.  `insert_exit_region` cannot enter
//! the first event-settling contrast.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;
use temporal_qd_contract::{Map, Value, canonical_sha256};

pub const COADAPTATION_SCHEMA: &str = "temporal_qd_topology_coadaptation_matrix_v3";
pub const COADAPTATION_MODE: &str = "frozen_parent_insert_setup_then_topology_local_event_v3";
pub const CLONE_CONTROL: &str = "re_evaluate_parent_on_frozen_panel";
pub const ARMS: [&str; 4] = [
    "exact_parent_clone",
    "topology_only_child",
    "event_only_control",
    "topology_then_topology_local_event",
];
const FIRST_EXPERIMENT_OPERATION: &str = "insert_setup";
const FORBIDDEN_FIRST_EXPERIMENT_OPERATIONS: [&str; 1] = ["insert_exit_region"];
const PARENT_ROLES: [&str; 3] = ["archive", "inactive_control", "active_negative_control"];
const ROOT_KEYS: [&str; 21] = [
    "schemaVersion",
    "mode",
    "includeCrossover",
    "cloneControl",
    "productionArchiveWrite",
    "mutationDepth",
    "morphologyNurseryDeferred",
    "lexicographicFirstSettlingPlanForbidden",
    "parents",
    "panelIdentities",
    "firstExperimentOperation",
    "forbiddenFirstExperimentOperations",
    "topologyLocalEventRequired",
    "arms",
    "topologyPlans",
    "eventPrimitives",
    "slots",
    "settling",
    "successCalculation",
    "notAdmittedOnFrontGenerationPath",
    "contractSha256",
];
const PARENT_KEYS: [&str; 4] = [
    "candidateId",
    "role",
    "longProgramSha256",
    "shortProgramSha256",
];
const PANEL_KEYS: [&str; 6] = [
    "developmentPanelId",
    "developmentRole",
    "replicationPanelIds",
    "replicationRole",
    "futureConfirmationPanel",
    "rotatingEvidenceSha256",
];
const FUTURE_PANEL_KEYS: [&str; 4] = [
    "createdInThisTask",
    "requiredBeforeProductionConclusion",
    "authorityMustBeBoundBeforeLaunch",
    "label",
];
const TOPOLOGY_PLAN_KEYS: [&str; 5] = [
    "schemaVersion",
    "operatorSchema",
    "operation",
    "sourceGenomeSha256",
    "arguments",
];
const TOPOLOGY_RECORD_KEYS: [&str; 8] = [
    "planId",
    "parentCandidateId",
    "side",
    "topologyPlan",
    "planSha256",
    "addedSetupNodeId",
    "applicability",
    "topologySemanticDeltaIdentity",
];
const EVENT_PRIMITIVE_KEYS: [&str; 8] = [
    "primitiveId",
    "parentCandidateId",
    "side",
    "indicatorId",
    "contract",
    "originalNodeId",
    "originalNodeZone",
    "source",
];
const SLOT_KEYS: [&str; 9] = [
    "slotId",
    "arm",
    "parentCandidateId",
    "side",
    "eligibility",
    "topologyPlanId",
    "eventPrimitiveId",
    "settlingNodeId",
    "ineligibilityReason",
];
const SETTLING_KEYS: [&str; 5] = [
    "kind",
    "mustTargetAddedSetupNodeId",
    "selection",
    "matchedControlSite",
    "ineligibleCellsRemainExplicit",
];
const SUCCESS_KEYS: [&str; 13] = [
    "schemaVersion",
    "metricEquality",
    "parentBeat",
    "riskQualifiedBeat",
    "fullEconomicPhenotypeTie",
    "supportDirectionQualityGates",
    "activityCostMechanismRequired",
    "parentBalancingRequired",
    "eventPlanBalancingRequired",
    "requireReplicationPanelSurvivalForPromisingClaim",
    "requireUntouchedConfirmationPanelBeforeProductionConclusion",
    "doNotPromoteOnDevelopmentPanelAlone",
    "noveltyIsNotQuality",
];

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum CoadaptationV3Error {
    #[error("{0}")]
    Contract(String),
}

pub type Result<T> = std::result::Result<T, CoadaptationV3Error>;

fn contract(message: impl Into<String>) -> CoadaptationV3Error {
    CoadaptationV3Error::Contract(message.into())
}

fn unexpected(label: &str) -> CoadaptationV3Error {
    contract(format!("{label} has an unexpected schema"))
}

fn exact_keys(object: &Map<String, Value>, required: &[&str], label: &str) -> Result<()> {
    let known: BTreeSet<&str> = required.iter().copied().collect();
    if !required.iter().all(|key| object.contains_key(*key))
        || object.keys().any(|key| !known.contains(key.as_str()))
    {
        return Err(unexpected(label));
    }
    Ok(())
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value.as_object().ok_or_else(|| unexpected(label))
}

fn text<'a>(fields: &'a Map<String, Value>, key: &str, label: &str) -> Result<&'a str> {
    fields
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| contract(format!("{label} drifted")))
}

fn require_text(fields: &Map<String, Value>, key: &str, expected: &str, label: &str) -> Result<()> {
    if text(fields, key, label)? != expected {
        return Err(contract(format!("{label} drifted")));
    }
    Ok(())
}

fn require_bool(fields: &Map<String, Value>, key: &str, expected: bool, label: &str) -> Result<()> {
    if fields.get(key) != Some(&Value::Bool(expected)) {
        return Err(contract(format!("{label} drifted")));
    }
    Ok(())
}

fn require_u64(fields: &Map<String, Value>, key: &str, expected: u64, label: &str) -> Result<()> {
    let got = fields
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| contract(format!("{label} drifted")))?;
    if got != expected {
        return Err(contract(format!("{label} drifted")));
    }
    Ok(())
}

fn require_sha(value: &Value, label: &str) -> Result<String> {
    let text = value
        .as_str()
        .ok_or_else(|| contract(format!("{label} drifted")))?;
    if text.len() != 71
        || !text.starts_with("sha256:")
        || !text.as_bytes()[7..]
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(contract(format!("{label} drifted")));
    }
    Ok(text.to_owned())
}

fn require_str_list(value: &Value, expected: &[&str], label: &str) -> Result<()> {
    let got = value
        .as_array()
        .ok_or_else(|| contract(format!("{label} drifted")))?;
    let items: Vec<&str> = got.iter().filter_map(Value::as_str).collect();
    if items != expected {
        return Err(contract(format!("{label} drifted")));
    }
    Ok(())
}

pub fn from_generation_config(config: &Value) -> Result<Option<Value>> {
    let Some(fields) = config.as_object() else {
        return Ok(None);
    };
    match fields.get("topologyCoadaptationMatrix") {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            validate(value)?;
            Ok(Some(value.clone()))
        }
    }
}

fn canonical_topology_plan(plan: &Value) -> Result<Value> {
    let fields = object(plan, "topology plan")?;
    if fields.contains_key("v38ExampleOperatorPlanSha256") {
        return Err(contract(
            "generic example topology plan SHAs cannot satisfy launch-grade slots",
        ));
    }
    exact_keys(fields, &TOPOLOGY_PLAN_KEYS, "topology plan")?;
    require_text(
        fields,
        "schemaVersion",
        "evolvable_module_topology_plan_v1",
        "topology plan schema",
    )?;
    require_text(
        fields,
        "operatorSchema",
        "evolvable_module_topology_operator_v1",
        "topology plan operator schema",
    )?;
    let operation = text(fields, "operation", "topology plan operation")?;
    if operation == "insert_exit_region" {
        return Err(contract(
            "insert_exit_region cannot enter the event-settling first contrast",
        ));
    }
    if operation != FIRST_EXPERIMENT_OPERATION {
        return Err(contract("topology plan operation drifted"));
    }
    require_sha(
        fields
            .get("sourceGenomeSha256")
            .ok_or_else(|| unexpected("topology plan"))?,
        "topology plan sourceGenomeSha256",
    )?;
    if !fields
        .get("arguments")
        .map(Value::is_object)
        .unwrap_or(false)
    {
        return Err(unexpected("topology plan arguments"));
    }
    Ok(plan.clone())
}

fn topology_plan_sha(plan: &Value) -> Result<String> {
    let canonical = canonical_topology_plan(plan)?;
    canonical_sha256(&canonical).map_err(|error| contract(error.to_string()))
}

fn added_setup_node_id(plan: &Value) -> Result<String> {
    let sha = topology_plan_sha(plan)?;
    Ok(format!("setup_{}", &sha[7..23]))
}

fn validate_parents(value: &Value) -> Result<BTreeMap<String, Map<String, Value>>> {
    let rows = value
        .as_array()
        .ok_or_else(|| contract("topology coadaptation v3 requires frozen parents"))?;
    if rows.is_empty() {
        return Err(contract("topology coadaptation v3 requires frozen parents"));
    }
    let mut parents = BTreeMap::new();
    let mut has_archive = false;
    for item in rows {
        let fields = object(item, "topology coadaptation v3 parent")?;
        exact_keys(fields, &PARENT_KEYS, "topology coadaptation v3 parent")?;
        let candidate_id = text(fields, "candidateId", "topology coadaptation v3 parent")?.to_owned();
        if candidate_id.trim().is_empty() || parents.contains_key(&candidate_id) {
            return Err(contract(format!(
                "topology coadaptation v3 repeats parent {candidate_id}"
            )));
        }
        let role = text(fields, "role", "topology coadaptation v3 parent role")?;
        if !PARENT_ROLES.contains(&role) {
            return Err(contract("topology coadaptation v3 parent role is invalid"));
        }
        if role == "archive" {
            has_archive = true;
        }
        require_sha(
            fields
                .get("longProgramSha256")
                .ok_or_else(|| unexpected("topology coadaptation v3 parent"))?,
            "parent longProgramSha256",
        )?;
        require_sha(
            fields
                .get("shortProgramSha256")
                .ok_or_else(|| unexpected("topology coadaptation v3 parent"))?,
            "parent shortProgramSha256",
        )?;
        parents.insert(candidate_id, fields.clone());
    }
    if !has_archive {
        return Err(contract(
            "topology coadaptation v3 requires at least one archive parent",
        ));
    }
    Ok(parents)
}

fn validate_panels(value: &Value) -> Result<()> {
    let fields = object(value, "topology coadaptation v3 panelIdentities")?;
    exact_keys(fields, &PANEL_KEYS, "topology coadaptation v3 panelIdentities")?;
    require_text(fields, "developmentPanelId", "panel-3", "developmentPanelId")?;
    require_text(
        fields,
        "developmentRole",
        "discovery_and_selection",
        "developmentRole",
    )?;
    require_str_list(
        fields
            .get("replicationPanelIds")
            .ok_or_else(|| unexpected("topology coadaptation v3 panelIdentities"))?,
        &["panel-1", "panel-2"],
        "replicationPanelIds",
    )?;
    require_text(
        fields,
        "replicationRole",
        "inspected_replication_not_untouched_confirmation",
        "replicationRole",
    )?;
    let future = object(
        fields
            .get("futureConfirmationPanel")
            .ok_or_else(|| unexpected("futureConfirmationPanel"))?,
        "futureConfirmationPanel",
    )?;
    exact_keys(future, &FUTURE_PANEL_KEYS, "futureConfirmationPanel")?;
    require_bool(future, "createdInThisTask", false, "futureConfirmationPanel.createdInThisTask")?;
    require_bool(
        future,
        "requiredBeforeProductionConclusion",
        true,
        "futureConfirmationPanel.requiredBeforeProductionConclusion",
    )?;
    require_bool(
        future,
        "authorityMustBeBoundBeforeLaunch",
        true,
        "futureConfirmationPanel.authorityMustBeBoundBeforeLaunch",
    )?;
    require_text(
        future,
        "label",
        "future_untouched_confirmation_panel",
        "futureConfirmationPanel.label",
    )?;
    require_sha(
        fields
            .get("rotatingEvidenceSha256")
            .ok_or_else(|| unexpected("rotatingEvidenceSha256"))?,
        "rotatingEvidenceSha256",
    )?;
    Ok(())
}

fn validate_topology_records(
    value: &Value,
    parents: &BTreeMap<String, Map<String, Value>>,
) -> Result<BTreeMap<String, Map<String, Value>>> {
    let rows = value
        .as_array()
        .ok_or_else(|| unexpected("topologyPlans"))?;
    let mut records = BTreeMap::new();
    let mut seen_shas = BTreeSet::new();
    for item in rows {
        let fields = object(item, "topology plan record")?;
        exact_keys(fields, &TOPOLOGY_RECORD_KEYS, "topology plan record")?;
        let plan_id = text(fields, "planId", "topology plan record")?.to_owned();
        if plan_id.is_empty() || records.contains_key(&plan_id) {
            return Err(unexpected("topology plan record"));
        }
        let parent_id = text(fields, "parentCandidateId", "topology plan record")?.to_owned();
        let side = text(fields, "side", "topology plan record")?;
        if !parents.contains_key(&parent_id) || (side != "long" && side != "short") {
            return Err(contract("topology plan record parent/side drifted"));
        }
        let plan = fields
            .get("topologyPlan")
            .ok_or_else(|| unexpected("topology plan record"))?;
        let expected_sha = topology_plan_sha(plan)?;
        if text(fields, "planSha256", "topology plan identity")? != expected_sha {
            return Err(contract("topology plan identity drift"));
        }
        if !seen_shas.insert(expected_sha.clone()) {
            return Err(contract("topology plan SHA reused across records"));
        }
        let added = added_setup_node_id(plan)?;
        if text(fields, "addedSetupNodeId", "added setup node")? != added {
            return Err(contract("added setup node identity drift"));
        }
        let source_key = if side == "long" {
            "longProgramSha256"
        } else {
            "shortProgramSha256"
        };
        let expected_source = parents[&parent_id][source_key]
            .as_str()
            .ok_or_else(|| contract("stale topology plan does not bind this exact parent genome"))?;
        let actual_source = object(plan, "topology plan")?
            .get("sourceGenomeSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| contract("stale topology plan does not bind this exact parent genome"))?;
        if actual_source != expected_source {
            return Err(contract(
                "stale topology plan does not bind this exact parent genome",
            ));
        }
        require_text(
            fields,
            "applicability",
            "source_genome_matches_parent_side_program",
            "topology plan applicability",
        )?;
        let delta = json!({
            "operation": FIRST_EXPERIMENT_OPERATION,
            "planSha256": expected_sha,
            "sourceGenomeSha256": actual_source,
            "addedSetupNodeId": added,
        });
        let expected_delta =
            canonical_sha256(&delta).map_err(|error| contract(error.to_string()))?;
        if text(
            fields,
            "topologySemanticDeltaIdentity",
            "topology semantic delta identity",
        )? != expected_delta
        {
            return Err(contract("topology semantic delta identity drift"));
        }
        records.insert(plan_id, fields.clone());
    }
    Ok(records)
}

fn validate_event_primitives(
    value: &Value,
    parents: &BTreeMap<String, Map<String, Value>>,
) -> Result<BTreeMap<String, Map<String, Value>>> {
    let rows = value
        .as_array()
        .ok_or_else(|| unexpected("eventPrimitives"))?;
    let mut records = BTreeMap::new();
    for item in rows {
        let fields = object(item, "event primitive")?;
        exact_keys(fields, &EVENT_PRIMITIVE_KEYS, "event primitive")?;
        let primitive_id = text(fields, "primitiveId", "event primitive")?.to_owned();
        if primitive_id.is_empty() || records.contains_key(&primitive_id) {
            return Err(unexpected("event primitive"));
        }
        let parent_id = text(fields, "parentCandidateId", "event primitive")?;
        let side = text(fields, "side", "event primitive")?;
        if !parents.contains_key(parent_id) || (side != "long" && side != "short") {
            return Err(contract("event primitive parent/side drifted"));
        }
        if text(fields, "indicatorId", "event primitive")?.is_empty() {
            return Err(unexpected("event primitive"));
        }
        if !fields.get("contract").map(Value::is_object).unwrap_or(false) {
            return Err(unexpected("event primitive contract"));
        }
        if text(fields, "originalNodeId", "event primitive")?.is_empty() {
            return Err(unexpected("event primitive"));
        }
        let zone = text(fields, "originalNodeZone", "event primitive originalNodeZone")?;
        if zone != "setup" && zone != "entry" {
            return Err(contract("event primitive originalNodeZone drifted"));
        }
        require_text(
            fields,
            "source",
            "v38_recovered_directional_event_insert",
            "event primitive source",
        )?;
        records.insert(primitive_id, fields.clone());
    }
    Ok(records)
}

fn optional_text<'a>(value: Option<&'a Value>) -> Option<&'a str> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.as_str()),
        Some(_) => Some("__invalid__"),
    }
}

fn validate_slots(
    value: &Value,
    parents: &BTreeMap<String, Map<String, Value>>,
    topology_plans: &BTreeMap<String, Map<String, Value>>,
    event_primitives: &BTreeMap<String, Map<String, Value>>,
) -> Result<()> {
    let rows = value
        .as_array()
        .ok_or_else(|| contract("topology coadaptation v3 requires slots"))?;
    if rows.is_empty() {
        return Err(contract("topology coadaptation v3 requires slots"));
    }
    let mut seen = BTreeSet::new();
    for item in rows {
        let fields = object(item, "topology coadaptation v3 slot")?;
        exact_keys(fields, &SLOT_KEYS, "topology coadaptation v3 slot")?;
        let slot_id = text(fields, "slotId", "topology coadaptation v3 slot")?;
        if slot_id.is_empty() || !seen.insert(slot_id.to_owned()) {
            return Err(unexpected("topology coadaptation v3 slot"));
        }
        let arm = text(fields, "arm", "topology coadaptation v3 slot arm")?;
        if !ARMS.contains(&arm) {
            return Err(contract("topology coadaptation v3 slot arm drifted"));
        }
        let parent_id = text(fields, "parentCandidateId", "topology coadaptation v3 slot parent")?;
        if !parents.contains_key(parent_id) {
            return Err(contract("topology coadaptation v3 slot parent drifted"));
        }
        let side = optional_text(fields.get("side"));
        if let Some(side) = side {
            if side != "long" && side != "short" {
                return Err(contract("topology coadaptation v3 slot side drifted"));
            }
        }
        let eligibility = text(fields, "eligibility", "slot eligibility")?;
        if eligibility != "eligible" && eligibility != "ineligible" {
            return Err(contract("slot eligibility drifted"));
        }
        if eligibility == "eligible" && !fields.get("ineligibilityReason").map(Value::is_null).unwrap_or(false)
        {
            return Err(contract("eligible slot cannot carry an ineligibility reason"));
        }
        if eligibility == "ineligible" && !fields.get("ineligibilityReason").and_then(Value::as_str).is_some()
        {
            return Err(contract("ineligible slot requires a reason"));
        }
        match arm {
            "exact_parent_clone" => {
                if side.is_some()
                    || optional_text(fields.get("topologyPlanId")).is_some()
                    || optional_text(fields.get("eventPrimitiveId")).is_some()
                    || optional_text(fields.get("settlingNodeId")).is_some()
                {
                    return Err(contract("clone slot must not carry topology or event plans"));
                }
            }
            "topology_only_child" => {
                if optional_text(fields.get("eventPrimitiveId")).is_some()
                    || optional_text(fields.get("settlingNodeId")).is_some()
                {
                    return Err(contract("topology-only slot must not include an event"));
                }
                let plan_id = optional_text(fields.get("topologyPlanId"));
                if eligibility == "ineligible" && plan_id.is_none() {
                } else {
                    let plan = plan_id.and_then(|id| topology_plans.get(id)).ok_or_else(|| {
                        contract("topology-only slot plan parent binding drifted")
                    })?;
                    if plan["parentCandidateId"].as_str() != Some(parent_id)
                        || plan["side"].as_str() != side
                    {
                        return Err(contract("topology-only slot plan parent binding drifted"));
                    }
                }
            }
            "event_only_control" => {
                if optional_text(fields.get("topologyPlanId")).is_some() {
                    return Err(contract("event-only control must not include topology"));
                }
                let primitive_id = optional_text(fields.get("eventPrimitiveId"));
                if eligibility == "ineligible" && primitive_id.is_none() {
                } else {
                    let primitive = primitive_id
                        .and_then(|id| event_primitives.get(id))
                        .ok_or_else(|| contract("event-only control primitive binding drifted"))?;
                    if primitive["parentCandidateId"].as_str() != Some(parent_id)
                        || primitive["side"].as_str() != side
                    {
                        return Err(contract("event-only control primitive binding drifted"));
                    }
                    if eligibility == "eligible"
                        && optional_text(fields.get("settlingNodeId")).is_none()
                    {
                        return Err(contract(
                            "event-only control must name the parent setup node",
                        ));
                    }
                }
            }
            "topology_then_topology_local_event" => {
                let plan_id = optional_text(fields.get("topologyPlanId"));
                let primitive_id = optional_text(fields.get("eventPrimitiveId"));
                let plan = plan_id.and_then(|id| topology_plans.get(id));
                let primitive = primitive_id.and_then(|id| event_primitives.get(id));
                if eligibility == "ineligible" && (plan.is_none() || primitive.is_none()) {
                } else {
                    let plan = plan.ok_or_else(|| {
                        contract("topology+event slot requires both plans")
                    })?;
                    let primitive = primitive.ok_or_else(|| {
                        contract("topology+event slot requires both plans")
                    })?;
                    if plan["parentCandidateId"].as_str() != Some(parent_id)
                        || plan["side"].as_str() != side
                    {
                        return Err(contract("topology+event topology plan parent drifted"));
                    }
                    if primitive["parentCandidateId"].as_str() != Some(parent_id)
                        || primitive["side"].as_str() != side
                    {
                        return Err(contract("topology+event event primitive parent drifted"));
                    }
                    if eligibility == "eligible"
                        && optional_text(fields.get("settlingNodeId"))
                            != plan["addedSetupNodeId"].as_str()
                    {
                        return Err(contract(
                            "topology+event slot must target the newly added setup node",
                        ));
                    }
                }
            }
            _ => return Err(contract("topology coadaptation v3 slot arm drifted")),
        }
    }
    Ok(())
}

fn validate_settling(value: &Value) -> Result<()> {
    let fields = object(value, "settling")?;
    exact_keys(fields, &SETTLING_KEYS, "settling")?;
    require_text(fields, "kind", "directional_event_insert", "settling.kind")?;
    require_bool(
        fields,
        "mustTargetAddedSetupNodeId",
        true,
        "mustTargetAddedSetupNodeId",
    )?;
    require_text(
        fields,
        "selection",
        "frozen_matched_v38_event_primitive_set",
        "settling.selection",
    )?;
    require_text(
        fields,
        "matchedControlSite",
        "parent_existing_setup_if_event_free_else_ineligible",
        "matchedControlSite",
    )?;
    require_bool(
        fields,
        "ineligibleCellsRemainExplicit",
        true,
        "ineligibleCellsRemainExplicit",
    )?;
    Ok(())
}

fn validate_success(value: &Value) -> Result<()> {
    let fields = object(value, "successCalculation")?;
    exact_keys(fields, &SUCCESS_KEYS, "successCalculation")?;
    require_text(
        fields,
        "schemaVersion",
        "temporal_qd_topology_coadaptation_success_v3",
        "successCalculation schema",
    )?;
    require_text(
        fields,
        "metricEquality",
        "canonical_json_number_roundtrip_with_1e-12_encoding_floor",
        "metricEquality",
    )?;
    require_text(
        fields,
        "parentBeat",
        "child_net_strictly_greater_under_canonical_metric_identity",
        "parentBeat",
    )?;
    require_text(
        fields,
        "riskQualifiedBeat",
        "parentBeat_and_non_worse_worst_window",
        "riskQualifiedBeat",
    )?;
    require_text(
        fields,
        "fullEconomicPhenotypeTie",
        "equal_net_worst_median_active_window_fraction",
        "fullEconomicPhenotypeTie",
    )?;
    require_text(
        fields,
        "supportDirectionQualityGates",
        "unchanged_production_gates",
        "supportDirectionQualityGates",
    )?;
    require_bool(
        fields,
        "activityCostMechanismRequired",
        true,
        "activityCostMechanismRequired",
    )?;
    require_bool(fields, "parentBalancingRequired", true, "parentBalancingRequired")?;
    require_bool(
        fields,
        "eventPlanBalancingRequired",
        true,
        "eventPlanBalancingRequired",
    )?;
    require_bool(
        fields,
        "requireReplicationPanelSurvivalForPromisingClaim",
        true,
        "requireReplicationPanelSurvivalForPromisingClaim",
    )?;
    require_bool(
        fields,
        "requireUntouchedConfirmationPanelBeforeProductionConclusion",
        true,
        "requireUntouchedConfirmationPanelBeforeProductionConclusion",
    )?;
    require_bool(
        fields,
        "doNotPromoteOnDevelopmentPanelAlone",
        true,
        "doNotPromoteOnDevelopmentPanelAlone",
    )?;
    require_bool(fields, "noveltyIsNotQuality", true, "noveltyIsNotQuality")?;
    Ok(())
}

pub fn validate(value: &Value) -> Result<()> {
    let fields = object(value, "topology coadaptation v3")?;
    exact_keys(fields, &ROOT_KEYS, "topology coadaptation v3")?;
    require_text(fields, "schemaVersion", COADAPTATION_SCHEMA, "schema")?;
    require_text(fields, "mode", COADAPTATION_MODE, "mode")?;
    require_bool(fields, "includeCrossover", false, "includeCrossover")?;
    require_text(fields, "cloneControl", CLONE_CONTROL, "cloneControl")?;
    if fields.get("productionArchiveWrite") != Some(&Value::Bool(false)) {
        return Err(contract(
            "topology coadaptation must not write the production archive",
        ));
    }
    require_u64(fields, "mutationDepth", 1, "topology coadaptation v3 mutationDepth")?;
    require_bool(
        fields,
        "morphologyNurseryDeferred",
        true,
        "morphologyNurseryDeferred",
    )?;
    require_bool(
        fields,
        "lexicographicFirstSettlingPlanForbidden",
        true,
        "lexicographicFirstSettlingPlanForbidden",
    )?;
    require_text(
        fields,
        "firstExperimentOperation",
        FIRST_EXPERIMENT_OPERATION,
        "firstExperimentOperation",
    )?;
    require_str_list(
        fields
            .get("forbiddenFirstExperimentOperations")
            .ok_or_else(|| unexpected("forbiddenFirstExperimentOperations"))?,
        &FORBIDDEN_FIRST_EXPERIMENT_OPERATIONS,
        "forbiddenFirstExperimentOperations",
    )?;
    require_bool(fields, "topologyLocalEventRequired", true, "topologyLocalEventRequired")?;
    require_str_list(
        fields
            .get("arms")
            .ok_or_else(|| unexpected("arms"))?,
        &ARMS,
        "topology coadaptation v3 arms",
    )?;
    require_bool(
        fields,
        "notAdmittedOnFrontGenerationPath",
        true,
        "notAdmittedOnFrontGenerationPath",
    )?;
    let parents = validate_parents(
        fields
            .get("parents")
            .ok_or_else(|| unexpected("topology coadaptation v3"))?,
    )?;
    validate_panels(
        fields
            .get("panelIdentities")
            .ok_or_else(|| unexpected("topology coadaptation v3"))?,
    )?;
    let topology_plans = validate_topology_records(
        fields
            .get("topologyPlans")
            .ok_or_else(|| unexpected("topologyPlans"))?,
        &parents,
    )?;
    let event_primitives = validate_event_primitives(
        fields
            .get("eventPrimitives")
            .ok_or_else(|| unexpected("eventPrimitives"))?,
        &parents,
    )?;
    validate_slots(
        fields
            .get("slots")
            .ok_or_else(|| unexpected("slots"))?,
        &parents,
        &topology_plans,
        &event_primitives,
    )?;
    validate_settling(
        fields
            .get("settling")
            .ok_or_else(|| unexpected("settling"))?,
    )?;
    validate_success(
        fields
            .get("successCalculation")
            .ok_or_else(|| unexpected("successCalculation"))?,
    )?;
    let claimed = require_sha(
        fields
            .get("contractSha256")
            .ok_or_else(|| unexpected("topology coadaptation v3"))?,
        "topology coadaptation v3",
    )?;
    let mut without_hash = value.clone();
    without_hash
        .as_object_mut()
        .ok_or_else(|| unexpected("topology coadaptation v3"))?
        .remove("contractSha256");
    let expected = canonical_sha256(&without_hash).map_err(|error| contract(error.to_string()))?;
    if claimed != expected {
        return Err(contract("topology coadaptation v3 identity drift"));
    }
    Ok(())
}
