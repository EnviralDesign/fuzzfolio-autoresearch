//! Experiment-only topology co-adaptation overlay v4.
//!
//! The qualifying unit is a complete P/T/E/TE block. Parsing this contract
//! never schedules market work. `insert_exit_region` cannot enter the first
//! contrast. Projected topology-delta hashes cannot replace the actual
//! `TopologySemanticDeltaV1`.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;
use temporal_qd_contract::{Map, Value, canonical_sha256};

pub const COADAPTATION_SCHEMA: &str = "temporal_qd_topology_coadaptation_matrix_v4";
pub const COADAPTATION_MODE: &str = "frozen_complete_2x2_insert_setup_then_topology_local_event_v4";
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
const ROOT_KEYS: [&str; 24] = [
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
    "blocks",
    "materializationReceipts",
    "designScope",
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
const TOPOLOGY_RECORD_KEYS: [&str; 9] = [
    "planId",
    "parentCandidateId",
    "side",
    "topologyPlan",
    "planSha256",
    "addedSetupNodeId",
    "applicability",
    "topologySemanticDelta",
    "topologySemanticDeltaSha256",
];
const EVENT_PRIMITIVE_KEYS: [&str; 9] = [
    "primitiveId",
    "parentCandidateId",
    "side",
    "indicatorId",
    "contract",
    "originalNodeId",
    "originalNodeZone",
    "source",
    "selectionProvenance",
];
const SLOT_KEYS: [&str; 10] = [
    "slotId",
    "arm",
    "parentCandidateId",
    "side",
    "eligibility",
    "blockId",
    "topologyPlanId",
    "eventPrimitiveId",
    "settlingNodeId",
    "ineligibilityReason",
];
const BLOCK_KEYS: [&str; 10] = [
    "blockId",
    "parentCandidateId",
    "side",
    "parentRole",
    "classification",
    "topologyPlanId",
    "eventPrimitiveId",
    "armSlotIds",
    "excludedFromPrimaryCoadaptationCalculation",
    "incompletenessReason",
];
const RECEIPT_KEYS: [&str; 18] = [
    "receiptId",
    "blockId",
    "arm",
    "parentCandidateId",
    "side",
    "eligibility",
    "genomeSha256",
    "programSha256",
    "profileSha256",
    "topologySignature",
    "resourceFingerprint",
    "pairIdentitySha256",
    "nativeCompileValidationIdentity",
    "topologySemanticDelta",
    "operatorApplicationAudit",
    "eventAttachesToAddedSetupNode",
    "productionArchiveWrite",
    "failureReason",
];
const SETTLING_KEYS: [&str; 6] = [
    "kind",
    "mustTargetAddedSetupNodeId",
    "selection",
    "matchedControlSite",
    "eventOnlySiteLabel",
    "ineligibleCellsRemainExplicit",
];
const SUCCESS_KEYS: [&str; 22] = [
    "schemaVersion",
    "metricEquality",
    "qualifyingUnit",
    "incompleteBlocksExcludedFromPrimaryCalculation",
    "parentBeat",
    "riskQualifiedBeat",
    "fullEconomicPhenotypeTie",
    "supportDirectionQualityGates",
    "activityCostMechanismRequired",
    "parentBalancingRequired",
    "eventPlanBalancingRequired",
    "sideStratifiedReportingRequired",
    "requireTeNetStrictlyGreaterThanT",
    "requireTeNetStrictlyGreaterThanE",
    "requireTeWorstWindowNotWorseThanTAndEOrExplicitNonqualifyingTradeoff",
    "interactionIdentity",
    "noFixedPnlMargin",
    "requireReplicationPanelSurvivalForPromisingClaim",
    "requireUntouchedConfirmationPanelBeforeProductionConclusion",
    "doNotPromoteOnDevelopmentPanelAlone",
    "noveltyIsNotQuality",
    "familyLevelInferenceForbidden",
];
const DESIGN_SCOPE_KEYS: [&str; 9] = [
    "unitOfInference",
    "familyLevelInferenceForbidden",
    "oneEventPerBlockCannotSupportOperatorRepeatability",
    "eventSelectionProvenance",
    "preferredFollowOnDesign",
    "preferredFollowOnNotLaunched",
    "computeScientificTradeoff",
    "futureUntouchedConfirmationPanelAuthorityMustBeFrozenBeforeExecution",
    "doNotLaunch",
];
const DELTA_KEYS: [&str; 12] = [
    "schemaVersion",
    "operation",
    "planSha256",
    "beforeGenomeSha256",
    "afterGenomeSha256",
    "beforeTopologySha256",
    "afterTopologySha256",
    "addedNodes",
    "removedNodes",
    "addedEdges",
    "removedEdges",
    "changedEdges",
];

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum CoadaptationV4Error {
    #[error("{0}")]
    Contract(String),
}

pub type Result<T> = std::result::Result<T, CoadaptationV4Error>;

fn contract(message: impl Into<String>) -> CoadaptationV4Error {
    CoadaptationV4Error::Contract(message.into())
}

fn unexpected(label: &str) -> CoadaptationV4Error {
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
    if !fields.get("arguments").map(Value::is_object).unwrap_or(false) {
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

fn canonical_delta(delta: &Value) -> Result<Value> {
    let fields = object(delta, "topology semantic delta")?;
    exact_keys(fields, &DELTA_KEYS, "topology semantic delta")?;
    require_text(
        fields,
        "schemaVersion",
        "evolvable_module_topology_delta_v1",
        "topology semantic delta schema",
    )?;
    require_text(
        fields,
        "operation",
        FIRST_EXPERIMENT_OPERATION,
        "topology semantic delta operation",
    )?;
    for key in [
        "planSha256",
        "beforeGenomeSha256",
        "afterGenomeSha256",
        "beforeTopologySha256",
        "afterTopologySha256",
    ] {
        require_sha(fields.get(key).ok_or_else(|| unexpected(key))?, key)?;
    }
    for key in ["addedNodes", "removedNodes", "addedEdges", "removedEdges", "changedEdges"] {
        let items = fields
            .get(key)
            .and_then(Value::as_array)
            .ok_or_else(|| unexpected(key))?;
        if items.iter().any(|item| !item.is_string()) {
            return Err(unexpected(key));
        }
    }
    Ok(delta.clone())
}

fn delta_sha(delta: &Value) -> Result<String> {
    canonical_sha256(&canonical_delta(delta)?).map_err(|error| contract(error.to_string()))
}

fn string_list(value: &Value, label: &str) -> Result<Vec<String>> {
    let items = value
        .as_array()
        .ok_or_else(|| unexpected(label))?;
    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| unexpected(label))
        })
        .collect()
}

pub fn validate(payload: &Value) -> Result<Value> {
    let fields = object(payload, "topology coadaptation v4")?;
    exact_keys(fields, &ROOT_KEYS, "topology coadaptation v4")?;
    require_text(fields, "schemaVersion", COADAPTATION_SCHEMA, "schema")?;
    require_text(fields, "mode", COADAPTATION_MODE, "mode")?;
    require_bool(fields, "includeCrossover", false, "includeCrossover")?;
    require_text(fields, "cloneControl", CLONE_CONTROL, "cloneControl")?;
    require_bool(fields, "productionArchiveWrite", false, "productionArchiveWrite")?;
    if fields.get("mutationDepth") != Some(&json!(1)) {
        return Err(contract("topology coadaptation v4 mutationDepth drifted"));
    }
    require_bool(fields, "morphologyNurseryDeferred", true, "morphologyNurseryDeferred")?;
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
    require_str_list(fields.get("arms").ok_or_else(|| unexpected("arms"))?, &ARMS, "arms")?;
    require_bool(
        fields,
        "notAdmittedOnFrontGenerationPath",
        true,
        "notAdmittedOnFrontGenerationPath",
    )?;

    let parents_value = fields.get("parents").ok_or_else(|| unexpected("parents"))?;
    let parent_rows = parents_value
        .as_array()
        .ok_or_else(|| contract("topology coadaptation v4 requires frozen parents"))?;
    if parent_rows.is_empty() {
        return Err(contract("topology coadaptation v4 requires frozen parents"));
    }
    let mut parents = BTreeMap::new();
    let mut has_archive = false;
    for item in parent_rows {
        let parent = object(item, "topology coadaptation v4 parent")?;
        exact_keys(parent, &PARENT_KEYS, "topology coadaptation v4 parent")?;
        let candidate_id = text(parent, "candidateId", "parent")?.to_owned();
        if candidate_id.is_empty() || parents.contains_key(&candidate_id) {
            return Err(contract("topology coadaptation v4 parent drifted"));
        }
        let role = text(parent, "role", "parent role")?;
        if !PARENT_ROLES.contains(&role) {
            return Err(contract("topology coadaptation v4 parent drifted"));
        }
        if role == "archive" {
            has_archive = true;
        }
        require_sha(parent.get("longProgramSha256").ok_or_else(|| unexpected("parent"))?, "longProgramSha256")?;
        require_sha(parent.get("shortProgramSha256").ok_or_else(|| unexpected("parent"))?, "shortProgramSha256")?;
        parents.insert(candidate_id, parent.clone());
    }
    if !has_archive {
        return Err(contract("topology coadaptation v4 requires at least one archive parent"));
    }

    let panels = object(
        fields
            .get("panelIdentities")
            .ok_or_else(|| unexpected("panelIdentities"))?,
        "panelIdentities",
    )?;
    exact_keys(panels, &PANEL_KEYS, "panelIdentities")?;
    require_text(panels, "developmentPanelId", "panel-3", "developmentPanelId")?;
    require_text(panels, "developmentRole", "discovery_and_selection", "developmentRole")?;
    require_str_list(
        panels
            .get("replicationPanelIds")
            .ok_or_else(|| unexpected("replicationPanelIds"))?,
        &["panel-1", "panel-2"],
        "replicationPanelIds",
    )?;
    require_text(
        panels,
        "replicationRole",
        "inspected_replication_not_untouched_confirmation",
        "replicationRole",
    )?;
    let future = object(
        panels
            .get("futureConfirmationPanel")
            .ok_or_else(|| unexpected("futureConfirmationPanel"))?,
        "futureConfirmationPanel",
    )?;
    exact_keys(future, &FUTURE_PANEL_KEYS, "futureConfirmationPanel")?;
    require_bool(future, "createdInThisTask", false, "createdInThisTask")?;
    require_bool(
        future,
        "requiredBeforeProductionConclusion",
        true,
        "requiredBeforeProductionConclusion",
    )?;
    require_bool(
        future,
        "authorityMustBeBoundBeforeLaunch",
        true,
        "authorityMustBeBoundBeforeLaunch",
    )?;
    require_text(
        future,
        "label",
        "future_untouched_confirmation_panel",
        "futureConfirmationPanel.label",
    )?;
    require_sha(
        panels
            .get("rotatingEvidenceSha256")
            .ok_or_else(|| unexpected("rotatingEvidenceSha256"))?,
        "rotatingEvidenceSha256",
    )?;

    let mut topology_plans = BTreeMap::new();
    for item in fields
        .get("topologyPlans")
        .and_then(Value::as_array)
        .ok_or_else(|| unexpected("topologyPlans"))?
    {
        let row = object(item, "topology plan record")?;
        exact_keys(row, &TOPOLOGY_RECORD_KEYS, "topology plan record")?;
        let plan_id = text(row, "planId", "topology plan record")?.to_owned();
        if plan_id.is_empty() || topology_plans.contains_key(&plan_id) {
            return Err(unexpected("topology plan record"));
        }
        let parent_id = text(row, "parentCandidateId", "topology plan record")?;
        let side = text(row, "side", "topology plan record")?;
        if !parents.contains_key(parent_id) || (side != "long" && side != "short") {
            return Err(contract("topology plan record parent/side drifted"));
        }
        let plan = row.get("topologyPlan").ok_or_else(|| unexpected("topologyPlan"))?;
        let expected_sha = topology_plan_sha(plan)?;
        if text(row, "planSha256", "planSha256")? != expected_sha {
            return Err(contract("topology plan identity drift"));
        }
        let added = added_setup_node_id(plan)?;
        if text(row, "addedSetupNodeId", "addedSetupNodeId")? != added {
            return Err(contract("added setup node identity drift"));
        }
        let source_key = if side == "long" { "longProgramSha256" } else { "shortProgramSha256" };
        let expected_source = parents[parent_id][source_key]
            .as_str()
            .ok_or_else(|| contract("stale topology plan does not bind this exact parent genome"))?;
        let actual_source = object(plan, "topology plan")?
            .get("sourceGenomeSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| contract("stale topology plan does not bind this exact parent genome"))?;
        if actual_source != expected_source {
            return Err(contract("stale topology plan does not bind this exact parent genome"));
        }
        require_text(
            row,
            "applicability",
            "source_genome_matches_parent_side_program",
            "applicability",
        )?;
        let delta = row
            .get("topologySemanticDelta")
            .ok_or_else(|| unexpected("topologySemanticDelta"))?;
        let canonical = canonical_delta(delta)?;
        let delta_fields = object(&canonical, "topology semantic delta")?;
        if text(delta_fields, "planSha256", "delta planSha256")? != expected_sha {
            return Err(contract("topology semantic delta does not bind the frozen plan"));
        }
        let added_nodes = string_list(
            delta_fields.get("addedNodes").ok_or_else(|| unexpected("addedNodes"))?,
            "addedNodes",
        )?;
        if !added_nodes.iter().any(|node| node == &added) {
            return Err(contract("topology semantic delta must include the added setup node"));
        }
        if text(row, "topologySemanticDeltaSha256", "topologySemanticDeltaSha256")? != delta_sha(delta)? {
            return Err(contract(
                "projected topology-delta hash cannot replace the actual application delta",
            ));
        }
        topology_plans.insert(plan_id, row.clone());
    }

    let mut event_primitives = BTreeMap::new();
    for item in fields
        .get("eventPrimitives")
        .and_then(Value::as_array)
        .ok_or_else(|| unexpected("eventPrimitives"))?
    {
        let row = object(item, "event primitive")?;
        exact_keys(row, &EVENT_PRIMITIVE_KEYS, "event primitive")?;
        let primitive_id = text(row, "primitiveId", "event primitive")?.to_owned();
        if primitive_id.is_empty() || event_primitives.contains_key(&primitive_id) {
            return Err(unexpected("event primitive"));
        }
        let parent_id = text(row, "parentCandidateId", "event primitive")?;
        let side = text(row, "side", "event primitive")?;
        if !parents.contains_key(parent_id) || (side != "long" && side != "short") {
            return Err(contract("event primitive parent/side drifted"));
        }
        require_text(
            row,
            "source",
            "v38_recovered_directional_event_insert",
            "event primitive source",
        )?;
        require_text(
            row,
            "selectionProvenance",
            "v38_development_panel_selected_heterogeneous",
            "event primitive selection provenance",
        )?;
        let zone = text(row, "originalNodeZone", "originalNodeZone")?;
        if zone != "setup" && zone != "entry" {
            return Err(contract("event primitive originalNodeZone drifted"));
        }
        event_primitives.insert(primitive_id, row.clone());
    }

    let mut slots = BTreeMap::new();
    let mut slots_by_block: BTreeMap<String, Vec<Map<String, Value>>> = BTreeMap::new();
    for item in fields
        .get("slots")
        .and_then(Value::as_array)
        .ok_or_else(|| unexpected("slots"))?
    {
        let row = object(item, "topology coadaptation v4 slot")?;
        exact_keys(row, &SLOT_KEYS, "topology coadaptation v4 slot")?;
        let slot_id = text(row, "slotId", "slot")?.to_owned();
        if slot_id.is_empty() || slots.contains_key(&slot_id) {
            return Err(unexpected("topology coadaptation v4 slot"));
        }
        let arm = text(row, "arm", "slot arm")?;
        if !ARMS.contains(&arm) {
            return Err(contract("topology coadaptation v4 slot arm drifted"));
        }
        let parent_id = text(row, "parentCandidateId", "slot parent")?;
        let side = text(row, "side", "slot side")?;
        if !parents.contains_key(parent_id) || (side != "long" && side != "short") {
            return Err(contract("topology coadaptation v4 slot parent/side drifted"));
        }
        let block_id = text(row, "blockId", "blockId")?.to_owned();
        if block_id.is_empty() {
            return Err(unexpected("topology coadaptation v4 blockId"));
        }
        let eligibility = text(row, "eligibility", "eligibility")?;
        if eligibility != "eligible" && eligibility != "ineligible" {
            return Err(contract("slot eligibility drifted"));
        }
        if arm == "topology_then_topology_local_event" && eligibility == "eligible" {
            let plan_id = text(row, "topologyPlanId", "topologyPlanId")?;
            let plan = topology_plans
                .get(plan_id)
                .ok_or_else(|| contract("topology+event slot requires both plans"))?;
            let added = text(plan, "addedSetupNodeId", "addedSetupNodeId")?;
            if text(row, "settlingNodeId", "settlingNodeId")? != added {
                return Err(contract("topology+event slot must target the actual added setup node"));
            }
        }
        slots_by_block.entry(block_id).or_default().push(row.clone());
        slots.insert(slot_id, row.clone());
    }

    let mut complete_blocks = BTreeSet::new();
    let blocks = fields
        .get("blocks")
        .and_then(Value::as_array)
        .ok_or_else(|| contract("topology coadaptation v4 requires blocks"))?;
    if blocks.is_empty() {
        return Err(contract("topology coadaptation v4 requires blocks"));
    }
    let mut seen_blocks = BTreeSet::new();
    for item in blocks {
        let row = object(item, "topology coadaptation v4 block")?;
        exact_keys(row, &BLOCK_KEYS, "topology coadaptation v4 block")?;
        let block_id = text(row, "blockId", "block")?.to_owned();
        if !seen_blocks.insert(block_id.clone()) {
            return Err(unexpected("topology coadaptation v4 block"));
        }
        let grouped = slots_by_block.get(&block_id).ok_or_else(|| contract("each block requires exactly one P/T/E/TE slot"))?;
        if grouped.len() != 4 {
            return Err(contract("each block requires exactly one P/T/E/TE slot"));
        }
        let classification = text(row, "classification", "classification")?;
        let arm_slots = object(
            row.get("armSlotIds").ok_or_else(|| unexpected("armSlotIds"))?,
            "block armSlotIds",
        )?;
        exact_keys(arm_slots, &ARMS, "block armSlotIds")?;
        if classification == "complete_2x2_block" {
            require_bool(
                row,
                "excludedFromPrimaryCoadaptationCalculation",
                false,
                "complete blocks must enter the primary calculation",
            )?;
            if !row.get("incompletenessReason").map(Value::is_null).unwrap_or(false) {
                return Err(contract("complete block cannot carry an incompleteness reason"));
            }
            complete_blocks.insert(block_id);
            let te_slot_id = text(arm_slots, "topology_then_topology_local_event", "TE slot")?;
            let e_slot_id = text(arm_slots, "event_only_control", "E slot")?;
            let te = slots.get(te_slot_id).ok_or_else(|| contract("complete block TE missing"))?;
            let ev = slots.get(e_slot_id).ok_or_else(|| contract("complete block E missing"))?;
            if text(te, "eventPrimitiveId", "TE primitive")? != text(ev, "eventPrimitiveId", "E primitive")? {
                return Err(contract("E and TE must use the identical event primitive"));
            }
        } else if classification == "exploratory_incomplete_block" {
            require_bool(
                row,
                "excludedFromPrimaryCoadaptationCalculation",
                true,
                "incomplete blocks cannot enter qualification",
            )?;
        } else {
            return Err(contract("block classification drifted"));
        }
    }

    let mut complete_arms: BTreeMap<String, BTreeSet<String>> = complete_blocks
        .iter()
        .cloned()
        .map(|block_id| (block_id, BTreeSet::new()))
        .collect();
    for item in fields
        .get("materializationReceipts")
        .and_then(Value::as_array)
        .ok_or_else(|| unexpected("materializationReceipts"))?
    {
        let row = object(item, "materialization receipt")?;
        exact_keys(row, &RECEIPT_KEYS, "materialization receipt")?;
        let block_id = text(row, "blockId", "receipt block")?;
        require_bool(row, "productionArchiveWrite", false, "productionArchiveWrite")?;
        if complete_blocks.contains(block_id) {
            require_text(row, "eligibility", "eligible", "complete-block receipts must materialize")?;
            for key in [
                "genomeSha256",
                "programSha256",
                "profileSha256",
                "topologySignature",
                "resourceFingerprint",
                "pairIdentitySha256",
            ] {
                require_sha(row.get(key).ok_or_else(|| unexpected(key))?, key)?;
            }
            let arm = text(row, "arm", "receipt arm")?.to_owned();
            if arm == "topology_then_topology_local_event"
                && row.get("eventAttachesToAddedSetupNode") != Some(&Value::Bool(true))
            {
                return Err(contract("TE receipt must prove the event attaches to the added setup node"));
            }
            complete_arms
                .entry(block_id.to_owned())
                .or_default()
                .insert(arm);
        }
    }
    for arms in complete_arms.values() {
        if arms.len() != 4 {
            return Err(contract("all P/T/E/TE children must materialize, compile, and audit"));
        }
    }

    let settling = object(fields.get("settling").ok_or_else(|| unexpected("settling"))?, "settling")?;
    exact_keys(settling, &SETTLING_KEYS, "settling")?;
    require_text(settling, "kind", "directional_event_insert", "settling.kind")?;
    require_bool(settling, "mustTargetAddedSetupNodeId", true, "mustTargetAddedSetupNodeId")?;
    require_bool(settling, "ineligibleCellsRemainExplicit", true, "ineligibleCellsRemainExplicit")?;

    let success = object(
        fields
            .get("successCalculation")
            .ok_or_else(|| unexpected("successCalculation"))?,
        "successCalculation",
    )?;
    exact_keys(success, &SUCCESS_KEYS, "successCalculation")?;
    require_text(
        success,
        "schemaVersion",
        "temporal_qd_topology_coadaptation_success_v4",
        "successCalculation.schemaVersion",
    )?;
    require_text(success, "qualifyingUnit", "complete_2x2_block", "qualifyingUnit")?;
    require_bool(
        success,
        "incompleteBlocksExcludedFromPrimaryCalculation",
        true,
        "incompleteBlocksExcludedFromPrimaryCalculation",
    )?;
    require_bool(success, "familyLevelInferenceForbidden", true, "familyLevelInferenceForbidden")?;
    require_text(success, "interactionIdentity", "TE_minus_T_minus_E_plus_P", "interactionIdentity")?;
    require_bool(success, "noFixedPnlMargin", true, "noFixedPnlMargin")?;
    require_bool(success, "requireTeNetStrictlyGreaterThanT", true, "requireTeNetStrictlyGreaterThanT")?;
    require_bool(success, "requireTeNetStrictlyGreaterThanE", true, "requireTeNetStrictlyGreaterThanE")?;
    require_bool(success, "doNotPromoteOnDevelopmentPanelAlone", true, "doNotPromoteOnDevelopmentPanelAlone")?;

    let design = object(fields.get("designScope").ok_or_else(|| unexpected("designScope"))?, "designScope")?;
    exact_keys(design, &DESIGN_SCOPE_KEYS, "designScope")?;
    require_bool(design, "familyLevelInferenceForbidden", true, "design familyLevelInferenceForbidden")?;
    require_bool(design, "doNotLaunch", true, "design doNotLaunch")?;
    require_bool(design, "preferredFollowOnNotLaunched", true, "preferredFollowOnNotLaunched")?;
    require_text(
        design,
        "unitOfInference",
        "deterministic_case_study_complete_2x2_block",
        "unitOfInference",
    )?;

    let mut unsigned = fields.clone();
    unsigned.remove("contractSha256");
    let expected = canonical_sha256(&Value::Object(unsigned)).map_err(|error| contract(error.to_string()))?;
    if text(fields, "contractSha256", "contractSha256")? != expected {
        return Err(contract("topology coadaptation v4 identity drift"));
    }
    Ok(payload.clone())
}
