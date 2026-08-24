//! Experiment-only topology co-adaptation overlay v6.
//!
//! The qualifying unit is a complete P/T/E/TE block. Parsing this contract
//! never schedules market work. `insert_exit_region` cannot enter the first
//! contrast. Projected topology-delta hashes cannot replace the actual
//! `TopologySemanticDeltaV1`.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;
use temporal_qd_contract::{Map, Value, canonical_sha256};

pub const COADAPTATION_SCHEMA: &str = "temporal_qd_topology_coadaptation_matrix_v6";
pub const COADAPTATION_MODE: &str = "frozen_complete_2x2_insert_setup_then_topology_local_event_pair_receipts_v6";
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
const RECEIPT_KEYS: [&str; 35] = [
    "receiptId",
    "blockId",
    "arm",
    "parentCandidateId",
    "side",
    "eligibility",
    "changedSideGenomeSha256",
    "changedSideProgramSha256",
    "changedSideProfileSha256",
    "topologySignature",
    "resourceFingerprint",
    "longProgramSha256",
    "shortProgramSha256",
    "unchangedOppositeProgramSha256",
    "unchangedOppositeProgramPreserved",
    "reconstructedPairProgramIdentitySha256",
    "frozenPairIdentitySha256",
    "pairCandidateIdentitySha256",
    "pairProfileSha256",
    "normalizedProfileSnapshotSha256",
    "moduleCompilerPolicySha256",
    "moduleCompileArtifactSha256",
    "canonicalPairCompilerAuthoritySha256",
    "canonicalPairCompileReportSha256",
    "nativeValidationRan",
    "nativeValidationAuthoritySha256",
    "nativeValidationReportSha256",
    "pairCompileStatus",
    "applicationParentGenomeSha256",
    "applicationChildGenomeSha256",
    "topologySemanticDelta",
    "operatorApplicationAudit",
    "eventAttachesToAddedSetupNode",
    "productionArchiveWrite",
    "failureReason",
];
const FORBIDDEN_RECEIPT_LABELS: [&str; 2] = ["pairIdentitySha256", "nativeCompileValidationIdentity"];
const CLONE_OR_TOPOLOGY_AUDIT_KEYS: [&str; 3] = ["arm", "productionArchiveWrite", "replayed"];
const EVENT_APPLICATION_AUDIT_KEYS: [&str; 12] = [
    "applicationSha256",
    "childGenomeSha256",
    "childSemanticTopologySha256",
    "constructionIdentitySha256",
    "operatorSpecSha256",
    "operatorVersion",
    "parentGenomeSha256",
    "parentSemanticTopologySha256",
    "planSha256",
    "schemaVersion",
    "semanticDelta",
    "staticInvariantReport",
];
const EVENT_DELTA_KEYS: [&str; 5] = ["eventId", "indicatorId", "indicatorInstanceId", "nodeId", "operation"];
const INSTRUMENTATION_KEYS: [&str; 14] = [
    "addedSetupNodeOccupancy",
    "barsInAddedSetup",
    "entryTimestampShiftVsP",
    "changedTradeOpportunityCount",
    "eventFreshnessAtE",
    "eventFreshnessAtTE",
    "transitionPathTraceOnAddedNode",
    "tradeCount",
    "costDrag",
    "supportStatus",
    "directionStatus",
    "qualityStatus",
    "computedInThisTask",
    "insertSetupRemainsTimingMutation",
];
const SETTLING_KEYS: [&str; 6] = [
    "kind",
    "mustTargetAddedSetupNodeId",
    "selection",
    "matchedControlSite",
    "eventOnlySiteLabel",
    "ineligibleCellsRemainExplicit",
];
const SUCCESS_KEYS: [&str; 31] = [
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
    "requireTeNetStrictlyGreaterThanPForUsefulInnovation",
    "requireTeWorstWindowNotWorseThanTAndEOrExplicitNonqualifyingTradeoff",
    "requireTeWorstWindowNotWorseThanPForUsefulInnovation",
    "interactionIdentity",
    "interactionObservedIsDescriptiveOnly",
    "usefulProgressiveInnovationRequiresTeGreaterThanP",
    "promisingMeansUsefulProgressiveInnovationNotMereInteraction",
    "noFixedPnlMargin",
    "requireReplicationPanelSurvivalForPromisingClaim",
    "requireUntouchedConfirmationPanelBeforeProductionConclusion",
    "doNotPromoteOnDevelopmentPanelAlone",
    "noveltyIsNotQuality",
    "familyLevelInferenceForbidden",
    "combinedOutperformsBothSingleMutationsReportedSeparately",
    "signedInteractionTermReportedSeparately",
    "teGreaterThanTAndEIsNotPositiveInteractionByItself",
    "usefulProgressiveInnovationRequiresTeWorstNotWorseThanPandTAndE",
];
const DESIGN_SCOPE_KEYS: [&str; 12] = [
    "unitOfInference",
    "familyLevelInferenceForbidden",
    "oneEventPerBlockCannotSupportOperatorRepeatability",
    "eventSelectionProvenance",
    "preferredFollowOnDesign",
    "preferredFollowOnNotLaunched",
    "computeScientificTradeoff",
    "futureUntouchedConfirmationPanelAuthorityMustBeFrozenBeforeExecution",
    "insertSetupIsTimingMutation",
    "insertSetupIsNotBehaviorPreservingWithoutReplay",
    "evaluationInstrumentation",
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
pub enum CoadaptationV6Error {
    #[error("{0}")]
    Contract(String),
}

pub type Result<T> = std::result::Result<T, CoadaptationV6Error>;

fn contract(message: impl Into<String>) -> CoadaptationV6Error {
    CoadaptationV6Error::Contract(message.into())
}

fn unexpected(label: &str) -> CoadaptationV6Error {
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


fn is_null(value: Option<&Value>) -> bool {
    matches!(value, None | Some(Value::Null))
}

fn event_delta(audit: &Map<String, Value>) -> Result<Map<String, Value>> {
    let delta = audit
        .get("semanticDelta")
        .and_then(Value::as_array)
        .ok_or_else(|| contract("event audit must include an exact semantic delta"))?;
    if delta.is_empty() {
        return Err(contract("event audit must include an exact semantic delta"));
    }
    let row = object(&delta[0], "event semantic delta")?;
    exact_keys(row, &EVENT_DELTA_KEYS, "event semantic delta")?;
    require_text(row, "operation", "directional_event_insert", "event semantic delta operation")?;
    Ok(row.clone())
}

fn validate_event_application_audit(audit: &Value) -> Result<Map<String, Value>> {
    let row = object(audit, "event operator application audit")?;
    exact_keys(row, &EVENT_APPLICATION_AUDIT_KEYS, "event operator application audit")?;
    require_text(
        row,
        "schemaVersion",
        "temporal_structural_operator_application_v1",
        "event application audit schema",
    )?;
    let stored = require_sha(
        row.get("applicationSha256").ok_or_else(|| unexpected("applicationSha256"))?,
        "applicationSha256",
    )?;
    let mut unsigned = row.clone();
    unsigned.remove("applicationSha256");
    let expected = canonical_sha256(&Value::Object(unsigned)).map_err(|error| contract(error.to_string()))?;
    if stored != expected {
        return Err(contract("application SHA must recompute"));
    }
    let report = object(
        row.get("staticInvariantReport").ok_or_else(|| unexpected("staticInvariantReport"))?,
        "staticInvariantReport",
    )?;
    require_bool(report, "allChecksPassed", true, "operator application audit must pass")?;
    if let Some(stored_report) = report.get("auditSha256") {
        let stored_report = require_sha(stored_report, "auditSha256")?;
        let mut report_body = report.clone();
        report_body.remove("auditSha256");
        let expected_report =
            canonical_sha256(&Value::Object(report_body)).map_err(|error| contract(error.to_string()))?;
        if stored_report != expected_report {
            return Err(contract("audit SHA must recompute"));
        }
    }
    event_delta(row)?;
    Ok(row.clone())
}

fn validate_clone_or_topology_audit(audit: &Value, arm: &str) -> Result<()> {
    let row = object(audit, "clone or topology operator audit")?;
    exact_keys(row, &CLONE_OR_TOPOLOGY_AUDIT_KEYS, "clone or topology operator audit")?;
    require_text(row, "arm", arm, "clone or topology audit arm")?;
    require_bool(row, "productionArchiveWrite", false, "operatorApplicationAudit.productionArchiveWrite")?;
    require_bool(row, "replayed", true, "operatorApplicationAudit.replayed")?;
    Ok(())
}

pub fn validate(payload: &Value) -> Result<Value> {
    let fields = object(payload, "topology coadaptation v6")?;
    exact_keys(fields, &ROOT_KEYS, "topology coadaptation v6")?;
    require_text(fields, "schemaVersion", COADAPTATION_SCHEMA, "schema")?;
    require_text(fields, "mode", COADAPTATION_MODE, "mode")?;
    require_bool(fields, "includeCrossover", false, "includeCrossover")?;
    require_text(fields, "cloneControl", CLONE_CONTROL, "cloneControl")?;
    require_bool(fields, "productionArchiveWrite", false, "productionArchiveWrite")?;
    if fields.get("mutationDepth") != Some(&json!(1)) {
        return Err(contract("topology coadaptation v6 mutationDepth drifted"));
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
        .ok_or_else(|| contract("topology coadaptation v6 requires frozen parents"))?;
    if parent_rows.is_empty() {
        return Err(contract("topology coadaptation v6 requires frozen parents"));
    }
    let mut parents = BTreeMap::new();
    let mut has_archive = false;
    for item in parent_rows {
        let parent = object(item, "topology coadaptation v6 parent")?;
        exact_keys(parent, &PARENT_KEYS, "topology coadaptation v6 parent")?;
        let candidate_id = text(parent, "candidateId", "parent")?.to_owned();
        if candidate_id.is_empty() || parents.contains_key(&candidate_id) {
            return Err(contract("topology coadaptation v6 parent drifted"));
        }
        let role = text(parent, "role", "parent role")?;
        if !PARENT_ROLES.contains(&role) {
            return Err(contract("topology coadaptation v6 parent drifted"));
        }
        if role == "archive" {
            has_archive = true;
        }
        require_sha(parent.get("longProgramSha256").ok_or_else(|| unexpected("parent"))?, "longProgramSha256")?;
        require_sha(parent.get("shortProgramSha256").ok_or_else(|| unexpected("parent"))?, "shortProgramSha256")?;
        parents.insert(candidate_id, parent.clone());
    }
    if !has_archive {
        return Err(contract("topology coadaptation v6 requires at least one archive parent"));
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
        let row = object(item, "topology coadaptation v6 slot")?;
        exact_keys(row, &SLOT_KEYS, "topology coadaptation v6 slot")?;
        let slot_id = text(row, "slotId", "slot")?.to_owned();
        if slot_id.is_empty() || slots.contains_key(&slot_id) {
            return Err(unexpected("topology coadaptation v6 slot"));
        }
        let arm = text(row, "arm", "slot arm")?;
        if !ARMS.contains(&arm) {
            return Err(contract("topology coadaptation v6 slot arm drifted"));
        }
        let parent_id = text(row, "parentCandidateId", "slot parent")?;
        let side = text(row, "side", "slot side")?;
        if !parents.contains_key(parent_id) || (side != "long" && side != "short") {
            return Err(contract("topology coadaptation v6 slot parent/side drifted"));
        }
        let block_id = text(row, "blockId", "blockId")?.to_owned();
        if block_id.is_empty() {
            return Err(unexpected("topology coadaptation v6 blockId"));
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
        .ok_or_else(|| contract("topology coadaptation v6 requires blocks"))?;
    if blocks.is_empty() {
        return Err(contract("topology coadaptation v6 requires blocks"));
    }
    let mut seen_blocks = BTreeSet::new();
    for item in blocks {
        let row = object(item, "topology coadaptation v6 block")?;
        exact_keys(row, &BLOCK_KEYS, "topology coadaptation v6 block")?;
        let block_id = text(row, "blockId", "block")?.to_owned();
        if !seen_blocks.insert(block_id.clone()) {
            return Err(unexpected("topology coadaptation v6 block"));
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

    let receipts_value = fields
        .get("materializationReceipts")
        .and_then(Value::as_array)
        .ok_or_else(|| unexpected("materializationReceipts"))?;
    if receipts_value.len() != slots.len() {
        return Err(contract("receipt count must equal declared slot count"));
    }
    let mut slot_by_arm: BTreeMap<(String, String), Map<String, Value>> = BTreeMap::new();
    for slot in slots.values() {
        let block_id = text(slot, "blockId", "slot block")?.to_owned();
        let arm = text(slot, "arm", "slot arm")?.to_owned();
        if slot_by_arm
            .insert((block_id, arm), slot.clone())
            .is_some()
        {
            return Err(contract("slots must be unique per block arm"));
        }
    }
    let mut seen_receipts = BTreeSet::new();
    let mut seen_arms = BTreeSet::new();
    let mut complete_arms: BTreeMap<String, BTreeMap<String, Map<String, Value>>> = complete_blocks
        .iter()
        .cloned()
        .map(|block_id| (block_id, BTreeMap::new()))
        .collect();
    for item in receipts_value {
        let row = object(item, "materialization receipt")?;
        for forbidden in FORBIDDEN_RECEIPT_LABELS {
            if row.contains_key(forbidden) {
                return Err(contract(
                    "module hashes cannot be labeled as pair or native validation identities",
                ));
            }
        }
        exact_keys(row, &RECEIPT_KEYS, "materialization receipt")?;
        let receipt_id = text(row, "receiptId", "receiptId")?.to_owned();
        if receipt_id.is_empty() || !seen_receipts.insert(receipt_id.clone()) {
            return Err(unexpected("materialization receipt"));
        }
        let block_id = text(row, "blockId", "receipt block")?.to_owned();
        let arm = text(row, "arm", "receipt arm")?.to_owned();
        let side = text(row, "side", "receipt side")?;
        if !ARMS.contains(&arm.as_str()) || (side != "long" && side != "short") {
            return Err(contract("materialization receipt arm/side drifted"));
        }
        if !seen_arms.insert((block_id.clone(), arm.clone())) {
            return Err(contract("duplicate receipt for a declared arm"));
        }
        let slot = slot_by_arm
            .get(&(block_id.clone(), arm.clone()))
            .ok_or_else(|| contract("receipt does not bind to a declared slot"))?;
        if receipt_id != text(slot, "slotId", "slotId")? {
            return Err(contract("receiptId must equal slotId"));
        }
        if text(row, "parentCandidateId", "receipt parent")? != text(slot, "parentCandidateId", "slot parent")?
            || side != text(slot, "side", "slot side")?
        {
            return Err(contract("receipt parent/side does not match its slot"));
        }
        require_bool(row, "productionArchiveWrite", false, "productionArchiveWrite")?;
        require_bool(row, "nativeValidationRan", false, "nativeValidationRan")?;
        if complete_blocks.contains(&block_id) {
            require_text(row, "eligibility", "eligible", "complete-block receipts must materialize")?;
            for key in [
                "changedSideGenomeSha256",
                "changedSideProgramSha256",
                "changedSideProfileSha256",
                "topologySignature",
                "resourceFingerprint",
                "longProgramSha256",
                "shortProgramSha256",
                "unchangedOppositeProgramSha256",
                "reconstructedPairProgramIdentitySha256",
                "pairCandidateIdentitySha256",
                "moduleCompilerPolicySha256",
                "moduleCompileArtifactSha256",
                "applicationParentGenomeSha256",
                "applicationChildGenomeSha256",
            ] {
                require_sha(row.get(key).ok_or_else(|| unexpected(key))?, key)?;
            }
            require_bool(row, "unchangedOppositeProgramPreserved", true, "unchangedOppositeProgramPreserved")?;
            if !row.get("failureReason").map(Value::is_null).unwrap_or(false) {
                return Err(contract("successful receipt cannot carry a failure reason"));
            }
            if !is_null(row.get("frozenPairIdentitySha256")) {
                return Err(contract("frozen pair identity cannot be claimed without FrozenPair.compile"));
            }
            if !is_null(row.get("pairProfileSha256")) || !is_null(row.get("normalizedProfileSnapshotSha256")) {
                return Err(contract("pair profile identities require canonical FrozenPair.compile"));
            }
            if !is_null(row.get("canonicalPairCompilerAuthoritySha256"))
                || !is_null(row.get("canonicalPairCompileReportSha256"))
            {
                return Err(contract("canonical pair compiler fields require an actual pair compiler"));
            }
            if !is_null(row.get("nativeValidationAuthoritySha256"))
                || !is_null(row.get("nativeValidationReportSha256"))
            {
                return Err(contract("native validation receipts require native validation"));
            }
            require_text(
                row,
                "pairCompileStatus",
                "module_sides_reconstructed_pair_compiler_unavailable",
                "complete-block pairCompileStatus drifted",
            )?;
            let parent = parents
                .get(text(row, "parentCandidateId", "parentCandidateId")?)
                .ok_or_else(|| contract("receipt parent is not in the frozen parent set"))?;
            let changed = text(row, "changedSideProgramSha256", "changedSideProgramSha256")?;
            let expected_long = if side == "long" {
                changed
            } else {
                text(parent, "longProgramSha256", "parent long")?
            };
            let expected_short = if side == "short" {
                changed
            } else {
                text(parent, "shortProgramSha256", "parent short")?
            };
            if text(row, "longProgramSha256", "longProgramSha256")? != expected_long
                || text(row, "shortProgramSha256", "shortProgramSha256")? != expected_short
            {
                return Err(contract("pair program legs drifted"));
            }
            let expected_pair = canonical_sha256(&json!({
                "parentCandidateId": text(row, "parentCandidateId", "parentCandidateId")?,
                "longProgramSha256": expected_long,
                "shortProgramSha256": expected_short,
            }))
            .map_err(|error| contract(error.to_string()))?;
            if text(row, "reconstructedPairProgramIdentitySha256", "pair identity")? != expected_pair {
                return Err(contract("reconstructed pair program identity drifted"));
            }
            let expected_candidate = canonical_sha256(&json!({
                "parentCandidateId": text(row, "parentCandidateId", "parentCandidateId")?,
                "arm": arm,
                "side": side,
                "reconstructedPairProgramIdentitySha256": expected_pair,
            }))
            .map_err(|error| contract(error.to_string()))?;
            if text(row, "pairCandidateIdentitySha256", "pair candidate")? != expected_candidate {
                return Err(contract("pair candidate identity drifted"));
            }
            if arm == "topology_only_child" || arm == "topology_then_topology_local_event" {
                let plan_id = text(slot, "topologyPlanId", "topologyPlanId")?;
                let plan = topology_plans
                    .get(plan_id)
                    .ok_or_else(|| contract("topology receipt is missing the declared topology plan"))?;
                let delta = canonical_delta(
                    row.get("topologySemanticDelta")
                        .ok_or_else(|| contract("topology receipt is missing the declared topology plan"))?,
                )?;
                let delta_fields = object(&delta, "topology semantic delta")?;
                if text(delta_fields, "planSha256", "delta planSha256")? != text(plan, "planSha256", "planSha256")? {
                    return Err(contract("topology receipt planSha256 must equal the declared topology plan"));
                }
                if &delta != plan.get("topologySemanticDelta").ok_or_else(|| unexpected("topologySemanticDelta"))? {
                    return Err(contract("topology receipt delta must equal the frozen topology record delta"));
                }
                if arm == "topology_only_child"
                    && text(row, "changedSideGenomeSha256", "T genome")?
                        != text(delta_fields, "afterGenomeSha256", "afterGenomeSha256")?
                {
                    return Err(contract("T receipt genome must match the actual topology application delta"));
                }
            }
            if arm == "event_only_control" || arm == "topology_then_topology_local_event" {
                let primitive_id = slot.get("eventPrimitiveId");
                if is_null(primitive_id) {
                    return Err(contract("event receipt must bind the declared event primitive"));
                }
                let primitive_id = text(slot, "eventPrimitiveId", "eventPrimitiveId")?;
                let primitive = event_primitives
                    .get(primitive_id)
                    .ok_or_else(|| contract("event receipt primitive is not in the frozen set"))?;
                let audit = validate_event_application_audit(
                    row.get("operatorApplicationAudit")
                        .ok_or_else(|| unexpected("operatorApplicationAudit"))?,
                )?;
                let event_delta = event_delta(&audit)?;
                if text(&event_delta, "indicatorId", "indicatorId")? != text(primitive, "indicatorId", "indicatorId")? {
                    return Err(contract("event receipt indicator must match the declared primitive"));
                }
                let declared_site = text(slot, "settlingNodeId", "settlingNodeId")?;
                if text(&event_delta, "nodeId", "nodeId")? != declared_site {
                    return Err(contract(
                        "event receipt node must equal the declared event-only or added-setup site",
                    ));
                }
            } else {
                validate_clone_or_topology_audit(
                    row.get("operatorApplicationAudit")
                        .ok_or_else(|| unexpected("operatorApplicationAudit"))?,
                    &arm,
                )?;
            }
            if arm == "topology_then_topology_local_event" {
                require_bool(
                    row,
                    "eventAttachesToAddedSetupNode",
                    true,
                    "TE receipt must prove the event attaches to the added setup node",
                )?;
            }
            complete_arms
                .entry(block_id)
                .or_default()
                .insert(arm, row.clone());
        } else {
            require_text(row, "eligibility", "ineligible", "incomplete-block receipts remain ineligible")?;
            require_text(
                row,
                "pairCompileStatus",
                "incomplete_block_not_materialized",
                "incomplete-block pairCompileStatus drifted",
            )?;
        }
    }
    if seen_arms.len() != slot_by_arm.len() {
        return Err(contract("receipts must cover every declared slot and no extras"));
    }
    for block_id in &complete_blocks {
        let arms = complete_arms
            .get(block_id)
            .ok_or_else(|| contract("all P/T/E/TE children must materialize, compile, and audit"))?;
        if ARMS.iter().any(|arm| !arms.contains_key(*arm)) {
            return Err(contract("all P/T/E/TE children must materialize, compile, and audit"));
        }
        let parent = arms.get("exact_parent_clone").expect("P");
        let topology = arms.get("topology_only_child").expect("T");
        let event = arms.get("event_only_control").expect("E");
        let combined = arms.get("topology_then_topology_local_event").expect("TE");
        let parent_genome = text(parent, "changedSideGenomeSha256", "P genome")?;
        let topology_genome = text(topology, "changedSideGenomeSha256", "T genome")?;
        if text(event, "applicationParentGenomeSha256", "E parent")? != parent_genome {
            return Err(contract("E application parent must be the P genome"));
        }
        if text(combined, "applicationParentGenomeSha256", "TE parent")? != topology_genome {
            return Err(contract("TE application parent must be the T genome"));
        }
        let te_slot = slot_by_arm
            .get(&(block_id.clone(), "topology_then_topology_local_event".to_owned()))
            .ok_or_else(|| contract("complete block TE missing"))?;
        let plan_id = text(te_slot, "topologyPlanId", "TE topologyPlanId")?;
        let plan = topology_plans
            .get(plan_id)
            .ok_or_else(|| contract("complete block topology plan missing"))?;
        let added = text(plan, "addedSetupNodeId", "addedSetupNodeId")?;
        let te_audit = object(
            combined
                .get("operatorApplicationAudit")
                .ok_or_else(|| unexpected("TE audit"))?,
            "TE audit",
        )?;
        let te_delta = event_delta(te_audit)?;
        if text(&te_delta, "nodeId", "TE node")? != added {
            return Err(contract("TE event node must be the added setup node"));
        }
        if combined.get("eventAttachesToAddedSetupNode") != Some(&Value::Bool(true)) {
            return Err(contract("TE receipt must attach to the added setup node"));
        }
        if event.get("eventAttachesToAddedSetupNode") == Some(&Value::Bool(true)) {
            return Err(contract("E receipt must not claim the added setup node"));
        }
        let e_slot = slot_by_arm
            .get(&(block_id.clone(), "event_only_control".to_owned()))
            .ok_or_else(|| contract("complete block E missing"))?;
        if text(e_slot, "eventPrimitiveId", "E primitive")? != text(te_slot, "eventPrimitiveId", "TE primitive")? {
            return Err(contract("E and TE must use the identical declared event primitive"));
        }
        let e_audit = object(
            event
                .get("operatorApplicationAudit")
                .ok_or_else(|| unexpected("E audit"))?,
            "E audit",
        )?;
        let e_delta = event_delta(e_audit)?;
        if text(&e_delta, "nodeId", "E node")? != text(e_slot, "settlingNodeId", "E site")? {
            return Err(contract("E must target the declared event-only site"));
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
        "temporal_qd_topology_coadaptation_success_v6",
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
    require_bool(success, "requireTeNetStrictlyGreaterThanPForUsefulInnovation", true, "requireTeNetStrictlyGreaterThanPForUsefulInnovation")?;
    require_bool(success, "requireTeWorstWindowNotWorseThanPForUsefulInnovation", true, "requireTeWorstWindowNotWorseThanPForUsefulInnovation")?;
    require_bool(success, "interactionObservedIsDescriptiveOnly", true, "interactionObservedIsDescriptiveOnly")?;
    require_bool(success, "usefulProgressiveInnovationRequiresTeGreaterThanP", true, "usefulProgressiveInnovationRequiresTeGreaterThanP")?;
    require_bool(success, "promisingMeansUsefulProgressiveInnovationNotMereInteraction", true, "promisingMeansUsefulProgressiveInnovationNotMereInteraction")?;
    require_bool(success, "combinedOutperformsBothSingleMutationsReportedSeparately", true, "combinedOutperformsBothSingleMutationsReportedSeparately")?;
    require_bool(success, "signedInteractionTermReportedSeparately", true, "signedInteractionTermReportedSeparately")?;
    require_bool(success, "teGreaterThanTAndEIsNotPositiveInteractionByItself", true, "teGreaterThanTAndEIsNotPositiveInteractionByItself")?;
    require_bool(success, "usefulProgressiveInnovationRequiresTeWorstNotWorseThanPandTAndE", true, "usefulProgressiveInnovationRequiresTeWorstNotWorseThanPandTAndE")?;
    require_bool(success, "doNotPromoteOnDevelopmentPanelAlone", true, "doNotPromoteOnDevelopmentPanelAlone")?;

    let design = object(fields.get("designScope").ok_or_else(|| unexpected("designScope"))?, "designScope")?;
    exact_keys(design, &DESIGN_SCOPE_KEYS, "designScope")?;
    require_bool(design, "familyLevelInferenceForbidden", true, "design familyLevelInferenceForbidden")?;
    require_bool(design, "doNotLaunch", true, "design doNotLaunch")?;
    require_bool(design, "preferredFollowOnNotLaunched", true, "preferredFollowOnNotLaunched")?;
    require_bool(design, "insertSetupIsTimingMutation", true, "insertSetupIsTimingMutation")?;
    require_bool(design, "insertSetupIsNotBehaviorPreservingWithoutReplay", true, "insertSetupIsNotBehaviorPreservingWithoutReplay")?;
    let instrumentation = object(
        design
            .get("evaluationInstrumentation")
            .ok_or_else(|| unexpected("evaluationInstrumentation"))?,
        "evaluationInstrumentation",
    )?;
    exact_keys(instrumentation, &INSTRUMENTATION_KEYS, "evaluationInstrumentation")?;
    require_bool(instrumentation, "computedInThisTask", false, "computedInThisTask")?;
    require_bool(
        instrumentation,
        "insertSetupRemainsTimingMutation",
        true,
        "insertSetupRemainsTimingMutation",
    )?;
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
        return Err(contract("topology coadaptation v6 identity drift"));
    }
    Ok(payload.clone())
}
