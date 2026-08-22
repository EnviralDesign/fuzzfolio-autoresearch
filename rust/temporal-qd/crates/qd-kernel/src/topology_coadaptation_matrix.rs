//! Experiment-only topology co-adaptation overlay.
//!
//! Production rotating 4/5 breeding omits `topologyCoadaptationMatrix`.
//! Parsing this contract never schedules market work.  The v1 schema is
//! rejected: it allowed a broad resource family to be labeled parameter-only
//! and did not require frozen parents or a self-hash.

use std::collections::BTreeSet;

use temporal_qd_contract::{Map, Value, canonical_sha256};

pub const COADAPTATION_SCHEMA: &str = "temporal_qd_topology_coadaptation_matrix_v2";
pub const COADAPTATION_MODE: &str = "frozen_parent_topology_then_matched_settling_v2";
pub const CLONE_CONTROL: &str = "re_evaluate_parent_on_frozen_panel";
pub const ARMS: [&str; 6] = [
    "exact_parent_clone",
    "topology_only_child",
    "parameter_only_control",
    "resource_semantic_control",
    "topology_then_parameter_only_settling",
    "topology_then_resource_semantic_settling",
];
pub const FIRST_EXPERIMENT_ARMS: [&str; 4] = [
    "exact_parent_clone",
    "topology_only_child",
    "resource_semantic_control",
    "topology_then_resource_semantic_settling",
];
const PARAMETER_ONLY_KINDS: [&str; 4] = [
    "indicator_period_mutate",
    "indicator_range_mutate",
    "indicator_timeframe_mutate",
    "indicator_lookback_mutate",
];
const RESOURCE_SEMANTIC_KINDS: [&str; 1] = ["directional_event_insert"];
const RESOURCE_KIND_UNIVERSE: [&str; 18] = [
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
];
const FIRST_EXPERIMENT_CONTRAST: &str = "resource_semantic_directional_event_insert";
const FIRST_EXPERIMENT_JUSTIFICATION: &str = "V38's recovered positive resource tail was directional_event_insert around two archive parents, not period/range/lookback/timeframe mutation. The first experiment therefore matches topology children against a directional_event_insert settling lane and an identical resource-semantic control. Parameter-only settling remains specified but is not the first contrast because parameter learning was sparsely sampled rather than demonstrated.";
const SETTLING_ALGORITHM: &str = "deterministic_matched_settling_v2";
const ROOT_KEYS: [&str; 23] = [
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
];
const PARENT_KEYS: [&str; 2] = ["candidateId", "role"];
const PANEL_KEYS: [&str; 3] = [
    "developmentPanelId",
    "confirmationPanelIds",
    "rotatingEvidenceSha256",
];
const PLAN_KEYS: [&str; 6] = [
    "planId",
    "operation",
    "operatorSchema",
    "schemaVersion",
    "arguments",
    "v38ExampleOperatorPlanSha256",
];
const CONTRAST_KEYS: [&str; 4] = [
    "contrastId",
    "settlingLane",
    "controlLane",
    "includedInFirstExperiment",
];
const SETTLING_KEYS: [&str; 5] = [
    "algorithmId",
    "parameterOnly",
    "resourceSemantic",
    "developmentPanelUse",
    "independentPanelConfirmation",
];
const LANE_KEYS: [&str; 8] = [
    "eligibleKinds",
    "forbiddenKinds",
    "maxSettlingPlans",
    "planSelection",
    "applicationMode",
    "evaluateIntermediatesOnDevelopmentPanel",
    "winnerSelection",
    "matchedControlBudget",
];
const SLOT_KEYS: [&str; 7] = [
    "cloneCountPerParent",
    "topologyOnlyCountPerParentPerPlan",
    "parameterOnlyControlCountPerParent",
    "resourceSemanticControlCountPerParent",
    "topologyThenParameterSettlingCountPerParentPerPlan",
    "topologyThenSemanticSettlingCountPerParentPerPlan",
    "firstExperimentSlotCount",
];
const SUCCESS_KEYS: [&str; 5] = [
    "noveltyIsNotQuality",
    "requireRepeatablePositiveParentRelativeTail",
    "forbidSystematicallyWorseWorstWindow",
    "requireIndependentPanelSurvival",
    "doNotPromoteOnDevelopmentPanelAlone",
];

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum CoadaptationError {
    #[error("{0}")]
    Contract(String),
}

pub type Result<T> = std::result::Result<T, CoadaptationError>;

fn contract(message: impl Into<String>) -> CoadaptationError {
    CoadaptationError::Contract(message.into())
}

fn unexpected(label: &str) -> CoadaptationError {
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

fn forbidden_for(eligible: &[&str]) -> Vec<&'static str> {
    RESOURCE_KIND_UNIVERSE
        .iter()
        .copied()
        .filter(|kind| !eligible.contains(kind))
        .collect()
}

fn validate_parents(value: &Value) -> Result<usize> {
    let rows = value
        .as_array()
        .ok_or_else(|| contract("topology coadaptation requires frozen parents"))?;
    if rows.is_empty() {
        return Err(contract("topology coadaptation requires frozen parents"));
    }
    let mut seen = BTreeSet::new();
    let mut has_archive = false;
    for item in rows {
        let fields = object(item, "topology coadaptation parent")?;
        exact_keys(fields, &PARENT_KEYS, "topology coadaptation parent")?;
        let candidate_id = text(fields, "candidateId", "topology coadaptation parent")?;
        if candidate_id.trim().is_empty() {
            return Err(unexpected("topology coadaptation parent"));
        }
        let role = text(fields, "role", "topology coadaptation parent")?;
        if role != "archive" && role != "inactive_control" && role != "active_negative_control" {
            return Err(contract("topology coadaptation parent role is invalid"));
        }
        if !seen.insert(candidate_id.to_owned()) {
            return Err(contract(format!(
                "topology coadaptation repeats parent {candidate_id}"
            )));
        }
        if role == "archive" {
            has_archive = true;
        }
    }
    if !has_archive {
        return Err(contract(
            "topology coadaptation requires at least one archive parent",
        ));
    }
    Ok(rows.len())
}

fn validate_panels(value: &Value) -> Result<()> {
    let fields = object(value, "topology coadaptation panelIdentities")?;
    exact_keys(fields, &PANEL_KEYS, "topology coadaptation panelIdentities")?;
    require_text(
        fields,
        "developmentPanelId",
        "panel-3",
        "topology coadaptation panelIdentities",
    )?;
    require_str_list(
        fields
            .get("confirmationPanelIds")
            .ok_or_else(|| unexpected("topology coadaptation panelIdentities"))?,
        &["panel-1", "panel-2"],
        "topology coadaptation panelIdentities",
    )?;
    require_sha(
        fields
            .get("rotatingEvidenceSha256")
            .ok_or_else(|| unexpected("topology coadaptation panelIdentities"))?,
        "topology coadaptation panelIdentities",
    )?;
    Ok(())
}

fn validate_arguments(operation: &str, value: &Value) -> Result<()> {
    let fields = object(value, "topology coadaptation topology plan arguments")?;
    let required: &[&str] = match operation {
        "insert_setup" => &["edgeId", "guard", "kind"],
        "insert_exit_region" => &["guard", "kind", "priority"],
        _ => {
            return Err(contract(
                "topology coadaptation topology plan operation drifted",
            ))
        }
    };
    exact_keys(
        fields,
        required,
        "topology coadaptation topology plan arguments",
    )
}

fn validate_plans(value: &Value) -> Result<usize> {
    let rows = value
        .as_array()
        .ok_or_else(|| contract("topology coadaptation requires exact topology plans"))?;
    if rows.is_empty() {
        return Err(contract(
            "topology coadaptation requires exact topology plans",
        ));
    }
    let mut seen_ids = BTreeSet::new();
    let mut seen_ops = BTreeSet::new();
    for item in rows {
        let fields = object(item, "topology coadaptation topology plan")?;
        exact_keys(fields, &PLAN_KEYS, "topology coadaptation topology plan")?;
        let operation = text(fields, "operation", "topology coadaptation topology plan")?;
        if operation != "insert_setup" && operation != "insert_exit_region" {
            return Err(contract(
                "topology coadaptation topology plan operation drifted",
            ));
        }
        let plan_id = text(fields, "planId", "topology coadaptation topology plan")?;
        if plan_id.trim().is_empty() || !seen_ids.insert(plan_id.to_owned()) {
            return Err(unexpected("topology coadaptation topology plan"));
        }
        if !seen_ops.insert(operation.to_owned()) {
            return Err(contract(
                "topology coadaptation mixed topology operations",
            ));
        }
        require_text(
            fields,
            "operatorSchema",
            "evolvable_module_topology_operator_v1",
            "topology coadaptation topology plan",
        )?;
        require_text(
            fields,
            "schemaVersion",
            "evolvable_module_topology_plan_v1",
            "topology coadaptation topology plan",
        )?;
        validate_arguments(
            operation,
            fields
                .get("arguments")
                .ok_or_else(|| unexpected("topology coadaptation topology plan"))?,
        )?;
        require_sha(
            fields
                .get("v38ExampleOperatorPlanSha256")
                .ok_or_else(|| unexpected("topology coadaptation topology plan"))?,
            "topology coadaptation topology plan",
        )?;
    }
    if !(seen_ops.contains("insert_setup") && seen_ops.contains("insert_exit_region")) {
        return Err(contract("topology coadaptation topology plans drifted"));
    }
    Ok(rows.len())
}

fn validate_contrasts(value: &Value) -> Result<()> {
    let rows = value
        .as_array()
        .ok_or_else(|| contract("topology coadaptation contrasts drifted"))?;
    if rows.len() != 2 {
        return Err(contract("topology coadaptation contrasts drifted"));
    }
    let expected = [
        (
            "topology_plus_parameter_only_vs_parameter_only_control",
            "parameter_only",
            "parameter_only",
            false,
        ),
        (
            "topology_plus_resource_semantic_vs_resource_semantic_control",
            "resource_semantic",
            "resource_semantic",
            true,
        ),
    ];
    for (item, (id, settling, control, included)) in rows.iter().zip(expected) {
        let fields = object(item, "topology coadaptation contrast")?;
        exact_keys(fields, &CONTRAST_KEYS, "topology coadaptation contrast")?;
        require_text(
            fields,
            "contrastId",
            id,
            "topology coadaptation contrasts",
        )?;
        require_text(
            fields,
            "settlingLane",
            settling,
            "topology coadaptation contrasts",
        )?;
        require_text(
            fields,
            "controlLane",
            control,
            "topology coadaptation contrasts",
        )?;
        require_bool(
            fields,
            "includedInFirstExperiment",
            included,
            "topology coadaptation contrasts",
        )?;
    }
    Ok(())
}

fn validate_lane(value: &Value, eligible: &[&str], label: &str) -> Result<()> {
    let fields = object(value, label)?;
    exact_keys(fields, &LANE_KEYS, label)?;
    require_str_list(
        fields
            .get("eligibleKinds")
            .ok_or_else(|| unexpected(label))?,
        eligible,
        label,
    )?;
    let forbidden = forbidden_for(eligible);
    require_str_list(
        fields
            .get("forbiddenKinds")
            .ok_or_else(|| unexpected(label))?,
        &forbidden,
        label,
    )?;
    if eligible == PARAMETER_ONLY_KINDS.as_slice()
        && fields
            .get("eligibleKinds")
            .and_then(Value::as_array)
            .is_some_and(|kinds| {
                kinds
                    .iter()
                    .any(|kind| kind.as_str() == Some("directional_event_insert"))
            })
    {
        return Err(contract(
            "parameter-only lane cannot admit directional_event_insert",
        ));
    }
    if eligible == PARAMETER_ONLY_KINDS.as_slice()
        && fields
            .get("eligibleKinds")
            .and_then(Value::as_array)
            .is_some_and(|kinds| {
                kinds.iter().any(|kind| {
                    kind.as_str()
                        .is_some_and(|name| !PARAMETER_ONLY_KINDS.contains(&name))
                })
            })
    {
        return Err(contract(
            "parameter-only lane cannot admit broad resource kinds",
        ));
    }
    require_u64(fields, "maxSettlingPlans", 1, label)?;
    require_text(
        fields,
        "planSelection",
        "lexicographic_canonical_construction_identity",
        label,
    )?;
    require_text(
        fields,
        "applicationMode",
        "sequential_single_step_from_pre_settling_genome",
        label,
    )?;
    require_bool(fields, "evaluateIntermediatesOnDevelopmentPanel", true, label)?;
    require_text(
        fields,
        "winnerSelection",
        "only_candidate_when_maxSettlingPlans_is_1",
        label,
    )?;
    require_text(
        fields,
        "matchedControlBudget",
        "identical_eligible_kind_set_order_and_maxSettlingPlans",
        label,
    )?;
    Ok(())
}

fn validate_settling(value: &Value) -> Result<()> {
    let fields = object(value, "topology coadaptation settling")?;
    exact_keys(fields, &SETTLING_KEYS, "topology coadaptation settling")?;
    require_text(
        fields,
        "algorithmId",
        SETTLING_ALGORITHM,
        "topology coadaptation settling",
    )?;
    require_text(
        fields,
        "developmentPanelUse",
        "frozen_v38_development_panel_only",
        "topology coadaptation settling",
    )?;
    require_text(
        fields,
        "independentPanelConfirmation",
        "required_before_any_production_conclusion",
        "topology coadaptation settling",
    )?;
    validate_lane(
        fields
            .get("parameterOnly")
            .ok_or_else(|| unexpected("topology coadaptation settling"))?,
        &PARAMETER_ONLY_KINDS,
        "topology coadaptation parameterOnly",
    )?;
    validate_lane(
        fields
            .get("resourceSemantic")
            .ok_or_else(|| unexpected("topology coadaptation settling"))?,
        &RESOURCE_SEMANTIC_KINDS,
        "topology coadaptation resourceSemantic",
    )?;
    Ok(())
}

fn validate_slot_budget(value: &Value, parent_count: usize, plan_count: usize) -> Result<()> {
    let fields = object(value, "topology coadaptation slotBudget")?;
    exact_keys(fields, &SLOT_KEYS, "topology coadaptation slotBudget")?;
    require_u64(fields, "cloneCountPerParent", 1, "topology coadaptation slotBudget")?;
    require_u64(
        fields,
        "topologyOnlyCountPerParentPerPlan",
        1,
        "topology coadaptation slotBudget",
    )?;
    require_u64(
        fields,
        "parameterOnlyControlCountPerParent",
        1,
        "topology coadaptation slotBudget",
    )?;
    require_u64(
        fields,
        "resourceSemanticControlCountPerParent",
        1,
        "topology coadaptation slotBudget",
    )?;
    require_u64(
        fields,
        "topologyThenParameterSettlingCountPerParentPerPlan",
        1,
        "topology coadaptation slotBudget",
    )?;
    require_u64(
        fields,
        "topologyThenSemanticSettlingCountPerParentPerPlan",
        1,
        "topology coadaptation slotBudget",
    )?;
    let expected = u64::try_from(parent_count * (1 + plan_count + 1 + plan_count))
        .map_err(|_| contract("topology coadaptation slotBudget drifted"))?;
    require_u64(
        fields,
        "firstExperimentSlotCount",
        expected,
        "topology coadaptation slotBudget",
    )?;
    Ok(())
}

fn validate_success(value: &Value) -> Result<()> {
    let fields = object(value, "topology coadaptation successRule")?;
    exact_keys(fields, &SUCCESS_KEYS, "topology coadaptation successRule")?;
    for key in SUCCESS_KEYS {
        require_bool(fields, key, true, "topology coadaptation successRule")?;
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

pub fn validate(value: &Value) -> Result<()> {
    let fields = object(value, "topology coadaptation")?;
    exact_keys(fields, &ROOT_KEYS, "topology coadaptation")?;
    require_text(
        fields,
        "schemaVersion",
        COADAPTATION_SCHEMA,
        "topology coadaptation schema",
    )?;
    require_text(
        fields,
        "mode",
        COADAPTATION_MODE,
        "topology coadaptation mode",
    )?;
    require_bool(
        fields,
        "includeCrossover",
        false,
        "topology coadaptation includeCrossover",
    )?;
    require_text(
        fields,
        "cloneControl",
        CLONE_CONTROL,
        "topology coadaptation cloneControl",
    )?;
    if fields.get("productionArchiveWrite") != Some(&Value::Bool(false)) {
        return Err(contract(
            "topology coadaptation must not write the production archive",
        ));
    }
    require_u64(
        fields,
        "mutationDepth",
        1,
        "topology coadaptation mutationDepth",
    )?;
    require_bool(
        fields,
        "morphologyNurseryDeferred",
        true,
        "topology coadaptation morphologyNurseryDeferred",
    )?;
    require_bool(
        fields,
        "noMixedTopologyOperations",
        true,
        "topology coadaptation noMixedTopologyOperations",
    )?;
    require_bool(
        fields,
        "preserveRawAndSettledIdentities",
        true,
        "topology coadaptation preserveRawAndSettledIdentities",
    )?;
    require_bool(
        fields,
        "independentConfirmationRequired",
        true,
        "topology coadaptation independentConfirmationRequired",
    )?;
    require_text(
        fields,
        "firstExperimentContrast",
        FIRST_EXPERIMENT_CONTRAST,
        "topology coadaptation firstExperimentContrast",
    )?;
    require_text(
        fields,
        "firstExperimentJustification",
        FIRST_EXPERIMENT_JUSTIFICATION,
        "topology coadaptation firstExperimentJustification",
    )?;
    require_str_list(
        fields
            .get("arms")
            .ok_or_else(|| unexpected("topology coadaptation"))?,
        &ARMS,
        "topology coadaptation arms",
    )?;
    require_str_list(
        fields
            .get("firstExperimentArms")
            .ok_or_else(|| unexpected("topology coadaptation"))?,
        &FIRST_EXPERIMENT_ARMS,
        "topology coadaptation firstExperimentArms",
    )?;
    require_bool(
        fields,
        "notAdmittedOnFrontGenerationPath",
        true,
        "topology coadaptation notAdmittedOnFrontGenerationPath",
    )?;
    let parent_count = validate_parents(
        fields
            .get("parents")
            .ok_or_else(|| contract("topology coadaptation requires frozen parents"))?,
    )?;
    validate_panels(
        fields
            .get("panelIdentities")
            .ok_or_else(|| unexpected("topology coadaptation"))?,
    )?;
    let plan_count = validate_plans(
        fields
            .get("topologyPlans")
            .ok_or_else(|| contract("topology coadaptation requires exact topology plans"))?,
    )?;
    validate_contrasts(
        fields
            .get("contrasts")
            .ok_or_else(|| unexpected("topology coadaptation"))?,
    )?;
    validate_settling(
        fields
            .get("settling")
            .ok_or_else(|| unexpected("topology coadaptation"))?,
    )?;
    validate_slot_budget(
        fields
            .get("slotBudget")
            .ok_or_else(|| unexpected("topology coadaptation"))?,
        parent_count,
        plan_count,
    )?;
    validate_success(
        fields
            .get("successRule")
            .ok_or_else(|| unexpected("topology coadaptation"))?,
    )?;
    let claimed = require_sha(
        fields
            .get("contractSha256")
            .ok_or_else(|| unexpected("topology coadaptation"))?,
        "topology coadaptation",
    )?;
    let mut without_hash = value.clone();
    without_hash
        .as_object_mut()
        .ok_or_else(|| unexpected("topology coadaptation"))?
        .remove("contractSha256");
    let expected = canonical_sha256(&without_hash).map_err(|error| contract(error.to_string()))?;
    if claimed != expected {
        return Err(contract("topology coadaptation identity drift"));
    }
    Ok(())
}
