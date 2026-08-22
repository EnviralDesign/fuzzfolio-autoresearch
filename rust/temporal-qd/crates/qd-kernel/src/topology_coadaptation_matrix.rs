//! Experiment-only topology co-adaptation overlay.
//!
//! Production rotating 4/5 breeding omits `topologyCoadaptationMatrix`.
//! Parsing this contract never schedules market work.

use temporal_qd_contract::{Map, Value};

pub const COADAPTATION_SCHEMA: &str = "temporal_qd_topology_coadaptation_matrix_v1";
pub const COADAPTATION_MODE: &str = "frozen_parent_topology_then_local_resource_settling_v1";
pub const CLONE_CONTROL: &str = "re_evaluate_parent_on_frozen_panel";
pub const NURSERY_SCHEMA: &str = "temporal_qd_morphology_nursery_archive_v1";
pub const ARMS: [&str; 4] = [
    "exact_parent_clone",
    "topology_only_child",
    "resource_parameter_only_control",
    "topology_then_bounded_resource_settling",
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
    let fields = value
        .as_object()
        .ok_or_else(|| contract("topology coadaptation must be an object"))?;
    if text(fields, "schemaVersion")? != COADAPTATION_SCHEMA {
        return Err(contract("topology coadaptation schema is incompatible"));
    }
    if text(fields, "mode")? != COADAPTATION_MODE {
        return Err(contract("topology coadaptation mode is incompatible"));
    }
    if fields.get("includeCrossover") != Some(&Value::Bool(false)) {
        return Err(contract(
            "topology coadaptation must keep crossover on a separate lane",
        ));
    }
    if text(fields, "cloneControl")? != CLONE_CONTROL {
        return Err(contract(
            "topology coadaptation clone control must re-evaluate parents on the frozen panel",
        ));
    }
    if fields.get("productionArchiveWrite") != Some(&Value::Bool(false)) {
        return Err(contract(
            "topology coadaptation must not write the production archive",
        ));
    }
    let arms = fields
        .get("arms")
        .and_then(Value::as_array)
        .ok_or_else(|| contract("topology coadaptation arms are required"))?;
    let got: Vec<&str> = arms.iter().filter_map(Value::as_str).collect();
    if got != ARMS {
        return Err(contract("topology coadaptation arms drifted"));
    }
    let settling = fields
        .get("settling")
        .and_then(Value::as_object)
        .ok_or_else(|| contract("topology coadaptation requires a bounded settling budget"))?;
    if settling.get("families") != Some(&Value::Array(vec![Value::String("resource".to_owned())]))
    {
        return Err(contract("settling may only use the resource family"));
    }
    let nursery = fields
        .get("morphologyNursery")
        .and_then(Value::as_object)
        .ok_or_else(|| contract("topology coadaptation requires a morphology nursery sidecar"))?;
    if text(nursery, "schemaVersion")? != NURSERY_SCHEMA {
        return Err(contract("morphology nursery schema is incompatible"));
    }
    if nursery.get("productionBreedingRights") != Some(&Value::Bool(false)) {
        return Err(contract(
            "nursery members must not receive production breeding rights",
        ));
    }
    Ok(())
}

fn text<'a>(fields: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    fields
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| contract(format!("topology coadaptation {key} is required")))
}
