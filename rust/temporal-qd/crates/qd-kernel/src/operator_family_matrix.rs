//! Frozen-parent × operator-family matrix experiment contract.
//!
//! Production rotating 4/5 breeding does not use this module.  A generation
//! activates it only when `operatorFamilyMatrix` is present on the sealed
//! pair-generation config.

use temporal_qd_contract::{Map, Value};

pub const MATRIX_SCHEMA: &str = "temporal_qd_operator_family_matrix_v1";
pub const MATRIX_MODE: &str = "frozen_parent_one_change_v1";
pub const CLONE_CONTROL: &str = "re_evaluate_parent_on_frozen_panel";
pub const MATRIX_FAMILIES: [&str; 5] = [
    "hold",
    "resource",
    "topology",
    "temporal",
    "initial_protection",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatrixParent {
    pub candidate_id: String,
    pub role: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorFamilyMatrixContract {
    pub children_per_family: u64,
    pub families: Vec<String>,
    pub parents: Vec<MatrixParent>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatrixConstructionSlot {
    pub proposal_ordinal: u64,
    pub parent_candidate_id: String,
    pub parent_role: String,
    pub operator_family: String,
    pub child_index: u64,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum MatrixError {
    #[error("{0}")]
    Contract(String),
}

pub type Result<T> = std::result::Result<T, MatrixError>;

fn contract(message: impl Into<String>) -> MatrixError {
    MatrixError::Contract(message.into())
}

pub fn is_matrix_family(family: &str) -> bool {
    MATRIX_FAMILIES.contains(&family)
}

impl OperatorFamilyMatrixContract {
    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object(value, "operator-family matrix")?;
        if text(fields, "schemaVersion")? != MATRIX_SCHEMA {
            return Err(contract("operator-family matrix schema is incompatible"));
        }
        if text(fields, "mode")? != MATRIX_MODE {
            return Err(contract("operator-family matrix mode is incompatible"));
        }
        if fields.get("includeCrossover") != Some(&Value::Bool(false)) {
            return Err(contract(
                "operator-family matrix must keep crossover on a separate lane",
            ));
        }
        if text(fields, "cloneControl")? != CLONE_CONTROL {
            return Err(contract(
                "operator-family matrix clone control must re-evaluate parents on the frozen panel",
            ));
        }
        if u64_field(fields, "mutationDepth")? != 1 {
            return Err(contract(
                "operator-family matrix mutation depth must be exactly 1",
            ));
        }
        let children_per_family = u64_field(fields, "childrenPerFamily")?;
        if children_per_family == 0 {
            return Err(contract(
                "operator-family matrix childrenPerFamily must be a positive integer",
            ));
        }
        let families = string_array(fields, "families")?;
        if families.is_empty() {
            return Err(contract(
                "operator-family matrix families must be a nonempty list",
            ));
        }
        let mut seen_families = std::collections::BTreeSet::new();
        for family in &families {
            if !is_matrix_family(family) {
                return Err(contract(
                    "operator-family matrix families contain an unsupported family",
                ));
            }
            if !seen_families.insert(family.clone()) {
                return Err(contract("operator-family matrix families must be unique"));
            }
        }
        let parents_value = fields
            .get("parents")
            .and_then(Value::as_array)
            .ok_or_else(|| contract("operator-family matrix parents must be a nonempty list"))?;
        if parents_value.is_empty() {
            return Err(contract(
                "operator-family matrix parents must be a nonempty list",
            ));
        }
        let mut parents = Vec::new();
        let mut seen_ids = std::collections::BTreeSet::new();
        let mut has_archive = false;
        for parent in parents_value {
            let parent_fields = object(parent, "operator-family matrix parent")?;
            let candidate_id = text(parent_fields, "candidateId")?;
            if candidate_id.trim().is_empty() {
                return Err(contract(
                    "operator-family matrix parent candidateId is invalid",
                ));
            }
            let role = text(parent_fields, "role")?;
            if role != "archive"
                && role != "inactive_control"
                && role != "active_negative_control"
            {
                return Err(contract("operator-family matrix parent role is invalid"));
            }
            if !seen_ids.insert(candidate_id.clone()) {
                return Err(contract(format!(
                    "operator-family matrix repeats parent {candidate_id}"
                )));
            }
            if role == "archive" {
                has_archive = true;
            }
            parents.push(MatrixParent {
                candidate_id,
                role,
            });
        }
        if !has_archive {
            return Err(contract(
                "operator-family matrix requires at least one archive parent",
            ));
        }
        Ok(Self {
            children_per_family,
            families,
            parents,
        })
    }

    pub fn from_generation_config(config: &Value) -> Result<Option<Self>> {
        let fields = object(config, "generation config")?;
        match fields.get("operatorFamilyMatrix") {
            None | Some(Value::Null) => Ok(None),
            Some(value) => Ok(Some(Self::from_value(value)?)),
        }
    }

    pub fn construction_slot_count(&self) -> Result<u64> {
        let families = u64::try_from(self.families.len())
            .map_err(|_| contract("operator-family matrix family count overflowed"))?;
        let parents = u64::try_from(self.parents.len())
            .map_err(|_| contract("operator-family matrix parent count overflowed"))?;
        parents
            .checked_mul(families)
            .and_then(|value| value.checked_mul(self.children_per_family))
            .ok_or_else(|| contract("operator-family matrix slot count overflowed"))
    }

    /// Publication is allowed after every construction slot is attempted once.
    /// Rejects and no-ops stay on their slot; accepted children may be fewer
    /// than the grid. This is not a retry-until-N-accepted fill.
    pub fn require_exhausted_slot_grid(
        &self,
        attempt_count: u64,
        accepted_count: u64,
        target_accepted: u64,
        max_attempts: u64,
    ) -> Result<()> {
        let slots = self.construction_slot_count()?;
        if target_accepted != slots || max_attempts != slots {
            return Err(contract(
                "operator-family matrix must bind targetAccepted and maxAttempts to the slot grid",
            ));
        }
        if attempt_count != slots {
            return Err(contract(
                "operator-family matrix must attempt every construction slot before publication",
            ));
        }
        if accepted_count > slots {
            return Err(contract(
                "operator-family matrix accepted more children than construction slots",
            ));
        }
        Ok(())
    }

    pub fn slot_at(&self, ordinal: u64) -> Result<Option<MatrixConstructionSlot>> {
        let total = self.construction_slot_count()?;
        if ordinal >= total {
            return Ok(None);
        }
        let family_count = u64::try_from(self.families.len())
            .map_err(|_| contract("operator-family matrix family count overflowed"))?;
        let per_parent = family_count
            .checked_mul(self.children_per_family)
            .ok_or_else(|| contract("operator-family matrix slot count overflowed"))?;
        let parent_index = usize::try_from(ordinal / per_parent)
            .map_err(|_| contract("operator-family matrix parent index overflowed"))?;
        let parent_offset = ordinal % per_parent;
        let family_index = usize::try_from(parent_offset / self.children_per_family)
            .map_err(|_| contract("operator-family matrix family index overflowed"))?;
        let child_index = parent_offset % self.children_per_family;
        let parent = &self.parents[parent_index];
        Ok(Some(MatrixConstructionSlot {
            proposal_ordinal: ordinal,
            parent_candidate_id: parent.candidate_id.clone(),
            parent_role: parent.role.clone(),
            operator_family: self.families[family_index].clone(),
            child_index,
        }))
    }
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| contract(format!("{label} must be an object")))
}

fn text(fields: &Map<String, Value>, key: &str) -> Result<String> {
    fields
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| contract(format!("operator-family matrix {key} is invalid")))
}

fn u64_field(fields: &Map<String, Value>, key: &str) -> Result<u64> {
    fields
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| contract(format!("operator-family matrix {key} is invalid")))
}

fn string_array(fields: &Map<String, Value>, key: &str) -> Result<Vec<String>> {
    let values = fields
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| contract(format!("operator-family matrix {key} is invalid")))?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| contract(format!("operator-family matrix {key} is invalid")))
        })
        .collect()
}
