//! Write-neutral public-artifact streaming for a sealed native v5 G0 result.
//!
//! This module deliberately owns semantic publication inputs and canonical
//! streams, but never opens a path.  `qd-batch` supplies its private temporary
//! files, fsync/link ordering, and inventory.  Rich selected candidates are
//! reconstructed one at a time from compact records; the construction pool is
//! never expanded into a `Vec` of legacy candidates.

use std::{
    collections::BTreeMap,
    io::{Read, Write},
    thread,
};

use temporal_qd_contract::{
    CanonicalSha256Writer, ContractError, Map, NativeProgressHandle, Value, canonical_sha256,
    canonical_sha256_without_object_field, write_canonical_json,
};

use crate::{
    factory::ParentReference,
    g0_funnel::{reproduction_allocation_accounting, validate_reproduction_allocation},
    journal::AcceptedReference,
    publication::{PublicationPolicy, PublicationRequest},
    v5::V5SharedConstructionAuthority,
    v5_transaction::{
        V5G0TransactionError, V5G0TransactionRequest, V5G0TransactionResult,
        V5SelectedG0Materialization, compact_record_object_relative_path,
        materialize_selected_v5_g0_record, verify_v5_g0_transaction_replay_with_authority,
    },
};

/// Optional one-pass sink for the compiler-owned parent references that a
/// fast-ephemeral successor needs. Durable publication uses the no-op sink;
/// the fast path writes one compact JSONL stream alongside its evaluation
/// population without reconstructing selected candidates twice.
pub trait V5G0ParentReferenceSink {
    fn write_parent_reference(&mut self, reference: &ParentReference) -> std::io::Result<()>;
}

struct NoopV5G0ParentReferenceSink;

impl V5G0ParentReferenceSink for NoopV5G0ParentReferenceSink {
    fn write_parent_reference(&mut self, _reference: &ParentReference) -> std::io::Result<()> {
        Ok(())
    }
}

pub const V5_G0_PUBLICATION_PLAN_SCHEMA: &str = "temporal_qd_v5_g0_publication_plan_v1";
pub const V5_G0_PUBLICATION_REQUEST_SCHEMA: &str = "temporal_qd_v5_g0_publication_request_v1";
pub const V5_G0_PUBLICATION_RECEIPT_SCHEMA: &str =
    "temporal_qd_v5_g0_publication_stream_receipt_v1";

#[derive(Debug, thiserror::Error)]
pub enum V5G0PublicationError {
    #[error("v5 transaction failure: {0}")]
    Transaction(#[from] V5G0TransactionError),
    #[error("canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("v5 G0 publication stream I/O failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("v5 G0 publication plan failure: {0}")]
    Contract(String),
}

pub type Result<T> = std::result::Result<T, V5G0PublicationError>;

fn contract(message: impl Into<String>) -> V5G0PublicationError {
    V5G0PublicationError::Contract(message.into())
}

fn object(rows: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut fields = Map::new();
    for (key, value) in rows {
        fields.insert(key.to_owned(), value);
    }
    Value::Object(fields)
}

fn object_ref<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| contract(format!("{label} must be an object")))
}

fn required<'a>(value: &'a Value, key: &str, label: &str) -> Result<&'a Value> {
    object_ref(value, label)?
        .get(key)
        .ok_or_else(|| contract(format!("{label} lacks {key}")))
}

fn exact_keys(fields: &Map<String, Value>, keys: &[&str], label: &str) -> Result<()> {
    if fields.len() != keys.len() || keys.iter().any(|key| !fields.contains_key(*key)) {
        return Err(contract(format!("{label} fields are not exact")));
    }
    Ok(())
}

fn exact_text(value: &Value, label: &str) -> Result<String> {
    let text = value
        .as_str()
        .filter(|text| !text.trim().is_empty() && *text == text.trim())
        .ok_or_else(|| contract(format!("{label} must be a canonical nonempty string")))?;
    Ok(text.to_owned())
}

fn exact_sha(value: &Value, label: &str) -> Result<String> {
    let value = exact_text(value, label)?;
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value.as_bytes()[7..]
            .iter()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(contract(format!(
            "{label} must be a lowercase SHA-256 identity"
        )));
    }
    Ok(value)
}

fn count_map_value(counts: &BTreeMap<String, u64>) -> Value {
    Value::Object(
        counts
            .iter()
            .map(|(key, value)| (key.clone(), Value::from(*value)))
            .collect(),
    )
}

fn sha_from_field(value: &Value, field: &str, label: &str) -> Result<String> {
    exact_sha(required(value, field, label)?, &format!("{label} {field}"))
}

/// The narrow raw manifest inputs which the kernel needs in addition to the
/// authenticated generation config and frozen construction authority.  This
/// is intentionally *not* self-hashed and is never accepted as a publication
/// plan: `V5G0PublicationPlan::derive` owns construction and hashing of the
/// durable plan after validating these copied raw values.
#[derive(Clone, Debug, PartialEq)]
pub struct V5G0PublicationInputs {
    pub final_newline: String,
    pub execution_authority: Value,
    pub inputs: Value,
}

impl V5G0PublicationInputs {
    pub fn from_manifest_values(
        final_newline: &Value,
        execution_authority: &Value,
        inputs: &Value,
    ) -> Result<Self> {
        let value = Self {
            final_newline: exact_text(final_newline, "v5 G0 publication final newline")?,
            execution_authority: execution_authority.clone(),
            inputs: inputs.clone(),
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<()> {
        if self.final_newline != "lf" || !self.execution_authority.is_object() {
            return Err(contract("v5 G0 publication manifest inputs are invalid"));
        }
        let inputs = object_ref(&self.inputs, "v5 G0 publication inputs")?;
        exact_keys(
            inputs,
            &["schemaVersion", "parentArchive", "identityLedger"],
            "v5 G0 publication inputs",
        )?;
        if inputs.get("schemaVersion").and_then(Value::as_str)
            != Some("temporal_qd_native_v5_proposal_inputs_v1")
            || !inputs.get("parentArchive").is_some_and(Value::is_null)
            || !inputs.get("identityLedger").is_some_and(Value::is_null)
        {
            return Err(contract(
                "v5 G0 publication inputs must be the exact empty G0 inputs",
            ));
        }
        Ok(())
    }
}

/// The cap-free, self-hashed authority that lets the native publication path
/// build public artifacts without borrowing the outer invocation manifest.
///
/// `threadCap`, output paths, timeouts, and other control-plane facts are
/// deliberately absent.  The whole frozen authority/config closure is kept
/// verbatim so batch does not infer a publication policy, archive authority,
/// operator implementation, or reproduction allocation on the kernel's
/// behalf.
#[derive(Clone, Debug, PartialEq)]
pub struct V5G0PublicationPlan {
    pub frozen_authority: Value,
    pub generation_config: Value,
    pub generation_config_sha256: String,
    pub generation_index: u64,
    pub target_unique_candidates: u64,
    pub max_proposal_attempts: u64,
    pub evaluation_population_size: u64,
    pub final_newline: String,
    /// Opaque but self-authenticated by this plan.  qd-batch separately
    /// validates its native-batch schema; core includes it so the semantic
    /// request is closed over the exact dispatched authority without taking
    /// ownership of process execution.
    pub execution_authority: Value,
    /// The existing v5 manifest `inputs` object.  For G0 it is exact and both
    /// child values are null; keeping it here prevents a later restart from
    /// silently substituting an initial archive/ledger authority.
    pub inputs: Value,
}

impl V5G0PublicationPlan {
    /// Construct the only accepted plan shape from authenticated transaction
    /// inputs.  Python/batch may transport `V5G0PublicationInputs`, but it
    /// cannot author a self-hashed publication plan or select its identity.
    pub(crate) fn derive(
        request: &V5G0TransactionRequest,
        authority: &V5SharedConstructionAuthority,
    ) -> Result<Self> {
        request.publication_inputs.validate()?;
        let plan = Self {
            frozen_authority: request.shared_authority.clone(),
            generation_config: request.generation_config.clone(),
            generation_config_sha256: request.generation_config_sha256.clone(),
            generation_index: request.generation_index,
            target_unique_candidates: request.target_accepted,
            max_proposal_attempts: request.max_attempts,
            evaluation_population_size: request.evaluation_width,
            final_newline: request.publication_inputs.final_newline.clone(),
            execution_authority: request.publication_inputs.execution_authority.clone(),
            inputs: request.publication_inputs.inputs.clone(),
        };
        plan.validate_shape()?;
        let frozen_sha = exact_sha(
            required(
                &plan.frozen_authority,
                "authoritySha256",
                "v5 G0 publication frozen authority",
            )?,
            "v5 G0 publication frozen authority SHA-256",
        )?;
        if frozen_sha != authority.shared_authority_sha256 {
            return Err(contract("derived v5 G0 publication plan authority drifted"));
        }
        let _ = publication_policy_from_authority(&plan, authority)?;
        Ok(plan)
    }

    fn semantic_value(&self) -> Result<Value> {
        self.validate_shape()?;
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_G0_PUBLICATION_PLAN_SCHEMA.to_owned()),
            ),
            ("frozenAuthority", self.frozen_authority.clone()),
            ("generationConfig", self.generation_config.clone()),
            (
                "generationConfigSha256",
                Value::String(self.generation_config_sha256.clone()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            (
                "targetUniqueCandidates",
                Value::from(self.target_unique_candidates),
            ),
            (
                "maxProposalAttempts",
                Value::from(self.max_proposal_attempts),
            ),
            (
                "evaluationPopulationSize",
                Value::from(self.evaluation_population_size),
            ),
            ("finalNewline", Value::String(self.final_newline.clone())),
            ("executionAuthority", self.execution_authority.clone()),
            ("inputs", self.inputs.clone()),
        ]))
    }

    pub fn publication_plan_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 G0 publication plan")
            .clone();
        fields.insert(
            "publicationPlanSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 G0 publication plan")?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "frozenAuthority",
                "generationConfig",
                "generationConfigSha256",
                "generationIndex",
                "targetUniqueCandidates",
                "maxProposalAttempts",
                "evaluationPopulationSize",
                "finalNewline",
                "executionAuthority",
                "inputs",
                "publicationPlanSha256",
            ],
            "v5 G0 publication plan",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_G0_PUBLICATION_PLAN_SCHEMA)
        {
            return Err(contract("v5 G0 publication plan schema is invalid"));
        }
        let plan = Self {
            frozen_authority: required(value, "frozenAuthority", "v5 G0 publication plan")?.clone(),
            generation_config: required(value, "generationConfig", "v5 G0 publication plan")?
                .clone(),
            generation_config_sha256: exact_sha(
                required(value, "generationConfigSha256", "v5 G0 publication plan")?,
                "v5 G0 publication plan generation config SHA-256",
            )?,
            generation_index: required(value, "generationIndex", "v5 G0 publication plan")?
                .as_u64()
                .ok_or_else(|| contract("v5 G0 publication plan generation index is invalid"))?,
            target_unique_candidates: required(
                value,
                "targetUniqueCandidates",
                "v5 G0 publication plan",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 G0 publication plan target is invalid"))?,
            max_proposal_attempts: required(
                value,
                "maxProposalAttempts",
                "v5 G0 publication plan",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 G0 publication plan attempt ceiling is invalid"))?,
            evaluation_population_size: required(
                value,
                "evaluationPopulationSize",
                "v5 G0 publication plan",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 G0 publication plan evaluation width is invalid"))?,
            final_newline: exact_text(
                required(value, "finalNewline", "v5 G0 publication plan")?,
                "v5 G0 publication plan final newline",
            )?,
            execution_authority: required(value, "executionAuthority", "v5 G0 publication plan")?
                .clone(),
            inputs: required(value, "inputs", "v5 G0 publication plan")?.clone(),
        };
        let supplied = exact_sha(
            required(value, "publicationPlanSha256", "v5 G0 publication plan")?,
            "v5 G0 publication plan SHA-256",
        )?;
        if supplied != plan.publication_plan_sha256()? || &plan.to_value()? != value {
            return Err(contract("v5 G0 publication plan identity drifted"));
        }
        Ok(plan)
    }

    pub fn validate_shape(&self) -> Result<()> {
        exact_sha(
            &Value::String(self.generation_config_sha256.clone()),
            "v5 G0 publication plan generation config SHA-256",
        )?;
        if self.generation_index != 1
            || self.target_unique_candidates == 0
            || self.max_proposal_attempts < self.target_unique_candidates
            || self.evaluation_population_size == 0
            || self.evaluation_population_size > self.target_unique_candidates
            || self.final_newline != "lf"
        {
            return Err(contract(
                "v5 G0 publication plan dimensions/newline are invalid",
            ));
        }
        if !self.execution_authority.is_object() {
            return Err(contract(
                "v5 G0 publication execution authority must be an object",
            ));
        }
        let frozen = object_ref(&self.frozen_authority, "v5 G0 publication frozen authority")?;
        exact_keys(
            frozen,
            &["schemaVersion", "authority", "authoritySha256"],
            "v5 G0 publication frozen authority",
        )?;
        let authority = frozen
            .get("authority")
            .ok_or_else(|| contract("v5 G0 publication frozen authority lacks authority"))?;
        let authority_sha = exact_sha(
            frozen
                .get("authoritySha256")
                .ok_or_else(|| contract("v5 G0 publication frozen authority lacks identity"))?,
            "v5 G0 publication frozen authority SHA-256",
        )?;
        if canonical_sha256(authority)? != authority_sha {
            return Err(contract(
                "v5 G0 publication frozen authority identity drifted",
            ));
        }
        let config = object_ref(
            &self.generation_config,
            "v5 G0 publication generation config",
        )?;
        if config.get("schemaVersion").and_then(Value::as_str)
            != Some("temporal_qd_pair_generation_v2")
            || config.get("configSha256").and_then(Value::as_str)
                != Some(self.generation_config_sha256.as_str())
            || canonical_sha256_without_object_field(&self.generation_config, "configSha256")?
                != self.generation_config_sha256
            || config.get("generationIndex").and_then(Value::as_u64) != Some(self.generation_index)
            || config.get("targetUniqueCandidates").and_then(Value::as_u64)
                != Some(self.target_unique_candidates)
            || config.get("maxProposalAttempts").and_then(Value::as_u64)
                != Some(self.max_proposal_attempts)
        {
            return Err(contract(
                "v5 G0 publication generation config binding drifted",
            ));
        }
        let allocation = config
            .get("reproductionAllocation")
            .ok_or_else(|| contract("v5 G0 publication config lacks reproduction allocation"))?;
        validate_reproduction_allocation(allocation).map_err(|error| {
            contract(format!("v5 G0 publication allocation is invalid: {error}"))
        })?;
        V5G0PublicationInputs {
            final_newline: self.final_newline.clone(),
            execution_authority: self.execution_authority.clone(),
            inputs: self.inputs.clone(),
        }
        .validate()?;
        Ok(())
    }

    /// Validate this plan against the already-parsed construction authority.
    /// This keeps `execute_v5_g0_transaction` and durable replay at exactly
    /// one authority decode: the plan contains the same sealed envelope and
    /// is compared as a value rather than reopened.
    pub(crate) fn validate_against_request(
        &self,
        request: &V5G0TransactionRequest,
        authority: &V5SharedConstructionAuthority,
    ) -> Result<()> {
        let expected = Self::derive(request, authority)?;
        if self != &expected {
            return Err(contract(
                "v5 G0 publication plan does not bind transaction request",
            ));
        }
        Ok(())
    }

    /// Cap-free semantic request identity used by the v5 public publication
    /// artifacts.  It intentionally supersedes the unshipped bridge's
    /// `manifestSha256` request token, because that outer control-plane hash
    /// includes `threadCap` and therefore cannot identify byte-stable native
    /// candidate/publication semantics.
    pub fn publication_request_sha256(&self, shared_authority_sha256: &str) -> Result<String> {
        let plan_sha = self.publication_plan_sha256()?;
        let inputs = object_ref(&self.inputs, "v5 G0 publication inputs")?;
        Ok(canonical_sha256(&object([
            (
                "schemaVersion",
                Value::String(V5_G0_PUBLICATION_REQUEST_SCHEMA.to_owned()),
            ),
            ("publicationPlanSha256", Value::String(plan_sha)),
            (
                "sharedAuthoritySha256",
                Value::String(exact_sha(
                    &Value::String(shared_authority_sha256.to_owned()),
                    "v5 G0 publication request shared authority SHA-256",
                )?),
            ),
            (
                "generationConfigSha256",
                Value::String(self.generation_config_sha256.clone()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            (
                "targetUniqueCandidates",
                Value::from(self.target_unique_candidates),
            ),
            (
                "maxProposalAttempts",
                Value::from(self.max_proposal_attempts),
            ),
            (
                "evaluationPopulationSize",
                Value::from(self.evaluation_population_size),
            ),
            (
                "parentArchive",
                inputs
                    .get("parentArchive")
                    .cloned()
                    .ok_or_else(|| contract("v5 G0 publication inputs lack parent archive"))?,
            ),
            (
                "initialIdentityLedger",
                inputs
                    .get("identityLedger")
                    .cloned()
                    .ok_or_else(|| contract("v5 G0 publication inputs lack identity ledger"))?,
            ),
        ]))?)
    }

    pub fn object_binding(&self) -> Result<V5G0PublicationPlanObjectBinding> {
        let publication_plan_sha256 = self.publication_plan_sha256()?;
        Ok(V5G0PublicationPlanObjectBinding {
            relative_path: compact_record_object_relative_path(&publication_plan_sha256)?,
            publication_plan_sha256,
        })
    }
}

/// Immutable canonical object location for the plan.  It deliberately shares
/// the compact-record object-store namespace: each content-addressed value is
/// a real canonical JSON object, never a JSONL fragment pseudo-reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5G0PublicationPlanObjectBinding {
    pub publication_plan_sha256: String,
    pub relative_path: String,
}

impl V5G0PublicationPlanObjectBinding {
    pub fn to_value(&self) -> Result<Value> {
        let expected = compact_record_object_relative_path(&self.publication_plan_sha256)?;
        if self.relative_path != expected {
            return Err(contract("v5 G0 publication plan object path drifted"));
        }
        Ok(object([
            (
                "publicationPlanSha256",
                Value::String(exact_sha(
                    &Value::String(self.publication_plan_sha256.clone()),
                    "v5 G0 publication plan object SHA-256",
                )?),
            ),
            ("relativePath", Value::String(expected)),
        ]))
    }
}

fn publication_policy_from_authority(
    plan: &V5G0PublicationPlan,
    authority: &V5SharedConstructionAuthority,
) -> Result<PublicationPolicy> {
    let frozen_authority = required(
        &plan.frozen_authority,
        "authority",
        "v5 G0 publication frozen authority",
    )?;
    let authority_fields = object_ref(frozen_authority, "v5 G0 publication authority")?;
    let pair_policy = authority_fields
        .get("bidirectionalPairPolicy")
        .cloned()
        .ok_or_else(|| contract("v5 G0 publication authority lacks pair policy"))?;
    if canonical_sha256(&pair_policy)? != authority.pair_policy_sha256 {
        return Err(contract("v5 G0 publication pair policy identity drifted"));
    }
    let evolvable = authority_fields
        .get("evolvableModuleAuthority")
        .ok_or_else(|| contract("v5 G0 publication authority lacks evolvable module authority"))?;
    let archive_policy_authority = object_ref(evolvable, "v5 G0 publication evolvable authority")?
        .get("archivePolicyAuthority")
        .cloned()
        .ok_or_else(|| contract("v5 G0 publication authority lacks archive policy authority"))?;
    let archive = object_ref(&archive_policy_authority, "v5 G0 archive policy authority")?;
    exact_keys(
        archive,
        &["qdVersion", "policyName", "policySha256", "frozenPolicy"],
        "v5 G0 archive policy authority",
    )?;
    let qd_version = exact_text(
        archive
            .get("qdVersion")
            .ok_or_else(|| contract("v5 G0 archive policy authority lacks qd version"))?,
        "v5 G0 archive policy qd version",
    )?;
    let policy_name = exact_text(
        archive
            .get("policyName")
            .ok_or_else(|| contract("v5 G0 archive policy authority lacks policy name"))?,
        "v5 G0 archive policy name",
    )?;
    let policy_sha256 = exact_sha(
        archive
            .get("policySha256")
            .ok_or_else(|| contract("v5 G0 archive policy authority lacks policy SHA-256"))?,
        "v5 G0 archive policy SHA-256",
    )?;
    let frozen_policy = archive
        .get("frozenPolicy")
        .ok_or_else(|| contract("v5 G0 archive policy authority lacks frozen policy"))?;
    if canonical_sha256(frozen_policy)? != policy_sha256
        || frozen_policy.get("policyName").and_then(Value::as_str) != Some(policy_name.as_str())
    {
        return Err(contract("v5 G0 archive policy authority identity drifted"));
    }
    // The historical source operator is audit evidence only.  Fresh v5
    // publication must carry the authority-owned generation binding emitted
    // by `EvolvableModulePairAuthority::generation_bindings`, which closes
    // over the executable compiler/registry/budget/archive/behavior facts.
    let operator_implementation_identity =
        validated_generation_operator_implementation(plan, authority_fields, evolvable)?;
    let policy = PublicationPolicy {
        qd_version,
        policy_name,
        policy_sha256,
        pair_policy,
        operator_implementation_identity,
        predeclared_evidence_context_sha256: None,
        archive_policy_authority: Some(archive_policy_authority),
    };
    policy.validate().map_err(|error| {
        contract(format!(
            "v5 G0 derived publication policy is invalid: {error}"
        ))
    })?;
    Ok(policy)
}

/// Reconstruct the *fresh* executable operator identity from the sealed
/// generation config and compare every claimed static fact to the frozen v5
/// authority.  `sourceOperatorImplementation` deliberately does not appear
/// here: it names the pre-v5 source implementation and is not executable
/// authority for a native generation.
fn validated_generation_operator_implementation(
    plan: &V5G0PublicationPlan,
    authority_fields: &Map<String, Value>,
    evolvable: &Value,
) -> Result<Value> {
    let config = object_ref(
        &plan.generation_config,
        "v5 G0 publication generation config",
    )?;
    let operator = config
        .get("operatorImplementation")
        .ok_or_else(|| contract("v5 G0 generation config lacks operator implementation"))?;
    let fields = object_ref(operator, "v5 G0 generation operator implementation")?;
    // `build_pair_generation_config` is the sole canonical v2 config
    // authority.  Its executable run closure retains the archive/behavior
    // requirements (and, when present, the capacity receipt) beneath
    // `runConfig`; those fields are intentionally not duplicated at the
    // generation-config top level.
    let run_config = object_ref(
        config
            .get("runConfig")
            .ok_or_else(|| contract("v5 G0 generation config lacks runConfig"))?,
        "v5 G0 generation runConfig",
    )?;
    let evolvable = object_ref(evolvable, "v5 G0 evolvable module authority")?;
    let archive = evolvable
        .get("archivePolicyAuthority")
        .ok_or_else(|| contract("v5 G0 evolvable authority lacks archive policy authority"))?;
    let behavior = evolvable
        .get("behaviorAttributionRequirement")
        .ok_or_else(|| contract("v5 G0 evolvable authority lacks behavior requirement"))?;
    let capacity_receipt = evolvable.get("capacityReceipt");
    let expected_keys = if capacity_receipt.is_some() {
        vec![
            "schemaVersion",
            "authoritySha256",
            "programKind",
            "codec",
            "compilerPolicySha256",
            "operatorRegistry",
            "budget",
            "capacityContract",
            "archivePolicyAuthoritySha256",
            "behaviorAttributionRequirementSha256",
            "capacityReceiptSha256",
            "operatorImplementationSha256",
        ]
    } else {
        vec![
            "schemaVersion",
            "authoritySha256",
            "programKind",
            "codec",
            "compilerPolicySha256",
            "operatorRegistry",
            "budget",
            "capacityContract",
            "archivePolicyAuthoritySha256",
            "behaviorAttributionRequirementSha256",
            "operatorImplementationSha256",
        ]
    };
    exact_keys(
        fields,
        &expected_keys,
        "v5 G0 generation operator implementation",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_evolvable_module_operator_implementation_v1")
    {
        return Err(contract(
            "v5 G0 generation operator implementation schema is invalid",
        ));
    }
    for key in [
        "authoritySha256",
        "programKind",
        "codec",
        "compilerPolicySha256",
        "operatorRegistry",
        "budget",
        "capacityContract",
    ] {
        if fields.get(key) != evolvable.get(key) {
            return Err(contract(format!(
                "v5 G0 generation operator implementation {key} drifted from evolvable authority"
            )));
        }
    }
    // The native closure must be the same executable closure as the enriched
    // generation binding, not merely a source-operator hash with a familiar
    // schema tag.
    let native = authority_fields
        .get("nativeOperatorAuthority")
        .ok_or_else(|| contract("v5 G0 publication authority lacks native operator authority"))?;
    let native = object_ref(native, "v5 G0 native operator authority")?;
    for key in [
        "compilerPolicySha256",
        "programKind",
        "codec",
        "operatorRegistry",
        "budget",
    ] {
        if fields.get(key) != native.get(key) {
            return Err(contract(format!(
                "v5 G0 generation operator implementation {key} drifted from native authority"
            )));
        }
    }
    if native.get("factoryAuthoritySha256") != fields.get("authoritySha256") {
        return Err(contract(
            "v5 G0 generation operator implementation authority drifted from native factory authority",
        ));
    }
    let configured_archive = run_config
        .get("archivePolicyAuthority")
        .ok_or_else(|| contract("v5 G0 generation runConfig lacks archive policy authority"))?;
    let configured_behavior = run_config
        .get("behaviorAttributionRequirement")
        .ok_or_else(|| contract("v5 G0 generation runConfig lacks behavior requirement"))?;
    if configured_archive != archive || configured_behavior != behavior {
        return Err(contract(
            "v5 G0 generation runConfig archive/behavior authority drifted",
        ));
    }
    if run_config.get("operatorImplementation") != Some(operator) {
        return Err(contract(
            "v5 G0 generation runConfig operator implementation drifted",
        ));
    }
    if fields.get("archivePolicyAuthoritySha256")
        != Some(&Value::String(canonical_sha256(archive)?))
    {
        return Err(contract(
            "v5 G0 generation operator implementation archive authority identity drifted",
        ));
    }
    let behavior_fields = object_ref(behavior, "v5 G0 behavior requirement")?;
    let behavior_sha = exact_sha(
        behavior_fields
            .get("requirementSha256")
            .ok_or_else(|| contract("v5 G0 behavior requirement lacks identity"))?,
        "v5 G0 behavior requirement SHA-256",
    )?;
    if canonical_sha256_without_object_field(behavior, "requirementSha256")? != behavior_sha
        || fields.get("behaviorAttributionRequirementSha256") != Some(&Value::String(behavior_sha))
    {
        return Err(contract(
            "v5 G0 generation operator implementation behavior requirement drifted",
        ));
    }
    match capacity_receipt {
        Some(receipt) => {
            if run_config.get("capacityReceipt") != Some(receipt) {
                return Err(contract(
                    "v5 G0 generation runConfig capacity receipt drifted",
                ));
            }
            let receipt_fields = object_ref(receipt, "v5 G0 capacity receipt")?;
            let receipt_sha = exact_sha(
                receipt_fields
                    .get("semanticReceiptSha256")
                    .ok_or_else(|| contract("v5 G0 capacity receipt lacks semantic identity"))?,
                "v5 G0 capacity receipt semantic SHA-256",
            )?;
            if fields.get("capacityReceiptSha256") != Some(&Value::String(receipt_sha)) {
                return Err(contract(
                    "v5 G0 generation operator implementation capacity receipt drifted",
                ));
            }
        }
        None if run_config.contains_key("capacityReceipt") => {
            return Err(contract(
                "v5 G0 generation runConfig has an unsealed capacity receipt",
            ));
        }
        None => {}
    }
    let implementation_sha = exact_sha(
        fields
            .get("operatorImplementationSha256")
            .ok_or_else(|| contract("v5 G0 generation operator implementation lacks identity"))?,
        "v5 G0 generation operator implementation SHA-256",
    )?;
    if canonical_sha256_without_object_field(operator, "operatorImplementationSha256")?
        != implementation_sha
    {
        return Err(contract(
            "v5 G0 generation operator implementation identity drifted",
        ));
    }
    Ok(operator.clone())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5G0StreamedArtifact {
    pub semantic_sha256: String,
    pub file_sha256: String,
    pub encoded_bytes: u64,
}

impl V5G0StreamedArtifact {
    fn validate(&self) -> Result<()> {
        exact_sha(
            &Value::String(self.semantic_sha256.clone()),
            "v5 G0 streamed artifact semantic SHA-256",
        )?;
        exact_sha(
            &Value::String(self.file_sha256.clone()),
            "v5 G0 streamed artifact file SHA-256",
        )?;
        if self.encoded_bytes == 0 {
            return Err(contract("v5 G0 streamed artifact cannot be empty"));
        }
        Ok(())
    }

    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(object([
            (
                "semanticSha256",
                Value::String(exact_sha(
                    &Value::String(self.semantic_sha256.clone()),
                    "v5 G0 streamed artifact semantic SHA-256",
                )?),
            ),
            (
                "fileSha256",
                Value::String(exact_sha(
                    &Value::String(self.file_sha256.clone()),
                    "v5 G0 streamed artifact file SHA-256",
                )?),
            ),
            ("encodedBytes", Value::from(self.encoded_bytes)),
        ]))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 G0 streamed artifact")?;
        exact_keys(
            fields,
            &["semanticSha256", "fileSha256", "encodedBytes"],
            "v5 G0 streamed artifact",
        )?;
        let artifact = Self {
            semantic_sha256: exact_sha(
                required(value, "semanticSha256", "v5 G0 streamed artifact")?,
                "v5 G0 streamed artifact semantic SHA-256",
            )?,
            file_sha256: exact_sha(
                required(value, "fileSha256", "v5 G0 streamed artifact")?,
                "v5 G0 streamed artifact file SHA-256",
            )?,
            encoded_bytes: required(value, "encodedBytes", "v5 G0 streamed artifact")?
                .as_u64()
                .ok_or_else(|| contract("v5 G0 streamed artifact byte count is invalid"))?,
        };
        artifact.validate()?;
        if &artifact.to_value()? != value {
            return Err(contract("v5 G0 streamed artifact is not canonical"));
        }
        Ok(artifact)
    }
}

/// A dynamic array fragment emitted by the single selected-materialization
/// pass.  The bytes are the exact comma-separated canonical JSON elements
/// which will later be spliced into a public artifact array (without `[` or
/// `]`).  They are intentionally private staging values, but their typed
/// receipts let core detect any changed/truncated fragment before it is used
/// to assemble an artifact.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum V5G0PublicationFragmentKind {
    PopulationCandidates,
    EvaluationCandidates,
    EvaluationFunnelEntries,
    GenerationJournalBindings,
}

impl V5G0PublicationFragmentKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PopulationCandidates => "populationCandidates",
            Self::EvaluationCandidates => "evaluationCandidates",
            Self::EvaluationFunnelEntries => "evaluationFunnelEntries",
            Self::GenerationJournalBindings => "generationJournalBindings",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5G0PublicationFragment {
    pub kind: V5G0PublicationFragmentKind,
    pub fragment_sha256: String,
    pub encoded_bytes: u64,
    pub row_count: u64,
}

impl V5G0PublicationFragment {
    fn validate(&self, expected_kind: V5G0PublicationFragmentKind) -> Result<()> {
        if self.kind != expected_kind || self.row_count == 0 {
            return Err(contract("v5 G0 publication fragment kind/count is invalid"));
        }
        exact_sha(
            &Value::String(self.fragment_sha256.clone()),
            "v5 G0 publication fragment SHA-256",
        )?;
        Ok(())
    }

    pub fn to_value(&self) -> Result<Value> {
        self.validate(self.kind)?;
        Ok(object([
            ("kind", Value::String(self.kind.as_str().to_owned())),
            (
                "fragmentSha256",
                Value::String(exact_sha(
                    &Value::String(self.fragment_sha256.clone()),
                    "v5 G0 publication fragment SHA-256",
                )?),
            ),
            ("encodedBytes", Value::from(self.encoded_bytes)),
            ("rowCount", Value::from(self.row_count)),
        ]))
    }
}

/// Typed receipt for all four file-backed dynamic-array fragments.  It is
/// returned before any public artifact is written, so batch can fsync/reset
/// private fragments and core can authenticate them while it later splices
/// the final canonical documents.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5G0PublicationFragments {
    pub population_candidates: V5G0PublicationFragment,
    pub evaluation_candidates: V5G0PublicationFragment,
    pub evaluation_funnel_entries: V5G0PublicationFragment,
    pub generation_journal_bindings: V5G0PublicationFragment,
}

impl V5G0PublicationFragments {
    pub fn fragment(&self, kind: V5G0PublicationFragmentKind) -> &V5G0PublicationFragment {
        match kind {
            V5G0PublicationFragmentKind::PopulationCandidates => &self.population_candidates,
            V5G0PublicationFragmentKind::EvaluationCandidates => &self.evaluation_candidates,
            V5G0PublicationFragmentKind::EvaluationFunnelEntries => &self.evaluation_funnel_entries,
            V5G0PublicationFragmentKind::GenerationJournalBindings => {
                &self.generation_journal_bindings
            }
        }
    }

    pub fn validate_for_selected(&self, selected_count: u64) -> Result<()> {
        for kind in [
            V5G0PublicationFragmentKind::PopulationCandidates,
            V5G0PublicationFragmentKind::EvaluationCandidates,
            V5G0PublicationFragmentKind::EvaluationFunnelEntries,
            V5G0PublicationFragmentKind::GenerationJournalBindings,
        ] {
            let fragment = self.fragment(kind);
            fragment.validate(kind)?;
            if fragment.row_count != selected_count {
                return Err(contract(
                    "v5 G0 publication fragment row count drifted from selected population",
                ));
            }
        }
        Ok(())
    }

    pub fn to_value(&self) -> Result<Value> {
        self.validate_for_selected(self.population_candidates.row_count)?;
        let semantic = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_v5_g0_publication_fragments_v1".to_owned()),
            ),
            (
                "populationCandidates",
                self.population_candidates.to_value()?,
            ),
            (
                "evaluationCandidates",
                self.evaluation_candidates.to_value()?,
            ),
            (
                "evaluationFunnelEntries",
                self.evaluation_funnel_entries.to_value()?,
            ),
            (
                "generationJournalBindings",
                self.generation_journal_bindings.to_value()?,
            ),
        ]);
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 G0 publication fragment receipt")
            .clone();
        fields.insert(
            "fragmentBundleSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }
}

/// Batch-owned, file-backed sink for a fresh selected-materialization pass.
/// Core supplies already-canonical bytes and never hands batch a rich Value;
/// the sink merely appends those bytes to the private fragment named by
/// `kind`.  It must preserve write order and not insert a newline.
pub trait V5G0PublicationFragmentSink {
    fn write_fragment(
        &mut self,
        kind: V5G0PublicationFragmentKind,
        canonical_bytes: &[u8],
    ) -> std::io::Result<()>;
}

/// Batch-owned resettable source for the same four private fragments.  Each
/// call must copy the complete fragment from byte zero without adding or
/// removing bytes.  Core hashes the copied bytes against
/// `V5G0PublicationFragments`, so a staged-file substitution fails closed.
pub trait V5G0PublicationFragmentSource {
    fn copy_fragment(
        &mut self,
        kind: V5G0PublicationFragmentKind,
        output: &mut dyn Write,
    ) -> std::io::Result<()>;
}

struct FragmentAccumulator {
    kind: V5G0PublicationFragmentKind,
    first: bool,
    hash: CanonicalSha256Writer,
    encoded_bytes: u64,
    row_count: u64,
}

impl FragmentAccumulator {
    fn new(kind: V5G0PublicationFragmentKind) -> Self {
        Self {
            kind,
            first: true,
            hash: CanonicalSha256Writer::default(),
            encoded_bytes: 0,
            row_count: 0,
        }
    }

    fn append<S: V5G0PublicationFragmentSink>(
        &mut self,
        sink: &mut S,
        value: &Value,
    ) -> Result<()> {
        let mut row = Vec::new();
        write_canonical_json(value, &mut row)?;
        if !self.first {
            sink.write_fragment(self.kind, b",")?;
            self.hash.write_all(b",")?;
            self.encoded_bytes = self
                .encoded_bytes
                .checked_add(1)
                .ok_or_else(|| contract("v5 G0 publication fragment byte count overflow"))?;
        }
        self.first = false;
        sink.write_fragment(self.kind, &row)?;
        self.hash.write_all(&row)?;
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(row.len() as u64)
            .ok_or_else(|| contract("v5 G0 publication fragment byte count overflow"))?;
        self.row_count = self
            .row_count
            .checked_add(1)
            .ok_or_else(|| contract("v5 G0 publication fragment row count overflow"))?;
        Ok(())
    }

    fn finish(self) -> Result<V5G0PublicationFragment> {
        let fragment = V5G0PublicationFragment {
            kind: self.kind,
            fragment_sha256: self.hash.finish(),
            encoded_bytes: self.encoded_bytes,
            row_count: self.row_count,
        };
        fragment.validate(self.kind)?;
        Ok(fragment)
    }
}

/// The only rich-candidate handoff in the v5 public path.  Consumers receive
/// one owned materialization synchronously, write what they need, and return;
/// the stream never collects a selected population in memory.
pub(crate) trait V5SelectedG0MaterializationSink {
    fn accept(&mut self, materialization: &V5SelectedG0Materialization) -> Result<()>;
}

/// Prepared semantic stream for one completed compact G0 transaction.
///
/// It owns compact indexes and the parsed authority only.  `for_each_selected`
/// reconstructs a rich candidate inside the loop body and drops it before the
/// next compact record is opened.
pub struct V5G0PublicationStream<'a> {
    plan: &'a V5G0PublicationPlan,
    transaction: &'a V5G0TransactionResult,
    authority: V5SharedConstructionAuthority,
    publication_request: PublicationRequest,
    selected_materialization_indexes: Vec<V5SelectedMaterializationIndex>,
    selected_references: Vec<AcceptedReference>,
    g0_binding: BTreeMap<String, String>,
    reproduction_allocation_accounting: Value,
}

#[derive(Clone, Copy, Debug)]
struct V5SelectedMaterializationIndex {
    record_index: usize,
    projection_index: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum V5G0PublicationPreparationValidation {
    PersistedReplay,
    FreshConstructed,
}

/// Parse/replay the compact transaction, then prepare a selected-only public
/// stream.  This is the batch-facing construction boundary: it does not write
/// a file and does not expose an unselected rich candidate.
pub fn prepare_v5_g0_publication_stream<'a>(
    request: &'a V5G0TransactionRequest,
    transaction: &'a V5G0TransactionResult,
) -> Result<V5G0PublicationStream<'a>> {
    prepare_v5_g0_publication_stream_with_validation(
        request,
        transaction,
        V5G0PublicationPreparationValidation::PersistedReplay,
    )
}

/// Prepare publication directly from the transaction returned by the current
/// in-process constructor.  Persisted/restart callers must continue to use
/// [`prepare_v5_g0_publication_stream`], which performs the full sealed replay.
/// The fresh route avoids reconstructing all 4,000 accepted candidates a
/// second time before selected-only materialization begins.
pub fn prepare_v5_g0_publication_stream_from_fresh_transaction<'a>(
    request: &'a V5G0TransactionRequest,
    transaction: &'a V5G0TransactionResult,
) -> Result<V5G0PublicationStream<'a>> {
    prepare_v5_g0_publication_stream_with_validation(
        request,
        transaction,
        V5G0PublicationPreparationValidation::FreshConstructed,
    )
}

fn prepare_v5_g0_publication_stream_with_validation<'a>(
    request: &'a V5G0TransactionRequest,
    transaction: &'a V5G0TransactionResult,
    validation: V5G0PublicationPreparationValidation,
) -> Result<V5G0PublicationStream<'a>> {
    let authority = V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)
        .map_err(V5G0TransactionError::from)?;
    match validation {
        V5G0PublicationPreparationValidation::PersistedReplay => {
            verify_v5_g0_transaction_replay_with_authority(request, transaction, &authority)?;
        }
        V5G0PublicationPreparationValidation::FreshConstructed => {
            request.validate()?;
            transaction.verify_fresh_construction(&authority)?;
        }
    }
    if !transaction.target_reached {
        return Err(contract("cannot publish an incomplete v5 G0 transaction"));
    }
    transaction
        .publication_plan
        .validate_against_request(request, &authority)?;
    let plan = &transaction.publication_plan;
    let policy = publication_policy_from_authority(plan, &authority)?;
    let config = object_ref(
        &plan.generation_config,
        "v5 G0 publication generation config",
    )?;
    let allocation = config
        .get("reproductionAllocation")
        .cloned()
        .ok_or_else(|| contract("v5 G0 publication config lacks allocation"))?;

    let mut origin_proposal_counts = BTreeMap::new();
    let mut origin_accepted_counts = BTreeMap::new();
    let mut rejected_by_origin = BTreeMap::<String, BTreeMap<String, u64>>::new();
    for attempt in &transaction.attempts {
        *origin_proposal_counts
            .entry(attempt.origin_kind.clone())
            .or_insert(0) += 1;
        if attempt.disposition == "accepted" {
            *origin_accepted_counts
                .entry(attempt.origin_kind.clone())
                .or_insert(0) += 1;
        } else {
            *rejected_by_origin
                .entry(attempt.origin_kind.clone())
                .or_default()
                .entry(attempt.reason_code.clone())
                .or_insert(0) += 1;
        }
    }
    let reproduction_allocation_accounting = reproduction_allocation_accounting(
        &allocation,
        &origin_proposal_counts,
        &origin_accepted_counts,
        &origin_accepted_counts,
        &rejected_by_origin,
    )
    .map_err(|error| contract(format!("v5 G0 allocation accounting failed: {error}")))?;
    if reproduction_allocation_accounting
        .get("complete")
        .and_then(Value::as_bool)
        != Some(true)
    {
        return Err(contract(
            "completed v5 G0 transaction has an allocation deficit",
        ));
    }

    let construction_references = transaction
        .accepted_records
        .iter()
        .map(|record| {
            Ok(AcceptedReference {
                proposal_ordinal: record.proposal_ordinal,
                candidate_id: record.candidate_id.clone(),
                candidate_identity_sha256: record.candidate_identity_sha256.clone(),
                executable_semantic_sha256: record.executable_semantic_sha256.clone(),
                entry_sha256: record.record_sha256()?,
                descriptor_projection: Some(record.descriptor_projection.clone()),
            })
        })
        .collect::<std::result::Result<Vec<_>, V5G0TransactionError>>()?;
    let entry_sha256s = transaction
        .attempts
        .iter()
        .map(|attempt| attempt.attempt_sha256().map_err(V5G0TransactionError::from))
        .collect::<std::result::Result<Vec<_>, V5G0TransactionError>>()?;
    let entry_ordinals = transaction
        .attempts
        .iter()
        .map(|attempt| attempt.proposal_ordinal)
        .collect::<Vec<_>>();
    let global_identity_ledger = object([
        (
            "pairExecutableSemanticCount",
            Value::from(
                transaction
                    .identity_ledger
                    .executable_semantic_sha256s
                    .len() as u64,
            ),
        ),
        (
            "pairExecutableSemanticDuplicateRejections",
            Value::from(
                transaction
                    .attempts
                    .iter()
                    .filter(|attempt| attempt.reason_code == "duplicate_pair_genome_global")
                    .count() as u64,
            ),
        ),
        (
            "identityLedgerSha256",
            Value::String(transaction.identity_ledger.identity_ledger_sha256()?),
        ),
    ]);
    let publication_request = PublicationRequest {
        request_sha256: plan.publication_request_sha256(&authority.shared_authority_sha256)?,
        config_sha256: plan.generation_config_sha256.clone(),
        generation_index: plan.generation_index,
        target_unique_candidates: plan.target_unique_candidates,
        max_proposal_attempts: plan.max_proposal_attempts,
        proposal_count: transaction.attempts.len() as u64,
        origin_proposal_counts,
        origin_accepted_counts,
        disposition_counts: transaction
            .schedule_state_receipt
            .disposition_counts
            .clone(),
        entry_sha256s,
        entry_ordinals,
        construction_references,
        g0_evaluation_width: Some(plan.evaluation_population_size),
        global_identity_ledger: Some(global_identity_ledger),
        reproduction_allocation: Some(allocation),
        reproduction_allocation_accounting: Some(reproduction_allocation_accounting.clone()),
        unique_pair_genome_count: Some(
            transaction.identity_ledger.pair_identity_sha256s.len() as u64
        ),
        policy,
    };
    publication_request
        .validate()
        .map_err(|error| contract(format!("v5 G0 publication request is invalid: {error}")))?;

    let pool = transaction
        .accepted_pool
        .as_ref()
        .ok_or_else(|| contract("completed v5 G0 transaction lacks accepted pool"))?;
    let selection = transaction
        .g0_selection
        .as_ref()
        .ok_or_else(|| contract("completed v5 G0 transaction lacks selection"))?;
    let campaign_ledger = transaction
        .campaign_ledger
        .as_ref()
        .ok_or_else(|| contract("completed v5 G0 transaction lacks campaign ledger"))?;
    let index = transaction
        .selected_projection_index
        .as_ref()
        .ok_or_else(|| contract("completed v5 G0 transaction lacks projection index"))?;
    let g0_binding = BTreeMap::from([
        (
            "constructionPoolIdentitySha256".to_owned(),
            sha_from_field(
                pool,
                "constructionPoolIdentitySha256",
                "v5 G0 accepted pool",
            )?,
        ),
        (
            "acceptedPoolSha256".to_owned(),
            sha_from_field(pool, "acceptedPoolSha256", "v5 G0 accepted pool")?,
        ),
        (
            "selectionSha256".to_owned(),
            sha_from_field(selection, "selectionSha256", "v5 G0 selection")?,
        ),
        (
            "ledgerSha256".to_owned(),
            sha_from_field(campaign_ledger, "ledgerSha256", "v5 G0 campaign ledger")?,
        ),
    ]);
    let by_record = transaction
        .accepted_records
        .iter()
        .enumerate()
        .map(|(index, record)| Ok((record.record_sha256()?, index)))
        .collect::<std::result::Result<BTreeMap<_, _>, V5G0TransactionError>>()?;
    let mut selected_materialization_indexes = index
        .projections
        .iter()
        .enumerate()
        .map(|(projection_index, projection)| {
            let record_index = by_record
                .get(&projection.record_sha256)
                .copied()
                .ok_or_else(|| {
                    contract("v5 G0 selected projection names an absent compact record")
                })?;
            Ok(V5SelectedMaterializationIndex {
                record_index,
                projection_index,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    selected_materialization_indexes.sort_by(|left, right| {
        transaction.accepted_records[left.record_index]
            .candidate_id
            .cmp(&transaction.accepted_records[right.record_index].candidate_id)
    });
    let selected_references = selected_materialization_indexes
        .iter()
        .map(|index| {
            let record = &transaction.accepted_records[index.record_index];
            Ok(AcceptedReference {
                proposal_ordinal: record.proposal_ordinal,
                candidate_id: record.candidate_id.clone(),
                candidate_identity_sha256: record.candidate_identity_sha256.clone(),
                executable_semantic_sha256: record.executable_semantic_sha256.clone(),
                entry_sha256: record.record_sha256()?,
                descriptor_projection: Some(record.descriptor_projection.clone()),
            })
        })
        .collect::<std::result::Result<Vec<_>, V5G0TransactionError>>()?;
    if selected_references.len() as u64 != plan.evaluation_population_size {
        return Err(contract("v5 G0 selected stream width drifted"));
    }
    Ok(V5G0PublicationStream {
        plan,
        transaction,
        authority,
        publication_request,
        selected_materialization_indexes,
        selected_references,
        g0_binding,
        reproduction_allocation_accounting,
    })
}

impl<'a> V5G0PublicationStream<'a> {
    pub fn publication_plan(&self) -> &V5G0PublicationPlan {
        self.plan
    }

    pub fn publication_request_sha256(&self) -> &str {
        &self.publication_request.request_sha256
    }

    pub fn selected_count(&self) -> usize {
        self.selected_materialization_indexes.len()
    }

    /// Synchronously materialize each selected compact record once.  The
    /// materialization is local to an iteration and cannot accumulate unless
    /// a caller deliberately clones it; normal public writers below never do.
    pub(crate) fn for_each_selected<S: V5SelectedG0MaterializationSink>(
        &self,
        sink: &mut S,
    ) -> Result<()> {
        for index in &self.selected_materialization_indexes {
            let materialization = self.materialize_selected(*index)?;
            sink.accept(&materialization)?;
        }
        Ok(())
    }

    fn materialize_selected(
        &self,
        index: V5SelectedMaterializationIndex,
    ) -> Result<V5SelectedG0Materialization> {
        let record = &self.transaction.accepted_records[index.record_index];
        let delta = &self.transaction.accepted_proposal_deltas[index.record_index];
        let projection = self
            .transaction
            .selected_projection_index
            .as_ref()
            .and_then(|projections| projections.projections.get(index.projection_index))
            .ok_or_else(|| contract("v5 G0 selected compact record lacks projection"))?;
        if projection.record_sha256 != record.record_sha256().map_err(V5G0TransactionError::from)? {
            return Err(contract(
                "v5 G0 selected materialization index drifted from compact record",
            ));
        }
        materialize_selected_v5_g0_record(&self.authority, projection, delta, record)
            .map_err(Into::into)
    }

    fn for_each_selected_with<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(&V5SelectedG0Materialization) -> Result<()>,
    {
        struct CallbackSink<'a, F>(&'a mut F);
        impl<F> V5SelectedG0MaterializationSink for CallbackSink<'_, F>
        where
            F: FnMut(&V5SelectedG0Materialization) -> Result<()>,
        {
            fn accept(&mut self, materialization: &V5SelectedG0Materialization) -> Result<()> {
                (self.0)(materialization)
            }
        }
        self.for_each_selected(&mut CallbackSink(&mut callback))
    }

    /// Perform the only rich selected-materialization traversal required for
    /// a fresh publication bundle.  The four dynamic arrays are emitted as
    /// exact canonical fragment bytes to batch-owned private files.  Later
    /// public-artifact hashing/writing reads those bytes back through
    /// `V5G0PublicationFragmentSource`; it never reconstructs a selected
    /// candidate a second time.
    pub fn materialize_selected_fragments<S: V5G0PublicationFragmentSink>(
        &self,
        sink: &mut S,
    ) -> Result<V5G0PublicationFragments> {
        self.materialize_selected_fragments_parallel(1, None, sink)
    }

    /// Materialize the selected evaluation width with bounded parallel
    /// reconstruction while preserving the exact candidate-ID output order.
    /// At most two work items per worker are retained before their canonical
    /// fragments are appended, so the 1,024-wide G0 selection does not become
    /// another population-sized rich-value allocation.
    pub fn materialize_selected_fragments_parallel<S: V5G0PublicationFragmentSink>(
        &self,
        thread_cap: u64,
        progress: Option<&NativeProgressHandle>,
        sink: &mut S,
    ) -> Result<V5G0PublicationFragments> {
        let mut parent_sink = NoopV5G0ParentReferenceSink;
        self.materialize_selected_fragments_and_parents_parallel(
            thread_cap,
            progress,
            sink,
            &mut parent_sink,
        )
    }

    /// Fast-ephemeral variant of selected publication. Parent references are
    /// emitted in the same deterministic selected order and during the same
    /// bounded materialization pass as the four public fragments.
    pub fn materialize_selected_fragments_and_parents_parallel<
        S: V5G0PublicationFragmentSink,
        P: V5G0ParentReferenceSink,
    >(
        &self,
        thread_cap: u64,
        progress: Option<&NativeProgressHandle>,
        sink: &mut S,
        parent_sink: &mut P,
    ) -> Result<V5G0PublicationFragments> {
        if !(1..=8).contains(&thread_cap) {
            return Err(contract(
                "v5 selected materialization thread cap must be in 1..=8",
            ));
        }
        let mut population =
            FragmentAccumulator::new(V5G0PublicationFragmentKind::PopulationCandidates);
        let mut evaluation =
            FragmentAccumulator::new(V5G0PublicationFragmentKind::EvaluationCandidates);
        let mut funnel =
            FragmentAccumulator::new(V5G0PublicationFragmentKind::EvaluationFunnelEntries);
        let mut journal =
            FragmentAccumulator::new(V5G0PublicationFragmentKind::GenerationJournalBindings);

        let mut append = |materialization: &V5SelectedG0Materialization| -> Result<()> {
            parent_sink.write_parent_reference(&materialization.parent_reference)?;
            population.append(sink, &materialization.rich_evaluation_candidate)?;
            let row = &materialization.publication_precomputed_row;
            let evaluation_candidate =
                required(row, "evaluationCandidate", "v5 selected publication row")?;
            evaluation.append(sink, evaluation_candidate)?;
            let funnel_entry = required(row, "funnelEntry", "v5 selected publication row")?;
            funnel.append(sink, funnel_entry)?;
            let evaluation_fields =
                object_ref(evaluation_candidate, "v5 selected evaluation candidate")?;
            let journal_binding = object([
                (
                    "candidateId",
                    evaluation_fields
                        .get("candidateId")
                        .cloned()
                        .ok_or_else(|| {
                            contract("v5 selected evaluation candidate lacks candidate ID")
                        })?,
                ),
                (
                    "proposalOrdinal",
                    evaluation_fields
                        .get("proposalOrdinal")
                        .cloned()
                        .ok_or_else(|| {
                            contract("v5 selected evaluation candidate lacks ordinal")
                        })?,
                ),
                (
                    "proposalEntrySha256",
                    evaluation_fields
                        .get("proposalEntrySha256")
                        .cloned()
                        .ok_or_else(|| {
                            contract("v5 selected evaluation candidate lacks entry SHA-256")
                        })?,
                ),
                (
                    "candidateProjectionSha256",
                    Value::String(canonical_sha256(evaluation_candidate)?),
                ),
            ]);
            journal.append(sink, &journal_binding)?;
            Ok(())
        };

        let workers = usize::min(
            thread_cap as usize,
            self.selected_materialization_indexes.len(),
        )
        .max(1);
        if workers == 1 {
            for index in &self.selected_materialization_indexes {
                append(&self.materialize_selected(*index)?)?;
                if let Some(progress) = progress {
                    progress.advance_completed(1);
                }
            }
        } else {
            let batch_width = workers.saturating_mul(2);
            for batch in self.selected_materialization_indexes.chunks(batch_width) {
                let chunk_size = batch.len().div_ceil(workers);
                let joined = thread::scope(|scope| {
                    let mut handles = Vec::new();
                    for (chunk_index, chunk) in batch.chunks(chunk_size).enumerate() {
                        handles.push(scope.spawn(move || -> Result<Vec<_>> {
                            if let Some(progress) = progress {
                                progress.worker_started();
                            }
                            let outcome = chunk
                                .iter()
                                .enumerate()
                                .map(|(offset, index)| {
                                    Ok((
                                        chunk_index * chunk_size + offset,
                                        self.materialize_selected(*index)?,
                                    ))
                                })
                                .collect::<Result<Vec<_>>>();
                            if let Some(progress) = progress {
                                progress.worker_finished();
                            }
                            outcome
                        }));
                    }
                    let mut materialized = Vec::with_capacity(batch.len());
                    for handle in handles {
                        let mut values = handle.join().map_err(|_| {
                            contract("v5 selected materialization worker panicked")
                        })??;
                        materialized.append(&mut values);
                    }
                    Ok::<_, V5G0PublicationError>(materialized)
                })?;
                let mut materialized = joined;
                materialized.sort_by_key(|(offset, _)| *offset);
                for (_, materialization) in materialized {
                    append(&materialization)?;
                    if let Some(progress) = progress {
                        progress.advance_completed(1);
                    }
                }
            }
        }
        let fragments = V5G0PublicationFragments {
            population_candidates: population.finish()?,
            evaluation_candidates: evaluation.finish()?,
            evaluation_funnel_entries: funnel.finish()?,
            generation_journal_bindings: journal.finish()?,
        };
        fragments.validate_for_selected(self.selected_count() as u64)?;
        Ok(fragments)
    }

    pub fn write_pair_config<W: Write>(&self, output: &mut W) -> Result<V5G0StreamedArtifact> {
        let semantic_sha256 = self.plan.generation_config_sha256.clone();
        write_static_artifact(&self.plan.generation_config, &semantic_sha256, output)
    }

    /// Test-only direct writer retained for small unit fixtures.  Production
    /// callers must use the fragment bundle APIs below so rich candidates are
    /// not reconstructed once per artifact/hash pass.
    #[cfg(test)]
    pub(crate) fn write_population<W: Write>(
        &self,
        output: &mut W,
    ) -> Result<V5G0StreamedArtifact> {
        let template = self.population_template()?;
        let mut semantic_writer = CanonicalSha256Writer::default();
        self.stream_population_document(&template, None, &mut semantic_writer)?;
        let semantic_sha256 = semantic_writer.finish();
        let mut final_template = template;
        final_template
            .as_object_mut()
            .expect("population template is object")
            .insert(
                "populationSha256".to_owned(),
                Value::String(semantic_sha256.clone()),
            );
        write_streamed_artifact(output, &semantic_sha256, |writer| {
            self.stream_population_document(&final_template, None, writer)
        })
    }

    #[cfg(test)]
    pub(crate) fn write_evaluation_population<W: Write>(
        &self,
        population: &V5G0StreamedArtifact,
        output: &mut W,
    ) -> Result<V5G0StreamedArtifact> {
        let template = self.evaluation_template(population)?;
        let mut semantic_writer = CanonicalSha256Writer::default();
        self.stream_evaluation_document(&template, None, &mut semantic_writer)?;
        let semantic_sha256 = semantic_writer.finish();
        let mut final_template = template;
        final_template
            .as_object_mut()
            .expect("evaluation template is object")
            .insert(
                "evaluationPopulationSha256".to_owned(),
                Value::String(semantic_sha256.clone()),
            );
        write_streamed_artifact(output, &semantic_sha256, |writer| {
            self.stream_evaluation_document(&final_template, None, writer)
        })
    }

    #[cfg(test)]
    pub(crate) fn write_generation_journal<W: Write>(
        &self,
        population: &V5G0StreamedArtifact,
        evaluation_population: &V5G0StreamedArtifact,
        output: &mut W,
    ) -> Result<V5G0StreamedArtifact> {
        let template = self.generation_journal_template(population, evaluation_population)?;
        let mut semantic_writer = CanonicalSha256Writer::default();
        self.stream_generation_journal_document(&template, None, &mut semantic_writer)?;
        let semantic_sha256 = semantic_writer.finish();
        let mut final_template = template;
        final_template
            .as_object_mut()
            .expect("generation journal template is object")
            .insert(
                "journalSha256".to_owned(),
                Value::String(semantic_sha256.clone()),
            );
        write_streamed_artifact(output, &semantic_sha256, |writer| {
            self.stream_generation_journal_document(&final_template, None, writer)
        })
    }

    pub fn verify_pair_config<R: Read>(&self, input: &mut R) -> Result<V5G0StreamedArtifact> {
        verify_static_artifact(
            &self.plan.generation_config,
            &self.plan.generation_config_sha256,
            input,
        )
    }

    #[cfg(test)]
    pub(crate) fn verify_population<R: Read>(&self, input: &mut R) -> Result<V5G0StreamedArtifact> {
        let template = self.population_template()?;
        let mut semantic_writer = CanonicalSha256Writer::default();
        self.stream_population_document(&template, None, &mut semantic_writer)?;
        let semantic_sha256 = semantic_writer.finish();
        let mut final_template = template;
        final_template
            .as_object_mut()
            .expect("population template is object")
            .insert(
                "populationSha256".to_owned(),
                Value::String(semantic_sha256.clone()),
            );
        verify_streamed_artifact(input, &semantic_sha256, |writer| {
            self.stream_population_document(&final_template, None, writer)
        })
    }

    #[cfg(test)]
    pub(crate) fn verify_evaluation_population<R: Read>(
        &self,
        population: &V5G0StreamedArtifact,
        input: &mut R,
    ) -> Result<V5G0StreamedArtifact> {
        let template = self.evaluation_template(population)?;
        let mut semantic_writer = CanonicalSha256Writer::default();
        self.stream_evaluation_document(&template, None, &mut semantic_writer)?;
        let semantic_sha256 = semantic_writer.finish();
        let mut final_template = template;
        final_template
            .as_object_mut()
            .expect("evaluation template is object")
            .insert(
                "evaluationPopulationSha256".to_owned(),
                Value::String(semantic_sha256.clone()),
            );
        verify_streamed_artifact(input, &semantic_sha256, |writer| {
            self.stream_evaluation_document(&final_template, None, writer)
        })
    }

    #[cfg(test)]
    pub(crate) fn verify_generation_journal<R: Read>(
        &self,
        population: &V5G0StreamedArtifact,
        evaluation_population: &V5G0StreamedArtifact,
        input: &mut R,
    ) -> Result<V5G0StreamedArtifact> {
        let template = self.generation_journal_template(population, evaluation_population)?;
        let mut semantic_writer = CanonicalSha256Writer::default();
        self.stream_generation_journal_document(&template, None, &mut semantic_writer)?;
        let semantic_sha256 = semantic_writer.finish();
        let mut final_template = template;
        final_template
            .as_object_mut()
            .expect("generation journal template is object")
            .insert(
                "journalSha256".to_owned(),
                Value::String(semantic_sha256.clone()),
            );
        verify_streamed_artifact(input, &semantic_sha256, |writer| {
            self.stream_generation_journal_document(&final_template, None, writer)
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    pub(crate) fn verify_published<
        RPair: Read,
        RPopulation: Read,
        REvaluation: Read,
        RJournal: Read,
    >(
        &self,
        pair_config: &mut RPair,
        population: &mut RPopulation,
        evaluation_population: &mut REvaluation,
        generation_journal: &mut RJournal,
    ) -> Result<V5G0PublicationReceipt> {
        let pair_config = self.verify_pair_config(pair_config)?;
        let population = self.verify_population(population)?;
        let evaluation_population =
            self.verify_evaluation_population(&population, evaluation_population)?;
        let generation_journal = self.verify_generation_journal(
            &population,
            &evaluation_population,
            generation_journal,
        )?;
        Ok(V5G0PublicationReceipt {
            publication_plan_sha256: self.plan.publication_plan_sha256()?,
            publication_request_sha256: self.publication_request.request_sha256.clone(),
            pair_config,
            population,
            evaluation_population,
            generation_journal,
        })
    }

    /// Assemble all four public artifacts from authenticated private
    /// fragments.  This is the production write path: it performs no rich
    /// materialization, and each selected candidate was materialized exactly
    /// once earlier by `materialize_selected_fragments`.
    #[allow(clippy::too_many_arguments)]
    pub fn write_bundle_from_fragments<
        S: V5G0PublicationFragmentSource,
        WPair: Write,
        WPopulation: Write,
        WEvaluation: Write,
        WJournal: Write,
    >(
        &self,
        fragments: &V5G0PublicationFragments,
        source: &mut S,
        pair_config: &mut WPair,
        population: &mut WPopulation,
        evaluation_population: &mut WEvaluation,
        generation_journal: &mut WJournal,
    ) -> Result<V5G0PublicationReceipt> {
        fragments.validate_for_selected(self.selected_count() as u64)?;
        let pair_config = self.write_pair_config(pair_config)?;
        let population = self.write_population_from_fragments(fragments, source, population)?;
        let evaluation_population = self.write_evaluation_population_from_fragments(
            &population,
            fragments,
            source,
            evaluation_population,
        )?;
        let generation_journal = self.write_generation_journal_from_fragments(
            &population,
            &evaluation_population,
            fragments,
            source,
            generation_journal,
        )?;
        Ok(V5G0PublicationReceipt {
            publication_plan_sha256: self.plan.publication_plan_sha256()?,
            publication_request_sha256: self.publication_request.request_sha256.clone(),
            pair_config,
            population,
            evaluation_population,
            generation_journal,
        })
    }

    /// Verify a fully staged public bundle using its private fragment source.
    /// The verifier only splices/hash-checks compact fragment bytes; it never
    /// reconstructs the rich selected population.
    #[allow(clippy::too_many_arguments)]
    pub fn verify_bundle_from_fragments<
        S: V5G0PublicationFragmentSource,
        RPair: Read,
        RPopulation: Read,
        REvaluation: Read,
        RJournal: Read,
    >(
        &self,
        fragments: &V5G0PublicationFragments,
        source: &mut S,
        pair_config: &mut RPair,
        population: &mut RPopulation,
        evaluation_population: &mut REvaluation,
        generation_journal: &mut RJournal,
    ) -> Result<V5G0PublicationReceipt> {
        fragments.validate_for_selected(self.selected_count() as u64)?;
        let pair_config = self.verify_pair_config(pair_config)?;
        let population = self.verify_population_from_fragments(fragments, source, population)?;
        let evaluation_population = self.verify_evaluation_population_from_fragments(
            &population,
            fragments,
            source,
            evaluation_population,
        )?;
        let generation_journal = self.verify_generation_journal_from_fragments(
            &population,
            &evaluation_population,
            fragments,
            source,
            generation_journal,
        )?;
        Ok(V5G0PublicationReceipt {
            publication_plan_sha256: self.plan.publication_plan_sha256()?,
            publication_request_sha256: self.publication_request.request_sha256.clone(),
            pair_config,
            population,
            evaluation_population,
            generation_journal,
        })
    }

    /// Verify an already-published bundle during adoption/recovery.
    ///
    /// Fresh publication must first mint `receipt` through
    /// `verify_bundle_from_fragments`, while the authenticated private
    /// fragments still exist.  After those fragments have been deleted, this
    /// method deliberately verifies the immutable receipt and streams the
    /// exact public bytes without reconstructing a selected rich candidate.
    /// The caller must authenticate the receipt's content-addressed object
    /// binding before calling this method; a self-hash alone is not a source
    /// of construction authority.
    #[allow(clippy::too_many_arguments)]
    pub fn verify_adopted_bundle<
        RPair: Read,
        RPopulation: Read,
        REvaluation: Read,
        RJournal: Read,
    >(
        &self,
        receipt: &V5G0PublicationReceipt,
        pair_config: &mut RPair,
        population: &mut RPopulation,
        evaluation_population: &mut REvaluation,
        generation_journal: &mut RJournal,
    ) -> Result<V5G0PublicationReceipt> {
        let expected_plan_sha256 = self.plan.publication_plan_sha256()?;
        if receipt.publication_plan_sha256 != expected_plan_sha256
            || receipt.publication_request_sha256 != self.publication_request.request_sha256
        {
            return Err(contract(
                "v5 G0 published receipt does not bind the compact transaction/publication plan",
            ));
        }
        // Pair configuration has no dynamic selected-candidate content, so
        // it remains fully re-streamed from sealed plan authority.
        let pair_config_receipt = self.verify_pair_config(pair_config)?;
        if pair_config_receipt != receipt.pair_config {
            return Err(contract(
                "v5 G0 published pair-config bytes do not match the sealed receipt",
            ));
        }
        // These three documents are potentially population-sized.  The
        // authenticated fresh receipt commits their exact canonical bytes;
        // stream their byte hashes/lengths here instead of rematerializing
        // rich candidates or retaining a document in memory.
        verify_receipted_streamed_artifact(
            population,
            &receipt.population,
            "v5 G0 published population",
        )?;
        verify_receipted_streamed_artifact(
            evaluation_population,
            &receipt.evaluation_population,
            "v5 G0 published evaluation population",
        )?;
        verify_receipted_streamed_artifact(
            generation_journal,
            &receipt.generation_journal,
            "v5 G0 published generation journal",
        )?;
        Ok(receipt.clone())
    }

    pub fn write_population_from_fragments<S: V5G0PublicationFragmentSource, W: Write>(
        &self,
        fragments: &V5G0PublicationFragments,
        source: &mut S,
        output: &mut W,
    ) -> Result<V5G0StreamedArtifact> {
        let template = self.population_template()?;
        let mut semantic_writer = CanonicalSha256Writer::default();
        self.stream_population_document_from_fragments(
            &template,
            fragments,
            source,
            &mut semantic_writer,
        )?;
        let semantic_sha256 = semantic_writer.finish();
        let mut final_template = template;
        final_template
            .as_object_mut()
            .expect("population template is object")
            .insert(
                "populationSha256".to_owned(),
                Value::String(semantic_sha256.clone()),
            );
        write_streamed_artifact(output, &semantic_sha256, |writer| {
            self.stream_population_document_from_fragments(
                &final_template,
                fragments,
                source,
                writer,
            )
        })
    }

    pub fn write_evaluation_population_from_fragments<
        S: V5G0PublicationFragmentSource,
        W: Write,
    >(
        &self,
        population: &V5G0StreamedArtifact,
        fragments: &V5G0PublicationFragments,
        source: &mut S,
        output: &mut W,
    ) -> Result<V5G0StreamedArtifact> {
        let template = self.evaluation_template(population)?;
        let mut semantic_writer = CanonicalSha256Writer::default();
        self.stream_evaluation_document_from_fragments(
            &template,
            fragments,
            source,
            &mut semantic_writer,
        )?;
        let semantic_sha256 = semantic_writer.finish();
        let mut final_template = template;
        final_template
            .as_object_mut()
            .expect("evaluation template is object")
            .insert(
                "evaluationPopulationSha256".to_owned(),
                Value::String(semantic_sha256.clone()),
            );
        write_streamed_artifact(output, &semantic_sha256, |writer| {
            self.stream_evaluation_document_from_fragments(
                &final_template,
                fragments,
                source,
                writer,
            )
        })
    }

    pub fn write_generation_journal_from_fragments<S: V5G0PublicationFragmentSource, W: Write>(
        &self,
        population: &V5G0StreamedArtifact,
        evaluation_population: &V5G0StreamedArtifact,
        fragments: &V5G0PublicationFragments,
        source: &mut S,
        output: &mut W,
    ) -> Result<V5G0StreamedArtifact> {
        let template = self.generation_journal_template(population, evaluation_population)?;
        let mut semantic_writer = CanonicalSha256Writer::default();
        self.stream_generation_journal_document_from_fragments(
            &template,
            fragments,
            source,
            &mut semantic_writer,
        )?;
        let semantic_sha256 = semantic_writer.finish();
        let mut final_template = template;
        final_template
            .as_object_mut()
            .expect("generation journal template is object")
            .insert(
                "journalSha256".to_owned(),
                Value::String(semantic_sha256.clone()),
            );
        write_streamed_artifact(output, &semantic_sha256, |writer| {
            self.stream_generation_journal_document_from_fragments(
                &final_template,
                fragments,
                source,
                writer,
            )
        })
    }

    pub fn verify_population_from_fragments<S: V5G0PublicationFragmentSource, R: Read>(
        &self,
        fragments: &V5G0PublicationFragments,
        source: &mut S,
        input: &mut R,
    ) -> Result<V5G0StreamedArtifact> {
        let template = self.population_template()?;
        let mut semantic_writer = CanonicalSha256Writer::default();
        self.stream_population_document_from_fragments(
            &template,
            fragments,
            source,
            &mut semantic_writer,
        )?;
        let semantic_sha256 = semantic_writer.finish();
        let mut final_template = template;
        final_template
            .as_object_mut()
            .expect("population template is object")
            .insert(
                "populationSha256".to_owned(),
                Value::String(semantic_sha256.clone()),
            );
        verify_streamed_artifact(input, &semantic_sha256, |writer| {
            self.stream_population_document_from_fragments(
                &final_template,
                fragments,
                source,
                writer,
            )
        })
    }

    pub fn verify_evaluation_population_from_fragments<
        S: V5G0PublicationFragmentSource,
        R: Read,
    >(
        &self,
        population: &V5G0StreamedArtifact,
        fragments: &V5G0PublicationFragments,
        source: &mut S,
        input: &mut R,
    ) -> Result<V5G0StreamedArtifact> {
        let template = self.evaluation_template(population)?;
        let mut semantic_writer = CanonicalSha256Writer::default();
        self.stream_evaluation_document_from_fragments(
            &template,
            fragments,
            source,
            &mut semantic_writer,
        )?;
        let semantic_sha256 = semantic_writer.finish();
        let mut final_template = template;
        final_template
            .as_object_mut()
            .expect("evaluation template is object")
            .insert(
                "evaluationPopulationSha256".to_owned(),
                Value::String(semantic_sha256.clone()),
            );
        verify_streamed_artifact(input, &semantic_sha256, |writer| {
            self.stream_evaluation_document_from_fragments(
                &final_template,
                fragments,
                source,
                writer,
            )
        })
    }

    pub fn verify_generation_journal_from_fragments<S: V5G0PublicationFragmentSource, R: Read>(
        &self,
        population: &V5G0StreamedArtifact,
        evaluation_population: &V5G0StreamedArtifact,
        fragments: &V5G0PublicationFragments,
        source: &mut S,
        input: &mut R,
    ) -> Result<V5G0StreamedArtifact> {
        let template = self.generation_journal_template(population, evaluation_population)?;
        let mut semantic_writer = CanonicalSha256Writer::default();
        self.stream_generation_journal_document_from_fragments(
            &template,
            fragments,
            source,
            &mut semantic_writer,
        )?;
        let semantic_sha256 = semantic_writer.finish();
        let mut final_template = template;
        final_template
            .as_object_mut()
            .expect("generation journal template is object")
            .insert(
                "journalSha256".to_owned(),
                Value::String(semantic_sha256.clone()),
            );
        verify_streamed_artifact(input, &semantic_sha256, |writer| {
            self.stream_generation_journal_document_from_fragments(
                &final_template,
                fragments,
                source,
                writer,
            )
        })
    }

    fn population_template(&self) -> Result<Value> {
        crate::publication::population_template(
            &self.publication_request,
            &self.selected_references,
            Some(&self.g0_binding),
            None,
        )
        .map_err(|error| contract(format!("v5 G0 population template failed: {error}")))
    }

    fn evaluation_template(&self, population: &V5G0StreamedArtifact) -> Result<Value> {
        if population.semantic_sha256.is_empty() || population.file_sha256.is_empty() {
            return Err(contract("v5 G0 population receipt is incomplete"));
        }
        let policy = &self.publication_request.policy;
        let mut value = object([
            (
                "schemaVersion",
                Value::String(crate::publication::EVALUATION_POPULATION_SCHEMA.to_owned()),
            ),
            (
                "generationIndex",
                Value::from(self.publication_request.generation_index),
            ),
            (
                "candidateCount",
                Value::from(self.selected_references.len() as u64),
            ),
            (
                "populationSha256",
                Value::String(population.semantic_sha256.clone()),
            ),
            (
                "populationFileSha256",
                Value::String(population.file_sha256.clone()),
            ),
            (
                "pairGenerationConfigSha256",
                Value::String(self.publication_request.config_sha256.clone()),
            ),
            ("policyName", Value::String(policy.policy_name.clone())),
            ("policySha256", Value::String(policy.policy_sha256.clone())),
            ("bidirectionalPairPolicy", policy.pair_policy.clone()),
            (
                "pairPolicySha256",
                Value::String(canonical_sha256(&policy.pair_policy)?),
            ),
            (
                "operatorImplementationSha256",
                Value::String(canonical_sha256(&policy.operator_implementation_identity)?),
            ),
            ("predeclaredEvidenceContextSha256", Value::Null),
            ("candidates", Value::Null),
            (
                "proposalAttempts",
                Value::from(self.selected_references.len() as u64),
            ),
            ("funnelEntries", Value::Null),
        ]);
        value.as_object_mut().expect("evaluation is object").insert(
            "g0Bootstrap".to_owned(),
            Value::Object(
                self.g0_binding
                    .iter()
                    .map(|(key, value)| (key.clone(), Value::String(value.clone())))
                    .collect(),
            ),
        );
        if let Some(authority) = &policy.archive_policy_authority {
            value
                .as_object_mut()
                .expect("evaluation is object")
                .insert("archivePolicyAuthority".to_owned(), authority.clone());
        }
        Ok(value)
    }

    fn generation_journal_template(
        &self,
        population: &V5G0StreamedArtifact,
        evaluation_population: &V5G0StreamedArtifact,
    ) -> Result<Value> {
        let policy = &self.publication_request.policy;
        let selected_entry_sha256s = self
            .selected_references
            .iter()
            .map(|reference| Value::String(reference.entry_sha256.clone()))
            .collect::<Vec<_>>();
        let selected_count = self.selected_references.len() as u64;
        let mut value = object([
            (
                "schemaVersion",
                Value::String(crate::publication::GENERATION_JOURNAL_SCHEMA.to_owned()),
            ),
            ("qdVersion", Value::String(policy.qd_version.clone())),
            ("policyName", Value::String(policy.policy_name.clone())),
            ("policySha256", Value::String(policy.policy_sha256.clone())),
            (
                "configSha256",
                Value::String(self.publication_request.config_sha256.clone()),
            ),
            (
                "generationIndex",
                Value::from(self.publication_request.generation_index),
            ),
            ("proposalCount", Value::from(selected_count)),
            ("acceptedCount", Value::from(selected_count)),
            (
                "maxProposalAttempts",
                Value::from(self.publication_request.max_proposal_attempts),
            ),
            ("nextImmigrantContinuationOrdinal", Value::from(0_u64)),
            (
                "originProposalCounts",
                count_map_value(&self.publication_request.origin_proposal_counts),
            ),
            (
                "originAcceptedCounts",
                count_map_value(&BTreeMap::from([(
                    "random_immigrant".to_owned(),
                    selected_count,
                )])),
            ),
            (
                "dispositionCounts",
                count_map_value(&self.publication_request.disposition_counts),
            ),
            (
                "proposalSlots",
                g0_proposal_slots(&self.publication_request, selected_count),
            ),
            (
                "uniqueIdentityCounts",
                object([
                    ("candidateIdentity", Value::from(selected_count)),
                    (
                        "pairGenome",
                        Value::from(
                            self.publication_request
                                .unique_pair_genome_count
                                .unwrap_or(0),
                        ),
                    ),
                ]),
            ),
            (
                "duplicateCounters",
                object([
                    (
                        "candidateIdentity",
                        Value::from(
                            self.publication_request
                                .disposition_counts
                                .get("duplicate_candidate_identity")
                                .copied()
                                .unwrap_or(0),
                        ),
                    ),
                    (
                        "pairGenome",
                        Value::from(
                            self.publication_request
                                .disposition_counts
                                .get("duplicate_pair_genome")
                                .copied()
                                .unwrap_or(0),
                        ),
                    ),
                    (
                        "pairGenomeGlobal",
                        Value::from(
                            self.publication_request
                                .disposition_counts
                                .get("duplicate_pair_genome_global")
                                .copied()
                                .unwrap_or(0),
                        ),
                    ),
                ]),
            ),
            (
                "proposalSlotCounters",
                object([
                    (
                        "proposalsObserved",
                        Value::from(self.publication_request.proposal_count),
                    ),
                    (
                        "maxProposalAttempts",
                        Value::from(self.publication_request.max_proposal_attempts),
                    ),
                ]),
            ),
            ("entrySha256s", Value::Array(selected_entry_sha256s)),
            ("evaluationCandidateBindings", Value::Null),
            (
                "operatorImplementation",
                policy.operator_implementation_identity.clone(),
            ),
            (
                "populationSha256",
                Value::String(population.semantic_sha256.clone()),
            ),
            (
                "populationFileSha256",
                Value::String(population.file_sha256.clone()),
            ),
            (
                "evaluationPopulationSha256",
                Value::String(evaluation_population.semantic_sha256.clone()),
            ),
            ("predeclaredEvidenceContextSha256", Value::Null),
        ]);
        let fields = value.as_object_mut().expect("journal is object");
        fields.insert(
            "g0Bootstrap".to_owned(),
            Value::Object(
                self.g0_binding
                    .iter()
                    .map(|(key, value)| (key.clone(), Value::String(value.clone())))
                    .collect(),
            ),
        );
        fields.insert(
            "constructionProposalCount".to_owned(),
            Value::from(self.publication_request.proposal_count),
        );
        fields.insert(
            "constructedAcceptedCount".to_owned(),
            Value::from(self.publication_request.construction_references.len() as u64),
        );
        fields.insert(
            "constructionOriginAcceptedCounts".to_owned(),
            count_map_value(&self.publication_request.origin_accepted_counts),
        );
        fields.insert(
            "constructionEntrySha256s".to_owned(),
            Value::Array(
                self.publication_request
                    .entry_sha256s
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
        fields.insert(
            "reproductionAllocation".to_owned(),
            self.publication_request
                .reproduction_allocation
                .clone()
                .expect("prepared request has allocation"),
        );
        fields.insert(
            "reproductionAllocationAccounting".to_owned(),
            self.reproduction_allocation_accounting.clone(),
        );
        if let Some(authority) = &policy.archive_policy_authority {
            fields.insert("archivePolicyAuthority".to_owned(), authority.clone());
        }
        fields.insert(
            "globalIdentityLedger".to_owned(),
            self.publication_request
                .global_identity_ledger
                .clone()
                .expect("prepared request has compact ledger summary"),
        );
        Ok(value)
    }

    fn stream_population_document<W: Write>(
        &self,
        template: &Value,
        self_hash_field: Option<&str>,
        output: &mut W,
    ) -> Result<()> {
        stream_object(template, self_hash_field, output, |key, writer| {
            if key != "candidates" {
                return Ok(false);
            }
            writer.write_all(b"[")?;
            let mut first = true;
            self.for_each_selected_with(|materialization| {
                if !first {
                    writer.write_all(b",")?;
                }
                first = false;
                write_canonical_json(&materialization.rich_evaluation_candidate, writer)?;
                Ok(())
            })?;
            writer.write_all(b"]")?;
            Ok(true)
        })
    }

    fn stream_evaluation_document<W: Write>(
        &self,
        template: &Value,
        self_hash_field: Option<&str>,
        output: &mut W,
    ) -> Result<()> {
        stream_object(template, self_hash_field, output, |key, writer| match key {
            "candidates" => {
                writer.write_all(b"[")?;
                let mut first = true;
                self.for_each_selected_with(|materialization| {
                    if !first {
                        writer.write_all(b",")?;
                    }
                    first = false;
                    write_canonical_json(
                        required(
                            &materialization.publication_precomputed_row,
                            "evaluationCandidate",
                            "v5 selected publication row",
                        )?,
                        writer,
                    )?;
                    Ok(())
                })?;
                writer.write_all(b"]")?;
                Ok(true)
            }
            "funnelEntries" => {
                writer.write_all(b"[")?;
                let mut first = true;
                self.for_each_selected_with(|materialization| {
                    if !first {
                        writer.write_all(b",")?;
                    }
                    first = false;
                    write_canonical_json(
                        required(
                            &materialization.publication_precomputed_row,
                            "funnelEntry",
                            "v5 selected publication row",
                        )?,
                        writer,
                    )?;
                    Ok(())
                })?;
                writer.write_all(b"]")?;
                Ok(true)
            }
            _ => Ok(false),
        })
    }

    fn stream_generation_journal_document<W: Write>(
        &self,
        template: &Value,
        self_hash_field: Option<&str>,
        output: &mut W,
    ) -> Result<()> {
        stream_object(template, self_hash_field, output, |key, writer| {
            if key != "evaluationCandidateBindings" {
                return Ok(false);
            }
            writer.write_all(b"[")?;
            let mut first = true;
            self.for_each_selected_with(|materialization| {
                if !first {
                    writer.write_all(b",")?;
                }
                first = false;
                let candidate = required(
                    &materialization.publication_precomputed_row,
                    "evaluationCandidate",
                    "v5 selected publication row",
                )?;
                let fields = object_ref(candidate, "v5 selected evaluation candidate")?;
                let candidate_id = fields.get("candidateId").cloned().ok_or_else(|| {
                    contract("v5 selected evaluation candidate lacks candidate ID")
                })?;
                let proposal_ordinal = fields
                    .get("proposalOrdinal")
                    .cloned()
                    .ok_or_else(|| contract("v5 selected evaluation candidate lacks ordinal"))?;
                let entry_sha256 = fields.get("proposalEntrySha256").cloned().ok_or_else(|| {
                    contract("v5 selected evaluation candidate lacks entry SHA-256")
                })?;
                write_canonical_json(
                    &object([
                        ("candidateId", candidate_id),
                        ("proposalOrdinal", proposal_ordinal),
                        ("proposalEntrySha256", entry_sha256),
                        (
                            "candidateProjectionSha256",
                            Value::String(canonical_sha256(candidate)?),
                        ),
                    ]),
                    writer,
                )?;
                Ok(())
            })?;
            writer.write_all(b"]")?;
            Ok(true)
        })
    }

    fn stream_population_document_from_fragments<S: V5G0PublicationFragmentSource, W: Write>(
        &self,
        template: &Value,
        fragments: &V5G0PublicationFragments,
        source: &mut S,
        output: &mut W,
    ) -> Result<()> {
        stream_object(template, None, output, |key, writer| {
            if key != "candidates" {
                return Ok(false);
            }
            writer.write_all(b"[")?;
            copy_fragment_checked(
                source,
                fragments.fragment(V5G0PublicationFragmentKind::PopulationCandidates),
                writer,
            )?;
            writer.write_all(b"]")?;
            Ok(true)
        })
    }

    fn stream_evaluation_document_from_fragments<S: V5G0PublicationFragmentSource, W: Write>(
        &self,
        template: &Value,
        fragments: &V5G0PublicationFragments,
        source: &mut S,
        output: &mut W,
    ) -> Result<()> {
        stream_object(template, None, output, |key, writer| {
            let kind = match key {
                "candidates" => V5G0PublicationFragmentKind::EvaluationCandidates,
                "funnelEntries" => V5G0PublicationFragmentKind::EvaluationFunnelEntries,
                _ => return Ok(false),
            };
            writer.write_all(b"[")?;
            copy_fragment_checked(source, fragments.fragment(kind), writer)?;
            writer.write_all(b"]")?;
            Ok(true)
        })
    }

    fn stream_generation_journal_document_from_fragments<
        S: V5G0PublicationFragmentSource,
        W: Write,
    >(
        &self,
        template: &Value,
        fragments: &V5G0PublicationFragments,
        source: &mut S,
        output: &mut W,
    ) -> Result<()> {
        stream_object(template, None, output, |key, writer| {
            if key != "evaluationCandidateBindings" {
                return Ok(false);
            }
            writer.write_all(b"[")?;
            copy_fragment_checked(
                source,
                fragments.fragment(V5G0PublicationFragmentKind::GenerationJournalBindings),
                writer,
            )?;
            writer.write_all(b"]")?;
            Ok(true)
        })
    }
}

fn g0_proposal_slots(request: &PublicationRequest, selected: u64) -> Value {
    object([
        ("targetUniqueCandidates", Value::from(selected)),
        ("acceptedUniqueCandidates", Value::from(selected)),
        ("proposalAttempts", Value::from(request.proposal_count)),
        (
            "maxProposalAttempts",
            Value::from(request.max_proposal_attempts),
        ),
        (
            "remainingUniqueCandidateSlots",
            Value::from(request.target_unique_candidates.saturating_sub(selected)),
        ),
        (
            "constructionPoolSize",
            Value::from(request.target_unique_candidates),
        ),
        (
            "constructedAcceptedCount",
            Value::from(request.construction_references.len() as u64),
        ),
        ("evaluationPopulationSize", Value::from(selected)),
    ])
}

fn stream_object<W: Write, F>(
    value: &Value,
    omitted_self_hash: Option<&str>,
    output: &mut W,
    mut dynamic: F,
) -> Result<()>
where
    F: FnMut(&str, &mut W) -> Result<bool>,
{
    let fields = object_ref(value, "v5 G0 streamed document")?;
    let ordered = fields
        .iter()
        .filter(|(key, _)| Some(key.as_str()) != omitted_self_hash)
        .map(|(key, value)| (key.as_str(), value))
        .collect::<BTreeMap<_, _>>();
    output.write_all(b"{")?;
    let mut first = true;
    for (key, value) in ordered {
        if !first {
            output.write_all(b",")?;
        }
        first = false;
        write_canonical_json(&Value::String(key.to_owned()), output)?;
        output.write_all(b":")?;
        if !dynamic(key, output)? {
            write_canonical_json(value, output)?;
        }
    }
    output.write_all(b"}")?;
    Ok(())
}

struct FragmentHashingWriter<'a, W: Write> {
    output: &'a mut W,
    hash: CanonicalSha256Writer,
    encoded_bytes: u64,
}

impl<W: Write> Write for FragmentHashingWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.output.write_all(bytes)?;
        self.hash.write_all(bytes)?;
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("v5 G0 fragment byte count overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.output.flush()?;
        self.hash.flush()
    }
}

fn copy_fragment_checked<S: V5G0PublicationFragmentSource, W: Write>(
    source: &mut S,
    expected: &V5G0PublicationFragment,
    output: &mut W,
) -> Result<()> {
    expected.validate(expected.kind)?;
    let (actual_sha256, actual_bytes) = {
        let mut writer = FragmentHashingWriter {
            output,
            hash: CanonicalSha256Writer::default(),
            encoded_bytes: 0,
        };
        source.copy_fragment(expected.kind, &mut writer)?;
        writer.flush()?;
        (writer.hash.finish(), writer.encoded_bytes)
    };
    if actual_sha256 != expected.fragment_sha256 || actual_bytes != expected.encoded_bytes {
        return Err(contract(format!(
            "v5 G0 {} fragment bytes do not match the authenticated materialization receipt",
            expected.kind.as_str()
        )));
    }
    Ok(())
}

struct TeeWriter<'a, W: Write> {
    output: &'a mut W,
    hash: &'a mut CanonicalSha256Writer,
    bytes: &'a mut u64,
}

impl<W: Write> Write for TeeWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.output.write_all(bytes)?;
        self.hash.write_all(bytes)?;
        *self.bytes = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("v5 G0 artifact byte count overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.output.flush()?;
        self.hash.flush()
    }
}

fn write_static_artifact<W: Write>(
    value: &Value,
    semantic_sha256: &str,
    output: &mut W,
) -> Result<V5G0StreamedArtifact> {
    write_streamed_artifact(output, semantic_sha256, |writer| {
        Ok(write_canonical_json(value, writer)?)
    })
}

fn write_streamed_artifact<W: Write, F>(
    output: &mut W,
    semantic_sha256: &str,
    stream: F,
) -> Result<V5G0StreamedArtifact>
where
    F: FnOnce(&mut TeeWriter<'_, W>) -> Result<()>,
{
    let semantic_sha256 = exact_sha(
        &Value::String(semantic_sha256.to_owned()),
        "v5 G0 streamed artifact semantic SHA-256",
    )?;
    let mut file_hasher = CanonicalSha256Writer::default();
    let mut encoded_bytes = 0_u64;
    {
        let mut tee = TeeWriter {
            output,
            hash: &mut file_hasher,
            bytes: &mut encoded_bytes,
        };
        stream(&mut tee)?;
        tee.write_all(b"\n")?;
        tee.flush()?;
    }
    Ok(V5G0StreamedArtifact {
        semantic_sha256,
        file_sha256: file_hasher.finish(),
        encoded_bytes,
    })
}

struct MatchingWriter<'a, R: Read> {
    input: &'a mut R,
    file_hash: CanonicalSha256Writer,
    encoded_bytes: u64,
}

impl<R: Read> MatchingWriter<'_, R> {
    fn finish(self) -> Result<V5G0StreamedArtifact> {
        let mut extra = [0_u8; 1];
        if self.input.read(&mut extra)? != 0 {
            return Err(contract("staged v5 G0 artifact has trailing bytes"));
        }
        Ok(V5G0StreamedArtifact {
            semantic_sha256: String::new(),
            file_sha256: self.file_hash.finish(),
            encoded_bytes: self.encoded_bytes,
        })
    }
}

impl<R: Read> Write for MatchingWriter<'_, R> {
    fn write(&mut self, expected: &[u8]) -> std::io::Result<usize> {
        let mut actual = vec![0_u8; expected.len()];
        self.input.read_exact(&mut actual)?;
        if actual != expected {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "staged v5 G0 artifact differs from canonical core stream",
            ));
        }
        self.file_hash.write_all(expected)?;
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(expected.len() as u64)
            .ok_or_else(|| std::io::Error::other("v5 G0 artifact byte count overflow"))?;
        Ok(expected.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn verify_static_artifact<R: Read>(
    value: &Value,
    semantic_sha256: &str,
    input: &mut R,
) -> Result<V5G0StreamedArtifact> {
    verify_streamed_artifact(input, semantic_sha256, |writer| {
        Ok(write_canonical_json(value, writer)?)
    })
}

fn verify_streamed_artifact<R: Read, F>(
    input: &mut R,
    semantic_sha256: &str,
    stream: F,
) -> Result<V5G0StreamedArtifact>
where
    F: FnOnce(&mut MatchingWriter<'_, R>) -> Result<()>,
{
    let semantic_sha256 = exact_sha(
        &Value::String(semantic_sha256.to_owned()),
        "v5 G0 staged artifact semantic SHA-256",
    )?;
    let mut writer = MatchingWriter {
        input,
        file_hash: CanonicalSha256Writer::default(),
        encoded_bytes: 0,
    };
    stream(&mut writer)?;
    writer.write_all(b"\n")?;
    let mut artifact = writer.finish()?;
    artifact.semantic_sha256 = semantic_sha256;
    Ok(artifact)
}

/// Stream-check a public artifact against an immutable receipt without
/// constructing an expected document.  This is the recovery-only counterpart
/// to `verify_streamed_artifact`: the receipt was minted by the fresh
/// fragment verifier before publication, and binds the exact canonical file
/// SHA/length after private fragments are no longer available.
fn verify_receipted_streamed_artifact<R: Read>(
    input: &mut R,
    expected: &V5G0StreamedArtifact,
    label: &str,
) -> Result<()> {
    expected.validate()?;
    let mut hasher = CanonicalSha256Writer::default();
    let mut encoded_bytes = 0_u64;
    let mut final_byte = None;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.write_all(&buffer[..read])?;
        encoded_bytes = encoded_bytes
            .checked_add(read as u64)
            .ok_or_else(|| contract(format!("{label} byte count overflowed")))?;
        final_byte = buffer[..read].last().copied();
    }
    if encoded_bytes != expected.encoded_bytes
        || hasher.finish() != expected.file_sha256
        || final_byte != Some(b'\n')
    {
        return Err(contract(format!(
            "{label} bytes do not match the authenticated v5 G0 publication receipt"
        )));
    }
    Ok(())
}

/// Typed aggregate returned by the streaming verifier.  It contains only
/// compact SHA/length receipts and never holds a rich selected candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5G0PublicationReceipt {
    pub publication_plan_sha256: String,
    pub publication_request_sha256: String,
    pub pair_config: V5G0StreamedArtifact,
    pub population: V5G0StreamedArtifact,
    pub evaluation_population: V5G0StreamedArtifact,
    pub generation_journal: V5G0StreamedArtifact,
}

impl V5G0PublicationReceipt {
    fn semantic_value(&self) -> Result<Value> {
        self.pair_config.validate()?;
        self.population.validate()?;
        self.evaluation_population.validate()?;
        self.generation_journal.validate()?;
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_G0_PUBLICATION_RECEIPT_SCHEMA.to_owned()),
            ),
            (
                "publicationPlanSha256",
                Value::String(exact_sha(
                    &Value::String(self.publication_plan_sha256.clone()),
                    "v5 G0 publication receipt plan SHA-256",
                )?),
            ),
            (
                "publicationRequestSha256",
                Value::String(exact_sha(
                    &Value::String(self.publication_request_sha256.clone()),
                    "v5 G0 publication receipt request SHA-256",
                )?),
            ),
            ("pairConfig", self.pair_config.to_value()?),
            ("population", self.population.to_value()?),
            (
                "evaluationPopulation",
                self.evaluation_population.to_value()?,
            ),
            ("generationJournal", self.generation_journal.to_value()?),
        ]))
    }

    pub fn publication_receipt_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 G0 publication receipt")
            .clone();
        fields.insert(
            "publicationReceiptSha256".to_owned(),
            Value::String(self.publication_receipt_sha256()?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 G0 publication receipt")?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "publicationPlanSha256",
                "publicationRequestSha256",
                "pairConfig",
                "population",
                "evaluationPopulation",
                "generationJournal",
                "publicationReceiptSha256",
            ],
            "v5 G0 publication receipt",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_G0_PUBLICATION_RECEIPT_SCHEMA)
        {
            return Err(contract("v5 G0 publication receipt schema is invalid"));
        }
        let receipt = Self {
            publication_plan_sha256: exact_sha(
                required(value, "publicationPlanSha256", "v5 G0 publication receipt")?,
                "v5 G0 publication receipt plan SHA-256",
            )?,
            publication_request_sha256: exact_sha(
                required(
                    value,
                    "publicationRequestSha256",
                    "v5 G0 publication receipt",
                )?,
                "v5 G0 publication receipt request SHA-256",
            )?,
            pair_config: V5G0StreamedArtifact::from_value(required(
                value,
                "pairConfig",
                "v5 G0 publication receipt",
            )?)?,
            population: V5G0StreamedArtifact::from_value(required(
                value,
                "population",
                "v5 G0 publication receipt",
            )?)?,
            evaluation_population: V5G0StreamedArtifact::from_value(required(
                value,
                "evaluationPopulation",
                "v5 G0 publication receipt",
            )?)?,
            generation_journal: V5G0StreamedArtifact::from_value(required(
                value,
                "generationJournal",
                "v5 G0 publication receipt",
            )?)?,
        };
        let supplied = exact_sha(
            required(
                value,
                "publicationReceiptSha256",
                "v5 G0 publication receipt",
            )?,
            "v5 G0 publication receipt SHA-256",
        )?;
        if supplied != receipt.publication_receipt_sha256()? || &receipt.to_value()? != value {
            return Err(contract("v5 G0 publication receipt identity drifted"));
        }
        Ok(receipt)
    }

    /// Canonical object-store binding for the receipt minted by fresh
    /// publication.  Batch places this object beside compact records/deltas
    /// and binds it into its outer receipt; adoption never treats a bare
    /// receipt JSON value as sufficient authority.
    pub fn object_binding(&self) -> Result<V5G0PublicationReceiptObjectBinding> {
        let publication_receipt_sha256 = self.publication_receipt_sha256()?;
        Ok(V5G0PublicationReceiptObjectBinding {
            relative_path: compact_record_object_relative_path(&publication_receipt_sha256)?,
            publication_receipt_sha256,
        })
    }
}

/// Immutable object location for a compact v5 publication receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5G0PublicationReceiptObjectBinding {
    pub publication_receipt_sha256: String,
    pub relative_path: String,
}

impl V5G0PublicationReceiptObjectBinding {
    pub fn to_value(&self) -> Result<Value> {
        let publication_receipt_sha256 = exact_sha(
            &Value::String(self.publication_receipt_sha256.clone()),
            "v5 G0 publication receipt object SHA-256",
        )?;
        let expected = compact_record_object_relative_path(&publication_receipt_sha256)?;
        if self.relative_path != expected {
            return Err(contract("v5 G0 publication receipt object path drifted"));
        }
        Ok(object([
            (
                "publicationReceiptSha256",
                Value::String(publication_receipt_sha256),
            ),
            ("relativePath", Value::String(expected)),
        ]))
    }
}

/// Public no-rich recovery verifier.  It first reconstructs/replays the
/// compact transaction and plan, parses the self-authenticating receipt, then
/// streams public file SHA/length checks.  It does not reopen private
/// fragments or invoke selected-candidate materialization.
#[allow(clippy::too_many_arguments)]
pub fn verify_v5_g0_publication_adoption<
    RPair: Read,
    RPopulation: Read,
    REvaluation: Read,
    RJournal: Read,
>(
    request: &V5G0TransactionRequest,
    transaction: &V5G0TransactionResult,
    receipt_value: &Value,
    pair_config: &mut RPair,
    population: &mut RPopulation,
    evaluation_population: &mut REvaluation,
    generation_journal: &mut RJournal,
) -> Result<V5G0PublicationReceipt> {
    let receipt = V5G0PublicationReceipt::from_value(receipt_value)?;
    let stream = prepare_v5_g0_publication_stream(request, transaction)?;
    stream.verify_adopted_bundle(
        &receipt,
        pair_config,
        population,
        evaluation_population,
        generation_journal,
    )
}
