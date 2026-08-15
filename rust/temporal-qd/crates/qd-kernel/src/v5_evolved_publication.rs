//! Write-neutral public-artifact streaming for a sealed native v5 evolved
//! transaction.
//!
//! The later-generation transaction owns scheduling, parent selection,
//! compiler replay, and compact durable evidence.  This sibling layer owns
//! only the cap-free publication authority and canonical artifact streams.
//! It never opens a filesystem path: `qd-batch` provides private fragment
//! files and public output writers, while core validates every copied byte.
//!
//! In particular, this is not a G0-pool adapter.  Every accepted evolved
//! record is part of the population/evaluation handoff in accepted birth
//! order; there is no construction pool, bootstrap selection, or G0 binding.

use std::{
    collections::BTreeMap,
    io::{Read, Write},
};

use temporal_qd_contract::{
    CanonicalSha256Writer, ContractError, Map, Value, canonical_sha256,
    canonical_sha256_without_object_field, write_canonical_json,
};

use crate::{
    CONTRACT_VERSION,
    factory::ParentReference,
    g0_funnel::{reproduction_allocation_accounting, validate_reproduction_allocation},
    journal::AcceptedReference,
    proposal::{CandidateIdentityLedger, IdentityLedger},
    publication::{PublicationPolicy, PublicationRequest},
    v5::{
        V5_PROPOSAL_FUNNEL_ENTRY_SCHEMA, V5AttemptOutcomeAudit, V5EvolvedAcceptedMaterial,
        V5FunnelAdmission, V5ProposalAttemptRecord, V5SharedConstructionAuthority,
        materialize_v5_evolved_rich_candidate, parent_reference_from_v5_evolved_material,
        v5_funnel_candidate_projection, v5_native_object_relative_path,
    },
    v5_evolved_transaction::{
        V5EvolvedAcceptedReplaySink, V5EvolvedTransactionError, V5EvolvedTransactionRequest,
        V5EvolvedTransactionResult, replay_v5_evolved_transaction_with_accepted_sink,
    },
};

/// Optional one-pass sink for accepted compiler-owned parent references.
/// Fast-ephemeral publication persists these while the verified material is
/// already live; durable publication uses the no-op implementation below.
pub trait V5EvolvedParentReferenceSink {
    fn write_parent_reference(&mut self, reference: &ParentReference) -> std::io::Result<()>;
}

struct NoopV5EvolvedParentReferenceSink;

impl V5EvolvedParentReferenceSink for NoopV5EvolvedParentReferenceSink {
    fn write_parent_reference(&mut self, _reference: &ParentReference) -> std::io::Result<()> {
        Ok(())
    }
}

/// Self-hashed, cap-free publication authority for a later-generation v5
/// transaction.  The outer manifest remains execution telemetry and may bind
/// `threadCap`; this plan deliberately does not.
pub const V5_EVOLVED_PUBLICATION_PLAN_SCHEMA: &str = "temporal_qd_v5_evolved_publication_plan_v1";
/// Cap-free semantic request identity for public evolved artifacts.  This is
/// a versioned successor to the manifest-bound pre-v5 request identity.
pub const V5_EVOLVED_PUBLICATION_REQUEST_SCHEMA: &str =
    "temporal_qd_v5_evolved_publication_request_v1";
/// Immutable compact receipt minted after staged public artifact verification.
pub const V5_EVOLVED_PUBLICATION_RECEIPT_SCHEMA: &str =
    "temporal_qd_v5_evolved_publication_stream_receipt_v1";
pub const V5_EVOLVED_PUBLICATION_FRAGMENTS_SCHEMA: &str =
    "temporal_qd_v5_evolved_publication_fragments_v2";
/// Fixed public handoff for the compact identity state which becomes the
/// authenticated identity-ledger input of the next evolved generation.
pub const V5_EVOLVED_IDENTITY_LEDGER_SCHEMA: &str = "temporal_qd_v5_evolved_identity_ledger_v1";
/// The supervisor-facing public location is deliberately stable.  Unlike the
/// transaction's schedule receipt, this document is an explicit publication
/// artifact and is safe to promote as the next generation's ledger input.
pub const V5_EVOLVED_IDENTITY_LEDGER_RELATIVE_PATH: &str = "v5-native/identity-ledger.json";

#[cfg(test)]
mod materialization_test_observer {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub static ACCEPTED_SINK_VISITS: AtomicU64 = AtomicU64::new(0);
    pub static RICH_MATERIALIZATIONS: AtomicU64 = AtomicU64::new(0);
    pub static PEAK_LIVE: AtomicU64 = AtomicU64::new(0);

    pub fn reset() {
        ACCEPTED_SINK_VISITS.store(0, Ordering::SeqCst);
        RICH_MATERIALIZATIONS.store(0, Ordering::SeqCst);
        PEAK_LIVE.store(0, Ordering::SeqCst);
    }

    pub fn observe_visit(live: u64) {
        ACCEPTED_SINK_VISITS.fetch_add(1, Ordering::SeqCst);
        PEAK_LIVE.fetch_max(live, Ordering::SeqCst);
    }

    pub fn observe_rich_materialization() {
        RICH_MATERIALIZATIONS.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum V5EvolvedPublicationError {
    #[error("v5 construction failure: {0}")]
    V5(#[from] crate::v5::V5Error),
    #[error("v5 evolved transaction failure: {0}")]
    Transaction(#[from] V5EvolvedTransactionError),
    #[error("canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("v5 evolved publication I/O failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("v5 evolved publication contract failure: {0}")]
    Contract(String),
}

pub type Result<T> = std::result::Result<T, V5EvolvedPublicationError>;

fn contract(message: impl Into<String>) -> V5EvolvedPublicationError {
    V5EvolvedPublicationError::Contract(message.into())
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
    let value = value
        .as_str()
        .filter(|value| !value.trim().is_empty() && *value == value.trim())
        .ok_or_else(|| contract(format!("{label} must be a canonical nonempty string")))?;
    Ok(value.to_owned())
}

fn exact_sha(value: &Value, label: &str) -> Result<String> {
    let value = exact_text(value, label)?;
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value.as_bytes()[7..]
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
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

/// Raw canonical manifest values transported to core before it derives the
/// only accepted self-hashed evolved publication plan.  Python/batch may
/// copy these authenticated values but cannot author a sealed plan.
#[derive(Clone, Debug, PartialEq)]
pub struct V5EvolvedPublicationInputs {
    pub generation_config: Value,
    pub final_newline: String,
    pub execution_authority: Value,
    pub inputs: Value,
}

impl V5EvolvedPublicationInputs {
    pub fn from_manifest_values(
        generation_config: &Value,
        final_newline: &Value,
        execution_authority: &Value,
        inputs: &Value,
    ) -> Result<Self> {
        let inputs = Self {
            generation_config: generation_config.clone(),
            final_newline: exact_text(final_newline, "v5 evolved publication final newline")?,
            execution_authority: execution_authority.clone(),
            inputs: inputs.clone(),
        };
        inputs.validate_shape()?;
        Ok(inputs)
    }

    fn validate_shape(&self) -> Result<()> {
        if self.final_newline != "lf" {
            return Err(contract(
                "v5 evolved publication requires canonical LF final newline",
            ));
        }
        let config = object_ref(
            &self.generation_config,
            "v5 evolved publication generation config",
        )?;
        if config.get("schemaVersion").and_then(Value::as_str)
            != Some("temporal_qd_pair_generation_v2")
        {
            return Err(contract(
                "v5 evolved publication generation config schema is invalid",
            ));
        }
        let config_sha = exact_sha(
            config
                .get("configSha256")
                .ok_or_else(|| contract("v5 evolved publication config lacks configSha256"))?,
            "v5 evolved publication generation config SHA-256",
        )?;
        if canonical_sha256_without_object_field(&self.generation_config, "configSha256")?
            != config_sha
        {
            return Err(contract(
                "v5 evolved publication generation config self-hash drifted",
            ));
        }
        validate_evolved_inputs(&self.inputs)?;
        if !self.execution_authority.is_object() {
            return Err(contract(
                "v5 evolved publication execution authority must be an object",
            ));
        }
        Ok(())
    }
}

/// The cap-free, self-hashed closure used to construct and later authenticate
/// public evolved artifacts.  It retains the full configuration, frozen
/// authority, native execution provenance, and immutable archive/ledger
/// input bindings verbatim, so a batch sink never needs to infer policy or
/// reproduction semantics.
#[derive(Clone, Debug, PartialEq)]
pub struct V5EvolvedPublicationPlan {
    pub frozen_authority: Value,
    pub generation_config: Value,
    pub generation_config_sha256: String,
    pub generation_index: u64,
    pub target_unique_candidates: u64,
    pub max_proposal_attempts: u64,
    pub evaluation_population_size: u64,
    pub final_newline: String,
    pub execution_authority: Value,
    pub inputs: Value,
}

impl V5EvolvedPublicationPlan {
    /// Derive the only accepted evolved plan from the transaction’s sealed
    /// authority/bounds and raw authenticated manifest inputs.
    pub fn derive(
        request: &V5EvolvedTransactionRequest,
        inputs: &V5EvolvedPublicationInputs,
    ) -> Result<Self> {
        inputs.validate_shape()?;
        let authority =
            V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)
                .map_err(V5EvolvedTransactionError::from)?;
        let plan = Self {
            frozen_authority: request.shared_authority.clone(),
            generation_config: inputs.generation_config.clone(),
            generation_config_sha256: request.generation_config_sha256.clone(),
            generation_index: request.generation_index,
            target_unique_candidates: request.target_accepted,
            max_proposal_attempts: request.max_attempts,
            evaluation_population_size: request.evaluation_width,
            final_newline: inputs.final_newline.clone(),
            execution_authority: inputs.execution_authority.clone(),
            inputs: inputs.inputs.clone(),
        };
        plan.validate_shape()?;
        plan.validate_against_transaction(request, &authority)?;
        Ok(plan)
    }

    fn semantic_value(&self) -> Result<Value> {
        self.validate_shape()?;
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_PUBLICATION_PLAN_SCHEMA.to_owned()),
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
            .expect("constructed v5 evolved publication plan")
            .clone();
        fields.insert(
            "publicationPlanSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 evolved publication plan")?;
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
            "v5 evolved publication plan",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_PUBLICATION_PLAN_SCHEMA)
        {
            return Err(contract("v5 evolved publication plan schema is invalid"));
        }
        let plan = Self {
            frozen_authority: required(value, "frozenAuthority", "v5 evolved publication plan")?
                .clone(),
            generation_config: required(value, "generationConfig", "v5 evolved publication plan")?
                .clone(),
            generation_config_sha256: exact_sha(
                required(
                    value,
                    "generationConfigSha256",
                    "v5 evolved publication plan",
                )?,
                "v5 evolved publication plan generation config SHA-256",
            )?,
            generation_index: required(value, "generationIndex", "v5 evolved publication plan")?
                .as_u64()
                .ok_or_else(|| {
                    contract("v5 evolved publication plan generation index is invalid")
                })?,
            target_unique_candidates: required(
                value,
                "targetUniqueCandidates",
                "v5 evolved publication plan",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 evolved publication plan target is invalid"))?,
            max_proposal_attempts: required(
                value,
                "maxProposalAttempts",
                "v5 evolved publication plan",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 evolved publication plan attempt ceiling is invalid"))?,
            evaluation_population_size: required(
                value,
                "evaluationPopulationSize",
                "v5 evolved publication plan",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 evolved publication plan evaluation width is invalid"))?,
            final_newline: exact_text(
                required(value, "finalNewline", "v5 evolved publication plan")?,
                "v5 evolved publication plan final newline",
            )?,
            execution_authority: required(
                value,
                "executionAuthority",
                "v5 evolved publication plan",
            )?
            .clone(),
            inputs: required(value, "inputs", "v5 evolved publication plan")?.clone(),
        };
        let supplied = exact_sha(
            required(
                value,
                "publicationPlanSha256",
                "v5 evolved publication plan",
            )?,
            "v5 evolved publication plan SHA-256",
        )?;
        if supplied != plan.publication_plan_sha256()? || &plan.to_value()? != value {
            return Err(contract("v5 evolved publication plan identity drifted"));
        }
        Ok(plan)
    }

    pub fn validate_shape(&self) -> Result<()> {
        if self.generation_index < 2
            || self.target_unique_candidates == 0
            || self.max_proposal_attempts < self.target_unique_candidates
            || self.evaluation_population_size == 0
            || self.evaluation_population_size != self.target_unique_candidates
            || self.final_newline != "lf"
        {
            return Err(contract(
                "v5 evolved publication plan dimensions/newline are invalid",
            ));
        }
        let frozen = object_ref(
            &self.frozen_authority,
            "v5 evolved publication frozen authority",
        )?;
        exact_keys(
            frozen,
            &["schemaVersion", "authority", "authoritySha256"],
            "v5 evolved publication frozen authority",
        )?;
        if frozen.get("schemaVersion").and_then(Value::as_str)
            != Some("temporal_qd_v5_shared_authority_object_v1")
        {
            return Err(contract(
                "v5 evolved publication frozen authority schema is invalid",
            ));
        }
        let frozen_sha = exact_sha(
            frozen.get("authoritySha256").ok_or_else(|| {
                contract("v5 evolved publication frozen authority lacks identity")
            })?,
            "v5 evolved publication frozen authority SHA-256",
        )?;
        if canonical_sha256(
            frozen
                .get("authority")
                .ok_or_else(|| contract("v5 evolved publication frozen authority lacks body"))?,
        )? != frozen_sha
        {
            return Err(contract(
                "v5 evolved publication frozen authority identity drifted",
            ));
        }
        let authority = V5SharedConstructionAuthority::from_shared_object(&self.frozen_authority)
            .map_err(V5EvolvedTransactionError::from)?;
        if authority.shared_authority_sha256 != frozen_sha {
            return Err(contract(
                "v5 evolved publication parsed authority identity drifted",
            ));
        }
        let config = object_ref(
            &self.generation_config,
            "v5 evolved publication generation config",
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
                "v5 evolved publication generation config binding drifted",
            ));
        }
        let allocation = config.get("reproductionAllocation").ok_or_else(|| {
            contract("v5 evolved publication config lacks reproduction allocation")
        })?;
        validate_reproduction_allocation(allocation).map_err(|error| {
            contract(format!(
                "v5 evolved publication allocation is invalid: {error}"
            ))
        })?;
        let (parent_binding, ledger_binding) = validate_evolved_inputs(&self.inputs)?;
        validate_execution_authority(
            &self.execution_authority,
            &frozen_sha,
            &self.generation_config_sha256,
        )?;
        if parent_binding.is_empty() || ledger_binding.is_empty() {
            return Err(contract(
                "v5 evolved publication inputs have empty binding identity",
            ));
        }
        let _ = publication_policy_from_authority(self, &authority)?;
        Ok(())
    }

    /// Validate this self-hashed plan against the sealed transaction request.
    /// `thread_cap` is intentionally not compared because it is execution
    /// telemetry rather than a durable publication semantic.
    pub fn validate_against_transaction(
        &self,
        request: &V5EvolvedTransactionRequest,
        authority: &V5SharedConstructionAuthority,
    ) -> Result<()> {
        self.validate_shape()?;
        if self.frozen_authority != request.shared_authority
            || self.generation_config_sha256 != request.generation_config_sha256
            || self.generation_index != request.generation_index
            || self.target_unique_candidates != request.target_accepted
            || self.max_proposal_attempts != request.max_attempts
            || self.evaluation_population_size != request.evaluation_width
            || authority.shared_authority_sha256
                != exact_sha(
                    required(
                        &self.frozen_authority,
                        "authoritySha256",
                        "v5 evolved publication frozen authority",
                    )?,
                    "v5 evolved publication frozen authority SHA-256",
                )?
        {
            return Err(contract(
                "v5 evolved publication plan does not bind transaction request",
            ));
        }
        let (parent_binding, ledger_binding) = validate_evolved_inputs(&self.inputs)?;
        if parent_binding != request.parent_archive_input_binding_sha256
            || ledger_binding != request.identity_ledger_input_binding_sha256
        {
            return Err(contract(
                "v5 evolved publication plan input bindings drift from transaction request",
            ));
        }
        Ok(())
    }

    /// The cap-free request identity for external public artifacts.  It binds
    /// the complete closed plan, including native execution provenance and
    /// final newline, but intentionally excludes output paths and timing.
    pub fn publication_request_sha256(&self) -> Result<String> {
        let (parent_binding, ledger_binding) = validate_evolved_inputs(&self.inputs)?;
        Ok(canonical_sha256(&object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_PUBLICATION_REQUEST_SCHEMA.to_owned()),
            ),
            (
                "publicationPlanSha256",
                Value::String(self.publication_plan_sha256()?),
            ),
            (
                "sharedAuthoritySha256",
                required(
                    &self.frozen_authority,
                    "authoritySha256",
                    "v5 evolved publication frozen authority",
                )?
                .clone(),
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
            ("finalNewline", Value::String(self.final_newline.clone())),
            ("executionAuthority", self.execution_authority.clone()),
            (
                "parentArchiveInputBindingSha256",
                Value::String(parent_binding),
            ),
            (
                "identityLedgerInputBindingSha256",
                Value::String(ledger_binding),
            ),
        ]))?)
    }

    /// Content-addressed plan object for batch’s immutable object inventory.
    pub fn object_binding(&self) -> Result<V5EvolvedPublicationPlanObjectBinding> {
        let publication_plan_sha256 = self.publication_plan_sha256()?;
        Ok(V5EvolvedPublicationPlanObjectBinding {
            relative_path: v5_native_object_relative_path(&publication_plan_sha256)
                .map_err(V5EvolvedTransactionError::from)?,
            publication_plan_sha256,
        })
    }
}

/// Canonical object-store location for an evolved publication plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5EvolvedPublicationPlanObjectBinding {
    pub publication_plan_sha256: String,
    pub relative_path: String,
}

impl V5EvolvedPublicationPlanObjectBinding {
    pub fn to_value(&self) -> Result<Value> {
        let publication_plan_sha256 = exact_sha(
            &Value::String(self.publication_plan_sha256.clone()),
            "v5 evolved publication plan object SHA-256",
        )?;
        let expected = v5_native_object_relative_path(&publication_plan_sha256)
            .map_err(V5EvolvedTransactionError::from)?;
        if self.relative_path != expected {
            return Err(contract("v5 evolved publication plan object path drifted"));
        }
        Ok(object([
            (
                "publicationPlanSha256",
                Value::String(publication_plan_sha256),
            ),
            ("relativePath", Value::String(expected)),
        ]))
    }
}

fn validate_evolved_inputs(value: &Value) -> Result<(String, String)> {
    let fields = object_ref(value, "v5 evolved publication inputs")?;
    exact_keys(
        fields,
        &["schemaVersion", "parentArchive", "identityLedger"],
        "v5 evolved publication inputs",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_native_v5_proposal_inputs_v1")
    {
        return Err(contract("v5 evolved publication inputs schema is invalid"));
    }
    let parent = validate_input_binding(
        fields
            .get("parentArchive")
            .ok_or_else(|| contract("v5 evolved publication inputs lack parent archive"))?,
        "parentArchive",
    )?;
    let ledger = validate_input_binding(
        fields
            .get("identityLedger")
            .ok_or_else(|| contract("v5 evolved publication inputs lack identity ledger"))?,
        "identityLedger",
    )?;
    Ok((parent, ledger))
}

fn validate_input_binding(value: &Value, expected_kind: &str) -> Result<String> {
    let fields = object_ref(value, "v5 evolved publication input binding")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "kind",
            "absolutePath",
            "fileSha256",
            "semanticSha256",
            "bindingSha256",
        ],
        "v5 evolved publication input binding",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_native_v5_proposal_input_binding_v1")
        || fields.get("kind").and_then(Value::as_str) != Some(expected_kind)
    {
        return Err(contract(
            "v5 evolved publication input binding schema/kind is invalid",
        ));
    }
    let path = exact_text(
        fields
            .get("absolutePath")
            .ok_or_else(|| contract("v5 evolved publication input binding lacks path"))?,
        "v5 evolved publication input binding path",
    )?;
    let normalized = path.replace('\\', "/");
    let windows_absolute = normalized.len() >= 3
        && normalized.as_bytes()[0].is_ascii_alphabetic()
        && normalized.as_bytes()[1] == b':'
        && normalized.as_bytes()[2] == b'/';
    if !(normalized.starts_with('/') || normalized.starts_with("//") || windows_absolute)
        || normalized.split('/').any(|part| matches!(part, "." | ".."))
    {
        return Err(contract(
            "v5 evolved publication input binding path is not safely absolute",
        ));
    }
    for key in ["fileSha256", "semanticSha256", "bindingSha256"] {
        let _ = exact_sha(
            fields.get(key).ok_or_else(|| {
                contract(format!("v5 evolved publication input binding lacks {key}"))
            })?,
            &format!("v5 evolved publication input binding {key}"),
        )?;
    }
    let binding = exact_sha(
        fields
            .get("bindingSha256")
            .expect("checked binding identity field"),
        "v5 evolved publication input binding SHA-256",
    )?;
    if canonical_sha256_without_object_field(value, "bindingSha256")? != binding {
        return Err(contract(
            "v5 evolved publication input binding self-hash drifted",
        ));
    }
    Ok(binding)
}

fn validate_execution_authority(
    value: &Value,
    frozen_authority_sha256: &str,
    generation_config_sha256: &str,
) -> Result<String> {
    let fields = object_ref(value, "v5 evolved publication execution authority")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "nativeBatchAuthority",
            "nativeBatchAuthoritySha256",
            "expectedAuthoritySha256",
            "frozenAuthoritySha256",
            "generationConfigSha256",
            "authoritySha256",
        ],
        "v5 evolved publication execution authority",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_native_v5_proposal_execution_authority_v1")
    {
        return Err(contract(
            "v5 evolved publication execution authority schema is invalid",
        ));
    }
    let batch = fields.get("nativeBatchAuthority").ok_or_else(|| {
        contract("v5 evolved publication execution authority lacks batch authority")
    })?;
    let batch_sha = validate_native_batch_authority(batch)?;
    for key in [
        "nativeBatchAuthoritySha256",
        "expectedAuthoritySha256",
        "frozenAuthoritySha256",
        "generationConfigSha256",
        "authoritySha256",
    ] {
        let _ = exact_sha(
            fields.get(key).ok_or_else(|| {
                contract(format!(
                    "v5 evolved publication execution authority lacks {key}"
                ))
            })?,
            &format!("v5 evolved publication execution authority {key}"),
        )?;
    }
    if fields
        .get("nativeBatchAuthoritySha256")
        .and_then(Value::as_str)
        != Some(&batch_sha)
        || fields
            .get("expectedAuthoritySha256")
            .and_then(Value::as_str)
            != Some(frozen_authority_sha256)
        || fields.get("frozenAuthoritySha256").and_then(Value::as_str)
            != Some(frozen_authority_sha256)
        || fields.get("generationConfigSha256").and_then(Value::as_str)
            != Some(generation_config_sha256)
    {
        return Err(contract(
            "v5 evolved publication execution authority binding drifted",
        ));
    }
    let authority_sha = exact_sha(
        fields
            .get("authoritySha256")
            .expect("checked authority identity field"),
        "v5 evolved publication execution authority SHA-256",
    )?;
    if canonical_sha256_without_object_field(value, "authoritySha256")? != authority_sha {
        return Err(contract(
            "v5 evolved publication execution authority self-hash drifted",
        ));
    }
    Ok(authority_sha)
}

fn validate_native_batch_authority(value: &Value) -> Result<String> {
    let fields = object_ref(value, "v5 evolved publication native batch authority")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "contractVersion",
            "crateVersion",
            "binaryName",
            "buildProfile",
            "executableSha256",
            "sourceSha256",
            "authoritySha256",
        ],
        "v5 evolved publication native batch authority",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_native_authority_v1")
        || fields.get("contractVersion").and_then(Value::as_str) != Some(CONTRACT_VERSION)
        || fields.get("binaryName").and_then(Value::as_str) != Some("temporal-qd-batch")
        || fields.get("buildProfile").and_then(Value::as_str) != Some("release")
    {
        return Err(contract(
            "v5 evolved publication native batch authority is incompatible",
        ));
    }
    let _ = exact_text(
        fields.get("crateVersion").ok_or_else(|| {
            contract("v5 evolved publication batch authority lacks crate version")
        })?,
        "v5 evolved publication batch authority crate version",
    )?;
    for key in ["executableSha256", "sourceSha256", "authoritySha256"] {
        let _ = exact_sha(
            fields.get(key).ok_or_else(|| {
                contract(format!(
                    "v5 evolved publication batch authority lacks {key}"
                ))
            })?,
            &format!("v5 evolved publication batch authority {key}"),
        )?;
    }
    let authority_sha = exact_sha(
        fields
            .get("authoritySha256")
            .expect("checked authority identity field"),
        "v5 evolved publication native batch authority SHA-256",
    )?;
    if canonical_sha256_without_object_field(value, "authoritySha256")? != authority_sha {
        return Err(contract(
            "v5 evolved publication native batch authority self-hash drifted",
        ));
    }
    Ok(authority_sha)
}

fn publication_policy_from_authority(
    plan: &V5EvolvedPublicationPlan,
    authority: &V5SharedConstructionAuthority,
) -> Result<PublicationPolicy> {
    let frozen_authority = required(
        &plan.frozen_authority,
        "authority",
        "v5 evolved publication frozen authority",
    )?;
    let authority_fields = object_ref(frozen_authority, "v5 evolved publication authority")?;
    let pair_policy = authority_fields
        .get("bidirectionalPairPolicy")
        .cloned()
        .ok_or_else(|| contract("v5 evolved publication authority lacks pair policy"))?;
    if canonical_sha256(&pair_policy)? != authority.pair_policy_sha256 {
        return Err(contract(
            "v5 evolved publication pair policy identity drifted",
        ));
    }
    let evolvable = authority_fields
        .get("evolvableModuleAuthority")
        .ok_or_else(|| {
            contract("v5 evolved publication authority lacks evolvable module authority")
        })?;
    let evolvable_fields = object_ref(evolvable, "v5 evolved publication evolvable authority")?;
    let archive_policy_authority = evolvable_fields
        .get("archivePolicyAuthority")
        .cloned()
        .ok_or_else(|| {
            contract("v5 evolved publication authority lacks archive policy authority")
        })?;
    let archive = object_ref(
        &archive_policy_authority,
        "v5 evolved publication archive policy authority",
    )?;
    exact_keys(
        archive,
        &["qdVersion", "policyName", "policySha256", "frozenPolicy"],
        "v5 evolved publication archive policy authority",
    )?;
    let qd_version = exact_text(
        archive
            .get("qdVersion")
            .ok_or_else(|| contract("v5 evolved archive authority lacks qd version"))?,
        "v5 evolved publication qd version",
    )?;
    let policy_name = exact_text(
        archive
            .get("policyName")
            .ok_or_else(|| contract("v5 evolved archive authority lacks policy name"))?,
        "v5 evolved publication policy name",
    )?;
    let policy_sha256 = exact_sha(
        archive
            .get("policySha256")
            .ok_or_else(|| contract("v5 evolved archive authority lacks policy identity"))?,
        "v5 evolved publication policy SHA-256",
    )?;
    let frozen_policy = archive
        .get("frozenPolicy")
        .ok_or_else(|| contract("v5 evolved archive authority lacks frozen policy"))?;
    if canonical_sha256(frozen_policy)? != policy_sha256
        || frozen_policy.get("policyName").and_then(Value::as_str) != Some(policy_name.as_str())
    {
        return Err(contract(
            "v5 evolved publication archive policy authority identity drifted",
        ));
    }
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
            "v5 evolved derived publication policy is invalid: {error}"
        ))
    })?;
    Ok(policy)
}

fn validated_generation_operator_implementation(
    plan: &V5EvolvedPublicationPlan,
    authority_fields: &Map<String, Value>,
    evolvable: &Value,
) -> Result<Value> {
    let config = object_ref(
        &plan.generation_config,
        "v5 evolved publication generation config",
    )?;
    let operator = config
        .get("operatorImplementation")
        .ok_or_else(|| contract("v5 evolved generation config lacks operator implementation"))?;
    let fields = object_ref(operator, "v5 evolved generation operator implementation")?;
    let run_config = object_ref(
        config
            .get("runConfig")
            .ok_or_else(|| contract("v5 evolved generation config lacks runConfig"))?,
        "v5 evolved generation runConfig",
    )?;
    let evolvable = object_ref(evolvable, "v5 evolved evolvable module authority")?;
    let archive = evolvable
        .get("archivePolicyAuthority")
        .ok_or_else(|| contract("v5 evolved authority lacks archive policy authority"))?;
    let behavior = evolvable
        .get("behaviorAttributionRequirement")
        .ok_or_else(|| contract("v5 evolved authority lacks behavior requirement"))?;
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
        "v5 evolved generation operator implementation",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_evolvable_module_operator_implementation_v1")
    {
        return Err(contract(
            "v5 evolved generation operator implementation schema is invalid",
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
                "v5 evolved generation operator implementation {key} drifted from evolvable authority"
            )));
        }
    }
    let native = authority_fields
        .get("nativeOperatorAuthority")
        .ok_or_else(|| {
            contract("v5 evolved publication authority lacks native operator authority")
        })?;
    let native = object_ref(native, "v5 evolved native operator authority")?;
    for key in [
        "compilerPolicySha256",
        "programKind",
        "codec",
        "operatorRegistry",
        "budget",
    ] {
        if fields.get(key) != native.get(key) {
            return Err(contract(format!(
                "v5 evolved generation operator implementation {key} drifted from native authority"
            )));
        }
    }
    if native.get("factoryAuthoritySha256") != fields.get("authoritySha256")
        || run_config.get("operatorImplementation") != Some(operator)
        || run_config.get("archivePolicyAuthority") != Some(archive)
        || run_config.get("behaviorAttributionRequirement") != Some(behavior)
    {
        return Err(contract(
            "v5 evolved generation runConfig executable authority drifted",
        ));
    }
    if fields.get("archivePolicyAuthoritySha256")
        != Some(&Value::String(canonical_sha256(archive)?))
    {
        return Err(contract(
            "v5 evolved generation operator archive authority identity drifted",
        ));
    }
    let behavior_fields = object_ref(behavior, "v5 evolved behavior requirement")?;
    let behavior_sha = exact_sha(
        behavior_fields
            .get("requirementSha256")
            .ok_or_else(|| contract("v5 evolved behavior requirement lacks identity"))?,
        "v5 evolved behavior requirement SHA-256",
    )?;
    if canonical_sha256_without_object_field(behavior, "requirementSha256")? != behavior_sha
        || fields.get("behaviorAttributionRequirementSha256") != Some(&Value::String(behavior_sha))
    {
        return Err(contract(
            "v5 evolved generation behavior requirement drifted",
        ));
    }
    match capacity_receipt {
        Some(receipt) => {
            if run_config.get("capacityReceipt") != Some(receipt) {
                return Err(contract(
                    "v5 evolved generation runConfig capacity receipt drifted",
                ));
            }
            let receipt_fields = object_ref(receipt, "v5 evolved capacity receipt")?;
            let receipt_sha = exact_sha(
                receipt_fields
                    .get("semanticReceiptSha256")
                    .ok_or_else(|| contract("v5 evolved capacity receipt lacks identity"))?,
                "v5 evolved capacity receipt semantic SHA-256",
            )?;
            if fields.get("capacityReceiptSha256") != Some(&Value::String(receipt_sha)) {
                return Err(contract(
                    "v5 evolved generation capacity receipt identity drifted",
                ));
            }
        }
        None if run_config.contains_key("capacityReceipt") => {
            return Err(contract(
                "v5 evolved generation runConfig has unsealed capacity receipt",
            ));
        }
        None => {}
    }
    let implementation_sha = exact_sha(
        fields
            .get("operatorImplementationSha256")
            .ok_or_else(|| contract("v5 evolved operator implementation lacks identity"))?,
        "v5 evolved operator implementation SHA-256",
    )?;
    if canonical_sha256_without_object_field(operator, "operatorImplementationSha256")?
        != implementation_sha
    {
        return Err(contract(
            "v5 evolved generation operator implementation self-hash drifted",
        ));
    }
    Ok(operator.clone())
}

/// The four dynamic arrays emitted by one verified accepted-material replay.
/// Fragment bytes are private batch staging material; their receipts are
/// typed so later public assembly can reject truncation, reordering, or file
/// substitution without rematerializing any candidate.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum V5EvolvedPublicationFragmentKind {
    PopulationCandidates,
    EvaluationCandidates,
    EvaluationFunnelEntries,
    GenerationJournalBindings,
}

impl V5EvolvedPublicationFragmentKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PopulationCandidates => "populationCandidates",
            Self::EvaluationCandidates => "evaluationCandidates",
            Self::EvaluationFunnelEntries => "evaluationFunnelEntries",
            Self::GenerationJournalBindings => "generationJournalBindings",
        }
    }

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "populationCandidates" => Ok(Self::PopulationCandidates),
            "evaluationCandidates" => Ok(Self::EvaluationCandidates),
            "evaluationFunnelEntries" => Ok(Self::EvaluationFunnelEntries),
            "generationJournalBindings" => Ok(Self::GenerationJournalBindings),
            _ => Err(contract("v5 evolved publication fragment kind is invalid")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5EvolvedPublicationFragment {
    pub kind: V5EvolvedPublicationFragmentKind,
    pub fragment_sha256: String,
    pub encoded_bytes: u64,
    pub row_count: u64,
}

impl V5EvolvedPublicationFragment {
    fn validate(&self, expected_kind: V5EvolvedPublicationFragmentKind) -> Result<()> {
        if self.kind != expected_kind || self.row_count == 0 {
            return Err(contract(
                "v5 evolved publication fragment kind/count is invalid",
            ));
        }
        let _ = exact_sha(
            &Value::String(self.fragment_sha256.clone()),
            "v5 evolved publication fragment SHA-256",
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
                    "v5 evolved publication fragment SHA-256",
                )?),
            ),
            ("encodedBytes", Value::from(self.encoded_bytes)),
            ("rowCount", Value::from(self.row_count)),
        ]))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 evolved publication fragment")?;
        exact_keys(
            fields,
            &["kind", "fragmentSha256", "encodedBytes", "rowCount"],
            "v5 evolved publication fragment",
        )?;
        let fragment = Self {
            kind: V5EvolvedPublicationFragmentKind::from_str(&exact_text(
                required(value, "kind", "v5 evolved publication fragment")?,
                "v5 evolved publication fragment kind",
            )?)?,
            fragment_sha256: exact_sha(
                required(value, "fragmentSha256", "v5 evolved publication fragment")?,
                "v5 evolved publication fragment SHA-256",
            )?,
            encoded_bytes: required(value, "encodedBytes", "v5 evolved publication fragment")?
                .as_u64()
                .ok_or_else(|| contract("v5 evolved publication fragment byte count is invalid"))?,
            row_count: required(value, "rowCount", "v5 evolved publication fragment")?
                .as_u64()
                .ok_or_else(|| contract("v5 evolved publication fragment row count is invalid"))?,
        };
        fragment.validate(fragment.kind)?;
        if &fragment.to_value()? != value {
            return Err(contract("v5 evolved publication fragment is not canonical"));
        }
        Ok(fragment)
    }
}

/// Typed receipt for the four private fragment files.  It is intentionally
/// ephemeral and omitted from adoption: a fresh verifier must authenticate
/// it before publication while the private staging files still exist.
///
/// Three fragment roles are accepted-population arrays.  The funnel role is a
/// complete proposal-ordinal transcript, so its count may exceed accepted
/// candidates after a duplicate, rejected, or no-op retry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5EvolvedPublicationFragments {
    pub accepted_candidate_count: u64,
    pub proposal_attempt_count: u64,
    pub population_candidates: V5EvolvedPublicationFragment,
    pub evaluation_candidates: V5EvolvedPublicationFragment,
    pub evaluation_funnel_entries: V5EvolvedPublicationFragment,
    pub generation_journal_bindings: V5EvolvedPublicationFragment,
}

/// Durable name for the versioned fragment receipt.  `Fragments` remains the
/// source-compatible API name for the private fresh-publication mechanics;
/// this alias makes its immutable, content-addressed role explicit at the
/// outer receipt/adapter boundary.
pub type V5EvolvedPublicationFragmentReceipt = V5EvolvedPublicationFragments;

impl V5EvolvedPublicationFragments {
    pub fn fragment(
        &self,
        kind: V5EvolvedPublicationFragmentKind,
    ) -> &V5EvolvedPublicationFragment {
        match kind {
            V5EvolvedPublicationFragmentKind::PopulationCandidates => &self.population_candidates,
            V5EvolvedPublicationFragmentKind::EvaluationCandidates => &self.evaluation_candidates,
            V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries => {
                &self.evaluation_funnel_entries
            }
            V5EvolvedPublicationFragmentKind::GenerationJournalBindings => {
                &self.generation_journal_bindings
            }
        }
    }

    pub fn validate_for_counts(
        &self,
        accepted_candidate_count: u64,
        proposal_attempt_count: u64,
    ) -> Result<()> {
        if self.accepted_candidate_count != accepted_candidate_count
            || self.proposal_attempt_count != proposal_attempt_count
        {
            return Err(contract(
                "v5 evolved publication fragment receipt count binding drifted",
            ));
        }
        if accepted_candidate_count == 0 {
            return Err(contract(
                "v5 evolved publication fragment accepted count must be positive",
            ));
        }
        if proposal_attempt_count < accepted_candidate_count {
            return Err(contract(
                "v5 evolved publication proposal-attempt count is below accepted population",
            ));
        }
        for kind in [
            V5EvolvedPublicationFragmentKind::PopulationCandidates,
            V5EvolvedPublicationFragmentKind::EvaluationCandidates,
            V5EvolvedPublicationFragmentKind::GenerationJournalBindings,
        ] {
            let fragment = self.fragment(kind);
            fragment.validate(kind)?;
            if fragment.row_count != accepted_candidate_count {
                return Err(contract(
                    "v5 evolved publication fragment row count drifted from accepted population",
                ));
            }
        }
        let funnel = self.fragment(V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries);
        funnel.validate(V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries)?;
        if funnel.row_count != proposal_attempt_count {
            return Err(contract(
                "v5 evolved publication funnel fragment row count drifted from proposal attempts",
            ));
        }
        Ok(())
    }

    fn semantic_value(&self) -> Result<Value> {
        self.validate_for_counts(self.accepted_candidate_count, self.proposal_attempt_count)?;
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_PUBLICATION_FRAGMENTS_SCHEMA.to_owned()),
            ),
            (
                "acceptedCandidateCount",
                Value::from(self.accepted_candidate_count),
            ),
            (
                "proposalAttemptCount",
                Value::from(self.proposal_attempt_count),
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
        ]))
    }

    pub fn fragment_bundle_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 evolved publication fragments")
            .clone();
        fields.insert(
            "fragmentBundleSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    /// Return the exact canonical v2 receipt together with the only valid
    /// content-addressed object-store path for its semantic identity.
    ///
    /// The transaction deliberately does not bind this fresh-publication
    /// staging receipt: it records fragment byte/count evidence after the
    /// transaction has already been sealed.  The outer batch receipt/result
    /// roots this binding when it needs to hand the funnel descriptor to a
    /// later native adapter.
    pub fn object_binding(&self) -> Result<V5EvolvedPublicationFragmentReceiptObjectBinding> {
        let value = self.to_value()?;
        let fragment_bundle_sha256 = self.fragment_bundle_sha256()?;
        Ok(V5EvolvedPublicationFragmentReceiptObjectBinding {
            relative_path: v5_native_object_relative_path(&fragment_bundle_sha256)
                .map_err(V5EvolvedTransactionError::from)?,
            fragment_bundle_sha256,
            value,
        })
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 evolved publication fragments")?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "acceptedCandidateCount",
                "proposalAttemptCount",
                "populationCandidates",
                "evaluationCandidates",
                "evaluationFunnelEntries",
                "generationJournalBindings",
                "fragmentBundleSha256",
            ],
            "v5 evolved publication fragments",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_PUBLICATION_FRAGMENTS_SCHEMA)
        {
            return Err(contract(
                "v5 evolved publication fragments schema is invalid",
            ));
        }
        let fragments = Self {
            accepted_candidate_count: required(
                value,
                "acceptedCandidateCount",
                "v5 evolved publication fragments",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 evolved publication accepted count is invalid"))?,
            proposal_attempt_count: required(
                value,
                "proposalAttemptCount",
                "v5 evolved publication fragments",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 evolved publication attempt count is invalid"))?,
            population_candidates: V5EvolvedPublicationFragment::from_value(required(
                value,
                "populationCandidates",
                "v5 evolved publication fragments",
            )?)?,
            evaluation_candidates: V5EvolvedPublicationFragment::from_value(required(
                value,
                "evaluationCandidates",
                "v5 evolved publication fragments",
            )?)?,
            evaluation_funnel_entries: V5EvolvedPublicationFragment::from_value(required(
                value,
                "evaluationFunnelEntries",
                "v5 evolved publication fragments",
            )?)?,
            generation_journal_bindings: V5EvolvedPublicationFragment::from_value(required(
                value,
                "generationJournalBindings",
                "v5 evolved publication fragments",
            )?)?,
        };
        let supplied = exact_sha(
            required(
                value,
                "fragmentBundleSha256",
                "v5 evolved publication fragments",
            )?,
            "v5 evolved publication fragment bundle SHA-256",
        )?;
        if supplied != fragments.fragment_bundle_sha256()? || &fragments.to_value()? != value {
            return Err(contract(
                "v5 evolved publication fragment bundle identity drifted",
            ));
        }
        Ok(fragments)
    }
}

/// Typed immutable object-store entry for a v2 fragment receipt.
///
/// Unlike an untyped inventory row, this owns both the canonical receipt value
/// and its content address, so a publisher cannot replace it with a
/// self-hashed receipt from another generation or alias it to another path.
#[derive(Clone, Debug, PartialEq)]
pub struct V5EvolvedPublicationFragmentReceiptObjectBinding {
    pub fragment_bundle_sha256: String,
    pub relative_path: String,
    pub value: Value,
}

impl V5EvolvedPublicationFragmentReceiptObjectBinding {
    /// Verify canonical receipt parsing, semantic identity, and immutable
    /// object-store location as one operation.
    pub fn validate(&self) -> Result<()> {
        let fragment_bundle_sha256 = exact_sha(
            &Value::String(self.fragment_bundle_sha256.clone()),
            "v5 evolved publication fragment receipt SHA-256",
        )?;
        let expected_path = v5_native_object_relative_path(&fragment_bundle_sha256)
            .map_err(V5EvolvedTransactionError::from)?;
        let receipt = V5EvolvedPublicationFragments::from_value(&self.value)?;
        if self.relative_path != expected_path
            || receipt.fragment_bundle_sha256()? != fragment_bundle_sha256
            || receipt.to_value()? != self.value
        {
            return Err(contract(
                "v5 evolved publication fragment receipt object binding drifted",
            ));
        }
        Ok(())
    }

    /// Canonical transport projection for an outer receipt/result inventory.
    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(object([
            (
                "fragmentBundleSha256",
                Value::String(self.fragment_bundle_sha256.clone()),
            ),
            ("relativePath", Value::String(self.relative_path.clone())),
            ("value", self.value.clone()),
        ]))
    }

    /// Strictly parse a persisted outer-binding value before an adapter trusts
    /// its fragment receipt descriptor.
    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(
            value,
            "v5 evolved publication fragment receipt object binding",
        )?;
        exact_keys(
            fields,
            &["fragmentBundleSha256", "relativePath", "value"],
            "v5 evolved publication fragment receipt object binding",
        )?;
        let binding = Self {
            fragment_bundle_sha256: exact_sha(
                required(
                    value,
                    "fragmentBundleSha256",
                    "v5 evolved publication fragment receipt object binding",
                )?,
                "v5 evolved publication fragment receipt SHA-256",
            )?,
            relative_path: exact_text(
                required(
                    value,
                    "relativePath",
                    "v5 evolved publication fragment receipt object binding",
                )?,
                "v5 evolved publication fragment receipt object path",
            )?,
            value: required(
                value,
                "value",
                "v5 evolved publication fragment receipt object binding",
            )?
            .clone(),
        };
        binding.validate()?;
        if &binding.to_value()? != value {
            return Err(contract(
                "v5 evolved publication fragment receipt object binding is not canonical",
            ));
        }
        Ok(binding)
    }
}

/// Batch-owned file-backed target for one fresh accepted-material traversal.
/// Core supplies comma-separated canonical JSON bytes only; the sink must
/// preserve order and must not add a newline or retain a rich `Value`.
pub trait V5EvolvedPublicationFragmentSink {
    fn write_fragment(
        &mut self,
        kind: V5EvolvedPublicationFragmentKind,
        canonical_bytes: &[u8],
    ) -> std::io::Result<()>;
}

/// Batch-owned resettable source for private fragments.  Core copies each
/// complete fragment from byte zero and hashes it against its typed receipt.
pub trait V5EvolvedPublicationFragmentSource {
    fn copy_fragment(
        &mut self,
        kind: V5EvolvedPublicationFragmentKind,
        output: &mut dyn Write,
    ) -> std::io::Result<()>;
}

struct FragmentAccumulator {
    kind: V5EvolvedPublicationFragmentKind,
    first: bool,
    hash: CanonicalSha256Writer,
    encoded_bytes: u64,
    row_count: u64,
}

impl FragmentAccumulator {
    fn new(kind: V5EvolvedPublicationFragmentKind) -> Self {
        Self {
            kind,
            first: true,
            hash: CanonicalSha256Writer::default(),
            encoded_bytes: 0,
            row_count: 0,
        }
    }

    fn append<S: V5EvolvedPublicationFragmentSink>(
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
                .ok_or_else(|| contract("v5 evolved publication fragment byte count overflow"))?;
        }
        self.first = false;
        sink.write_fragment(self.kind, &row)?;
        self.hash.write_all(&row)?;
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(row.len() as u64)
            .ok_or_else(|| contract("v5 evolved publication fragment byte count overflow"))?;
        self.row_count = self
            .row_count
            .checked_add(1)
            .ok_or_else(|| contract("v5 evolved publication fragment row count overflow"))?;
        Ok(())
    }

    fn finish(self) -> Result<V5EvolvedPublicationFragment> {
        let fragment = V5EvolvedPublicationFragment {
            kind: self.kind,
            fragment_sha256: self.hash.finish(),
            encoded_bytes: self.encoded_bytes,
            row_count: self.row_count,
        };
        fragment.validate(self.kind)?;
        Ok(fragment)
    }
}

/// Compact receipt for one canonical public artifact.  `semantic_sha256`
/// names the canonical object semantics; `file_sha256`/`encoded_bytes` bind
/// the exact LF-terminated file bytes staged by batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5EvolvedStreamedArtifact {
    pub semantic_sha256: String,
    pub file_sha256: String,
    pub encoded_bytes: u64,
}

impl V5EvolvedStreamedArtifact {
    fn validate(&self) -> Result<()> {
        let _ = exact_sha(
            &Value::String(self.semantic_sha256.clone()),
            "v5 evolved streamed artifact semantic SHA-256",
        )?;
        let _ = exact_sha(
            &Value::String(self.file_sha256.clone()),
            "v5 evolved streamed artifact file SHA-256",
        )?;
        if self.encoded_bytes == 0 {
            return Err(contract("v5 evolved streamed artifact cannot be empty"));
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
                    "v5 evolved streamed artifact semantic SHA-256",
                )?),
            ),
            (
                "fileSha256",
                Value::String(exact_sha(
                    &Value::String(self.file_sha256.clone()),
                    "v5 evolved streamed artifact file SHA-256",
                )?),
            ),
            ("encodedBytes", Value::from(self.encoded_bytes)),
        ]))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 evolved streamed artifact")?;
        exact_keys(
            fields,
            &["semanticSha256", "fileSha256", "encodedBytes"],
            "v5 evolved streamed artifact",
        )?;
        let artifact = Self {
            semantic_sha256: exact_sha(
                required(value, "semanticSha256", "v5 evolved streamed artifact")?,
                "v5 evolved streamed artifact semantic SHA-256",
            )?,
            file_sha256: exact_sha(
                required(value, "fileSha256", "v5 evolved streamed artifact")?,
                "v5 evolved streamed artifact file SHA-256",
            )?,
            encoded_bytes: required(value, "encodedBytes", "v5 evolved streamed artifact")?
                .as_u64()
                .ok_or_else(|| contract("v5 evolved streamed artifact byte count is invalid"))?,
        };
        artifact.validate()?;
        if &artifact.to_value()? != value {
            return Err(contract("v5 evolved streamed artifact is not canonical"));
        }
        Ok(artifact)
    }
}

/// The public, self-authenticating identity-ledger handoff for one completed
/// evolved transaction.  This is intentionally not a projection of a private
/// schedule object: it binds both the previous input ledger and the exact
/// final compact `CandidateIdentityLedger` state that the next generation
/// restores.
#[derive(Clone, Debug, PartialEq)]
pub struct V5EvolvedIdentityLedger {
    pub generation_index: u64,
    pub generation_config_sha256: String,
    pub shared_authority_sha256: String,
    pub identity_ledger_input_binding_sha256: String,
    pub transaction_sha256: String,
    pub identity_ledger_identity: Value,
    pub final_identity_ledger_state: Value,
}

impl V5EvolvedIdentityLedger {
    /// Derive the only public ledger value from a typed, complete transaction.
    /// No batch-owned reduction is permitted at this boundary.
    pub fn from_transaction(transaction: &V5EvolvedTransactionResult) -> Result<Self> {
        transaction.verify_replay()?;
        let receipt = &transaction.schedule_state_receipt;
        let ledger = Self {
            generation_index: transaction.generation_index,
            generation_config_sha256: transaction.generation_config_sha256.clone(),
            shared_authority_sha256: transaction.shared_authority_sha256.clone(),
            identity_ledger_input_binding_sha256: transaction
                .identity_ledger_input_binding_sha256
                .clone(),
            transaction_sha256: transaction.transaction_sha256()?,
            identity_ledger_identity: receipt.identity_ledger_identity.clone(),
            final_identity_ledger_state: receipt.final_identity_ledger_state.clone(),
        };
        ledger.validate_shape()?;
        Ok(ledger)
    }

    fn semantic_value(&self) -> Result<Value> {
        self.validate_shape()?;
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_IDENTITY_LEDGER_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            (
                "generationConfigSha256",
                Value::String(self.generation_config_sha256.clone()),
            ),
            (
                "sharedAuthoritySha256",
                Value::String(self.shared_authority_sha256.clone()),
            ),
            (
                "identityLedgerInputBindingSha256",
                Value::String(self.identity_ledger_input_binding_sha256.clone()),
            ),
            (
                "transactionSha256",
                Value::String(self.transaction_sha256.clone()),
            ),
            (
                "identityLedgerIdentity",
                self.identity_ledger_identity.clone(),
            ),
            (
                "identityLedgerIdentitySha256",
                Value::String(canonical_sha256(&self.identity_ledger_identity)?),
            ),
            (
                "finalIdentityLedgerState",
                self.final_identity_ledger_state.clone(),
            ),
            (
                "finalIdentityLedgerStateSha256",
                Value::String(canonical_sha256(&self.final_identity_ledger_state)?),
            ),
        ]))
    }

    /// Semantic identity of the public next-generation ledger document.
    pub fn identity_ledger_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 evolved identity ledger")
            .clone();
        fields.insert(
            "identityLedgerSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 evolved identity ledger")?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "generationIndex",
                "generationConfigSha256",
                "sharedAuthoritySha256",
                "identityLedgerInputBindingSha256",
                "transactionSha256",
                "identityLedgerIdentity",
                "identityLedgerIdentitySha256",
                "finalIdentityLedgerState",
                "finalIdentityLedgerStateSha256",
                "identityLedgerSha256",
            ],
            "v5 evolved identity ledger",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_IDENTITY_LEDGER_SCHEMA)
        {
            return Err(contract("v5 evolved identity ledger schema is invalid"));
        }
        let ledger = Self {
            generation_index: required(value, "generationIndex", "v5 evolved identity ledger")?
                .as_u64()
                .ok_or_else(|| contract("v5 evolved identity ledger generation is invalid"))?,
            generation_config_sha256: exact_sha(
                required(
                    value,
                    "generationConfigSha256",
                    "v5 evolved identity ledger",
                )?,
                "v5 evolved identity ledger generation config SHA-256",
            )?,
            shared_authority_sha256: exact_sha(
                required(value, "sharedAuthoritySha256", "v5 evolved identity ledger")?,
                "v5 evolved identity ledger shared authority SHA-256",
            )?,
            identity_ledger_input_binding_sha256: exact_sha(
                required(
                    value,
                    "identityLedgerInputBindingSha256",
                    "v5 evolved identity ledger",
                )?,
                "v5 evolved identity ledger input binding SHA-256",
            )?,
            transaction_sha256: exact_sha(
                required(value, "transactionSha256", "v5 evolved identity ledger")?,
                "v5 evolved identity ledger transaction SHA-256",
            )?,
            identity_ledger_identity: required(
                value,
                "identityLedgerIdentity",
                "v5 evolved identity ledger",
            )?
            .clone(),
            final_identity_ledger_state: required(
                value,
                "finalIdentityLedgerState",
                "v5 evolved identity ledger",
            )?
            .clone(),
        };
        let identity_sha = exact_sha(
            required(
                value,
                "identityLedgerIdentitySha256",
                "v5 evolved identity ledger",
            )?,
            "v5 evolved identity ledger identity SHA-256",
        )?;
        let state_sha = exact_sha(
            required(
                value,
                "finalIdentityLedgerStateSha256",
                "v5 evolved identity ledger",
            )?,
            "v5 evolved identity ledger state SHA-256",
        )?;
        let supplied = exact_sha(
            required(value, "identityLedgerSha256", "v5 evolved identity ledger")?,
            "v5 evolved identity ledger SHA-256",
        )?;
        if identity_sha != canonical_sha256(&ledger.identity_ledger_identity)?
            || state_sha != canonical_sha256(&ledger.final_identity_ledger_state)?
            || supplied != ledger.identity_ledger_sha256()?
            || &ledger.to_value()? != value
        {
            return Err(contract("v5 evolved identity ledger identity drifted"));
        }
        Ok(ledger)
    }

    /// Reconstruct the exact compact runtime ledger for the next evolved
    /// scheduler.  This refuses a plausible self-hashed state whose embedded
    /// ledger identity or canonical compact fields do not agree.
    pub fn restore_candidate_identity_ledger(&self) -> Result<CandidateIdentityLedger> {
        self.validate_shape()?;
        let mut ledger = CandidateIdentityLedger::new(self.identity_ledger_identity.clone(), [])
            .map_err(|error| contract(format!("restore v5 evolved identity ledger: {error}")))?;
        ledger
            .restore_compact_state(&self.final_identity_ledger_state)
            .map_err(|error| contract(format!("restore v5 evolved identity ledger: {error}")))?;
        if ledger.compact_state() != self.final_identity_ledger_state {
            return Err(contract(
                "v5 evolved identity ledger final compact state is not canonical",
            ));
        }
        Ok(ledger)
    }

    fn validate_shape(&self) -> Result<()> {
        if self.generation_index < 2 {
            return Err(contract(
                "v5 evolved identity ledger requires later-generation index",
            ));
        }
        for (value, label) in [
            (
                &self.generation_config_sha256,
                "v5 evolved identity ledger generation config SHA-256",
            ),
            (
                &self.shared_authority_sha256,
                "v5 evolved identity ledger shared authority SHA-256",
            ),
            (
                &self.identity_ledger_input_binding_sha256,
                "v5 evolved identity ledger input binding SHA-256",
            ),
            (
                &self.transaction_sha256,
                "v5 evolved identity ledger transaction SHA-256",
            ),
        ] {
            let _ = exact_sha(&Value::String(value.clone()), label)?;
        }
        if !self.identity_ledger_identity.is_object() {
            return Err(contract(
                "v5 evolved identity ledger identity must be an object",
            ));
        }
        let _ = self.restore_candidate_identity_ledger_unchecked()?;
        Ok(())
    }

    fn restore_candidate_identity_ledger_unchecked(&self) -> Result<CandidateIdentityLedger> {
        let mut ledger = CandidateIdentityLedger::new(self.identity_ledger_identity.clone(), [])
            .map_err(|error| contract(format!("restore v5 evolved identity ledger: {error}")))?;
        ledger
            .restore_compact_state(&self.final_identity_ledger_state)
            .map_err(|error| contract(format!("restore v5 evolved identity ledger: {error}")))?;
        if ledger.compact_state() != self.final_identity_ledger_state {
            return Err(contract(
                "v5 evolved identity ledger final compact state is not canonical",
            ));
        }
        Ok(ledger)
    }

    fn validate_against_transaction(&self, transaction: &V5EvolvedTransactionResult) -> Result<()> {
        transaction.verify_replay()?;
        let receipt = &transaction.schedule_state_receipt;
        if self.generation_index != transaction.generation_index
            || self.generation_config_sha256 != transaction.generation_config_sha256
            || self.shared_authority_sha256 != transaction.shared_authority_sha256
            || self.identity_ledger_input_binding_sha256
                != transaction.identity_ledger_input_binding_sha256
            || self.transaction_sha256 != transaction.transaction_sha256()?
            || self.identity_ledger_identity != receipt.identity_ledger_identity
            || self.final_identity_ledger_state != receipt.final_identity_ledger_state
        {
            return Err(contract(
                "v5 evolved public identity ledger does not bind transaction state",
            ));
        }
        self.validate_shape()
    }
}

/// Typed receipt minted only after a fresh public bundle has been assembled
/// and verified from authenticated fragments.  Recovery uses this receipt to
/// verify exact public bytes without reopening fragments or rematerializing a
/// rich candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5EvolvedPublicationReceipt {
    pub publication_plan_sha256: String,
    pub publication_request_sha256: String,
    pub pair_config: V5EvolvedStreamedArtifact,
    pub identity_ledger: V5EvolvedStreamedArtifact,
    pub population: V5EvolvedStreamedArtifact,
    pub evaluation_population: V5EvolvedStreamedArtifact,
    pub generation_journal: V5EvolvedStreamedArtifact,
}

impl V5EvolvedPublicationReceipt {
    fn semantic_value(&self) -> Result<Value> {
        self.pair_config.validate()?;
        self.identity_ledger.validate()?;
        self.population.validate()?;
        self.evaluation_population.validate()?;
        self.generation_journal.validate()?;
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_PUBLICATION_RECEIPT_SCHEMA.to_owned()),
            ),
            (
                "publicationPlanSha256",
                Value::String(exact_sha(
                    &Value::String(self.publication_plan_sha256.clone()),
                    "v5 evolved publication receipt plan SHA-256",
                )?),
            ),
            (
                "publicationRequestSha256",
                Value::String(exact_sha(
                    &Value::String(self.publication_request_sha256.clone()),
                    "v5 evolved publication receipt request SHA-256",
                )?),
            ),
            ("pairConfig", self.pair_config.to_value()?),
            ("identityLedger", self.identity_ledger.to_value()?),
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
            .expect("constructed v5 evolved publication receipt")
            .clone();
        fields.insert(
            "publicationReceiptSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 evolved publication receipt")?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "publicationPlanSha256",
                "publicationRequestSha256",
                "pairConfig",
                "identityLedger",
                "population",
                "evaluationPopulation",
                "generationJournal",
                "publicationReceiptSha256",
            ],
            "v5 evolved publication receipt",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_PUBLICATION_RECEIPT_SCHEMA)
        {
            return Err(contract("v5 evolved publication receipt schema is invalid"));
        }
        let receipt = Self {
            publication_plan_sha256: exact_sha(
                required(
                    value,
                    "publicationPlanSha256",
                    "v5 evolved publication receipt",
                )?,
                "v5 evolved publication receipt plan SHA-256",
            )?,
            publication_request_sha256: exact_sha(
                required(
                    value,
                    "publicationRequestSha256",
                    "v5 evolved publication receipt",
                )?,
                "v5 evolved publication receipt request SHA-256",
            )?,
            pair_config: V5EvolvedStreamedArtifact::from_value(required(
                value,
                "pairConfig",
                "v5 evolved publication receipt",
            )?)?,
            identity_ledger: V5EvolvedStreamedArtifact::from_value(required(
                value,
                "identityLedger",
                "v5 evolved publication receipt",
            )?)?,
            population: V5EvolvedStreamedArtifact::from_value(required(
                value,
                "population",
                "v5 evolved publication receipt",
            )?)?,
            evaluation_population: V5EvolvedStreamedArtifact::from_value(required(
                value,
                "evaluationPopulation",
                "v5 evolved publication receipt",
            )?)?,
            generation_journal: V5EvolvedStreamedArtifact::from_value(required(
                value,
                "generationJournal",
                "v5 evolved publication receipt",
            )?)?,
        };
        let supplied = exact_sha(
            required(
                value,
                "publicationReceiptSha256",
                "v5 evolved publication receipt",
            )?,
            "v5 evolved publication receipt SHA-256",
        )?;
        if supplied != receipt.publication_receipt_sha256()? || &receipt.to_value()? != value {
            return Err(contract("v5 evolved publication receipt identity drifted"));
        }
        Ok(receipt)
    }

    pub fn object_binding(&self) -> Result<V5EvolvedPublicationReceiptObjectBinding> {
        let publication_receipt_sha256 = self.publication_receipt_sha256()?;
        Ok(V5EvolvedPublicationReceiptObjectBinding {
            relative_path: v5_native_object_relative_path(&publication_receipt_sha256)
                .map_err(V5EvolvedTransactionError::from)?,
            publication_receipt_sha256,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5EvolvedPublicationReceiptObjectBinding {
    pub publication_receipt_sha256: String,
    pub relative_path: String,
}

impl V5EvolvedPublicationReceiptObjectBinding {
    pub fn to_value(&self) -> Result<Value> {
        let publication_receipt_sha256 = exact_sha(
            &Value::String(self.publication_receipt_sha256.clone()),
            "v5 evolved publication receipt object SHA-256",
        )?;
        let expected = v5_native_object_relative_path(&publication_receipt_sha256)
            .map_err(V5EvolvedTransactionError::from)?;
        if self.relative_path != expected {
            return Err(contract(
                "v5 evolved publication receipt object path drifted",
            ));
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

/// Prepared, write-neutral publication view over one complete later-generation
/// compact transaction.  It holds only compact references and small envelopes;
/// rich candidates exist only synchronously inside `materialize_accepted_fragments`.
pub struct V5EvolvedPublicationStream<'a> {
    request: &'a V5EvolvedTransactionRequest,
    transaction: &'a V5EvolvedTransactionResult,
    plan: &'a V5EvolvedPublicationPlan,
    publication_request: PublicationRequest,
    accepted_references: Vec<AcceptedReference>,
    /// The full proposal-attempt identity corresponding to each accepted
    /// record in accepted/birth order.  Compact accepted references continue
    /// to point at immutable record objects; this separate list prevents the
    /// evaluation population from conflating those object identities with the
    /// historical proposal-funnel entry identity.
    accepted_attempt_entry_sha256s: Vec<String>,
    reproduction_allocation_accounting: Value,
}

/// Parse the sealed compact result and prepare its cap-free evolved
/// publication semantics.  This performs typed compact validation only: fresh
/// structural replay happens exactly once later when fragments are emitted;
/// adoption intentionally never invokes that expensive path.
pub fn prepare_v5_evolved_publication_stream<'a>(
    request: &'a V5EvolvedTransactionRequest,
    transaction: &'a V5EvolvedTransactionResult,
    plan: &'a V5EvolvedPublicationPlan,
) -> Result<V5EvolvedPublicationStream<'a>> {
    transaction.verify_replay()?;
    if !transaction.target_reached
        || transaction.accepted_records.len() as u64 != transaction.target_accepted
    {
        return Err(contract(
            "cannot publish an incomplete v5 evolved transaction",
        ));
    }
    let authority = V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)
        .map_err(V5EvolvedTransactionError::from)?;
    plan.validate_against_transaction(request, &authority)?;
    if plan.target_unique_candidates != transaction.accepted_records.len() as u64
        || plan.evaluation_population_size != transaction.accepted_records.len() as u64
    {
        return Err(contract(
            "v5 evolved publication plan width does not bind completed accepted records",
        ));
    }

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
    let allocation = object_ref(
        &plan.generation_config,
        "v5 evolved publication generation config",
    )?
    .get("reproductionAllocation")
    .cloned()
    .ok_or_else(|| contract("v5 evolved publication config lacks allocation"))?;
    let reproduction_allocation_accounting = reproduction_allocation_accounting(
        &allocation,
        &origin_proposal_counts,
        &origin_accepted_counts,
        &origin_accepted_counts,
        &rejected_by_origin,
    )
    .map_err(|error| contract(format!("v5 evolved allocation accounting failed: {error}")))?;
    if reproduction_allocation_accounting
        .get("complete")
        .and_then(Value::as_bool)
        != Some(true)
    {
        return Err(contract(
            "completed v5 evolved transaction has a reproduction allocation deficit",
        ));
    }

    let accepted_references = transaction
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
        .collect::<std::result::Result<Vec<_>, crate::v5::V5Error>>()?;
    let mut accepted_attempt_by_record_sha = BTreeMap::new();
    for attempt in &transaction.attempts {
        let Some(record_sha256) = &attempt.accepted_record_sha256 else {
            continue;
        };
        if attempt.disposition != "accepted" {
            return Err(contract(
                "v5 evolved accepted-record attempt does not carry accepted disposition",
            ));
        }
        if accepted_attempt_by_record_sha
            .insert(record_sha256.clone(), attempt)
            .is_some()
        {
            return Err(contract(
                "v5 evolved compact accepted record is bound by multiple attempts",
            ));
        }
    }
    let mut accepted_attempt_entry_sha256s = Vec::with_capacity(transaction.accepted_records.len());
    for record in &transaction.accepted_records {
        let record_sha256 = record.record_sha256()?;
        let attempt = accepted_attempt_by_record_sha
            .get(&record_sha256)
            .ok_or_else(|| {
                contract("v5 evolved accepted compact record lacks its proposal attempt")
            })?;
        if attempt.proposal_ordinal != record.proposal_ordinal
            || attempt.origin_kind != record.origin_kind
            || attempt.accepted_record_sha256.as_deref() != Some(record_sha256.as_str())
        {
            return Err(contract(
                "v5 evolved accepted record/attempt birth binding drifted",
            ));
        }
        accepted_attempt_entry_sha256s.push(attempt.attempt_sha256()?);
    }
    if accepted_attempt_by_record_sha.len() != transaction.accepted_records.len() {
        return Err(contract(
            "v5 evolved accepted record/attempt cardinality drifted",
        ));
    }
    let entry_sha256s = transaction
        .attempts
        .iter()
        .map(|attempt| attempt.attempt_sha256())
        .collect::<std::result::Result<Vec<_>, crate::v5::V5Error>>()?;
    let entry_ordinals = transaction
        .attempts
        .iter()
        .map(|attempt| attempt.proposal_ordinal)
        .collect::<Vec<_>>();
    let policy = publication_policy_from_authority(plan, &authority)?;
    let global_identity_ledger = evolved_global_identity_ledger(transaction)?;
    let publication_request = PublicationRequest {
        request_sha256: plan.publication_request_sha256()?,
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
        construction_references: accepted_references.clone(),
        g0_evaluation_width: None,
        global_identity_ledger: Some(global_identity_ledger),
        reproduction_allocation: Some(allocation),
        reproduction_allocation_accounting: Some(reproduction_allocation_accounting.clone()),
        unique_pair_genome_count: Some(transaction.accepted_records.len() as u64),
        policy,
    };
    publication_request.validate().map_err(|error| {
        contract(format!(
            "v5 evolved publication request is invalid: {error}"
        ))
    })?;
    Ok(V5EvolvedPublicationStream {
        request,
        transaction,
        plan,
        publication_request,
        accepted_references,
        accepted_attempt_entry_sha256s,
        reproduction_allocation_accounting,
    })
}

fn evolved_global_identity_ledger(transaction: &V5EvolvedTransactionResult) -> Result<Value> {
    let public_ledger = V5EvolvedIdentityLedger::from_transaction(transaction)?;
    let final_state = &transaction
        .schedule_state_receipt
        .final_identity_ledger_state;
    let fields = object_ref(final_state, "v5 evolved final identity ledger state")?;
    let identities = fields
        .get("candidateIdentities")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            contract(
                "v5 evolved final identity ledger state lacks candidate identities for publication",
            )
        })?;
    let mut unique = std::collections::BTreeSet::new();
    for identity in identities {
        unique.insert(exact_sha(
            identity,
            "v5 evolved final identity ledger candidate identity SHA-256",
        )?);
    }
    if unique.len() != identities.len() {
        return Err(contract(
            "v5 evolved final identity ledger repeats candidate identities",
        ));
    }
    Ok(object([
        (
            "pairExecutableSemanticCount",
            Value::from(unique.len() as u64),
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
            Value::String(public_ledger.identity_ledger_sha256()?),
        ),
        (
            "finalIdentityLedgerStateSha256",
            Value::String(canonical_sha256(final_state)?),
        ),
    ]))
}

impl<'a> V5EvolvedPublicationStream<'a> {
    pub fn publication_plan(&self) -> &V5EvolvedPublicationPlan {
        self.plan
    }

    pub fn publication_request_sha256(&self) -> &str {
        &self.publication_request.request_sha256
    }

    pub fn accepted_count(&self) -> usize {
        self.accepted_references.len()
    }

    /// Number of every-attempt rows that must be present in the evaluation
    /// funnel.  This may be greater than [`Self::accepted_count`] after any
    /// retry and is deliberately independent of rich materialization count.
    pub fn proposal_attempt_count(&self) -> usize {
        self.transaction.attempts.len()
    }

    /// Run the sealed offline transaction replay exactly once.  Every verified
    /// proposal attempt is appended to the funnel transcript; only accepted
    /// attempts invoke the second callback that materializes one transient rich
    /// candidate for the other three arrays.  No `Vec` of rich candidates or
    /// public rows is constructed.
    pub fn materialize_accepted_fragments<S: V5EvolvedPublicationFragmentSink>(
        &self,
        sink: &mut S,
    ) -> Result<V5EvolvedPublicationFragments> {
        let mut parent_sink = NoopV5EvolvedParentReferenceSink;
        self.materialize_accepted_fragments_and_parents(sink, &mut parent_sink)
    }

    /// Fast-ephemeral variant of evolved publication. It writes the next-
    /// generation parent handoff during the existing single replay, avoiding
    /// a second population-wide compiler pass.
    pub fn materialize_accepted_fragments_and_parents<
        S: V5EvolvedPublicationFragmentSink,
        P: V5EvolvedParentReferenceSink,
    >(
        &self,
        sink: &mut S,
        parent_sink: &mut P,
    ) -> Result<V5EvolvedPublicationFragments> {
        struct FragmentReplaySink<'a, S, P> {
            sink: &'a mut S,
            parent_sink: &'a mut P,
            transaction: &'a V5EvolvedTransactionResult,
            accepted_references: &'a [AcceptedReference],
            accepted_attempt_entry_sha256s: &'a [String],
            next_attempt_ordinal: u64,
            next_accepted: usize,
            rich_live: u64,
            peak_rich_live: u64,
            population: FragmentAccumulator,
            evaluation: FragmentAccumulator,
            funnel: FragmentAccumulator,
            journal: FragmentAccumulator,
        }

        impl<S: V5EvolvedPublicationFragmentSink, P: V5EvolvedParentReferenceSink>
            FragmentReplaySink<'_, S, P>
        {
            fn append_attempt(
                &mut self,
                attempt: &V5ProposalAttemptRecord,
                audit: &V5AttemptOutcomeAudit,
                material: Option<&V5EvolvedAcceptedMaterial>,
            ) -> Result<()> {
                if attempt.proposal_ordinal != self.next_attempt_ordinal {
                    return Err(contract(
                        "v5 evolved publication replay attempt ordinal is not contiguous",
                    ));
                }
                self.funnel.append(
                    self.sink,
                    &evolved_funnel_attempt_entry(attempt, audit, material)?,
                )?;
                self.next_attempt_ordinal = self
                    .next_attempt_ordinal
                    .checked_add(1)
                    .ok_or_else(|| contract("v5 evolved funnel attempt ordinal overflowed"))?;
                Ok(())
            }

            fn append(
                &mut self,
                authority: &V5SharedConstructionAuthority,
                material: &V5EvolvedAcceptedMaterial,
            ) -> Result<()> {
                if self.rich_live != 0 {
                    return Err(contract(
                        "v5 evolved publication attempted to retain more than one rich materialization",
                    ));
                }
                self.rich_live = 1;
                self.peak_rich_live = self.peak_rich_live.max(self.rich_live);
                #[cfg(test)]
                materialization_test_observer::observe_visit(self.rich_live);
                let result = (|| {
                    let expected_record = self
                        .transaction
                        .accepted_records
                        .get(self.next_accepted)
                        .ok_or_else(|| {
                            contract(
                                "v5 evolved replay produced more accepted materials than compact records",
                            )
                        })?;
                    if material.record.to_value()? != expected_record.to_value()? {
                        return Err(contract(
                            "v5 evolved replay accepted material does not match accepted birth order",
                        ));
                    }
                    let reference = self
                        .accepted_references
                        .get(self.next_accepted)
                        .ok_or_else(|| {
                            contract(
                                "v5 evolved replay accepted material lacks publication reference",
                            )
                        })?;
                    if reference.entry_sha256 != material.record.record_sha256()?
                        || reference.candidate_id != material.record.candidate_id
                        || reference.candidate_identity_sha256
                            != material.record.candidate_identity_sha256
                    {
                        return Err(contract(
                            "v5 evolved replay accepted reference binding drifted",
                        ));
                    }
                    let attempt_entry_sha256 = self
                        .accepted_attempt_entry_sha256s
                        .get(self.next_accepted)
                        .ok_or_else(|| {
                            contract(
                                "v5 evolved replay accepted material lacks proposal attempt identity",
                            )
                        })?;
                    let rich = materialize_v5_evolved_rich_candidate(authority, material)?;
                    let parent_reference =
                        parent_reference_from_v5_evolved_material(&material.parent_material)?;
                    self.parent_sink.write_parent_reference(&parent_reference)?;
                    #[cfg(test)]
                    materialization_test_observer::observe_rich_materialization();
                    let evaluation =
                        evolved_evaluation_candidate(&rich, reference, attempt_entry_sha256)?;
                    let journal = evolved_journal_binding(&evaluation)?;
                    self.population.append(self.sink, &rich)?;
                    self.evaluation.append(self.sink, &evaluation)?;
                    self.journal.append(self.sink, &journal)?;
                    self.next_accepted = self.next_accepted.checked_add(1).ok_or_else(|| {
                        contract("v5 evolved publication accepted material counter overflowed")
                    })?;
                    Ok(())
                })();
                self.rich_live = 0;
                result
            }
        }

        impl<S: V5EvolvedPublicationFragmentSink, P: V5EvolvedParentReferenceSink>
            V5EvolvedAcceptedReplaySink for FragmentReplaySink<'_, S, P>
        {
            fn observe_attempt(
                &mut self,
                _authority: &V5SharedConstructionAuthority,
                attempt: &V5ProposalAttemptRecord,
                audit: &V5AttemptOutcomeAudit,
                material: Option<&V5EvolvedAcceptedMaterial>,
            ) -> std::result::Result<(), V5EvolvedTransactionError> {
                self.append_attempt(attempt, audit, material)
                    .map_err(|error| {
                        V5EvolvedTransactionError::Contract(format!(
                            "v5 evolved public funnel materialization failed: {error}"
                        ))
                    })
            }

            fn accept(
                &mut self,
                authority: &V5SharedConstructionAuthority,
                material: &V5EvolvedAcceptedMaterial,
            ) -> std::result::Result<(), V5EvolvedTransactionError> {
                self.append(authority, material).map_err(|error| {
                    V5EvolvedTransactionError::Contract(format!(
                        "v5 evolved public fragment materialization failed: {error}"
                    ))
                })
            }
        }

        let mut replay_sink = FragmentReplaySink {
            sink,
            parent_sink,
            transaction: self.transaction,
            accepted_references: &self.accepted_references,
            accepted_attempt_entry_sha256s: &self.accepted_attempt_entry_sha256s,
            next_attempt_ordinal: 0,
            next_accepted: 0,
            rich_live: 0,
            peak_rich_live: 0,
            population: FragmentAccumulator::new(
                V5EvolvedPublicationFragmentKind::PopulationCandidates,
            ),
            evaluation: FragmentAccumulator::new(
                V5EvolvedPublicationFragmentKind::EvaluationCandidates,
            ),
            funnel: FragmentAccumulator::new(
                V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries,
            ),
            journal: FragmentAccumulator::new(
                V5EvolvedPublicationFragmentKind::GenerationJournalBindings,
            ),
        };
        replay_v5_evolved_transaction_with_accepted_sink(
            self.request,
            self.transaction,
            &mut replay_sink,
        )?;
        if replay_sink.next_attempt_ordinal != self.proposal_attempt_count() as u64
            || replay_sink.next_accepted != self.accepted_count()
            || replay_sink.peak_rich_live != 1
        {
            return Err(contract(
                "v5 evolved publication one-pass materialization count/live-state drifted",
            ));
        }
        let fragments = V5EvolvedPublicationFragments {
            accepted_candidate_count: self.accepted_count() as u64,
            proposal_attempt_count: self.proposal_attempt_count() as u64,
            population_candidates: replay_sink.population.finish()?,
            evaluation_candidates: replay_sink.evaluation.finish()?,
            evaluation_funnel_entries: replay_sink.funnel.finish()?,
            generation_journal_bindings: replay_sink.journal.finish()?,
        };
        fragments.validate_for_counts(
            self.accepted_count() as u64,
            self.proposal_attempt_count() as u64,
        )?;
        Ok(fragments)
    }

    pub fn write_pair_config<W: Write>(&self, output: &mut W) -> Result<V5EvolvedStreamedArtifact> {
        write_static_artifact(
            &self.plan.generation_config,
            &self.plan.generation_config_sha256,
            output,
        )
    }

    pub fn verify_pair_config<R: Read>(&self, input: &mut R) -> Result<V5EvolvedStreamedArtifact> {
        verify_static_artifact(
            &self.plan.generation_config,
            &self.plan.generation_config_sha256,
            input,
        )
    }

    /// Exact next-generation ledger handoff.  Batch must publish this at
    /// [`V5_EVOLVED_IDENTITY_LEDGER_RELATIVE_PATH`] rather than aliasing a
    /// private durable schedule receipt.
    pub fn identity_ledger(&self) -> Result<V5EvolvedIdentityLedger> {
        let ledger = V5EvolvedIdentityLedger::from_transaction(self.transaction)?;
        ledger.validate_against_transaction(self.transaction)?;
        Ok(ledger)
    }

    pub fn write_identity_ledger<W: Write>(
        &self,
        output: &mut W,
    ) -> Result<V5EvolvedStreamedArtifact> {
        let ledger = self.identity_ledger()?;
        write_static_artifact(
            &ledger.to_value()?,
            &ledger.identity_ledger_sha256()?,
            output,
        )
    }

    pub fn verify_identity_ledger<R: Read>(
        &self,
        input: &mut R,
    ) -> Result<V5EvolvedStreamedArtifact> {
        let ledger = self.identity_ledger()?;
        verify_static_artifact(
            &ledger.to_value()?,
            &ledger.identity_ledger_sha256()?,
            input,
        )
    }

    /// Assemble a fresh public artifact bundle from previously authenticated
    /// private fragments.  This performs no rich reconstruction; the source
    /// may only copy exact bytes emitted by `materialize_accepted_fragments`.
    #[allow(clippy::too_many_arguments)]
    pub fn write_bundle_from_fragments<
        S: V5EvolvedPublicationFragmentSource,
        WPair: Write,
        WLedger: Write,
        WPopulation: Write,
        WEvaluation: Write,
        WJournal: Write,
    >(
        &self,
        fragments: &V5EvolvedPublicationFragments,
        source: &mut S,
        pair_config: &mut WPair,
        identity_ledger: &mut WLedger,
        population: &mut WPopulation,
        evaluation_population: &mut WEvaluation,
        generation_journal: &mut WJournal,
    ) -> Result<V5EvolvedPublicationReceipt> {
        fragments.validate_for_counts(
            self.accepted_count() as u64,
            self.proposal_attempt_count() as u64,
        )?;
        let pair_config = self.write_pair_config(pair_config)?;
        let identity_ledger = self.write_identity_ledger(identity_ledger)?;
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
        Ok(V5EvolvedPublicationReceipt {
            publication_plan_sha256: self.plan.publication_plan_sha256()?,
            publication_request_sha256: self.publication_request.request_sha256.clone(),
            pair_config,
            identity_ledger,
            population,
            evaluation_population,
            generation_journal,
        })
    }

    /// Verify a fully staged fresh bundle while the private fragment files
    /// still exist.  Batch must mint and persist the returned receipt only
    /// after this function succeeds.
    #[allow(clippy::too_many_arguments)]
    pub fn verify_bundle_from_fragments<
        S: V5EvolvedPublicationFragmentSource,
        RPair: Read,
        RLedger: Read,
        RPopulation: Read,
        REvaluation: Read,
        RJournal: Read,
    >(
        &self,
        fragments: &V5EvolvedPublicationFragments,
        source: &mut S,
        pair_config: &mut RPair,
        identity_ledger: &mut RLedger,
        population: &mut RPopulation,
        evaluation_population: &mut REvaluation,
        generation_journal: &mut RJournal,
    ) -> Result<V5EvolvedPublicationReceipt> {
        fragments.validate_for_counts(
            self.accepted_count() as u64,
            self.proposal_attempt_count() as u64,
        )?;
        let pair_config = self.verify_pair_config(pair_config)?;
        let identity_ledger = self.verify_identity_ledger(identity_ledger)?;
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
        Ok(V5EvolvedPublicationReceipt {
            publication_plan_sha256: self.plan.publication_plan_sha256()?,
            publication_request_sha256: self.publication_request.request_sha256.clone(),
            pair_config,
            identity_ledger,
            population,
            evaluation_population,
            generation_journal,
        })
    }

    /// Fast recovery verifier.  It checks the typed compact transaction/plan
    /// in `prepare_*`, validates the static pair-config and public
    /// identity-ledger bytes, and hashes the three population-sized files
    /// against their fresh immutable receipt.  It deliberately does not
    /// require private fragments or run
    /// structural/operator replay; deep audit remains `verify_offline_replay`.
    #[allow(clippy::too_many_arguments)]
    pub fn verify_adopted_bundle<
        RPair: Read,
        RLedger: Read,
        RPopulation: Read,
        REvaluation: Read,
        RJournal: Read,
    >(
        &self,
        receipt: &V5EvolvedPublicationReceipt,
        pair_config: &mut RPair,
        identity_ledger: &mut RLedger,
        population: &mut RPopulation,
        evaluation_population: &mut REvaluation,
        generation_journal: &mut RJournal,
    ) -> Result<V5EvolvedPublicationReceipt> {
        if receipt.publication_plan_sha256 != self.plan.publication_plan_sha256()?
            || receipt.publication_request_sha256 != self.publication_request.request_sha256
        {
            return Err(contract(
                "v5 evolved publication receipt does not bind the typed plan/request",
            ));
        }
        let pair_config_receipt = self.verify_pair_config(pair_config)?;
        if pair_config_receipt != receipt.pair_config {
            return Err(contract(
                "v5 evolved published pair-config bytes do not match receipt",
            ));
        }
        let identity_ledger_receipt = self.verify_identity_ledger(identity_ledger)?;
        if identity_ledger_receipt != receipt.identity_ledger {
            return Err(contract(
                "v5 evolved published identity-ledger bytes do not match receipt",
            ));
        }
        verify_receipted_streamed_artifact(
            population,
            &receipt.population,
            "v5 evolved published population",
        )?;
        verify_receipted_streamed_artifact(
            evaluation_population,
            &receipt.evaluation_population,
            "v5 evolved published evaluation population",
        )?;
        verify_receipted_streamed_artifact(
            generation_journal,
            &receipt.generation_journal,
            "v5 evolved published generation journal",
        )?;
        Ok(receipt.clone())
    }

    pub fn write_population_from_fragments<S: V5EvolvedPublicationFragmentSource, W: Write>(
        &self,
        fragments: &V5EvolvedPublicationFragments,
        source: &mut S,
        output: &mut W,
    ) -> Result<V5EvolvedStreamedArtifact> {
        let template = self.population_template()?;
        let mut semantic_writer = CanonicalSha256Writer::default();
        self.stream_population_document_from_fragments(
            &template,
            fragments,
            source,
            &mut semantic_writer,
        )?;
        let semantic_sha256 = semantic_writer.finish();
        write_streamed_artifact(output, &semantic_sha256, |writer| {
            self.stream_population_document_from_fragments(&template, fragments, source, writer)
        })
    }

    pub fn write_evaluation_population_from_fragments<
        S: V5EvolvedPublicationFragmentSource,
        W: Write,
    >(
        &self,
        population: &V5EvolvedStreamedArtifact,
        fragments: &V5EvolvedPublicationFragments,
        source: &mut S,
        output: &mut W,
    ) -> Result<V5EvolvedStreamedArtifact> {
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
            .expect("constructed evolved evaluation template")
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

    pub fn write_generation_journal_from_fragments<
        S: V5EvolvedPublicationFragmentSource,
        W: Write,
    >(
        &self,
        population: &V5EvolvedStreamedArtifact,
        evaluation_population: &V5EvolvedStreamedArtifact,
        fragments: &V5EvolvedPublicationFragments,
        source: &mut S,
        output: &mut W,
    ) -> Result<V5EvolvedStreamedArtifact> {
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
            .expect("constructed evolved generation journal template")
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

    pub fn verify_population_from_fragments<S: V5EvolvedPublicationFragmentSource, R: Read>(
        &self,
        fragments: &V5EvolvedPublicationFragments,
        source: &mut S,
        input: &mut R,
    ) -> Result<V5EvolvedStreamedArtifact> {
        let template = self.population_template()?;
        let mut semantic_writer = CanonicalSha256Writer::default();
        self.stream_population_document_from_fragments(
            &template,
            fragments,
            source,
            &mut semantic_writer,
        )?;
        let semantic_sha256 = semantic_writer.finish();
        verify_streamed_artifact(input, &semantic_sha256, |writer| {
            self.stream_population_document_from_fragments(&template, fragments, source, writer)
        })
    }

    pub fn verify_evaluation_population_from_fragments<
        S: V5EvolvedPublicationFragmentSource,
        R: Read,
    >(
        &self,
        population: &V5EvolvedStreamedArtifact,
        fragments: &V5EvolvedPublicationFragments,
        source: &mut S,
        input: &mut R,
    ) -> Result<V5EvolvedStreamedArtifact> {
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
            .expect("constructed evolved evaluation template")
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

    pub fn verify_generation_journal_from_fragments<
        S: V5EvolvedPublicationFragmentSource,
        R: Read,
    >(
        &self,
        population: &V5EvolvedStreamedArtifact,
        evaluation_population: &V5EvolvedStreamedArtifact,
        fragments: &V5EvolvedPublicationFragments,
        source: &mut S,
        input: &mut R,
    ) -> Result<V5EvolvedStreamedArtifact> {
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
            .expect("constructed evolved generation journal template")
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
            &self.accepted_references,
            None,
            None,
        )
        .map_err(|error| contract(format!("v5 evolved population template failed: {error}")))
    }

    fn evaluation_template(&self, population: &V5EvolvedStreamedArtifact) -> Result<Value> {
        population.validate()?;
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
            ("candidateCount", Value::from(self.accepted_count() as u64)),
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
                Value::from(self.publication_request.proposal_count),
            ),
            ("funnelEntries", Value::Null),
        ]);
        if let Some(authority) = &policy.archive_policy_authority {
            value
                .as_object_mut()
                .expect("constructed evolved evaluation template")
                .insert("archivePolicyAuthority".to_owned(), authority.clone());
        }
        Ok(value)
    }

    fn generation_journal_template(
        &self,
        population: &V5EvolvedStreamedArtifact,
        evaluation_population: &V5EvolvedStreamedArtifact,
    ) -> Result<Value> {
        population.validate()?;
        evaluation_population.validate()?;
        let policy = &self.publication_request.policy;
        let accepted_count = self.accepted_count() as u64;
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
            (
                "proposalCount",
                Value::from(self.publication_request.proposal_count),
            ),
            ("acceptedCount", Value::from(accepted_count)),
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
                count_map_value(&self.publication_request.origin_accepted_counts),
            ),
            (
                "dispositionCounts",
                count_map_value(&self.publication_request.disposition_counts),
            ),
            (
                "proposalSlots",
                evolved_proposal_slots(&self.publication_request, accepted_count),
            ),
            (
                "uniqueIdentityCounts",
                object([
                    ("candidateIdentity", Value::from(accepted_count)),
                    (
                        "pairGenome",
                        Value::from(
                            self.publication_request
                                .unique_pair_genome_count
                                .unwrap_or(accepted_count),
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
            (
                "entrySha256s",
                Value::Array(
                    self.publication_request
                        .entry_sha256s
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            ),
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
        let fields = value
            .as_object_mut()
            .expect("constructed evolved generation journal template");
        fields.insert(
            "reproductionAllocation".to_owned(),
            self.publication_request
                .reproduction_allocation
                .clone()
                .expect("prepared evolved request has allocation"),
        );
        fields.insert(
            "reproductionAllocationAccounting".to_owned(),
            self.reproduction_allocation_accounting.clone(),
        );
        fields.insert(
            "globalIdentityLedger".to_owned(),
            self.publication_request
                .global_identity_ledger
                .clone()
                .expect("prepared evolved request has ledger summary"),
        );
        if let Some(authority) = &policy.archive_policy_authority {
            fields.insert("archivePolicyAuthority".to_owned(), authority.clone());
        }
        Ok(value)
    }

    fn stream_population_document_from_fragments<
        S: V5EvolvedPublicationFragmentSource,
        W: Write,
    >(
        &self,
        template: &Value,
        fragments: &V5EvolvedPublicationFragments,
        source: &mut S,
        output: &mut W,
    ) -> Result<()> {
        stream_object(template, output, |key, writer| {
            if key != "candidates" {
                return Ok(false);
            }
            writer.write_all(b"[")?;
            copy_fragment_checked(
                source,
                fragments.fragment(V5EvolvedPublicationFragmentKind::PopulationCandidates),
                writer,
            )?;
            writer.write_all(b"]")?;
            Ok(true)
        })
    }

    fn stream_evaluation_document_from_fragments<
        S: V5EvolvedPublicationFragmentSource,
        W: Write,
    >(
        &self,
        template: &Value,
        fragments: &V5EvolvedPublicationFragments,
        source: &mut S,
        output: &mut W,
    ) -> Result<()> {
        stream_object(template, output, |key, writer| {
            let kind = match key {
                "candidates" => V5EvolvedPublicationFragmentKind::EvaluationCandidates,
                "funnelEntries" => V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries,
                _ => return Ok(false),
            };
            writer.write_all(b"[")?;
            copy_fragment_checked(source, fragments.fragment(kind), writer)?;
            writer.write_all(b"]")?;
            Ok(true)
        })
    }

    fn stream_generation_journal_document_from_fragments<
        S: V5EvolvedPublicationFragmentSource,
        W: Write,
    >(
        &self,
        template: &Value,
        fragments: &V5EvolvedPublicationFragments,
        source: &mut S,
        output: &mut W,
    ) -> Result<()> {
        stream_object(template, output, |key, writer| {
            if key != "evaluationCandidateBindings" {
                return Ok(false);
            }
            writer.write_all(b"[")?;
            copy_fragment_checked(
                source,
                fragments.fragment(V5EvolvedPublicationFragmentKind::GenerationJournalBindings),
                writer,
            )?;
            writer.write_all(b"]")?;
            Ok(true)
        })
    }
}

fn evolved_evaluation_candidate(
    candidate: &Value,
    reference: &AcceptedReference,
    proposal_entry_sha256: &str,
) -> Result<Value> {
    let fields = object_ref(candidate, "v5 evolved rich evaluation candidate")?;
    let require = |field: &str| {
        fields
            .get(field)
            .cloned()
            .ok_or_else(|| contract(format!("v5 evolved rich candidate lacks {field}")))
    };
    let mut value = object([
        ("candidateId", require("candidateId")?),
        ("sourceMode", require("sourceMode")?),
        ("seedId", require("seedId")?),
        (
            "candidateIdentitySha256",
            require("candidateIdentitySha256")?,
        ),
        ("programSha256", require("programSha256")?),
        ("sourceProfile", require("sourceProfile")?),
        ("sourceProfileSha256", require("sourceProfileSha256")?),
        (
            "structuralOperatorHistory",
            require("structuralOperatorHistory")?,
        ),
        ("proposalOrdinal", Value::from(reference.proposal_ordinal)),
        (
            "proposalEntrySha256",
            Value::String(proposal_entry_sha256.to_owned()),
        ),
    ]);
    // The compact v5 candidate always supplies a profile snapshot.  Evidence
    // identity is bound by the compact record/funnel but remains optional in
    // the historical evaluation projection schema.
    if let Some(profile_snapshot) = fields.get("profileSnapshotSha256") {
        value
            .as_object_mut()
            .expect("constructed evolved evaluation candidate")
            .insert("profileSnapshotSha256".to_owned(), profile_snapshot.clone());
    }
    if let Some(evidence) = fields.get("canonicalEvidenceIdentitySha256") {
        value
            .as_object_mut()
            .expect("constructed evolved evaluation candidate")
            .insert(
                "canonicalEvidenceIdentitySha256".to_owned(),
                evidence.clone(),
            );
    }
    Ok(value)
}

/// Construct one public proposal-funnel row from an already replay-verified
/// attempt.  The row is intentionally compact: failures that never reached a
/// complete candidate remain candidate-free, while identity-ledger duplicates
/// retain their independently compiled stage projection without claiming an
/// accepted-record object.
fn evolved_funnel_attempt_entry(
    attempt: &V5ProposalAttemptRecord,
    audit: &V5AttemptOutcomeAudit,
    material: Option<&V5EvolvedAcceptedMaterial>,
) -> Result<Value> {
    audit.verify_binds_attempt(attempt)?;
    let mut fields = Map::new();
    fields.insert(
        "schemaVersion".to_owned(),
        Value::String(V5_PROPOSAL_FUNNEL_ENTRY_SCHEMA.to_owned()),
    );
    fields.insert(
        "entrySha256".to_owned(),
        Value::String(attempt.attempt_sha256()?),
    );
    fields.insert(
        "proposalOrdinal".to_owned(),
        Value::from(attempt.proposal_ordinal),
    );
    fields.insert(
        "originKind".to_owned(),
        Value::String(attempt.origin_kind.clone()),
    );
    fields.insert(
        "disposition".to_owned(),
        Value::String(attempt.disposition.clone()),
    );
    if let Some(material) = material {
        let record = &material.record;
        let record_sha256 = record.record_sha256()?;
        if record.proposal_ordinal != attempt.proposal_ordinal
            || record.origin_kind != attempt.origin_kind
        {
            return Err(contract(
                "v5 evolved funnel candidate material does not bind proposal attempt",
            ));
        }
        let admission = match &attempt.accepted_record_sha256 {
            Some(expected_record_sha256) => {
                if attempt.disposition != "accepted"
                    || audit.stage != "accepted"
                    || expected_record_sha256 != &record_sha256
                {
                    return Err(contract(
                        "v5 evolved accepted funnel record binding drifted",
                    ));
                }
                fields.insert(
                    "acceptedCompactRecordSha256".to_owned(),
                    Value::String(record_sha256),
                );
                V5FunnelAdmission::Accepted
            }
            None => {
                if attempt.disposition != "rejected"
                    || !matches!(audit.stage.as_str(), "admission" | "identity_ledger")
                {
                    return Err(contract(
                        "v5 evolved candidate-bearing rejected attempt is not a duplicate admission",
                    ));
                }
                V5FunnelAdmission::Duplicate {
                    reason_code: attempt.reason_code.clone(),
                }
            }
        };
        let source_profile_sha256 = Value::String(record.compiled.raw_pair_sha256.clone());
        fields.insert(
            "candidate".to_owned(),
            object([
                ("candidateId", Value::String(record.candidate_id.clone())),
                ("sourceProfileSha256", source_profile_sha256.clone()),
            ]),
        );
        fields.insert(
            "proposal".to_owned(),
            object([
                ("candidateId", Value::String(record.candidate_id.clone())),
                ("rawSourceProfileSha256", source_profile_sha256),
            ]),
        );
        fields.insert(
            "funnelCandidate".to_owned(),
            v5_funnel_candidate_projection(record, admission)?,
        );
    } else if attempt.accepted_record_sha256.is_some() {
        return Err(contract(
            "v5 evolved accepted attempt omitted funnel candidate material",
        ));
    }
    Ok(Value::Object(fields))
}

fn evolved_journal_binding(evaluation: &Value) -> Result<Value> {
    let fields = object_ref(evaluation, "v5 evolved evaluation candidate")?;
    Ok(object([
        (
            "candidateId",
            fields
                .get("candidateId")
                .cloned()
                .ok_or_else(|| contract("v5 evolved evaluation candidate lacks candidate ID"))?,
        ),
        (
            "proposalOrdinal",
            fields
                .get("proposalOrdinal")
                .cloned()
                .ok_or_else(|| contract("v5 evolved evaluation candidate lacks ordinal"))?,
        ),
        (
            "proposalEntrySha256",
            fields
                .get("proposalEntrySha256")
                .cloned()
                .ok_or_else(|| contract("v5 evolved evaluation candidate lacks entry SHA-256"))?,
        ),
        (
            "candidateProjectionSha256",
            Value::String(canonical_sha256(evaluation)?),
        ),
    ]))
}

fn evolved_proposal_slots(request: &PublicationRequest, accepted: u64) -> Value {
    object([
        ("targetUniqueCandidates", Value::from(accepted)),
        ("acceptedUniqueCandidates", Value::from(accepted)),
        ("proposalAttempts", Value::from(request.proposal_count)),
        (
            "maxProposalAttempts",
            Value::from(request.max_proposal_attempts),
        ),
        (
            "remainingUniqueCandidateSlots",
            Value::from(request.target_unique_candidates.saturating_sub(accepted)),
        ),
    ])
}

fn stream_object<W: Write, F>(value: &Value, output: &mut W, mut dynamic: F) -> Result<()>
where
    F: FnMut(&str, &mut W) -> Result<bool>,
{
    let fields = object_ref(value, "v5 evolved streamed document")?;
    let ordered = fields
        .iter()
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
            .ok_or_else(|| std::io::Error::other("v5 evolved fragment byte count overflow"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.output.flush()?;
        self.hash.flush()
    }
}

fn copy_fragment_checked<S: V5EvolvedPublicationFragmentSource, W: Write>(
    source: &mut S,
    expected: &V5EvolvedPublicationFragment,
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
            "v5 evolved {} fragment bytes do not match the authenticated materialization receipt",
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
            .ok_or_else(|| std::io::Error::other("v5 evolved artifact byte count overflow"))?;
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
) -> Result<V5EvolvedStreamedArtifact> {
    write_streamed_artifact(output, semantic_sha256, |writer| {
        Ok(write_canonical_json(value, writer)?)
    })
}

fn write_streamed_artifact<W: Write, F>(
    output: &mut W,
    semantic_sha256: &str,
    stream: F,
) -> Result<V5EvolvedStreamedArtifact>
where
    F: FnOnce(&mut TeeWriter<'_, W>) -> Result<()>,
{
    let semantic_sha256 = exact_sha(
        &Value::String(semantic_sha256.to_owned()),
        "v5 evolved streamed artifact semantic SHA-256",
    )?;
    let mut file_hash = CanonicalSha256Writer::default();
    let mut encoded_bytes = 0_u64;
    {
        let mut tee = TeeWriter {
            output,
            hash: &mut file_hash,
            bytes: &mut encoded_bytes,
        };
        stream(&mut tee)?;
        tee.write_all(b"\n")?;
        tee.flush()?;
    }
    Ok(V5EvolvedStreamedArtifact {
        semantic_sha256,
        file_sha256: file_hash.finish(),
        encoded_bytes,
    })
}

struct MatchingWriter<'a, R: Read> {
    input: &'a mut R,
    file_hash: CanonicalSha256Writer,
    encoded_bytes: u64,
}

impl<R: Read> MatchingWriter<'_, R> {
    fn finish(self) -> Result<V5EvolvedStreamedArtifact> {
        let mut extra = [0_u8; 1];
        if self.input.read(&mut extra)? != 0 {
            return Err(contract("staged v5 evolved artifact has trailing bytes"));
        }
        Ok(V5EvolvedStreamedArtifact {
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
                "staged v5 evolved artifact differs from canonical core stream",
            ));
        }
        self.file_hash.write_all(expected)?;
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(expected.len() as u64)
            .ok_or_else(|| std::io::Error::other("v5 evolved artifact byte count overflow"))?;
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
) -> Result<V5EvolvedStreamedArtifact> {
    verify_streamed_artifact(input, semantic_sha256, |writer| {
        Ok(write_canonical_json(value, writer)?)
    })
}

fn verify_streamed_artifact<R: Read, F>(
    input: &mut R,
    semantic_sha256: &str,
    stream: F,
) -> Result<V5EvolvedStreamedArtifact>
where
    F: FnOnce(&mut MatchingWriter<'_, R>) -> Result<()>,
{
    let semantic_sha256 = exact_sha(
        &Value::String(semantic_sha256.to_owned()),
        "v5 evolved staged artifact semantic SHA-256",
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

fn verify_receipted_streamed_artifact<R: Read>(
    input: &mut R,
    expected: &V5EvolvedStreamedArtifact,
    label: &str,
) -> Result<()> {
    expected.validate()?;
    let mut hasher = CanonicalSha256Writer::default();
    let mut encoded_bytes = 0_u64;
    let mut final_byte = None;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.write_all(&buffer[..count])?;
        encoded_bytes = encoded_bytes
            .checked_add(count as u64)
            .ok_or_else(|| contract(format!("{label} byte count overflowed")))?;
        final_byte = buffer[..count].last().copied();
    }
    if encoded_bytes != expected.encoded_bytes
        || hasher.finish() != expected.file_sha256
        || final_byte != Some(b'\n')
    {
        return Err(contract(format!(
            "{label} bytes do not match the authenticated evolved publication receipt"
        )));
    }
    Ok(())
}

/// Public no-rich adoption verifier.  The caller must first authenticate the
/// plan/receipt object bindings and reconstruct the typed transaction from its
/// durable object inventory.  This function then performs only compact plan
/// validation and streamed public-file receipt checks; it never needs private
/// fragments, a parent archive, or a second structural replay.
#[allow(clippy::too_many_arguments)]
pub fn verify_v5_evolved_publication_adoption<
    RPair: Read,
    RLedger: Read,
    RPopulation: Read,
    REvaluation: Read,
    RJournal: Read,
>(
    request: &V5EvolvedTransactionRequest,
    transaction: &V5EvolvedTransactionResult,
    publication_plan_value: &Value,
    receipt_value: &Value,
    pair_config: &mut RPair,
    identity_ledger: &mut RLedger,
    population: &mut RPopulation,
    evaluation_population: &mut REvaluation,
    generation_journal: &mut RJournal,
) -> Result<V5EvolvedPublicationReceipt> {
    let plan = V5EvolvedPublicationPlan::from_value(publication_plan_value)?;
    let receipt = V5EvolvedPublicationReceipt::from_value(receipt_value)?;
    let stream = prepare_v5_evolved_publication_stream(request, transaction, &plan)?;
    stream.verify_adopted_bundle(
        &receipt,
        pair_config,
        identity_ledger,
        population,
        evaluation_population,
        generation_journal,
    )
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        io::{Cursor, Read, Write},
        sync::{Mutex, OnceLock},
    };

    use flate2::read::GzDecoder;

    use super::*;
    use crate::{
        factory::ParentReference,
        proposal::{
            CandidateIdentityLedger, ExplicitParentRing, IdentityLedger, ParentSelector,
            ProposalError,
        },
        schedule::{RotatingParentSchedule, accepted_quota_immigrant_count},
        v5::{
            build_v5_g0_accepted_material, parent_reference_from_v5_compact_record,
            v5_proposal_seed,
        },
        v5_evolved_transaction::{
            execute_v5_evolved_transaction,
            execute_v5_evolved_transaction_fast_ephemeral_with_progress,
        },
    };

    fn sha(value: Value) -> String {
        canonical_sha256(&value).expect("canonical test identity")
    }

    fn materialization_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[derive(Default)]
    struct ParentCollector(Vec<ParentReference>);

    impl V5EvolvedParentReferenceSink for ParentCollector {
        fn write_parent_reference(&mut self, reference: &ParentReference) -> std::io::Result<()> {
            self.0.push(reference.clone());
            Ok(())
        }
    }

    #[derive(Default)]
    struct EmptyParents;

    impl ParentSelector for EmptyParents {
        fn has_parents(&self) -> bool {
            false
        }

        fn eligible_parent_count(&self) -> usize {
            0
        }

        fn archive_cell_count(&self) -> usize {
            0
        }

        fn compact_state(&self) -> Value {
            object([(
                "schemaVersion",
                Value::String(
                    "temporal_qd_v5_evolved_publication_empty_parent_selector_v1".to_owned(),
                ),
            )])
        }

        fn restore_compact_state(
            &mut self,
            state: &Value,
        ) -> std::result::Result<(), ProposalError> {
            if state == &self.compact_state() {
                Ok(())
            } else {
                Err(ProposalError::Contract(
                    "evolved publication empty parent selector state drifted".to_owned(),
                ))
            }
        }

        fn select(
            &mut self,
            _label: &str,
            _structural_selection_ordinal: u64,
        ) -> std::result::Result<ParentReference, ProposalError> {
            Err(ProposalError::ParentSelectorUnavailable)
        }
    }

    #[derive(Default)]
    struct InMemoryFragments {
        bytes: BTreeMap<V5EvolvedPublicationFragmentKind, Vec<u8>>,
    }

    impl V5EvolvedPublicationFragmentSink for InMemoryFragments {
        fn write_fragment(
            &mut self,
            kind: V5EvolvedPublicationFragmentKind,
            canonical_bytes: &[u8],
        ) -> std::io::Result<()> {
            self.bytes
                .entry(kind)
                .or_default()
                .write_all(canonical_bytes)
        }
    }

    impl V5EvolvedPublicationFragmentSource for InMemoryFragments {
        fn copy_fragment(
            &mut self,
            kind: V5EvolvedPublicationFragmentKind,
            output: &mut dyn Write,
        ) -> std::io::Result<()> {
            output.write_all(self.bytes.get(&kind).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "test evolved publication fragment is absent",
                )
            })?)
        }
    }

    #[derive(Default)]
    struct CapturedReplayAttempts {
        rows: Vec<(
            V5ProposalAttemptRecord,
            V5AttemptOutcomeAudit,
            Option<V5EvolvedAcceptedMaterial>,
        )>,
    }

    impl V5EvolvedAcceptedReplaySink for CapturedReplayAttempts {
        fn observe_attempt(
            &mut self,
            _authority: &V5SharedConstructionAuthority,
            attempt: &V5ProposalAttemptRecord,
            audit: &V5AttemptOutcomeAudit,
            material: Option<&V5EvolvedAcceptedMaterial>,
        ) -> std::result::Result<(), V5EvolvedTransactionError> {
            self.rows
                .push((attempt.clone(), audit.clone(), material.cloned()));
            Ok(())
        }

        fn accept(
            &mut self,
            _authority: &V5SharedConstructionAuthority,
            _material: &V5EvolvedAcceptedMaterial,
        ) -> std::result::Result<(), V5EvolvedTransactionError> {
            Ok(())
        }
    }

    fn shared_authority_fixture() -> Value {
        let compressed = include_bytes!(
            "../../../../../tests/fixtures/temporal_qd_v5_shared_authority_oracle.json.gz"
        );
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut payload = Vec::new();
        decoder
            .read_to_end(&mut payload)
            .expect("decompress shared authority fixture");
        serde_json::from_slice::<Value>(&payload)
            .expect("parse shared authority fixture")
            .get("sealedAuthority")
            .cloned()
            .expect("shared authority fixture has sealed envelope")
    }

    /// This exactly mirrors the existing full v2 generation-config layout:
    /// allocation/operator are top-level while archive/behavior/capacity stay
    /// within `runConfig`.  Keeping it here makes the publication tests prove
    /// the production authority projection rather than accept a mock plan.
    fn fixture_generation_config(
        generation_index: u64,
        target_accepted: u64,
        max_attempts: u64,
    ) -> Value {
        let shared = shared_authority_fixture();
        let authority = shared.get("authority").expect("fixture authority body");
        let evolvable = authority
            .get("evolvableModuleAuthority")
            .expect("fixture evolvable authority");
        let archive = evolvable
            .get("archivePolicyAuthority")
            .expect("fixture archive authority")
            .clone();
        let behavior = evolvable
            .get("behaviorAttributionRequirement")
            .expect("fixture behavior requirement")
            .clone();
        let behavior_sha256 = behavior
            .get("requirementSha256")
            .expect("fixture behavior identity")
            .clone();
        let mut operator = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_evolvable_module_operator_implementation_v1".to_owned()),
            ),
            (
                "authoritySha256",
                evolvable
                    .get("authoritySha256")
                    .expect("evolvable authority identity")
                    .clone(),
            ),
            (
                "programKind",
                evolvable
                    .get("programKind")
                    .expect("evolvable program kind")
                    .clone(),
            ),
            (
                "codec",
                evolvable.get("codec").expect("evolvable codec").clone(),
            ),
            (
                "compilerPolicySha256",
                evolvable
                    .get("compilerPolicySha256")
                    .expect("evolvable compiler identity")
                    .clone(),
            ),
            (
                "operatorRegistry",
                evolvable
                    .get("operatorRegistry")
                    .expect("evolvable operator registry")
                    .clone(),
            ),
            (
                "budget",
                evolvable.get("budget").expect("evolvable budget").clone(),
            ),
            (
                "capacityContract",
                evolvable
                    .get("capacityContract")
                    .expect("evolvable capacity contract")
                    .clone(),
            ),
            (
                "archivePolicyAuthoritySha256",
                Value::String(canonical_sha256(&archive).expect("archive authority identity")),
            ),
            ("behaviorAttributionRequirementSha256", behavior_sha256),
        ]);
        if let Some(receipt) = evolvable.get("capacityReceipt") {
            operator.as_object_mut().expect("operator object").insert(
                "capacityReceiptSha256".to_owned(),
                receipt
                    .get("semanticReceiptSha256")
                    .expect("capacity receipt identity")
                    .clone(),
            );
        }
        let operator_sha256 = canonical_sha256(&operator).expect("operator identity");
        operator.as_object_mut().expect("operator object").insert(
            "operatorImplementationSha256".to_owned(),
            Value::String(operator_sha256),
        );

        let allocation_semantic = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_reproduction_allocation_v2".to_owned()),
            ),
            ("targetAcceptedCandidates", Value::from(target_accepted)),
            ("desiredAcceptedOffspringCount", Value::from(0_u64)),
            (
                "desiredAcceptedImmigrantCount",
                Value::from(target_accepted),
            ),
        ]);
        let mut allocation = allocation_semantic
            .as_object()
            .expect("allocation object")
            .clone();
        allocation.insert(
            "allocationSha256".to_owned(),
            Value::String(canonical_sha256(&allocation_semantic).expect("allocation identity")),
        );
        let mut run_config = object([
            ("archivePolicyAuthority", archive),
            ("behaviorAttributionRequirement", behavior),
            ("operatorImplementation", operator.clone()),
        ]);
        if let Some(receipt) = evolvable.get("capacityReceipt") {
            run_config
                .as_object_mut()
                .expect("run config object")
                .insert("capacityReceipt".to_owned(), receipt.clone());
        }
        let mut config = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_pair_generation_v2".to_owned()),
            ),
            ("generationIndex", Value::from(generation_index)),
            ("targetUniqueCandidates", Value::from(target_accepted)),
            ("maxProposalAttempts", Value::from(max_attempts)),
            ("reproductionAllocation", Value::Object(allocation)),
            ("runConfig", run_config),
            ("operatorImplementation", operator),
        ]);
        let config_sha256 = canonical_sha256(&config).expect("generation config identity");
        config
            .as_object_mut()
            .expect("generation config object")
            .insert("configSha256".to_owned(), Value::String(config_sha256));
        config
    }

    fn structural_generation_config(
        generation_index: u64,
        target_accepted: u64,
        max_attempts: u64,
    ) -> Value {
        let mut config = fixture_generation_config(generation_index, target_accepted, max_attempts);
        let desired_immigrants = accepted_quota_immigrant_count(target_accepted, true);
        let allocation_semantic = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_reproduction_allocation_v2".to_owned()),
            ),
            ("targetAcceptedCandidates", Value::from(target_accepted)),
            (
                "desiredAcceptedOffspringCount",
                Value::from(target_accepted - desired_immigrants),
            ),
            (
                "desiredAcceptedImmigrantCount",
                Value::from(desired_immigrants),
            ),
        ]);
        let mut allocation = allocation_semantic
            .as_object()
            .expect("structural allocation object")
            .clone();
        allocation.insert(
            "allocationSha256".to_owned(),
            Value::String(
                canonical_sha256(&allocation_semantic).expect("structural allocation identity"),
            ),
        );
        let fields = config
            .as_object_mut()
            .expect("structural generation config object");
        fields.insert(
            "reproductionAllocation".to_owned(),
            Value::Object(allocation),
        );
        fields.remove("configSha256");
        let config_sha256 = canonical_sha256(&config).expect("structural generation identity");
        config
            .as_object_mut()
            .expect("structural generation config object")
            .insert("configSha256".to_owned(), Value::String(config_sha256));
        config
    }

    fn input_binding(kind: &str, path_suffix: &str) -> Value {
        let file_sha256 = sha(object([("kind", Value::String(format!("{kind}:file")))]));
        let semantic_sha256 = sha(object([(
            "kind",
            Value::String(format!("{kind}:semantic")),
        )]));
        let semantic = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_native_v5_proposal_input_binding_v1".to_owned()),
            ),
            ("kind", Value::String(kind.to_owned())),
            (
                "absolutePath",
                Value::String(format!(
                    "C:/v5-evolved-publication-tests/{path_suffix}.json"
                )),
            ),
            ("fileSha256", Value::String(file_sha256)),
            ("semanticSha256", Value::String(semantic_sha256)),
        ]);
        let mut binding = semantic.as_object().expect("input binding object").clone();
        binding.insert(
            "bindingSha256".to_owned(),
            Value::String(canonical_sha256(&semantic).expect("input binding identity")),
        );
        Value::Object(binding)
    }

    fn execution_authority(shared: &Value, config_sha256: &str) -> Value {
        let frozen_sha256 = shared
            .get("authoritySha256")
            .expect("shared authority identity")
            .clone();
        let batch_semantic = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_native_authority_v1".to_owned()),
            ),
            (
                "contractVersion",
                Value::String(CONTRACT_VERSION.to_owned()),
            ),
            ("crateVersion", Value::String("0.1.0".to_owned())),
            ("binaryName", Value::String("temporal-qd-batch".to_owned())),
            ("buildProfile", Value::String("release".to_owned())),
            (
                "executableSha256",
                Value::String(sha(object([(
                    "fixture",
                    Value::String("executable".to_owned()),
                )]))),
            ),
            (
                "sourceSha256",
                Value::String(sha(object([(
                    "fixture",
                    Value::String("source".to_owned()),
                )]))),
            ),
        ]);
        let mut batch = batch_semantic
            .as_object()
            .expect("batch authority object")
            .clone();
        let batch_sha256 = canonical_sha256(&batch_semantic).expect("batch authority identity");
        batch.insert(
            "authoritySha256".to_owned(),
            Value::String(batch_sha256.clone()),
        );
        let semantic = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_native_v5_proposal_execution_authority_v1".to_owned()),
            ),
            ("nativeBatchAuthority", Value::Object(batch)),
            ("nativeBatchAuthoritySha256", Value::String(batch_sha256)),
            ("expectedAuthoritySha256", frozen_sha256.clone()),
            ("frozenAuthoritySha256", frozen_sha256),
            (
                "generationConfigSha256",
                Value::String(config_sha256.to_owned()),
            ),
        ]);
        let mut execution = semantic
            .as_object()
            .expect("execution authority object")
            .clone();
        execution.insert(
            "authoritySha256".to_owned(),
            Value::String(canonical_sha256(&semantic).expect("execution authority identity")),
        );
        Value::Object(execution)
    }

    fn publication_fixture_with_limits(
        thread_cap: u64,
        target_accepted: u64,
        max_attempts: u64,
    ) -> (
        V5EvolvedTransactionRequest,
        V5EvolvedPublicationInputs,
        EmptyParents,
        CandidateIdentityLedger,
    ) {
        let shared_authority = shared_authority_fixture();
        let generation_config = fixture_generation_config(2, target_accepted, max_attempts);
        let generation_config_sha256 = generation_config
            .get("configSha256")
            .and_then(Value::as_str)
            .expect("generation config identity")
            .to_owned();
        let parent_archive = input_binding("parentArchive", "parent-archive");
        let identity_ledger = input_binding("identityLedger", "identity-ledger");
        let inputs = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_native_v5_proposal_inputs_v1".to_owned()),
            ),
            ("parentArchive", parent_archive.clone()),
            ("identityLedger", identity_ledger.clone()),
        ]);
        let execution_authority = execution_authority(&shared_authority, &generation_config_sha256);
        let final_newline = Value::String("lf".to_owned());
        let publication_inputs = V5EvolvedPublicationInputs::from_manifest_values(
            &generation_config,
            &final_newline,
            &execution_authority,
            &inputs,
        )
        .expect("parse full evolved publication fixture inputs");
        let parents = EmptyParents;
        let ledger = CandidateIdentityLedger::new(
            object([(
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_publication_test_ledger_v1".to_owned()),
            )]),
            Vec::<String>::new(),
        )
        .expect("construct test candidate ledger");
        let request = V5EvolvedTransactionRequest {
            shared_authority,
            generation_config_sha256,
            parent_archive_input_binding_sha256: parent_archive
                .get("bindingSha256")
                .and_then(Value::as_str)
                .expect("parent input binding identity")
                .to_owned(),
            identity_ledger_input_binding_sha256: identity_ledger
                .get("bindingSha256")
                .and_then(Value::as_str)
                .expect("ledger input binding identity")
                .to_owned(),
            generation_index: 2,
            target_accepted,
            max_attempts,
            evaluation_width: target_accepted,
            thread_cap,
            parent_schedule: None,
            parent_selector_state_sha256: sha(parents.compact_state()),
            identity_ledger_identity_sha256: sha(ledger.identity().clone()),
            identity_ledger_state_sha256: sha(ledger.compact_state()),
        };
        (request, publication_inputs, parents, ledger)
    }

    fn publication_fixture(
        thread_cap: u64,
    ) -> (
        V5EvolvedTransactionRequest,
        V5EvolvedPublicationInputs,
        EmptyParents,
        CandidateIdentityLedger,
    ) {
        publication_fixture_with_limits(thread_cap, 2, 2)
    }

    fn structural_publication_fixture(
        thread_cap: u64,
    ) -> (
        V5EvolvedTransactionRequest,
        V5EvolvedPublicationInputs,
        ExplicitParentRing,
        CandidateIdentityLedger,
    ) {
        // Two accepted candidates require one offspring and one immigrant;
        // the accepted-quota scheduler makes ordinal zero structural.
        let target_accepted = 2;
        let max_attempts = 8;
        let shared_authority = shared_authority_fixture();
        let authority = V5SharedConstructionAuthority::from_shared_object(&shared_authority)
            .expect("parse structural publication authority");
        let parent_config_sha256 = sha(object([(
            "fixture",
            Value::String("structural publication parents".to_owned()),
        )]));
        let parent_references = (0_u64..2)
            .map(|ordinal| {
                let seed = v5_proposal_seed(&parent_config_sha256, ordinal)
                    .expect("derive structural parent proposal seed");
                let material =
                    build_v5_g0_accepted_material(&authority, 1, ordinal, ordinal, &seed)
                        .expect("construct structural publication parent");
                parent_reference_from_v5_compact_record(
                    &authority,
                    &material.proposal_delta,
                    &material.record,
                )
                .expect("seal structural publication parent")
            })
            .collect();
        let parents = ExplicitParentRing::new(parent_references)
            .expect("construct structural publication parent ring");
        let generation_config = structural_generation_config(2, target_accepted, max_attempts);
        let generation_config_sha256 = generation_config
            .get("configSha256")
            .and_then(Value::as_str)
            .expect("structural generation config identity")
            .to_owned();
        let parent_archive = input_binding("parentArchive", "structural-parent-archive");
        let identity_ledger = input_binding("identityLedger", "structural-identity-ledger");
        let inputs = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_native_v5_proposal_inputs_v1".to_owned()),
            ),
            ("parentArchive", parent_archive.clone()),
            ("identityLedger", identity_ledger.clone()),
        ]);
        let execution_authority = execution_authority(&shared_authority, &generation_config_sha256);
        let publication_inputs = V5EvolvedPublicationInputs::from_manifest_values(
            &generation_config,
            &Value::String("lf".to_owned()),
            &execution_authority,
            &inputs,
        )
        .expect("parse structural evolved publication inputs");
        let ledger = CandidateIdentityLedger::new(
            object([(
                "schemaVersion",
                Value::String(
                    "temporal_qd_v5_evolved_structural_publication_test_ledger_v1".to_owned(),
                ),
            )]),
            Vec::<String>::new(),
        )
        .expect("construct structural candidate ledger");
        let request = V5EvolvedTransactionRequest {
            shared_authority,
            generation_config_sha256,
            parent_archive_input_binding_sha256: parent_archive
                .get("bindingSha256")
                .and_then(Value::as_str)
                .expect("structural parent input binding identity")
                .to_owned(),
            identity_ledger_input_binding_sha256: identity_ledger
                .get("bindingSha256")
                .and_then(Value::as_str)
                .expect("structural ledger input binding identity")
                .to_owned(),
            generation_index: 2,
            target_accepted,
            max_attempts,
            evaluation_width: target_accepted,
            thread_cap,
            parent_schedule: Some(
                RotatingParentSchedule::from_counts(2, 2)
                    .expect("construct structural parent schedule"),
            ),
            parent_selector_state_sha256: sha(parents.compact_state()),
            identity_ledger_identity_sha256: sha(ledger.identity().clone()),
            identity_ledger_state_sha256: sha(ledger.compact_state()),
        };
        (request, publication_inputs, parents, ledger)
    }

    #[derive(Debug)]
    struct PublishedStructuralBundle {
        fragment_receipt: Value,
        publication_receipt: V5EvolvedPublicationReceipt,
        pair_config: Vec<u8>,
        identity_ledger: Vec<u8>,
        population: Vec<u8>,
        evaluation: Vec<u8>,
        journal: Vec<u8>,
        parent_material: Vec<u8>,
    }

    fn canonical_parent_material(references: &[ParentReference]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for reference in references {
            assert!(reference.selection_audit.is_none());
            let semantic = object([
                (
                    "schemaVersion",
                    Value::String("temporal_qd_v5_fast_ephemeral_parent_material_v1".to_owned()),
                ),
                ("candidateId", Value::String(reference.candidate_id.clone())),
                (
                    "pairIdentitySha256",
                    Value::String(reference.pair_identity_sha256.clone()),
                ),
                ("pairPayload", reference.pair_payload.clone()),
            ]);
            let mut fields = semantic
                .as_object()
                .expect("canonical parent material row")
                .clone();
            fields.insert(
                "rowSha256".to_owned(),
                Value::String(canonical_sha256(&semantic).expect("parent material row identity")),
            );
            write_canonical_json(&Value::Object(fields), &mut bytes)
                .expect("encode canonical parent material row");
            bytes.push(b'\n');
        }
        bytes
    }

    fn publish_structural_bundle(
        request: &V5EvolvedTransactionRequest,
        inputs: &V5EvolvedPublicationInputs,
        transaction: &V5EvolvedTransactionResult,
    ) -> PublishedStructuralBundle {
        let plan = V5EvolvedPublicationPlan::derive(request, inputs)
            .expect("derive structural publication plan");
        let stream = prepare_v5_evolved_publication_stream(request, transaction, &plan)
            .expect("prepare structural publication stream");
        let mut private = InMemoryFragments::default();
        let mut parents = ParentCollector::default();
        let fragments = stream
            .materialize_accepted_fragments_and_parents(&mut private, &mut parents)
            .expect("materialize structural publication fragments and parents");
        let mut pair_config = Vec::new();
        let mut identity_ledger = Vec::new();
        let mut population = Vec::new();
        let mut evaluation = Vec::new();
        let mut journal = Vec::new();
        let publication_receipt = stream
            .write_bundle_from_fragments(
                &fragments,
                &mut private,
                &mut pair_config,
                &mut identity_ledger,
                &mut population,
                &mut evaluation,
                &mut journal,
            )
            .expect("assemble structural publication bundle");
        let verified = stream
            .verify_bundle_from_fragments(
                &fragments,
                &mut private,
                &mut Cursor::new(pair_config.clone()),
                &mut Cursor::new(identity_ledger.clone()),
                &mut Cursor::new(population.clone()),
                &mut Cursor::new(evaluation.clone()),
                &mut Cursor::new(journal.clone()),
            )
            .expect("verify structural publication bundle");
        assert_eq!(verified, publication_receipt);
        PublishedStructuralBundle {
            fragment_receipt: fragments.to_value().expect("encode fragment receipt"),
            publication_receipt,
            pair_config,
            identity_ledger,
            population,
            evaluation,
            journal,
            parent_material: canonical_parent_material(&parents.0),
        }
    }

    fn complete_two_immigrant_transaction(
        thread_cap: u64,
    ) -> (
        V5EvolvedTransactionRequest,
        V5EvolvedPublicationInputs,
        V5EvolvedTransactionResult,
    ) {
        let (request, inputs, mut parents, mut ledger) = publication_fixture(thread_cap);
        let result = execute_v5_evolved_transaction(request.clone(), &mut parents, &mut ledger)
            .expect("execute two accepted evolved immigrants");
        assert!(result.target_reached);
        assert_eq!(result.accepted_records.len(), 2);
        assert_eq!(result.attempts.len(), 2);
        (request, inputs, result)
    }

    fn complete_one_immigrant_after_global_duplicate() -> (
        V5EvolvedTransactionRequest,
        V5EvolvedPublicationInputs,
        V5EvolvedTransactionResult,
    ) {
        let (seed_request, inputs, mut seed_parents, mut seed_ledger) =
            publication_fixture_with_limits(1, 1, 2);
        let ledger_identity = seed_ledger.identity().clone();
        let seed_result = execute_v5_evolved_transaction(
            seed_request.clone(),
            &mut seed_parents,
            &mut seed_ledger,
        )
        .expect("construct first deterministic evolved immigrant");
        let duplicate_identity = seed_result.accepted_records[0]
            .candidate_identity_sha256
            .clone();
        let mut duplicate_ledger =
            CandidateIdentityLedger::new(ledger_identity, [duplicate_identity])
                .expect("preload first native evolved candidate identity");
        let mut retry_request = seed_request;
        retry_request.identity_ledger_identity_sha256 = sha(duplicate_ledger.identity().clone());
        retry_request.identity_ledger_state_sha256 = sha(duplicate_ledger.compact_state());
        let mut retry_parents = EmptyParents;
        let retry_result = execute_v5_evolved_transaction(
            retry_request.clone(),
            &mut retry_parents,
            &mut duplicate_ledger,
        )
        .expect("retry deterministic evolved immigrant after global duplicate");
        assert!(retry_result.target_reached);
        assert_eq!(retry_result.attempts.len(), 2);
        assert_eq!(retry_result.attempts[0].disposition, "rejected");
        assert!(retry_result.attempts[0].accepted_record_sha256.is_none());
        assert_eq!(retry_result.attempts[1].disposition, "accepted");
        assert_eq!(retry_result.accepted_records[0].proposal_ordinal, 1);
        (retry_request, inputs, retry_result)
    }

    fn candidate_free_attempt(
        base: &V5ProposalAttemptRecord,
        proposal_ordinal: u64,
        disposition: &str,
        reason_code: &str,
    ) -> (V5ProposalAttemptRecord, V5AttemptOutcomeAudit) {
        let mut attempt = base.clone();
        attempt.proposal_ordinal = proposal_ordinal;
        attempt.proposal_delta_sha256 = None;
        attempt.disposition = disposition.to_owned();
        attempt.reason_code = reason_code.to_owned();
        attempt.identity_ledger_effect = "not_checked".to_owned();
        attempt.accepted_record_sha256 = None;
        let audit = V5AttemptOutcomeAudit {
            generation_index: attempt.generation_index,
            proposal_ordinal,
            generation_config_sha256: attempt.generation_config_sha256.clone(),
            shared_authority_sha256: attempt.shared_authority_sha256.clone(),
            proposal_seed: attempt.proposal_seed.clone(),
            origin_kind: attempt.origin_kind.clone(),
            disposition: disposition.to_owned(),
            reason_code: reason_code.to_owned(),
            stage: "pre_plan".to_owned(),
            proposal_delta_sha256: None,
            lineage_refs_sha256: canonical_sha256(
                &attempt
                    .lineage_refs
                    .to_value()
                    .expect("encode candidate-free attempt lineage"),
            )
            .expect("hash candidate-free attempt lineage"),
            identity_ledger_effect: "not_checked".to_owned(),
            accepted_record_sha256: None,
        };
        attempt.outcome_audit_sha256 = audit.audit_sha256().expect("hash candidate-free audit");
        (attempt, audit)
    }

    fn rehash_object_field(value: &mut Value, field: &str) {
        let fields = value.as_object_mut().expect("self-hashed test object");
        fields.remove(field).expect("self-hash field present");
        let digest = canonical_sha256(&Value::Object(fields.clone())).expect("recompute self hash");
        fields.insert(field.to_owned(), Value::String(digest));
    }

    #[test]
    fn structural_publication_is_byte_identical_for_durable_and_fast() {
        let _guard = materialization_lock()
            .lock()
            .expect("serialize structural publication materialization");
        let (request, inputs, parents, ledger) = structural_publication_fixture(1);
        let mut durable_parents = parents.clone();
        let mut durable_ledger = ledger.clone();
        let durable = execute_v5_evolved_transaction(
            request.clone(),
            &mut durable_parents,
            &mut durable_ledger,
        )
        .expect("execute durable structural transaction");
        let mut fast_parents = parents;
        let mut fast_ledger = ledger;
        let fast = execute_v5_evolved_transaction_fast_ephemeral_with_progress(
            request.clone(),
            &mut fast_parents,
            &mut fast_ledger,
            None,
        )
        .expect("execute fast-ephemeral structural transaction");

        assert!(durable.target_reached && fast.target_reached);
        assert!(durable.proposal_deltas.iter().any(|delta| {
            delta.scheduled_kind == "structural_offspring"
                && delta.terminal_disposition == "accepted"
        }));
        assert_eq!(
            durable.to_value().expect("encode durable transaction"),
            fast.to_value().expect("encode fast transaction"),
            "fast construction must preserve the complete durable transaction",
        );

        let durable_bundle = publish_structural_bundle(&request, &inputs, &durable);
        let fast_bundle = publish_structural_bundle(&request, &inputs, &fast);
        assert_eq!(
            durable_bundle.fragment_receipt,
            fast_bundle.fragment_receipt
        );
        assert_eq!(
            durable_bundle.publication_receipt,
            fast_bundle.publication_receipt
        );
        assert_eq!(durable_bundle.pair_config, fast_bundle.pair_config);
        assert_eq!(durable_bundle.identity_ledger, fast_bundle.identity_ledger);
        assert_eq!(durable_bundle.population, fast_bundle.population);
        assert_eq!(durable_bundle.evaluation, fast_bundle.evaluation);
        assert_eq!(durable_bundle.journal, fast_bundle.journal);
        assert_eq!(
            durable_bundle.parent_material, fast_bundle.parent_material,
            "mandatory replay must publish the same canonical next-generation parent rows",
        );
    }

    #[test]
    fn two_accepted_records_stream_once_and_adopt_after_private_fragment_deletion() {
        let _guard = materialization_lock()
            .lock()
            .expect("serialize materialization observer");
        materialization_test_observer::reset();
        let (request, inputs, transaction) = complete_two_immigrant_transaction(1);
        let plan = V5EvolvedPublicationPlan::derive(&request, &inputs)
            .expect("derive cap-free evolved publication plan");
        let stream = prepare_v5_evolved_publication_stream(&request, &transaction, &plan)
            .expect("prepare evolved publication stream");

        let (fragments, receipt, pair_config, identity_ledger, population, evaluation, journal) = {
            let mut private = InMemoryFragments::default();
            let mut parents = ParentCollector::default();
            let fragments = stream
                .materialize_accepted_fragments_and_parents(&mut private, &mut parents)
                .expect("materialize exactly two accepted candidates");
            assert_eq!(parents.0.len(), 2);
            let authority =
                V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)
                    .expect("parse evolved parent authority");
            for parent in &parents.0 {
                crate::v5::verify_v5_evolved_parent_reference(&authority, parent)
                    .expect("recompile streamed evolved parent");
            }
            assert_eq!(
                materialization_test_observer::ACCEPTED_SINK_VISITS
                    .load(std::sync::atomic::Ordering::SeqCst),
                2,
                "one sealed replay callback per accepted record",
            );
            assert_eq!(
                materialization_test_observer::RICH_MATERIALIZATIONS
                    .load(std::sync::atomic::Ordering::SeqCst),
                2,
                "one rich materialization per accepted record",
            );
            assert_eq!(
                materialization_test_observer::PEAK_LIVE.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "rich candidate lifetime remains bounded to one",
            );
            for kind in [
                V5EvolvedPublicationFragmentKind::PopulationCandidates,
                V5EvolvedPublicationFragmentKind::EvaluationCandidates,
                V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries,
                V5EvolvedPublicationFragmentKind::GenerationJournalBindings,
            ] {
                assert_eq!(fragments.fragment(kind).row_count, 2);
            }

            let mut pair_config = Vec::new();
            let mut identity_ledger = Vec::new();
            let mut population = Vec::new();
            let mut evaluation = Vec::new();
            let mut journal = Vec::new();
            let receipt = stream
                .write_bundle_from_fragments(
                    &fragments,
                    &mut private,
                    &mut pair_config,
                    &mut identity_ledger,
                    &mut population,
                    &mut evaluation,
                    &mut journal,
                )
                .expect("assemble public evolved bundle from private fragments");
            let verified = stream
                .verify_bundle_from_fragments(
                    &fragments,
                    &mut private,
                    &mut Cursor::new(pair_config.clone()),
                    &mut Cursor::new(identity_ledger.clone()),
                    &mut Cursor::new(population.clone()),
                    &mut Cursor::new(evaluation.clone()),
                    &mut Cursor::new(journal.clone()),
                )
                .expect("verify fresh bundle against private fragments");
            assert_eq!(verified, receipt);
            (
                fragments,
                receipt,
                pair_config,
                identity_ledger,
                population,
                evaluation,
                journal,
            )
        };
        // `private` is out of scope here: recovery must authenticate only the
        // durable transaction/plan/receipt and public bytes.
        assert_eq!(fragments.population_candidates.row_count, 2);
        let ledger_value: Value =
            serde_json::from_slice(&identity_ledger[..identity_ledger.len() - 1])
                .expect("parse LF-terminated public identity ledger");
        let restored = V5EvolvedIdentityLedger::from_value(&ledger_value)
            .expect("parse self-authenticating public evolved ledger")
            .restore_candidate_identity_ledger()
            .expect("restore next-generation candidate ledger");
        assert_eq!(
            restored.compact_state(),
            transaction
                .schedule_state_receipt
                .final_identity_ledger_state,
        );
        assert_eq!(
            receipt.identity_ledger.semantic_sha256,
            ledger_value
                .get("identityLedgerSha256")
                .and_then(Value::as_str)
                .expect("public ledger semantic identity"),
        );
        let journal_value: Value = serde_json::from_slice(&journal[..journal.len() - 1])
            .expect("parse LF-terminated public generation journal");
        let final_state_sha256 = canonical_sha256(
            &transaction
                .schedule_state_receipt
                .final_identity_ledger_state,
        )
        .expect("final compact ledger state identity");
        assert_eq!(
            journal_value
                .get("globalIdentityLedger")
                .and_then(|value| value.get("identityLedgerSha256"))
                .and_then(Value::as_str),
            Some(receipt.identity_ledger.semantic_sha256.as_str()),
            "generation journal must name the actual fixed public ledger artifact",
        );
        assert_eq!(
            journal_value
                .get("globalIdentityLedger")
                .and_then(|value| value.get("finalIdentityLedgerStateSha256"))
                .and_then(Value::as_str),
            Some(final_state_sha256.as_str()),
            "final compact state identity remains distinct from public ledger identity",
        );

        let plan_value = plan.to_value().expect("encode publication plan");
        let receipt_value = receipt.to_value().expect("encode publication receipt");
        let adopted = verify_v5_evolved_publication_adoption(
            &request,
            &transaction,
            &plan_value,
            &receipt_value,
            &mut Cursor::new(pair_config.clone()),
            &mut Cursor::new(identity_ledger.clone()),
            &mut Cursor::new(population.clone()),
            &mut Cursor::new(evaluation.clone()),
            &mut Cursor::new(journal.clone()),
        )
        .expect("adopt public bundle after private fragment deletion");
        assert_eq!(adopted, receipt);
        assert_eq!(
            materialization_test_observer::RICH_MATERIALIZATIONS
                .load(std::sync::atomic::Ordering::SeqCst),
            2,
            "no-rich adoption must not rematerialize candidates",
        );

        let mut same_length_tamper = population;
        same_length_tamper[0] ^= 1;
        assert_eq!(
            same_length_tamper.len(),
            receipt.population.encoded_bytes as usize
        );
        assert!(
            verify_v5_evolved_publication_adoption(
                &request,
                &transaction,
                &plan_value,
                &receipt_value,
                &mut Cursor::new(pair_config),
                &mut Cursor::new(identity_ledger),
                &mut Cursor::new(same_length_tamper),
                &mut Cursor::new(evaluation),
                &mut Cursor::new(journal),
            )
            .is_err()
        );
    }

    #[test]
    fn funnel_v2_preserves_duplicate_accepted_and_candidate_free_attempt_order() {
        let _guard = materialization_lock()
            .lock()
            .expect("serialize materialization observer");
        let (request, inputs, transaction) = complete_one_immigrant_after_global_duplicate();

        // The offline constructor reconstructs an independently compiled
        // material for the duplicate even though it is rejected by the ledger.
        // Capture that sealed callback so the row-shape test never invents a
        // rich candidate or a fake compact record.
        let mut replay = CapturedReplayAttempts::default();
        replay_v5_evolved_transaction_with_accepted_sink(&request, &transaction, &mut replay)
            .expect("replay duplicate and accepted native attempts");
        assert_eq!(replay.rows.len(), 2);
        let (duplicate_attempt, duplicate_audit, duplicate_material) = replay.rows.remove(0);
        let (accepted_attempt, accepted_audit, accepted_material) = replay.rows.remove(0);
        assert!(duplicate_material.is_some());
        assert!(accepted_material.is_some());

        // Candidate-free pre-plan rows never borrow a compact material.  They
        // still get immutable attempt identities and must remain in ordinal
        // order beside duplicate/accepted candidate-stage rows.
        let (no_op_attempt, no_op_audit) =
            candidate_free_attempt(&duplicate_attempt, 2, "no_op", "no_eligible_operation");
        let (rejected_attempt, rejected_audit) =
            candidate_free_attempt(&duplicate_attempt, 3, "rejected", "pre_plan_rejected");
        let rows = [
            evolved_funnel_attempt_entry(
                &duplicate_attempt,
                &duplicate_audit,
                duplicate_material.as_ref(),
            )
            .expect("project duplicate attempt row"),
            evolved_funnel_attempt_entry(
                &accepted_attempt,
                &accepted_audit,
                accepted_material.as_ref(),
            )
            .expect("project accepted attempt row"),
            evolved_funnel_attempt_entry(&no_op_attempt, &no_op_audit, None)
                .expect("project candidate-free no-op row"),
            evolved_funnel_attempt_entry(&rejected_attempt, &rejected_audit, None)
                .expect("project candidate-free rejected row"),
        ];
        assert_eq!(
            rows.iter()
                .map(|row| row.get("proposalOrdinal").and_then(Value::as_u64))
                .collect::<Vec<_>>(),
            vec![Some(0), Some(1), Some(2), Some(3)],
        );
        let expected_attempt_shas = [
            duplicate_attempt
                .attempt_sha256()
                .expect("duplicate attempt SHA"),
            accepted_attempt
                .attempt_sha256()
                .expect("accepted attempt SHA"),
            no_op_attempt.attempt_sha256().expect("no-op attempt SHA"),
            rejected_attempt
                .attempt_sha256()
                .expect("rejected attempt SHA"),
        ];
        assert_eq!(
            rows.iter()
                .map(|row| row.get("entrySha256").and_then(Value::as_str))
                .collect::<Vec<_>>(),
            expected_attempt_shas
                .iter()
                .map(String::as_str)
                .map(Some)
                .collect::<Vec<_>>(),
            "outer funnel identities are proposal attempt SHA-256 values, never compact-record aliases",
        );
        assert_eq!(
            rows[0]
                .get("funnelCandidate")
                .and_then(|value| value.get("admission"))
                .and_then(|value| value.get("outcome"))
                .and_then(Value::as_str),
            Some("rejected_duplicate"),
        );
        assert!(rows[0].get("acceptedCompactRecordSha256").is_none());
        assert_eq!(
            rows[1].get("disposition").and_then(Value::as_str),
            Some("accepted")
        );
        assert!(rows[1].get("acceptedCompactRecordSha256").is_some());
        for row in [&rows[2], &rows[3]] {
            assert!(row.get("candidate").is_none());
            assert!(row.get("proposal").is_none());
            assert!(row.get("funnelCandidate").is_none());
            assert!(row.get("acceptedCompactRecordSha256").is_none());
        }

        materialization_test_observer::reset();
        let plan = V5EvolvedPublicationPlan::derive(&request, &inputs)
            .expect("derive duplicate-aware evolved publication plan");
        let stream = prepare_v5_evolved_publication_stream(&request, &transaction, &plan)
            .expect("prepare duplicate-aware evolved publication stream");
        let mut private = InMemoryFragments::default();
        let fragments = stream
            .materialize_accepted_fragments(&mut private)
            .expect("materialize one accepted rich candidate and two funnel attempts");
        assert_eq!(fragments.accepted_candidate_count, 1);
        assert_eq!(fragments.proposal_attempt_count, 2);
        assert_eq!(fragments.population_candidates.row_count, 1);
        assert_eq!(fragments.evaluation_candidates.row_count, 1);
        assert_eq!(fragments.generation_journal_bindings.row_count, 1);
        assert_eq!(fragments.evaluation_funnel_entries.row_count, 2);
        assert_eq!(
            materialization_test_observer::RICH_MATERIALIZATIONS
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "duplicate candidate-stage projection must not rich-materialize",
        );
        assert_eq!(
            materialization_test_observer::PEAK_LIVE.load(std::sync::atomic::Ordering::SeqCst),
            1,
        );

        let funnel_bytes = private
            .bytes
            .get(&V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries)
            .expect("funnel fragment bytes");
        let mut wrapped = Vec::with_capacity(funnel_bytes.len() + 2);
        wrapped.extend_from_slice(b"[");
        wrapped.extend_from_slice(funnel_bytes);
        wrapped.extend_from_slice(b"]");
        let emitted: Value =
            serde_json::from_slice(&wrapped).expect("parse ordered funnel fragment");
        let emitted = emitted.as_array().expect("funnel fragment array");
        assert_eq!(emitted.len(), 2);
        assert_eq!(
            emitted[0].get("entrySha256").and_then(Value::as_str),
            Some(expected_attempt_shas[0].as_str()),
        );
        assert_eq!(
            emitted[1].get("entrySha256").and_then(Value::as_str),
            Some(expected_attempt_shas[1].as_str()),
        );
        assert_eq!(
            emitted[0]
                .get("funnelCandidate")
                .and_then(|value| value.get("schemaVersion"))
                .and_then(Value::as_str),
            Some("temporal_qd_proposal_funnel_stage_v1"),
        );
    }

    #[test]
    fn fragment_receipt_binding_is_canonical_rejects_substitution_and_is_cap_invariant() {
        let _guard = materialization_lock()
            .lock()
            .expect("serialize fragment materialization");
        let (serial_request, serial_inputs, mut serial_parents, mut serial_ledger) =
            publication_fixture_with_limits(1, 1, 1);
        let serial_transaction = execute_v5_evolved_transaction(
            serial_request.clone(),
            &mut serial_parents,
            &mut serial_ledger,
        )
        .expect("execute serial evolved transaction");
        let serial_plan = V5EvolvedPublicationPlan::derive(&serial_request, &serial_inputs)
            .expect("derive serial publication plan");
        let serial_stream = prepare_v5_evolved_publication_stream(
            &serial_request,
            &serial_transaction,
            &serial_plan,
        )
        .expect("prepare serial publication stream");
        let mut serial_private = InMemoryFragments::default();
        let serial_fragments = serial_stream
            .materialize_accepted_fragments(&mut serial_private)
            .expect("materialize serial fragment receipt");
        let binding = serial_fragments
            .object_binding()
            .expect("derive canonical fragment receipt binding");
        binding
            .validate()
            .expect("validate canonical fragment binding");
        assert_eq!(
            binding.relative_path,
            v5_native_object_relative_path(&binding.fragment_bundle_sha256)
                .expect("derive fragment receipt object path"),
        );
        assert_eq!(
            binding.value,
            serial_fragments
                .to_value()
                .expect("encode canonical fragment receipt"),
        );
        let binding_value = binding.to_value().expect("encode fragment binding");
        assert_eq!(
            V5EvolvedPublicationFragmentReceiptObjectBinding::from_value(&binding_value)
                .expect("strictly parse canonical fragment binding"),
            binding,
        );
        assert_eq!(
            V5EvolvedPublicationFragmentReceipt::from_value(
                &serial_fragments.to_value().expect("encode receipt")
            )
            .expect("strictly parse canonical fragment receipt"),
            serial_fragments,
        );

        let mut missing = binding_value.clone();
        missing
            .as_object_mut()
            .expect("binding object")
            .remove("value");
        assert!(V5EvolvedPublicationFragmentReceiptObjectBinding::from_value(&missing).is_err());

        // A separately self-hashed receipt with a different fragment digest is
        // still not a replacement for the object named by this binding.
        let mut replacement_receipt = serial_fragments.clone();
        replacement_receipt.population_candidates.fragment_sha256 = sha(object([(
            "replacement",
            Value::String("fragment receipt".to_owned()),
        )]));
        let mut replacement = binding.clone();
        replacement.value = replacement_receipt
            .to_value()
            .expect("encode independently self-hashed replacement receipt");
        assert!(replacement.validate().is_err());

        let mut alias = binding.clone();
        alias.relative_path = "v5-native/objects/sha256/fragment-alias.json".to_owned();
        assert!(alias.validate().is_err());

        let (parallel_request, parallel_inputs, mut parallel_parents, mut parallel_ledger) =
            publication_fixture_with_limits(8, 1, 1);
        let parallel_transaction = execute_v5_evolved_transaction(
            parallel_request.clone(),
            &mut parallel_parents,
            &mut parallel_ledger,
        )
        .expect("execute bounded-parallel evolved transaction");
        let parallel_plan = V5EvolvedPublicationPlan::derive(&parallel_request, &parallel_inputs)
            .expect("derive bounded-parallel publication plan");
        let parallel_stream = prepare_v5_evolved_publication_stream(
            &parallel_request,
            &parallel_transaction,
            &parallel_plan,
        )
        .expect("prepare bounded-parallel publication stream");
        let mut parallel_private = InMemoryFragments::default();
        let parallel_fragments = parallel_stream
            .materialize_accepted_fragments(&mut parallel_private)
            .expect("materialize bounded-parallel fragment receipt");
        assert_eq!(
            serial_fragments
                .to_value()
                .expect("serial fragment receipt value"),
            parallel_fragments
                .to_value()
                .expect("parallel fragment receipt value"),
            "thread cap is execution telemetry only and cannot alter the fragment receipt",
        );
        assert_eq!(
            binding.to_value().expect("serial fragment receipt binding"),
            parallel_fragments
                .object_binding()
                .expect("parallel fragment receipt binding")
                .to_value()
                .expect("encode parallel fragment receipt binding"),
        );
    }

    #[test]
    fn publication_plan_is_cap_free_and_rejects_authority_input_and_config_drift() {
        let (request, inputs, _, _) = publication_fixture(1);
        let plan = V5EvolvedPublicationPlan::derive(&request, &inputs)
            .expect("derive thread-cap-one publication plan");
        let mut cap_eight = request.clone();
        cap_eight.thread_cap = 8;
        let cap_eight_plan = V5EvolvedPublicationPlan::derive(&cap_eight, &inputs)
            .expect("derive thread-cap-eight publication plan");
        assert_eq!(
            plan.publication_plan_sha256()
                .expect("cap-one plan identity"),
            cap_eight_plan
                .publication_plan_sha256()
                .expect("cap-eight plan identity"),
        );
        assert_eq!(
            plan.publication_request_sha256()
                .expect("cap-one request identity"),
            cap_eight_plan
                .publication_request_sha256()
                .expect("cap-eight request identity"),
        );

        let mut authority_drift = inputs.clone();
        authority_drift
            .execution_authority
            .as_object_mut()
            .expect("execution authority object")
            .insert(
                "expectedAuthoritySha256".to_owned(),
                Value::String(sha(object([(
                    "drift",
                    Value::String("authority".to_owned()),
                )]))),
            );
        rehash_object_field(&mut authority_drift.execution_authority, "authoritySha256");
        let authority_drift = V5EvolvedPublicationInputs::from_manifest_values(
            &authority_drift.generation_config,
            &Value::String(authority_drift.final_newline.clone()),
            &authority_drift.execution_authority,
            &authority_drift.inputs,
        )
        .expect("parse self-rehashed execution authority transport");
        assert!(V5EvolvedPublicationPlan::derive(&request, &authority_drift).is_err());

        let mut input_drift = inputs.clone();
        input_drift
            .inputs
            .as_object_mut()
            .expect("publication inputs object")
            .insert(
                "parentArchive".to_owned(),
                input_binding("parentArchive", "substituted-parent-archive"),
            );
        let input_drift = V5EvolvedPublicationInputs::from_manifest_values(
            &input_drift.generation_config,
            &Value::String(input_drift.final_newline.clone()),
            &input_drift.execution_authority,
            &input_drift.inputs,
        )
        .expect("parse self-authenticating substituted input binding");
        assert!(V5EvolvedPublicationPlan::derive(&request, &input_drift).is_err());

        let mut config_drift = inputs.clone();
        config_drift
            .generation_config
            .as_object_mut()
            .expect("generation config object")
            .get_mut("runConfig")
            .and_then(Value::as_object_mut)
            .expect("generation run config")
            .insert("archivePolicyAuthority".to_owned(), Value::Null);
        rehash_object_field(&mut config_drift.generation_config, "configSha256");
        let config_sha256 = config_drift
            .generation_config
            .get("configSha256")
            .and_then(Value::as_str)
            .expect("rehashed config identity")
            .to_owned();
        let mut config_request = request.clone();
        config_request.generation_config_sha256 = config_sha256;
        let config_drift = V5EvolvedPublicationInputs::from_manifest_values(
            &config_drift.generation_config,
            &Value::String(config_drift.final_newline.clone()),
            &config_drift.execution_authority,
            &config_drift.inputs,
        )
        .expect("parse self-rehashed config transport");
        assert!(V5EvolvedPublicationPlan::derive(&config_request, &config_drift).is_err());
    }
}
