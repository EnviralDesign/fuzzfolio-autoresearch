//! Compact, durable G0 funnel authority.
//!
//! A G0 transaction admits a complete construction pool but evaluates only
//! its bootstrap selection.  This module keeps those two populations
//! separate: the proposal stream covers every scheduler attempt, while the
//! evaluation-funnel stream covers only selected candidates.  All remaining
//! facts are bound through the already replayed compact pool/index/ledger
//! roots.  No rich candidate is reconstructed here.

use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
};

use temporal_qd_contract::{
    CanonicalSha256Writer, ContractError, Map, Value, canonical_sha256, write_canonical_json,
};

use crate::{
    v5::{
        V5_PROPOSAL_FUNNEL_ENTRY_SCHEMA, V5AttemptOutcomeAudit, V5CompactAcceptedRecord,
        V5FunnelAdmission, V5ProposalAttemptRecord, v5_funnel_candidate_projection,
        v5_native_object_relative_path,
    },
    v5_publication::{V5G0PublicationFragments, V5G0PublicationReceipt},
    v5_transaction::{
        V5G0TransactionError, V5G0TransactionRequest, V5G0TransactionResult,
        verify_v5_g0_transaction_replay,
    },
};

pub const V5_G0_FUNNEL_FRAGMENTS_SCHEMA: &str = "temporal_qd_v5_g0_funnel_fragments_v1";
pub const V5_G0_FUNNEL_PROJECTION_STREAM_SCHEMA: &str =
    "temporal_qd_v5_g0_funnel_projection_stream_receipt_v1";
pub const V5_G0_FUNNEL_PROJECTION_STREAM_PATH: &str = "v5-native/g0-funnel-projections.jsonl";

#[derive(Debug, thiserror::Error)]
pub enum V5G0FunnelError {
    #[error("v5 G0 transaction failure: {0}")]
    Transaction(#[from] V5G0TransactionError),
    #[error("canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("v5 G0 funnel stream I/O failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("v5 G0 funnel contract failure: {0}")]
    Contract(String),
}

pub type Result<T> = std::result::Result<T, V5G0FunnelError>;

fn contract(message: impl Into<String>) -> V5G0FunnelError {
    V5G0FunnelError::Contract(message.into())
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
        .filter(|value| !value.is_empty() && *value == value.trim())
        .ok_or_else(|| contract(format!("{label} must be a canonical nonempty string")))?;
    Ok(value.to_owned())
}

fn exact_sha(value: &Value, label: &str) -> Result<String> {
    let value = exact_text(value, label)?;
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value.as_bytes()[7..]
            .iter()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(contract(format!("{label} must be lowercase SHA-256")));
    }
    Ok(value)
}

fn sha_field(value: &Value, key: &str, label: &str) -> Result<String> {
    exact_sha(required(value, key, label)?, &format!("{label} {key}"))
}

/// The two compact byte streams exposed to a rotating prefinalizer.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum V5G0FunnelProjectionKind {
    ProposalAttemptEntries,
    EvaluationPopulationFunnelEntries,
}

impl V5G0FunnelProjectionKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProposalAttemptEntries => "proposalAttemptEntries",
            Self::EvaluationPopulationFunnelEntries => "evaluationPopulationFunnelEntries",
        }
    }
}

/// A caller-owned file sink.  Core supplies exact comma-separated canonical
/// array-fragment bytes; the sink may persist them but must not add framing or
/// newlines.  Rows contain only compact proposal/candidate facts.
pub trait V5G0FunnelProjectionSink {
    fn write_projection(
        &mut self,
        kind: V5G0FunnelProjectionKind,
        canonical_bytes: &[u8],
    ) -> std::io::Result<()>;
}

/// File-backed sink for the durable all-attempt JSONL projection.  Each call
/// receives one complete canonical JSON object followed by exactly one LF.
/// Core owns ordering/framing; batch only persists the supplied bytes.
pub trait V5G0FunnelProjectionStreamSink {
    fn write_projection_row(&mut self, canonical_json_line: &[u8]) -> std::io::Result<()>;
}

impl<T: Write> V5G0FunnelProjectionStreamSink for T {
    fn write_projection_row(&mut self, canonical_json_line: &[u8]) -> std::io::Result<()> {
        self.write_all(canonical_json_line)
    }
}

/// Authenticated descriptor for the fixed public G0 proposal-projection
/// stream.  The semantic input hashes tie JSONL framing back to the compact
/// funnel receipt; `raw_sha256` and `size_bytes` authenticate the actual file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5G0FunnelProjectionStreamReceipt {
    pub relative_path: String,
    pub row_schema: String,
    pub input_g0_funnel_fragments_sha256: String,
    pub input_proposal_attempt_fragment_sha256: String,
    pub raw_sha256: String,
    pub size_bytes: u64,
    pub row_count: u64,
}

impl V5G0FunnelProjectionStreamReceipt {
    fn semantic_value(&self) -> Result<Value> {
        if self.relative_path != V5_G0_FUNNEL_PROJECTION_STREAM_PATH {
            return Err(contract("v5 G0 funnel projection stream path drifted"));
        }
        if self.row_schema != V5_PROPOSAL_FUNNEL_ENTRY_SCHEMA {
            return Err(contract("v5 G0 funnel projection row schema drifted"));
        }
        for (label, identity) in [
            (
                "input funnel fragments",
                &self.input_g0_funnel_fragments_sha256,
            ),
            (
                "input proposal attempt fragment",
                &self.input_proposal_attempt_fragment_sha256,
            ),
            ("raw file", &self.raw_sha256),
        ] {
            let _ = exact_sha(
                &Value::String(identity.clone()),
                &format!("v5 G0 funnel projection {label} SHA-256"),
            )?;
        }
        if self.size_bytes == 0 || self.row_count == 0 {
            return Err(contract("v5 G0 funnel projection stream is empty"));
        }
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_G0_FUNNEL_PROJECTION_STREAM_SCHEMA.to_owned()),
            ),
            ("relativePath", Value::String(self.relative_path.clone())),
            ("rowSchema", Value::String(self.row_schema.clone())),
            (
                "inputG0FunnelFragmentsSha256",
                Value::String(self.input_g0_funnel_fragments_sha256.clone()),
            ),
            (
                "inputProposalAttemptFragmentSha256",
                Value::String(self.input_proposal_attempt_fragment_sha256.clone()),
            ),
            ("rawSha256", Value::String(self.raw_sha256.clone())),
            ("sizeBytes", Value::from(self.size_bytes)),
            ("rowCount", Value::from(self.row_count)),
        ]))
    }

    pub fn projection_stream_receipt_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 G0 funnel projection stream receipt")
            .clone();
        fields.insert(
            "projectionStreamReceiptSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 G0 funnel projection stream receipt")?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "relativePath",
                "rowSchema",
                "inputG0FunnelFragmentsSha256",
                "inputProposalAttemptFragmentSha256",
                "rawSha256",
                "sizeBytes",
                "rowCount",
                "projectionStreamReceiptSha256",
            ],
            "v5 G0 funnel projection stream receipt",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_G0_FUNNEL_PROJECTION_STREAM_SCHEMA)
        {
            return Err(contract(
                "v5 G0 funnel projection stream receipt schema is invalid",
            ));
        }
        let receipt = Self {
            relative_path: exact_text(
                required(
                    value,
                    "relativePath",
                    "v5 G0 funnel projection stream receipt",
                )?,
                "v5 G0 funnel projection stream path",
            )?,
            row_schema: exact_text(
                required(value, "rowSchema", "v5 G0 funnel projection stream receipt")?,
                "v5 G0 funnel projection row schema",
            )?,
            input_g0_funnel_fragments_sha256: sha_field(
                value,
                "inputG0FunnelFragmentsSha256",
                "v5 G0 funnel projection stream receipt",
            )?,
            input_proposal_attempt_fragment_sha256: sha_field(
                value,
                "inputProposalAttemptFragmentSha256",
                "v5 G0 funnel projection stream receipt",
            )?,
            raw_sha256: sha_field(value, "rawSha256", "v5 G0 funnel projection stream receipt")?,
            size_bytes: required(value, "sizeBytes", "v5 G0 funnel projection stream receipt")?
                .as_u64()
                .ok_or_else(|| contract("v5 G0 funnel projection byte count is invalid"))?,
            row_count: required(value, "rowCount", "v5 G0 funnel projection stream receipt")?
                .as_u64()
                .ok_or_else(|| contract("v5 G0 funnel projection row count is invalid"))?,
        };
        let supplied = sha_field(
            value,
            "projectionStreamReceiptSha256",
            "v5 G0 funnel projection stream receipt",
        )?;
        if supplied != receipt.projection_stream_receipt_sha256()? || &receipt.to_value()? != value
        {
            return Err(contract(
                "v5 G0 funnel projection stream receipt identity drifted",
            ));
        }
        Ok(receipt)
    }

    /// Canonical immutable object-store binding for the stream descriptor.
    /// The descriptor's `relativePath` names the fixed public JSONL artifact;
    /// this binding separately names the descriptor object by its self-hash.
    pub fn object_binding(&self) -> Result<V5G0FunnelProjectionStreamReceiptObjectBinding> {
        let value = self.to_value()?;
        let g0_funnel_projection_stream_receipt_sha256 = self.projection_stream_receipt_sha256()?;
        Ok(V5G0FunnelProjectionStreamReceiptObjectBinding {
            relative_path: v5_native_object_relative_path(
                &g0_funnel_projection_stream_receipt_sha256,
            )
            .map_err(V5G0TransactionError::from)?,
            g0_funnel_projection_stream_receipt_sha256,
            value,
        })
    }
}

/// Content-addressed durable object for a G0 projection-stream descriptor.
#[derive(Clone, Debug, PartialEq)]
pub struct V5G0FunnelProjectionStreamReceiptObjectBinding {
    pub g0_funnel_projection_stream_receipt_sha256: String,
    pub relative_path: String,
    pub value: Value,
}

impl V5G0FunnelProjectionStreamReceiptObjectBinding {
    pub fn validate(&self) -> Result<()> {
        let identity = exact_sha(
            &Value::String(self.g0_funnel_projection_stream_receipt_sha256.clone()),
            "v5 G0 funnel projection stream receipt object SHA-256",
        )?;
        let expected_path =
            v5_native_object_relative_path(&identity).map_err(V5G0TransactionError::from)?;
        let receipt = V5G0FunnelProjectionStreamReceipt::from_value(&self.value)?;
        if self.relative_path != expected_path
            || receipt.projection_stream_receipt_sha256()? != identity
            || receipt.to_value()? != self.value
        {
            return Err(contract(
                "v5 G0 funnel projection stream receipt object binding drifted",
            ));
        }
        Ok(())
    }

    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(object([
            (
                "g0FunnelProjectionStreamReceiptSha256",
                Value::String(self.g0_funnel_projection_stream_receipt_sha256.clone()),
            ),
            ("relativePath", Value::String(self.relative_path.clone())),
            ("value", self.value.clone()),
        ]))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(
            value,
            "v5 G0 funnel projection stream receipt object binding",
        )?;
        exact_keys(
            fields,
            &[
                "g0FunnelProjectionStreamReceiptSha256",
                "relativePath",
                "value",
            ],
            "v5 G0 funnel projection stream receipt object binding",
        )?;
        let binding = Self {
            g0_funnel_projection_stream_receipt_sha256: sha_field(
                value,
                "g0FunnelProjectionStreamReceiptSha256",
                "v5 G0 funnel projection stream receipt object binding",
            )?,
            relative_path: exact_text(
                required(
                    value,
                    "relativePath",
                    "v5 G0 funnel projection stream receipt object binding",
                )?,
                "v5 G0 funnel projection stream receipt object path",
            )?,
            value: required(
                value,
                "value",
                "v5 G0 funnel projection stream receipt object binding",
            )?
            .clone(),
        };
        binding.validate()?;
        if &binding.to_value()? != value {
            return Err(contract(
                "v5 G0 funnel projection stream receipt object binding is not canonical",
            ));
        }
        Ok(binding)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5G0FunnelStreamDescriptor {
    pub fragment_sha256: String,
    pub encoded_bytes: u64,
    pub row_count: u64,
}

impl V5G0FunnelStreamDescriptor {
    fn validate(&self, label: &str) -> Result<()> {
        let _ = exact_sha(
            &Value::String(self.fragment_sha256.clone()),
            &format!("{label} fragment SHA-256"),
        )?;
        if self.row_count == 0 || self.encoded_bytes == 0 {
            return Err(contract(format!("{label} descriptor is empty")));
        }
        Ok(())
    }

    pub fn to_value(&self) -> Result<Value> {
        self.validate("v5 G0 funnel stream")?;
        Ok(object([
            (
                "fragmentSha256",
                Value::String(self.fragment_sha256.clone()),
            ),
            ("encodedBytes", Value::from(self.encoded_bytes)),
            ("rowCount", Value::from(self.row_count)),
        ]))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 G0 funnel stream descriptor")?;
        exact_keys(
            fields,
            &["fragmentSha256", "encodedBytes", "rowCount"],
            "v5 G0 funnel stream descriptor",
        )?;
        let descriptor = Self {
            fragment_sha256: exact_sha(
                required(value, "fragmentSha256", "v5 G0 funnel stream descriptor")?,
                "v5 G0 funnel stream descriptor SHA-256",
            )?,
            encoded_bytes: required(value, "encodedBytes", "v5 G0 funnel stream descriptor")?
                .as_u64()
                .ok_or_else(|| contract("v5 G0 funnel stream byte count is invalid"))?,
            row_count: required(value, "rowCount", "v5 G0 funnel stream descriptor")?
                .as_u64()
                .ok_or_else(|| contract("v5 G0 funnel stream row count is invalid"))?,
        };
        descriptor.validate("v5 G0 funnel stream")?;
        if &descriptor.to_value()? != value {
            return Err(contract("v5 G0 funnel stream descriptor is not canonical"));
        }
        Ok(descriptor)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5G0FunnelSetDescriptor {
    pub semantic_sha256: String,
    pub row_count: u64,
}

impl V5G0FunnelSetDescriptor {
    fn validate(&self, label: &str) -> Result<()> {
        let _ = exact_sha(
            &Value::String(self.semantic_sha256.clone()),
            &format!("{label} semantic SHA-256"),
        )?;
        if self.row_count == 0 {
            return Err(contract(format!("{label} set is empty")));
        }
        Ok(())
    }

    pub fn to_value(&self) -> Result<Value> {
        self.validate("v5 G0 funnel set")?;
        Ok(object([
            (
                "semanticSha256",
                Value::String(self.semantic_sha256.clone()),
            ),
            ("rowCount", Value::from(self.row_count)),
        ]))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 G0 funnel set descriptor")?;
        exact_keys(
            fields,
            &["semanticSha256", "rowCount"],
            "v5 G0 funnel set descriptor",
        )?;
        let descriptor = Self {
            semantic_sha256: exact_sha(
                required(value, "semanticSha256", "v5 G0 funnel set descriptor")?,
                "v5 G0 funnel set semantic SHA-256",
            )?,
            row_count: required(value, "rowCount", "v5 G0 funnel set descriptor")?
                .as_u64()
                .ok_or_else(|| contract("v5 G0 funnel set row count is invalid"))?,
        };
        descriptor.validate("v5 G0 funnel set")?;
        if &descriptor.to_value()? != value {
            return Err(contract("v5 G0 funnel set descriptor is not canonical"));
        }
        Ok(descriptor)
    }
}

/// Durable compact authority handed to the rotating prefinalizer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5G0FunnelFragments {
    pub generation_index: u64,
    pub generation_config_sha256: String,
    pub shared_authority_sha256: String,
    pub transaction_sha256: String,
    pub publication_plan_sha256: String,
    pub publication_receipt_sha256: String,
    pub attempt_journal_sha256: String,
    pub proposal_attempt_count: u64,
    pub accepted_count: u64,
    pub selected_count: u64,
    pub proposal_attempt_stream: V5G0FunnelStreamDescriptor,
    pub accepted_pool_membership: V5G0FunnelSetDescriptor,
    pub selected_projection_index: V5G0FunnelSetDescriptor,
    pub campaign_ledger: V5G0FunnelSetDescriptor,
    pub g0_selection: V5G0FunnelSetDescriptor,
    pub evaluation_population_funnel_entries: V5G0FunnelStreamDescriptor,
    pub evaluation_population_sha256: String,
    pub evaluation_population_file_sha256: String,
    pub compact_identity_ledger: V5G0FunnelSetDescriptor,
}

/// Explicit durable name for the self-hashed G0 receipt.
pub type V5G0FunnelFragmentReceipt = V5G0FunnelFragments;

impl V5G0FunnelFragments {
    fn semantic_value(&self) -> Result<Value> {
        for (label, sha) in [
            ("generation config", &self.generation_config_sha256),
            ("shared authority", &self.shared_authority_sha256),
            ("transaction", &self.transaction_sha256),
            ("publication plan", &self.publication_plan_sha256),
            ("publication receipt", &self.publication_receipt_sha256),
            ("attempt journal", &self.attempt_journal_sha256),
            ("evaluation population", &self.evaluation_population_sha256),
            (
                "evaluation population file",
                &self.evaluation_population_file_sha256,
            ),
        ] {
            let _ = exact_sha(
                &Value::String(sha.clone()),
                &format!("v5 G0 funnel {label} SHA-256"),
            )?;
        }
        if self.generation_index != 1
            || self.proposal_attempt_count < self.accepted_count
            || self.accepted_count < self.selected_count
            || self.selected_count == 0
            || self.proposal_attempt_stream.row_count != self.proposal_attempt_count
            || self.accepted_pool_membership.row_count != self.accepted_count
            || self.selected_projection_index.row_count != self.selected_count
            || self.campaign_ledger.row_count != self.accepted_count
            || self.g0_selection.row_count != self.selected_count
            || self.evaluation_population_funnel_entries.row_count != self.selected_count
            || self.compact_identity_ledger.row_count != self.accepted_count
        {
            return Err(contract("v5 G0 funnel local accounting drifted"));
        }
        self.proposal_attempt_stream
            .validate("v5 G0 proposal attempt stream")?;
        self.accepted_pool_membership
            .validate("v5 G0 accepted pool membership")?;
        self.selected_projection_index
            .validate("v5 G0 selected projection index")?;
        self.campaign_ledger.validate("v5 G0 campaign ledger")?;
        self.g0_selection.validate("v5 G0 selection")?;
        self.evaluation_population_funnel_entries
            .validate("v5 G0 evaluation funnel")?;
        self.compact_identity_ledger
            .validate("v5 G0 compact identity ledger")?;
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_G0_FUNNEL_FRAGMENTS_SCHEMA.to_owned()),
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
                "transactionSha256",
                Value::String(self.transaction_sha256.clone()),
            ),
            (
                "publicationPlanSha256",
                Value::String(self.publication_plan_sha256.clone()),
            ),
            (
                "publicationReceiptSha256",
                Value::String(self.publication_receipt_sha256.clone()),
            ),
            (
                "attemptJournalSha256",
                Value::String(self.attempt_journal_sha256.clone()),
            ),
            (
                "proposalAttemptCount",
                Value::from(self.proposal_attempt_count),
            ),
            ("acceptedCount", Value::from(self.accepted_count)),
            ("selectedCount", Value::from(self.selected_count)),
            (
                "proposalAttemptStream",
                self.proposal_attempt_stream.to_value()?,
            ),
            (
                "acceptedPoolMembership",
                self.accepted_pool_membership.to_value()?,
            ),
            (
                "selectedProjectionIndex",
                self.selected_projection_index.to_value()?,
            ),
            ("campaignLedger", self.campaign_ledger.to_value()?),
            ("g0Selection", self.g0_selection.to_value()?),
            (
                "evaluationPopulationFunnelEntries",
                self.evaluation_population_funnel_entries.to_value()?,
            ),
            (
                "evaluationPopulationSha256",
                Value::String(self.evaluation_population_sha256.clone()),
            ),
            (
                "evaluationPopulationFileSha256",
                Value::String(self.evaluation_population_file_sha256.clone()),
            ),
            (
                "compactIdentityLedger",
                self.compact_identity_ledger.to_value()?,
            ),
        ]))
    }

    pub fn funnel_fragments_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 G0 funnel fragments")
            .clone();
        fields.insert(
            "g0FunnelFragmentsSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 G0 funnel fragments")?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "generationIndex",
                "generationConfigSha256",
                "sharedAuthoritySha256",
                "transactionSha256",
                "publicationPlanSha256",
                "publicationReceiptSha256",
                "attemptJournalSha256",
                "proposalAttemptCount",
                "acceptedCount",
                "selectedCount",
                "proposalAttemptStream",
                "acceptedPoolMembership",
                "selectedProjectionIndex",
                "campaignLedger",
                "g0Selection",
                "evaluationPopulationFunnelEntries",
                "evaluationPopulationSha256",
                "evaluationPopulationFileSha256",
                "compactIdentityLedger",
                "g0FunnelFragmentsSha256",
            ],
            "v5 G0 funnel fragments",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_G0_FUNNEL_FRAGMENTS_SCHEMA)
        {
            return Err(contract("v5 G0 funnel fragment schema is invalid"));
        }
        let count = |key: &str| -> Result<u64> {
            required(value, key, "v5 G0 funnel fragments")?
                .as_u64()
                .ok_or_else(|| contract(format!("v5 G0 funnel {key} is invalid")))
        };
        let receipt = Self {
            generation_index: count("generationIndex")?,
            generation_config_sha256: sha_field(
                value,
                "generationConfigSha256",
                "v5 G0 funnel fragments",
            )?,
            shared_authority_sha256: sha_field(
                value,
                "sharedAuthoritySha256",
                "v5 G0 funnel fragments",
            )?,
            transaction_sha256: sha_field(value, "transactionSha256", "v5 G0 funnel fragments")?,
            publication_plan_sha256: sha_field(
                value,
                "publicationPlanSha256",
                "v5 G0 funnel fragments",
            )?,
            publication_receipt_sha256: sha_field(
                value,
                "publicationReceiptSha256",
                "v5 G0 funnel fragments",
            )?,
            attempt_journal_sha256: sha_field(
                value,
                "attemptJournalSha256",
                "v5 G0 funnel fragments",
            )?,
            proposal_attempt_count: count("proposalAttemptCount")?,
            accepted_count: count("acceptedCount")?,
            selected_count: count("selectedCount")?,
            proposal_attempt_stream: V5G0FunnelStreamDescriptor::from_value(required(
                value,
                "proposalAttemptStream",
                "v5 G0 funnel fragments",
            )?)?,
            accepted_pool_membership: V5G0FunnelSetDescriptor::from_value(required(
                value,
                "acceptedPoolMembership",
                "v5 G0 funnel fragments",
            )?)?,
            selected_projection_index: V5G0FunnelSetDescriptor::from_value(required(
                value,
                "selectedProjectionIndex",
                "v5 G0 funnel fragments",
            )?)?,
            campaign_ledger: V5G0FunnelSetDescriptor::from_value(required(
                value,
                "campaignLedger",
                "v5 G0 funnel fragments",
            )?)?,
            g0_selection: V5G0FunnelSetDescriptor::from_value(required(
                value,
                "g0Selection",
                "v5 G0 funnel fragments",
            )?)?,
            evaluation_population_funnel_entries: V5G0FunnelStreamDescriptor::from_value(
                required(
                    value,
                    "evaluationPopulationFunnelEntries",
                    "v5 G0 funnel fragments",
                )?,
            )?,
            evaluation_population_sha256: sha_field(
                value,
                "evaluationPopulationSha256",
                "v5 G0 funnel fragments",
            )?,
            evaluation_population_file_sha256: sha_field(
                value,
                "evaluationPopulationFileSha256",
                "v5 G0 funnel fragments",
            )?,
            compact_identity_ledger: V5G0FunnelSetDescriptor::from_value(required(
                value,
                "compactIdentityLedger",
                "v5 G0 funnel fragments",
            )?)?,
        };
        let supplied = sha_field(value, "g0FunnelFragmentsSha256", "v5 G0 funnel fragments")?;
        if supplied != receipt.funnel_fragments_sha256()? || &receipt.to_value()? != value {
            return Err(contract("v5 G0 funnel fragment identity drifted"));
        }
        Ok(receipt)
    }

    pub fn object_binding(&self) -> Result<V5G0FunnelFragmentReceiptObjectBinding> {
        let value = self.to_value()?;
        let g0_funnel_fragments_sha256 = self.funnel_fragments_sha256()?;
        Ok(V5G0FunnelFragmentReceiptObjectBinding {
            relative_path: v5_native_object_relative_path(&g0_funnel_fragments_sha256)
                .map_err(V5G0TransactionError::from)?,
            g0_funnel_fragments_sha256,
            value,
        })
    }

    /// Recompute every compact descriptor and reject a receipt which is
    /// individually self-hashed but belongs to another transaction, public
    /// evaluation artifact, or G0 selection.
    pub fn verify_against(
        &self,
        request: &V5G0TransactionRequest,
        transaction: &V5G0TransactionResult,
        publication_receipt: &V5G0PublicationReceipt,
    ) -> Result<()> {
        let expected = derive_v5_g0_funnel_fragments(request, transaction, publication_receipt)?;
        if self != &expected {
            return Err(contract(
                "v5 G0 funnel receipt diverged from compact transaction/publication",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct V5G0FunnelFragmentReceiptObjectBinding {
    pub g0_funnel_fragments_sha256: String,
    pub relative_path: String,
    pub value: Value,
}

impl V5G0FunnelFragmentReceiptObjectBinding {
    pub fn validate(&self) -> Result<()> {
        let identity = exact_sha(
            &Value::String(self.g0_funnel_fragments_sha256.clone()),
            "v5 G0 funnel fragment object SHA-256",
        )?;
        let expected_path =
            v5_native_object_relative_path(&identity).map_err(V5G0TransactionError::from)?;
        let receipt = V5G0FunnelFragments::from_value(&self.value)?;
        if self.relative_path != expected_path
            || receipt.funnel_fragments_sha256()? != identity
            || receipt.to_value()? != self.value
        {
            return Err(contract("v5 G0 funnel fragment object binding drifted"));
        }
        Ok(())
    }

    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(object([
            (
                "g0FunnelFragmentsSha256",
                Value::String(self.g0_funnel_fragments_sha256.clone()),
            ),
            ("relativePath", Value::String(self.relative_path.clone())),
            ("value", self.value.clone()),
        ]))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 G0 funnel fragment object binding")?;
        exact_keys(
            fields,
            &["g0FunnelFragmentsSha256", "relativePath", "value"],
            "v5 G0 funnel fragment object binding",
        )?;
        let binding = Self {
            g0_funnel_fragments_sha256: sha_field(
                value,
                "g0FunnelFragmentsSha256",
                "v5 G0 funnel fragment object binding",
            )?,
            relative_path: exact_text(
                required(
                    value,
                    "relativePath",
                    "v5 G0 funnel fragment object binding",
                )?,
                "v5 G0 funnel fragment object path",
            )?,
            value: required(value, "value", "v5 G0 funnel fragment object binding")?.clone(),
        };
        binding.validate()?;
        if &binding.to_value()? != value {
            return Err(contract(
                "v5 G0 funnel fragment object binding is not canonical",
            ));
        }
        Ok(binding)
    }
}

struct ProjectionAccumulator {
    kind: V5G0FunnelProjectionKind,
    first: bool,
    hash: CanonicalSha256Writer,
    encoded_bytes: u64,
    row_count: u64,
}

impl ProjectionAccumulator {
    fn new(kind: V5G0FunnelProjectionKind) -> Self {
        Self {
            kind,
            first: true,
            hash: CanonicalSha256Writer::default(),
            encoded_bytes: 0,
            row_count: 0,
        }
    }

    fn append<S: V5G0FunnelProjectionSink>(&mut self, sink: &mut S, value: &Value) -> Result<()> {
        let mut row = Vec::new();
        write_canonical_json(value, &mut row)?;
        if !self.first {
            sink.write_projection(self.kind, b",")?;
            self.hash.write_all(b",")?;
            self.encoded_bytes = self
                .encoded_bytes
                .checked_add(1)
                .ok_or_else(|| contract("v5 G0 funnel byte count overflowed"))?;
        }
        self.first = false;
        sink.write_projection(self.kind, &row)?;
        self.hash.write_all(&row)?;
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(row.len() as u64)
            .ok_or_else(|| contract("v5 G0 funnel byte count overflowed"))?;
        self.row_count = self
            .row_count
            .checked_add(1)
            .ok_or_else(|| contract("v5 G0 funnel row count overflowed"))?;
        Ok(())
    }

    fn finish(self) -> Result<V5G0FunnelStreamDescriptor> {
        let descriptor = V5G0FunnelStreamDescriptor {
            fragment_sha256: self.hash.finish(),
            encoded_bytes: self.encoded_bytes,
            row_count: self.row_count,
        };
        descriptor.validate("v5 G0 funnel projection")?;
        Ok(descriptor)
    }
}

#[derive(Default)]
struct HashOnlySink;

impl V5G0FunnelProjectionSink for HashOnlySink {
    fn write_projection(
        &mut self,
        _kind: V5G0FunnelProjectionKind,
        _canonical_bytes: &[u8],
    ) -> std::io::Result<()> {
        Ok(())
    }
}

fn compact_funnel_fields(record: &V5CompactAcceptedRecord) -> Result<(Value, Value, Value)> {
    let source_profile = Value::String(record.compiled.raw_pair_sha256.clone());
    Ok((
        object([
            ("candidateId", Value::String(record.candidate_id.clone())),
            ("sourceProfileSha256", source_profile.clone()),
        ]),
        object([
            ("candidateId", Value::String(record.candidate_id.clone())),
            ("rawSourceProfileSha256", source_profile),
        ]),
        v5_funnel_candidate_projection(record, V5FunnelAdmission::G0BootstrapAccepted)
            .map_err(V5G0TransactionError::from)?,
    ))
}

fn proposal_attempt_entry(
    attempt: &V5ProposalAttemptRecord,
    audit: &V5AttemptOutcomeAudit,
    accepted_records: &BTreeMap<String, &V5CompactAcceptedRecord>,
) -> Result<Value> {
    audit
        .verify_binds_attempt(attempt)
        .map_err(V5G0TransactionError::from)?;
    let mut fields = Map::new();
    fields.insert(
        "schemaVersion".to_owned(),
        Value::String(V5_PROPOSAL_FUNNEL_ENTRY_SCHEMA.to_owned()),
    );
    fields.insert(
        "entrySha256".to_owned(),
        Value::String(
            attempt
                .attempt_sha256()
                .map_err(V5G0TransactionError::from)?,
        ),
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
    if let Some(record_sha256) = &attempt.accepted_record_sha256 {
        let record = accepted_records
            .get(record_sha256)
            .ok_or_else(|| contract("v5 G0 accepted attempt names an absent compact record"))?;
        if record.proposal_ordinal != attempt.proposal_ordinal
            || record.origin_kind != attempt.origin_kind
            || attempt.disposition != "accepted"
        {
            return Err(contract(
                "v5 G0 proposal funnel accepted record binding drifted",
            ));
        }
        let (candidate, proposal, funnel_candidate) = compact_funnel_fields(record)?;
        fields.insert(
            "acceptedCompactRecordSha256".to_owned(),
            Value::String(record_sha256.clone()),
        );
        fields.insert("candidate".to_owned(), candidate);
        fields.insert("proposal".to_owned(), proposal);
        fields.insert("funnelCandidate".to_owned(), funnel_candidate);
    } else if attempt.disposition == "accepted" {
        return Err(contract(
            "v5 G0 accepted proposal funnel attempt lacks compact record",
        ));
    }
    Ok(Value::Object(fields))
}

fn evaluation_funnel_entry(record: &V5CompactAcceptedRecord) -> Result<Value> {
    let record_sha256 = record.record_sha256().map_err(V5G0TransactionError::from)?;
    let (candidate, proposal, funnel_candidate) = compact_funnel_fields(record)?;
    Ok(object([
        (
            "schemaVersion",
            Value::String(V5_PROPOSAL_FUNNEL_ENTRY_SCHEMA.to_owned()),
        ),
        ("entrySha256", Value::String(record_sha256.clone())),
        ("proposalOrdinal", Value::from(record.proposal_ordinal)),
        ("originKind", Value::String(record.origin_kind.clone())),
        ("disposition", Value::String("accepted".to_owned())),
        ("acceptedCompactRecordSha256", Value::String(record_sha256)),
        ("candidate", candidate),
        ("proposal", proposal),
        ("funnelCandidate", funnel_candidate),
    ]))
}

fn for_each_proposal_attempt_entry(
    transaction: &V5G0TransactionResult,
    mut callback: impl FnMut(&Value) -> Result<()>,
) -> Result<()> {
    let records = transaction
        .accepted_records
        .iter()
        .map(|record| {
            Ok((
                record.record_sha256().map_err(V5G0TransactionError::from)?,
                record,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let audits = transaction
        .outcome_audits
        .iter()
        .map(|audit| {
            Ok((
                audit.audit_sha256().map_err(V5G0TransactionError::from)?,
                audit,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    for (ordinal, attempt) in transaction.attempts.iter().enumerate() {
        if attempt.proposal_ordinal != ordinal as u64 {
            return Err(contract("v5 G0 funnel attempt order drifted"));
        }
        let audit = audits
            .get(&attempt.outcome_audit_sha256)
            .ok_or_else(|| contract("v5 G0 funnel attempt lacks outcome audit"))?;
        callback(&proposal_attempt_entry(attempt, audit, &records)?)?;
    }
    Ok(())
}

fn stream_compact_projections<S: V5G0FunnelProjectionSink>(
    transaction: &V5G0TransactionResult,
    sink: &mut S,
) -> Result<(V5G0FunnelStreamDescriptor, V5G0FunnelStreamDescriptor)> {
    let records = transaction
        .accepted_records
        .iter()
        .map(|record| {
            Ok((
                record.record_sha256().map_err(V5G0TransactionError::from)?,
                record,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut attempts = ProjectionAccumulator::new(V5G0FunnelProjectionKind::ProposalAttemptEntries);
    for_each_proposal_attempt_entry(transaction, |row| attempts.append(sink, row))?;

    let selected_index = transaction
        .selected_projection_index
        .as_ref()
        .ok_or_else(|| contract("v5 G0 funnel transaction lacks selected projection index"))?;
    let mut selected = selected_index
        .projections
        .iter()
        .map(|projection| {
            let record = records.get(&projection.record_sha256).ok_or_else(|| {
                contract("v5 G0 funnel selected projection names an absent record")
            })?;
            if record.candidate_id != projection.candidate_id
                || record.candidate_identity_sha256 != projection.candidate_identity_sha256
                || record.pair_identity_sha256 != projection.pair_identity_sha256
            {
                return Err(contract(
                    "v5 G0 funnel selected projection candidate binding drifted",
                ));
            }
            Ok(*record)
        })
        .collect::<Result<Vec<_>>>()?;
    selected.sort_by(|left, right| left.candidate_id.cmp(&right.candidate_id));
    let mut evaluation =
        ProjectionAccumulator::new(V5G0FunnelProjectionKind::EvaluationPopulationFunnelEntries);
    for record in selected {
        evaluation.append(sink, &evaluation_funnel_entry(record)?)?;
    }
    Ok((attempts.finish()?, evaluation.finish()?))
}

/// Build the immutable receipt by replaying only compact transaction facts.
/// The selected evaluation-funnel descriptor is independently recomputed and
/// must exactly equal the one emitted by the single rich publication pass.
fn derive_v5_g0_funnel_fragments(
    request: &V5G0TransactionRequest,
    transaction: &V5G0TransactionResult,
    publication_receipt: &V5G0PublicationReceipt,
) -> Result<V5G0FunnelFragments> {
    verify_v5_g0_transaction_replay(request, transaction)?;
    for binding in transaction.durable_object_bindings()? {
        binding.validate()?;
    }
    if !transaction.target_reached {
        return Err(contract(
            "cannot build G0 funnel receipt for incomplete transaction",
        ));
    }
    let selected_index = transaction
        .selected_projection_index
        .as_ref()
        .ok_or_else(|| contract("completed v5 G0 transaction lacks selected projection index"))?;
    let selected_count = selected_index.projections.len() as u64;
    let plan_sha256 = transaction
        .publication_plan
        .publication_plan_sha256()
        .map_err(|error| contract(format!("v5 G0 publication plan identity failed: {error}")))?;
    let request_sha256 = transaction
        .publication_plan
        .publication_request_sha256(&transaction.shared_authority_sha256)
        .map_err(|error| {
            contract(format!(
                "v5 G0 publication request identity failed: {error}"
            ))
        })?;
    if publication_receipt.publication_plan_sha256 != plan_sha256
        || publication_receipt.publication_request_sha256 != request_sha256
    {
        return Err(contract(
            "v5 G0 funnel publication receipt authority binding drifted",
        ));
    }
    let (proposal_attempt_stream, evaluation_population_funnel_entries) =
        stream_compact_projections(transaction, &mut HashOnlySink)?;
    let pool = transaction
        .accepted_pool
        .as_ref()
        .ok_or_else(|| contract("completed v5 G0 transaction lacks accepted pool"))?;
    let campaign = transaction
        .campaign_ledger
        .as_ref()
        .ok_or_else(|| contract("completed v5 G0 transaction lacks campaign ledger"))?;
    let selection = transaction
        .g0_selection
        .as_ref()
        .ok_or_else(|| contract("completed v5 G0 transaction lacks selection"))?;
    let accepted_count = transaction.accepted_records.len() as u64;
    let campaign_count = campaign
        .get("rows")
        .and_then(Value::as_array)
        .map(|rows| rows.len() as u64)
        .ok_or_else(|| contract("v5 G0 campaign ledger rows are invalid"))?;
    let selection_count = selection
        .get("selected")
        .and_then(Value::as_array)
        .map(|rows| rows.len() as u64)
        .ok_or_else(|| contract("v5 G0 selection rows are invalid"))?;
    if campaign_count != accepted_count
        || selection_count != selected_count
        || transaction.identity_ledger.attempt_count != transaction.attempts.len() as u64
        || transaction.identity_ledger.accepted_count != accepted_count
    {
        return Err(contract("v5 G0 funnel selected-local accounting drifted"));
    }

    let receipt = V5G0FunnelFragments {
        generation_index: transaction.generation_index,
        generation_config_sha256: transaction.generation_config_sha256.clone(),
        shared_authority_sha256: transaction.shared_authority_sha256.clone(),
        transaction_sha256: transaction.transaction_sha256()?,
        publication_plan_sha256: plan_sha256,
        publication_receipt_sha256: publication_receipt.publication_receipt_sha256().map_err(
            |error| {
                contract(format!(
                    "v5 G0 publication receipt identity failed: {error}"
                ))
            },
        )?,
        attempt_journal_sha256: transaction
            .attempt_journal
            .attempt_journal_sha256()
            .map_err(V5G0TransactionError::from)?,
        proposal_attempt_count: transaction.attempts.len() as u64,
        accepted_count,
        selected_count,
        proposal_attempt_stream,
        accepted_pool_membership: V5G0FunnelSetDescriptor {
            semantic_sha256: sha_field(pool, "acceptedPoolSha256", "v5 G0 accepted pool")?,
            row_count: accepted_count,
        },
        selected_projection_index: V5G0FunnelSetDescriptor {
            semantic_sha256: selected_index.selected_projection_index_sha256()?,
            row_count: selected_count,
        },
        campaign_ledger: V5G0FunnelSetDescriptor {
            semantic_sha256: sha_field(campaign, "ledgerSha256", "v5 G0 campaign ledger")?,
            row_count: campaign_count,
        },
        g0_selection: V5G0FunnelSetDescriptor {
            semantic_sha256: sha_field(selection, "selectionSha256", "v5 G0 selection")?,
            row_count: selection_count,
        },
        evaluation_population_funnel_entries,
        evaluation_population_sha256: publication_receipt
            .evaluation_population
            .semantic_sha256
            .clone(),
        evaluation_population_file_sha256: publication_receipt
            .evaluation_population
            .file_sha256
            .clone(),
        compact_identity_ledger: V5G0FunnelSetDescriptor {
            semantic_sha256: transaction.identity_ledger.identity_ledger_sha256()?,
            row_count: accepted_count,
        },
    };
    let _ = receipt.to_value()?;
    Ok(receipt)
}

/// Build the immutable receipt at fresh publication time.  In addition to the
/// compact derivation used by adoption, this checks that the one-pass rich
/// publication traversal emitted exactly the same selected funnel bytes.
pub fn build_v5_g0_funnel_fragments(
    request: &V5G0TransactionRequest,
    transaction: &V5G0TransactionResult,
    publication_fragments: &V5G0PublicationFragments,
    publication_receipt: &V5G0PublicationReceipt,
) -> Result<V5G0FunnelFragments> {
    let receipt = derive_v5_g0_funnel_fragments(request, transaction, publication_receipt)?;
    publication_fragments
        .validate_for_selected(receipt.selected_count)
        .map_err(|error| contract(format!("v5 G0 publication fragments are invalid: {error}")))?;
    let staged_funnel = &publication_fragments.evaluation_funnel_entries;
    if receipt.evaluation_population_funnel_entries.fragment_sha256 != staged_funnel.fragment_sha256
        || receipt.evaluation_population_funnel_entries.encoded_bytes != staged_funnel.encoded_bytes
        || receipt.evaluation_population_funnel_entries.row_count != staged_funnel.row_count
    {
        return Err(contract(
            "v5 G0 compact evaluation funnel diverged from publication fragment",
        ));
    }
    Ok(receipt)
}

/// Emit the exact compact streams named by a previously built receipt and
/// verify every resulting byte/count against that receipt.  This is safe to
/// call after private publication fragments have been deleted.
pub fn stream_v5_g0_funnel_projections<S: V5G0FunnelProjectionSink>(
    request: &V5G0TransactionRequest,
    transaction: &V5G0TransactionResult,
    publication_receipt: &V5G0PublicationReceipt,
    receipt: &V5G0FunnelFragments,
    sink: &mut S,
) -> Result<()> {
    receipt.verify_against(request, transaction, publication_receipt)?;
    let (attempts, evaluation) = stream_compact_projections(transaction, sink)?;
    if attempts != receipt.proposal_attempt_stream
        || evaluation != receipt.evaluation_population_funnel_entries
    {
        return Err(contract(
            "v5 G0 funnel projection bytes drifted from receipt",
        ));
    }
    Ok(())
}

/// Strict persisted-object verifier.  It rejects a family substitution (for
/// example an evolved fragment receipt), an alias path, or any self-rehashed
/// descriptor whose compact roots do not replay against this transaction.
pub fn verify_v5_g0_funnel_fragment_receipt(
    request: &V5G0TransactionRequest,
    transaction: &V5G0TransactionResult,
    publication_receipt: &V5G0PublicationReceipt,
    object_binding_value: &Value,
) -> Result<V5G0FunnelFragments> {
    let binding = V5G0FunnelFragmentReceiptObjectBinding::from_value(object_binding_value)?;
    let receipt = V5G0FunnelFragments::from_value(&binding.value)?;
    receipt.verify_against(request, transaction, publication_receipt)?;
    Ok(receipt)
}

fn validate_persisted_projection_row(value: &Value, expected_ordinal: u64) -> Result<()> {
    let fields = object_ref(value, "v5 G0 persisted funnel projection row")?;
    let accepted = fields.get("disposition").and_then(Value::as_str) == Some("accepted");
    let expected = if accepted {
        &[
            "schemaVersion",
            "entrySha256",
            "proposalOrdinal",
            "originKind",
            "disposition",
            "acceptedCompactRecordSha256",
            "candidate",
            "proposal",
            "funnelCandidate",
        ][..]
    } else {
        &[
            "schemaVersion",
            "entrySha256",
            "proposalOrdinal",
            "originKind",
            "disposition",
        ][..]
    };
    exact_keys(fields, expected, "v5 G0 persisted funnel projection row")?;
    if fields.get("schemaVersion").and_then(Value::as_str) != Some(V5_PROPOSAL_FUNNEL_ENTRY_SCHEMA)
        || fields.get("proposalOrdinal").and_then(Value::as_u64) != Some(expected_ordinal)
        || fields.get("originKind").and_then(Value::as_str) != Some("random_immigrant")
    {
        return Err(contract(
            "v5 G0 persisted funnel projection row authority/order drifted",
        ));
    }
    let _ = sha_field(
        value,
        "entrySha256",
        "v5 G0 persisted funnel projection row",
    )?;
    let disposition = exact_text(
        required(
            value,
            "disposition",
            "v5 G0 persisted funnel projection row",
        )?,
        "v5 G0 persisted funnel disposition",
    )?;
    if !matches!(disposition.as_str(), "accepted" | "rejected" | "no_op") {
        return Err(contract(
            "v5 G0 persisted funnel projection disposition is invalid",
        ));
    }
    if !accepted {
        return Ok(());
    }
    let _ = sha_field(
        value,
        "acceptedCompactRecordSha256",
        "v5 G0 persisted funnel projection row",
    )?;
    let candidate = required(value, "candidate", "v5 G0 persisted funnel projection row")?;
    exact_keys(
        object_ref(candidate, "v5 G0 persisted funnel candidate")?,
        &["candidateId", "sourceProfileSha256"],
        "v5 G0 persisted funnel candidate",
    )?;
    let candidate_id = exact_text(
        required(candidate, "candidateId", "v5 G0 persisted funnel candidate")?,
        "v5 G0 persisted funnel candidate ID",
    )?;
    let source_profile = sha_field(
        candidate,
        "sourceProfileSha256",
        "v5 G0 persisted funnel candidate",
    )?;
    let proposal = required(value, "proposal", "v5 G0 persisted funnel projection row")?;
    exact_keys(
        object_ref(proposal, "v5 G0 persisted funnel proposal")?,
        &["candidateId", "rawSourceProfileSha256"],
        "v5 G0 persisted funnel proposal",
    )?;
    if required(proposal, "candidateId", "v5 G0 persisted funnel proposal")?.as_str()
        != Some(&candidate_id)
        || sha_field(
            proposal,
            "rawSourceProfileSha256",
            "v5 G0 persisted funnel proposal",
        )? != source_profile
    {
        return Err(contract(
            "v5 G0 persisted funnel candidate/proposal binding drifted",
        ));
    }
    let funnel = required(
        value,
        "funnelCandidate",
        "v5 G0 persisted funnel projection row",
    )?;
    exact_keys(
        object_ref(funnel, "v5 G0 persisted funnel candidate stage")?,
        &[
            "schemaVersion",
            "candidateId",
            "rawSourceProfileSha256",
            "staticReachability",
            "nativeValidation",
            "admission",
        ],
        "v5 G0 persisted funnel candidate stage",
    )?;
    if funnel.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_proposal_funnel_stage_v1")
        || funnel.get("candidateId").and_then(Value::as_str) != Some(&candidate_id)
        || sha_field(
            funnel,
            "rawSourceProfileSha256",
            "v5 G0 persisted funnel candidate stage",
        )? != source_profile
    {
        return Err(contract(
            "v5 G0 persisted funnel candidate stage binding drifted",
        ));
    }
    let admission = required(
        funnel,
        "admission",
        "v5 G0 persisted funnel candidate stage",
    )?;
    exact_keys(
        object_ref(admission, "v5 G0 persisted funnel admission")?,
        &["outcome", "reasons", "canonicalEvidenceIdentitySha256"],
        "v5 G0 persisted funnel admission",
    )?;
    if admission.get("outcome").and_then(Value::as_str) != Some("admitted")
        || admission
            .get("reasons")
            .and_then(Value::as_array)
            .is_none_or(|reasons| !reasons.is_empty())
        || admission.get("canonicalEvidenceIdentitySha256") != Some(&Value::Null)
    {
        return Err(contract(
            "v5 G0 persisted funnel bootstrap admission drifted",
        ));
    }
    Ok(())
}

/// Fresh write-neutral persistence boundary.  Core derives every row from the
/// replayed transaction and emits canonical JSONL; batch supplies only the
/// file-backed sink.  No selected rich candidate is reconstructed.
pub fn write_v5_g0_funnel_projection_stream<S: V5G0FunnelProjectionStreamSink>(
    request: &V5G0TransactionRequest,
    transaction: &V5G0TransactionResult,
    publication_receipt: &V5G0PublicationReceipt,
    funnel_receipt: &V5G0FunnelFragments,
    sink: &mut S,
) -> Result<V5G0FunnelProjectionStreamReceipt> {
    funnel_receipt.verify_against(request, transaction, publication_receipt)?;
    let mut raw_hash = CanonicalSha256Writer::default();
    let mut size_bytes = 0_u64;
    let mut row_count = 0_u64;
    for_each_proposal_attempt_entry(transaction, |row| {
        validate_persisted_projection_row(row, row_count)?;
        let mut line = Vec::new();
        write_canonical_json(row, &mut line)?;
        line.push(b'\n');
        sink.write_projection_row(&line)?;
        raw_hash.write_all(&line)?;
        size_bytes = size_bytes
            .checked_add(line.len() as u64)
            .ok_or_else(|| contract("v5 G0 funnel projection byte count overflowed"))?;
        row_count = row_count
            .checked_add(1)
            .ok_or_else(|| contract("v5 G0 funnel projection row count overflowed"))?;
        Ok(())
    })?;
    if row_count != funnel_receipt.proposal_attempt_count {
        return Err(contract("v5 G0 funnel persisted projection count drifted"));
    }
    let receipt = V5G0FunnelProjectionStreamReceipt {
        relative_path: V5_G0_FUNNEL_PROJECTION_STREAM_PATH.to_owned(),
        row_schema: V5_PROPOSAL_FUNNEL_ENTRY_SCHEMA.to_owned(),
        input_g0_funnel_fragments_sha256: funnel_receipt.funnel_fragments_sha256()?,
        input_proposal_attempt_fragment_sha256: funnel_receipt
            .proposal_attempt_stream
            .fragment_sha256
            .clone(),
        raw_sha256: raw_hash.finish(),
        size_bytes,
        row_count,
    };
    let _ = receipt.to_value()?;
    Ok(receipt)
}

/// Restart/adoption verifier for the fixed public JSONL stream.  It needs
/// only the durable G0 funnel receipt, the self-hashed stream descriptor, and
/// the public bytes.  Besides raw SHA/length, it validates canonical row
/// framing/order and recomputes the original comma-fragment semantic root.
pub fn verify_v5_g0_funnel_projection_stream<R: Read>(
    funnel_receipt: &V5G0FunnelFragments,
    stream_receipt_value: &Value,
    input: &mut R,
) -> Result<V5G0FunnelProjectionStreamReceipt> {
    let receipt = V5G0FunnelProjectionStreamReceipt::from_value(stream_receipt_value)?;
    if receipt.input_g0_funnel_fragments_sha256 != funnel_receipt.funnel_fragments_sha256()?
        || receipt.input_proposal_attempt_fragment_sha256
            != funnel_receipt.proposal_attempt_stream.fragment_sha256
        || receipt.row_count != funnel_receipt.proposal_attempt_count
    {
        return Err(contract(
            "v5 G0 funnel persisted stream input receipt binding drifted",
        ));
    }
    let mut reader = BufReader::new(input);
    let mut raw_hash = CanonicalSha256Writer::default();
    let mut fragment_hash = CanonicalSha256Writer::default();
    let mut size_bytes = 0_u64;
    let mut fragment_bytes = 0_u64;
    let mut row_count = 0_u64;
    loop {
        let mut line = Vec::new();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        if line.last() != Some(&b'\n') || line[..line.len() - 1].contains(&b'\r') {
            return Err(contract(
                "v5 G0 funnel persisted projection is not LF-delimited JSONL",
            ));
        }
        let row_bytes = &line[..line.len() - 1];
        if row_bytes.is_empty() {
            return Err(contract(
                "v5 G0 funnel persisted projection contains an empty row",
            ));
        }
        let row: Value = serde_json::from_slice(row_bytes)
            .map_err(|error| contract(format!("parse v5 G0 funnel projection row: {error}")))?;
        let mut canonical = Vec::new();
        write_canonical_json(&row, &mut canonical)?;
        if canonical != row_bytes {
            return Err(contract(
                "v5 G0 funnel persisted projection row is not canonical",
            ));
        }
        validate_persisted_projection_row(&row, row_count)?;
        raw_hash.write_all(&line)?;
        size_bytes = size_bytes
            .checked_add(line.len() as u64)
            .ok_or_else(|| contract("v5 G0 funnel persisted byte count overflowed"))?;
        if row_count != 0 {
            fragment_hash.write_all(b",")?;
            fragment_bytes = fragment_bytes
                .checked_add(1)
                .ok_or_else(|| contract("v5 G0 funnel fragment byte count overflowed"))?;
        }
        fragment_hash.write_all(row_bytes)?;
        fragment_bytes = fragment_bytes
            .checked_add(row_bytes.len() as u64)
            .ok_or_else(|| contract("v5 G0 funnel fragment byte count overflowed"))?;
        row_count = row_count
            .checked_add(1)
            .ok_or_else(|| contract("v5 G0 funnel persisted row count overflowed"))?;
    }
    if raw_hash.finish() != receipt.raw_sha256
        || size_bytes != receipt.size_bytes
        || row_count != receipt.row_count
        || fragment_hash.finish() != funnel_receipt.proposal_attempt_stream.fragment_sha256
        || fragment_bytes != funnel_receipt.proposal_attempt_stream.encoded_bytes
        || row_count != funnel_receipt.proposal_attempt_stream.row_count
    {
        return Err(contract(
            "v5 G0 funnel persisted projection bytes/count/root drifted",
        ));
    }
    Ok(receipt)
}
