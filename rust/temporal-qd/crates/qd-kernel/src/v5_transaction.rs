//! Write-neutral native v5 G0 proposal transaction.
//!
//! The implementation is intentionally isolated from `v5.rs`: the latter
//! reconstructs one compact candidate, while this module owns scheduling,
//! ordered admission, compact receipts, and G0 selection.

use std::{
    collections::{BTreeMap, BTreeSet},
    thread,
};

use temporal_qd_contract::{
    ContractError, Map, Value, canonical_sha256, canonical_sha256_without_object_field,
};

use crate::{
    factory::{ParentReference, ProposalIntent},
    g0::{
        self, AdmittedAcceptedPairEntry, build_accepted_pool, materialize_campaign_ledger,
        project_admitted_pair_entry, select_g0_bootstrap, verify_campaign_ledger,
        verify_g0_bootstrap_selection,
    },
    identity::{Side, immigrant_side_seed},
    proposal::{
        AcceptedProposal, CandidateIdentityLedger, IdentityLedger, LedgerProposal, ParentSelector,
        ProposalError, ProposalPlanner, ProposalSchedule, ProposalState,
    },
    v5::{
        self, V5_PROPOSAL_FUNNEL_ENTRY_SCHEMA, V5AttemptJournal, V5AttemptLineageRefs,
        V5AttemptOutcomeAudit, V5CompactAcceptedRecord, V5Error, V5FunnelAdmission,
        V5ProposalAttemptRecord, V5SelectedProjection, V5SharedConstructionAuthority,
        build_v5_g0_accepted_material, materialize_selected_v5_g0_rich_candidate,
        verify_reconstruct_compact_g0_record,
    },
    v5_publication::{V5G0PublicationInputs, V5G0PublicationPlan},
};

pub const V5_G0_TRANSACTION_SCHEMA: &str = "temporal_qd_v5_g0_transaction_v1";
pub const V5_G0_COMPACT_IDENTITY_LEDGER_SCHEMA: &str =
    "temporal_qd_v5_g0_compact_identity_ledger_v1";
pub const V5_G0_SCHEDULE_STATE_RECEIPT_SCHEMA: &str = "temporal_qd_v5_g0_schedule_state_receipt_v1";
pub const V5_SELECTED_PROJECTION_INDEX_SCHEMA: &str = "temporal_qd_v5_selected_projection_index_v1";
pub const V5_SELECTED_PUBLICATION_ROW_SCHEMA: &str = "temporal_qd_v5_selected_publication_row_v1";

/// The ordered compact JSONL stream remains an audit/index artifact, but G0
/// accepted references always target the immutable object path below.
pub const V5_G0_COMPACT_JOURNAL_PATH: &str = "v5-native/accepted-records.jsonl";

/// Canonical immutable object path for one accepted compact record.  The G0
/// `journalReference` points here (rather than into a JSONL fragment) so a
/// selected reference remains independently resolvable after the compact
/// journal has been streamed or archived.
pub fn compact_record_object_relative_path(record_sha256: &str) -> Result<String> {
    v5::v5_native_object_relative_path(record_sha256).map_err(V5G0TransactionError::from)
}

/// Canonical immutable object path for a compact proposal delta.  Deltas are
/// independently durable for *every* semantic attempt (including rejected
/// duplicates), so an audit's non-null `proposalDeltaSha256` can never dangle.
pub fn compact_delta_object_relative_path(delta_sha256: &str) -> Result<String> {
    v5::v5_native_object_relative_path(delta_sha256).map_err(V5G0TransactionError::from)
}

#[derive(Debug, thiserror::Error)]
pub enum V5G0TransactionError {
    #[error("v5 construction failure: {0}")]
    V5(#[from] V5Error),
    #[error("proposal schedule failure: {0}")]
    Proposal(#[from] ProposalError),
    #[error("G0 compact selection failure: {0}")]
    G0(#[from] g0::G0Error),
    #[error("canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("v5 G0 transaction contract failure: {0}")]
    Contract(String),
    #[error("v5 G0 construction worker panicked")]
    WorkerPanic,
}

pub type Result<T> = std::result::Result<T, V5G0TransactionError>;

fn contract(message: impl Into<String>) -> V5G0TransactionError {
    V5G0TransactionError::Contract(message.into())
}

fn object(rows: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut map = Map::new();
    for (key, value) in rows {
        map.insert(key.to_owned(), value);
    }
    Value::Object(map)
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

fn parse_count_map(value: &Value, label: &str) -> Result<BTreeMap<String, u64>> {
    let fields = object_ref(value, label)?;
    let mut out = BTreeMap::new();
    for (key, value) in fields {
        if key.trim().is_empty()
            || !key
                .bytes()
                .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b':'))
        {
            return Err(contract(format!("{label} has an invalid machine-code key")));
        }
        let count = value
            .as_u64()
            .ok_or_else(|| contract(format!("{label} contains a non-integer count")))?;
        out.insert(key.clone(), count);
    }
    Ok(out)
}

fn exact_sha_vec(value: &Value, label: &str) -> Result<Vec<String>> {
    let values = value
        .as_array()
        .ok_or_else(|| contract(format!("{label} must be an ordered array")))?;
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(exact_sha(value, label)?);
    }
    if out.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(contract(format!(
            "{label} must be strictly sorted and unique"
        )));
    }
    Ok(out)
}

fn sha_vec_value(values: &BTreeSet<String>) -> Value {
    Value::Array(values.iter().cloned().map(Value::String).collect())
}

/// Immutable input for a single G0 proposal construction transaction.  The
/// shared authority envelope is parsed exactly once by `execute`; per-worker
/// construction receives only the validated typed authority.
#[derive(Clone, Debug, PartialEq)]
pub struct V5G0TransactionRequest {
    pub shared_authority: Value,
    /// Exact self-hashed pair-generation configuration.  The compact
    /// constructor consumes only its identity, while the native publication
    /// plan derives the public allocation/pair-config authority from this
    /// authenticated value.
    pub generation_config: Value,
    pub generation_config_sha256: String,
    pub generation_index: u64,
    pub target_accepted: u64,
    pub max_attempts: u64,
    pub evaluation_width: u64,
    /// Bounded expensive-construction parallelism.  Scheduler, ledger, and
    /// proposal-state transitions remain serial and ordinal ordered.
    pub thread_cap: u64,
    /// Narrow raw manifest facts needed to derive a publication plan.  This
    /// is not a caller-authored plan: core constructs and hashes the durable
    /// plan after parsing the sealed authority exactly once.
    pub publication_inputs: V5G0PublicationInputs,
}

impl V5G0TransactionRequest {
    /// Validate only the scalar/request-envelope bounds.  Execution and
    /// durable replay call this before parsing `shared_authority`, so the
    /// sealed authority is decoded exactly once per operation.
    fn validate_bounds(&self) -> Result<()> {
        let _ = exact_sha(
            &Value::String(self.generation_config_sha256.clone()),
            "v5 G0 generation config SHA-256",
        )?;
        if self.generation_index != 1 {
            return Err(contract(
                "native v5 G0 transaction requires generation index one",
            ));
        }
        if self.target_accepted == 0 {
            return Err(contract(
                "native v5 G0 transaction targetAccepted must be positive",
            ));
        }
        if self.max_attempts < self.target_accepted {
            return Err(contract(
                "native v5 G0 transaction maxAttempts must cover targetAccepted",
            ));
        }
        if self.evaluation_width == 0 || self.evaluation_width > self.target_accepted {
            return Err(contract(
                "native v5 G0 transaction evaluationWidth must be within targetAccepted",
            ));
        }
        if !(1..=8).contains(&self.thread_cap) {
            return Err(contract(
                "native v5 G0 transaction threadCap must be in 1..=8",
            ));
        }
        Ok(())
    }

    fn validate_parsed_authority(authority: &V5SharedConstructionAuthority) -> Result<()> {
        if authority.qd_engine_version.trim().is_empty() {
            return Err(contract(
                "native v5 G0 authority QD engine version is empty",
            ));
        }
        Ok(())
    }

    /// Standalone input validation for callers which have not yet parsed the
    /// sealed authority.  Construction/replay use the two narrower helpers
    /// above to preserve their parse-once guarantee.
    pub fn validate(&self) -> Result<()> {
        self.validate_bounds()?;
        let authority = V5SharedConstructionAuthority::from_shared_object(&self.shared_authority)?;
        Self::validate_parsed_authority(&authority)?;
        V5G0PublicationPlan::derive(self, &authority).map_err(|error| {
            contract(format!("v5 G0 publication plan validation failed: {error}"))
        })?;
        Ok(())
    }
}

/// Compact, transaction-local identity ledger.  It records the exact native
/// candidate/semantic/pair identities admitted by this G0 transaction without
/// retaining a rich candidate graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5G0CompactIdentityLedger {
    pub generation_index: u64,
    pub generation_config_sha256: String,
    pub shared_authority_sha256: String,
    pub candidate_identity_sha256s: BTreeSet<String>,
    pub executable_semantic_sha256s: BTreeSet<String>,
    pub pair_identity_sha256s: BTreeSet<String>,
    pub attempt_count: u64,
    pub accepted_count: u64,
    pub disposition_counts: BTreeMap<String, u64>,
}

impl V5G0CompactIdentityLedger {
    fn semantic_value(&self) -> Result<Value> {
        if self.generation_index != 1
            || self.accepted_count != self.candidate_identity_sha256s.len() as u64
        {
            return Err(contract(
                "v5 G0 compact identity ledger count binding is invalid",
            ));
        }
        if self.candidate_identity_sha256s.len() != self.executable_semantic_sha256s.len()
            || self.candidate_identity_sha256s.len() != self.pair_identity_sha256s.len()
        {
            return Err(contract(
                "v5 G0 compact identity ledger unique sets drifted",
            ));
        }
        for identity in self
            .candidate_identity_sha256s
            .iter()
            .chain(self.executable_semantic_sha256s.iter())
            .chain(self.pair_identity_sha256s.iter())
        {
            exact_sha(
                &Value::String(identity.clone()),
                "v5 G0 compact ledger identity",
            )?;
        }
        let accepted = self
            .disposition_counts
            .get("accepted")
            .copied()
            .unwrap_or_default();
        let total = self
            .disposition_counts
            .values()
            .try_fold(0_u64, |total, value| total.checked_add(*value))
            .ok_or_else(|| {
                contract("v5 G0 compact identity ledger disposition count overflowed")
            })?;
        if accepted != self.accepted_count || total != self.attempt_count {
            return Err(contract(
                "v5 G0 compact identity ledger disposition totals drifted",
            ));
        }
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_G0_COMPACT_IDENTITY_LEDGER_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            (
                "generationConfigSha256",
                Value::String(exact_sha(
                    &Value::String(self.generation_config_sha256.clone()),
                    "v5 G0 compact ledger generation config SHA-256",
                )?),
            ),
            (
                "sharedAuthoritySha256",
                Value::String(exact_sha(
                    &Value::String(self.shared_authority_sha256.clone()),
                    "v5 G0 compact ledger shared authority SHA-256",
                )?),
            ),
            (
                "candidateIdentitySha256s",
                sha_vec_value(&self.candidate_identity_sha256s),
            ),
            (
                "executableSemanticSha256s",
                sha_vec_value(&self.executable_semantic_sha256s),
            ),
            (
                "pairIdentitySha256s",
                sha_vec_value(&self.pair_identity_sha256s),
            ),
            ("attemptCount", Value::from(self.attempt_count)),
            ("acceptedCount", Value::from(self.accepted_count)),
            (
                "dispositionCounts",
                count_map_value(&self.disposition_counts),
            ),
        ]))
    }

    pub fn identity_ledger_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    /// Restore the exact general-purpose candidate ledger for the first
    /// evolved generation.  G0 intentionally stores a compact public ledger,
    /// but the authority identity and transition counters are core-owned; a
    /// batch adapter must not invent a parallel
    /// `temporal_qd_v5_g0_identity_ledger_authority_v1` schema.
    pub fn restore_candidate_identity_ledger(&self) -> Result<CandidateIdentityLedger> {
        let _ = self.semantic_value()?;
        let identity = g0_compact_ledger_identity(self)?;
        let mut ledger = CandidateIdentityLedger::new(
            identity.clone(),
            self.candidate_identity_sha256s.iter().cloned(),
        )?;
        let state = object([
            ("identity", identity),
            (
                "candidateIdentities",
                sha_vec_value(&self.candidate_identity_sha256s),
            ),
            ("proposalCount", Value::from(self.attempt_count)),
            (
                "dispositionCounts",
                count_map_value(&self.disposition_counts),
            ),
        ]);
        ledger.restore_compact_state(&state)?;
        if ledger.compact_state() != state {
            return Err(contract(
                "v5 G0 compact ledger restoration is not canonical",
            ));
        }
        Ok(ledger)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 G0 compact identity ledger")
            .clone();
        fields.insert(
            "identityLedgerSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 G0 compact identity ledger")?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "generationIndex",
                "generationConfigSha256",
                "sharedAuthoritySha256",
                "candidateIdentitySha256s",
                "executableSemanticSha256s",
                "pairIdentitySha256s",
                "attemptCount",
                "acceptedCount",
                "dispositionCounts",
                "identityLedgerSha256",
            ],
            "v5 G0 compact identity ledger",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_G0_COMPACT_IDENTITY_LEDGER_SCHEMA)
        {
            return Err(contract("v5 G0 compact identity ledger schema is invalid"));
        }
        let ledger = Self {
            generation_index: required(value, "generationIndex", "v5 G0 compact identity ledger")?
                .as_u64()
                .ok_or_else(|| {
                    contract("v5 G0 compact identity ledger generation index is invalid")
                })?,
            generation_config_sha256: exact_sha(
                required(
                    value,
                    "generationConfigSha256",
                    "v5 G0 compact identity ledger",
                )?,
                "v5 G0 compact identity ledger generation config SHA-256",
            )?,
            shared_authority_sha256: exact_sha(
                required(
                    value,
                    "sharedAuthoritySha256",
                    "v5 G0 compact identity ledger",
                )?,
                "v5 G0 compact identity ledger shared authority SHA-256",
            )?,
            candidate_identity_sha256s: exact_sha_vec(
                required(
                    value,
                    "candidateIdentitySha256s",
                    "v5 G0 compact identity ledger",
                )?,
                "v5 G0 compact identity ledger candidate identities",
            )?
            .into_iter()
            .collect(),
            executable_semantic_sha256s: exact_sha_vec(
                required(
                    value,
                    "executableSemanticSha256s",
                    "v5 G0 compact identity ledger",
                )?,
                "v5 G0 compact identity ledger executable semantics",
            )?
            .into_iter()
            .collect(),
            pair_identity_sha256s: exact_sha_vec(
                required(
                    value,
                    "pairIdentitySha256s",
                    "v5 G0 compact identity ledger",
                )?,
                "v5 G0 compact identity ledger pair identities",
            )?
            .into_iter()
            .collect(),
            attempt_count: required(value, "attemptCount", "v5 G0 compact identity ledger")?
                .as_u64()
                .ok_or_else(|| {
                    contract("v5 G0 compact identity ledger attempt count is invalid")
                })?,
            accepted_count: required(value, "acceptedCount", "v5 G0 compact identity ledger")?
                .as_u64()
                .ok_or_else(|| {
                    contract("v5 G0 compact identity ledger accepted count is invalid")
                })?,
            disposition_counts: parse_count_map(
                required(value, "dispositionCounts", "v5 G0 compact identity ledger")?,
                "v5 G0 compact identity ledger disposition counts",
            )?,
        };
        let supplied = exact_sha(
            required(
                value,
                "identityLedgerSha256",
                "v5 G0 compact identity ledger",
            )?,
            "v5 G0 compact identity ledger SHA-256",
        )?;
        if supplied != ledger.identity_ledger_sha256()? || &ledger.to_value()? != value {
            return Err(contract("v5 G0 compact identity ledger identity drifted"));
        }
        Ok(ledger)
    }
}

fn g0_compact_ledger_identity(ledger: &V5G0CompactIdentityLedger) -> Result<Value> {
    let semantic = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_v5_g0_identity_ledger_authority_v1".to_owned()),
        ),
        (
            "generationConfigSha256",
            Value::String(ledger.generation_config_sha256.clone()),
        ),
        ("generationIndex", Value::from(ledger.generation_index)),
        (
            "sharedAuthoritySha256",
            Value::String(ledger.shared_authority_sha256.clone()),
        ),
    ]);
    let mut fields = semantic
        .as_object()
        .expect("constructed G0 compact ledger authority")
        .clone();
    fields.insert(
        "authoritySha256".to_owned(),
        Value::String(canonical_sha256(&semantic)?),
    );
    Ok(Value::Object(fields))
}

/// A content-addressed receipt for the common proposal scheduler/state.  G0
/// has no parent schedule by contract, and must not silently consume a parent
/// draw or crossover attempt.
#[derive(Clone, Debug, PartialEq)]
pub struct V5G0ScheduleStateReceipt {
    pub generation_index: u64,
    pub generation_config_sha256: String,
    pub shared_authority_sha256: String,
    pub target_accepted: u64,
    pub max_attempts: u64,
    pub parent_schedule_sha256: Option<String>,
    pub accepted_by_origin: BTreeMap<String, u64>,
    pub disposition_counts: BTreeMap<String, u64>,
    pub next_proposal_ordinal: u64,
    pub structural_parent_selections: u64,
    pub crossover_attempts: u64,
    pub structural_parent_draws: u64,
    pub proposal_state: Value,
}

impl V5G0ScheduleStateReceipt {
    fn semantic_value(&self) -> Result<Value> {
        if self.generation_index != 1
            || self.parent_schedule_sha256.is_some()
            || self.structural_parent_selections != 0
            || self.structural_parent_draws != 0
            || self.crossover_attempts != 0
        {
            return Err(contract(
                "v5 G0 schedule/state receipt has non-G0 parent semantics",
            ));
        }
        let proposal_state_sha256 = canonical_sha256(&self.proposal_state)?;
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_G0_SCHEDULE_STATE_RECEIPT_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            (
                "generationConfigSha256",
                Value::String(exact_sha(
                    &Value::String(self.generation_config_sha256.clone()),
                    "v5 G0 schedule receipt generation config SHA-256",
                )?),
            ),
            (
                "sharedAuthoritySha256",
                Value::String(exact_sha(
                    &Value::String(self.shared_authority_sha256.clone()),
                    "v5 G0 schedule receipt shared authority SHA-256",
                )?),
            ),
            ("targetAccepted", Value::from(self.target_accepted)),
            ("maxAttempts", Value::from(self.max_attempts)),
            ("parentScheduleSha256", Value::Null),
            (
                "acceptedByOrigin",
                count_map_value(&self.accepted_by_origin),
            ),
            (
                "dispositionCounts",
                count_map_value(&self.disposition_counts),
            ),
            (
                "nextProposalOrdinal",
                Value::from(self.next_proposal_ordinal),
            ),
            (
                "structuralParentSelections",
                Value::from(self.structural_parent_selections),
            ),
            ("crossoverAttempts", Value::from(self.crossover_attempts)),
            (
                "structuralParentDraws",
                Value::from(self.structural_parent_draws),
            ),
            ("proposalState", self.proposal_state.clone()),
            ("proposalStateSha256", Value::String(proposal_state_sha256)),
        ]))
    }

    pub fn schedule_state_receipt_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 G0 schedule/state receipt")
            .clone();
        fields.insert(
            "scheduleStateReceiptSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 G0 schedule/state receipt")?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "generationIndex",
                "generationConfigSha256",
                "sharedAuthoritySha256",
                "targetAccepted",
                "maxAttempts",
                "parentScheduleSha256",
                "acceptedByOrigin",
                "dispositionCounts",
                "nextProposalOrdinal",
                "structuralParentSelections",
                "crossoverAttempts",
                "structuralParentDraws",
                "proposalState",
                "proposalStateSha256",
                "scheduleStateReceiptSha256",
            ],
            "v5 G0 schedule/state receipt",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_G0_SCHEDULE_STATE_RECEIPT_SCHEMA)
            || !matches!(fields.get("parentScheduleSha256"), Some(Value::Null))
        {
            return Err(contract("v5 G0 schedule/state receipt schema is invalid"));
        }
        let proposal_state =
            required(value, "proposalState", "v5 G0 schedule/state receipt")?.clone();
        let proposal_state_sha = exact_sha(
            required(value, "proposalStateSha256", "v5 G0 schedule/state receipt")?,
            "v5 G0 schedule/state receipt proposal state SHA-256",
        )?;
        if proposal_state_sha != canonical_sha256(&proposal_state)? {
            return Err(contract(
                "v5 G0 schedule/state receipt proposal state identity drifted",
            ));
        }
        let receipt = Self {
            generation_index: required(value, "generationIndex", "v5 G0 schedule/state receipt")?
                .as_u64()
                .ok_or_else(|| {
                    contract("v5 G0 schedule/state receipt generation index is invalid")
                })?,
            generation_config_sha256: exact_sha(
                required(
                    value,
                    "generationConfigSha256",
                    "v5 G0 schedule/state receipt",
                )?,
                "v5 G0 schedule/state receipt generation config SHA-256",
            )?,
            shared_authority_sha256: exact_sha(
                required(
                    value,
                    "sharedAuthoritySha256",
                    "v5 G0 schedule/state receipt",
                )?,
                "v5 G0 schedule/state receipt shared authority SHA-256",
            )?,
            target_accepted: required(value, "targetAccepted", "v5 G0 schedule/state receipt")?
                .as_u64()
                .ok_or_else(|| contract("v5 G0 schedule/state receipt target is invalid"))?,
            max_attempts: required(value, "maxAttempts", "v5 G0 schedule/state receipt")?
                .as_u64()
                .ok_or_else(|| {
                    contract("v5 G0 schedule/state receipt maximum attempts is invalid")
                })?,
            parent_schedule_sha256: None,
            accepted_by_origin: parse_count_map(
                required(value, "acceptedByOrigin", "v5 G0 schedule/state receipt")?,
                "v5 G0 schedule/state receipt accepted-by-origin",
            )?,
            disposition_counts: parse_count_map(
                required(value, "dispositionCounts", "v5 G0 schedule/state receipt")?,
                "v5 G0 schedule/state receipt dispositions",
            )?,
            next_proposal_ordinal: required(
                value,
                "nextProposalOrdinal",
                "v5 G0 schedule/state receipt",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 G0 schedule/state receipt next ordinal is invalid"))?,
            structural_parent_selections: required(
                value,
                "structuralParentSelections",
                "v5 G0 schedule/state receipt",
            )?
            .as_u64()
            .ok_or_else(|| {
                contract("v5 G0 schedule/state receipt structural selections are invalid")
            })?,
            crossover_attempts: required(
                value,
                "crossoverAttempts",
                "v5 G0 schedule/state receipt",
            )?
            .as_u64()
            .ok_or_else(|| {
                contract("v5 G0 schedule/state receipt crossover attempts are invalid")
            })?,
            structural_parent_draws: required(
                value,
                "structuralParentDraws",
                "v5 G0 schedule/state receipt",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 G0 schedule/state receipt parent draws are invalid"))?,
            proposal_state,
        };
        let supplied = exact_sha(
            required(
                value,
                "scheduleStateReceiptSha256",
                "v5 G0 schedule/state receipt",
            )?,
            "v5 G0 schedule/state receipt SHA-256",
        )?;
        if supplied != receipt.schedule_state_receipt_sha256()? || &receipt.to_value()? != value {
            return Err(contract("v5 G0 schedule/state receipt identity drifted"));
        }
        Ok(receipt)
    }
}

/// Ordered selected-only projections.  This is the only selected population
/// handoff; no unselected rich candidate is reconstructed by this module.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5SelectedProjectionIndex {
    pub generation_index: u64,
    pub shared_authority_sha256: String,
    pub accepted_pool_sha256: String,
    pub selection_sha256: String,
    pub projections: Vec<V5SelectedProjection>,
}

impl V5SelectedProjectionIndex {
    fn semantic_value(&self) -> Result<Value> {
        if self.generation_index != 1 || self.projections.is_empty() {
            return Err(contract(
                "v5 selected projection index is empty or has wrong generation",
            ));
        }
        let mut records = BTreeSet::new();
        for projection in &self.projections {
            let _ = projection.to_value()?;
            if !records.insert(projection.record_sha256.clone()) {
                return Err(contract(
                    "v5 selected projection index repeats a compact record",
                ));
            }
        }
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_SELECTED_PROJECTION_INDEX_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            (
                "sharedAuthoritySha256",
                Value::String(exact_sha(
                    &Value::String(self.shared_authority_sha256.clone()),
                    "v5 selected projection index shared authority SHA-256",
                )?),
            ),
            (
                "acceptedPoolSha256",
                Value::String(exact_sha(
                    &Value::String(self.accepted_pool_sha256.clone()),
                    "v5 selected projection index accepted pool SHA-256",
                )?),
            ),
            (
                "selectionSha256",
                Value::String(exact_sha(
                    &Value::String(self.selection_sha256.clone()),
                    "v5 selected projection index selection SHA-256",
                )?),
            ),
            (
                "projections",
                Value::Array(
                    self.projections
                        .iter()
                        .map(V5SelectedProjection::to_value)
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                ),
            ),
        ]))
    }

    pub fn selected_projection_index_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 selected projection index")
            .clone();
        fields.insert(
            "selectedProjectionIndexSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 selected projection index")?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "generationIndex",
                "sharedAuthoritySha256",
                "acceptedPoolSha256",
                "selectionSha256",
                "projections",
                "selectedProjectionIndexSha256",
            ],
            "v5 selected projection index",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_SELECTED_PROJECTION_INDEX_SCHEMA)
        {
            return Err(contract("v5 selected projection index schema is invalid"));
        }
        let projections = required(value, "projections", "v5 selected projection index")?
            .as_array()
            .ok_or_else(|| contract("v5 selected projection index projections must be an array"))?
            .iter()
            .map(V5SelectedProjection::from_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let index = Self {
            generation_index: required(value, "generationIndex", "v5 selected projection index")?
                .as_u64()
                .ok_or_else(|| {
                    contract("v5 selected projection index generation index is invalid")
                })?,
            shared_authority_sha256: exact_sha(
                required(
                    value,
                    "sharedAuthoritySha256",
                    "v5 selected projection index",
                )?,
                "v5 selected projection index shared authority SHA-256",
            )?,
            accepted_pool_sha256: exact_sha(
                required(value, "acceptedPoolSha256", "v5 selected projection index")?,
                "v5 selected projection index accepted pool SHA-256",
            )?,
            selection_sha256: exact_sha(
                required(value, "selectionSha256", "v5 selected projection index")?,
                "v5 selected projection index selection SHA-256",
            )?,
            projections,
        };
        let supplied = exact_sha(
            required(
                value,
                "selectedProjectionIndexSha256",
                "v5 selected projection index",
            )?,
            "v5 selected projection index SHA-256",
        )?;
        if supplied != index.selected_projection_index_sha256()? || &index.to_value()? != value {
            return Err(contract("v5 selected projection index identity drifted"));
        }
        Ok(index)
    }
}

/// Ordered semantic root for the compact accepted-record JSONL stream.  File
/// byte SHA/length remain qd-batch inventory facts; this root binds the
/// parsed, canonical record order and prevents the batch layer from inventing
/// a second compact-journal schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5CompactAcceptedJournal {
    pub generation_index: u64,
    pub generation_config_sha256: String,
    pub shared_authority_sha256: String,
    pub ordered_record_sha256s: Vec<String>,
}

impl V5CompactAcceptedJournal {
    fn semantic_value(&self) -> Result<Value> {
        if self.generation_index != 1 {
            return Err(contract("v5 compact accepted journal has wrong generation"));
        }
        exact_sha(
            &Value::String(self.generation_config_sha256.clone()),
            "v5 compact accepted journal generation config SHA-256",
        )?;
        exact_sha(
            &Value::String(self.shared_authority_sha256.clone()),
            "v5 compact accepted journal shared authority SHA-256",
        )?;
        let mut seen = BTreeSet::new();
        for sha in &self.ordered_record_sha256s {
            exact_sha(
                &Value::String(sha.clone()),
                "v5 compact accepted journal record SHA-256",
            )?;
            if !seen.insert(sha) {
                return Err(contract(
                    "v5 compact accepted journal repeats a record SHA-256",
                ));
            }
        }
        Ok(object([
            (
                "schemaVersion",
                Value::String(v5::V5_COMPACT_JOURNAL_SCHEMA.to_owned()),
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
                "acceptedRecordCount",
                Value::from(self.ordered_record_sha256s.len() as u64),
            ),
            (
                "orderedRecordSha256s",
                Value::Array(
                    self.ordered_record_sha256s
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            ),
        ]))
    }

    pub fn compact_journal_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 compact accepted journal")
            .clone();
        fields.insert(
            "compactJournalSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 compact accepted journal")?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "generationIndex",
                "generationConfigSha256",
                "sharedAuthoritySha256",
                "acceptedRecordCount",
                "orderedRecordSha256s",
                "compactJournalSha256",
            ],
            "v5 compact accepted journal",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(v5::V5_COMPACT_JOURNAL_SCHEMA)
        {
            return Err(contract("v5 compact accepted journal schema is invalid"));
        }
        let journal = Self {
            generation_index: required(value, "generationIndex", "v5 compact accepted journal")?
                .as_u64()
                .ok_or_else(|| {
                    contract("v5 compact accepted journal generation index is invalid")
                })?,
            generation_config_sha256: exact_sha(
                required(
                    value,
                    "generationConfigSha256",
                    "v5 compact accepted journal",
                )?,
                "v5 compact accepted journal generation config SHA-256",
            )?,
            shared_authority_sha256: exact_sha(
                required(
                    value,
                    "sharedAuthoritySha256",
                    "v5 compact accepted journal",
                )?,
                "v5 compact accepted journal shared authority SHA-256",
            )?,
            ordered_record_sha256s: required(
                value,
                "orderedRecordSha256s",
                "v5 compact accepted journal",
            )?
            .as_array()
            .ok_or_else(|| contract("v5 compact accepted journal record SHAs must be an array"))?
            .iter()
            .map(|value| exact_sha(value, "v5 compact accepted journal record SHA-256"))
            .collect::<Result<Vec<_>>>()?,
        };
        if required(value, "acceptedRecordCount", "v5 compact accepted journal")?.as_u64()
            != Some(journal.ordered_record_sha256s.len() as u64)
        {
            return Err(contract("v5 compact accepted journal record count drifted"));
        }
        let supplied = exact_sha(
            required(value, "compactJournalSha256", "v5 compact accepted journal")?,
            "v5 compact accepted journal SHA-256",
        )?;
        if supplied != journal.compact_journal_sha256()? || &journal.to_value()? != value {
            return Err(contract("v5 compact accepted journal identity drifted"));
        }
        Ok(journal)
    }

    pub fn verify_records(&self, records: &[V5CompactAcceptedRecord]) -> Result<()> {
        if records.len() != self.ordered_record_sha256s.len() {
            return Err(contract(
                "v5 compact accepted journal record count does not replay",
            ));
        }
        for (birth_ordinal, (record, expected_sha)) in
            records.iter().zip(&self.ordered_record_sha256s).enumerate()
        {
            if record.generation_index != self.generation_index
                || record.shared_authority_sha256 != self.shared_authority_sha256
                || record.birth_ordinal != birth_ordinal as u64
                || record.record_sha256()? != *expected_sha
            {
                return Err(contract(
                    "v5 compact accepted journal record order/binding drifted",
                ));
            }
        }
        Ok(())
    }
}

/// Typed, write-neutral result.  qd-batch serializes each content-addressed
/// value atomically; this kernel result intentionally owns no paths or files.
#[derive(Clone, Debug)]
pub struct V5G0TransactionResult {
    pub generation_index: u64,
    pub generation_config_sha256: String,
    pub shared_authority_sha256: String,
    pub target_accepted: u64,
    pub max_attempts: u64,
    pub evaluation_width: u64,
    pub thread_cap: u64,
    pub target_reached: bool,
    pub stop_reason: String,
    pub attempts: Vec<V5ProposalAttemptRecord>,
    pub outcome_audits: Vec<V5AttemptOutcomeAudit>,
    pub attempt_journal: V5AttemptJournal,
    /// Ordered by birth ordinal.  These are the compact accepted records that
    /// replace the former population-sized rich-entry funnel.
    pub accepted_records: Vec<V5CompactAcceptedRecord>,
    pub compact_accepted_journal: V5CompactAcceptedJournal,
    /// Exact compact delta aligned by proposal ordinal with every attempt.
    /// A null slot is reserved for a future semantic no-op; any non-null
    /// attempt SHA must resolve to the corresponding immutable delta object.
    pub attempt_proposal_deltas: Vec<Option<Value>>,
    /// Exact delta aligned by index with `accepted_records`, retained only for
    /// reconstruction/adoption of accepted compact records.
    pub accepted_proposal_deltas: Vec<Value>,
    pub identity_ledger: V5G0CompactIdentityLedger,
    pub schedule_state_receipt: V5G0ScheduleStateReceipt,
    pub accepted_pool: Option<Value>,
    pub campaign_ledger: Option<Value>,
    pub g0_selection: Option<Value>,
    pub selected_projection_index: Option<V5SelectedProjectionIndex>,
    /// Core-derived, cap-free publication authority.  Batch persists this as
    /// a real content-addressed object and must never synthesize it from its
    /// control-plane manifest.
    pub publication_plan: V5G0PublicationPlan,
}

/// Exact compact-record object selected references are allowed to resolve.
/// Batch writes these values as canonical objects and authenticates their
/// bytes/inventory; the kernel derives them from the record itself so a path
/// cannot be caller-selected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5G0CompactRecordObjectBinding {
    pub record_sha256: String,
    pub relative_path: String,
}

/// Content-addressed binding for any non-null per-attempt compact delta.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5G0CompactDeltaObjectBinding {
    pub proposal_ordinal: u64,
    pub delta_sha256: String,
    pub relative_path: String,
}

/// The closed set of canonical compact objects a batch publisher must stage
/// for a G0 transaction.  The list intentionally contains no rich candidate
/// object: selected rich rows are private fragment inputs during fresh
/// publication and are never a durable adoption dependency.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum V5G0DurableObjectKind {
    PublicationPlan,
    AttemptJournal,
    AttemptOutcomeAudit,
    CompactProposalDelta,
    CompactAcceptedRecord,
    CompactAcceptedJournal,
    IdentityLedger,
    ScheduleStateReceipt,
    AcceptedPool,
    CampaignLedger,
    G0Selection,
    SelectedProjectionIndex,
}

impl V5G0DurableObjectKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PublicationPlan => "publicationPlan",
            Self::AttemptJournal => "attemptJournal",
            Self::AttemptOutcomeAudit => "attemptOutcomeAudit",
            Self::CompactProposalDelta => "compactProposalDelta",
            Self::CompactAcceptedRecord => "compactAcceptedRecord",
            Self::CompactAcceptedJournal => "compactAcceptedJournal",
            Self::IdentityLedger => "identityLedger",
            Self::ScheduleStateReceipt => "scheduleStateReceipt",
            Self::AcceptedPool => "acceptedPool",
            Self::CampaignLedger => "campaignLedger",
            Self::G0Selection => "g0Selection",
            Self::SelectedProjectionIndex => "selectedProjectionIndex",
        }
    }
}

/// A typed content-addressed object-store entry.  `value` is the exact
/// canonical semantic value that must be written at `relative_path`; the
/// publisher owns bytes/fsync/linking but may not substitute a path or invent
/// a parallel object schema.
#[derive(Clone, Debug, PartialEq)]
pub struct V5G0DurableObjectBinding {
    pub kind: V5G0DurableObjectKind,
    pub object_sha256: String,
    pub relative_path: String,
    pub value: Value,
}

impl V5G0DurableObjectBinding {
    fn new(kind: V5G0DurableObjectKind, value: Value) -> Result<Self> {
        let object_sha256 = durable_object_identity(kind, &value)?;
        Ok(Self {
            kind,
            relative_path: compact_record_object_relative_path(&object_sha256)?,
            object_sha256,
            value,
        })
    }

    pub fn validate(&self) -> Result<()> {
        let supplied = exact_sha(
            &Value::String(self.object_sha256.clone()),
            "v5 G0 durable object SHA-256",
        )?;
        let expected_path = compact_record_object_relative_path(&supplied)?;
        if self.relative_path != expected_path
            || durable_object_identity(self.kind, &self.value)? != supplied
        {
            return Err(contract("v5 G0 durable object binding drifted"));
        }
        Ok(())
    }

    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(object([
            ("kind", Value::String(self.kind.as_str().to_owned())),
            ("objectSha256", Value::String(self.object_sha256.clone())),
            ("relativePath", Value::String(self.relative_path.clone())),
            ("value", self.value.clone()),
        ]))
    }
}

fn self_hashed_object_identity(value: &Value, key: &str, label: &str) -> Result<String> {
    let identity = exact_sha(required(value, key, label)?, &format!("{label} SHA-256"))?;
    if canonical_sha256_without_object_field(value, key)? != identity {
        return Err(contract(format!("{label} self-hash drifted")));
    }
    Ok(identity)
}

/// A G0 accepted-pool hash is a semantic projection over references sorted by
/// their identity, not merely the ordinary object value with its self-hash
/// field removed.  Validate through the G0 type first, then require the
/// persisted object itself to use that canonical ordering so one object SHA
/// names exactly one canonical JSON value.
fn accepted_pool_durable_object_identity(value: &Value) -> Result<String> {
    let _ = g0::validate_accepted_pool(value)?;
    let identity = exact_sha(
        required(value, "acceptedPoolSha256", "v5 G0 accepted pool")?,
        "v5 G0 accepted pool SHA-256",
    )?;
    if canonical_sha256_without_object_field(value, "acceptedPoolSha256")? != identity {
        return Err(contract(
            "v5 G0 accepted pool is semantically valid but not canonically reference-SHA ordered",
        ));
    }
    Ok(identity)
}

fn durable_object_identity(kind: V5G0DurableObjectKind, value: &Value) -> Result<String> {
    match kind {
        V5G0DurableObjectKind::PublicationPlan => V5G0PublicationPlan::from_value(value)
            .map_err(|error| {
                contract(format!(
                    "v5 G0 durable publication plan is invalid: {error}"
                ))
            })?
            .publication_plan_sha256()
            .map_err(|error| {
                contract(format!(
                    "v5 G0 durable publication plan identity failed: {error}"
                ))
            }),
        V5G0DurableObjectKind::AttemptJournal => V5AttemptJournal::from_value(value)?
            .attempt_journal_sha256()
            .map_err(V5G0TransactionError::from),
        V5G0DurableObjectKind::AttemptOutcomeAudit => V5AttemptOutcomeAudit::from_value(value)?
            .audit_sha256()
            .map_err(V5G0TransactionError::from),
        V5G0DurableObjectKind::CompactProposalDelta => {
            v5::validate_proposal_delta(value)?;
            exact_sha(
                required(value, "deltaSha256", "v5 G0 compact proposal delta")?,
                "v5 G0 compact proposal delta SHA-256",
            )
        }
        V5G0DurableObjectKind::CompactAcceptedRecord => V5CompactAcceptedRecord::from_value(value)?
            .record_sha256()
            .map_err(V5G0TransactionError::from),
        V5G0DurableObjectKind::CompactAcceptedJournal => {
            V5CompactAcceptedJournal::from_value(value)?.compact_journal_sha256()
        }
        V5G0DurableObjectKind::IdentityLedger => {
            V5G0CompactIdentityLedger::from_value(value)?.identity_ledger_sha256()
        }
        V5G0DurableObjectKind::ScheduleStateReceipt => {
            V5G0ScheduleStateReceipt::from_value(value)?.schedule_state_receipt_sha256()
        }
        V5G0DurableObjectKind::AcceptedPool => accepted_pool_durable_object_identity(value),
        V5G0DurableObjectKind::CampaignLedger => {
            self_hashed_object_identity(value, "ledgerSha256", "v5 G0 campaign ledger")
        }
        V5G0DurableObjectKind::G0Selection => {
            self_hashed_object_identity(value, "selectionSha256", "v5 G0 selection")
        }
        V5G0DurableObjectKind::SelectedProjectionIndex => {
            V5SelectedProjectionIndex::from_value(value)?.selected_projection_index_sha256()
        }
    }
}

impl V5G0CompactDeltaObjectBinding {
    pub fn to_value(&self) -> Result<Value> {
        let expected_path = compact_delta_object_relative_path(&self.delta_sha256)?;
        if self.relative_path != expected_path {
            return Err(contract("v5 compact delta object binding path drifted"));
        }
        Ok(object([
            ("proposalOrdinal", Value::from(self.proposal_ordinal)),
            (
                "deltaSha256",
                Value::String(exact_sha(
                    &Value::String(self.delta_sha256.clone()),
                    "v5 compact delta object binding SHA-256",
                )?),
            ),
            ("relativePath", Value::String(expected_path)),
        ]))
    }
}

impl V5G0CompactRecordObjectBinding {
    pub fn to_value(&self) -> Result<Value> {
        let expected_path = compact_record_object_relative_path(&self.record_sha256)?;
        if self.relative_path != expected_path {
            return Err(contract("v5 compact record object binding path drifted"));
        }
        Ok(object([
            (
                "recordSha256",
                Value::String(exact_sha(
                    &Value::String(self.record_sha256.clone()),
                    "v5 compact record object binding SHA-256",
                )?),
            ),
            ("relativePath", Value::String(expected_path)),
        ]))
    }
}

/// The opt-in selected-only bridge from compact G0 construction to a rich
/// evaluator/publication candidate.  It is intentionally absent from
/// `V5G0TransactionResult`: G0 construction never expands an unselected
/// population.  A caller first authenticates the transaction and selection
/// index, then materializes just the selected compact record it needs.
#[derive(Clone, Debug, PartialEq)]
pub struct V5SelectedG0Materialization {
    /// Rich native candidate containing the reconstructed source profile. It
    /// is suitable for the selected evaluation/population stream only.
    pub rich_evaluation_candidate: Value,
    /// Canonical compact row carrying the evaluator/funnel projections needed
    /// by a later write-neutral publication plan.  It is self-hashed and
    /// binds the selected projection and immutable compact-record object.
    pub publication_precomputed_row: Value,
}

/// Reconstruct one selected compact G0 record for evaluation/publication.
///
/// The caller must first prove `selected_projection` belongs to the verified
/// transaction's selected-projection index.  This narrow signature then
/// independently reconstructs the exact program/profile/pair facts from the
/// sealed authority and compact delta; neither qd-batch nor a Python bridge
/// is allowed to re-run factory construction.
pub fn materialize_selected_v5_g0_record(
    authority: &V5SharedConstructionAuthority,
    selected_projection: &V5SelectedProjection,
    compact_delta: &Value,
    compact_record: &V5CompactAcceptedRecord,
) -> Result<V5SelectedG0Materialization> {
    selected_projection.verify_against_record(compact_record)?;
    verify_reconstruct_compact_g0_record(authority, compact_delta, compact_record)?;
    let rich_evaluation_candidate = materialize_selected_v5_g0_rich_candidate(
        authority,
        selected_projection,
        compact_delta,
        compact_record,
    )?;
    let compact_record_sha256 = compact_record.record_sha256()?;
    let selected_projection_sha256 = exact_sha(
        selected_projection
            .to_value()?
            .get("projectionSha256")
            .ok_or_else(|| contract("selected v5 projection lacks identity"))?,
        "selected v5 projection SHA-256",
    )?;
    let rich_evaluation_candidate_sha256 = canonical_sha256(&rich_evaluation_candidate)?;
    let evaluation_candidate = object([
        (
            "candidateId",
            rich_evaluation_candidate
                .get("candidateId")
                .cloned()
                .ok_or_else(|| contract("selected rich candidate lacks candidate ID"))?,
        ),
        (
            "sourceMode",
            rich_evaluation_candidate
                .get("sourceMode")
                .cloned()
                .ok_or_else(|| contract("selected rich candidate lacks source mode"))?,
        ),
        (
            "seedId",
            rich_evaluation_candidate
                .get("seedId")
                .cloned()
                .ok_or_else(|| contract("selected rich candidate lacks seed ID"))?,
        ),
        (
            "candidateIdentitySha256",
            rich_evaluation_candidate
                .get("candidateIdentitySha256")
                .cloned()
                .ok_or_else(|| contract("selected rich candidate lacks identity"))?,
        ),
        (
            "programSha256",
            rich_evaluation_candidate
                .get("programSha256")
                .cloned()
                .ok_or_else(|| contract("selected rich candidate lacks program SHA-256"))?,
        ),
        (
            "sourceProfile",
            rich_evaluation_candidate
                .get("sourceProfile")
                .cloned()
                .ok_or_else(|| contract("selected rich candidate lacks source profile"))?,
        ),
        (
            "sourceProfileSha256",
            rich_evaluation_candidate
                .get("sourceProfileSha256")
                .cloned()
                .ok_or_else(|| contract("selected rich candidate lacks source profile SHA-256"))?,
        ),
        (
            "profileSnapshotSha256",
            rich_evaluation_candidate
                .get("profileSnapshotSha256")
                .cloned()
                .ok_or_else(|| {
                    contract("selected rich candidate lacks profile snapshot SHA-256")
                })?,
        ),
        (
            "validationReportSha256",
            rich_evaluation_candidate
                .get("validationReportSha256")
                .cloned()
                .ok_or_else(|| {
                    contract("selected rich candidate lacks validation report SHA-256")
                })?,
        ),
        (
            "structuralOperatorHistory",
            rich_evaluation_candidate
                .get("structuralOperatorHistory")
                .cloned()
                .ok_or_else(|| contract("selected rich candidate lacks operator history"))?,
        ),
        (
            "proposalOrdinal",
            Value::from(compact_record.proposal_ordinal),
        ),
        (
            "proposalEntrySha256",
            Value::String(compact_record_sha256.clone()),
        ),
    ]);
    let funnel_entry = object([
        (
            "schemaVersion",
            Value::String(V5_PROPOSAL_FUNNEL_ENTRY_SCHEMA.to_owned()),
        ),
        ("entrySha256", Value::String(compact_record_sha256.clone())),
        (
            "proposalOrdinal",
            Value::from(compact_record.proposal_ordinal),
        ),
        (
            "originKind",
            Value::String(compact_record.origin_kind.clone()),
        ),
        ("disposition", Value::String("accepted".to_owned())),
        (
            "acceptedCompactRecordSha256",
            Value::String(compact_record_sha256.clone()),
        ),
        (
            "candidate",
            object([
                (
                    "candidateId",
                    Value::String(compact_record.candidate_id.clone()),
                ),
                (
                    "sourceProfileSha256",
                    rich_evaluation_candidate
                        .get("sourceProfileSha256")
                        .cloned()
                        .ok_or_else(|| {
                            contract("selected rich candidate lacks source profile SHA-256")
                        })?,
                ),
            ]),
        ),
        (
            "proposal",
            object([
                (
                    "candidateId",
                    Value::String(compact_record.candidate_id.clone()),
                ),
                (
                    "rawSourceProfileSha256",
                    rich_evaluation_candidate
                        .get("sourceProfileSha256")
                        .cloned()
                        .ok_or_else(|| {
                            contract("selected rich candidate lacks source profile SHA-256")
                        })?,
                ),
            ]),
        ),
        (
            "funnelCandidate",
            v5::v5_funnel_candidate_projection(
                compact_record,
                V5FunnelAdmission::G0BootstrapAccepted,
            )?,
        ),
    ]);
    let semantic_row = object([
        (
            "schemaVersion",
            Value::String(V5_SELECTED_PUBLICATION_ROW_SCHEMA.to_owned()),
        ),
        ("compactRecordSha256", Value::String(compact_record_sha256)),
        (
            "selectedProjectionSha256",
            Value::String(selected_projection_sha256),
        ),
        (
            "richEvaluationCandidateSha256",
            Value::String(rich_evaluation_candidate_sha256),
        ),
        ("evaluationCandidate", evaluation_candidate),
        ("funnelEntry", funnel_entry),
    ]);
    let mut row_fields = semantic_row
        .as_object()
        .expect("constructed selected v5 publication row")
        .clone();
    row_fields.insert(
        "publicationPrecomputedRowSha256".to_owned(),
        Value::String(canonical_sha256(&semantic_row)?),
    );
    Ok(V5SelectedG0Materialization {
        rich_evaluation_candidate,
        publication_precomputed_row: Value::Object(row_fields),
    })
}

/// Recompute a selected materialization from sealed authority and reject any
/// substituted rich candidate or precomputed publication row.  This is the
/// selected-only replay counterpart to `verify_v5_g0_transaction_replay`.
pub fn verify_selected_v5_g0_materialization(
    authority: &V5SharedConstructionAuthority,
    selected_projection: &V5SelectedProjection,
    compact_delta: &Value,
    compact_record: &V5CompactAcceptedRecord,
    materialization: &V5SelectedG0Materialization,
) -> Result<()> {
    let expected = materialize_selected_v5_g0_record(
        authority,
        selected_projection,
        compact_delta,
        compact_record,
    )?;
    if materialization != &expected {
        return Err(contract(
            "selected v5 G0 materialization does not replay from compact authority",
        ));
    }
    Ok(())
}

impl V5G0TransactionResult {
    /// Parse one complete canonical proposal checkpoint without replaying the
    /// scientific constructor.  Every nested component still validates its
    /// exact schema and self-hash, and the complete canonical transaction root
    /// is checked byte-for-byte.  A caller that needs audited reconstruction
    /// invokes [`Self::verify_checkpoint_replay`] explicitly.
    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = value
            .as_object()
            .ok_or_else(|| contract("v5 G0 transaction checkpoint must be an object"))?;
        let expected = [
            "schemaVersion",
            "generationIndex",
            "generationConfigSha256",
            "sharedAuthoritySha256",
            "targetAccepted",
            "maxAttempts",
            "evaluationWidth",
            "targetReached",
            "stopReason",
            "attemptJournal",
            "attempts",
            "outcomeAudits",
            "acceptedRecords",
            "compactAcceptedJournal",
            "attemptProposalDeltas",
            "acceptedProposalDeltas",
            "identityLedger",
            "scheduleStateReceipt",
            "acceptedPool",
            "campaignLedger",
            "g0Selection",
            "selectedProjectionIndex",
            "publicationPlanSha256",
            "publicationPlan",
            "transactionSha256",
        ];
        if fields.len() != expected.len() || expected.iter().any(|key| !fields.contains_key(*key)) {
            return Err(contract("v5 G0 transaction checkpoint fields are not exact"));
        }
        if required(value, "schemaVersion", "v5 G0 transaction checkpoint")?.as_str()
            != Some(V5_G0_TRANSACTION_SCHEMA)
        {
            return Err(contract("v5 G0 transaction checkpoint schema is invalid"));
        }
        let supplied = exact_sha(
            required(value, "transactionSha256", "v5 G0 transaction checkpoint")?,
            "v5 G0 transaction checkpoint SHA-256",
        )?;
        if canonical_sha256_without_object_field(value, "transactionSha256")? != supplied {
            return Err(contract("v5 G0 transaction checkpoint identity drifted"));
        }
        let attempt_rows = required(value, "attempts", "v5 G0 transaction checkpoint")?
            .as_array()
            .ok_or_else(|| contract("v5 G0 transaction attempts must be an array"))?;
        let outcome_audits = required(value, "outcomeAudits", "v5 G0 transaction checkpoint")?
            .as_array()
            .ok_or_else(|| contract("v5 G0 transaction audits must be an array"))?;
        let accepted_records = required(value, "acceptedRecords", "v5 G0 transaction checkpoint")?
            .as_array()
            .ok_or_else(|| contract("v5 G0 accepted records must be an array"))?;
        let attempt_deltas = required(
            value,
            "attemptProposalDeltas",
            "v5 G0 transaction checkpoint",
        )?
        .as_array()
        .ok_or_else(|| contract("v5 G0 attempt deltas must be an array"))?;
        let accepted_deltas = required(
            value,
            "acceptedProposalDeltas",
            "v5 G0 transaction checkpoint",
        )?
        .as_array()
        .ok_or_else(|| contract("v5 G0 accepted deltas must be an array"))?;
        let optional = |field: &str| -> Result<Option<&Value>> {
            let value = required(value, field, "v5 G0 transaction checkpoint")?;
            Ok((!value.is_null()).then_some(value))
        };
        let durable = V5G0DurableArtifacts::from_canonical_values(
            required(value, "attemptJournal", "v5 G0 transaction checkpoint")?,
            attempt_rows,
            outcome_audits,
            accepted_records,
            required(
                value,
                "compactAcceptedJournal",
                "v5 G0 transaction checkpoint",
            )?,
            attempt_deltas,
            accepted_deltas,
            required(value, "identityLedger", "v5 G0 transaction checkpoint")?,
            required(
                value,
                "scheduleStateReceipt",
                "v5 G0 transaction checkpoint",
            )?,
            optional("acceptedPool")?,
            optional("campaignLedger")?,
            optional("g0Selection")?,
            optional("selectedProjectionIndex")?,
            required(value, "publicationPlan", "v5 G0 transaction checkpoint")?,
        )?;
        let result = Self {
            generation_index: required(value, "generationIndex", "v5 G0 transaction checkpoint")?
                .as_u64()
                .ok_or_else(|| contract("v5 G0 transaction generation index is invalid"))?,
            generation_config_sha256: exact_sha(
                required(
                    value,
                    "generationConfigSha256",
                    "v5 G0 transaction checkpoint",
                )?,
                "v5 G0 transaction generation config SHA-256",
            )?,
            shared_authority_sha256: exact_sha(
                required(
                    value,
                    "sharedAuthoritySha256",
                    "v5 G0 transaction checkpoint",
                )?,
                "v5 G0 transaction shared authority SHA-256",
            )?,
            target_accepted: required(value, "targetAccepted", "v5 G0 transaction checkpoint")?
                .as_u64()
                .ok_or_else(|| contract("v5 G0 transaction target is invalid"))?,
            max_attempts: required(value, "maxAttempts", "v5 G0 transaction checkpoint")?
                .as_u64()
                .ok_or_else(|| contract("v5 G0 transaction maximum attempts is invalid"))?,
            evaluation_width: required(
                value,
                "evaluationWidth",
                "v5 G0 transaction checkpoint",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 G0 transaction evaluation width is invalid"))?,
            // The thread cap is deliberately excluded from semantic identity.
            thread_cap: 1,
            target_reached: required(value, "targetReached", "v5 G0 transaction checkpoint")?
                .as_bool()
                .ok_or_else(|| contract("v5 G0 transaction target state is invalid"))?,
            stop_reason: exact_text(
                required(value, "stopReason", "v5 G0 transaction checkpoint")?,
                "v5 G0 transaction stop reason",
            )?,
            attempts: durable.attempts,
            outcome_audits: durable.outcome_audits,
            attempt_journal: durable.attempt_journal,
            accepted_records: durable.accepted_records,
            compact_accepted_journal: durable.compact_accepted_journal,
            attempt_proposal_deltas: durable.attempt_proposal_deltas,
            accepted_proposal_deltas: durable.accepted_proposal_deltas,
            identity_ledger: durable.identity_ledger,
            schedule_state_receipt: durable.schedule_state_receipt,
            accepted_pool: durable.accepted_pool,
            campaign_ledger: durable.campaign_ledger,
            g0_selection: durable.g0_selection,
            selected_projection_index: durable.selected_projection_index,
            publication_plan: durable.publication_plan,
        };
        if result.transaction_sha256()? != supplied || result.to_value()? != *value {
            return Err(contract(
                "v5 G0 transaction checkpoint differs from its typed components",
            ));
        }
        Ok(result)
    }

    /// Run the expensive sealed-authority reconstruction only when an audited
    /// proof is requested.  Normal trusted restart uses [`Self::from_value`].
    pub fn verify_checkpoint_replay(&self, request: &V5G0TransactionRequest) -> Result<()> {
        verify_v5_g0_transaction_replay(request, self)
    }

    pub fn publication_plan_sha256(&self) -> Result<String> {
        self.publication_plan
            .publication_plan_sha256()
            .map_err(|error| contract(format!("v5 G0 publication plan identity failed: {error}")))
    }

    pub fn publication_plan_object_binding(
        &self,
    ) -> Result<crate::v5_publication::V5G0PublicationPlanObjectBinding> {
        self.publication_plan.object_binding().map_err(|error| {
            contract(format!(
                "v5 G0 publication plan object binding failed: {error}"
            ))
        })
    }

    pub fn compact_record_object_bindings(&self) -> Result<Vec<V5G0CompactRecordObjectBinding>> {
        self.accepted_records
            .iter()
            .map(|record| {
                let record_sha256 = record.record_sha256()?;
                Ok(V5G0CompactRecordObjectBinding {
                    relative_path: compact_record_object_relative_path(&record_sha256)?,
                    record_sha256,
                })
            })
            .collect()
    }

    /// Immutable compact-delta objects needed to replay all attempts and
    /// audits.  This includes rejected/duplicate attempts; values are ordered
    /// by proposal ordinal rather than by acceptance/birth ordinal.
    pub fn compact_delta_object_bindings(&self) -> Result<Vec<V5G0CompactDeltaObjectBinding>> {
        if self.attempt_proposal_deltas.len() != self.attempts.len() {
            return Err(contract(
                "v5 compact delta bindings are not aligned with attempts",
            ));
        }
        self.attempts
            .iter()
            .zip(&self.attempt_proposal_deltas)
            .filter_map(|(attempt, delta)| {
                delta.as_ref().map(|delta| {
                    let delta_sha256 = exact_sha(
                        required(delta, "deltaSha256", "v5 compact attempt proposal delta")?,
                        "v5 compact attempt proposal delta SHA-256",
                    )?;
                    Ok(V5G0CompactDeltaObjectBinding {
                        proposal_ordinal: attempt.proposal_ordinal,
                        relative_path: compact_delta_object_relative_path(&delta_sha256)?,
                        delta_sha256,
                    })
                })
            })
            .collect()
    }

    /// Exact content-addressed semantic objects required to reconstruct this
    /// transaction after a restart.  Values are compact only: no rich
    /// candidate, evaluation row, or private publication fragment is exposed.
    ///
    /// `V5G0PublicationReceipt::object_binding` is intentionally separate:
    /// that receipt is minted only after the four public documents have been
    /// fresh-verified, so it cannot exist at transaction construction time.
    pub fn durable_object_bindings(&self) -> Result<Vec<V5G0DurableObjectBinding>> {
        self.validate_shape()?;
        let mut bindings = Vec::new();
        let mut seen = BTreeSet::new();
        let mut append = |kind: V5G0DurableObjectKind, value: Value| -> Result<()> {
            let binding = V5G0DurableObjectBinding::new(kind, value)?;
            if !seen.insert(binding.object_sha256.clone()) {
                return Err(contract(
                    "v5 G0 durable object list repeats a content-addressed identity",
                ));
            }
            bindings.push(binding);
            Ok(())
        };

        append(
            V5G0DurableObjectKind::PublicationPlan,
            self.publication_plan.to_value().map_err(|error| {
                contract(format!("v5 G0 publication plan encoding failed: {error}"))
            })?,
        )?;
        append(
            V5G0DurableObjectKind::AttemptJournal,
            self.attempt_journal.to_value()?,
        )?;
        for audit in &self.outcome_audits {
            append(
                V5G0DurableObjectKind::AttemptOutcomeAudit,
                audit.to_value()?,
            )?;
        }
        for delta in self.attempt_proposal_deltas.iter().flatten() {
            append(V5G0DurableObjectKind::CompactProposalDelta, delta.clone())?;
        }
        for record in &self.accepted_records {
            append(
                V5G0DurableObjectKind::CompactAcceptedRecord,
                record.to_value()?,
            )?;
        }
        append(
            V5G0DurableObjectKind::CompactAcceptedJournal,
            self.compact_accepted_journal.to_value()?,
        )?;
        append(
            V5G0DurableObjectKind::IdentityLedger,
            self.identity_ledger.to_value()?,
        )?;
        append(
            V5G0DurableObjectKind::ScheduleStateReceipt,
            self.schedule_state_receipt.to_value()?,
        )?;
        if let Some(pool) = &self.accepted_pool {
            append(V5G0DurableObjectKind::AcceptedPool, pool.clone())?;
        }
        if let Some(ledger) = &self.campaign_ledger {
            append(V5G0DurableObjectKind::CampaignLedger, ledger.clone())?;
        }
        if let Some(selection) = &self.g0_selection {
            append(V5G0DurableObjectKind::G0Selection, selection.clone())?;
        }
        if let Some(index) = &self.selected_projection_index {
            append(
                V5G0DurableObjectKind::SelectedProjectionIndex,
                index.to_value()?,
            )?;
        }
        Ok(bindings)
    }

    /// A small semantic root for the in-memory transaction result.  Durable
    /// writers normally persist the typed child objects separately, but this
    /// root gives a receiver one deterministic replay binding without adding
    /// a batch-owned file format.
    pub fn transaction_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 G0 transaction result")
            .clone();
        fields.insert(
            "transactionSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    fn semantic_value(&self) -> Result<Value> {
        self.validate_shape()?;
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_G0_TRANSACTION_SCHEMA.to_owned()),
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
            ("targetAccepted", Value::from(self.target_accepted)),
            ("maxAttempts", Value::from(self.max_attempts)),
            ("evaluationWidth", Value::from(self.evaluation_width)),
            // `threadCap` is execution-control-plane input only.  It is
            // intentionally omitted from every *kernel* durable semantic
            // root so cap 1 and cap 8 yield byte-identical transaction,
            // candidate, and G0 artifacts.  qd-batch may still report it in
            // outer control-plane receipt/manifest telemetry.
            ("targetReached", Value::Bool(self.target_reached)),
            ("stopReason", Value::String(self.stop_reason.clone())),
            ("attemptJournal", self.attempt_journal.to_value()?),
            (
                "attempts",
                Value::Array(
                    self.attempts
                        .iter()
                        .map(V5ProposalAttemptRecord::to_value)
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                ),
            ),
            (
                "outcomeAudits",
                Value::Array(
                    self.outcome_audits
                        .iter()
                        .map(V5AttemptOutcomeAudit::to_value)
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                ),
            ),
            (
                "acceptedRecords",
                Value::Array(
                    self.accepted_records
                        .iter()
                        .map(V5CompactAcceptedRecord::to_value)
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                ),
            ),
            (
                "compactAcceptedJournal",
                self.compact_accepted_journal.to_value()?,
            ),
            (
                "attemptProposalDeltas",
                Value::Array(
                    self.attempt_proposal_deltas
                        .iter()
                        .cloned()
                        .map(|delta| delta.unwrap_or(Value::Null))
                        .collect(),
                ),
            ),
            (
                "acceptedProposalDeltas",
                Value::Array(self.accepted_proposal_deltas.clone()),
            ),
            ("identityLedger", self.identity_ledger.to_value()?),
            (
                "scheduleStateReceipt",
                self.schedule_state_receipt.to_value()?,
            ),
            (
                "acceptedPool",
                self.accepted_pool.clone().unwrap_or(Value::Null),
            ),
            (
                "campaignLedger",
                self.campaign_ledger.clone().unwrap_or(Value::Null),
            ),
            (
                "g0Selection",
                self.g0_selection.clone().unwrap_or(Value::Null),
            ),
            (
                "selectedProjectionIndex",
                self.selected_projection_index
                    .as_ref()
                    .map(V5SelectedProjectionIndex::to_value)
                    .transpose()?
                    .unwrap_or(Value::Null),
            ),
            (
                "publicationPlanSha256",
                Value::String(self.publication_plan_sha256()?),
            ),
            (
                "publicationPlan",
                self.publication_plan.to_value().map_err(|error| {
                    contract(format!("v5 G0 publication plan encoding failed: {error}"))
                })?,
            ),
        ]))
    }

    fn validate_shape(&self) -> Result<()> {
        if self.generation_index != 1
            || self.target_accepted == 0
            || self.max_attempts < self.target_accepted
            || self.evaluation_width == 0
            || self.evaluation_width > self.target_accepted
            || !(1..=8).contains(&self.thread_cap)
        {
            return Err(contract(
                "v5 G0 transaction result request bounds are invalid",
            ));
        }
        exact_sha(
            &Value::String(self.generation_config_sha256.clone()),
            "v5 G0 transaction generation config SHA-256",
        )?;
        self.publication_plan.validate_shape().map_err(|error| {
            contract(format!("v5 G0 result publication plan is invalid: {error}"))
        })?;
        if self.publication_plan.generation_config_sha256 != self.generation_config_sha256
            || self.publication_plan.generation_index != self.generation_index
            || self.publication_plan.target_unique_candidates != self.target_accepted
            || self.publication_plan.max_proposal_attempts != self.max_attempts
            || self.publication_plan.evaluation_population_size != self.evaluation_width
            || exact_sha(
                required(
                    &self.publication_plan.frozen_authority,
                    "authoritySha256",
                    "v5 G0 result publication plan frozen authority",
                )?,
                "v5 G0 result publication plan frozen authority SHA-256",
            )? != self.shared_authority_sha256
        {
            return Err(contract(
                "v5 G0 transaction publication plan binding drifted",
            ));
        }
        exact_sha(
            &Value::String(self.shared_authority_sha256.clone()),
            "v5 G0 transaction shared authority SHA-256",
        )?;
        if self.attempts != self.attempt_journal.attempts {
            return Err(contract(
                "v5 G0 transaction attempts diverge from attempt journal",
            ));
        }
        if self.compact_accepted_journal.generation_index != self.generation_index
            || self.compact_accepted_journal.generation_config_sha256
                != self.generation_config_sha256
            || self.compact_accepted_journal.shared_authority_sha256 != self.shared_authority_sha256
        {
            return Err(contract(
                "v5 G0 transaction compact journal authority binding drifted",
            ));
        }
        if self.attempts.len() as u64 > self.max_attempts
            || self.accepted_records.len() as u64 > self.target_accepted
            || self.accepted_records.len() != self.accepted_proposal_deltas.len()
            || self.attempts.len() != self.attempt_proposal_deltas.len()
        {
            return Err(contract("v5 G0 transaction count bounds drifted"));
        }
        let expected_stop = if self.target_reached {
            "accepted_target_reached"
        } else {
            "max_attempts_reached"
        };
        if self.stop_reason != expected_stop
            || self.target_reached != (self.accepted_records.len() as u64 == self.target_accepted)
            || (!self.target_reached && self.attempts.len() as u64 != self.max_attempts)
        {
            return Err(contract("v5 G0 transaction stop status is invalid"));
        }
        Ok(())
    }

    /// Verify all compact facts against the exact parsed sealed authority.
    /// This is the replay gate used before batch publication/adoption; it
    /// rejects a self-hashed but non-reconstructible compact record without
    /// ever expanding a legacy rich candidate.
    pub fn verify_replay(&self, authority: &V5SharedConstructionAuthority) -> Result<()> {
        self.validate_shape()?;
        if self.shared_authority_sha256 != authority.shared_authority_sha256 {
            return Err(contract("v5 G0 transaction authority identity drifted"));
        }
        let _ = self.attempt_journal.to_value()?;
        self.attempt_journal
            .verify_outcome_audit_replay(self.outcome_audits.clone())?;
        self.attempt_journal
            .verify_accepted_record_replay(self.accepted_records.clone())?;
        self.compact_accepted_journal
            .verify_records(&self.accepted_records)?;
        verify_attempt_proposal_deltas(self)?;
        for (record, delta) in self
            .accepted_records
            .iter()
            .zip(self.accepted_proposal_deltas.iter())
        {
            verify_reconstruct_compact_g0_record(authority, delta, record)?;
        }
        verify_identity_ledger(self)?;
        verify_schedule_state_receipt(self)?;
        self.verify_g0_products()?;
        Ok(())
    }

    fn verify_g0_products(&self) -> Result<()> {
        match (
            &self.accepted_pool,
            &self.campaign_ledger,
            &self.g0_selection,
            &self.selected_projection_index,
        ) {
            (Some(pool), Some(ledger), Some(selection), Some(index)) if self.target_reached => {
                let selection = verify_g0_bootstrap_selection(selection, pool)?;
                let selected = selection
                    .get("selected")
                    .and_then(Value::as_array)
                    .ok_or_else(|| contract("v5 G0 selection lacks selected rows"))?;
                let selected_hashes = selected
                    .iter()
                    .map(|row| {
                        exact_sha(
                            row.get("referenceSha256").ok_or_else(|| {
                                contract("v5 G0 selected row lacks reference SHA-256")
                            })?,
                            "v5 G0 selected reference SHA-256",
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let expected_ledger = materialize_campaign_ledger(pool, &selected_hashes)?;
                if ledger != &expected_ledger {
                    return Err(contract(
                        "v5 G0 campaign ledger diverges from pool/selection",
                    ));
                }
                let _ = verify_campaign_ledger(ledger, pool, &selected_hashes)?;
                verify_compact_record_object_paths(pool)?;
                verify_selected_projection_index(self, pool, &selection, index, &selected_hashes)?;
                Ok(())
            }
            (None, None, None, None) if !self.target_reached => Ok(()),
            _ => Err(contract(
                "v5 G0 products are incomplete or present before target completion",
            )),
        }
    }
}

/// Parse the authority once and replay a typed transaction result.  It is the
/// public batch/adoption entry point for persisted compact objects.
pub fn verify_v5_g0_transaction_replay(
    request: &V5G0TransactionRequest,
    result: &V5G0TransactionResult,
) -> Result<()> {
    request.validate_bounds()?;
    let authority = V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)?;
    verify_v5_g0_transaction_replay_with_authority(request, result, &authority)
}

/// Crate-visible replay variant for another native v5 boundary which has
/// already parsed the sealed authority.  In particular the publication stream
/// uses this to avoid a second authority decode before it materializes a
/// selected compact record.
pub(crate) fn verify_v5_g0_transaction_replay_with_authority(
    request: &V5G0TransactionRequest,
    result: &V5G0TransactionResult,
    authority: &V5SharedConstructionAuthority,
) -> Result<()> {
    request.validate_bounds()?;
    V5G0TransactionRequest::validate_parsed_authority(&authority)?;
    if result.generation_index != request.generation_index
        || result.generation_config_sha256 != request.generation_config_sha256
        || result.target_accepted != request.target_accepted
        || result.max_attempts != request.max_attempts
        || result.evaluation_width != request.evaluation_width
    {
        return Err(contract("v5 G0 transaction result does not bind request"));
    }
    result
        .publication_plan
        .validate_against_request(request, authority)
        .map_err(|error| {
            contract(format!(
                "v5 G0 transaction publication plan replay failed: {error}"
            ))
        })?;
    result.verify_replay(&authority)
}

/// Parsed canonical durable values owned by qd-batch.  This is deliberately a
/// typed *projection*, not a second batch schema: parsers below are the same
/// kernel types used by construction and `into_verified_result` enters the
/// exact transaction replay gate.
#[derive(Clone, Debug)]
pub struct V5G0DurableArtifacts {
    pub attempt_journal: V5AttemptJournal,
    pub attempts: Vec<V5ProposalAttemptRecord>,
    pub outcome_audits: Vec<V5AttemptOutcomeAudit>,
    pub accepted_records: Vec<V5CompactAcceptedRecord>,
    pub compact_accepted_journal: V5CompactAcceptedJournal,
    pub attempt_proposal_deltas: Vec<Option<Value>>,
    pub accepted_proposal_deltas: Vec<Value>,
    pub identity_ledger: V5G0CompactIdentityLedger,
    pub schedule_state_receipt: V5G0ScheduleStateReceipt,
    pub accepted_pool: Option<Value>,
    pub campaign_ledger: Option<Value>,
    pub g0_selection: Option<Value>,
    pub selected_projection_index: Option<V5SelectedProjectionIndex>,
    pub publication_plan: V5G0PublicationPlan,
}

impl V5G0DurableArtifacts {
    #[allow(clippy::too_many_arguments)]
    pub fn from_canonical_values(
        attempt_journal: &Value,
        attempt_rows: &[Value],
        outcome_audits: &[Value],
        accepted_records: &[Value],
        compact_accepted_journal: &Value,
        attempt_proposal_deltas: &[Value],
        accepted_proposal_deltas: &[Value],
        identity_ledger: &Value,
        schedule_state_receipt: &Value,
        accepted_pool: Option<&Value>,
        campaign_ledger: Option<&Value>,
        g0_selection: Option<&Value>,
        selected_projection_index: Option<&Value>,
        publication_plan: &Value,
    ) -> Result<Self> {
        let attempt_journal = V5AttemptJournal::from_value(attempt_journal)?;
        let attempts = attempt_rows
            .iter()
            .map(V5ProposalAttemptRecord::from_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if attempts != attempt_journal.attempts {
            return Err(contract(
                "v5 durable attempt rows diverge from attempt journal root",
            ));
        }
        let outcome_audits = outcome_audits
            .iter()
            .map(V5AttemptOutcomeAudit::from_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let accepted_records = accepted_records
            .iter()
            .map(V5CompactAcceptedRecord::from_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let compact_accepted_journal =
            V5CompactAcceptedJournal::from_value(compact_accepted_journal)?;
        compact_accepted_journal.verify_records(&accepted_records)?;
        if attempts.len() != attempt_proposal_deltas.len() {
            return Err(contract(
                "v5 durable attempt rows/deltas have different counts",
            ));
        }
        let attempt_proposal_deltas = attempts
            .iter()
            .zip(attempt_proposal_deltas)
            .map(|(attempt, value)| match value {
                Value::Null => {
                    if attempt.proposal_delta_sha256.is_some() {
                        return Err(contract(
                            "v5 durable attempt delta is null despite a delta reference",
                        ));
                    }
                    Ok(None)
                }
                value => {
                    v5::validate_proposal_delta(value)?;
                    let delta_sha256 = exact_sha(
                        required(value, "deltaSha256", "v5 durable attempt proposal delta")?,
                        "v5 durable attempt proposal delta SHA-256",
                    )?;
                    if attempt.proposal_delta_sha256.as_deref() != Some(delta_sha256.as_str()) {
                        return Err(contract(
                            "v5 durable attempt delta identity diverges from attempt",
                        ));
                    }
                    Ok(Some(value.clone()))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        if accepted_records.len() != accepted_proposal_deltas.len() {
            return Err(contract(
                "v5 durable accepted records/deltas have different counts",
            ));
        }
        let accepted_proposal_deltas = accepted_proposal_deltas
            .iter()
            .map(|value| {
                v5::validate_proposal_delta(value)?;
                Ok(value.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        let identity_ledger = V5G0CompactIdentityLedger::from_value(identity_ledger)?;
        let schedule_state_receipt = V5G0ScheduleStateReceipt::from_value(schedule_state_receipt)?;
        let selected_projection_index = selected_projection_index
            .map(V5SelectedProjectionIndex::from_value)
            .transpose()?;
        let publication_plan =
            V5G0PublicationPlan::from_value(publication_plan).map_err(|error| {
                contract(format!("v5 durable publication plan is invalid: {error}"))
            })?;
        Ok(Self {
            attempt_journal,
            attempts,
            outcome_audits,
            accepted_records,
            compact_accepted_journal,
            attempt_proposal_deltas,
            accepted_proposal_deltas,
            identity_ledger,
            schedule_state_receipt,
            accepted_pool: accepted_pool.cloned(),
            campaign_ledger: campaign_ledger.cloned(),
            g0_selection: g0_selection.cloned(),
            selected_projection_index,
            publication_plan,
        })
    }

    pub fn into_verified_result(
        self,
        request: &V5G0TransactionRequest,
    ) -> Result<V5G0TransactionResult> {
        let target_reached = self.accepted_records.len() as u64 == request.target_accepted;
        let result = V5G0TransactionResult {
            generation_index: request.generation_index,
            generation_config_sha256: request.generation_config_sha256.clone(),
            shared_authority_sha256: self.attempt_journal.shared_authority_sha256.clone(),
            target_accepted: request.target_accepted,
            max_attempts: request.max_attempts,
            evaluation_width: request.evaluation_width,
            thread_cap: request.thread_cap,
            target_reached,
            stop_reason: if target_reached {
                "accepted_target_reached".to_owned()
            } else {
                "max_attempts_reached".to_owned()
            },
            attempts: self.attempts,
            outcome_audits: self.outcome_audits,
            attempt_journal: self.attempt_journal,
            accepted_records: self.accepted_records,
            compact_accepted_journal: self.compact_accepted_journal,
            attempt_proposal_deltas: self.attempt_proposal_deltas,
            accepted_proposal_deltas: self.accepted_proposal_deltas,
            identity_ledger: self.identity_ledger,
            schedule_state_receipt: self.schedule_state_receipt,
            accepted_pool: self.accepted_pool,
            campaign_ledger: self.campaign_ledger,
            g0_selection: self.g0_selection,
            selected_projection_index: self.selected_projection_index,
            publication_plan: self.publication_plan,
        };
        verify_v5_g0_transaction_replay(request, &result)?;
        Ok(result)
    }
}

/// Parse canonical durable values and run full sealed-authority compact
/// replay.  Batch uses this before publication/adoption, then separately
/// authenticates file bytes, object-store paths, and receipt ordering.
#[allow(clippy::too_many_arguments)]
pub fn reconstruct_v5_g0_transaction_from_artifacts(
    request: &V5G0TransactionRequest,
    attempt_journal: &Value,
    attempt_rows: &[Value],
    outcome_audits: &[Value],
    accepted_records: &[Value],
    compact_accepted_journal: &Value,
    attempt_proposal_deltas: &[Value],
    accepted_proposal_deltas: &[Value],
    identity_ledger: &Value,
    schedule_state_receipt: &Value,
    accepted_pool: Option<&Value>,
    campaign_ledger: Option<&Value>,
    g0_selection: Option<&Value>,
    selected_projection_index: Option<&Value>,
    publication_plan: &Value,
) -> Result<V5G0TransactionResult> {
    V5G0DurableArtifacts::from_canonical_values(
        attempt_journal,
        attempt_rows,
        outcome_audits,
        accepted_records,
        compact_accepted_journal,
        attempt_proposal_deltas,
        accepted_proposal_deltas,
        identity_ledger,
        schedule_state_receipt,
        accepted_pool,
        campaign_ledger,
        g0_selection,
        selected_projection_index,
        publication_plan,
    )?
    .into_verified_result(request)
}

/// Execute the native G0 construction transaction.  The only mutable state is
/// local and returned to the caller; qd-kernel performs no filesystem or
/// object-store writes.
pub fn execute_v5_g0_transaction(request: V5G0TransactionRequest) -> Result<V5G0TransactionResult> {
    request.validate_bounds()?;
    let authority = V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)?;
    V5G0TransactionRequest::validate_parsed_authority(&authority)?;
    let ledger_identity = g0_ledger_identity(&request, &authority)?;
    let mut ledger = CandidateIdentityLedger::new(ledger_identity, Vec::<String>::new())?;
    execute_with_ledger(request, authority, &mut ledger)
}

fn g0_ledger_identity(
    request: &V5G0TransactionRequest,
    authority: &V5SharedConstructionAuthority,
) -> Result<Value> {
    g0_compact_ledger_identity(&V5G0CompactIdentityLedger {
        generation_index: request.generation_index,
        generation_config_sha256: request.generation_config_sha256.clone(),
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        candidate_identity_sha256s: BTreeSet::new(),
        executable_semantic_sha256s: BTreeSet::new(),
        pair_identity_sha256s: BTreeSet::new(),
        attempt_count: 0,
        accepted_count: 0,
        disposition_counts: BTreeMap::new(),
    })
}

/// Empty parent selector used only to prove that G0 cannot reach parent
/// selection.  `ProposalPlanner` is still used for every ordinal; requesting
/// a non-immigrant intent is a hard contract failure before construction.
#[derive(Default)]
struct G0NoParents;

impl ParentSelector for G0NoParents {
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
            Value::String("temporal_qd_v5_g0_no_parent_selector_v1".to_owned()),
        )])
    }

    fn restore_compact_state(&mut self, state: &Value) -> std::result::Result<(), ProposalError> {
        if state != &self.compact_state() {
            return Err(ProposalError::Contract(
                "G0 no-parent selector state drifted".to_owned(),
            ));
        }
        Ok(())
    }

    fn select(
        &mut self,
        _label: &str,
        _structural_selection_ordinal: u64,
    ) -> std::result::Result<ParentReference, ProposalError> {
        Err(ProposalError::ParentSelectorUnavailable)
    }
}

fn execute_with_ledger(
    request: V5G0TransactionRequest,
    authority: V5SharedConstructionAuthority,
    ledger: &mut dyn IdentityLedger,
) -> Result<V5G0TransactionResult> {
    request.validate_bounds()?;
    V5G0TransactionRequest::validate_parsed_authority(&authority)?;
    // This is deliberately derived after the sealed authority has been parsed
    // and before any candidate work starts.  No caller provides an opaque
    // plan, and every durable result below carries this same cap-free plan.
    let publication_plan = V5G0PublicationPlan::derive(&request, &authority)
        .map_err(|error| contract(format!("v5 G0 publication plan derivation failed: {error}")))?;
    let schedule = ProposalSchedule {
        config_sha256: request.generation_config_sha256.clone(),
        generation_index: request.generation_index,
        parent_schedule: None,
        desired_evaluated_offspring: 0,
        desired_evaluated_immigrants: request.target_accepted,
    };
    schedule.validate()?;
    let mut parents = G0NoParents;
    let mut planner = ProposalPlanner {
        schedule: schedule.clone(),
        parents: &mut parents,
    };
    let mut state = ProposalState::default();
    let mut attempts = Vec::new();
    let mut audits = Vec::new();
    let mut accepted_records = Vec::new();
    let mut attempt_deltas = Vec::new();
    let mut accepted_deltas = Vec::new();

    while accepted_records.len() as u64 != request.target_accepted
        && attempts.len() as u64 != request.max_attempts
    {
        let remaining = request
            .max_attempts
            .checked_sub(attempts.len() as u64)
            .ok_or_else(|| contract("v5 G0 attempt accounting underflowed"))?;
        let batch_count = remaining.min(request.thread_cap) as usize;
        let batch_start = state.next_proposal_ordinal;
        let provisional_birth = accepted_records.len() as u64;
        let constructed = construct_batch(
            &authority,
            request.generation_index,
            &request.generation_config_sha256,
            batch_start,
            provisional_birth,
            batch_count,
            request.thread_cap,
        )?;
        for mut material in constructed {
            if accepted_records.len() as u64 == request.target_accepted {
                break;
            }
            let planned = planner.plan_next(&mut state)?;
            let proposal_seed = validate_g0_planned_immigrant(
                &planned.intent,
                &request.generation_config_sha256,
                planned.proposal_ordinal,
            )?;
            if material.record.proposal_ordinal != planned.proposal_ordinal
                || material.record.proposal_seed != proposal_seed
            {
                return Err(contract(
                    "parallel G0 construction merged out of proposal ordinal order",
                ));
            }
            // A rejected candidate does not consume a birth ordinal.  Work in
            // the same bounded batch may have speculated a later birth; rebuild
            // that compact material before it can become durable.
            let birth_ordinal = accepted_records.len() as u64;
            if material.record.birth_ordinal != birth_ordinal {
                let rebuilt = build_v5_g0_accepted_material(
                    &authority,
                    request.generation_index,
                    birth_ordinal,
                    planned.proposal_ordinal,
                    &proposal_seed,
                )?;
                material = ConstructedMaterial {
                    proposal_delta: rebuilt.proposal_delta,
                    record: rebuilt.record,
                };
            }
            let (attempt, audit, accepted) =
                admit_compact_material(&request, &mut state, ledger, &material)?;
            // The ledger commit happens inside `admit_compact_material` after
            // its typed decision and before this common scheduler transition.
            state.observe_compact_attempt(
                attempt.proposal_ordinal,
                &attempt.origin_kind,
                &attempt.disposition,
                &attempt.attempt_sha256()?,
                accepted.as_ref(),
            )?;
            if accepted.is_some() {
                accepted_deltas.push(material.proposal_delta);
                attempt_deltas.push(Some(
                    accepted_deltas
                        .last()
                        .expect("accepted delta was just appended")
                        .clone(),
                ));
                accepted_records.push(material.record);
            } else {
                attempt_deltas.push(Some(material.proposal_delta));
            }
            audits.push(audit);
            attempts.push(attempt);
        }
    }

    let target_reached = accepted_records.len() as u64 == request.target_accepted;
    let attempt_journal = V5AttemptJournal {
        generation_index: request.generation_index,
        generation_config_sha256: request.generation_config_sha256.clone(),
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        attempts: attempts.clone(),
    };
    let _ = attempt_journal.to_value()?;
    let compact_accepted_journal = V5CompactAcceptedJournal {
        generation_index: request.generation_index,
        generation_config_sha256: request.generation_config_sha256.clone(),
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        ordered_record_sha256s: accepted_records
            .iter()
            .map(V5CompactAcceptedRecord::record_sha256)
            .collect::<std::result::Result<Vec<_>, _>>()?,
    };
    compact_accepted_journal.verify_records(&accepted_records)?;
    let identity_ledger = compact_identity_ledger(
        request.generation_index,
        &request.generation_config_sha256,
        &authority.shared_authority_sha256,
        &accepted_records,
        &attempts,
    )?;
    let schedule_state_receipt = V5G0ScheduleStateReceipt {
        generation_index: request.generation_index,
        generation_config_sha256: request.generation_config_sha256.clone(),
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        target_accepted: request.target_accepted,
        max_attempts: request.max_attempts,
        parent_schedule_sha256: None,
        accepted_by_origin: state.origin_accepted_counts.clone(),
        disposition_counts: state.disposition_counts.clone(),
        next_proposal_ordinal: state.next_proposal_ordinal,
        structural_parent_selections: state.structural_parent_selections,
        crossover_attempts: 0,
        structural_parent_draws: 0,
        proposal_state: state.compact_value(),
    };
    let _ = schedule_state_receipt.to_value()?;
    let (accepted_pool, campaign_ledger, g0_selection, selected_projection_index) =
        if target_reached {
            let products = g0_products(&request, &authority, &accepted_records)?;
            (
                Some(products.0),
                Some(products.1),
                Some(products.2),
                Some(products.3),
            )
        } else {
            (None, None, None, None)
        };
    let result = V5G0TransactionResult {
        generation_index: request.generation_index,
        generation_config_sha256: request.generation_config_sha256.clone(),
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        target_accepted: request.target_accepted,
        max_attempts: request.max_attempts,
        evaluation_width: request.evaluation_width,
        thread_cap: request.thread_cap,
        target_reached,
        stop_reason: if target_reached {
            "accepted_target_reached".to_owned()
        } else {
            "max_attempts_reached".to_owned()
        },
        attempts,
        outcome_audits: audits,
        attempt_journal,
        accepted_records,
        compact_accepted_journal,
        attempt_proposal_deltas: attempt_deltas,
        accepted_proposal_deltas: accepted_deltas,
        identity_ledger,
        schedule_state_receipt,
        accepted_pool,
        campaign_ledger,
        g0_selection,
        selected_projection_index,
        publication_plan,
    };
    result.verify_replay(&authority)?;
    result
        .publication_plan
        .validate_against_request(&request, &authority)
        .map_err(|error| {
            contract(format!(
                "v5 G0 derived publication plan replay failed: {error}"
            ))
        })?;
    Ok(result)
}

struct ConstructedMaterial {
    proposal_delta: Value,
    record: V5CompactAcceptedRecord,
}

fn construct_batch(
    authority: &V5SharedConstructionAuthority,
    generation_index: u64,
    generation_config_sha256: &str,
    proposal_start: u64,
    provisional_birth_start: u64,
    count: usize,
    thread_cap: u64,
) -> Result<Vec<ConstructedMaterial>> {
    let jobs = (0..count)
        .map(|offset| {
            let proposal_ordinal = proposal_start
                .checked_add(offset as u64)
                .ok_or_else(|| contract("v5 G0 proposal ordinal overflowed"))?;
            let birth_ordinal = provisional_birth_start
                .checked_add(offset as u64)
                .ok_or_else(|| contract("v5 G0 birth ordinal overflowed"))?;
            Ok((
                proposal_ordinal,
                birth_ordinal,
                v5::v5_proposal_seed(generation_config_sha256, proposal_ordinal)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let workers = usize::min(thread_cap as usize, jobs.len()).max(1);
    let chunk_size = jobs.len().div_ceil(workers);
    let joined = thread::scope(|scope| {
        let mut handles = Vec::new();
        for chunk in jobs.chunks(chunk_size) {
            handles.push(scope.spawn(move || {
                chunk
                    .iter()
                    .map(|(ordinal, birth, seed)| {
                        let material = build_v5_g0_accepted_material(
                            authority,
                            generation_index,
                            *birth,
                            *ordinal,
                            seed,
                        )?;
                        Ok::<_, V5Error>((
                            *ordinal,
                            ConstructedMaterial {
                                proposal_delta: material.proposal_delta,
                                record: material.record,
                            },
                        ))
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().map_err(|_| V5G0TransactionError::WorkerPanic))
            .collect::<Result<Vec<_>>>()
    })?;
    let mut flattened = Vec::with_capacity(count);
    for batch in joined {
        for item in batch.map_err(V5G0TransactionError::from)? {
            flattened.push(item);
        }
    }
    flattened.sort_by_key(|(ordinal, _)| *ordinal);
    if flattened
        .iter()
        .enumerate()
        .any(|(offset, (ordinal, _))| *ordinal != proposal_start + offset as u64)
    {
        return Err(contract("parallel G0 construction result order drifted"));
    }
    Ok(flattened
        .into_iter()
        .map(|(_, material)| material)
        .collect())
}

fn validate_g0_planned_immigrant(
    intent: &ProposalIntent,
    generation_config_sha256: &str,
    proposal_ordinal: u64,
) -> Result<String> {
    let ProposalIntent::RichImmigrant {
        proposal_seed,
        long_seed,
        short_seed,
    } = intent
    else {
        return Err(contract(
            "ProposalPlanner selected a non-immigrant during G0",
        ));
    };
    let expected_seed = v5::v5_proposal_seed(generation_config_sha256, proposal_ordinal)?;
    if proposal_seed != &expected_seed
        || long_seed != &immigrant_side_seed(proposal_seed, Side::Long)
        || short_seed != &immigrant_side_seed(proposal_seed, Side::Short)
    {
        return Err(contract("ProposalPlanner G0 intent seed binding drifted"));
    }
    // The v5 factory derives distinct native side seeds internally from this
    // same proposal seed.  Force both derivations at the planner/constructor
    // seam so a future builder cannot accidentally change its side namespace
    // while still accepting the common proposal ordinal.
    let _ = v5::side_seed(proposal_seed, "long")?;
    let _ = v5::side_seed(proposal_seed, "short")?;
    Ok(proposal_seed.clone())
}

fn compact_candidate(record: &V5CompactAcceptedRecord) -> Value {
    object([
        ("candidateId", Value::String(record.candidate_id.clone())),
        (
            "candidateIdentitySha256",
            Value::String(record.candidate_identity_sha256.clone()),
        ),
        (
            "pairIdentitySha256",
            Value::String(record.pair_identity_sha256.clone()),
        ),
        (
            "executableSemanticSha256",
            Value::String(record.executable_semantic_sha256.clone()),
        ),
    ])
}

fn admit_compact_material(
    request: &V5G0TransactionRequest,
    state: &mut ProposalState,
    ledger: &mut dyn IdentityLedger,
    material: &ConstructedMaterial,
) -> Result<(
    V5ProposalAttemptRecord,
    V5AttemptOutcomeAudit,
    Option<AcceptedProposal>,
)> {
    let record = &material.record;
    let candidate = compact_candidate(record);
    let accepted = AcceptedProposal {
        candidate_id: record.candidate_id.clone(),
        candidate_identity_sha256: record.candidate_identity_sha256.clone(),
        executable_semantic_sha256: record.executable_semantic_sha256.clone(),
        descriptor_projection: Some(record.descriptor_projection.clone()),
    };
    let (tentative, local_reason, local_effect) = if state
        .local_executable_semantics
        .contains(&accepted.executable_semantic_sha256)
    {
        (
            "duplicate_pair_genome",
            "duplicate_pair_genome",
            "duplicate_executable",
        )
    } else if state
        .local_candidate_identities
        .contains(&accepted.candidate_identity_sha256)
    {
        (
            "duplicate_candidate_identity",
            "duplicate_candidate_identity",
            "duplicate_candidate",
        )
    } else {
        ("accepted", "accepted", "inserted")
    };
    // This decision is intentionally made on the compact candidate surface,
    // before `observe_compact_attempt`.  It is the same injected ledger seam
    // used by the existing proposal path, without fabricating a rich entry.
    let decision = ledger.prepare_proposal(LedgerProposal {
        proposal_ordinal: record.proposal_ordinal,
        candidate: Some(&candidate),
        executable_semantic_sha256: Some(&record.executable_semantic_sha256),
        tentative_disposition: tentative,
    })?;
    let (disposition, reason_code, stage, ledger_effect, accepted_ref) = if tentative == "accepted"
    {
        match decision.disposition.as_str() {
            "accepted" => (
                "accepted",
                "accepted",
                "accepted",
                "inserted",
                Some(accepted),
            ),
            "duplicate_candidate_identity_global" => (
                "rejected",
                "duplicate_candidate_identity_global",
                "identity_ledger",
                "duplicate_candidate",
                None,
            ),
            other => {
                return Err(contract(format!(
                    "v5 G0 identity ledger returned unsupported disposition {other}"
                )));
            }
        }
    } else {
        if decision.disposition != tentative {
            return Err(contract(
                "v5 G0 identity ledger changed a local duplicate disposition",
            ));
        }
        ("rejected", local_reason, "admission", local_effect, None)
    };
    ledger.commit_prepared_delta(&decision.prepared_delta)?;
    let lineage_refs = V5AttemptLineageRefs {
        parent: None,
        mate: None,
        parent_selection_receipt_sha256: None,
        operator_plan_sha256: None,
        operator_application_sha256: None,
        operator_trace_sha256: None,
        step_index: None,
    };
    let lineage_refs_sha256 = canonical_sha256(&lineage_refs.to_value()?)?;
    let accepted_record_sha256 = if disposition == "accepted" {
        Some(record.record_sha256()?)
    } else {
        None
    };
    let audit = V5AttemptOutcomeAudit {
        generation_index: request.generation_index,
        proposal_ordinal: record.proposal_ordinal,
        generation_config_sha256: request.generation_config_sha256.clone(),
        shared_authority_sha256: record.shared_authority_sha256.clone(),
        proposal_seed: record.proposal_seed.clone(),
        origin_kind: "random_immigrant".to_owned(),
        disposition: disposition.to_owned(),
        reason_code: reason_code.to_owned(),
        stage: stage.to_owned(),
        proposal_delta_sha256: Some(record.proposal_delta_sha256.clone()),
        lineage_refs_sha256,
        identity_ledger_effect: ledger_effect.to_owned(),
        accepted_record_sha256: accepted_record_sha256.clone(),
    };
    let attempt = V5ProposalAttemptRecord {
        generation_index: request.generation_index,
        proposal_ordinal: record.proposal_ordinal,
        generation_config_sha256: request.generation_config_sha256.clone(),
        shared_authority_sha256: record.shared_authority_sha256.clone(),
        proposal_seed: record.proposal_seed.clone(),
        origin_kind: "random_immigrant".to_owned(),
        proposal_delta_sha256: Some(record.proposal_delta_sha256.clone()),
        disposition: disposition.to_owned(),
        reason_code: reason_code.to_owned(),
        lineage_refs,
        identity_ledger_effect: ledger_effect.to_owned(),
        outcome_audit_sha256: audit.audit_sha256()?,
        accepted_record_sha256,
    };
    audit.verify_binds_attempt(&attempt)?;
    Ok((attempt, audit, accepted_ref))
}

fn compact_identity_ledger(
    generation_index: u64,
    generation_config_sha256: &str,
    shared_authority_sha256: &str,
    records: &[V5CompactAcceptedRecord],
    attempts: &[V5ProposalAttemptRecord],
) -> Result<V5G0CompactIdentityLedger> {
    let candidate_identity_sha256s = records
        .iter()
        .map(|record| record.candidate_identity_sha256.clone())
        .collect::<BTreeSet<_>>();
    let executable_semantic_sha256s = records
        .iter()
        .map(|record| record.executable_semantic_sha256.clone())
        .collect::<BTreeSet<_>>();
    let pair_identity_sha256s = records
        .iter()
        .map(|record| record.pair_identity_sha256.clone())
        .collect::<BTreeSet<_>>();
    let mut disposition_counts = BTreeMap::new();
    for attempt in attempts {
        *disposition_counts
            .entry(attempt.disposition.clone())
            .or_insert(0_u64) += 1;
    }
    let ledger = V5G0CompactIdentityLedger {
        generation_index,
        generation_config_sha256: generation_config_sha256.to_owned(),
        shared_authority_sha256: shared_authority_sha256.to_owned(),
        candidate_identity_sha256s,
        executable_semantic_sha256s,
        pair_identity_sha256s,
        attempt_count: attempts.len() as u64,
        accepted_count: records.len() as u64,
        disposition_counts,
    };
    let _ = ledger.to_value()?;
    Ok(ledger)
}

fn verify_identity_ledger(result: &V5G0TransactionResult) -> Result<()> {
    let expected = compact_identity_ledger(
        result.generation_index,
        &result.generation_config_sha256,
        &result.shared_authority_sha256,
        &result.accepted_records,
        &result.attempts,
    )?;
    if result.identity_ledger != expected {
        return Err(contract(
            "v5 G0 compact identity ledger diverges from attempts/records",
        ));
    }
    Ok(())
}

/// Prove that an audit/attempt's compact-delta reference always resolves to a
/// real canonical value.  Accepted deltas are intentionally retained both in
/// the proposal-ordinal journal below and in the birth-ordinal accepted list;
/// this cross-check prevents either view from being silently substituted.
fn verify_attempt_proposal_deltas(result: &V5G0TransactionResult) -> Result<()> {
    if result.attempts.len() != result.attempt_proposal_deltas.len() {
        return Err(contract("v5 G0 attempt delta journal length drifted"));
    }
    let mut by_ordinal = BTreeMap::<u64, &Value>::new();
    for (attempt, delta) in result.attempts.iter().zip(&result.attempt_proposal_deltas) {
        let (Some(expected_sha256), Some(delta)) = (&attempt.proposal_delta_sha256, delta) else {
            if attempt.proposal_delta_sha256.is_some() || delta.is_some() {
                return Err(contract(
                    "v5 G0 attempt delta nullability diverges from its attempt",
                ));
            }
            continue;
        };
        v5::validate_proposal_delta(delta)?;
        let actual_sha256 = exact_sha(
            required(delta, "deltaSha256", "v5 G0 attempt proposal delta")?,
            "v5 G0 attempt proposal delta SHA-256",
        )?;
        if &actual_sha256 != expected_sha256
            || delta.get("proposalOrdinal").and_then(Value::as_u64)
                != Some(attempt.proposal_ordinal)
            || delta.get("proposalSeed").and_then(Value::as_str)
                != Some(attempt.proposal_seed.as_str())
        {
            return Err(contract("v5 G0 attempt delta does not bind its attempt"));
        }
        if by_ordinal.insert(attempt.proposal_ordinal, delta).is_some() {
            return Err(contract(
                "v5 G0 attempt delta proposal ordinal is duplicated",
            ));
        }
    }
    for (record, accepted_delta) in result
        .accepted_records
        .iter()
        .zip(&result.accepted_proposal_deltas)
    {
        v5::validate_proposal_delta(accepted_delta)?;
        let attempted = by_ordinal
            .get(&record.proposal_ordinal)
            .ok_or_else(|| contract("v5 G0 accepted record has no durable attempt delta"))?;
        let accepted_sha256 = exact_sha(
            required(
                accepted_delta,
                "deltaSha256",
                "v5 G0 accepted proposal delta",
            )?,
            "v5 G0 accepted proposal delta SHA-256",
        )?;
        if accepted_delta != *attempted || accepted_sha256 != record.proposal_delta_sha256 {
            return Err(contract(
                "v5 G0 accepted delta diverges from its durable attempt/object",
            ));
        }
    }
    Ok(())
}

fn verify_schedule_state_receipt(result: &V5G0TransactionResult) -> Result<()> {
    let receipt = &result.schedule_state_receipt;
    let state = ProposalState::from_compact_value(&receipt.proposal_state)?;
    if receipt.generation_index != result.generation_index
        || receipt.generation_config_sha256 != result.generation_config_sha256
        || receipt.shared_authority_sha256 != result.shared_authority_sha256
        || receipt.target_accepted != result.target_accepted
        || receipt.max_attempts != result.max_attempts
        || receipt.parent_schedule_sha256.is_some()
        || receipt.next_proposal_ordinal != result.attempts.len() as u64
        || receipt.next_proposal_ordinal != state.next_proposal_ordinal
        || receipt.structural_parent_selections != 0
        || receipt.structural_parent_draws != 0
        || receipt.crossover_attempts != 0
        || state.structural_parent_selections != 0
        || receipt.accepted_by_origin != state.origin_accepted_counts
        || receipt.disposition_counts != state.disposition_counts
        || state.entry_sha256s
            != result
                .attempts
                .iter()
                .map(V5ProposalAttemptRecord::attempt_sha256)
                .collect::<std::result::Result<Vec<_>, _>>()?
    {
        return Err(contract(
            "v5 G0 schedule/state receipt diverges from attempt replay",
        ));
    }
    if receipt
        .accepted_by_origin
        .get("random_immigrant")
        .copied()
        .unwrap_or_default()
        != result.accepted_records.len() as u64
    {
        return Err(contract("v5 G0 schedule receipt acceptedByOrigin drifted"));
    }
    let _ = receipt.to_value()?;
    Ok(())
}

fn construction_pool_identity(request: &V5G0TransactionRequest) -> Result<String> {
    Ok(canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String("temporal_qd_g0_construction_identity_v1".to_owned()),
        ),
        (
            "configSha256",
            Value::String(request.generation_config_sha256.clone()),
        ),
        ("generationIndex", Value::from(request.generation_index)),
        ("constructionPoolSize", Value::from(request.target_accepted)),
        (
            "evaluationPopulationSize",
            Value::from(request.evaluation_width),
        ),
    ]))?)
}

fn g0_products(
    request: &V5G0TransactionRequest,
    authority: &V5SharedConstructionAuthority,
    records: &[V5CompactAcceptedRecord],
) -> Result<(Value, Value, Value, V5SelectedProjectionIndex)> {
    let construction_pool_identity_sha256 = construction_pool_identity(request)?;
    // The accepted-pool semantic identity is defined over reference-SHA order
    // (see `g0::pool_material`).  Persist that same order in the durable
    // object so its content-addressed JSON has one canonical byte/value
    // representation instead of merely a canonical semantic hash.
    let mut references = records
        .iter()
        .map(|record| {
            let record_sha256 = record.record_sha256().map_err(V5G0TransactionError::from)?;
            let admitted = AdmittedAcceptedPairEntry {
                entry_sha256: record_sha256.clone(),
                proposal_ordinal: record.proposal_ordinal,
                generation_index: record.generation_index,
                birth_ordinal: record.birth_ordinal,
                candidate_id: record.candidate_id.clone(),
                candidate_identity_sha256: record.candidate_identity_sha256.clone(),
                executable_semantic_sha256: record.executable_semantic_sha256.clone(),
                descriptor_projection: record.descriptor_projection.clone(),
            };
            let reference = project_admitted_pair_entry(
                &construction_pool_identity_sha256,
                &compact_record_object_relative_path(&record_sha256)?,
                &admitted,
            )?;
            let reference_sha256 = exact_sha(
                reference
                    .get("referenceSha256")
                    .ok_or_else(|| contract("constructed G0 reference lacks reference SHA-256"))?,
                "constructed G0 reference SHA-256",
            )?;
            Ok((reference_sha256, record, reference))
        })
        .collect::<Result<Vec<_>>>()?;
    references.sort_by(|(left, _, _), (right, _, _)| left.cmp(right));
    let record_by_reference = references
        .iter()
        .map(|(reference_sha, record, _)| (reference_sha.clone(), *record))
        .collect::<BTreeMap<_, _>>();
    let references = references
        .into_iter()
        .map(|(_, _, reference)| reference)
        .collect::<Vec<_>>();
    let pool = build_accepted_pool(&construction_pool_identity_sha256, &references)?;
    let selection = select_g0_bootstrap(&pool, request.evaluation_width, None)?;
    let selected = selection
        .get("selected")
        .and_then(Value::as_array)
        .ok_or_else(|| contract("constructed G0 selection lacks selected rows"))?;
    let selected_hashes = selected
        .iter()
        .map(|row| {
            exact_sha(
                row.get("referenceSha256").ok_or_else(|| {
                    contract("constructed G0 selection row lacks reference SHA-256")
                })?,
                "constructed G0 selected reference SHA-256",
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let ledger = materialize_campaign_ledger(&pool, &selected_hashes)?;
    let projections = selected_hashes
        .iter()
        .map(|reference_sha| {
            record_by_reference
                .get(reference_sha)
                .ok_or_else(|| contract("G0 selection names a foreign compact reference"))?
                .selected_projection()
                .map_err(V5G0TransactionError::from)
        })
        .collect::<Result<Vec<_>>>()?;
    let index = V5SelectedProjectionIndex {
        generation_index: request.generation_index,
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        accepted_pool_sha256: exact_sha(
            pool.get("acceptedPoolSha256")
                .ok_or_else(|| contract("constructed G0 pool lacks SHA-256"))?,
            "constructed G0 pool SHA-256",
        )?,
        selection_sha256: exact_sha(
            selection
                .get("selectionSha256")
                .ok_or_else(|| contract("constructed G0 selection lacks SHA-256"))?,
            "constructed G0 selection SHA-256",
        )?,
        projections,
    };
    let _ = index.to_value()?;
    Ok((pool, ledger, selection, index))
}

fn verify_selected_projection_index(
    result: &V5G0TransactionResult,
    pool: &Value,
    selection: &Value,
    index: &V5SelectedProjectionIndex,
    selected_hashes: &[String],
) -> Result<()> {
    let pool_sha = exact_sha(
        pool.get("acceptedPoolSha256")
            .ok_or_else(|| contract("v5 G0 pool lacks acceptedPoolSha256"))?,
        "v5 G0 pool acceptedPoolSha256",
    )?;
    let selection_sha = exact_sha(
        selection
            .get("selectionSha256")
            .ok_or_else(|| contract("v5 G0 selection lacks selectionSha256"))?,
        "v5 G0 selection selectionSha256",
    )?;
    if index.generation_index != result.generation_index
        || index.shared_authority_sha256 != result.shared_authority_sha256
        || index.accepted_pool_sha256 != pool_sha
        || index.selection_sha256 != selection_sha
        || index.projections.len() != selected_hashes.len()
    {
        return Err(contract("v5 selected projection index binding drifted"));
    }
    let by_record = result
        .accepted_records
        .iter()
        .map(|record| Ok((record.record_sha256()?, record)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let references = pool
        .get("acceptedReferences")
        .and_then(Value::as_array)
        .ok_or_else(|| contract("v5 G0 accepted pool lacks references"))?;
    let by_reference = references
        .iter()
        .map(|reference| {
            Ok((
                exact_sha(
                    reference
                        .get("referenceSha256")
                        .ok_or_else(|| contract("v5 G0 reference lacks SHA-256"))?,
                    "v5 G0 reference SHA-256",
                )?,
                exact_sha(
                    reference
                        .get("acceptedPairEntrySha256")
                        .ok_or_else(|| contract("v5 G0 reference lacks record SHA-256"))?,
                    "v5 G0 reference record SHA-256",
                )?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    for (projection, selected_sha) in index.projections.iter().zip(selected_hashes) {
        let record_sha = by_reference
            .get(selected_sha)
            .ok_or_else(|| contract("v5 selected projection names foreign selection reference"))?;
        let record = by_record
            .get(record_sha)
            .ok_or_else(|| contract("v5 selected projection lacks compact record"))?;
        projection.verify_against_record(record)?;
    }
    let _ = index.to_value()?;
    Ok(())
}

fn verify_compact_record_object_paths(pool: &Value) -> Result<()> {
    let references = pool
        .get("acceptedReferences")
        .and_then(Value::as_array)
        .ok_or_else(|| contract("v5 G0 accepted pool lacks references"))?;
    for reference in references {
        let record_sha = exact_sha(
            reference
                .get("acceptedPairEntrySha256")
                .ok_or_else(|| contract("v5 G0 accepted reference lacks record SHA-256"))?,
            "v5 G0 accepted reference record SHA-256",
        )?;
        let journal = reference
            .get("journalReference")
            .and_then(Value::as_object)
            .ok_or_else(|| contract("v5 G0 accepted reference lacks journal reference"))?;
        if journal.get("entrySha256").and_then(Value::as_str) != Some(record_sha.as_str())
            || journal.get("journalRelativePath").and_then(Value::as_str)
                != Some(compact_record_object_relative_path(&record_sha)?.as_str())
        {
            return Err(contract(
                "v5 G0 accepted reference does not resolve to its compact record object",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        io::{Cursor, Read, Write},
    };

    use flate2::read::GzDecoder;

    use super::*;
    use crate::v5_g0_funnel::{
        V5_G0_FUNNEL_PROJECTION_STREAM_PATH, V5G0FunnelFragmentReceiptObjectBinding,
        V5G0FunnelFragments, V5G0FunnelProjectionKind, V5G0FunnelProjectionSink,
        V5G0FunnelProjectionStreamReceipt, V5G0FunnelProjectionStreamReceiptObjectBinding,
        build_v5_g0_funnel_fragments, stream_v5_g0_funnel_projections,
        verify_v5_g0_funnel_fragment_receipt, verify_v5_g0_funnel_projection_stream,
        write_v5_g0_funnel_projection_stream,
    };
    use crate::v5_publication::{
        V5G0PublicationFragmentKind, V5G0PublicationFragmentSink, V5G0PublicationFragmentSource,
        prepare_v5_g0_publication_stream, verify_v5_g0_publication_adoption,
    };

    #[derive(Default)]
    struct InMemoryFragments {
        bytes: BTreeMap<V5G0PublicationFragmentKind, Vec<u8>>,
    }

    impl V5G0PublicationFragmentSink for InMemoryFragments {
        fn write_fragment(
            &mut self,
            kind: V5G0PublicationFragmentKind,
            canonical_bytes: &[u8],
        ) -> std::io::Result<()> {
            self.bytes
                .entry(kind)
                .or_default()
                .write_all(canonical_bytes)
        }
    }

    impl V5G0PublicationFragmentSource for InMemoryFragments {
        fn copy_fragment(
            &mut self,
            kind: V5G0PublicationFragmentKind,
            output: &mut dyn Write,
        ) -> std::io::Result<()> {
            output.write_all(self.bytes.get(&kind).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "test publication fragment is absent",
                )
            })?)
        }
    }

    #[derive(Default)]
    struct InMemoryFunnelProjections {
        bytes: BTreeMap<V5G0FunnelProjectionKind, Vec<u8>>,
    }

    impl V5G0FunnelProjectionSink for InMemoryFunnelProjections {
        fn write_projection(
            &mut self,
            kind: V5G0FunnelProjectionKind,
            canonical_bytes: &[u8],
        ) -> std::io::Result<()> {
            self.bytes
                .entry(kind)
                .or_default()
                .write_all(canonical_bytes)
        }
    }

    impl InMemoryFunnelProjections {
        fn rows(&self, kind: V5G0FunnelProjectionKind) -> Vec<Value> {
            let bytes = self.bytes.get(&kind).expect("funnel projection exists");
            let mut framed = Vec::with_capacity(bytes.len() + 2);
            framed.push(b'[');
            framed.extend_from_slice(bytes);
            framed.push(b']');
            serde_json::from_slice(&framed).expect("parse canonical funnel projection rows")
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
            .expect("decompress v5 shared-authority fixture");
        let fixture: Value =
            serde_json::from_slice(&payload).expect("parse v5 shared-authority fixture");
        fixture
            .get("sealedAuthority")
            .cloned()
            .expect("fixture has sealed authority")
    }

    fn fixture_generation_config(target_accepted: u64, max_attempts: u64) -> Value {
        let shared = shared_authority_fixture();
        let authority = shared
            .get("authority")
            .expect("shared authority has payload");
        let evolvable = authority
            .get("evolvableModuleAuthority")
            .expect("shared authority has evolvable authority");
        let archive = evolvable
            .get("archivePolicyAuthority")
            .expect("evolvable authority has archive policy")
            .clone();
        let behavior = evolvable
            .get("behaviorAttributionRequirement")
            .expect("evolvable authority has behavior requirement")
            .clone();
        let behavior_sha256 = behavior
            .get("requirementSha256")
            .and_then(Value::as_str)
            .expect("behavior requirement has identity")
            .to_owned();
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
                    .expect("evolvable registry")
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
                Value::String(canonical_sha256(&archive).expect("archive identity")),
            ),
            (
                "behaviorAttributionRequirementSha256",
                Value::String(behavior_sha256),
            ),
        ]);
        if let Some(receipt) = evolvable.get("capacityReceipt") {
            operator
                .as_object_mut()
                .expect("operator implementation is object")
                .insert(
                    "capacityReceiptSha256".to_owned(),
                    receipt
                        .get("semanticReceiptSha256")
                        .expect("capacity receipt semantic identity")
                        .clone(),
                );
        }
        let operator_sha256 =
            canonical_sha256(&operator).expect("operator implementation identity");
        operator
            .as_object_mut()
            .expect("operator implementation is object")
            .insert(
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
            .expect("allocation is object")
            .clone();
        allocation.insert(
            "allocationSha256".to_owned(),
            Value::String(canonical_sha256(&allocation_semantic).expect("allocation identity")),
        );
        // Match the canonical `build_pair_generation_config` layout: the
        // immutable allocation and enriched operator identity are top-level,
        // while executable archive/behavior/capacity facts remain inside the
        // original generation run closure.
        let mut run_config = object([
            ("archivePolicyAuthority", archive.clone()),
            ("behaviorAttributionRequirement", behavior.clone()),
            ("operatorImplementation", operator.clone()),
        ]);
        if let Some(receipt) = evolvable.get("capacityReceipt") {
            run_config
                .as_object_mut()
                .expect("generation run config is object")
                .insert("capacityReceipt".to_owned(), receipt.clone());
        }
        let mut config = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_pair_generation_v2".to_owned()),
            ),
            ("generationIndex", Value::from(1_u64)),
            ("targetUniqueCandidates", Value::from(target_accepted)),
            ("maxProposalAttempts", Value::from(max_attempts)),
            ("reproductionAllocation", Value::Object(allocation)),
            ("runConfig", run_config),
            ("operatorImplementation", operator),
        ]);
        let config_sha256 = canonical_sha256(&config).expect("generation config identity");
        config
            .as_object_mut()
            .expect("generation config is object")
            .insert("configSha256".to_owned(), Value::String(config_sha256));
        config
    }

    fn request(thread_cap: u64, target_accepted: u64, max_attempts: u64) -> V5G0TransactionRequest {
        let generation_config = fixture_generation_config(target_accepted, max_attempts);
        let generation_config_sha256 = generation_config
            .get("configSha256")
            .and_then(Value::as_str)
            .expect("fixture config identity")
            .to_owned();
        V5G0TransactionRequest {
            shared_authority: shared_authority_fixture(),
            generation_config,
            generation_config_sha256,
            generation_index: 1,
            target_accepted,
            max_attempts,
            evaluation_width: 1,
            thread_cap,
            publication_inputs: V5G0PublicationInputs {
                final_newline: "lf".to_owned(),
                execution_authority: Value::Object(Map::new()),
                inputs: object([
                    (
                        "schemaVersion",
                        Value::String("temporal_qd_native_v5_proposal_inputs_v1".to_owned()),
                    ),
                    ("parentArchive", Value::Null),
                    ("identityLedger", Value::Null),
                ]),
            },
        }
    }

    /// Mutate a fixture config exactly as an attacker could: preserve the
    /// outer canonical self-hash after changing an inner authority value.
    /// Publication validation must still reject drift against the separately
    /// sealed shared authority.
    fn reseal_generation_config(request: &mut V5G0TransactionRequest) {
        request
            .generation_config
            .as_object_mut()
            .expect("fixture generation config is object")
            .remove("configSha256")
            .expect("fixture generation config has identity");
        let config_sha256 = canonical_sha256(&request.generation_config)
            .expect("recompute fixture generation config identity");
        request
            .generation_config
            .as_object_mut()
            .expect("fixture generation config is object")
            .insert(
                "configSha256".to_owned(),
                Value::String(config_sha256.clone()),
            );
        request.generation_config_sha256 = config_sha256;
    }

    fn values<T>(
        items: &[T],
        encode: impl Fn(&T) -> std::result::Result<Value, V5Error>,
    ) -> Vec<Value> {
        items
            .iter()
            .map(encode)
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("encode typed compact rows")
    }

    #[test]
    fn v5_g0_transaction_replays_typed_artifacts_and_real_record_paths() {
        let request = request(2, 2, 2);
        let result =
            execute_v5_g0_transaction(request.clone()).expect("execute native v5 G0 transaction");

        assert!(result.target_reached);
        assert_eq!(result.attempts.len(), 2);
        assert_eq!(result.accepted_records.len(), 2);
        assert_eq!(
            result.schedule_state_receipt.parent_schedule_sha256, None,
            "G0 has no parent schedule"
        );
        assert_eq!(
            result.schedule_state_receipt.structural_parent_selections,
            0
        );
        assert_eq!(result.schedule_state_receipt.structural_parent_draws, 0);
        assert_eq!(result.schedule_state_receipt.crossover_attempts, 0);
        assert_eq!(
            result
                .schedule_state_receipt
                .accepted_by_origin
                .get("random_immigrant"),
            Some(&2),
        );
        assert_eq!(
            result.compact_accepted_journal.ordered_record_sha256s.len(),
            result.accepted_records.len(),
        );

        verify_v5_g0_transaction_replay(&request, &result).expect("typed full replay");

        let bindings = result
            .compact_record_object_bindings()
            .expect("derive compact object bindings");
        assert_eq!(bindings.len(), 2);
        for binding in bindings {
            assert_eq!(
                binding.relative_path,
                compact_record_object_relative_path(&binding.record_sha256)
                    .expect("derive expected compact object path"),
            );
            assert!(
                binding
                    .relative_path
                    .starts_with("v5-native/objects/sha256/")
            );
            assert!(binding.relative_path.ends_with(".json"));
        }

        let attempt_proposal_deltas = result
            .attempt_proposal_deltas
            .iter()
            .cloned()
            .map(|delta| delta.unwrap_or(Value::Null))
            .collect::<Vec<_>>();
        let publication_plan = result
            .publication_plan
            .to_value()
            .expect("publication plan");
        let reconstructed = reconstruct_v5_g0_transaction_from_artifacts(
            &request,
            &result.attempt_journal.to_value().expect("attempt journal"),
            &values(&result.attempts, V5ProposalAttemptRecord::to_value),
            &values(&result.outcome_audits, V5AttemptOutcomeAudit::to_value),
            &values(&result.accepted_records, V5CompactAcceptedRecord::to_value),
            &result
                .compact_accepted_journal
                .to_value()
                .expect("compact accepted journal"),
            &attempt_proposal_deltas,
            &result.accepted_proposal_deltas,
            &result.identity_ledger.to_value().expect("identity ledger"),
            &result
                .schedule_state_receipt
                .to_value()
                .expect("schedule receipt"),
            result.accepted_pool.as_ref(),
            result.campaign_ledger.as_ref(),
            result.g0_selection.as_ref(),
            result
                .selected_projection_index
                .as_ref()
                .map(|index| index.to_value().expect("projection index"))
                .as_ref(),
            &publication_plan,
        )
        .expect("reconstruct canonical durable artifacts");
        assert_eq!(
            reconstructed.to_value().expect("reconstructed result"),
            result.to_value().expect("original result"),
        );
    }

    #[test]
    fn v5_g0_compact_ledger_restores_the_core_owned_g2_runtime_state() {
        let request = request(1, 2, 2);
        let result =
            execute_v5_g0_transaction(request.clone()).expect("execute native v5 G0 transaction");
        let restored = result
            .identity_ledger
            .restore_candidate_identity_ledger()
            .expect("restore G0 compact ledger for first evolved generation");
        let authority =
            V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)
                .expect("parse G0 shared authority");
        assert_eq!(
            restored.identity(),
            &g0_ledger_identity(&request, &authority).expect("derive G0 ledger authority"),
        );
        assert_eq!(
            restored.compact_state().get("candidateIdentities"),
            Some(&sha_vec_value(
                &result.identity_ledger.candidate_identity_sha256s
            )),
        );
        assert_eq!(
            restored
                .compact_state()
                .get("proposalCount")
                .and_then(Value::as_u64),
            Some(result.identity_ledger.attempt_count),
        );
    }

    #[test]
    fn v5_g0_publication_rejects_self_rehashed_nested_run_config_drift() {
        let mut archive_drift = request(1, 1, 1);
        archive_drift
            .generation_config
            .get_mut("runConfig")
            .and_then(Value::as_object_mut)
            .expect("fixture run config")
            .insert("archivePolicyAuthority".to_owned(), Value::Null);
        reseal_generation_config(&mut archive_drift);
        let authority =
            V5SharedConstructionAuthority::from_shared_object(&archive_drift.shared_authority)
                .expect("parse fixture authority");
        let archive_error = V5G0PublicationPlan::derive(&archive_drift, &authority)
            .expect_err("self-rehashed nested archive drift must fail closed");
        assert!(
            archive_error
                .to_string()
                .contains("runConfig archive/behavior authority drifted")
        );

        let mut operator_drift = request(1, 1, 1);
        operator_drift
            .generation_config
            .get_mut("runConfig")
            .and_then(Value::as_object_mut)
            .expect("fixture run config")
            .insert("operatorImplementation".to_owned(), Value::Null);
        reseal_generation_config(&mut operator_drift);
        let authority =
            V5SharedConstructionAuthority::from_shared_object(&operator_drift.shared_authority)
                .expect("parse fixture authority");
        let operator_error = V5G0PublicationPlan::derive(&operator_drift, &authority)
            .expect_err("self-rehashed nested operator drift must fail closed");
        assert!(
            operator_error
                .to_string()
                .contains("runConfig operator implementation drifted")
        );
    }

    #[test]
    fn v5_g0_transaction_retries_after_global_duplicate_without_spending_birth() {
        let request = request(1, 1, 2);
        let authority =
            V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)
                .expect("parse fixture authority");
        let first_seed = v5::v5_proposal_seed(&request.generation_config_sha256, 0)
            .expect("first proposal seed");
        let first = build_v5_g0_accepted_material(&authority, 1, 0, 0, &first_seed)
            .expect("construct first compact G0 material");
        let ledger_identity = g0_ledger_identity(&request, &authority).expect("ledger identity");
        let mut ledger = CandidateIdentityLedger::new(
            ledger_identity,
            [first.record.candidate_identity_sha256.clone()],
        )
        .expect("preload exact global duplicate");

        let result = execute_with_ledger(request.clone(), authority, &mut ledger)
            .expect("retry after exact global duplicate");
        assert!(result.target_reached);
        assert_eq!(result.attempts.len(), 2);
        assert_eq!(result.attempts[0].proposal_ordinal, 0);
        assert_eq!(result.attempts[0].disposition, "rejected");
        assert_eq!(
            result.attempts[0].reason_code,
            "duplicate_candidate_identity_global"
        );
        assert_eq!(result.attempts[1].proposal_ordinal, 1);
        assert_eq!(result.attempts[1].disposition, "accepted");
        assert_eq!(result.accepted_records[0].birth_ordinal, 0);
        assert_eq!(result.accepted_records[0].proposal_ordinal, 1);
        assert_eq!(result.schedule_state_receipt.next_proposal_ordinal, 2);
        verify_v5_g0_transaction_replay(&request, &result).expect("retry result replays");
    }

    #[test]
    fn v5_g0_transaction_respects_max_attempt_ceiling_after_duplicate() {
        let request = request(1, 1, 1);
        let authority =
            V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)
                .expect("parse fixture authority");
        let first_seed = v5::v5_proposal_seed(&request.generation_config_sha256, 0)
            .expect("first proposal seed");
        let first = build_v5_g0_accepted_material(&authority, 1, 0, 0, &first_seed)
            .expect("construct first compact G0 material");
        let ledger_identity = g0_ledger_identity(&request, &authority).expect("ledger identity");
        let mut ledger = CandidateIdentityLedger::new(
            ledger_identity,
            [first.record.candidate_identity_sha256.clone()],
        )
        .expect("preload exact global duplicate");

        let result = execute_with_ledger(request, authority, &mut ledger)
            .expect("finish at exact max-attempt ceiling");
        assert!(!result.target_reached);
        assert_eq!(result.stop_reason, "max_attempts_reached");
        assert_eq!(result.attempts.len(), 1);
        assert!(result.accepted_records.is_empty());
        assert_eq!(
            result.compact_accepted_journal.ordered_record_sha256s.len(),
            0
        );
        assert!(result.accepted_pool.is_none());
        assert!(result.campaign_ledger.is_none());
        assert!(result.g0_selection.is_none());
    }

    #[test]
    fn v5_g0_cap_one_and_cap_eight_have_identical_durable_semantics() {
        let serial =
            execute_v5_g0_transaction(request(1, 2, 2)).expect("serial native G0 transaction");
        let parallel = execute_v5_g0_transaction(request(8, 2, 2))
            .expect("bounded-parallel native G0 transaction");
        assert_eq!(serial.thread_cap, 1);
        assert_eq!(parallel.thread_cap, 8);
        assert_eq!(
            serial.to_value().expect("serial semantic value"),
            parallel.to_value().expect("parallel semantic value"),
            "thread cap is telemetry only; ordinal merge must preserve every durable byte",
        );
        assert_eq!(
            serial
                .compact_accepted_journal
                .to_value()
                .expect("serial compact journal"),
            parallel
                .compact_accepted_journal
                .to_value()
                .expect("parallel compact journal"),
        );
    }

    #[test]
    fn v5_g0_transaction_checkpoint_round_trips_and_replays_on_demand() {
        let request = request(1, 2, 2);
        let result = execute_v5_g0_transaction(request.clone())
            .expect("execute native G0 transaction checkpoint fixture");
        let encoded = result.to_value().expect("encode transaction checkpoint");
        let parsed = V5G0TransactionResult::from_value(&encoded)
            .expect("parse transaction checkpoint without constructor replay");
        assert_eq!(
            parsed.to_value().expect("re-encode transaction checkpoint"),
            encoded,
        );
        parsed
            .verify_checkpoint_replay(&request)
            .expect("explicit audited checkpoint replay");

        let mut tampered = encoded;
        tampered["targetAccepted"] = Value::from(3_u64);
        assert!(V5G0TransactionResult::from_value(&tampered).is_err());
    }

    #[test]
    fn v5_g0_products_keep_selected_projection_record_binding_across_input_orders() {
        let request = request(1, 2, 2);
        let result = execute_v5_g0_transaction(request.clone())
            .expect("execute complete native G0 transaction");
        let authority =
            V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)
                .expect("parse fixture authority");
        let mut reversed = result.accepted_records.clone();
        reversed.reverse();

        // `g0_products` canonicalizes accepted references by reference SHA.
        // Exercise both record orders so a later zip of sorted references to
        // positional records cannot silently bind a selected projection to a
        // different compact record.
        for records in [result.accepted_records.clone(), reversed] {
            let (_, _, _, index) =
                g0_products(&request, &authority, &records).expect("build canonical G0 products");
            for projection in index.projections {
                let expected = records
                    .iter()
                    .find(|record| {
                        record.record_sha256().expect("compact record identity")
                            == projection.record_sha256
                    })
                    .expect("projection names one supplied compact record")
                    .selected_projection()
                    .expect("derive selected projection from named record");
                assert_eq!(projection, expected);
            }
        }
    }

    #[test]
    fn v5_g0_durable_bindings_and_no_rich_adoption_replay() {
        let request = request(1, 2, 2);
        let result = execute_v5_g0_transaction(request.clone())
            .expect("execute complete native G0 transaction");

        let bindings = result
            .durable_object_bindings()
            .expect("derive closed durable object inventory");
        assert!(bindings.iter().all(|binding| binding.validate().is_ok()));
        let kinds = bindings
            .iter()
            .map(|binding| binding.kind)
            .collect::<BTreeSet<_>>();
        for kind in [
            V5G0DurableObjectKind::PublicationPlan,
            V5G0DurableObjectKind::AttemptJournal,
            V5G0DurableObjectKind::AttemptOutcomeAudit,
            V5G0DurableObjectKind::CompactProposalDelta,
            V5G0DurableObjectKind::CompactAcceptedRecord,
            V5G0DurableObjectKind::CompactAcceptedJournal,
            V5G0DurableObjectKind::IdentityLedger,
            V5G0DurableObjectKind::ScheduleStateReceipt,
            V5G0DurableObjectKind::AcceptedPool,
            V5G0DurableObjectKind::CampaignLedger,
            V5G0DurableObjectKind::G0Selection,
            V5G0DurableObjectKind::SelectedProjectionIndex,
        ] {
            assert!(
                kinds.contains(&kind),
                "missing durable object kind {}",
                kind.as_str()
            );
        }
        assert!(bindings.iter().all(|binding| {
            binding
                .relative_path
                .starts_with("v5-native/objects/sha256/")
                && binding.relative_path.ends_with(".json")
        }));

        let pool = result
            .accepted_pool
            .as_ref()
            .expect("completed G0 has an accepted pool");
        let reference_sha256s = pool
            .get("acceptedReferences")
            .and_then(Value::as_array)
            .expect("accepted pool references")
            .iter()
            .map(|reference| {
                reference
                    .get("referenceSha256")
                    .and_then(Value::as_str)
                    .expect("accepted reference identity")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        let mut canonical_reference_sha256s = reference_sha256s.clone();
        canonical_reference_sha256s.sort();
        assert_eq!(reference_sha256s, canonical_reference_sha256s);
        accepted_pool_durable_object_identity(pool)
            .expect("fresh accepted pool has canonical durable bytes");

        // G0's semantic pool hash itself tolerates an arbitrary input
        // reference order.  A content-addressed native object cannot: one
        // object SHA must name one canonical JSON value.
        let mut reordered_pool = pool.clone();
        reordered_pool
            .get_mut("acceptedReferences")
            .and_then(Value::as_array_mut)
            .expect("accepted pool references")
            .reverse();
        g0::validate_accepted_pool(&reordered_pool)
            .expect("the reordered pool remains semantically valid");
        assert!(accepted_pool_durable_object_identity(&reordered_pool).is_err());

        let stream = prepare_v5_g0_publication_stream(&request, &result)
            .expect("prepare compact selected-only publication stream");
        let mut fragment_storage = InMemoryFragments::default();
        let fragment_receipt = stream
            .materialize_selected_fragments(&mut fragment_storage)
            .expect("materialize exactly one selected-only fragment pass");
        assert_eq!(
            fragment_receipt.population_candidates.row_count,
            stream.selected_count() as u64,
        );

        let mut pair_config = Vec::new();
        let mut population = Vec::new();
        let mut evaluation_population = Vec::new();
        let mut generation_journal = Vec::new();
        let receipt = stream
            .write_bundle_from_fragments(
                &fragment_receipt,
                &mut fragment_storage,
                &mut pair_config,
                &mut population,
                &mut evaluation_population,
                &mut generation_journal,
            )
            .expect("write public bundle from compact fragments");
        let receipt_binding = receipt
            .object_binding()
            .expect("derive publication receipt object binding");
        receipt_binding
            .to_value()
            .expect("validate publication receipt object binding");

        // Fresh verification still has the private fragments.  Recovery must
        // not: drop them before exercising the public no-rich verifier.
        stream
            .verify_bundle_from_fragments(
                &fragment_receipt,
                &mut fragment_storage,
                &mut Cursor::new(pair_config.clone()),
                &mut Cursor::new(population.clone()),
                &mut Cursor::new(evaluation_population.clone()),
                &mut Cursor::new(generation_journal.clone()),
            )
            .expect("fresh bundle verifies against its private fragments");
        drop(fragment_storage);

        let receipt_value = receipt.to_value().expect("encode publication receipt");
        let adopted = verify_v5_g0_publication_adoption(
            &request,
            &result,
            &receipt_value,
            &mut Cursor::new(pair_config.clone()),
            &mut Cursor::new(population.clone()),
            &mut Cursor::new(evaluation_population.clone()),
            &mut Cursor::new(generation_journal.clone()),
        )
        .expect("adoption verifies public bytes without rich reconstruction");
        assert_eq!(adopted, receipt);

        let mut tampered_population = population;
        tampered_population[0] ^= 1;
        assert!(
            verify_v5_g0_publication_adoption(
                &request,
                &result,
                &receipt_value,
                &mut Cursor::new(pair_config),
                &mut Cursor::new(tampered_population),
                &mut Cursor::new(evaluation_population),
                &mut Cursor::new(generation_journal),
            )
            .is_err()
        );
    }

    #[test]
    fn v5_g0_funnel_receipt_binds_all_attempts_nonselected_bootstrap_and_public_evaluation() {
        fn execute_with_first_duplicate(
            thread_cap: u64,
        ) -> (V5G0TransactionRequest, V5G0TransactionResult) {
            let mut request = request(thread_cap, 2, 3);
            request.evaluation_width = 1;
            let authority =
                V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)
                    .expect("parse fixture authority");
            let first_seed = v5::v5_proposal_seed(&request.generation_config_sha256, 0)
                .expect("first proposal seed");
            let first = build_v5_g0_accepted_material(&authority, 1, 0, 0, &first_seed)
                .expect("construct first compact G0 material");
            let ledger_identity =
                g0_ledger_identity(&request, &authority).expect("derive G0 ledger authority");
            let mut ledger = CandidateIdentityLedger::new(
                ledger_identity,
                [first.record.candidate_identity_sha256],
            )
            .expect("preload exact first-attempt duplicate");
            let result = execute_with_ledger(request.clone(), authority, &mut ledger)
                .expect("execute rejected plus accepted G0 attempts");
            (request, result)
        }

        let (request, result) = execute_with_first_duplicate(1);
        assert_eq!(result.attempts.len(), 3);
        assert_eq!(result.accepted_records.len(), 2);
        assert_eq!(result.attempts[0].disposition, "rejected");

        let stream = prepare_v5_g0_publication_stream(&request, &result)
            .expect("prepare selected-only G0 public stream");
        let mut private_fragments = InMemoryFragments::default();
        let publication_fragments = stream
            .materialize_selected_fragments(&mut private_fragments)
            .expect("materialize one selected candidate");
        let mut pair_config = Vec::new();
        let mut population = Vec::new();
        let mut evaluation_population = Vec::new();
        let mut generation_journal = Vec::new();
        let publication_receipt = stream
            .write_bundle_from_fragments(
                &publication_fragments,
                &mut private_fragments,
                &mut pair_config,
                &mut population,
                &mut evaluation_population,
                &mut generation_journal,
            )
            .expect("write authenticated G0 public bundle");

        let receipt = build_v5_g0_funnel_fragments(
            &request,
            &result,
            &publication_fragments,
            &publication_receipt,
        )
        .expect("build compact G0 funnel receipt");
        assert_eq!(receipt.proposal_attempt_count, 3);
        assert_eq!(receipt.accepted_count, 2);
        assert_eq!(receipt.selected_count, 1);
        assert_eq!(receipt.proposal_attempt_stream.row_count, 3);
        assert_eq!(receipt.accepted_pool_membership.row_count, 2);
        assert_eq!(receipt.campaign_ledger.row_count, 2);
        assert_eq!(receipt.selected_projection_index.row_count, 1);
        assert_eq!(receipt.g0_selection.row_count, 1);
        assert_eq!(receipt.evaluation_population_funnel_entries.row_count, 1);
        assert_eq!(
            receipt.evaluation_population_funnel_entries.fragment_sha256,
            publication_fragments
                .evaluation_funnel_entries
                .fragment_sha256
        );
        assert_eq!(
            receipt.evaluation_population_sha256,
            publication_receipt.evaluation_population.semantic_sha256
        );

        // Private rich fragments are no longer needed by the rotating handoff.
        drop(private_fragments);
        let mut projection_sink = InMemoryFunnelProjections::default();
        stream_v5_g0_funnel_projections(
            &request,
            &result,
            &publication_receipt,
            &receipt,
            &mut projection_sink,
        )
        .expect("stream compact G0 funnel projections after fragment deletion");
        let attempt_rows = projection_sink.rows(V5G0FunnelProjectionKind::ProposalAttemptEntries);
        assert_eq!(attempt_rows.len(), 3);
        assert_eq!(
            attempt_rows
                .iter()
                .map(|row| row["proposalOrdinal"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(attempt_rows[0]["disposition"], "rejected");
        assert!(attempt_rows[0].get("candidate").is_none());
        assert_eq!(attempt_rows[1]["disposition"], "accepted");
        assert_eq!(attempt_rows[2]["disposition"], "accepted");
        assert!(attempt_rows[1].get("funnelCandidate").is_some());
        assert!(attempt_rows[2].get("funnelCandidate").is_some());
        let evaluation_rows =
            projection_sink.rows(V5G0FunnelProjectionKind::EvaluationPopulationFunnelEntries);
        assert_eq!(evaluation_rows.len(), 1);
        assert_eq!(
            evaluation_rows[0]["funnelCandidate"]["admission"]["canonicalEvidenceIdentitySha256"],
            Value::Null,
            "G0 bootstrap is the explicit pre-evaluation evidence exception",
        );
        let evaluated_record = evaluation_rows[0]["acceptedCompactRecordSha256"]
            .as_str()
            .expect("evaluation row has compact record");
        assert!(
            attempt_rows[1..].iter().any(|row| {
                row["acceptedCompactRecordSha256"].as_str() != Some(evaluated_record)
            }),
            "one accepted candidate remains deliberately nonselected"
        );

        let value = receipt.to_value().expect("encode funnel receipt");
        assert_eq!(
            V5G0FunnelFragments::from_value(&value).expect("parse funnel receipt"),
            receipt
        );
        let binding = receipt
            .object_binding()
            .expect("derive funnel object binding");
        assert!(
            binding
                .relative_path
                .starts_with("v5-native/objects/sha256/")
        );
        assert!(binding.relative_path.ends_with(".json"));
        let binding_value = binding.to_value().expect("encode funnel object binding");
        assert_eq!(
            V5G0FunnelFragmentReceiptObjectBinding::from_value(&binding_value)
                .expect("parse funnel object binding"),
            binding
        );
        assert_eq!(
            verify_v5_g0_funnel_fragment_receipt(
                &request,
                &result,
                &publication_receipt,
                &binding_value,
            )
            .expect("verify persisted G0 funnel receipt"),
            receipt
        );

        let mut persisted_projection = Vec::new();
        let projection_stream_receipt = write_v5_g0_funnel_projection_stream(
            &request,
            &result,
            &publication_receipt,
            &receipt,
            &mut persisted_projection,
        )
        .expect("write fixed public G0 projection JSONL after private-fragment deletion");
        assert_eq!(
            projection_stream_receipt.relative_path,
            V5_G0_FUNNEL_PROJECTION_STREAM_PATH
        );
        assert_eq!(projection_stream_receipt.row_count, 3);
        assert_eq!(
            projection_stream_receipt.size_bytes,
            persisted_projection.len() as u64
        );
        assert_eq!(persisted_projection.last(), Some(&b'\n'));
        let persisted_rows = persisted_projection
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<Value>(line).expect("parse persisted G0 row"))
            .collect::<Vec<_>>();
        assert_eq!(
            persisted_rows, attempt_rows,
            "JSONL preserves exact Core row order"
        );
        let projection_stream_value = projection_stream_receipt
            .to_value()
            .expect("encode G0 projection stream receipt");
        assert_eq!(
            V5G0FunnelProjectionStreamReceipt::from_value(&projection_stream_value)
                .expect("parse G0 projection stream receipt"),
            projection_stream_receipt
        );
        let projection_stream_binding = projection_stream_receipt
            .object_binding()
            .expect("derive G0 projection stream receipt object binding");
        assert!(
            projection_stream_binding
                .relative_path
                .starts_with("v5-native/objects/sha256/")
        );
        assert!(projection_stream_binding.relative_path.ends_with(".json"));
        let projection_stream_binding_value = projection_stream_binding
            .to_value()
            .expect("encode G0 projection stream receipt object binding");
        assert_eq!(
            V5G0FunnelProjectionStreamReceiptObjectBinding::from_value(
                &projection_stream_binding_value,
            )
            .expect("parse G0 projection stream receipt object binding"),
            projection_stream_binding
        );
        assert_eq!(
            verify_v5_g0_funnel_projection_stream(
                &receipt,
                &projection_stream_value,
                &mut Cursor::new(persisted_projection.clone()),
            )
            .expect("verify public G0 projection stream without transaction/private fragments"),
            projection_stream_receipt
        );

        let mut missing_row = persisted_projection.clone();
        let previous_lf = missing_row[..missing_row.len() - 1]
            .iter()
            .rposition(|byte| *byte == b'\n')
            .expect("projection has multiple rows");
        missing_row.truncate(previous_lf + 1);
        assert!(
            verify_v5_g0_funnel_projection_stream(
                &receipt,
                &projection_stream_value,
                &mut Cursor::new(missing_row),
            )
            .is_err()
        );
        let mut reordered_rows = persisted_rows.clone();
        reordered_rows.swap(1, 2);
        let mut reordered_bytes = Vec::new();
        for row in reordered_rows {
            temporal_qd_contract::write_canonical_json(&row, &mut reordered_bytes)
                .expect("encode reordered projection row");
            reordered_bytes.push(b'\n');
        }
        assert!(
            verify_v5_g0_funnel_projection_stream(
                &receipt,
                &projection_stream_value,
                &mut Cursor::new(reordered_bytes),
            )
            .is_err()
        );

        let mut raw_substitution = projection_stream_receipt.clone();
        raw_substitution.raw_sha256 = format!("sha256:{}", "0".repeat(64));
        assert!(
            verify_v5_g0_funnel_projection_stream(
                &receipt,
                &raw_substitution
                    .to_value()
                    .expect("self-hash raw substitution"),
                &mut Cursor::new(persisted_projection.clone()),
            )
            .is_err()
        );
        let mut size_substitution = projection_stream_receipt.clone();
        size_substitution.size_bytes += 1;
        assert!(
            verify_v5_g0_funnel_projection_stream(
                &receipt,
                &size_substitution
                    .to_value()
                    .expect("self-hash size substitution"),
                &mut Cursor::new(persisted_projection.clone()),
            )
            .is_err()
        );
        let mut count_substitution = projection_stream_receipt.clone();
        count_substitution.row_count += 1;
        assert!(
            verify_v5_g0_funnel_projection_stream(
                &receipt,
                &count_substitution
                    .to_value()
                    .expect("self-hash count substitution"),
                &mut Cursor::new(persisted_projection.clone()),
            )
            .is_err()
        );
        let mut input_substitution = projection_stream_receipt.clone();
        input_substitution.input_g0_funnel_fragments_sha256 = format!("sha256:{}", "0".repeat(64));
        assert!(
            verify_v5_g0_funnel_projection_stream(
                &receipt,
                &input_substitution
                    .to_value()
                    .expect("self-hash input substitution"),
                &mut Cursor::new(persisted_projection.clone()),
            )
            .is_err()
        );
        let mut path_alias = projection_stream_receipt.clone();
        path_alias.relative_path = "v5-native/alias.jsonl".to_owned();
        assert!(path_alias.to_value().is_err());
        let mut missing_binding = projection_stream_binding_value.clone();
        missing_binding.as_object_mut().unwrap().remove("value");
        assert!(
            V5G0FunnelProjectionStreamReceiptObjectBinding::from_value(&missing_binding).is_err()
        );
        let mut replaced_binding = projection_stream_binding.clone();
        replaced_binding.value = receipt.to_value().expect("encode wrong-family G0 receipt");
        assert!(replaced_binding.validate().is_err());
        let mut aliased_binding = projection_stream_binding.clone();
        aliased_binding.relative_path =
            "v5-native/objects/sha256/0000000000000000000000000000000000000000000000000000000000000000.json"
                .to_owned();
        assert!(aliased_binding.validate().is_err());
        let mut substituted_binding = projection_stream_binding.clone();
        substituted_binding.value = raw_substitution
            .to_value()
            .expect("encode substituted stream receipt");
        assert!(substituted_binding.validate().is_err());

        // A cap change is control-plane telemetry only.  Replaying the exact
        // same semantic publication facts under cap eight yields the same
        // receipt identity and therefore the same object path.
        let (parallel_request, parallel_result) = execute_with_first_duplicate(8);
        let parallel_receipt = build_v5_g0_funnel_fragments(
            &parallel_request,
            &parallel_result,
            &publication_fragments,
            &publication_receipt,
        )
        .expect("build cap-eight compact funnel receipt");
        assert_eq!(parallel_receipt, receipt);
        assert_eq!(
            parallel_receipt
                .object_binding()
                .expect("parallel object binding")
                .relative_path,
            binding.relative_path
        );
        let mut parallel_projection = Vec::new();
        let parallel_stream_receipt = write_v5_g0_funnel_projection_stream(
            &parallel_request,
            &parallel_result,
            &publication_receipt,
            &parallel_receipt,
            &mut parallel_projection,
        )
        .expect("write cap-eight G0 projection stream");
        assert_eq!(parallel_stream_receipt, projection_stream_receipt);
        assert_eq!(parallel_projection, persisted_projection);
        assert_eq!(
            parallel_stream_receipt
                .object_binding()
                .expect("parallel stream receipt object binding"),
            projection_stream_binding
        );

        // Missing/extra/family substitutions fail before contextual replay.
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove("campaignLedger");
        assert!(V5G0FunnelFragments::from_value(&missing).is_err());
        let mut extra = value.clone();
        extra
            .as_object_mut()
            .unwrap()
            .insert("unexpected".to_owned(), Value::Null);
        assert!(V5G0FunnelFragments::from_value(&extra).is_err());
        let evolved_family = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_publication_fragments_v2".to_owned()),
            ),
            ("fragmentBundleSha256", Value::String("0".repeat(64))),
        ]);
        assert!(V5G0FunnelFragments::from_value(&evolved_family).is_err());

        // A self-consistent outer rehash still cannot substitute any inner
        // stream/set root, public receipt, or immutable object path.
        let mut substituted = receipt.clone();
        substituted.proposal_attempt_stream.fragment_sha256 = format!("sha256:{}", "0".repeat(64));
        let substituted_value = substituted.to_value().expect("rehash substituted receipt");
        let substituted = V5G0FunnelFragments::from_value(&substituted_value)
            .expect("substituted receipt is individually self-hashed");
        assert!(
            substituted
                .verify_against(&request, &result, &publication_receipt)
                .is_err()
        );
        let mut alias = binding.clone();
        alias.relative_path = "v5-native/objects/sha256/0000000000000000000000000000000000000000000000000000000000000000.json".to_owned();
        assert!(alias.validate().is_err());
        let mut staged_substitution = publication_fragments.clone();
        staged_substitution
            .evaluation_funnel_entries
            .fragment_sha256 = format!("sha256:{}", "0".repeat(64));
        assert!(
            build_v5_g0_funnel_fragments(
                &request,
                &result,
                &staged_substitution,
                &publication_receipt,
            )
            .is_err()
        );

        // Reordering a canonical accepted-pool object cannot be smuggled
        // through the otherwise order-independent G0 selection semantics.
        let mut reordered = result.clone();
        reordered
            .accepted_pool
            .as_mut()
            .and_then(|pool| pool.get_mut("acceptedReferences"))
            .and_then(Value::as_array_mut)
            .expect("accepted pool references")
            .reverse();
        assert!(
            build_v5_g0_funnel_fragments(
                &request,
                &reordered,
                &publication_fragments,
                &publication_receipt,
            )
            .is_err()
        );
    }
}
