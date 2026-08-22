//! AutoResearch proposal scheduling, duplicate ordering, and rich-entry shell.
//!
//! Candidate and profile semantics stay opaque.  This module decides the
//! deterministic proposal slot and records the compatibility envelope around a
//! Dashboard-native result; it never attempts to construct a graph or infer a
//! Dashboard validation outcome.

use std::collections::{BTreeMap, BTreeSet};

use temporal_qd_contract::{
    ContractError, Map, Value, canonical_sha256, canonical_sha256_streaming,
    canonical_sha256_without_object_field,
};

use crate::{
    factory::{FactoryError, NativeProposal, ParentReference, ProposalIntent},
    identity::{Side, immigrant_side_seed, proposal_seed, proposal_side},
    operator_family_matrix::OperatorFamilyMatrixContract,
    schedule::{
        RotatingParentSchedule, is_crossover_slot, mutation_depth_for_seed,
        scheduled_immigrant_for_accepted_quota,
    },
};

pub const PROPOSAL_ENTRY_SCHEMA: &str = "temporal_qd_proposal_entry_v3";
pub const PROPOSAL_STATE_SCHEMA: &str = "temporal_qd_native_proposal_state_v1";

#[derive(Debug, thiserror::Error)]
pub enum ProposalError {
    #[error("canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("factory failure: {0}")]
    Factory(#[from] FactoryError),
    #[error("proposal contract failure: {0}")]
    Contract(String),
    #[error(
        "archive parent selection is not admitted in this kernel request; provide an explicit AutoResearch parent selector"
    )]
    ParentSelectorUnavailable,
}

pub type Result<T> = std::result::Result<T, ProposalError>;

fn contract(message: impl Into<String>) -> ProposalError {
    ProposalError::Contract(message.into())
}

fn object(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut map = Map::new();
    for (key, value) in entries {
        map.insert(key.to_owned(), value);
    }
    Value::Object(map)
}

fn sha(value: &str, label: &str) -> Result<()> {
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value.as_bytes()[7..]
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(contract(format!(
            "{label} must be a lowercase SHA-256 identity"
        )));
    }
    Ok(())
}

fn candidate_identity(candidate: &Value) -> Result<String> {
    candidate
        .as_object()
        .and_then(|fields| fields.get("candidateIdentitySha256"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| contract("native candidate lacks candidateIdentitySha256"))
        .and_then(|value| {
            sha(&value, "candidateIdentitySha256")?;
            Ok(value)
        })
}

fn candidate_id(candidate: &Value) -> Result<String> {
    candidate
        .as_object()
        .and_then(|fields| fields.get("candidateId"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| contract("native candidate lacks candidateId"))
}

/// Parent selection is explicitly injected because the archive-cell selection
/// policy has its own rank/crowding/negative-novelty semantics.  The explicit
/// parent ring below is admitted; a caller must not silently substitute it for
/// an archive selector.
pub trait ParentSelector {
    fn has_parents(&self) -> bool;
    fn eligible_parent_count(&self) -> usize;
    fn archive_cell_count(&self) -> usize;
    fn compact_state(&self) -> Value;
    fn restore_compact_state(&mut self, state: &Value) -> Result<()>;
    fn select(&mut self, label: &str, structural_selection_ordinal: u64)
    -> Result<ParentReference>;
}

/// Exact identity-sorted explicit parent ring from the Python fallback path.
#[derive(Clone, Debug)]
pub struct ExplicitParentRing {
    parents: Vec<ParentReference>,
}

impl ExplicitParentRing {
    pub fn new(mut parents: Vec<ParentReference>) -> Result<Self> {
        for parent in &parents {
            parent.validate()?;
        }
        parents.sort_by(|left, right| left.pair_identity_sha256.cmp(&right.pair_identity_sha256));
        Ok(Self { parents })
    }
}

impl ParentSelector for ExplicitParentRing {
    fn has_parents(&self) -> bool {
        !self.parents.is_empty()
    }

    fn eligible_parent_count(&self) -> usize {
        self.parents.len()
    }

    fn archive_cell_count(&self) -> usize {
        0
    }

    fn compact_state(&self) -> Value {
        object([(
            "schemaVersion",
            Value::String("temporal_qd_explicit_parent_ring_state_v1".to_owned()),
        )])
    }

    fn restore_compact_state(&mut self, state: &Value) -> Result<()> {
        if state != &self.compact_state() {
            return Err(contract("explicit parent ring state mismatched checkpoint"));
        }
        Ok(())
    }

    fn select(
        &mut self,
        _label: &str,
        structural_selection_ordinal: u64,
    ) -> Result<ParentReference> {
        let count = self.parents.len();
        if count == 0 {
            return Err(contract("explicit parent ring has no parents"));
        }
        Ok(self.parents[(structural_selection_ordinal % count as u64) as usize].clone())
    }
}

#[derive(Clone, Debug)]
pub struct ProposalSchedule {
    pub config_sha256: String,
    pub generation_index: u64,
    pub parent_schedule: Option<RotatingParentSchedule>,
    pub desired_evaluated_offspring: u64,
    pub desired_evaluated_immigrants: u64,
    pub operator_family_matrix: Option<OperatorFamilyMatrixContract>,
    pub matrix_parents: BTreeMap<String, ParentReference>,
}

impl ProposalSchedule {
    pub fn validate(&self) -> Result<()> {
        sha(&self.config_sha256, "generation config SHA-256")
    }
}

#[derive(Clone, Debug)]
pub struct PlannedProposal {
    pub proposal_ordinal: u64,
    pub intent: ProposalIntent,
}

/// State which changes only after a proposal segment is sealed.
#[derive(Clone, Debug, Default)]
pub struct ProposalState {
    pub next_proposal_ordinal: u64,
    pub structural_parent_selections: u64,
    pub local_candidate_identities: BTreeSet<String>,
    pub local_executable_semantics: BTreeSet<String>,
    pub entry_sha256s: Vec<String>,
    pub origin_proposal_counts: BTreeMap<String, u64>,
    pub origin_accepted_counts: BTreeMap<String, u64>,
    pub disposition_counts: BTreeMap<String, u64>,
    /// These are retained independently of generic origin counters because
    /// the frozen immigrant collision tripwire is evaluated between durable
    /// proposal receipts and must resume without rediscovering history.
    pub immigrant_attempts: u64,
    pub immigrant_accepted: u64,
}

impl ProposalState {
    pub fn compact_value(&self) -> Value {
        object([
            (
                "schemaVersion",
                Value::String(PROPOSAL_STATE_SCHEMA.to_owned()),
            ),
            (
                "nextProposalOrdinal",
                Value::from(self.next_proposal_ordinal),
            ),
            (
                "structuralParentSelections",
                Value::from(self.structural_parent_selections),
            ),
            (
                "localCandidateIdentities",
                Value::Array(
                    self.local_candidate_identities
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            ),
            (
                "localExecutableSemantics",
                Value::Array(
                    self.local_executable_semantics
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            ),
            (
                "entrySha256s",
                Value::Array(
                    self.entry_sha256s
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            ),
            (
                "originProposalCounts",
                Value::Object(
                    self.origin_proposal_counts
                        .iter()
                        .map(|(key, value)| (key.clone(), Value::from(*value)))
                        .collect(),
                ),
            ),
            (
                "originAcceptedCounts",
                Value::Object(
                    self.origin_accepted_counts
                        .iter()
                        .map(|(key, value)| (key.clone(), Value::from(*value)))
                        .collect(),
                ),
            ),
            (
                "dispositionCounts",
                Value::Object(
                    self.disposition_counts
                        .iter()
                        .map(|(key, value)| (key.clone(), Value::from(*value)))
                        .collect(),
                ),
            ),
            ("immigrantAttempts", Value::from(self.immigrant_attempts)),
            ("immigrantAccepted", Value::from(self.immigrant_accepted)),
        ])
    }

    pub fn observe(&mut self, record: &PreparedProposal) -> Result<()> {
        self.observe_fields(
            record.proposal_ordinal,
            &record.origin_kind,
            &record.disposition,
            &record.entry_sha256,
            record.accepted.as_ref(),
        )
    }

    /// Advance the common proposal scheduler from a compact native attempt.
    ///
    /// The v5 transaction never fabricates a legacy rich proposal entry just
    /// to update scheduling counters.  It still uses the exact same state
    /// transition as the rich path: this compact attempt identity occupies
    /// the historical `entry_sha256` bookkeeping slot solely as the durable
    /// per-ordinal receipt identity.
    pub fn observe_compact_attempt(
        &mut self,
        proposal_ordinal: u64,
        origin_kind: &str,
        disposition: &str,
        attempt_sha256: &str,
        accepted: Option<&AcceptedProposal>,
    ) -> Result<()> {
        self.observe_fields(
            proposal_ordinal,
            origin_kind,
            disposition,
            attempt_sha256,
            accepted,
        )
    }

    /// Advance from the compact facts retained in a verified durable segment.
    /// Recovery must not retain the segment's rich candidate solely to rebuild
    /// bookkeeping that never reads it.
    pub(crate) fn observe_recovered(
        &mut self,
        proposal_ordinal: u64,
        origin_kind: &str,
        disposition: &str,
        entry_sha256: &str,
        accepted: Option<&AcceptedProposal>,
    ) -> Result<()> {
        self.observe_fields(
            proposal_ordinal,
            origin_kind,
            disposition,
            entry_sha256,
            accepted,
        )
    }

    fn observe_fields(
        &mut self,
        proposal_ordinal: u64,
        origin_kind: &str,
        disposition: &str,
        entry_sha256: &str,
        accepted: Option<&AcceptedProposal>,
    ) -> Result<()> {
        self.next_proposal_ordinal = proposal_ordinal
            .checked_add(1)
            .ok_or_else(|| contract("proposal ordinal overflowed"))?;
        *self
            .origin_proposal_counts
            .entry(origin_kind.to_owned())
            .or_default() += 1;
        if origin_kind == "random_immigrant" {
            self.immigrant_attempts = self
                .immigrant_attempts
                .checked_add(1)
                .ok_or_else(|| contract("immigrant attempt count overflowed"))?;
        }
        *self
            .disposition_counts
            .entry(disposition.to_owned())
            .or_default() += 1;
        self.entry_sha256s.push(entry_sha256.to_owned());
        if let Some(accepted) = accepted {
            self.local_candidate_identities
                .insert(accepted.candidate_identity_sha256.clone());
            self.local_executable_semantics
                .insert(accepted.executable_semantic_sha256.clone());
            *self
                .origin_accepted_counts
                .entry(origin_kind.to_owned())
                .or_default() += 1;
            if origin_kind == "random_immigrant" {
                self.immigrant_accepted = self
                    .immigrant_accepted
                    .checked_add(1)
                    .ok_or_else(|| contract("immigrant accepted count overflowed"))?;
            }
        }
        Ok(())
    }

    pub fn from_compact_value(value: &Value) -> Result<Self> {
        let fields = value
            .as_object()
            .ok_or_else(|| contract("proposal state must be an object"))?;
        if fields.get("schemaVersion").and_then(Value::as_str) != Some(PROPOSAL_STATE_SCHEMA) {
            return Err(contract("proposal state schema is incompatible"));
        }
        Ok(Self {
            next_proposal_ordinal: integer(fields, "nextProposalOrdinal")?,
            structural_parent_selections: integer(fields, "structuralParentSelections")?,
            local_candidate_identities: identities(fields, "localCandidateIdentities")?,
            local_executable_semantics: identities(fields, "localExecutableSemantics")?,
            entry_sha256s: identity_list(fields, "entrySha256s")?,
            origin_proposal_counts: counts(fields, "originProposalCounts")?,
            origin_accepted_counts: counts(fields, "originAcceptedCounts")?,
            disposition_counts: counts(fields, "dispositionCounts")?,
            immigrant_attempts: integer(fields, "immigrantAttempts")?,
            immigrant_accepted: integer(fields, "immigrantAccepted")?,
        })
    }
}

/// An injected ledger makes the existing full campaign-wide duplicate policy
/// explicit.  The native generation engine does not reinterpret program or
/// evidence identities and must persist `prepared_delta` before calling
/// `commit_prepared_delta`.
pub trait IdentityLedger {
    fn identity(&self) -> &Value;
    fn prepare_proposal(&self, proposal: LedgerProposal<'_>) -> Result<LedgerDecision>;
    fn commit_prepared_delta(&mut self, prepared_delta: &Value) -> Result<()>;
    fn compact_state(&self) -> Value;
    fn restore_compact_state(&mut self, state: &Value) -> Result<()>;
    /// Python-compatible ledgers may expose their immutable final public
    /// facade. The kernel checkpoints mutable state per receipt, then writes
    /// this write-once facade only at completed-generation publication.
    fn public_ledger(&self) -> Option<Value> {
        None
    }
}

pub struct LedgerProposal<'a> {
    pub proposal_ordinal: u64,
    pub candidate: Option<&'a Value>,
    pub executable_semantic_sha256: Option<&'a str>,
    pub tentative_disposition: &'a str,
}

#[derive(Clone, Debug)]
pub struct LedgerDecision {
    pub disposition: String,
    pub identity_checks: Value,
    pub prepared_delta: Value,
}

impl LedgerDecision {
    pub fn new(
        disposition: impl Into<String>,
        identity_checks: Value,
        prepared_delta: Value,
    ) -> Self {
        Self {
            disposition: disposition.into(),
            identity_checks,
            prepared_delta,
        }
    }
}

/// The minimum explicit ledger useful to fake-authority admission tests.
/// Production requests should provide the complete AutoResearch ledger adapter
/// so all program/evidence identities and counters retain Python parity.
#[derive(Clone, Debug, Default)]
pub struct CandidateIdentityLedger {
    identity: Value,
    candidate_identities: BTreeSet<String>,
    proposal_count: u64,
    disposition_counts: BTreeMap<String, u64>,
}

impl CandidateIdentityLedger {
    pub fn new(
        identity: Value,
        candidate_identities: impl IntoIterator<Item = String>,
    ) -> Result<Self> {
        if !identity.is_object() {
            return Err(contract(
                "identity ledger authority identity must be an object",
            ));
        }
        Ok(Self {
            identity,
            candidate_identities: candidate_identities.into_iter().collect(),
            proposal_count: 0,
            disposition_counts: BTreeMap::new(),
        })
    }
}

impl IdentityLedger for CandidateIdentityLedger {
    fn identity(&self) -> &Value {
        &self.identity
    }

    fn prepare_proposal(&self, proposal: LedgerProposal<'_>) -> Result<LedgerDecision> {
        let (candidate_identity, duplicate) = match proposal.candidate {
            Some(candidate) => {
                let identity = candidate_identity(candidate)?;
                let duplicate = self.candidate_identities.contains(&identity);
                (Some(identity), duplicate)
            }
            None => (None, false),
        };
        let disposition = if proposal.tentative_disposition != "accepted" {
            proposal.tentative_disposition.to_owned()
        } else if duplicate {
            "duplicate_candidate_identity_global".to_owned()
        } else {
            "accepted".to_owned()
        };
        let checks = object([("candidateIdentity", Value::Bool(duplicate))]);
        let delta = object([
            ("proposalOrdinal", Value::from(proposal.proposal_ordinal)),
            ("disposition", Value::String(disposition.clone())),
            (
                "candidateIdentitySha256",
                candidate_identity.map(Value::String).unwrap_or(Value::Null),
            ),
            (
                "executableSemanticSha256",
                proposal
                    .executable_semantic_sha256
                    .map(|value| Value::String(value.to_owned()))
                    .unwrap_or(Value::Null),
            ),
        ]);
        Ok(LedgerDecision::new(disposition, checks, delta))
    }

    fn commit_prepared_delta(&mut self, prepared_delta: &Value) -> Result<()> {
        let fields = prepared_delta
            .as_object()
            .ok_or_else(|| contract("candidate ledger delta is invalid"))?;
        let disposition = fields
            .get("disposition")
            .and_then(Value::as_str)
            .ok_or_else(|| contract("candidate ledger delta lacks disposition"))?;
        if disposition == "accepted" {
            let identity = fields
                .get("candidateIdentitySha256")
                .and_then(Value::as_str)
                .ok_or_else(|| contract("accepted candidate ledger delta lacks identity"))?;
            sha(identity, "candidate ledger delta")?;
            self.candidate_identities.insert(identity.to_owned());
        }
        self.proposal_count = self.proposal_count.saturating_add(1);
        *self
            .disposition_counts
            .entry(disposition.to_owned())
            .or_default() += 1;
        Ok(())
    }

    fn compact_state(&self) -> Value {
        object([
            ("identity", self.identity.clone()),
            (
                "candidateIdentities",
                Value::Array(
                    self.candidate_identities
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            ),
            ("proposalCount", Value::from(self.proposal_count)),
            (
                "dispositionCounts",
                Value::Object(
                    self.disposition_counts
                        .iter()
                        .map(|(key, value)| (key.clone(), Value::from(*value)))
                        .collect(),
                ),
            ),
        ])
    }

    fn restore_compact_state(&mut self, state: &Value) -> Result<()> {
        let fields = state
            .as_object()
            .ok_or_else(|| contract("candidate ledger compact state is invalid"))?;
        let identity = fields
            .get("identity")
            .cloned()
            .ok_or_else(|| contract("candidate ledger compact state lacks identity"))?;
        if identity != self.identity {
            return Err(contract(
                "candidate ledger authority identity mismatched checkpoint",
            ));
        }
        self.candidate_identities = identities(fields, "candidateIdentities")?;
        self.proposal_count = integer(fields, "proposalCount")?;
        self.disposition_counts = counts(fields, "dispositionCounts")?;
        Ok(())
    }
}

pub struct ProposalPlanner<'parents> {
    pub schedule: ProposalSchedule,
    pub parents: &'parents mut dyn ParentSelector,
}

impl ProposalPlanner<'_> {
    pub fn plan_next(&mut self, state: &mut ProposalState) -> Result<PlannedProposal> {
        self.schedule.validate()?;
        if self.schedule.operator_family_matrix.is_some() {
            return self.plan_matrix_next(state);
        }
        let ordinal = state.next_proposal_ordinal;
        let seed = proposal_seed(&self.schedule.config_sha256, ordinal);
        let accepted_immigrants = *state
            .origin_accepted_counts
            .get("random_immigrant")
            .unwrap_or(&0);
        let accepted_offspring = state
            .origin_accepted_counts
            .iter()
            .filter(|(origin, _)| origin.as_str() != "random_immigrant")
            .map(|(_, count)| *count)
            .sum();
        let use_immigrant = scheduled_immigrant_for_accepted_quota(
            self.schedule.desired_evaluated_offspring,
            self.schedule.desired_evaluated_immigrants,
            accepted_offspring,
            accepted_immigrants,
        )
        .map_err(|error| contract(error.to_string()))?;
        let crossover = is_crossover_slot(
            use_immigrant,
            ordinal,
            self.parents.archive_cell_count(),
            self.parents.eligible_parent_count(),
        );
        let intent = if use_immigrant {
            ProposalIntent::RichImmigrant {
                long_seed: immigrant_side_seed(&seed, Side::Long),
                short_seed: immigrant_side_seed(&seed, Side::Short),
                proposal_seed: seed,
            }
        } else if crossover {
            let parent = self.select("crossover_parent", state)?;
            let mut mate = self.select("crossover_mate", state)?;
            let mut attempts = Vec::new();
            while parent.pair_identity_sha256 == mate.pair_identity_sha256
                && self.parents.eligible_parent_count() > 1
            {
                attempts.push(mate.selection_audit.clone().unwrap_or(Value::Null));
                mate = self.select(&format!("crossover_mate_retry_{}", attempts.len()), state)?;
            }
            ProposalIntent::SameSideCrossover {
                side: proposal_side(&seed).map_err(|error| contract(error.to_string()))?,
                proposal_seed: seed,
                parent,
                mate,
                mate_selection_attempts: attempts,
            }
        } else {
            let parent = self.select("mutation", state)?;
            let mutation_depth =
                mutation_depth_for_seed(&seed).map_err(|error| contract(error.to_string()))?;
            ProposalIntent::StructuralMutation {
                proposal_seed: seed,
                parent,
                mutation_depth,
                forced_operator_family: None,
            }
        };
        intent.validate()?;
        Ok(PlannedProposal {
            proposal_ordinal: ordinal,
            intent,
        })
    }

    fn select(&mut self, label: &str, state: &mut ProposalState) -> Result<ParentReference> {
        if !self.parents.has_parents() {
            return Err(ProposalError::ParentSelectorUnavailable);
        }
        let ordinal = state.structural_parent_selections;
        let parent = self.parents.select(label, ordinal)?;
        state.structural_parent_selections = state
            .structural_parent_selections
            .checked_add(1)
            .ok_or_else(|| contract("structural parent selection ordinal overflowed"))?;
        parent.validate()?;
        Ok(parent)
    }

    fn plan_matrix_next(&self, state: &mut ProposalState) -> Result<PlannedProposal> {
        let matrix = self
            .schedule
            .operator_family_matrix
            .as_ref()
            .ok_or_else(|| contract("operator-family matrix planner lost its contract"))?;
        let ordinal = state.next_proposal_ordinal;
        let slot = matrix
            .slot_at(ordinal)
            .map_err(|error| contract(error.to_string()))?
            .ok_or_else(|| {
                contract("operator-family matrix has no remaining construction slots")
            })?;
        let parent = self
            .schedule
            .matrix_parents
            .get(&slot.parent_candidate_id)
            .cloned()
            .ok_or_else(|| {
                contract(format!(
                    "operator-family matrix parent {} is not bound",
                    slot.parent_candidate_id
                ))
            })?;
        parent
            .validate()
            .map_err(|error| contract(error.to_string()))?;
        // Matrix parents are bound by ordinal, not drawn from the rotating
        // selector. Replay still credits one structural-offspring receipt per
        // committed attempt, so the compact counter must advance here without
        // mutating selector state.
        state.structural_parent_selections = state
            .structural_parent_selections
            .checked_add(1)
            .ok_or_else(|| contract("structural parent selection ordinal overflowed"))?;
        let seed = proposal_seed(&self.schedule.config_sha256, ordinal);
        let intent = ProposalIntent::StructuralMutation {
            proposal_seed: seed,
            parent,
            mutation_depth: 1,
            forced_operator_family: Some(slot.operator_family),
        };
        intent.validate().map_err(ProposalError::from)?;
        Ok(PlannedProposal {
            proposal_ordinal: ordinal,
            intent,
        })
    }
}

#[derive(Clone, Debug)]
pub struct AcceptedProposal {
    pub candidate_id: String,
    pub candidate_identity_sha256: String,
    pub executable_semantic_sha256: String,
    pub descriptor_projection: Option<Value>,
}

#[derive(Clone, Debug)]
pub struct PreparedProposal {
    pub proposal_ordinal: u64,
    pub origin_kind: String,
    pub disposition: String,
    pub entry: Value,
    pub entry_sha256: String,
    pub accepted: Option<AcceptedProposal>,
    pub ledger_delta: Value,
}

pub struct ProposalAssembler<'a> {
    pub schedule: &'a ProposalSchedule,
    pub operator_implementation_identity: &'a Value,
    pub ledger: &'a dyn IdentityLedger,
    pub g0_evaluation_width: Option<u64>,
}

impl<'a> ProposalAssembler<'a> {
    pub fn prepare(
        &self,
        state: &ProposalState,
        planned: &PlannedProposal,
        native: NativeProposal,
    ) -> Result<PreparedProposal> {
        if planned.proposal_ordinal != state.next_proposal_ordinal {
            return Err(contract("proposal ordinal does not match sealed state"));
        }
        self.schedule.validate()?;
        validate_native_proposal(&native.proposal, planned)?;
        let operator_implementation_sha256 =
            canonical_sha256(self.operator_implementation_identity)?;
        let mut entry = object([
            (
                "schemaVersion",
                Value::String(PROPOSAL_ENTRY_SCHEMA.to_owned()),
            ),
            (
                "configSha256",
                Value::String(self.schedule.config_sha256.clone()),
            ),
            (
                "generationIndex",
                Value::from(self.schedule.generation_index),
            ),
            ("proposalOrdinal", Value::from(planned.proposal_ordinal)),
            (
                "originKind",
                Value::String(planned.intent.origin_kind().to_owned()),
            ),
            ("proposal", native.proposal),
            (
                "operatorImplementationSha256",
                Value::String(operator_implementation_sha256),
            ),
        ]);
        if let Some(scope) = native.predeclared_lake_scope {
            entry
                .as_object_mut()
                .expect("proposal entry is object")
                .insert("predeclaredLakeScope".to_owned(), scope);
        }

        let funnel_material = native.funnel_material.clone();
        let mut accepted = None;
        let ledger_delta;
        let disposition = match (native.candidate, native.executable_semantic_sha256) {
            (None, None) => {
                let tentative = proposal_disposition(entry.as_object().expect("entry is object"))?;
                let decision = self.ledger.prepare_proposal(LedgerProposal {
                    proposal_ordinal: planned.proposal_ordinal,
                    candidate: None,
                    executable_semantic_sha256: None,
                    tentative_disposition: &tentative,
                })?;
                // A non-materialized operation rejection has no candidate
                // identity to publish.  Python still prepares and commits the
                // ledger delta for durable ordinal/disposition accounting, but
                // omits identity-check evidence from the public journal.
                ledger_delta = decision.prepared_delta;
                decision.disposition
            }
            (Some(candidate), Some(executable_semantic_sha256)) => {
                sha(
                    &executable_semantic_sha256,
                    "executable pair semantic identity",
                )?;
                validate_materialized_candidate(
                    &candidate,
                    entry
                        .as_object()
                        .expect("proposal entry is object")
                        .get("proposal")
                        .expect("proposal entry has native proposal"),
                )?;
                let identity = candidate_identity(&candidate)?;
                let id = candidate_id(&candidate)?;
                let scope_rejected =
                    !scope_acceptable(entry.as_object().expect("entry is object"))?;
                let tentative = if scope_rejected {
                    "predeclared_lake_scope_rejected".to_owned()
                } else if state
                    .local_executable_semantics
                    .contains(&executable_semantic_sha256)
                {
                    "duplicate_pair_genome".to_owned()
                } else if state.local_candidate_identities.contains(&identity) {
                    "duplicate_candidate_identity".to_owned()
                } else {
                    "accepted".to_owned()
                };
                let decision = self.ledger.prepare_proposal(LedgerProposal {
                    proposal_ordinal: planned.proposal_ordinal,
                    candidate: Some(&candidate),
                    executable_semantic_sha256: Some(&executable_semantic_sha256),
                    tentative_disposition: &tentative,
                })?;
                // Python resolves scope and generation-local duplicate checks
                // before consulting the durable identity ledger.  Those early
                // rejections still commit a ledger receipt, but they do not
                // publish identity-check evidence.  Only proposals that reach
                // the ledger duplicate check expose its result (including
                // global duplicate rejections).
                if tentative == "accepted" {
                    entry
                        .as_object_mut()
                        .expect("proposal entry is object")
                        .insert("identityChecks".to_owned(), decision.identity_checks);
                }
                ledger_delta = decision.prepared_delta;
                if decision.disposition == "accepted" {
                    accepted = Some(AcceptedProposal {
                        candidate_id: id,
                        candidate_identity_sha256: identity,
                        executable_semantic_sha256,
                        descriptor_projection: None,
                    });
                    entry
                        .as_object_mut()
                        .expect("proposal entry is object")
                        .insert("candidate".to_owned(), candidate);
                }
                decision.disposition
            }
            _ => {
                return Err(contract(
                    "native proposal supplied only one of candidate/executable semantic identity",
                ));
            }
        };
        entry
            .as_object_mut()
            .expect("proposal entry is object")
            .insert("disposition".to_owned(), Value::String(disposition.clone()));
        if disposition == "accepted" {
            if self.g0_evaluation_width.is_some() {
                entry = project_g0_accepted_entry(&entry)?;
                let candidate = entry
                    .as_object()
                    .and_then(|fields| fields.get("candidate"))
                    .ok_or_else(|| contract("G0 projection lacks candidate"))?;
                let accepted = accepted
                    .as_mut()
                    .ok_or_else(|| contract("accepted entry lacks accepted reference"))?;
                accepted.candidate_id = candidate_id(candidate)?;
                accepted.candidate_identity_sha256 = candidate_identity(candidate)?;
            }
            if let Some(funnel_material) = funnel_material {
                let funnel = funnel_candidate(&entry, &funnel_material)?;
                entry
                    .as_object_mut()
                    .expect("proposal entry is object")
                    .insert("funnelCandidate".to_owned(), funnel);
            }
        }
        let entry_sha256 = canonical_sha256_streaming(&entry)?;
        entry
            .as_object_mut()
            .expect("proposal entry is object")
            .insert(
                "entrySha256".to_owned(),
                Value::String(entry_sha256.clone()),
            );
        if let Some(accepted) = accepted.as_mut() {
            if self.g0_evaluation_width.is_some() {
                accepted.descriptor_projection = Some(
                    crate::g0::derive_descriptor_projection_from_rich_entry(&entry)
                        .map_err(|error| contract(error.to_string()))?,
                );
            }
        }
        Ok(PreparedProposal {
            proposal_ordinal: planned.proposal_ordinal,
            origin_kind: planned.intent.origin_kind().to_owned(),
            disposition,
            entry,
            entry_sha256,
            accepted,
            ledger_delta,
        })
    }
}

fn validate_native_proposal(proposal: &Value, planned: &PlannedProposal) -> Result<()> {
    let fields = proposal
        .as_object()
        .ok_or_else(|| contract("native proposal must be an object"))?;
    if fields.get("schemaVersion").and_then(Value::as_str) != Some("temporal_qd_pair_proposal_v2") {
        return Err(contract("native proposal schema is incompatible"));
    }
    let supplied = fields
        .get("proposalSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| contract("native proposal lacks proposalSha256"))?;
    sha(supplied, "native proposal SHA-256")?;
    if canonical_sha256_without_object_field(proposal, "proposalSha256")? != supplied {
        return Err(contract("native proposal identity mismatch"));
    }
    if fields.get("proposalSeed").and_then(Value::as_str) != Some(planned.intent.proposal_seed())
        || fields.get("originKind").and_then(Value::as_str) != Some(planned.intent.origin_kind())
    {
        return Err(contract(
            "native proposal seed/origin diverged from its scheduled intent",
        ));
    }
    Ok(())
}

fn validate_materialized_candidate(candidate: &Value, proposal: &Value) -> Result<()> {
    let fields = candidate
        .as_object()
        .ok_or_else(|| contract("native candidate must be an object"))?;
    if fields.get("pairProposal") != Some(proposal) {
        return Err(contract(
            "native candidate pairProposal does not bind its proposal",
        ));
    }
    let proposal_sha256 = proposal
        .as_object()
        .and_then(|fields| fields.get("proposalSha256"))
        .and_then(Value::as_str)
        .ok_or_else(|| contract("native proposal lacks proposal identity"))?;
    if fields.get("pairProposalSha256").and_then(Value::as_str) != Some(proposal_sha256) {
        return Err(contract(
            "native candidate pairProposalSha256 does not bind its proposal",
        ));
    }
    let material = fields
        .get("candidateIdentityMaterial")
        .ok_or_else(|| contract("native candidate lacks candidate identity material"))?;
    let identity = candidate_identity(candidate)?;
    if canonical_sha256(material)? != identity {
        return Err(contract(
            "native candidate identity does not bind its identity material",
        ));
    }
    let expected_id = format!("qd_{}", &identity[7..35]);
    if candidate_id(candidate)? != expected_id {
        return Err(contract("native candidate ID does not bind its identity"));
    }
    Ok(())
}

fn project_g0_accepted_entry(entry: &Value) -> Result<Value> {
    let fields = entry
        .as_object()
        .ok_or_else(|| contract("G0 projection entry must be an object"))?;
    if fields.get("disposition").and_then(Value::as_str) != Some("accepted") {
        return Err(contract(
            "G0 projection requires an accepted proposal entry",
        ));
    }
    let mut proposal = fields
        .get("proposal")
        .cloned()
        .ok_or_else(|| contract("G0 projection lacks proposal"))?;
    proposal
        .as_object_mut()
        .ok_or_else(|| contract("G0 projection proposal must be an object"))?
        .remove("proposalSha256");
    let proposal_sha256 = canonical_sha256(&proposal)?;
    proposal
        .as_object_mut()
        .expect("checked G0 projection proposal")
        .insert(
            "proposalSha256".to_owned(),
            Value::String(proposal_sha256.clone()),
        );

    let mut candidate = fields
        .get("candidate")
        .cloned()
        .ok_or_else(|| contract("G0 projection lacks candidate"))?;
    let candidate_fields = candidate
        .as_object_mut()
        .ok_or_else(|| contract("G0 projection candidate must be an object"))?;
    candidate_fields.remove("canonicalEvidenceIdentitySha256");
    candidate_fields.insert("pairProposal".to_owned(), proposal.clone());
    candidate_fields.insert(
        "pairProposalSha256".to_owned(),
        Value::String(proposal_sha256.clone()),
    );
    let mut material = candidate_fields
        .get("candidateIdentityMaterial")
        .cloned()
        .ok_or_else(|| contract("G0 projection lacks candidate identity material"))?;
    material
        .as_object_mut()
        .ok_or_else(|| contract("G0 candidate identity material must be an object"))?
        .insert(
            "materializedPairProposalSha256".to_owned(),
            Value::String(proposal_sha256),
        );
    let identity = canonical_sha256(&material)?;
    let candidate_id = format!("qd_{}", &identity[7..35]);
    candidate_fields.insert("candidateIdentityMaterial".to_owned(), material);
    candidate_fields.insert(
        "candidateIdentitySha256".to_owned(),
        Value::String(identity.clone()),
    );
    candidate_fields.insert(
        "candidateId".to_owned(),
        Value::String(candidate_id.clone()),
    );
    let lineage = candidate_fields
        .get_mut("lineage")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| contract("G0 projection lacks candidate lineage"))?;
    lineage.insert("candidateId".to_owned(), Value::String(candidate_id));
    lineage.insert(
        "candidateIdentitySha256".to_owned(),
        Value::String(identity),
    );

    let mut projected = fields.clone();
    for field in [
        "entrySha256",
        "identityChecks",
        "predeclaredLakeScope",
        "funnelCandidate",
        "candidate",
        "proposal",
    ] {
        projected.remove(field);
    }
    projected.insert("proposal".to_owned(), proposal);
    projected.insert("candidate".to_owned(), candidate);
    Ok(Value::Object(projected))
}

fn funnel_candidate(
    entry: &Value,
    material: &crate::factory::NativeFunnelMaterial,
) -> Result<Value> {
    let candidate = entry
        .as_object()
        .and_then(|fields| fields.get("candidate"))
        .and_then(Value::as_object)
        .ok_or_else(|| contract("funnel audit requires an accepted candidate"))?;
    let candidate_id = candidate
        .get("candidateId")
        .and_then(Value::as_str)
        .ok_or_else(|| contract("funnel audit candidate lacks candidate ID"))?;
    Ok(object([
        (
            "schemaVersion",
            Value::String("temporal_qd_proposal_funnel_stage_v1".to_owned()),
        ),
        ("candidateId", Value::String(candidate_id.to_owned())),
        (
            "rawSourceProfileSha256",
            Value::String(material.raw_source_profile_sha256.clone()),
        ),
        (
            "staticReachability",
            object([
                ("outcome", Value::String("reachable".to_owned())),
                ("reasons", Value::Array(Vec::new())),
            ]),
        ),
        (
            "nativeValidation",
            object([
                ("outcome", Value::String("valid".to_owned())),
                ("reasons", Value::Array(Vec::new())),
                (
                    "resolvedProfileSha256",
                    Value::String(material.resolved_profile_sha256.clone()),
                ),
                (
                    "programSha256",
                    Value::String(material.program_sha256.clone()),
                ),
                (
                    "validationReportSha256",
                    Value::String(material.validation_report_sha256.clone()),
                ),
            ]),
        ),
        (
            "admission",
            object([
                ("outcome", Value::String("admitted".to_owned())),
                ("reasons", Value::Array(Vec::new())),
                (
                    "canonicalEvidenceIdentitySha256",
                    candidate
                        .get("canonicalEvidenceIdentitySha256")
                        .cloned()
                        .unwrap_or(Value::Null),
                ),
            ]),
        ),
    ]))
}

fn proposal_disposition(entry: &Map<String, Value>) -> Result<String> {
    entry
        .get("proposal")
        .and_then(Value::as_object)
        .and_then(|proposal| proposal.get("disposition"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| contract("rejected native proposal lacks proposal disposition"))
}

fn scope_acceptable(entry: &Map<String, Value>) -> Result<bool> {
    let Some(scope) = entry.get("predeclaredLakeScope") else {
        return Ok(true);
    };
    scope
        .as_object()
        .and_then(|scope| scope.get("acceptable"))
        .and_then(Value::as_bool)
        .ok_or_else(|| contract("predeclared lake scope lacks boolean acceptable"))
}

fn integer(fields: &Map<String, Value>, field: &str) -> Result<u64> {
    fields
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| contract(format!("proposal state lacks unsigned {field}")))
}

fn identities(fields: &Map<String, Value>, field: &str) -> Result<BTreeSet<String>> {
    Ok(identity_list(fields, field)?.into_iter().collect())
}

fn identity_list(fields: &Map<String, Value>, field: &str) -> Result<Vec<String>> {
    fields
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| contract(format!("proposal state lacks {field}")))?
        .iter()
        .map(|value| {
            let value = value
                .as_str()
                .ok_or_else(|| contract(format!("proposal state {field} is invalid")))?
                .to_owned();
            sha(&value, field)?;
            Ok(value)
        })
        .collect()
}

fn counts(fields: &Map<String, Value>, field: &str) -> Result<BTreeMap<String, u64>> {
    fields
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| contract(format!("proposal state lacks {field}")))?
        .iter()
        .map(|(key, value)| {
            let value = value
                .as_u64()
                .ok_or_else(|| contract(format!("proposal state {field} is invalid")))?;
            Ok((key.clone(), value))
        })
        .collect()
}
