//! Write-neutral, native later-generation v5 proposal transaction.
//!
//! This module owns the part of later-generation construction which must not
//! be delegated to a caller: accepted-slot scheduling, parent draw ordering,
//! retry accounting, compact every-attempt evidence, and identity-ledger
//! admission.  The sealed v5 core owns the pair compiler and rich static
//! authority reconstruction; its narrow materializer/recompiler seam is used
//! below rather than admitting a fixture, Python process, Dashboard bridge,
//! or a caller-provided candidate.

use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use temporal_qd_contract::{
    ContractError, Map, NativeProgressHandle, NativeProgressSection, Value, canonical_sha256,
};

use crate::{
    factory::{ParentReference, ProposalIntent},
    identity::{Side, immigrant_side_seed},
    proposal::{
        AcceptedProposal, IdentityLedger, LedgerProposal, ParentSelector, PlannedProposal,
        ProposalError, ProposalPlanner, ProposalSchedule, ProposalState,
    },
    schedule::{RotatingParentSchedule, accepted_quota_immigrant_count},
    v5::{
        V5AttemptJournal, V5AttemptLineageRefs, V5AttemptOutcomeAudit, V5AttemptParentReference,
        V5CompactAcceptedRecord, V5Error, V5EvolvedAcceptedBuildInput,
        V5EvolvedAcceptedMaterial as V5CoreEvolvedAcceptedMaterial, V5EvolvedBuildKind,
        V5EvolvedParentMaterial, V5EvolvedParentSnapshot, V5ProposalAttemptRecord,
        V5SealedEvolvedPairRecompiler, V5SharedConstructionAuthority,
        build_v5_evolved_accepted_material, build_v5_evolved_immigrant_programs,
        load_v5_evolved_parent, load_v5_evolved_parent_from_snapshot,
        parent_reference_from_v5_evolved_material,
    },
    v5_operators::{
        V5AdmittedEvolvedOperatorSelection, V5EvolvedChildAdmission, V5EvolvedOperatorDelta,
        V5EvolvedOperatorExecution, V5EvolvedPairStepResult, V5EvolvedSameSideCrossoverExecution,
        V5EvolvedSideState, V5OperatorDisposition, attempt_evolved_same_side_crossover_from_states,
        execute_admitted_evolved_operator_selection,
        execute_admitted_evolved_operator_step_from_state,
        execute_evolved_operator_selection_with_admission, proposal_side_for_seed,
        select_admitted_evolved_operator_choice_from_state,
    },
};

#[derive(Default)]
struct V5EvolvedAdmissionTelemetry {
    vocabulary_enumerations: AtomicU64,
    speculative_full_pair_admissions: AtomicU64,
    changed_side_probes: AtomicU64,
    selected_rebuilds: AtomicU64,
    fallback_sweeps: AtomicU64,
    changed_side_probe_nanos: AtomicU64,
    rejection_histogram: Mutex<BTreeMap<String, u64>>,
}

impl V5EvolvedAdmissionTelemetry {
    fn classify_rejection(error: &str) -> &'static str {
        if error.contains("semantic.guard_depth_exceeded") {
            "guardDepthExceeded"
        } else if error.contains("semantic.action_cooldown_wrong_occurrence") {
            "actionCooldownWrongOccurrence"
        } else if error.contains("semantic.action_cooldown_wrong_event_class") {
            "actionCooldownWrongEventClass"
        } else if error.contains("semantic.action_cooldown_unknown_action") {
            "actionCooldownUnknownAction"
        } else if error.contains("semantic.action_cooldown_terminal_action") {
            "actionCooldownTerminalAction"
        } else {
            "other"
        }
    }

    fn counters(&self) -> BTreeMap<String, u64> {
        let mut counters = BTreeMap::from([
            (
                "vocabularyEnumerations".to_owned(),
                self.vocabulary_enumerations.load(Ordering::Relaxed),
            ),
            (
                "speculativeFullPairAdmissions".to_owned(),
                self.speculative_full_pair_admissions
                    .load(Ordering::Relaxed),
            ),
            (
                "changedSideProbes".to_owned(),
                self.changed_side_probes.load(Ordering::Relaxed),
            ),
            (
                "selectedRebuilds".to_owned(),
                self.selected_rebuilds.load(Ordering::Relaxed),
            ),
            (
                "fallbackSweeps".to_owned(),
                self.fallback_sweeps.load(Ordering::Relaxed),
            ),
        ]);
        if let Ok(histogram) = self.rejection_histogram.lock() {
            for (reason, count) in histogram.iter() {
                counters.insert(format!("rejection.{reason}"), *count);
            }
        }
        counters
    }
}

/// Per-construction telemetry shard. Candidate admission is hot and runs in
/// bounded parallel workers, so recording directly into shared atomics (and a
/// shared histogram mutex) materially perturbs the work being measured. Each
/// proposal accumulates locally and merges once on drop, including all early
/// return paths. The entire shard is absent when progress telemetry is off.
struct V5EvolvedAdmissionSample<'a> {
    aggregate: &'a V5EvolvedAdmissionTelemetry,
    vocabulary_enumerations: Cell<u64>,
    speculative_full_pair_admissions: Cell<u64>,
    changed_side_probes: Cell<u64>,
    selected_rebuilds: Cell<u64>,
    fallback_sweeps: Cell<u64>,
    changed_side_probe_nanos: Cell<u64>,
    rejection_histogram: RefCell<BTreeMap<&'static str, u64>>,
}

impl<'a> V5EvolvedAdmissionSample<'a> {
    fn new(aggregate: &'a V5EvolvedAdmissionTelemetry) -> Self {
        Self {
            aggregate,
            vocabulary_enumerations: Cell::new(0),
            speculative_full_pair_admissions: Cell::new(0),
            changed_side_probes: Cell::new(0),
            selected_rebuilds: Cell::new(0),
            fallback_sweeps: Cell::new(0),
            changed_side_probe_nanos: Cell::new(0),
            rejection_histogram: RefCell::new(BTreeMap::new()),
        }
    }

    fn increment(cell: &Cell<u64>) {
        cell.set(cell.get().saturating_add(1));
    }

    fn observe_rejection(&self, error: &str) {
        let reason = V5EvolvedAdmissionTelemetry::classify_rejection(error);
        let mut histogram = self.rejection_histogram.borrow_mut();
        let count = histogram.entry(reason).or_default();
        *count = count.saturating_add(1);
    }

    fn add_probe_wall(&self, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.changed_side_probe_nanos
            .set(self.changed_side_probe_nanos.get().saturating_add(nanos));
    }
}

impl Drop for V5EvolvedAdmissionSample<'_> {
    fn drop(&mut self) {
        for (target, source) in [
            (
                &self.aggregate.vocabulary_enumerations,
                &self.vocabulary_enumerations,
            ),
            (
                &self.aggregate.speculative_full_pair_admissions,
                &self.speculative_full_pair_admissions,
            ),
            (
                &self.aggregate.changed_side_probes,
                &self.changed_side_probes,
            ),
            (&self.aggregate.selected_rebuilds, &self.selected_rebuilds),
            (&self.aggregate.fallback_sweeps, &self.fallback_sweeps),
            (
                &self.aggregate.changed_side_probe_nanos,
                &self.changed_side_probe_nanos,
            ),
        ] {
            target.fetch_add(source.get(), Ordering::Relaxed);
        }
        if let Ok(mut aggregate) = self.aggregate.rejection_histogram.lock() {
            for (reason, count) in self.rejection_histogram.get_mut().iter() {
                let aggregate_count = aggregate.entry((*reason).to_owned()).or_default();
                *aggregate_count = aggregate_count.saturating_add(*count);
            }
        }
    }
}

struct V5InstrumentedChildAdmission<'sample, 'aggregate> {
    inner: &'sample V5SealedEvolvedPairRecompiler,
    telemetry: &'sample V5EvolvedAdmissionSample<'aggregate>,
}

impl V5EvolvedChildAdmission for V5InstrumentedChildAdmission<'_, '_> {
    fn admit_evolved_child(
        &self,
        operator_id: &str,
        child_program: &Value,
    ) -> crate::v5_operators::Result<()> {
        V5EvolvedAdmissionSample::increment(&self.telemetry.changed_side_probes);
        let started = Instant::now();
        let result = self.inner.admit_evolved_child(operator_id, child_program);
        self.telemetry.add_probe_wall(started.elapsed());
        if let Err(error) = &result {
            self.telemetry.observe_rejection(&error.to_string());
        }
        result
    }
}

struct V5FullPairChildAdmission<'sample, 'aggregate> {
    inner: &'sample V5SealedEvolvedPairRecompiler,
    telemetry: Option<&'sample V5EvolvedAdmissionSample<'aggregate>>,
}

impl V5EvolvedChildAdmission for V5FullPairChildAdmission<'_, '_> {
    fn admit_evolved_child(
        &self,
        operator_id: &str,
        child_program: &Value,
    ) -> crate::v5_operators::Result<()> {
        if let Some(telemetry) = self.telemetry {
            V5EvolvedAdmissionSample::increment(&telemetry.speculative_full_pair_admissions);
        }
        let result = self
            .inner
            .admit_evolved_child_full_pair(operator_id, child_program)
            .map_err(|error| crate::v5_operators::V5OperatorError::Invalid(error.to_string()));
        if let (Some(telemetry), Err(error)) = (self.telemetry, &result) {
            telemetry.observe_rejection(&error.to_string());
        }
        result
    }
}

/// The immutable schema for a compact later-generation transaction result.
pub const V5_EVOLVED_TRANSACTION_SCHEMA: &str = "temporal_qd_v5_evolved_transaction_v1";
/// The exact all-attempt delta retained for a later-generation proposal.
pub const V5_EVOLVED_PROPOSAL_DELTA_SCHEMA: &str = "temporal_qd_v5_evolved_proposal_delta_v1";
/// Semantic root for the non-null all-attempt delta stream.
pub const V5_EVOLVED_DELTA_JOURNAL_SCHEMA: &str = "temporal_qd_v5_evolved_delta_journal_v1";
/// Semantic root for compact accepted records in a later generation.
pub const V5_EVOLVED_COMPACT_JOURNAL_SCHEMA: &str =
    "temporal_qd_v5_evolved_compact_accepted_journal_v1";
/// Exact selection receipt retained for every structural attempt, including
/// a rejection before any operator can be selected.
pub const V5_EVOLVED_PARENT_SELECTION_RECEIPT_SCHEMA: &str =
    "temporal_qd_v5_evolved_parent_selection_receipt_v1";
/// Receipt binding the scheduler and injected archive/ledger state.
pub const V5_EVOLVED_SCHEDULE_RECEIPT_SCHEMA: &str =
    "temporal_qd_v5_evolved_schedule_state_receipt_v1";
/// Self-hashed, ordered terminal trace for a native structural attempt.
pub const V5_EVOLVED_OPERATOR_TRACE_SCHEMA: &str = "temporal_qd_v5_evolved_operator_trace_v1";
/// Typed terminal evidence used when selection/application/recompilation
/// reaches a deterministic rejection rather than a child record.
pub const V5_EVOLVED_OPERATOR_TERMINAL_SCHEMA: &str = "temporal_qd_v5_evolved_operator_terminal_v1";
/// Self-contained, content-addressed inventory of the exact parent objects
/// required to replay later-generation structural attempts without reopening
/// a prior-generation archive file.
pub const V5_EVOLVED_PARENT_SNAPSHOT_INVENTORY_SCHEMA: &str =
    "temporal_qd_v5_evolved_parent_snapshot_inventory_v1";
/// Per-attempt references into the deterministic parent snapshot inventory.
pub const V5_EVOLVED_ATTEMPT_SNAPSHOT_REFS_SCHEMA: &str =
    "temporal_qd_v5_evolved_attempt_snapshot_refs_v1";

/// Immediate constructor verification policy. This is operational execution
/// policy only: neither variant participates in proposal, transaction, or
/// population identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum V5ImmediateConstructionReplayPolicy {
    /// Durable/default behavior: independently reconstruct every proposal
    /// before duplicate and ledger admission.
    IndependentReplayV1,
    /// Fast-ephemeral behavior: defer the independent reconstruction to the
    /// mandatory publication replay which runs before the completion marker.
    DeferredToFastEphemeralPublicationV1,
}

impl V5ImmediateConstructionReplayPolicy {
    fn telemetry_section_name(self) -> &'static str {
        match self {
            Self::IndependentReplayV1 => "evolved_immediate_replay_independent_v1",
            Self::DeferredToFastEphemeralPublicationV1 => {
                "evolved_immediate_replay_deferred_to_publication_v1"
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum V5EvolvedTransactionError {
    #[error("v5 construction failure: {0}")]
    V5(#[from] V5Error),
    #[error("proposal schedule failure: {0}")]
    Proposal(#[from] ProposalError),
    #[error("canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("v5 evolved transaction contract failure: {0}")]
    Contract(String),
}

pub type Result<T> = std::result::Result<T, V5EvolvedTransactionError>;

fn contract(message: impl Into<String>) -> V5EvolvedTransactionError {
    V5EvolvedTransactionError::Contract(message.into())
}

fn object(rows: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut map = Map::new();
    for (key, value) in rows {
        map.insert(key.to_owned(), value);
    }
    Value::Object(map)
}

fn exact_sha(value: &Value, label: &str) -> Result<String> {
    let value = value
        .as_str()
        .ok_or_else(|| contract(format!("{label} must be a lowercase SHA-256 identity")))?;
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
    Ok(value.to_owned())
}

fn exact_sha_string(value: &str, label: &str) -> Result<String> {
    exact_sha(&Value::String(value.to_owned()), label)
}

fn exact_text(value: &Value, label: &str) -> Result<String> {
    value
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| contract(format!("{label} must be nonempty text")))
}

fn required<'a>(value: &'a Value, key: &str, label: &str) -> Result<&'a Value> {
    value
        .as_object()
        .and_then(|fields| fields.get(key))
        .ok_or_else(|| contract(format!("{label} lacks {key}")))
}

fn exact_keys(fields: &Map<String, Value>, keys: &[&str], label: &str) -> Result<()> {
    let actual = fields.keys().map(String::as_str).collect::<Vec<_>>();
    let mut expected = keys.to_vec();
    expected.sort_unstable();
    if actual != expected {
        return Err(contract(format!("{label} fields are not exact")));
    }
    Ok(())
}

fn nullable_value(value: Option<&Value>) -> Value {
    value.cloned().unwrap_or(Value::Null)
}

fn nullable_sha(value: Option<&str>) -> Value {
    value
        .map(|value| Value::String(value.to_owned()))
        .unwrap_or(Value::Null)
}

fn optional_value(value: &Value, label: &str) -> Result<Option<Value>> {
    if value.is_null() {
        Ok(None)
    } else if value.is_object() || value.is_array() {
        Ok(Some(value.clone()))
    } else {
        Err(contract(format!("{label} must be object/array or null")))
    }
}

fn optional_parent(value: &Value, label: &str) -> Result<Option<V5AttemptParentReference>> {
    if value.is_null() {
        Ok(None)
    } else {
        V5AttemptParentReference::from_value(value)
            .map(Some)
            .map_err(|error| contract(format!("{label}: {error}")))
    }
}

fn optional_u8(value: &Value, label: &str) -> Result<Option<u8>> {
    if value.is_null() {
        return Ok(None);
    }
    let value = value
        .as_u64()
        .ok_or_else(|| contract(format!("{label} must be an unsigned integer or null")))?;
    u8::try_from(value)
        .map(Some)
        .map_err(|_| contract(format!("{label} exceeds u8")))
}

fn stable_code(value: &str, label: &str) -> Result<String> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(contract(format!("{label} must be a stable lowercase code")));
    }
    Ok(value.to_owned())
}

/// Exact parent-draw evidence for a structural proposal.  The planner's mate
/// redraws are part of deterministic state, so retaining just the final two
/// parent references would make a resumed crossover consume a different
/// selector ordinal.  This receipt closes that gap even when no operator plan
/// is available yet.
#[derive(Clone, Debug, PartialEq)]
pub struct V5EvolvedParentSelectionReceipt {
    pub scheduled_kind: String,
    pub parent: V5AttemptParentReference,
    pub mate: Option<V5AttemptParentReference>,
    pub parent_selection_audit: Value,
    pub mate_selection_audit: Option<Value>,
    pub mate_selection_attempts: Vec<Value>,
}

impl V5EvolvedParentSelectionReceipt {
    fn selection_count(&self) -> Result<u64> {
        match self.scheduled_kind.as_str() {
            "structural_offspring" => Ok(1),
            "same_side_crossover" => 2_u64
                .checked_add(self.mate_selection_attempts.len() as u64)
                .ok_or_else(|| contract("v5 evolved parent selection count overflowed")),
            _ => Err(contract(
                "v5 evolved parent selection receipt scheduled kind is invalid",
            )),
        }
    }

    fn semantic_value(&self) -> Result<Value> {
        let scheduled_kind = stable_code(
            &self.scheduled_kind,
            "v5 evolved parent selection receipt scheduled kind",
        )?;
        let parent = self.parent.to_value()?;
        let mate = match &self.mate {
            Some(mate) => mate.to_value()?,
            None => Value::Null,
        };
        let (mate_selection_audit, mate_selection_attempts) = match scheduled_kind.as_str() {
            "structural_offspring" => {
                if self.mate.is_some()
                    || self.mate_selection_audit.is_some()
                    || !self.mate_selection_attempts.is_empty()
                {
                    return Err(contract(
                        "v5 evolved mutation selection receipt carries crossover-only facts",
                    ));
                }
                (Value::Null, Value::Array(Vec::new()))
            }
            "same_side_crossover" => {
                if self.mate.is_none() || self.mate_selection_audit.is_none() {
                    return Err(contract(
                        "v5 evolved crossover selection receipt lacks final mate evidence",
                    ));
                }
                (
                    self.mate_selection_audit.clone().unwrap_or(Value::Null),
                    Value::Array(self.mate_selection_attempts.clone()),
                )
            }
            _ => {
                return Err(contract(
                    "v5 evolved parent selection receipt scheduled kind is invalid",
                ));
            }
        };
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_PARENT_SELECTION_RECEIPT_SCHEMA.to_owned()),
            ),
            ("scheduledKind", Value::String(scheduled_kind)),
            ("parent", parent),
            ("mate", mate),
            ("parentSelectionAudit", self.parent_selection_audit.clone()),
            ("mateSelectionAudit", mate_selection_audit),
            ("mateSelectionAttempts", mate_selection_attempts),
            ("selectionCount", Value::from(self.selection_count()?)),
        ]))
    }

    pub fn receipt_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 evolved parent selection receipt")
            .clone();
        fields.insert(
            "selectionSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = value
            .as_object()
            .ok_or_else(|| contract("v5 evolved parent selection receipt must be an object"))?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "scheduledKind",
                "parent",
                "mate",
                "parentSelectionAudit",
                "mateSelectionAudit",
                "mateSelectionAttempts",
                "selectionCount",
                "selectionSha256",
            ],
            "v5 evolved parent selection receipt",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_PARENT_SELECTION_RECEIPT_SCHEMA)
        {
            return Err(contract(
                "v5 evolved parent selection receipt schema is invalid",
            ));
        }
        let mate_selection_attempts = required(
            value,
            "mateSelectionAttempts",
            "v5 evolved parent selection receipt",
        )?
        .as_array()
        .ok_or_else(|| {
            contract("v5 evolved parent selection receipt mate attempts must be an array")
        })?
        .clone();
        let scheduled_kind = exact_text(
            required(
                value,
                "scheduledKind",
                "v5 evolved parent selection receipt",
            )?,
            "v5 evolved parent selection receipt scheduled kind",
        )?;
        let mate_selection_audit = required(
            value,
            "mateSelectionAudit",
            "v5 evolved parent selection receipt",
        )?
        .clone();
        let receipt = Self {
            scheduled_kind: scheduled_kind.clone(),
            parent: V5AttemptParentReference::from_value(required(
                value,
                "parent",
                "v5 evolved parent selection receipt",
            )?)?,
            mate: optional_parent(
                required(value, "mate", "v5 evolved parent selection receipt")?,
                "v5 evolved parent selection receipt mate",
            )?,
            parent_selection_audit: required(
                value,
                "parentSelectionAudit",
                "v5 evolved parent selection receipt",
            )?
            .clone(),
            // A crossover from `ExplicitParentRing` has no optional archive
            // audit, but its canonical receipt must still retain the null as
            // final mate-selection evidence.  Dropping it here made the
            // parser reject a byte-identical receipt it had just emitted.
            mate_selection_audit: (scheduled_kind == "same_side_crossover")
                .then_some(mate_selection_audit),
            mate_selection_attempts,
        };
        let selection_count = required(
            value,
            "selectionCount",
            "v5 evolved parent selection receipt",
        )?
        .as_u64()
        .ok_or_else(|| contract("v5 evolved parent selection receipt count is invalid"))?;
        if selection_count != receipt.selection_count()? {
            return Err(contract(
                "v5 evolved parent selection receipt count drifted",
            ));
        }
        let supplied = exact_sha(
            required(
                value,
                "selectionSha256",
                "v5 evolved parent selection receipt",
            )?,
            "v5 evolved parent selection receipt SHA-256",
        )?;
        if supplied != receipt.receipt_sha256()? || &receipt.to_value()? != value {
            return Err(contract(
                "v5 evolved parent selection receipt identity drifted",
            ));
        }
        Ok(receipt)
    }
}

/// Compact, complete deterministic construction evidence for one attempt.
///
/// A successful record references this object by its self hash.  Rejections,
/// no-ops, and duplicate rejections retain this object too once a deterministic
/// parent/plan trace exists, so a restart cannot reseed or erase a failed
/// proposal's evidence.
#[derive(Clone, Debug, PartialEq)]
pub struct V5EvolvedProposalDelta {
    pub generation_index: u64,
    pub proposal_ordinal: u64,
    pub generation_config_sha256: String,
    pub shared_authority_sha256: String,
    pub proposal_seed: String,
    pub origin_kind: String,
    pub scheduled_kind: String,
    pub parent: Option<V5AttemptParentReference>,
    pub mate: Option<V5AttemptParentReference>,
    pub parent_selection_receipt: Option<Value>,
    pub mutation_depth: Option<u8>,
    pub long_program: Value,
    pub long_program_sha256: String,
    pub short_program: Value,
    pub short_program_sha256: String,
    /// Ordered per-step selection/application/recompile evidence.  Each item
    /// is a closed, exact JSON value produced by the operator/recompiler seam.
    pub steps: Vec<Value>,
    pub terminal_operator_plan: Option<Value>,
    pub terminal_operator_application: Option<Value>,
    pub terminal_operator_trace: Option<Value>,
    pub terminal_disposition: String,
    pub terminal_reason_code: String,
}

impl V5EvolvedProposalDelta {
    fn semantic_value(&self) -> Result<Value> {
        if self.generation_index < 2 {
            return Err(contract(
                "v5 evolved proposal delta requires generation index at least two",
            ));
        }
        let generation_config_sha256 = exact_sha_string(
            &self.generation_config_sha256,
            "v5 evolved delta generation config SHA-256",
        )?;
        let shared_authority_sha256 = exact_sha_string(
            &self.shared_authority_sha256,
            "v5 evolved delta shared authority SHA-256",
        )?;
        let proposal_seed =
            exact_sha_string(&self.proposal_seed, "v5 evolved delta proposal seed")?;
        if proposal_seed
            != crate::v5::v5_proposal_seed(&generation_config_sha256, self.proposal_ordinal)?
        {
            return Err(contract(
                "v5 evolved proposal delta seed does not bind generation config and ordinal",
            ));
        }
        let origin_kind = stable_code(&self.origin_kind, "v5 evolved delta origin kind")?;
        let scheduled_kind = stable_code(&self.scheduled_kind, "v5 evolved delta scheduled kind")?;
        let terminal_disposition = stable_code(
            &self.terminal_disposition,
            "v5 evolved delta terminal disposition",
        )?;
        let terminal_reason_code = stable_code(
            &self.terminal_reason_code,
            "v5 evolved delta terminal reason code",
        )?;
        if !matches!(
            origin_kind.as_str(),
            "random_immigrant" | "structural_offspring"
        ) || !matches!(
            scheduled_kind.as_str(),
            "random_immigrant" | "structural_offspring" | "same_side_crossover"
        ) || !matches!(
            terminal_disposition.as_str(),
            "accepted" | "rejected" | "no_op"
        ) {
            return Err(contract("v5 evolved proposal delta codes are incompatible"));
        }
        let long_program_sha256 = canonical_sha256(&self.long_program)?;
        let short_program_sha256 = canonical_sha256(&self.short_program)?;
        if self.long_program_sha256 != long_program_sha256
            || self.short_program_sha256 != short_program_sha256
        {
            return Err(contract(
                "v5 evolved proposal delta program identities do not bind program bytes",
            ));
        }
        let has_parent = self.parent.is_some();
        let has_mate = self.mate.is_some();
        let has_selection = self.parent_selection_receipt.is_some();
        let has_terminal_plan = self.terminal_operator_plan.is_some();
        let has_terminal_application = self.terminal_operator_application.is_some();
        let has_terminal_trace = self.terminal_operator_trace.is_some();
        match scheduled_kind.as_str() {
            "random_immigrant" => {
                if origin_kind != "random_immigrant"
                    || has_parent
                    || has_mate
                    || has_selection
                    || self.mutation_depth.is_some()
                    || !self.steps.is_empty()
                    || has_terminal_plan
                    || has_terminal_application
                    || has_terminal_trace
                {
                    return Err(contract(
                        "v5 evolved immigrant delta carries offspring-only evidence",
                    ));
                }
            }
            "structural_offspring" => {
                if origin_kind != "structural_offspring"
                    || !has_parent
                    || has_mate
                    || !has_selection
                    || !matches!(self.mutation_depth, Some(1..=3))
                {
                    return Err(contract(
                        "v5 evolved mutation delta lacks exact parent/depth evidence",
                    ));
                }
            }
            "same_side_crossover" => {
                if origin_kind != "structural_offspring"
                    || !has_parent
                    || !has_mate
                    || !has_selection
                    || self.mutation_depth.is_some()
                {
                    return Err(contract(
                        "v5 evolved crossover delta lacks exact two-parent evidence",
                    ));
                }
            }
            _ => unreachable!("scheduled kind was checked"),
        }
        if let Some(receipt_value) = &self.parent_selection_receipt {
            let receipt = V5EvolvedParentSelectionReceipt::from_value(receipt_value)?;
            if receipt.scheduled_kind != scheduled_kind
                || Some(&receipt.parent) != self.parent.as_ref()
                || receipt.mate.as_ref() != self.mate.as_ref()
            {
                return Err(contract(
                    "v5 evolved delta parent selection receipt does not bind selected parents",
                ));
            }
        }
        if (has_terminal_application || has_terminal_trace) && !has_terminal_plan
            || has_terminal_plan != has_terminal_trace
        {
            return Err(contract("v5 evolved terminal operator evidence is partial"));
        }
        if self.steps.iter().any(|step| !step.is_object()) {
            return Err(contract("v5 evolved delta steps must be exact objects"));
        }
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_PROPOSAL_DELTA_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            ("proposalOrdinal", Value::from(self.proposal_ordinal)),
            (
                "generationConfigSha256",
                Value::String(generation_config_sha256),
            ),
            (
                "sharedAuthoritySha256",
                Value::String(shared_authority_sha256),
            ),
            ("proposalSeed", Value::String(proposal_seed)),
            ("originKind", Value::String(origin_kind)),
            ("scheduledKind", Value::String(scheduled_kind)),
            (
                "parent",
                match &self.parent {
                    Some(parent) => parent.to_value()?,
                    None => Value::Null,
                },
            ),
            (
                "mate",
                match &self.mate {
                    Some(mate) => mate.to_value()?,
                    None => Value::Null,
                },
            ),
            (
                "parentSelectionReceipt",
                nullable_value(self.parent_selection_receipt.as_ref()),
            ),
            (
                "mutationDepth",
                self.mutation_depth.map(Value::from).unwrap_or(Value::Null),
            ),
            ("longProgram", self.long_program.clone()),
            ("longProgramSha256", Value::String(long_program_sha256)),
            ("shortProgram", self.short_program.clone()),
            ("shortProgramSha256", Value::String(short_program_sha256)),
            ("steps", Value::Array(self.steps.clone())),
            (
                "terminalOperatorPlan",
                nullable_value(self.terminal_operator_plan.as_ref()),
            ),
            (
                "terminalOperatorApplication",
                nullable_value(self.terminal_operator_application.as_ref()),
            ),
            (
                "terminalOperatorTrace",
                nullable_value(self.terminal_operator_trace.as_ref()),
            ),
            ("terminalDisposition", Value::String(terminal_disposition)),
            ("terminalReasonCode", Value::String(terminal_reason_code)),
        ]))
    }

    pub fn delta_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 evolved delta")
            .clone();
        fields.insert(
            "deltaSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = value
            .as_object()
            .ok_or_else(|| contract("v5 evolved proposal delta must be an object"))?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "generationIndex",
                "proposalOrdinal",
                "generationConfigSha256",
                "sharedAuthoritySha256",
                "proposalSeed",
                "originKind",
                "scheduledKind",
                "parent",
                "mate",
                "parentSelectionReceipt",
                "mutationDepth",
                "longProgram",
                "longProgramSha256",
                "shortProgram",
                "shortProgramSha256",
                "steps",
                "terminalOperatorPlan",
                "terminalOperatorApplication",
                "terminalOperatorTrace",
                "terminalDisposition",
                "terminalReasonCode",
                "deltaSha256",
            ],
            "v5 evolved proposal delta",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_PROPOSAL_DELTA_SCHEMA)
        {
            return Err(contract("v5 evolved proposal delta schema is invalid"));
        }
        let steps = required(value, "steps", "v5 evolved proposal delta")?
            .as_array()
            .ok_or_else(|| contract("v5 evolved proposal delta steps must be an array"))?
            .clone();
        let delta = Self {
            generation_index: required(value, "generationIndex", "v5 evolved proposal delta")?
                .as_u64()
                .ok_or_else(|| contract("v5 evolved delta generation index is invalid"))?,
            proposal_ordinal: required(value, "proposalOrdinal", "v5 evolved proposal delta")?
                .as_u64()
                .ok_or_else(|| contract("v5 evolved delta proposal ordinal is invalid"))?,
            generation_config_sha256: exact_sha(
                required(value, "generationConfigSha256", "v5 evolved proposal delta")?,
                "v5 evolved delta generation config SHA-256",
            )?,
            shared_authority_sha256: exact_sha(
                required(value, "sharedAuthoritySha256", "v5 evolved proposal delta")?,
                "v5 evolved delta shared authority SHA-256",
            )?,
            proposal_seed: exact_sha(
                required(value, "proposalSeed", "v5 evolved proposal delta")?,
                "v5 evolved delta proposal seed",
            )?,
            origin_kind: exact_text(
                required(value, "originKind", "v5 evolved proposal delta")?,
                "v5 evolved delta origin kind",
            )?,
            scheduled_kind: exact_text(
                required(value, "scheduledKind", "v5 evolved proposal delta")?,
                "v5 evolved delta scheduled kind",
            )?,
            parent: optional_parent(
                required(value, "parent", "v5 evolved proposal delta")?,
                "v5 evolved delta parent",
            )?,
            mate: optional_parent(
                required(value, "mate", "v5 evolved proposal delta")?,
                "v5 evolved delta mate",
            )?,
            parent_selection_receipt: optional_value(
                required(value, "parentSelectionReceipt", "v5 evolved proposal delta")?,
                "v5 evolved delta parent selection receipt",
            )?,
            mutation_depth: optional_u8(
                required(value, "mutationDepth", "v5 evolved proposal delta")?,
                "v5 evolved delta mutation depth",
            )?,
            long_program: required(value, "longProgram", "v5 evolved proposal delta")?.clone(),
            long_program_sha256: exact_sha(
                required(value, "longProgramSha256", "v5 evolved proposal delta")?,
                "v5 evolved delta long program SHA-256",
            )?,
            short_program: required(value, "shortProgram", "v5 evolved proposal delta")?.clone(),
            short_program_sha256: exact_sha(
                required(value, "shortProgramSha256", "v5 evolved proposal delta")?,
                "v5 evolved delta short program SHA-256",
            )?,
            steps,
            terminal_operator_plan: optional_value(
                required(value, "terminalOperatorPlan", "v5 evolved proposal delta")?,
                "v5 evolved delta terminal plan",
            )?,
            terminal_operator_application: optional_value(
                required(
                    value,
                    "terminalOperatorApplication",
                    "v5 evolved proposal delta",
                )?,
                "v5 evolved delta terminal application",
            )?,
            terminal_operator_trace: optional_value(
                required(value, "terminalOperatorTrace", "v5 evolved proposal delta")?,
                "v5 evolved delta terminal trace",
            )?,
            terminal_disposition: exact_text(
                required(value, "terminalDisposition", "v5 evolved proposal delta")?,
                "v5 evolved delta terminal disposition",
            )?,
            terminal_reason_code: exact_text(
                required(value, "terminalReasonCode", "v5 evolved proposal delta")?,
                "v5 evolved delta terminal reason code",
            )?,
        };
        let supplied = exact_sha(
            required(value, "deltaSha256", "v5 evolved proposal delta")?,
            "v5 evolved proposal delta SHA-256",
        )?;
        if supplied != delta.delta_sha256()? || &delta.to_value()? != value {
            return Err(contract("v5 evolved proposal delta identity drifted"));
        }
        Ok(delta)
    }
}

/// Ordered semantic root for every non-null later-generation delta.
#[derive(Clone, Debug, PartialEq)]
pub struct V5EvolvedDeltaJournal {
    pub generation_index: u64,
    pub generation_config_sha256: String,
    pub shared_authority_sha256: String,
    pub deltas: Vec<V5EvolvedProposalDelta>,
}

impl V5EvolvedDeltaJournal {
    fn semantic_value(&self) -> Result<Value> {
        if self.generation_index < 2 {
            return Err(contract(
                "v5 evolved delta journal requires generation at least two",
            ));
        }
        let config = exact_sha_string(
            &self.generation_config_sha256,
            "v5 evolved delta journal generation config SHA-256",
        )?;
        let authority = exact_sha_string(
            &self.shared_authority_sha256,
            "v5 evolved delta journal shared authority SHA-256",
        )?;
        let mut previous = None;
        let mut identities = BTreeSet::new();
        for delta in &self.deltas {
            if delta.generation_index != self.generation_index
                || delta.generation_config_sha256 != config
                || delta.shared_authority_sha256 != authority
                || previous.is_some_and(|ordinal| ordinal >= delta.proposal_ordinal)
                || !identities.insert(delta.delta_sha256()?)
            {
                return Err(contract(
                    "v5 evolved delta journal order or binding drifted",
                ));
            }
            previous = Some(delta.proposal_ordinal);
        }
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_DELTA_JOURNAL_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            ("generationConfigSha256", Value::String(config)),
            ("sharedAuthoritySha256", Value::String(authority)),
            (
                "deltas",
                Value::Array(
                    self.deltas
                        .iter()
                        .map(V5EvolvedProposalDelta::to_value)
                        .collect::<Result<Vec<_>>>()?,
                ),
            ),
        ]))
    }

    pub fn delta_journal_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed evolved delta journal")
            .clone();
        fields.insert(
            "deltaJournalSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = value
            .as_object()
            .ok_or_else(|| contract("v5 evolved delta journal must be an object"))?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "generationIndex",
                "generationConfigSha256",
                "sharedAuthoritySha256",
                "deltas",
                "deltaJournalSha256",
            ],
            "v5 evolved delta journal",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_DELTA_JOURNAL_SCHEMA)
        {
            return Err(contract("v5 evolved delta journal schema is invalid"));
        }
        let journal = Self {
            generation_index: required(value, "generationIndex", "v5 evolved delta journal")?
                .as_u64()
                .ok_or_else(|| contract("v5 evolved delta journal generation is invalid"))?,
            generation_config_sha256: exact_sha(
                required(value, "generationConfigSha256", "v5 evolved delta journal")?,
                "v5 evolved delta journal generation config SHA-256",
            )?,
            shared_authority_sha256: exact_sha(
                required(value, "sharedAuthoritySha256", "v5 evolved delta journal")?,
                "v5 evolved delta journal authority SHA-256",
            )?,
            deltas: required(value, "deltas", "v5 evolved delta journal")?
                .as_array()
                .ok_or_else(|| contract("v5 evolved delta journal deltas must be an array"))?
                .iter()
                .map(V5EvolvedProposalDelta::from_value)
                .collect::<Result<Vec<_>>>()?,
        };
        let supplied = exact_sha(
            required(value, "deltaJournalSha256", "v5 evolved delta journal")?,
            "v5 evolved delta journal SHA-256",
        )?;
        if supplied != journal.delta_journal_sha256()? || &journal.to_value()? != value {
            return Err(contract("v5 evolved delta journal identity drifted"));
        }
        Ok(journal)
    }
}

/// Ordered compact accepted-record root for a later generation.  It is kept
/// independent of the G0-only journal so generation indices above one cannot
/// accidentally inherit G0 assumptions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5EvolvedCompactAcceptedJournal {
    pub generation_index: u64,
    pub generation_config_sha256: String,
    pub shared_authority_sha256: String,
    pub ordered_record_sha256s: Vec<String>,
}

impl V5EvolvedCompactAcceptedJournal {
    fn semantic_value(&self) -> Result<Value> {
        if self.generation_index < 2 {
            return Err(contract(
                "v5 evolved compact journal requires generation index at least two",
            ));
        }
        let config = exact_sha_string(
            &self.generation_config_sha256,
            "v5 evolved compact journal generation config SHA-256",
        )?;
        let authority = exact_sha_string(
            &self.shared_authority_sha256,
            "v5 evolved compact journal shared authority SHA-256",
        )?;
        let mut seen = BTreeSet::new();
        for record_sha in &self.ordered_record_sha256s {
            let record_sha = exact_sha_string(record_sha, "v5 evolved compact record SHA-256")?;
            if !seen.insert(record_sha) {
                return Err(contract(
                    "v5 evolved compact journal repeats accepted record",
                ));
            }
        }
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_COMPACT_JOURNAL_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            ("generationConfigSha256", Value::String(config)),
            ("sharedAuthoritySha256", Value::String(authority)),
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
            .expect("constructed evolved compact accepted journal")
            .clone();
        fields.insert(
            "compactJournalSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = value
            .as_object()
            .ok_or_else(|| contract("v5 evolved compact accepted journal must be an object"))?;
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
            "v5 evolved compact accepted journal",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_COMPACT_JOURNAL_SCHEMA)
        {
            return Err(contract(
                "v5 evolved compact accepted journal schema is invalid",
            ));
        }
        let ordered_record_sha256s = required(
            value,
            "orderedRecordSha256s",
            "v5 evolved compact accepted journal",
        )?
        .as_array()
        .ok_or_else(|| contract("v5 evolved compact record SHA list must be an array"))?
        .iter()
        .map(|value| exact_sha(value, "v5 evolved compact accepted record SHA-256"))
        .collect::<Result<Vec<_>>>()?;
        let supplied_count = required(
            value,
            "acceptedRecordCount",
            "v5 evolved compact accepted journal",
        )?
        .as_u64()
        .ok_or_else(|| contract("v5 evolved compact accepted record count is invalid"))?;
        if supplied_count != ordered_record_sha256s.len() as u64 {
            return Err(contract(
                "v5 evolved compact accepted journal count drifted",
            ));
        }
        let journal = Self {
            generation_index: required(
                value,
                "generationIndex",
                "v5 evolved compact accepted journal",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 evolved compact accepted journal generation is invalid"))?,
            generation_config_sha256: exact_sha(
                required(
                    value,
                    "generationConfigSha256",
                    "v5 evolved compact accepted journal",
                )?,
                "v5 evolved compact accepted journal generation config SHA-256",
            )?,
            shared_authority_sha256: exact_sha(
                required(
                    value,
                    "sharedAuthoritySha256",
                    "v5 evolved compact accepted journal",
                )?,
                "v5 evolved compact accepted journal authority SHA-256",
            )?,
            ordered_record_sha256s,
        };
        let supplied = exact_sha(
            required(
                value,
                "compactJournalSha256",
                "v5 evolved compact accepted journal",
            )?,
            "v5 evolved compact accepted journal SHA-256",
        )?;
        if supplied != journal.compact_journal_sha256()? || &journal.to_value()? != value {
            return Err(contract(
                "v5 evolved compact accepted journal identity drifted",
            ));
        }
        Ok(journal)
    }

    pub fn verify_records(&self, records: &[V5CompactAcceptedRecord]) -> Result<()> {
        if records.len() != self.ordered_record_sha256s.len() {
            return Err(contract("v5 evolved compact record count drifted"));
        }
        for (birth_ordinal, (record, record_sha)) in
            records.iter().zip(&self.ordered_record_sha256s).enumerate()
        {
            if record.generation_index != self.generation_index
                || record.shared_authority_sha256 != self.shared_authority_sha256
                || record.birth_ordinal != birth_ordinal as u64
                || record.record_sha256()? != *record_sha
            {
                return Err(contract("v5 evolved compact record order/binding drifted"));
            }
        }
        Ok(())
    }
}

/// Request data which remains valid independently of filesystem transport.
/// The batch/runtime layer validates file paths and reopens raw archive/ledger
/// artifacts; this write-neutral kernel receives the already-verified trait
/// objects and binds their exact compact states before it draws a parent.
#[derive(Clone, Debug)]
pub struct V5EvolvedTransactionRequest {
    pub shared_authority: Value,
    pub generation_config_sha256: String,
    /// Self-hash of the already-validated, transport-level archive input
    /// manifest.  The write-neutral kernel never opens the path again.
    pub parent_archive_input_binding_sha256: String,
    /// Self-hash of the already-validated, transport-level ledger input
    /// manifest.  This remains distinct from its parsed ledger state.
    pub identity_ledger_input_binding_sha256: String,
    pub generation_index: u64,
    pub target_accepted: u64,
    pub max_attempts: u64,
    pub evaluation_width: u64,
    /// Control-plane only.  It is bounded to 1..=8 and deliberately omitted
    /// from the result's semantic identity.
    pub thread_cap: u64,
    pub parent_schedule: Option<RotatingParentSchedule>,
    pub parent_selector_state_sha256: String,
    pub identity_ledger_identity_sha256: String,
    pub identity_ledger_state_sha256: String,
}

impl V5EvolvedTransactionRequest {
    fn validate_shape(&self) -> Result<()> {
        if self.generation_index < 2 {
            return Err(contract(
                "native v5 evolved transaction requires generation index at least two",
            ));
        }
        let _ = exact_sha_string(
            &self.generation_config_sha256,
            "v5 evolved transaction generation config SHA-256",
        )?;
        let _ = exact_sha_string(
            &self.parent_archive_input_binding_sha256,
            "v5 evolved transaction parent archive input binding SHA-256",
        )?;
        let _ = exact_sha_string(
            &self.identity_ledger_input_binding_sha256,
            "v5 evolved transaction identity ledger input binding SHA-256",
        )?;
        let _ = exact_sha_string(
            &self.parent_selector_state_sha256,
            "v5 evolved transaction parent selector state SHA-256",
        )?;
        let _ = exact_sha_string(
            &self.identity_ledger_identity_sha256,
            "v5 evolved transaction identity ledger identity SHA-256",
        )?;
        let _ = exact_sha_string(
            &self.identity_ledger_state_sha256,
            "v5 evolved transaction identity ledger state SHA-256",
        )?;
        if self.target_accepted == 0 {
            return Err(contract(
                "native v5 evolved transaction targetAccepted must be positive",
            ));
        }
        if self.max_attempts < self.target_accepted {
            return Err(contract(
                "native v5 evolved transaction maxAttempts must cover targetAccepted",
            ));
        }
        if self.evaluation_width == 0 || self.evaluation_width > self.target_accepted {
            return Err(contract(
                "native v5 evolved transaction evaluationWidth must be within targetAccepted",
            ));
        }
        if !(1..=8).contains(&self.thread_cap) {
            return Err(contract(
                "native v5 evolved transaction threadCap must be in 1..=8",
            ));
        }
        Ok(())
    }
}

/// Immutable receipt for the common scheduler and injected mutable state.
#[derive(Clone, Debug, PartialEq)]
pub struct V5EvolvedScheduleStateReceipt {
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
    pub proposal_state: Value,
    pub initial_parent_selector_state: Value,
    pub final_parent_selector_state: Value,
    pub identity_ledger_identity: Value,
    pub initial_identity_ledger_state: Value,
    pub final_identity_ledger_state: Value,
}

fn counts_value(counts: &BTreeMap<String, u64>) -> Value {
    Value::Object(
        counts
            .iter()
            .map(|(key, value)| (key.clone(), Value::from(*value)))
            .collect(),
    )
}

fn parse_counts(value: &Value, label: &str) -> Result<BTreeMap<String, u64>> {
    let fields = value
        .as_object()
        .ok_or_else(|| contract(format!("{label} must be an object")))?;
    let mut counts = BTreeMap::new();
    for (key, value) in fields {
        let key = stable_code(key, label)?;
        let count = value
            .as_u64()
            .ok_or_else(|| contract(format!("{label} has a non-integer count")))?;
        counts.insert(key, count);
    }
    Ok(counts)
}

impl V5EvolvedScheduleStateReceipt {
    fn semantic_value(&self) -> Result<Value> {
        if self.generation_index < 2 {
            return Err(contract(
                "v5 evolved schedule receipt requires generation index at least two",
            ));
        }
        let config = exact_sha_string(
            &self.generation_config_sha256,
            "v5 evolved schedule receipt generation config SHA-256",
        )?;
        let authority = exact_sha_string(
            &self.shared_authority_sha256,
            "v5 evolved schedule receipt authority SHA-256",
        )?;
        let state = ProposalState::from_compact_value(&self.proposal_state)?;
        if state.next_proposal_ordinal != self.next_proposal_ordinal
            || state.structural_parent_selections != self.structural_parent_selections
            || state.origin_accepted_counts != self.accepted_by_origin
            || state.disposition_counts != self.disposition_counts
        {
            return Err(contract(
                "v5 evolved schedule receipt compact proposal state drifted",
            ));
        }
        let parent_schedule_sha256 = match self.parent_schedule_sha256.as_deref() {
            Some(value) => Value::String(exact_sha_string(
                value,
                "v5 evolved schedule receipt parent schedule SHA-256",
            )?),
            None => Value::Null,
        };
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_SCHEDULE_RECEIPT_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            ("generationConfigSha256", Value::String(config)),
            ("sharedAuthoritySha256", Value::String(authority)),
            ("targetAccepted", Value::from(self.target_accepted)),
            ("maxAttempts", Value::from(self.max_attempts)),
            ("parentScheduleSha256", parent_schedule_sha256),
            ("acceptedByOrigin", counts_value(&self.accepted_by_origin)),
            ("dispositionCounts", counts_value(&self.disposition_counts)),
            (
                "nextProposalOrdinal",
                Value::from(self.next_proposal_ordinal),
            ),
            (
                "structuralParentSelections",
                Value::from(self.structural_parent_selections),
            ),
            ("crossoverAttempts", Value::from(self.crossover_attempts)),
            ("proposalState", self.proposal_state.clone()),
            (
                "proposalStateSha256",
                Value::String(canonical_sha256(&self.proposal_state)?),
            ),
            (
                "initialParentSelectorState",
                self.initial_parent_selector_state.clone(),
            ),
            (
                "initialParentSelectorStateSha256",
                Value::String(canonical_sha256(&self.initial_parent_selector_state)?),
            ),
            (
                "finalParentSelectorState",
                self.final_parent_selector_state.clone(),
            ),
            (
                "finalParentSelectorStateSha256",
                Value::String(canonical_sha256(&self.final_parent_selector_state)?),
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
                "initialIdentityLedgerState",
                self.initial_identity_ledger_state.clone(),
            ),
            (
                "initialIdentityLedgerStateSha256",
                Value::String(canonical_sha256(&self.initial_identity_ledger_state)?),
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

    pub fn schedule_state_receipt_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 evolved schedule receipt")
            .clone();
        fields.insert(
            "scheduleStateReceiptSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    /// Parse a durable receipt before a restart resumes any parent draw or
    /// ledger transition.  Every redundant SHA is recomputed by `to_value`,
    /// so transport cannot replace a compact state with a merely plausible
    /// self-hash.
    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = value
            .as_object()
            .ok_or_else(|| contract("v5 evolved schedule receipt must be an object"))?;
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
                "proposalState",
                "proposalStateSha256",
                "initialParentSelectorState",
                "initialParentSelectorStateSha256",
                "finalParentSelectorState",
                "finalParentSelectorStateSha256",
                "identityLedgerIdentity",
                "identityLedgerIdentitySha256",
                "initialIdentityLedgerState",
                "initialIdentityLedgerStateSha256",
                "finalIdentityLedgerState",
                "finalIdentityLedgerStateSha256",
                "scheduleStateReceiptSha256",
            ],
            "v5 evolved schedule receipt",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_SCHEDULE_RECEIPT_SCHEMA)
        {
            return Err(contract("v5 evolved schedule receipt schema is invalid"));
        }
        let receipt = Self {
            generation_index: required(value, "generationIndex", "v5 evolved schedule receipt")?
                .as_u64()
                .ok_or_else(|| {
                    contract("v5 evolved schedule receipt generation index is invalid")
                })?,
            generation_config_sha256: exact_sha(
                required(
                    value,
                    "generationConfigSha256",
                    "v5 evolved schedule receipt",
                )?,
                "v5 evolved schedule receipt generation config SHA-256",
            )?,
            shared_authority_sha256: exact_sha(
                required(
                    value,
                    "sharedAuthoritySha256",
                    "v5 evolved schedule receipt",
                )?,
                "v5 evolved schedule receipt shared authority SHA-256",
            )?,
            target_accepted: required(value, "targetAccepted", "v5 evolved schedule receipt")?
                .as_u64()
                .ok_or_else(|| contract("v5 evolved schedule receipt target is invalid"))?,
            max_attempts: required(value, "maxAttempts", "v5 evolved schedule receipt")?
                .as_u64()
                .ok_or_else(|| contract("v5 evolved schedule receipt attempt cap is invalid"))?,
            parent_schedule_sha256: match required(
                value,
                "parentScheduleSha256",
                "v5 evolved schedule receipt",
            )? {
                Value::Null => None,
                value => Some(exact_sha(
                    value,
                    "v5 evolved schedule receipt parent schedule SHA-256",
                )?),
            },
            accepted_by_origin: parse_counts(
                required(value, "acceptedByOrigin", "v5 evolved schedule receipt")?,
                "v5 evolved schedule receipt accepted counts",
            )?,
            disposition_counts: parse_counts(
                required(value, "dispositionCounts", "v5 evolved schedule receipt")?,
                "v5 evolved schedule receipt disposition counts",
            )?,
            next_proposal_ordinal: required(
                value,
                "nextProposalOrdinal",
                "v5 evolved schedule receipt",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 evolved schedule receipt next ordinal is invalid"))?,
            structural_parent_selections: required(
                value,
                "structuralParentSelections",
                "v5 evolved schedule receipt",
            )?
            .as_u64()
            .ok_or_else(|| {
                contract("v5 evolved schedule receipt parent-selection count is invalid")
            })?,
            crossover_attempts: required(
                value,
                "crossoverAttempts",
                "v5 evolved schedule receipt",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 evolved schedule receipt crossover count is invalid"))?,
            proposal_state: required(value, "proposalState", "v5 evolved schedule receipt")?
                .clone(),
            initial_parent_selector_state: required(
                value,
                "initialParentSelectorState",
                "v5 evolved schedule receipt",
            )?
            .clone(),
            final_parent_selector_state: required(
                value,
                "finalParentSelectorState",
                "v5 evolved schedule receipt",
            )?
            .clone(),
            identity_ledger_identity: required(
                value,
                "identityLedgerIdentity",
                "v5 evolved schedule receipt",
            )?
            .clone(),
            initial_identity_ledger_state: required(
                value,
                "initialIdentityLedgerState",
                "v5 evolved schedule receipt",
            )?
            .clone(),
            final_identity_ledger_state: required(
                value,
                "finalIdentityLedgerState",
                "v5 evolved schedule receipt",
            )?
            .clone(),
        };
        let supplied = exact_sha(
            required(
                value,
                "scheduleStateReceiptSha256",
                "v5 evolved schedule receipt",
            )?,
            "v5 evolved schedule receipt SHA-256",
        )?;
        if supplied != receipt.schedule_state_receipt_sha256()? || &receipt.to_value()? != value {
            return Err(contract(
                "v5 evolved schedule receipt identity or canonical form drifted",
            ));
        }
        Ok(receipt)
    }
}

/// One typed attempt-parent link into the content-addressed snapshot
/// inventory.  The stored attempt reference prevents a snapshot for another
/// accepted record from being substituted merely because its own self-hash is
/// valid.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct V5EvolvedParentSnapshotRef {
    pub attempt_reference: V5AttemptParentReference,
    pub parent_snapshot_sha256: String,
}

impl V5EvolvedParentSnapshotRef {
    fn to_value(&self) -> Result<Value> {
        Ok(object([
            ("attemptReference", self.attempt_reference.to_value()?),
            (
                "parentSnapshotSha256",
                Value::String(exact_sha_string(
                    &self.parent_snapshot_sha256,
                    "v5 evolved parent snapshot reference SHA-256",
                )?),
            ),
        ]))
    }

    fn from_value(value: &Value, label: &str) -> Result<Self> {
        let fields = value
            .as_object()
            .ok_or_else(|| contract(format!("{label} must be an object")))?;
        exact_keys(fields, &["attemptReference", "parentSnapshotSha256"], label)?;
        let reference = Self {
            attempt_reference: V5AttemptParentReference::from_value(required(
                value,
                "attemptReference",
                label,
            )?)?,
            parent_snapshot_sha256: exact_sha(
                required(value, "parentSnapshotSha256", label)?,
                "v5 evolved parent snapshot reference SHA-256",
            )?,
        };
        if &reference.to_value()? != value {
            return Err(contract(format!("{label} is not canonical")));
        }
        Ok(reference)
    }
}

fn nullable_snapshot_ref(value: Option<&V5EvolvedParentSnapshotRef>) -> Result<Value> {
    match value {
        Some(value) => value.to_value(),
        None => Ok(Value::Null),
    }
}

fn optional_snapshot_ref(value: &Value, label: &str) -> Result<Option<V5EvolvedParentSnapshotRef>> {
    if value.is_null() {
        Ok(None)
    } else {
        V5EvolvedParentSnapshotRef::from_value(value, label).map(Some)
    }
}

/// Ordered parent/mate snapshot links for one attempt.  This remains present
/// for immigrants as an all-null self-hashed receipt, which makes the replay
/// stream contiguous and prevents an omitted structural link from being
/// hidden by a shortened list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct V5EvolvedAttemptSnapshotRefs {
    pub proposal_ordinal: u64,
    pub parent: Option<V5EvolvedParentSnapshotRef>,
    pub mate: Option<V5EvolvedParentSnapshotRef>,
}

impl V5EvolvedAttemptSnapshotRefs {
    fn semantic_value(&self) -> Result<Value> {
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_ATTEMPT_SNAPSHOT_REFS_SCHEMA.to_owned()),
            ),
            ("proposalOrdinal", Value::from(self.proposal_ordinal)),
            ("parent", nullable_snapshot_ref(self.parent.as_ref())?),
            ("mate", nullable_snapshot_ref(self.mate.as_ref())?),
        ]))
    }

    pub(crate) fn snapshot_refs_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub(crate) fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed evolved attempt snapshot references")
            .clone();
        fields.insert(
            "snapshotRefsSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub(crate) fn from_value(value: &Value) -> Result<Self> {
        let fields = value
            .as_object()
            .ok_or_else(|| contract("v5 evolved attempt snapshot references must be an object"))?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "proposalOrdinal",
                "parent",
                "mate",
                "snapshotRefsSha256",
            ],
            "v5 evolved attempt snapshot references",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_ATTEMPT_SNAPSHOT_REFS_SCHEMA)
        {
            return Err(contract(
                "v5 evolved attempt snapshot references schema is invalid",
            ));
        }
        let refs = Self {
            proposal_ordinal: required(
                value,
                "proposalOrdinal",
                "v5 evolved attempt snapshot references",
            )?
            .as_u64()
            .ok_or_else(|| contract("v5 evolved attempt snapshot references ordinal is invalid"))?,
            parent: optional_snapshot_ref(
                required(value, "parent", "v5 evolved attempt snapshot references")?,
                "v5 evolved attempt snapshot parent reference",
            )?,
            mate: optional_snapshot_ref(
                required(value, "mate", "v5 evolved attempt snapshot references")?,
                "v5 evolved attempt snapshot mate reference",
            )?,
        };
        let supplied = exact_sha(
            required(
                value,
                "snapshotRefsSha256",
                "v5 evolved attempt snapshot references",
            )?,
            "v5 evolved attempt snapshot references SHA-256",
        )?;
        if supplied != refs.snapshot_refs_sha256()? || &refs.to_value()? != value {
            return Err(contract(
                "v5 evolved attempt snapshot references identity drifted",
            ));
        }
        Ok(refs)
    }
}

/// Deduplicated, self-contained parent snapshot objects and the ordered
/// attempt links which consume them.  The inventory deliberately embeds full
/// snapshot values rather than archive paths: after online construction has
/// authenticated the archive, offline adoption needs only these objects and
/// the sealed shared authority.
#[derive(Clone, Debug)]
pub(crate) struct V5EvolvedParentSnapshotInventory {
    pub generation_index: u64,
    pub generation_config_sha256: String,
    pub shared_authority_sha256: String,
    pub source_parent_archive_input_binding_sha256: String,
    pub source_parent_archive_semantic_sha256: String,
    pub snapshots: Vec<V5EvolvedParentSnapshot>,
    pub attempt_snapshot_refs: Vec<V5EvolvedAttemptSnapshotRefs>,
}

impl V5EvolvedParentSnapshotInventory {
    fn snapshot_map(&self) -> Result<BTreeMap<String, &V5EvolvedParentSnapshot>> {
        let mut snapshots = BTreeMap::new();
        let mut previous = None;
        for snapshot in &self.snapshots {
            if snapshot.source_parent_archive_input_binding_sha256
                != self.source_parent_archive_input_binding_sha256
                || snapshot.source_parent_archive_semantic_sha256
                    != self.source_parent_archive_semantic_sha256
            {
                return Err(contract(
                    "v5 evolved parent snapshot source binding drifted from inventory",
                ));
            }
            let snapshot_sha = snapshot.parent_snapshot_sha256()?;
            if previous
                .as_ref()
                .is_some_and(|previous| previous >= &snapshot_sha)
                || snapshots.insert(snapshot_sha.clone(), snapshot).is_some()
            {
                return Err(contract(
                    "v5 evolved parent snapshot inventory order or deduplication drifted",
                ));
            }
            let _ = snapshot.to_value()?;
            previous = Some(snapshot_sha);
        }
        Ok(snapshots)
    }

    fn resolve_ref<'a>(
        snapshots: &'a BTreeMap<String, &'a V5EvolvedParentSnapshot>,
        reference: &V5EvolvedParentSnapshotRef,
        label: &str,
    ) -> Result<&'a V5EvolvedParentSnapshot> {
        let snapshot_sha = exact_sha_string(
            &reference.parent_snapshot_sha256,
            "v5 evolved parent snapshot reference SHA-256",
        )?;
        let snapshot = snapshots
            .get(&snapshot_sha)
            .copied()
            .ok_or_else(|| contract(format!("{label} names a missing parent snapshot")))?;
        if snapshot.attempt_reference != reference.attempt_reference {
            return Err(contract(format!(
                "{label} attempt reference does not bind its parent snapshot"
            )));
        }
        Ok(snapshot)
    }

    fn semantic_value(&self) -> Result<Value> {
        if self.generation_index < 2 {
            return Err(contract(
                "v5 evolved parent snapshot inventory requires generation at least two",
            ));
        }
        let config = exact_sha_string(
            &self.generation_config_sha256,
            "v5 evolved parent snapshot inventory generation config SHA-256",
        )?;
        let authority = exact_sha_string(
            &self.shared_authority_sha256,
            "v5 evolved parent snapshot inventory authority SHA-256",
        )?;
        let source_binding = exact_sha_string(
            &self.source_parent_archive_input_binding_sha256,
            "v5 evolved parent snapshot inventory source archive input binding SHA-256",
        )?;
        let source_semantic = exact_sha_string(
            &self.source_parent_archive_semantic_sha256,
            "v5 evolved parent snapshot inventory source archive semantic SHA-256",
        )?;
        let snapshots = self.snapshot_map()?;
        let mut previous_ordinal = None;
        for refs in &self.attempt_snapshot_refs {
            if previous_ordinal.is_some_and(|previous| previous >= refs.proposal_ordinal) {
                return Err(contract(
                    "v5 evolved parent snapshot references are not ordinal ordered",
                ));
            }
            if let Some(parent) = &refs.parent {
                let _ = Self::resolve_ref(&snapshots, parent, "v5 evolved snapshot parent")?;
            }
            if let Some(mate) = &refs.mate {
                let _ = Self::resolve_ref(&snapshots, mate, "v5 evolved snapshot mate")?;
            }
            let _ = refs.to_value()?;
            previous_ordinal = Some(refs.proposal_ordinal);
        }
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_PARENT_SNAPSHOT_INVENTORY_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            ("generationConfigSha256", Value::String(config)),
            ("sharedAuthoritySha256", Value::String(authority)),
            (
                "sourceParentArchiveInputBindingSha256",
                Value::String(source_binding),
            ),
            (
                "sourceParentArchiveSemanticSha256",
                Value::String(source_semantic),
            ),
            (
                "snapshots",
                Value::Array(
                    self.snapshots
                        .iter()
                        .map(V5EvolvedParentSnapshot::to_value)
                        .collect::<std::result::Result<Vec<_>, _>>()?,
                ),
            ),
            (
                "attemptSnapshotRefs",
                Value::Array(
                    self.attempt_snapshot_refs
                        .iter()
                        .map(V5EvolvedAttemptSnapshotRefs::to_value)
                        .collect::<Result<Vec<_>>>()?,
                ),
            ),
        ]))
    }

    pub(crate) fn parent_snapshot_inventory_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub(crate) fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed evolved parent snapshot inventory")
            .clone();
        fields.insert(
            "parentSnapshotInventorySha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub(crate) fn from_value(value: &Value) -> Result<Self> {
        let fields = value
            .as_object()
            .ok_or_else(|| contract("v5 evolved parent snapshot inventory must be an object"))?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "generationIndex",
                "generationConfigSha256",
                "sharedAuthoritySha256",
                "sourceParentArchiveInputBindingSha256",
                "sourceParentArchiveSemanticSha256",
                "snapshots",
                "attemptSnapshotRefs",
                "parentSnapshotInventorySha256",
            ],
            "v5 evolved parent snapshot inventory",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_PARENT_SNAPSHOT_INVENTORY_SCHEMA)
        {
            return Err(contract(
                "v5 evolved parent snapshot inventory schema is invalid",
            ));
        }
        let snapshots = required(value, "snapshots", "v5 evolved parent snapshot inventory")?
            .as_array()
            .ok_or_else(|| contract("v5 evolved parent snapshots must be an array"))?
            .iter()
            .map(V5EvolvedParentSnapshot::from_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let attempt_snapshot_refs = required(
            value,
            "attemptSnapshotRefs",
            "v5 evolved parent snapshot inventory",
        )?
        .as_array()
        .ok_or_else(|| contract("v5 evolved attempt snapshot references must be an array"))?
        .iter()
        .map(V5EvolvedAttemptSnapshotRefs::from_value)
        .collect::<Result<Vec<_>>>()?;
        let inventory = Self {
            generation_index: required(
                value,
                "generationIndex",
                "v5 evolved parent snapshot inventory",
            )?
            .as_u64()
            .ok_or_else(|| {
                contract("v5 evolved parent snapshot inventory generation is invalid")
            })?,
            generation_config_sha256: exact_sha(
                required(
                    value,
                    "generationConfigSha256",
                    "v5 evolved parent snapshot inventory",
                )?,
                "v5 evolved parent snapshot inventory generation config SHA-256",
            )?,
            shared_authority_sha256: exact_sha(
                required(
                    value,
                    "sharedAuthoritySha256",
                    "v5 evolved parent snapshot inventory",
                )?,
                "v5 evolved parent snapshot inventory authority SHA-256",
            )?,
            source_parent_archive_input_binding_sha256: exact_sha(
                required(
                    value,
                    "sourceParentArchiveInputBindingSha256",
                    "v5 evolved parent snapshot inventory",
                )?,
                "v5 evolved parent snapshot inventory source archive input binding SHA-256",
            )?,
            source_parent_archive_semantic_sha256: exact_sha(
                required(
                    value,
                    "sourceParentArchiveSemanticSha256",
                    "v5 evolved parent snapshot inventory",
                )?,
                "v5 evolved parent snapshot inventory source archive semantic SHA-256",
            )?,
            snapshots,
            attempt_snapshot_refs,
        };
        let supplied = exact_sha(
            required(
                value,
                "parentSnapshotInventorySha256",
                "v5 evolved parent snapshot inventory",
            )?,
            "v5 evolved parent snapshot inventory SHA-256",
        )?;
        if supplied != inventory.parent_snapshot_inventory_sha256()?
            || &inventory.to_value()? != value
        {
            return Err(contract(
                "v5 evolved parent snapshot inventory identity drifted",
            ));
        }
        Ok(inventory)
    }
}

/// The sealed construction engine's pre-ledger outcome.  This is crate-local:
/// callers cannot inject a Python/fixture builder into the public production
/// path.  Unit tests use it only to exercise scheduler accounting in isolation.
#[derive(Clone, Debug)]
pub(crate) struct V5EvolvedConstructionOutcome {
    pub delta: Option<V5EvolvedProposalDelta>,
    pub lineage_refs: V5AttemptLineageRefs,
    pub disposition: String,
    pub reason_code: String,
    pub stage: String,
    pub accepted: Option<V5CoreEvolvedAcceptedMaterial>,
}

/// Test-only construction seam used to isolate scheduler accounting.  The
/// public production entry point below always binds the native sealed
/// constructor directly; no caller can inject fixture admission, Python, or
/// an arbitrary materializer.
#[cfg(test)]
trait V5EvolvedConstructionEngine: Send {
    fn construct(
        &mut self,
        authority: &V5SharedConstructionAuthority,
        request: &V5EvolvedTransactionRequest,
        planned: &PlannedProposal,
        birth_ordinal: u64,
    ) -> Result<V5EvolvedConstructionOutcome>;
}

fn planned_scheduled_kind(intent: &ProposalIntent) -> &'static str {
    intent.scheduled_kind()
}

/// Check the archive selector's public reference against the compiler-owned
/// parent material before any operator draw.  The loader validates the raw
/// payload; this additional comparison prevents a valid payload from being
/// substituted for a different scheduled archive slot.
fn require_planned_parent_binding(
    planned_pair_identity_sha256: &str,
    planned_candidate_id: &str,
    material: &V5EvolvedParentMaterial,
    label: &str,
) -> Result<()> {
    if material.pair_identity_sha256 != planned_pair_identity_sha256
        || material.candidate_id != planned_candidate_id
        || material.attempt_reference.candidate_id != planned_candidate_id
        || material.attempt_reference.candidate_identity_sha256
            != material.candidate_identity_sha256
    {
        return Err(contract(format!(
            "v5 evolved sealed {label} material does not bind the scheduled parent reference"
        )));
    }
    Ok(())
}

/// Preserve the scheduler's original primary/mate order even when the
/// Python-compatible crossover ordering selects the mate as recipient.  The
/// accepted-material compiler may subsequently receive recipient/donor
/// material in its own parent/mate slots, but the transaction delta must keep
/// the selector transcript exactly as consumed.
fn parent_selection_receipt_for_planned(
    planned: &PlannedProposal,
    parent: &V5EvolvedParentMaterial,
    mate: Option<&V5EvolvedParentMaterial>,
) -> Result<V5EvolvedParentSelectionReceipt> {
    match &planned.intent {
        ProposalIntent::StructuralMutation {
            parent: scheduled_parent,
            ..
        } => {
            require_planned_parent_binding(
                &scheduled_parent.pair_identity_sha256,
                &scheduled_parent.candidate_id,
                parent,
                "mutation parent",
            )?;
            if mate.is_some() {
                return Err(contract(
                    "v5 evolved mutation unexpectedly carries a sealed mate",
                ));
            }
            Ok(V5EvolvedParentSelectionReceipt {
                scheduled_kind: "structural_offspring".to_owned(),
                parent: parent.attempt_reference.clone(),
                mate: None,
                parent_selection_audit: scheduled_parent
                    .selection_audit
                    .clone()
                    .unwrap_or(Value::Null),
                mate_selection_audit: None,
                mate_selection_attempts: Vec::new(),
            })
        }
        ProposalIntent::SameSideCrossover {
            parent: scheduled_parent,
            mate: scheduled_mate,
            mate_selection_attempts,
            ..
        } => {
            let mate = mate.ok_or_else(|| {
                contract("v5 evolved same-side crossover omits sealed scheduled mate")
            })?;
            require_planned_parent_binding(
                &scheduled_parent.pair_identity_sha256,
                &scheduled_parent.candidate_id,
                parent,
                "crossover primary parent",
            )?;
            require_planned_parent_binding(
                &scheduled_mate.pair_identity_sha256,
                &scheduled_mate.candidate_id,
                mate,
                "crossover mate",
            )?;
            Ok(V5EvolvedParentSelectionReceipt {
                scheduled_kind: "same_side_crossover".to_owned(),
                parent: parent.attempt_reference.clone(),
                mate: Some(mate.attempt_reference.clone()),
                parent_selection_audit: scheduled_parent
                    .selection_audit
                    .clone()
                    .unwrap_or(Value::Null),
                mate_selection_audit: Some(
                    scheduled_mate
                        .selection_audit
                        .clone()
                        .unwrap_or(Value::Null),
                ),
                mate_selection_attempts: mate_selection_attempts.clone(),
            })
        }
        ProposalIntent::RichImmigrant { .. } => Err(contract(
            "v5 evolved immigrant has no parent-selection receipt",
        )),
    }
}

/// Produce the exact trace-rooted attempt lineage links used by every
/// structural disposition.  A rejection before application still retains the
/// selected plan and complete trace, while immigrants intentionally carry no
/// parent/operator links.
fn lineage_refs_for_attempt(
    parent: Option<&V5EvolvedParentMaterial>,
    mate: Option<&V5EvolvedParentMaterial>,
    receipt: Option<&V5EvolvedParentSelectionReceipt>,
    terminal_operator_plan: Option<&Value>,
    terminal_operator_application: Option<&Value>,
    terminal_operator_trace: Option<&Value>,
    step_index: Option<u64>,
) -> Result<V5AttemptLineageRefs> {
    match (parent, mate, receipt) {
        (None, None, None) => {
            if terminal_operator_plan.is_some()
                || terminal_operator_application.is_some()
                || terminal_operator_trace.is_some()
                || step_index.is_some()
            {
                return Err(contract(
                    "v5 evolved immigrant lineage unexpectedly carries operator facts",
                ));
            }
            Ok(V5AttemptLineageRefs {
                parent: None,
                mate: None,
                parent_selection_receipt_sha256: None,
                operator_plan_sha256: None,
                operator_application_sha256: None,
                operator_trace_sha256: None,
                step_index: None,
            })
        }
        (Some(parent), mate, Some(receipt)) => {
            let plan = terminal_operator_plan.ok_or_else(|| {
                contract("v5 evolved structural lineage omits terminal operator plan")
            })?;
            let trace = terminal_operator_trace.ok_or_else(|| {
                contract("v5 evolved structural lineage omits terminal operator trace")
            })?;
            let step_index = step_index.ok_or_else(|| {
                contract("v5 evolved structural lineage omits terminal operator step index")
            })?;
            if receipt.parent != parent.attempt_reference
                || receipt.mate.as_ref() != mate.map(|value| &value.attempt_reference)
            {
                return Err(contract(
                    "v5 evolved structural lineage receipt parent binding drifted",
                ));
            }
            Ok(V5AttemptLineageRefs {
                parent: Some(parent.attempt_reference.clone()),
                mate: mate.map(|value| value.attempt_reference.clone()),
                parent_selection_receipt_sha256: Some(receipt.receipt_sha256()?),
                operator_plan_sha256: Some(canonical_sha256(plan)?),
                operator_application_sha256: terminal_operator_application
                    .map(canonical_sha256)
                    .transpose()?,
                operator_trace_sha256: Some(canonical_sha256(trace)?),
                step_index: Some(step_index),
            })
        }
        _ => Err(contract(
            "v5 evolved lineage parent, mate, and selection receipt presence drifted",
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn proposal_delta_for_attempt(
    authority: &V5SharedConstructionAuthority,
    request: &V5EvolvedTransactionRequest,
    planned: &PlannedProposal,
    parent: Option<&V5EvolvedParentMaterial>,
    mate: Option<&V5EvolvedParentMaterial>,
    receipt: Option<&V5EvolvedParentSelectionReceipt>,
    mutation_depth: Option<u8>,
    long_program: Value,
    short_program: Value,
    steps: Vec<Value>,
    terminal_operator_plan: Option<Value>,
    terminal_operator_application: Option<Value>,
    terminal_operator_trace: Option<Value>,
    terminal_disposition: &str,
    terminal_reason_code: &str,
) -> Result<V5EvolvedProposalDelta> {
    let delta = V5EvolvedProposalDelta {
        generation_index: request.generation_index,
        proposal_ordinal: planned.proposal_ordinal,
        generation_config_sha256: request.generation_config_sha256.clone(),
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        proposal_seed: planned.intent.proposal_seed().to_owned(),
        origin_kind: planned.intent.origin_kind().to_owned(),
        scheduled_kind: planned_scheduled_kind(&planned.intent).to_owned(),
        parent: parent.map(|value| value.attempt_reference.clone()),
        mate: mate.map(|value| value.attempt_reference.clone()),
        parent_selection_receipt: receipt
            .map(V5EvolvedParentSelectionReceipt::to_value)
            .transpose()?,
        mutation_depth,
        long_program_sha256: canonical_sha256(&long_program)?,
        long_program,
        short_program_sha256: canonical_sha256(&short_program)?,
        short_program,
        steps,
        terminal_operator_plan,
        terminal_operator_application,
        terminal_operator_trace,
        terminal_disposition: terminal_disposition.to_owned(),
        terminal_reason_code: terminal_reason_code.to_owned(),
    };
    let _ = delta.to_value()?;
    Ok(delta)
}

fn with_self_hash(semantic: Value, field_name: &str, label: &str) -> Result<Value> {
    let mut fields = semantic
        .as_object()
        .ok_or_else(|| contract(format!("{label} semantic value must be an object")))?
        .clone();
    fields.insert(
        field_name.to_owned(),
        Value::String(canonical_sha256(&semantic)?),
    );
    Ok(Value::Object(fields))
}

fn terminal_operator_failure(
    scheduled_kind: &str,
    phase: &str,
    reason_code: &str,
    reason_detail: Value,
) -> Result<Value> {
    let scheduled_kind = stable_code(scheduled_kind, "v5 evolved terminal scheduled kind")?;
    let phase = stable_code(phase, "v5 evolved terminal phase")?;
    let reason_code = stable_code(reason_code, "v5 evolved terminal reason code")?;
    with_self_hash(
        object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_OPERATOR_TERMINAL_SCHEMA.to_owned()),
            ),
            ("scheduledKind", Value::String(scheduled_kind)),
            ("phase", Value::String(phase)),
            ("reasonCode", Value::String(reason_code)),
            ("reasonDetail", reason_detail),
        ]),
        "terminalSha256",
        "v5 evolved operator terminal",
    )
}

fn ordered_operator_trace(
    scheduled_kind: &str,
    steps: &[Value],
    terminal_step_index: u64,
    terminal_disposition: &str,
    terminal_reason_code: &str,
    terminal_operator_plan: &Value,
    terminal_operator_application: Option<&Value>,
) -> Result<Value> {
    let scheduled_kind = stable_code(scheduled_kind, "v5 evolved operator trace scheduled kind")?;
    let terminal_disposition = stable_code(
        terminal_disposition,
        "v5 evolved operator trace terminal disposition",
    )?;
    let terminal_reason_code = stable_code(
        terminal_reason_code,
        "v5 evolved operator trace terminal reason code",
    )?;
    if !matches!(
        terminal_disposition.as_str(),
        "accepted" | "rejected" | "no_op"
    ) || steps.iter().any(|step| !step.is_object())
    {
        return Err(contract(
            "v5 evolved operator trace terminal disposition or steps are invalid",
        ));
    }
    with_self_hash(
        object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_OPERATOR_TRACE_SCHEMA.to_owned()),
            ),
            ("scheduledKind", Value::String(scheduled_kind)),
            ("steps", Value::Array(steps.to_vec())),
            ("terminalStepIndex", Value::from(terminal_step_index)),
            ("terminalDisposition", Value::String(terminal_disposition)),
            ("terminalReasonCode", Value::String(terminal_reason_code)),
            (
                "terminalOperatorPlanSha256",
                Value::String(canonical_sha256(terminal_operator_plan)?),
            ),
            (
                "terminalOperatorApplicationSha256",
                nullable_sha(
                    terminal_operator_application
                        .map(canonical_sha256)
                        .transpose()?
                        .as_deref(),
                ),
            ),
        ]),
        "operatorTraceSha256",
        "v5 evolved operator trace",
    )
}

fn evolved_operator_selection_value(execution: &V5EvolvedOperatorExecution) -> Result<Value> {
    with_self_hash(
        object([
            (
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_operator_selection_v1".to_owned()),
            ),
            ("nativePlan", execution.selection.native_plan.clone()),
            ("legacyChoice", execution.selection.legacy_choice.clone()),
            ("selectionReceipt", execution.selection.receipt.clone()),
        ]),
        "selectionSha256",
        "v5 evolved operator selection",
    )
}

fn evolved_operator_application_value(
    execution: &V5EvolvedOperatorExecution,
) -> Result<Option<Value>> {
    execution
        .application
        .as_ref()
        .map(|application| {
            with_self_hash(
                object([
                    (
                        "schemaVersion",
                        Value::String(
                            "temporal_qd_v5_evolved_operator_application_evidence_v1".to_owned(),
                        ),
                    ),
                    ("plan", application.plan.clone()),
                    ("childProgram", application.child_program.clone()),
                    ("applicationAudit", application.audit.clone()),
                ]),
                "applicationSha256",
                "v5 evolved operator application evidence",
            )
        })
        .transpose()
}

fn evolved_operator_delta_value(delta: &V5EvolvedOperatorDelta) -> Result<Value> {
    with_self_hash(
        object([
            (
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_operator_step_delta_v1".to_owned()),
            ),
            ("side", Value::String(delta.side.clone())),
            (
                "parentPairIdentitySha256",
                Value::String(delta.parent_pair_identity_sha256.clone()),
            ),
            (
                "parentProgramSha256",
                Value::String(delta.parent_program_sha256.clone()),
            ),
            ("childProgram", delta.child_program.clone()),
            (
                "childProgramSha256",
                Value::String(delta.child_program_sha256.clone()),
            ),
            ("nativePlan", delta.native_plan.clone()),
            ("legacyChoice", delta.legacy_choice.clone()),
            ("trace", delta.trace.clone()),
        ]),
        "stepDeltaSha256",
        "v5 evolved operator step delta",
    )
}

fn evolved_operator_step_value(
    step_index: u64,
    disposition: V5OperatorDisposition,
    reason_code: &str,
    reason_detail: Value,
    execution: &V5EvolvedOperatorExecution,
    delta: Option<&V5EvolvedOperatorDelta>,
) -> Result<Value> {
    let reason_code = stable_code(reason_code, "v5 evolved operator step reason code")?;
    with_self_hash(
        object([
            (
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_operator_step_v1".to_owned()),
            ),
            ("stepIndex", Value::from(step_index)),
            (
                "disposition",
                Value::String(disposition.as_str().to_owned()),
            ),
            ("reasonCode", Value::String(reason_code)),
            ("reasonDetail", reason_detail),
            ("selection", evolved_operator_selection_value(execution)?),
            (
                "application",
                evolved_operator_application_value(execution)?.unwrap_or(Value::Null),
            ),
            ("operatorResult", execution.result.value.clone()),
            (
                "operatorDelta",
                delta
                    .map(evolved_operator_delta_value)
                    .transpose()?
                    .unwrap_or(Value::Null),
            ),
        ]),
        "stepSha256",
        "v5 evolved operator step",
    )
}

fn structural_failure_step_value(
    step_index: u64,
    scheduled_kind: &str,
    phase: &str,
    disposition: &str,
    reason_code: &str,
    reason_detail: Value,
) -> Result<Value> {
    let disposition = stable_code(disposition, "v5 evolved failure step disposition")?;
    if !matches!(disposition.as_str(), "rejected" | "no_op") {
        return Err(contract(
            "v5 evolved failure step must have rejected/no-op disposition",
        ));
    }
    with_self_hash(
        object([
            (
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_operator_step_v1".to_owned()),
            ),
            ("stepIndex", Value::from(step_index)),
            ("disposition", Value::String(disposition)),
            (
                "terminal",
                terminal_operator_failure(scheduled_kind, phase, reason_code, reason_detail)?,
            ),
        ]),
        "stepSha256",
        "v5 evolved failure step",
    )
}

fn evolved_crossover_application_value(
    execution: &V5EvolvedSameSideCrossoverExecution,
) -> Result<Option<Value>> {
    execution
        .application
        .as_ref()
        .map(|application| {
            with_self_hash(
                object([
                    (
                        "schemaVersion",
                        Value::String(
                            "temporal_qd_v5_evolved_same_side_crossover_application_evidence_v1"
                                .to_owned(),
                        ),
                    ),
                    ("plan", application.plan.clone()),
                    ("childProgram", application.child_program.clone()),
                    ("applicationAudit", application.audit.clone()),
                ]),
                "applicationSha256",
                "v5 evolved same-side crossover application evidence",
            )
        })
        .transpose()
}

fn evolved_crossover_execution_value(
    step_index: u64,
    execution: &V5EvolvedSameSideCrossoverExecution,
) -> Result<Value> {
    let selection = execution
        .selection
        .as_ref()
        .map(|selection| {
            with_self_hash(
                object([
                    (
                        "schemaVersion",
                        Value::String(
                            "temporal_qd_v5_evolved_same_side_crossover_selection_v1".to_owned(),
                        ),
                    ),
                    (
                        "proposalSeed",
                        Value::String(selection.proposal_seed.clone()),
                    ),
                    ("side", Value::String(selection.side.clone())),
                    (
                        "recipientPairIdentitySha256",
                        Value::String(selection.recipient_pair_identity_sha256.clone()),
                    ),
                    (
                        "donorPairIdentitySha256",
                        Value::String(selection.donor_pair_identity_sha256.clone()),
                    ),
                    (
                        "recipientModuleIdentitySha256",
                        Value::String(selection.recipient_module_identity_sha256.clone()),
                    ),
                    (
                        "donorModuleIdentitySha256",
                        Value::String(selection.donor_module_identity_sha256.clone()),
                    ),
                    ("nativePlan", selection.native_plan.clone()),
                    ("selection", selection.selection.clone()),
                ]),
                "selectionSha256",
                "v5 evolved same-side crossover selection",
            )
        })
        .transpose()?
        .unwrap_or(Value::Null);
    let application = evolved_crossover_application_value(execution)?.unwrap_or(Value::Null);
    let delta = execution
        .delta
        .as_ref()
        .map(|delta| {
            with_self_hash(
                object([
                    (
                        "schemaVersion",
                        Value::String(
                            "temporal_qd_v5_evolved_same_side_crossover_delta_v1".to_owned(),
                        ),
                    ),
                    ("side", Value::String(delta.side.clone())),
                    (
                        "recipientPairIdentitySha256",
                        Value::String(delta.recipient_pair_identity_sha256.clone()),
                    ),
                    (
                        "donorPairIdentitySha256",
                        Value::String(delta.donor_pair_identity_sha256.clone()),
                    ),
                    (
                        "recipientModuleIdentitySha256",
                        Value::String(delta.recipient_module_identity_sha256.clone()),
                    ),
                    (
                        "donorModuleIdentitySha256",
                        Value::String(delta.donor_module_identity_sha256.clone()),
                    ),
                    (
                        "recipientProgramSha256",
                        Value::String(delta.recipient_program_sha256.clone()),
                    ),
                    (
                        "donorProgramSha256",
                        Value::String(delta.donor_program_sha256.clone()),
                    ),
                    ("childProgram", delta.child_program.clone()),
                    (
                        "childProgramSha256",
                        Value::String(delta.child_program_sha256.clone()),
                    ),
                    ("nativePlan", delta.native_plan.clone()),
                    ("selection", delta.selection.clone()),
                    ("trace", delta.trace.clone()),
                ]),
                "crossoverDeltaSha256",
                "v5 evolved same-side crossover delta",
            )
        })
        .transpose()?
        .unwrap_or(Value::Null);
    with_self_hash(
        object([
            (
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_same_side_crossover_step_v1".to_owned()),
            ),
            ("stepIndex", Value::from(step_index)),
            (
                "disposition",
                Value::String(execution.disposition.as_str().to_owned()),
            ),
            (
                "reasonCode",
                Value::String(stable_code(
                    &execution.reason_code,
                    "v5 evolved crossover step reason code",
                )?),
            ),
            ("reasonDetail", execution.reason_detail.clone()),
            ("selection", selection),
            ("application", application),
            ("crossoverDelta", delta),
        ]),
        "stepSha256",
        "v5 evolved same-side crossover step",
    )
}

/// Native sealed construction for a scheduled later-generation immigrant.
/// Program bytes originate solely from the authority-owned factory wrapper;
/// the transaction then journals those bytes before asking the independent
/// materializer to rebuild them.  This deliberately gives an immigrant the
/// same all-attempt delta durability as a structural child.
fn construct_sealed_immigrant(
    authority: &V5SharedConstructionAuthority,
    request: &V5EvolvedTransactionRequest,
    planned: &PlannedProposal,
    birth_ordinal: u64,
) -> Result<V5EvolvedConstructionOutcome> {
    if !matches!(&planned.intent, ProposalIntent::RichImmigrant { .. }) {
        return Err(contract(
            "v5 evolved sealed immigrant constructor received structural intent",
        ));
    }
    let (long_program, short_program) =
        build_v5_evolved_immigrant_programs(authority, planned.intent.proposal_seed())?;
    let delta = proposal_delta_for_attempt(
        authority,
        request,
        planned,
        None,
        None,
        None,
        None,
        long_program.clone(),
        short_program.clone(),
        Vec::new(),
        None,
        None,
        None,
        "accepted",
        "accepted",
    )?;
    let delta_value = delta.to_value()?;
    let material = build_v5_evolved_accepted_material(
        authority,
        V5EvolvedAcceptedBuildInput {
            generation_config_sha256: request.generation_config_sha256.clone(),
            generation_index: request.generation_index,
            birth_ordinal,
            proposal_ordinal: planned.proposal_ordinal,
            proposal_seed: planned.intent.proposal_seed().to_owned(),
            kind: V5EvolvedBuildKind::Immigrant,
            parent: None,
            mate: None,
            parent_selection_receipt: None,
            long_program: Some(long_program),
            short_program: Some(short_program),
            operator_trace: None,
            terminal_operator_plan: None,
            terminal_operator_application: None,
            proposal_delta: delta_value.clone(),
        },
    )?;
    if material.proposal_delta != delta_value {
        return Err(contract(
            "v5 evolved sealed immigrant materializer changed the exact proposal delta",
        ));
    }
    Ok(V5EvolvedConstructionOutcome {
        delta: Some(delta),
        lineage_refs: lineage_refs_for_attempt(None, None, None, None, None, None, None)?,
        disposition: "accepted".to_owned(),
        reason_code: "accepted".to_owned(),
        stage: "compile".to_owned(),
        accepted: Some(material),
    })
}

#[allow(clippy::too_many_arguments)]
fn structural_terminal_outcome(
    authority: &V5SharedConstructionAuthority,
    request: &V5EvolvedTransactionRequest,
    planned: &PlannedProposal,
    journal_parent: &V5EvolvedParentMaterial,
    journal_mate: Option<&V5EvolvedParentMaterial>,
    receipt: &V5EvolvedParentSelectionReceipt,
    mutation_depth: Option<u8>,
    long_program: Value,
    short_program: Value,
    steps: Vec<Value>,
    terminal_operator_plan: Value,
    terminal_operator_application: Option<Value>,
    terminal_operator_trace: Value,
    terminal_disposition: &str,
    terminal_reason_code: &str,
    stage: &str,
    step_index: u64,
) -> Result<V5EvolvedConstructionOutcome> {
    let delta = proposal_delta_for_attempt(
        authority,
        request,
        planned,
        Some(journal_parent),
        journal_mate,
        Some(receipt),
        mutation_depth,
        long_program,
        short_program,
        steps,
        Some(terminal_operator_plan.clone()),
        terminal_operator_application.clone(),
        Some(terminal_operator_trace.clone()),
        terminal_disposition,
        terminal_reason_code,
    )?;
    Ok(V5EvolvedConstructionOutcome {
        delta: Some(delta),
        lineage_refs: lineage_refs_for_attempt(
            Some(journal_parent),
            journal_mate,
            Some(receipt),
            Some(&terminal_operator_plan),
            terminal_operator_application.as_ref(),
            Some(&terminal_operator_trace),
            Some(step_index),
        )?,
        disposition: terminal_disposition.to_owned(),
        reason_code: terminal_reason_code.to_owned(),
        stage: stage.to_owned(),
        accepted: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn construct_sealed_structural_accept(
    authority: &V5SharedConstructionAuthority,
    request: &V5EvolvedTransactionRequest,
    planned: &PlannedProposal,
    birth_ordinal: u64,
    kind: V5EvolvedBuildKind,
    journal_parent: &V5EvolvedParentMaterial,
    journal_mate: Option<&V5EvolvedParentMaterial>,
    build_parent: V5EvolvedParentMaterial,
    build_mate: Option<V5EvolvedParentMaterial>,
    receipt: &V5EvolvedParentSelectionReceipt,
    mutation_depth: Option<u8>,
    long_program: Value,
    short_program: Value,
    steps: Vec<Value>,
    terminal_operator_plan: Value,
    terminal_operator_application: Value,
    terminal_operator_trace: Value,
    step_index: u64,
) -> Result<V5EvolvedConstructionOutcome> {
    // Every individual structural step can be valid while a multi-step
    // mutation sequence still returns the targeted side to the original
    // parent program.  That aggregate outcome is a no-op proposal, not a
    // malformed accepted candidate.  Keep the accepted-material invariant
    // strict and journal the complete step evidence as a terminal no-op.
    if long_program == build_parent.long_program && short_program == build_parent.short_program {
        let reason_code = "structural_aggregate_no_op";
        let no_op_trace = ordered_operator_trace(
            planned_scheduled_kind(&planned.intent),
            &steps,
            step_index,
            "no_op",
            reason_code,
            &terminal_operator_plan,
            Some(&terminal_operator_application),
        )?;
        return structural_terminal_outcome(
            authority,
            request,
            planned,
            journal_parent,
            journal_mate,
            receipt,
            mutation_depth,
            long_program,
            short_program,
            steps,
            terminal_operator_plan,
            Some(terminal_operator_application),
            no_op_trace,
            "no_op",
            reason_code,
            "operator_apply",
            step_index,
        );
    }
    let delta = proposal_delta_for_attempt(
        authority,
        request,
        planned,
        Some(journal_parent),
        journal_mate,
        Some(receipt),
        mutation_depth,
        long_program.clone(),
        short_program.clone(),
        steps,
        Some(terminal_operator_plan.clone()),
        Some(terminal_operator_application.clone()),
        Some(terminal_operator_trace.clone()),
        "accepted",
        "accepted",
    )?;
    let delta_value = delta.to_value()?;
    let material = build_v5_evolved_accepted_material(
        authority,
        V5EvolvedAcceptedBuildInput {
            generation_config_sha256: request.generation_config_sha256.clone(),
            generation_index: request.generation_index,
            birth_ordinal,
            proposal_ordinal: planned.proposal_ordinal,
            proposal_seed: planned.intent.proposal_seed().to_owned(),
            kind,
            parent: Some(build_parent),
            mate: build_mate,
            parent_selection_receipt: Some(receipt.to_value()?),
            long_program: Some(long_program),
            short_program: Some(short_program),
            operator_trace: Some(terminal_operator_trace.clone()),
            terminal_operator_plan: Some(terminal_operator_plan.clone()),
            terminal_operator_application: Some(terminal_operator_application.clone()),
            proposal_delta: delta_value.clone(),
        },
    )?;
    if material.proposal_delta != delta_value {
        return Err(contract(
            "v5 evolved sealed structural materializer changed the exact proposal delta",
        ));
    }
    Ok(V5EvolvedConstructionOutcome {
        delta: Some(delta),
        lineage_refs: lineage_refs_for_attempt(
            Some(journal_parent),
            journal_mate,
            Some(receipt),
            Some(&terminal_operator_plan),
            Some(&terminal_operator_application),
            Some(&terminal_operator_trace),
            Some(step_index),
        )?,
        disposition: "accepted".to_owned(),
        reason_code: "accepted".to_owned(),
        stage: "compile".to_owned(),
        accepted: Some(material),
    })
}

fn set_program_for_side(
    side: &str,
    child_program: &Value,
    long_program: &mut Value,
    short_program: &mut Value,
) -> Result<()> {
    match side {
        "long" => *long_program = child_program.clone(),
        "short" => *short_program = child_program.clone(),
        _ => {
            return Err(contract(
                "v5 evolved operator delta names an unsupported side",
            ));
        }
    }
    Ok(())
}

fn mutation_selection_failure(
    authority: &V5SharedConstructionAuthority,
    request: &V5EvolvedTransactionRequest,
    planned: &PlannedProposal,
    parent: &V5EvolvedParentMaterial,
    receipt: &V5EvolvedParentSelectionReceipt,
    depth: u8,
    long_program: Value,
    short_program: Value,
    steps: &mut Vec<Value>,
    step_index: u64,
    phase: &str,
    reason_code: &str,
    reason_detail: Value,
    stage: &str,
) -> Result<V5EvolvedConstructionOutcome> {
    let terminal_plan = terminal_operator_failure(
        "structural_offspring",
        phase,
        reason_code,
        reason_detail.clone(),
    )?;
    steps.push(structural_failure_step_value(
        step_index,
        "structural_offspring",
        phase,
        "rejected",
        reason_code,
        reason_detail,
    )?);
    let trace = ordered_operator_trace(
        "structural_offspring",
        steps,
        step_index,
        "rejected",
        reason_code,
        &terminal_plan,
        None,
    )?;
    structural_terminal_outcome(
        authority,
        request,
        planned,
        parent,
        None,
        receipt,
        Some(depth),
        long_program,
        short_program,
        steps.clone(),
        terminal_plan,
        None,
        trace,
        "rejected",
        reason_code,
        stage,
        step_index,
    )
}

fn execute_fast_ephemeral_operator_step_once(
    state: &V5EvolvedSideState,
    authority: &crate::v5_operators::V5OperatorAuthority,
    admitted: V5AdmittedEvolvedOperatorSelection,
    recompiler: &V5SealedEvolvedPairRecompiler,
    admission_telemetry: Option<&V5EvolvedAdmissionSample<'_>>,
) -> Result<(
    V5EvolvedPairStepResult,
    Option<V5SealedEvolvedPairRecompiler>,
)> {
    let replay_selection = admitted.replay_selection();
    let operator_execution = execute_admitted_evolved_operator_selection(
        &state.pair_identity_sha256,
        &state.program,
        authority,
        &state.compiled_profile,
        admitted,
    )
    .map_err(|error| contract(error.to_string()))?;
    let delta = operator_execution.delta.clone();
    if operator_execution.disposition != V5OperatorDisposition::Accepted {
        return Ok((
            V5EvolvedPairStepResult {
                disposition: operator_execution.disposition,
                reason_code: operator_execution.reason_code.clone(),
                reason_detail: object([(
                    "operatorResult",
                    operator_execution.result.value.clone(),
                )]),
                operator_execution,
                delta,
                next_side_state: None,
            },
            None,
        ));
    }
    let delta_ref = delta.as_ref().ok_or_else(|| {
        contract("accepted evolved operator execution omitted its side-local delta")
    })?;
    match recompiler.advance_evolved_operator_once(delta_ref) {
        Ok((advanced, recompiled)) => {
            if recompiled.pair_identity_sha256 == state.pair_identity_sha256 {
                return Err(contract(
                    "pair recompiler reused the parent pair identity for an evolved child",
                ));
            }
            let next_side_state = match V5EvolvedSideState::from_recompiled_pair(
                recompiled.pair_identity_sha256,
                recompiled.module_identity_sha256,
                authority,
                delta_ref.child_program.clone(),
                recompiled.compiled_profile,
            ) {
                Ok(next_side_state) => next_side_state,
                Err(error) => {
                    return Ok((
                        V5EvolvedPairStepResult {
                            disposition: V5OperatorDisposition::Rejected,
                            reason_code: "pair_recompile_rejected".to_owned(),
                            reason_detail: Value::String(error.to_string()),
                            operator_execution,
                            delta,
                            next_side_state: None,
                        },
                        None,
                    ));
                }
            };
            Ok((
                V5EvolvedPairStepResult {
                    disposition: V5OperatorDisposition::Accepted,
                    reason_code: "accepted".to_owned(),
                    reason_detail: object([
                        ("kind", Value::String("pair_recompiled".to_owned())),
                        (
                            "childPairIdentitySha256",
                            Value::String(next_side_state.pair_identity_sha256.clone()),
                        ),
                    ]),
                    operator_execution,
                    delta,
                    next_side_state: Some(next_side_state),
                },
                Some(advanced),
            ))
        }
        Err(error) => {
            // A failed sealed rebuild is rare. Re-run the historical child
            // admission only on this error path so an invalid child retains
            // the exact durable rejection code/detail instead of being
            // relabeled as a pair-recompile failure. Successful fast-path
            // children still pay for one compile, not two.
            if let Some(telemetry) = admission_telemetry {
                V5EvolvedAdmissionSample::increment(&telemetry.fallback_sweeps);
            }
            let verified_execution = execute_evolved_operator_selection_with_admission(
                &state.pair_identity_sha256,
                &state.program,
                authority,
                &state.compiled_profile,
                replay_selection,
                &V5FullPairChildAdmission {
                    inner: recompiler,
                    telemetry: admission_telemetry,
                },
            )
            .map_err(|admission_error| contract(admission_error.to_string()))?;
            let verified_delta = verified_execution.delta.clone();
            if verified_execution.disposition != V5OperatorDisposition::Accepted {
                return Ok((
                    V5EvolvedPairStepResult {
                        disposition: verified_execution.disposition,
                        reason_code: verified_execution.reason_code.clone(),
                        reason_detail: object([(
                            "operatorResult",
                            verified_execution.result.value.clone(),
                        )]),
                        operator_execution: verified_execution,
                        delta: verified_delta,
                        next_side_state: None,
                    },
                    None,
                ));
            }
            Ok((
                V5EvolvedPairStepResult {
                    disposition: V5OperatorDisposition::Rejected,
                    reason_code: "pair_recompile_rejected".to_owned(),
                    reason_detail: Value::String(
                        crate::v5_operators::V5OperatorError::Invalid(error.to_string())
                            .to_string(),
                    ),
                    operator_execution: verified_execution,
                    delta: verified_delta,
                    next_side_state: None,
                },
                None,
            ))
        }
    }
}

fn construct_sealed_mutation(
    authority: &V5SharedConstructionAuthority,
    request: &V5EvolvedTransactionRequest,
    planned: &PlannedProposal,
    birth_ordinal: u64,
    parent_cache: Option<&V5VerifiedParentCache>,
    admission_telemetry: Option<&V5EvolvedAdmissionTelemetry>,
) -> Result<V5EvolvedConstructionOutcome> {
    let (scheduled_parent, depth) = match &planned.intent {
        ProposalIntent::StructuralMutation {
            parent,
            mutation_depth,
            ..
        } => (parent, *mutation_depth),
        _ => {
            return Err(contract(
                "v5 evolved sealed mutation constructor received a non-mutation intent",
            ));
        }
    };
    let parent = load_parent_for_construction(parent_cache, authority, scheduled_parent)?;
    let receipt = parent_selection_receipt_for_planned(planned, &parent, None)?;
    let proposal_seed = planned.intent.proposal_seed();
    let side = proposal_side_for_seed(proposal_seed).map_err(|error| {
        contract(format!(
            "v5 evolved mutation side selection failed: {error}"
        ))
    })?;
    let projection = authority.operator_authority_projection()?;
    let operator_authority = projection.operator_authority(side)?;
    let mut recompiler = if parent_cache.is_some() {
        V5SealedEvolvedPairRecompiler::from_verified_parent(authority, &parent, proposal_seed)?
    } else {
        V5SealedEvolvedPairRecompiler::from_parent(authority, &parent, proposal_seed)?
    };
    let mut current_state = recompiler.state_for_side(side)?;
    let mut long_program = parent.long_program.clone();
    let mut short_program = parent.short_program.clone();
    let mut steps = Vec::new();
    let mut terminal_plan = None;
    let mut terminal_application = None;
    let admission_sample = admission_telemetry.map(V5EvolvedAdmissionSample::new);

    for step_index in 0..u64::from(depth) {
        if let Some(telemetry) = admission_sample.as_ref() {
            V5EvolvedAdmissionSample::increment(&telemetry.vocabulary_enumerations);
        }
        let instrumented_admission =
            admission_sample
                .as_ref()
                .map(|telemetry| V5InstrumentedChildAdmission {
                    inner: &recompiler,
                    telemetry,
                });
        let admission: &dyn V5EvolvedChildAdmission = instrumented_admission
            .as_ref()
            .map(|admission| admission as &dyn V5EvolvedChildAdmission)
            .unwrap_or(&recompiler);
        let selection = match select_admitted_evolved_operator_choice_from_state(
            proposal_seed,
            &current_state,
            &operator_authority,
            admission,
        ) {
            Ok(selection) => selection,
            Err(error) => {
                return mutation_selection_failure(
                    authority,
                    request,
                    planned,
                    &parent,
                    &receipt,
                    depth,
                    long_program,
                    short_program,
                    &mut steps,
                    step_index,
                    "selection",
                    "operator_selection_rejected",
                    Value::String(error.to_string()),
                    "operator_plan",
                );
            }
        };
        if let Some(telemetry) = admission_sample.as_ref() {
            V5EvolvedAdmissionSample::increment(&telemetry.selected_rebuilds);
        }
        let step_result = if parent_cache.is_some() {
            execute_fast_ephemeral_operator_step_once(
                &current_state,
                &operator_authority,
                selection,
                &recompiler,
                admission_sample.as_ref(),
            )
        } else {
            execute_admitted_evolved_operator_step_from_state(
                &current_state,
                &operator_authority,
                selection,
                &recompiler,
            )
            .map(|result| (result, None))
            .map_err(|error| contract(error.to_string()))
        };
        let (pair_step, fast_advanced) = match step_result {
            Ok(result) => result,
            Err(error) => {
                return mutation_selection_failure(
                    authority,
                    request,
                    planned,
                    &parent,
                    &receipt,
                    depth,
                    long_program,
                    short_program,
                    &mut steps,
                    step_index,
                    "execution",
                    "operator_execution_rejected",
                    Value::String(error.to_string()),
                    "operator_apply",
                );
            }
        };
        let step_plan = pair_step.operator_execution.selection.native_plan.clone();
        let step_application = evolved_operator_application_value(&pair_step.operator_execution)?;
        let mut attempted_long = long_program.clone();
        let mut attempted_short = short_program.clone();
        if let Some(delta) = pair_step.delta.as_ref() {
            if delta.side != side {
                return Err(contract(
                    "v5 evolved mutation operator delta side drifted from selected side",
                ));
            }
            set_program_for_side(
                &delta.side,
                &delta.child_program,
                &mut attempted_long,
                &mut attempted_short,
            )?;
        }
        steps.push(evolved_operator_step_value(
            step_index,
            pair_step.disposition,
            &pair_step.reason_code,
            pair_step.reason_detail.clone(),
            &pair_step.operator_execution,
            pair_step.delta.as_ref(),
        )?);
        match pair_step.disposition {
            V5OperatorDisposition::Accepted => {
                let delta = pair_step.delta.as_ref().ok_or_else(|| {
                    contract("accepted v5 evolved mutation step omits operator delta")
                })?;
                let expected_next_state = pair_step.next_side_state.as_ref().ok_or_else(|| {
                    contract("accepted v5 evolved mutation step omits recompiled side state")
                })?;
                let advanced_result = match fast_advanced {
                    Some(advanced) => Ok(advanced),
                    None => recompiler.advance_evolved_operator(delta),
                };
                let advanced = match advanced_result {
                    Ok(advanced) => advanced,
                    Err(error) => {
                        let reason_code = "pair_recompile_rejected";
                        steps.push(structural_failure_step_value(
                            step_index + 1,
                            "structural_offspring",
                            "pair_recompile",
                            "rejected",
                            reason_code,
                            Value::String(error.to_string()),
                        )?);
                        let trace = ordered_operator_trace(
                            "structural_offspring",
                            &steps,
                            step_index + 1,
                            "rejected",
                            reason_code,
                            &step_plan,
                            step_application.as_ref(),
                        )?;
                        return structural_terminal_outcome(
                            authority,
                            request,
                            planned,
                            &parent,
                            None,
                            &receipt,
                            Some(depth),
                            attempted_long,
                            attempted_short,
                            steps,
                            step_plan,
                            step_application,
                            trace,
                            "rejected",
                            reason_code,
                            "compile",
                            step_index + 1,
                        );
                    }
                };
                let advanced_state = advanced.state_for_side(side)?;
                if advanced_state != *expected_next_state {
                    return Err(contract(
                        "v5 evolved mutation advance state disagrees with sealed step recompilation",
                    ));
                }
                if step_application.is_none() {
                    return Err(contract(
                        "accepted v5 evolved mutation step omits application evidence",
                    ));
                }
                long_program = attempted_long;
                short_program = attempted_short;
                terminal_plan = Some(step_plan);
                terminal_application = step_application;
                recompiler = advanced;
                current_state = advanced_state;
            }
            V5OperatorDisposition::Rejected | V5OperatorDisposition::NoOp => {
                let disposition = pair_step.disposition.as_str();
                let stage = if pair_step.reason_code == "pair_recompile_rejected" {
                    "compile"
                } else {
                    "operator_apply"
                };
                let trace = ordered_operator_trace(
                    "structural_offspring",
                    &steps,
                    step_index,
                    disposition,
                    &pair_step.reason_code,
                    &step_plan,
                    step_application.as_ref(),
                )?;
                return structural_terminal_outcome(
                    authority,
                    request,
                    planned,
                    &parent,
                    None,
                    &receipt,
                    Some(depth),
                    attempted_long,
                    attempted_short,
                    steps,
                    step_plan,
                    step_application,
                    trace,
                    disposition,
                    &pair_step.reason_code,
                    stage,
                    step_index,
                );
            }
        }
    }
    let terminal_plan = terminal_plan
        .ok_or_else(|| contract("v5 evolved mutation completed without terminal operator plan"))?;
    let terminal_application = terminal_application.ok_or_else(|| {
        contract("v5 evolved mutation completed without terminal application evidence")
    })?;
    let terminal_step_index = u64::from(depth) - 1;
    let terminal_trace = ordered_operator_trace(
        "structural_offspring",
        &steps,
        terminal_step_index,
        "accepted",
        "accepted",
        &terminal_plan,
        Some(&terminal_application),
    )?;
    construct_sealed_structural_accept(
        authority,
        request,
        planned,
        birth_ordinal,
        V5EvolvedBuildKind::Mutation,
        &parent,
        None,
        (*parent).clone(),
        None,
        &receipt,
        Some(depth),
        long_program,
        short_program,
        steps,
        terminal_plan,
        terminal_application,
        terminal_trace,
        terminal_step_index,
    )
}

fn crossover_recipient_and_donor<'a>(
    selection_recipient_pair_identity_sha256: &str,
    selection_donor_pair_identity_sha256: &str,
    primary: &'a V5EvolvedParentMaterial,
    mate: &'a V5EvolvedParentMaterial,
) -> Result<(&'a V5EvolvedParentMaterial, &'a V5EvolvedParentMaterial)> {
    if selection_recipient_pair_identity_sha256 == primary.pair_identity_sha256
        && selection_donor_pair_identity_sha256 == mate.pair_identity_sha256
    {
        Ok((primary, mate))
    } else if selection_recipient_pair_identity_sha256 == mate.pair_identity_sha256
        && selection_donor_pair_identity_sha256 == primary.pair_identity_sha256
    {
        Ok((mate, primary))
    } else {
        Err(contract(
            "v5 evolved crossover selection does not bind the scheduled distinct parents",
        ))
    }
}

fn construct_sealed_crossover(
    authority: &V5SharedConstructionAuthority,
    request: &V5EvolvedTransactionRequest,
    planned: &PlannedProposal,
    birth_ordinal: u64,
    parent_cache: Option<&V5VerifiedParentCache>,
) -> Result<V5EvolvedConstructionOutcome> {
    let (scheduled_parent, scheduled_mate, scheduled_side) = match &planned.intent {
        ProposalIntent::SameSideCrossover {
            parent, mate, side, ..
        } => (parent, mate, side.as_str()),
        _ => {
            return Err(contract(
                "v5 evolved sealed crossover constructor received a non-crossover intent",
            ));
        }
    };
    let primary = load_parent_for_construction(parent_cache, authority, scheduled_parent)?;
    let mate = load_parent_for_construction(parent_cache, authority, scheduled_mate)?;
    let receipt = parent_selection_receipt_for_planned(planned, &primary, Some(&mate))?;
    let proposal_seed = planned.intent.proposal_seed();
    let side = proposal_side_for_seed(proposal_seed).map_err(|error| {
        contract(format!(
            "v5 evolved crossover side selection failed: {error}"
        ))
    })?;
    if side != scheduled_side {
        return Err(contract(
            "v5 evolved crossover scheduler side does not match sealed proposal-side routing",
        ));
    }
    let projection = authority.operator_authority_projection()?;
    let operator_authority = projection.operator_authority(side)?;
    let first_state = if side == "long" {
        &primary.long_state
    } else {
        &primary.short_state
    };
    let second_state = if side == "long" {
        &mate.long_state
    } else {
        &mate.short_state
    };
    let execution = attempt_evolved_same_side_crossover_from_states(
        proposal_seed,
        first_state,
        second_state,
        &operator_authority,
    );
    let mut steps = vec![evolved_crossover_execution_value(0, &execution)?];
    match execution.disposition {
        V5OperatorDisposition::Accepted => {
            let selection = execution.selection.as_ref().ok_or_else(|| {
                contract("accepted v5 evolved crossover omits recipient/donor selection")
            })?;
            let delta = execution
                .delta
                .as_ref()
                .ok_or_else(|| contract("accepted v5 evolved crossover omits two-parent delta"))?;
            let terminal_application = evolved_crossover_application_value(&execution)?
                .ok_or_else(|| {
                    contract("accepted v5 evolved crossover omits application evidence")
                })?;
            let (recipient, donor) = crossover_recipient_and_donor(
                &selection.recipient_pair_identity_sha256,
                &selection.donor_pair_identity_sha256,
                &primary,
                &mate,
            )?;
            if delta.side != side
                || delta.recipient_pair_identity_sha256 != recipient.pair_identity_sha256
                || delta.donor_pair_identity_sha256 != donor.pair_identity_sha256
            {
                return Err(contract(
                    "v5 evolved accepted crossover delta drifts from selected recipient/donor",
                ));
            }
            let terminal_plan = selection.native_plan.clone();
            let mut long_program = recipient.long_program.clone();
            let mut short_program = recipient.short_program.clone();
            set_program_for_side(
                side,
                &delta.child_program,
                &mut long_program,
                &mut short_program,
            )?;
            let recompiler = if parent_cache.is_some() {
                V5SealedEvolvedPairRecompiler::from_verified_parent(
                    authority,
                    recipient,
                    proposal_seed,
                )?
            } else {
                V5SealedEvolvedPairRecompiler::from_parent(authority, recipient, proposal_seed)?
            };
            let fast_advanced = if parent_cache.is_some() {
                match recompiler.advance_same_side_crossover_once(donor, delta) {
                    Ok(result) => Some(result),
                    Err(error) => {
                        let reason_code = "pair_recompile_rejected";
                        steps.push(structural_failure_step_value(
                            1,
                            "same_side_crossover",
                            "pair_recompile",
                            "rejected",
                            reason_code,
                            Value::String(error.to_string()),
                        )?);
                        let trace = ordered_operator_trace(
                            "same_side_crossover",
                            &steps,
                            1,
                            "rejected",
                            reason_code,
                            &terminal_plan,
                            Some(&terminal_application),
                        )?;
                        return structural_terminal_outcome(
                            authority,
                            request,
                            planned,
                            &primary,
                            Some(&mate),
                            &receipt,
                            None,
                            long_program,
                            short_program,
                            steps,
                            terminal_plan,
                            Some(terminal_application),
                            trace,
                            "rejected",
                            reason_code,
                            "compile",
                            1,
                        );
                    }
                }
            } else {
                None
            };
            let direct = match fast_advanced.as_ref() {
                Some((_, direct)) => direct.clone(),
                None => match recompiler.recompile_same_side_crossover_pair(donor, delta) {
                    Ok(recompiled) => recompiled,
                    Err(error) => {
                        let reason_code = "pair_recompile_rejected";
                        steps.push(structural_failure_step_value(
                            1,
                            "same_side_crossover",
                            "pair_recompile",
                            "rejected",
                            reason_code,
                            Value::String(error.to_string()),
                        )?);
                        let trace = ordered_operator_trace(
                            "same_side_crossover",
                            &steps,
                            1,
                            "rejected",
                            reason_code,
                            &terminal_plan,
                            Some(&terminal_application),
                        )?;
                        return structural_terminal_outcome(
                            authority,
                            request,
                            planned,
                            &primary,
                            Some(&mate),
                            &receipt,
                            None,
                            long_program,
                            short_program,
                            steps,
                            terminal_plan,
                            Some(terminal_application),
                            trace,
                            "rejected",
                            reason_code,
                            "compile",
                            1,
                        );
                    }
                },
            };
            let advanced = match fast_advanced {
                Some((advanced, _)) => advanced,
                None => match recompiler.advance_same_side_crossover(donor, delta) {
                    Ok(advanced) => advanced,
                    Err(error) => {
                        let reason_code = "pair_recompile_rejected";
                        steps.push(structural_failure_step_value(
                            1,
                            "same_side_crossover",
                            "pair_recompile",
                            "rejected",
                            reason_code,
                            Value::String(error.to_string()),
                        )?);
                        let trace = ordered_operator_trace(
                            "same_side_crossover",
                            &steps,
                            1,
                            "rejected",
                            reason_code,
                            &terminal_plan,
                            Some(&terminal_application),
                        )?;
                        return structural_terminal_outcome(
                            authority,
                            request,
                            planned,
                            &primary,
                            Some(&mate),
                            &receipt,
                            None,
                            long_program,
                            short_program,
                            steps,
                            terminal_plan,
                            Some(terminal_application),
                            trace,
                            "rejected",
                            reason_code,
                            "compile",
                            1,
                        );
                    }
                },
            };
            let advanced_state = advanced.state_for_side(side)?;
            if advanced_state.pair_identity_sha256 != direct.pair_identity_sha256
                || advanced_state.module_identity_sha256 != direct.module_identity_sha256
                || advanced_state.compiled_profile != direct.compiled_profile
            {
                return Err(contract(
                    "v5 evolved crossover advance state disagrees with sealed direct recompilation",
                ));
            }
            let trace = ordered_operator_trace(
                "same_side_crossover",
                &steps,
                0,
                "accepted",
                "accepted",
                &terminal_plan,
                Some(&terminal_application),
            )?;
            construct_sealed_structural_accept(
                authority,
                request,
                planned,
                birth_ordinal,
                V5EvolvedBuildKind::Crossover,
                &primary,
                Some(&mate),
                recipient.clone(),
                Some(donor.clone()),
                &receipt,
                None,
                long_program,
                short_program,
                steps,
                terminal_plan,
                terminal_application,
                trace,
                0,
            )
        }
        V5OperatorDisposition::Rejected | V5OperatorDisposition::NoOp => {
            let disposition = execution.disposition.as_str();
            let (terminal_plan, long_program, short_program) = match execution.selection.as_ref() {
                Some(selection) => {
                    let (recipient, _) = crossover_recipient_and_donor(
                        &selection.recipient_pair_identity_sha256,
                        &selection.donor_pair_identity_sha256,
                        &primary,
                        &mate,
                    )?;
                    (
                        selection.native_plan.clone(),
                        recipient.long_program.clone(),
                        recipient.short_program.clone(),
                    )
                }
                None => {
                    let terminal_plan = terminal_operator_failure(
                        "same_side_crossover",
                        "selection",
                        &execution.reason_code,
                        execution.reason_detail.clone(),
                    )?;
                    steps = vec![structural_failure_step_value(
                        0,
                        "same_side_crossover",
                        "selection",
                        disposition,
                        &execution.reason_code,
                        execution.reason_detail.clone(),
                    )?];
                    (
                        terminal_plan,
                        primary.long_program.clone(),
                        primary.short_program.clone(),
                    )
                }
            };
            let terminal_application = evolved_crossover_application_value(&execution)?;
            let trace = ordered_operator_trace(
                "same_side_crossover",
                &steps,
                0,
                disposition,
                &execution.reason_code,
                &terminal_plan,
                terminal_application.as_ref(),
            )?;
            structural_terminal_outcome(
                authority,
                request,
                planned,
                &primary,
                Some(&mate),
                &receipt,
                None,
                long_program,
                short_program,
                steps,
                terminal_plan,
                terminal_application,
                trace,
                disposition,
                &execution.reason_code,
                "operator_apply",
                0,
            )
        }
    }
}

/// The only production construction engine.  It is intentionally private so
/// no caller can substitute fixtures, Python, a subprocess, or an arbitrary
/// materializer for the sealed v5 compiler path.
#[derive(Default)]
struct NativeV5EvolvedConstructionEngine {
    parent_cache: Option<Arc<V5VerifiedParentCache>>,
    admission_telemetry: Option<Arc<V5EvolvedAdmissionTelemetry>>,
}

impl NativeV5EvolvedConstructionEngine {
    fn durable() -> Self {
        Self::default()
    }

    fn fast_ephemeral() -> Self {
        Self {
            parent_cache: Some(Arc::new(V5VerifiedParentCache::default())),
            admission_telemetry: None,
        }
    }

    fn enable_admission_telemetry(&mut self) {
        self.admission_telemetry = Some(Arc::new(V5EvolvedAdmissionTelemetry::default()));
    }

    fn admission_telemetry(&self) -> Option<&V5EvolvedAdmissionTelemetry> {
        self.admission_telemetry.as_deref()
    }

    fn parent_cache_telemetry(&self) -> Option<(u64, u64, Duration)> {
        self.parent_cache.as_ref().map(|cache| {
            (
                cache.hits.load(Ordering::Relaxed),
                cache.misses.load(Ordering::Relaxed),
                Duration::from_nanos(cache.verification_nanos.load(Ordering::Relaxed)),
            )
        })
    }

    fn construct(
        &self,
        authority: &V5SharedConstructionAuthority,
        request: &V5EvolvedTransactionRequest,
        planned: &PlannedProposal,
        birth_ordinal: u64,
    ) -> Result<V5EvolvedConstructionOutcome> {
        match &planned.intent {
            ProposalIntent::RichImmigrant { .. } => {
                construct_sealed_immigrant(authority, request, planned, birth_ordinal)
            }
            ProposalIntent::StructuralMutation { .. } => construct_sealed_mutation(
                authority,
                request,
                planned,
                birth_ordinal,
                self.parent_cache.as_deref(),
                self.admission_telemetry(),
            ),
            ProposalIntent::SameSideCrossover { .. } => construct_sealed_crossover(
                authority,
                request,
                planned,
                birth_ordinal,
                self.parent_cache.as_deref(),
            ),
        }
    }
}

/// Re-run the sealed constructor before a native attempt reaches duplicate
/// admission or archive adoption.  A durable delta is a corruption witness,
/// never construction authority: even a self-hashed delta must reproduce the
/// exact ordered operator selection/application sequence and final pair from
/// authenticated parent material.  Fresh parent loading and recompilation
/// prevent a stale cached profile from making a tampered final program appear
/// admissible.
fn verify_native_construction_replay(
    authority: &V5SharedConstructionAuthority,
    request: &V5EvolvedTransactionRequest,
    planned: &PlannedProposal,
    birth_ordinal: u64,
    outcome: &V5EvolvedConstructionOutcome,
) -> Result<()> {
    let replayed = match &planned.intent {
        ProposalIntent::RichImmigrant { .. } => {
            construct_sealed_immigrant(authority, request, planned, birth_ordinal)?
        }
        ProposalIntent::StructuralMutation { .. } => {
            construct_sealed_mutation(authority, request, planned, birth_ordinal, None, None)?
        }
        ProposalIntent::SameSideCrossover { .. } => {
            construct_sealed_crossover(authority, request, planned, birth_ordinal, None)?
        }
    };
    let same_delta = match (&outcome.delta, &replayed.delta) {
        (Some(left), Some(right)) => left.to_value()? == right.to_value()?,
        (None, None) => true,
        _ => false,
    };
    if !same_delta
        || outcome.disposition != replayed.disposition
        || outcome.reason_code != replayed.reason_code
        || outcome.stage != replayed.stage
        || outcome.lineage_refs.to_value()? != replayed.lineage_refs.to_value()?
    {
        return Err(contract(
            "v5 evolved native construction does not replay its exact ordered operator transcript",
        ));
    }
    match (&outcome.accepted, &replayed.accepted) {
        (None, None) => Ok(()),
        (Some(left), Some(right))
            if left.proposal_delta == right.proposal_delta
                && left.record.to_value()? == right.record.to_value()?
                && left.ledger_candidate == right.ledger_candidate =>
        {
            Ok(())
        }
        _ => Err(contract(
            "v5 evolved native construction replay does not reproduce accepted material",
        )),
    }
}

/// Capture one selected archive reference as an offline replay object.  Native
/// construction has already loaded this exact reference before this point;
/// this function only seals the authenticated compact material and preserves
/// its selector audit for a later constructor replay.
fn snapshot_ref_for_selected_parent(
    request: &V5EvolvedTransactionRequest,
    selected_parent: &ParentReference,
    expected_attempt_reference: &V5AttemptParentReference,
    snapshots: &mut BTreeMap<String, V5EvolvedParentSnapshot>,
    label: &str,
) -> Result<V5EvolvedParentSnapshotRef> {
    let snapshot = V5EvolvedParentSnapshot::from_parent_reference(
        &request.parent_archive_input_binding_sha256,
        &request.parent_selector_state_sha256,
        selected_parent,
    )?;
    if snapshot.attempt_reference != *expected_attempt_reference {
        return Err(contract(format!(
            "v5 evolved {label} snapshot does not bind the constructed parent reference"
        )));
    }
    let snapshot_sha = snapshot.parent_snapshot_sha256()?;
    if let Some(existing) = snapshots.get(&snapshot_sha) {
        if existing.to_value()? != snapshot.to_value()? {
            return Err(contract(
                "v5 evolved equal parent snapshot identity has non-identical content",
            ));
        }
    } else {
        snapshots.insert(snapshot_sha.clone(), snapshot);
    }
    Ok(V5EvolvedParentSnapshotRef {
        attempt_reference: expected_attempt_reference.clone(),
        parent_snapshot_sha256: snapshot_sha,
    })
}

/// Derive exactly one ordered snapshot-ref receipt for every native attempt.
/// Structural children need both the scheduler-selected source object and the
/// compiler-owned attempt reference; retaining either one alone is not enough
/// to prevent a self-hashed cross-parent substitution at offline adoption.
fn snapshot_refs_for_native_outcome(
    request: &V5EvolvedTransactionRequest,
    planned: &PlannedProposal,
    outcome: &V5EvolvedConstructionOutcome,
    snapshots: &mut BTreeMap<String, V5EvolvedParentSnapshot>,
) -> Result<V5EvolvedAttemptSnapshotRefs> {
    let (parent, mate) =
        match &planned.intent {
            ProposalIntent::RichImmigrant { .. } => {
                if outcome.lineage_refs.parent.is_some() || outcome.lineage_refs.mate.is_some() {
                    return Err(contract(
                        "v5 evolved immigrant construction unexpectedly carries parent lineage",
                    ));
                }
                (None, None)
            }
            ProposalIntent::StructuralMutation {
                parent: selected_parent,
                ..
            } => {
                let attempt_parent = outcome.lineage_refs.parent.as_ref().ok_or_else(|| {
                    contract("v5 evolved mutation construction omits parent lineage")
                })?;
                if outcome.lineage_refs.mate.is_some() {
                    return Err(contract(
                        "v5 evolved mutation construction unexpectedly carries mate lineage",
                    ));
                }
                (
                    Some(snapshot_ref_for_selected_parent(
                        request,
                        selected_parent,
                        attempt_parent,
                        snapshots,
                        "mutation parent",
                    )?),
                    None,
                )
            }
            ProposalIntent::SameSideCrossover {
                parent: selected_parent,
                mate: selected_mate,
                ..
            } => {
                let attempt_parent = outcome.lineage_refs.parent.as_ref().ok_or_else(|| {
                    contract("v5 evolved crossover construction omits parent lineage")
                })?;
                let attempt_mate = outcome.lineage_refs.mate.as_ref().ok_or_else(|| {
                    contract("v5 evolved crossover construction omits mate lineage")
                })?;
                (
                    Some(snapshot_ref_for_selected_parent(
                        request,
                        selected_parent,
                        attempt_parent,
                        snapshots,
                        "crossover parent",
                    )?),
                    Some(snapshot_ref_for_selected_parent(
                        request,
                        selected_mate,
                        attempt_mate,
                        snapshots,
                        "crossover mate",
                    )?),
                )
            }
        };
    Ok(V5EvolvedAttemptSnapshotRefs {
        proposal_ordinal: planned.proposal_ordinal,
        parent,
        mate,
    })
}

fn compact_candidate(record: &V5CompactAcceptedRecord, ledger_candidate: &Value) -> Result<Value> {
    let fields = ledger_candidate
        .as_object()
        .ok_or_else(|| contract("v5 evolved ledger candidate must be an object"))?;
    for key in [
        "candidateIdentitySha256",
        "programSha256",
        "sourceProfileSha256",
        "profileSnapshotSha256",
        "canonicalEvidenceIdentitySha256",
    ] {
        let _ = exact_sha(
            fields
                .get(key)
                .ok_or_else(|| contract(format!("v5 evolved ledger candidate lacks {key}")))?,
            &format!("v5 evolved ledger candidate {key}"),
        )?;
    }
    if fields
        .get("candidateIdentitySha256")
        .and_then(Value::as_str)
        != Some(record.candidate_identity_sha256.as_str())
    {
        return Err(contract(
            "v5 evolved ledger candidate does not bind compact record identity",
        ));
    }
    Ok(ledger_candidate.clone())
}

fn validate_outcome(
    request: &V5EvolvedTransactionRequest,
    authority: &V5SharedConstructionAuthority,
    planned: &PlannedProposal,
    outcome: &V5EvolvedConstructionOutcome,
) -> Result<()> {
    let disposition = stable_code(&outcome.disposition, "v5 evolved outcome disposition")?;
    let _ = stable_code(&outcome.reason_code, "v5 evolved outcome reason code")?;
    let stage = stable_code(&outcome.stage, "v5 evolved outcome stage")?;
    if !matches!(disposition.as_str(), "accepted" | "rejected" | "no_op")
        || !matches!(
            stage.as_str(),
            "pre_plan" | "operator_plan" | "operator_apply" | "compile"
        )
    {
        return Err(contract(
            "v5 evolved construction outcome code is unsupported",
        ));
    }
    let expected_origin = planned.intent.origin_kind();
    outcome.lineage_refs.to_value()?;
    let delta = outcome.delta.as_ref().ok_or_else(|| {
        contract("v5 evolved construction omits its required all-attempt compact proposal delta")
    })?;
    if delta.generation_index != request.generation_index
        || delta.proposal_ordinal != planned.proposal_ordinal
        || delta.generation_config_sha256 != request.generation_config_sha256
        || delta.shared_authority_sha256 != authority.shared_authority_sha256
        || delta.proposal_seed != planned.intent.proposal_seed()
        || delta.origin_kind != expected_origin
        || delta.scheduled_kind != planned_scheduled_kind(&planned.intent)
        || delta.terminal_disposition != disposition
        || delta.terminal_reason_code != outcome.reason_code
    {
        return Err(contract(
            "v5 evolved construction delta does not bind planned proposal",
        ));
    }
    let _ = delta.to_value()?;
    match (&outcome.accepted, disposition.as_str()) {
        (Some(material), "accepted") => {
            let record = &material.record;
            if record.generation_index != request.generation_index
                || record.proposal_ordinal != planned.proposal_ordinal
                || record.proposal_seed != planned.intent.proposal_seed()
                || record.origin_kind != expected_origin
                || record.shared_authority_sha256 != authority.shared_authority_sha256
                || record.proposal_delta_sha256 != delta.delta_sha256()?
            {
                return Err(contract(
                    "v5 evolved compact record does not bind planned construction",
                ));
            }
            let _ = record.to_value()?;
            let _ = compact_candidate(record, &material.ledger_candidate)?;
        }
        (None, "accepted") => {
            return Err(contract(
                "accepted v5 evolved construction omits compact record",
            ));
        }
        (Some(_), _) => {
            return Err(contract(
                "rejected/no-op v5 evolved construction must not expose accepted material",
            ));
        }
        (None, _) => {}
    }
    Ok(())
}

fn local_admission(
    state: &ProposalState,
    material: &V5CoreEvolvedAcceptedMaterial,
) -> (&'static str, &'static str, &'static str) {
    if state
        .local_executable_semantics
        .contains(&material.record.executable_semantic_sha256)
    {
        (
            "duplicate_pair_genome",
            "duplicate_pair_genome",
            "duplicate_executable",
        )
    } else if state
        .local_candidate_identities
        .contains(&material.record.candidate_identity_sha256)
    {
        (
            "duplicate_candidate_identity",
            "duplicate_candidate_identity",
            "duplicate_candidate",
        )
    } else {
        ("accepted", "accepted", "inserted")
    }
}

fn admitted_attempt(
    request: &V5EvolvedTransactionRequest,
    authority: &V5SharedConstructionAuthority,
    state: &ProposalState,
    ledger: &mut dyn IdentityLedger,
    planned: &PlannedProposal,
    outcome: V5EvolvedConstructionOutcome,
) -> Result<(
    V5ProposalAttemptRecord,
    V5AttemptOutcomeAudit,
    Option<V5CompactAcceptedRecord>,
)> {
    validate_outcome(request, authority, planned, &outcome)?;
    let origin_kind = planned.intent.origin_kind().to_owned();
    let (tentative, candidate, semantic, local_reason, local_effect) = match &outcome.accepted {
        Some(material) => {
            let (tentative, reason, effect) = local_admission(state, material);
            (
                tentative,
                Some(compact_candidate(
                    &material.record,
                    &material.ledger_candidate,
                )?),
                Some(material.record.executable_semantic_sha256.as_str()),
                reason,
                effect,
            )
        }
        None => (
            outcome.disposition.as_str(),
            None,
            None,
            outcome.reason_code.as_str(),
            "not_checked",
        ),
    };
    let decision = ledger.prepare_proposal(LedgerProposal {
        proposal_ordinal: planned.proposal_ordinal,
        candidate: candidate.as_ref(),
        executable_semantic_sha256: semantic,
        tentative_disposition: tentative,
    })?;
    let (disposition, reason_code, stage, ledger_effect, accepted) = match tentative {
        "accepted" => match decision.disposition.as_str() {
            "accepted" => (
                "accepted",
                "accepted",
                "accepted",
                "inserted",
                outcome.accepted.map(|material| material.record),
            ),
            "duplicate_candidate_identity_global" | "duplicate_canonical_evidence_global" => (
                "rejected",
                decision.disposition.as_str(),
                "identity_ledger",
                "duplicate_candidate",
                None,
            ),
            "duplicate_pair_genome_global" => (
                "rejected",
                "duplicate_pair_genome_global",
                "identity_ledger",
                "duplicate_executable",
                None,
            ),
            other => {
                return Err(contract(format!(
                    "v5 evolved identity ledger returned unsupported disposition {other}"
                )));
            }
        },
        "duplicate_pair_genome" | "duplicate_candidate_identity" => {
            if decision.disposition != tentative {
                return Err(contract(
                    "v5 evolved identity ledger changed a local duplicate disposition",
                ));
            }
            ("rejected", local_reason, "admission", local_effect, None)
        }
        "rejected" | "no_op" => {
            if decision.disposition != tentative {
                return Err(contract(
                    "v5 evolved identity ledger changed a construction disposition",
                ));
            }
            (
                tentative,
                outcome.reason_code.as_str(),
                outcome.stage.as_str(),
                "not_checked",
                None,
            )
        }
        other => {
            return Err(contract(format!(
                "v5 evolved construction produced unsupported tentative disposition {other}"
            )));
        }
    };
    ledger.commit_prepared_delta(&decision.prepared_delta)?;
    let proposal_delta_sha256 = Some(
        outcome
            .delta
            .as_ref()
            .expect("validate_outcome requires an all-attempt proposal delta")
            .delta_sha256()?,
    );
    let accepted_record_sha256 = accepted
        .as_ref()
        .map(V5CompactAcceptedRecord::record_sha256)
        .transpose()?;
    let lineage_refs_sha256 = canonical_sha256(&outcome.lineage_refs.to_value()?)?;
    let audit = V5AttemptOutcomeAudit {
        generation_index: request.generation_index,
        proposal_ordinal: planned.proposal_ordinal,
        generation_config_sha256: request.generation_config_sha256.clone(),
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        proposal_seed: planned.intent.proposal_seed().to_owned(),
        origin_kind: origin_kind.clone(),
        disposition: disposition.to_owned(),
        reason_code: reason_code.to_owned(),
        stage: stage.to_owned(),
        proposal_delta_sha256: proposal_delta_sha256.clone(),
        lineage_refs_sha256,
        identity_ledger_effect: ledger_effect.to_owned(),
        accepted_record_sha256: accepted_record_sha256.clone(),
    };
    let attempt = V5ProposalAttemptRecord {
        generation_index: request.generation_index,
        proposal_ordinal: planned.proposal_ordinal,
        generation_config_sha256: request.generation_config_sha256.clone(),
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        proposal_seed: planned.intent.proposal_seed().to_owned(),
        origin_kind,
        proposal_delta_sha256,
        disposition: disposition.to_owned(),
        reason_code: reason_code.to_owned(),
        lineage_refs: outcome.lineage_refs,
        identity_ledger_effect: ledger_effect.to_owned(),
        outcome_audit_sha256: audit.audit_sha256()?,
        accepted_record_sha256,
    };
    audit.verify_binds_attempt(&attempt)?;
    Ok((attempt, audit, accepted))
}

/// Typed, write-neutral result.  `thread_cap` remains in the Rust control
/// object as telemetry, but intentionally has no effect on `to_value()`.
#[derive(Clone, Debug)]
pub struct V5EvolvedTransactionResult {
    pub generation_index: u64,
    pub generation_config_sha256: String,
    pub shared_authority_sha256: String,
    pub parent_archive_input_binding_sha256: String,
    pub identity_ledger_input_binding_sha256: String,
    pub target_accepted: u64,
    pub max_attempts: u64,
    pub evaluation_width: u64,
    pub thread_cap: u64,
    pub target_reached: bool,
    pub stop_reason: String,
    pub attempts: Vec<V5ProposalAttemptRecord>,
    pub outcome_audits: Vec<V5AttemptOutcomeAudit>,
    pub attempt_journal: V5AttemptJournal,
    pub proposal_deltas: Vec<V5EvolvedProposalDelta>,
    pub delta_journal: V5EvolvedDeltaJournal,
    pub accepted_records: Vec<V5CompactAcceptedRecord>,
    pub compact_accepted_journal: V5EvolvedCompactAcceptedJournal,
    pub schedule_state_receipt: V5EvolvedScheduleStateReceipt,
    /// Present for every production result.  The `None` state is retained
    /// only for crate-local scheduler tests that deliberately inject a fake
    /// constructor; public construction always seals this inventory.
    pub(crate) parent_snapshot_inventory: Option<V5EvolvedParentSnapshotInventory>,
}

/// Internal one-pass replay handoff for the public evolved publication layer.
///
/// Every callback is made only after the corresponding
/// attempt/audit/delta/record transcript has been verified.  `observe_attempt`
/// retains the full proposal-ordinal stream for the native funnel; its
/// optional material is present for both accepted candidates and ledger
/// duplicate candidates that reached a complete compact record.  `accept`
/// remains limited to durable accepted records, which is the only path that
/// may construct a transient rich candidate.  It is intentionally
/// crate-visible: batch never receives a rich candidate and cannot bypass
/// snapshot/operator replay.
pub(crate) trait V5EvolvedAcceptedReplaySink {
    fn observe_attempt(
        &mut self,
        _authority: &V5SharedConstructionAuthority,
        _attempt: &V5ProposalAttemptRecord,
        _audit: &V5AttemptOutcomeAudit,
        _material: Option<&V5CoreEvolvedAcceptedMaterial>,
    ) -> Result<()> {
        Ok(())
    }

    fn accept(
        &mut self,
        authority: &V5SharedConstructionAuthority,
        material: &V5CoreEvolvedAcceptedMaterial,
    ) -> Result<()>;
}

struct NoopV5EvolvedAcceptedReplaySink;

impl V5EvolvedAcceptedReplaySink for NoopV5EvolvedAcceptedReplaySink {
    fn accept(
        &mut self,
        _authority: &V5SharedConstructionAuthority,
        _material: &V5CoreEvolvedAcceptedMaterial,
    ) -> Result<()> {
        Ok(())
    }
}

/// Operational timings and bounded-concurrency accounting for one offline
/// transaction replay. None of these fields participate in transaction or
/// publication identity.
#[derive(Clone, Debug, Default)]
pub(crate) struct V5EvolvedOfflineReplayTelemetry {
    pub(crate) semantic_validation_wall: Duration,
    pub(crate) planning_wall: Duration,
    pub(crate) snapshot_reconstruction_wall: Duration,
    pub(crate) snapshot_reconstruction_worker_sum: Duration,
    pub(crate) construction_wall: Duration,
    pub(crate) construction_worker_sum: Duration,
    pub(crate) verification_wall: Duration,
    pub(crate) sink_wall: Duration,
    pub(crate) snapshot_count: u64,
    pub(crate) attempt_count: u64,
    pub(crate) accepted_count: u64,
    pub(crate) snapshot_wave_count: u64,
    pub(crate) construction_wave_count: u64,
    pub(crate) peak_active_workers: u64,
}

fn replay_timer(enabled: bool) -> Option<Instant> {
    enabled.then(Instant::now)
}

fn replay_elapsed(started: Option<Instant>) -> Duration {
    started.map_or(Duration::ZERO, |started| started.elapsed())
}

impl V5EvolvedTransactionResult {
    fn semantic_value(&self) -> Result<Value> {
        if self.generation_index < 2
            || self.target_accepted == 0
            || self.max_attempts < self.target_accepted
            || self.evaluation_width == 0
            || self.evaluation_width > self.target_accepted
            || self.attempts.len() as u64 > self.max_attempts
            || self.accepted_records.len() as u64 > self.target_accepted
            || !(1..=8).contains(&self.thread_cap)
        {
            return Err(contract("v5 evolved transaction result bounds are invalid"));
        }
        let config = exact_sha_string(
            &self.generation_config_sha256,
            "v5 evolved transaction result generation config SHA-256",
        )?;
        let authority = exact_sha_string(
            &self.shared_authority_sha256,
            "v5 evolved transaction result authority SHA-256",
        )?;
        let parent_archive_input_binding = exact_sha_string(
            &self.parent_archive_input_binding_sha256,
            "v5 evolved transaction result parent archive input binding SHA-256",
        )?;
        let identity_ledger_input_binding = exact_sha_string(
            &self.identity_ledger_input_binding_sha256,
            "v5 evolved transaction result identity ledger input binding SHA-256",
        )?;
        let target_reached = self.accepted_records.len() as u64 == self.target_accepted;
        if self.target_reached != target_reached
            || self.stop_reason
                != if target_reached {
                    "accepted_target_reached"
                } else {
                    "max_attempts_reached"
                }
            || (!target_reached && self.attempts.len() as u64 != self.max_attempts)
        {
            return Err(contract("v5 evolved transaction stop state is invalid"));
        }
        if self.attempt_journal.generation_index != self.generation_index
            || self.attempt_journal.generation_config_sha256 != config
            || self.attempt_journal.shared_authority_sha256 != authority
            || self.attempt_journal.attempts != self.attempts
            || self.delta_journal.generation_index != self.generation_index
            || self.delta_journal.generation_config_sha256 != config
            || self.delta_journal.shared_authority_sha256 != authority
            || self.delta_journal.deltas != self.proposal_deltas
            || self.compact_accepted_journal.generation_index != self.generation_index
            || self.compact_accepted_journal.generation_config_sha256 != config
            || self.compact_accepted_journal.shared_authority_sha256 != authority
            || self.schedule_state_receipt.generation_index != self.generation_index
            || self.schedule_state_receipt.generation_config_sha256 != config
            || self.schedule_state_receipt.shared_authority_sha256 != authority
        {
            return Err(contract("v5 evolved transaction component binding drifted"));
        }
        let _ = self.attempt_journal.to_value()?;
        let _ = self.delta_journal.to_value()?;
        self.compact_accepted_journal
            .verify_records(&self.accepted_records)?;
        let _ = self.schedule_state_receipt.to_value()?;
        let (parent_snapshot_inventory_value, snapshot_refs_by_ordinal, snapshots_by_sha): (
            Value,
            BTreeMap<u64, &V5EvolvedAttemptSnapshotRefs>,
            BTreeMap<String, &V5EvolvedParentSnapshot>,
        ) = match &self.parent_snapshot_inventory {
            Some(inventory) => {
                if inventory.generation_index != self.generation_index
                    || inventory.generation_config_sha256 != config
                    || inventory.shared_authority_sha256 != authority
                    || inventory.source_parent_archive_input_binding_sha256
                        != parent_archive_input_binding
                    || inventory.source_parent_archive_semantic_sha256
                        != canonical_sha256(
                            &self.schedule_state_receipt.initial_parent_selector_state,
                        )?
                {
                    return Err(contract(
                        "v5 evolved parent snapshot inventory binding drifted from result",
                    ));
                }
                let snapshots = inventory.snapshot_map()?;
                let refs = inventory
                    .attempt_snapshot_refs
                    .iter()
                    .map(|refs| {
                        if refs.proposal_ordinal >= self.max_attempts {
                            return Err(contract(
                                "v5 evolved parent snapshot reference ordinal exceeds transaction bound",
                            ));
                        }
                        Ok((refs.proposal_ordinal, refs))
                    })
                    .collect::<Result<BTreeMap<_, _>>>()?;
                if refs.len() != inventory.attempt_snapshot_refs.len()
                    || refs.len() != self.attempts.len()
                {
                    return Err(contract(
                        "v5 evolved parent snapshot reference count or ordering drifted",
                    ));
                }
                (inventory.to_value()?, refs, snapshots)
            }
            None => {
                // Fixture construction is available only to unit tests which
                // isolate scheduler accounting.  A non-test build can only
                // emit a production result with self-contained snapshots.
                #[cfg(test)]
                {
                    (Value::Null, BTreeMap::new(), BTreeMap::new())
                }
                #[cfg(not(test))]
                {
                    return Err(contract(
                        "v5 evolved production result omits parent snapshot inventory",
                    ));
                }
            }
        };
        let deltas = self
            .proposal_deltas
            .iter()
            .map(|delta| Ok((delta.delta_sha256()?, delta)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        let records = self
            .accepted_records
            .iter()
            .map(|record| Ok((record.record_sha256()?, record)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        if deltas.len() != self.proposal_deltas.len()
            || records.len() != self.accepted_records.len()
        {
            return Err(contract("v5 evolved transaction repeats durable objects"));
        }
        if self.outcome_audits.len() != self.attempts.len()
            || self.proposal_deltas.len() != self.attempts.len()
        {
            return Err(contract(
                "v5 evolved transaction all-attempt audit or delta count drifted",
            ));
        }
        let mut replay_state = ProposalState::default();
        let mut replayed_crossover_attempts = 0_u64;
        for (attempt, audit) in self.attempts.iter().zip(&self.outcome_audits) {
            audit.verify_binds_attempt(attempt)?;
            let delta_sha = attempt.proposal_delta_sha256.as_ref().ok_or_else(|| {
                contract("v5 evolved attempt omits its required all-attempt delta")
            })?;
            let delta = deltas
                .get(delta_sha)
                .ok_or_else(|| contract("v5 evolved attempt names missing delta"))?;
            if delta.proposal_ordinal != attempt.proposal_ordinal
                || delta.proposal_seed != attempt.proposal_seed
                || delta.origin_kind != attempt.origin_kind
            {
                return Err(contract("v5 evolved attempt delta binding drifted"));
            }
            if self.parent_snapshot_inventory.is_some() {
                let snapshot_refs = snapshot_refs_by_ordinal
                    .get(&attempt.proposal_ordinal)
                    .ok_or_else(|| {
                        contract("v5 evolved attempt omits parent snapshot reference receipt")
                    })?;
                match delta.scheduled_kind.as_str() {
                    "random_immigrant" => {
                        if snapshot_refs.parent.is_some()
                            || snapshot_refs.mate.is_some()
                            || attempt.lineage_refs.parent.is_some()
                            || attempt.lineage_refs.mate.is_some()
                        {
                            return Err(contract(
                                "v5 evolved immigrant snapshot references are not all null",
                            ));
                        }
                    }
                    "structural_offspring" => {
                        let parent = snapshot_refs.parent.as_ref().ok_or_else(|| {
                            contract("v5 evolved mutation snapshot references omit parent")
                        })?;
                        if snapshot_refs.mate.is_some()
                            || parent.attempt_reference
                                != *attempt.lineage_refs.parent.as_ref().ok_or_else(|| {
                                    contract("v5 evolved mutation lineage omits parent")
                                })?
                            || V5EvolvedParentSnapshotInventory::resolve_ref(
                                &snapshots_by_sha,
                                parent,
                                "v5 evolved mutation snapshot reference",
                            )?
                            .attempt_reference
                                != parent.attempt_reference
                        {
                            return Err(contract(
                                "v5 evolved mutation snapshot reference binding drifted",
                            ));
                        }
                    }
                    "same_side_crossover" => {
                        let parent = snapshot_refs.parent.as_ref().ok_or_else(|| {
                            contract("v5 evolved crossover snapshot references omit parent")
                        })?;
                        let mate = snapshot_refs.mate.as_ref().ok_or_else(|| {
                            contract("v5 evolved crossover snapshot references omit mate")
                        })?;
                        if parent.attempt_reference
                            != *attempt.lineage_refs.parent.as_ref().ok_or_else(|| {
                                contract("v5 evolved crossover lineage omits parent")
                            })?
                            || mate.attempt_reference
                                != *attempt.lineage_refs.mate.as_ref().ok_or_else(|| {
                                    contract("v5 evolved crossover lineage omits mate")
                                })?
                            || V5EvolvedParentSnapshotInventory::resolve_ref(
                                &snapshots_by_sha,
                                parent,
                                "v5 evolved crossover parent snapshot reference",
                            )?
                            .attempt_reference
                                != parent.attempt_reference
                            || V5EvolvedParentSnapshotInventory::resolve_ref(
                                &snapshots_by_sha,
                                mate,
                                "v5 evolved crossover mate snapshot reference",
                            )?
                            .attempt_reference
                                != mate.attempt_reference
                        {
                            return Err(contract(
                                "v5 evolved crossover snapshot reference binding drifted",
                            ));
                        }
                    }
                    _ => {
                        return Err(contract(
                            "v5 evolved attempt has unsupported snapshot scheduled kind",
                        ));
                    }
                }
            }
            if attempt.origin_kind == "structural_offspring" {
                let receipt_value = delta.parent_selection_receipt.as_ref().ok_or_else(|| {
                    contract("v5 evolved structural attempt delta lacks parent selection receipt")
                })?;
                let receipt = V5EvolvedParentSelectionReceipt::from_value(receipt_value)?;
                if Some(&receipt.parent) != attempt.lineage_refs.parent.as_ref()
                    || receipt.mate.as_ref() != attempt.lineage_refs.mate.as_ref()
                    || attempt
                        .lineage_refs
                        .parent_selection_receipt_sha256
                        .as_deref()
                        != Some(receipt.receipt_sha256()?.as_str())
                {
                    return Err(contract(
                        "v5 evolved structural lineage does not bind parent selection receipt",
                    ));
                }
                replay_state.structural_parent_selections = replay_state
                    .structural_parent_selections
                    .checked_add(receipt.selection_count()?)
                    .ok_or_else(|| {
                        contract("v5 evolved replay structural parent selection counter overflowed")
                    })?;
                if delta.scheduled_kind == "same_side_crossover" {
                    replayed_crossover_attempts =
                        replayed_crossover_attempts.checked_add(1).ok_or_else(|| {
                            contract("v5 evolved replay crossover attempt counter overflowed")
                        })?;
                }
            }
            let accepted = match &attempt.accepted_record_sha256 {
                Some(record_sha) => {
                    let record = records.get(record_sha).ok_or_else(|| {
                        contract("v5 evolved accepted attempt names missing record")
                    })?;
                    if record.proposal_ordinal != attempt.proposal_ordinal
                        || record.proposal_seed != attempt.proposal_seed
                        || record.origin_kind != attempt.origin_kind
                        || attempt.proposal_delta_sha256.as_deref()
                            != Some(record.proposal_delta_sha256.as_str())
                    {
                        return Err(contract("v5 evolved accepted record binding drifted"));
                    }
                    Some(AcceptedProposal {
                        candidate_id: record.candidate_id.clone(),
                        candidate_identity_sha256: record.candidate_identity_sha256.clone(),
                        executable_semantic_sha256: record.executable_semantic_sha256.clone(),
                        descriptor_projection: Some(record.descriptor_projection.clone()),
                    })
                }
                None => None,
            };
            replay_state.observe_compact_attempt(
                attempt.proposal_ordinal,
                &attempt.origin_kind,
                &attempt.disposition,
                &attempt.attempt_sha256()?,
                accepted.as_ref(),
            )?;
        }
        if replay_state.compact_value() != self.schedule_state_receipt.proposal_state {
            return Err(contract("v5 evolved transaction scheduler replay drifted"));
        }
        if replayed_crossover_attempts != self.schedule_state_receipt.crossover_attempts {
            return Err(contract(
                "v5 evolved transaction crossover replay counter drifted",
            ));
        }
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_TRANSACTION_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            ("generationConfigSha256", Value::String(config)),
            ("sharedAuthoritySha256", Value::String(authority)),
            (
                "parentArchiveInputBindingSha256",
                Value::String(parent_archive_input_binding),
            ),
            (
                "identityLedgerInputBindingSha256",
                Value::String(identity_ledger_input_binding),
            ),
            ("targetAccepted", Value::from(self.target_accepted)),
            ("maxAttempts", Value::from(self.max_attempts)),
            ("evaluationWidth", Value::from(self.evaluation_width)),
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
            ("deltaJournal", self.delta_journal.to_value()?),
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
                "scheduleStateReceipt",
                self.schedule_state_receipt.to_value()?,
            ),
            ("parentSnapshotInventory", parent_snapshot_inventory_value),
        ]))
    }

    pub fn transaction_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 evolved transaction")
            .clone();
        fields.insert(
            "transactionSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    /// Parse a persisted self-contained transaction object.  `thread_cap` is
    /// deliberately absent from the semantic object, so deserialization uses
    /// the canonical control-plane default of one; it cannot affect replay or
    /// the transaction identity.
    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = value
            .as_object()
            .ok_or_else(|| contract("v5 evolved transaction result must be an object"))?;
        exact_keys(
            fields,
            &[
                "schemaVersion",
                "generationIndex",
                "generationConfigSha256",
                "sharedAuthoritySha256",
                "parentArchiveInputBindingSha256",
                "identityLedgerInputBindingSha256",
                "targetAccepted",
                "maxAttempts",
                "evaluationWidth",
                "targetReached",
                "stopReason",
                "attemptJournal",
                "attempts",
                "outcomeAudits",
                "deltaJournal",
                "acceptedRecords",
                "compactAcceptedJournal",
                "scheduleStateReceipt",
                "parentSnapshotInventory",
                "transactionSha256",
            ],
            "v5 evolved transaction result",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_TRANSACTION_SCHEMA)
        {
            return Err(contract("v5 evolved transaction result schema is invalid"));
        }
        let attempts = required(value, "attempts", "v5 evolved transaction result")?
            .as_array()
            .ok_or_else(|| contract("v5 evolved transaction attempts must be an array"))?
            .iter()
            .map(V5ProposalAttemptRecord::from_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let outcome_audits = required(value, "outcomeAudits", "v5 evolved transaction result")?
            .as_array()
            .ok_or_else(|| contract("v5 evolved transaction audits must be an array"))?
            .iter()
            .map(V5AttemptOutcomeAudit::from_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let proposal_deltas = required(value, "deltaJournal", "v5 evolved transaction result")?
            .get("deltas")
            .and_then(Value::as_array)
            .ok_or_else(|| contract("v5 evolved transaction delta journal lacks deltas"))?
            .iter()
            .map(V5EvolvedProposalDelta::from_value)
            .collect::<Result<Vec<_>>>()?;
        let accepted_records = required(value, "acceptedRecords", "v5 evolved transaction result")?
            .as_array()
            .ok_or_else(|| contract("v5 evolved transaction records must be an array"))?
            .iter()
            .map(V5CompactAcceptedRecord::from_value)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let parent_snapshot_inventory = {
            let inventory = required(
                value,
                "parentSnapshotInventory",
                "v5 evolved transaction result",
            )?;
            if inventory.is_null() {
                None
            } else {
                Some(V5EvolvedParentSnapshotInventory::from_value(inventory)?)
            }
        };
        let result = Self {
            generation_index: required(value, "generationIndex", "v5 evolved transaction result")?
                .as_u64()
                .ok_or_else(|| contract("v5 evolved transaction generation is invalid"))?,
            generation_config_sha256: exact_sha(
                required(
                    value,
                    "generationConfigSha256",
                    "v5 evolved transaction result",
                )?,
                "v5 evolved transaction generation config SHA-256",
            )?,
            shared_authority_sha256: exact_sha(
                required(
                    value,
                    "sharedAuthoritySha256",
                    "v5 evolved transaction result",
                )?,
                "v5 evolved transaction authority SHA-256",
            )?,
            parent_archive_input_binding_sha256: exact_sha(
                required(
                    value,
                    "parentArchiveInputBindingSha256",
                    "v5 evolved transaction result",
                )?,
                "v5 evolved transaction parent archive input binding SHA-256",
            )?,
            identity_ledger_input_binding_sha256: exact_sha(
                required(
                    value,
                    "identityLedgerInputBindingSha256",
                    "v5 evolved transaction result",
                )?,
                "v5 evolved transaction identity ledger input binding SHA-256",
            )?,
            target_accepted: required(value, "targetAccepted", "v5 evolved transaction result")?
                .as_u64()
                .ok_or_else(|| contract("v5 evolved transaction target is invalid"))?,
            max_attempts: required(value, "maxAttempts", "v5 evolved transaction result")?
                .as_u64()
                .ok_or_else(|| contract("v5 evolved transaction maximum attempts is invalid"))?,
            evaluation_width: required(value, "evaluationWidth", "v5 evolved transaction result")?
                .as_u64()
                .ok_or_else(|| contract("v5 evolved transaction evaluation width is invalid"))?,
            thread_cap: 1,
            target_reached: required(value, "targetReached", "v5 evolved transaction result")?
                .as_bool()
                .ok_or_else(|| contract("v5 evolved transaction target state is invalid"))?,
            stop_reason: exact_text(
                required(value, "stopReason", "v5 evolved transaction result")?,
                "v5 evolved transaction stop reason",
            )?,
            attempts,
            outcome_audits,
            attempt_journal: V5AttemptJournal::from_value(required(
                value,
                "attemptJournal",
                "v5 evolved transaction result",
            )?)?,
            proposal_deltas,
            delta_journal: V5EvolvedDeltaJournal::from_value(required(
                value,
                "deltaJournal",
                "v5 evolved transaction result",
            )?)?,
            accepted_records,
            compact_accepted_journal: V5EvolvedCompactAcceptedJournal::from_value(required(
                value,
                "compactAcceptedJournal",
                "v5 evolved transaction result",
            )?)?,
            schedule_state_receipt: V5EvolvedScheduleStateReceipt::from_value(required(
                value,
                "scheduleStateReceipt",
                "v5 evolved transaction result",
            )?)?,
            parent_snapshot_inventory,
        };
        let supplied = exact_sha(
            required(value, "transactionSha256", "v5 evolved transaction result")?,
            "v5 evolved transaction SHA-256",
        )?;
        if supplied != result.transaction_sha256()? || &result.to_value()? != value {
            return Err(contract("v5 evolved transaction result identity drifted"));
        }
        Ok(result)
    }

    pub fn verify_replay(&self) -> Result<()> {
        let _ = self.semantic_value()?;
        Ok(())
    }

    /// Offline adoption/replay boundary.  It deliberately consumes no parent
    /// selector, identity ledger implementation, archive path, or source
    /// file: the request contributes only its preserved transport bindings
    /// and sealed authority, while every structural parent is reconstructed
    /// from the result's content-addressed snapshot inventory.
    pub fn verify_offline_replay(&self, request: &V5EvolvedTransactionRequest) -> Result<()> {
        let mut sink = NoopV5EvolvedAcceptedReplaySink;
        replay_v5_evolved_transaction_with_accepted_sink(request, self, &mut sink)
    }

    fn verify_offline_replay_with_authority_and_accepted_sink(
        &self,
        request: &V5EvolvedTransactionRequest,
        authority: &V5SharedConstructionAuthority,
        thread_cap: u64,
        collect_telemetry: bool,
        sink: &mut dyn V5EvolvedAcceptedReplaySink,
    ) -> Result<V5EvolvedOfflineReplayTelemetry> {
        let mut telemetry = V5EvolvedOfflineReplayTelemetry::default();
        let semantic_validation_started = replay_timer(collect_telemetry);
        if !(1..=8).contains(&thread_cap) {
            return Err(contract(
                "v5 evolved offline replay thread cap must be between one and eight",
            ));
        }
        self.verify_replay()?;
        if self.generation_index != request.generation_index
            || self.generation_config_sha256 != request.generation_config_sha256
            || self.shared_authority_sha256 != authority.shared_authority_sha256
            || self.parent_archive_input_binding_sha256
                != request.parent_archive_input_binding_sha256
            || self.identity_ledger_input_binding_sha256
                != request.identity_ledger_input_binding_sha256
            || self.target_accepted != request.target_accepted
            || self.max_attempts != request.max_attempts
            || self.evaluation_width != request.evaluation_width
            || self.schedule_state_receipt.parent_schedule_sha256
                != request
                    .parent_schedule
                    .map(RotatingParentSchedule::schedule_sha256)
            || canonical_sha256(&self.schedule_state_receipt.initial_parent_selector_state)?
                != request.parent_selector_state_sha256
            || canonical_sha256(&self.schedule_state_receipt.identity_ledger_identity)?
                != request.identity_ledger_identity_sha256
            || canonical_sha256(&self.schedule_state_receipt.initial_identity_ledger_state)?
                != request.identity_ledger_state_sha256
        {
            return Err(contract(
                "v5 evolved offline replay request binding drifted from transaction result",
            ));
        }
        let inventory = self.parent_snapshot_inventory.as_ref().ok_or_else(|| {
            contract("v5 evolved offline replay requires a parent snapshot inventory")
        })?;
        if inventory.source_parent_archive_input_binding_sha256
            != request.parent_archive_input_binding_sha256
            || inventory.source_parent_archive_semantic_sha256
                != request.parent_selector_state_sha256
        {
            return Err(contract(
                "v5 evolved offline snapshot source binding drifted from preserved request",
            ));
        }
        let snapshots = inventory.snapshot_map()?;
        let snapshot_refs = inventory
            .attempt_snapshot_refs
            .iter()
            .map(|refs| Ok((refs.proposal_ordinal, refs)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        let deltas = self
            .proposal_deltas
            .iter()
            .map(|delta| Ok((delta.proposal_ordinal, delta)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        let records = self
            .accepted_records
            .iter()
            .map(|record| Ok((record.record_sha256()?, record)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        if snapshot_refs.len() != self.attempts.len()
            || deltas.len() != self.attempts.len()
            || records.len() != self.accepted_records.len()
        {
            return Err(contract(
                "v5 evolved offline replay durable object counts drifted",
            ));
        }
        telemetry.semantic_validation_wall = replay_elapsed(semantic_validation_started);

        // Snapshot objects are immutable and content-addressed. They are
        // reconstructed serially on first use during ordered planning below,
        // preserving the cap-one callback and failure boundary while still
        // caching each distinct source parent exactly once.
        let replay_engine = NativeV5EvolvedConstructionEngine::fast_ephemeral();
        let replay_parent_cache = replay_engine
            .parent_cache
            .as_deref()
            .expect("fast-ephemeral replay engine owns a verified-parent cache");
        let mut verified_snapshot_references = BTreeMap::new();

        // Planning remains serial and ordinal ordered. Each plan/construct/
        // verify/sink wave is bounded by the cap, so rich accepted material
        // cannot accumulate to population size. The birth ordinal is the
        // durable accepted count in the strict prefix, already sealed by the
        // transaction and independent of worker completion order.
        let construction_activity = V5ConstructionActivity::default();
        let mut planned_birth_ordinal = 0_u64;
        let mut replayed_birth_count = 0_u64;
        for attempt_wave_start in (0..self.attempts.len()).step_by(thread_cap as usize) {
            let attempt_wave_end = self
                .attempts
                .len()
                .min(attempt_wave_start.saturating_add(thread_cap as usize));
            let planning_started = replay_timer(collect_telemetry);
            let snapshot_count_before_wave = telemetry.snapshot_count;
            let snapshot_wall_before_wave = telemetry.snapshot_reconstruction_wall;
            let mut plans = Vec::with_capacity(attempt_wave_end - attempt_wave_start);
            let mut deferred_planning_error = None;
            for attempt_index in attempt_wave_start..attempt_wave_end {
                let attempt = &self.attempts[attempt_index];
                let planned = (|| {
                    let delta = deltas.get(&attempt.proposal_ordinal).ok_or_else(|| {
                        contract("v5 evolved offline replay attempt names missing delta ordinal")
                    })?;
                    let refs = snapshot_refs.get(&attempt.proposal_ordinal).ok_or_else(|| {
                        contract(
                            "v5 evolved offline replay attempt names missing snapshot references",
                        )
                    })?;
                    offline_planned_proposal_from_snapshot(
                        authority,
                        inventory,
                        &snapshots,
                        &mut verified_snapshot_references,
                        replay_parent_cache,
                        &mut telemetry,
                        collect_telemetry,
                        attempt,
                        delta,
                        refs,
                    )
                })();
                let planned = match planned {
                    Ok(planned) => planned,
                    Err(error) => {
                        deferred_planning_error = Some(error);
                        break;
                    }
                };
                plans.push((attempt_index, planned, planned_birth_ordinal));
                if attempt.accepted_record_sha256.is_some() {
                    planned_birth_ordinal =
                        planned_birth_ordinal.checked_add(1).ok_or_else(|| {
                            contract("v5 evolved offline replay birth ordinal overflowed")
                        })?;
                }
            }
            if telemetry.snapshot_count != snapshot_count_before_wave {
                telemetry.snapshot_wave_count = telemetry.snapshot_wave_count.saturating_add(1);
                telemetry.peak_active_workers = telemetry.peak_active_workers.max(1);
            }
            telemetry.planning_wall += replay_elapsed(planning_started).saturating_sub(
                telemetry
                    .snapshot_reconstruction_wall
                    .saturating_sub(snapshot_wall_before_wave),
            );

            if !plans.is_empty() {
                telemetry.construction_wave_count =
                    telemetry.construction_wave_count.saturating_add(1);
            }
            let construction_started = replay_timer(collect_telemetry);
            let wave_results = if plans.is_empty() {
                Vec::new()
            } else if plans.len() == 1 {
                let (_, planned, birth_ordinal) = &plans[0];
                let _worker = V5ConstructionWorkerGuard::new(&construction_activity, None);
                let worker_started = replay_timer(collect_telemetry);
                let result = replay_engine.construct(authority, request, planned, *birth_ordinal);
                vec![(result, replay_elapsed(worker_started))]
            } else {
                std::thread::scope(|scope| {
                    let mut handles = Vec::with_capacity(plans.len());
                    for (_, planned, birth_ordinal) in &plans {
                        let activity = &construction_activity;
                        let replay_engine = &replay_engine;
                        handles.push(scope.spawn(move || {
                            let _worker = V5ConstructionWorkerGuard::new(activity, None);
                            let worker_started = replay_timer(collect_telemetry);
                            let result = replay_engine.construct(
                                authority,
                                request,
                                planned,
                                *birth_ordinal,
                            );
                            (result, replay_elapsed(worker_started))
                        }));
                    }
                    handles
                        .into_iter()
                        .map(|handle| match handle.join() {
                            Ok(result) => result,
                            Err(_) => (
                                Err(contract("v5 evolved offline construction worker panicked")),
                                Duration::ZERO,
                            ),
                        })
                        .collect::<Vec<_>>()
                })
            };
            telemetry.construction_wall += replay_elapsed(construction_started);

            // All workers have joined before the first result is inspected.
            // `plans` and `wave_results` retain proposal order, making `?`
            // report the lowest-ordinal construction failure in the wave.
            for ((attempt_index, _, _), (result, worker_wall)) in
                plans.into_iter().zip(wave_results)
            {
                telemetry.construction_worker_sum += worker_wall;
                let replayed = result?;
                let attempt = &self.attempts[attempt_index];
                let audit = &self.outcome_audits[attempt_index];
                let delta = deltas.get(&attempt.proposal_ordinal).ok_or_else(|| {
                    contract("v5 evolved offline replay attempt names missing delta ordinal")
                })?;
                let verification_started = replay_timer(collect_telemetry);
                verify_offline_replayed_outcome(attempt, audit, delta, &replayed, &records)?;
                telemetry.verification_wall += replay_elapsed(verification_started);
                // Preserve the durable proposal ordinal even when no rich
                // candidate exists. Publication uses this hook to emit a
                // candidate-free funnel attempt rather than shortening the
                // pre-finalizer's proposal ledger.
                let sink_started = replay_timer(collect_telemetry);
                sink.observe_attempt(authority, attempt, audit, replayed.accepted.as_ref())?;
                if attempt.accepted_record_sha256.is_some() {
                    let material = replayed.accepted.as_ref().ok_or_else(|| {
                        contract(
                            "v5 evolved offline accepted attempt omitted reconstructed accepted material",
                        )
                    })?;
                    sink.accept(authority, material)?;
                    replayed_birth_count =
                        replayed_birth_count.checked_add(1).ok_or_else(|| {
                            contract("v5 evolved offline replay birth ordinal overflowed")
                        })?;
                }
                telemetry.sink_wall += replay_elapsed(sink_started);
                telemetry.attempt_count = telemetry.attempt_count.saturating_add(1);
            }
            if let Some(error) = deferred_planning_error {
                return Err(error);
            }
        }
        telemetry.peak_active_workers = telemetry
            .peak_active_workers
            .max(construction_activity.peak.load(Ordering::Relaxed));
        if construction_activity.active.load(Ordering::Relaxed) != 0 {
            return Err(contract(
                "v5 evolved offline construction workers did not return to zero",
            ));
        }
        if replayed_birth_count != self.accepted_records.len() as u64
            || planned_birth_ordinal != replayed_birth_count
        {
            return Err(contract(
                "v5 evolved offline replay accepted birth count drifted",
            ));
        }
        telemetry.accepted_count = replayed_birth_count;
        Ok(telemetry)
    }
}

/// Replay an evolved transaction exactly once and synchronously hand each
/// verified proposal attempt to `sink`, with a second callback for durable
/// accepted material. Fresh publication uses this to emit its full funnel
/// attempt stream plus the three accepted-only arrays in one traversal;
/// normal offline replay uses a no-op sink. The function never opens archived
/// files, invokes a caller construction callback, or retains a
/// population-sized rich vector.
pub(crate) fn replay_v5_evolved_transaction_with_accepted_sink(
    request: &V5EvolvedTransactionRequest,
    result: &V5EvolvedTransactionResult,
    sink: &mut dyn V5EvolvedAcceptedReplaySink,
) -> Result<()> {
    replay_v5_evolved_transaction_with_accepted_sink_internal(request, result, 1, false, sink)
        .map(|_| ())
}

/// Cap-aware operational variant of the offline replay boundary. Only sealed
/// attempt construction uses the supplied worker bound; snapshot
/// reconstruction, planning, verification, and every sink callback remain
/// strictly ordered.
pub(crate) fn replay_v5_evolved_transaction_with_accepted_sink_and_cap(
    request: &V5EvolvedTransactionRequest,
    result: &V5EvolvedTransactionResult,
    thread_cap: u64,
    sink: &mut dyn V5EvolvedAcceptedReplaySink,
) -> Result<V5EvolvedOfflineReplayTelemetry> {
    replay_v5_evolved_transaction_with_accepted_sink_internal(
        request, result, thread_cap, true, sink,
    )
}

pub(crate) fn replay_v5_evolved_transaction_with_accepted_sink_and_cap_without_telemetry(
    request: &V5EvolvedTransactionRequest,
    result: &V5EvolvedTransactionResult,
    thread_cap: u64,
    sink: &mut dyn V5EvolvedAcceptedReplaySink,
) -> Result<()> {
    replay_v5_evolved_transaction_with_accepted_sink_internal(
        request, result, thread_cap, false, sink,
    )
    .map(|_| ())
}

fn replay_v5_evolved_transaction_with_accepted_sink_internal(
    request: &V5EvolvedTransactionRequest,
    result: &V5EvolvedTransactionResult,
    thread_cap: u64,
    collect_telemetry: bool,
    sink: &mut dyn V5EvolvedAcceptedReplaySink,
) -> Result<V5EvolvedOfflineReplayTelemetry> {
    let validation_started = replay_timer(collect_telemetry);
    request.validate_shape()?;
    let authority = V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)?;
    let request_validation_wall = replay_elapsed(validation_started);
    let mut telemetry = result.verify_offline_replay_with_authority_and_accepted_sink(
        request,
        &authority,
        thread_cap,
        collect_telemetry,
        sink,
    )?;
    telemetry.semantic_validation_wall += request_validation_wall;
    Ok(telemetry)
}

/// Recover only the archive-retained children from a sealed evolved
/// transaction.  The public result contains no rich genome payload, so the
/// next generation must replay the authenticated transaction/snapshot chain
/// through the compiler before it can obtain an opaque `ParentReference`.
/// Callers name the bounded retained candidate set; no unselected rich parent
/// material crosses this boundary.
pub fn reconstruct_selected_parent_references(
    request: &V5EvolvedTransactionRequest,
    result: &V5EvolvedTransactionResult,
    selected_candidate_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, ParentReference>> {
    struct SelectedParentSink<'a> {
        selected: &'a BTreeSet<String>,
        references: BTreeMap<String, ParentReference>,
    }

    impl V5EvolvedAcceptedReplaySink for SelectedParentSink<'_> {
        fn accept(
            &mut self,
            _authority: &V5SharedConstructionAuthority,
            material: &V5CoreEvolvedAcceptedMaterial,
        ) -> Result<()> {
            if self.selected.contains(&material.record.candidate_id) {
                let reference =
                    parent_reference_from_v5_evolved_material(&material.parent_material)?;
                if self
                    .references
                    .insert(material.record.candidate_id.clone(), reference)
                    .is_some()
                {
                    return Err(contract(
                        "v5 evolved selected-parent replay repeats a candidate",
                    ));
                }
            }
            Ok(())
        }
    }

    let mut sink = SelectedParentSink {
        selected: selected_candidate_ids,
        references: BTreeMap::new(),
    };
    replay_v5_evolved_transaction_with_accepted_sink(request, result, &mut sink)?;
    if sink.references.keys().collect::<BTreeSet<_>>()
        != selected_candidate_ids.iter().collect::<BTreeSet<_>>()
    {
        return Err(contract(
            "v5 evolved selected-parent replay lacks an archive candidate",
        ));
    }
    Ok(sink.references)
}

fn snapshot_parent_reference_for_offline_replay(
    authority: &V5SharedConstructionAuthority,
    snapshots: &BTreeMap<String, &V5EvolvedParentSnapshot>,
    verified_references: &mut BTreeMap<String, ParentReference>,
    replay_parent_cache: &V5VerifiedParentCache,
    telemetry: &mut V5EvolvedOfflineReplayTelemetry,
    collect_telemetry: bool,
    snapshot_ref: &V5EvolvedParentSnapshotRef,
    expected_attempt_reference: &V5AttemptParentReference,
    label: &str,
) -> Result<ParentReference> {
    if snapshot_ref.attempt_reference != *expected_attempt_reference {
        return Err(contract(format!(
            "v5 evolved offline {label} snapshot reference does not bind attempt lineage"
        )));
    }
    let snapshot = V5EvolvedParentSnapshotInventory::resolve_ref(snapshots, snapshot_ref, label)?;
    if let Some(reference) = verified_references.get(&snapshot_ref.parent_snapshot_sha256) {
        return Ok(reference.clone());
    }
    let snapshot_started = replay_timer(collect_telemetry);
    let material = load_v5_evolved_parent_from_snapshot(authority, snapshot)?;
    if material.attempt_reference != *expected_attempt_reference
        || material.pair_identity_sha256 != snapshot.parent_reference.pair_identity_sha256
        || material.candidate_id != snapshot.parent_reference.candidate_id
    {
        return Err(contract(format!(
            "v5 evolved offline {label} sealed loader drifted from snapshot identity"
        )));
    }
    let reference = snapshot.parent_reference_for_replay()?;
    replay_parent_cache.seed_verified(authority, &reference, material)?;
    let elapsed = replay_elapsed(snapshot_started);
    telemetry.snapshot_reconstruction_wall += elapsed;
    telemetry.snapshot_reconstruction_worker_sum += elapsed;
    telemetry.snapshot_count = telemetry.snapshot_count.saturating_add(1);
    verified_references.insert(
        snapshot_ref.parent_snapshot_sha256.clone(),
        reference.clone(),
    );
    Ok(reference)
}

fn offline_planned_proposal_from_snapshot(
    authority: &V5SharedConstructionAuthority,
    inventory: &V5EvolvedParentSnapshotInventory,
    snapshots: &BTreeMap<String, &V5EvolvedParentSnapshot>,
    verified_references: &mut BTreeMap<String, ParentReference>,
    replay_parent_cache: &V5VerifiedParentCache,
    telemetry: &mut V5EvolvedOfflineReplayTelemetry,
    collect_telemetry: bool,
    attempt: &V5ProposalAttemptRecord,
    delta: &V5EvolvedProposalDelta,
    snapshot_refs: &V5EvolvedAttemptSnapshotRefs,
) -> Result<PlannedProposal> {
    if delta.proposal_ordinal != attempt.proposal_ordinal
        || delta.proposal_seed != attempt.proposal_seed
        || delta.origin_kind != attempt.origin_kind
        || snapshot_refs.proposal_ordinal != attempt.proposal_ordinal
    {
        return Err(contract(
            "v5 evolved offline proposal/delta/snapshot ordinal binding drifted",
        ));
    }
    let intent = match delta.scheduled_kind.as_str() {
        "random_immigrant" => {
            if snapshot_refs.parent.is_some()
                || snapshot_refs.mate.is_some()
                || delta.parent.is_some()
                || delta.mate.is_some()
            {
                return Err(contract(
                    "v5 evolved offline immigrant carries structural snapshot linkage",
                ));
            }
            ProposalIntent::RichImmigrant {
                proposal_seed: delta.proposal_seed.clone(),
                long_seed: immigrant_side_seed(&delta.proposal_seed, Side::Long),
                short_seed: immigrant_side_seed(&delta.proposal_seed, Side::Short),
            }
        }
        "structural_offspring" => {
            let snapshot_ref = snapshot_refs.parent.as_ref().ok_or_else(|| {
                contract("v5 evolved offline mutation omits parent snapshot reference")
            })?;
            if snapshot_refs.mate.is_some() || delta.mate.is_some() {
                return Err(contract(
                    "v5 evolved offline mutation carries a mate snapshot reference",
                ));
            }
            let attempt_parent = delta.parent.as_ref().ok_or_else(|| {
                contract("v5 evolved offline mutation delta omits parent lineage")
            })?;
            let parent = snapshot_parent_reference_for_offline_replay(
                authority,
                snapshots,
                verified_references,
                replay_parent_cache,
                telemetry,
                collect_telemetry,
                snapshot_ref,
                attempt_parent,
                "mutation parent",
            )?;
            ProposalIntent::StructuralMutation {
                proposal_seed: delta.proposal_seed.clone(),
                parent,
                mutation_depth: delta.mutation_depth.ok_or_else(|| {
                    contract("v5 evolved offline mutation delta omits mutation depth")
                })?,
            }
        }
        "same_side_crossover" => {
            let parent_ref = snapshot_refs.parent.as_ref().ok_or_else(|| {
                contract("v5 evolved offline crossover omits parent snapshot reference")
            })?;
            let mate_ref = snapshot_refs.mate.as_ref().ok_or_else(|| {
                contract("v5 evolved offline crossover omits mate snapshot reference")
            })?;
            let attempt_parent = delta.parent.as_ref().ok_or_else(|| {
                contract("v5 evolved offline crossover delta omits parent lineage")
            })?;
            let attempt_mate = delta
                .mate
                .as_ref()
                .ok_or_else(|| contract("v5 evolved offline crossover delta omits mate lineage"))?;
            let parent = snapshot_parent_reference_for_offline_replay(
                authority,
                snapshots,
                verified_references,
                replay_parent_cache,
                telemetry,
                collect_telemetry,
                parent_ref,
                attempt_parent,
                "crossover parent",
            )?;
            let mate = snapshot_parent_reference_for_offline_replay(
                authority,
                snapshots,
                verified_references,
                replay_parent_cache,
                telemetry,
                collect_telemetry,
                mate_ref,
                attempt_mate,
                "crossover mate",
            )?;
            let receipt = V5EvolvedParentSelectionReceipt::from_value(
                delta.parent_selection_receipt.as_ref().ok_or_else(|| {
                    contract("v5 evolved offline crossover delta lacks selection receipt")
                })?,
            )?;
            let side = match proposal_side_for_seed(&delta.proposal_seed).map_err(|error| {
                contract(format!(
                    "v5 evolved offline crossover side routing failed: {error}"
                ))
            })? {
                "long" => Side::Long,
                "short" => Side::Short,
                _ => return Err(contract("v5 evolved offline crossover side is unsupported")),
            };
            ProposalIntent::SameSideCrossover {
                proposal_seed: delta.proposal_seed.clone(),
                side,
                parent,
                mate,
                mate_selection_attempts: receipt.mate_selection_attempts,
            }
        }
        _ => {
            return Err(contract(
                "v5 evolved offline replay delta has unsupported scheduled kind",
            ));
        }
    };
    if inventory.generation_index != attempt.generation_index {
        return Err(contract(
            "v5 evolved offline proposal inventory generation binding drifted",
        ));
    }
    intent.validate().map_err(|error| {
        contract(format!(
            "v5 evolved offline planned proposal is invalid: {error}"
        ))
    })?;
    Ok(PlannedProposal {
        proposal_ordinal: attempt.proposal_ordinal,
        intent,
    })
}

fn verify_offline_replayed_outcome(
    attempt: &V5ProposalAttemptRecord,
    audit: &V5AttemptOutcomeAudit,
    expected_delta: &V5EvolvedProposalDelta,
    replayed: &V5EvolvedConstructionOutcome,
    records: &BTreeMap<String, &V5CompactAcceptedRecord>,
) -> Result<()> {
    let replayed_delta = replayed.delta.as_ref().ok_or_else(|| {
        contract("v5 evolved offline sealed constructor omitted all-attempt delta")
    })?;
    if replayed_delta.to_value()? != expected_delta.to_value()?
        || replayed.lineage_refs.to_value()? != attempt.lineage_refs.to_value()?
    {
        return Err(contract(
            "v5 evolved offline sealed constructor does not reproduce operator transcript",
        ));
    }
    match &replayed.accepted {
        Some(material) => {
            if replayed.disposition != "accepted"
                || material.proposal_delta != expected_delta.to_value()?
            {
                return Err(contract(
                    "v5 evolved offline accepted material does not bind reconstructed delta",
                ));
            }
            match &attempt.accepted_record_sha256 {
                Some(record_sha) => {
                    let record = records.get(record_sha).ok_or_else(|| {
                        contract("v5 evolved offline accepted attempt names missing record")
                    })?;
                    if attempt.disposition != "accepted"
                        || audit.stage != "accepted"
                        || audit.reason_code != "accepted"
                        || material.record.to_value()? != record.to_value()?
                    {
                        return Err(contract(
                            "v5 evolved offline accepted record does not reproduce sealed material",
                        ));
                    }
                }
                None => {
                    if attempt.disposition != "rejected"
                        || !matches!(audit.stage.as_str(), "admission" | "identity_ledger")
                        || !matches!(
                            audit.reason_code.as_str(),
                            "duplicate_pair_genome"
                                | "duplicate_candidate_identity"
                                | "duplicate_pair_genome_global"
                                | "duplicate_candidate_identity_global"
                                | "duplicate_canonical_evidence_global"
                        )
                    {
                        return Err(contract(
                            "v5 evolved offline accepted construction has invalid duplicate admission audit",
                        ));
                    }
                }
            }
        }
        None => {
            if attempt.accepted_record_sha256.is_some()
                || attempt.disposition != replayed.disposition
                || attempt.reason_code != replayed.reason_code
                || audit.stage != replayed.stage
                || audit.identity_ledger_effect != "not_checked"
            {
                return Err(contract(
                    "v5 evolved offline rejected/no-op transcript drifted from sealed constructor",
                ));
            }
        }
    }
    Ok(())
}

/// Public offline adoption entry point for an already-typed later-generation
/// result.  It does not reopen the source parent archive or identity ledger.
pub fn verify_v5_evolved_transaction_replay(
    request: &V5EvolvedTransactionRequest,
    result: &V5EvolvedTransactionResult,
) -> Result<()> {
    result.verify_offline_replay(request)
}

/// Execute scheduling, ordinal merge, local duplicate detection, and global
/// ledger admission using a sealed construction engine. Construction is
/// performed in bounded optimistic waves; all semantic state is re-planned and
/// committed in exact proposal-ordinal order.
#[derive(Clone, Copy)]
struct V5EvolvedExecutionOptions {
    immediate_replay_policy: V5ImmediateConstructionReplayPolicy,
    capture_parent_snapshots: bool,
}

impl V5EvolvedExecutionOptions {
    const fn durable() -> Self {
        Self {
            immediate_replay_policy: V5ImmediateConstructionReplayPolicy::IndependentReplayV1,
            capture_parent_snapshots: true,
        }
    }

    const fn fast_ephemeral() -> Self {
        Self {
            immediate_replay_policy:
                V5ImmediateConstructionReplayPolicy::DeferredToFastEphemeralPublicationV1,
            capture_parent_snapshots: true,
        }
    }

    #[cfg(test)]
    const fn scheduler_test() -> Self {
        Self {
            immediate_replay_policy:
                V5ImmediateConstructionReplayPolicy::DeferredToFastEphemeralPublicationV1,
            capture_parent_snapshots: false,
        }
    }
}

#[derive(Debug, Default)]
struct V5EvolvedExecutionTelemetry {
    plan_wall: Duration,
    construct_wall: Duration,
    immediate_replay_wall: Duration,
    snapshot_wall: Duration,
    admission_ledger_wall: Duration,
    state_commit_wall: Duration,
    planned_count: u64,
    constructed_count: u64,
    immediate_replay_count: u64,
    snapshot_count: u64,
    committed_count: u64,
    wave_count: u64,
    speculative_discarded_count: u64,
    peak_active_workers: u64,
    peak_in_flight: u64,
}

#[derive(Default)]
struct V5ConstructionActivity {
    active: AtomicU64,
    peak: AtomicU64,
}

type V5VerifiedParentCacheCell =
    OnceLock<std::result::Result<Arc<V5EvolvedParentMaterial>, String>>;

/// Generation-scoped, authority-bound cache for opaque parent material. The
/// complete selector reference is the key and each distinct parent is
/// reconstructed once; concurrent requests single-flight through `OnceLock`.
#[derive(Default)]
struct V5VerifiedParentCache {
    entries: Mutex<BTreeMap<String, Arc<V5VerifiedParentCacheCell>>>,
    hits: AtomicU64,
    misses: AtomicU64,
    verification_nanos: AtomicU64,
}

impl V5VerifiedParentCache {
    fn key(shared_authority_sha256: &str, parent: &ParentReference) -> Result<String> {
        Ok(canonical_sha256(&object([
            (
                "sharedAuthoritySha256",
                Value::String(shared_authority_sha256.to_owned()),
            ),
            (
                "pairIdentitySha256",
                Value::String(parent.pair_identity_sha256.clone()),
            ),
            ("candidateId", Value::String(parent.candidate_id.clone())),
            ("pairPayload", parent.pair_payload.clone()),
        ]))?)
    }

    fn load(
        &self,
        authority: &V5SharedConstructionAuthority,
        parent: &ParentReference,
    ) -> Result<Arc<V5EvolvedParentMaterial>> {
        // Selection evidence is draw-specific and must be validated on every
        // use, but it does not change the immutable parent compiler material.
        // Keeping it out of the cache key lets repeated selections of the
        // same archive member share one reconstruction without allowing an
        // invalid selection receipt to bypass its own admission gate.
        parent
            .validate()
            .map_err(|error| contract(format!("v5 evolved parent reference: {error}")))?;
        let key = Self::key(&authority.shared_authority_sha256, parent)?;
        let (cell, inserted) = {
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| contract("v5 evolved verified-parent cache lock was poisoned"))?;
            match entries.get(&key) {
                Some(cell) => (Arc::clone(cell), false),
                None => {
                    let cell = Arc::new(OnceLock::new());
                    entries.insert(key, Arc::clone(&cell));
                    (cell, true)
                }
            }
        };
        if inserted {
            self.misses.fetch_add(1, Ordering::Relaxed);
        } else {
            self.hits.fetch_add(1, Ordering::Relaxed);
        }
        let loaded = cell.get_or_init(|| {
            let started = Instant::now();
            let result = load_v5_evolved_parent(authority, parent)
                .map(Arc::new)
                .map_err(|error| error.to_string());
            let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let _ = self.verification_nanos.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |value| Some(value.saturating_add(nanos)),
            );
            result
        });
        loaded.clone().map_err(|error| {
            contract(format!(
                "v5 evolved verified-parent cache reconstruction failed: {error}"
            ))
        })
    }

    /// Seed material reconstructed from an authenticated offline snapshot so
    /// the immediately following constructor does not compile the same
    /// content-addressed parent again. The snapshot loader remains the source
    /// of the material; this cache accepts it only after its opaque selector
    /// payload and authority bindings reproduce exactly.
    fn seed_verified(
        &self,
        authority: &V5SharedConstructionAuthority,
        parent: &ParentReference,
        material: V5EvolvedParentMaterial,
    ) -> Result<()> {
        parent
            .validate()
            .map_err(|error| contract(format!("v5 evolved parent reference: {error}")))?;
        let reconstructed = parent_reference_from_v5_evolved_material(&material)?;
        if material.accepted_record.shared_authority_sha256 != authority.shared_authority_sha256
            || Self::key(&authority.shared_authority_sha256, parent)?
                != Self::key(&authority.shared_authority_sha256, &reconstructed)?
        {
            return Err(contract(
                "v5 evolved snapshot material does not bind the verified-parent cache key",
            ));
        }
        let key = Self::key(&authority.shared_authority_sha256, parent)?;
        let cell = {
            let mut entries = self
                .entries
                .lock()
                .map_err(|_| contract("v5 evolved verified-parent cache lock was poisoned"))?;
            Arc::clone(
                entries
                    .entry(key)
                    .or_insert_with(|| Arc::new(OnceLock::new())),
            )
        };
        match cell.set(Ok(Arc::new(material))) {
            Ok(()) => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(_) => Ok(()),
        }
    }
}

fn load_parent_for_construction(
    cache: Option<&V5VerifiedParentCache>,
    authority: &V5SharedConstructionAuthority,
    parent: &ParentReference,
) -> Result<Arc<V5EvolvedParentMaterial>> {
    match cache {
        Some(cache) => cache.load(authority, parent),
        None => Ok(Arc::new(load_v5_evolved_parent(authority, parent)?)),
    }
}

impl V5ConstructionActivity {
    fn worker_started(&self) {
        let active = self
            .active
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let mut observed = self.peak.load(Ordering::Relaxed);
        while active > observed {
            match self.peak.compare_exchange_weak(
                observed,
                active,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => observed = current,
            }
        }
    }

    fn worker_finished(&self) {
        let _ = self
            .active
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            });
    }
}

struct V5ConstructionWorkerGuard<'a> {
    activity: &'a V5ConstructionActivity,
    progress: Option<NativeProgressHandle>,
}

impl<'a> V5ConstructionWorkerGuard<'a> {
    fn new(activity: &'a V5ConstructionActivity, progress: Option<&NativeProgressHandle>) -> Self {
        activity.worker_started();
        if let Some(progress) = progress {
            progress.worker_started();
        }
        Self {
            activity,
            progress: progress.cloned(),
        }
    }
}

impl Drop for V5ConstructionWorkerGuard<'_> {
    fn drop(&mut self) {
        if let Some(progress) = &self.progress {
            progress.worker_finished();
        }
        self.activity.worker_finished();
    }
}

fn exact_parent_reference(left: &ParentReference, right: &ParentReference) -> bool {
    left.pair_identity_sha256 == right.pair_identity_sha256
        && left.candidate_id == right.candidate_id
        && left.pair_payload == right.pair_payload
        && left.selection_audit == right.selection_audit
}

fn exact_side(left: &Side, right: &Side) -> bool {
    matches!(
        (left, right),
        (&Side::Long, &Side::Long) | (&Side::Short, &Side::Short)
    )
}

fn exact_planned_proposal(left: &PlannedProposal, right: &PlannedProposal) -> bool {
    if left.proposal_ordinal != right.proposal_ordinal {
        return false;
    }
    match (&left.intent, &right.intent) {
        (
            ProposalIntent::RichImmigrant {
                proposal_seed: left_proposal_seed,
                long_seed: left_long_seed,
                short_seed: left_short_seed,
            },
            ProposalIntent::RichImmigrant {
                proposal_seed: right_proposal_seed,
                long_seed: right_long_seed,
                short_seed: right_short_seed,
            },
        ) => {
            left_proposal_seed == right_proposal_seed
                && left_long_seed == right_long_seed
                && left_short_seed == right_short_seed
        }
        (
            ProposalIntent::StructuralMutation {
                proposal_seed: left_seed,
                parent: left_parent,
                mutation_depth: left_depth,
            },
            ProposalIntent::StructuralMutation {
                proposal_seed: right_seed,
                parent: right_parent,
                mutation_depth: right_depth,
            },
        ) => {
            left_seed == right_seed
                && left_depth == right_depth
                && exact_parent_reference(left_parent, right_parent)
        }
        (
            ProposalIntent::SameSideCrossover {
                proposal_seed: left_seed,
                side: left_side,
                parent: left_parent,
                mate: left_mate,
                mate_selection_attempts: left_attempts,
            },
            ProposalIntent::SameSideCrossover {
                proposal_seed: right_seed,
                side: right_side,
                parent: right_parent,
                mate: right_mate,
                mate_selection_attempts: right_attempts,
            },
        ) => {
            left_seed == right_seed
                && exact_side(left_side, right_side)
                && exact_parent_reference(left_parent, right_parent)
                && exact_parent_reference(left_mate, right_mate)
                && left_attempts == right_attempts
        }
        _ => false,
    }
}

/// Advance only the state read by `ProposalPlanner` under the temporary wave
/// assumption that this proposal will be accepted. No identity, ledger,
/// journal, disposition, or publication state is fabricated here.
fn assume_speculative_acceptance(
    state: &mut ProposalState,
    planned: &PlannedProposal,
) -> Result<()> {
    if state.next_proposal_ordinal != planned.proposal_ordinal {
        return Err(contract(
            "v5 evolved speculative planner ordinal diverged from planning state",
        ));
    }
    state.next_proposal_ordinal = planned
        .proposal_ordinal
        .checked_add(1)
        .ok_or_else(|| contract("v5 evolved speculative proposal ordinal overflowed"))?;
    let origin_kind = planned.intent.origin_kind().to_owned();
    let accepted = state
        .origin_accepted_counts
        .entry(origin_kind.clone())
        .or_default();
    *accepted = accepted
        .checked_add(1)
        .ok_or_else(|| contract("v5 evolved speculative accepted count overflowed"))?;
    if origin_kind == "random_immigrant" {
        state.immigrant_accepted = state
            .immigrant_accepted
            .checked_add(1)
            .ok_or_else(|| contract("v5 evolved speculative immigrant count overflowed"))?;
    }
    Ok(())
}

fn record_evolved_execution_sections(
    progress: Option<&NativeProgressHandle>,
    telemetry: &V5EvolvedExecutionTelemetry,
    replay_policy: V5ImmediateConstructionReplayPolicy,
) {
    let Some(progress) = progress else {
        return;
    };
    progress.record_section(NativeProgressSection {
        name: "evolved_plan".to_owned(),
        wall: telemetry.plan_wall,
        completed_work_units: Some(telemetry.planned_count),
        parallel_workers: Some(1),
        ..NativeProgressSection::default()
    });
    progress.record_section(NativeProgressSection {
        name: "evolved_construct".to_owned(),
        wall: telemetry.construct_wall,
        completed_work_units: Some(telemetry.constructed_count),
        parallel_workers: Some(telemetry.peak_active_workers),
        ..NativeProgressSection::default()
    });
    progress.record_section(NativeProgressSection {
        name: replay_policy.telemetry_section_name().to_owned(),
        wall: telemetry.immediate_replay_wall,
        completed_work_units: Some(telemetry.immediate_replay_count),
        parallel_workers: Some(1),
        ..NativeProgressSection::default()
    });
    progress.record_section(NativeProgressSection {
        name: "evolved_snapshot".to_owned(),
        wall: telemetry.snapshot_wall,
        completed_work_units: Some(telemetry.snapshot_count),
        parallel_workers: Some(1),
        ..NativeProgressSection::default()
    });
    progress.record_section(NativeProgressSection {
        name: "evolved_admission_ledger".to_owned(),
        wall: telemetry.admission_ledger_wall,
        completed_work_units: Some(telemetry.committed_count),
        parallel_workers: Some(1),
        ..NativeProgressSection::default()
    });
    progress.record_section(NativeProgressSection {
        name: "evolved_state_commit".to_owned(),
        wall: telemetry.state_commit_wall,
        completed_work_units: Some(telemetry.committed_count),
        parallel_workers: Some(1),
        ..NativeProgressSection::default()
    });
    progress.record_section(NativeProgressSection {
        name: "evolved_speculative_discard".to_owned(),
        wall: Duration::from_secs(0),
        completed_work_units: Some(telemetry.speculative_discarded_count),
        parallel_workers: Some(1),
        ..NativeProgressSection::default()
    });
    progress.record_section(NativeProgressSection {
        name: "evolved_waves".to_owned(),
        wall: Duration::from_secs(0),
        completed_work_units: Some(telemetry.wave_count),
        parallel_workers: Some(telemetry.peak_in_flight),
        ..NativeProgressSection::default()
    });
}

fn execute_with_constructor_and_telemetry<F>(
    request: V5EvolvedTransactionRequest,
    parents: &mut dyn ParentSelector,
    ledger: &mut dyn IdentityLedger,
    options: V5EvolvedExecutionOptions,
    progress: Option<&NativeProgressHandle>,
    construct: F,
) -> Result<(V5EvolvedTransactionResult, V5EvolvedExecutionTelemetry)>
where
    F: Fn(
            &V5SharedConstructionAuthority,
            &V5EvolvedTransactionRequest,
            &PlannedProposal,
            u64,
        ) -> Result<V5EvolvedConstructionOutcome>
        + Sync,
{
    request.validate_shape()?;
    let authority = V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)?;
    let initial_parent_selector_state = parents.compact_state();
    let initial_identity_ledger_state = ledger.compact_state();
    if canonical_sha256(&initial_parent_selector_state)? != request.parent_selector_state_sha256
        || canonical_sha256(ledger.identity())? != request.identity_ledger_identity_sha256
        || canonical_sha256(&initial_identity_ledger_state)? != request.identity_ledger_state_sha256
    {
        return Err(contract(
            "v5 evolved transaction parent archive or identity ledger input binding drifted",
        ));
    }
    let has_parents = parents.has_parents();
    let parent_schedule = match (has_parents, request.parent_schedule) {
        (true, Some(schedule)) => {
            if schedule.breeder_parent_count != parents.eligible_parent_count() as u64 {
                return Err(contract(
                    "v5 evolved parent schedule does not bind eligible parent count",
                ));
            }
            Some(schedule)
        }
        (true, None) => {
            return Err(contract(
                "v5 evolved transaction with parents requires rotating parent schedule",
            ));
        }
        (false, Some(_)) => {
            return Err(contract(
                "v5 evolved transaction without parents must not carry a parent schedule",
            ));
        }
        (false, None) => None,
    };
    let desired_immigrants = accepted_quota_immigrant_count(request.target_accepted, has_parents);
    let desired_offspring = request
        .target_accepted
        .checked_sub(desired_immigrants)
        .ok_or_else(|| contract("v5 evolved accepted quota underflowed"))?;
    let schedule = ProposalSchedule {
        config_sha256: request.generation_config_sha256.clone(),
        generation_index: request.generation_index,
        parent_schedule,
        desired_evaluated_offspring: desired_offspring,
        desired_evaluated_immigrants: desired_immigrants,
    };
    let parent_schedule_sha256 = schedule
        .parent_schedule
        .map(RotatingParentSchedule::schedule_sha256);
    let mut planner = ProposalPlanner { schedule, parents };
    let mut state = ProposalState::default();
    let mut attempts = Vec::new();
    let mut audits = Vec::new();
    let mut deltas = Vec::new();
    let mut records = Vec::new();
    let mut parent_snapshots = BTreeMap::new();
    let mut attempt_snapshot_refs = Vec::new();
    let mut crossover_attempts = 0_u64;
    let activity = V5ConstructionActivity::default();
    let mut telemetry = V5EvolvedExecutionTelemetry::default();

    while (records.len() as u64) < request.target_accepted
        && (attempts.len() as u64) < request.max_attempts
    {
        let remaining_target = request.target_accepted.saturating_sub(records.len() as u64);
        let remaining_attempts = request.max_attempts.saturating_sub(attempts.len() as u64);
        let wave_width = request
            .thread_cap
            .min(remaining_target)
            .min(remaining_attempts) as usize;
        if wave_width == 0 {
            break;
        }
        telemetry.wave_count = telemetry.wave_count.saturating_add(1);
        telemetry.peak_in_flight = telemetry.peak_in_flight.max(wave_width as u64);

        let selector_checkpoint = planner.parents.compact_state();
        let mut planning_state = state.clone();
        let base_birth_ordinal = records.len() as u64;
        let planning_started = Instant::now();
        let planning_result = (|| -> Result<Vec<(PlannedProposal, u64)>> {
            let mut wave = Vec::with_capacity(wave_width);
            for offset in 0..wave_width {
                let planned = planner.plan_next(&mut planning_state)?;
                let birth_ordinal = base_birth_ordinal
                    .checked_add(offset as u64)
                    .ok_or_else(|| contract("v5 evolved speculative birth ordinal overflowed"))?;
                assume_speculative_acceptance(&mut planning_state, &planned)?;
                wave.push((planned, birth_ordinal));
            }
            Ok(wave)
        })();
        telemetry.plan_wall += planning_started.elapsed();
        let restore_result = planner
            .parents
            .restore_compact_state(&selector_checkpoint)
            .map_err(V5EvolvedTransactionError::from);
        if let Err(error) = restore_result {
            return Err(error);
        }
        if planner.parents.compact_state() != selector_checkpoint {
            return Err(contract(
                "v5 evolved parent selector failed to restore its speculative checkpoint",
            ));
        }
        let wave = planning_result?;
        telemetry.planned_count = telemetry.planned_count.saturating_add(wave.len() as u64);

        let construction_started = Instant::now();
        let wave_results = if wave.len() == 1 {
            let (planned, birth_ordinal) = &wave[0];
            let _worker = V5ConstructionWorkerGuard::new(&activity, progress);
            let result = construct(&authority, &request, planned, *birth_ordinal);
            if result.is_ok() {
                if let Some(progress) = progress {
                    progress.advance_constructed(1);
                }
            }
            vec![result]
        } else {
            std::thread::scope(|scope| {
                let mut handles = Vec::with_capacity(wave.len());
                for (planned, birth_ordinal) in &wave {
                    let progress = progress.cloned();
                    let activity = &activity;
                    let authority = &authority;
                    let request = &request;
                    let construct = &construct;
                    handles.push(scope.spawn(move || {
                        let _worker = V5ConstructionWorkerGuard::new(activity, progress.as_ref());
                        let result = construct(authority, request, planned, *birth_ordinal);
                        if result.is_ok() {
                            if let Some(progress) = &progress {
                                progress.advance_constructed(1);
                            }
                        }
                        result
                    }));
                }
                let mut results = Vec::with_capacity(handles.len());
                for handle in handles {
                    match handle.join() {
                        Ok(result) => results.push(result),
                        Err(_) => {
                            results.push(Err(contract("v5 evolved construction worker panicked")))
                        }
                    }
                }
                results
            })
        };
        telemetry.construct_wall += construction_started.elapsed();
        telemetry.peak_active_workers = telemetry
            .peak_active_workers
            .max(activity.peak.load(Ordering::Relaxed));
        if activity.active.load(Ordering::Relaxed) != 0 {
            return Err(contract(
                "v5 evolved construction workers did not return to zero",
            ));
        }

        let mut outcomes = Vec::with_capacity(wave_results.len());
        let mut first_error = None;
        for result in wave_results {
            match result {
                Ok(outcome) => outcomes.push(outcome),
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        if outcomes.len() != wave.len() {
            return Err(contract(
                "v5 evolved construction wave did not return one outcome per plan",
            ));
        }
        telemetry.constructed_count = telemetry
            .constructed_count
            .saturating_add(outcomes.len() as u64);

        let wave_len = wave.len();
        for (index, ((speculative, birth_ordinal), outcome)) in
            wave.into_iter().zip(outcomes).enumerate()
        {
            let plan_started = Instant::now();
            let authoritative = planner.plan_next(&mut state)?;
            telemetry.plan_wall += plan_started.elapsed();
            telemetry.planned_count = telemetry.planned_count.saturating_add(1);
            if !exact_planned_proposal(&speculative, &authoritative) {
                return Err(contract(
                    "v5 evolved speculative plan diverged from authoritative serial re-plan",
                ));
            }
            if matches!(
                &authoritative.intent,
                ProposalIntent::SameSideCrossover { .. }
            ) {
                crossover_attempts = crossover_attempts
                    .checked_add(1)
                    .ok_or_else(|| contract("v5 evolved crossover attempt counter overflowed"))?;
            }

            if options.immediate_replay_policy
                == V5ImmediateConstructionReplayPolicy::IndependentReplayV1
            {
                let replay_started = Instant::now();
                verify_native_construction_replay(
                    &authority,
                    &request,
                    &authoritative,
                    birth_ordinal,
                    &outcome,
                )?;
                telemetry.immediate_replay_wall += replay_started.elapsed();
                telemetry.immediate_replay_count =
                    telemetry.immediate_replay_count.saturating_add(1);
            }
            if options.capture_parent_snapshots {
                let snapshot_started = Instant::now();
                attempt_snapshot_refs.push(snapshot_refs_for_native_outcome(
                    &request,
                    &authoritative,
                    &outcome,
                    &mut parent_snapshots,
                )?);
                telemetry.snapshot_wall += snapshot_started.elapsed();
                telemetry.snapshot_count = telemetry.snapshot_count.saturating_add(1);
            }
            let delta = outcome.delta.clone().ok_or_else(|| {
                contract("v5 evolved constructor omitted its required all-attempt compact delta")
            })?;

            let admission_started = Instant::now();
            let (attempt, audit, accepted) = admitted_attempt(
                &request,
                &authority,
                &state,
                ledger,
                &authoritative,
                outcome,
            )?;
            telemetry.admission_ledger_wall += admission_started.elapsed();

            let accepted_proposal = accepted.as_ref().map(|record| AcceptedProposal {
                candidate_id: record.candidate_id.clone(),
                candidate_identity_sha256: record.candidate_identity_sha256.clone(),
                executable_semantic_sha256: record.executable_semantic_sha256.clone(),
                descriptor_projection: Some(record.descriptor_projection.clone()),
            });
            let commit_started = Instant::now();
            state.observe_compact_attempt(
                attempt.proposal_ordinal,
                &attempt.origin_kind,
                &attempt.disposition,
                &attempt.attempt_sha256()?,
                accepted_proposal.as_ref(),
            )?;
            deltas.push(delta);
            let accepted_now = accepted.is_some();
            if let Some(record) = accepted {
                records.push(record);
            }
            audits.push(audit);
            attempts.push(attempt);
            telemetry.state_commit_wall += commit_started.elapsed();
            telemetry.committed_count = telemetry.committed_count.saturating_add(1);

            if let Some(progress) = progress {
                progress.advance_attempted(1);
                if accepted_now {
                    progress.advance_accepted(1);
                    progress.set_completed_work_units(records.len() as u64);
                } else {
                    progress.advance_rejected(1);
                }
            }
            if !accepted_now {
                telemetry.speculative_discarded_count = telemetry
                    .speculative_discarded_count
                    .saturating_add((wave_len - index - 1) as u64);
                break;
            }
        }
    }

    // Parent selection occurs inside `ProposalPlanner`; extract the mutable
    // selector after the loop so its exact final state is sealed in the result.
    let final_parent_selector_state = planner.parents.compact_state();
    let final_identity_ledger_state = ledger.compact_state();
    let target_reached = records.len() as u64 == request.target_accepted;
    let attempt_journal = V5AttemptJournal {
        generation_index: request.generation_index,
        generation_config_sha256: request.generation_config_sha256.clone(),
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        attempts: attempts.clone(),
    };
    let delta_journal = V5EvolvedDeltaJournal {
        generation_index: request.generation_index,
        generation_config_sha256: request.generation_config_sha256.clone(),
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        deltas: deltas.clone(),
    };
    let compact_accepted_journal = V5EvolvedCompactAcceptedJournal {
        generation_index: request.generation_index,
        generation_config_sha256: request.generation_config_sha256.clone(),
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        ordered_record_sha256s: records
            .iter()
            .map(|record| {
                record
                    .record_sha256()
                    .map_err(V5EvolvedTransactionError::from)
            })
            .collect::<Result<Vec<_>>>()?,
    };
    let schedule_state_receipt = V5EvolvedScheduleStateReceipt {
        generation_index: request.generation_index,
        generation_config_sha256: request.generation_config_sha256.clone(),
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        target_accepted: request.target_accepted,
        max_attempts: request.max_attempts,
        parent_schedule_sha256,
        accepted_by_origin: state.origin_accepted_counts.clone(),
        disposition_counts: state.disposition_counts.clone(),
        next_proposal_ordinal: state.next_proposal_ordinal,
        structural_parent_selections: state.structural_parent_selections,
        crossover_attempts,
        proposal_state: state.compact_value(),
        initial_parent_selector_state,
        final_parent_selector_state,
        identity_ledger_identity: ledger.identity().clone(),
        initial_identity_ledger_state,
        final_identity_ledger_state,
    };
    let parent_snapshot_inventory =
        options
            .capture_parent_snapshots
            .then_some(V5EvolvedParentSnapshotInventory {
                generation_index: request.generation_index,
                generation_config_sha256: request.generation_config_sha256.clone(),
                shared_authority_sha256: authority.shared_authority_sha256.clone(),
                source_parent_archive_input_binding_sha256: request
                    .parent_archive_input_binding_sha256
                    .clone(),
                source_parent_archive_semantic_sha256: request.parent_selector_state_sha256.clone(),
                snapshots: parent_snapshots.into_values().collect(),
                attempt_snapshot_refs,
            });
    let result = V5EvolvedTransactionResult {
        generation_index: request.generation_index,
        generation_config_sha256: request.generation_config_sha256,
        shared_authority_sha256: authority.shared_authority_sha256,
        parent_archive_input_binding_sha256: request.parent_archive_input_binding_sha256,
        identity_ledger_input_binding_sha256: request.identity_ledger_input_binding_sha256,
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
        proposal_deltas: deltas,
        delta_journal,
        accepted_records: records,
        compact_accepted_journal,
        schedule_state_receipt,
        parent_snapshot_inventory,
    };
    result.verify_replay()?;
    record_evolved_execution_sections(progress, &telemetry, options.immediate_replay_policy);
    Ok((result, telemetry))
}

fn execute_with_constructor<F>(
    request: V5EvolvedTransactionRequest,
    parents: &mut dyn ParentSelector,
    ledger: &mut dyn IdentityLedger,
    options: V5EvolvedExecutionOptions,
    progress: Option<&NativeProgressHandle>,
    construct: F,
) -> Result<V5EvolvedTransactionResult>
where
    F: Fn(
            &V5SharedConstructionAuthority,
            &V5EvolvedTransactionRequest,
            &PlannedProposal,
            u64,
        ) -> Result<V5EvolvedConstructionOutcome>
        + Sync,
{
    execute_with_constructor_and_telemetry(request, parents, ledger, options, progress, construct)
        .map(|(result, _)| result)
}

fn record_admission_telemetry(
    progress: Option<&NativeProgressHandle>,
    telemetry: Option<&V5EvolvedAdmissionTelemetry>,
) {
    let (Some(progress), Some(telemetry)) = (progress, telemetry) else {
        return;
    };
    let probe_count = telemetry.changed_side_probes.load(Ordering::Relaxed);
    progress.record_section(NativeProgressSection {
        name: "evolved_changed_side_admission".to_owned(),
        wall: Duration::from_nanos(telemetry.changed_side_probe_nanos.load(Ordering::Relaxed)),
        completed_work_units: Some(probe_count),
        parallel_workers: None,
        ..NativeProgressSection::default()
    });
    progress.emit_counters("evolved_admission", &telemetry.counters());
}

/// Execute the write-neutral native later-generation v5 transaction.
///
/// The only construction implementation bound here is the sealed Rust
/// compiler/operator engine above. Parent and ledger trait objects supply
/// already-opened in-memory state, but cannot inject a candidate builder.
pub fn execute_v5_evolved_transaction(
    request: V5EvolvedTransactionRequest,
    parents: &mut dyn ParentSelector,
    ledger: &mut dyn IdentityLedger,
) -> Result<V5EvolvedTransactionResult> {
    execute_v5_evolved_transaction_with_progress(request, parents, ledger, None)
}

/// Execute an evolved transaction with optional operational progress. The
/// durable/default path keeps the historical independent constructor replay.
pub fn execute_v5_evolved_transaction_with_progress(
    request: V5EvolvedTransactionRequest,
    parents: &mut dyn ParentSelector,
    ledger: &mut dyn IdentityLedger,
    progress: Option<&NativeProgressHandle>,
) -> Result<V5EvolvedTransactionResult> {
    let mut engine = NativeV5EvolvedConstructionEngine::durable();
    if progress.is_some_and(NativeProgressHandle::is_enabled) {
        engine.enable_admission_telemetry();
    }
    let result = execute_with_constructor(
        request,
        parents,
        ledger,
        V5EvolvedExecutionOptions::durable(),
        progress,
        |authority, request, planned, birth_ordinal| {
            engine.construct(authority, request, planned, birth_ordinal)
        },
    )?;
    record_admission_telemetry(progress, engine.admission_telemetry());
    Ok(result)
}

/// Fast-ephemeral-only evolved construction. It preserves the exact serial
/// scheduler, duplicate, ledger, and transaction semantics while deferring the
/// expensive independent constructor replay to the mandatory publication
/// replay. The caller must not publish a completion marker until that replay
/// has succeeded.
pub fn execute_v5_evolved_transaction_fast_ephemeral_with_progress(
    request: V5EvolvedTransactionRequest,
    parents: &mut dyn ParentSelector,
    ledger: &mut dyn IdentityLedger,
    progress: Option<&NativeProgressHandle>,
) -> Result<V5EvolvedTransactionResult> {
    let mut engine = NativeV5EvolvedConstructionEngine::fast_ephemeral();
    if progress.is_some_and(NativeProgressHandle::is_enabled) {
        engine.enable_admission_telemetry();
    }
    let result = execute_with_constructor(
        request,
        parents,
        ledger,
        V5EvolvedExecutionOptions::fast_ephemeral(),
        progress,
        |authority, request, planned, birth_ordinal| {
            engine.construct(authority, request, planned, birth_ordinal)
        },
    )?;
    if let (Some(progress), Some((hits, misses, verification_wall))) =
        (progress, engine.parent_cache_telemetry())
    {
        progress.record_section(NativeProgressSection {
            name: "evolved_parent_cache_verification".to_owned(),
            wall: verification_wall,
            completed_work_units: Some(misses),
            parallel_workers: Some(1),
            ..NativeProgressSection::default()
        });
        progress.record_section(NativeProgressSection {
            name: "evolved_parent_cache_hits".to_owned(),
            wall: Duration::ZERO,
            completed_work_units: Some(hits),
            parallel_workers: Some(1),
            ..NativeProgressSection::default()
        });
    }
    record_admission_telemetry(progress, engine.admission_telemetry());
    Ok(result)
}

#[cfg(test)]
fn execute_with_engine_and_telemetry(
    request: V5EvolvedTransactionRequest,
    parents: &mut dyn ParentSelector,
    ledger: &mut dyn IdentityLedger,
    engine: &mut dyn V5EvolvedConstructionEngine,
) -> Result<(V5EvolvedTransactionResult, V5EvolvedExecutionTelemetry)> {
    let engine = std::sync::Mutex::new(engine);
    execute_with_constructor_and_telemetry(
        request,
        parents,
        ledger,
        V5EvolvedExecutionOptions::scheduler_test(),
        None,
        |authority, request, planned, birth_ordinal| {
            let mut engine = engine
                .lock()
                .map_err(|_| contract("v5 evolved test construction engine lock poisoned"))?;
            engine.construct(authority, request, planned, birth_ordinal)
        },
    )
}

#[cfg(test)]
fn execute_with_engine(
    request: V5EvolvedTransactionRequest,
    parents: &mut dyn ParentSelector,
    ledger: &mut dyn IdentityLedger,
    engine: &mut dyn V5EvolvedConstructionEngine,
) -> Result<V5EvolvedTransactionResult> {
    execute_with_engine_and_telemetry(request, parents, ledger, engine).map(|(result, _)| result)
}

#[cfg(test)]
mod scheduler_tests {
    use std::{
        io::Read,
        sync::{
            Arc, Barrier,
            atomic::{AtomicU64 as TestAtomicU64, Ordering as TestOrdering},
        },
    };

    use flate2::read::GzDecoder;

    use super::*;
    use crate::{
        factory::ParentReference,
        proposal::{CandidateIdentityLedger, ExplicitParentRing, ParentSelector},
        v5::{
            build_v5_g0_accepted_material, parent_reference_from_v5_compact_record,
            v5_proposal_seed,
        },
    };

    fn sha(value: Value) -> String {
        canonical_sha256(&value).expect("canonical test SHA-256")
    }

    #[test]
    fn admission_telemetry_is_absent_until_enabled_and_shards_merge_once() {
        let mut engine = NativeV5EvolvedConstructionEngine::fast_ephemeral();
        assert!(engine.admission_telemetry().is_none());
        engine.enable_admission_telemetry();
        let aggregate = engine
            .admission_telemetry()
            .expect("explicitly enabled admission telemetry");
        {
            let shard = V5EvolvedAdmissionSample::new(aggregate);
            V5EvolvedAdmissionSample::increment(&shard.vocabulary_enumerations);
            V5EvolvedAdmissionSample::increment(&shard.changed_side_probes);
            V5EvolvedAdmissionSample::increment(&shard.selected_rebuilds);
            shard.add_probe_wall(Duration::from_nanos(17));
            shard.observe_rejection("v5 evolved program admission failed: fixture rejection");
        }
        let counters = aggregate.counters();
        assert_eq!(counters.get("vocabularyEnumerations"), Some(&1));
        assert_eq!(counters.get("changedSideProbes"), Some(&1));
        assert_eq!(counters.get("selectedRebuilds"), Some(&1));
        assert_eq!(counters.get("rejection.other"), Some(&1));
        assert_eq!(
            aggregate.changed_side_probe_nanos.load(Ordering::Relaxed),
            17
        );
    }

    fn shared_authority_fixture() -> Value {
        let compressed = include_bytes!(
            "../../../../../tests/fixtures/temporal_qd_v5_shared_authority_oracle.json.gz"
        );
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut payload = Vec::new();
        decoder
            .read_to_end(&mut payload)
            .expect("decompress sealed shared authority");
        let fixture: Value = serde_json::from_slice(&payload).expect("parse sealed authority");
        fixture
            .get("sealedAuthority")
            .cloned()
            .expect("fixture sealed authority")
    }

    fn parent(pair_label: &str) -> ParentReference {
        ParentReference {
            pair_identity_sha256: sha(object([("pair", Value::String(pair_label.to_owned()))])),
            candidate_id: format!("candidate_{pair_label}"),
            pair_payload: object([("opaqueParent", Value::String(pair_label.to_owned()))]),
            selection_audit: Some(object([(
                "selectedParent",
                Value::String(pair_label.to_owned()),
            )])),
        }
    }

    #[derive(Clone)]
    struct TestParentSelector {
        parents: Vec<ParentReference>,
        draws: u64,
    }

    impl TestParentSelector {
        fn new() -> Self {
            Self {
                parents: vec![parent("a"), parent("b")],
                draws: 0,
            }
        }
    }

    impl ParentSelector for TestParentSelector {
        fn has_parents(&self) -> bool {
            true
        }

        fn eligible_parent_count(&self) -> usize {
            self.parents.len()
        }

        fn archive_cell_count(&self) -> usize {
            self.parents.len()
        }

        fn compact_state(&self) -> Value {
            object([
                (
                    "schemaVersion",
                    Value::String(
                        "temporal_qd_v5_evolved_scheduler_test_parent_state_v1".to_owned(),
                    ),
                ),
                ("draws", Value::from(self.draws)),
                (
                    "pairIdentitySha256s",
                    Value::Array(
                        self.parents
                            .iter()
                            .map(|parent| Value::String(parent.pair_identity_sha256.clone()))
                            .collect(),
                    ),
                ),
            ])
        }

        fn restore_compact_state(
            &mut self,
            state: &Value,
        ) -> std::result::Result<(), ProposalError> {
            let draws = state.get("draws").and_then(Value::as_u64).ok_or_else(|| {
                ProposalError::Contract(
                    "v5 evolved scheduler test parent checkpoint lacks draws".to_owned(),
                )
            })?;
            let previous = self.draws;
            self.draws = draws;
            if state != &self.compact_state() {
                self.draws = previous;
                return Err(ProposalError::Contract(
                    "v5 evolved scheduler test parent state drifted".to_owned(),
                ));
            }
            Ok(())
        }

        fn select(
            &mut self,
            _label: &str,
            structural_selection_ordinal: u64,
        ) -> std::result::Result<ParentReference, ProposalError> {
            let index = (structural_selection_ordinal % self.parents.len() as u64) as usize;
            self.draws += 1;
            Ok(self.parents[index].clone())
        }
    }

    fn attempt_parent_reference(parent: &ParentReference) -> V5AttemptParentReference {
        V5AttemptParentReference {
            candidate_id: parent.candidate_id.clone(),
            candidate_identity_sha256: sha(object([(
                "candidate",
                Value::String(parent.candidate_id.clone()),
            )])),
            accepted_record_sha256: sha(object([(
                "record",
                Value::String(parent.pair_identity_sha256.clone()),
            )])),
            long_program_sha256: sha(object([(
                "long",
                Value::String(parent.pair_identity_sha256.clone()),
            )])),
            short_program_sha256: sha(object([(
                "short",
                Value::String(parent.pair_identity_sha256.clone()),
            )])),
        }
    }

    fn selection_receipt(
        scheduled_kind: &str,
        parent: &ParentReference,
        mate: Option<&ParentReference>,
    ) -> V5EvolvedParentSelectionReceipt {
        V5EvolvedParentSelectionReceipt {
            scheduled_kind: scheduled_kind.to_owned(),
            parent: attempt_parent_reference(parent),
            mate: mate.map(attempt_parent_reference),
            parent_selection_audit: parent.selection_audit.clone().unwrap_or(Value::Null),
            mate_selection_audit: mate.and_then(|mate| mate.selection_audit.clone()),
            mate_selection_attempts: Vec::new(),
        }
    }

    fn rejected_delta(
        authority: &V5SharedConstructionAuthority,
        request: &V5EvolvedTransactionRequest,
        planned: &PlannedProposal,
        parent: Option<V5AttemptParentReference>,
        mate: Option<V5AttemptParentReference>,
        parent_selection_receipt: Option<Value>,
        mutation_depth: Option<u8>,
    ) -> V5EvolvedProposalDelta {
        let terminal_plan = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_scheduler_test_rejection_plan_v1".to_owned()),
            ),
            ("reasonCode", Value::String("operation_rejected".to_owned())),
        ]);
        let terminal_trace = object([
            (
                "schemaVersion",
                Value::String(
                    "temporal_qd_v5_evolved_scheduler_test_rejection_trace_v1".to_owned(),
                ),
            ),
            ("reasonCode", Value::String("operation_rejected".to_owned())),
        ]);
        let long_program = object([(
            "schemaVersion",
            Value::String("temporal_qd_v5_evolved_scheduler_test_long_program_v1".to_owned()),
        )]);
        let short_program = object([(
            "schemaVersion",
            Value::String("temporal_qd_v5_evolved_scheduler_test_short_program_v1".to_owned()),
        )]);
        let is_immigrant = matches!(&planned.intent, ProposalIntent::RichImmigrant { .. });
        V5EvolvedProposalDelta {
            generation_index: request.generation_index,
            proposal_ordinal: planned.proposal_ordinal,
            generation_config_sha256: request.generation_config_sha256.clone(),
            shared_authority_sha256: authority.shared_authority_sha256.clone(),
            proposal_seed: planned.intent.proposal_seed().to_owned(),
            origin_kind: planned.intent.origin_kind().to_owned(),
            scheduled_kind: planned_scheduled_kind(&planned.intent).to_owned(),
            parent,
            mate,
            parent_selection_receipt,
            mutation_depth,
            long_program_sha256: sha(long_program.clone()),
            long_program,
            short_program_sha256: sha(short_program.clone()),
            short_program,
            steps: Vec::new(),
            terminal_operator_plan: (!is_immigrant).then_some(terminal_plan),
            terminal_operator_application: None,
            terminal_operator_trace: (!is_immigrant).then_some(terminal_trace),
            terminal_disposition: "rejected".to_owned(),
            terminal_reason_code: "operation_rejected".to_owned(),
        }
    }

    fn rejecting_outcome(
        authority: &V5SharedConstructionAuthority,
        request: &V5EvolvedTransactionRequest,
        planned: &PlannedProposal,
    ) -> Result<V5EvolvedConstructionOutcome> {
        let (lineage_refs, delta, stage) = match &planned.intent {
            ProposalIntent::RichImmigrant { .. } => {
                let delta = rejected_delta(authority, request, planned, None, None, None, None);
                (
                    V5AttemptLineageRefs {
                        parent: None,
                        mate: None,
                        parent_selection_receipt_sha256: None,
                        operator_plan_sha256: None,
                        operator_application_sha256: None,
                        operator_trace_sha256: None,
                        step_index: None,
                    },
                    delta,
                    "pre_plan",
                )
            }
            ProposalIntent::StructuralMutation {
                parent,
                mutation_depth,
                ..
            } => {
                let receipt = selection_receipt("structural_offspring", parent, None);
                let receipt_value = receipt.to_value()?;
                let parent_reference = attempt_parent_reference(parent);
                let delta = rejected_delta(
                    authority,
                    request,
                    planned,
                    Some(parent_reference.clone()),
                    None,
                    Some(receipt_value),
                    Some(*mutation_depth),
                );
                (
                    V5AttemptLineageRefs {
                        parent: Some(parent_reference),
                        mate: None,
                        parent_selection_receipt_sha256: Some(receipt.receipt_sha256()?),
                        operator_plan_sha256: Some(canonical_sha256(
                            delta
                                .terminal_operator_plan
                                .as_ref()
                                .expect("test terminal plan"),
                        )?),
                        operator_application_sha256: None,
                        operator_trace_sha256: Some(canonical_sha256(
                            delta
                                .terminal_operator_trace
                                .as_ref()
                                .expect("test terminal trace"),
                        )?),
                        step_index: Some(0),
                    },
                    delta,
                    "operator_plan",
                )
            }
            ProposalIntent::SameSideCrossover { parent, mate, .. } => {
                let receipt = selection_receipt("same_side_crossover", parent, Some(mate));
                let receipt_value = receipt.to_value()?;
                let parent_reference = attempt_parent_reference(parent);
                let mate_reference = attempt_parent_reference(mate);
                let delta = rejected_delta(
                    authority,
                    request,
                    planned,
                    Some(parent_reference.clone()),
                    Some(mate_reference.clone()),
                    Some(receipt_value),
                    None,
                );
                (
                    V5AttemptLineageRefs {
                        parent: Some(parent_reference),
                        mate: Some(mate_reference),
                        parent_selection_receipt_sha256: Some(receipt.receipt_sha256()?),
                        operator_plan_sha256: Some(canonical_sha256(
                            delta
                                .terminal_operator_plan
                                .as_ref()
                                .expect("test terminal plan"),
                        )?),
                        operator_application_sha256: None,
                        operator_trace_sha256: Some(canonical_sha256(
                            delta
                                .terminal_operator_trace
                                .as_ref()
                                .expect("test terminal trace"),
                        )?),
                        step_index: Some(0),
                    },
                    delta,
                    "operator_plan",
                )
            }
        };
        Ok(V5EvolvedConstructionOutcome {
            delta: Some(delta),
            lineage_refs,
            disposition: "rejected".to_owned(),
            reason_code: "operation_rejected".to_owned(),
            stage: stage.to_owned(),
            accepted: None,
        })
    }

    #[derive(Default)]
    struct RejectingEngine {
        scheduled_kinds: Vec<String>,
        mutation_depths: Vec<u8>,
    }

    impl V5EvolvedConstructionEngine for RejectingEngine {
        fn construct(
            &mut self,
            authority: &V5SharedConstructionAuthority,
            request: &V5EvolvedTransactionRequest,
            planned: &PlannedProposal,
            _birth_ordinal: u64,
        ) -> Result<V5EvolvedConstructionOutcome> {
            self.scheduled_kinds
                .push(planned_scheduled_kind(&planned.intent).to_owned());
            if let ProposalIntent::StructuralMutation { mutation_depth, .. } = &planned.intent {
                self.mutation_depths.push(*mutation_depth);
            }
            rejecting_outcome(authority, request, planned)
        }
    }

    fn request(
        thread_cap: u64,
        selector: &TestParentSelector,
        ledger: &CandidateIdentityLedger,
    ) -> V5EvolvedTransactionRequest {
        V5EvolvedTransactionRequest {
            shared_authority: shared_authority_fixture(),
            generation_config_sha256: sha(object([(
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_scheduler_test_config_v1".to_owned()),
            )])),
            parent_archive_input_binding_sha256: sha(object([(
                "schemaVersion",
                Value::String(
                    "temporal_qd_v5_evolved_scheduler_test_archive_binding_v1".to_owned(),
                ),
            )])),
            identity_ledger_input_binding_sha256: sha(object([(
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_scheduler_test_ledger_binding_v1".to_owned()),
            )])),
            generation_index: 2,
            target_accepted: 2,
            max_attempts: 7,
            evaluation_width: 1,
            thread_cap,
            parent_schedule: Some(
                RotatingParentSchedule::from_counts(2, 2).expect("test parent schedule"),
            ),
            parent_selector_state_sha256: sha(selector.compact_state()),
            identity_ledger_identity_sha256: sha(ledger.identity().clone()),
            identity_ledger_state_sha256: sha(ledger.compact_state()),
        }
    }

    fn ledger() -> CandidateIdentityLedger {
        CandidateIdentityLedger::new(
            object([(
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_scheduler_test_ledger_v1".to_owned()),
            )]),
            Vec::<String>::new(),
        )
        .expect("test identity ledger")
    }

    fn sealed_authority() -> V5SharedConstructionAuthority {
        V5SharedConstructionAuthority::from_shared_object(&shared_authority_fixture())
            .expect("parse sealed shared authority")
    }

    fn sealed_g0_parent(
        authority: &V5SharedConstructionAuthority,
        config_sha256: &str,
        proposal_ordinal: u64,
    ) -> ParentReference {
        let proposal_seed = v5_proposal_seed(config_sha256, proposal_ordinal)
            .expect("derive deterministic G0 proposal seed");
        let material = build_v5_g0_accepted_material(
            authority,
            1,
            proposal_ordinal,
            proposal_ordinal,
            &proposal_seed,
        )
        .expect("construct authenticated G0 compact parent");
        parent_reference_from_v5_compact_record(
            authority,
            &material.proposal_delta,
            &material.record,
        )
        .expect("bind authenticated G0 compact parent")
    }

    #[test]
    fn aggregate_structural_reversion_is_journaled_as_no_op() {
        let config_sha256 = sha(object([(
            "schemaVersion",
            Value::String("temporal_qd_v5_evolved_aggregate_no_op_probe_v1".to_owned()),
        )]));
        let authority = sealed_authority();
        let parent_reference = sealed_g0_parent(&authority, &config_sha256, 0);
        let parent = load_v5_evolved_parent(&authority, &parent_reference)
            .expect("load authenticated aggregate-no-op parent");
        let request = direct_native_request(config_sha256.clone());
        let planned = PlannedProposal {
            proposal_ordinal: 0,
            intent: ProposalIntent::StructuralMutation {
                proposal_seed: v5_proposal_seed(&config_sha256, 0)
                    .expect("derive aggregate-no-op proposal seed"),
                parent: parent_reference,
                mutation_depth: 2,
            },
        };
        let receipt = parent_selection_receipt_for_planned(&planned, &parent, None)
            .expect("bind aggregate-no-op parent receipt");
        let accepted_step =
            construct_sealed_mutation(&authority, &request, &planned, 0, None, None)
                .expect("construct evidence-bearing mutation probe");
        let accepted_delta = accepted_step
            .delta
            .expect("mutation probe must retain its delta");
        let terminal_plan = accepted_delta
            .terminal_operator_plan
            .expect("mutation probe must retain terminal plan");
        let terminal_application = accepted_delta
            .terminal_operator_application
            .expect("mutation probe must retain terminal application");
        let terminal_trace = accepted_delta
            .terminal_operator_trace
            .expect("mutation probe must retain terminal trace");

        let outcome = construct_sealed_structural_accept(
            &authority,
            &request,
            &planned,
            0,
            V5EvolvedBuildKind::Mutation,
            &parent,
            None,
            parent.clone(),
            None,
            &receipt,
            Some(2),
            parent.long_program.clone(),
            parent.short_program.clone(),
            accepted_delta.steps,
            terminal_plan,
            terminal_application,
            terminal_trace,
            1,
        )
        .expect("aggregate structural reversion must not abort construction");

        assert_eq!(outcome.disposition, "no_op");
        assert_eq!(outcome.reason_code, "structural_aggregate_no_op");
        assert_eq!(outcome.stage, "operator_apply");
        assert!(outcome.accepted.is_none());
        let delta = outcome
            .delta
            .expect("aggregate no-op must retain exact evidence");
        assert_eq!(delta.terminal_disposition, "no_op");
        assert_eq!(delta.terminal_reason_code, "structural_aggregate_no_op");
        assert_eq!(delta.long_program, parent.long_program);
        assert_eq!(delta.short_program, parent.short_program);
    }

    fn direct_native_request(config_sha256: String) -> V5EvolvedTransactionRequest {
        V5EvolvedTransactionRequest {
            shared_authority: shared_authority_fixture(),
            generation_config_sha256: config_sha256,
            parent_archive_input_binding_sha256: sha(object([(
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_direct_native_archive_binding_v1".to_owned()),
            )])),
            identity_ledger_input_binding_sha256: sha(object([(
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_direct_native_ledger_binding_v1".to_owned()),
            )])),
            generation_index: 2,
            target_accepted: 1,
            max_attempts: 1,
            evaluation_width: 1,
            thread_cap: 1,
            parent_schedule: None,
            parent_selector_state_sha256: sha(object([(
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_direct_native_parent_state_v1".to_owned()),
            )])),
            identity_ledger_identity_sha256: sha(object([(
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_direct_native_ledger_identity_v1".to_owned()),
            )])),
            identity_ledger_state_sha256: sha(object([(
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_direct_native_ledger_state_v1".to_owned()),
            )])),
        }
    }

    /// Build a minimal accepted structural segment without retaining a live
    /// parent selector.  The only parent object is consumed into the snapshot
    /// inventory, which lets the test model a source archive that disappears
    /// immediately after online construction.
    fn sealed_native_mutation_result() -> (V5EvolvedTransactionRequest, V5EvolvedTransactionResult)
    {
        let config_sha256 = sha(object([(
            "schemaVersion",
            Value::String("temporal_qd_v5_evolved_native_mutation_probe_v1".to_owned()),
        )]));
        let authority = sealed_authority();
        let parent = sealed_g0_parent(&authority, &config_sha256, 0);
        let initial_parent_selector_state = object([(
            "schemaVersion",
            Value::String("temporal_qd_v5_evolved_snapshot_only_parent_state_v1".to_owned()),
        )]);
        let mut ledger = CandidateIdentityLedger::new(
            object([(
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_snapshot_only_ledger_v1".to_owned()),
            )]),
            Vec::<String>::new(),
        )
        .expect("construct snapshot-only identity ledger");
        let initial_identity_ledger_state = ledger.compact_state();
        let mut request = direct_native_request(config_sha256.clone());
        request.parent_selector_state_sha256 = sha(initial_parent_selector_state.clone());
        request.identity_ledger_identity_sha256 = sha(ledger.identity().clone());
        request.identity_ledger_state_sha256 = sha(initial_identity_ledger_state.clone());
        let proposal_seed =
            v5_proposal_seed(&config_sha256, 0).expect("derive deterministic mutation seed");
        let planned = PlannedProposal {
            proposal_ordinal: 0,
            intent: ProposalIntent::StructuralMutation {
                proposal_seed,
                parent,
                mutation_depth: 1,
            },
        };
        let scheduled_parent = match &planned.intent {
            ProposalIntent::StructuralMutation { parent, .. } => parent,
            _ => unreachable!("constructed mutation intent"),
        };
        let parent_material = load_v5_evolved_parent(&authority, scheduled_parent)
            .expect("load differential admission parent");
        let side = proposal_side_for_seed(planned.intent.proposal_seed())
            .expect("derive differential admission side");
        let operator_authority = authority
            .operator_authority_projection()
            .expect("project differential admission authority")
            .operator_authority(side)
            .expect("select differential admission side authority");
        let recompiler = V5SealedEvolvedPairRecompiler::from_verified_parent(
            &authority,
            &parent_material,
            planned.intent.proposal_seed(),
        )
        .expect("start differential admission recompiler");
        let state = recompiler
            .state_for_side(side)
            .expect("load differential admission state");
        let probed = crate::v5_operators::enumerate_evolved_operator_choices_with_admission(
            &state.program,
            &operator_authority,
            &state.compiled_profile,
            &recompiler,
        )
        .expect("enumerate changed-side admission vocabulary");
        let full = crate::v5_operators::enumerate_evolved_operator_choices_with_admission(
            &state.program,
            &operator_authority,
            &state.compiled_profile,
            &V5FullPairChildAdmission {
                inner: &recompiler,
                telemetry: None,
            },
        )
        .expect("enumerate full-pair reference vocabulary");
        assert_eq!(
            probed, full,
            "changed-side probe must preserve the exact full-pair admitted vocabulary",
        );
        let durable_outcome =
            construct_sealed_mutation(&authority, &request, &planned, 0, None, None)
                .expect("sealed durable native mutation path");
        let cache = V5VerifiedParentCache::default();
        let outcome =
            construct_sealed_mutation(&authority, &request, &planned, 0, Some(&cache), None)
                .expect("sealed fast-ephemeral native mutation path");
        assert_eq!(outcome.disposition, durable_outcome.disposition);
        assert_eq!(outcome.reason_code, durable_outcome.reason_code);
        assert_eq!(outcome.stage, durable_outcome.stage);
        assert_eq!(outcome.lineage_refs, durable_outcome.lineage_refs);
        assert_eq!(
            outcome
                .delta
                .as_ref()
                .expect("fast mutation delta")
                .to_value()
                .expect("encode fast mutation delta"),
            durable_outcome
                .delta
                .as_ref()
                .expect("durable mutation delta")
                .to_value()
                .expect("encode durable mutation delta"),
            "deferred fast admission must preserve the exact durable mutation delta",
        );
        assert_eq!(
            outcome
                .accepted
                .as_ref()
                .expect("fast accepted mutation")
                .record
                .to_value()
                .expect("encode fast accepted mutation"),
            durable_outcome
                .accepted
                .as_ref()
                .expect("durable accepted mutation")
                .record
                .to_value()
                .expect("encode durable accepted mutation"),
            "deferred fast admission must preserve the exact durable accepted record",
        );
        assert_eq!(outcome.disposition, "accepted");
        let delta = outcome
            .delta
            .clone()
            .expect("all native attempts retain a compact delta");
        let mut snapshots = BTreeMap::new();
        let snapshot_refs =
            snapshot_refs_for_native_outcome(&request, &planned, &outcome, &mut snapshots)
                .expect("seal authenticated parent snapshot");
        // `ProposalPlanner::select` normally increments this before native
        // construction.  This focused helper supplies its preselected sealed
        // parent directly, so reproduce that one scheduler fact explicitly.
        let mut state = ProposalState {
            structural_parent_selections: 1,
            ..ProposalState::default()
        };
        let (attempt, audit, accepted) =
            admitted_attempt(&request, &authority, &state, &mut ledger, &planned, outcome)
                .expect("admit sealed native mutation");
        let record = accepted.expect("sealed mutation must materialize one compact record");
        let accepted_proposal = AcceptedProposal {
            candidate_id: record.candidate_id.clone(),
            candidate_identity_sha256: record.candidate_identity_sha256.clone(),
            executable_semantic_sha256: record.executable_semantic_sha256.clone(),
            descriptor_projection: Some(record.descriptor_projection.clone()),
        };
        state
            .observe_compact_attempt(
                attempt.proposal_ordinal,
                &attempt.origin_kind,
                &attempt.disposition,
                &attempt.attempt_sha256().expect("attempt identity"),
                Some(&accepted_proposal),
            )
            .expect("advance compact proposal state");
        let attempt_journal = V5AttemptJournal {
            generation_index: request.generation_index,
            generation_config_sha256: request.generation_config_sha256.clone(),
            shared_authority_sha256: authority.shared_authority_sha256.clone(),
            attempts: vec![attempt.clone()],
        };
        let delta_journal = V5EvolvedDeltaJournal {
            generation_index: request.generation_index,
            generation_config_sha256: request.generation_config_sha256.clone(),
            shared_authority_sha256: authority.shared_authority_sha256.clone(),
            deltas: vec![delta],
        };
        let compact_accepted_journal = V5EvolvedCompactAcceptedJournal {
            generation_index: request.generation_index,
            generation_config_sha256: request.generation_config_sha256.clone(),
            shared_authority_sha256: authority.shared_authority_sha256.clone(),
            ordered_record_sha256s: vec![record.record_sha256().expect("record identity")],
        };
        let result = V5EvolvedTransactionResult {
            generation_index: request.generation_index,
            generation_config_sha256: request.generation_config_sha256.clone(),
            shared_authority_sha256: authority.shared_authority_sha256.clone(),
            parent_archive_input_binding_sha256: request
                .parent_archive_input_binding_sha256
                .clone(),
            identity_ledger_input_binding_sha256: request
                .identity_ledger_input_binding_sha256
                .clone(),
            target_accepted: 1,
            max_attempts: 1,
            evaluation_width: 1,
            thread_cap: 1,
            target_reached: true,
            stop_reason: "accepted_target_reached".to_owned(),
            attempts: vec![attempt],
            outcome_audits: vec![audit],
            attempt_journal,
            proposal_deltas: delta_journal.deltas.clone(),
            delta_journal,
            accepted_records: vec![record],
            compact_accepted_journal,
            schedule_state_receipt: V5EvolvedScheduleStateReceipt {
                generation_index: request.generation_index,
                generation_config_sha256: request.generation_config_sha256.clone(),
                shared_authority_sha256: authority.shared_authority_sha256.clone(),
                target_accepted: 1,
                max_attempts: 1,
                parent_schedule_sha256: None,
                accepted_by_origin: state.origin_accepted_counts.clone(),
                disposition_counts: state.disposition_counts.clone(),
                next_proposal_ordinal: state.next_proposal_ordinal,
                structural_parent_selections: state.structural_parent_selections,
                crossover_attempts: 0,
                proposal_state: state.compact_value(),
                initial_parent_selector_state: initial_parent_selector_state.clone(),
                final_parent_selector_state: initial_parent_selector_state,
                identity_ledger_identity: ledger.identity().clone(),
                initial_identity_ledger_state,
                final_identity_ledger_state: ledger.compact_state(),
            },
            parent_snapshot_inventory: Some(V5EvolvedParentSnapshotInventory {
                generation_index: request.generation_index,
                generation_config_sha256: request.generation_config_sha256.clone(),
                shared_authority_sha256: authority.shared_authority_sha256.clone(),
                source_parent_archive_input_binding_sha256: request
                    .parent_archive_input_binding_sha256
                    .clone(),
                source_parent_archive_semantic_sha256: request.parent_selector_state_sha256.clone(),
                snapshots: snapshots.into_values().collect(),
                attempt_snapshot_refs: vec![snapshot_refs],
            }),
        };
        result
            .verify_replay()
            .expect("self-contained native mutation result replays structurally");
        (request, result)
    }

    #[test]
    fn fast_ephemeral_parent_cache_key_binds_material_not_draw_audit() {
        let config_sha256 = sha(object([(
            "schemaVersion",
            Value::String("temporal_qd_v5_evolved_parent_cache_probe_v1".to_owned()),
        )]));
        let authority = sealed_authority();
        let parent = sealed_g0_parent(&authority, &config_sha256, 0);
        let original = V5VerifiedParentCache::key(&authority.shared_authority_sha256, &parent)
            .expect("exact parent cache key");

        let mut relabeled = parent.clone();
        relabeled.candidate_id.push_str("-substituted");
        assert_ne!(
            original,
            V5VerifiedParentCache::key(&authority.shared_authority_sha256, &relabeled)
                .expect("relabeled parent cache key")
        );
        let mut payload_substitution = parent.clone();
        payload_substitution.pair_payload = object([("substituted", Value::Bool(true))]);
        assert_ne!(
            original,
            V5VerifiedParentCache::key(&authority.shared_authority_sha256, &payload_substitution,)
                .expect("payload-substituted parent cache key")
        );
        let mut audit_substitution = parent;
        audit_substitution.selection_audit = Some(object([("substituted", Value::Bool(true))]));
        assert_eq!(
            original,
            V5VerifiedParentCache::key(&authority.shared_authority_sha256, &audit_substitution)
                .expect("audit-substituted parent cache key")
        );
        assert_ne!(
            original,
            V5VerifiedParentCache::key(
                &sha(object([("differentAuthority", Value::Bool(true))])),
                &audit_substitution,
            )
            .expect("authority-substituted parent cache key")
        );
    }

    #[test]
    fn sealed_mutation_snapshot_replays_after_source_archive_is_unavailable() {
        let (request, result) = sealed_native_mutation_result();
        let encoded = result.to_value().expect("encode self-contained result");
        let parsed = V5EvolvedTransactionResult::from_value(&encoded)
            .expect("parse self-contained snapshot result");
        // No ParentSelector or source parent payload is retained here.  The
        // replay must use the persisted snapshot inventory exclusively.
        verify_v5_evolved_transaction_replay(&request, &parsed)
            .expect("offline sealed mutation replay after source archive removal");

        let mut missing_snapshot = encoded.clone();
        missing_snapshot
            .get_mut("parentSnapshotInventory")
            .and_then(Value::as_object_mut)
            .expect("snapshot inventory object")
            .insert("snapshots".to_owned(), Value::Array(Vec::new()));
        assert!(V5EvolvedTransactionResult::from_value(&missing_snapshot).is_err());

        let mut tampered_snapshot = encoded;
        tampered_snapshot
            .get_mut("parentSnapshotInventory")
            .and_then(Value::as_object_mut)
            .and_then(|inventory| inventory.get_mut("snapshots"))
            .and_then(Value::as_array_mut)
            .and_then(|snapshots| snapshots.first_mut())
            .and_then(Value::as_object_mut)
            .expect("sealed snapshot object")
            .insert(
                "sourceParentArchiveInputBindingSha256".to_owned(),
                sha(object([("tampered", Value::Bool(true))])).into(),
            );
        assert!(V5EvolvedTransactionResult::from_value(&tampered_snapshot).is_err());
    }

    #[test]
    fn scheduler_selected_distinct_parents_reach_real_crossover_terminal_and_ledger() {
        let config_sha256 = sha(object([(
            "schemaVersion",
            Value::String("temporal_qd_v5_evolved_native_crossover_probe_v1".to_owned()),
        )]));
        let authority = sealed_authority();
        let first_parent = sealed_g0_parent(&authority, &config_sha256, 0);
        let second_parent = sealed_g0_parent(&authority, &config_sha256, 1);
        assert_ne!(
            first_parent.pair_identity_sha256, second_parent.pair_identity_sha256,
            "the crossover witness must use distinct authenticated G0 parents",
        );
        let mut selector =
            ExplicitParentRing::new(vec![first_parent, second_parent]).expect("seal parent ring");
        let schedule = ProposalSchedule {
            config_sha256: config_sha256.clone(),
            generation_index: 2,
            parent_schedule: Some(
                RotatingParentSchedule::from_counts(2, 2).expect("two-parent schedule"),
            ),
            // Isolate a scheduler-owned offspring slot at ordinal six without
            // fabricating a crossover proposal by hand.
            desired_evaluated_offspring: 1,
            desired_evaluated_immigrants: 0,
        };
        let mut planner = ProposalPlanner {
            schedule,
            parents: &mut selector,
        };
        let mut state = ProposalState {
            next_proposal_ordinal: 6,
            ..ProposalState::default()
        };
        let planned = planner
            .plan_next(&mut state)
            .expect("scheduler must select the ordinal-six crossover");
        let (scheduled_parent, scheduled_mate) = match &planned.intent {
            ProposalIntent::SameSideCrossover { parent, mate, .. } => (parent, mate),
            _ => panic!("ordinal-six scheduler witness did not produce a crossover"),
        };
        assert_ne!(
            scheduled_parent.pair_identity_sha256, scheduled_mate.pair_identity_sha256,
            "scheduler must preserve distinct crossover inputs",
        );
        let mut ledger = ledger();
        let mut request = direct_native_request(config_sha256);
        request.identity_ledger_identity_sha256 = sha(ledger.identity().clone());
        request.identity_ledger_state_sha256 = sha(ledger.compact_state());
        let durable_outcome = construct_sealed_crossover(&authority, &request, &planned, 0, None)
            .expect("sealed durable crossover terminal");
        let cache = V5VerifiedParentCache::default();
        let outcome = construct_sealed_crossover(&authority, &request, &planned, 0, Some(&cache))
            .expect("sealed fast-ephemeral crossover terminal");
        assert_eq!(outcome.disposition, durable_outcome.disposition);
        assert_eq!(outcome.reason_code, durable_outcome.reason_code);
        assert_eq!(outcome.stage, durable_outcome.stage);
        assert_eq!(outcome.lineage_refs, durable_outcome.lineage_refs);
        assert_eq!(
            outcome
                .delta
                .as_ref()
                .expect("fast crossover delta")
                .to_value()
                .expect("encode fast crossover delta"),
            durable_outcome
                .delta
                .as_ref()
                .expect("durable crossover delta")
                .to_value()
                .expect("encode durable crossover delta"),
            "one-pass fast crossover must preserve the exact durable delta",
        );
        assert_eq!(
            outcome
                .accepted
                .as_ref()
                .expect("fast accepted crossover")
                .record
                .to_value()
                .expect("encode fast accepted crossover"),
            durable_outcome
                .accepted
                .as_ref()
                .expect("durable accepted crossover")
                .record
                .to_value()
                .expect("encode durable accepted crossover"),
            "one-pass fast crossover must preserve the exact durable accepted record",
        );
        let primary = cache
            .load(&authority, scheduled_parent)
            .expect("reload cached crossover primary");
        let mate = cache
            .load(&authority, scheduled_mate)
            .expect("reload cached crossover mate");
        let side = proposal_side_for_seed(planned.intent.proposal_seed())
            .expect("derive crossover test side");
        let operator_authority = authority
            .operator_authority_projection()
            .expect("project crossover operator authority")
            .operator_authority(side)
            .expect("select crossover side authority");
        let execution = attempt_evolved_same_side_crossover_from_states(
            planned.intent.proposal_seed(),
            if side == "long" {
                &primary.long_state
            } else {
                &primary.short_state
            },
            if side == "long" {
                &mate.long_state
            } else {
                &mate.short_state
            },
            &operator_authority,
        );
        let selection = execution
            .selection
            .as_ref()
            .expect("accepted crossover test selection");
        let crossover_delta = execution
            .delta
            .as_ref()
            .expect("accepted crossover test delta");
        let (recipient, donor) = crossover_recipient_and_donor(
            &selection.recipient_pair_identity_sha256,
            &selection.donor_pair_identity_sha256,
            &primary,
            &mate,
        )
        .expect("resolve crossover test recipient/donor");
        let recompiler = V5SealedEvolvedPairRecompiler::from_verified_parent(
            &authority,
            recipient,
            planned.intent.proposal_seed(),
        )
        .expect("start verified crossover test recompiler");
        recompiler
            .advance_same_side_crossover_once(donor, crossover_delta)
            .expect("verified donor fast path must accept authentic cached material");
        let mut tampered_donor = donor.clone();
        if side == "long" {
            tampered_donor.long_state.module_identity_sha256 =
                sha(object([("tamperedDonorModule", Value::Bool(true))]));
        } else {
            tampered_donor.short_state.module_identity_sha256 =
                sha(object([("tamperedDonorModule", Value::Bool(true))]));
        }
        assert!(
            recompiler
                .advance_same_side_crossover_once(&tampered_donor, crossover_delta)
                .is_err(),
            "verified-donor reuse must reject stale or substituted compiled state",
        );
        assert_eq!(cache.misses.load(Ordering::Relaxed), 2);
        assert!(cache.hits.load(Ordering::Relaxed) >= 2);
        let delta = outcome
            .delta
            .clone()
            .expect("every real crossover terminal retains an all-attempt delta");
        assert_eq!(delta.scheduled_kind, "same_side_crossover");
        assert!(delta.parent.is_some());
        assert!(delta.mate.is_some());
        assert!(outcome.lineage_refs.parent.is_some());
        assert!(outcome.lineage_refs.mate.is_some());
        let (attempt, audit, accepted) =
            admitted_attempt(&request, &authority, &state, &mut ledger, &planned, outcome)
                .expect("ledger must retain the real crossover terminal disposition");
        assert_eq!(
            attempt.proposal_delta_sha256.as_deref(),
            Some(
                delta
                    .delta_sha256()
                    .expect("crossover delta identity")
                    .as_str()
            )
        );
        audit
            .verify_binds_attempt(&attempt)
            .expect("crossover audit binds compact attempt");
        let record = accepted.expect("sealed crossover must materialize a compact record");
        assert_eq!(attempt.disposition, "accepted");
        assert_eq!(audit.stage, "accepted");
        assert_eq!(
            record.proposal_delta_sha256,
            delta.delta_sha256().expect("crossover delta identity"),
        );
    }

    #[test]
    fn retries_hold_the_accepted_quota_and_record_the_ordinal_six_crossover() {
        let mut parents = TestParentSelector::new();
        let mut ledger = ledger();
        let mut engine = RejectingEngine::default();
        let result = execute_with_engine(
            request(1, &parents, &ledger),
            &mut parents,
            &mut ledger,
            &mut engine,
        )
        .expect("execute rejecting scheduler harness");

        assert!(!result.target_reached);
        assert_eq!(result.stop_reason, "max_attempts_reached");
        assert_eq!(result.attempts.len(), 7);
        assert_eq!(result.schedule_state_receipt.next_proposal_ordinal, 7);
        assert_eq!(result.schedule_state_receipt.crossover_attempts, 1);
        assert_eq!(
            result.schedule_state_receipt.structural_parent_selections,
            8
        );
        assert_eq!(
            engine.scheduled_kinds,
            vec![
                "structural_offspring",
                "structural_offspring",
                "structural_offspring",
                "structural_offspring",
                "structural_offspring",
                "structural_offspring",
                "same_side_crossover",
            ]
        );
        assert_eq!(engine.mutation_depths.len(), 6);
        assert!(result.attempts[6].lineage_refs.mate.is_some());
        assert_eq!(result.proposal_deltas.len(), result.attempts.len());
        result.verify_replay().expect("retry stream replays");
    }

    #[test]
    fn caps_one_two_and_eight_have_exact_ordered_retry_semantics() {
        let mut semantic_values = Vec::new();
        for thread_cap in [1_u64, 2, 8] {
            let mut parents = TestParentSelector::new();
            let mut ledger = ledger();
            let mut engine = RejectingEngine::default();
            let (result, telemetry) = execute_with_engine_and_telemetry(
                request(thread_cap, &parents, &ledger),
                &mut parents,
                &mut ledger,
                &mut engine,
            )
            .expect("bounded evolved scheduler harness");

            assert_eq!(result.thread_cap, thread_cap);
            assert_eq!(result.attempts.len(), 7);
            assert_eq!(telemetry.committed_count, 7);
            assert!(telemetry.peak_in_flight <= thread_cap);
            if thread_cap == 1 {
                assert_eq!(telemetry.speculative_discarded_count, 0);
                assert_eq!(telemetry.constructed_count, telemetry.committed_count);
            } else {
                assert!(telemetry.speculative_discarded_count > 0);
                assert!(telemetry.constructed_count > telemetry.committed_count);
            }
            semantic_values.push(result.to_value().expect("semantic transaction value"));
        }

        assert_eq!(semantic_values[0], semantic_values[1]);
        assert_eq!(semantic_values[0], semantic_values[2]);
    }

    #[test]
    fn first_rejection_discards_speculative_suffix_before_authoritative_replan() {
        let mut serial_parents = TestParentSelector::new();
        let mut serial_ledger = ledger();
        let mut serial_engine = RejectingEngine::default();
        let (serial, serial_telemetry) = execute_with_engine_and_telemetry(
            request(1, &serial_parents, &serial_ledger),
            &mut serial_parents,
            &mut serial_ledger,
            &mut serial_engine,
        )
        .expect("serial rejection oracle");

        let mut parallel_parents = TestParentSelector::new();
        let mut parallel_ledger = ledger();
        let mut parallel_engine = RejectingEngine::default();
        let (parallel, parallel_telemetry) = execute_with_engine_and_telemetry(
            request(8, &parallel_parents, &parallel_ledger),
            &mut parallel_parents,
            &mut parallel_ledger,
            &mut parallel_engine,
        )
        .expect("parallel rejection executor");

        assert_eq!(
            serial.to_value().expect("serial value"),
            parallel.to_value().expect("parallel value"),
        );
        assert_eq!(serial_telemetry.speculative_discarded_count, 0);
        assert!(parallel_telemetry.speculative_discarded_count > 0);
        assert_eq!(
            parallel.attempts.len() as u64,
            parallel_telemetry.committed_count
        );
        assert_eq!(
            parallel.schedule_state_receipt.final_parent_selector_state,
            serial.schedule_state_receipt.final_parent_selector_state,
        );
        assert_eq!(
            parallel.schedule_state_receipt.final_identity_ledger_state,
            serial.schedule_state_receipt.final_identity_ledger_state,
        );
    }

    #[test]
    fn constructor_wave_has_real_bounded_concurrency_without_wall_clock_assertions() {
        let mut parents = TestParentSelector::new();
        let mut ledger = ledger();
        let mut bounded_request = request(8, &parents, &ledger);
        bounded_request.max_attempts = 2;
        let first_wave_barrier = Arc::new(Barrier::new(2));
        let construction_calls = Arc::new(TestAtomicU64::new(0));

        let (_, telemetry) = execute_with_constructor_and_telemetry(
            bounded_request,
            &mut parents,
            &mut ledger,
            V5EvolvedExecutionOptions::scheduler_test(),
            None,
            {
                let first_wave_barrier = Arc::clone(&first_wave_barrier);
                let construction_calls = Arc::clone(&construction_calls);
                move |authority, request, planned, _birth_ordinal| {
                    let call = construction_calls.fetch_add(1, TestOrdering::SeqCst);
                    if call < 2 {
                        first_wave_barrier.wait();
                    }
                    rejecting_outcome(authority, request, planned)
                }
            },
        )
        .expect("bounded parallel construction witness");

        assert!(telemetry.peak_active_workers > 1);
        assert!(telemetry.peak_active_workers <= 8);
        assert!(telemetry.peak_in_flight <= 8);
        assert_eq!(telemetry.committed_count, 2);
        assert_eq!(telemetry.speculative_discarded_count, 1);
        assert_eq!(telemetry.constructed_count, 3);
        assert_eq!(construction_calls.load(TestOrdering::SeqCst), 3);
    }

    #[test]
    fn progress_worker_guard_releases_active_count_on_every_drop_path() {
        let activity = V5ConstructionActivity::default();
        assert_eq!(activity.active.load(Ordering::Relaxed), 0);
        {
            let _first = V5ConstructionWorkerGuard::new(&activity, None);
            assert_eq!(activity.active.load(Ordering::Relaxed), 1);
            {
                let _second = V5ConstructionWorkerGuard::new(&activity, None);
                assert_eq!(activity.active.load(Ordering::Relaxed), 2);
                assert_eq!(activity.peak.load(Ordering::Relaxed), 2);
            }
            assert_eq!(activity.active.load(Ordering::Relaxed), 1);
        }
        assert_eq!(activity.active.load(Ordering::Relaxed), 0);
        assert_eq!(activity.peak.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn construction_error_joins_the_entire_wave_without_partial_commit_or_deadlock() {
        let mut parents = TestParentSelector::new();
        let mut ledger = ledger();
        let mut bounded_request = request(8, &parents, &ledger);
        bounded_request.max_attempts = 2;
        let first_wave_barrier = Arc::new(Barrier::new(2));
        let construction_calls = Arc::new(TestAtomicU64::new(0));

        let error = execute_with_constructor_and_telemetry(
            bounded_request,
            &mut parents,
            &mut ledger,
            V5EvolvedExecutionOptions::scheduler_test(),
            None,
            {
                let first_wave_barrier = Arc::clone(&first_wave_barrier);
                let construction_calls = Arc::clone(&construction_calls);
                move |authority, request, planned, _birth_ordinal| {
                    let call = construction_calls.fetch_add(1, TestOrdering::SeqCst);
                    first_wave_barrier.wait();
                    if call == 0 {
                        Err(contract("intentional parallel construction failure"))
                    } else {
                        rejecting_outcome(authority, request, planned)
                    }
                }
            },
        )
        .expect_err("one worker failure must abort the uncommitted wave");

        assert!(
            error
                .to_string()
                .contains("intentional parallel construction failure")
        );
        assert_eq!(construction_calls.load(TestOrdering::SeqCst), 2);
        assert_eq!(
            parents.draws, 0,
            "speculative selector draws must be restored"
        );
        assert_eq!(
            ledger.compact_state().get("proposalCount"),
            Some(&Value::from(0_u64)),
        );
    }

    fn real_immigrant_fixture(
        thread_cap: u64,
    ) -> (
        V5EvolvedTransactionRequest,
        ExplicitParentRing,
        CandidateIdentityLedger,
    ) {
        let parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
        let ledger = ledger();
        let config_sha256 = sha(object([(
            "schemaVersion",
            Value::String("temporal_qd_v5_evolved_replay_policy_test_v1".to_owned()),
        )]));
        let mut request = direct_native_request(config_sha256);
        request.thread_cap = thread_cap;
        request.parent_selector_state_sha256 = sha(parents.compact_state());
        request.identity_ledger_identity_sha256 = sha(ledger.identity().clone());
        request.identity_ledger_state_sha256 = sha(ledger.compact_state());
        (request, parents, ledger)
    }

    #[test]
    fn accepted_public_projection_reuses_only_its_exact_sealed_pair() {
        let (request, _parents, _ledger) = real_immigrant_fixture(1);
        let authority =
            V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)
                .expect("parse accepted projection authority");
        let proposal_seed = v5_proposal_seed(&request.generation_config_sha256, 0)
            .expect("derive accepted projection seed");
        let planned = PlannedProposal {
            proposal_ordinal: 0,
            intent: ProposalIntent::RichImmigrant {
                long_seed: immigrant_side_seed(&proposal_seed, Side::Long),
                short_seed: immigrant_side_seed(&proposal_seed, Side::Short),
                proposal_seed,
            },
        };
        let material = NativeV5EvolvedConstructionEngine::durable()
            .construct(&authority, &request, &planned, 0)
            .expect("construct accepted projection material")
            .accepted
            .expect("immigrant projection material must be accepted");
        let first = crate::v5::materialize_v5_evolved_rich_candidate(&authority, &material)
            .expect("project exact sealed pair");
        let second = crate::v5::materialize_v5_evolved_rich_candidate(&authority, &material)
            .expect("repeat exact sealed pair projection");
        assert_eq!(
            first, second,
            "sealed projection bytes must be deterministic"
        );

        let mut substituted = material;
        substituted.parent_material.long_program = Value::Null;
        assert!(
            crate::v5::materialize_v5_evolved_rich_candidate(&authority, &substituted).is_err(),
            "sealed pair projection must reject detached program material",
        );
    }

    fn global_duplicate_immigrant_fixture(
        thread_cap: u64,
    ) -> (
        V5EvolvedTransactionRequest,
        ExplicitParentRing,
        CandidateIdentityLedger,
    ) {
        let (mut request, parents, base_ledger) = real_immigrant_fixture(thread_cap);
        request.target_accepted = 2;
        request.max_attempts = 6;
        let authority =
            V5SharedConstructionAuthority::from_shared_object(&request.shared_authority)
                .expect("duplicate fixture authority");
        let proposal_seed = v5_proposal_seed(&request.generation_config_sha256, 0)
            .expect("duplicate fixture proposal seed");
        let planned = PlannedProposal {
            proposal_ordinal: 0,
            intent: ProposalIntent::RichImmigrant {
                long_seed: immigrant_side_seed(&proposal_seed, Side::Long),
                short_seed: immigrant_side_seed(&proposal_seed, Side::Short),
                proposal_seed,
            },
        };
        let outcome = NativeV5EvolvedConstructionEngine::durable()
            .construct(&authority, &request, &planned, 0)
            .expect("duplicate fixture native immigrant");
        let duplicate_identity = outcome
            .accepted
            .expect("duplicate fixture immigrant must materialize")
            .record
            .candidate_identity_sha256;
        let ledger =
            CandidateIdentityLedger::new(base_ledger.identity().clone(), vec![duplicate_identity])
                .expect("duplicate fixture identity ledger");
        request.identity_ledger_identity_sha256 = sha(ledger.identity().clone());
        request.identity_ledger_state_sha256 = sha(ledger.compact_state());
        (request, parents, ledger)
    }

    #[test]
    fn global_duplicate_rejection_discards_suffix_and_matches_serial() {
        let mut values = Vec::new();
        let mut discard_counts = Vec::new();
        for thread_cap in [1_u64, 8] {
            let (request, mut parents, mut ledger) = global_duplicate_immigrant_fixture(thread_cap);
            let engine = NativeV5EvolvedConstructionEngine::fast_ephemeral();
            let (result, telemetry) = execute_with_constructor_and_telemetry(
                request,
                &mut parents,
                &mut ledger,
                V5EvolvedExecutionOptions::fast_ephemeral(),
                None,
                |authority, request, planned, birth_ordinal| {
                    engine.construct(authority, request, planned, birth_ordinal)
                },
            )
            .expect("global duplicate exact-parity transaction");

            assert!(result.target_reached);
            assert_eq!(result.attempts[0].disposition, "rejected");
            assert_eq!(
                result.attempts[0].reason_code,
                "duplicate_candidate_identity_global",
            );
            assert_eq!(result.outcome_audits[0].stage, "identity_ledger");
            values.push(result.to_value().expect("global duplicate semantic value"));
            discard_counts.push(telemetry.speculative_discarded_count);
        }
        assert_eq!(values[0], values[1]);
        assert_eq!(discard_counts[0], 0);
        assert!(discard_counts[1] > 0);
    }

    #[test]
    fn native_accepted_population_is_identical_across_thread_caps() {
        let mut semantic_values = Vec::new();
        let mut serial_replay = None;
        for thread_cap in [1_u64, 2, 8] {
            let (mut request, mut parents, mut ledger) = real_immigrant_fixture(thread_cap);
            request.target_accepted = 2;
            request.max_attempts = 8;
            let engine = NativeV5EvolvedConstructionEngine::fast_ephemeral();
            let (result, telemetry) = execute_with_constructor_and_telemetry(
                request.clone(),
                &mut parents,
                &mut ledger,
                V5EvolvedExecutionOptions::fast_ephemeral(),
                None,
                |authority, request, planned, birth_ordinal| {
                    engine.construct(authority, request, planned, birth_ordinal)
                },
            )
            .expect("native accepted parallel transaction");

            assert!(result.target_reached);
            assert_eq!(result.accepted_records.len(), 2);
            assert!(telemetry.peak_in_flight <= thread_cap);
            let semantic = result.to_value().expect("native accepted semantic value");
            if thread_cap == 1 {
                serial_replay = Some((request, result));
            }
            semantic_values.push(semantic);
        }

        assert_eq!(semantic_values[0], semantic_values[1]);
        assert_eq!(semantic_values[0], semantic_values[2]);
        let (request, result) = serial_replay.expect("serial accepted replay oracle");
        let mut serial_sink = NoopV5EvolvedAcceptedReplaySink;
        let serial_telemetry = replay_v5_evolved_transaction_with_accepted_sink_and_cap(
            &request,
            &result,
            1,
            &mut serial_sink,
        )
        .expect("native accepted cap-one transaction offline replays");
        let mut parallel_sink = NoopV5EvolvedAcceptedReplaySink;
        let parallel_telemetry = replay_v5_evolved_transaction_with_accepted_sink_and_cap(
            &request,
            &result,
            8,
            &mut parallel_sink,
        )
        .expect("native accepted cap-eight transaction offline replays");
        assert_eq!(serial_telemetry.attempt_count, result.attempts.len() as u64);
        assert_eq!(
            parallel_telemetry.attempt_count,
            result.attempts.len() as u64
        );
        assert_eq!(serial_telemetry.accepted_count, 2);
        assert_eq!(parallel_telemetry.accepted_count, 2);
        assert_eq!(serial_telemetry.construction_wave_count, 2);
        assert_eq!(parallel_telemetry.construction_wave_count, 1);
        assert_eq!(serial_telemetry.peak_active_workers, 1);
        assert!((1..=8).contains(&parallel_telemetry.peak_active_workers));
        assert_eq!(serial_telemetry.snapshot_count, 0);
        assert_eq!(parallel_telemetry.snapshot_count, 0);
    }

    #[test]
    fn bounded_offline_replay_preserves_lowest_ordinal_sink_error() {
        struct FailingOrderedSink {
            seen: Vec<u64>,
        }

        impl V5EvolvedAcceptedReplaySink for FailingOrderedSink {
            fn observe_attempt(
                &mut self,
                _authority: &V5SharedConstructionAuthority,
                attempt: &V5ProposalAttemptRecord,
                _audit: &V5AttemptOutcomeAudit,
                _material: Option<&V5CoreEvolvedAcceptedMaterial>,
            ) -> Result<()> {
                self.seen.push(attempt.proposal_ordinal);
                Err(contract("ordered replay sink failure at ordinal zero"))
            }

            fn accept(
                &mut self,
                _authority: &V5SharedConstructionAuthority,
                _material: &V5CoreEvolvedAcceptedMaterial,
            ) -> Result<()> {
                Ok(())
            }
        }

        let (request, mut parents, mut ledger) = real_immigrant_fixture(1);
        let transaction = execute_v5_evolved_transaction_fast_ephemeral_with_progress(
            request.clone(),
            &mut parents,
            &mut ledger,
            None,
        )
        .expect("construct ordered replay error fixture");
        let mut serial = FailingOrderedSink { seen: Vec::new() };
        let serial_error = replay_v5_evolved_transaction_with_accepted_sink_and_cap(
            &request,
            &transaction,
            1,
            &mut serial,
        )
        .expect_err("cap-one replay must surface sink failure");
        let mut parallel = FailingOrderedSink { seen: Vec::new() };
        let parallel_error = replay_v5_evolved_transaction_with_accepted_sink_and_cap(
            &request,
            &transaction,
            8,
            &mut parallel,
        )
        .expect_err("cap-eight replay must surface the same sink failure");
        assert_eq!(serial_error.to_string(), parallel_error.to_string());
        assert_eq!(serial.seen, vec![0]);
        assert_eq!(parallel.seen, vec![0]);
    }

    #[test]
    fn fast_ephemeral_defers_only_immediate_replay_and_remains_offline_replayable() {
        let (durable_request, mut durable_parents, mut durable_ledger) = real_immigrant_fixture(1);
        let durable_engine = NativeV5EvolvedConstructionEngine::durable();
        let (durable, durable_telemetry) = execute_with_constructor_and_telemetry(
            durable_request.clone(),
            &mut durable_parents,
            &mut durable_ledger,
            V5EvolvedExecutionOptions::durable(),
            None,
            |authority, request, planned, birth_ordinal| {
                durable_engine.construct(authority, request, planned, birth_ordinal)
            },
        )
        .expect("durable replay-policy oracle");

        let (fast_request, mut fast_parents, mut fast_ledger) = real_immigrant_fixture(1);
        let fast_engine = NativeV5EvolvedConstructionEngine::fast_ephemeral();
        let (fast, fast_telemetry) = execute_with_constructor_and_telemetry(
            fast_request.clone(),
            &mut fast_parents,
            &mut fast_ledger,
            V5EvolvedExecutionOptions::fast_ephemeral(),
            None,
            |authority, request, planned, birth_ordinal| {
                fast_engine.construct(authority, request, planned, birth_ordinal)
            },
        )
        .expect("fast-ephemeral replay policy");

        assert_eq!(
            durable_telemetry.immediate_replay_count,
            durable.attempts.len() as u64,
        );
        assert_eq!(fast_telemetry.immediate_replay_count, 0);
        assert!(fast.parent_snapshot_inventory.is_some());
        assert_eq!(
            durable.to_value().expect("durable semantic value"),
            fast.to_value().expect("fast semantic value"),
            "execution-only replay policy must not alter transaction semantics",
        );
        verify_v5_evolved_transaction_replay(&fast_request, &fast)
            .expect("deferred fast-ephemeral transaction must pass full offline replay");
    }

    #[test]
    fn stale_parent_selector_binding_fails_before_the_first_draw() {
        let mut parents = TestParentSelector::new();
        let mut ledger = ledger();
        let mut request = request(1, &parents, &ledger);
        request.parent_selector_state_sha256 = sha(object([("tampered", Value::Bool(true))]));
        let mut engine = RejectingEngine::default();
        let error = execute_with_engine(request, &mut parents, &mut ledger, &mut engine)
            .expect_err("stale parent-selector state must fail closed");
        assert!(
            error
                .to_string()
                .contains("parent archive or identity ledger input binding drifted")
        );
        assert!(engine.scheduled_kinds.is_empty());
    }
}
