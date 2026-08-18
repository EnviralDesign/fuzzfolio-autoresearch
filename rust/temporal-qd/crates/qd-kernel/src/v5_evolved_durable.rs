//! Typed durable-object inventory and offline reconstruction for native v5
//! later-generation transactions.
//!
//! This layer deliberately contains no scheduler, compiler, or publication
//! policy.  It names the exact compact objects a write-neutral kernel result
//! requires a batch sink to persist, then reconstructs only from those
//! objects before invoking the sealed offline replay path.  In particular it
//! never reopens a prior-generation archive or expands a rich candidate.

use std::collections::BTreeSet;

use temporal_qd_contract::{Map, Value};

use crate::{
    v5::{
        V5AttemptJournal, V5AttemptOutcomeAudit, V5CompactAcceptedRecord, V5EvolvedParentSnapshot,
        V5ProposalAttemptRecord, v5_native_object_relative_path,
    },
    v5_evolved_transaction::{
        Result, V5EvolvedAttemptSnapshotRefs, V5EvolvedCompactAcceptedJournal,
        V5EvolvedDeltaJournal, V5EvolvedParentSnapshotInventory, V5EvolvedProposalDelta,
        V5EvolvedScheduleStateReceipt, V5EvolvedTransactionError, V5EvolvedTransactionRequest,
        V5EvolvedTransactionResult,
    },
};

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

/// The canonical object families needed to reconstruct a native evolved
/// transaction after a receipt is present but its control-plane result is
/// absent.  The enum order is the on-disk inventory order; within each family
/// result order is already semantically constrained (proposal ordinal, birth
/// ordinal, or snapshot SHA).
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum V5EvolvedDurableObjectKind {
    Transaction,
    AttemptJournal,
    ProposalAttempt,
    AttemptOutcomeAudit,
    ProposalDelta,
    DeltaJournal,
    CompactAcceptedRecord,
    CompactAcceptedJournal,
    ScheduleStateReceipt,
    ParentSnapshotInventory,
    AttemptSnapshotRefs,
    ParentSnapshot,
}

impl V5EvolvedDurableObjectKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transaction => "transaction",
            Self::AttemptJournal => "attemptJournal",
            Self::ProposalAttempt => "proposalAttempt",
            Self::AttemptOutcomeAudit => "attemptOutcomeAudit",
            Self::ProposalDelta => "proposalDelta",
            Self::DeltaJournal => "deltaJournal",
            Self::CompactAcceptedRecord => "compactAcceptedRecord",
            Self::CompactAcceptedJournal => "compactAcceptedJournal",
            Self::ScheduleStateReceipt => "scheduleStateReceipt",
            Self::ParentSnapshotInventory => "parentSnapshotInventory",
            Self::AttemptSnapshotRefs => "attemptSnapshotRefs",
            Self::ParentSnapshot => "parentSnapshot",
        }
    }

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "transaction" => Ok(Self::Transaction),
            "attemptJournal" => Ok(Self::AttemptJournal),
            "proposalAttempt" => Ok(Self::ProposalAttempt),
            "attemptOutcomeAudit" => Ok(Self::AttemptOutcomeAudit),
            "proposalDelta" => Ok(Self::ProposalDelta),
            "deltaJournal" => Ok(Self::DeltaJournal),
            "compactAcceptedRecord" => Ok(Self::CompactAcceptedRecord),
            "compactAcceptedJournal" => Ok(Self::CompactAcceptedJournal),
            "scheduleStateReceipt" => Ok(Self::ScheduleStateReceipt),
            "parentSnapshotInventory" => Ok(Self::ParentSnapshotInventory),
            "attemptSnapshotRefs" => Ok(Self::AttemptSnapshotRefs),
            "parentSnapshot" => Ok(Self::ParentSnapshot),
            _ => Err(contract("v5 evolved durable object kind is invalid")),
        }
    }
}

/// A typed content-addressed object-store entry.  The batch publisher owns
/// bytes, fsync, and write-once linking; it must preserve this exact canonical
/// value at `relative_path` and cannot substitute another object family.
#[derive(Clone, Debug, PartialEq)]
pub struct V5EvolvedDurableObjectBinding {
    pub kind: V5EvolvedDurableObjectKind,
    pub object_sha256: String,
    pub relative_path: String,
    pub value: Value,
}

impl V5EvolvedDurableObjectBinding {
    fn new(kind: V5EvolvedDurableObjectKind, value: Value) -> Result<Self> {
        let object_sha256 = durable_object_identity(kind, &value)?;
        Ok(Self {
            kind,
            relative_path: v5_native_object_relative_path(&object_sha256)?,
            object_sha256,
            value,
        })
    }

    /// Prove the object family, self-hash, and immutable object-store path
    /// agree.  This is intentionally typed: a self-hashed JSON object alone
    /// is insufficient to change its durable role.
    pub fn validate(&self) -> Result<()> {
        let supplied = exact_sha(
            &Value::String(self.object_sha256.clone()),
            "v5 evolved durable object SHA-256",
        )?;
        let expected_path = v5_native_object_relative_path(&supplied)?;
        if self.relative_path != expected_path
            || durable_object_identity(self.kind, &self.value)? != supplied
        {
            return Err(contract("v5 evolved durable object binding drifted"));
        }
        Ok(())
    }

    /// Canonical transport representation for an inventory entry.
    pub fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(object([
            ("kind", Value::String(self.kind.as_str().to_owned())),
            ("objectSha256", Value::String(self.object_sha256.clone())),
            ("relativePath", Value::String(self.relative_path.clone())),
            ("value", self.value.clone()),
        ]))
    }

    /// Strictly parse one persisted inventory entry before it participates in
    /// offline replay.
    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = value
            .as_object()
            .ok_or_else(|| contract("v5 evolved durable object binding must be an object"))?;
        exact_keys(
            fields,
            &["kind", "objectSha256", "relativePath", "value"],
            "v5 evolved durable object binding",
        )?;
        let kind = V5EvolvedDurableObjectKind::from_str(
            required(value, "kind", "v5 evolved durable object binding")?
                .as_str()
                .ok_or_else(|| contract("v5 evolved durable object kind must be text"))?,
        )?;
        let binding = Self {
            kind,
            object_sha256: exact_sha(
                required(value, "objectSha256", "v5 evolved durable object binding")?,
                "v5 evolved durable object SHA-256",
            )?,
            relative_path: required(value, "relativePath", "v5 evolved durable object binding")?
                .as_str()
                .filter(|path| !path.is_empty())
                .map(ToOwned::to_owned)
                .ok_or_else(|| contract("v5 evolved durable object path must be nonempty text"))?,
            value: required(value, "value", "v5 evolved durable object binding")?.clone(),
        };
        binding.validate()?;
        if &binding.to_value()? != value {
            return Err(contract(
                "v5 evolved durable object binding is not canonical",
            ));
        }
        Ok(binding)
    }
}

fn durable_object_identity(kind: V5EvolvedDurableObjectKind, value: &Value) -> Result<String> {
    match kind {
        V5EvolvedDurableObjectKind::Transaction => {
            V5EvolvedTransactionResult::from_value(value)?.transaction_sha256()
        }
        V5EvolvedDurableObjectKind::AttemptJournal => {
            Ok(V5AttemptJournal::from_value(value)?.attempt_journal_sha256()?)
        }
        V5EvolvedDurableObjectKind::ProposalAttempt => {
            Ok(V5ProposalAttemptRecord::from_value(value)?.attempt_sha256()?)
        }
        V5EvolvedDurableObjectKind::AttemptOutcomeAudit => {
            Ok(V5AttemptOutcomeAudit::from_value(value)?.audit_sha256()?)
        }
        V5EvolvedDurableObjectKind::ProposalDelta => {
            V5EvolvedProposalDelta::from_value(value)?.delta_sha256()
        }
        V5EvolvedDurableObjectKind::DeltaJournal => {
            V5EvolvedDeltaJournal::from_value(value)?.delta_journal_sha256()
        }
        V5EvolvedDurableObjectKind::CompactAcceptedRecord => {
            Ok(V5CompactAcceptedRecord::from_value(value)?.record_sha256()?)
        }
        V5EvolvedDurableObjectKind::CompactAcceptedJournal => {
            V5EvolvedCompactAcceptedJournal::from_value(value)?.compact_journal_sha256()
        }
        V5EvolvedDurableObjectKind::ScheduleStateReceipt => {
            V5EvolvedScheduleStateReceipt::from_value(value)?.schedule_state_receipt_sha256()
        }
        V5EvolvedDurableObjectKind::ParentSnapshotInventory => {
            V5EvolvedParentSnapshotInventory::from_value(value)?.parent_snapshot_inventory_sha256()
        }
        V5EvolvedDurableObjectKind::AttemptSnapshotRefs => {
            V5EvolvedAttemptSnapshotRefs::from_value(value)?.snapshot_refs_sha256()
        }
        V5EvolvedDurableObjectKind::ParentSnapshot => {
            Ok(V5EvolvedParentSnapshot::from_value(value)?.parent_snapshot_sha256()?)
        }
    }
}

impl V5EvolvedTransactionResult {
    /// Return the closed, deterministic object inventory for offline
    /// later-generation adoption.  This includes both compact root objects
    /// and all individually addressable all-attempt/snapshot witnesses.
    pub fn durable_object_bindings(&self) -> Result<Vec<V5EvolvedDurableObjectBinding>> {
        self.verify_replay()?;
        let inventory = self.parent_snapshot_inventory.as_ref().ok_or_else(|| {
            contract("v5 evolved durable inventory requires a parent snapshot inventory")
        })?;
        let mut bindings = Vec::new();
        let mut seen = BTreeSet::new();
        let mut append = |kind: V5EvolvedDurableObjectKind, value: Value| -> Result<()> {
            let binding = V5EvolvedDurableObjectBinding::new(kind, value)?;
            if !seen.insert(binding.object_sha256.clone()) {
                return Err(contract(
                    "v5 evolved durable inventory repeats an object SHA-256",
                ));
            }
            bindings.push(binding);
            Ok(())
        };

        append(V5EvolvedDurableObjectKind::Transaction, self.to_value()?)?;
        append(
            V5EvolvedDurableObjectKind::AttemptJournal,
            self.attempt_journal.to_value()?,
        )?;
        for attempt in &self.attempts {
            append(
                V5EvolvedDurableObjectKind::ProposalAttempt,
                attempt.to_value()?,
            )?;
        }
        for audit in &self.outcome_audits {
            append(
                V5EvolvedDurableObjectKind::AttemptOutcomeAudit,
                audit.to_value()?,
            )?;
        }
        for delta in &self.proposal_deltas {
            append(V5EvolvedDurableObjectKind::ProposalDelta, delta.to_value()?)?;
        }
        append(
            V5EvolvedDurableObjectKind::DeltaJournal,
            self.delta_journal.to_value()?,
        )?;
        for record in &self.accepted_records {
            append(
                V5EvolvedDurableObjectKind::CompactAcceptedRecord,
                record.to_value()?,
            )?;
        }
        append(
            V5EvolvedDurableObjectKind::CompactAcceptedJournal,
            self.compact_accepted_journal.to_value()?,
        )?;
        append(
            V5EvolvedDurableObjectKind::ScheduleStateReceipt,
            self.schedule_state_receipt.to_value()?,
        )?;
        append(
            V5EvolvedDurableObjectKind::ParentSnapshotInventory,
            inventory.to_value()?,
        )?;
        for refs in &inventory.attempt_snapshot_refs {
            append(
                V5EvolvedDurableObjectKind::AttemptSnapshotRefs,
                refs.to_value()?,
            )?;
        }
        for snapshot in &inventory.snapshots {
            append(
                V5EvolvedDurableObjectKind::ParentSnapshot,
                snapshot.to_value()?,
            )?;
        }
        Ok(bindings)
    }
}

/// Reconstruct and offline-replay a complete later-generation transaction
/// from its exact immutable object inventory.  Missing, extra, reordered, or
/// aliased bindings are rejected before the authority-bound replay runs.
pub fn reconstruct_v5_evolved_transaction_from_durable_objects(
    request: &V5EvolvedTransactionRequest,
    bindings: &[V5EvolvedDurableObjectBinding],
) -> Result<V5EvolvedTransactionResult> {
    if bindings.is_empty() {
        return Err(contract("v5 evolved durable reconstruction has no objects"));
    }
    let mut seen = BTreeSet::new();
    let mut roots = Vec::new();
    for binding in bindings {
        binding.validate()?;
        if !seen.insert(binding.object_sha256.clone()) {
            return Err(contract(
                "v5 evolved durable reconstruction repeats an object SHA-256",
            ));
        }
        if binding.kind == V5EvolvedDurableObjectKind::Transaction {
            roots.push(&binding.value);
        }
    }
    if roots.len() != 1 {
        return Err(contract(
            "v5 evolved durable reconstruction requires exactly one transaction root",
        ));
    }
    let result = V5EvolvedTransactionResult::from_value(roots[0])?;
    result.verify_offline_replay(request)?;
    let expected = result.durable_object_bindings()?;
    if bindings != expected.as_slice() {
        return Err(contract(
            "v5 evolved durable reconstruction object inventory is missing, extra, reordered, or aliased",
        ));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use flate2::read::GzDecoder;
    use temporal_qd_contract::canonical_sha256;

    use super::*;
    use crate::{
        factory::ParentReference,
        proposal::{CandidateIdentityLedger, IdentityLedger, ParentSelector, ProposalError},
        v5_evolved_transaction::execute_v5_evolved_transaction,
    };

    fn sha(value: Value) -> String {
        canonical_sha256(&value).expect("canonical test value")
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
            serde_json::json!({
                "schemaVersion": "temporal_qd_v5_evolved_durable_empty_parent_selector_v1"
            })
        }

        fn restore_compact_state(
            &mut self,
            state: &Value,
        ) -> std::result::Result<(), ProposalError> {
            if state == &self.compact_state() {
                Ok(())
            } else {
                Err(ProposalError::Contract(
                    "evolved durable empty parent selector state drifted".to_owned(),
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

    fn shared_authority_fixture() -> Value {
        let compressed = include_bytes!(
            "../../../../../tests/fixtures/temporal_qd_v5_shared_authority_oracle.json.gz"
        );
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut text = String::new();
        decoder
            .read_to_string(&mut text)
            .expect("inflate sealed shared authority fixture");
        serde_json::from_str::<Value>(&text)
            .expect("parse sealed shared authority fixture")
            .get("sealedAuthority")
            .cloned()
            .expect("read sealed shared authority")
    }

    fn sealed_immigrant_result() -> (V5EvolvedTransactionRequest, V5EvolvedTransactionResult) {
        let mut parents = EmptyParents;
        let mut ledger = CandidateIdentityLedger::new(
            serde_json::json!({
                "schemaVersion": "temporal_qd_v5_evolved_durable_ledger_v1"
            }),
            Vec::<String>::new(),
        )
        .expect("construct durable test ledger");
        let request = V5EvolvedTransactionRequest {
            shared_authority: shared_authority_fixture(),
            generation_config_sha256: sha(serde_json::json!({
                "schemaVersion": "temporal_qd_v5_evolved_durable_config_v1"
            })),
            parent_archive_input_binding_sha256: sha(serde_json::json!({
                "schemaVersion": "temporal_qd_native_v5_proposal_input_binding_v1",
                "kind": "durable-empty-parent-archive"
            })),
            identity_ledger_input_binding_sha256: sha(serde_json::json!({
                "schemaVersion": "temporal_qd_native_v5_proposal_input_binding_v1",
                "kind": "durable-candidate-identity-ledger"
            })),
            generation_index: 2,
            target_accepted: 1,
            max_attempts: 1,
            evaluation_width: 1,
            thread_cap: 1,
            desired_accepted_offspring: 0,
            desired_accepted_immigrants: 1,
            parent_schedule: None,
            parent_selector_state_sha256: sha(parents.compact_state()),
            identity_ledger_identity_sha256: sha(ledger.identity().clone()),
            identity_ledger_state_sha256: sha(ledger.compact_state()),
        };
        let result = execute_v5_evolved_transaction(request.clone(), &mut parents, &mut ledger)
            .expect("execute sealed evolved durable immigrant");
        (request, result)
    }

    #[test]
    fn evolved_durable_bindings_are_complete_canonical_and_reconstructable() {
        let (request, result) = sealed_immigrant_result();
        let bindings = result
            .durable_object_bindings()
            .expect("derive evolved durable bindings");
        assert_eq!(
            bindings
                .iter()
                .map(|binding| binding.kind)
                .collect::<Vec<_>>(),
            vec![
                V5EvolvedDurableObjectKind::Transaction,
                V5EvolvedDurableObjectKind::AttemptJournal,
                V5EvolvedDurableObjectKind::ProposalAttempt,
                V5EvolvedDurableObjectKind::AttemptOutcomeAudit,
                V5EvolvedDurableObjectKind::ProposalDelta,
                V5EvolvedDurableObjectKind::DeltaJournal,
                V5EvolvedDurableObjectKind::CompactAcceptedRecord,
                V5EvolvedDurableObjectKind::CompactAcceptedJournal,
                V5EvolvedDurableObjectKind::ScheduleStateReceipt,
                V5EvolvedDurableObjectKind::ParentSnapshotInventory,
                V5EvolvedDurableObjectKind::AttemptSnapshotRefs,
            ],
        );
        for binding in &bindings {
            binding.validate().expect("validate durable binding");
            let encoded = binding.to_value().expect("encode durable binding");
            assert_eq!(
                V5EvolvedDurableObjectBinding::from_value(&encoded)
                    .expect("strictly parse durable binding"),
                *binding
            );
        }
        let rebuilt = reconstruct_v5_evolved_transaction_from_durable_objects(&request, &bindings)
            .expect("reconstruct and offline-replay durable inventory");
        assert_eq!(
            rebuilt.to_value().expect("encode rebuilt result"),
            result.to_value().expect("encode original result"),
        );

        let mut reordered = bindings.clone();
        reordered.swap(0, 1);
        assert!(
            reconstruct_v5_evolved_transaction_from_durable_objects(&request, &reordered).is_err()
        );

        let mut duplicate = bindings.clone();
        duplicate.push(bindings[0].clone());
        assert!(
            reconstruct_v5_evolved_transaction_from_durable_objects(&request, &duplicate).is_err()
        );

        let mut aliased = bindings.clone();
        aliased[0].relative_path = "v5-native/objects/sha256/alias.json".to_owned();
        assert!(
            reconstruct_v5_evolved_transaction_from_durable_objects(&request, &aliased).is_err()
        );
    }
}
