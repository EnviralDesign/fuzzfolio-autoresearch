//! Python-compatible, campaign-global identity ledger.
//!
//! This module is intentionally the sole Rust implementation of the v3
//! five-identity duplicate policy.  It keeps the public ledger byte-compatible
//! with AutoResearch while its compact state is a private restart mechanism.

use std::collections::{BTreeMap, BTreeSet};

use temporal_qd_contract::{Map, Value, canonical_sha256};
use temporal_qd_kernel::proposal::{
    IdentityLedger, LedgerDecision, LedgerProposal, ProposalError, Result,
};

pub const IDENTITY_LEDGER_SCHEMA: &str = "temporal_qd_identity_ledger_v3";
pub const PAIR_EXECUTABLE_SEMANTIC_RECORD_SCHEMA: &str =
    "temporal_qd_pair_executable_semantic_record_v1";
pub const DELTA_SCHEMA: &str = "temporal_qd_global_identity_ledger_delta_v1";
pub const COMPACT_STATE_SCHEMA: &str = "temporal_qd_global_identity_ledger_compact_v1";

const FIELDS: [(&str, &str); 5] = [
    ("candidateIdentity", "candidateIdentitySha256"),
    ("program", "programSha256"),
    ("sourceProfile", "sourceProfileSha256"),
    ("profileSnapshot", "profileSnapshotSha256"),
    ("canonicalEvidence", "canonicalEvidenceIdentitySha256"),
];

fn contract(message: impl Into<String>) -> ProposalError {
    ProposalError::Contract(message.into())
}

fn object<'a>(entries: impl IntoIterator<Item = (&'a str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
    )
}

fn exact_keys(map: &Map<String, Value>, expected: &[&str], name: &str) -> Result<()> {
    let actual = map.keys().map(String::as_str).collect::<Vec<_>>();
    let mut expected = expected.to_vec();
    expected.sort_unstable();
    if actual != expected {
        return Err(contract(format!("{name} fields are not exact")));
    }
    Ok(())
}

fn sha(value: &Value, name: &str) -> Result<String> {
    let value = value
        .as_str()
        .ok_or_else(|| contract(format!("{name} must be a lowercase SHA-256 identity")))?;
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value.as_bytes()[7..]
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(contract(format!(
            "{name} must be a lowercase SHA-256 identity"
        )));
    }
    Ok(value.to_owned())
}

fn integer(value: &Value, name: &str) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| contract(format!("{name} must be a non-negative integer")))
}

fn member<'a>(map: &'a Map<String, Value>, key: &str, name: &str) -> Result<&'a Value> {
    map.get(key)
        .ok_or_else(|| contract(format!("{name} lacks {key}")))
}

fn record_from_candidate(candidate: &Value) -> Result<Value> {
    let candidate = candidate
        .as_object()
        .ok_or_else(|| contract("ledger candidate must be an object"))?;
    Ok(object(
        FIELDS
            .into_iter()
            .map(|(_, key)| {
                let value = member(candidate, key, "ledger candidate")?;
                Ok::<_, ProposalError>((key, Value::String(sha(value, key)?)))
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn parse_record(value: &Value, name: &str) -> Result<Value> {
    let map = value
        .as_object()
        .ok_or_else(|| contract(format!("{name} must be an object")))?;
    exact_keys(map, &FIELDS.map(|(_, key)| key), name)?;
    Ok(object(
        FIELDS
            .into_iter()
            .map(|(_, key)| {
                Ok::<_, ProposalError>((key, Value::String(sha(member(map, key, name)?, key)?)))
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn record_field<'a>(record: &'a Value, key: &str) -> &'a str {
    record
        .as_object()
        .expect("validated ledger record")
        .get(key)
        .and_then(Value::as_str)
        .expect("validated ledger record field")
}

fn initial_counters() -> BTreeMap<String, u64> {
    let mut counters = BTreeMap::new();
    for (name, _) in FIELDS {
        counters.insert(name.to_owned(), 0);
    }
    counters.insert("programDifferentEvidenceAllowed".to_owned(), 0);
    counters
}

fn initial_unique_counts() -> BTreeMap<String, u64> {
    FIELDS
        .into_iter()
        .map(|(name, _)| (name.to_owned(), 0))
        .collect()
}

fn initial_slots() -> BTreeMap<String, u64> {
    BTreeMap::from([
        ("proposalsObserved".to_owned(), 0),
        ("acceptedUniqueProposalSlots".to_owned(), 0),
        ("duplicateRejections".to_owned(), 0),
    ])
}

fn counts_value(counts: &BTreeMap<String, u64>) -> Value {
    Value::Object(
        counts
            .iter()
            .map(|(key, value)| (key.clone(), Value::from(*value)))
            .collect(),
    )
}

fn parse_counts(value: &Value, expected: &[&str], name: &str) -> Result<BTreeMap<String, u64>> {
    let map = value
        .as_object()
        .ok_or_else(|| contract(format!("{name} must be an object")))?;
    exact_keys(map, expected, name)?;
    expected
        .iter()
        .map(|key| Ok(((*key).to_owned(), integer(member(map, key, name)?, key)?)))
        .collect()
}

fn record_indices(records: &[Value]) -> BTreeMap<String, BTreeSet<String>> {
    FIELDS
        .into_iter()
        .map(|(name, key)| {
            (
                name.to_owned(),
                records
                    .iter()
                    .map(|record| record_field(record, key).to_owned())
                    .collect(),
            )
        })
        .collect()
}

fn unique_counts(records: &[Value]) -> BTreeMap<String, u64> {
    record_indices(records)
        .into_iter()
        .map(|(name, values)| (name, values.len() as u64))
        .collect()
}

/// Exact bootstrap input.  The native runtime deliberately requires identity
/// material already frozen by the Python authority rather than recreating a
/// root candidate or evidence identity under a subtly different contract.
#[derive(Clone, Debug)]
pub struct ArchiveBootstrapInput {
    pub candidate: Value,
    pub canonical_evidence_identity_sha256: String,
    pub executable_semantic_sha256: Option<String>,
}

/// Immutable outcome of [`GlobalIdentityLedger::prepare_proposal`].  It is
/// serialized into the sealed proposal segment before it mutates ledger state.
#[derive(Clone, Debug)]
struct Delta {
    proposal_ordinal: u64,
    disposition: String,
    checks: BTreeMap<String, bool>,
    candidate_record: Option<Value>,
    executable_semantic_sha256: Option<String>,
}

impl Delta {
    fn value(&self, authority_sha256: &str) -> Result<Value> {
        let checks = Value::Object(
            self.checks
                .iter()
                .map(|(key, value)| (key.clone(), Value::Bool(*value)))
                .collect(),
        );
        let mut value = object([
            ("schemaVersion", Value::String(DELTA_SCHEMA.to_owned())),
            (
                "ledgerAuthoritySha256",
                Value::String(authority_sha256.to_owned()),
            ),
            ("proposalOrdinal", Value::from(self.proposal_ordinal)),
            ("disposition", Value::String(self.disposition.clone())),
            ("identityChecks", checks),
            (
                "record",
                self.candidate_record.clone().unwrap_or(Value::Null),
            ),
            (
                "executableSemanticSha256",
                self.executable_semantic_sha256
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
        ]);
        let hash = canonical_sha256(&value)?;
        value
            .as_object_mut()
            .expect("object")
            .insert("preparedDeltaSha256".to_owned(), Value::String(hash));
        Ok(value)
    }

    fn parse(value: &Value, authority_sha256: &str) -> Result<Self> {
        let map = value
            .as_object()
            .ok_or_else(|| contract("identity ledger delta must be an object"))?;
        exact_keys(
            map,
            &[
                "schemaVersion",
                "ledgerAuthoritySha256",
                "proposalOrdinal",
                "disposition",
                "identityChecks",
                "record",
                "executableSemanticSha256",
                "preparedDeltaSha256",
            ],
            "identity ledger delta",
        )?;
        if member(map, "schemaVersion", "identity ledger delta")?.as_str() != Some(DELTA_SCHEMA) {
            return Err(contract("identity ledger delta schema is incompatible"));
        }
        if sha(
            member(map, "ledgerAuthoritySha256", "identity ledger delta")?,
            "ledger authority",
        )? != authority_sha256
        {
            return Err(contract(
                "identity ledger delta is bound to another authority",
            ));
        }
        let supplied = sha(
            member(map, "preparedDeltaSha256", "identity ledger delta")?,
            "prepared delta",
        )?;
        let mut material = map.clone();
        material.remove("preparedDeltaSha256");
        if canonical_sha256(&Value::Object(material))? != supplied {
            return Err(contract(
                "identity ledger prepared delta identity mismatched",
            ));
        }
        let checks_map = member(map, "identityChecks", "identity ledger delta")?
            .as_object()
            .ok_or_else(|| contract("identity ledger delta checks must be an object"))?;
        exact_keys(
            checks_map,
            &FIELDS.map(|(name, _)| name),
            "identity ledger delta checks",
        )?;
        let checks = FIELDS
            .into_iter()
            .map(|(name, _)| {
                let value = member(checks_map, name, "identity ledger delta checks")?
                    .as_bool()
                    .ok_or_else(|| contract("identity ledger delta check must be boolean"))?;
                Ok((name.to_owned(), value))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let record = match member(map, "record", "identity ledger delta")? {
            Value::Null => None,
            value => Some(parse_record(value, "identity ledger delta record")?),
        };
        let semantic = match member(map, "executableSemanticSha256", "identity ledger delta")? {
            Value::Null => None,
            value => Some(sha(value, "identity ledger executable semantic")?),
        };
        let disposition = member(map, "disposition", "identity ledger delta")?
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| contract("identity ledger delta disposition is invalid"))?
            .to_owned();
        if disposition == "accepted" && record.is_none() {
            return Err(contract("accepted identity ledger delta lacks record"));
        }
        Ok(Self {
            proposal_ordinal: integer(
                member(map, "proposalOrdinal", "identity ledger delta")?,
                "proposal ordinal",
            )?,
            disposition,
            checks,
            candidate_record: record,
            executable_semantic_sha256: semantic,
        })
    }
}

/// The complete v3 policy and its restart-safe private indices.
#[derive(Clone, Debug)]
pub struct GlobalIdentityLedger {
    policy_identity: Value,
    authority_sha256: String,
    public: Value,
    records: Vec<Value>,
    indices: BTreeMap<String, BTreeSet<String>>,
    pair_semantics: BTreeMap<String, String>,
    // Python persists pair semantic records in first-acceptance order; the
    // BTreeMap is only an ephemeral membership/conflict index.
    pair_semantic_order: Vec<String>,
    duplicate_counters: BTreeMap<String, u64>,
    proposal_slot_counters: BTreeMap<String, u64>,
}

impl GlobalIdentityLedger {
    /// Parse an immutable Python-written ledger facade and verify every public
    /// self-hash/count/policy binding before use.
    pub fn from_public(ledger: Value) -> Result<Self> {
        let map = ledger
            .as_object()
            .ok_or_else(|| contract("identity ledger must be an object"))?;
        let has_pair = map.contains_key("pairExecutableSemantics")
            || map.contains_key("pairExecutableSemanticDuplicateRejections");
        let mut expected = vec![
            "schemaVersion",
            "qdVersion",
            "policyName",
            "policySha256",
            "identityPolicy",
            "records",
            "uniqueCounts",
            "duplicateCounters",
            "proposalSlotCounters",
            "ledgerSha256",
        ];
        if has_pair {
            expected.extend([
                "pairExecutableSemantics",
                "pairExecutableSemanticDuplicateRejections",
            ]);
        }
        exact_keys(map, &expected, "identity ledger")?;
        if member(map, "schemaVersion", "identity ledger")?.as_str() != Some(IDENTITY_LEDGER_SCHEMA)
        {
            return Err(contract("identity ledger schema is incompatible"));
        }
        let supplied = sha(
            member(map, "ledgerSha256", "identity ledger")?,
            "identity ledger",
        )?;
        let mut material = map.clone();
        material.remove("ledgerSha256");
        if canonical_sha256(&Value::Object(material))? != supplied {
            return Err(contract("identity ledger identity mismatch"));
        }
        let policy_sha256 = sha(
            member(map, "policySha256", "identity ledger")?,
            "identity ledger policy",
        )?;
        if !member(map, "identityPolicy", "identity ledger")?.is_object() {
            return Err(contract(
                "identity ledger identity policy must be an object",
            ));
        }
        let policy_identity = object([
            (
                "schemaVersion",
                member(map, "schemaVersion", "identity ledger")?.clone(),
            ),
            (
                "qdVersion",
                member(map, "qdVersion", "identity ledger")?.clone(),
            ),
            (
                "policyName",
                member(map, "policyName", "identity ledger")?.clone(),
            ),
            ("policySha256", Value::String(policy_sha256.clone())),
            (
                "identityPolicy",
                member(map, "identityPolicy", "identity ledger")?.clone(),
            ),
        ]);
        let authority_sha256 = canonical_sha256(&policy_identity)?;
        let records = member(map, "records", "identity ledger")?
            .as_array()
            .ok_or_else(|| contract("identity ledger records must be an array"))?
            .iter()
            .map(|record| parse_record(record, "identity ledger record"))
            .collect::<Result<Vec<_>>>()?;
        let candidate_ids = records
            .iter()
            .map(|record| record_field(record, "candidateIdentitySha256"))
            .collect::<BTreeSet<_>>();
        if candidate_ids.len() != records.len() {
            return Err(contract(
                "identity ledger repeats a candidate identity record",
            ));
        }
        let expected_unique = FIELDS.map(|(name, _)| name);
        let unique = parse_counts(
            member(map, "uniqueCounts", "identity ledger")?,
            &expected_unique,
            "identity ledger unique counts",
        )?;
        if unique != unique_counts(&records) {
            return Err(contract("identity ledger unique counts mismatched records"));
        }
        let expected_duplicate = [
            "candidateIdentity",
            "program",
            "sourceProfile",
            "profileSnapshot",
            "canonicalEvidence",
            "programDifferentEvidenceAllowed",
        ];
        let duplicate_counters = parse_counts(
            member(map, "duplicateCounters", "identity ledger")?,
            &expected_duplicate,
            "identity ledger duplicate counters",
        )?;
        let expected_slots = [
            "proposalsObserved",
            "acceptedUniqueProposalSlots",
            "duplicateRejections",
        ];
        let proposal_slot_counters = parse_counts(
            member(map, "proposalSlotCounters", "identity ledger")?,
            &expected_slots,
            "identity ledger proposal slot counters",
        )?;
        let mut pair_semantics = BTreeMap::new();
        let mut pair_semantic_order = Vec::new();
        if has_pair {
            let rejections = integer(
                member(
                    map,
                    "pairExecutableSemanticDuplicateRejections",
                    "identity ledger",
                )?,
                "pair executable semantic duplicate rejections",
            )?;
            let pairs = member(map, "pairExecutableSemantics", "identity ledger")?
                .as_array()
                .ok_or_else(|| contract("pair executable semantics must be an array"))?;
            for pair in pairs {
                let pair = pair
                    .as_object()
                    .ok_or_else(|| contract("pair executable semantic record must be an object"))?;
                exact_keys(
                    pair,
                    &[
                        "schemaVersion",
                        "pairGenomeSemanticSha256",
                        "candidateIdentitySha256",
                    ],
                    "pair executable semantic record",
                )?;
                if member(pair, "schemaVersion", "pair executable semantic record")?.as_str()
                    != Some(PAIR_EXECUTABLE_SEMANTIC_RECORD_SCHEMA)
                {
                    return Err(contract("pair executable semantic schema is incompatible"));
                }
                let semantic = sha(
                    member(
                        pair,
                        "pairGenomeSemanticSha256",
                        "pair executable semantic record",
                    )?,
                    "pair executable semantic",
                )?;
                let candidate = sha(
                    member(
                        pair,
                        "candidateIdentitySha256",
                        "pair executable semantic record",
                    )?,
                    "pair executable semantic candidate",
                )?;
                if pair_semantics.insert(semantic.clone(), candidate).is_some() {
                    return Err(contract(
                        "identity ledger has duplicate executable pair semantics",
                    ));
                }
                pair_semantic_order.push(semantic);
            }
            let _ = rejections;
        }
        let mut parsed = Self {
            policy_identity,
            authority_sha256,
            public: ledger,
            records,
            indices: BTreeMap::new(),
            pair_semantics,
            pair_semantic_order,
            duplicate_counters,
            proposal_slot_counters,
        };
        parsed.indices = record_indices(&parsed.records);
        parsed.refresh_public()?;
        Ok(parsed)
    }

    /// Create the exact public empty ledger used by Python's v3 evolution
    /// policy.  The caller supplies frozen policy identity material, so this
    /// function cannot accidentally hard-code a different campaign policy.
    pub fn empty(
        qd_version: String,
        policy_name: String,
        policy_sha256: String,
        identity_policy: Value,
    ) -> Result<Self> {
        sha(
            &Value::String(policy_sha256.clone()),
            "identity ledger policy",
        )?;
        if !identity_policy.is_object() {
            return Err(contract(
                "identity ledger identity policy must be an object",
            ));
        }
        let mut public = object([
            (
                "schemaVersion",
                Value::String(IDENTITY_LEDGER_SCHEMA.to_owned()),
            ),
            ("qdVersion", Value::String(qd_version)),
            ("policyName", Value::String(policy_name)),
            ("policySha256", Value::String(policy_sha256)),
            ("identityPolicy", identity_policy),
            ("records", Value::Array(Vec::new())),
            ("uniqueCounts", counts_value(&initial_unique_counts())),
            ("duplicateCounters", counts_value(&initial_counters())),
            ("proposalSlotCounters", counts_value(&initial_slots())),
        ]);
        let hash = canonical_sha256(&public)?;
        public
            .as_object_mut()
            .expect("object")
            .insert("ledgerSha256".to_owned(), Value::String(hash));
        Self::from_public(public)
    }

    /// Construct the pair-generation ledger facade.  Pair runs expose their
    /// semantic extension at CP0—even before the first proposal is accepted—
    /// so a first local rejection cannot produce a different ledger identity
    /// from Python.
    pub fn empty_pair(
        qd_version: String,
        policy_name: String,
        policy_sha256: String,
        identity_policy: Value,
    ) -> Result<Self> {
        let mut ledger = Self::empty(qd_version, policy_name, policy_sha256, identity_policy)?;
        ledger.enable_pair_mode()?;
        Ok(ledger)
    }

    /// Upgrade an empty/general v3 facade into the explicit pair-generation
    /// mode.  This is idempotent and is deliberately public for callers that
    /// reopen a general Python ledger before initializing pair generation.
    pub fn enable_pair_mode(&mut self) -> Result<()> {
        let map = self
            .public
            .as_object_mut()
            .expect("validated public ledger object");
        if !map.contains_key("pairExecutableSemantics") {
            map.insert(
                "pairExecutableSemantics".to_owned(),
                Value::Array(Vec::new()),
            );
        }
        if !map.contains_key("pairExecutableSemanticDuplicateRejections") {
            map.insert(
                "pairExecutableSemanticDuplicateRejections".to_owned(),
                Value::from(0_u64),
            );
        }
        self.refresh_public()
    }

    /// Add frozen archive inputs before proposal execution.  This is
    /// idempotent by candidate identity and refuses a semantic collision owned
    /// by another candidate rather than silently selecting a representative.
    pub fn bootstrap_archive(
        &mut self,
        inputs: impl IntoIterator<Item = ArchiveBootstrapInput>,
    ) -> Result<()> {
        for input in inputs {
            let mut candidate = input
                .candidate
                .as_object()
                .ok_or_else(|| contract("archive bootstrap candidate must be an object"))?
                .clone();
            if !candidate.contains_key("profileSnapshotSha256") {
                let source = member(
                    &candidate,
                    "sourceProfileSha256",
                    "archive bootstrap candidate",
                )?
                .clone();
                candidate.insert("profileSnapshotSha256".to_owned(), source);
            }
            candidate.insert(
                "canonicalEvidenceIdentitySha256".to_owned(),
                Value::String(input.canonical_evidence_identity_sha256),
            );
            let record = record_from_candidate(&Value::Object(candidate))?;
            let candidate_identity = record_field(&record, "candidateIdentitySha256").to_owned();
            if !self.indices["candidateIdentity"].contains(&candidate_identity) {
                self.records.push(record.clone());
                for (name, key) in FIELDS {
                    self.indices
                        .get_mut(name)
                        .expect("initialized")
                        .insert(record_field(&record, key).to_owned());
                }
                // Python archive bootstrap supplies historical identity records
                // but does not retroactively count proposal slots for this run.
            }
            if let Some(semantic) = input.executable_semantic_sha256 {
                sha(
                    &Value::String(semantic.clone()),
                    "archive executable semantic",
                )?;
                match self.pair_semantics.get(&semantic) {
                    Some(existing) if existing != &candidate_identity => {
                        return Err(contract(
                            "identity ledger has duplicate executable pair semantics",
                        ));
                    }
                    Some(_) => {}
                    None => {
                        self.pair_semantics
                            .insert(semantic.clone(), candidate_identity);
                        self.pair_semantic_order.push(semantic);
                    }
                }
            }
        }
        self.refresh_public()
    }

    /// Recover an accepted proposal that was sealed before the prior process
    /// could checkpoint the ledger.  Repeating the same candidate is safe;
    /// another candidate claiming that semantic is a hard contradiction.
    pub fn recover_accepted(
        &mut self,
        candidate: &Value,
        executable_semantic_sha256: &str,
    ) -> Result<()> {
        let record = record_from_candidate(candidate)?;
        let candidate_identity = record_field(&record, "candidateIdentitySha256").to_owned();
        if !self.indices["candidateIdentity"].contains(&candidate_identity) {
            self.records.push(record.clone());
            for (name, key) in FIELDS {
                self.indices
                    .get_mut(name)
                    .expect("initialized")
                    .insert(record_field(&record, key).to_owned());
            }
            // `_pair_ledger_recover_accepted_entry` delegates to
            // `_ledger_accept` when this record is absent, so recovery does
            // increment accepted unique proposal slots.  The kernel owns the
            // separately persisted/replayed proposal watermark.
            *self
                .proposal_slot_counters
                .get_mut("acceptedUniqueProposalSlots")
                .expect("initialized") += 1;
        }
        sha(
            &Value::String(executable_semantic_sha256.to_owned()),
            "recovered executable semantic",
        )?;
        match self.pair_semantics.get(executable_semantic_sha256) {
            Some(existing) if existing != &candidate_identity => {
                return Err(contract(
                    "pair accepted proposal duplicates a global executable pair semantic",
                ));
            }
            Some(_) => {}
            None => {
                self.pair_semantics
                    .insert(executable_semantic_sha256.to_owned(), candidate_identity);
                self.pair_semantic_order
                    .push(executable_semantic_sha256.to_owned());
            }
        }
        self.refresh_public()
    }

    fn refresh_public(&mut self) -> Result<()> {
        let pair_rejections = self.pair_semantic_rejections().unwrap_or(0);
        let map = self
            .public
            .as_object_mut()
            .expect("validated public ledger object");
        map.insert("records".to_owned(), Value::Array(self.records.clone()));
        map.insert(
            "uniqueCounts".to_owned(),
            counts_value(&unique_counts(&self.records)),
        );
        map.insert(
            "duplicateCounters".to_owned(),
            counts_value(&self.duplicate_counters),
        );
        map.insert(
            "proposalSlotCounters".to_owned(),
            counts_value(&self.proposal_slot_counters),
        );
        if map.contains_key("pairExecutableSemantics") || !self.pair_semantics.is_empty() {
            map.insert(
                "pairExecutableSemantics".to_owned(),
                Value::Array(
                    self.pair_semantic_order
                        .iter()
                        .map(|semantic| {
                            let candidate = self
                                .pair_semantics
                                .get(semantic)
                                .expect("pair semantic order/index are coherent");
                            object([
                                (
                                    "schemaVersion",
                                    Value::String(
                                        PAIR_EXECUTABLE_SEMANTIC_RECORD_SCHEMA.to_owned(),
                                    ),
                                ),
                                ("pairGenomeSemanticSha256", Value::String(semantic.clone())),
                                ("candidateIdentitySha256", Value::String(candidate.clone())),
                            ])
                        })
                        .collect(),
                ),
            );
            map.insert(
                "pairExecutableSemanticDuplicateRejections".to_owned(),
                Value::from(pair_rejections),
            );
        }
        map.remove("ledgerSha256");
        let hash = canonical_sha256(&self.public)?;
        self.public
            .as_object_mut()
            .expect("object")
            .insert("ledgerSha256".to_owned(), Value::String(hash));
        Ok(())
    }

    fn pair_semantic_rejections(&self) -> Option<u64> {
        self.public
            .as_object()
            .and_then(|map| map.get("pairExecutableSemanticDuplicateRejections"))
            .and_then(Value::as_u64)
    }

    fn prepare(&self, proposal: LedgerProposal<'_>) -> Result<LedgerDecision> {
        // Python only enters the global ledger after local construction and
        // local semantic duplicate checks have admitted a materialized pair.
        // A locally rejected proposal advances the proposal-slot watermark but
        // must not perturb global duplicate counters or identity indices.
        let record = if proposal.tentative_disposition == "accepted" {
            proposal.candidate.map(record_from_candidate).transpose()?
        } else {
            None
        };
        let checks: BTreeMap<String, bool> = if let Some(record) = &record {
            FIELDS
                .into_iter()
                .map(|(name, key)| {
                    (
                        name.to_owned(),
                        self.indices[name].contains(record_field(record, key)),
                    )
                })
                .collect()
        } else {
            FIELDS
                .into_iter()
                .map(|(name, _)| (name.to_owned(), false))
                .collect()
        };
        let mut disposition = proposal.tentative_disposition.to_owned();
        if proposal.tentative_disposition == "accepted" {
            let record = record
                .as_ref()
                .ok_or_else(|| contract("accepted proposal lacks ledger candidate"))?;
            let semantic = proposal
                .executable_semantic_sha256
                .ok_or_else(|| contract("accepted proposal lacks executable semantic identity"))?;
            sha(
                &Value::String(semantic.to_owned()),
                "proposal executable semantic",
            )?;
            if self.pair_semantics.contains_key(semantic) {
                disposition = "duplicate_pair_genome_global".to_owned();
            } else if checks["candidateIdentity"] {
                disposition = "duplicate_candidate_identity_global".to_owned();
            } else if checks["canonicalEvidence"] {
                disposition = "duplicate_canonical_evidence_global".to_owned();
            }
            let _ = record;
        }
        let delta = Delta {
            proposal_ordinal: proposal.proposal_ordinal,
            disposition: disposition.clone(),
            checks: checks.clone(),
            candidate_record: record,
            executable_semantic_sha256: if proposal.tentative_disposition == "accepted" {
                proposal.executable_semantic_sha256.map(ToOwned::to_owned)
            } else {
                None
            },
        };
        Ok(LedgerDecision::new(
            disposition,
            Value::Object(
                checks
                    .into_iter()
                    .map(|(key, value)| (key, Value::Bool(value)))
                    .collect(),
            ),
            delta.value(&self.authority_sha256)?,
        ))
    }
}

impl IdentityLedger for GlobalIdentityLedger {
    fn identity(&self) -> &Value {
        &self.policy_identity
    }

    fn prepare_proposal(&self, proposal: LedgerProposal<'_>) -> Result<LedgerDecision> {
        self.prepare(proposal)
    }

    fn commit_prepared_delta(&mut self, prepared_delta: &Value) -> Result<()> {
        let delta = Delta::parse(prepared_delta, &self.authority_sha256)?;
        let expected = self.proposal_slot_counters["proposalsObserved"];
        if delta.proposal_ordinal != expected {
            return Err(contract(
                "identity ledger delta proposal ordinal is stale or out of order",
            ));
        }
        *self
            .proposal_slot_counters
            .get_mut("proposalsObserved")
            .expect("initialized") += 1;
        if let Some(record) = &delta.candidate_record {
            for (name, _) in FIELDS {
                if delta.checks[name] {
                    *self.duplicate_counters.get_mut(name).expect("initialized") += 1;
                }
            }
            if delta.checks["program"] && !delta.checks["canonicalEvidence"] {
                *self
                    .duplicate_counters
                    .get_mut("programDifferentEvidenceAllowed")
                    .expect("initialized") += 1;
            }
            // `_ledger_duplicate_check` increments this before pair-semantic
            // precedence is applied.  A global pair-semantic duplicate can
            // therefore still carry an identity duplicate rejection.
            if delta.checks["candidateIdentity"] || delta.checks["canonicalEvidence"] {
                *self
                    .proposal_slot_counters
                    .get_mut("duplicateRejections")
                    .expect("initialized") += 1;
            }
            if delta.disposition == "duplicate_pair_genome_global" {
                let current = self.pair_semantic_rejections().unwrap_or(0);
                self.public.as_object_mut().expect("object").insert(
                    "pairExecutableSemanticDuplicateRejections".to_owned(),
                    Value::from(current + 1),
                );
            }
            if delta.disposition == "accepted" {
                let candidate_identity = record_field(record, "candidateIdentitySha256").to_owned();
                if self.indices["candidateIdentity"].contains(&candidate_identity) {
                    return Err(contract(
                        "accepted identity ledger delta repeats candidate identity",
                    ));
                }
                let semantic = delta.executable_semantic_sha256.as_ref().ok_or_else(|| {
                    contract("accepted identity ledger delta lacks executable semantic")
                })?;
                if let Some(existing) = self.pair_semantics.get(semantic) {
                    if existing != &candidate_identity {
                        return Err(contract(
                            "identity ledger has duplicate executable pair semantics",
                        ));
                    }
                    return Err(contract(
                        "accepted identity ledger delta repeats executable pair semantic",
                    ));
                }
                self.records.push(record.clone());
                for (name, key) in FIELDS {
                    self.indices
                        .get_mut(name)
                        .expect("initialized")
                        .insert(record_field(record, key).to_owned());
                }
                self.pair_semantics
                    .insert(semantic.clone(), candidate_identity);
                self.pair_semantic_order.push(semantic.clone());
                *self
                    .proposal_slot_counters
                    .get_mut("acceptedUniqueProposalSlots")
                    .expect("initialized") += 1;
            }
        }
        self.refresh_public()
    }

    fn compact_state(&self) -> Value {
        object([
            (
                "schemaVersion",
                Value::String(COMPACT_STATE_SCHEMA.to_owned()),
            ),
            (
                "ledgerAuthoritySha256",
                Value::String(self.authority_sha256.clone()),
            ),
            ("publicLedger", self.public.clone()),
        ])
    }

    fn restore_compact_state(&mut self, state: &Value) -> Result<()> {
        let map = state
            .as_object()
            .ok_or_else(|| contract("identity ledger compact state must be an object"))?;
        exact_keys(
            map,
            &["schemaVersion", "ledgerAuthoritySha256", "publicLedger"],
            "identity ledger compact state",
        )?;
        if member(map, "schemaVersion", "identity ledger compact state")?.as_str()
            != Some(COMPACT_STATE_SCHEMA)
        {
            return Err(contract(
                "identity ledger compact state schema is incompatible",
            ));
        }
        if sha(
            member(
                map,
                "ledgerAuthoritySha256",
                "identity ledger compact state",
            )?,
            "identity ledger compact authority",
        )? != self.authority_sha256
        {
            return Err(contract(
                "identity ledger compact state is bound to another authority",
            ));
        }
        let restored = Self::from_public(
            member(map, "publicLedger", "identity ledger compact state")?.clone(),
        )?;
        if restored.authority_sha256 != self.authority_sha256 {
            return Err(contract(
                "identity ledger compact policy mismatched checkpoint",
            ));
        }
        *self = restored;
        Ok(())
    }

    fn public_ledger(&self) -> Option<Value> {
        Some(self.public.clone())
    }
}
