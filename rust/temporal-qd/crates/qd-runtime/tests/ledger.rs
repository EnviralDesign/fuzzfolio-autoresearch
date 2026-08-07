use temporal_qd_contract::{Map, Value, canonical_sha256};
use temporal_qd_kernel::proposal::{IdentityLedger, LedgerProposal};
use temporal_qd_runtime::ledger::{GlobalIdentityLedger, IDENTITY_LEDGER_SCHEMA};

fn object(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect::<Map<_, _>>(),
    )
}

fn sha(character: char) -> String {
    format!("sha256:{}", character.to_string().repeat(64))
}

fn candidate(
    candidate: char,
    program: char,
    source: char,
    snapshot: char,
    evidence: char,
) -> Value {
    object([
        ("candidateIdentitySha256", Value::String(sha(candidate))),
        ("programSha256", Value::String(sha(program))),
        ("sourceProfileSha256", Value::String(sha(source))),
        ("profileSnapshotSha256", Value::String(sha(snapshot))),
        (
            "canonicalEvidenceIdentitySha256",
            Value::String(sha(evidence)),
        ),
    ])
}

fn python_empty_ledger() -> Value {
    // Generated with autoresearch.temporal_qd_evolution._empty_identity_ledger
    // and frozen here as a Python-derived public-contract golden.
    let identity_policy = object([
        (
            "candidateIdentity",
            Value::String("reject_exact_repeat".to_owned()),
        ),
        (
            "canonicalEvidence",
            Value::String("candidate_program_ordered_window_semantic_cost_execution".to_owned()),
        ),
        (
            "program",
            Value::String("allow_only_for_different_canonical_evidence".to_owned()),
        ),
        (
            "sourceProfile",
            Value::String("reject_same_evidence_repeat".to_owned()),
        ),
    ]);
    let zero_five = object([
        ("candidateIdentity", Value::from(0_u64)),
        ("program", Value::from(0_u64)),
        ("sourceProfile", Value::from(0_u64)),
        ("profileSnapshot", Value::from(0_u64)),
        ("canonicalEvidence", Value::from(0_u64)),
    ]);
    let duplicate = object([
        ("candidateIdentity", Value::from(0_u64)),
        ("program", Value::from(0_u64)),
        ("sourceProfile", Value::from(0_u64)),
        ("profileSnapshot", Value::from(0_u64)),
        ("canonicalEvidence", Value::from(0_u64)),
        ("programDifferentEvidenceAllowed", Value::from(0_u64)),
    ]);
    let mut ledger = object([
        (
            "schemaVersion",
            Value::String(IDENTITY_LEDGER_SCHEMA.to_owned()),
        ),
        (
            "qdVersion",
            Value::String("temporal_qd_evolution_v3".to_owned()),
        ),
        (
            "policyName",
            Value::String("stage5e7_v3_robust_quality_archive".to_owned()),
        ),
        (
            "policySha256",
            Value::String(
                "sha256:837c670a3cec80246a3231397d945b7cfd602035752eddbe0593dc9644579ca8"
                    .to_owned(),
            ),
        ),
        ("identityPolicy", identity_policy),
        ("records", Value::Array(Vec::new())),
        ("uniqueCounts", zero_five),
        ("duplicateCounters", duplicate),
        (
            "proposalSlotCounters",
            object([
                ("proposalsObserved", Value::from(0_u64)),
                ("acceptedUniqueProposalSlots", Value::from(0_u64)),
                ("duplicateRejections", Value::from(0_u64)),
            ]),
        ),
    ]);
    let hash = canonical_sha256(&ledger).expect("canonical Python-shaped ledger");
    ledger
        .as_object_mut()
        .expect("object")
        .insert("ledgerSha256".to_owned(), Value::String(hash));
    ledger
}

fn python_empty_pair_ledger() -> Value {
    // Equivalent to Python's `_empty_identity_ledger()` followed by the pair
    // extension initialization performed before proposal ordinal zero.
    let mut ledger = python_empty_ledger();
    let fields = ledger.as_object_mut().expect("ledger object");
    fields.remove("ledgerSha256");
    fields.insert(
        "pairExecutableSemantics".to_owned(),
        Value::Array(Vec::new()),
    );
    fields.insert(
        "pairExecutableSemanticDuplicateRejections".to_owned(),
        Value::from(0_u64),
    );
    let hash = canonical_sha256(&ledger).expect("canonical Python pair ledger");
    ledger
        .as_object_mut()
        .expect("ledger object")
        .insert("ledgerSha256".to_owned(), Value::String(hash));
    ledger
}

fn with_proposal_slot_counters(
    mut ledger: Value,
    proposals_observed: u64,
    accepted_unique_proposal_slots: u64,
    duplicate_rejections: u64,
) -> Value {
    let fields = ledger.as_object_mut().expect("ledger object");
    fields.remove("ledgerSha256");
    fields.insert(
        "proposalSlotCounters".to_owned(),
        object([
            ("proposalsObserved", Value::from(proposals_observed)),
            (
                "acceptedUniqueProposalSlots",
                Value::from(accepted_unique_proposal_slots),
            ),
            ("duplicateRejections", Value::from(duplicate_rejections)),
        ]),
    );
    let hash = canonical_sha256(&ledger).expect("canonical seeded Python-shaped ledger");
    ledger
        .as_object_mut()
        .expect("ledger object")
        .insert("ledgerSha256".to_owned(), Value::String(hash));
    ledger
}

fn as_legacy_unbound_delta(mut delta: Value) -> Value {
    let fields = delta.as_object_mut().expect("delta object");
    fields.remove("preparedDeltaSha256");
    fields.remove("generationProposalOrdinalBase");
    fields.insert(
        "schemaVersion".to_owned(),
        Value::String("temporal_qd_global_identity_ledger_delta_v1".to_owned()),
    );
    let hash = canonical_sha256(&delta).expect("canonical legacy delta");
    delta
        .as_object_mut()
        .expect("delta object")
        .insert("preparedDeltaSha256".to_owned(), Value::String(hash));
    delta
}

#[test]
fn python_v3_empty_ledger_golden_is_accepted() {
    let ledger = python_empty_ledger();
    let expected = "sha256:1dadee73d8ef485167012cab082a44f34b888c702238cec09f13850bc9a5f22a";
    assert_eq!(
        ledger["ledgerSha256"].as_str(),
        Some(expected),
        "update only from a Python oracle run"
    );
    GlobalIdentityLedger::from_public(ledger).expect("Python v3 public ledger");
}

#[test]
fn python_v3_empty_pair_ledger_golden_is_preserved_at_cp0() {
    let ledger = python_empty_pair_ledger();
    assert_eq!(
        ledger["ledgerSha256"].as_str(),
        Some("sha256:7da2d3b813162ae1870dd05114095025bff5c6ac92463ff3a128e21daea8c15d"),
        "update only from a Python oracle run"
    );
    let parsed = GlobalIdentityLedger::from_public(ledger.clone()).unwrap();
    assert_eq!(parsed.public_ledger(), Some(ledger));
    let checkpoint = parsed.compact_state();
    let mut restored = GlobalIdentityLedger::from_public(python_empty_ledger()).unwrap();
    restored.restore_compact_state(&checkpoint).unwrap();
    assert_eq!(restored.public_ledger(), parsed.public_ledger());
}

#[test]
fn nonzero_python_public_counter_starts_a_new_local_generation_at_zero() {
    let seeded = with_proposal_slot_counters(python_empty_ledger(), 4_021, 0, 0);
    let mut ledger = GlobalIdentityLedger::from_public(seeded.clone()).unwrap();
    assert_eq!(ledger.public_ledger(), Some(seeded));

    let first = candidate('a', 'b', 'c', 'd', 'e');
    let first_delta = ledger
        .prepare_proposal(LedgerProposal {
            proposal_ordinal: 0,
            candidate: Some(&first),
            executable_semantic_sha256: Some(&sha('9')),
            tentative_disposition: "accepted",
        })
        .unwrap();
    assert_eq!(
        first_delta.prepared_delta["generationProposalOrdinalBase"],
        Value::from(4_021_u64)
    );
    ledger
        .commit_prepared_delta(&first_delta.prepared_delta)
        .unwrap();
    assert_eq!(
        ledger.public_ledger().unwrap()["proposalSlotCounters"]["proposalsObserved"],
        Value::from(4_022_u64)
    );

    let second = candidate('f', 'a', 'b', 'c', 'd');
    let second_delta = ledger
        .prepare_proposal(LedgerProposal {
            proposal_ordinal: 1,
            candidate: Some(&second),
            executable_semantic_sha256: Some(&sha('8')),
            tentative_disposition: "accepted",
        })
        .unwrap();
    ledger
        .commit_prepared_delta(&second_delta.prepared_delta)
        .unwrap();
    assert_eq!(
        ledger.public_ledger().unwrap()["proposalSlotCounters"]["proposalsObserved"],
        Value::from(4_023_u64)
    );
    assert!(
        ledger
            .commit_prepared_delta(&first_delta.prepared_delta)
            .is_err()
    );
}

#[test]
fn compact_restart_keeps_the_nonzero_generation_base_and_local_ordering() {
    let seeded = with_proposal_slot_counters(python_empty_ledger(), 4_021, 0, 0);
    let mut ledger = GlobalIdentityLedger::from_public(seeded).unwrap();
    for (ordinal, (candidate, semantic)) in [
        (candidate('a', 'b', 'c', 'd', 'e'), sha('9')),
        (candidate('f', 'a', 'b', 'c', 'd'), sha('8')),
    ]
    .into_iter()
    .enumerate()
    {
        let decision = ledger
            .prepare_proposal(LedgerProposal {
                proposal_ordinal: ordinal as u64,
                candidate: Some(&candidate),
                executable_semantic_sha256: Some(&semantic),
                tentative_disposition: "accepted",
            })
            .unwrap();
        ledger
            .commit_prepared_delta(&decision.prepared_delta)
            .unwrap();
    }
    let compact = ledger.compact_state();
    assert_eq!(
        compact["generationProposalOrdinalBase"],
        Value::from(4_021_u64)
    );
    let mut restored = GlobalIdentityLedger::from_public(python_empty_ledger()).unwrap();
    restored.restore_compact_state(&compact).unwrap();
    assert_eq!(restored.public_ledger(), ledger.public_ledger());

    let third = candidate('b', 'c', 'd', 'e', 'f');
    let decision = restored
        .prepare_proposal(LedgerProposal {
            proposal_ordinal: 2,
            candidate: Some(&third),
            executable_semantic_sha256: Some(&sha('7')),
            tentative_disposition: "accepted",
        })
        .unwrap();
    restored
        .commit_prepared_delta(&decision.prepared_delta)
        .unwrap();
    assert_eq!(
        restored.public_ledger().unwrap()["proposalSlotCounters"]["proposalsObserved"],
        Value::from(4_024_u64)
    );
}

#[test]
fn unbound_legacy_delta_cannot_cross_into_a_later_generation() {
    let first = candidate('a', 'b', 'c', 'd', 'e');
    let zero_generation = GlobalIdentityLedger::from_public(python_empty_ledger()).unwrap();
    let legacy_delta = as_legacy_unbound_delta(
        zero_generation
            .prepare_proposal(LedgerProposal {
                proposal_ordinal: 0,
                candidate: Some(&first),
                executable_semantic_sha256: Some(&sha('9')),
                tentative_disposition: "accepted",
            })
            .unwrap()
            .prepared_delta,
    );

    let mut zero_generation = zero_generation;
    zero_generation
        .commit_prepared_delta(&legacy_delta)
        .unwrap();

    let seeded = with_proposal_slot_counters(python_empty_ledger(), 4_021, 0, 0);
    let mut later_generation = GlobalIdentityLedger::from_public(seeded).unwrap();
    assert!(
        later_generation
            .commit_prepared_delta(&legacy_delta)
            .is_err()
    );
}

#[test]
fn first_local_rejection_keeps_explicit_empty_pair_extension() {
    let mut ledger = GlobalIdentityLedger::from_public(python_empty_ledger()).unwrap();
    ledger.enable_pair_mode().unwrap();
    let rejected = ledger
        .prepare_proposal(LedgerProposal {
            proposal_ordinal: 0,
            candidate: Some(&candidate('a', 'b', 'c', 'd', 'e')),
            executable_semantic_sha256: Some(&sha('9')),
            tentative_disposition: "duplicate_pair_genome",
        })
        .unwrap();
    assert_eq!(rejected.disposition, "duplicate_pair_genome");
    ledger
        .commit_prepared_delta(&rejected.prepared_delta)
        .unwrap();
    let public = ledger.public_ledger().unwrap();
    assert_eq!(public["pairExecutableSemantics"], Value::Array(Vec::new()));
    assert_eq!(
        public["pairExecutableSemanticDuplicateRejections"],
        Value::from(0_u64)
    );
    assert_eq!(
        public["proposalSlotCounters"]["proposalsObserved"],
        Value::from(1_u64)
    );
}

#[test]
fn five_identities_preserve_program_different_evidence_policy() {
    let mut ledger = GlobalIdentityLedger::from_public(python_empty_ledger()).unwrap();
    let first = candidate('a', 'b', 'c', 'd', 'e');
    let second = candidate('f', 'b', 'c', 'd', '1');
    for (ordinal, (candidate, semantic)) in [(first, sha('9')), (second, sha('8'))]
        .into_iter()
        .enumerate()
    {
        let decision = ledger
            .prepare_proposal(LedgerProposal {
                proposal_ordinal: ordinal as u64,
                candidate: Some(&candidate),
                executable_semantic_sha256: Some(&semantic),
                tentative_disposition: "accepted",
            })
            .unwrap();
        assert_eq!(decision.disposition, "accepted");
        ledger
            .commit_prepared_delta(&decision.prepared_delta)
            .unwrap();
    }
    let public = ledger.public_ledger().unwrap();
    assert_eq!(
        public["uniqueCounts"]["candidateIdentity"],
        Value::from(2_u64)
    );
    assert_eq!(public["uniqueCounts"]["program"], Value::from(1_u64));
    assert_eq!(public["duplicateCounters"]["program"], Value::from(1_u64));
    assert_eq!(
        public["duplicateCounters"]["programDifferentEvidenceAllowed"],
        Value::from(1_u64)
    );
}

#[test]
fn candidate_then_evidence_rejections_are_counted_in_python_order() {
    let mut ledger = GlobalIdentityLedger::from_public(python_empty_ledger()).unwrap();
    let first = candidate('a', 'b', 'c', 'd', 'e');
    let semantic = sha('9');
    let decision = ledger
        .prepare_proposal(LedgerProposal {
            proposal_ordinal: 0,
            candidate: Some(&first),
            executable_semantic_sha256: Some(&semantic),
            tentative_disposition: "accepted",
        })
        .unwrap();
    ledger
        .commit_prepared_delta(&decision.prepared_delta)
        .unwrap();

    // Change the semantic so candidate identity, rather than pair semantics,
    // is the deciding duplicate class.
    let duplicate_candidate = ledger
        .prepare_proposal(LedgerProposal {
            proposal_ordinal: 1,
            candidate: Some(&first),
            executable_semantic_sha256: Some(&sha('8')),
            tentative_disposition: "accepted",
        })
        .unwrap();
    assert_eq!(
        duplicate_candidate.disposition,
        "duplicate_candidate_identity_global"
    );
    ledger
        .commit_prepared_delta(&duplicate_candidate.prepared_delta)
        .unwrap();

    let evidence_duplicate = candidate('f', '7', '6', '5', 'e');
    let duplicate_evidence = ledger
        .prepare_proposal(LedgerProposal {
            proposal_ordinal: 2,
            candidate: Some(&evidence_duplicate),
            executable_semantic_sha256: Some(&sha('4')),
            tentative_disposition: "accepted",
        })
        .unwrap();
    assert_eq!(
        duplicate_evidence.disposition,
        "duplicate_canonical_evidence_global"
    );
    ledger
        .commit_prepared_delta(&duplicate_evidence.prepared_delta)
        .unwrap();
    let public = ledger.public_ledger().unwrap();
    assert_eq!(
        public["proposalSlotCounters"]["duplicateRejections"],
        Value::from(2_u64)
    );
}

#[test]
fn pair_semantic_conflict_rejects_and_increments_dedicated_counter() {
    let mut ledger = GlobalIdentityLedger::from_public(python_empty_ledger()).unwrap();
    let first = candidate('a', 'b', 'c', 'd', 'e');
    let semantic = sha('9');
    let accepted = ledger
        .prepare_proposal(LedgerProposal {
            proposal_ordinal: 0,
            candidate: Some(&first),
            executable_semantic_sha256: Some(&semantic),
            tentative_disposition: "accepted",
        })
        .unwrap();
    ledger
        .commit_prepared_delta(&accepted.prepared_delta)
        .unwrap();
    let conflict = candidate('f', '7', '6', '5', '4');
    let rejected = ledger
        .prepare_proposal(LedgerProposal {
            proposal_ordinal: 1,
            candidate: Some(&conflict),
            executable_semantic_sha256: Some(&semantic),
            tentative_disposition: "accepted",
        })
        .unwrap();
    assert_eq!(rejected.disposition, "duplicate_pair_genome_global");
    ledger
        .commit_prepared_delta(&rejected.prepared_delta)
        .unwrap();
    assert_eq!(
        ledger.public_ledger().unwrap()["pairExecutableSemanticDuplicateRejections"],
        Value::from(1_u64)
    );
}

#[test]
fn interrupted_accepted_recovery_is_idempotent_but_conflicting_semantic_fails_closed() {
    let mut ledger = GlobalIdentityLedger::from_public(python_empty_ledger()).unwrap();
    let recovered = candidate('a', 'b', 'c', 'd', 'e');
    let semantic = sha('9');
    ledger.recover_accepted(&recovered, &semantic).unwrap();
    let before = ledger.public_ledger().unwrap();
    ledger.recover_accepted(&recovered, &semantic).unwrap();
    assert_eq!(before, ledger.public_ledger().unwrap());
    assert!(
        ledger
            .recover_accepted(&candidate('f', '7', '6', '5', '4'), &semantic)
            .is_err()
    );
}

#[test]
fn compact_restore_preserves_prepared_delta_ordering() {
    let mut ledger = GlobalIdentityLedger::from_public(python_empty_ledger()).unwrap();
    let first = candidate('a', 'b', 'c', 'd', 'e');
    let semantic = sha('9');
    let decision = ledger
        .prepare_proposal(LedgerProposal {
            proposal_ordinal: 0,
            candidate: Some(&first),
            executable_semantic_sha256: Some(&semantic),
            tentative_disposition: "accepted",
        })
        .unwrap();
    ledger
        .commit_prepared_delta(&decision.prepared_delta)
        .unwrap();
    let compact = ledger.compact_state();
    let mut restored = GlobalIdentityLedger::from_public(python_empty_ledger()).unwrap();
    restored.restore_compact_state(&compact).unwrap();
    assert_eq!(ledger.public_ledger(), restored.public_ledger());
    assert!(
        restored
            .commit_prepared_delta(&decision.prepared_delta)
            .is_err()
    );
}
