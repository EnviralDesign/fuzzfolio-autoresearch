use std::io::Read;

use flate2::read::GzDecoder;
use temporal_qd_contract::{Value, canonical_sha256};
use temporal_qd_kernel::{
    factory::ParentReference,
    proposal::{CandidateIdentityLedger, IdentityLedger, ParentSelector, ProposalError},
    v5::v5_proposal_seed,
    v5_evolved_transaction::{
        V5_EVOLVED_PROPOSAL_DELTA_SCHEMA, V5EvolvedProposalDelta, V5EvolvedTransactionError,
        V5EvolvedTransactionRequest, V5EvolvedTransactionResult, execute_v5_evolved_transaction,
        verify_v5_evolved_transaction_replay,
    },
};

fn sha(value: Value) -> String {
    canonical_sha256(&value).expect("canonical test value")
}

fn delta() -> V5EvolvedProposalDelta {
    let generation_config_sha256 = sha(serde_json::json!({
        "schemaVersion": "temporal_qd_v5_evolved_transaction_test_config_v1"
    }));
    let proposal_seed = v5_proposal_seed(&generation_config_sha256, 0)
        .expect("derive proposal seed from generation config");
    let long_program = serde_json::json!({"direction": "long", "test": true});
    let short_program = serde_json::json!({"direction": "short", "test": true});
    V5EvolvedProposalDelta {
        generation_index: 2,
        proposal_ordinal: 0,
        generation_config_sha256,
        shared_authority_sha256: sha(serde_json::json!({"authority": "test"})),
        proposal_seed,
        origin_kind: "random_immigrant".to_owned(),
        scheduled_kind: "random_immigrant".to_owned(),
        parent: None,
        mate: None,
        parent_selection_receipt: None,
        mutation_depth: None,
        long_program_sha256: sha(long_program.clone()),
        long_program,
        short_program_sha256: sha(short_program.clone()),
        short_program,
        steps: Vec::new(),
        terminal_operator_plan: None,
        terminal_operator_application: None,
        terminal_operator_trace: None,
        terminal_disposition: "accepted".to_owned(),
        terminal_reason_code: "accepted".to_owned(),
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
        serde_json::json!({
            "schemaVersion": "temporal_qd_v5_evolved_empty_parent_selector_v1"
        })
    }

    fn restore_compact_state(&mut self, state: &Value) -> Result<(), ProposalError> {
        if state == &self.compact_state() {
            Ok(())
        } else {
            Err(ProposalError::Contract(
                "empty evolved parent selector state drifted".to_owned(),
            ))
        }
    }

    fn select(
        &mut self,
        _label: &str,
        _structural_selection_ordinal: u64,
    ) -> Result<ParentReference, ProposalError> {
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

#[test]
fn public_native_immigrant_transaction_is_all_attempt_durable() {
    let mut parents = EmptyParents;
    let mut ledger = CandidateIdentityLedger::new(
        serde_json::json!({
            "schemaVersion": "temporal_qd_v5_evolved_public_native_ledger_v1"
        }),
        Vec::<String>::new(),
    )
    .expect("construct identity ledger");
    let generation_config_sha256 = sha(serde_json::json!({
        "schemaVersion": "temporal_qd_v5_evolved_public_native_config_v1"
    }));
    let request = V5EvolvedTransactionRequest {
        shared_authority: shared_authority_fixture(),
        generation_config_sha256,
        parent_archive_input_binding_sha256: sha(serde_json::json!({
            "schemaVersion": "temporal_qd_native_v5_proposal_input_binding_v1",
            "kind": "empty-parent-archive"
        })),
        identity_ledger_input_binding_sha256: sha(serde_json::json!({
            "schemaVersion": "temporal_qd_native_v5_proposal_input_binding_v1",
            "kind": "candidate-identity-ledger"
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
        .expect("execute public sealed native immigrant transaction");

    assert!(result.target_reached);
    assert_eq!(result.attempts.len(), 1);
    assert_eq!(result.proposal_deltas.len(), result.attempts.len());
    assert!(result.attempts[0].proposal_delta_sha256.is_some());
    assert_eq!(result.accepted_records.len(), 1);
    result
        .verify_replay()
        .expect("public native transaction replay");
    let persisted = result
        .to_value()
        .expect("persist native transaction result");
    let parsed = V5EvolvedTransactionResult::from_value(&persisted)
        .expect("parse self-hashed native transaction result");
    verify_v5_evolved_transaction_replay(&request, &parsed)
        .expect("replay persisted native transaction without a source archive");

    let mut wrong_parent_binding = request.clone();
    wrong_parent_binding.parent_archive_input_binding_sha256 = sha(serde_json::json!({
        "schemaVersion": "temporal_qd_native_v5_proposal_input_binding_v1",
        "kind": "wrong-parent-archive"
    }));
    assert!(verify_v5_evolved_transaction_replay(&wrong_parent_binding, &parsed).is_err());

    let mut wrong_ledger_binding = request;
    wrong_ledger_binding.identity_ledger_input_binding_sha256 = sha(serde_json::json!({
        "schemaVersion": "temporal_qd_native_v5_proposal_input_binding_v1",
        "kind": "wrong-identity-ledger"
    }));
    assert!(verify_v5_evolved_transaction_replay(&wrong_ledger_binding, &parsed).is_err());
}

#[test]
fn all_attempt_delta_is_self_hashed_and_round_trips() {
    let delta = delta();
    let value = delta.to_value().expect("serialize exact delta");
    assert_eq!(value["schemaVersion"], V5_EVOLVED_PROPOSAL_DELTA_SCHEMA,);
    assert_eq!(
        value["deltaSha256"],
        delta.delta_sha256().expect("derive delta hash"),
    );
    assert_eq!(
        V5EvolvedProposalDelta::from_value(&value).expect("parse self-hashed delta"),
        delta,
    );
}

#[test]
fn all_attempt_delta_rejects_tampered_program_identity() {
    let mut value = delta().to_value().expect("serialize exact delta");
    value.as_object_mut().expect("delta object").insert(
        "longProgramSha256".to_owned(),
        Value::String(sha(serde_json::json!("tampered"))),
    );
    assert!(matches!(
        V5EvolvedProposalDelta::from_value(&value),
        Err(V5EvolvedTransactionError::Contract(_))
    ));
}

#[test]
fn corrected_operator_oracle_corpus_is_the_bounded_transcript_gate() {
    let compressed = include_bytes!(
        "../../../../../tests/fixtures/temporal_qd_v5_operator_python_oracle_corpus.json.gz"
    );
    let mut decoder = GzDecoder::new(compressed.as_slice());
    let mut text = String::new();
    decoder
        .read_to_string(&mut text)
        .expect("inflate checked operator corpus");
    let corpus: Value = serde_json::from_str(&text).expect("parse checked operator corpus");
    assert_eq!(
        corpus["corpusSha256"],
        "sha256:c362bac1a60a879b677a92934a664bb19d4f04d55a64206a21eed9471ba6c96e"
    );
}
