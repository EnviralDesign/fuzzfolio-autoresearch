use std::collections::BTreeMap;

use serde_json::json;
use temporal_qd_contract::Value;
use temporal_qd_kernel::{
    factory::{ParentReference, ProposalIntent},
    operator_family_matrix::{MATRIX_SCHEMA, OperatorFamilyMatrixContract},
    proposal::{
        ExplicitParentRing, ParentSelector, ProposalPlanner, ProposalSchedule, ProposalState,
    },
};

fn parent(candidate_id: &str, digest: &str) -> ParentReference {
    ParentReference {
        pair_identity_sha256: format!("sha256:{digest}"),
        candidate_id: candidate_id.to_owned(),
        pair_payload: json!({"schemaVersion": "test-parent-payload"}),
        selection_audit: None,
    }
}

fn contract_value() -> Value {
    json!({
        "schemaVersion": MATRIX_SCHEMA,
        "mode": "frozen_parent_one_change_v1",
        "includeCrossover": false,
        "cloneControl": "re_evaluate_parent_on_frozen_panel",
        "mutationDepth": 1,
        "childrenPerFamily": 2,
        "families": ["hold", "resource"],
        "parents": [
            {"candidateId": "parent_archive_a", "role": "archive"},
            {"candidateId": "parent_inactive", "role": "inactive_control"},
            {"candidateId": "parent_negative", "role": "active_negative_control"}
        ],
        "constructionSlotCount": 12,
        "cloneParentCandidateIds": [
            "parent_archive_a",
            "parent_inactive",
            "parent_negative"
        ]
    })
}

#[test]
fn construction_slots_match_python_order() {
    let matrix = OperatorFamilyMatrixContract::from_value(&contract_value()).unwrap();
    assert_eq!(matrix.construction_slot_count().unwrap(), 12);
    let first = matrix.slot_at(0).unwrap().unwrap();
    assert_eq!(first.parent_candidate_id, "parent_archive_a");
    assert_eq!(first.operator_family, "hold");
    assert_eq!(first.child_index, 0);
    let resource = matrix.slot_at(3).unwrap().unwrap();
    assert_eq!(resource.operator_family, "resource");
    assert_eq!(resource.parent_candidate_id, "parent_archive_a");
    let next_parent = matrix.slot_at(4).unwrap().unwrap();
    assert_eq!(next_parent.parent_candidate_id, "parent_inactive");
    assert_eq!(next_parent.operator_family, "hold");
    assert!(matrix.slot_at(12).unwrap().is_none());
}

#[test]
fn rejects_crossover_and_mixed_depth() {
    let mut crossover = contract_value();
    crossover
        .as_object_mut()
        .unwrap()
        .insert("includeCrossover".to_owned(), Value::Bool(true));
    assert!(OperatorFamilyMatrixContract::from_value(&crossover).is_err());
    let mut depth = contract_value();
    depth
        .as_object_mut()
        .unwrap()
        .insert("mutationDepth".to_owned(), Value::from(2_u64));
    assert!(OperatorFamilyMatrixContract::from_value(&depth).is_err());
}

#[test]
fn default_five_parent_matrix_is_800_slots() {
    let value = json!({
        "schemaVersion": MATRIX_SCHEMA,
        "mode": "frozen_parent_one_change_v1",
        "includeCrossover": false,
        "cloneControl": "re_evaluate_parent_on_frozen_panel",
        "mutationDepth": 1,
        "childrenPerFamily": 32,
        "families": ["hold", "resource", "topology", "temporal", "initial_protection"],
        "parents": [
            {"candidateId": "p0", "role": "archive"},
            {"candidateId": "p1", "role": "archive"},
            {"candidateId": "p2", "role": "archive"},
            {"candidateId": "inactive", "role": "inactive_control"},
            {"candidateId": "negative", "role": "active_negative_control"}
        ]
    });
    let matrix = OperatorFamilyMatrixContract::from_value(&value).unwrap();
    assert_eq!(matrix.construction_slot_count().unwrap(), 5 * 5 * 32);
}

#[test]
fn planner_uses_forced_family_only_when_matrix_is_present() {
    let archive = parent(
        "parent_archive_a",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    let inactive = parent(
        "parent_inactive",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );
    let negative = parent(
        "parent_negative",
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    );
    let matrix = OperatorFamilyMatrixContract::from_value(&contract_value()).unwrap();
    let mut parents_by_id = BTreeMap::new();
    parents_by_id.insert(archive.candidate_id.clone(), archive.clone());
    parents_by_id.insert(inactive.candidate_id.clone(), inactive.clone());
    parents_by_id.insert(negative.candidate_id.clone(), negative.clone());
    let mut selector =
        ExplicitParentRing::new(vec![archive.clone(), inactive.clone(), negative.clone()])
            .unwrap();
    let schedule = ProposalSchedule {
        config_sha256: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
            .to_owned(),
        generation_index: 2,
        parent_schedule: None,
        desired_evaluated_offspring: 12,
        desired_evaluated_immigrants: 0,
        operator_family_matrix: Some(matrix),
        matrix_parents: parents_by_id,
    };
    let mut planner = ProposalPlanner {
        schedule,
        parents: &mut selector,
    };
    let mut state = ProposalState::default();
    let planned = planner.plan_next(&mut state).unwrap();
    match planned.intent {
        ProposalIntent::StructuralMutation {
            mutation_depth,
            forced_operator_family,
            parent,
            ..
        } => {
            assert_eq!(mutation_depth, 1);
            assert_eq!(forced_operator_family.as_deref(), Some("hold"));
            assert_eq!(parent.candidate_id, "parent_archive_a");
        }
        other => panic!("matrix planner produced {other:?}"),
    }
    assert_eq!(state.structural_parent_selections, 1);

    let mut production_selector =
        ExplicitParentRing::new(vec![archive, inactive, negative]).unwrap();
    let production_schedule = ProposalSchedule {
        config_sha256: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
            .to_owned(),
        generation_index: 2,
        parent_schedule: Some(
            temporal_qd_kernel::schedule::RotatingParentSchedule::from_counts(3, 3).unwrap(),
        ),
        desired_evaluated_offspring: 0,
        desired_evaluated_immigrants: 1,
        operator_family_matrix: None,
        matrix_parents: BTreeMap::new(),
    };
    let mut production_planner = ProposalPlanner {
        schedule: production_schedule,
        parents: &mut production_selector,
    };
    let mut production_state = ProposalState::default();
    let production = production_planner.plan_next(&mut production_state).unwrap();
    assert!(
        matches!(production.intent, ProposalIntent::RichImmigrant { .. }),
        "production planner without a matrix must keep immigrant quota scheduling"
    );
    assert_eq!(production_state.structural_parent_selections, 0);
}

#[test]
fn matrix_planner_credits_one_structural_binding_per_slot_without_selector_draws() {
    let archive = parent(
        "parent_archive_a",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    let inactive = parent(
        "parent_inactive",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );
    let negative = parent(
        "parent_negative",
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    );
    let matrix = OperatorFamilyMatrixContract::from_value(&contract_value()).unwrap();
    let mut parents_by_id = BTreeMap::new();
    parents_by_id.insert(archive.candidate_id.clone(), archive.clone());
    parents_by_id.insert(inactive.candidate_id.clone(), inactive.clone());
    parents_by_id.insert(negative.candidate_id.clone(), negative.clone());
    let mut selector =
        ExplicitParentRing::new(vec![archive, inactive, negative]).unwrap();
    let initial_selector = selector.compact_state();
    let schedule = ProposalSchedule {
        config_sha256: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
            .to_owned(),
        generation_index: 2,
        parent_schedule: None,
        desired_evaluated_offspring: 12,
        desired_evaluated_immigrants: 0,
        operator_family_matrix: Some(matrix),
        matrix_parents: parents_by_id,
    };
    let mut planner = ProposalPlanner {
        schedule,
        parents: &mut selector,
    };
    let mut state = ProposalState::default();
    for expected_ordinal in 0..12 {
        let planned = planner.plan_next(&mut state).unwrap();
        assert_eq!(planned.proposal_ordinal, expected_ordinal);
        assert_eq!(state.structural_parent_selections, expected_ordinal + 1);
        // The planner does not advance the ordinal; execute observes that
        // after the attempt is committed.
        state.next_proposal_ordinal = expected_ordinal + 1;
    }
    assert!(planner.plan_next(&mut state).is_err());
    assert_eq!(state.structural_parent_selections, 12);
    assert_eq!(planner.parents.compact_state(), initial_selector);
}

#[test]
fn exhausted_slot_grid_allows_an_accept_deficit() {
    let matrix = OperatorFamilyMatrixContract::from_value(&contract_value()).unwrap();
    assert_eq!(matrix.construction_slot_count().unwrap(), 12);
    matrix
        .require_exhausted_slot_grid(12, 7, 12, 12)
        .expect("every slot attempted is publishable even with rejects");
    matrix
        .require_exhausted_slot_grid(12, 0, 12, 12)
        .expect("zero accepts after a full slot grid is still Outcome B");
    assert!(matrix.require_exhausted_slot_grid(11, 7, 12, 12).is_err());
    assert!(matrix.require_exhausted_slot_grid(12, 7, 12, 13).is_err());
    assert!(matrix.require_exhausted_slot_grid(12, 13, 12, 12).is_err());
}
