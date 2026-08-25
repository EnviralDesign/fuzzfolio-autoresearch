//! Frozen cross-panel replication operator for the topology case study.

use serde::{Deserialize, Serialize};

pub const SCHEMA: &str = "temporal_qd_topology_replication_survival_result_v1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationSurvivalResult {
    pub schema_version: String,
    pub evidence_complete_and_identity_valid: bool,
    pub development_qualified: bool,
    pub replication_surviving: bool,
    pub inspected_promising: bool,
    pub reporting_category: String,
    pub confirmation_status: String,
}

pub fn evaluate(
    development_panel_3: Option<bool>,
    replication_panel_1: Option<bool>,
    replication_panel_2: Option<bool>,
    identities_valid: bool,
) -> ReplicationSurvivalResult {
    let complete = identities_valid
        && development_panel_3.is_some()
        && replication_panel_1.is_some()
        && replication_panel_2.is_some();
    let development = complete && development_panel_3 == Some(true);
    let replication =
        complete && replication_panel_1 == Some(true) && replication_panel_2 == Some(true);
    let promising = development && replication;
    let category = if !complete {
        "incomplete_invalid"
    } else if promising {
        "inspected_promising_pending_untouched_confirmation"
    } else if development {
        "development_only_not_replicated"
    } else if replication_panel_1 == Some(true) || replication_panel_2 == Some(true) {
        "replication_only_discordant_not_promising"
    } else {
        "complete_no_useful_panel"
    };
    ReplicationSurvivalResult {
        schema_version: SCHEMA.to_owned(),
        evidence_complete_and_identity_valid: complete,
        development_qualified: development,
        replication_surviving: replication,
        inspected_promising: promising,
        reporting_category: category.to_owned(),
        confirmation_status: "pending".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn exact_truth_table_is_strict_all_three() {
        for development in [false, true] {
            for panel_1 in [false, true] {
                for panel_2 in [false, true] {
                    let result = evaluate(Some(development), Some(panel_1), Some(panel_2), true);
                    assert_eq!(
                        result.inspected_promising,
                        development && panel_1 && panel_2
                    );
                    assert_eq!(result.replication_surviving, panel_1 && panel_2);
                }
            }
        }
    }

    #[test]
    fn missing_or_identity_invalid_is_incomplete() {
        for result in [
            evaluate(Some(true), Some(true), None, true),
            evaluate(Some(true), Some(true), Some(true), false),
        ] {
            assert!(!result.inspected_promising);
            assert!(!result.evidence_complete_and_identity_valid);
            assert_eq!(result.reporting_category, "incomplete_invalid");
        }
    }

    #[test]
    fn committed_python_corpus_has_rust_parity() {
        let corpus: Value = serde_json::from_str(include_str!(
            "../../../../../research/temporal-qd/rust-canonical-authority-v2-3/topology-replication-survival-corpus-v1.json"
        ))
        .expect("replication corpus JSON");
        for case in corpus["cases"].as_array().expect("cases") {
            let inputs = &case["inputs"];
            let read = |panel: &str| inputs.get(panel).and_then(Value::as_bool);
            let observed = evaluate(
                read("panel-3"),
                read("panel-1"),
                read("panel-2"),
                case["identitiesValid"].as_bool().expect("identitiesValid"),
            );
            let expected = &case["expected"];
            assert_eq!(observed.inspected_promising, expected["inspectedPromising"]);
            assert_eq!(
                observed.replication_surviving,
                expected["replicationSurviving"]
            );
            assert_eq!(
                observed.development_qualified,
                expected["developmentQualified"]
            );
            assert_eq!(observed.reporting_category, expected["reportingCategory"]);
        }
    }
}
