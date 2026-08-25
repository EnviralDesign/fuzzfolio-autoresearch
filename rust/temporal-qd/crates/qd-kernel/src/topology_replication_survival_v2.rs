//! Exact shared semantic projection over the frozen V1 replication Boolean.

use serde_json::{json, Value};
use temporal_qd_contract::canonical_sha256;

pub const SCHEMA: &str = "temporal_qd_topology_replication_survival_projection_v2";

pub fn evaluate(
    development_panel_3: Option<bool>,
    replication_panel_1: Option<bool>,
    replication_panel_2: Option<bool>,
    identities_valid: bool,
) -> Value {
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
    } else if development
        && replication_panel_1 == Some(false)
        && replication_panel_2 == Some(false)
    {
        "development_only_not_replicated"
    } else if development {
        "mixed_panel_nonqualifying"
    } else if replication_panel_1 == Some(true) || replication_panel_2 == Some(true) {
        "replication_only_discordant_not_promising"
    } else {
        "complete_no_useful_panel"
    };
    let mut result = json!({
        "schemaVersion": SCHEMA,
        "panelUsefulProgressiveInnovation": {
            "panel-3": development_panel_3,
            "panel-1": replication_panel_1,
            "panel-2": replication_panel_2,
        },
        "evidenceCompleteAndIdentityValid": complete,
        "developmentQualified": development,
        "replicationSurviving": replication,
        "inspectedPromising": promising,
        "reportingCategory": category,
        "confirmationStatus": "pending",
    });
    let hash = canonical_sha256(&result).expect("replication projection is canonical");
    result
        .as_object_mut()
        .expect("replication projection object")
        .insert("projectionSha256".into(), Value::String(hash));
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn strict_boolean_is_preserved_and_every_reporting_category_is_reachable() {
        let mut categories = BTreeSet::new();
        for development in [false, true] {
            for panel_1 in [false, true] {
                for panel_2 in [false, true] {
                    let result = evaluate(Some(development), Some(panel_1), Some(panel_2), true);
                    assert_eq!(
                        result["inspectedPromising"],
                        Value::Bool(development && panel_1 && panel_2)
                    );
                    categories.insert(result["reportingCategory"].as_str().unwrap().to_owned());
                }
            }
        }
        categories.insert(
            evaluate(Some(true), Some(true), None, true)["reportingCategory"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
        assert_eq!(
            categories,
            BTreeSet::from([
                "complete_no_useful_panel".to_owned(),
                "development_only_not_replicated".to_owned(),
                "incomplete_invalid".to_owned(),
                "inspected_promising_pending_untouched_confirmation".to_owned(),
                "mixed_panel_nonqualifying".to_owned(),
                "replication_only_discordant_not_promising".to_owned(),
            ])
        );
    }

    #[test]
    fn python_and_rust_emit_the_exact_same_canonical_projection() {
        let corpus: Value = serde_json::from_str(include_str!(
            "../../../../../research/temporal-qd/rust-canonical-authority-v2-4/topology-replication-parity-corpus-v2.json"
        ))
        .expect("V2 parity corpus JSON");
        for case in corpus["cases"].as_array().expect("cases") {
            let inputs = &case["inputs"];
            let read = |panel: &str| inputs.get(panel).and_then(Value::as_bool);
            assert_eq!(
                evaluate(
                    read("panel-3"),
                    read("panel-1"),
                    read("panel-2"),
                    case["identitiesValid"].as_bool().expect("identitiesValid"),
                ),
                case["expected"]
            );
        }
    }
}
