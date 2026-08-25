"""Pre-results cross-panel replication authority for the topology case study."""

from __future__ import annotations

from collections.abc import Mapping
import argparse
import json
from pathlib import Path
from typing import Any

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_discovery_base import TemporalDiscoveryContractError

SCHEMA = "temporal_qd_topology_replication_survival_rule_v1"
OUTPUT_SCHEMA = "temporal_qd_topology_replication_survival_result_v1"
PANELS = ("panel-3", "panel-1", "panel-2")


def build_replication_survival_rule(*, scientific_contract_sha256: str) -> dict[str, Any]:
    rule: dict[str, Any] = {
        "schemaVersion": SCHEMA,
        "scientificContractSha256": scientific_contract_sha256,
        "developmentPanelId": "panel-3",
        "replicationPanelIds": ["panel-1", "panel-2"],
        "panelLocalPredicate": "usefulProgressiveInnovation",
        "crossPanelOperator": "all",
        "requiredPanelOrder": list(PANELS),
        "poolingPermitted": False,
        "compensationPermitted": False,
        "missingOrInvalidDisposition": "incomplete_invalid",
        "untouchedConfirmation": {
            "statusBeforeExecution": "pending",
            "predicate": "inspectedPromising_and_panel_local_usefulProgressiveInnovation",
            "poolingPermitted": False,
            "mayRescueInspectedFailure": False,
        },
        "reportingCategories": [
            "inspected_promising_pending_untouched_confirmation",
            "development_only_not_replicated",
            "replication_only_discordant_not_promising",
            "mixed_panel_nonqualifying",
            "complete_no_useful_panel",
            "incomplete_invalid",
        ],
    }
    rule["replicationRuleSha256"] = canonical_sha256(rule)
    return rule


def validate_replication_survival_rule(rule: Mapping[str, Any]) -> None:
    unsigned = dict(rule)
    stored = unsigned.pop("replicationRuleSha256", None)
    if stored != canonical_sha256(unsigned):
        raise TemporalDiscoveryContractError("replication survival rule self-hash mismatch")
    expected = build_replication_survival_rule(
        scientific_contract_sha256=str(rule.get("scientificContractSha256"))
    )
    if dict(rule) != expected:
        raise TemporalDiscoveryContractError("replication survival rule is incompatible")


def evaluate_replication_survival(
    panel_useful: Mapping[str, bool | None], *, identities_valid: bool = True
) -> dict[str, Any]:
    values = {panel: panel_useful.get(panel) for panel in PANELS}
    complete = identities_valid and all(type(values[panel]) is bool for panel in PANELS)
    if not complete:
        promising = False
        category = "incomplete_invalid"
    else:
        development = bool(values["panel-3"])
        replication = bool(values["panel-1"]) and bool(values["panel-2"])
        promising = development and replication
        useful_count = sum(bool(values[panel]) for panel in PANELS)
        if promising:
            category = "inspected_promising_pending_untouched_confirmation"
        elif development and not replication:
            category = "development_only_not_replicated"
        elif not development and useful_count:
            category = "replication_only_discordant_not_promising"
        elif useful_count == 0:
            category = "complete_no_useful_panel"
        else:
            category = "mixed_panel_nonqualifying"
    result: dict[str, Any] = {
        "schemaVersion": OUTPUT_SCHEMA,
        "panelUsefulProgressiveInnovation": values,
        "evidenceCompleteAndIdentityValid": complete,
        "developmentQualified": bool(values["panel-3"]) if complete else False,
        "replicationSurviving": (
            bool(values["panel-1"]) and bool(values["panel-2"]) if complete else False
        ),
        "inspectedPromising": promising,
        "reportingCategory": category,
        "confirmationStatus": "pending",
    }
    result["resultSha256"] = canonical_sha256(result)
    return result


def build_truth_table_corpus(*, replication_rule_sha256: str) -> dict[str, Any]:
    cases: list[dict[str, Any]] = []
    for development in (False, True):
        for panel_1 in (False, True):
            for panel_2 in (False, True):
                inputs = {
                    "panel-3": development,
                    "panel-1": panel_1,
                    "panel-2": panel_2,
                }
                cases.append({"inputs": inputs, "identitiesValid": True, "expected": evaluate_replication_survival(inputs)})
    for case_id, inputs, identities_valid in (
        ("missing-panel-2", {"panel-3": True, "panel-1": True}, True),
        ("null-panel-1", {"panel-3": True, "panel-1": None, "panel-2": True}, True),
        ("identity-drift", {"panel-3": True, "panel-1": True, "panel-2": True}, False),
    ):
        cases.append(
            {
                "caseId": case_id,
                "inputs": inputs,
                "identitiesValid": identities_valid,
                "expected": evaluate_replication_survival(inputs, identities_valid=identities_valid),
            }
        )
    corpus: dict[str, Any] = {
        "schemaVersion": "temporal_qd_topology_replication_survival_corpus_v1",
        "replicationRuleSha256": replication_rule_sha256,
        "cases": cases,
        "prohibitedTransformations": [
            "cross_panel_net_pooling",
            "cross_panel_worst_window_pooling",
            "any_or_majority_operator",
            "missing_as_observed_failure",
            "untouched_confirmation_rescues_inspected_failure",
        ],
    }
    corpus["corpusSha256"] = canonical_sha256(corpus)
    return corpus


def write_authority(*, output_root: Path, scientific_contract_path: Path) -> None:
    scientific = json.loads(scientific_contract_path.read_text(encoding="utf-8"))
    rule = build_replication_survival_rule(
        scientific_contract_sha256=str(scientific["scientificContractSha256"])
    )
    corpus = build_truth_table_corpus(
        replication_rule_sha256=str(rule["replicationRuleSha256"])
    )
    output_root.mkdir(parents=True, exist_ok=True)
    for name, value in (
        ("topology-replication-survival-rule-v1.json", rule),
        ("topology-replication-survival-corpus-v1.json", corpus),
    ):
        (output_root / name).write_text(canonical_json(value) + "\n", encoding="utf-8", newline="\n")


def _main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--scientific-contract", type=Path, required=True)
    args = parser.parse_args()
    write_authority(output_root=args.output_root, scientific_contract_path=args.scientific_contract)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())


__all__ = [
    "OUTPUT_SCHEMA",
    "PANELS",
    "SCHEMA",
    "build_replication_survival_rule",
    "build_truth_table_corpus",
    "evaluate_replication_survival",
    "validate_replication_survival_rule",
    "write_authority",
]
