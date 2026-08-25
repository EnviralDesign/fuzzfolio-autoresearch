"""Deterministic, preregistered reducer for the topology P/T/E/TE study."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping

from .evidence_plan import canonical_json, canonical_sha256
from .temporal_qd_topology_coadaptation_v7 import promising_coadaptation_observation
from .temporal_qd_topology_replication_survival_v1 import evaluate_replication_survival

SCHEMA = "temporal_qd_topology_post_run_analysis_v1"
CONTRACT_SCHEMA = "temporal_qd_topology_post_run_analyzer_contract_v1"
ARMS = ("P", "T", "E", "TE")


def _greater(left: float, right: float) -> bool:
    return float(left) - float(right) > 1e-12


def _not_worse(left: float, right: float) -> bool:
    return float(left) - float(right) >= -1e-12


def evaluate_panel(arms: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    if set(arms) != set(ARMS):
        raise ValueError("panel must contain the exact P/T/E/TE block")
    required = ("conservativeNetR", "worstWindowConservativeNetR", "tradeCount", "costDragR", "support", "direction", "quality")
    if any(any(key not in arms[arm] for key in required) for arm in ARMS):
        raise ValueError("panel arm metrics are incomplete")
    for arm in ARMS:
        for key in ("conservativeNetR", "worstWindowConservativeNetR", "costDragR"):
            value = arms[arm][key]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                raise ValueError(f"{arm} {key} must be finite numeric evidence")
        trades = arms[arm]["tradeCount"]
        if isinstance(trades, bool) or not isinstance(trades, int) or trades < 0:
            raise ValueError(f"{arm} tradeCount must be a nonnegative integer")
        for gate in ("support", "direction", "quality"):
            if type(arms[arm][gate]) is not bool:
                raise ValueError(f"{arm} {gate} must be a Boolean gate")
    observation = promising_coadaptation_observation(
        parent_net=float(arms["P"]["conservativeNetR"]),
        topology_net=float(arms["T"]["conservativeNetR"]),
        event_net=float(arms["E"]["conservativeNetR"]),
        combined_net=float(arms["TE"]["conservativeNetR"]),
        parent_worst=float(arms["P"]["worstWindowConservativeNetR"]),
        topology_worst=float(arms["T"]["worstWindowConservativeNetR"]),
        event_worst=float(arms["E"]["worstWindowConservativeNetR"]),
        combined_worst=float(arms["TE"]["worstWindowConservativeNetR"]),
        metric_greater=_greater,
        metric_not_worse=_not_worse,
    )
    gates = all(bool(arms[arm][gate]) for arm in ARMS for gate in ("support", "direction", "quality"))
    useful = bool(observation["usefulProgressiveInnovation"] and gates)
    te = float(arms["TE"]["conservativeNetR"])
    return {
        "arms": {arm: dict(arms[arm]) for arm in ARMS},
        "teMinusP": te - float(arms["P"]["conservativeNetR"]),
        "teMinusT": te - float(arms["T"]["conservativeNetR"]),
        "teMinusE": te - float(arms["E"]["conservativeNetR"]),
        "signedInteraction": observation["interactionNetR"],
        "supportDirectionQualityPassed": gates,
        "nonqualifyingRiskTradeoff": observation["nonqualifyingRiskTradeoff"],
        "usefulProgressiveInnovation": useful,
    }


def analyze_block(*, block_id: str, panels: Mapping[str, Mapping[str, Mapping[str, Any]]], identities_valid: bool = True) -> dict[str, Any]:
    panel_reports: dict[str, Any] = {}
    valid = identities_valid and set(panels) == {"panel-1", "panel-2", "panel-3"}
    for panel_id in ("panel-3", "panel-1", "panel-2"):
        try:
            panel_reports[panel_id] = evaluate_panel(panels[panel_id])
        except (KeyError, TypeError, ValueError):
            valid = False
    survival = evaluate_replication_survival(
        {panel: panel_reports.get(panel, {}).get("usefulProgressiveInnovation") for panel in ("panel-3", "panel-1", "panel-2")},
        identities_valid=valid,
    )
    result: dict[str, Any] = {
        "schemaVersion": SCHEMA,
        "blockId": block_id,
        "panelReports": panel_reports,
        "replication": survival,
        "familyLevelInferencePermitted": False,
        "productionConfirmed": False,
        "confirmationStatus": "pending",
    }
    result["analysisSha256"] = canonical_sha256(result)
    return result


def write_contract(*, output_path: Path, scientific_contract_sha256: str, replication_rule_sha256: str) -> None:
    source_path = Path(__file__).resolve()
    contract: dict[str, Any] = {
        "schemaVersion": CONTRACT_SCHEMA,
        "analysisSchema": SCHEMA,
        "sourcePath": "autoresearch/temporal_qd_topology_post_run_analyzer_v1.py",
        "sourceRawSha256": "sha256:" + hashlib.sha256(source_path.read_bytes()).hexdigest(),
        "scientificContractSha256": scientific_contract_sha256,
        "replicationRuleSha256": replication_rule_sha256,
        "metricEquality": "canonical_json_number_roundtrip_with_1e-12_encoding_floor",
        "fixedPnlMarginPermitted": False,
        "familyLevelInferencePermitted": False,
        "untouchedConfirmationRequired": True,
    }
    contract["analyzerContractSha256"] = canonical_sha256(contract)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(canonical_json(contract) + "\n", encoding="utf-8", newline="\n")


def analyze_file(*, input_path: Path, output_path: Path) -> dict[str, Any]:
    value = json.loads(input_path.read_text(encoding="utf-8"))
    if set(value) != {"schemaVersion", "blockId", "panels", "identitiesValid"}:
        raise ValueError("post-run analyzer input field set is incompatible")
    if value["schemaVersion"] != "temporal_qd_topology_post_run_input_v1":
        raise ValueError("post-run analyzer input schema is incompatible")
    result = analyze_block(
        block_id=str(value["blockId"]),
        panels=value["panels"],
        identities_valid=value["identitiesValid"],
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(canonical_json(result) + "\n", encoding="utf-8", newline="\n")
    return result


def _main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-contract", type=Path)
    parser.add_argument("--scientific-contract-sha256")
    parser.add_argument("--replication-rule-sha256")
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.write_contract:
        write_contract(
            output_path=args.write_contract,
            scientific_contract_sha256=args.scientific_contract_sha256,
            replication_rule_sha256=args.replication_rule_sha256,
        )
    if args.input or args.output:
        if args.input is None or args.output is None:
            parser.error("--input and --output must be supplied together")
        analyze_file(input_path=args.input, output_path=args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
