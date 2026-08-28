from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

from autoresearch.evidence_plan import canonical_json, canonical_sha256
from autoresearch.temporal_qd_v2_6_5_post_result import write_post_result_artifacts


BLOCKS = (
    ("block|qd_001958c8b3288892a458207c9b76|long", "long"),
    ("block|qd_69e5a3407ab21e82d787eb48c8d5|short", "short"),
    ("block|qd_ed27f99ba0a8dfd7c76c69687efb|short", "short"),
)
ARMS = ("P", "T", "E", "TE")
PANELS = ("panel-1", "panel-2", "panel-3")


def _seal(value: dict, field: str) -> dict:
    value[field] = canonical_sha256(value)
    return value


def _profile(side: str, *, added_setup: str | None = None) -> dict:
    event_id = f"{side}_event"
    setup = f"{side}_setup"
    states = [{"id": setup, "timeoutBars": None}]
    transitions = [
        {
            "id": f"{side}_event_entry",
            "sourceStateId": "flat",
            "destinationStateId": setup,
            "priority": 10,
            "reasonCode": "entry",
            "guard": {"kind": "fresh_event", "eventId": event_id},
        }
    ]
    if added_setup:
        states.append({"id": added_setup, "timeoutBars": None})
        transitions.extend(
            [
                {
                    "id": f"{side}_added_in",
                    "sourceStateId": "flat",
                    "destinationStateId": added_setup,
                    "priority": 10,
                    "reasonCode": "setup.advance",
                    "guard": {"kind": "fresh_event", "eventId": event_id},
                },
                {
                    "id": f"{side}_added_out",
                    "sourceStateId": added_setup,
                    "destinationStateId": setup,
                    "priority": 10,
                    "reasonCode": "setup.advance",
                    "guard": {"kind": "fresh_event", "eventId": event_id},
                },
            ]
        )
    return {
        "graph": {
            "states": states,
            "transitions": transitions,
            "eventBindings": [{"id": event_id, "indicatorInstanceId": f"{side}_indicator"}],
        },
        "indicators": [
            {
                "meta": {"instanceId": f"{side}_indicator", "baseIndicatorId": "CHANNEL", "id": "CHANNEL_REENTRY"},
                "config": {"lookbackBars": 1, "timeframe": "M5", "talibConfig": [{"name": "timeperiod", "value": 20}]},
            }
        ],
    }


def _arm(candidate_id: str, net: float, trades: int) -> dict:
    return {
        "candidateId": candidate_id,
        "conservativeNetR": net,
        "tradeCount": trades,
        "costDragR": float(trades) / 20.0,
        "supportEligibility": {"eligible": trades >= 8, "reasonCodes": ["eligible" if trades >= 8 else "minimum_total_trades_failed"]},
        "qualityLaneEligibility": {"eligible": False, "reasonCode": "negative_worst_window_robust_return"},
        "directionSelection": {"eligible": False, "reasonCode": "inactive_or_unsupported"},
    }


def _report(arms: dict[str, str], side: str, *, near_absorbing: bool) -> dict:
    setup = f"{side}_added_setup"
    arm_rows = {
        "P": _arm(arms["P"], -2.0, 30),
        "T": _arm(arms["T"], -1.0, 28),
        "E": _arm(arms["E"], 1.0, 1),
        "TE": _arm(arms["TE"], 0.0, 0 if near_absorbing else 8),
    }
    mechanism = {}
    for arm in ARMS:
        mechanism[arm] = {
            "addedSetupNodeOccupancy": {"status": "available", "values": {setup: 0.999 if near_absorbing and arm == "TE" else 0.2}},
            "eventSpecificActivation": {"status": "unavailable", "reason": "not retained"},
            "entrySequenceComparison": {"status": "available", "candidateEntrySequence": [{"entry": 1}], "parentEntrySequence": [{"entry": 2}]},
        }
    mechanism["addedSetupNodeIds"] = [setup]
    return {"arms": arm_rows, "mechanism": mechanism, "usefulProgressiveInnovationV2": False}


def _write_sealed_zip(path: Path) -> None:
    population = {"candidates": []}
    blocks = {}
    for block_id, side in BLOCKS:
        arms = {arm: f"{block_id}|{arm}" for arm in ARMS}
        for arm, candidate_id in arms.items():
            population["candidates"].append(
                {"candidateId": candidate_id, "sourceProfile": _profile(side, added_setup=f"{side}_added_setup" if arm == "TE" else None)}
            )
        blocks[block_id] = {
            "changedSide": side,
            "arms": arms,
            "panelReports": {panel: _report(arms, side, near_absorbing=side == "short") for panel in PANELS},
        }
    analysis = _seal(
        {
            "schemaVersion": "temporal_qd_topology_production_analysis_v3",
            "status": "complete",
            "originalScientificContractSha256": "sha256:scientific",
            "originalReplicationRuleSha256": "sha256:replication",
            "untouchedConfirmationStatus": "pending",
            "blocks": blocks,
        },
        "analysisSha256",
    )
    validation = _seal(
        {"status": "complete", "analysisSha256": analysis["analysisSha256"], "byteIdenticalSecondPass": True},
        "reducerValidationSha256",
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("salvage/reducer/pass-1.json", canonical_json(analysis))
        archive.writestr("salvage/reducer/reducer-validation.json", canonical_json(validation))
        archive.writestr("sealed-run/inputs/panel-1/cohort-population.json", canonical_json(population))


def _hashes(path: Path) -> dict[str, str]:
    return {item.name: hashlib.sha256(item.read_bytes()).hexdigest() for item in path.iterdir() if item.is_file()}


def test_post_result_artifacts_are_deterministic_and_terminal(tmp_path: Path) -> None:
    sealed = tmp_path / "sealed.zip"
    _write_sealed_zip(sealed)
    first = tmp_path / "first"
    second = tmp_path / "second"
    write_post_result_artifacts(sealed_zip=sealed, output_dir=first)
    write_post_result_artifacts(sealed_zip=sealed, output_dir=second)
    assert _hashes(first) == _hashes(second)
    terminal = json.loads((first / "terminal-inspected-decision.json").read_text(encoding="utf-8"))
    assert terminal["confirmationExecutionAuthorized"] is False
    assert terminal["generationAuthorized"] is False
    assert terminal["completeBlockCount"] == 3
    forensic = json.loads((first / "topology-mechanism-forensic.json").read_text(encoding="utf-8"))
    short_blocks = [item for item in forensic["blocks"] if item["changedSide"] == "short"]
    assert all("setup_state_near_absorbing" in item["labels"] for item in short_blocks)
    rendered = canonical_json(forensic)
    assert '"candidateEntrySequence":' not in rendered
    assert "candidateEntrySequenceSha256" in rendered
