"""Generate a tiny tail-reduction fixture from the production Python oracle.

This is intentionally test-only. It freezes the current Python component
semantics into portable compact inputs and one expected semantic output.
"""

from __future__ import annotations

import base64
import gzip
import json
import sys
from pathlib import Path

from autoresearch.result_codec import canonical_json_bytes, semantic_sha256, sha256
from autoresearch.temporal_discovery_results import _aggregate_candidate, _result_set_sha256
from autoresearch.temporal_qd_evolution import (
    _finite_data_validity,
    _objective_row,
    qd_behavior_descriptor,
)
from autoresearch.temporal_qd_rotating_evidence import reduce_provisional_diverse_survivors


def digest(label: str) -> str:
    import hashlib

    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def candidate(name: str, *, operators: list[str], duplicate_break_even: bool = False) -> dict:
    actions = [{"kind": "move_stop_to_break_even_next_open"}]
    if duplicate_break_even:
        actions.append({"kind": "move_stop_to_break_even_next_open"})
    profile = {
        "directionMode": "long",
        "indicators": [{"indicatorId": "ema"}],
        "executionConfig": {"managementLibrary": {"plans": [{"planId": "base"}]}},
        "graph": {
            "states": [{"stateId": "flat"}, {"stateId": "long"}],
            "transitions": [
                {
                    "sourceStateId": "flat",
                    "guard": {"kind": "fresh_event", "eventId": "cross"},
                    "actions": [{"kind": "enter_next_open"}],
                },
                {"sourceStateId": "long", "guard": {"kind": "always"}, "actions": actions},
            ],
        },
    }
    profile_sha = semantic_sha256(profile)
    return {
        "candidateId": name,
        "candidateIdentitySha256": digest(name + ":identity"),
        "profileSnapshotSha256": digest(name + ":snapshot"),
        "programSha256": digest(name + ":authored-program"),
        "proposalEntrySha256": digest(name + ":entry"),
        "proposalOrdinal": int(name.rsplit("_", 1)[-1]),
        "seedId": "seed",
        "sourceMode": "mutation" if operators else "reference",
        "sourceProfile": profile,
        "sourceProfileSha256": profile_sha,
        "structuralOperatorHistory": [{"operatorId": value} for value in operators],
    }


def window(row: dict, index: int, net: float, trades: int) -> dict:
    start = f"2024-0{index + 1}-01T00:00:00Z"
    end = f"2024-0{index + 2}-01T00:00:00Z"
    resolved_program = digest(row["candidateId"] + ":resolved-program")
    terminal = {
        "terminalPolicy": "mark_to_market",
        "terminalPolicySchemaVersion": "v1",
        "terminalLastCompletedBarId": index + 100,
        "terminalLastCompletedBarStart": start,
        "terminalLastCompletedBarClose": 101.25 + index,
        "terminalPositionStatus": "flat",
        "terminalPendingEffectStatus": "none",
        "terminalMarkPrice": 101.25 + index,
        "terminalGrossR": 0.25,
        "terminalNetR": 0.2,
        "terminalExitCostPercent": 0.01,
        "terminalAdjustedMaxDrawdownR": abs(net) / 2.0,
    }
    return {
        "candidateId": row["candidateId"],
        "windowId": f"{start}/{end}",
        "analysisWindowStart": start,
        "analysisWindowEnd": end,
        "economicsBasis": "stage5e7_v3_terminal_adjusted",
        "v3Admissible": True,
        "evaluationRejected": False,
        "programSha256": resolved_program,
        "resolvedProgramSha256": resolved_program,
        "resolvedProfileSnapshotSha256": digest(row["candidateId"] + ":resolved-profile"),
        "sourceProfileSnapshotSha256": row["profileSnapshotSha256"],
        "trades": trades,
        "observations": 1000 + index,
        "conservativeNetR": net,
        "noCostNetR": net + 0.2,
        "maxDrawdownR": abs(net) / 2.0,
        "rawClosedConservativeNetR": net - 0.1,
        "rawClosedNoCostNetR": net + 0.1,
        "rawClosedMaxDrawdownR": abs(net) / 2.0 + 0.1,
        "terminalAdjustedConservativeNetR": net,
        "terminalAdjustedNoCostNetR": net + 0.2,
        "terminalAdjustedCostViewDeltaR": 0.2,
        "averageHoldingBars": 30.0 + index,
        "holdingBars": [20 + index, 40 + index],
        "winRate": 0.5,
        "exposureRatio": 0.25,
        "transitionEntropy": 0.75,
        "averageMfeR": 0.4,
        "averageMaeR": -0.2,
        "equityCurveR": [0.0, net / 2.0, net],
        "actionCounts": {"enter": trades, "hold": 2},
        "closeReasonCounts": {"signal": trades},
        "stateOccupancy": {"flat": 500, "long": 500 + index},
        "transitionCounts": {"flat->long": trades},
        "entryHourCounts": {"8": trades},
        "conservativeTerminal": terminal,
        "noCostTerminal": {**terminal, "terminalNetR": 0.25},
        "evidenceContractEndpoints": {"start": start, "end": end},
    }


def rejection_window(row: dict, index: int) -> dict:
    start = f"2024-0{index + 1}-01T00:00:00Z"
    end = f"2024-0{index + 2}-01T00:00:00Z"
    rejection = {
        "schema_version": "temporal_candidate_window_rejection_v1",
        "disposition": "rejected",
        "reason_code": "aligned_scoring_warmup_insufficient",
        "replay_executed": False,
        "worker_attempt_id": "attempt",
        "worker_lease_id": "lease",
        "worker_error": None,
        "worker_error_sha256": digest("worker-error"),
        "worker_completion_sha256": digest("worker-completion"),
    }
    return {
        "economicsBasis": "not_evaluated_aligned_scoring_warmup_insufficient",
        "v3Admissible": False,
        "evaluationRejected": True,
        "rejection": rejection,
        "candidateId": row["candidateId"],
        "windowId": f"{start}/{end}",
        "analysisWindowStart": start,
        "analysisWindowEnd": end,
    }


def projection(record: dict) -> dict:
    semantic = canonical_json_bytes(record)
    return {
        "schemaVersion": "temporal_qd_tail_stage_projection_v1",
        "codec": "gzip-canonical-json-v1",
        "semanticSha256": sha256(semantic),
        "semanticSizeBytes": len(semantic),
        "blobBase64": base64.b64encode(gzip.compress(semantic, compresslevel=9, mtime=0)).decode(),
    }


def entry(task_id: str, record: dict) -> dict:
    task = {
        "taskId": task_id,
        "candidateId": record["candidateId"],
        "analysisWindowStart": record["analysisWindowStart"],
        "analysisWindowEnd": record["analysisWindowEnd"],
        "evidencePlanSemanticSha256": digest("plan"),
        "taskPayloadSha256": digest(task_id + ":payload"),
    }
    raw = {
        "schemaVersion": "temporal_qd_tail_result_index_entry_v3",
        "task": task,
        "rawResultRef": {
            "schemaVersion": "temporal_qd_tail_raw_result_ref_v1",
            "relativePath": f"results/{task_id}.json.gz",
            "codec": "gzip-json-v1",
            "resultSha256": digest(task_id + ":result"),
            "semanticSizeBytes": 1,
            "uncompressedSha256": digest(task_id + ":uncompressed"),
            "uncompressedSizeBytes": 1,
            "blobSha256": digest(task_id + ":blob"),
            "blobSizeBytes": 1,
        },
        "rawTaskProvenance": {"taskId": task_id, "resultSha256": digest(task_id + ":result")},
    }
    if record.get("evaluationRejected"):
        raw["rejection"] = record["rejection"]
    else:
        raw["stageProjection"] = projection(record)
        raw["rotatingEvidenceMetrics"] = {
            "conservativeNetR": record["conservativeNetR"],
            "noCostNetR": record["noCostNetR"],
            "maxDrawdownR": record["maxDrawdownR"],
            "closedTrades": record["trades"],
            "observations": record["observations"],
            "v3Admissible": True,
            "resolvedProgramSha256": record["resolvedProgramSha256"],
            "resolvedProfileSnapshotSha256": record["resolvedProfileSnapshotSha256"],
            "sourceProfileSnapshotSha256": record["sourceProfileSnapshotSha256"],
        }
    raw["entrySha256"] = semantic_sha256(raw)
    return raw


def main() -> None:
    out = Path(sys.argv[1])
    rows = [
        candidate("qd_fixture_1", operators=[]),
        candidate("qd_fixture_2", operators=[]),
        candidate("qd_fixture_3", operators=["op_a", "op_b"]),
        candidate("qd_fixture_4", operators=[]),
        candidate("qd_fixture_5", operators=[], duplicate_break_even=True),
    ]
    grouped = {
        rows[0]["candidateId"]: [window(rows[0], 1, 1.0, 5), window(rows[0], 2, 0.5, 5)],
        rows[1]["candidateId"]: [window(rows[1], 1, 2.0, 6), window(rows[1], 2, 1.0, 6)],
        rows[2]["candidateId"]: [window(rows[2], 1, -0.25, 4), window(rows[2], 2, 0.25, 4)],
        rows[3]["candidateId"]: [rejection_window(rows[3], 1), rejection_window(rows[3], 2)],
        rows[4]["candidateId"]: [window(rows[4], 1, 5.0, 10), window(rows[4], 2, 5.0, 10)],
    }
    evaluation = {
        "schemaVersion": "temporal_qd_evaluation_population_v1",
        "generationIndex": 7,
        "populationSha256": digest("population"),
        "populationFileSha256": digest("population-file"),
        "pairGenerationConfigSha256": digest("config"),
        "policyName": "stage5e7_v3_robust_quality_archive",
        "policySha256": digest("policy"),
        "pairPolicySha256": semantic_sha256({}),
        "bidirectionalPairPolicy": {},
        "operatorImplementationSha256": digest("operators"),
        "predeclaredEvidenceContextSha256": None,
        "g0Bootstrap": None,
        "proposalAttempts": len(rows),
        "funnelEntries": [
            {
                "proposalOrdinal": index,
                "entrySha256": digest(f"funnel:{index}"),
                "originKind": "mutation",
                "disposition": "accepted",
            }
            for index in range(len(rows))
        ],
        "candidateCount": len(rows),
        "candidates": rows,
    }
    evaluation["evaluationPopulationSha256"] = semantic_sha256(evaluation)
    all_entries = []
    for candidate_id in sorted(grouped):
        for window_index, record in enumerate(grouped[candidate_id]):
            all_entries.append(entry(f"task-{candidate_id}-{window_index}", record))
    all_entries.sort(key=lambda value: value["task"]["taskId"])
    index = {
        "schemaVersion": "temporal_qd_tail_result_index_v3",
        "authorityId": "fixture-authority",
        "authoritySha256": digest("authority"),
        "taskMatrixSha256": digest("matrix"),
        "taskManifestSha256": digest("manifest"),
        "checkpointSha256": digest("checkpoint"),
        "taskCount": len(all_entries),
        "funnelProjectionIncluded": False,
        "sourceResultBlobBytes": len(all_entries),
        "entries": all_entries,
    }
    index["tailResultIndexSha256"] = semantic_sha256(index)
    (out / "evaluation-population.json").write_bytes(canonical_json_bytes(evaluation))
    (out / "tail-result-index-v3.json").write_bytes(canonical_json_bytes(index))

    members = []
    rejected = []
    for row in sorted(rows, key=lambda value: value["candidateId"]):
        candidate_id = row["candidateId"]
        windows = grouped[candidate_id]
        if candidate_id == "qd_fixture_5":
            rejected.append({
                "candidateId": candidate_id,
                "disposition": "rejected",
                "reasonCode": "duplicate_break_even_execution_invariant",
                "structuralProvenance": {"modules": [{"direction": "long", "breakEvenActionCount": 2}]},
            })
            continue
        selected_rejections = [value for value in windows if value.get("evaluationRejected") is True]
        if selected_rejections:
            rejected.append({
                "candidateId": candidate_id,
                "disposition": "rejected",
                "reasonCode": selected_rejections[0]["rejection"]["reason_code"],
                "windowRejections": [{"windowId": value["windowId"], "rejection": value["rejection"]} for value in selected_rejections],
            })
            continue
        aggregate = _aggregate_candidate(row, windows)
        members.append({
            "candidateId": candidate_id,
            "generationIndex": 7,
            "candidate": row,
            "aggregate": aggregate,
            "descriptor": qd_behavior_descriptor(row, aggregate),
            "objectives": _objective_row(row, aggregate),
            "finiteDataValidity": _finite_data_validity(aggregate, minimum_total_trades=8, minimum_trades_per_window=4, cap_trades=20),
            "cappedTradeSupport": float(min(max(0, int(aggregate.get("totalTrades") or 0)), 20)),
        })
    counts = {}
    for member in members:
        cell = member["descriptor"]["cellId"]
        counts[cell] = counts.get(cell, 0) + 1
    provisional_input = [{
        "candidateId": member["candidateId"],
        "candidateIdentitySha256": member["candidate"]["candidateIdentitySha256"],
        "programSha256": member["candidate"]["programSha256"],
        "profileSnapshotSha256": member["candidate"]["profileSnapshotSha256"],
        "cellId": member["descriptor"]["cellId"],
        "costView": "research_conservative",
        "currentPanelRank": float(member["aggregate"].get("totalConservativeNetR") or 0.0),
        "novelty": 1.0 / float(counts[member["descriptor"]["cellId"]]),
    } for member in members]
    expected = {
        "members": members,
        "evaluationRejectedCandidates": rejected,
        "resultSetSha256": _result_set_sha256(grouped),
        "provisional": reduce_provisional_diverse_survivors(provisional_input, limit=2),
    }
    (out / "expected.json").write_bytes(canonical_json_bytes(expected))
    manifest = {
        "schemaVersion": "temporal_qd_native_tail_reduction_manifest_v1",
        "contractVersion": "temporal_qd_native_foundation_v1",
        "operation": "reduce_evaluated_members_and_provisional",
        "evaluationPopulationPath": str((out / "evaluation-population.json").resolve()),
        "evaluationPopulationSha256": evaluation["evaluationPopulationSha256"],
        "tailResultIndexPath": str((out / "tail-result-index-v3.json").resolve()),
        "tailResultIndexSha256": index["tailResultIndexSha256"],
        "generationIndex": 7,
        "minimumTotalTrades": 8,
        "minimumTradesPerWindow": 4,
        "capTrades": 20,
        "provisionalLimit": 2,
        "resultPath": "tail-reduction-result.json",
    }
    manifest["manifestSha256"] = semantic_sha256(manifest)
    (out / "manifest.json").write_bytes(canonical_json_bytes(manifest) + b"\n")


if __name__ == "__main__":
    main()
