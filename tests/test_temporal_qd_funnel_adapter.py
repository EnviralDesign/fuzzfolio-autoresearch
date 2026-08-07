from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from autoresearch.temporal_qd_funnel_adapter import _result_behavior, build_qd_generation_funnel
from autoresearch.temporal_search import _rejected_result_material


def _sha(letter: str) -> str:
    return "sha256:" + letter * 64


def _entry(ordinal: int, candidate_id: str | None, stage: str) -> dict:
    row = {
        "entrySha256": _sha(chr(97 + ordinal)),
        "proposalOrdinal": ordinal,
        "originKind": "random_immigrant",
        "disposition": stage,
    }
    if candidate_id is None:
        return row
    raw = _sha(chr(97 + ordinal))
    candidate = {
        "schemaVersion": "temporal_qd_proposal_funnel_stage_v1",
        "candidateId": candidate_id,
        "rawSourceProfileSha256": raw,
        "staticReachability": {"outcome": "reachable", "reasons": []},
    }
    # The attempt adapter deliberately reads the real proposal journal shape;
    # funnelCandidate is the enabled-stage projection, not a second authority.
    row["proposal"] = {
        "candidateId": candidate_id,
        "rawSourceProfileSha256": raw,
    }
    if stage == "static_reachability_rejected":
        candidate["staticReachability"] = {"outcome": "rejected", "reasons": [stage]}
    else:
        candidate["nativeValidation"] = {
            "outcome": "rejected" if stage == "native_validator_rejected" else "valid",
            "reasons": [stage] if stage == "native_validator_rejected" else [],
            "resolvedProfileSha256": _sha("p"),
            "programSha256": _sha("q"),
            "validationReportSha256": _sha("r"),
        }
        if stage != "native_validator_rejected":
            candidate["admission"] = {
                "outcome": "admitted" if stage == "accepted" else "rejected_duplicate",
                "reasons": [] if stage == "accepted" else [stage],
                **({"canonicalEvidenceIdentitySha256": _sha("s")} if stage == "accepted" else {}),
            }
    row["funnelCandidate"] = candidate
    return row


def _result(
    candidate_id: str,
    *,
    statuses: list[str] | None = None,
    conservative_terminal_net_r: float = 1.0,
) -> dict:
    return {
        "candidate_id": candidate_id,
        "cost_view_results": {
            "research_conservative": {
                "replay_result": {
                    "executionTraces": [
                        {"status": status}
                        for status in (statuses or ["scheduled", "filled", "rejected"])
                    ],
                    "trades": [{"id": "one"}],
                    "metrics": {
                        "terminalAdjustedTotalNetR": conservative_terminal_net_r,
                        "terminalAdjustedMaxDrawdownR": 0.5,
                    },
                }
            }
        },
    }


def _inputs(tmp_path: Path, *, partial: bool = False, reverse: bool = False) -> dict:
    candidates = [
        ("qd_static", "static_reachability_rejected"),
        ("qd_native", "native_validator_rejected"),
        ("qd_duplicate", "duplicate_candidate_identity"),
        ("qd_retained", "accepted"),
        ("qd_resolved_duplicate", "accepted"),
    ]
    entries = [_entry(index, candidate_id, disposition) for index, (candidate_id, disposition) in enumerate(candidates)]
    entries.append(_entry(5, None, "proposal_unavailable"))
    tasks = []
    completed: dict[str, dict] = {}
    for index, candidate_id in enumerate(("qd_retained", "qd_resolved_duplicate")):
        task_id = f"task-{candidate_id}"
        tasks.append({
            "task_id": task_id,
            "payload": {
                "candidate_id": candidate_id,
                "analysis_window_start": "2025-01-01T00:00:00Z",
                "analysis_window_end": "2025-02-01T00:00:00Z",
            },
        })
        material = _result(candidate_id)
        result_path = tmp_path / f"{task_id}.json"
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(json.dumps(material), encoding="utf-8")
        completed[task_id] = {
            "candidateId": candidate_id,
            "resultPath": str(result_path),
            "resultSha256": canonical_sha256(material),
        }
    if partial:
        completed.pop("task-qd_resolved_duplicate")
    if reverse:
        completed = dict(reversed(list(completed.items())))
    accounting = {
        "dispositionCounts": {
            "accepted": 2,
            "duplicate_candidate_identity": 1,
            "native_validator_rejected": 1,
            "proposal_unavailable": 1,
            "static_reachability_rejected": 1,
        },
        "originProposalCounts": {"random_immigrant": 6},
    }
    return {
        "proposal_entries": entries,
        "proposal_accounting": accounting,
        "population": {
            "candidateCount": 2,
            "candidates": [
                {
                    "candidateId": candidate_id,
                    "sourceProfileSha256": entries[index]["funnelCandidate"]["rawSourceProfileSha256"],
                }
                for index, candidate_id in ((3, "qd_retained"), (4, "qd_resolved_duplicate"))
            ],
        },
        "authority": {"developmentWindows": [{"windowId": "w1", "analysisWindowStart": "2025-01-01T00:00:00Z", "analysisWindowEnd": "2025-02-01T00:00:00Z"}]},
        "task_manifest": {"tasks": tasks},
        "checkpoint": {"completed": completed},
        "archive": {
            "cells": [{"members": [{"candidateId": "qd_retained", "archiveLane": "quality"}]}],
            "resolvedExecutionDeduplication": {"duplicates": [{"discardedCandidateIds": ["qd_resolved_duplicate"]}]},
        },
        "minimum_total_trades": 1,
        "minimum_trades_per_window": 1,
    }


def test_adapter_records_every_qd_boundary_and_explicit_not_retained_decision(tmp_path: Path) -> None:
    artifact = build_qd_generation_funnel(**_inputs(tmp_path))
    assert artifact["attemptLedger"]["attemptCount"] == 6
    assert artifact["attemptLedger"]["nonMaterializedAttemptCount"] == 1
    terminals = {row["candidateId"]: row["terminalDisposition"] for row in artifact["candidates"]}
    assert terminals == {
        "qd_duplicate": "duplicate_rejected",
        "qd_native": "native_validation_rejected",
        "qd_resolved_duplicate": "not_retained",
        "qd_retained": "retained",
        "qd_static": "static_reachability_rejected",
    }
    discarded = next(row for row in artifact["candidates"] if row["candidateId"] == "qd_resolved_duplicate")
    assert discarded["stages"]["archiveRetention"]["outcome"] == "not_retained"


def test_adapter_partial_candidate_is_terminal_and_has_no_stale_archive_row(tmp_path: Path) -> None:
    artifact = build_qd_generation_funnel(**_inputs(tmp_path, partial=True))
    row = next(item for item in artifact["candidates"] if item["candidateId"] == "qd_resolved_duplicate")
    assert row["terminalDisposition"] == "evaluation_rejected"
    assert "activationQuality" not in row["stages"]
    assert "archiveRetention" not in row["stages"]


def test_adapter_treats_warmup_rejection_as_terminal_without_activation_or_retention(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    record = inputs["checkpoint"]["completed"]["task-qd_retained"]
    task = next(task for task in inputs["task_manifest"]["tasks"] if task["task_id"] == "task-qd_retained")
    task.update({"lane_id": "qd_retained", "attempt_id": "task-qd_retained", "task_kind": "temporal_graph_candidate_window"})
    task["payload"].update({
        "job_id": "task-qd_retained",
        "authority_id": "sha256:" + "a" * 64,
        "evidence_plan": {"plan_id": "sha256:" + "b" * 64},
        "lake_window_semantic_sha256": "sha256:" + "c" * 64,
        "shared_observation_stream_id": "sha256:" + "d" * 64,
    })
    rejected = _rejected_result_material(task, {
        "status": "failed",
        "task_id": task["task_id"],
        "lane_id": task["lane_id"],
        "attempt_id": task["attempt_id"],
        "lease_id": "warmup-lease",
        "error": {"type": "AlignedScoringWarmupInsufficientError", "attempt": 8},
    })
    Path(record["resultPath"]).write_text(json.dumps(rejected), encoding="utf-8")
    record["resultSha256"] = canonical_sha256(rejected)

    artifact = build_qd_generation_funnel(**inputs)

    row = next(item for item in artifact["candidates"] if item["candidateId"] == "qd_retained")
    assert row["terminalDisposition"] == "evaluation_rejected"
    assert "activationQuality" not in row["stages"]
    assert "archiveRetention" not in row["stages"]


def test_adapter_refuses_tampered_warmup_rejection(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    record = inputs["checkpoint"]["completed"]["task-qd_retained"]
    # Merely claiming the rejection schema is not enough: every binding and
    # immutable artifact identity must validate before the funnel can skip it.
    malformed = {"schema_version": "temporal_graph_candidate_window_rejected_result_v1"}
    Path(record["resultPath"]).write_text(json.dumps(malformed), encoding="utf-8")
    record["resultSha256"] = canonical_sha256(malformed)

    with pytest.raises(TemporalDiscoveryContractError, match="warmup rejection does not match its task"):
        build_qd_generation_funnel(**inputs)


def test_adapter_is_reordered_checkpoint_exact_and_refuses_tampered_blob(tmp_path: Path) -> None:
    normal = build_qd_generation_funnel(**_inputs(tmp_path / "normal"))
    reversed_inputs = _inputs(tmp_path / "reverse", reverse=True)
    reversed_artifact = build_qd_generation_funnel(**reversed_inputs)
    assert normal["artifactSha256"] == reversed_artifact["artifactSha256"]
    record = next(iter(reversed_inputs["checkpoint"]["completed"].values()))
    Path(record["resultPath"]).write_text('{"tampered":true}', encoding="utf-8")
    with pytest.raises(TemporalDiscoveryContractError, match="semantic identity mismatch"):
        build_qd_generation_funnel(**reversed_inputs)


def test_adapter_counts_canceled_trace_as_activation_and_rejected_attrition(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    record = inputs["checkpoint"]["completed"]["task-qd_retained"]
    material = _result("qd_retained", statuses=["canceled"])
    Path(record["resultPath"]).write_text(json.dumps(material), encoding="utf-8")
    record["resultSha256"] = canonical_sha256(material)
    behavior = _result_behavior(
        material,
        result_sha=record["resultSha256"],
        window_id="w1",
    )
    assert behavior["activationCount"] == 1
    assert behavior["canceledIntentOrEffectCount"] == 1
    assert behavior["rejectedIntentOrEffectCount"] == 1
    assert behavior["neverActivated"] is False
    artifact = build_qd_generation_funnel(**inputs)
    row = next(item for item in artifact["candidates"] if item["candidateId"] == "qd_retained")
    assert row["stages"]["activationQuality"]["outcome"] == "recorded"
    assert row["stages"]["activationQuality"]["qualityDisposition"] == "eligible"


def test_adapter_negative_conservative_candidate_is_quality_rejected_not_promoted(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    record = inputs["checkpoint"]["completed"]["task-qd_retained"]
    material = _result("qd_retained", conservative_terminal_net_r=-0.25)
    Path(record["resultPath"]).write_text(json.dumps(material), encoding="utf-8")
    record["resultSha256"] = canonical_sha256(material)
    # The archive may retain one negative row only for bounded novelty
    # exploration.  That must never cross the quality/promotion boundary.
    inputs["archive"]["cells"][0]["members"][0]["archiveLane"] = "negative_novelty"

    artifact = build_qd_generation_funnel(**inputs)

    row = next(item for item in artifact["candidates"] if item["candidateId"] == "qd_retained")
    assert row["stages"]["activationQuality"] == {
        "outcome": "quality_rejected",
        "reasons": ["nonnegative_worst_window_conservative_net_r"],
        "qualityDisposition": "not_eligible",
    }
    assert "archiveRetention" not in row["stages"]
    assert row["stages"]["exploratoryRetention"] == {
        "outcome": "retained_for_scheduled_negative_novelty_exploration",
        "reasons": ["non_promotable_scheduled_negative_novelty_exploration"],
        "archiveLane": "negative_novelty",
        "promotionEligible": False,
    }
    assert row["terminalDisposition"] == "activation_quality_rejected"


def test_rotating_proposal_funnel_defers_to_cumulative_robust_archive_lane(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    record = inputs["checkpoint"]["completed"]["task-qd_retained"]
    material = _result("qd_retained", conservative_terminal_net_r=-0.25)
    Path(record["resultPath"]).write_text(json.dumps(material), encoding="utf-8")
    record["resultSha256"] = canonical_sha256(material)
    inputs["archive"]["rotatingEvidenceTransaction"] = {
        "schemaVersion": "temporal_qd_rotating_parent_projection_v1"
    }
    inputs["archive"]["cells"][0]["members"][0]["archiveLane"] = "quality"

    artifact = build_qd_generation_funnel(**inputs)

    row = next(
        item for item in artifact["candidates"] if item["candidateId"] == "qd_retained"
    )
    assert row["stages"]["activationQuality"]["outcome"] == "recorded"
    assert row["stages"]["archiveRetention"]["outcome"] == "retained"
    assert row["terminalDisposition"] == "retained"


def test_adapter_refuses_stripped_materialized_stage_projection(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    inputs["proposal_entries"][3].pop("funnelCandidate")
    with pytest.raises(TemporalDiscoveryContractError, match="lacks its required funnel stage projection"):
        build_qd_generation_funnel(**inputs)
