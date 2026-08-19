import json
import random
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_json, canonical_sha256
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_generation_quality_audit import (
    AUDIT_SCHEMA,
    audit_temporal_qd_generation_quality,
    observe_generation_quality_audit,
    _parse_parent_material,
)


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(value, dict):
        path.write_text(canonical_json(value) + "\n", encoding="utf-8", newline="\n")
    else:
        path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8", newline="\n")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(canonical_json(row) + "\n" for row in rows),
        encoding="utf-8",
        newline="\n",
    )


def _behavior(window_count: int = 4, *, long_net: float = 1.0, short_net: float = 0.5) -> dict[str, object]:
    return {
        "schemaVersion": "temporal_realized_behavior_v1",
        "windowCount": window_count,
        "identitySha256": canonical_sha256(
            {"windowCount": window_count, "longNet": long_net, "shortNet": short_net}
        ),
        "sides": {
            "long": {
                "closedTrades": 4,
                "activeWindowCount": window_count,
                "activeWindowFraction": 1.0,
                "grossR": long_net + 0.1,
                "netR": long_net,
                "costR": 0.1,
                "active": True,
                "terminalDirectionCount": 0,
            },
            "short": {
                "closedTrades": 4,
                "activeWindowCount": window_count,
                "activeWindowFraction": 1.0,
                "grossR": short_net + 0.1,
                "netR": short_net,
                "costR": 0.1,
                "active": True,
                "terminalDirectionCount": 0,
            },
        },
    }


def _evaluated_row(
    candidate_id: str,
    *,
    cell_id: str = "cell-a",
    net_r: float = 1.0,
    closed_trades: int = 4,
    behavior: dict[str, object] | None = None,
) -> dict[str, object]:
    behavior = behavior or _behavior()
    return {
        "candidateId": candidate_id,
        "candidate": {
            "candidateId": candidate_id,
            "candidateIdentitySha256": canonical_sha256({"candidateId": candidate_id}),
            "programSha256": canonical_sha256({"program": candidate_id}),
        },
        "aggregate": {
            "resolvedProgramSha256": canonical_sha256({"resolved": candidate_id}),
            "fingerprintSha256": canonical_sha256({"fingerprint": candidate_id}),
            "behaviorIdentitySha256": behavior["identitySha256"],
            "totalTrades": closed_trades * 4,
            "totalConservativeNetR": net_r * 4,
            "worstWindowConservativeNetR": net_r - 0.1,
            "maxWindowDrawdownR": 0.2,
            "coveredMonths": 4.0,
            "realizedBehavior": behavior,
            "windowRecords": [
                {
                    "windowId": f"w{i}",
                    "metrics": {
                        "conservativeNetR": net_r,
                        "closedTrades": closed_trades,
                        "maxDrawdownR": 0.1,
                    },
                }
                for i in range(4)
            ],
        },
        "descriptor": {"cellId": cell_id},
        "cellId": cell_id,
        "currentPanelRank": net_r,
    }


def _cumulative_member(candidate_id: str, *, net_r: float = 1.0) -> dict[str, object]:
    return {
        "candidateId": candidate_id,
        "candidateIdentitySha256": canonical_sha256({"candidateId": candidate_id}),
        "programSha256": canonical_sha256({"program": candidate_id}),
        "resolvedProgramSha256": canonical_sha256({"resolved": candidate_id}),
        "coveredMonths": 4.0,
        "windowMetrics": [
            {
                "windowId": f"w{i}",
                "conservativeNetR": net_r,
                "closedTrades": 4,
                "maxDrawdownR": 0.1,
            }
            for i in range(4)
        ],
        "cumulativeRealizedBehavior": _behavior(),
        "robustObjectives": {
            "worstWindowConservativeNetR": net_r - 0.1,
            "drawdown": 0.2,
            "costDrag": 0.1,
            "novelty": 0.5,
        },
    }


def _archive_member(candidate_id: str, *, lane: str = "quality", cell_id: str = "cell-a") -> dict[str, object]:
    return {
        "candidateId": candidate_id,
        "archiveLane": lane,
        "robustBreederEligible": True,
        "descriptor": {"cellId": cell_id},
        "cumulativeEvidence": {
            "requiredPanelIds": ["panel-current"],
            "robustParetoFront": 0,
        },
        "requiredPanelIds": ["panel-current"],
    }


def _build_fixture(
    tmp_path: Path,
    *,
    generation_index: int = 1,
    include_attempts: bool = False,
    include_parent_eval: bool = True,
    shuffle_inputs: bool = False,
) -> Path:
    run_root = tmp_path / "run"
    generation_root = run_root / "generations" / f"generation-{generation_index:04d}"
    finalization_root = generation_root / "native-finalization"

    parent_rows = [
        {
            "candidateId": "child-a",
            "pairPayload": {
                "proposalDelta": {
                    "originKind": "qd_structural_offspring",
                    "scheduledKind": "qd_structural_offspring",
                }
            },
            "parent": {
                "candidateId": "parent-a",
                "candidateIdentitySha256": canonical_sha256({"candidateId": "parent-a"}),
            },
            "mate": {"candidateId": "parent-b"},
            "mutationDepth": 2,
            "steps": [
                {
                    "application": {
                        "applicationAudit": {
                            "operatorId": "evolvable_resource_v1",
                            "mutationTrace": ["resource"],
                        }
                    }
                },
                {
                    "application": {
                        "applicationAudit": {
                            "operatorId": "evolvable_hold_policy_v1",
                            "mutationTrace": ["hold"],
                        }
                    }
                },
            ],
            "terminalDisposition": "materialized",
            "proposalOrdinal": 1,
        },
        {
            "candidateId": "immigrant-a",
            "pairPayload": {
                "proposalDelta": {
                    "originKind": "qd_random_immigrant",
                    "scheduledKind": "qd_random_immigrant",
                }
            },
            "parent": None,
            "mate": None,
            "mutationDepth": 0,
            "steps": [],
            "terminalDisposition": "materialized",
            "proposalOrdinal": 2,
        },
    ]
    evaluated_rows = [
        _evaluated_row("child-a", net_r=1.2, cell_id="cell-a", behavior=_behavior(long_net=1.2, short_net=0.6)),
        _evaluated_row("immigrant-a", net_r=0.8, cell_id="cell-b", behavior=_behavior(long_net=0.7, short_net=0.9)),
    ]
    if include_parent_eval:
        evaluated_rows.append(
            _evaluated_row(
                "parent-a",
                net_r=0.4,
                cell_id="cell-a",
                closed_trades=1,
                behavior=_behavior(long_net=0.2, short_net=0.1),
            )
        )

    if shuffle_inputs:
        random.Random(0).shuffle(parent_rows)
        random.Random(1).shuffle(evaluated_rows)

    _write_jsonl(generation_root / "proposal" / "parent-material.jsonl", parent_rows)
    _write_jsonl(
        generation_root
        / "campaign"
        / "proposal-current-panel"
        / "campaign-output"
        / "evaluated-members.jsonl",
        evaluated_rows,
    )

    provisional_ids = ["child-a", "immigrant-a"]
    if include_parent_eval:
        provisional_ids.append("parent-a")
    selected_rows = [
        {
            "candidateId": candidate_id,
            "candidateIdentitySha256": canonical_sha256({"candidateId": candidate_id}),
            "programSha256": canonical_sha256({"program": candidate_id}),
        }
        for candidate_id in provisional_ids
    ]
    prefinalizer_round = generation_root / "prefinalizer" / "round-0001"
    _write_jsonl(prefinalizer_round / "selected-rich-members.jsonl", selected_rows)
    result = {
        "schemaVersion": "temporal_qd_v5_prefinalizer_result_v1",
        "status": "ready_for_finalizer",
        "generationIndex": generation_index,
        "roundIndex": 1,
        "provisional": {
            "candidates": [
                {"candidateId": candidate_id} for candidate_id in provisional_ids
            ]
        },
    }
    result["resultSha256"] = canonical_sha256(result)
    _write(prefinalizer_round / "result.json", result)
    receipt = {
        "schemaVersion": "temporal_qd_v5_prefinalizer_execution_receipt_v1",
        "status": "ready_for_finalizer",
        "generationIndex": generation_index,
        "roundIndex": 1,
    }
    receipt["receiptSha256"] = canonical_sha256(receipt)
    _write(prefinalizer_round / "execution-receipt.json", receipt)

    if include_attempts:
        attempts = [
            {
                "proposalOrdinal": 1,
                "disposition": "accepted",
                "operatorId": "evolvable_resource_v1",
                "mutationDepth": 1,
            },
            {
                "proposalOrdinal": 2,
                "disposition": "operation_rejected",
                "operatorId": "evolvable_hold_policy_v1",
                "mutationDepth": 2,
            },
        ]
        _write_jsonl(
            generation_root / "prefinalizer" / "proposal-attempts" / "proposal-attempts.jsonl",
            attempts,
        )
        attempt_receipt = {
            "schemaVersion": "temporal_qd_v5_evolved_attempt_stream_receipt_v1",
            "recordCount": len(attempts),
        }
        attempt_receipt["receiptSha256"] = canonical_sha256(attempt_receipt)
        _write(
            generation_root
            / "prefinalizer"
            / "proposal-attempts"
            / "proposal-attempts-receipt.json",
            attempt_receipt,
        )

    cumulative = {
        "schemaVersion": "temporal_qd_cumulative_archive_v1",
        "generationIndex": generation_index,
        "qualityCandidateIds": ["child-a"],
        "frontierCandidateIds": ["immigrant-a"],
        "members": [
            _cumulative_member("child-a", net_r=1.2),
            _cumulative_member("immigrant-a", net_r=0.8),
        ],
    }
    if include_parent_eval:
        cumulative["members"].append(_cumulative_member("parent-a", net_r=0.4))
    cumulative["archiveSha256"] = canonical_sha256(
        {key: value for key, value in cumulative.items() if key != "archiveSha256"}
    )
    _write(finalization_root / "evidence" / "cumulative-archive.json", cumulative)

    archive = {
        "schemaVersion": "temporal_qd_archive_v3",
        "generationIndex": generation_index,
        "frozenPolicy": {},
        "cellCapacity": 4,
        "occupiedCellCount": 2,
        "qualityMemberCount": 1,
        "memberCount": 2,
        "cells": [
            {
                "cellId": "cell-a",
                "members": [_archive_member("child-a", lane="quality", cell_id="cell-a")],
            },
            {
                "cellId": "cell-b",
                "members": [_archive_member("immigrant-a", lane="rotating_frontier", cell_id="cell-b")],
            },
        ],
    }
    archive["archiveSha256"] = canonical_sha256(
        {key: value for key, value in archive.items() if key != "archiveSha256"}
    )
    _write(finalization_root / "archive.json", archive)
    _write(
        finalization_root / "source.json",
        {
            "schemaVersion": "temporal_qd_generation_finalization_source_v2",
            "generationIndex": generation_index,
            "sourceSha256": canonical_sha256({"generation": generation_index}),
        },
    )
    return run_root


def test_joins_candidate_across_artifacts(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path)
    audit = audit_temporal_qd_generation_quality(run_root, 1)
    assert audit["schemaVersion"] == AUDIT_SCHEMA
    assert audit["construction"]["acceptedCandidateCount"] == 2
    assert audit["evaluation"]["evaluatedCandidateCount"] == 3
    assert audit["cumulativeQualification"]["archiveRetained"] == 2
    assert any(row["candidateId"] == "parent-a" for row in audit["incumbentLifecycle"])


def test_multi_step_operator_extraction(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path)
    audit = audit_temporal_qd_generation_quality(run_root, 1)
    sequences = [
        row["operatorSequence"]
        for row in audit["operatorYield"]
        if row.get("operatorSequence")
    ]
    assert "evolvable_resource_v1 > evolvable_hold_policy_v1" in sequences


def test_immigrant_rows_have_null_parents(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path)
    audit = audit_temporal_qd_generation_quality(run_root, 1)
    immigrant = next(row for row in audit["originYield"] if row["originKind"] == "immigrant")
    assert immigrant["constructedCandidateCount"] == 1
    assert immigrant["evaluatedCandidateCount"] == 1


def test_missing_attempt_telemetry_is_not_inferred(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path, include_attempts=False)
    audit = audit_temporal_qd_generation_quality(run_root, 1)
    assert audit["construction"]["attemptTelemetryAvailable"] is False
    assert "attemptCount" not in audit["construction"]


def test_attempt_telemetry_when_present(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path, include_attempts=True)
    audit = audit_temporal_qd_generation_quality(run_root, 1)
    assert audit["construction"]["attemptTelemetryAvailable"] is True
    assert audit["construction"]["attemptCount"] == 2


def test_same_panel_parent_comparison_available_and_unavailable(tmp_path: Path) -> None:
    with_parent = _build_fixture(tmp_path / "with", include_parent_eval=True)
    without_parent = _build_fixture(tmp_path / "without", include_parent_eval=False)
    with_row = next(
        row
        for row in audit_temporal_qd_generation_quality(with_parent, 1)["parentYield"]
        if row["parentCandidateId"] == "parent-a"
    )
    without_row = next(
        row
        for row in audit_temporal_qd_generation_quality(without_parent, 1)["parentYield"]
        if row["parentCandidateId"] == "parent-a"
    )
    assert with_row["samePanelParentComparisonAvailable"] is True
    assert "parentCurrentPanelConservativeNetR" in with_row
    assert without_row["samePanelParentComparisonAvailable"] is False
    assert "parentCurrentPanelConservativeNetR" not in without_row


def test_first_failure_counts_sum_to_entering_cohort(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path, include_parent_eval=True)
    audit = audit_temporal_qd_generation_quality(run_root, 1)
    cumulative = audit["cumulativeQualification"]
    entering = cumulative["enteringCohortSize"]
    terminal = sum(
        cumulative[key]
        for key in (
            "failedActiveWindowFraction",
            "failedTradesPerMonth",
            "failedDirectionNoNonnegativeSide",
            "failedDirectionMildNegativeOpposite",
            "failedDirectionHarmfulOpposite",
            "failedCumulativeEconomics",
            "failedMedianEconomics",
            "paretoRemoved",
            "cellCapacityRemoved",
            "resolvedExecutionDeduplicated",
            "archiveRetained",
        )
    )
    assert terminal == entering


def test_behavior_inverse_simpson(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path, include_parent_eval=False)
    audit = audit_temporal_qd_generation_quality(run_root, 1)
    assert audit["behaviorDiversity"]["effectiveBehaviorCount"] == pytest.approx(2.0)
    assert audit["behaviorDiversity"]["evaluatedBehaviorIdentityCount"] == 2


def test_deterministic_ordering_and_hash(tmp_path: Path) -> None:
    ordered = _build_fixture(tmp_path / "ordered", shuffle_inputs=False)
    shuffled = _build_fixture(tmp_path / "shuffled", shuffle_inputs=True)
    first = audit_temporal_qd_generation_quality(ordered, 1)
    second = audit_temporal_qd_generation_quality(shuffled, 1)
    assert first["auditSha256"] == second["auditSha256"]
    assert canonical_json(first) == canonical_json(second)
    assert first["auditSha256"] == canonical_sha256(
        {key: value for key, value in first.items() if key != "auditSha256"}
    )


def test_no_candidate_sized_arrays_except_capped_lists(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path, include_parent_eval=True)
    audit = audit_temporal_qd_generation_quality(run_root, 1)
    assert len(audit["incumbentLifecycle"]) <= 32

    def walk(value: object) -> None:
        if isinstance(value, list):
            if len(value) > 32:
                assert value is audit.get("incumbentLifecycle") or all(
                    not isinstance(item, dict) or "candidateId" not in item for item in value
                )
            for item in value:
                walk(item)
        elif isinstance(value, dict):
            for item in value.values():
                walk(item)

    walk(audit)


def test_duplicate_candidate_ids_fail_audit(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path)
    evaluated_path = (
        run_root
        / "generations"
        / "generation-0001"
        / "campaign"
        / "proposal-current-panel"
        / "campaign-output"
        / "evaluated-members.jsonl"
    )
    rows = evaluated_path.read_text(encoding="utf-8").splitlines()
    evaluated_path.write_text(rows[0] + "\n" + rows[0] + "\n", encoding="utf-8", newline="\n")
    with pytest.raises(TemporalDiscoveryContractError, match="duplicate candidateId"):
        audit_temporal_qd_generation_quality(run_root, 1)


def test_observe_writes_error_without_raising(tmp_path: Path) -> None:
    run_root = tmp_path / "empty"
    run_root.mkdir()
    result = observe_generation_quality_audit(run_root, 1)
    assert result["status"] == "error"
    error_path = (
        run_root
        / "generations"
        / "generation-0001"
        / "quality-audit"
        / "audit-error.json"
    )
    assert error_path.is_file()


def test_observe_writes_success_file(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path)
    result = observe_generation_quality_audit(run_root, 1)
    assert result["status"] == "ok"
    success_path = (
        run_root
        / "generations"
        / "generation-0001"
        / "quality-audit"
        / "generation-quality-audit.json"
    )
    assert success_path.is_file()


def test_oversized_prefinalizer_result_is_discovered(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path)
    path = (
        run_root
        / "generations"
        / "generation-0001"
        / "prefinalizer"
        / "round-0001"
        / "result.json"
    )
    result = json.loads(path.read_text(encoding="utf-8"))
    result.pop("resultSha256", None)
    result["observationalPadding"] = "x" * (1_048_576 + 64)
    result["resultSha256"] = canonical_sha256(
        {key: value for key, value in result.items() if key != "resultSha256"}
    )
    _write(path, result)
    assert path.stat().st_size > 1_048_576
    audit = audit_temporal_qd_generation_quality(run_root, 1)
    assert "latest ready prefinalizer round unavailable" not in audit["limitations"]
    assert audit["provisionalSelection"]["provisionalCandidateCount"] == 3


def test_oversized_cumulative_archive_still_audits(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path)
    path = (
        run_root
        / "generations"
        / "generation-0001"
        / "native-finalization"
        / "evidence"
        / "cumulative-archive.json"
    )
    cumulative = json.loads(path.read_text(encoding="utf-8"))
    cumulative.pop("archiveSha256", None)
    cumulative["observationalPadding"] = "x" * (1_048_576 + 64)
    cumulative["archiveSha256"] = canonical_sha256(
        {key: value for key, value in cumulative.items() if key != "archiveSha256"}
    )
    _write(path, cumulative)
    assert path.stat().st_size > 1_048_576
    audit = audit_temporal_qd_generation_quality(run_root, 1)
    assert audit["schemaVersion"] == AUDIT_SCHEMA
    assert audit["cumulativeQualification"]["archiveRetained"] == 2


def test_finite_support_alias_is_not_quality(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path)
    audit = audit_temporal_qd_generation_quality(run_root, 1)
    all_eval = audit["evaluation"]["allEvaluated"]
    assert all_eval["finiteSupportEligibleCount"] == all_eval["combinedSupportPass"]
    cumulative = audit["cumulativeQualification"]
    assert cumulative["finiteSupportEligibleCount"] == cumulative["supportPass"]
    assert cumulative["currentPanelQualityLikeCount"] == cumulative["rawQualityEligible"]
    offspring = next(row for row in audit["originYield"] if row["originKind"] == "offspring")
    assert offspring["finiteSupportEligibleCount"] == offspring["supportPassCount"]
    assert "currentPanelQualityLikeCount" in offspring


def test_fast_ephemeral_attempt_sidecar_telemetry(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path, include_attempts=False)
    generation_root = run_root / "generations" / "generation-0001"
    attempts = [
        {
            "proposalOrdinal": 1,
            "disposition": "accepted",
            "originKind": "structural_offspring",
            "reasonCode": "accepted",
            "lineageRefs": {"parent": {"candidateId": "parent-a"}},
        },
        {
            "proposalOrdinal": 2,
            "disposition": "rejected",
            "originKind": "structural_offspring",
            "reasonCode": "pre_plan_rejected",
            "lineageRefs": {"parent": {"candidateId": "parent-a"}},
        },
        {
            "proposalOrdinal": 3,
            "disposition": "no_op",
            "originKind": "structural_offspring",
            "reasonCode": "no_eligible_operation",
            "lineageRefs": {"parent": {"candidateId": "parent-a"}},
        },
    ]
    _write_jsonl(generation_root / "proposal" / "proposal-attempts.jsonl", attempts)
    receipt = {
        "schemaVersion": "temporal_qd_v5_fast_ephemeral_attempt_stream_receipt_v1",
        "recordCount": len(attempts),
    }
    receipt["receiptSha256"] = canonical_sha256(receipt)
    _write(generation_root / "proposal" / "proposal-attempts-receipt.json", receipt)
    audit = audit_temporal_qd_generation_quality(run_root, 1)
    assert audit["construction"]["attemptTelemetryAvailable"] is True
    assert audit["construction"]["attemptCount"] == 3
    assert audit["construction"]["acceptedAttemptCount"] == 1
    assert audit["construction"]["rejectedAttemptCount"] == 1
    assert audit["construction"]["noOpAttemptCount"] == 1
    assert audit["construction"]["attemptsByParent"]["parent-a"] == 3
    assert audit["construction"]["attemptsByOriginKind"]["structural_offspring"] == 3


def test_nested_parent_material_resolves_parent_sha_and_construction_kind(
    tmp_path: Path,
) -> None:
    parent_identity = canonical_sha256({"candidateId": "parent-a"})
    child_identity = canonical_sha256({"candidateId": "child-a"})
    parsed = _parse_parent_material(
        [
            {
                "candidateId": "child-a",
                "pairPayload": {
                    "acceptedRecord": {
                        "candidateId": "child-a",
                        "candidateIdentitySha256": child_identity,
                        "originKind": "qd_structural_offspring",
                        "constructionAudit": {
                            "kind": "mutation_trace",
                            "evolvedAudit": {
                                "kind": "mutation_trace",
                                "parentCandidateIdentitySha256": parent_identity,
                            },
                        },
                    },
                    "proposalDelta": {
                        "originKind": "qd_structural_offspring",
                        "scheduledKind": "qd_structural_offspring",
                    },
                },
            }
        ]
    )
    assert parsed[0]["parentCandidateId"] is None
    assert parsed[0]["parentCandidateIdentitySha256"] == parent_identity
    assert parsed[0]["constructionKind"] == "mutation_trace"

    run_root = _build_fixture(tmp_path)
    _write_jsonl(
        run_root / "generations" / "generation-0001" / "proposal" / "parent-material.jsonl",
        [
            {
                "candidateId": "child-a",
                "candidateIdentitySha256": child_identity,
                "pairPayload": {
                    "acceptedRecord": {
                        "candidateId": "child-a",
                        "candidateIdentitySha256": child_identity,
                        "originKind": "qd_structural_offspring",
                        "constructionAudit": {
                            "kind": "mutation_trace",
                            "evolvedAudit": {
                                "kind": "mutation_trace",
                                "parentCandidateIdentitySha256": parent_identity,
                            },
                        },
                    },
                    "proposalDelta": {
                        "originKind": "qd_structural_offspring",
                        "scheduledKind": "qd_structural_offspring",
                    },
                },
            },
            {
                "candidateId": "immigrant-a",
                "pairPayload": {
                    "proposalDelta": {
                        "originKind": "qd_random_immigrant",
                        "scheduledKind": "qd_random_immigrant",
                    }
                },
            },
        ],
    )
    audit = audit_temporal_qd_generation_quality(run_root, 1)
    parent_row = next(
        row for row in audit["parentYield"] if row["parentCandidateId"] == "parent-a"
    )
    assert parent_row["offspringConstructedCount"] == 1
    mutation = next(
        row
        for row in audit["constructionKindYield"]
        if row["constructionKind"] == "mutation_trace"
    )
    assert mutation["constructedCandidateCount"] == 1
    assert mutation["samePanelParentRelative"]["comparisonCount"] == 1
    assert mutation["samePanelParentRelative"]["meanParentRelativeConservativeNetR"] == pytest.approx(
        3.2
    )


def test_previous_generation_parent_relative_uses_prior_evals(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path, generation_index=1, include_parent_eval=True)
    _build_fixture(tmp_path, generation_index=2, include_parent_eval=False)
    audit = audit_temporal_qd_generation_quality(run_root, 2)
    offspring = next(
        row for row in audit["constructionKindYield"] if row["constructionKind"] != "immigrant"
    )
    assert offspring["samePanelParentRelative"]["comparisonCount"] == 0
    assert offspring["previousGenerationParentRelative"]["comparisonCount"] == 1
    assert offspring["previousGenerationParentRelative"][
        "meanParentRelativeConservativeNetR"
    ] == pytest.approx(3.2)


def test_observe_unlinks_stale_audit_error(tmp_path: Path) -> None:
    run_root = _build_fixture(tmp_path)
    error_path = (
        run_root
        / "generations"
        / "generation-0001"
        / "quality-audit"
        / "audit-error.json"
    )
    _write(error_path, {"schemaVersion": AUDIT_SCHEMA, "message": "stale"})
    result = observe_generation_quality_audit(run_root, 1)
    assert result["status"] == "ok"
    assert not error_path.is_file()
    assert (
        run_root
        / "generations"
        / "generation-0001"
        / "quality-audit"
        / "generation-quality-audit.json"
    ).is_file()

