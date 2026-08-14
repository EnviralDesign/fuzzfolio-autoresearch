import json
from pathlib import Path

import pytest

from autoresearch.evidence_plan import canonical_sha256
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_post_g5_audit import (
    DECISION_HOLD,
    DECISION_PROCEED,
    DECISION_STOP,
    LADDER_SUMMARY_SCHEMA,
    audit_temporal_qd_g5,
)


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")


def _run(tmp_path: Path) -> Path:
    root = tmp_path / "run"
    config = {
            "generationPlan": {
                "lastGenerationIndex": 5,
                "targetUniqueCandidatesPerGeneration": 1024,
            },
            "g0Bootstrap": {"initialConstructionPoolSize": 4000},
    }
    config["configSha256"] = canonical_sha256(config)
    _write(root / "config.json", config)
    records = []
    previous = "candidate-0"
    for generation in range(1, 6):
        candidate_id = previous if generation == 1 else f"candidate-{generation}"
        member = {
            "candidateId": candidate_id,
            "candidate": {
                "candidateId": candidate_id,
                "sourceMode": "qd_structural_offspring" if generation > 1 else "qd_random_immigrant",
            },
            "objectives": {"worstWindowConservativeNetR": float(generation)},
            "robustObjectives": {"worstWindowConservativeNetR": float(generation)},
        }
        archive = {
            "schemaVersion": "temporal_qd_archive_v3",
            "generationIndex": generation,
            "cellCapacity": 4,
            "cells": [{"cellId": "cell-a", "members": [member]}],
            "memberCount": 1,
            "occupiedCellCount": 1,
            "newCellCount": 1,
            "qualityMemberCount": 1,
            "observationalMemberCount": 0,
            "negativeNoveltyMemberCount": 0,
        }
        archive["archiveSha256"] = canonical_sha256(archive)
        archive_path = (
            root
            / "generations"
            / f"generation-{generation:04d}"
            / "native-finalization"
            / "archive.json"
        )
        _write(archive_path, archive)
        proposal = {
            "schemaVersion": "temporal_qd_v5_fast_ephemeral_result_v1",
            "generationIndex": generation,
            "attemptCount": 4000 if generation == 1 else 1024,
            "acceptedCandidateCount": 1024,
            "selectedEvaluationCandidateCount": 1024,
            "timings": {"totalMilliseconds": 10},
        }
        proposal["resultSha256"] = canonical_sha256(proposal)
        _write(
            root
            / "generations"
            / f"generation-{generation:04d}"
            / "proposal"
            / "native-batch"
            / "v5-proposal"
            / "fixture"
            / "v5-proposal-result.json",
            proposal,
        )
        record = {
            "schemaVersion": "temporal_qd_v5_fast_ephemeral_generation_record_v1",
            "generationIndex": generation,
            "candidateCount": 1024,
            "totalGenerationTaskCount": 1024,
            "archivePath": "archive.json",
            "archiveSha256": archive["archiveSha256"],
        }
        record["generationRecordSha256"] = canonical_sha256(record)
        records.append(record)
        previous = candidate_id
    state = {
        "status": "completed",
        "configSha256": config["configSha256"],
        "completedGenerations": records,
    }
    state["stateSha256"] = canonical_sha256(state)
    _write(root / "state.json", state)
    return root


def _ladder_summary(root: Path, *, gains: list[float], confidence: bool) -> dict[str, object]:
    config = json.loads((root / "config.json").read_text(encoding="utf-8"))
    archive = json.loads(
        (
            root
            / "generations"
            / "generation-0005"
            / "native-finalization"
            / "archive.json"
        ).read_text(encoding="utf-8")
    )
    summary: dict[str, object] = {
        "schemaVersion": LADDER_SUMMARY_SCHEMA,
        "configSha256": config["configSha256"],
        "sourceGenerationIndex": 5,
        "sourceArchiveSha256": archive["archiveSha256"],
        "ladderAuthoritySha256": "sha256:" + "1" * 64,
        "validationTailAuthoritySha256": "sha256:" + "2" * 64,
        "scrutinyTailAuthoritySha256": "sha256:" + "3" * 64,
        "commonPanelHypervolumeGainPercent": gains,
        "confidenceIntervalsExcludeZero": confidence,
    }
    summary["summarySha256"] = canonical_sha256(summary)
    return summary


def test_post_g5_audit_holds_until_the_ladder_and_then_allows_g10(tmp_path: Path) -> None:
    root = _run(tmp_path)
    held = audit_temporal_qd_g5(root)
    assert held["decision"] == DECISION_HOLD
    ladder = tmp_path / "ladder.json"
    _write(
        ladder,
        _ladder_summary(root, gains=[2.0, 1.5], confidence=True),
    )
    allowed = audit_temporal_qd_g5(root, ladder_summary_path=ladder)
    assert allowed["decision"] == DECISION_PROCEED
    assert allowed["auditSha256"] == canonical_sha256(
        {key: value for key, value in allowed.items() if key != "auditSha256"}
    )


def test_post_g5_audit_stops_on_plateau_or_archive_corruption(tmp_path: Path) -> None:
    root = _run(tmp_path)
    ladder = tmp_path / "ladder.json"
    _write(
        ladder,
        _ladder_summary(root, gains=[0.8, 0.4], confidence=True),
    )
    assert audit_temporal_qd_g5(root, ladder_summary_path=ladder)["decision"] == DECISION_STOP

    archive_path = (
        root
        / "generations"
        / "generation-0005"
        / "native-finalization"
        / "archive.json"
    )
    archive = json.loads(archive_path.read_text(encoding="utf-8"))
    archive["memberCount"] = 2
    archive["archiveSha256"] = canonical_sha256(
        {key: value for key, value in archive.items() if key != "archiveSha256"}
    )
    _write(archive_path, archive)
    corrupted = audit_temporal_qd_g5(root)
    assert corrupted["decision"] == DECISION_STOP
    assert any("member count" in value for value in corrupted["hardFailures"])


def test_post_g5_audit_rejects_unbound_ladder_and_nonnumeric_objectives(
    tmp_path: Path,
) -> None:
    root = _run(tmp_path)
    ladder = tmp_path / "ladder.json"
    forged = _ladder_summary(root, gains=[2.0, 1.5], confidence=True)
    forged["sourceArchiveSha256"] = "sha256:" + "9" * 64
    forged["summarySha256"] = canonical_sha256(
        {key: value for key, value in forged.items() if key != "summarySha256"}
    )
    _write(ladder, forged)
    with pytest.raises(TemporalDiscoveryContractError, match="not bound"):
        audit_temporal_qd_g5(root, ladder_summary_path=ladder)

    archive_path = (
        root
        / "generations"
        / "generation-0005"
        / "native-finalization"
        / "archive.json"
    )
    archive = json.loads(archive_path.read_text(encoding="utf-8"))
    archive["cells"][0]["members"][0]["objectives"] = {"score": None}
    archive["archiveSha256"] = canonical_sha256(
        {key: value for key, value in archive.items() if key != "archiveSha256"}
    )
    _write(archive_path, archive)
    state = json.loads((root / "state.json").read_text(encoding="utf-8"))
    state["completedGenerations"][-1]["archiveSha256"] = archive["archiveSha256"]
    record = state["completedGenerations"][-1]
    record["generationRecordSha256"] = canonical_sha256(
        {key: value for key, value in record.items() if key != "generationRecordSha256"}
    )
    state["stateSha256"] = canonical_sha256(
        {key: value for key, value in state.items() if key != "stateSha256"}
    )
    _write(root / "state.json", state)
    malformed = audit_temporal_qd_g5(root)
    assert malformed["decision"] == DECISION_STOP
    assert any("non-finite objectives" in value for value in malformed["hardFailures"])


@pytest.mark.parametrize("gains", [[], [float("nan"), float("nan")], [True, True]])
def test_post_g5_audit_rejects_malformed_ladder_gains(
    tmp_path: Path,
    gains: list[object],
) -> None:
    root = _run(tmp_path)
    ladder = tmp_path / "ladder.json"
    summary = _ladder_summary(root, gains=[2.0, 1.5], confidence=True)
    summary["commonPanelHypervolumeGainPercent"] = gains
    summary["summarySha256"] = canonical_sha256(
        {key: value for key, value in summary.items() if key != "summarySha256"}
    )
    _write(ladder, summary)
    with pytest.raises(TemporalDiscoveryContractError, match="common-panel gains"):
        audit_temporal_qd_g5(root, ladder_summary_path=ladder)
