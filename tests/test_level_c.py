from __future__ import annotations

import json
import hashlib
import shutil
from pathlib import Path

import pytest

from autoresearch.evidence_plan import build_replay_evidence_plan
from autoresearch.level_c import (
    LevelCCohortError,
    bind_level_c_evidence_rows,
    cohort_attempt_ids,
    freeze_level_c_cohort,
    validate_level_c_cohort,
)
from autoresearch.evidence_artifacts import (
    build_evidence_artifact_manifest,
    evidence_artifact_paths,
    write_immutable_json,
)
from autoresearch.ledger import write_run_metadata
from autoresearch.instrument_universe import universe_provenance


def _fixture(tmp_path: Path) -> tuple[Path, Path, str, str]:
    cutoff = "2025-06-30T00:00:00Z"
    lake = "sha256:" + "a" * 64
    universe = universe_provenance()
    lineage = {
        "research_generation_id": "generation-001",
        "level_c_protocol_id": "sha256:" + "b" * 64,
        "cutoff_key": "A",
        "as_of_date": cutoff,
        "lake_manifest_sha256": lake,
        "source_snapshot_sha256": "sha256:" + "c" * 64,
        "universe_id": universe["universe_id"],
        "universe_manifest_sha256": universe["universe_hash"],
        "execution_plan_id": "sha256:" + "d" * 64,
    }
    runs_root = tmp_path / "runs"
    atlas_root = runs_root / "derived" / "atlas-runs" / "atlas-1"
    atlas_root.mkdir(parents=True)
    (atlas_root / "atlas-lab-run.json").write_text(
        json.dumps(
            {
                "status": "completed",
                "runtime": {
                    "as_of_date": cutoff,
                    "lake_manifest_sha256": lake,
                    "signal_atlas_executor": "gateway",
                    **lineage,
                },
                "historical_lineage": lineage,
                "universe_contract": universe,
            }
        ),
        encoding="utf-8",
    )
    (atlas_root / "signal-atlas.json").write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "status": "ok",
                        "evidence_plan_id": "sha256:" + "1" * 64,
                        "observed_lake_manifest_sha256": lake,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    atlas_seed_plan = atlas_root / "recipe-priors" / "play-hand-seed-plan.json"
    atlas_seed_plan.parent.mkdir(parents=True)
    atlas_seed_plan.write_text(
        json.dumps({"recipes": {}, "historical_lineage": lineage}), encoding="utf-8"
    )
    recipe_priors_path = atlas_root / "recipe-priors" / "recipe-priors.json"
    summary_path = atlas_root / "recipe-priors" / "recipe-priors-summary.json"
    recipe_priors_path.write_text(
        json.dumps({"recipes": {}, "historical_lineage": lineage}), encoding="utf-8"
    )
    summary_path.write_text(json.dumps({"historical_lineage": lineage}), encoding="utf-8")
    lineage_path = atlas_root / "recipe-priors" / "level-c-lineage.json"
    lineage_path.write_text(
        json.dumps(
            {
                "schema_version": "atlas_level_c_lineage_v1",
                "historical_lineage": lineage,
                "artifact_sha256": {
                    path.name: "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in (atlas_seed_plan, recipe_priors_path, summary_path)
                },
            }
        ),
        encoding="utf-8",
    )

    campaign_id = "playhand-campaign-1"
    campaign_root = runs_root / "derived" / "play-hand-lab-campaigns" / campaign_id
    campaign_summary_path = campaign_root / "play-hand-lab-campaign-summary.json"
    campaign_summary_path.parent.mkdir(parents=True, exist_ok=True)
    campaign_summary_path.write_text(
        json.dumps(
            {
                "campaign_id": campaign_id,
                "status": "completed",
                "target_runs": 1,
                "lane_count": 1,
                "retained_lane_count": 1,
                "pruned_lane_count": 0,
                "lanes_truncated": False,
                "total_tasks": 1,
                "completed_tasks": 1,
                "failed_tasks": 0,
                "recorded_result_count": 1,
                "lanes": [
                    {
                        "run_id": "lane-1",
                        "run_dir": str((runs_root / "lane-1").resolve()),
                        "task_ids": ["lane-1-task-00001"],
                        "completed_task_count": 1,
                        "terminal": True,
                        "failed_task_count": 0,
                        "run_promoted": True,
                        "terminal_outcome_category": "promoted",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    write_run_metadata(
        campaign_root,
        {
            "schema_version": "play_hand_lab_campaign_v1",
            "runner": "play_hand_v1",
            "generated_by_runner": "play_hand_lab_v1",
            "run_kind": "play_hand_lab_campaign",
            "run_id": campaign_id,
            "run_status": "completed",
            "campaign_mode": "finite",
            "target_runs": 1,
            "formal_historical_level_c": True,
            "failed_task_count": 0,
            "total_task_count": 1,
            "completed_task_count": 1,
            "historical_completion_failure_reason": None,
            "as_of_date": cutoff,
            "lake_manifest_sha256": lake,
            "research_generation_id": lineage["research_generation_id"],
            "level_c_protocol_id": lineage["level_c_protocol_id"],
            "cutoff_key": lineage["cutoff_key"],
            "execution_plan_id": lineage["execution_plan_id"],
            "play_hand_seed_plan_path": str(atlas_seed_plan.resolve()),
            "play_hand_seed_plan_sha256": "sha256:"
            + hashlib.sha256(atlas_seed_plan.read_bytes()).hexdigest(),
            "summary_path": str(campaign_summary_path.resolve()),
        },
    )
    lane_root = runs_root / "lane-1"
    profile_path = lane_root / "profile.json"
    profile_path.parent.mkdir(parents=True)
    profile = {"name": "Bounded", "notificationThreshold": 80}
    profile_path.write_text(json.dumps(profile), encoding="utf-8")
    plan = build_replay_evidence_plan(
        campaign_plan_id="playhand-lab:lane-1",
        evidence_role="training",
        selection_data_end=cutoff,
        analysis_window_start="2022-06-30T00:00:00Z",
        analysis_window_end=cutoff,
        requested_horizon_months=36,
        profile_snapshot=profile,
        lake_manifest_sha256=lake,
        data_availability_cutoff=cutoff,
    )
    attempt_id = "lane-1-attempt-00001"
    write_run_metadata(
        lane_root,
        {
            "schema_version": "play_hand_lab_lane_v1",
            "runner": "play_hand_v1",
            "generated_by_runner": "play_hand_lab_v1",
            "run_kind": "play_hand_lab_lane",
            "run_id": "lane-1",
            "run_status": "promoted",
            "terminal": True,
            "failed_task_count": 0,
            "parent_campaign_id": campaign_id,
            "lab_campaign_id": campaign_id,
            "campaign_dir": str(campaign_root.resolve()),
            "lab_lane_index": 0,
            "canonical_attempt_id": attempt_id,
            "as_of_date": cutoff,
            "lake_manifest_sha256": lake,
            "research_generation_id": lineage["research_generation_id"],
            "level_c_protocol_id": lineage["level_c_protocol_id"],
            "cutoff_key": lineage["cutoff_key"],
            "execution_plan_id": lineage["execution_plan_id"],
        },
    )
    (lane_root / "attempts.jsonl").write_text(
        json.dumps(
            {
                "attempt_id": attempt_id,
                "run_id": "lane-1",
                "runner": "play_hand_v1",
                "play_hand_stage": "final_36mo",
                "profile_path": str(profile_path),
                "evidence_plan": plan.model_dump(mode="json"),
                "execution_evidence": {
                    "plan_id": plan.plan_id,
                    "profile_snapshot_sha256": plan.profile_snapshot_sha256,
                    "execution_cell_sha256": plan.execution_cell_sha256,
                    "observed_lake_manifest_sha256": lake,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return runs_root, atlas_root, cutoff, lake


def _artifact_hashes_for_fixture(runs_root: Path, atlas_root: Path) -> dict[str, str]:
    roots = [
        ("atlas", atlas_root),
        (
            "playhand-campaign",
            runs_root / "derived" / "play-hand-lab-campaigns" / "playhand-campaign-1",
        ),
        ("playhand-lanes/lane-1", runs_root / "lane-1"),
    ]
    hashes: dict[str, str] = {}
    for namespace, root in roots:
        for path in sorted(item for item in root.rglob("*") if item.is_file()):
            hashes[f"{namespace}/{path.relative_to(root).as_posix()}"] = "sha256:" + hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
    return hashes


def _rewrite_manifest_id(payload: dict[str, object]) -> None:
    identity = {key: value for key, value in payload.items() if key != "manifest_id"}
    payload["manifest_id"] = "sha256:" + hashlib.sha256(
        json.dumps(identity, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _make_fixture_no_signal(runs_root: Path) -> None:
    lane_metadata_path = runs_root / "lane-1" / "run-metadata.json"
    lane_metadata = json.loads(lane_metadata_path.read_text(encoding="utf-8"))
    lane_metadata["run_status"] = "no_signal"
    lane_metadata["canonical_attempt_id"] = ""
    lane_metadata["terminal"] = True
    lane_metadata["failed_task_count"] = 0
    lane_metadata["tombstone_reason"] = "no_signal"
    lane_metadata_path.write_text(json.dumps(lane_metadata), encoding="utf-8")
    summary_path = (
        runs_root
        / "derived"
        / "play-hand-lab-campaigns"
        / "playhand-campaign-1"
        / "play-hand-lab-campaign-summary.json"
    )
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["lanes"][0].update(
        {
            "run_promoted": False,
            "terminal_outcome_category": "research_nonviable",
            "tombstone_reason": "no_signal",
        }
    )
    summary_path.write_text(json.dumps(summary), encoding="utf-8")


def _campaign_summary_path(runs_root: Path) -> Path:
    return (
        runs_root
        / "derived"
        / "play-hand-lab-campaigns"
        / "playhand-campaign-1"
        / "play-hand-lab-campaign-summary.json"
    )


def _add_tombstoned_fixture_lane(runs_root: Path) -> None:
    campaign_root = runs_root / "derived" / "play-hand-lab-campaigns" / "playhand-campaign-1"
    first_lane = runs_root / "lane-1"
    second_lane = runs_root / "lane-2"
    shutil.copytree(first_lane, second_lane)
    metadata_path = second_lane / "run-metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata.update(
        {
            "run_id": "lane-2",
            "run_status": "tombstoned",
            "terminal": True,
            "failed_task_count": 0,
            "canonical_attempt_id": None,
            "lab_lane_index": 1,
            "campaign_dir": str(campaign_root.resolve()),
            "tombstone_reason": "validation_score_below_45",
            "terminal_outcome_category": "research_nonviable",
            "run_promoted": False,
        }
    )
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    summary_path = _campaign_summary_path(runs_root)
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.update(
        {
            "target_runs": 2,
            "lane_count": 2,
            "retained_lane_count": 2,
            "pruned_lane_count": 0,
            "total_tasks": 2,
            "completed_tasks": 2,
            "failed_tasks": 0,
            "recorded_result_count": 2,
        }
    )
    summary["lanes"].append(
        {
            "run_id": "lane-2",
            "run_dir": str(second_lane.resolve()),
            "task_ids": ["lane-2-task-00001"],
            "completed_task_count": 1,
            "terminal": True,
            "failed_task_count": 0,
            "run_promoted": False,
            "terminal_outcome_category": "research_nonviable",
            "tombstone_reason": "validation_score_below_45",
        }
    )
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    campaign_metadata_path = campaign_root / "run-metadata.json"
    campaign_metadata = json.loads(campaign_metadata_path.read_text(encoding="utf-8"))
    campaign_metadata.update(
        {
            "target_runs": 2,
            "total_task_count": 2,
            "completed_task_count": 2,
            "failed_task_count": 0,
        }
    )
    campaign_metadata_path.write_text(json.dumps(campaign_metadata), encoding="utf-8")


def test_freeze_and_validate_level_c_cohort(tmp_path: Path) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    output = runs_root / "derived" / "level-c-cohorts" / "cohort-1.json"

    payload = freeze_level_c_cohort(
        runs_root=runs_root,
        atlas_run_root=atlas_root,
        playhand_campaign_id="playhand-campaign-1",
        as_of_date=cutoff,
        lake_manifest_sha256=lake,
        output_path=output,
        cohort_id="cohort-1",
    )

    assert payload["candidate_count"] == 1
    assert payload["playhand_campaign_root"] == str(
        (
            runs_root
            / "derived"
            / "play-hand-lab-campaigns"
            / "playhand-campaign-1"
        ).resolve()
    )
    assert payload["playhand_campaign_layout"] == "playhand_lab_v2"
    assert payload["historical_lineage"]["research_generation_id"] == "generation-001"
    assert payload["historical_lineage"]["cutoff_key"] == "A"
    assert payload["candidates"][0]["attempt_id"] == "lane-1-attempt-00001"
    assert validate_level_c_cohort(output)["manifest_id"] == payload["manifest_id"]


def test_level_c_cohort_freezes_and_validates_audible_no_defensible_candidates(
    tmp_path: Path,
) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    _make_fixture_no_signal(runs_root)
    output = runs_root / "derived" / "level-c-cohorts" / "cohort-no-signal.json"

    payload = freeze_level_c_cohort(
        runs_root=runs_root,
        atlas_run_root=atlas_root,
        playhand_campaign_id="playhand-campaign-1",
        as_of_date=cutoff,
        lake_manifest_sha256=lake,
        output_path=output,
        cohort_id="cohort-no-signal",
    )

    assert payload["candidate_count"] == 0
    assert payload["candidates"] == []
    assert payload["outcome"] == "no_defensible_candidates"
    assert payload["outcome_reason"] == "no_canonical_cutoff_bounded_candidates"
    assert cohort_attempt_ids(payload) == []
    assert validate_level_c_cohort(output)["outcome"] == "no_defensible_candidates"
    bound, rejected = bind_level_c_evidence_rows(
        [{"attempt_id": "not-a-candidate", "artifact_dir": str(tmp_path)}], cohort=payload
    )
    assert bound == []
    assert rejected == [{"attempt_id": "not-a-candidate", "reason": "outside_frozen_cohort"}]


def test_level_c_cohort_accepts_current_all_tombstoned_summary_shape(
    tmp_path: Path,
) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    _make_fixture_no_signal(runs_root)
    summary_path = _campaign_summary_path(runs_root)
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert "terminal_lanes" not in summary
    output = runs_root / "derived" / "level-c-cohorts" / "all-tombstoned.json"

    payload = freeze_level_c_cohort(
        runs_root=runs_root,
        atlas_run_root=atlas_root,
        playhand_campaign_id="playhand-campaign-1",
        as_of_date=cutoff,
        lake_manifest_sha256=lake,
        output_path=output,
        cohort_id="all-tombstoned",
    )

    assert payload["candidate_count"] == 0
    assert payload["outcome"] == "no_defensible_candidates"
    assert validate_level_c_cohort(output)["outcome"] == "no_defensible_candidates"


def test_level_c_cohort_preserves_mixed_promoted_and_nonviable_lanes(
    tmp_path: Path,
) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    _add_tombstoned_fixture_lane(runs_root)
    output = runs_root / "derived" / "level-c-cohorts" / "mixed.json"

    payload = freeze_level_c_cohort(
        runs_root=runs_root,
        atlas_run_root=atlas_root,
        playhand_campaign_id="playhand-campaign-1",
        as_of_date=cutoff,
        lake_manifest_sha256=lake,
        output_path=output,
        cohort_id="mixed",
    )

    assert payload["outcome"] == "candidates_frozen"
    assert [row["attempt_id"] for row in payload["candidates"]] == [
        "lane-1-attempt-00001"
    ]


def test_level_c_cohort_rejects_malformed_current_summary_task_accounting(
    tmp_path: Path,
) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    summary_path = _campaign_summary_path(runs_root)
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["completed_tasks"] = 0
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    with pytest.raises(LevelCCohortError, match="summary task accounting is invalid"):
        freeze_level_c_cohort(
            runs_root=runs_root,
            atlas_run_root=atlas_root,
            playhand_campaign_id="playhand-campaign-1",
            as_of_date=cutoff,
            lake_manifest_sha256=lake,
            output_path=runs_root / "derived" / "level-c-cohorts" / "malformed.json",
            cohort_id="malformed",
        )


def test_level_c_cohort_ignores_explicit_forensic_lane_quarantine(
    tmp_path: Path,
) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    shutil.copytree(
        runs_root / "lane-1",
        runs_root / "lane-1.corrupt-legacy-result-identity-20260718T000000Z",
    )
    output = runs_root / "derived" / "level-c-cohorts" / "quarantine-safe.json"

    payload = freeze_level_c_cohort(
        runs_root=runs_root,
        atlas_run_root=atlas_root,
        playhand_campaign_id="playhand-campaign-1",
        as_of_date=cutoff,
        lake_manifest_sha256=lake,
        output_path=output,
        cohort_id="quarantine-safe",
    )

    assert payload["candidate_count"] == 1


def test_level_c_cohort_ignores_malformed_explicit_forensic_lane_quarantine(
    tmp_path: Path,
) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    quarantine = runs_root / "lane-1.corrupt-legacy-result-identity-20260718T000000Z"
    quarantine.mkdir()
    (quarantine / "run-metadata.json").write_text("not-json", encoding="utf-8")
    output = runs_root / "derived" / "level-c-cohorts" / "malformed-quarantine.json"

    payload = freeze_level_c_cohort(
        runs_root=runs_root,
        atlas_run_root=atlas_root,
        playhand_campaign_id="playhand-campaign-1",
        as_of_date=cutoff,
        lake_manifest_sha256=lake,
        output_path=output,
        cohort_id="malformed-quarantine",
    )

    assert payload["candidate_count"] == 1


def test_level_c_cohort_rejects_summary_and_metadata_counts_that_disagree_with_receipts(
    tmp_path: Path,
) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    summary_path = _campaign_summary_path(runs_root)
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.update({"total_tasks": 0, "completed_tasks": 0, "recorded_result_count": 0})
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    campaign_metadata_path = summary_path.parent / "run-metadata.json"
    campaign_metadata = json.loads(campaign_metadata_path.read_text(encoding="utf-8"))
    campaign_metadata.update({"total_task_count": 0, "completed_task_count": 0})
    campaign_metadata_path.write_text(json.dumps(campaign_metadata), encoding="utf-8")

    with pytest.raises(LevelCCohortError, match="completed task receipts mismatch"):
        freeze_level_c_cohort(
            runs_root=runs_root,
            atlas_run_root=atlas_root,
            playhand_campaign_id="playhand-campaign-1",
            as_of_date=cutoff,
            lake_manifest_sha256=lake,
            output_path=runs_root / "derived" / "level-c-cohorts" / "receipt-mismatch.json",
            cohort_id="receipt-mismatch",
        )


def test_level_c_legacy_summary_without_v2_task_counters_remains_valid(
    tmp_path: Path,
) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    campaign_id = "playhand-campaign-1"
    v2_root = runs_root / "derived" / "play-hand-lab-campaigns" / campaign_id
    legacy_root = runs_root / campaign_id
    v2_root.rename(legacy_root)
    summary_path = legacy_root / "play-hand-lab-campaign-summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.pop("lanes_truncated")
    summary.pop("total_tasks")
    summary.pop("completed_tasks")
    summary.pop("recorded_result_count")
    summary["terminal_lanes"] = 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    campaign_metadata_path = legacy_root / "run-metadata.json"
    campaign_metadata = json.loads(campaign_metadata_path.read_text(encoding="utf-8"))
    campaign_metadata.pop("total_task_count")
    campaign_metadata.pop("completed_task_count")
    campaign_metadata["summary_path"] = str(summary_path.resolve())
    campaign_metadata_path.write_text(json.dumps(campaign_metadata), encoding="utf-8")
    lane_metadata_path = runs_root / "lane-1" / "run-metadata.json"
    lane_metadata = json.loads(lane_metadata_path.read_text(encoding="utf-8"))
    lane_metadata["campaign_dir"] = str(legacy_root.resolve())
    lane_metadata_path.write_text(json.dumps(lane_metadata), encoding="utf-8")

    payload = freeze_level_c_cohort(
        runs_root=runs_root,
        atlas_run_root=atlas_root,
        playhand_campaign_id=campaign_id,
        as_of_date=cutoff,
        lake_manifest_sha256=lake,
        output_path=runs_root / "derived" / "level-c-cohorts" / "legacy.json",
        cohort_id="legacy",
    )

    assert payload["candidate_count"] == 1
    assert payload["playhand_campaign_layout"] == "legacy_top_level"


def test_level_c_cohort_rejects_tampered_no_defensible_candidate_outcome(
    tmp_path: Path,
) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    _make_fixture_no_signal(runs_root)
    output = runs_root / "derived" / "level-c-cohorts" / "cohort-no-signal.json"
    freeze_level_c_cohort(
        runs_root=runs_root,
        atlas_run_root=atlas_root,
        playhand_campaign_id="playhand-campaign-1",
        as_of_date=cutoff,
        lake_manifest_sha256=lake,
        output_path=output,
        cohort_id="cohort-no-signal",
    )
    forged = json.loads(output.read_text(encoding="utf-8"))
    forged["outcome"] = "candidates_frozen"
    forged["outcome_reason"] = None
    _rewrite_manifest_id(forged)
    output.write_text(json.dumps(forged), encoding="utf-8")

    with pytest.raises(LevelCCohortError, match="Frozen Level C outcome mismatch"):
        validate_level_c_cohort(output)


def test_level_c_cohort_rejects_no_signal_evidence_drift(tmp_path: Path) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    _make_fixture_no_signal(runs_root)
    output = runs_root / "derived" / "level-c-cohorts" / "cohort-no-signal.json"
    freeze_level_c_cohort(
        runs_root=runs_root,
        atlas_run_root=atlas_root,
        playhand_campaign_id="playhand-campaign-1",
        as_of_date=cutoff,
        lake_manifest_sha256=lake,
        output_path=output,
        cohort_id="cohort-no-signal",
    )
    lane_metadata_path = runs_root / "lane-1" / "run-metadata.json"
    lane_metadata = json.loads(lane_metadata_path.read_text(encoding="utf-8"))
    lane_metadata["audit_note"] = "tampered after freeze"
    lane_metadata_path.write_text(json.dumps(lane_metadata), encoding="utf-8")

    with pytest.raises(LevelCCohortError, match="evidence inventory mismatch"):
        validate_level_c_cohort(output)


def test_level_c_cohort_rejects_incomplete_lane_as_zero_candidate_outcome(
    tmp_path: Path,
) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    lane_metadata_path = runs_root / "lane-1" / "run-metadata.json"
    lane_metadata = json.loads(lane_metadata_path.read_text(encoding="utf-8"))
    lane_metadata.update(
        {
            "run_status": "running",
            "canonical_attempt_id": "",
            "terminal": False,
            "tombstone_reason": None,
        }
    )
    lane_metadata_path.write_text(json.dumps(lane_metadata), encoding="utf-8")

    with pytest.raises(LevelCCohortError, match="is not terminal"):
        freeze_level_c_cohort(
            runs_root=runs_root,
            atlas_run_root=atlas_root,
            playhand_campaign_id="playhand-campaign-1",
            as_of_date=cutoff,
            lake_manifest_sha256=lake,
            output_path=runs_root / "derived" / "level-c-cohorts" / "bad.json",
            cohort_id="bad",
        )


def test_level_c_cohort_rejects_missing_campaign_lane_as_zero_candidate_outcome(
    tmp_path: Path,
) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    _make_fixture_no_signal(runs_root)
    (runs_root / "lane-1").rename(tmp_path / "removed-lane-1")

    with pytest.raises(LevelCCohortError, match="lane accounting mismatch"):
        freeze_level_c_cohort(
            runs_root=runs_root,
            atlas_run_root=atlas_root,
            playhand_campaign_id="playhand-campaign-1",
            as_of_date=cutoff,
            lake_manifest_sha256=lake,
            output_path=runs_root / "derived" / "level-c-cohorts" / "missing.json",
            cohort_id="missing",
        )


def test_level_c_cohort_validates_after_whole_runs_root_relocation(tmp_path: Path) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    output = runs_root / "derived" / "level-c-cohorts" / "cohort-1.json"
    payload = freeze_level_c_cohort(
        runs_root=runs_root,
        atlas_run_root=atlas_root,
        playhand_campaign_id="playhand-campaign-1",
        as_of_date=cutoff,
        lake_manifest_sha256=lake,
        output_path=output,
        cohort_id="cohort-1",
    )
    archived_runs_root = tmp_path / "runs_archive" / "archive-001" / "runs"
    archived_runs_root.parent.mkdir(parents=True)
    runs_root.replace(archived_runs_root)
    archived_output = archived_runs_root / "derived" / "level-c-cohorts" / "cohort-1.json"

    assert validate_level_c_cohort(archived_output)["manifest_id"] == payload["manifest_id"]


def test_level_c_cohort_rejects_unsafe_relocated_root_or_evidence_root(
    tmp_path: Path,
) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    output = runs_root / "derived" / "level-c-cohorts" / "cohort-1.json"
    freeze_level_c_cohort(
        runs_root=runs_root,
        atlas_run_root=atlas_root,
        playhand_campaign_id="playhand-campaign-1",
        as_of_date=cutoff,
        lake_manifest_sha256=lake,
        output_path=output,
        cohort_id="cohort-1",
    )
    outside_root = tmp_path / "outside"
    outside_root.mkdir()
    with pytest.raises(LevelCCohortError, match="evidence root does not exist"):
        validate_level_c_cohort(output, relocated_runs_root=outside_root)

    forged = json.loads(output.read_text(encoding="utf-8"))
    forged["evidence_roots"]["atlas_run_root"] = "../outside"
    _rewrite_manifest_id(forged)
    output.write_text(json.dumps(forged), encoding="utf-8")
    with pytest.raises(LevelCCohortError, match="must be a non-empty relative path"):
        validate_level_c_cohort(output)


def test_level_c_cohort_rejects_playhand_protocol_lineage_mismatch(tmp_path: Path) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    campaign_metadata_path = (
        runs_root
        / "derived"
        / "play-hand-lab-campaigns"
        / "playhand-campaign-1"
        / "run-metadata.json"
    )
    campaign_metadata = json.loads(campaign_metadata_path.read_text(encoding="utf-8"))
    campaign_metadata["level_c_protocol_id"] = "sha256:" + "d" * 64
    campaign_metadata_path.write_text(json.dumps(campaign_metadata), encoding="utf-8")

    with pytest.raises(LevelCCohortError, match="level_c_protocol_id mismatch"):
        freeze_level_c_cohort(
            runs_root=runs_root,
            atlas_run_root=atlas_root,
            playhand_campaign_id="playhand-campaign-1",
            as_of_date=cutoff,
            lake_manifest_sha256=lake,
            output_path=runs_root / "derived" / "level-c-cohorts" / "bad.json",
            cohort_id="bad",
        )


def test_level_c_cohort_rejects_tampered_atlas_seed_lineage(tmp_path: Path) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    seed_plan_path = atlas_root / "recipe-priors" / "play-hand-seed-plan.json"
    seed_plan = json.loads(seed_plan_path.read_text(encoding="utf-8"))
    seed_plan["historical_lineage"]["cutoff_key"] = "B"
    seed_plan_path.write_text(json.dumps(seed_plan), encoding="utf-8")
    campaign_metadata_path = (
        runs_root
        / "derived"
        / "play-hand-lab-campaigns"
        / "playhand-campaign-1"
        / "run-metadata.json"
    )
    campaign_metadata = json.loads(campaign_metadata_path.read_text(encoding="utf-8"))
    campaign_metadata["play_hand_seed_plan_sha256"] = "sha256:" + hashlib.sha256(
        seed_plan_path.read_bytes()
    ).hexdigest()
    campaign_metadata_path.write_text(json.dumps(campaign_metadata), encoding="utf-8")
    lineage_path = atlas_root / "recipe-priors" / "level-c-lineage.json"
    lineage_artifact = json.loads(lineage_path.read_text(encoding="utf-8"))
    lineage_artifact["artifact_sha256"][seed_plan_path.name] = campaign_metadata[
        "play_hand_seed_plan_sha256"
    ]
    lineage_path.write_text(json.dumps(lineage_artifact), encoding="utf-8")

    with pytest.raises(LevelCCohortError, match="seed plan lineage mismatch"):
        freeze_level_c_cohort(
            runs_root=runs_root,
            atlas_run_root=atlas_root,
            playhand_campaign_id="playhand-campaign-1",
            as_of_date=cutoff,
            lake_manifest_sha256=lake,
            output_path=runs_root / "derived" / "level-c-cohorts" / "bad.json",
            cohort_id="bad",
        )


def test_level_c_cohort_rejects_post_freeze_artifact_drift(tmp_path: Path) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    output = runs_root / "derived" / "level-c-cohorts" / "cohort-1.json"
    freeze_level_c_cohort(
        runs_root=runs_root,
        atlas_run_root=atlas_root,
        playhand_campaign_id="playhand-campaign-1",
        as_of_date=cutoff,
        lake_manifest_sha256=lake,
        output_path=output,
        cohort_id="cohort-1",
    )
    (atlas_root / "signal-atlas.json").write_text('{"changed":true}', encoding="utf-8")

    with pytest.raises(LevelCCohortError, match="Historical Atlas|evidence changed"):
        validate_level_c_cohort(output)


def test_level_c_cohort_rejects_future_aware_candidate(tmp_path: Path) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    attempts_path = runs_root / "lane-1" / "attempts.jsonl"
    attempt = json.loads(attempts_path.read_text(encoding="utf-8"))
    profile = json.loads((runs_root / "lane-1" / "profile.json").read_text(encoding="utf-8"))
    attempt["evidence_plan"] = build_replay_evidence_plan(
        campaign_plan_id="playhand-lab:lane-1",
        evidence_role="training",
        selection_data_end=cutoff,
        analysis_window_start="2022-06-30T00:00:00Z",
        analysis_window_end=cutoff,
        requested_horizon_months=36,
        profile_snapshot=profile,
        lake_manifest_sha256=lake,
        data_availability_cutoff="2025-07-31T00:00:00Z",
    ).model_dump(mode="json")
    attempts_path.write_text(json.dumps(attempt) + "\n", encoding="utf-8")

    with pytest.raises(LevelCCohortError, match="future-aware"):
        freeze_level_c_cohort(
            runs_root=runs_root,
            atlas_run_root=atlas_root,
            playhand_campaign_id="playhand-campaign-1",
            as_of_date=cutoff,
            lake_manifest_sha256=lake,
            output_path=runs_root / "derived" / "level-c-cohorts" / "bad.json",
            cohort_id="bad",
        )


def test_level_c_cohort_rejects_v2_campaign_layout_ambiguity(tmp_path: Path) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    (runs_root / "playhand-campaign-1").mkdir()

    with pytest.raises(LevelCCohortError, match="Ambiguous PlayHand campaign layout"):
        freeze_level_c_cohort(
            runs_root=runs_root,
            atlas_run_root=atlas_root,
            playhand_campaign_id="playhand-campaign-1",
            as_of_date=cutoff,
            lake_manifest_sha256=lake,
            output_path=runs_root / "derived" / "level-c-cohorts" / "bad.json",
            cohort_id="bad",
        )


def test_level_c_cohort_rejects_v2_lane_campaign_path_mismatch(tmp_path: Path) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    lane_metadata_path = runs_root / "lane-1" / "run-metadata.json"
    lane_metadata = json.loads(lane_metadata_path.read_text(encoding="utf-8"))
    lane_metadata["campaign_dir"] = str((runs_root / "wrong-campaign").resolve())
    lane_metadata_path.write_text(json.dumps(lane_metadata), encoding="utf-8")

    with pytest.raises(LevelCCohortError, match="campaign path mismatch"):
        freeze_level_c_cohort(
            runs_root=runs_root,
            atlas_run_root=atlas_root,
            playhand_campaign_id="playhand-campaign-1",
            as_of_date=cutoff,
            lake_manifest_sha256=lake,
            output_path=runs_root / "derived" / "level-c-cohorts" / "bad.json",
            cohort_id="bad",
        )


def test_level_c_cohort_rejects_self_consistent_future_drift(tmp_path: Path) -> None:
    runs_root, atlas_root, cutoff, lake = _fixture(tmp_path)
    output = runs_root / "derived" / "level-c-cohorts" / "cohort-1.json"
    freeze_level_c_cohort(
        runs_root=runs_root,
        atlas_run_root=atlas_root,
        playhand_campaign_id="playhand-campaign-1",
        as_of_date=cutoff,
        lake_manifest_sha256=lake,
        output_path=output,
        cohort_id="cohort-1",
    )
    attempts_path = runs_root / "lane-1" / "attempts.jsonl"
    attempt = json.loads(attempts_path.read_text(encoding="utf-8"))
    profile = json.loads((runs_root / "lane-1" / "profile.json").read_text(encoding="utf-8"))
    future_plan = build_replay_evidence_plan(
        campaign_plan_id="playhand-lab:lane-1",
        evidence_role="training",
        selection_data_end="2025-07-31T00:00:00Z",
        analysis_window_start="2022-07-31T00:00:00Z",
        analysis_window_end="2025-07-31T00:00:00Z",
        requested_horizon_months=36,
        profile_snapshot=profile,
        lake_manifest_sha256=lake,
        data_availability_cutoff="2025-07-31T00:00:00Z",
    )
    attempt["evidence_plan"] = future_plan.model_dump(mode="json")
    attempt["execution_evidence"].update(
        {
            "plan_id": future_plan.plan_id,
            "profile_snapshot_sha256": future_plan.profile_snapshot_sha256,
            "execution_cell_sha256": future_plan.execution_cell_sha256,
        }
    )
    attempts_path.write_text(json.dumps(attempt) + "\n", encoding="utf-8")

    forged = json.loads(output.read_text(encoding="utf-8"))
    forged["candidates"][0]["discovery_evidence_plan_id"] = future_plan.plan_id
    forged["artifact_sha256"] = _artifact_hashes_for_fixture(runs_root, atlas_root)
    identity = {key: value for key, value in forged.items() if key != "manifest_id"}
    forged["manifest_id"] = "sha256:" + hashlib.sha256(
        json.dumps(identity, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    output.write_text(json.dumps(forged), encoding="utf-8")

    with pytest.raises(LevelCCohortError, match="reads beyond"):
        validate_level_c_cohort(output)


def test_bind_level_c_evidence_rejects_catalog_outsiders(tmp_path: Path) -> None:
    profile = {"name": "Bounded", "notificationThreshold": 80}
    lake = "sha256:" + "e" * 64
    cutoff = "2025-06-30T00:00:00Z"
    cohort = {
        "cohort_id": "cohort-1",
        "manifest_id": "sha256:" + "f" * 64,
        "as_of_date": cutoff,
        "lake_manifest_sha256": lake,
        "outcome": "candidates_frozen",
        "outcome_reason": None,
        "candidate_count": 1,
        "candidates": [{"attempt_id": "allowed"}],
    }
    plan = build_replay_evidence_plan(
        campaign_plan_id=cohort["manifest_id"],
        evidence_role="portfolio_selection",
        selection_data_end=cutoff,
        analysis_window_start="2022-06-30T00:00:00Z",
        analysis_window_end=cutoff,
        requested_horizon_months=36,
        profile_snapshot=profile,
        lake_manifest_sha256=lake,
    )
    artifact_dir = tmp_path / "artifact"
    paths = evidence_artifact_paths(artifact_dir, plan)
    result = {
        "data": {
            "aggregate": {
                "score_lab": {"version": "score_lab_v2_5_3", "score": 71.0},
                "resolved_trade_count_max": 12,
                "best_cell_path_metrics": {"avg_holding_hours": 4.0},
            }
        }
    }
    curve = {"curve": {"points": [{"date": "2025-06-30", "equity_r": 1.0}]}}
    job = {"evidence_plan": plan.model_dump(mode="json")}
    execution_evidence = {
        "plan_id": plan.plan_id,
        "profile_snapshot_sha256": plan.profile_snapshot_sha256,
        "execution_cell_sha256": None,
        "observed_lake_manifest_sha256": lake,
    }
    payloads = {
        "result": result,
        "curve": curve,
        "calendar_curve": curve,
        "recommended_curve": curve,
        "job": job,
    }
    for name, payload in payloads.items():
        write_immutable_json(getattr(paths, name), payload)
    manifest = build_evidence_artifact_manifest(
        evidence_plan=plan,
        provenance={},
        execution_evidence=execution_evidence,
        artifact_payloads=payloads,
    )
    write_immutable_json(paths.manifest, manifest)

    bound, rejected = bind_level_c_evidence_rows(
        [
            {"attempt_id": "allowed", "artifact_dir": str(artifact_dir)},
            {"attempt_id": "later", "artifact_dir": str(artifact_dir)},
        ],
        cohort=cohort,
    )

    assert [row["attempt_id"] for row in bound] == ["allowed"]
    assert bound[0]["score_36m"] == 71.0
    assert rejected == [{"attempt_id": "later", "reason": "outside_frozen_cohort"}]
