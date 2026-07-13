from __future__ import annotations

import json
import hashlib
from pathlib import Path

import pytest

from autoresearch.evidence_plan import build_replay_evidence_plan
from autoresearch.level_c import (
    LevelCCohortError,
    bind_level_c_evidence_rows,
    freeze_level_c_cohort,
    validate_level_c_cohort,
)
from autoresearch.evidence_artifacts import (
    build_evidence_artifact_manifest,
    evidence_artifact_paths,
    write_immutable_json,
)
from autoresearch.ledger import write_run_metadata


def _fixture(tmp_path: Path) -> tuple[Path, Path, str, str]:
    cutoff = "2025-06-30T00:00:00Z"
    lake = "sha256:" + "a" * 64
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
                },
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
    atlas_seed_plan.write_text('{"seed":"bounded"}', encoding="utf-8")

    campaign_id = "playhand-campaign-1"
    campaign_root = runs_root / campaign_id
    write_run_metadata(
        campaign_root,
        {
            "run_status": "completed",
            "as_of_date": cutoff,
            "lake_manifest_sha256": lake,
            "play_hand_seed_plan_path": str(atlas_seed_plan.resolve()),
            "play_hand_seed_plan_sha256": "sha256:"
            + hashlib.sha256(atlas_seed_plan.read_bytes()).hexdigest(),
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
            "run_status": "promoted",
            "parent_campaign_id": campaign_id,
            "canonical_attempt_id": attempt_id,
            "as_of_date": cutoff,
            "lake_manifest_sha256": lake,
        },
    )
    (lane_root / "attempts.jsonl").write_text(
        json.dumps(
            {
                "attempt_id": attempt_id,
                "run_id": "lane-1",
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
    assert payload["candidates"][0]["attempt_id"] == "lane-1-attempt-00001"
    assert validate_level_c_cohort(output)["manifest_id"] == payload["manifest_id"]


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
        evidence_role="training",
        selection_data_end="2025-07-31T00:00:00Z",
        analysis_window_start="2022-07-31T00:00:00Z",
        analysis_window_end="2025-07-31T00:00:00Z",
        requested_horizon_months=36,
        profile_snapshot=profile,
        lake_manifest_sha256=lake,
    ).model_dump(mode="json")
    attempts_path.write_text(json.dumps(attempt) + "\n", encoding="utf-8")

    with pytest.raises(LevelCCohortError, match="reads beyond"):
        freeze_level_c_cohort(
            runs_root=runs_root,
            atlas_run_root=atlas_root,
            playhand_campaign_id="playhand-campaign-1",
            as_of_date=cutoff,
            lake_manifest_sha256=lake,
            output_path=runs_root / "derived" / "level-c-cohorts" / "bad.json",
            cohort_id="bad",
        )


def test_bind_level_c_evidence_rejects_catalog_outsiders(tmp_path: Path) -> None:
    profile = {"name": "Bounded", "notificationThreshold": 80}
    lake = "sha256:" + "e" * 64
    cutoff = "2025-06-30T00:00:00Z"
    cohort = {
        "cohort_id": "cohort-1",
        "manifest_id": "sha256:" + "f" * 64,
        "as_of_date": cutoff,
        "lake_manifest_sha256": lake,
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
