from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest
from rich.console import Console

from autoresearch import play_hand_lab as lab
from autoresearch import play_hand_lab_cli as lab_cli


def _parse_play_hand_lab_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    lab_cli.add_play_hand_lab_subparsers(subparsers)
    return parser.parse_args(argv)


def test_historical_cli_forwards_formal_identity_and_normalizes(tmp_path: Path, monkeypatch) -> None:
    seed_plan_path = tmp_path / "seed-plan.json"
    seed_plan_path.write_text(
        json.dumps(
            {
                "sampling_policy": {"guided_prior_fraction": 1.0},
                "recipes": {"pair": {"pair_menu": []}},
            }
        ),
        encoding="utf-8",
    )
    expected_seed_plan_sha256 = lab._file_sha256(seed_plan_path)
    execution_plan_path = tmp_path / "execution-plan.json"
    execution_plan_path.write_text("{}", encoding="utf-8")
    from autoresearch.instrument_universe import universe_provenance

    universe = universe_provenance()
    monkeypatch.setattr(
        "autoresearch.level_c_operator.executor_arguments_from_plan",
        lambda path, executor: (
            {
                "execution_plan_path": str(path),
                "execution_plan_id": "sha256:" + "e" * 64,
                "as_of_date": "2025-06-30T00:00:00Z",
                "campaign_id": "formal-campaign-2025-06",
                "research_generation_id": "generation-2025-06",
                "level_c_protocol_id": "sha256:" + "c" * 64,
                "cutoff_key": "A",
                "seed_plan_path": str(seed_plan_path),
                "expected_seed_plan_sha256": expected_seed_plan_sha256,
                "lake_manifest_sha256": "sha256:" + "b" * 64,
                "worker_contract_hash": "sha256:" + "a" * 64,
                "source_snapshot_sha256": "sha256:" + "d" * 64,
                "universe_id": str(universe["universe_id"]),
                "universe_manifest_sha256": str(universe["universe_hash"]),
                "seed": 17,
                "campaign_mode": "finite",
                "task_mode": "deep_replay",
                "pipeline_mode": "play_hand",
                "strict_scoring": True,
                "target_runs": 7,
                "validation_months": 18,
            },
            {},
        ),
    )
    args = _parse_play_hand_lab_args(
        [
            "play-hand-lab",
            "--execution-plan",
            str(execution_plan_path),
            "--target-runs",
            "1",
        ]
    )
    captured: list[lab.PlayHandLabRuntimeConfig] = []
    monkeypatch.setattr(lab_cli, "cmd_play_hand_lab", lambda runtime: captured.append(runtime) or 0)

    assert lab_cli.dispatch_play_hand_lab_command(args, console=Console()) == 0
    assert len(captured) == 1
    runtime = lab._normalize_runtime(captured[0])

    assert runtime.campaign_id == "formal-campaign-2025-06"
    assert runtime.research_generation_id == "generation-2025-06"
    assert runtime.level_c_protocol_id == "sha256:" + "c" * 64
    assert runtime.cutoff_key == "A"
    assert runtime.expected_seed_plan_sha256 == expected_seed_plan_sha256
    assert runtime.target_runs == 7
    assert runtime.validation_months == 18


def test_exploratory_cli_leaves_campaign_id_unset_for_auto_id() -> None:
    args = _parse_play_hand_lab_args(["play-hand-lab"])

    assert args.campaign_id is None


def test_formal_cli_rejects_independent_lineage_with_execution_plan(tmp_path: Path) -> None:
    args = _parse_play_hand_lab_args(
        [
            "play-hand-lab",
            "--execution-plan",
            str(tmp_path / "plan.json"),
            "--as-of-date",
            "2025-06-30T00:00:00Z",
        ]
    )

    with pytest.raises(ValueError, match="must come only from --execution-plan"):
        lab_cli.dispatch_play_hand_lab_command(args, console=Console())
