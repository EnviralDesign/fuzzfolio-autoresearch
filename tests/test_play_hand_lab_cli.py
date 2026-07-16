from __future__ import annotations

import argparse
import json
from pathlib import Path

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
    args = _parse_play_hand_lab_args(
        [
            "play-hand-lab",
            "--as-of-date",
            "2025-06-30T00:00:00Z",
            "--campaign-id",
            "formal-campaign-2025-06",
            "--research-generation-id",
            "generation-2025-06",
            "--level-c-protocol-id",
            "sha256:" + "c" * 64,
            "--cutoff-key",
            "A",
            "--seed-plan-path",
            str(seed_plan_path),
            "--expected-seed-plan-sha256",
            expected_seed_plan_sha256,
            "--lake-manifest-sha256",
            "sha256:" + "b" * 64,
            "--worker-contract-hash",
            "sha256:" + "a" * 64,
            "--seed",
            "17",
            "--target-runs",
            "1",
            "--strict-scoring",
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


def test_exploratory_cli_leaves_campaign_id_unset_for_auto_id() -> None:
    args = _parse_play_hand_lab_args(["play-hand-lab"])

    assert args.campaign_id is None
