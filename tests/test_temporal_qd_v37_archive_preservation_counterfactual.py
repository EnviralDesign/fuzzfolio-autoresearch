from __future__ import annotations

import json
from pathlib import Path

from autoresearch.temporal_qd_v37_archive_preservation_counterfactual import (
    analyze_v37_control_replay,
    write_control_replay_report,
)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _archive(*, generation_index: int, members: list[dict[str, str]]) -> dict[str, object]:
    return {
        "archiveSha256": f"sha256:{generation_index:064x}",
        "memberCount": len(members),
        "cells": [
            {
                "members": [
                    {
                        "candidateId": row["candidateId"],
                        "archiveLane": row["archiveLane"],
                        "descriptor": {"cellId": row["cellId"]},
                    }
                    for row in members
                ]
            }
        ],
    }


def _v37_fixture(tmp_path: Path) -> Path:
    root = tmp_path / "temporal-qd-v37"
    _write_json(
        root / "launch-identity.json",
        {
            "source": {
                "autoresearchHead": "a" * 40,
                "autoresearchWorktree": "dirty-v37-finalizer",
            }
        },
    )
    run = root / "run" / "broad-v37"
    for generation_index, count in enumerate((3, 3, 0, 0, 0), start=1):
        members = [
            {
                "candidateId": f"candidate-{generation_index}-{ordinal}",
                "archiveLane": "quality",
                "cellId": f"cell-{generation_index}-{ordinal}",
            }
            for ordinal in range(count)
        ]
        _write_json(
            run
            / "generations"
            / f"generation-{generation_index:04d}"
            / "native-finalization"
            / "archive.json",
            _archive(generation_index=generation_index, members=members),
        )
    _write_json(
        run
        / "generations"
        / "generation-0001"
        / "native-finalization"
        / "source.json",
        {
            "candidatePanelBundles": [
                {
                    "candidateId": "candidate-1-0",
                    "panelId": "panel-1",
                    "windowEvidence": [
                        {
                            "windowId": "year-1-q1",
                            "metrics": {
                                "realizedBehavior": {
                                    "schemaVersion": "temporal_realized_behavior_v1"
                                }
                            },
                        }
                    ],
                }
            ]
        },
    )
    _write_json(
        run
        / "generations"
        / "generation-0001"
        / "native-finalization"
        / "evidence"
        / "cumulative-archive.json",
        {"archiveSha256": "sha256:cumulative"},
    )
    return root


def test_v37_python_parity_diagnostic_stops_before_counterfactual_execution(
    tmp_path: Path,
) -> None:
    root = _v37_fixture(tmp_path)

    report = analyze_v37_control_replay(root)

    assert report["status"] == "python_parity_incompatible"
    assert report["controlReplay"]["memberCounts"] == [3, 3, 0, 0, 0]
    assert report["controlReplay"]["matchesRequiredTrajectory"] is True
    assert report["blocker"]["missingIdentityRecordCount"] == 1
    assert report["counterfactualExecution"]["state"] == "not_authorized_without_exact_native_control_replay"
    assert report["counterfactualExecution"]["executedVariants"] == []
    assert report["counterfactualExecution"]["marketEvaluation"] is False


def test_v37_control_replay_report_is_compact_and_checksum_bound(tmp_path: Path) -> None:
    root = _v37_fixture(tmp_path)
    output = tmp_path / "report"

    report = write_control_replay_report(v37_root=root, output_dir=output)

    assert report["status"] == "python_parity_incompatible"
    assert (output / "control-replay-preflight.json").is_file()
    assert (output / "README.md").is_file()
    checksums = (output / "CHECKSUMS.sha256").read_text(encoding="utf-8")
    assert "control-replay-preflight.json" in checksums
    assert "README.md" in checksums
    assert not any(path.name.startswith("variant-") for path in output.iterdir())
