"""Oracle parity for the bounded native candidate/window matrix seam."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.temporal_discovery_base import canonical_sha256
from autoresearch.temporal_qd_campaign_native import (
    materialize_qd_campaign_task_matrix_native,
)
from autoresearch.temporal_search import build_authority, materialize_plan


def _profile() -> dict:
    return {
        "version": "v2",
        "graph": {"kind": "temporal_graph_v1"},
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "executionConfig": {
            "exitPolicy": {
                "selectedCell": {
                    "rewardMultiple": 2.0,
                    "stopLossPercent": 0.5,
                    "takeProfitPercent": 1.0,
                }
            }
        },
    }


def _authority(*, candidate_count: int, window_count: int) -> dict:
    profile = _profile()
    source_sha = canonical_sha256(profile)
    windows = []
    inputs = []
    for ordinal in range(window_count):
        window_id = f"development_{ordinal}"
        windows.append(
            {
                "windowId": window_id,
                "analysisWindowStart": "2024-02-01T00:00:00Z",
                "analysisWindowEnd": "2024-03-01T00:00:00Z",
            }
        )
        plan = {
            "schema_version": "fuzzfolio.replay-evidence-plan.v2",
            "profile_snapshot_sha256": source_sha,
            "analysis_window_start": "2024-02-01T00:00:00Z",
            "analysis_window_end": "2024-03-01T00:00:00Z",
            "execution_cell_sha256": canonical_sha256(
                profile["executionConfig"]["exitPolicy"]["selectedCell"]
            ),
            # Deliberately distinct finite plans over the same attested
            # development interval; this makes the oracle assert task order.
            "coverage_policy": f"fixture_{ordinal}",
            "lake_window_binding": {
                "window_semantic_sha256": "sha256:" + f"{ordinal:x}" * 64,
                "request": {
                    "data_start": "2024-01-01T00:00:00Z",
                    "data_end": "2024-03-01T00:00:00Z",
                    "pairs": ["EURUSD"],
                    "timeframes": ["M5"],
                },
            },
        }
        plan["plan_id"] = canonical_sha256(plan)
        inputs.append({"windowId": window_id, "evidencePlan": plan})
    candidates = [
        {
            "candidateId": f"candidate_{ordinal}",
            "sourceProfile": profile,
            "sourceProfileSha256": source_sha,
            "instrument": "EURUSD",
            "timeframe": "M5",
            "barLimit": 5000,
            "windowInputs": inputs,
        }
        for ordinal in range(candidate_count)
    ]
    return build_authority(
        {
            "schemaVersion": "temporal_graph_candidate_window_preparation_v1",
            "authorityLabel": "native-task-matrix-oracle",
            "workerContract": {
                "workerContractSha256": "sha256:" + "c" * 64,
                "workerContractSchema": "replay-worker-contract-v1",
            },
            "candidates": candidates,
            "developmentWindows": windows,
            "prohibitedEvidence": [
                {
                    "windowId": "reserved",
                    "analysisWindowStart": "2024-06-29T00:00:00Z",
                    "analysisWindowEnd": "2024-07-01T00:00:00Z",
                    "reason": "reserved",
                }
            ],
            "bounds": {
                "maxCandidates": candidate_count,
                "maxDevelopmentWindows": window_count,
                "maxTasks": candidate_count * window_count,
                "maxAttempts": 2,
                "deadlineSeconds": 60,
            },
        }
    )


@pytest.mark.parametrize("candidate_count", [2, 64])
def test_native_task_matrix_is_byte_exact_python_oracle(
    tmp_path: Path, candidate_count: int
) -> None:
    authority = _authority(candidate_count=candidate_count, window_count=4)
    authority_path = tmp_path / "authority-input.json"
    authority_path.write_text(
        json.dumps(authority, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    oracle_root = tmp_path / "oracle"
    expected = materialize_plan(authority, oracle_root)
    native_root = tmp_path / "native"
    actual = materialize_qd_campaign_task_matrix_native(
        authority_path=authority_path, output_root=native_root
    )
    assert actual["taskMatrixSha256"] == expected["taskMatrixSha256"]
    assert actual["taskCount"] == candidate_count * 4
    assert actual["telemetry"]["peakLiveTasks"] == 1
    for name in ("authority.json", "task-manifest.json", "checkpoint.json"):
        assert (native_root / name).read_bytes() == (oracle_root / name).read_bytes()
    # Reopen uses the same immutable output and never takes a Python fallback.
    assert materialize_qd_campaign_task_matrix_native(
        authority_path=authority_path, output_root=native_root
    )["taskMatrixSha256"] == expected["taskMatrixSha256"]


def test_native_task_matrix_rejects_divergent_checkpoint(tmp_path: Path) -> None:
    authority = _authority(candidate_count=2, window_count=4)
    authority_path = tmp_path / "authority-input.json"
    authority_path.write_text(
        json.dumps(authority, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    root = tmp_path / "native"
    materialize_qd_campaign_task_matrix_native(authority_path=authority_path, output_root=root)
    checkpoint = json.loads((root / "checkpoint.json").read_text(encoding="utf-8"))
    checkpoint["taskMatrixSha256"] = "sha256:" + "0" * 64
    (root / "checkpoint.json").write_text(
        json.dumps(checkpoint, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with pytest.raises(Exception, match="checkpoint"):
        materialize_qd_campaign_task_matrix_native(
            authority_path=authority_path, output_root=root
        )
