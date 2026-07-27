from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch import play_hand_lab
from autoresearch import play_hand_lab_result_fastpath as fastpath
from autoresearch import play_hand_lab_throughput as throughput
from autoresearch.scoring import CANONICAL_SCORE_LAB_VERSION


def _reset_fastpath_state() -> None:
    with fastpath._LOCK:
        fastpath._IDENTITY_CACHE.clear()
        fastpath._SCORE_CACHE.clear()
        fastpath._RECEIPT_CACHE.clear()
        for key in fastpath._COUNTERS:
            fastpath._COUNTERS[key] = 0.0 if "seconds" in key else 0


def test_phase3_runtime_uses_a_larger_default_result_drain() -> None:
    runtime = play_hand_lab.PlayHandLabRuntimeConfig(
        formal_authority_kind="phase3",
        result_batch_size=25,
        max_results_per_cycle=200,
        max_drain_seconds=0.5,
    )

    normalized = play_hand_lab._normalize_runtime(runtime)

    assert normalized.result_batch_size == 64
    assert normalized.max_results_per_cycle == 2048
    assert normalized.max_drain_seconds == 5.0

    larger = play_hand_lab._normalize_runtime(
        play_hand_lab.PlayHandLabRuntimeConfig(
            formal_authority_kind="phase3",
            result_batch_size=128,
            max_results_per_cycle=8192,
            max_drain_seconds=10.0,
        )
    )
    assert larger.result_batch_size == 128
    assert larger.max_results_per_cycle == 8192
    assert larger.max_drain_seconds == 10.0


def test_worker_result_identity_is_hashed_once_per_delivered_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_fastpath_state()
    calls = 0

    def original(_payload):
        nonlocal calls
        calls += 1
        return "sha256:" + "a" * 64

    monkeypatch.setattr(fastpath, "_ORIGINAL_WORKER_RESULT_IDENTITY", original)
    payload = {
        "task_id": "task-1",
        "lease_id": "lease-1",
        "status": "success",
        "result": {"result": {"large": "x" * 100_000}},
    }

    first = play_hand_lab._worker_result_identity(payload)
    second = play_hand_lab._worker_result_identity(payload)

    assert first == second
    assert calls == 1
    diagnostics = fastpath.result_fastpath_diagnostics()
    assert diagnostics["identity_cache_hits"] == 1
    assert diagnostics["identity_cache_misses"] == 1


def test_canonical_worker_score_avoids_the_compare_sensitivity_subprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_fastpath_state()
    sensitivity = {
        "status": "success",
        "requested_timeframe": "M5",
        "effective_timeframe": "M5",
        "data": {
            "aggregate": {
                "score_lab": {
                    "version": CANONICAL_SCORE_LAB_VERSION,
                    "score": 78.25,
                    "combiner": "canonical",
                },
                "quality_score": {"score": 73.0},
                "best_cell_path_metrics": {
                    "psr": 0.91,
                    "k_ratio": 2.4,
                    "sharpe_r": 1.5,
                },
            },
            "per_instrument": {"EURUSD": {"large": "x" * 100_000}},
        },
    }
    play_hand_lab._write_json(tmp_path / "sensitivity-response.json", sensitivity)

    monkeypatch.setattr(
        fastpath,
        "_ORIGINAL_SCORE_LAB_ARTIFACT",
        lambda **_kwargs: pytest.fail("canonical score should not spawn the CLI"),
    )

    score, warning = play_hand_lab._score_lab_artifact(
        cli=object(),
        artifact_dir=tmp_path,
        strict=True,
    )

    assert warning is None
    assert score.composite_score == pytest.approx(78.25)
    assert score.score_basis == f"{CANONICAL_SCORE_LAB_VERSION}:canonical"
    diagnostics = fastpath.result_fastpath_diagnostics()
    assert diagnostics["direct_scores"] == 1
    assert diagnostics["cli_score_fallbacks"] == 0


def test_deep_replay_job_artifact_keeps_summary_not_the_duplicate_full_result() -> None:
    compact = fastpath._compact_deep_replay_job_payload(
        {
            "status": "success",
            "request": {"profile_id": "profile-1"},
            "execution_evidence": {"plan_id": "plan-1"},
            "result": {
                "aggregate": {
                    "score_lab": {
                        "version": CANONICAL_SCORE_LAB_VERSION,
                        "score": 80.0,
                    }
                },
                "per_instrument": {"EURUSD": {"large": "x" * 100_000}},
                "matrix": [[1] * 1000],
            },
        }
    )

    assert compact["request"] == {"profile_id": "profile-1"}
    assert compact["execution_evidence"] == {"plan_id": "plan-1"}
    assert compact["result"]["aggregate"]["score_lab"]["score"] == 80.0
    assert "per_instrument" not in compact["result"]
    assert "matrix" not in compact["result"]
    assert compact["full_result_omitted"] is True


def test_fresh_receipt_is_not_rehashed_twice_in_the_same_result_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_fastpath_state()
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    (artifact_dir / "sensitivity-response.json").write_text(
        json.dumps({"data": {"aggregate": {"score": 1}}}),
        encoding="utf-8",
    )
    task_id = "task-1"
    worker_sha = "sha256:" + "b" * 64
    receipt_path = artifact_dir / "task-result-receipt.json"

    written = play_hand_lab._write_task_result_receipt(
        receipt_path,
        task_id=task_id,
        worker_result_sha256=worker_sha,
        recorded_result={
            "task_id": task_id,
            "artifact_dir": str(artifact_dir),
            "status": "success",
            "task_kind": "deep_replay",
        },
    )

    monkeypatch.setattr(
        fastpath,
        "_ORIGINAL_VALIDATE_TASK_RESULT_RECEIPT",
        lambda *_args, **_kwargs: pytest.fail("hot receipt should not rehash artifacts"),
    )
    validated = play_hand_lab._validate_task_result_receipt(
        receipt_path,
        task_id=task_id,
        worker_result_sha256=worker_sha,
    )

    assert validated == written
    assert fastpath.result_fastpath_diagnostics()["receipt_cache_hits"] == 1


def test_sweep_receipt_is_compact_on_the_first_write(tmp_path: Path) -> None:
    _reset_fastpath_state()
    artifact_dir = tmp_path / "sweep"
    artifact_dir.mkdir()
    ranked = [
        {
            "permutation_index": 0,
            "child_job_id": "child-0",
            "status": "success",
            "score": 77.0,
            "fitness_value": 77.0,
            "parameters": {"period": 14},
        }
    ]
    sweep_payload = {
        "sweep_id": "sweep-1",
        "shard_id": "shard-1",
        "ranked_permutations": ranked,
        "ranked": ranked,
        "failed_permutations": [],
    }
    play_hand_lab._write_json(artifact_dir / "sweep-results.json", sweep_payload)
    task_id = "task-sweep"
    worker_sha = "sha256:" + "c" * 64

    play_hand_lab._write_task_result_receipt(
        artifact_dir / "task-result-receipt.json",
        task_id=task_id,
        worker_result_sha256=worker_sha,
        recorded_result={
            "task_id": task_id,
            "artifact_dir": str(artifact_dir),
            "status": "success",
            "task_kind": "sweep_shard",
            "sweep_payload": sweep_payload,
        },
    )

    stored_sweep = json.loads(
        (artifact_dir / "sweep-results.json").read_text(encoding="utf-8")
    )
    receipt = json.loads(
        (artifact_dir / "task-result-receipt.json").read_text(encoding="utf-8")
    )
    assert "ranked" not in stored_sweep
    assert "sweep_payload" not in receipt["recorded_result"]
    assert receipt["recorded_result"]["sweep_payload_artifact"] == "sweep-results.json"

    compact_again = throughput._compact_pending_sweep_receipt(
        play_hand_lab,
        recorded={
            "task_id": task_id,
            "artifact_dir": str(artifact_dir),
            "task_kind": "sweep_shard",
            "sweep_payload": sweep_payload,
        },
        lab_result={"task_id": task_id, "result": {}},
        runtime=play_hand_lab.PlayHandLabRuntimeConfig(
            formal_authority_kind="phase3",
            as_of_date="2026-01-14T00:00:00Z",
        ),
    )
    assert "sweep_payload" not in compact_again


def test_barrier_exposes_actual_result_processing_cost() -> None:
    rendered = play_hand_lab._format_lab_barrier_snapshot(
        barrier_index=1,
        campaign_id="phase3-fastpath-test",
        runtime=play_hand_lab.PlayHandLabRuntimeConfig(
            formal_authority_kind="phase3",
            as_of_date="2026-01-14T00:00:00Z",
        ),
        lanes=[],
        tasks=[],
        snapshot={},
        metric_baseline={},
        recorded_result_count=0,
    )

    assert "result fastpath avg=" in rendered
    assert "direct-score=" in rendered
