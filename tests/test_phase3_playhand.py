from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from autoresearch.phase3_authority import PHASE3_PLAYHAND_SEMANTIC_DEFAULTS
from autoresearch.play_hand import DEFAULT_INSTRUMENT_POOL
from autoresearch.play_hand_lab import _require_historical_task_evidence
from autoresearch.phase3_playhand import (
    Phase3PlayHandError,
    cmd_phase3_playhand,
    prepare_phase3_playhand_runtime,
)
from autoresearch.config import load_config
from autoresearch.level_c_operator import (
    PROFILE_MODEL_SOURCE_FILES,
    build_profile_model_source_lock,
)
from autoresearch.runtime_policy_lock import build_runtime_policy_lock


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _semantic(*, campaign_id: str = "phase3-test") -> dict[str, object]:
    return {
        "campaign_mode": "finite",
        "task_mode": "deep_replay",
        "pipeline_mode": "play_hand",
        "target_runs": 12,
        "campaign_id": campaign_id,
        "seed": 123,
        "as_of_date": "2026-01-14T00:00:00Z",
        "expected_seed_plan_sha256": "sha256:" + "a" * 64,
        "lake_manifest_sha256": "sha256:" + "b" * 64,
        "source_snapshot_sha256": "sha256:" + "c" * 64,
        "universe_id": "fuzzfolio-development-darwinex-zero",
        "universe_manifest_sha256": "sha256:" + "d" * 64,
        "worker_contract_hash": "sha256:" + "e" * 64,
        "operator_launch_worker_image": "test-image",
        "current_atlas_generation": "level-c-v3-phase2-rich-priors",
        "current_atlas_run_sequence": 4,
        "campaign_policy_manifest_sha256": "sha256:" + "f" * 64,
        "campaign_policy_source_file_sha256": "sha256:" + "0" * 64,
        "worker_contract_schema": "replay-worker-contract-v1",
        "reserved_tail": {"start": "2026-01-14T00:00:00Z"},
        "tail_access": "forbidden_during_phase3_construction",
        **PHASE3_PLAYHAND_SEMANTIC_DEFAULTS,
    }


def _args(tmp_path: Path, *, fresh: bool, resume: bool) -> argparse.Namespace:
    authority_path = tmp_path / "authority.json"
    _write_json(
        authority_path,
        {
            "authority_id": "sha256:" + "1" * 64,
            "bound_contract": {},
            "worker_execution_enforcement": {
                "gateway_claim_correctness": "worker_contract_sha256_and_required_capabilities",
                "gateway_enforced_worker_contract_sha256": "sha256:" + "e" * 64,
                "operator_launch_provenance": "exact_image_required_before_worker_launch",
                "operator_launch_worker_image": "test-image",
                "worker_image_gateway_claim_enforced": False,
            },
        },
    )
    (tmp_path / "capsule").mkdir()
    _write_json(tmp_path / "policy.json", {})
    return argparse.Namespace(
        authority_path=authority_path,
        phase2_capsule_root=tmp_path / "capsule",
        policy_manifest=tmp_path / "policy.json",
        fresh=fresh,
        resume=resume,
        gateway_url="http://127.0.0.1:8799",
        gateway_token=None,
        active_runs=17,
        poll_interval_seconds=1.0,
        max_wait_seconds=100.0,
        result_batch_size=25,
        max_results_per_cycle=200,
        max_drain_seconds=2.0,
        result_read_failure_limit=5,
        enqueue_failure_limit=3,
        enqueue_retry_base_seconds=1.0,
        terminal_lane_retention=128,
        trading_dashboard_root=None,
        dry_run=True,
        log_mode="quiet",
        barrier_interval_seconds=5.0,
        barrier_lane_limit=24,
        json=True,
    )


def _patch_authority(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, semantic: dict[str, object]) -> None:
    from autoresearch import phase3_playhand

    seed = tmp_path / "play-hand-seed-plan.json"
    _write_json(seed, {"recipes": {}})
    resolved = {**semantic, "seed_plan_path": str(seed)}
    monkeypatch.setattr(
        phase3_playhand,
        "resolve_phase3_playhand_runtime_arguments",
        lambda **_: resolved,
    )
    monkeypatch.setattr(
        phase3_playhand,
        "load_config",
        lambda: SimpleNamespace(runs_root=tmp_path / "runs"),
    )
    monkeypatch.setattr(phase3_playhand, "_validate_live_runtime_contract", lambda **_: None)


def test_fresh_derives_all_semantics_and_binds_reserved_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _args(tmp_path, fresh=True, resume=False)
    _patch_authority(monkeypatch, tmp_path, _semantic())

    runtime, payload = prepare_phase3_playhand_runtime(args)

    assert runtime.formal_authority_kind == "phase3"
    assert runtime.as_of_date == "2026-01-14T00:00:00Z"
    assert runtime.cutoff_key == "P3"
    assert runtime.target_runs == 12
    assert runtime.instrument_pool == list(DEFAULT_INSTRUMENT_POOL)
    assert runtime.operator_launch_worker_image == "test-image"
    assert payload["operator_launch_worker_image"] == "test-image"
    assert payload["gateway_enforced_worker_contract_sha256"] == "sha256:" + "e" * 64
    assert runtime.resume is False
    assert payload["reserved_tail"]["start"] == "2026-01-14T00:00:00Z"


def test_resume_requires_exact_durable_campaign_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _args(tmp_path, fresh=False, resume=True)
    _patch_authority(monkeypatch, tmp_path, _semantic())
    root = tmp_path / "runs" / "derived" / "play-hand-lab-campaigns" / "phase3-test"
    root.mkdir(parents=True)
    for name in ("run-metadata.json", "play-hand-lab-state.json", "play-hand-lab-execution-journal.json"):
        _write_json(root / name, {})

    runtime, _payload = prepare_phase3_playhand_runtime(args)

    assert runtime.resume is True


def test_fresh_rejects_existing_campaign_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    args = _args(tmp_path, fresh=True, resume=False)
    _patch_authority(monkeypatch, tmp_path, _semantic())
    (tmp_path / "runs" / "derived" / "play-hand-lab-campaigns" / "phase3-test").mkdir(parents=True)

    with pytest.raises(Phase3PlayHandError, match="fresh campaign root already exists"):
        prepare_phase3_playhand_runtime(args)


def test_dry_run_uses_read_only_preflight_instead_of_coordinator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    args = _args(tmp_path, fresh=True, resume=False)
    _patch_authority(monkeypatch, tmp_path, _semantic())
    from autoresearch import phase3_playhand

    observed: dict[str, object] = {}
    monkeypatch.setattr(
        phase3_playhand,
        "preflight_play_hand_lab",
        lambda runtime: observed.setdefault("preflight", runtime) and {"gateway_ok": True},
    )
    monkeypatch.setattr(
        phase3_playhand,
        "cmd_play_hand_lab",
        lambda _runtime: pytest.fail("dry-run must not invoke the mutating coordinator"),
    )

    assert cmd_phase3_playhand(args) == 0
    assert observed["preflight"].dry_run is True
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "completed"
    assert payload["preflight"] == {"gateway_ok": True}


@pytest.mark.parametrize("gateway_ok", [True, False])
def test_real_dry_run_preflight_never_creates_campaign_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    gateway_ok: bool,
) -> None:
    args = _args(tmp_path, fresh=True, resume=False)
    _patch_authority(monkeypatch, tmp_path, _semantic())
    from autoresearch import phase3_playhand, play_hand_lab

    config = replace(load_config(), repo_root=tmp_path)
    seed_path = tmp_path / "play-hand-seed-plan.json"
    campaign_root = (
        config.runs_root
        / "derived"
        / "play-hand-lab-campaigns"
        / "phase3-test"
    )

    class _Gateway:
        def health(self) -> dict[str, bool]:
            if not gateway_ok:
                raise ConnectionError("gateway unavailable")
            return {"ok": True}

    monkeypatch.setattr(play_hand_lab, "_normalize_runtime", lambda runtime: runtime)
    monkeypatch.setattr(play_hand_lab, "load_config", lambda: config)
    monkeypatch.setattr(play_hand_lab, "FuzzfolioCli", lambda _config: SimpleNamespace())
    monkeypatch.setattr(play_hand_lab, "LabGatewayClient", lambda **_: _Gateway())
    monkeypatch.setattr(
        play_hand_lab,
        "_seed_indicators",
        lambda **_: ([SimpleNamespace(id="RSI")], {}, seed_path),
    )
    monkeypatch.setattr(
        phase3_playhand,
        "cmd_play_hand_lab",
        lambda _runtime: pytest.fail("dry-run must not invoke the mutating coordinator"),
    )

    if gateway_ok:
        assert cmd_phase3_playhand(args) == 0
    else:
        with pytest.raises(ConnectionError, match="gateway unavailable"):
            cmd_phase3_playhand(args)
    assert not campaign_root.exists()


def test_json_reports_nonzero_coordinator_exit_as_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    args = _args(tmp_path, fresh=True, resume=False)
    args.dry_run = False
    _patch_authority(monkeypatch, tmp_path, _semantic())
    from autoresearch import phase3_playhand

    monkeypatch.setattr(phase3_playhand, "cmd_play_hand_lab", lambda _runtime: 2)

    assert cmd_phase3_playhand(args) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed"
    assert payload["exit_code"] == 2


def test_tail_or_authority_drift_blocks_before_launch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _args(tmp_path, fresh=True, resume=False)
    semantic = _semantic()
    semantic["as_of_date"] = "2026-07-14T00:00:00Z"
    _patch_authority(monkeypatch, tmp_path, semantic)

    with pytest.raises(Phase3PlayHandError, match="reserved-tail cutoff"):
        prepare_phase3_playhand_runtime(args)


def test_live_worker_contract_drift_blocks_before_coordinator_invocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _args(tmp_path, fresh=True, resume=False)
    from autoresearch import phase3_playhand

    real_validator = phase3_playhand._validate_live_runtime_contract
    _patch_authority(monkeypatch, tmp_path, _semantic())
    runtime, _payload = prepare_phase3_playhand_runtime(args)
    monkeypatch.setattr(phase3_playhand, "_validate_live_runtime_contract", real_validator)

    monkeypatch.setattr(phase3_playhand, "_trading_dashboard_root", lambda **_: tmp_path)
    monkeypatch.setattr(
        phase3_playhand,
        "_resolve_worker_contract_hash",
        lambda **_: "sha256:" + "0" * 64,
    )
    with pytest.raises(Phase3PlayHandError, match="worker contract differs"):
        phase3_playhand._validate_live_runtime_contract(
            authority={
                "bound_contract": {
                    "operator_launch_worker_image": runtime.operator_launch_worker_image,
                },
                "worker_execution_enforcement": {
                    "gateway_claim_correctness": "worker_contract_sha256_and_required_capabilities",
                    "gateway_enforced_worker_contract_sha256": runtime.worker_contract_hash,
                    "operator_launch_provenance": "exact_image_required_before_worker_launch",
                    "operator_launch_worker_image": runtime.operator_launch_worker_image,
                    "worker_image_gateway_claim_enforced": False,
                },
            },
            runtime=runtime,
            config=SimpleNamespace(),
        )


def test_attested_engine_policy_revalidates_with_supplied_profile_source_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _args(tmp_path, fresh=True, resume=False)
    from autoresearch import phase3_playhand

    real_validator = phase3_playhand._validate_live_runtime_contract
    _patch_authority(monkeypatch, tmp_path, _semantic())
    runtime, _payload = prepare_phase3_playhand_runtime(args)
    monkeypatch.setattr(phase3_playhand, "_validate_live_runtime_contract", real_validator)

    source_root = tmp_path / "Trading-Dashboard"
    for index, relative in enumerate(PROFILE_MODEL_SOURCE_FILES):
        path = source_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"profile-model-source-{index}\n", encoding="utf-8")

    config = load_config()
    runtime = replace(runtime, trading_dashboard_root=source_root)
    contract = str(runtime.worker_contract_hash)
    authority = {
        "bound_contract": {
            "operator_launch_worker_image": runtime.operator_launch_worker_image,
            "profile_model_source_lock": build_profile_model_source_lock(source_root),
            "runtime_policy_lock": build_runtime_policy_lock(
                config,
                worker_contract_sha256=contract,
                trading_dashboard_root=None,
            ),
        },
        "worker_execution_enforcement": {
            "gateway_claim_correctness": "worker_contract_sha256_and_required_capabilities",
            "gateway_enforced_worker_contract_sha256": contract,
            "operator_launch_provenance": "exact_image_required_before_worker_launch",
            "operator_launch_worker_image": runtime.operator_launch_worker_image,
            "worker_image_gateway_claim_enforced": False,
        },
    }
    monkeypatch.setattr(phase3_playhand, "_trading_dashboard_root", lambda **_: source_root)
    monkeypatch.setattr(
        phase3_playhand,
        "_resolve_worker_contract_hash",
        lambda **_: contract,
    )

    phase3_playhand._validate_live_runtime_contract(
        authority=authority,
        runtime=runtime,
        config=config,
    )

    first_source = source_root / PROFILE_MODEL_SOURCE_FILES[0]
    first_source.write_text("profile-model-source-drift\n", encoding="utf-8")
    with pytest.raises(Phase3PlayHandError, match="profile-model source differs"):
        phase3_playhand._validate_live_runtime_contract(
            authority=authority,
            runtime=runtime,
            config=config,
        )


def test_authority_bound_runtime_rejects_any_training_window_that_reaches_the_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _args(tmp_path, fresh=True, resume=False)
    _patch_authority(monkeypatch, tmp_path, _semantic())
    runtime, _payload = prepare_phase3_playhand_runtime(args)

    evidence_plan = SimpleNamespace(
        model_dump=lambda **_: {
            "evidence_role": "training",
            "selection_data_end": "2026-01-14T00:00:00Z",
            "data_availability_cutoff": "2026-01-14T00:00:00Z",
        }
    )
    with pytest.raises(ValueError, match="analysis_window_end must equal as_of_date"):
        _require_historical_task_evidence(
            runtime=runtime,
            analysis_window_start="2023-01-14T00:00:00Z",
            analysis_window_end="2026-07-14T00:00:00Z",
            evidence_plan=evidence_plan,
        )
