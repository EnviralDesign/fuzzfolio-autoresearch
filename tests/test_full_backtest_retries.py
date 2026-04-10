from __future__ import annotations

import json
from pathlib import Path

from autoresearch import __main__ as ar_main
from autoresearch import dashboard as dashboard_mod
from autoresearch.config import FuzzfolioConfig
from autoresearch.fuzzfolio import CliError


class _StubConfig:
    def __init__(self, workspace_root: Path):
        self.validation_cache_root = workspace_root / "validation-cache"
        self.validation_cache_root.mkdir(parents=True, exist_ok=True)
        self.fuzzfolio = FuzzfolioConfig(
            workspace_root=workspace_root,
            cli_command="fuzzfolio-agent-cli",
            base_url="http://localhost:7946/api/dev",
            auth_profile="robot",
        )


def test_run_full_backtest_retries_with_local_profile_on_profile_not_found(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_dir = tmp_path / "eval"
    artifact_dir.mkdir()
    profile_path = tmp_path / "profile.json"
    profile_path.write_text("{}", encoding="utf-8")
    config = _StubConfig(tmp_path)
    attempt = {
        "attempt_id": "run-attempt-1",
        "artifact_dir": str(artifact_dir),
        "profile_ref": "stale-cloud-ref",
        "profile_path": str(profile_path),
    }

    seen_profile_refs: list[str] = []

    def fake_run_full_backtest(cfg, candidate):
        seen_profile_refs.append(str(candidate.get("profile_ref") or ""))
        if len(seen_profile_refs) == 1:
            raise RuntimeError("Profile not found")
        return {"curve_path": "curve.json", "result_path": "result.json"}

    monkeypatch.setattr(ar_main, "_run_full_backtest_for_attempt", fake_run_full_backtest)

    result = ar_main._run_full_backtest_with_retry(config, attempt)

    assert result["curve_path"] == "curve.json"
    assert seen_profile_refs == ["stale-cloud-ref", ""]


def test_run_full_backtest_retries_with_local_profile_on_missing_curve_message(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_dir = tmp_path / "eval"
    artifact_dir.mkdir()
    profile_path = tmp_path / "profile.json"
    profile_path.write_text("{}", encoding="utf-8")
    config = _StubConfig(tmp_path)
    attempt = {
        "attempt_id": "run-attempt-missing-curve",
        "artifact_dir": str(artifact_dir),
        "profile_ref": "stale-cloud-ref",
        "profile_path": str(profile_path),
    }

    seen_profile_refs: list[str] = []

    def fake_run_full_backtest(cfg, candidate):
        seen_profile_refs.append(str(candidate.get("profile_ref") or ""))
        if len(seen_profile_refs) == 1:
            raise RuntimeError(
                "sensitivity-basket did not produce best-cell-path-detail.json. Files in output dir: []"
            )
        return {"curve_path": "curve.json", "result_path": "result.json"}

    monkeypatch.setattr(ar_main, "_run_full_backtest_for_attempt", fake_run_full_backtest)

    result = ar_main._run_full_backtest_with_retry(config, attempt)

    assert result["curve_path"] == "curve.json"
    assert result["retry_mode"] == "local_profile_reupload"
    assert seen_profile_refs == ["stale-cloud-ref", ""]


def test_run_full_backtest_salvages_outputs_after_timeout(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_dir = tmp_path / "eval"
    artifact_dir.mkdir()
    profile_path = tmp_path / "profile.json"
    profile_path.write_text("{}", encoding="utf-8")
    config = _StubConfig(tmp_path)
    attempt = {
        "attempt_id": "run-attempt-2",
        "artifact_dir": str(artifact_dir),
        "profile_ref": "cloud-ref",
        "profile_path": str(profile_path),
    }

    def fake_run(self, args, **kwargs):
        output_dir = Path(args[args.index("--output-dir") + 1])
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "best-cell-path-detail.json").write_text(
            json.dumps({"curve": {"points": []}}), encoding="utf-8"
        )
        (output_dir / "sensitivity-response.json").write_text(
            json.dumps({"data": {"aggregate": {"quality_score": {"score": 55.0}}}}),
            encoding="utf-8",
        )
        raise CliError("Command timed out after 1800s: fuzzfolio-agent-cli ...")

    monkeypatch.setattr(dashboard_mod.FuzzfolioCli, "run", fake_run)

    result = dashboard_mod._run_full_backtest_for_attempt(config, attempt)

    curve_path = artifact_dir / dashboard_mod.FULL_BACKTEST_CURVE_FILENAME
    result_path = artifact_dir / dashboard_mod.FULL_BACKTEST_RESULT_FILENAME
    assert result["curve_path"] == str(curve_path)
    assert result["result_path"] == str(result_path)
    assert curve_path.exists()
    assert result_path.exists()


def test_copy_full_backtest_outputs_surfaces_profile_not_found_from_result_json(
    tmp_path: Path,
) -> None:
    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir()
    sensitivity_output_dir = tmp_path / "out"
    sensitivity_output_dir.mkdir()
    (sensitivity_output_dir / "sensitivity-response.json").write_text(
        json.dumps({"error": {"message": "Profile not found"}}),
        encoding="utf-8",
    )

    try:
        dashboard_mod._copy_full_backtest_outputs(artifact_dir, sensitivity_output_dir)
    except RuntimeError as exc:
        assert "Profile not found" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError")
