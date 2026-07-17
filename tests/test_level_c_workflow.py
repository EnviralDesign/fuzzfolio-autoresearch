from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from autoresearch.__main__ import build_parser
from autoresearch.config import load_config
from autoresearch.instrument_universe import universe_provenance
from autoresearch.level_c_workflow import (
    STAGES,
    LevelCWorkflowError,
    _stage_receipt_path,
    audit_level_c,
    bootstrap_level_c,
    run_level_c_cutoff,
)
from autoresearch.level_c_operator import LevelCOperatorError


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _fixed_hash(character: str) -> str:
    return "sha256:" + character * 64


def _nested_report(path: Path) -> Path:
    geometry = [
        ("2021-06-29", "2024-06-28", "2024-07-14", "2025-01-13"),
        ("2021-12-30", "2024-12-29", "2025-01-14", "2025-07-13"),
        ("2022-06-29", "2025-06-28", "2025-07-14", "2026-01-13"),
        ("2022-12-30", "2025-12-29", "2026-01-14", "2026-07-13"),
    ]
    folds = [
        {
            "fold": {
                "fold_id": f"fold-{index:02d}",
                "train_start": train_start,
                "train_end": train_end,
                "test_start": test_start,
                "test_end": test_end,
                "embargo_days": 15,
            }
        }
        for index, (train_start, train_end, test_start, test_end) in enumerate(
            geometry, start=1
        )
    ]
    path.write_text(
        json.dumps(
            {
                "status": "complete",
                "campaign_id": "archived-completed-nested",
                "completed_at": "2026-07-16T12:00:00Z",
                "fold_results": folds,
            }
        ),
        encoding="utf-8",
    )
    return path


def _bootstrap_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    active = tmp_path / "runs"
    archive = tmp_path / "archived-runs"
    archive.mkdir(parents=True)
    catalog = archive / "attempt-catalog.sqlite"
    catalog.write_bytes(b"fixture catalog")
    controls = archive / "legacy-controls.json"
    controls.write_text('{"status":"frozen"}', encoding="utf-8")
    report = _nested_report(archive / "nested-evidence-report.json")
    base_config = load_config()
    (tmp_path / "portfolio.research-suites.json").write_bytes(
        (base_config.repo_root / "portfolio.research-suites.json").read_bytes()
    )
    config = replace(
        base_config,
        repo_root=tmp_path,
        fuzzfolio=replace(
            base_config.fuzzfolio,
            workspace_root=Path("C:/repos/Trading-Dashboard"),
        ),
    )
    monkeypatch.setattr("autoresearch.level_c_operator.load_config", lambda: config)
    universe = universe_provenance()
    arguments = {
        "config": config,
        "active_runs_root": active,
        "archive_root": archive,
        "archived_attempt_catalog": catalog,
        "archived_attempt_catalog_sha256": _sha256(catalog),
        "legacy_controls": controls,
        "legacy_controls_sha256": _sha256(controls),
        "completed_nested_report": report,
        "completed_nested_report_sha256": _sha256(report),
        "archive_id": "manual-atomic-archive-001",
        "new_generation_id": "level-c-generation-001",
        "lake_semantic_sha256": _fixed_hash("a"),
        "source_snapshot_sha256": _fixed_hash("b"),
        "universe_id": str(universe["universe_id"]),
        "universe_manifest_sha256": str(universe["universe_hash"]),
        "worker_contract_id": "replay-worker-contract-v1",
        "worker_contract_sha256": _fixed_hash("c"),
        "worker_image": "example.invalid/replay-worker:immutable",
        "global_seed": 73,
    }
    result = bootstrap_level_c(**arguments)
    return config, active, archive, arguments, result


def test_bootstrap_is_exact_idempotent_and_does_not_inventory_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, active, archive, arguments, first = _bootstrap_fixture(tmp_path, monkeypatch)
    (archive / "unrelated-71-gib-placeholder.bin").write_bytes(b"not inspected")

    second = bootstrap_level_c(**arguments)

    assert second == first
    assert first["operator_semantics"] == {
        "suite_name": "darwin-master-v1",
        "suite_config_relative_path": "portfolio.research-suites.json",
        "suite_config_sha256": _sha256(tmp_path / "portfolio.research-suites.json"),
        "selection_basis": "recommended_cell",
        "optimizer_backend": "python",
    }
    assert set(first["execution_plans"]) == set("ABCD")
    assert audit_level_c(config=config, active_runs_root=active)["status"] == "valid"


def test_bootstrap_rejects_source_drift_and_ambiguous_partial_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, _, arguments, _ = _bootstrap_fixture(tmp_path / "drift", monkeypatch)
    arguments["legacy_controls"].write_text('{"status":"changed"}', encoding="utf-8")
    with pytest.raises(LevelCWorkflowError, match="hash mismatch"):
        bootstrap_level_c(**arguments)

    partial_repo = tmp_path / "partial"
    partial_repo.mkdir(parents=True)
    (partial_repo / "portfolio.research-suites.json").write_bytes(
        (arguments["config"].repo_root / "portfolio.research-suites.json").read_bytes()
    )
    partial_config = replace(arguments["config"], repo_root=partial_repo)
    monkeypatch.setattr("autoresearch.level_c_operator.load_config", lambda: partial_config)
    active = partial_config.runs_root
    control = active / "derived" / "level-c" / "control"
    control.mkdir(parents=True)
    (control / "protocol.json").write_text("{}", encoding="utf-8")
    partial_args = dict(arguments)
    partial_args["config"] = partial_config
    partial_args["active_runs_root"] = active
    with pytest.raises(LevelCWorkflowError, match="ambiguous bootstrap partial state"):
        bootstrap_level_c(**partial_args)


def test_runner_recovers_exactly_once_across_every_stage_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    attempts = {stage: 0 for stage in STAGES}
    artifacts = active / "derived" / "level-c" / "fixture-artifacts"

    def fault_once(stage: str, **_kwargs):
        artifacts.mkdir(parents=True, exist_ok=True)
        artifact = artifacts / f"{stage}.json"
        content = json.dumps({"stage": stage}, sort_keys=True)
        if artifact.exists():
            assert artifact.read_text(encoding="utf-8") == content
        else:
            artifact.write_text(content, encoding="utf-8")
        attempts[stage] += 1
        if attempts[stage] == 1:
            raise RuntimeError(f"injected crash after {stage} output")
        return "candidates_frozen" if stage == "frozen_cohort" else "complete", [artifact]

    handlers = {stage: fault_once for stage in STAGES}
    for expected_stage in STAGES:
        with pytest.raises(RuntimeError, match=f"after {expected_stage} output"):
            run_level_c_cutoff(
                config=config,
                active_runs_root=active,
                cutoff="A",
                resume=True,
                stage_handlers=handlers,
            )
        assert not _stage_receipt_path(active, "A", expected_stage).exists()

    result = run_level_c_cutoff(
        config=config,
        active_runs_root=active,
        cutoff="A",
        resume=True,
        stage_handlers=handlers,
    )

    assert result["status"] == "complete"
    assert result["completed_stages"] == list(STAGES)
    assert attempts == {stage: 2 for stage in STAGES}

    resumed = run_level_c_cutoff(
        config=config,
        active_runs_root=active,
        cutoff="A",
        resume=True,
        stage_handlers={stage: lambda **_kwargs: pytest.fail("completed stage reran") for stage in STAGES},
    )
    assert resumed["completed_stages"] == list(STAGES)


def test_runner_fails_closed_on_mutated_stage_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)

    def complete(stage: str, **_kwargs):
        artifact = active / "derived" / "level-c" / f"{stage}.txt"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text(stage, encoding="utf-8")
        return "candidates_frozen" if stage == "frozen_cohort" else "complete", [artifact]

    run_level_c_cutoff(
        config=config,
        active_runs_root=active,
        cutoff="A",
        resume=True,
        stage_handlers={stage: complete for stage in STAGES},
    )
    (active / "derived" / "level-c" / "atlas.txt").write_text("mutated", encoding="utf-8")

    with pytest.raises(LevelCWorkflowError, match="artifact drift"):
        run_level_c_cutoff(
            config=config,
            active_runs_root=active,
            cutoff="A",
            resume=True,
            stage_handlers={stage: complete for stage in STAGES},
        )

    (active / "derived" / "level-c" / "atlas.txt").write_text("atlas", encoding="utf-8")
    _stage_receipt_path(active, "A", "playhand").unlink()
    with pytest.raises(LevelCWorkflowError, match="missing predecessor"):
        run_level_c_cutoff(
            config=config,
            active_runs_root=active,
            cutoff="A",
            resume=True,
            stage_handlers={stage: complete for stage in STAGES},
        )


def test_runner_rejects_changed_portfolio_suite_after_bootstrap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    suite_path = tmp_path / "portfolio.research-suites.json"
    document = json.loads(suite_path.read_text(encoding="utf-8"))
    document["research_suites"]["darwin-master-v1"]["description"] = "semantic drift"
    suite_path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(LevelCOperatorError, match="authoritative sources"):
        run_level_c_cutoff(
            config=config,
            active_runs_root=active,
            cutoff="A",
            resume=True,
            stage_handlers={},
        )


def test_zero_candidate_cutoff_is_cleanly_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    called: list[str] = []

    def handler(stage: str, **_kwargs):
        called.append(stage)
        artifact = active / "derived" / "level-c" / f"zero-{stage}.json"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text("{}", encoding="utf-8")
        outcome = "no_defensible_candidates" if stage == "frozen_cohort" else "complete"
        return outcome, [artifact]

    result = run_level_c_cutoff(
        config=config,
        active_runs_root=active,
        cutoff="A",
        resume=True,
        stage_handlers={stage: handler for stage in STAGES},
    )

    assert result["status"] == "non_promotable"
    assert called == ["atlas", "playhand", "frozen_cohort"]


def test_audit_is_non_mutating(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config, active, _, _, _ = _bootstrap_fixture(tmp_path, monkeypatch)
    before = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in active.rglob("*")
        if path.is_file()
    }

    result = audit_level_c(config=config, active_runs_root=active)

    after = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in active.rglob("*")
        if path.is_file()
    }
    assert result["status"] == "valid"
    assert before == after


@pytest.mark.parametrize(
    ("command", "arguments"),
    [
        ("level-c-bootstrap", ["--help"]),
        ("level-c-run-cutoff", ["--help"]),
        ("level-c-audit", ["--help"]),
    ],
)
def test_level_c_cli_help(command: str, arguments: list[str]) -> None:
    parser = build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args([command, *arguments])
    assert exc.value.code == 0
