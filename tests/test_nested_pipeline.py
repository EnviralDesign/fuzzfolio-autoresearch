from __future__ import annotations

import json
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

import autoresearch.nested_pipeline as pipeline
from autoresearch.corpus_lab_backtests import LabBacktestConfig
from autoresearch.nested_pipeline import (
    NestedPipelineContext,
    NestedPipelineError,
    prepare_nested_pipeline,
    run_nested_final_report_phase,
    run_nested_frozen_cells_phase,
    run_nested_frozen_portfolio_phase,
    run_nested_selected_outer_phase,
    run_nested_training_phase,
)


def _context(tmp_path: Path) -> NestedPipelineContext:
    config = SimpleNamespace(
        repo_root=tmp_path,
        runs_root=tmp_path / "runs",
        derived_root=tmp_path / "runs" / "derived",
    )
    fold = {
        "fold_id": "fold-01",
        "train_start": "2021-01-01",
        "train_end": "2023-12-31",
        "test_start": "2024-01-16",
        "test_end": "2024-07-15",
        "embargo_days": 15,
    }
    rows = (
        {"attempt_id": "attempt-a", "run_id": "run-a"},
        {"attempt_id": "attempt-b", "run_id": "run-b"},
    )
    return NestedPipelineContext(
        config=config,
        campaign_id="phase-test",
        campaign_plan_id="phase-test:execution-plan:sha256:plan",
        execution_plan_id="sha256:" + "a" * 64,
        suite_name="suite",
        suite={},
        account={},
        campaign_root=config.derived_root / "nested-evidence" / "phase-test",
        cohort_path=None,
        cohort_manifest_id="sha256:" + "b" * 64,
        requested_attempt_ids=("attempt-a", "attempt-b"),
        items=tuple(
            (config.runs_root / row["run_id"], dict(row), dict(row), {}) for row in rows
        ),
        catalog_rows=rows,
        folds=(fold,),
        train_months=36,
        test_months=6,
        selection_basis="recommended_cell",
        optimizer_backend="python",
        max_workers=2,
        lake_manifest_sha256="sha256:" + "c" * 64,
        lab_config=LabBacktestConfig(worker_contract_hash="sha256:" + "d" * 64),
        preview={
            "campaign_id": "phase-test",
            "attempt_count": 2,
            "fold_count": 1,
            "selection_basis": "recommended_cell",
        },
    )


def test_real_phased_adapter_has_no_early_outer_and_resumes_each_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _context(tmp_path)
    calls = {"training": 0, "cells": 0, "portfolio": 0, "outer": 0, "final": 0}

    def training(**kwargs):
        calls["training"] += 1
        assert kwargs["submit_outer"] is False if "submit_outer" in kwargs else True
        return {
            "status": "training_complete",
            "fold": context.folds[0],
            "strategy_count": 2,
            "requested_attempt_ids": ["attempt-a", "attempt-b"],
        }

    def cells(**_kwargs):
        calls["cells"] += 1
        return {
            "status": "cells_frozen",
            "fold": context.folds[0],
            "strategy_count": 2,
            "train_nonviable_count": 1,
            "records": [
                {
                    "attempt_id": "attempt-a",
                    "train_validation_status": "valid",
                    "cell_receipt": {"execution_cell_sha256": "sha256:" + "e" * 64},
                },
                {
                    "attempt_id": "attempt-b",
                    "train_validation_status": "nonviable",
                    "outer_validation_status": "not_applicable",
                },
            ],
        }

    def portfolio(*, freeze_only: bool = False, root: Path, **_kwargs):
        calls["portfolio" if freeze_only else "final"] += 1
        result = [
            {
                "fold": {"fold_id": "fold-01"},
                "selected_attempt_ids": ["attempt-a"],
            },
            {
                "fold": {"fold_id": "fold-01"},
                "selected_attempt_ids": ["attempt-a"],
            },
        ]
        root.mkdir(parents=True, exist_ok=True)
        (root / "nested-temporal-results.json").write_text(
            json.dumps(result, separators=(",", ":")), encoding="utf-8"
        )
        return result

    def outer(*, outer_selected_attempt_ids, **_kwargs):
        calls["outer"] += 1
        assert outer_selected_attempt_ids == ["attempt-a"]
        return {
            "status": "complete",
            "fold": context.folds[0],
            "strategy_count": 2,
            "train_nonviable_count": 1,
            "outer_nonviable_count": 0,
            "outer_failed_count": 0,
            "records": [
                {
                    "attempt_id": "attempt-a",
                    "train_validation_status": "valid",
                    "outer_validation_status": "valid",
                    "cell_receipt": {"execution_cell_sha256": "sha256:" + "e" * 64},
                },
                {
                    "attempt_id": "attempt-b",
                    "train_validation_status": "nonviable",
                    "outer_validation_status": "not_applicable",
                },
            ],
        }

    monkeypatch.setattr(pipeline, "run_nested_gateway_training_fold", training)
    monkeypatch.setattr(pipeline, "freeze_nested_gateway_cells_fold", cells)
    monkeypatch.setattr(pipeline, "run_nested_cell_temporal_validation", portfolio)
    monkeypatch.setattr(pipeline, "run_nested_gateway_selected_outer_fold", outer)

    run_nested_training_phase(context)
    assert calls == {"training": 1, "cells": 0, "portfolio": 0, "outer": 0, "final": 0}
    assert not (context.campaign_root / "phases" / "selected-outer.json").exists()
    run_nested_frozen_cells_phase(context)
    assert calls["outer"] == 0
    run_nested_frozen_portfolio_phase(context)
    assert calls["outer"] == 0
    run_nested_selected_outer_phase(context)
    run_nested_final_report_phase(context)
    assert calls == {"training": 1, "cells": 1, "portfolio": 1, "outer": 1, "final": 1}

    for function in (
        run_nested_training_phase,
        run_nested_frozen_cells_phase,
        run_nested_frozen_portfolio_phase,
        run_nested_selected_outer_phase,
        run_nested_final_report_phase,
    ):
        function(context)
    assert calls == {"training": 1, "cells": 1, "portfolio": 1, "outer": 1, "final": 1}

    final_results = (
        context.campaign_root
        / "portfolio-validation"
        / "final"
        / "nested-temporal-results.json"
    )
    final_results.write_text("[]", encoding="utf-8")
    with pytest.raises(NestedPipelineError, match="final portfolio artifact drift"):
        run_nested_final_report_phase(context)

    frozen_results = (
        context.campaign_root
        / "portfolio-validation"
        / "frozen"
        / "nested-temporal-results.json"
    )
    frozen_results.write_text("[]", encoding="utf-8")
    with pytest.raises(NestedPipelineError, match="frozen portfolio artifact drift"):
        run_nested_frozen_portfolio_phase(context)


def test_frozen_portfolio_treats_empty_variant_results_as_no_consensus(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _context(tmp_path)

    monkeypatch.setattr(
        pipeline,
        "run_nested_gateway_training_fold",
        lambda **_kwargs: {
            "status": "training_complete",
            "fold": context.folds[0],
            "requested_attempt_ids": ["attempt-a", "attempt-b"],
        },
    )
    monkeypatch.setattr(
        pipeline,
        "freeze_nested_gateway_cells_fold",
        lambda **_kwargs: {
            "status": "cells_frozen",
            "fold": context.folds[0],
            "records": [
                {"attempt_id": attempt_id, "train_validation_status": "valid"}
                for attempt_id in ("attempt-a", "attempt-b")
            ],
        },
    )

    def no_consensus(*, root: Path, **_kwargs):
        result = [
            {
                "fold": {"fold_id": "fold-01"},
                "status": "no_defensible_consensus",
                "selected_attempt_ids": [],
            }
        ]
        root.mkdir(parents=True, exist_ok=True)
        (root / "nested-temporal-results.json").write_text(
            json.dumps(result, separators=(",", ":")), encoding="utf-8"
        )
        return result

    monkeypatch.setattr(pipeline, "run_nested_cell_temporal_validation", no_consensus)
    run_nested_training_phase(context)
    run_nested_frozen_cells_phase(context)
    phase_path = run_nested_frozen_portfolio_phase(context)

    phase = json.loads(phase_path.read_text(encoding="utf-8"))
    assert phase["status"] == "no_consensus"
    assert phase["selected_attempt_ids_by_fold"] == {"fold-01": []}


def _prepare_config(tmp_path: Path):
    return SimpleNamespace(
        repo_root=tmp_path,
        runs_root=tmp_path / "runs",
        derived_root=tmp_path / "runs" / "derived",
        fuzzfolio=SimpleNamespace(workspace_root=tmp_path / "Trading-Dashboard"),
    )


def _patch_prepare_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    *,
    rows: list[dict],
    live_contract: str,
) -> None:
    monkeypatch.setattr(
        pipeline,
        "_cohort_attempts",
        lambda _path, _root, **_kwargs: (["attempt-a"], {"manifest_id": "cohort"}),
    )
    monkeypatch.setattr(pipeline, "load_research_suite", lambda *_args: ({}, {}))
    monkeypatch.setattr(
        pipeline,
        "temporal_folds",
        lambda **_kwargs: [
            {
                "fold_id": "fold-01",
                "train_start": "2021-01-01",
                "train_end": "2023-12-31",
                "test_start": "2024-01-16",
                "test_end": "2024-07-15",
                "embargo_days": 15,
            }
        ],
    )
    monkeypatch.setattr(pipeline, "iter_catalog_rows", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(pipeline, "load_run_metadata", lambda _path: {})
    monkeypatch.setattr(pipeline, "_resolve_account", lambda *_args: {})
    monkeypatch.setattr(pipeline, "validate_profile_model_source_lock", lambda *_args: {})
    monkeypatch.setattr(pipeline, "_live_worker_contract", lambda _root: live_contract)


def _prepare(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **overrides):
    tmp_path.mkdir(parents=True, exist_ok=True)
    config = _prepare_config(tmp_path)
    cohort = tmp_path / "cohort.json"
    cohort.write_text("{}", encoding="utf-8")
    arguments = {
        "config": config,
        "campaign_id": "campaign",
        "suite_name": "suite",
        "suite_config_path": None,
        "run_ids": None,
        "attempt_ids": None,
        "scope": "all",
        "start": "2021-01-01",
        "end": "2024-07-16",
        "train_months": 36,
        "test_months": 6,
        "step_months": 6,
        "embargo_days": 15,
        "selection_basis": "recommended_cell",
        "max_workers": 2,
        "gateway_url": None,
        "gateway_token": None,
        "lake_manifest_sha256": "sha256:" + "a" * 64,
        "trading_dashboard_root": None,
        "optimizer_backend": "python",
        "attempt_cohort": cohort,
        "execution_plan_id": "sha256:" + "b" * 64,
        "bound_worker_contract_hash": "sha256:" + "c" * 64,
        "profile_model_source_lock": {},
    }
    arguments.update(overrides)
    return prepare_nested_pipeline(**arguments)


def test_prepare_uses_configured_root_when_cli_root_is_omitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contract = "sha256:" + "c" * 64
    _patch_prepare_dependencies(
        monkeypatch,
        rows=[{"attempt_id": "attempt-a", "run_id": "run-a"}],
        live_contract=contract,
    )
    captured = {}

    def lab_config(**kwargs):
        captured.update(kwargs)
        return LabBacktestConfig(worker_contract_hash=kwargs["worker_contract_hash"])

    monkeypatch.setattr(pipeline, "resolve_lab_backtest_config", lab_config)
    context = _prepare(tmp_path, monkeypatch)

    assert captured["trading_dashboard_root"] == (tmp_path / "Trading-Dashboard").resolve()
    assert context.lab_config.worker_contract_hash == contract
    assert context.execution_plan_id in context.campaign_plan_id


def test_prepare_passes_worker_ready_resolver_to_level_c_cohort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contract = "sha256:" + "c" * 64
    _patch_prepare_dependencies(
        monkeypatch,
        rows=[{"attempt_id": "attempt-a", "run_id": "run-a"}],
        live_contract=contract,
    )
    expected_resolver = object()
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        pipeline,
        "_worker_ready_profile_snapshot_resolver",
        lambda **_kwargs: expected_resolver,
    )

    def cohort_attempts(_path, _runs_root, *, profile_snapshot_resolver=None):
        captured["resolver"] = profile_snapshot_resolver
        return ["attempt-a"], {"manifest_id": "cohort"}

    monkeypatch.setattr(pipeline, "_cohort_attempts", cohort_attempts)
    _prepare(tmp_path, monkeypatch)

    assert captured["resolver"] is expected_resolver


def test_prepare_materializes_worker_ready_profile_for_level_c_cohort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contract = "sha256:" + "c" * 64
    profile = tmp_path / "runs" / "run-a" / "profile.json"
    profile.parent.mkdir(parents=True, exist_ok=True)
    authoring_profile = {
        "format": "fuzzfolio.scoring-profile",
        "formatVersion": 1,
        "profile": {"name": "Bounded", "notificationThreshold": 80},
    }
    worker_ready_profile = {"name": "Bounded", "notificationThreshold": 80.0}
    profile.write_text(json.dumps(authoring_profile), encoding="utf-8")
    profile_sha256 = "sha256:" + hashlib.sha256(profile.read_bytes()).hexdigest()
    row = {
        "attempt_id": "attempt-a",
        "run_id": "run-a",
        "profile_path": str(profile),
    }
    _patch_prepare_dependencies(monkeypatch, rows=[row], live_contract=contract)
    monkeypatch.setattr(
        pipeline,
        "_cohort_attempts",
        lambda _path, _root, **_kwargs: (
            ["attempt-a"],
            {
                "schema": pipeline.LEVEL_C_COHORT_SCHEMA,
                "manifest_id": "cohort",
                "candidates": [
                    {
                        "attempt_id": "attempt-a",
                        "run_id": "run-a",
                        "profile_path_relative_to_runs_root": "run-a/profile.json",
                        "profile_sha256": profile_sha256,
                    }
                ],
            },
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "_worker_ready_profile_snapshot_resolver",
        lambda **_kwargs: lambda payload: (
            worker_ready_profile if payload == authoring_profile else pytest.fail("wrong profile")
        ),
    )

    context = _prepare(tmp_path, monkeypatch)

    assert context.items[0][1]["_worker_ready_profile_snapshot"] == worker_ready_profile


def test_prepare_rejects_worker_contract_drift_and_missing_cohort_member(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_prepare_dependencies(
        monkeypatch,
        rows=[{"attempt_id": "attempt-a", "run_id": "run-a"}],
        live_contract="sha256:" + "d" * 64,
    )
    with pytest.raises(NestedPipelineError, match="worker contract"):
        _prepare(tmp_path / "contract", monkeypatch)

    _patch_prepare_dependencies(monkeypatch, rows=[], live_contract="sha256:" + "c" * 64)
    with pytest.raises(NestedPipelineError, match=r"missing=\['attempt-a'\]"):
        _prepare(tmp_path / "missing", monkeypatch)


def test_prepare_rejects_alternate_trading_dashboard_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_prepare_dependencies(
        monkeypatch,
        rows=[{"attempt_id": "attempt-a", "run_id": "run-a"}],
        live_contract="sha256:" + "c" * 64,
    )
    with pytest.raises(NestedPipelineError, match="alternate Trading-Dashboard root"):
        _prepare(
            tmp_path,
            monkeypatch,
            trading_dashboard_root=tmp_path / "different-Trading-Dashboard",
        )
    with pytest.raises(NestedPipelineError, match="alternate Trading-Dashboard root"):
        _prepare(
            tmp_path,
            monkeypatch,
            bound_trading_dashboard_root=tmp_path / "plan-bound-other-root",
        )


def test_prepare_binds_level_c_cohort_run_and_profile_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile = tmp_path / "runs" / "run-a" / "profile.json"
    profile.parent.mkdir(parents=True, exist_ok=True)
    profile.write_text('{"profile":"frozen"}', encoding="utf-8")
    profile_sha256 = "sha256:" + hashlib.sha256(profile.read_bytes()).hexdigest()
    row = {
        "attempt_id": "attempt-a",
        "run_id": "run-b",
        "profile_path": str(profile),
    }
    _patch_prepare_dependencies(
        monkeypatch, rows=[row], live_contract="sha256:" + "c" * 64
    )
    monkeypatch.setattr(
        pipeline,
        "_cohort_attempts",
        lambda _path, _root, **_kwargs: (
            ["attempt-a"],
            {
                "schema": pipeline.LEVEL_C_COHORT_SCHEMA,
                "manifest_id": "cohort",
                "candidates": [
                    {
                        "attempt_id": "attempt-a",
                        "run_id": "run-a",
                        "profile_path_relative_to_runs_root": "run-a/profile.json",
                        "profile_sha256": profile_sha256,
                    }
                ],
            },
        ),
    )

    with pytest.raises(NestedPipelineError, match="catalog identity differs"):
        _prepare(tmp_path, monkeypatch)
