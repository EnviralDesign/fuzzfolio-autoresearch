from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from autoresearch.evidence_plan import build_replay_evidence_plan, canonical_sha256
from autoresearch.temporal_search import (
    TemporalSearchContractError,
    build_authority,
    build_task_matrix,
)
from autoresearch.temporal_search_preflight_cli import build_preparation


def _source_profile() -> dict:
    return {
        "version": "v2",
        "name": "Development profile",
        "description": "Test-only admitted development profile.",
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "indicators": [
            {
                "meta": {
                    "id": "RSI_MEAN_REVERSION",
                    "instanceId": "stretch_score_m5",
                },
                "config": {
                    "isActive": True,
                    "timeframe": "M5",
                    "lookbackBars": 1,
                    "useFormingBar": False,
                    "ranges": {"buy": [20.0, 40.0], "sell": [60.0, 80.0]},
                    "talibConfig": [],
                },
            }
        ],
        "graph": {"kind": "temporal_graph_v1"},
        "executionConfig": {
            "exitPolicy": {
                "evidenceStatus": "none",
                "selectedCell": {
                    "rewardMultiple": 2.0,
                    "stopLossPercent": 0.5,
                    "takeProfitPercent": 1.0,
                },
                "sourceKind": "manual",
            },
            "sizingPolicy": {"mode": "inherit_global"},
        },
    }


def _source_task(path: Path) -> None:
    profile = _source_profile()
    binding = {
        "schema_version": "fuzzfolio.market-data-window-binding.v1",
        "request": {
            "schema_version": "fuzzfolio.market-data-window-request.v1",
            "dataset": "bars",
            "pairs": ["EURUSD"],
            "timeframes": ["M5"],
            "data_start": "2021-07-30T00:00:00Z",
            "data_end": "2022-02-01T00:00:00Z",
            "coverage_policy": "require_complete",
        },
        "window_semantic_sha256": "sha256:" + "a" * 64,
        "semantic_contract_id": "fuzzfolio.canonical-bars.semantic-digest.v2",
    }
    plan = build_replay_evidence_plan(
        evidence_role="development_parity",
        selection_data_end="2022-02-01T00:00:00Z",
        analysis_window_start="2021-08-01T00:00:00Z",
        analysis_window_end="2022-02-01T00:00:00Z",
        requested_horizon_months=6,
        profile_snapshot=profile,
        campaign_plan_id="admitted-development-test",
        execution_cell_sha256=canonical_sha256(
            profile["executionConfig"]["exitPolicy"]["selectedCell"]
        ),
        lake_window_binding=binding,
        data_availability_cutoff="2022-02-01T00:00:00Z",
    ).model_dump(mode="json", exclude_none=False)
    path.write_text(
        json.dumps(
            {
                "payload": {
                    "inline_profile_snapshot": profile,
                    "timeframe": "M5",
                    "evidence_plan": plan,
                }
            }
        ),
        encoding="utf-8",
    )


def _args(source_task: Path, output_root: Path, *, confirmed: bool) -> argparse.Namespace:
    return argparse.Namespace(
        source_task=source_task,
        output_root=output_root,
        authority_label="stage5d-preflight-test",
        candidate_id="atr-management-preflight",
        window_id="development-window-a",
        bar_limit=5000,
        deadline_seconds=900.0,
        worker_contract_sha256="sha256:" + "b" * 64,
        prohibited_window_id="reserved-and-future",
        prohibited_window_start="2024-06-29T00:00:00Z",
        prohibited_window_end="2100-01-01T00:00:00Z",
        prohibited_reason="reserved holdout and all later evidence",
        confirm_non_reserved_development_window=confirmed,
    )


def test_preflight_requires_explicit_non_reserved_confirmation(tmp_path: Path) -> None:
    source_task = tmp_path / "request.json"
    _source_task(source_task)

    with pytest.raises(TemporalSearchContractError, match="confirm-non-reserved"):
        build_preparation(_args(source_task, tmp_path / "output", confirmed=False))


def test_preflight_builds_one_strict_atr_candidate_window_task(tmp_path: Path) -> None:
    source_task = tmp_path / "request.json"
    output_root = tmp_path / "output"
    _source_task(source_task)

    result = build_preparation(_args(source_task, output_root, confirmed=True))
    preparation = json.loads((output_root / "preparation.json").read_text(encoding="utf-8"))
    profile = json.loads((output_root / "candidate-profile.json").read_text(encoding="utf-8"))
    plan = json.loads((output_root / "evidence-plan.json").read_text(encoding="utf-8"))

    assert result["taskCount"] == 1
    assert result["sourceProfileSha256"] == canonical_sha256(profile)
    assert preparation["candidates"][0]["sourceProfileSha256"] == canonical_sha256(profile)
    assert plan["profile_snapshot_sha256"] == canonical_sha256(profile)
    assert plan["execution_cell_sha256"] is None
    assert profile["indicators"][-1]["meta"] == {
        "id": "ATR_VOLATILITY_FILTER",
        "instanceId": "atr_management_m5",
    }
    library = profile["executionConfig"]["managementLibrary"]
    assert library["scalarBindings"] == [
        {
            "availability": "completed_bar",
            "id": "atr_distance",
            "indicatorInstanceId": "atr_management_m5",
            "outputKey": "atr_raw",
            "valueKind": "price_distance",
        }
    ]
    tasks = build_task_matrix(build_authority(preparation))
    assert len(tasks) == 1
    assert tasks[0]["payload"]["execution_config_sha256"] == canonical_sha256(
        profile["executionConfig"]
    )
    assert "execution_cell" not in tasks[0]["payload"]
