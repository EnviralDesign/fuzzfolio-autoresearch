from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from autoresearch import __main__ as ar_main
from autoresearch.fixed_cohort import (
    FixedCohortError,
    freeze_fixed_corpus_cohort,
    validate_fixed_corpus_cohort,
)


def _write_source_campaign(tmp_path: Path) -> Path:
    campaign_root = tmp_path / "portfolio-campaign"
    inputs = campaign_root / "inputs"
    inputs.mkdir(parents=True)
    snapshot_path = inputs / "candidate-snapshot.json"
    snapshot = {
        "schema_version": 1,
        "candidate_count": 3,
        "candidates": [
            {"attempt_id": "alpha", "instruments": ["EURUSD"]},
            {"attempt_id": "bravo", "instruments": ["JP225"]},
            {"attempt_id": "charlie", "instruments": ["US500", "EURUSD"]},
        ],
    }
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    (inputs / "candidate-snapshot-manifest.json").write_text(
        json.dumps(
            {
                "path": str(snapshot_path),
                "sha256": hashlib.sha256(snapshot_path.read_bytes()).hexdigest(),
                "candidate_count": 3,
            }
        ),
        encoding="utf-8",
    )
    return campaign_root


def _freeze(tmp_path: Path) -> tuple[Path, dict]:
    campaign_root = _write_source_campaign(tmp_path)
    output_path = tmp_path / "derived" / "fixed-corpus-cohorts" / "darwin" / "cohort.json"
    return output_path, freeze_fixed_corpus_cohort(
        campaign_root=campaign_root, cohort_id="darwin", output_path=output_path
    )


def test_freeze_is_idempotent_and_records_deterministic_exclusions(tmp_path: Path) -> None:
    output_path, first = _freeze(tmp_path)
    second = freeze_fixed_corpus_cohort(
        campaign_root=tmp_path / "portfolio-campaign",
        cohort_id="darwin",
        output_path=output_path,
    )

    assert first == second
    assert first["attempt_ids"] == ["alpha"]
    assert [item["attempt_id"] for item in first["excluded_candidates"]] == [
        "bravo",
        "charlie",
    ]
    assert first["excluded_candidates"][0]["reasons"] == [
        {"code": "ineligible_instruments", "instruments": ["JP225"]}
    ]
    assert validate_fixed_corpus_cohort(output_path)["manifest_id"] == first["manifest_id"]


def test_validation_fails_closed_on_manifest_or_source_mutation(tmp_path: Path) -> None:
    output_path, _payload = _freeze(tmp_path)
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    payload["created"]["created_at"] = "tampered"
    output_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(FixedCohortError, match="authenticate"):
        validate_fixed_corpus_cohort(output_path)

    output_path, _payload = _freeze(tmp_path / "source-drift")
    snapshot_path = tmp_path / "source-drift" / "portfolio-campaign" / "inputs" / "candidate-snapshot.json"
    snapshot_path.write_text(snapshot_path.read_text(encoding="utf-8").replace("EURUSD", "GBPUSD", 1), encoding="utf-8")
    with pytest.raises(FixedCohortError, match="changed"):
        validate_fixed_corpus_cohort(output_path)


def test_duplicate_source_attempt_ids_are_rejected(tmp_path: Path) -> None:
    campaign_root = _write_source_campaign(tmp_path)
    snapshot_path = campaign_root / "inputs" / "candidate-snapshot.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot["candidates"][1]["attempt_id"] = "alpha"
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    manifest_path = campaign_root / "inputs" / "candidate-snapshot-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["sha256"] = hashlib.sha256(snapshot_path.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(FixedCohortError, match="duplicate attempt id"):
        freeze_fixed_corpus_cohort(
            campaign_root=campaign_root,
            cohort_id="duplicate",
            output_path=tmp_path / "out" / "cohort.json",
        )


def test_attempt_cohort_conflict_is_rejected_and_nested_identity_records_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    output_path, payload = _freeze(tmp_path)
    with pytest.raises(SystemExit, match="conflicts"):
        ar_main._resolve_attempt_cohort(
            attempt_cohort=output_path, attempt_ids=["not-alpha"]
        )

    config = SimpleNamespace(
        repo_root=tmp_path,
        derived_root=tmp_path / "derived",
        runs_root=tmp_path / "runs",
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)
    monkeypatch.setattr(
        ar_main,
        "iter_catalog_rows",
        lambda _config, **kwargs: [
            {
                "attempt_id": "alpha",
                "run_id": "run-alpha",
                "is_canonical_attempt": True,
            },
            {
                "attempt_id": "outside-cohort",
                "run_id": "run-outside",
                "is_canonical_attempt": True,
            },
        ]
        if kwargs["attempt_ids"] == ["alpha"]
        else (_ for _ in ()).throw(AssertionError("cohort filter was not applied")),
    )
    monkeypatch.setattr(ar_main, "load_run_metadata", lambda _path: {})
    import autoresearch.portfolio_research as portfolio_research

    monkeypatch.setattr(portfolio_research, "load_research_suite", lambda *_args: ({}, {}))
    monkeypatch.setattr(
        portfolio_research,
        "temporal_folds",
        lambda **_kwargs: [{"fold_id": "fold-1", "train_start": "2024-01-01", "train_end": "2024-12-31"}],
    )

    assert (
        ar_main.cmd_nested_evidence(
            campaign_id="nested-test",
            suite_name="unit",
            suite_config_path=None,
            run_ids=None,
            attempt_ids=None,
            attempt_cohort=output_path,
            scope="canonical",
            start="2024-01-01",
            end="2025-01-01",
            train_months=6,
            test_months=1,
            step_months=1,
            embargo_days=0,
            selection_basis="recommended_cell",
            max_workers=1,
            gateway_url=None,
            gateway_token=None,
            lake_url=None,
            lake_token=None,
            lake_manifest_sha256=None,
            trading_dashboard_root=None,
            optimizer_backend="python",
            dry_run=True,
            as_json=True,
        )
        == 0
    )
    preview = json.loads(capsys.readouterr().out)
    assert preview["attempt_count"] == 1
    assert preview["attempt_cohort_manifest_id"] == payload["manifest_id"]
    assert preview["evidence_campaign_plan_id"].endswith(payload["manifest_id"])


def test_calculate_full_backtests_canonical_scope_cannot_expand_attempt_cohort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_path, _payload = _freeze(tmp_path)
    config = SimpleNamespace(
        repo_root=tmp_path,
        runs_root=tmp_path / "runs",
        attempt_catalog_sqlite_path=tmp_path / "attempt-catalog.sqlite",
    )
    monkeypatch.setattr(ar_main, "load_config", lambda: config)
    monkeypatch.setattr(
        ar_main,
        "iter_catalog_rows",
        lambda _config, **_kwargs: [
            {"attempt_id": "alpha", "is_canonical_attempt": True},
            {"attempt_id": "outside-cohort", "is_canonical_attempt": True},
        ],
    )

    def assert_exact_filter(_config, **kwargs):
        assert kwargs["attempt_ids"] == ["alpha"]
        return []

    monkeypatch.setattr(ar_main, "_matched_attempt_items", assert_exact_filter)
    assert (
        ar_main.cmd_calculate_full_backtests(
            run_ids=None,
            attempt_ids=None,
            attempt_cohort=output_path,
            limit=None,
            max_workers=None,
            use_dev_sim_worker_count=False,
            require_scrutiny_36=False,
            force_rebuild=False,
            job_timeout_seconds=None,
            dry_run=True,
            as_json=True,
            scope="canonical",
            catalog_already_refreshed=True,
            emit_summary=False,
        )
        == 0
    )
