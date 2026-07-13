from __future__ import annotations

import json

import pytest

from autoresearch.evidence_artifacts import (
    build_evidence_artifact_manifest,
    evidence_artifact_paths,
    discover_evidence_artifact_bundles,
    validate_evidence_artifact_bundle,
    write_immutable_json,
)
from autoresearch.evidence_plan import build_replay_evidence_plan


PROFILE = {"name": "frozen", "notificationThreshold": 73, "indicators": []}


def _plan(*, horizon: int = 36, start: str = "2023-07-08T23:59:59Z"):
    return build_replay_evidence_plan(
        campaign_plan_id="campaign:test",
        evidence_role="full_backtest",
        selection_data_end="2026-07-08T23:59:59Z",
        analysis_window_start=start,
        analysis_window_end="2026-07-08T23:59:59Z",
        requested_horizon_months=horizon,
        profile_snapshot=PROFILE,
    )


def test_bundle_identity_separates_horizons_and_windows(tmp_path) -> None:
    first = evidence_artifact_paths(tmp_path, _plan())
    second = evidence_artifact_paths(
        tmp_path,
        _plan(horizon=60, start="2021-07-08T23:59:59Z"),
    )

    assert first.root != second.root
    assert first.root.parent == second.root.parent


def test_immutable_json_allows_idempotent_write_and_rejects_mutation(tmp_path) -> None:
    path = tmp_path / "artifact.json"
    write_immutable_json(path, {"b": 2, "a": 1})
    write_immutable_json(path, {"a": 1, "b": 2})

    with pytest.raises(RuntimeError, match="different content"):
        write_immutable_json(path, {"a": 2, "b": 2})


def test_bundle_validation_requires_complete_exact_plan(tmp_path) -> None:
    plan = _plan()
    paths = evidence_artifact_paths(tmp_path, plan)
    payloads = {
        "result": {"ok": True},
        "curve": {"curve": {"points": []}},
        "calendar_curve": {"ok": True},
        "recommended_curve": {"ok": True},
        "job": {"ok": True},
    }
    manifest = build_evidence_artifact_manifest(
        evidence_plan=plan,
        provenance={"attempt_id": "attempt-1"},
        execution_evidence=plan.model_dump(mode="json"),
        artifact_payloads=payloads,
    )
    for name, payload in payloads.items():
        write_immutable_json(getattr(paths, name), payload)
    write_immutable_json(paths.manifest, manifest)

    assert validate_evidence_artifact_bundle(tmp_path, plan)["status"] == "valid"

    payload = json.loads(paths.manifest.read_text(encoding="utf-8"))
    payload["requested_horizon_months"] = 60
    paths.manifest.write_text(json.dumps(payload), encoding="utf-8")
    validation = validate_evidence_artifact_bundle(tmp_path, plan)
    assert validation["status"] == "invalid"
    assert "horizon_mismatch" in validation["reason_codes"]

    paths.manifest.write_text(json.dumps(manifest), encoding="utf-8")
    paths.curve.write_text(json.dumps({"curve": {"points": [{"date": "x"}]}}), encoding="utf-8")
    validation = validate_evidence_artifact_bundle(tmp_path, plan)
    assert "curve_hash_mismatch" in validation["reason_codes"]


def test_discovery_returns_each_plan_qualified_bundle(tmp_path) -> None:
    plans = [
        _plan(),
        _plan(horizon=60, start="2021-07-08T23:59:59Z"),
    ]
    for plan in plans:
        paths = evidence_artifact_paths(tmp_path, plan)
        payloads = {
            "result": {"ok": True},
            "curve": {"curve": {"points": []}},
            "calendar_curve": {"ok": True},
            "recommended_curve": {"ok": True},
            "job": {"ok": True},
        }
        for name, payload in payloads.items():
            write_immutable_json(getattr(paths, name), payload)
        write_immutable_json(
            paths.manifest,
            build_evidence_artifact_manifest(
                evidence_plan=plan,
                provenance={},
                execution_evidence=plan.model_dump(mode="json"),
                artifact_payloads=payloads,
            ),
        )

    records = discover_evidence_artifact_bundles(tmp_path)

    assert {record["requested_horizon_months"] for record in records} == {36, 60}
    assert all(record["validation_status"] == "valid" for record in records)
