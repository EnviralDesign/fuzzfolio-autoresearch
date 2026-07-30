from __future__ import annotations

from argparse import Namespace
import json
from pathlib import Path

import pytest

from autoresearch.lake_window import LakeWindowBinding, LakeWindowRequest
from autoresearch import temporal_graph_lab_evidence_cli as evidence_cli
from autoresearch.evidence_plan import canonical_sha256


def _profile() -> dict:
    return {
        "version": "v2",
        "name": "native-stage4-development",
        "description": "test",
        "isActive": False,
        "directionMode": "long",
        "instruments": ["EURUSD"],
        "indicators": [],
        "notificationThreshold": 80.0,
        "executionConfig": {
            "exitPolicy": {
                "sourceKind": "manual",
                "evidenceStatus": "none",
                "selectedCell": {
                    "stopLossPercent": 0.5,
                    "rewardMultiple": 2.0,
                    "takeProfitPercent": 1.0,
                },
            },
            "sizingPolicy": {"mode": "inherit_global"},
        },
        "graph": {"kind": "temporal_graph_v1"},
    }


def _args(tmp_path: Path, profile_path: Path, *, confirmed: bool) -> Namespace:
    return Namespace(
        profile=profile_path,
        timeframe="M5",
        analysis_window_start="2026-01-01T00:00:00Z",
        analysis_window_end="2026-02-01T00:00:00Z",
        requested_horizon_months=1,
        selection_data_end=None,
        data_availability_cutoff=None,
        legacy_selection_manifest_sha256=None,
        campaign_plan_id=None,
        scope_resolution=None,
        timeout_seconds=30.0,
        evidence_plan_out=tmp_path / "evidence-plan.json",
        authority_out=tmp_path / "window-authority.json",
        window_authority_note="Confirmed against the development evidence registry.",
        confirm_non_reserved_development_window=confirmed,
    )


def test_freeze_requires_explicit_non_reserved_confirmation(tmp_path: Path) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile()), encoding="utf-8")

    with pytest.raises(ValueError, match="confirm-non-reserved"):
        evidence_cli.freeze_temporal_graph_lab_evidence(
            _args(tmp_path, profile_path, confirmed=False)
        )


def test_freeze_writes_strict_v2_plan_and_authority_record(
    monkeypatch,
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile()), encoding="utf-8")
    request = LakeWindowRequest(
        pairs=["EURUSD"],
        timeframes=["M5"],
        data_start="2026-01-01T00:00:00Z",
        data_end="2026-02-01T00:00:00Z",
    )
    binding = LakeWindowBinding(
        request=request,
        window_semantic_sha256="sha256:" + "a" * 64,
        attestation_sha256="sha256:" + "b" * 64,
        creation_global_coverage_sha256="sha256:" + "c" * 64,
        creation_source_coverage_sha256="sha256:" + "d" * 64,
    )
    monkeypatch.setattr(
        evidence_cli,
        "resolve_replay_lake_window_request",
        lambda **kwargs: request,
    )
    monkeypatch.setattr(
        evidence_cli,
        "resolve_lake_window_binding",
        lambda request, **kwargs: binding,
    )

    result = evidence_cli.freeze_temporal_graph_lab_evidence(
        _args(tmp_path, profile_path, confirmed=True)
    )

    plan = json.loads((tmp_path / "evidence-plan.json").read_text(encoding="utf-8"))
    authority = json.loads(
        (tmp_path / "window-authority.json").read_text(encoding="utf-8")
    )
    assert plan["schema_version"] == "fuzzfolio.replay-evidence-plan.v2"
    assert plan["evidence_role"] == "development_parity"
    assert plan["lake_manifest_sha256"] is None
    assert (
        plan["lake_window_binding"]["window_semantic_sha256"]
        == binding.window_semantic_sha256
    )
    assert authority["confirmedNonReservedDevelopmentWindow"] is True
    assert authority["evidencePlanId"] == plan["plan_id"] == result["evidencePlanId"]


def test_freeze_uses_supplied_resolved_scope_without_sparse_fallback(
    monkeypatch,
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "profile.json"
    profile = _profile()
    profile_path.write_text(json.dumps(profile), encoding="utf-8")
    request = LakeWindowRequest(
        pairs=["EURUSD"],
        timeframes=["H1", "M5"],
        data_start="2025-12-23T00:00:00Z",
        data_end="2026-02-01T00:00:00Z",
    )
    scope = {
        "schemaVersion": "temporal_graph_lake_scope_resolution_v1",
        "evidenceProfileSnapshotSha256": canonical_sha256(profile),
        "temporalSourceProfileSha256": "sha256:" + "1" * 64,
        "resolvedProfileSnapshotSha256": "sha256:" + "2" * 64,
        "programSha256": "sha256:" + "3" * 64,
        "baseDecisionTimeframe": "M5",
        "analysisWindowStart": "2026-01-01T00:00:00Z",
        "analysisWindowEnd": "2026-02-01T00:00:00Z",
        "coveredWarmupMinutes": 12660,
        "catalogIndicatorRequirements": [],
        "lakeWindowRequest": request.canonical_payload(),
    }
    scope["resolutionSha256"] = canonical_sha256(
        {key: value for key, value in scope.items() if key != "resolutionSha256"}
    )
    scope_path = tmp_path / "scope.json"
    scope_path.write_text(json.dumps(scope), encoding="utf-8")
    binding = LakeWindowBinding(
        request=request,
        window_semantic_sha256="sha256:" + "a" * 64,
        attestation_sha256="sha256:" + "b" * 64,
        creation_global_coverage_sha256="sha256:" + "c" * 64,
        creation_source_coverage_sha256="sha256:" + "d" * 64,
    )
    monkeypatch.setattr(
        evidence_cli,
        "resolve_replay_lake_window_request",
        lambda **kwargs: pytest.fail("sparse scope fallback must not run"),
    )
    monkeypatch.setattr(
        evidence_cli,
        "resolve_lake_window_binding",
        lambda received, **kwargs: binding if received == request else pytest.fail("wrong request"),
    )
    args = _args(tmp_path, profile_path, confirmed=True)
    args.scope_resolution = scope_path

    evidence_cli.freeze_temporal_graph_lab_evidence(args)

    authority = json.loads((tmp_path / "window-authority.json").read_text(encoding="utf-8"))
    assert authority["profileSnapshotSha256"] == canonical_sha256(profile)
    assert authority["scopeResolutionSha256"] == scope["resolutionSha256"]
    assert authority["lakeWindowRequest"] == request.canonical_payload()


def test_scope_resolution_tamper_is_rejected(tmp_path: Path) -> None:
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_profile()), encoding="utf-8")
    args = _args(tmp_path, profile_path, confirmed=True)
    scope_path = tmp_path / "scope.json"
    scope_path.write_text(json.dumps({"schemaVersion": "temporal_graph_lake_scope_resolution_v1"}), encoding="utf-8")
    args.scope_resolution = scope_path
    with pytest.raises(ValueError, match="SHA-256"):
        evidence_cli.freeze_temporal_graph_lab_evidence(args)
