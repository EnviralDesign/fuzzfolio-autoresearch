from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from autoresearch.temporal_discovery_base import canonical_sha256
from scripts.temporal_qd_runtime_oracle_fixture import (
    FIXTURE_SCHEMA,
    RUNTIME_MANIFEST_SCHEMA,
    TRANSCRIPT_SCHEMA,
    _generator_source_identity,
    _portable_source_sha256,
    materialize_runtime_oracle_fixture,
)


def test_real_frozen_runtime_fixture_covers_native_authority_cases(tmp_path: Path) -> None:
    dashboard_root = Path(
        os.environ.get("FUZZFOLIO_DASHBOARD_ROOT", r"C:\repos\Trading-Dashboard")
    )
    required_dashboard_inputs = (
        dashboard_root / "shared/constants/indicators.json",
        dashboard_root / "scripts/temporal_search_validate_candidate.py",
    )
    if not all(path.is_file() for path in required_dashboard_inputs):
        pytest.skip(
            "authoritative runtime-oracle regeneration requires a local "
            "Trading-Dashboard checkout; CI verifies the committed identity-bound bundle"
        )
    fixture = materialize_runtime_oracle_fixture(
        tmp_path / "runtime-oracle", dashboard_root=dashboard_root
    )
    assert fixture["schemaVersion"] == FIXTURE_SCHEMA
    assert fixture["predeclaredScope"]["inScope"]["acceptable"] is True
    assert fixture["predeclaredScope"]["outOfScope"]["acceptable"] is False
    for name in ("richImmigrant", "sequentialMutationDepth2", "sequentialMutationDepth3", "sameSideCrossoverMaterialized"):
        assert fixture["cases"][name]["candidateIdentitySha256"].startswith("sha256:")
    assert fixture["cases"]["sameSideCrossoverRejected"]["operation"]["disposition"] == "operation_rejected"
    manifest = json.loads((tmp_path / "runtime-oracle" / "runtime-manifest.json").read_text(encoding="utf-8"))
    assert manifest["schemaVersion"] == RUNTIME_MANIFEST_SCHEMA
    assert canonical_sha256(manifest) == fixture["runtimeManifestSha256"]
    assert manifest["evidenceIdentityContextSha256"] == manifest["evidenceIdentityContext"]["predeclaredEvidenceContextSha256"]
    transcript = json.loads((tmp_path / "runtime-oracle" / "dashboard-jsonl-transcript.json").read_text(encoding="utf-8"))
    assert transcript["schemaVersion"] == TRANSCRIPT_SCHEMA
    assert {row["request"]["operation"] for row in transcript["records"]} == {"validate_candidate", "compile_bidirectional"}
    assert [row["request"]["requestId"] for row in transcript["records"]] == [f"runtime-oracle-{ordinal:04d}" for ordinal in range(len(transcript["records"]))]
    for row in transcript["records"]:
        assert row["response"]["requestId"] == row["request"]["requestId"]
    for name in ("richImmigrant", "sequentialMutationDepth2", "sequentialMutationDepth3", "sameSideCrossoverMaterialized"):
        case = fixture["cases"][name]
        assert case["proposalSha256"].startswith("sha256:")
        assert case["candidateIdentitySha256"].startswith("sha256:")
        assert case["pairExecutableSemanticSha256"].startswith("sha256:")
        assert case["funnel"]["funnelSha256"] == canonical_sha256({key: value for key, value in case["funnel"].items() if key != "funnelSha256"})
    assert fixture["fixtureSha256"] == canonical_sha256({key: value for key, value in fixture.items() if key != "fixtureSha256"})


def test_committed_runtime_oracle_bundle_is_compact_and_identity_bound() -> None:
    root = Path(__file__).parent / "fixtures" / "temporal_qd_runtime_oracle"
    fixture = json.loads((root / "fixture.json").read_text(encoding="utf-8"))
    manifest = json.loads((root / "runtime-manifest.json").read_text(encoding="utf-8"))
    transcript = json.loads((root / "dashboard-jsonl-transcript.json").read_text(encoding="utf-8"))

    assert sum(path.stat().st_size for path in root.iterdir()) < 1_500_000
    assert fixture["fixtureSha256"] == canonical_sha256({key: value for key, value in fixture.items() if key != "fixtureSha256"})
    assert fixture["runtimeManifestSha256"] == canonical_sha256(manifest)
    assert fixture["transcriptSha256"] == canonical_sha256(transcript)
    assert fixture["generatorSourceIdentity"] == _generator_source_identity()
    assert fixture["cases"]["sameSideCrossoverRejected"]["operation"]["disposition"] == "operation_rejected"


def test_generator_source_identity_is_checkout_line_ending_independent() -> None:
    assert _portable_source_sha256(b"alpha\nbeta\n") == _portable_source_sha256(
        b"alpha\r\nbeta\r\n"
    )
