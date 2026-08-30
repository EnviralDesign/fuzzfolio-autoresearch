from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path

import pytest


SCRIPT = Path(__file__).parents[1] / "scripts" / "run_evolutionary_substrate_neighborhood.py"
SPEC = importlib.util.spec_from_file_location("neighborhood_runner", SCRIPT)
assert SPEC and SPEC.loader
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def artifact(path: Path, name: str) -> tuple[dict[str, object], dict[str, object]]:
    payload = path.read_bytes()
    portable = {
        "name": name,
        "rootRole": "v38RunRoot",
        "relativePath": name,
        "bytes": len(payload),
        "sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
    }
    return portable, {**portable, "absolutePath": str(path)}


def test_build_request_uses_resolved_fixture_artifacts(tmp_path: Path) -> None:
    frozen = tmp_path / "frozen.json"
    parent_material = tmp_path / "parents.jsonl"
    config = tmp_path / "config.json"
    frozen.write_text("{}", encoding="utf-8")
    parent_material.write_text("{}\n", encoding="utf-8")
    config.write_text(
        '{"operatorFamilyMatrix":{"schemaVersion":"temporal_qd_operator_family_matrix_v1","mode":"frozen_parent_one_change_v1","includeCrossover":false,"mutationDepth":1,"parents":[{"candidateId":"p","role":"archive"}]}}',
        encoding="utf-8",
    )
    bindings = [artifact(frozen, "frozen-authority"), artifact(parent_material, "v38-parent-material"), artifact(config, "v38-run-config")]
    fixture = {
        "schemaVersion": "evolutionary_substrate_existing_construction_fixture_v3",
        "fixtureSha256": "sha256:fixture",
        "manifestSha256": "sha256:" + "a" * 64,
        "contents": [portable for portable, _ in bindings],
    }
    resolver = {"fixtureSha256": fixture["fixtureSha256"], "contents": [local for _, local in bindings]}

    request = runner.build_request(fixture, resolver, 17)

    assert request["maxPlans"] == 17
    assert request["parents"] == [{"candidateId": "p", "role": "archive"}]
    assert request["frozenAuthorityPath"] == str(frozen)


def test_resolved_artifact_rejects_digest_drift(tmp_path: Path) -> None:
    path = tmp_path / "authority.json"
    path.write_text("{}", encoding="utf-8")
    portable, local = artifact(path, "frozen-authority")
    path.write_text("changed", encoding="utf-8")
    with pytest.raises(runner.NeighborhoodError, match="identity drifted"):
        runner.resolved_artifact({"contents": [portable]}, {"contents": [local]}, "frozen-authority")
