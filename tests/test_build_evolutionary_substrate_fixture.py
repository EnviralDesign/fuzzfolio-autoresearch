from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


SCRIPT = Path(__file__).parents[1] / "scripts" / "build_evolutionary_substrate_fixture.py"
SPEC = importlib.util.spec_from_file_location("fixture_builder", SCRIPT)
assert SPEC and SPEC.loader
fixture_builder = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fixture_builder)


def write_manifest(root: Path, name: str, digest: str) -> None:
    directory = root / "generations" / "generation-0003" / "proposal" / "native-batch" / "v5-proposal" / name
    directory.mkdir(parents=True)
    (directory / "manifest.json").write_text(
        json.dumps({"manifestSha256": f"sha256:{digest}"}), encoding="utf-8"
    )


def test_resolve_invocation_requires_one_manifest_identity_match(tmp_path: Path) -> None:
    digest = "a" * 64
    write_manifest(tmp_path, "other", "b" * 64)
    write_manifest(tmp_path, "expected", digest)

    assert fixture_builder.resolve_invocation(tmp_path, digest).name == "expected"


def test_resolve_invocation_rejects_zero_or_multiple_matches(tmp_path: Path) -> None:
    write_manifest(tmp_path, "other", "d" * 64)
    with pytest.raises(RuntimeError, match="exactly one retained invocation"):
        fixture_builder.resolve_invocation(tmp_path, "c" * 64)

    write_manifest(tmp_path, "first", "c" * 64)
    write_manifest(tmp_path, "second", "c" * 64)
    with pytest.raises(RuntimeError, match="found 2"):
        fixture_builder.resolve_invocation(tmp_path, "c" * 64)
