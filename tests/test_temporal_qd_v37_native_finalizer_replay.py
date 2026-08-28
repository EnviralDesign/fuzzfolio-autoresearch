from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from autoresearch.temporal_qd_v37_native_finalizer_replay import (
    NativeControlReplayError,
    _prepare_replay_input,
    run_native_control_replay,
)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )


def test_prepare_replay_copies_only_source_and_path_rebound_manifest(tmp_path: Path) -> None:
    historical = tmp_path / "historical"
    source = {"sourceSha256": "sha256:" + "a" * 64, "payload": {"stable": True}}
    manifest = {
        "schemaVersion": "temporal_qd_v5_fast_ephemeral_finalization_manifest_v1",
        "operation": "finalize_fast_ephemeral_rotating_generation",
        "runtimeAuthoritySha256": "sha256:" + "b" * 64,
        "sourcePath": "C:\\historical\\source.json",
        "sourceSha256": source["sourceSha256"],
        "resultPath": "fast-ephemeral-result.json",
    }
    manifest["manifestSha256"] = "sha256:" + hashlib.sha256(
        json.dumps(manifest, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    _write_json(historical / "source.json", source)
    _write_json(historical / "manifest.json", manifest)

    target = tmp_path / "fresh" / "native-finalization"
    prepared = _prepare_replay_input(historical_dir=historical, target_dir=target)

    assert prepared["sourceBytesMatchHistorical"] is True
    assert prepared["preExecutionFiles"] == ["manifest.json", "source.json"]
    assert (target / "source.json").read_bytes() == (historical / "source.json").read_bytes()
    replay_manifest = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
    assert replay_manifest["sourceSha256"] == manifest["sourceSha256"]
    assert replay_manifest["sourcePath"] != manifest["sourcePath"]
    assert replay_manifest["manifestSha256"] != manifest["manifestSha256"]


def test_native_control_rejects_binary_not_bound_to_runtime_authority(tmp_path: Path) -> None:
    v37 = tmp_path / "v37"
    run = v37 / "run" / "broad-v37"
    (run / "generations").mkdir(parents=True)
    _write_json(
        run / "native-finalization-authority.json",
        {
            "binaries": {
                "generationFinalizer": {"fileSha256": "sha256:" + "0" * 64}
            }
        },
    )
    binary = tmp_path / "not-the-finalizer.exe"
    binary.write_bytes(b"not a bound finalizer")

    with pytest.raises(NativeControlReplayError, match="runtime authority"):
        run_native_control_replay(
            v37_root=v37,
            finalizer=binary,
            output_dir=tmp_path / "fresh-control",
        )

    assert not (tmp_path / "fresh-control").exists()
