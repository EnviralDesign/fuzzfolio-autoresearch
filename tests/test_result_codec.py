from __future__ import annotations

import json
from pathlib import Path

import pytest

import autoresearch.result_codec as result_codec
from autoresearch.result_codec import (
    GZIP_JSON_CODEC,
    LEGACY_JSON_CODEC,
    ResultCodecError,
    gzip_json_bytes,
    read_json_object,
    write_gzip_json_once,
)
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_discovery_results import _result_files, load_stage_results


def _result() -> dict:
    metrics = {
        "observationsProcessed": 100,
        "tradesClosed": 1,
        "wins": 1,
        "losses": 0,
        "flatTrades": 0,
        "totalNetR": 1.0,
        "totalGrossR": 1.1,
        "maxDrawdownR": 0.2,
        "averageHoldingBars": 3.0,
        "exposureRatio": 0.1,
        "transitionEntropy": 0.5,
        "winRate": 1.0,
        "profitFactor": 2.0,
        "equityCurveR": [0.0, 1.0],
        "actionCounts": {},
        "closeReasonCounts": {},
        "stateOccupancy": {},
        "transitionCounts": {},
    }
    stream = "sha256:" + "a" * 64
    return {
        "schema_version": "temporal_graph_candidate_window_result_v1",
        "candidate_id": "candidate_a",
        "analysis_window_start": "2024-01-01T00:00:00Z",
        "analysis_window_end": "2024-02-01T00:00:00Z",
        "program_sha256": "sha256:" + "b" * 64,
        "observation_stream_sha256": stream,
        "cost_view_results": {
            "research_conservative": {
                "replay_result": {"streamSha256": stream, "metrics": metrics}
            },
            "none": {
                "replay_result": {"streamSha256": stream, "metrics": metrics}
            },
        },
    }


def test_gzip_codec_is_deterministic_and_reduces_repetitive_json() -> None:
    payload = {"z": ["same diagnostic row"] * 5000, "a": {"value": 1}}
    first_blob, first_metadata = gzip_json_bytes(payload)
    second_blob, second_metadata = gzip_json_bytes(
        {"a": {"value": 1}, "z": ["same diagnostic row"] * 5000}
    )

    assert first_blob == second_blob
    assert first_metadata == second_metadata
    assert first_metadata["codec"] == GZIP_JSON_CODEC
    assert first_metadata["blobSizeBytes"] < first_metadata["uncompressedSizeBytes"] * 0.02


def test_gzip_codec_write_once_verifies_and_rejects_divergence(tmp_path: Path) -> None:
    path = tmp_path / "results" / "candidate.json.gz"
    payload = {"rows": ["repeat"] * 100}
    metadata = write_gzip_json_once(path, payload)
    original = path.read_bytes()

    assert read_json_object(path, expected=metadata)[0] == payload
    assert write_gzip_json_once(path, payload) == metadata
    with pytest.raises(ResultCodecError, match="divergent immutable blob"):
        write_gzip_json_once(path, {"rows": ["changed"]})
    assert path.read_bytes() == original
    assert not list(path.parent.glob("*.tmp"))


def test_atomic_publish_never_replaces_a_racing_representation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "candidate.json.gz"
    competing, _ = gzip_json_bytes({"racing": "writer"})

    def lose_publish(source: Path, destination: Path) -> None:
        destination.write_bytes(competing)
        raise FileExistsError(destination)

    monkeypatch.setattr(result_codec.os, "link", lose_publish)
    with pytest.raises(ResultCodecError, match="divergent immutable blob"):
        write_gzip_json_once(path, {"our": "result"})
    assert path.read_bytes() == competing
    assert not list(path.parent.glob("*.tmp"))


def test_gzip_codec_rejects_corruption_and_bad_metadata(tmp_path: Path) -> None:
    path = tmp_path / "candidate.json.gz"
    metadata = write_gzip_json_once(path, {"result": [1, 2, 3]})
    with pytest.raises(ResultCodecError, match="result metadata mismatch"):
        read_json_object(
            path,
            expected={**metadata, "blobSizeBytes": metadata["blobSizeBytes"] + 1},
        )
    damaged = bytearray(path.read_bytes())
    damaged[-1] ^= 1
    path.write_bytes(damaged)

    with pytest.raises(ResultCodecError):
        read_json_object(path, expected=metadata)


def test_loader_enumerates_verified_gzip_and_legacy_json_without_siblings(
    tmp_path: Path,
) -> None:
    result_root = tmp_path / "run"
    results = result_root / "results"
    results.mkdir(parents=True)
    legacy = results / "legacy.json"
    legacy.write_text(json.dumps(_result(), indent=2) + "\n", encoding="utf-8")
    compressed = results / "compressed.json.gz"
    compressed_payload = _result()
    compressed_payload["candidate_id"] = "candidate_b"
    write_gzip_json_once(compressed, compressed_payload)

    files = _result_files(result_root)
    assert files == [compressed, legacy]
    assert read_json_object(legacy)[1]["codec"] == LEGACY_JSON_CODEC
    assert read_json_object(compressed)[1]["codec"] == GZIP_JSON_CODEC
    loaded = load_stage_results(result_root)
    assert set(loaded) == {"candidate_a", "candidate_b"}

    write_gzip_json_once(results / "legacy.json.gz", _result())
    with pytest.raises(TemporalDiscoveryContractError, match="ambiguous duplicate"):
        _result_files(result_root)
